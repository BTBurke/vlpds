//! Firehose: merges durable node-log streams into one seq-ordered stream.
//!
//! Each log (this node's, each live peer's, and any dead node's log still
//! being drained from S3) delivers durable batches and a watermark W_l (every
//! event with seq <= W_l has been delivered). The merger emits events with
//! seq <= min_l W_l in seq order, so the merged order is total, stable and the
//! same on every node.
//!
//! The merged stream starts at a floor F (the clock at startup): every log
//! delivers its events with seq > F (followers catch up from S3, see
//! remote.rs), the merger drops anything <= F, and cursors at or below F are
//! backfilled from S3 (backfill.rs). A log followed later starts at the
//! merger's position at that moment, so nothing above it is skipped either,
//! and nothing at or below it is ever delivered: a joining node acks
//! nothing until every peer follows its log and its seqs pass every such
//! floor (`Cluster::try_join`).

use crate::backfill::{Reader, SegCache};
use crate::events;
use crate::metrics;
use crate::nodelog::{LogBatch, Watermark};
use crate::segment::{self, LogObject};
use crate::slots::SlotRange;
use crate::stats::STATS;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

/// Where a log's watermark comes from: our own log, or a peer stream / S3
/// drain (the last watermark it reported).
#[derive(Clone)]
pub enum Source {
    Local(Arc<Watermark>),
    Remote(Arc<AtomicI64>),
}

impl Source {
    fn get(&self) -> i64 {
        match self {
            Source::Local(w) => w.get(),
            Source::Remote(a) => a.load(Ordering::Acquire),
        }
    }
}

pub struct MergedBatch {
    pub first: i64,
    pub last: i64,
    /// (seq, frame); each frame is a slice of `wire`
    pub events: Vec<(i64, Bytes)>,
    /// `wire.len()`
    pub bytes: usize,
    /// Wire bytes emitted through this batch since startup (subscriber lag
    /// is measured in these).
    pub end: u64,
    /// The events as consecutive binary websocket messages, written as-is
    /// to every subscriber. The frames are copied out of their segments, so
    /// the ring doesn't pin whole segment bodies.
    wire: Bytes,
    /// start of each event's message in `wire`
    offs: Vec<usize>,
    /// Computed by the first sharded subscriber to read the batch.
    slots: OnceLock<Vec<u16>>,
}

impl MergedBatch {
    /// `events` in seq order; `emitted` = wire bytes emitted before it.
    fn new(events: Vec<(i64, Bytes)>, emitted: u64) -> MergedBatch {
        let mut buf = Vec::with_capacity(events.iter().map(|(_, f)| f.len() + 10).sum());
        let mut offs = Vec::with_capacity(events.len());
        let mut payload = Vec::with_capacity(events.len());
        for (_, f) in &events {
            offs.push(buf.len());
            push_message(&mut buf, OP_BINARY, f);
            payload.push(buf.len() - f.len());
        }
        let wire = Bytes::from(buf);
        let events: Vec<(i64, Bytes)> =
            events.iter().zip(payload).map(|((seq, f), at)| (*seq, wire.slice(at..at + f.len()))).collect();
        MergedBatch {
            first: events[0].0,
            last: events[events.len() - 1].0,
            bytes: wire.len(),
            end: emitted + wire.len() as u64,
            events,
            wire,
            offs,
            slots: OnceLock::new(),
        }
    }

    fn start(&self) -> u64 {
        self.end - self.bytes as u64
    }

    fn wire_from(&self, i: usize) -> Bytes {
        self.wire.slice(self.offs[i]..)
    }

    fn slots(&self) -> &[u16] {
        self.slots.get_or_init(|| self.events.iter().map(|(_, f)| event_slot(f)).collect())
    }

    /// The messages of the events from index `i` on whose repo is in
    /// `range`: one slice of `wire` per run of consecutive matching events.
    /// Returns the slices and the number of events.
    fn wire_runs(&self, i: usize, range: &SlotRange) -> (Vec<std::io::IoSlice<'_>>, usize) {
        let slots = self.slots();
        let end = |j: usize| self.offs.get(j).copied().unwrap_or(self.wire.len());
        let (mut runs, mut n, mut j) = (Vec::new(), 0, i);
        while j < slots.len() {
            if !range.contains(slots[j]) {
                j += 1;
                continue;
            }
            let a = j;
            while j < slots.len() && range.contains(slots[j]) {
                j += 1;
            }
            n += j - a;
            runs.push(std::io::IoSlice::new(&self.wire[self.offs[a]..end(j)]));
        }
        (runs, n)
    }
}

/// The hash slot of an event's repo (`repo` of a #commit, `did` of the
/// others), read straight from the frame's DAG-CBOR without decoding it. A
/// frame without one (none are produced) counts as slot 0, so a sharded
/// stream union still carries it exactly once.
pub fn event_slot(frame: &[u8]) -> u16 {
    frame_did(frame).map(crate::slots::slot_of_bytes).unwrap_or(0)
}

fn frame_did(f: &[u8]) -> Option<&[u8]> {
    let mut i = 0;
    cbor_skip(f, &mut i, 0)?; // header
    let (major, n) = cbor_head(f, &mut i)?;
    if major != 5 {
        return None;
    }
    for _ in 0..n {
        let key = cbor_text(f, &mut i)?;
        if key == b"repo" || key == b"did" {
            return cbor_text(f, &mut i);
        }
        cbor_skip(f, &mut i, 0)?;
    }
    None
}

/// (major type, argument) of the item at `i`; definite lengths only (DAG-CBOR).
fn cbor_head(f: &[u8], i: &mut usize) -> Option<(u8, u64)> {
    let b = *f.get(*i)?;
    *i += 1;
    let n = match b & 0x1f {
        n @ 0..=23 => return Some((b >> 5, n as u64)),
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return None,
    };
    let bytes = f.get(*i..*i + n)?;
    *i += n;
    Some((b >> 5, bytes.iter().fold(0u64, |a, x| a << 8 | *x as u64)))
}

fn cbor_text<'a>(f: &'a [u8], i: &mut usize) -> Option<&'a [u8]> {
    let (major, n) = cbor_head(f, i)?;
    if major != 3 {
        return None;
    }
    let s = f.get(*i..i.checked_add(usize::try_from(n).ok()?)?)?;
    *i += s.len();
    Some(s)
}

fn cbor_skip(f: &[u8], i: &mut usize, depth: u32) -> Option<()> {
    if depth > 64 {
        return None;
    }
    let (major, n) = cbor_head(f, i)?;
    match major {
        2 | 3 => {
            let end = i.checked_add(usize::try_from(n).ok()?)?;
            if end > f.len() {
                return None;
            }
            *i = end;
        }
        4 => {
            for _ in 0..n {
                cbor_skip(f, i, depth + 1)?;
            }
        }
        5 => {
            for _ in 0..n.checked_mul(2)? {
                cbor_skip(f, i, depth + 1)?;
            }
        }
        6 => cbor_skip(f, i, depth + 1)?,
        _ => {}
    }
    Some(())
}

#[derive(Clone)]
pub struct Options {
    /// Bytes of merged batches kept in memory for cursors and slow readers.
    pub ring_bytes: usize,
    /// A live subscriber further than this behind the stream head gets
    /// ConsumerTooSlow and is closed (it resumes from its cursor).
    pub max_lag_bytes: usize,
    /// Read-ahead per cursor backfill, across all logs.
    pub readahead_bytes: usize,
    pub backfill_cache_bytes: usize,
    /// More wait for a slot: read-ahead memory is at most this x
    /// `readahead_bytes`.
    pub max_backfills: usize,
    /// Per client IP (IPv6: per /64); 0 = no cap.
    pub max_per_ip: usize,
    /// A write outside the live path (backfill, info frames, pongs) that
    /// makes no progress for this long drops the subscriber.
    pub write_idle: Duration,
    /// None = the caller's runtime.
    pub runtime: Option<tokio::runtime::Handle>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            ring_bytes: 64 << 20,
            max_lag_bytes: DEFAULT_MAX_LAG_BYTES,
            readahead_bytes: crate::backfill::DEFAULT_READAHEAD_BYTES,
            backfill_cache_bytes: crate::backfill::DEFAULT_CACHE_BYTES,
            max_backfills: DEFAULT_MAX_BACKFILLS,
            max_per_ip: DEFAULT_MAX_PER_IP,
            write_idle: DEFAULT_WRITE_IDLE,
            runtime: None,
        }
    }
}

pub const DEFAULT_MAX_BACKFILLS: usize = 16;
/// A relay may open one per `?shard=k/n` slice.
pub const DEFAULT_MAX_PER_IP: usize = 256;
pub const DEFAULT_WRITE_IDLE: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_LAG_BYTES: usize = 128 << 20;

/// The process-wide runtime for subscriber connections (subscribeRepos
/// fan-out, cursor backfills): their socket writes and frame copies stay off
/// the request runtime, so heavy fan-out can't stall writes. The first
/// caller's thread count wins.
pub fn runtime(threads: usize) -> tokio::runtime::Handle {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.max(1))
            .thread_name("firehose")
            .enable_all()
            .build()
            .expect("firehose runtime")
    })
    .handle()
    .clone()
}

pub struct Firehose {
    /// Bytes emitted so far (the newest batch's `end`): subscribers wait on it.
    head: watch::Sender<u64>,
    ring: RwLock<VecDeque<Arc<MergedBatch>>>,
    ring_bytes: AtomicI64,
    max_ring_bytes: i64,
    pub last_emitted: AtomicI64,
    pub sources: RwLock<HashMap<Arc<str>, Source>>,
    /// The ring holds every event with seq > ring_floor (the start floor
    /// until the ring evicts). Older cursors are backfilled from S3.
    ring_floor: AtomicI64,
    /// The merged stream's start floor F: events <= F are only served by the
    /// S3 backfill.
    start_floor: i64,
    /// Highest min watermark the merger has acted on: every event <= it (and
    /// > start_floor) of every followed log has been emitted.
    settled: AtomicI64,
    /// Set once the node log is known.
    pub store: RwLock<Option<crate::store::Store>>,
    max_queue_bytes: AtomicUsize,
    queued_bytes: AtomicUsize,
    /// Set (for good) when this node leaves the cluster: the merger emits
    /// nothing more (see `freeze`).
    frozen: AtomicBool,
    runtime: tokio::runtime::Handle,
    max_lag_bytes: u64,
    readahead_bytes: usize,
    backfill_cache: Arc<SegCache>,
    backfill_slots: Arc<tokio::sync::Semaphore>,
    max_per_ip: usize,
    per_ip: parking_lot::Mutex<HashMap<std::net::IpAddr, usize>>,
    write_idle: Duration,
    /// `settled`, for backfills waiting on it.
    settled_tx: watch::Sender<i64>,
}

impl Firehose {
    pub fn new(opts: Options) -> Arc<Firehose> {
        let floor = crate::nodelog::seq_floor(crate::tid::now_micros());
        Arc::new(Firehose {
            head: watch::channel(0).0,
            ring: RwLock::new(VecDeque::new()),
            ring_bytes: AtomicI64::new(0),
            max_ring_bytes: opts.ring_bytes as i64,
            last_emitted: AtomicI64::new(0),
            sources: RwLock::new(HashMap::new()),
            ring_floor: AtomicI64::new(floor),
            start_floor: floor,
            settled: AtomicI64::new(i64::MIN),
            store: RwLock::new(None),
            max_queue_bytes: AtomicUsize::new(DEFAULT_MERGE_QUEUE_BYTES),
            queued_bytes: AtomicUsize::new(0),
            frozen: AtomicBool::new(false),
            runtime: opts.runtime.unwrap_or_else(tokio::runtime::Handle::current),
            max_lag_bytes: opts.max_lag_bytes as u64,
            readahead_bytes: opts.readahead_bytes,
            backfill_cache: SegCache::new(opts.backfill_cache_bytes),
            backfill_slots: Arc::new(tokio::sync::Semaphore::new(opts.max_backfills.max(1))),
            max_per_ip: opts.max_per_ip,
            per_ip: Default::default(),
            write_idle: opts.write_idle,
            settled_tx: watch::channel(i64::MIN).0,
        })
    }

    /// Changes whenever a batch is emitted (its value: bytes emitted so far).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }

    pub fn min_watermark(&self) -> Option<i64> {
        self.sources.read().values().map(|s| s.get()).min()
    }

    /// Everything at or below this has been emitted (or is below the start
    /// floor): a log followed from now on only owes us its events above it.
    pub fn position(&self) -> i64 {
        self.start_floor.max(self.settled.load(Ordering::Acquire))
    }

    /// Registers a newly followed peer log: returns the floor its follower
    /// must deliver every event above, and its watermark (starting just
    /// below it). Taken under the sources lock, so no merger tick that
    /// ignored this log can settle past the floor afterwards.
    ///
    /// The watermark starts *below* the floor: at startup the S3 backfill
    /// serves events <= floor and waits for `settled` to reach the floor as
    /// proof that every log is durable up to it, which only the peer can
    /// vouch for (its first heartbeat, or a segment read back from S3).
    /// Otherwise a backfill could run while the peer still had segments in
    /// flight with seqs <= floor and skip them for good.
    pub fn add_remote(&self, log_id: &str) -> (i64, Arc<AtomicI64>) {
        let mut s = self.sources.write();
        let floor = self.position();
        let wm = Arc::new(AtomicI64::new(floor - 1));
        s.insert(log_id.into(), Source::Remote(wm.clone()));
        (floor, wm)
    }

    /// Adds (Some) or removes (None) a log's watermark source. Remove a log
    /// only after every one of its events has been handed to the merger.
    pub fn set_source(&self, log_id: &str, source: Option<Source>) {
        let mut s = self.sources.write();
        match source {
            Some(src) => {
                s.insert(log_id.into(), src);
            }
            None => {
                s.remove(log_id);
            }
        }
    }

    /// Stops the merger for good: called as a node leaves the cluster
    /// (graceful shutdown, before its lease is deleted). It no longer
    /// discovers joiners, and a node joining once our lease is gone neither
    /// counts nor greets us, so merging on could emit past a joiner's first
    /// events without them. Subscribers keep what was emitted; they resume
    /// elsewhere from their cursors when we close.
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    /// Over this many queued bytes, logs are spilled (see `spawn_merger`).
    pub fn set_max_queue_bytes(&self, n: usize) {
        self.max_queue_bytes.store(n, Ordering::Relaxed);
    }

    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    /// Merges every followed log's batches by seq.
    ///
    /// While one log holds the min watermark back (a dead peer whose fence
    /// hasn't been drained yet), every other log queues here. Past the byte
    /// budget the merger *spills* a log instead of queueing it: it ignores
    /// that log's batches from then on and later reads them back from its S3
    /// segments, one chunk at a time as the watermark lets them out, until it
    /// meets the live stream again. Memory stays near the budget however long
    /// the stall lasts; the merged order is unchanged (events are durable in
    /// S3 before any producer hands them to us).
    pub fn spawn_merger(self: &Arc<Self>, mut rx: mpsc::UnboundedReceiver<LogBatch>) {
        let fh = self.clone();
        // critical: a panic here fail-stops the node (lifecycle.rs)
        tokio::spawn(crate::lifecycle::critical("firehose_merger", async move {
            let mut logs: HashMap<Arc<str>, LogQ> = HashMap::new();
            // Everything at or below this has been emitted (or is below the
            // start floor): later events at or below it are late.
            let mut emitted = fh.start_floor;
            let mut total = 0usize;
            let mut tick = tokio::time::interval(Duration::from_millis(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut behind = false;
            let mut pushed = 0u64;
            loop {
                if !behind {
                    tick.tick().await;
                }
                behind = false;
                if fh.frozen.load(Ordering::Acquire) {
                    loop {
                        match rx.try_recv() {
                            Ok(_) => {}
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => return,
                        }
                    }
                    continue;
                }
                // Read the watermark *before* draining: anything at or below it
                // was sent to us before the watermark was published. Settle it
                // under the sources lock (see add_remote): from here on the
                // merger owes exactly the events <= w of the logs it saw.
                let w = {
                    let s = fh.sources.read();
                    let Some(w) = s.values().map(|s| s.get()).min() else {
                        continue;
                    };
                    if fh.settled.fetch_max(w, Ordering::AcqRel) < w {
                        fh.settled_tx.send_if_modified(|v| std::mem::replace(v, w) < w);
                    }
                    w
                };
                let max = fh.max_queue_bytes.load(Ordering::Relaxed);
                let store = fh.store.read().clone();
                let mut late = 0usize;
                loop {
                    let b = match rx.try_recv() {
                        Ok(b) => b,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    };
                    let lq = logs.entry(b.log_id.clone()).or_default();
                    match &lq.spill {
                        // already read back from S3, or will be
                        Some(sp) if b.ordinal != sp.next || sp.end || total >= max / 2 => continue,
                        Some(_) => {
                            // the read-back met the live stream: queue it again
                            tracing::info!(log_id = %b.log_id, ordinal = b.ordinal, "firehose merger: spilled log rejoined the live stream");
                            lq.spill = None;
                        }
                        None if total >= max && store.is_some() => {
                            tracing::warn!(log_id = %b.log_id, ordinal = b.ordinal, queued = total, "firehose merger: queue over budget, spilling log to S3 read-back");
                            metrics::FIREHOSE_SPILLS.inc();
                            lq.spill = Some(Spill { next: b.ordinal, loaded: lq.high, end: false, checked: None });
                            continue;
                        }
                        None => {}
                    }
                    total += lq.accept(b.events, emitted, fh.start_floor, &mut late);
                }
                let mut bound = w;
                if let Some(store) = &store {
                    let more;
                    (bound, more) = read_back(
                        store,
                        &mut logs,
                        w,
                        (max / 16).max(1),
                        emitted,
                        fh.start_floor,
                        &mut total,
                        &mut late,
                    )
                    .await;
                    behind |= more;
                }
                if late > 0 {
                    tracing::warn!(late, emitted, "firehose merger: dropped late events below the emitted watermark");
                }
                let mut out = Vec::new();
                for lq in logs.values_mut() {
                    while let Some((seq, f)) = lq.q.front() {
                        if *seq > bound {
                            break;
                        }
                        lq.bytes -= f.len();
                        total -= f.len();
                        out.push(lq.q.pop_front().unwrap());
                    }
                }
                emitted = emitted.max(bound);
                // forget logs that are gone and fully emitted
                {
                    let s = fh.sources.read();
                    logs.retain(|id, lq| {
                        !lq.q.is_empty() || s.contains_key(id) || lq.spill.as_ref().is_some_and(|sp| !sp.end)
                    });
                }
                fh.queued_bytes.store(total, Ordering::Relaxed);
                metrics::FIREHOSE_MERGE_QUEUE_BYTES.set(total as i64);
                if out.is_empty() {
                    continue;
                }
                out.sort_unstable_by_key(|(s, _)| *s);
                let batch = Arc::new(MergedBatch::new(out, pushed));
                pushed = batch.end;
                STATS.firehose_events.fetch_add(batch.events.len() as u64, Ordering::Relaxed);
                metrics::FIREHOSE_EVENTS.inc_by(batch.events.len() as u64);
                metrics::FIREHOSE_BATCH.observe(batch.events.len() as f64);
                metrics::FIREHOSE_EMIT_DELAY
                    .observe(crate::tid::now_micros().saturating_sub((batch.first >> 8) as u64) as f64 / 1e6);
                fh.push(batch);
            }
        }));
    }

    fn push(&self, batch: Arc<MergedBatch>) {
        {
            let mut ring = self.ring.write();
            self.ring_bytes.fetch_add(batch.bytes as i64, Ordering::Relaxed);
            ring.push_back(batch.clone());
            while self.ring_bytes.load(Ordering::Relaxed) > self.max_ring_bytes && ring.len() > 1 {
                let old = ring.pop_front().unwrap();
                self.ring_floor.fetch_max(old.last, Ordering::AcqRel);
                self.ring_bytes.fetch_sub(old.bytes as i64, Ordering::Relaxed);
            }
        }
        metrics::FIREHOSE_RING_BYTES.set(self.ring_bytes.load(Ordering::Relaxed));
        self.last_emitted.store(batch.last, Ordering::Release);
        self.head.send_replace(batch.end);
    }

    /// Batches with events with seq > `after` currently in the ring, and
    /// whether the ring still reaches back to `after` (false = some were
    /// already dropped).
    pub fn from_ring(&self, after: i64) -> (Vec<Arc<MergedBatch>>, bool) {
        let ring = self.ring.read();
        // seqs are gappy: completeness is about what was evicted, not adjacency.
        let complete = after >= self.ring_floor.load(Ordering::Acquire);
        let i = ring.partition_point(|b| b.last <= after);
        (ring.range(i..).cloned().collect(), complete)
    }

    /// subscribeRepos: answers the websocket handshake and serves the
    /// connection on the firehose runtime (see [`runtime`]).
    ///
    /// The upgrade is done by hand rather than with axum's `WebSocket` so a
    /// subscriber owns its raw socket: every event goes out as the batch's
    /// pre-built websocket messages (`MergedBatch::wire`), one write per
    /// batch shared byte-for-byte by every subscriber, instead of a framing
    /// pass, a sink send and a flush per event per subscriber.
    ///
    /// `shard` (vlpds extension, `?shard=k/n`): only events whose repo hashes
    /// into that slice of the slot space. Same seqs, order and cursors as the
    /// full stream (a cursor from either works on the other); the union of
    /// the n streams is the full stream.
    ///
    /// `client` (the trusted-proxy-resolved client address): at most
    /// `Options::max_per_ip` connections per address (IPv6: per /64), 429
    /// past it.
    pub fn upgrade(
        self: &Arc<Self>,
        mut req: axum::extract::Request,
        cursor: Option<i64>,
        shard: Option<SlotRange>,
        client: Option<std::net::IpAddr>,
    ) -> Response {
        let accept = match handshake(req.headers()) {
            Ok(a) => a,
            Err(e) => return e.into_response(),
        };
        let slot = match client.map(|ip| self.ip_slot(ip)) {
            Some(None) => {
                metrics::FIREHOSE_REJECTED.with_label_values(&["per_ip"]).inc();
                let body = serde_json::json!({"error": "RateLimitExceeded", "message": "too many subscribeRepos connections from this address"});
                return (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
            }
            Some(Some(s)) => Some(s),
            None => None,
        };
        let on_upgrade = hyper::upgrade::on(&mut req);
        let fh = self.clone();
        self.runtime.spawn(async move {
            let _slot = slot;
            match on_upgrade.await {
                Ok(up) => fh.serve(up, cursor, shard).await,
                Err(e) => tracing::debug!("subscribeRepos upgrade failed: {e}"),
            }
        });
        (
            StatusCode::SWITCHING_PROTOCOLS,
            [
                (header::CONNECTION, "upgrade".to_string()),
                (header::UPGRADE, "websocket".to_string()),
                (header::SEC_WEBSOCKET_ACCEPT, accept),
            ],
        )
            .into_response()
    }

    /// None = at the cap.
    fn ip_slot(self: &Arc<Self>, ip: std::net::IpAddr) -> Option<IpSlot> {
        if self.max_per_ip == 0 {
            return Some(IpSlot { fh: None, key: ip });
        }
        let key = ip_key(ip);
        let mut m = self.per_ip.lock();
        let n = m.entry(key).or_default();
        if *n >= self.max_per_ip {
            return None;
        }
        *n += 1;
        Some(IpSlot { fh: Some(self.clone()), key })
    }

    pub fn connections_from(&self, ip: std::net::IpAddr) -> usize {
        self.per_ip.lock().get(&ip_key(ip)).copied().unwrap_or(0)
    }

    async fn serve(self: Arc<Self>, up: hyper::upgrade::Upgraded, cursor: Option<i64>, shard: Option<SlotRange>) {
        use hyper_util::rt::TokioIo;
        use tokio::net::TcpStream;
        metrics::FIREHOSE_SUBSCRIBERS.inc();
        // Move the socket onto this runtime's reactor (it was accepted on
        // the request runtime), so its readiness events are ours too.
        let reason = match hyper_util::server::conn::auto::upgrade::downcast::<TokioIo<TcpStream>>(up) {
            Ok(parts) => match parts.io.into_inner().into_std().and_then(TcpStream::from_std) {
                Ok(tcp) => {
                    let (r, w) = tcp.into_split();
                    self.serve_conn(std::io::Cursor::new(parts.read_buf).chain(r), w, cursor, shard).await
                }
                Err(e) => {
                    tracing::debug!("subscribeRepos socket: {e}");
                    "client_gone"
                }
            },
            Err(up) => {
                tracing::debug!(
                    "subscribeRepos: upgraded connection isn't a plain TCP stream; serving it through hyper's IO"
                );
                let (r, w) = tokio::io::split(TokioIo::new(up));
                self.serve_conn(r, w, cursor, shard).await
            }
        };
        metrics::FIREHOSE_SUBSCRIBERS.dec();
        metrics::FIREHOSE_DISCONNECTS.with_label_values(&[reason]).inc();
    }

    async fn serve_conn<R, W>(&self, r: R, w: W, cursor: Option<i64>, shard: Option<SlotRange>) -> &'static str
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        let (ctl_tx, ctl) = mpsc::channel(8);
        let reader = tokio::spawn(read_client(r, ctl_tx));
        let mut out = Out { w, ctl, idle: self.write_idle };
        let reason = match self.stream(&mut out, cursor, shard).await {
            Ok(()) => "shutdown",
            Err(reason) => reason,
        };
        reader.abort();
        reason
    }

    async fn stream<W: AsyncWrite + Unpin>(
        &self,
        out: &mut Out<W>,
        cursor: Option<i64>,
        shard: Option<SlotRange>,
    ) -> Result<(), &'static str> {
        let mut head = self.head.subscribe();
        let mut last = match cursor {
            Some(c) => c,
            // the ring holds everything above its floor
            None => self.last_emitted.load(Ordering::Acquire).max(self.ring_floor.load(Ordering::Acquire)),
        };
        if let Some(c) = cursor {
            // seqs are time-based: a cursor beyond both the stream head and the
            // current clock can't have been issued by us
            let now = crate::nodelog::seq_floor(crate::tid::now_micros()) | 0xff;
            if c > self.last_emitted.load(Ordering::Acquire).max(now) {
                out.finish(&events::error_frame("FutureCursor", "cursor in the future")).await;
                return Err("future_cursor");
            }
            // older than the ring: stream it from the S3 segments first
            self.catch_up(out, &mut last, shard).await?;
        }
        // Live. A subscriber more than `allowance` bytes behind the head is
        // dropped: the configured bound, or (a cursor replaying the ring)
        // what it started with, so it may catch up but not fall further back.
        let mut allowance = None;
        // stream offset up to which this subscriber got the ring's batches
        // (None: it came from S3 or hasn't been sent any yet)
        let mut sent_to: Option<u64> = None;
        loop {
            head.borrow_and_update();
            let (batches, complete) = self.from_ring(last);
            if !complete {
                // The ring is a memory budget, not the lag rule: it can drop
                // batches a subscriber within its allowance hasn't been sent
                // (a ring smaller than the allowance; or a backfill handing
                // over right at the ring floor as the next batch evicts it).
                // Those catch up from S3 again; only one past its allowance
                // is too slow.
                let lag = sent_to.map(|p| self.head.borrow().saturating_sub(p));
                let within = lag.is_none_or(|l| l <= allowance.unwrap_or(self.max_lag_bytes));
                if within && self.store.read().is_some() {
                    self.catch_up(out, &mut last, shard).await?;
                    sent_to = None;
                    continue;
                }
                out.finish(&events::error_frame("ConsumerTooSlow", "fell behind the in-memory window")).await;
                return Err("too_slow");
            }
            if batches.is_empty() {
                tokio::select! {
                    r = head.changed() => {
                        if r.is_err() {
                            return Ok(());
                        }
                    }
                    c = out.ctl.recv() => out.control(c).await?,
                }
                continue;
            }
            let allowance = *allowance
                .get_or_insert_with(|| self.max_lag_bytes.max(self.head.borrow().saturating_sub(batches[0].start())));
            for b in &batches {
                let i = b.events.partition_point(|(seq, _)| *seq <= last);
                if i == b.events.len() {
                    continue;
                }
                let sent = match &shard {
                    None => {
                        let wire = b.wire_from(i);
                        out.send_live(&mut [std::io::IoSlice::new(&wire)], &mut head, b.start(), allowance).await?;
                        b.events.len() - i
                    }
                    // only the matching events: each run of them is one
                    // slice of the shared bytes, all written in one go
                    Some(range) => {
                        let (mut runs, n) = b.wire_runs(i, range);
                        if n > 0 {
                            out.send_live(&mut runs, &mut head, b.start(), allowance).await?;
                        }
                        n
                    }
                };
                metrics::FIREHOSE_SENT.inc_by(sent as u64);
                last = b.last;
                sent_to = Some(b.end);
                while let Ok(c) = out.ctl.try_recv() {
                    out.control(Some(c)).await?;
                }
            }
        }
    }

    /// Streams (`last`, ring floor] from the S3 segments until the ring
    /// reaches back to `last` (the floor moves while it backfills); history
    /// that's gone is skipped with an `OutdatedCursor` info.
    async fn catch_up<W: AsyncWrite + Unpin>(
        &self,
        out: &mut Out<W>,
        last: &mut i64,
        shard: Option<SlotRange>,
    ) -> Result<(), &'static str> {
        loop {
            if !self.backfill_to_ring(out, last, shard).await? {
                out.send(&info_frame("OutdatedCursor", OUTDATED_CURSOR)).await?;
                *last = (*last).max(self.ring_floor.load(Ordering::Acquire));
            }
            if *last >= self.ring_floor.load(Ordering::Acquire) {
                return Ok(());
            }
        }
    }

    /// Sends the events in (`last`, ring floor] from S3, once every log is
    /// durable up to the floor. Ok(false) = there's no store (nothing older
    /// than the ring exists): the caller skips to the ring. A backfill that
    /// keeps failing disconnects the subscriber.
    async fn backfill_to_ring<W: AsyncWrite + Unpin>(
        &self,
        out: &mut Out<W>,
        last: &mut i64,
        shard: Option<SlotRange>,
    ) -> Result<bool, &'static str> {
        let Some(store) = self.store.read().clone() else { return Ok(false) };
        let reader = Reader { store, cache: self.backfill_cache.clone(), readahead_bytes: self.readahead_bytes, shard };
        let (mut overtaken, mut failures) = (0u32, 0u32);
        // a running-backfill slot, taken once there is something to read
        let mut slot: Option<BackfillSlot> = None;
        loop {
            let floor = self.ring_floor.load(Ordering::Acquire);
            if *last >= floor {
                return Ok(true);
            }
            // right after startup the start floor can be ahead of a peer's
            // watermark: its events <= F may not be in S3 yet. Wait for the
            // merger to settle past it, answering the client meanwhile (and
            // noticing it leave).
            if self.settled.load(Ordering::Acquire) < floor {
                let mut settled = self.settled_tx.subscribe();
                tokio::select! {
                    _ = async { settled.wait_for(|s| *s >= floor).await.is_ok() } => {}
                    c = out.ctl.recv() => out.control(c).await?,
                    // the floor moves as the ring evicts: look again
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
            if slot.is_none() {
                slot = Some(self.backfill_slot(out).await?);
                continue; // the floor moved while it waited
            }
            // older than what log retention deleted: OutdatedCursor, then the
            // oldest events left (retention.rs raises this before deleting)
            match crate::retention::retained_floor(&reader.store).await {
                Ok(pruned) if *last < pruned => {
                    out.send(&info_frame("OutdatedCursor", OUTDATED_CURSOR)).await?;
                    *last = pruned;
                    continue;
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("reading the retained floor failed: {e:#}"),
            }
            let (tx, mut rx) = mpsc::channel(BACKFILL_CHANNEL);
            let (r, from) = (reader.clone(), *last);
            let mut job =
                AbortOnDrop(tokio::spawn(async move { crate::backfill::backfill_with(&r, from, floor, &tx).await }));
            let mut chunk = Vec::with_capacity(1024);
            let mut buf = Vec::new();
            while rx.recv_many(&mut chunk, 1024).await > 0 {
                buf.clear();
                for (_, f) in &chunk {
                    push_message(&mut buf, OP_BINARY, f);
                }
                out.write(&buf).await?;
                metrics::FIREHOSE_SENT.inc_by(chunk.len() as u64);
                metrics::FIREHOSE_BACKFILL_EVENTS.inc_by(chunk.len() as u64);
                *last = chunk.last().expect("non-empty").0;
                chunk.clear();
                while let Ok(c) = out.ctl.try_recv() {
                    out.control(Some(c)).await?;
                }
            }
            let err = match (&mut job.0).await {
                Ok(Ok(_)) => {
                    *last = (*last).max(floor); // everything <= floor that exists was sent
                    (overtaken, failures) = (0, 0);
                    continue;
                }
                Ok(Err(e)) => e,
                Err(e) => anyhow::anyhow!("backfill task: {e}"),
            };
            if err.downcast_ref::<crate::backfill::Pruned>().is_some() && overtaken < MAX_PRUNED_RETRIES {
                // Retention deleted segments the reader was walking. All of
                // them are <= the retained floor (raised before deleting):
                // past `last`, the check above sends OutdatedCursor; at or
                // below it, nothing we owe was deleted (a log's head below
                // the cursor pruned under the seek, e.g. a dead log wholly
                // below it): read again from `last`. Each retry needs a new
                // delete, so this ends.
                overtaken += 1;
                metrics::FIREHOSE_BACKFILL_RETRIES.with_label_values(&["pruned"]).inc();
                tracing::debug!(from, floor, "firehose backfill overtaken by retention, retrying: {err:#}");
                continue;
            }
            // Anything else (an S3 error) is retried a few times, then the
            // subscriber is disconnected to resume from its cursor: skipping
            // to the ring would drop stored events behind an OutdatedCursor.
            failures += 1;
            if failures >= BACKFILL_ATTEMPTS {
                tracing::warn!(from, floor, "firehose backfill failed, disconnecting: {err:#}");
                out.close(1011).await;
                return Err("backfill_failed");
            }
            metrics::FIREHOSE_BACKFILL_RETRIES.with_label_values(&["error"]).inc();
            tracing::warn!(from, floor, "firehose backfill failed, retrying: {err:#}");
            tokio::time::sleep(Duration::from_millis(100) * failures).await;
        }
    }
}

const BACKFILL_ATTEMPTS: u32 = 3;
/// Frames between a backfill reader and its subscriber's writer (they are
/// slices of segments the reader holds anyway).
const BACKFILL_CHANNEL: usize = 1024;

struct BackfillSlot(#[allow(dead_code)] tokio::sync::OwnedSemaphorePermit);

impl Drop for BackfillSlot {
    fn drop(&mut self) {
        metrics::FIREHOSE_BACKFILLS.with_label_values(&["running"]).dec();
    }
}

impl Firehose {
    /// Waits for a backfill slot, answering the client meanwhile.
    async fn backfill_slot<W: AsyncWrite + Unpin>(&self, out: &mut Out<W>) -> Result<BackfillSlot, &'static str> {
        let p = match self.backfill_slots.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let waiting = metrics::FIREHOSE_BACKFILLS.with_label_values(&["waiting"]);
                waiting.inc();
                let acquire = self.backfill_slots.clone().acquire_owned();
                tokio::pin!(acquire);
                let r = loop {
                    tokio::select! {
                        p = &mut acquire => break Ok(p.expect("never closed")),
                        c = out.ctl.recv() => {
                            if let Err(e) = out.control(c).await {
                                break Err(e);
                            }
                        }
                    }
                };
                waiting.dec();
                r?
            }
        };
        metrics::FIREHOSE_BACKFILLS.with_label_values(&["running"]).inc();
        Ok(BackfillSlot(p))
    }
}

struct IpSlot {
    fh: Option<Arc<Firehose>>,
    key: std::net::IpAddr,
}

impl Drop for IpSlot {
    fn drop(&mut self) {
        let Some(fh) = &self.fh else { return };
        let mut m = fh.per_ip.lock();
        if let Some(n) = m.get_mut(&self.key) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.key);
            }
        }
    }
}

/// The IPv4 address, or the IPv6 /64 (one host's usual allocation).
fn ip_key(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip.to_canonical() {
        std::net::IpAddr::V6(v6) => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(u128::from(v6) & !((1u128 << 64) - 1)))
        }
        v4 => v4,
    }
}

const MAX_PRUNED_RETRIES: u32 = 64;

const OUTDATED_CURSOR: &str = "cursor is older than the retained history; starting from the oldest available event";

pub const DEFAULT_MERGE_QUEUE_BYTES: usize = 256 << 20;

/// One log's events waiting in the merger.
#[derive(Default)]
struct LogQ {
    q: VecDeque<(i64, Bytes)>,
    bytes: usize,
    /// Highest seq accepted: drops duplicates (S3 catch-up overlapping a
    /// live stream).
    high: i64,
    spill: Option<Spill>,
}

/// A log the merger stopped queueing: its batches are read back from S3.
struct Spill {
    next: u64,
    /// every event of the log <= this is queued or emitted
    loaded: i64,
    /// read up to the log's fence
    end: bool,
    /// When `next` was last checked against the log's first ordinal.
    checked: Option<std::time::Instant>,
}

/// How often a spilled log's missing next segment is checked (one LIST) for
/// having been pruned.
const SPILL_PRUNE_CHECK: Duration = Duration::from_secs(1);

impl LogQ {
    /// Queues a log's events in seq order; returns the bytes added.
    fn accept(&mut self, events: Vec<(i64, Bytes)>, emitted: i64, start_floor: i64, late: &mut usize) -> usize {
        let mut added = 0;
        for (seq, frame) in events {
            if seq <= self.high {
                continue;
            }
            self.high = seq;
            // At or below what we already emitted: the start of a follower's
            // S3 catch-up (<= the start floor, backfill serves it), or a late
            // event (a log we weren't following yet, or a watermark that
            // overpromised), which live order can't take.
            if seq <= emitted {
                if seq > start_floor {
                    *late += 1;
                }
                continue;
            }
            added += frame.len();
            self.q.push_back((seq, frame));
        }
        self.bytes += added;
        added
    }
}

/// Reads spilled logs back from S3 up to `w`, a `chunk` of queued bytes at a
/// time. Returns the bound the merger may emit up to (what every spilled log
/// has loaded) and whether more is to be read once that is emitted.
#[allow(clippy::too_many_arguments)]
async fn read_back(
    store: &crate::store::Store,
    logs: &mut HashMap<Arc<str>, LogQ>,
    w: i64,
    chunk: usize,
    emitted: i64,
    start_floor: i64,
    total: &mut usize,
    late: &mut usize,
) -> (i64, bool) {
    let (mut bound, mut more) = (w, false);
    for (log_id, lq) in logs.iter_mut() {
        let Some(mut sp) = lq.spill.take() else { continue };
        let mut caught_up = sp.end || sp.loaded >= w;
        let mut failed = false;
        while !caught_up && lq.bytes < chunk {
            match crate::nodelog::read_object(store, log_id, sp.next).await {
                Ok(Some(LogObject::Segment(_, entries))) => {
                    metrics::FIREHOSE_SPILL_SEGMENTS.inc();
                    sp.next += 1;
                    if let Some(l) = entries.last().map(|e| e.seq) {
                        sp.loaded = sp.loaded.max(l);
                    }
                    *total += lq.accept(segment::events(entries), emitted, start_floor, late);
                    caught_up = sp.loaded >= w;
                }
                Ok(Some(LogObject::Fence { .. })) => {
                    sp.end = true;
                    caught_up = true;
                }
                // Not written: every event <= w of this log was PUT before w
                // was published, so all are loaded (w only covers the log's
                // gap-free prefix). Unless retention deleted it (the read-back
                // is a whole window behind): then it never appears, and live
                // batches only rejoin at `next`, so skip to the log's first
                // segment.
                Ok(None) => {
                    if sp.checked.is_none_or(|t| t.elapsed() >= SPILL_PRUNE_CHECK) {
                        sp.checked = Some(std::time::Instant::now());
                        match crate::backfill::first_ordinal(store, log_id).await {
                            Ok(Some(first)) if first > sp.next => {
                                tracing::warn!(%log_id, from = sp.next, to = first, "firehose merger: spilled log pruned ahead of its read-back; skipping");
                                sp.next = first;
                                continue;
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!(%log_id, "firehose merger: listing a spilled log failed: {e:#}"),
                        }
                    }
                    caught_up = true;
                }
                Err(e) => {
                    tracing::warn!(%log_id, ordinal = sp.next, "firehose merger: reading back a spilled log failed: {e:#}");
                    failed = true;
                    break;
                }
            }
        }
        if !caught_up {
            bound = bound.min(sp.loaded);
            // errors wait for the next tick
            more |= !failed;
        }
        lq.spill = Some(sp);
    }
    (bound, more)
}

// ---- subscriber connections (a minimal RFC 6455 server) ----

const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xa;

/// How long a subscriber being dropped gets to take its final frames.
const FINAL_GRACE: Duration = Duration::from_secs(10);

/// subscribeRepos takes no client data messages; larger ones are refused.
const MAX_CLIENT_MESSAGE: u64 = 1 << 20;

/// Appends one unmasked, final websocket message (server to client).
fn push_message(out: &mut Vec<u8>, op: u8, payload: &[u8]) {
    out.push(0x80 | op);
    match payload.len() {
        n if n < 126 => out.push(n as u8),
        n if n <= u16::MAX as usize => {
            out.push(126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
}

/// Returns the Sec-WebSocket-Accept value.
fn handshake(h: &HeaderMap) -> Result<String, (StatusCode, &'static str)> {
    let has = |name: header::HeaderName, token: &str| {
        h.get_all(name)
            .iter()
            .any(|v| v.to_str().is_ok_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token))))
    };
    if !has(header::CONNECTION, "upgrade") || !has(header::UPGRADE, "websocket") {
        return Err((StatusCode::BAD_REQUEST, "expected a websocket upgrade"));
    }
    if h.get(header::SEC_WEBSOCKET_VERSION).is_none_or(|v| v != "13") {
        return Err((StatusCode::UPGRADE_REQUIRED, "Sec-WebSocket-Version must be 13"));
    }
    let key = h.get(header::SEC_WEBSOCKET_KEY).ok_or((StatusCode::BAD_REQUEST, "missing Sec-WebSocket-Key"))?;
    Ok(tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes()))
}

enum Ctl {
    Ping(Vec<u8>),
    Close,
}

/// Ends on a close frame, EOF or a protocol error; dropping `ctl` tells the
/// writer the client is gone.
async fn read_client<R: AsyncRead + Unpin>(mut r: R, ctl: mpsc::Sender<Ctl>) {
    let res: std::io::Result<()> = async {
        loop {
            let mut h = [0u8; 2];
            r.read_exact(&mut h).await?;
            let op = h[0] & 0x0f;
            let len = match h[1] & 0x7f {
                126 => r.read_u16().await? as u64,
                127 => r.read_u64().await?,
                n => n as u64,
            };
            let mut mask = [0u8; 4];
            if h[1] & 0x80 != 0 {
                r.read_exact(&mut mask).await?;
            }
            if op & 0x8 != 0 {
                if len > 125 {
                    return Err(std::io::ErrorKind::InvalidData.into());
                }
                let mut p = vec![0u8; len as usize];
                r.read_exact(&mut p).await?;
                for (i, b) in p.iter_mut().enumerate() {
                    *b ^= mask[i % 4];
                }
                match op {
                    OP_CLOSE => {
                        let _ = ctl.try_send(Ctl::Close);
                        return Ok(());
                    }
                    OP_PING => {
                        let _ = ctl.try_send(Ctl::Ping(p));
                    }
                    _ => {}
                }
            } else {
                if len > MAX_CLIENT_MESSAGE {
                    return Err(std::io::ErrorKind::InvalidData.into());
                }
                tokio::io::copy(&mut (&mut r).take(len), &mut tokio::io::sink()).await?;
            }
        }
    }
    .await;
    if let Err(e) = res {
        tracing::trace!("subscriber read side ended: {e}");
    }
}

struct Out<W> {
    w: W,
    ctl: mpsc::Receiver<Ctl>,
    /// Longest a write outside the live path may go without progress.
    idle: Duration,
}

impl<W: AsyncWrite + Unpin> Out<W> {
    /// Writes `data`; a client that takes nothing for `idle` is dropped
    /// (the live path bounds lag instead, `send_live`).
    async fn write(&mut self, data: &[u8]) -> Result<(), &'static str> {
        let mut at = 0;
        while at < data.len() {
            match tokio::time::timeout(self.idle, self.w.write(&data[at..])).await {
                Ok(Ok(0)) | Ok(Err(_)) => return Err("client_gone"),
                Ok(Ok(n)) => at += n,
                Err(_) => return Err("write_stalled"),
            }
        }
        metrics::FIREHOSE_SENT_BYTES.inc_by(data.len() as u64);
        Ok(())
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), &'static str> {
        let mut m = Vec::with_capacity(payload.len() + 10);
        push_message(&mut m, OP_BINARY, payload);
        self.write(&m).await
    }

    /// Writes live data that starts at stream offset `pos`. While the write
    /// waits on the socket, every new batch re-checks the lag: past
    /// `allowance` bytes behind the head the subscriber is dropped with
    /// ConsumerTooSlow, so a stalled reader holds nothing but its place in
    /// the shared ring and can't slow anyone else.
    async fn send_live(
        &mut self,
        data: &mut [std::io::IoSlice<'_>],
        head: &mut watch::Receiver<u64>,
        pos: u64,
        allowance: u64,
    ) -> Result<(), &'static str> {
        let len: usize = data.iter().map(|d| d.len()).sum();
        let too_slow = {
            let mut write = std::pin::pin!(write_all_vectored(&mut self.w, data));
            loop {
                tokio::select! {
                    r = &mut write => {
                        r.map_err(|_| "client_gone")?;
                        break false;
                    }
                    r = head.changed() => {
                        if r.is_err() {
                            // the stream is shutting down: just finish
                            (&mut write).await.map_err(|_| "client_gone")?;
                            break false;
                        }
                        if head.borrow_and_update().saturating_sub(pos) <= allowance {
                            continue;
                        }
                        // finish the message in flight so the error frame
                        // can follow it
                        match tokio::time::timeout(FINAL_GRACE, &mut write).await {
                            Ok(Ok(())) => break true,
                            _ => return Err("too_slow"),
                        }
                    }
                }
            }
        };
        metrics::FIREHOSE_SENT_BYTES.inc_by(len as u64);
        if too_slow {
            self.finish(&events::error_frame(
                "ConsumerTooSlow",
                "fell too far behind the stream; reconnect with a cursor",
            ))
            .await;
            return Err("too_slow");
        }
        Ok(())
    }

    /// Best effort: a close frame with `code`, then the caller drops the
    /// connection.
    async fn close(&mut self, code: u16) {
        let mut m = Vec::with_capacity(4);
        push_message(&mut m, OP_CLOSE, &code.to_be_bytes());
        self.write_final(&m).await;
    }

    /// Best effort: a last message and a close frame, then the caller drops
    /// the connection.
    async fn finish(&mut self, payload: &[u8]) {
        let mut m = Vec::with_capacity(payload.len() + 12);
        push_message(&mut m, OP_BINARY, payload);
        push_message(&mut m, OP_CLOSE, &1000u16.to_be_bytes());
        self.write_final(&m).await;
    }

    async fn write_final(&mut self, m: &[u8]) {
        let _ = tokio::time::timeout(FINAL_GRACE, async {
            self.w.write_all(m).await?;
            self.w.shutdown().await
        })
        .await;
    }

    /// Answers the read side: a pong, or the close handshake (None = the
    /// client is gone).
    async fn control(&mut self, c: Option<Ctl>) -> Result<(), &'static str> {
        let mut m = Vec::new();
        match c {
            Some(Ctl::Ping(p)) => {
                push_message(&mut m, OP_PONG, &p);
                self.write(&m).await
            }
            Some(Ctl::Close) => {
                push_message(&mut m, OP_CLOSE, &1000u16.to_be_bytes());
                let _ = tokio::time::timeout(FINAL_GRACE, self.w.write_all(&m)).await;
                Err("client_closed")
            }
            None => Err("client_gone"),
        }
    }
}

/// Most slices per vectored write (IOV_MAX is 1024 on Linux and macOS).
const MAX_IOV: usize = 1024;

/// `write_all` over several slices: one writev per call where the socket
/// supports it (a single slice is a plain write).
async fn write_all_vectored<W: AsyncWrite + Unpin>(
    w: &mut W,
    mut bufs: &mut [std::io::IoSlice<'_>],
) -> std::io::Result<()> {
    std::io::IoSlice::advance_slices(&mut bufs, 0);
    while !bufs.is_empty() {
        let n = w.write_vectored(&bufs[..bufs.len().min(MAX_IOV)]).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        std::io::IoSlice::advance_slices(&mut bufs, n);
    }
    Ok(())
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn info_frame(name: &str, message: &str) -> Vec<u8> {
    use crate::cbor::*;
    let mut out = Vec::new();
    write_map_head(&mut out, 2);
    write_text(&mut out, "t");
    write_text(&mut out, "#info");
    write_text(&mut out, "op");
    write_uint(&mut out, 1);
    write_map_head(&mut out, 2);
    write_text(&mut out, "name");
    write_text(&mut out, name);
    write_text(&mut out, "message");
    write_text(&mut out, message);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentBuilder;
    use object_store::{ObjectStoreExt, PutPayload};

    /// event_slot finds the repo of every event kind (a #commit's `repo`
    /// sits after its ops), and wire_runs writes exactly the matching
    /// events' messages, one slice per run.
    #[test]
    fn event_slots_and_runs() {
        let cid = crate::cid::Cid::dag_cbor(b"x");
        let dids: Vec<String> = (0..64).map(crate::state::bulk_did).collect();
        let frame = |i: usize| -> Bytes {
            let did = dids[i].as_str();
            let f = match i % 4 {
                0 => {
                    let ops = [
                        events::RepoOp { action: "create", path: "app.bsky.feed.post/3k", cid: Some(cid), prev: None },
                        events::RepoOp {
                            action: "update",
                            path: "app.bsky.feed.like/3j",
                            cid: Some(cid),
                            prev: Some(cid),
                        },
                    ];
                    events::commit_frame(&events::CommitFrame {
                        repo: did,
                        rev: "3kabc",
                        since: Some("3kabb"),
                        commit: cid,
                        prev_data: Some(cid),
                        blocks: &[7u8; 300],
                        ops: &ops,
                        time: "2026-01-01T00:00:00Z",
                    })
                }
                1 => events::sync_frame(did, "3kabc", &[1, 2, 3], "t"),
                2 => events::identity_frame(did, "a.test", "t"),
                _ => events::account_frame(did, false, Some("takendown"), "t"),
            };
            let mut out = Vec::new();
            f.finish(1000 + i as i64, &mut out);
            Bytes::from(out)
        };
        let evs: Vec<(i64, Bytes)> = (0..dids.len()).map(|i| (1000 + i as i64, frame(i))).collect();
        for (i, (_, f)) in evs.iter().enumerate() {
            assert_eq!(event_slot(f), crate::slots::slot_of(&dids[i]), "event {i}");
        }
        assert_eq!(event_slot(b"\xa0"), 0);
        assert_eq!(event_slot(&evs[0].1[..20]), 0);
        let batch = MergedBatch::new(evs.clone(), 0);
        for n in [1u32, 2, 5] {
            for k in 0..n {
                let range = SlotRange::new(k, n).unwrap();
                for from in [0, 17] {
                    let (runs, count) = batch.wire_runs(from, &range);
                    let got: Vec<u8> = runs.iter().flat_map(|r| r.to_vec()).collect();
                    let want: Vec<(i64, Bytes)> =
                        evs[from..].iter().filter(|(_, f)| range.contains(event_slot(f))).cloned().collect();
                    let mut buf = Vec::new();
                    for (_, f) in &want {
                        push_message(&mut buf, OP_BINARY, f);
                    }
                    assert_eq!((got, count), (buf, want.len()), "{k}/{n} from {from}");
                    assert!(runs.len() <= count);
                }
            }
        }
    }

    /// Sharded fan-out cost per event (ignored; --ignored --nocapture):
    /// computing a batch's slots once, then each of 16 sharded subscribers
    /// picking its runs.
    #[test]
    #[ignore]
    fn sharded_filter_cost() {
        let cid = crate::cid::Cid::dag_cbor(b"x");
        let ops =
            [events::RepoOp { action: "create", path: "app.bsky.feed.post/3kabcdefghij2", cid: Some(cid), prev: None }];
        let evs: Vec<(i64, Bytes)> = (0..2000u64)
            .map(|i| {
                let did = crate::state::bulk_did(i);
                let f = events::commit_frame(&events::CommitFrame {
                    repo: &did,
                    rev: "3kabc",
                    since: Some("3kabb"),
                    commit: cid,
                    prev_data: Some(cid),
                    blocks: &[7u8; 1200],
                    ops: &ops,
                    time: "2026-01-01T00:00:00.000Z",
                });
                let mut out = Vec::new();
                f.finish(i as i64, &mut out);
                (i as i64, Bytes::from(out))
            })
            .collect();
        let rounds = 50;
        let (mut slots_t, mut runs_t) = (Duration::ZERO, Duration::ZERO);
        for _ in 0..rounds {
            let b = MergedBatch::new(evs.clone(), 0);
            let t = std::time::Instant::now();
            std::hint::black_box(b.slots());
            slots_t += t.elapsed();
            let t = std::time::Instant::now();
            for k in 0..16 {
                std::hint::black_box(b.wire_runs(0, &SlotRange::new(k, 16).unwrap()));
            }
            runs_t += t.elapsed();
        }
        let per = |d: Duration| d.as_nanos() as f64 / (rounds * evs.len()) as f64;
        eprintln!(
            "slots: {:.0} ns/event once per batch; runs for 16 sharded subscribers: {:.0} ns/event total",
            per(slots_t),
            per(runs_t)
        );
    }

    /// The next emitted batches after `last` (advanced past them).
    async fn next_batches(fh: &Firehose, sub: &mut watch::Receiver<u64>, last: &mut i64) -> Vec<Arc<MergedBatch>> {
        loop {
            sub.borrow_and_update();
            let (b, _) = fh.from_ring(*last);
            if let Some(l) = b.last() {
                *last = l.last;
                return b;
            }
            tokio::time::timeout(Duration::from_secs(5), sub.changed()).await.expect("merged stream stalled").unwrap();
        }
    }

    async fn put_seg(store: &crate::store::Store, log: &str, ord: u64, seq: i64, frame_len: usize) -> LogBatch {
        let frame = Bytes::from(vec![ord as u8; frame_len]);
        let mut b = SegmentBuilder::new();
        b.push(seq, crate::slots::ShardId(0), 1, |o| o.extend_from_slice(&frame), &[]);
        let mut obj = b.header(log, ord);
        obj.extend_from_slice(&b.body);
        store.raw.put(&crate::nodelog::segment_path(store, log, ord), PutPayload::from(obj)).await.unwrap();
        LogBatch { log_id: log.into(), ordinal: ord, events: vec![(seq, frame)] }
    }

    /// A spilled log whose next segment retention deleted (the read-back a
    /// whole window behind) skips to the log's first segment instead of
    /// waiting for it forever: its later events are emitted and its live
    /// batches rejoin.
    #[tokio::test]
    async fn spilled_log_skips_pruned_segments() {
        let store = crate::store::Store::memory(None);
        let fh = Firehose::new(Options::default());
        fh.set_max_queue_bytes(2000);
        *fh.store.write() = Some(store.clone());
        let (_, wa) = fh.add_remote("A");
        let (_, wb) = fh.add_remote("B");
        let (tx, rx) = mpsc::unbounded_channel();
        let mut sub = fh.subscribe();
        let mut last = i64::MIN;
        fh.spawn_merger(rx);
        let base = fh.position();
        let seq = |k: i64| base + k * 256 + 1;
        let n = 60u64;
        for k in 0..n {
            tx.send(put_seg(&store, "B", k, seq(k as i64), 200).await).unwrap();
            wb.store(seq(k as i64), Ordering::Release);
            if k % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        // retention deletes B's oldest segments, past where its read-back is
        for k in 0..=40u64 {
            store.raw.delete(&crate::nodelog::segment_path(&store, "B", k)).await.unwrap();
        }
        wa.store(seq(n as i64 + 10), Ordering::Release);
        let mut got = Vec::new();
        while got.last() != Some(&seq(n as i64 - 1)) {
            for b in next_batches(&fh, &mut sub, &mut last).await {
                got.extend(b.events.iter().map(|(s, _)| *s));
            }
        }
        assert!(got.windows(2).all(|w| w[0] < w[1]), "in order");
        // and B's live stream is taken again
        tx.send(put_seg(&store, "B", n, seq(n as i64), 200).await).unwrap();
        wb.store(seq(n as i64), Ordering::Release);
        let b = next_batches(&fh, &mut sub, &mut last).await;
        assert_eq!(b[0].events[0].0, seq(n as i64));
    }

    /// Writes outside the live path give up on a client that takes nothing
    /// for `idle`.
    #[tokio::test]
    async fn stalled_writes_drop_the_subscriber() {
        let (w, _r) = tokio::io::duplex(64);
        let (_ctl_tx, ctl) = mpsc::channel(1);
        let mut out = Out { w, ctl, idle: Duration::from_millis(100) };
        let t = std::time::Instant::now();
        assert_eq!(out.write(&[0u8; 4096]).await, Err("write_stalled"));
        assert!(t.elapsed() < Duration::from_secs(2));
        // a reader that keeps taking bytes is fine, however slowly
        let (w, mut r) = tokio::io::duplex(64);
        let (_ctl_tx, ctl) = mpsc::channel(1);
        let mut out = Out { w, ctl, idle: Duration::from_millis(100) };
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let mut n = 0;
            while n < 4096 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                n += r.read(&mut buf).await.unwrap();
            }
        });
        assert_eq!(out.write(&[0u8; 4096]).await, Ok(()));
        reader.await.unwrap();
    }

    /// Backfills beyond `max_backfills` wait for a slot (answering their
    /// client meanwhile), and one whose client leaves stops waiting.
    #[tokio::test]
    async fn backfills_wait_for_a_slot() {
        let fh = Firehose::new(Options { max_backfills: 1, ..Options::default() });
        let out = || {
            let (w, _r) = tokio::io::duplex(1 << 16);
            let (tx, ctl) = mpsc::channel(1);
            (Out { w, ctl, idle: Duration::from_secs(5) }, tx, _r)
        };
        let (mut a, _a_tx, _ar) = out();
        let first = fh.backfill_slot(&mut a).await.unwrap();
        // the second waits; its client leaving ends the wait
        let (mut b, b_tx, _br) = out();
        let fh2 = fh.clone();
        let waiting = tokio::spawn(async move { fh2.backfill_slot(&mut b).await.map(|_| ()) });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());
        drop(b_tx);
        assert_eq!(waiting.await.unwrap(), Err("client_gone"));
        // a third gets the slot once the first ends
        let (mut c, _c_tx, _cr) = out();
        let fh3 = fh.clone();
        let next = tokio::spawn(async move { fh3.backfill_slot(&mut c).await.is_ok() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!next.is_finished());
        drop(first);
        assert!(tokio::time::timeout(Duration::from_secs(5), next).await.unwrap().unwrap());
    }

    /// At most `max_per_ip` subscriber connections per address (IPv6: per
    /// /64); a closed one frees its slot.
    #[test]
    fn subscribers_per_ip_are_capped() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _g = rt.enter();
        let fh = Firehose::new(Options { max_per_ip: 2, ..Options::default() });
        let v4: std::net::IpAddr = "192.0.2.7".parse().unwrap();
        let a = fh.ip_slot(v4).expect("first");
        let _b = fh.ip_slot(v4).expect("second");
        assert!(fh.ip_slot(v4).is_none(), "third from the same address");
        assert!(fh.ip_slot("192.0.2.8".parse().unwrap()).is_some(), "another address");
        drop(a);
        assert!(fh.ip_slot(v4).is_some(), "a closed connection frees its slot");
        let x: std::net::IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let y: std::net::IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let _x = fh.ip_slot(x).unwrap();
        let _y = fh.ip_slot(y).unwrap();
        assert!(fh.ip_slot("2001:db8:1:2::77".parse().unwrap()).is_none(), "same /64");
        assert!(fh.ip_slot("2001:db8:1:3::1".parse().unwrap()).is_some(), "another /64");
        assert_eq!(fh.connections_from(x), 2);
        let open = Firehose::new(Options { max_per_ip: 0, ..Options::default() });
        let all: Vec<_> = (0..10).map(|_| open.ip_slot(v4).unwrap()).collect();
        assert_eq!((all.len(), open.connections_from(v4)), (10, 0), "0 = no cap");
    }

    /// A stalled log holds the min watermark back while another keeps
    /// writing: the merger's queue stays near its budget (the busy log is
    /// read back from S3 later) and the merged stream is still complete and
    /// in order once the stall ends.
    #[tokio::test]
    async fn merger_queue_is_bounded_while_a_log_stalls() {
        let store = crate::store::Store::memory(None);
        let fh = Firehose::new(Options::default());
        fh.set_max_queue_bytes(2000);
        *fh.store.write() = Some(store.clone());
        let (_, wa) = fh.add_remote("A");
        let (_, wb) = fh.add_remote("B");
        let (tx, rx) = mpsc::unbounded_channel();
        let mut sub = fh.subscribe();
        let mut last = i64::MIN;
        fh.spawn_merger(rx);
        let base = fh.position();
        let seq = |k: i64| base + k * 256 + 1;
        let n = 60;
        for k in 0..n {
            tx.send(put_seg(&store, "B", k as u64, seq(2 * k + 1), 200).await).unwrap();
            wb.store(seq(2 * k + 1), Ordering::Release);
            if k % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            assert!(fh.queued_bytes() <= 2000 + 200, "queued {}", fh.queued_bytes());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(fh.queued_bytes() <= 2000 + 200, "queued {}", fh.queued_bytes());
        // the stalled log delivers interleaved events, then catches up
        for k in 0..n {
            tx.send(put_seg(&store, "A", k as u64, seq(2 * k), 10).await).unwrap();
        }
        wa.store(seq(2 * n), Ordering::Release);
        let mut got = Vec::new();
        while got.len() < 2 * n as usize {
            for b in next_batches(&fh, &mut sub, &mut last).await {
                got.extend(b.events.iter().map(|(s, _)| *s));
            }
        }
        assert_eq!(got, (0..2 * n).map(seq).collect::<Vec<_>>());
        // B rejoins the live stream after the read-back
        tx.send(put_seg(&store, "B", n as u64, seq(2 * n + 1), 200).await).unwrap();
        wb.store(seq(2 * n + 1), Ordering::Release);
        wa.store(seq(2 * n + 1), Ordering::Release);
        let b = next_batches(&fh, &mut sub, &mut last).await;
        assert_eq!(b[0].events[0].0, seq(2 * n + 1));
    }
}
