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
    /// `wire` bytes
    pub bytes: usize,
    /// Wire bytes emitted through this batch since startup (subscriber lag
    /// is measured in these).
    pub end: u64,
    /// The events as consecutive binary websocket messages, written as-is
    /// to every subscriber. Built once by the merger; the frames are copied
    /// out of their segments, so the ring doesn't pin whole segment bodies.
    wire: Bytes,
    /// start of each event's message in `wire`
    offs: Vec<usize>,
    /// Each event's repo hash slot, computed by the first sharded subscriber
    /// to read the batch and shared by the rest (see [`event_slot`]).
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
        let events: Vec<(i64, Bytes)> = events.iter().zip(payload).map(|((seq, f), at)| (*seq, wire.slice(at..at + f.len()))).collect();
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

    /// The websocket messages of the events from index `i` on.
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

/// `repo` / `did` of the body map following the header map.
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

/// Subscriber serving settings.
#[derive(Clone)]
pub struct Options {
    /// Bytes of merged batches kept in memory for cursors and slow readers.
    pub ring_bytes: usize,
    /// A live subscriber further than this behind the stream head gets
    /// ConsumerTooSlow and is closed (it resumes from its cursor).
    pub max_lag_bytes: usize,
    /// Read-ahead per cursor backfill, across all logs.
    pub readahead_bytes: usize,
    /// Segments cached for backfills replaying the same range.
    pub backfill_cache_bytes: usize,
    /// Runtime subscriber connections run on (None = the caller's).
    pub runtime: Option<tokio::runtime::Handle>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            ring_bytes: 64 << 20,
            max_lag_bytes: DEFAULT_MAX_LAG_BYTES,
            readahead_bytes: crate::backfill::DEFAULT_READAHEAD_BYTES,
            backfill_cache_bytes: crate::backfill::DEFAULT_CACHE_BYTES,
            runtime: None,
        }
    }
}

/// Default bound on a live subscriber's lag behind the head.
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
    /// Watermark source per log id.
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
    /// Object store for S3 backfill (set once the node log is known).
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
    /// The watermark starts *below* the floor, not at it: the merger isn't
    /// owed the log's events <= floor, but at startup (floor = the start
    /// floor) the S3 backfill serves them, and it waits for `settled` to
    /// reach the floor as proof that every log is durable up to it. Only
    /// the peer can vouch for that (its first heartbeat, or a segment read
    /// back from S3). Starting at the floor let a backfill run while the
    /// peer still had segments in flight with seqs <= floor and skip them
    /// for good (the merger drops them as the backfill's).
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

    /// Byte budget of the merger's queues (events waiting for the min
    /// watermark). Over it, logs are spilled: see `spawn_merger`.
    pub fn set_max_queue_bytes(&self, n: usize) {
        self.max_queue_bytes.store(n, Ordering::Relaxed);
    }

    /// Frame bytes currently queued in the merger.
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
        tokio::spawn(async move {
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
                    fh.settled.fetch_max(w, Ordering::AcqRel);
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
                            lq.spill = Some(Spill { next: b.ordinal, loaded: lq.high, end: false });
                            continue;
                        }
                        None => {}
                    }
                    total += lq.accept(b.events, emitted, fh.start_floor, &mut late);
                }
                // Read spilled logs back, up to w, a chunk at a time: emit only
                // up to what every spilled log has loaded.
                let mut bound = w;
                if let Some(store) = &store {
                    let chunk = (max / 16).max(1);
                    for (log_id, lq) in logs.iter_mut() {
                        let Some(sp) = &mut lq.spill else { continue };
                        let mut caught_up = sp.end || sp.loaded >= w;
                        let mut failed = false;
                        while !caught_up && lq.bytes < chunk {
                            match read_segment(store, log_id, sp.next).await {
                                Ok(Some(LogObject::Segment(_, entries))) => {
                                    metrics::FIREHOSE_SPILL_SEGMENTS.inc();
                                    sp.next += 1;
                                    let last = entries.last().map(|e| e.seq);
                                    let events = entries.into_iter().filter(|e| !e.frame.is_empty()).map(|e| (e.seq, e.frame)).collect();
                                    let LogQ { q, bytes, high, .. } = lq;
                                    let n = accept_into(q, bytes, high, events, emitted, fh.start_floor, &mut late);
                                    total += n;
                                    if let Some(l) = last {
                                        sp.loaded = sp.loaded.max(l);
                                    }
                                    caught_up = sp.loaded >= w;
                                }
                                // the fence: the log is complete
                                Ok(Some(LogObject::Fence { .. })) => {
                                    sp.end = true;
                                    caught_up = true;
                                }
                                // not written: every event <= w of this log was
                                // PUT before w was published, so all are loaded
                                // (w only covers the log's gap-free prefix: a
                                // later ordinal that landed early is past it)
                                Ok(None) => caught_up = true,
                                Err(e) => {
                                    tracing::warn!(%log_id, ordinal = sp.next, "firehose merger: reading back a spilled log failed: {e:#}");
                                    failed = true;
                                    break;
                                }
                            }
                        }
                        if !caught_up {
                            bound = bound.min(sp.loaded);
                            // more to read once this round is emitted (errors
                            // wait for the next tick)
                            behind |= !failed;
                        }
                    }
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
                    logs.retain(|id, lq| !lq.q.is_empty() || s.contains_key(id) || lq.spill.as_ref().is_some_and(|sp| !sp.end));
                }
                fh.queued_bytes.store(total, Ordering::Relaxed);
                metrics::FIREHOSE_MERGE_QUEUE_BYTES.set(total as i64);
                if out.is_empty() {
                    continue;
                }
                out.sort_unstable_by_key(|(s, _)| *s);
                let batch = Arc::new(MergedBatch::new(out, pushed));
                pushed = batch.end;
                STATS
                    .firehose_events
                    .fetch_add(batch.events.len() as u64, Ordering::Relaxed);
                metrics::FIREHOSE_EVENTS.inc_by(batch.events.len() as u64);
                metrics::FIREHOSE_BATCH.observe(batch.events.len() as f64);
                metrics::FIREHOSE_EMIT_DELAY.observe(crate::tid::now_micros().saturating_sub((batch.first >> 8) as u64) as f64 / 1e6);
                fh.push(batch);
            }
        });
    }

    fn push(&self, batch: Arc<MergedBatch>) {
        {
            let mut ring = self.ring.write();
            self.ring_bytes
                .fetch_add(batch.bytes as i64, Ordering::Relaxed);
            ring.push_back(batch.clone());
            while self.ring_bytes.load(Ordering::Relaxed) > self.max_ring_bytes && ring.len() > 1 {
                let old = ring.pop_front().unwrap();
                self.ring_floor.fetch_max(old.last, Ordering::AcqRel);
                self.ring_bytes
                    .fetch_sub(old.bytes as i64, Ordering::Relaxed);
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
    pub fn upgrade(self: &Arc<Self>, mut req: axum::extract::Request, cursor: Option<i64>, shard: Option<SlotRange>) -> Response {
        let accept = match handshake(req.headers()) {
            Ok(a) => a,
            Err(e) => return e.into_response(),
        };
        let on_upgrade = hyper::upgrade::on(&mut req);
        let fh = self.clone();
        self.runtime.spawn(async move {
            match on_upgrade.await {
                Ok(up) => fh.serve(up, cursor, shard).await,
                Err(e) => tracing::debug!("subscribeRepos upgrade failed: {e}"),
            }
        });
        (
            StatusCode::SWITCHING_PROTOCOLS,
            [(header::CONNECTION, "upgrade".to_string()), (header::UPGRADE, "websocket".to_string()), (header::SEC_WEBSOCKET_ACCEPT, accept)],
        )
            .into_response()
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
                tracing::debug!("subscribeRepos: upgraded connection isn't a plain TCP stream; serving it through hyper's IO");
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
        let mut out = Out { w, ctl };
        let reason = match self.stream(&mut out, cursor, shard).await {
            Ok(()) => "shutdown",
            Err(reason) => reason,
        };
        reader.abort();
        reason
    }

    async fn stream<W: AsyncWrite + Unpin>(&self, out: &mut Out<W>, cursor: Option<i64>, shard: Option<SlotRange>) -> Result<(), &'static str> {
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
            let allowance = *allowance.get_or_insert_with(|| self.max_lag_bytes.max(self.head.borrow().saturating_sub(batches[0].start())));
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
    async fn catch_up<W: AsyncWrite + Unpin>(&self, out: &mut Out<W>, last: &mut i64, shard: Option<SlotRange>) -> Result<(), &'static str> {
        loop {
            if !self.backfill_to_ring(out, last, shard).await? {
                out.send(&info_frame("OutdatedCursor", "cursor is older than the retained history; starting from the oldest available event")).await?;
                *last = (*last).max(self.ring_floor.load(Ordering::Acquire));
            }
            if *last >= self.ring_floor.load(Ordering::Acquire) {
                return Ok(());
            }
        }
    }

    /// Sends the events in (`last`, ring floor] from S3, once every log is
    /// durable up to the floor. Ok(false) = the backfill failed (or there's
    /// no store): the caller skips to the ring.
    async fn backfill_to_ring<W: AsyncWrite + Unpin>(&self, out: &mut Out<W>, last: &mut i64, shard: Option<SlotRange>) -> Result<bool, &'static str> {
        let Some(store) = self.store.read().clone() else { return Ok(false) };
        let reader = Reader { store, cache: self.backfill_cache.clone(), readahead_bytes: self.readahead_bytes, shard };
        loop {
            let floor = self.ring_floor.load(Ordering::Acquire);
            if *last >= floor {
                return Ok(true);
            }
            // right after startup the start floor can be ahead of a peer's
            // watermark: its events <= F may not be in S3 yet
            if self.settled.load(Ordering::Acquire) < floor {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            // older than what log retention deleted: OutdatedCursor, then the
            // oldest events left (retention.rs raises this before deleting)
            match crate::retention::retained_floor(&reader.store).await {
                Ok(pruned) if *last < pruned => {
                    out.send(&info_frame("OutdatedCursor", "cursor is older than the retained history; starting from the oldest available event")).await?;
                    *last = pruned;
                    continue;
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("reading the retained floor failed: {e:#}"),
            }
            let (tx, mut rx) = mpsc::channel(4096);
            let (r, from) = (reader.clone(), *last);
            let mut job = AbortOnDrop(tokio::spawn(async move { crate::backfill::backfill_with(&r, from, floor, &tx).await }));
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
            match (&mut job.0).await {
                Ok(Ok(_)) => *last = (*last).max(floor), // everything <= floor that exists was sent
                // retention deleted segments ahead of us mid-read: the floor
                // check above moves us past them
                Ok(Err(e)) if e.downcast_ref::<crate::backfill::Pruned>().is_some() => {
                    if crate::retention::retained_floor(&reader.store).await.is_ok_and(|p| *last < p) {
                        continue;
                    }
                    tracing::warn!(from, floor, "firehose backfill failed: {e:#}");
                    return Ok(false);
                }
                Ok(Err(e)) => {
                    tracing::warn!(from, floor, "firehose backfill failed: {e:#}");
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!(from, floor, "firehose backfill task failed: {e}");
                    return Ok(false);
                }
            }
        }
    }
}

/// Default byte budget of the merger's queues (see `Firehose::spawn_merger`).
pub const DEFAULT_MERGE_QUEUE_BYTES: usize = 256 << 20;

/// One log's events waiting in the merger.
#[derive(Default)]
struct LogQ {
    q: VecDeque<(i64, Bytes)>,
    bytes: usize,
    /// Highest seq accepted: drops duplicates when a log's events arrive
    /// twice (S3 catch-up overlapping a live stream).
    high: i64,
    spill: Option<Spill>,
}

/// A log the merger stopped queueing: its batches are read back from S3.
struct Spill {
    /// next ordinal to read
    next: u64,
    /// every event of the log <= this is queued or emitted
    loaded: i64,
    /// read up to the log's fence
    end: bool,
}

impl LogQ {
    fn accept(&mut self, events: Vec<(i64, Bytes)>, emitted: i64, start_floor: i64, late: &mut usize) -> usize {
        accept_into(&mut self.q, &mut self.bytes, &mut self.high, events, emitted, start_floor, late)
    }
}

/// Queues a log's events in seq order; returns the bytes added.
fn accept_into(
    q: &mut VecDeque<(i64, Bytes)>,
    bytes: &mut usize,
    high: &mut i64,
    events: Vec<(i64, Bytes)>,
    emitted: i64,
    start_floor: i64,
    late: &mut usize,
) -> usize {
    let mut added = 0;
    for (seq, frame) in events {
        if seq <= *high {
            continue;
        }
        *high = seq;
        // At or below what we already emitted: the start of a follower's S3
        // catch-up (<= the start floor, backfill serves it), or a late event
        // (a log we weren't following yet, or a watermark that overpromised),
        // which live order can't take.
        if seq <= emitted {
            if seq > start_floor {
                *late += 1;
            }
            continue;
        }
        added += frame.len();
        q.push_back((seq, frame));
    }
    *bytes += added;
    added
}

/// A log object, checked against the path it was read from (None = missing).
async fn read_segment(store: &crate::store::Store, log_id: &str, ordinal: u64) -> anyhow::Result<Option<LogObject>> {
    use object_store::ObjectStoreExt;
    let data = match store.raw.get(&crate::nodelog::segment_path(store, log_id, ordinal)).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let obj = segment::parse(data, false, None)?;
    if let LogObject::Segment(h, _) = &obj {
        anyhow::ensure!(h.log_id == log_id && h.ordinal == ordinal, "log object {log_id}/{ordinal} has header {}/{}", h.log_id, h.ordinal);
    }
    Ok(Some(obj))
}

// ---- subscriber connections (a minimal RFC 6455 server) ----

const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xa;

/// How long a subscriber being dropped gets to take its final frames.
const FINAL_GRACE: Duration = Duration::from_secs(10);

/// Largest client data message we skip over (subscribeRepos takes none).
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

/// Validates a websocket upgrade request; returns the Sec-WebSocket-Accept value.
fn handshake(h: &HeaderMap) -> Result<String, (StatusCode, &'static str)> {
    let has = |name: header::HeaderName, token: &str| {
        h.get_all(name).iter().any(|v| v.to_str().is_ok_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token))))
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

/// What the client sent that the writer must answer.
enum Ctl {
    Ping(Vec<u8>),
    Close,
}

/// Reads the client's side: answers pings (via the writer), ends on a close
/// frame, EOF or a protocol error. Dropping `ctl` tells the writer the
/// client is gone.
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

/// A subscriber's write side plus what its read side asks of it.
struct Out<W> {
    w: W,
    ctl: mpsc::Receiver<Ctl>,
}

impl<W: AsyncWrite + Unpin> Out<W> {
    async fn write(&mut self, data: &[u8]) -> Result<(), &'static str> {
        self.w.write_all(data).await.map_err(|_| "client_gone")?;
        metrics::FIREHOSE_SENT_BYTES.inc_by(data.len() as u64);
        Ok(())
    }

    /// One binary message.
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
    async fn send_live(&mut self, data: &mut [std::io::IoSlice<'_>], head: &mut watch::Receiver<u64>, pos: u64, allowance: u64) -> Result<(), &'static str> {
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
            self.finish(&events::error_frame("ConsumerTooSlow", "fell too far behind the stream; reconnect with a cursor")).await;
            return Err("too_slow");
        }
        Ok(())
    }

    /// Best effort: a last message and a close frame, then the caller drops
    /// the connection.
    async fn finish(&mut self, payload: &[u8]) {
        let mut m = Vec::with_capacity(payload.len() + 12);
        push_message(&mut m, OP_BINARY, payload);
        push_message(&mut m, OP_CLOSE, &1000u16.to_be_bytes());
        let _ = tokio::time::timeout(FINAL_GRACE, async {
            self.w.write_all(&m).await?;
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
async fn write_all_vectored<W: AsyncWrite + Unpin>(w: &mut W, mut bufs: &mut [std::io::IoSlice<'_>]) -> std::io::Result<()> {
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

/// Aborts a task when its handle is dropped (a subscriber that went away).
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
                        events::RepoOp { action: "update", path: "app.bsky.feed.like/3j", cid: Some(cid), prev: Some(cid) },
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
                    let want: Vec<(i64, Bytes)> = evs[from..].iter().filter(|(_, f)| range.contains(event_slot(f))).cloned().collect();
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
        let ops = [events::RepoOp { action: "create", path: "app.bsky.feed.post/3kabcdefghij2", cid: Some(cid), prev: None }];
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
        eprintln!("slots: {:.0} ns/event once per batch; runs for 16 sharded subscribers: {:.0} ns/event total", per(slots_t), per(runs_t));
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
