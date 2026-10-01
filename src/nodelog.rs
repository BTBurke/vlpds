//! One commit log per node incarnation (`log_id`), shared by every shard the
//! node owns. Group commit across shards means segment size and PUT rate
//! scale with node throughput, not with shard count.
//!
//! workers ──LogEntry{shard}──► sequencer ──(one PUT in flight)──► finalizer
//!                               assign seq, append            per touched shard:
//!                               (shard, epoch)-tagged         SlateDB batch + applied
//!                               entries                       marker, then acks,
//!                                                             firehose, watermark
//!
//! Segments go to `log/{log_id}/{ordinal:012}.seg` with If-None-Match on dense
//! ordinals. A dead node's log is closed by a fence object at its next ordinal
//! (see cluster.rs), so a zombie's next PUT collides and it fail-stops.

use crate::events::Frame;
use crate::metrics;
use crate::segment::{self, LogObject, Mutation, SegmentBuilder};
use crate::stats::STATS;
use crate::store::Store;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parking_lot::{Mutex, RwLock};
use slatedb::{Db, WriteBatch};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub type AckFn = Box<dyn FnOnce(Result<(), Arc<anyhow::Error>>) + Send>;
/// Returns false once this node may no longer act as an owner (lease lapsed).
pub type LeaseCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Per-shard applied marker: everything for this shard in `log_id` up to and
/// including `ordinal` is in the shard's SlateDB.
pub const META_APPLIED: &[u8] = b"meta/applied2";

pub struct LogEntry {
    pub shard: u16,
    pub frames: Vec<Frame>,
    pub muts: Vec<Mutation>,
    pub ack: Option<AckFn>,
    /// Per-repo in-flight counter (repos with pending commits aren't evicted).
    pub pending: Option<Arc<AtomicU32>>,
    pub enqueued: Instant,
}

/// Durable, ordered events from one log, handed to the firehose merger.
#[derive(Clone)]
pub struct LogBatch {
    pub log_id: Arc<str>,
    pub ordinal: u64,
    pub events: Vec<(i64, Bytes)>,
}

/// Default byte budget of a log's live ring (see `LiveRing`).
pub const DEFAULT_LIVE_RING_BYTES: usize = 128 << 20;

/// This log's recent durable batches, for peers following it (remote.rs).
/// A batch is kept until every subscriber has read it, up to a byte budget:
/// each batch pins its whole segment, so the old 1024-batch broadcast let one
/// slow follower pin GBs. A subscriber the budget evicts batches from is told
/// it lagged and catches up from S3 instead.
pub struct LiveRing {
    inner: Mutex<LiveInner>,
    max_bytes: AtomicUsize,
}

struct LiveInner {
    /// (batch, pinned bytes), consecutive ordinals
    buf: VecDeque<(Arc<LogBatch>, usize)>,
    bytes: usize,
    /// ordinal of the next batch to be pushed
    next: u64,
    /// subscriber id -> next ordinal it will read
    subs: HashMap<u64, u64>,
    next_id: u64,
}

impl LiveInner {
    /// Drops batches every subscriber has read, then the oldest while over
    /// budget (always keeping the newest).
    fn trim(&mut self, max: usize) {
        let min_next = self.subs.values().copied().min().unwrap_or(self.next);
        while let Some((b, n)) = self.buf.front() {
            if b.ordinal >= min_next && (self.bytes <= max || self.buf.len() == 1) {
                break;
            }
            self.bytes -= n;
            self.buf.pop_front();
        }
        metrics::LOG_LIVE_BYTES.set(self.bytes as i64);
    }
}

pub enum LiveRecv {
    Batch(Arc<LogBatch>),
    Empty,
    /// Batches this subscriber hadn't read were evicted: catch up from S3.
    Lagged,
}

pub struct LiveSub {
    ring: Arc<LiveRing>,
    id: u64,
    next: u64,
}

impl LiveRing {
    pub fn new(max_bytes: usize) -> Arc<LiveRing> {
        let inner = LiveInner { buf: VecDeque::new(), bytes: 0, next: 0, subs: HashMap::new(), next_id: 0 };
        Arc::new(LiveRing { inner: Mutex::new(inner), max_bytes: AtomicUsize::new(max_bytes) })
    }

    pub fn set_max_bytes(&self, n: usize) {
        self.max_bytes.store(n, Ordering::Relaxed);
    }

    /// Bytes currently pinned by the ring.
    pub fn bytes(&self) -> usize {
        self.inner.lock().bytes
    }

    /// Receives every batch pushed from now on (while it keeps up).
    pub fn subscribe(self: &Arc<Self>) -> LiveSub {
        let mut g = self.inner.lock();
        let (id, next) = (g.next_id, g.next);
        g.next_id += 1;
        g.subs.insert(id, next);
        LiveSub { ring: self.clone(), id, next }
    }

    fn push(&self, b: Arc<LogBatch>, bytes: usize) {
        let mut g = self.inner.lock();
        g.next = b.ordinal + 1;
        g.buf.push_back((b, bytes));
        g.bytes += bytes;
        g.trim(self.max_bytes.load(Ordering::Relaxed));
    }
}

impl LiveSub {
    pub fn try_recv(&mut self) -> LiveRecv {
        let mut g = self.ring.inner.lock();
        if self.next >= g.next {
            return LiveRecv::Empty;
        }
        let front = g.buf.front().map(|(b, _)| b.ordinal).unwrap_or(g.next);
        if self.next < front {
            return LiveRecv::Lagged;
        }
        let b = g.buf[(self.next - front) as usize].0.clone();
        self.next += 1;
        g.subs.insert(self.id, self.next);
        if front < self.next {
            g.trim(self.ring.max_bytes.load(Ordering::Relaxed));
        }
        LiveRecv::Batch(b)
    }
}

impl Drop for LiveSub {
    fn drop(&mut self) {
        let mut g = self.ring.inner.lock();
        g.subs.remove(&self.id);
        g.trim(self.ring.max_bytes.load(Ordering::Relaxed));
    }
}

pub fn seq_floor(now_us: u64) -> i64 {
    (now_us as i64) << 8
}

/// The highest seq <= `v` carrying `writer` in its low byte: as `assigned`, it
/// makes the next seq (>= it + 256) exceed `v` and keep the writer byte.
fn own_seq_at_or_below(v: i64, writer: u8) -> i64 {
    let wr = writer as i64;
    if v & 0xff >= wr {
        (v & !0xff) | wr
    } else {
        ((v & !0xff) - 256) | wr
    }
}

/// Every event with seq <= `get()` is durable and has been handed to the merger.
pub struct Watermark {
    writer: u8,
    inner: Mutex<(i64, i64)>, // (assigned, durable)
    cap: std::sync::atomic::AtomicI64,
}

impl Watermark {
    pub fn new(writer: u8, last: i64) -> Watermark {
        Watermark { writer, inner: Mutex::new((own_seq_at_or_below(last, writer), last)), cap: std::sync::atomic::AtomicI64::new(i64::MAX) }
    }

    /// Time-based, strictly increasing; the low byte is this node's writer id
    /// (unique among live nodes), so seqs are unique across logs.
    fn assign(&self) -> i64 {
        let mut w = self.inner.lock();
        let now = seq_floor(crate::tid::now_micros()) | self.writer as i64;
        let seq = now.max(w.0 + 256);
        w.0 = seq;
        seq
    }

    fn set_durable(&self, seq: i64) {
        self.inner.lock().1 = seq;
    }

    pub fn idle(&self) -> bool {
        let w = self.inner.lock();
        w.0 <= w.1
    }

    pub fn get(&self) -> i64 {
        let mut w = self.inner.lock();
        if w.0 > w.1 {
            return w.1;
        }
        let v = w.1.max(seq_floor(crate::tid::now_micros()) - 1).min(self.cap.load(Ordering::Acquire).max(w.1));
        if v > w.1 {
            // Idle: we advertise the clock. Record it, so a later seq can't land
            // at or below it if the wall clock steps back (assign() is
            // max(now, last + 256)); the merger would drop such events as late.
            w.0 = w.0.max(own_seq_at_or_below(v, self.writer));
            w.1 = v;
        }
        v
    }

    /// Never announce beyond our node lease: a successor's seqs start after it.
    pub fn set_lease_expiry(&self, expiry_us: u64) {
        self.cap.store(seq_floor(expiry_us), Ordering::Release);
    }
}

/// A shard this node applies into.
pub struct ShardSink {
    pub id: u16,
    pub epoch: u64,
    pub db: Arc<Db>,
    /// Held (write) across apply + ack of a segment; export readers take it
    /// (read) to pair a repo's durable view with a matching SlateDB snapshot.
    pub apply_lock: Arc<tokio::sync::RwLock<()>>,
}

#[derive(Default)]
pub struct ShardSinks {
    map: RwLock<HashMap<u16, Arc<ShardSink>>>,
}

impl ShardSinks {
    pub fn get(&self, id: u16) -> Option<Arc<ShardSink>> {
        self.map.read().get(&id).cloned()
    }
    pub fn insert(&self, s: Arc<ShardSink>) {
        self.map.write().insert(s.id, s);
    }
    pub fn remove(&self, id: u16) -> Option<Arc<ShardSink>> {
        self.map.write().remove(&id)
    }
    pub fn all(&self) -> Vec<Arc<ShardSink>> {
        self.map.read().values().cloned().collect()
    }
}

pub struct NodeLogConfig {
    pub log_id: String,
    pub writer: u8,
    pub max_segment_bytes: usize,
    pub hedge_after: Duration,
    pub lease_ok: Option<LeaseCheck>,
}

pub struct NodeLog {
    pub log_id: Arc<str>,
    pub tx: mpsc::Sender<LogEntry>,
    pub wm: Arc<Watermark>,
    pub live: Arc<LiveRing>,
    /// Last durable+applied ordinal (u64::MAX = none yet).
    pub durable_ordinal: Arc<AtomicU64>,
    pub sinks: Arc<ShardSinks>,
}

pub fn segment_path(store: &Store, log_id: &str, ordinal: u64) -> Path {
    Path::from(format!("{}/log/{}/{:012}.seg", store.prefix, log_id, ordinal))
}

pub fn encode_marker(log_id: &str, ordinal: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(10 + log_id.len());
    b.extend_from_slice(&(log_id.len() as u16).to_be_bytes());
    b.extend_from_slice(log_id.as_bytes());
    b.extend_from_slice(&ordinal.to_be_bytes());
    b
}

pub fn decode_marker(b: &[u8]) -> Option<(String, u64)> {
    let n = u16::from_be_bytes(b.get(..2)?.try_into().ok()?) as usize;
    let id = String::from_utf8(b.get(2..2 + n)?.to_vec()).ok()?;
    let ord = u64::from_be_bytes(b.get(2 + n..10 + n)?.try_into().ok()?);
    Some((id, ord))
}

impl NodeLog {
    /// Starts a fresh log (a node never reopens an old log: a restarted node
    /// gets a new log id; its previous log is fenced and replayed by owners).
    pub fn start(store: Store, cfg: NodeLogConfig, merger_tx: mpsc::UnboundedSender<LogBatch>) -> Arc<NodeLog> {
        let wm = Arc::new(Watermark::new(cfg.writer, seq_floor(crate::tid::now_micros())));
        let (tx, rx) = mpsc::channel(64 * 1024);
        let (fin_tx, fin_rx) = mpsc::channel(4);
        let live = LiveRing::new(DEFAULT_LIVE_RING_BYTES);
        let sinks: Arc<ShardSinks> = Arc::default();
        let durable_ordinal = Arc::new(AtomicU64::new(u64::MAX));
        let log_id: Arc<str> = cfg.log_id.clone().into();
        tokio::spawn(run_sequencer(store, cfg.log_id.clone(), cfg.max_segment_bytes, cfg.hedge_after, cfg.lease_ok.clone(), wm.clone(), sinks.clone(), rx, fin_tx));
        tokio::spawn(run_finalizer(
            log_id.clone(),
            sinks.clone(),
            wm.clone(),
            fin_rx,
            merger_tx,
            live.clone(),
            cfg.lease_ok,
            durable_ordinal.clone(),
        ));
        Arc::new(NodeLog { log_id, tx, wm, live, durable_ordinal, sinks })
    }

    /// The ordinal the next segment will get (an owner records it as the start
    /// of its span when it takes a shard).
    pub fn next_ordinal(&self) -> u64 {
        self.durable_ordinal.load(Ordering::Acquire).wrapping_add(1)
    }

    /// Writes an applied marker for every shard and flushes their memtables,
    /// bounding how much of this log a successor must replay after a crash.
    pub async fn checkpoint_all(&self) {
        let ord = self.durable_ordinal.load(Ordering::Acquire);
        if ord == u64::MAX {
            return;
        }
        // HA tests (bench/ha kill9-mid-checkpoint) key off this line.
        tracing::info!(ordinal = ord, shards = self.sinks.all().len(), "checkpoint start");
        for s in self.sinks.all() {
            let _g = s.apply_lock.write().await;
            // the finalizer has applied every segment <= ord (it updates
            // durable_ordinal only after applying)
            let mut wb = WriteBatch::new();
            wb.put(META_APPLIED, encode_marker(&self.log_id, ord));
            let _ = s.db.write(wb).await;
            drop(_g);
            let _ = s.db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await;
        }
    }
}

struct Sealed {
    ordinal: u64,
    data: Bytes,
    frames: Vec<(i64, std::ops::Range<usize>)>,
    /// muts grouped by shard, in log order
    muts: BTreeMap<u16, Vec<Mutation>>,
    acks: Vec<(Option<AckFn>, Option<Arc<AtomicU32>>, Instant)>,
    last_seq: i64,
    put_secs: f64,
}

struct Open {
    seg: SegmentBuilder,
    frames: Vec<(i64, std::ops::Range<usize>)>,
    muts: BTreeMap<u16, Vec<Mutation>>,
    acks: Vec<(Option<AckFn>, Option<Arc<AtomicU32>>, Instant)>,
}

impl Open {
    fn new() -> Open {
        Open { seg: SegmentBuilder::new(), frames: Vec::new(), muts: BTreeMap::new(), acks: Vec::new() }
    }

    fn push(&mut self, wm: &Watermark, sinks: &ShardSinks, mut e: LogEntry) {
        // HA fix: an entry for a shard we no longer hold (a worker's repo load or
        // cached repo that outlived close()) used to be logged with epoch 0 and
        // *acked*. But replay only applies entries whose epoch matches a span,
        // so the successor never saw it: an acked write lost. Refuse it.
        let Some(epoch) = sinks.get(e.shard).map(|s| s.epoch) else {
            tracing::warn!(shard = e.shard, "log entry for a shard this node no longer holds: rejected");
            if let Some(p) = e.pending {
                p.fetch_sub(1, Ordering::Release);
            }
            if let Some(ack) = e.ack {
                ack(Err(Arc::new(anyhow::anyhow!("partition not owned by this node (moved)"))));
            }
            return;
        };
        if e.frames.is_empty() {
            // private-state write: an entry with an empty frame (skipped by the firehose)
            e.frames.push(Frame { prefix: Vec::new(), suffix: Vec::new() });
        }
        let n = e.frames.len();
        for (i, f) in e.frames.iter().enumerate() {
            let seq = wm.assign();
            let muts: &[Mutation] = if i + 1 == n { &e.muts } else { &[] };
            let empty = f.prefix.is_empty() && f.suffix.is_empty();
            let range = self.seg.push(seq, e.shard, epoch, |out| if !empty { f.finish(seq, out) }, muts);
            self.frames.push((seq, range));
        }
        self.muts.entry(e.shard).or_default().append(&mut e.muts);
        self.acks.push((e.ack, e.pending, e.enqueued));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sequencer(
    store: Store,
    log_id: String,
    max_segment_bytes: usize,
    hedge_after: Duration,
    lease_ok: Option<LeaseCheck>,
    wm: Arc<Watermark>,
    sinks: Arc<ShardSinks>,
    mut rx: mpsc::Receiver<LogEntry>,
    fin_tx: mpsc::Sender<Sealed>,
) {
    let mut ordinal = 0u64;
    let mut open = Open::new();
    let mut inflight: Option<tokio::task::JoinHandle<Sealed>> = None;
    let mut closed = false;
    loop {
        let can_recv = !closed && open.seg.len() < max_segment_bytes;
        tokio::select! {
            biased;
            res = async { inflight.as_mut().unwrap().await }, if inflight.is_some() => {
                inflight = None;
                match res {
                    Ok(sealed) => {
                        if fin_tx.send(sealed).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::error!(%log_id, "segment upload task failed: {e}; exiting");
                        std::process::exit(2);
                    }
                }
            }
            e = rx.recv(), if can_recv => match e {
                Some(e) => {
                    open.push(&wm, &sinks, e);
                    while open.seg.len() < max_segment_bytes {
                        match rx.try_recv() {
                            Ok(e) => open.push(&wm, &sinks, e),
                            Err(_) => break,
                        }
                    }
                }
                None => closed = true,
            },
        }
        metrics::SEQ_QUEUE.with_label_values(&["node"]).set((rx.max_capacity() - rx.capacity()) as i64);
        if inflight.is_none() && !open.seg.is_empty() {
            if let Some(ok) = &lease_ok {
                if !ok() {
                    tracing::error!(%log_id, "node lease lapsed before segment PUT: fail-stop");
                    std::process::exit(5);
                }
            }
            let o = std::mem::replace(&mut open, Open::new());
            metrics::SEGMENT_EVENTS.observe(o.frames.len() as f64);
            let header = o.seg.header(&log_id, ordinal);
            let off = header.len();
            let mut data = header;
            data.extend_from_slice(&o.seg.body);
            let sealed = Sealed {
                ordinal,
                data: Bytes::from(data),
                frames: o.frames.into_iter().map(|(s, r)| (s, r.start + off..r.end + off)).collect(),
                muts: o.muts,
                acks: o.acks,
                last_seq: o.seg.last_seq,
                put_secs: 0.0,
            };
            ordinal += 1;
            let (store, log_id) = (store.clone(), log_id.clone());
            inflight = Some(tokio::spawn(async move {
                let mut sealed = sealed;
                let t = Instant::now();
                upload(&store, &log_id, sealed.ordinal, sealed.data.clone(), hedge_after).await;
                sealed.put_secs = t.elapsed().as_secs_f64();
                sealed
            }));
        }
        if closed && inflight.is_none() && open.seg.is_empty() {
            return;
        }
    }
}

async fn put_once(store: &Store, path: &Path, data: Bytes) -> object_store::Result<()> {
    store.inject_latency().await;
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    let r = store.raw.put_opts(path, PutPayload::from_bytes(data), opts).await.map(|_| ());
    metrics::PUT_ATTEMPTS
        .with_label_values(&[match &r {
            Ok(()) => "ok",
            Err(object_store::Error::AlreadyExists { .. }) => "already_exists",
            Err(_) => "error",
        }])
        .inc();
    r
}

/// PUTs a segment with If-None-Match until durable, hedging slow attempts.
/// A different object at our ordinal (another writer, or a fence) means this
/// log was closed out from under us: fail-stop.
async fn upload(store: &Store, log_id: &str, ordinal: u64, data: Bytes, hedge_after: Duration) {
    use futures::stream::{FuturesUnordered, StreamExt};
    let path = segment_path(store, log_id, ordinal);
    let t = Instant::now();
    let mut backoff = Duration::from_millis(20);
    loop {
        let mut attempts = FuturesUnordered::new();
        attempts.push(put_once(store, &path, data.clone()));
        let hedge = tokio::time::sleep(hedge_after);
        tokio::pin!(hedge);
        let mut hedged = false;
        let result = loop {
            tokio::select! {
                Some(r) = attempts.next() => match r {
                    Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => break r,
                    Err(e) if attempts.is_empty() => break Err(e),
                    Err(_) => continue,
                },
                _ = &mut hedge, if !hedged => {
                    hedged = true;
                    STATS.hedges.fetch_add(1, Ordering::Relaxed);
                    metrics::PUT_HEDGES.inc();
                    attempts.push(put_once(store, &path, data.clone()));
                }
            }
        };
        match result {
            Ok(()) => {
                STATS.record_put(t.elapsed(), data.len());
                return;
            }
            Err(object_store::Error::AlreadyExists { .. }) => match resolve_conflict(store, &path, &data).await {
                Conflict::Ours => {
                    STATS.record_put(t.elapsed(), data.len());
                    return;
                }
                Conflict::Fenced => {
                    tracing::error!(log_id, ordinal, "our log was fenced by a successor: fail-stop");
                    std::process::exit(3);
                }
                Conflict::Other => {
                    tracing::error!(log_id, ordinal, "segment ordinal taken by another writer: fail-stop");
                    std::process::exit(3);
                }
                Conflict::Missing => {
                    // S3 answers 409 (mapped to AlreadyExists) on conditional
                    // write conflicts too, e.g. our own hedge racing: nothing
                    // is there (yet), so PUT again.
                    tracing::warn!(log_id, ordinal, "segment PUT conflicted but no object is there; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
            },
            Err(e) => {
                tracing::warn!(log_id, ordinal, "segment PUT failed, retrying: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Conflict {
    /// Our bytes are there (a hedge or an earlier attempt won).
    Ours,
    /// A fence object: a successor closed this log.
    Fenced,
    /// A different segment: another writer.
    Other,
    /// Nothing there: the conflict was transient.
    Missing,
}

/// What occupies a segment path our conditional PUT conflicted on. Transient
/// GET errors are retried: only a confirmed fence or different bytes may make
/// the caller fail-stop.
async fn resolve_conflict(store: &Store, path: &Path, data: &Bytes) -> Conflict {
    let mut backoff = Duration::from_millis(20);
    loop {
        let got = match store.raw.get(path).await {
            Ok(r) => r.bytes().await,
            Err(e) => Err(e),
        };
        match got {
            Ok(b) if b == *data => return Conflict::Ours,
            Ok(b) => {
                return match segment::parse(b, false, None) {
                    Ok(LogObject::Fence { .. }) => Conflict::Fenced,
                    _ => Conflict::Other,
                };
            }
            Err(object_store::Error::NotFound { .. }) => return Conflict::Missing,
            Err(e) => {
                tracing::warn!(%path, "reading a conflicting segment failed, retrying: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_finalizer(
    log_id: Arc<str>,
    sinks: Arc<ShardSinks>,
    wm: Arc<Watermark>,
    mut rx: mpsc::Receiver<Sealed>,
    merger_tx: mpsc::UnboundedSender<LogBatch>,
    live: Arc<LiveRing>,
    lease_ok: Option<LeaseCheck>,
    durable_ordinal: Arc<AtomicU64>,
) {
    while let Some(s) = rx.recv().await {
        // Take every touched shard's apply lock (id order) and hold it across
        // apply *and* ack, so export readers never see a SlateDB snapshot newer
        // than the repo views published by these acks.
        let mut guards = Vec::new();
        let mut targets = Vec::new();
        for (shard, muts) in &s.muts {
            match sinks.get(*shard) {
                Some(sink) => {
                    guards.push(sink.apply_lock.clone().write_owned().await);
                    targets.push((sink, muts));
                }
                None => tracing::error!(shard, ordinal = s.ordinal, "durable entries for a shard we no longer hold; its owner will replay them"),
            }
        }
        let t = Instant::now();
        // Shards are independent DBs: apply them concurrently, so one shard
        // stalled on memtable backpressure doesn't serialize the rest (a
        // segment touches up to every owned shard).
        let writes = targets.iter().map(|(sink, muts)| {
            let mut wb = WriteBatch::new();
            for m in muts.iter() {
                match &m.val {
                    Some(v) => wb.put(&m.key, v),
                    None => wb.delete(&m.key),
                }
            }
            wb.put(META_APPLIED, encode_marker(&log_id, s.ordinal));
            async move { (sink.id, sink.db.write(wb).await) }
        });
        for (shard, r) in futures::future::join_all(writes).await {
            if let Err(e) = r {
                tracing::error!(shard, "state apply failed: {e}; exiting");
                std::process::exit(4);
            }
        }
        let _ = STATS.apply_us.lock().record(t.elapsed().as_micros().max(1) as u64);
        metrics::APPLY_DURATION.observe(t.elapsed().as_secs_f64());
        let events: Vec<(i64, Bytes)> =
            s.frames.iter().filter(|(_, r)| !r.is_empty()).map(|(seq, r)| (*seq, s.data.slice(r.clone()))).collect();
        let batch = LogBatch { log_id: log_id.clone(), ordinal: s.ordinal, events };
        live.push(Arc::new(batch.clone()), s.data.len());
        let _ = merger_tx.send(batch);
        if let Some(ok) = &lease_ok {
            if !ok() {
                tracing::error!(%log_id, "node lease lapsed before ack: fail-stop");
                std::process::exit(5);
            }
        }
        wm.set_durable(s.last_seq);
        durable_ordinal.store(s.ordinal, Ordering::Release);
        metrics::SEGMENTS.with_label_values(&["node"]).inc();
        metrics::SEGMENT_BYTES.observe(s.data.len() as f64);
        metrics::SEGMENT_BYTES_TOTAL.inc_by(s.data.len() as u64);
        metrics::PUT_DURATION.with_label_values(&["node"]).observe(s.put_secs);
        let n = s.acks.len();
        for (ack, pending, enq) in s.acks {
            STATS.record_commit_latency(enq.elapsed());
            metrics::COMMIT_LATENCY.observe(enq.elapsed().as_secs_f64());
            if let Some(p) = pending {
                p.fetch_sub(1, Ordering::Release);
            }
            if let Some(ack) = ack {
                ack(Ok(()));
            }
        }
        drop(guards);
        STATS.entries_durable.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// One ownership span of a shard in some node's log: entries for the shard in
/// `log_id` with ordinals in [start, end) belong to ownership `epoch`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Span {
    pub log_id: String,
    pub epoch: u64,
    pub start: u64,
    /// None = still open (current owner).
    pub end: Option<u64>,
}

/// The span an applied marker `(log, ord)` was written in: the *earliest* span
/// of `log` that covers it (start - 1 <= ord < end; start - 1 = nothing of the
/// span applied yet). A node can hold a shard twice in one log (A -> B -> A),
/// so the log id alone is ambiguous: matching its last span skipped every
/// span in between (B's acked writes). Where the marker sits on the boundary
/// of two spans of the same log, the earlier one wins: replaying more than
/// needed is safe (absolute puts/deletes, in log order), replaying less is not.
fn marker_span(history: &[Span], log: &str, ord: u64) -> Option<usize> {
    history.iter().position(|s| s.log_id == log && s.start <= ord.saturating_add(1) && s.end.is_none_or(|e| ord < e))
}

/// Brings a shard's SlateDB up to date from the log spans of its previous
/// owners (chronological), starting after its applied marker.
pub async fn replay_shard(store: &Store, shard: u16, db: &Db, history: &[Span]) -> anyhow::Result<u64> {
    replay_many(store, &[(shard, db, history)]).await
}

/// Replays many shards at once (e.g. taking over a dead node's shards): each
/// log segment is fetched once (16 in flight, applied in order) and its
/// entries dispatched to every shard whose span covers it. Returns segments read.
pub async fn replay_many(store: &Store, shards: &[(u16, &Db, &[Span])]) -> anyhow::Result<u64> {
    use futures::StreamExt;
    // per shard: the spans still to apply, with the ordinal to start from
    let mut todo: Vec<Vec<(Span, u64)>> = Vec::with_capacity(shards.len());
    for (_, db, history) in shards {
        let marker = db.get(META_APPLIED).await?.and_then(|b| decode_marker(&b));
        let first = match &marker {
            Some((log, ord)) => marker_span(history, log, *ord).unwrap_or(0),
            None => 0,
        };
        let mut v = Vec::new();
        for (i, span) in history.iter().enumerate().skip(first) {
            let from = match &marker {
                Some((log, ord)) if i == first && &span.log_id == log => (ord + 1).max(span.start),
                _ => span.start,
            };
            if span.end.is_none_or(|e| from < e) {
                v.push((span.clone(), from));
            }
        }
        todo.push(v);
    }
    let mut read = 0u64;
    let rounds = todo.iter().map(|v| v.len()).max().unwrap_or(0);
    for k in 0..rounds {
        // group this round's spans by log
        let mut by_log: std::collections::BTreeMap<String, Vec<(usize, Span, u64)>> = Default::default();
        for (i, v) in todo.iter().enumerate() {
            if let Some((span, from)) = v.get(k) {
                by_log.entry(span.log_id.clone()).or_default().push((i, span.clone(), *from));
            }
        }
        for (log_id, members) in by_log {
            let lo = members.iter().map(|m| m.2).min().unwrap_or(0);
            let hi = if members.iter().any(|m| m.1.end.is_none()) { u64::MAX } else { members.iter().filter_map(|m| m.1.end).max().unwrap_or(0) };
            let fetch = |ord: u64| {
                let (store, path) = (store.clone(), segment_path(store, &log_id, ord));
                async move {
                    match store.raw.get(&path).await {
                        Ok(r) => r.bytes().await.map(Some).map_err(anyhow::Error::from),
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(e) => Err(e.into()),
                    }
                }
            };
            let mut objs = futures::stream::iter(lo..hi).map(fetch).buffered(16);
            let mut ord = lo;
            while let Some(obj) = objs.next().await {
                // The end of the log (missing object or its fence) is only
                // legitimate past every closed span: a closed span's end is the
                // fence ordinal, so every segment before it exists.
                let seg = match obj? {
                    Some(data) => match segment::parse(data, true, None)? {
                        LogObject::Segment(h, entries) => Some((h, entries)),
                        LogObject::Fence { .. } => None,
                    },
                    None => None,
                };
                let Some((h, entries)) = seg else {
                    if let Some((_, span, _)) = members.iter().find(|(_, s, from)| ord >= *from && s.end.is_some_and(|e| ord < e)) {
                        anyhow::bail!("log {log_id} ends at ordinal {ord} inside closed span {span:?}");
                    }
                    break;
                };
                anyhow::ensure!(
                    h.log_id == log_id && h.ordinal == ord,
                    "log object {log_id}/{ord} has header {}/{}",
                    h.log_id,
                    h.ordinal
                );
                read += 1;
                for (i, span, from) in &members {
                    if ord < *from || span.end.is_some_and(|e| ord >= e) {
                        continue;
                    }
                    let (shard, db, _) = &shards[*i];
                    let mut wb = WriteBatch::new();
                    for e in entries.iter().filter(|e| e.shard == *shard && e.epoch == span.epoch) {
                        for m in &e.muts {
                            match &m.val {
                                Some(v) => wb.put(&m.key, v),
                                None => wb.delete(&m.key),
                            }
                        }
                    }
                    wb.put(META_APPLIED, encode_marker(&span.log_id, ord));
                    db.write(wb).await?;
                }
                ord += 1;
            }
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentBuilder;

    fn seg_bytes(log: &str, ord: u64, shard: u16, epoch: u64, key: &str) -> Vec<u8> {
        let mut b = SegmentBuilder::new();
        let m = Mutation { key: Bytes::from(key.to_string()), val: Some(Bytes::from_static(b"v")) };
        b.push(1000 + ord as i64, shard, epoch, |_| {}, &[m]);
        let mut obj = b.header(log, ord);
        obj.extend_from_slice(&b.body);
        obj
    }

    async fn put_seg(store: &Store, log: &str, ord: u64, shard: u16, epoch: u64, key: &str) {
        store.raw.put(&segment_path(store, log, ord), PutPayload::from(seg_bytes(log, ord, shard, epoch, key))).await.unwrap();
    }

    fn span(log: &str, epoch: u64, start: u64, end: Option<u64>) -> Span {
        Span { log_id: log.into(), epoch, start, end }
    }

    /// A -> B -> A in one incarnation of A: the marker (A, 1) left by B's
    /// takeover replay must not match A's second span, or B's acked writes
    /// are skipped.
    #[tokio::test]
    async fn replay_aba_keeps_middle_span() {
        let store = Store::memory(None);
        let shard = 7u16;
        put_seg(&store, "A", 0, shard, 1, "a0").await;
        put_seg(&store, "A", 1, shard, 1, "a1").await;
        put_seg(&store, "B", 0, shard, 2, "b0").await;
        put_seg(&store, "B", 1, shard, 2, "b1").await;
        let db = crate::partition::open_db(&store, shard, None).await.unwrap();
        let mut wb = WriteBatch::new();
        wb.put(b"a0", b"v");
        wb.put(b"a1", b"v");
        wb.put(META_APPLIED, encode_marker("A", 1));
        db.write(wb).await.unwrap();
        let history = vec![span("A", 1, 0, Some(2)), span("B", 2, 0, Some(2)), span("A", 3, 2, None)];
        let n = replay_many(&store, &[(shard, &db, &history)]).await.unwrap();
        assert_eq!(n, 2);
        assert!(db.get(b"b0").await.unwrap().is_some());
        assert!(db.get(b"b1").await.unwrap().is_some());
        // marker inside A's second span: nothing before it is replayed
        put_seg(&store, "A", 2, shard, 3, "a2").await;
        put_seg(&store, "A", 3, shard, 3, "a3").await;
        let mut wb = WriteBatch::new();
        wb.put(META_APPLIED, encode_marker("A", 2));
        db.write(wb).await.unwrap();
        assert_eq!(replay_many(&store, &[(shard, &db, &history)]).await.unwrap(), 1);
        assert!(db.get(b"a3").await.unwrap().is_some());
    }

    #[test]
    fn marker_span_picks_the_covering_span() {
        let h = vec![span("A", 1, 0, Some(5)), span("B", 2, 0, Some(3)), span("A", 3, 9, None)];
        assert_eq!(marker_span(&h, "A", 2), Some(0));
        assert_eq!(marker_span(&h, "A", 4), Some(0));
        assert_eq!(marker_span(&h, "A", 8), Some(2)); // start - 1: nothing of it applied
        assert_eq!(marker_span(&h, "A", 20), Some(2));
        assert_eq!(marker_span(&h, "A", 6), None);
        assert_eq!(marker_span(&h, "B", 2), Some(1));
        assert_eq!(marker_span(&h, "C", 0), None);
        // back-to-back spans of one log: the earlier wins (replays more)
        let h = vec![span("A", 1, 0, Some(4)), span("A", 2, 4, None)];
        assert_eq!(marker_span(&h, "A", 3), Some(0));
    }

    #[tokio::test]
    async fn replay_rejects_holes_and_mislabeled_segments() {
        let store = Store::memory(None);
        let shard = 1u16;
        put_seg(&store, "A", 0, shard, 1, "a0").await;
        // ordinal 1 missing inside the closed span [0, 3)
        put_seg(&store, "A", 2, shard, 1, "a2").await;
        let db = crate::partition::open_db(&store, shard, None).await.unwrap();
        let history = vec![span("A", 1, 0, Some(3)), span("B", 2, 0, None)];
        let e = replay_many(&store, &[(shard, &db, &history)]).await.unwrap_err();
        assert!(e.to_string().contains("inside closed span"), "{e}");
        // a segment stored under the wrong ordinal
        let p = segment_path(&store, "A", 1);
        store.raw.put(&p, PutPayload::from(seg_bytes("A", 7, shard, 1, "a1"))).await.unwrap();
        let e = replay_many(&store, &[(shard, &db, &history)]).await.unwrap_err();
        assert!(e.to_string().contains("has header"), "{e}");
        // the end of an open span is fine (0 was applied before the errors)
        store.raw.put(&p, PutPayload::from(seg_bytes("A", 1, shard, 1, "a1"))).await.unwrap();
        assert_eq!(replay_many(&store, &[(shard, &db, &history)]).await.unwrap(), 2);
        assert!(db.get(b"a2").await.unwrap().is_some());
    }

    /// An idle watermark advertises the clock; after the clock steps back,
    /// new seqs must still land above what was advertised.
    #[test]
    fn idle_watermark_is_monotonic_across_clock_steps() {
        let writer = 9u8;
        let wm = Watermark::new(writer, seq_floor(crate::tid::now_micros()));
        let advertised = wm.get();
        assert!(wm.idle());
        crate::tid::set_test_skew_us(-5_000_000);
        let seq = wm.assign();
        crate::tid::set_test_skew_us(0);
        assert!(seq > advertised, "seq {seq} <= advertised watermark {advertised}");
        assert_eq!(seq & 0xff, writer as i64);
        assert!(!wm.idle());
        assert!(wm.get() < seq);
        wm.set_durable(seq);
        assert!(wm.get() >= seq);
        // a log started with the clock ahead of its first seq keeps the writer byte
        let wm = Watermark::new(writer, seq_floor(crate::tid::now_micros() + 1_000_000));
        assert_eq!(wm.assign() & 0xff, writer as i64);
        // the bump keeps the writer byte under a lease cap that ends in 0x00
        let wm = Watermark::new(writer, 0);
        wm.set_lease_expiry(crate::tid::now_micros() - 1_000_000);
        let capped = wm.get();
        assert!(wm.idle());
        crate::tid::set_test_skew_us(-60_000_000);
        let seq = wm.assign();
        crate::tid::set_test_skew_us(0);
        assert!(seq > capped && seq & 0xff == writer as i64);
    }

    #[tokio::test]
    async fn conflict_resolution() {
        let store = Store::memory(None);
        let path = segment_path(&store, "A", 0);
        let ours = Bytes::from(seg_bytes("A", 0, 1, 1, "k"));
        assert_eq!(resolve_conflict(&store, &path, &ours).await, Conflict::Missing);
        store.raw.put(&path, PutPayload::from_bytes(ours.clone())).await.unwrap();
        assert_eq!(resolve_conflict(&store, &path, &ours).await, Conflict::Ours);
        let other = Bytes::from(seg_bytes("A", 0, 1, 1, "other"));
        assert_eq!(resolve_conflict(&store, &path, &other).await, Conflict::Other);
        store.raw.put(&path, PutPayload::from_bytes(segment::fence_object("B"))).await.unwrap();
        assert_eq!(resolve_conflict(&store, &path, &ours).await, Conflict::Fenced);
    }

    fn batch(ordinal: u64, n: usize) -> Arc<LogBatch> {
        Arc::new(LogBatch { log_id: "A".into(), ordinal, events: vec![(ordinal as i64, Bytes::from(vec![0u8; n]))] })
    }

    #[test]
    fn live_ring_is_bounded_by_bytes() {
        let ring = LiveRing::new(1000);
        // no subscribers: nothing retained
        ring.push(batch(0, 400), 400);
        assert_eq!(ring.bytes(), 0);
        let mut fast = ring.subscribe();
        let mut slow = ring.subscribe();
        for o in 1..=2 {
            ring.push(batch(o, 400), 400);
            assert!(matches!(fast.try_recv(), LiveRecv::Batch(b) if b.ordinal == o));
        }
        assert!(matches!(fast.try_recv(), LiveRecv::Empty));
        assert_eq!(ring.bytes(), 800); // the slow one hasn't read them
        assert!(matches!(slow.try_recv(), LiveRecv::Batch(b) if b.ordinal == 1));
        assert_eq!(ring.bytes(), 400); // read by everyone: released
        for o in 3..=6 {
            ring.push(batch(o, 400), 400);
            assert!(matches!(fast.try_recv(), LiveRecv::Batch(b) if b.ordinal == o));
        }
        assert!(ring.bytes() <= 1000);
        assert!(matches!(slow.try_recv(), LiveRecv::Lagged));
        drop(slow);
        assert_eq!(ring.bytes(), 0);
    }
}
