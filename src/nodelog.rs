//! One commit log per node incarnation (`log_id`), shared by every shard the
//! node owns. Group commit across shards means segment size and PUT rate
//! scale with node throughput, not with shard count.
//!
//! workers ──LogEntry{shard}──► sequencer ──(K PUTs in flight)──► finalizer
//!                               assign seq, append  completions    per touched shard:
//!                               (shard, epoch)-     taken in       SlateDB batch + applied
//!                               tagged entries      ordinal order  marker, then acks,
//!                                                                  firehose, watermark
//!
//! Segments go to `log/{log_id}/{ordinal:012}.seg` with If-None-Match on dense
//! ordinals, up to `inflight` (K) PUTs at once. Ordinals are assigned at seal
//! time; the finalizer takes completed PUTs strictly in ordinal order, so acks,
//! apply, the live ring and the watermark only ever cover a gap-free prefix.
//! A crash can leave holes (n missing, n+1 present): the log's *durable
//! prefix* ends at its first missing ordinal, nothing past it was acked, and a
//! dead node's log is closed by a fence object at that first hole (see
//! `first_free` and cluster.rs), so a zombie's PUT there collides and it
//! fail-stops. Objects past the fence are garbage no reader ever reaches.
//! DESIGN.md "Pipelined segment PUTs" has the argument.

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

/// Default segment PUTs in flight per log (`--log-inflight`).
pub const DEFAULT_LOG_INFLIGHT: usize = 4;

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

pub struct ShardSinks {
    map: RwLock<HashMap<u16, Arc<ShardSink>>>,
    /// The log's last durable ordinal (`NodeLog::durable_ordinal`).
    durable: Arc<AtomicU64>,
    retain: Mutex<Retain>,
}

/// How long a closed shard still holds back this log's replay floor: a
/// close that fails after its sink is removed fail-stops the node well
/// within it, and a successor then replays from the shard's durable marker.
const RETIRED_GRACE: Duration = Duration::from_secs(120);

/// What retention (retention.rs) may delete from this log, and what this
/// log's owner has certified (DESIGN.md "Log retention").
#[derive(Default)]
struct Retain {
    /// shard -> (replay floor, insert floor). The replay floor is the lowest
    /// ordinal of this log a crash replay of the shard could still need: the
    /// insert floor (the log's next ordinal when the sink was inserted:
    /// none of the shard's entries at this epoch are below it) until a
    /// checkpoint at or past the insert floor is durable, then its ordinal + 1.
    floors: HashMap<u16, (u64, u64)>,
    /// replay floors of recently closed shards (see RETIRED_GRACE)
    retired: Vec<(u64, Instant)>,
    /// shard -> highest epoch opened here. Opening replays and flushes every
    /// earlier span, so from then on the shard's durable state never replays
    /// a span from before that epoch.
    opened: std::collections::BTreeMap<u16, u64>,
}

impl Default for ShardSinks {
    fn default() -> Self {
        ShardSinks::new(Arc::new(AtomicU64::new(u64::MAX)))
    }
}

impl ShardSinks {
    pub fn new(durable: Arc<AtomicU64>) -> ShardSinks {
        ShardSinks { map: RwLock::default(), durable, retain: Mutex::default() }
    }
    pub fn get(&self, id: u16) -> Option<Arc<ShardSink>> {
        self.map.read().get(&id).cloned()
    }
    /// A shard opened (replayed and flushed) at `s.epoch`: it starts applying.
    pub fn insert(&self, s: Arc<ShardSink>) {
        let floor = self.durable.load(Ordering::Acquire).wrapping_add(1);
        {
            let mut r = self.retain.lock();
            r.floors.insert(s.id, (floor, floor));
            let e = r.opened.entry(s.id).or_default();
            *e = (*e).max(s.epoch);
        }
        self.map.write().insert(s.id, s);
    }
    pub fn remove(&self, id: u16) -> Option<Arc<ShardSink>> {
        let mut r = self.retain.lock();
        if let Some((floor, _)) = r.floors.remove(&id) {
            r.retired.push((floor, Instant::now()));
        }
        drop(r);
        self.map.write().remove(&id)
    }
    pub fn all(&self) -> Vec<Arc<ShardSink>> {
        self.map.read().values().cloned().collect()
    }

    /// The ordinal a checkpoint marker for `shard` must reach (its insert
    /// floor): a marker below it is ambiguous (it can name the end of an
    /// earlier span of this log for the shard, and replay would start there).
    fn insert_floor(&self, shard: u16) -> Option<u64> {
        self.retain.lock().floors.get(&shard).map(|f| f.1)
    }

    /// A checkpoint marker at `ordinal` is durable for `shard`.
    fn checkpointed(&self, shard: u16, ordinal: u64) {
        if let Some(f) = self.retain.lock().floors.get_mut(&shard) {
            if ordinal >= f.1 {
                f.0 = f.0.max(ordinal + 1);
            }
        }
    }

    /// Every ordinal of this log below this may be deleted as far as replay
    /// is concerned: no shard applying from it (or closed within
    /// RETIRED_GRACE) can need it after a crash. Never past the last durable
    /// segment, which is kept so fencing finds the end of the log.
    pub fn replay_floor(&self) -> u64 {
        let durable = self.durable.load(Ordering::Acquire);
        if durable == u64::MAX {
            return 0;
        }
        let mut r = self.retain.lock();
        r.retired.retain(|(_, at)| at.elapsed() < RETIRED_GRACE);
        r.floors.values().map(|f| f.0).chain(r.retired.iter().map(|f| f.0)).fold(durable, u64::min)
    }

    /// shard -> highest epoch this log's owner has opened it at.
    pub fn opened(&self) -> std::collections::BTreeMap<u16, u64> {
        self.retain.lock().opened.clone()
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

/// What occupies one ordinal of a log.
#[derive(Debug)]
pub enum Head {
    Missing,
    Fence,
    Segment(segment::SegHeader),
}

impl Head {
    pub fn is_segment(&self) -> bool {
        matches!(self, Head::Segment(_))
    }
}

/// The header of the object at `log_id/ordinal`, via one small range GET. A
/// segment's header must name the log and ordinal it was read from.
pub async fn read_head(store: &Store, log_id: &str, ordinal: u64) -> anyhow::Result<Head> {
    use object_store::{GetOptions, GetRange};
    let opts = GetOptions { range: Some(GetRange::Bounded(0..4096)), ..Default::default() };
    let data = match store.raw.get_opts(&segment_path(store, log_id, ordinal), opts).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(Head::Missing),
        Err(e) => return Err(e.into()),
    };
    let Some((h, _)) = segment::parse_header(&data)? else { return Ok(Head::Fence) };
    anyhow::ensure!(
        h.log_id == log_id && h.ordinal == ordinal,
        "log object {log_id}/{ordinal} has header {}/{}",
        h.log_id,
        h.ordinal
    );
    Ok(Head::Segment(h))
}

/// The first ordinal below segment `h` that isn't a segment (missing, or a
/// fence), or None if `h` is inside its log's gap-free prefix. Only
/// [h.prefix_end, h.ordinal) needs probing (at most K - 1 small GETs): the
/// writer had every ordinal below prefix_end durable when it sealed `h`.
/// A segment past a hole is garbage (never acked: acks are in order), and
/// past a fence it can only be a zombie's. Ordinals below `floor` (the
/// lowest still stored: retention prunes a log's head) aren't probed.
pub async fn prefix_hole(store: &Store, h: &segment::SegHeader, floor: u64) -> anyhow::Result<Option<u64>> {
    for ord in h.prefix_end.max(floor)..h.ordinal {
        if !read_head(store, &h.log_id, ord).await?.is_segment() {
            return Ok(Some(ord));
        }
    }
    Ok(None)
}

/// The end of `log_id`'s durable prefix: its first ordinal that isn't a
/// segment, and whether a fence is there already. Every segment below it
/// exists; everything above it is garbage (or, on a live log, not acked yet).
/// Probes the highest segment's header and its prefix_end window, so it
/// costs one LIST plus a few small GETs however long the log is.
pub async fn first_free(store: &Store, log_id: &str) -> anyhow::Result<(u64, bool)> {
    use futures::StreamExt;
    let prefix = Path::from(format!("{}/log/{}", store.prefix, log_id));
    let mut listed = Vec::new();
    let mut list = store.raw.list(Some(&prefix));
    while let Some(meta) = list.next().await {
        if let Some(ord) = meta?.location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse::<u64>().ok()) {
            listed.push(ord);
        }
    }
    listed.sort_unstable();
    // the highest segment: everything listed above it is a fence (or gone).
    // With none (retention pruned a dead log down to its fence) the end is
    // the lowest object left.
    let mut free = listed.first().copied().unwrap_or(0);
    for &ord in listed.iter().rev() {
        if let Head::Segment(h) = read_head(store, log_id, ord).await? {
            free = prefix_hole(store, &h, listed[0]).await?.unwrap_or(ord + 1);
            break;
        }
    }
    let fenced = matches!(read_head(store, log_id, free).await?, Head::Fence);
    Ok((free, fenced))
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
        Self::start_with_inflight(store, cfg, DEFAULT_LOG_INFLIGHT, merger_tx)
    }

    /// [`NodeLog::start`] with `inflight` segment PUTs at once.
    pub fn start_with_inflight(store: Store, cfg: NodeLogConfig, inflight: usize, merger_tx: mpsc::UnboundedSender<LogBatch>) -> Arc<NodeLog> {
        let wm = Arc::new(Watermark::new(cfg.writer, seq_floor(crate::tid::now_micros())));
        let (tx, rx) = mpsc::channel(64 * 1024);
        let (fin_tx, fin_rx) = mpsc::channel(4);
        let live = LiveRing::new(DEFAULT_LIVE_RING_BYTES);
        let durable_ordinal = Arc::new(AtomicU64::new(u64::MAX));
        let sinks = Arc::new(ShardSinks::new(durable_ordinal.clone()));
        let log_id: Arc<str> = cfg.log_id.clone().into();
        let seq_cfg = SeqConfig { log_id: cfg.log_id.clone(), max_segment_bytes: cfg.max_segment_bytes, inflight: inflight.max(1), hedge_after: cfg.hedge_after };
        tokio::spawn(run_sequencer(store, seq_cfg, cfg.lease_ok.clone(), wm.clone(), sinks.clone(), rx, fin_tx));
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
            // Nothing of the shard is in this log before its insert floor; a
            // marker below it could also name the end of an earlier span of
            // this log for the shard (A -> B -> A), and replay would start
            // there (DESIGN.md "Log retention"). Its replay marker stands.
            if self.sinks.insert_floor(s.id).is_none_or(|f| ord < f) {
                continue;
            }
            let _g = s.apply_lock.write().await;
            // the finalizer has applied every segment <= ord (it updates
            // durable_ordinal only after applying)
            let mut wb = WriteBatch::new();
            wb.put(META_APPLIED, encode_marker(&self.log_id, ord));
            let written = s.db.write(wb).await.is_ok();
            drop(_g);
            let flushed = s.db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await;
            if written && flushed.is_ok() {
                self.sinks.checkpointed(s.id, ord);
            }
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
    fn new(log_id: &str) -> Open {
        Open { seg: SegmentBuilder::for_log(log_id), frames: Vec::new(), muts: BTreeMap::new(), acks: Vec::new() }
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
            e.frames.push(Frame { prefix: Vec::new(), suffix: Vec::new(), derived_muts: 0 });
        }
        let n = e.frames.len();
        for (i, f) in e.frames.iter().enumerate() {
            let seq = wm.assign();
            let (muts, derived): (&[Mutation], usize) = if i + 1 == n { (&e.muts, f.derived_muts) } else { (&[], 0) };
            let empty = f.prefix.is_empty() && f.suffix.is_empty();
            let range = self.seg.push_derived(seq, e.shard, epoch, |out| if !empty { f.finish(seq, out) }, muts, derived);
            self.frames.push((seq, range));
        }
        self.muts.entry(e.shard).or_default().append(&mut e.muts);
        self.acks.push((e.ack, e.pending, e.enqueued));
    }
}

struct SeqConfig {
    log_id: String,
    max_segment_bytes: usize,
    /// K: segment PUTs in flight at once.
    inflight: usize,
    hedge_after: Duration,
}

/// Seals segments and keeps up to K PUTs in flight. Completions are taken in
/// ordinal order (a later segment that lands first waits for the earlier
/// ones), so the finalizer sees a gap-free ordinal sequence.
///
/// A segment is sealed when a slot is free and either nothing is in flight
/// (the K = 1 behavior: whatever queued during a PUT is the next segment) or
/// it holds at least max_segment_bytes / K. Extra concurrent PUTs therefore
/// only start under load, so the PUT rate at low load stays one per PUT
/// latency while the ceiling is K full segments per PUT latency.
async fn run_sequencer(
    store: Store,
    cfg: SeqConfig,
    lease_ok: Option<LeaseCheck>,
    wm: Arc<Watermark>,
    sinks: Arc<ShardSinks>,
    mut rx: mpsc::Receiver<LogEntry>,
    fin_tx: mpsc::Sender<Sealed>,
) {
    use futures::stream::{FuturesOrdered, StreamExt};
    let SeqConfig { log_id, max_segment_bytes, inflight: k, hedge_after } = cfg;
    let concurrent_fill = (max_segment_bytes / k).max(1);
    let mut ordinal = 0u64;
    // Every ordinal below this has been PUT (the oldest PUT not yet taken
    // from `inflight`): the `prefix_end` recorded in each sealed header.
    let mut prefix_end = 0u64;
    let mut open = Open::new(&log_id);
    let mut inflight: FuturesOrdered<tokio::task::JoinHandle<Sealed>> = FuturesOrdered::new();
    let mut closed = false;
    loop {
        let can_recv = !closed && open.seg.len() < max_segment_bytes;
        tokio::select! {
            biased;
            res = inflight.next(), if !inflight.is_empty() => match res {
                Some(Ok(sealed)) => {
                    prefix_end = sealed.ordinal + 1;
                    if fin_tx.send(sealed).await.is_err() {
                        return;
                    }
                }
                Some(Err(e)) => {
                    tracing::error!(%log_id, "segment upload task failed: {e}; exiting");
                    std::process::exit(2);
                }
                None => {}
            },
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
        let want = if inflight.is_empty() { 1 } else { concurrent_fill };
        if inflight.len() < k && !open.seg.is_empty() && open.seg.len() >= want {
            if let Some(ok) = &lease_ok {
                if !ok() {
                    tracing::error!(%log_id, "node lease lapsed before segment PUT: fail-stop");
                    std::process::exit(5);
                }
            }
            let o = std::mem::replace(&mut open, Open::new(&log_id));
            metrics::SEGMENT_EVENTS.observe(o.frames.len() as f64);
            metrics::COMMIT_STAGE.with_label_values(&["seal_wait"]).observe(o.acks.first().map_or(0.0, |a| a.2.elapsed().as_secs_f64()));
            if inflight.is_empty() {
                prefix_end = ordinal;
            }
            // the header goes into the room the builder left: no copy of
            // the body, and entry ranges are already object offsets
            let last_seq = o.seg.last_seq;
            let data = o.seg.seal(&log_id, ordinal, prefix_end);
            let sealed = Sealed {
                ordinal,
                data: Bytes::from(data),
                frames: o.frames,
                muts: o.muts,
                acks: o.acks,
                last_seq,
                put_secs: 0.0,
            };
            ordinal += 1;
            let (store, log_id) = (store.clone(), log_id.clone());
            inflight.push_back(tokio::spawn(async move {
                let mut sealed = sealed;
                let t = Instant::now();
                upload(&store, &log_id, sealed.ordinal, sealed.data.clone(), hedge_after).await;
                sealed.put_secs = t.elapsed().as_secs_f64();
                sealed
            }));
        }
        if closed && inflight.is_empty() && open.seg.is_empty() {
            return;
        }
    }
}

async fn put_once(store: &Store, path: &Path, data: Bytes) -> object_store::Result<()> {
    let _inflight = metrics::InflightGuard::new(&metrics::PUTS_INFLIGHT);
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
    // At most one hedge per ordinal: with K segments in flight, hedging every
    // retry round of every segment could multiply PUT load K-fold just when
    // S3 is slow.
    let mut hedged = false;
    loop {
        let mut attempts = FuturesUnordered::new();
        attempts.push(put_once(store, &path, data.clone()));
        let hedge = tokio::time::sleep(hedge_after);
        tokio::pin!(hedge);
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
    let mut expect = 0u64;
    while let Some(s) = rx.recv().await {
        // The sequencer hands segments over in ordinal order: everything this
        // does (apply, live ring, merger, watermark, acks) covers a gap-free
        // prefix of the log.
        assert_eq!(s.ordinal, expect, "log {log_id}: finalizer got ordinal {} out of order", s.ordinal);
        expect += 1;
        let t_lock = Instant::now();
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
        metrics::COMMIT_STAGE.with_label_values(&["apply_lock"]).observe((t - t_lock).as_secs_f64());
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
        metrics::COMMIT_STAGE.with_label_values(&["apply"]).observe(t.elapsed().as_secs_f64());
        let t_ack = Instant::now();
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
        metrics::COMMIT_STAGE.with_label_values(&["put"]).observe(s.put_secs);
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
        metrics::COMMIT_STAGE.with_label_values(&["ack"]).observe(t_ack.elapsed().as_secs_f64());
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
            let mut lo = members.iter().map(|m| m.2).min().unwrap_or(0);
            // Retention may have pruned the log's head. What it pruned holds
            // nothing these spans still need (no entries of their shard and
            // epoch, or entries already durable): DESIGN.md "Log retention".
            if let Some(first) = crate::backfill::first_ordinal(store, &log_id).await? {
                lo = lo.max(first);
            }
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

    /// In-memory object store whose segment PUTs can be delayed, or held
    /// forever (the node crashed before they landed), per ordinal.
    #[derive(Debug, Default)]
    struct FaultStore {
        inner: object_store::memory::InMemory,
        delays: Mutex<HashMap<u64, Duration>>,
        holds: Mutex<std::collections::HashSet<u64>>,
    }

    impl std::fmt::Display for FaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FaultStore {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
            if let Some(ord) = location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse::<u64>().ok()) {
                if self.holds.lock().contains(&ord) {
                    futures::future::pending::<()>().await;
                }
                let delay = self.delays.lock().get(&ord).copied();
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(&self, location: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn fault_store() -> (Arc<FaultStore>, Store) {
        let fs = Arc::new(FaultStore::default());
        (fs.clone(), Store { raw: fs, ..Store::memory(None) })
    }

    /// A log on `store` with K = `k`, owning `shard` (epoch 1) into a DB
    /// under its own prefix. Segments hold one entry each (`entry` makes
    /// entries bigger than max_segment_bytes).
    async fn test_log(store: &Store, k: usize, shard: u16, max_segment_bytes: usize) -> (Arc<NodeLog>, Arc<Db>, mpsc::UnboundedReceiver<LogBatch>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let cfg = NodeLogConfig { log_id: "L".into(), writer: 1, max_segment_bytes, hedge_after: Duration::from_secs(10), lease_ok: None };
        let log = NodeLog::start_with_inflight(store.clone(), cfg, k, tx);
        let db = Arc::new(crate::partition::open_db(&Store { prefix: "apply".into(), ..store.clone() }, shard, None).await.unwrap());
        log.sinks.insert(Arc::new(ShardSink { id: shard, epoch: 1, db: db.clone(), apply_lock: Default::default() }));
        (log, db, rx)
    }

    fn entry(shard: u16, key: String, val_len: usize, ack: Option<AckFn>) -> LogEntry {
        LogEntry {
            shard,
            frames: vec![Frame { prefix: key.clone().into_bytes(), suffix: Vec::new(), derived_muts: 0 }],
            muts: vec![Mutation { key: Bytes::from(key), val: Some(Bytes::from(vec![7u8; val_len])) }],
            ack,
            pending: None,
            enqueued: Instant::now(),
        }
    }

    /// Sends `n` one-entry segments; returns the order acks arrived in.
    async fn send_n(log: &NodeLog, shard: u16, n: usize) -> Arc<Mutex<Vec<usize>>> {
        let acked = Arc::new(Mutex::new(Vec::new()));
        for i in 0..n {
            let a = acked.clone();
            let ack: AckFn = Box::new(move |r| {
                r.unwrap();
                a.lock().push(i);
            });
            log.tx.send(entry(shard, format!("k{i}"), 2000, Some(ack))).await.ok().unwrap();
            // let the sequencer seal it alone
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        acked
    }

    async fn exists(store: &Store, ord: u64) -> bool {
        read_head(store, "L", ord).await.unwrap().is_segment()
    }

    /// K = 4 with segment 0 slow: 1..3 land first but are acked, applied,
    /// pushed to the live ring and the merger only after 0, in order.
    #[tokio::test]
    async fn out_of_order_completion_finalizes_in_order() {
        let (fs, store) = fault_store();
        fs.delays.lock().insert(0, Duration::from_millis(300));
        let (log, db, mut merger) = test_log(&store, 4, 3, 1024).await;
        let mut live = log.live.subscribe();
        let acked = send_n(&log, 3, 4).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        for o in 1..4 {
            assert!(exists(&store, o).await, "segment {o} landed");
        }
        assert!(!exists(&store, 0).await);
        assert!(acked.lock().is_empty(), "nothing acked before segment 0");
        assert_eq!(log.durable_ordinal.load(Ordering::Acquire), u64::MAX);
        assert!(matches!(live.try_recv(), LiveRecv::Empty));
        assert!(merger.try_recv().is_err());
        assert!(db.get(b"k1").await.unwrap().is_none(), "segment 1 not applied before 0");
        assert!(!log.wm.idle());
        // segments sealed while 0 was pending record it as the prefix end
        let Head::Segment(h) = read_head(&store, "L", 3).await.unwrap() else { panic!() };
        assert_eq!((h.prefix_end, prefix_hole(&store, &h, 0).await.unwrap()), (0, Some(0)));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(*acked.lock(), vec![0, 1, 2, 3]);
        assert_eq!(log.durable_ordinal.load(Ordering::Acquire), 3);
        assert!(log.wm.idle());
        for o in 0..4 {
            assert!(matches!(live.try_recv(), LiveRecv::Batch(b) if b.ordinal == o));
            assert_eq!(merger.try_recv().unwrap().ordinal, o);
        }
        assert_eq!(prefix_hole(&store, &h, 0).await.unwrap(), None, "segment 3 is in the gap-free prefix now");
        assert!(db.get(b"k3").await.unwrap().is_some());
        assert_eq!(first_free(&store, "L").await.unwrap(), (4, false));
    }

    /// A crash with K = 4 in flight leaves ordinal 1 missing and 2, 3
    /// present. Only 0 was acked; the fence goes at the hole, a zombie can't
    /// write it, and no reader gets past it.
    #[tokio::test]
    async fn crash_hole_is_fenced_and_never_read_past() {
        let (fs, store) = fault_store();
        fs.holds.lock().insert(1); // its PUT never lands: the node "crashed"
        let (log, _db, mut merger) = test_log(&store, 4, 3, 1024).await;
        let acked = send_n(&log, 3, 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(*acked.lock(), vec![0], "acks stop at the hole");
        assert_eq!(merger.try_recv().unwrap().ordinal, 0);
        assert!(merger.try_recv().is_err());
        assert!(exists(&store, 2).await && exists(&store, 3).await, "garbage past the hole");
        assert_eq!(log.durable_ordinal.load(Ordering::Acquire), 0);

        // the fencer's rule: the first non-segment ordinal
        assert_eq!(first_free(&store, "L").await.unwrap(), (1, false));
        let p = segment_path(&store, "L", 1);
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        // (straight to the inner store: the faulty one holds every PUT at 1)
        use object_store::ObjectStore as _;
        fs.inner.put_opts(&p, PutPayload::from_bytes(segment::fence_object("B")), create.clone()).await.unwrap();
        // a zombie's PUT at the fence collides; a second fencer finds the same end
        let zombie = fs.inner.put_opts(&p, PutPayload::from_static(b"zombie"), create).await;
        assert!(matches!(zombie, Err(object_store::Error::AlreadyExists { .. })));
        assert_eq!(first_free(&store, "L").await.unwrap(), (1, true));

        // replay: the dead span ends at the fence; an open span stops at the hole
        let fresh = |name: &'static str| {
            let s = Store { prefix: name.into(), ..store.clone() };
            async move { crate::partition::open_db(&s, 3, None).await.unwrap() }
        };
        for (name, end) in [("r1", Some(1)), ("r2", None)] {
            let db = fresh(name).await;
            let history = vec![span("L", 1, 0, end)];
            assert_eq!(replay_many(&store, &[(3, &db, &history)]).await.unwrap(), 1, "{name}");
            assert!(db.get(b"k0").await.unwrap().is_some());
            for k in ["k1", "k2", "k3"] {
                assert!(db.get(k.as_bytes()).await.unwrap().is_none(), "{name}: {k} replayed past the hole");
            }
        }
        // backfill and seek stop at the fence
        let (tx, mut rx) = mpsc::channel(64);
        crate::backfill::backfill(&store, 0, i64::MAX, &tx).await.unwrap();
        drop(tx);
        let mut n = 0;
        while rx.recv().await.is_some() {
            n += 1;
        }
        assert_eq!(n, 1, "only segment 0's event is served");
        assert_eq!(crate::backfill::seek(&store, "L", i64::MAX - 1).await.unwrap(), 1);
    }

    /// Single-log throughput, K = 1 vs K = 4, 25 ms (sigma 0.5) injected PUT
    /// latency, 256 KB segments of 1 KB entries.
    /// `cargo test --lib nodelog::tests::bench_inflight -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn bench_inflight() {
        const N: usize = 30_000;
        for k in [1, 2, 4] {
            let store = Store::memory(Some((25.0, 0.5)));
            let (log, _db, mut merger) = test_log(&store, k, 0, 256 << 10).await;
            tokio::spawn(async move { while merger.recv().await.is_some() {} });
            let done = Arc::new(AtomicUsize::new(0));
            let all = Arc::new(tokio::sync::Notify::new());
            let t = Instant::now();
            for i in 0..N {
                let (d, all) = (done.clone(), all.clone());
                let ack: AckFn = Box::new(move |r| {
                    r.unwrap();
                    if d.fetch_add(1, Ordering::AcqRel) + 1 == N {
                        all.notify_one();
                    }
                });
                log.tx.send(entry(0, format!("k{i:08}"), 1000, Some(ack))).await.ok().unwrap();
            }
            all.notified().await;
            let secs = t.elapsed().as_secs_f64();
            let segs = log.durable_ordinal.load(Ordering::Acquire) + 1;
            println!("K={k}: {N} entries in {secs:.2} s = {:.0}/s, {segs} segments ({:.1} segs/s)", N as f64 / secs, segs as f64 / secs);
        }
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
