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
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

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

pub fn seq_floor(now_us: u64) -> i64 {
    (now_us as i64) << 8
}

/// Every event with seq <= `get()` is durable and has been handed to the merger.
pub struct Watermark {
    writer: u8,
    inner: Mutex<(i64, i64)>, // (assigned, durable)
    cap: std::sync::atomic::AtomicI64,
}

impl Watermark {
    pub fn new(writer: u8, last: i64) -> Watermark {
        Watermark { writer, inner: Mutex::new((last, last)), cap: std::sync::atomic::AtomicI64::new(i64::MAX) }
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
        let w = self.inner.lock();
        let v = if w.0 > w.1 { w.1 } else { w.1.max(seq_floor(crate::tid::now_micros()) - 1) };
        v.min(self.cap.load(Ordering::Acquire).max(w.1))
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
    pub live: broadcast::Sender<Arc<LogBatch>>,
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
        let (live, _) = broadcast::channel(1024);
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
            Err(object_store::Error::AlreadyExists { .. }) => {
                if let Ok(r) = store.raw.get(&path).await {
                    if let Ok(b) = r.bytes().await {
                        if b == data {
                            STATS.record_put(t.elapsed(), data.len());
                            return;
                        }
                        if matches!(segment::parse(b, false, None), Ok(LogObject::Fence { .. })) {
                            tracing::error!(log_id, ordinal, "our log was fenced by a successor: fail-stop");
                            std::process::exit(3);
                        }
                    }
                }
                tracing::error!(log_id, ordinal, "segment ordinal taken by another writer: fail-stop");
                std::process::exit(3);
            }
            Err(e) => {
                tracing::warn!(log_id, ordinal, "segment PUT failed, retrying: {e}");
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
    live: broadcast::Sender<Arc<LogBatch>>,
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
        for (sink, muts) in &targets {
            let mut wb = WriteBatch::new();
            for m in muts.iter() {
                match &m.val {
                    Some(v) => wb.put(&m.key, v),
                    None => wb.delete(&m.key),
                }
            }
            wb.put(META_APPLIED, encode_marker(&log_id, s.ordinal));
            if let Err(e) = sink.db.write(wb).await {
                tracing::error!(shard = sink.id, "state apply failed: {e}; exiting");
                std::process::exit(4);
            }
        }
        let _ = STATS.apply_us.lock().record(t.elapsed().as_micros().max(1) as u64);
        metrics::APPLY_DURATION.observe(t.elapsed().as_secs_f64());
        let events: Vec<(i64, Bytes)> =
            s.frames.iter().filter(|(_, r)| !r.is_empty()).map(|(seq, r)| (*seq, s.data.slice(r.clone()))).collect();
        let batch = LogBatch { log_id: log_id.clone(), ordinal: s.ordinal, events };
        let _ = live.send(Arc::new(batch.clone()));
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
            Some((log, _)) => history.iter().rposition(|s| &s.log_id == log).unwrap_or(0),
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
                let Some(data) = obj? else { break };
                let LogObject::Segment(_, entries) = segment::parse(data, true, None)? else { break };
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
