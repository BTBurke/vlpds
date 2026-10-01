//! CPU workers. Each repo is owned by one worker thread (hash of DID), which
//! keeps its MST in memory, coalesces queued writes into commits, signs them
//! and hands them to the repo's partition sequencer without waiting for
//! durability (acks are released by the partition in log order).

use crate::car;
use crate::cid::Cid;
use crate::crypto::Keypair;
use crate::events::{self, RepoOp};
use crate::metrics;
use crate::mst::Tree;
use crate::partition::{LogEntry, Partition};
use crate::segment::Mutation;
use crate::state::{self, Head};
use crate::stats::STATS;
use crate::tid::{self, Tid};
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Spec limits for a single commit.
pub const MAX_COMMIT_OPS: usize = 200;
pub const MAX_COMMIT_RECORD_BYTES: usize = 1_000_000;

#[derive(Debug, Clone)]
pub enum WriteError {
    RepoNotFound,
    /// Account is deactivated / taken down / deleted (the status string).
    RepoInactive(String),
    InvalidSwap(String),
    Invalid(String),
    Internal(String),
    /// The repo's shard isn't served by this node right now (moving between
    /// owners); clients should retry.
    Unavailable(String),
}

pub enum Write {
    /// `blobs`: blob CIDs referenced by the record (for blob ref tracking).
    Create {
        collection: String,
        rkey: String,
        cid: Cid,
        bytes: Bytes,
        blobs: Vec<Cid>,
    },
    /// putRecord (upsert) / applyWrites#update (`must_exist`: the record has
    /// to be there, as MST update requires). `swap`: Some(None) = must not exist.
    Update {
        collection: String,
        rkey: String,
        cid: Cid,
        bytes: Bytes,
        blobs: Vec<Cid>,
        swap: Option<Option<Cid>>,
        must_exist: bool,
    },
    Delete {
        collection: String,
        rkey: String,
        swap: Option<Option<Cid>>,
    },
}

impl Write {
    fn path(&self) -> String {
        let (c, r) = match self {
            Write::Create {
                collection, rkey, ..
            }
            | Write::Update {
                collection, rkey, ..
            }
            | Write::Delete {
                collection, rkey, ..
            } => (collection, rkey),
        };
        format!("{c}/{r}")
    }
}

#[derive(Debug, Clone)]
pub enum WriteOutcome {
    Create { path: String, cid: Cid },
    Update { path: String, cid: Cid },
    Delete,
}

#[derive(Debug, Clone)]
pub struct CommitAck {
    pub commit: Cid,
    pub rev: Tid,
    pub results: Vec<WriteOutcome>,
}

pub type WriteReply = oneshot::Sender<Result<CommitAck, WriteError>>;

pub struct WriteReq {
    pub did: Arc<str>,
    pub writes: Vec<Write>,
    pub swap_commit: Option<Cid>,
    pub reply: WriteReply,
}

pub struct CreateRepoReq {
    pub did: Arc<str>,
    pub handle: String,
    pub key: Arc<Keypair>,
    pub account_json: Bytes,
    /// Genesis records (path, cid, bytes); empty for normal account creation.
    pub records: Vec<(String, Cid, Bytes)>,
    pub reply: oneshot::Sender<Result<Head, WriteError>>,
}

/// Account-level changes, ordered with the repo's commits by its worker.
pub struct AccountReq {
    pub did: Arc<str>,
    pub op: AccountOp,
    pub reply: oneshot::Sender<Result<Head, WriteError>>,
}

/// A read-modify-write of the account, run by the repo's worker on its
/// current copy, so concurrent changes compose instead of one overwriting the
/// other with a stale snapshot. Preconditions belong inside it: it sees the
/// state it changes. `Ok(false)` = nothing to do (no write, no events); an
/// error rejects the op unchanged.
pub type AccountMutation = Box<dyn FnOnce(&mut state::Account) -> Result<bool, WriteError> + Send>;

pub enum AccountOp {
    /// Persist the mutated account record. A changed handle moves the handle
    /// index. `identity_event` emits #identity; `account_event` emits #account
    /// with the account's status (status None = active). Writes are rejected
    /// while status is Some.
    Update {
        mutate: AccountMutation,
        identity_event: bool,
        account_event: bool,
    },
    /// Replace the whole repo contents (importRepo / reset): new commit, #sync
    /// (none while the account is deactivated: activation emits it).
    /// Records are (path, cid, bytes, blob refs).
    ReplaceRepo {
        records: Vec<(String, Cid, Bytes, Vec<Cid>)>,
    },
    /// Delete the account and repo: #account(active=false, status=deleted).
    Delete,
    /// Persist a reactivated account: #account, #identity and #sync of the
    /// current commit, as the reference's sequenceAccountActivation.
    Activate { mutate: AccountMutation },
}

pub enum Queued {
    Write(WriteReq),
    Account(AccountReq),
    Snapshot(SnapshotReq),
}

impl Queued {
    fn did(&self) -> &Arc<str> {
        match self {
            Queued::Write(r) => &r.did,
            Queued::Account(r) => &r.did,
            Queued::Snapshot(r) => &r.did,
        }
    }
    fn fail(self, e: WriteError) {
        match self {
            Queued::Write(r) => {
                let _ = r.reply.send(Err(e));
            }
            Queued::Account(r) => {
                let _ = r.reply.send(Err(e));
            }
            Queued::Snapshot(r) => {
                let _ = r.reply.send(Err(e));
            }
        }
    }
}

pub enum WorkerMsg {
    Write(WriteReq),
    Account(AccountReq),
    Snapshot(SnapshotReq),
    CreateRepo(CreateRepoReq),
    /// Forget cached repos of a partition this node no longer owns; replies
    /// once no repo state referencing it remains in this worker.
    DropPartition(u16, oneshot::Sender<()>),
    Loaded {
        did: Arc<str>,
        res: anyhow::Result<Option<RepoState>>,
    },
    /// The last [`Workers`] handle is gone: finish the batch and exit (each
    /// worker holds a sender to its own channel for `Loaded`, so the channel
    /// alone never disconnects).
    Shutdown,
    /// Load a large repo ahead of its first request (after a shard open);
    /// `done` gets whether a load was started and cached it.
    Preload { did: Arc<str>, done: oneshot::Sender<bool> },
    /// What the worker holds for a repo (None = not cached).
    CacheInfo { did: Arc<str>, reply: oneshot::Sender<Option<CachedRepo>> },
}

/// A cached repo, as [`WorkerMsg::CacheInfo`] reports it.
#[derive(Clone, Debug)]
pub struct CachedRepo {
    pub records: u64,
    /// Pinned as a large repo.
    pub large: bool,
    /// Bytes charged to the cache budget.
    pub charge: usize,
}

/// The repo as of its latest *durable* commit: what exports and proofs serve.
/// Published by the commit's ack (after the state apply), so it never shows a
/// commit that could still be lost.
pub struct DurableView {
    pub head: Head,
    pub tree: Tree,
    /// The repo's MST node index (getBlocks), shared with the worker.
    pub nodes: crate::mst::SharedNodeIndex,
}

pub type ViewCell = Arc<parking_lot::RwLock<Arc<DurableView>>>;

pub struct SnapshotReq {
    pub did: Arc<str>,
    pub reply: oneshot::Sender<Result<ViewCell, WriteError>>,
}

pub struct RepoState {
    pub did: Arc<str>,
    pub partition: Arc<Partition>,
    pub tree: Tree,
    pub head: Head,
    pub key: Arc<Keypair>,
    pub pending: Arc<AtomicU32>,
    pub account: state::Account,
    /// Records per collection (drives the C/{collection}\0{did} index).
    pub collections: HashMap<String, u32>,
    /// Blob refs per record path (drives the b/{did}\0{blob}\0{path} index).
    pub blob_refs: HashMap<String, Vec<Cid>>,
    pub view: ViewCell,
    pub nodes: crate::mst::SharedNodeIndex,
    /// Large (pinned in the cache, `L/` index key written); see `Worker::settle`.
    pub large: bool,
    /// Approximate heap charged to the worker's cache ([`repo_bytes`]).
    pub charge: usize,
}

impl RepoState {
    fn durable_view(&self) -> Arc<DurableView> {
        Arc::new(DurableView { head: self.head.clone(), tree: self.tree.clone(), nodes: self.nodes.clone() })
    }

    /// Records in the repo (from the per-collection counts).
    pub fn records(&self) -> u64 {
        self.collections.values().map(|n| *n as u64).sum()
    }
}

/// In-memory MST bytes per record: 220-250 B measured on real repos
/// (bench/results/storage-2026-10-02, jemalloc deltas of cold loads).
pub const MST_BYTES_PER_RECORD: usize = 240;
/// Per-repo overhead outside the tree (account, key, head, views, maps).
const REPO_BASE_BYTES: usize = 2048;

/// Approximate heap of a cached repo: what the cache budget counts.
pub fn repo_bytes(st: &RepoState, records: u64) -> usize {
    REPO_BASE_BYTES + records as usize * MST_BYTES_PER_RECORD + st.blob_refs.len() * 96
}

/// Repo cache limits, per worker.
#[derive(Clone, Copy, Debug)]
pub struct CacheLimits {
    /// Unpinned repos held (LRU).
    pub entries: usize,
    /// Approximate heap of unpinned repos ([`repo_bytes`]); 0 = unbounded.
    pub bytes: usize,
    /// Repos with at least this many records are pinned: never evicted by
    /// the LRU, indexed under `L/` and preloaded when their shard opens
    /// (a 5M-record repo is ~1.2 GB of heap and ~8 s of cold load). They
    /// stay pinned until they drop below half of it. 0 = pin nothing.
    pub pin_records: u64,
}

pub const DEFAULT_PIN_RECORDS: u64 = 500_000;

impl From<usize> for CacheLimits {
    fn from(entries: usize) -> CacheLimits {
        CacheLimits { entries, bytes: 0, pin_records: DEFAULT_PIN_RECORDS }
    }
}

fn new_view(head: &Head, tree: &Tree, nodes: &crate::mst::SharedNodeIndex) -> ViewCell {
    Arc::new(parking_lot::RwLock::new(Arc::new(DurableView {
        head: head.clone(),
        tree: tree.clone(),
        nodes: nodes.clone(),
    })))
}

#[derive(Clone)]
pub struct Workers {
    pub senders: Arc<WorkerSenders>,
}

/// The workers' channels. Dropping the last handle stops the threads.
pub struct WorkerSenders(Vec<Sender<WorkerMsg>>);

impl std::ops::Deref for WorkerSenders {
    type Target = Vec<Sender<WorkerMsg>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for WorkerSenders {
    fn drop(&mut self) {
        for tx in &self.0 {
            let _ = tx.send(WorkerMsg::Shutdown);
        }
    }
}

impl Workers {
    pub fn route(&self, did: &str) -> &Sender<WorkerMsg> {
        let h = state::did_hash(did);
        &self.senders[((h >> 32) % self.senders.len() as u64) as usize]
    }
}

pub type PartitionLookup = Arc<dyn Fn(&str) -> Option<Arc<Partition>> + Send + Sync>;

pub fn spawn(
    n: usize,
    limits: impl Into<CacheLimits>,
    partitions: PartitionLookup,
    rt: tokio::runtime::Handle,
) -> Workers {
    let limits = limits.into();
    let mut senders = Vec::with_capacity(n);
    let mut receivers = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = crossbeam_channel::unbounded();
        senders.push(tx);
        receivers.push(rx);
    }
    for (i, rx) in receivers.into_iter().enumerate() {
        let me = senders[i].clone();
        let partitions = partitions.clone();
        let rt = rt.clone();
        std::thread::Builder::new()
            .name(format!("repo-worker-{i}"))
            .spawn(move || Worker::new(i, me, partitions, rt, limits).run(rx))
            .unwrap();
    }
    Workers {
        senders: Arc::new(WorkerSenders(senders)),
    }
}

struct Worker {
    label: String,
    me: Sender<WorkerMsg>,
    partitions: PartitionLookup,
    rt: tokio::runtime::Handle,
    cache: lru::LruCache<Arc<str>, RepoState>,
    limits: CacheLimits,
    /// Sum of the cached repos' charges, and the pinned (large) share.
    bytes: usize,
    pinned: usize,
    pinned_bytes: usize,
    loading: HashMap<Arc<str>, Vec<Queued>>,
    /// Preloads in flight (their `loading` entry starts empty).
    preloads: HashMap<Arc<str>, oneshot::Sender<bool>>,
    /// Repos whose commit failed while earlier commits were still in flight:
    /// held out of the cache (their requests buffer in `loading`) until those
    /// commits are durable, then reloaded. Reloading any sooner would build
    /// the next commit on a durable head that lacks them: a fork.
    draining: HashMap<Arc<str>, RepoState>,
    clock_id: u64,
    /// Got [`WorkerMsg::Shutdown`]: exit after this batch.
    stop: bool,
}

impl Worker {
    fn new(
        idx: usize,
        me: Sender<WorkerMsg>,
        partitions: PartitionLookup,
        rt: tokio::runtime::Handle,
        limits: CacheLimits,
    ) -> Worker {
        Worker {
            label: idx.to_string(),
            me,
            partitions,
            rt,
            cache: lru::LruCache::unbounded(),
            limits,
            bytes: 0,
            pinned: 0,
            pinned_bytes: 0,
            loading: HashMap::new(),
            preloads: HashMap::new(),
            draining: HashMap::new(),
            clock_id: rand::random::<u64>() & 0x3ff,
            stop: false,
        }
    }

    fn run(mut self, rx: Receiver<WorkerMsg>) {
        let mut msgs = Vec::with_capacity(8192);
        loop {
            let first = if self.draining.is_empty() {
                match rx.recv() {
                    Ok(m) => m,
                    Err(_) => break,
                }
            } else {
                match rx.recv_timeout(Duration::from_millis(5)) {
                    Ok(m) => m,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        self.release_drained();
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            };
            msgs.push(first);
            while msgs.len() < 8192 {
                match rx.try_recv() {
                    Ok(m) => msgs.push(m),
                    Err(_) => break,
                }
            }
            metrics::WORKER_BATCH.observe(msgs.len() as f64);
            metrics::WORKER_QUEUE
                .with_label_values(&[&self.label])
                .set(rx.len() as i64);
            // Group writes per repo (preserving arrival order) so each repo's
            // queue becomes as few commits as possible.
            let mut order: Vec<Arc<str>> = Vec::new();
            let mut groups: HashMap<Arc<str>, Vec<Queued>> = HashMap::new();
            for m in msgs.drain(..) {
                let queued = match m {
                    WorkerMsg::Write(req) => Some(Queued::Write(req)),
                    WorkerMsg::Account(req) => Some(Queued::Account(req)),
                    WorkerMsg::Snapshot(req) => Some(Queued::Snapshot(req)),
                    other => {
                        self.handle_control(other, &mut order, &mut groups);
                        None
                    }
                };
                if let Some(q) = queued {
                    let did = q.did().clone();
                    if let Some(buf) = self.loading.get_mut(&did) {
                        metrics::REPO_CACHE.with_label_values(&["loading"]).inc();
                        buf.push(q);
                    } else if self.cache.contains(&did) {
                        metrics::REPO_CACHE.with_label_values(&["hit"]).inc();
                        let g = groups.entry(did.clone()).or_insert_with(|| {
                            order.push(did.clone());
                            Vec::new()
                        });
                        g.push(q);
                    } else {
                        metrics::REPO_CACHE.with_label_values(&["miss"]).inc();
                        self.start_load(q);
                    }
                }
            }
            for did in order {
                let reqs = groups.remove(&did).unwrap_or_default();
                if reqs.is_empty() {
                    continue;
                }
                let Some(st) = self.cache.get_mut(&did) else {
                    // Dropped by a DropPartition in this same batch (its shard
                    // closed: a handback, a takeover or a split's freeze).
                    // These requests were silently dropped (500 "worker
                    // dropped request"); load again instead, which answers
                    // 503 "not owned" (retryable) or serves a new owner.
                    let mut reqs = reqs.into_iter();
                    if let Some(first) = reqs.next() {
                        match self.loading.get_mut(&did) {
                            Some(buf) => buf.push(first),
                            None => self.start_load(first),
                        }
                        if let Some(buf) = self.loading.get_mut(&did) {
                            buf.extend(reqs);
                        }
                    }
                    continue;
                };
                if let Err(e) = process(st, reqs, self.clock_id) {
                    // MST errors mean in-memory state can't be trusted: drop it and
                    // reload from durable state, once nothing is in flight.
                    tracing::error!(%did, "commit failed, evicting repo: {e:#}");
                    self.discard(did);
                    continue;
                }
                self.settle(&did);
            }
            self.release_drained();
            self.evict();
            metrics::CACHED_REPOS
                .with_label_values(&[&self.label])
                .set(self.cache.len() as i64);
            metrics::REPO_CACHE_BYTES.with_label_values(&[&self.label]).set(self.bytes as i64);
            metrics::PINNED_REPOS.with_label_values(&[&self.label]).set(self.pinned as i64);
            if self.stop {
                break;
            }
        }
    }

    fn handle_control(
        &mut self,
        m: WorkerMsg,
        order: &mut Vec<Arc<str>>,
        groups: &mut HashMap<Arc<str>, Vec<Queued>>,
    ) {
        {
            {
                match m {
                    WorkerMsg::Write(_) | WorkerMsg::Account(_) | WorkerMsg::Snapshot(_) => unreachable!(),
                    WorkerMsg::Loaded { did, res } => {
                        let buffered = self.loading.remove(&did).unwrap_or_default();
                        let preload = self.preloads.remove(&did);
                        let preloaded = preload.is_some();
                        let cached = matches!(&res, Ok(Some(st)) if (self.partitions)(&did).is_some_and(|p| Arc::ptr_eq(&p, &st.partition)));
                        if let Some(done) = preload {
                            metrics::REPO_PRELOADS.with_label_values(&[match &res {
                                _ if cached => "loaded",
                                Ok(Some(_)) => "stale",
                                Ok(None) => "not_found",
                                Err(_) => "error",
                            }]).inc();
                            let _ = done.send(cached);
                        }
                        match res {
                            // The shard closed (and maybe reopened) while the load
                            // was in flight: the state belongs to an ownership that
                            // ended and must never be cached, or commits built on it
                            // chain past whatever the shard saw since (bench/ha N6).
                            // close() purges the cache after unrouting the shard, so
                            // checking here closes the window. Reload against the
                            // current partition, if any.
                            Ok(Some(st)) if !(self.partitions)(&did).is_some_and(|p| Arc::ptr_eq(&p, &st.partition)) => {
                                metrics::REPO_LOADS.with_label_values(&["stale"]).inc();
                                let mut buffered = buffered.into_iter();
                                if let Some(first) = buffered.next() {
                                    self.start_load(first);
                                    if let Some(buf) = self.loading.get_mut(&did) {
                                        buf.extend(buffered);
                                    }
                                }
                            }
                            Ok(Some(mut st)) => {
                                STATS.repo_loads.fetch_add(1, Ordering::Relaxed);
                                metrics::REPO_LOADS.with_label_values(&["ok"]).inc();
                                // preloaded: found in the L/ index, so the key
                                // exists (settle deletes it if it's stale)
                                st.large |= preloaded;
                                self.cache_put(did.clone(), st);
                                self.settle(&did);
                                order.push(did.clone());
                                groups.insert(did, buffered);
                            }
                            Ok(None) => {
                                metrics::REPO_LOADS.with_label_values(&["not_found"]).inc();
                                for r in buffered {
                                    r.fail(WriteError::RepoNotFound);
                                }
                            }
                            Err(e) => {
                                tracing::error!(%did, "repo load failed: {e:#}");
                                metrics::REPO_LOADS.with_label_values(&["error"]).inc();
                                for r in buffered {
                                    let msg = format!("repo load failed: {e}");
                                    r.fail(if msg.contains("not owned") { WriteError::Unavailable(msg) } else { WriteError::Internal(msg) });
                                }
                            }
                        }
                    }
                    WorkerMsg::CreateRepo(req) => self.create_repo(req),
                    WorkerMsg::Shutdown => self.stop = true,
                    WorkerMsg::CacheInfo { did, reply } => {
                        let _ = reply.send(self.cache.peek(&did).map(|st| CachedRepo { records: st.records(), large: st.large, charge: st.charge }));
                    }
                    WorkerMsg::Preload { did, done } => {
                        if self.cache.contains(&did) || self.loading.contains_key(&did) || self.draining.contains_key(&did) {
                            metrics::REPO_PRELOADS.with_label_values(&["cached"]).inc();
                            let _ = done.send(false);
                        } else {
                            self.loading.insert(did.clone(), Vec::new());
                            self.preloads.insert(did.clone(), done);
                            self.spawn_load(did);
                        }
                    }
                    WorkerMsg::DropPartition(p, done) => {
                        let drop: Vec<Arc<str>> = self
                            .cache
                            .iter()
                            .filter(|(_, st)| st.partition.id == p)
                            .map(|(d, _)| d.clone())
                            .collect();
                        for d in drop {
                            self.cache_pop(&d);
                        }
                        // the shard is unrouted (and its close barrier settles
                        // the in-flight commits): buffered requests go through a
                        // load, which fails over as "not owned"
                        let drained: Vec<Arc<str>> =
                            self.draining.iter().filter(|(_, st)| st.partition.id == p).map(|(d, _)| d.clone()).collect();
                        for d in drained {
                            self.draining.remove(&d);
                            self.reload_buffered(&d);
                        }
                        let _ = done.send(());
                    }
                }
            }
        }
    }

    /// Drops a repo's in-memory state; it reloads from durable state on the
    /// next write, but only once its in-flight commits are durable.
    fn discard(&mut self, did: Arc<str>) {
        let Some(st) = self.cache_pop(&did) else { return };
        if st.pending.load(Ordering::Acquire) > 0 {
            self.loading.entry(did.clone()).or_default();
            self.draining.insert(did, st);
        }
    }

    /// Reloads drained repos (see `draining`) whose commits are all durable.
    fn release_drained(&mut self) {
        let done: Vec<Arc<str>> =
            self.draining.iter().filter(|(_, st)| st.pending.load(Ordering::Acquire) == 0).map(|(d, _)| d.clone()).collect();
        for did in done {
            self.draining.remove(&did);
            self.reload_buffered(&did);
        }
    }

    /// Starts a load for the requests buffered under `did` (if any).
    fn reload_buffered(&mut self, did: &Arc<str>) {
        let mut buffered = self.loading.remove(did).unwrap_or_default().into_iter();
        if let Some(first) = buffered.next() {
            self.start_load(first);
            if let Some(buf) = self.loading.get_mut(did) {
                buf.extend(buffered);
            }
        }
    }

    fn start_load(&mut self, req: Queued) {
        let did = req.did().clone();
        self.loading.insert(did.clone(), vec![req]);
        self.spawn_load(did);
    }

    /// Loads `did` in the background (its `loading` entry is set); the
    /// result comes back as [`WorkerMsg::Loaded`].
    fn spawn_load(&mut self, did: Arc<str>) {
        metrics::LOADING_REPOS.inc();
        let me = self.me.clone();
        let Some(partition) = (self.partitions)(&did) else {
            let _ = me.send(WorkerMsg::Loaded {
                did,
                res: Err(anyhow::anyhow!("partition not owned by this node")),
            });
            return;
        };
        self.rt.spawn(async move {
            let t = Instant::now();
            let res = load_repo(partition, did.clone()).await;
            let _ = STATS
                .load_us
                .lock()
                .record(t.elapsed().as_micros().max(1) as u64);
            metrics::REPO_LOAD_DURATION.observe(t.elapsed().as_secs_f64());
            if let Ok(Some(st)) = &res {
                metrics::REPO_LOAD_BY_SIZE.with_label_values(&[size_bucket(st.records())]).observe(t.elapsed().as_secs_f64());
            }
            metrics::LOADING_REPOS.dec();
            let _ = me.send(WorkerMsg::Loaded { did, res });
        });
    }

    /// Inserts a repo, keeping the byte and pin counts.
    fn cache_put(&mut self, did: Arc<str>, st: RepoState) {
        self.count(&st, true);
        if let Some(old) = self.cache.put(did, st) {
            self.count(&old, false);
        }
    }

    fn cache_pop(&mut self, did: &Arc<str>) -> Option<RepoState> {
        let st = self.cache.pop(did)?;
        self.count(&st, false);
        Some(st)
    }

    fn count(&mut self, st: &RepoState, add: bool) {
        let sign = |v: &mut usize, n: usize| if add { *v += n } else { *v -= n };
        sign(&mut self.bytes, st.charge);
        if st.large {
            sign(&mut self.pinned, 1);
            sign(&mut self.pinned_bytes, st.charge);
        }
    }

    /// Re-charges a cached repo after it changed, and pins or unpins it by
    /// size. Crossing the threshold also writes or deletes its `L/` index
    /// key (a private-state log entry), so the next owner preloads it.
    fn settle(&mut self, did: &Arc<str>) {
        let pin = self.limits.pin_records;
        let Some(st) = self.cache.peek_mut(did) else { return };
        let records = st.records();
        let large = pin > 0 && records >= if st.large { pin / 2 } else { pin };
        let charge = repo_bytes(st, records);
        let (was_large, was_charge) = (st.large, st.charge);
        if charge == was_charge && large == was_large {
            return;
        }
        st.charge = charge;
        st.large = large;
        if large != was_large {
            let key = Bytes::from(state::large_repo_key(&st.did));
            let val = large.then(|| Bytes::copy_from_slice(&records.to_be_bytes()));
            let entry = LogEntry {
                shard: st.partition.id,
                frames: Vec::new(),
                muts: vec![Mutation { key, val }],
                ack: None,
                pending: Some(st.pending.clone()),
                enqueued: Instant::now(),
            };
            if let Err(e) = send_entry(st, entry) {
                tracing::warn!(%did, "large-repo index update not logged: {e}");
            }
        }
        self.bytes = self.bytes + charge - was_charge;
        if was_large {
            self.pinned -= 1;
            self.pinned_bytes -= was_charge;
        }
        if large {
            self.pinned += 1;
            self.pinned_bytes += charge;
        }
    }

    /// Evicts least recently used repos while the unpinned ones exceed the
    /// entry or byte budget. Pinned (large) repos and repos with commits in
    /// flight (durable state lags memory) are skipped.
    fn evict(&mut self) {
        let over = |n: usize, b: usize, l: &CacheLimits| n > l.entries || (l.bytes > 0 && b > l.bytes);
        let (mut n, mut b) = (self.cache.len() - self.pinned, self.bytes - self.pinned_bytes);
        if !over(n, b, &self.limits) {
            return;
        }
        let mut victims = Vec::new();
        for (scanned, (did, st)) in self.cache.iter().rev().enumerate() {
            if !over(n, b, &self.limits) || scanned > self.pinned + 1024 {
                break;
            }
            if st.large || st.pending.load(Ordering::Acquire) > 0 {
                continue;
            }
            n -= 1;
            b -= st.charge;
            victims.push(did.clone());
        }
        for did in victims {
            self.cache_pop(&did);
            metrics::REPO_EVICTIONS.inc();
        }
    }

    fn create_repo(&mut self, req: CreateRepoReq) {
        // a deleted repo's cached state doesn't block its DID coming back
        // (migration in), once nothing of it is in flight
        if self.cache.peek(&req.did).is_some_and(|st| {
            st.account.status.as_deref() == Some("deleted") && st.pending.load(Ordering::Acquire) == 0
        }) {
            self.cache_pop(&req.did);
        }
        if self.cache.contains(&req.did) || self.loading.contains_key(&req.did) {
            let _ = req
                .reply
                .send(Err(WriteError::Invalid("repo already exists".into())));
            return;
        }
        let Some(partition) = (self.partitions)(&req.did) else {
            let _ = req.reply.send(Err(WriteError::Unavailable(
                "partition not owned by this node".into(),
            )));
            return;
        };
        let mut tree = Tree::new();
        for (path, cid, _) in &req.records {
            if let Err(e) = tree.insert_no_proof(path.as_bytes(), *cid) {
                let _ = req.reply.send(Err(WriteError::Invalid(e.to_string())));
                return;
            }
        }
        let data = match tree.root_cid() {
            Ok(d) => d,
            Err(e) => {
                let _ = req.reply.send(Err(WriteError::Internal(e.to_string())));
                return;
            }
        };
        let rev = tid::next_rev(None, self.clock_id);
        let (commit, commit_block) = sign_commit(&req.did, &rev.to_string(), &data, &req.key);
        let head = Head {
            commit,
            data,
            rev,
            commit_block: commit_block.clone(),
        };
        let time = events::now_rfc3339();
        let mut car_bytes = Vec::with_capacity(commit_block.len() + 128);
        car::write_header(&mut car_bytes, &commit);
        car::write_block(&mut car_bytes, &commit, &commit_block);
        let account: state::Account = match serde_json::from_slice(&req.account_json) {
            Ok(a) => a,
            Err(e) => {
                let _ = req.reply.send(Err(WriteError::Internal(format!("bad account json: {e}"))));
                return;
            }
        };
        // An account created inactive (migration in) is announced only when
        // activated (reference createAccount: no events when deactivated).
        let frames = if account.status.is_some() {
            Vec::new()
        } else {
            vec![
                events::identity_frame(&req.did, &req.handle, &time),
                events::account_frame(&req.did, true, None, &time),
                events::sync_frame(&req.did, &rev.to_string(), &car_bytes, &time),
            ]
        };
        let mut muts = Vec::with_capacity(3 + req.records.len());
        let mut colls = HashSet::new();
        for (path, cid, bytes) in &req.records {
            muts.push(Mutation {
                key: state::record_key(&req.did, path).into(),
                val: Some(state::record_value(cid, rev.0, bytes)),
            });
            muts.push(Mutation {
                key: state::record_cid_key(&req.did, cid, path).into(),
                val: Some(Bytes::new()),
            });
            if colls.insert(collection_of(path)) {
                muts.push(Mutation {
                    key: state::collection_key(collection_of(path), &req.did).into(),
                    val: Some(Bytes::new()),
                });
            }
        }
        muts.extend([
            Mutation {
                key: state::account_key(&req.did).into(),
                val: Some(req.account_json.clone()),
            },
            Mutation {
                key: state::handle_key(&req.did, &req.handle).into(),
                val: Some(Bytes::from(req.did.to_string())),
            },
            Mutation {
                key: state::head_key(&req.did).into(),
                val: Some(head.encode()),
            },
        ]);
        let pending = Arc::new(AtomicU32::new(1));
        let reply = req.reply;
        let h2 = head.clone();
        let entry = LogEntry {
            shard: partition.id,
            frames,
            muts,
            ack: Some(Box::new(move |r| {
                let _ = reply.send(
                    r.map(|_| h2)
                        .map_err(|e| WriteError::Internal(e.to_string())),
                );
            })),
            pending: Some(pending.clone()),
            enqueued: Instant::now(),
        };
        let mut collections = HashMap::new();
        for (path, _, _) in &req.records {
            *collections
                .entry(collection_of(path).to_string())
                .or_insert(0) += 1;
        }
        let nodes = crate::mst::SharedNodeIndex::default();
        let view = new_view(&head, &tree, &nodes);
        let st = RepoState {
            did: req.did.clone(),
            partition: partition.clone(),
            tree,
            head,
            key: req.key,
            pending,
            account,
            collections,
            blob_refs: HashMap::new(),
            view,
            nodes,
            large: false,
            charge: 0,
        };
        let did = req.did.clone();
        self.cache_put(req.did, st);
        if partition.tx.blocking_send(entry).is_err() {
            tracing::error!("partition sequencer gone");
        }
        self.settle(&did);
    }
}

/// Large-repo preloads in flight per node: each is a full repo scan (a 1M-
/// record repo is ~1.5 s of CPU and ~240 MB), so a takeover of many shards
/// doesn't starve request-driven loads.
const PRELOAD_CONCURRENCY: usize = 4;

/// Preloads the large repos (`L/` index) of freshly opened shards in the
/// background, [`PRELOAD_CONCURRENCY`] at a time, so their first write
/// doesn't pay the cold load. Stops early if the workers shut down; a shard
/// closed meanwhile just fails its loads (the worker drops stale ones).
pub fn spawn_preload(workers: &Workers, shards: Vec<(u16, Arc<slatedb::Db>)>) {
    use futures::StreamExt;
    let senders = Arc::downgrade(&workers.senders);
    tokio::spawn(async move {
        let t = Instant::now();
        let mut dids: Vec<Arc<str>> = Vec::new();
        for (shard, db) in shards {
            let r: anyhow::Result<()> = async {
                let mut it = state::FamilyScan::new(db.as_ref(), state::LARGE_REPO_FAMILY, None, &Default::default()).await?;
                while let Some(kv) = it.next().await? {
                    dids.push(String::from_utf8_lossy(state::slot_did(&kv.key, state::LARGE_REPO_FAMILY.len()).1).into());
                }
                Ok(())
            }
            .await;
            if let Err(e) = r {
                tracing::warn!(shard, "large-repo index scan failed: {e:#}");
            }
        }
        if dids.is_empty() {
            return;
        }
        let n = dids.len();
        let loaded = futures::stream::iter(dids)
            .map(|did| {
                let senders = senders.clone();
                async move {
                    let tx = {
                        let senders = senders.upgrade()?;
                        let (tx, rx) = oneshot::channel();
                        let w = Workers { senders };
                        w.route(&did).send(WorkerMsg::Preload { did, done: tx }).ok()?;
                        rx
                    };
                    tx.await.ok()
                }
            })
            .buffer_unordered(PRELOAD_CONCURRENCY)
            .filter(|r| std::future::ready(*r == Some(true)))
            .count()
            .await;
        tracing::info!(repos = n, loaded, elapsed_ms = t.elapsed().as_millis() as u64, "large repos preloaded");
    });
}

pub fn sign_commit(did: &str, rev: &str, data: &Cid, key: &Keypair) -> (Cid, Bytes) {
    let unsigned = events::encode_commit(did, rev, data, None);
    let sig = key.sign(&unsigned);
    let signed = events::encode_commit(did, rev, data, Some(&sig));
    (Cid::dag_cbor(&signed), Bytes::from(signed))
}

pub async fn load_repo(
    partition: Arc<Partition>,
    did: Arc<str>,
) -> anyhow::Result<Option<RepoState>> {
    let db = &partition.db;
    let Some(hv) = db.get(state::head_key(&did)).await? else {
        return Ok(None);
    };
    let head = Head::decode(&hv)?;
    let av = db
        .get(state::account_key(&did))
        .await?
        .ok_or_else(|| anyhow::anyhow!("head without account"))?;
    let acct: state::Account = serde_json::from_slice(&av)?;
    let key = Keypair::from_bytes(&hex::decode(&acct.signing_key)?)?;
    let tree = load_tree(db, &did).await?;
    let mut collections: HashMap<String, u32> = HashMap::new();
    tree.walk(&mut |k, _| {
        if let Ok(path) = std::str::from_utf8(k) {
            *collections
                .entry(collection_of(path).to_string())
                .or_insert(0) += 1;
        }
    });
    let blob_refs = load_blob_refs(db, &did).await?;
    let mut st = finish_load(partition.clone(), did, tree, head, key, acct)?;
    st.collections = collections;
    st.blob_refs = blob_refs;
    Ok(Some(st))
}

async fn load_blob_refs(db: &slatedb::Db, did: &str) -> anyhow::Result<HashMap<String, Vec<Cid>>> {
    let prefix = state::blob_ref_prefix(did);
    let mut iter = db.scan(prefix.clone()..state::prefix_end(&prefix)).await?;
    let mut out: HashMap<String, Vec<Cid>> = HashMap::new();
    while let Some(kv) = iter.next().await? {
        let rest = std::str::from_utf8(&kv.key[prefix.len()..])?;
        let (cid, path) = rest
            .split_once('\0')
            .ok_or_else(|| anyhow::anyhow!("bad blob ref key"))?;
        out.entry(path.to_string())
            .or_default()
            .push(Cid::parse(cid)?);
    }
    Ok(out)
}

pub fn collection_of(path: &str) -> &str {
    path.split_once('/').map(|(c, _)| c).unwrap_or(path)
}

/// Bounds concurrent cold loads so a restart/takeover doesn't stampede the
/// object store with every repo's scan at once.
static LOAD_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(256);

pub async fn load_tree(db: &slatedb::Db, did: &str) -> anyhow::Result<Tree> {
    let prefix = state::record_prefix(did);
    let end = state::prefix_end(&prefix);
    let _permit = LOAD_PERMITS.acquire().await?;
    // A repo's records are contiguous: read ahead in large ranges rather than
    // one block per GET (the default).
    let opts = slatedb::config::ScanOptions {
        read_ahead_bytes: 1 << 20,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    };
    let mut iter = db.scan_with_options(prefix.clone()..end, &opts).await?;
    let mut tree = Tree::new();
    let mut n = 0u64;
    while let Some(kv) = iter.next().await? {
        let (cid, _) = state::decode_record_value(&kv.value)?;
        tree.insert_no_proof(&kv.key[prefix.len()..], cid)?;
        n += 1;
    }
    metrics::REPO_LOAD_RECORDS.observe(n as f64);
    Ok(tree)
}

fn finish_load(
    partition: Arc<Partition>,
    did: Arc<str>,
    mut tree: Tree,
    head: Head,
    key: Keypair,
    account: state::Account,
) -> anyhow::Result<RepoState> {
    let root = tree.root_cid()?;
    anyhow::ensure!(
        root == head.data,
        "rebuilt MST root {root} != head data {}",
        head.data
    );
    let nodes = crate::mst::SharedNodeIndex::default();
    let view = new_view(&head, &tree, &nodes);
    Ok(RepoState {
        did,
        partition,
        tree,
        head,
        key: Arc::new(key),
        pending: Arc::new(AtomicU32::new(0)),
        account,
        collections: HashMap::new(),
        blob_refs: HashMap::new(),
        view,
        nodes,
        large: false,
        charge: 0,
    })
}

/// Size label for the cold-load histogram.
fn size_bucket(records: u64) -> &'static str {
    match records {
        0..1_000 => "<1k",
        1_000..10_000 => "1k-10k",
        10_000..100_000 => "10k-100k",
        100_000..1_000_000 => "100k-1M",
        _ => ">=1M",
    }
}

/// Net change per path within one commit: (value before the commit, value after).
struct Batch {
    ops: BTreeMap<String, (Option<Cid>, Option<Cid>)>,
    records: HashMap<Cid, Bytes>,
    record_bytes: usize,
    /// Blob refs of each path's latest value in this batch.
    blobs: HashMap<String, Vec<Cid>>,
    waiters: Vec<(WriteReply, Vec<WriteOutcome>)>,
}

impl Batch {
    fn new() -> Batch {
        Batch {
            ops: BTreeMap::new(),
            records: HashMap::new(),
            record_bytes: 0,
            blobs: HashMap::new(),
            waiters: Vec::new(),
        }
    }
    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

fn valid_path_part(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 512
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_:~".contains(&b))
}

/// Coalesces a repo's queued write requests into as few commits as possible.
fn process(st: &mut RepoState, reqs: Vec<Queued>, clock_id: u64) -> anyhow::Result<()> {
    let mut batch = Batch::new();
    for q in reqs {
        let req = match q {
            Queued::Write(r) => r,
            Queued::Snapshot(r) => {
                let _ = r.reply.send(Ok(st.view.clone()));
                continue;
            }
            Queued::Account(a) => {
                if !batch.is_empty() {
                    flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id)?;
                }
                apply_account(st, a, clock_id)?;
                continue;
            }
        };
        if let Some(status) = &st.account.status {
            let _ = req
                .reply
                .send(Err(WriteError::RepoInactive(status.clone())));
            continue;
        }
        if req.writes.len() > MAX_COMMIT_OPS {
            let _ = req.reply.send(Err(WriteError::Invalid(format!(
                "too many writes (max {MAX_COMMIT_OPS})"
            ))));
            continue;
        }
        if let Some(sc) = req.swap_commit {
            // swapCommit must be evaluated against a real commit boundary
            if !batch.is_empty() {
                flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id)?;
            }
            if sc != st.head.commit {
                let _ = req.reply.send(Err(WriteError::InvalidSwap(format!(
                    "commit was at {}",
                    st.head.commit
                ))));
                continue;
            }
        }
        let new_paths: HashSet<String> = req
            .writes
            .iter()
            .map(|w| w.path())
            .filter(|p| !batch.ops.contains_key(p))
            .collect();
        let incoming_bytes: usize = req
            .writes
            .iter()
            .map(|w| match w {
                Write::Create { bytes, .. } | Write::Update { bytes, .. } => bytes.len(),
                _ => 0,
            })
            .sum();
        if !batch.is_empty()
            && (batch.ops.len() + new_paths.len() > MAX_COMMIT_OPS
                || batch.record_bytes + incoming_bytes > MAX_COMMIT_RECORD_BYTES)
        {
            flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id)?;
        }
        match validate(st, &req.writes) {
            Ok(()) => {}
            Err(e) => {
                let _ = req.reply.send(Err(e));
                continue;
            }
        }
        let mut outcomes = Vec::with_capacity(req.writes.len());
        for w in req.writes {
            let path = w.path();
            match w {
                Write::Create {
                    cid, bytes, blobs, ..
                }
                | Write::Update {
                    cid, bytes, blobs, ..
                } => {
                    let prev = st.tree.insert(path.as_bytes(), cid)?;
                    batch.ops.entry(path.clone()).or_insert((prev, None)).1 = Some(cid);
                    batch.blobs.insert(path.clone(), blobs);
                    batch.record_bytes += bytes.len();
                    batch.records.insert(cid, bytes);
                    outcomes.push(if prev.is_some() {
                        WriteOutcome::Update { path, cid }
                    } else {
                        WriteOutcome::Create { path, cid }
                    });
                }
                Write::Delete { .. } => {
                    let prev = st.tree.remove(path.as_bytes())?;
                    if prev.is_some() {
                        batch.blobs.remove(&path);
                        batch.ops.entry(path).or_insert((prev, None)).1 = None;
                    }
                    outcomes.push(WriteOutcome::Delete);
                }
            }
        }
        batch.waiters.push((req.reply, outcomes));
    }
    if !batch.is_empty() {
        flush(st, batch, clock_id)?;
    }
    Ok(())
}

/// Checks a request against the current tree (including earlier writes in
/// the batch) without mutating anything, so applyWrites stays atomic.
fn validate(st: &RepoState, writes: &[Write]) -> Result<(), WriteError> {
    let mut overlay: HashMap<String, Option<Cid>> = HashMap::new();
    for w in writes {
        let (coll, rkey) = match w {
            Write::Create {
                collection, rkey, ..
            }
            | Write::Update {
                collection, rkey, ..
            }
            | Write::Delete {
                collection, rkey, ..
            } => (collection, rkey),
        };
        if !valid_path_part(coll) || !coll.contains('.') || !valid_path_part(rkey) {
            return Err(WriteError::Invalid(format!(
                "invalid record path {coll}/{rkey}"
            )));
        }
        let path = w.path();
        let cur = match overlay.get(&path) {
            Some(v) => *v,
            None => st
                .tree
                .get(path.as_bytes())
                .map_err(|e| WriteError::Internal(e.to_string()))?,
        };
        let check_swap = |swap: &Option<Option<Cid>>| -> Result<(), WriteError> {
            match swap {
                Some(expected) if *expected != cur => Err(WriteError::InvalidSwap(format!(
                    "record was at {}",
                    cur.map(|c| c.to_string()).unwrap_or_else(|| "null".into())
                ))),
                _ => Ok(()),
            }
        };
        let new = match w {
            Write::Create { cid, .. } => {
                if cur.is_some() {
                    return Err(WriteError::Invalid(format!(
                        "record already exists: {path}"
                    )));
                }
                Some(*cid)
            }
            Write::Update {
                cid,
                swap,
                must_exist,
                ..
            } => {
                check_swap(swap)?;
                if *must_exist && cur.is_none() {
                    return Err(WriteError::Invalid(format!(
                        "Could not find a record with key: {path}"
                    )));
                }
                Some(*cid)
            }
            Write::Delete { swap, .. } => {
                check_swap(swap)?;
                None
            }
        };
        overlay.insert(path, new);
    }
    Ok(())
}

/// Builds, signs and enqueues one commit for the batch.
fn flush(st: &mut RepoState, batch: Batch, clock_id: u64) -> anyhow::Result<()> {
    let build_start = Instant::now();
    let mut mst_blocks = Vec::with_capacity(16);
    let prev_data = st.head.data;
    // once getBlocks has asked for node blocks, report where written nodes sit
    let mut node_refs = st.nodes.lock().wanted.then(Vec::new);
    let data = match &mut node_refs {
        Some(refs) => st.tree.write_diff_blocks_with_refs(&mut mst_blocks, refs)?,
        None => st.tree.write_diff_blocks(&mut mst_blocks)?,
    };
    // A batch that nets to no change (e.g. deleting a missing record) leaves no
    // dirty nodes; the commit's CAR must still carry the root node so it can be
    // loaded and verified.
    if !mst_blocks.iter().any(|(c, _)| *c == data) {
        if let Some(root) = st.tree.proof_blocks(b"_")?.into_iter().next() {
            mst_blocks.push(root);
        }
    }
    let rev = tid::next_rev(Some(st.head.rev), clock_id);
    let rev_s = rev.to_string();
    let since_s = st.head.rev.to_string();
    let (commit, commit_block) = sign_commit(&st.did, &rev_s, &data, &st.key);

    let mut ops = Vec::with_capacity(batch.ops.len());
    let mut muts = Vec::with_capacity(batch.ops.len() + 1);
    let mut car_bytes =
        Vec::with_capacity(commit_block.len() + mst_blocks.len() * 300 + batch.record_bytes + 128);
    car::write_header(&mut car_bytes, &commit);
    car::write_block(&mut car_bytes, &commit, &commit_block);
    for (c, b) in &mst_blocks {
        car::write_block(&mut car_bytes, c, b);
    }
    // `muts` gets what replay can rebuild from the #commit frame (record CID
    // index keys, records, head: segment::derive_commit_muts), `extra` the
    // rest (collection index, blob refs); the segment stores only `extra`
    let mut extra = Vec::new();
    let mut written: HashSet<Cid> = HashSet::new();
    for (path, (prev, new)) in &batch.ops {
        if prev == new {
            continue; // net no-op (e.g. created then deleted, or identical update)
        }
        let action = match (prev, new) {
            (None, Some(_)) => "create",
            (Some(_), Some(_)) => "update",
            _ => "delete",
        };
        ops.push(RepoOp {
            action,
            path,
            cid: *new,
            prev: *prev,
        });
        index_mutations(st, rev.0, path,
            prev.is_some(),
            new.is_some(),
            batch.blobs.get(path.as_str()),
            &mut extra,
        );
        let key = Bytes::from(state::record_key(&st.did, path));
        if let Some(p) = prev {
            muts.push(Mutation { key: state::record_cid_key(&st.did, p, path).into(), val: None });
        }
        if let Some(c) = new {
            muts.push(Mutation { key: state::record_cid_key(&st.did, c, path).into(), val: Some(Bytes::new()) });
        }
        match new {
            Some(c) => {
                let bytes = &batch.records[c];
                if written.insert(*c) {
                    car::write_block(&mut car_bytes, c, bytes);
                }
                muts.push(Mutation {
                    key,
                    val: Some(state::record_value(c, rev.0, bytes)),
                });
            }
            None => muts.push(Mutation { key, val: None }),
        }
    }
    let head = Head {
        commit,
        data,
        rev,
        commit_block,
    };
    muts.push(Mutation {
        key: state::head_key(&st.did).into(),
        val: Some(head.encode()),
    });
    let derived = muts.len();
    muts.append(&mut extra);

    let time = events::now_rfc3339();
    let mut frame = events::commit_frame(&events::CommitFrame {
        repo: &st.did,
        rev: &rev_s,
        since: Some(&since_s),
        commit,
        prev_data: Some(prev_data),
        blocks: &car_bytes,
        ops: &ops,
        time: &time,
    });
    frame.derived_muts = derived;
    #[cfg(debug_assertions)]
    {
        let mut f = Vec::new();
        frame.finish(0, &mut f);
        let d = crate::segment::derive_commit_muts(&f).expect("derive commit muts");
        assert!(
            d.len() == derived && d.iter().zip(&muts).all(|(a, b)| a.key == b.key && a.val == b.val),
            "muts derived from the #commit frame differ from the commit's"
        );
    }
    STATS.commits.fetch_add(1, Ordering::Relaxed);
    STATS.ops.fetch_add(ops.len() as u64, Ordering::Relaxed);
    metrics::COMMITS.inc();
    for op in &ops {
        metrics::OPS.with_label_values(&[op.action]).inc();
    }
    metrics::COMMIT_OPS.observe(ops.len() as f64);
    metrics::COMMIT_REQUESTS.observe(batch.waiters.len() as f64);
    metrics::COMMIT_BLOCKS_BYTES.observe(car_bytes.len() as f64);
    metrics::COMMIT_BUILD.observe(build_start.elapsed().as_secs_f64());
    if let Some(refs) = node_refs {
        st.nodes.lock().commit(st.head.rev.0, head.rev.0, refs);
    }
    st.head = head;
    st.pending.fetch_add(1, Ordering::AcqRel);

    let waiters = batch.waiters;
    let (view, snap) = (st.view.clone(), st.durable_view());
    let entry = LogEntry {
        shard: st.partition.id,
        frames: vec![frame],
        muts,
        ack: Some(Box::new(move |r| {
            if r.is_ok() {
                *view.write() = snap;
            }
            for (reply, results) in waiters {
                let _ = reply.send(match &r {
                    Ok(()) => Ok(CommitAck {
                        commit,
                        rev,
                        results,
                    }),
                    Err(e) => Err(WriteError::Internal(e.to_string())),
                });
            }
        })),
        pending: Some(st.pending.clone()),
        enqueued: Instant::now(),
    };
    enqueue(st, entry)
}

/// Hands a commit's log entry to the node log.
fn enqueue(st: &RepoState, entry: LogEntry) -> anyhow::Result<()> {
    st.partition.tx.blocking_send(entry).map_err(|e| {
        // never logged: it must not hold the repo out of reloads (see `draining`)
        if let Some(p) = &e.0.pending {
            p.fetch_sub(1, Ordering::AcqRel);
        }
        anyhow::anyhow!("partition sequencer gone")
    })
}

/// Maintains the collection index and blob-ref index for one net op.
fn index_mutations(
    st: &mut RepoState,
    rev: u64,
    path: &str,
    existed: bool,
    exists: bool,
    new_blobs: Option<&Vec<Cid>>,
    muts: &mut Vec<Mutation>,
) {
    let coll = collection_of(path);
    if !existed && exists {
        let n = st.collections.entry(coll.to_string()).or_insert(0);
        *n += 1;
        if *n == 1 {
            muts.push(Mutation {
                key: state::collection_key(coll, &st.did).into(),
                val: Some(Bytes::new()),
            });
        }
    } else if existed && !exists {
        if let Some(n) = st.collections.get_mut(coll) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                st.collections.remove(coll);
                muts.push(Mutation {
                    key: state::collection_key(coll, &st.did).into(),
                    val: None,
                });
            }
        }
    }
    let old = st.blob_refs.remove(path).unwrap_or_default();
    let new: Vec<Cid> = if exists {
        new_blobs.cloned().unwrap_or_default()
    } else {
        Vec::new()
    };
    for b in old.iter().filter(|b| !new.contains(b)) {
        muts.push(Mutation {
            key: state::blob_ref_key(&st.did, b, path).into(),
            val: None,
        });
    }
    // (re)write every current ref with the referencing record's rev, so
    // listBlobs `since` sees blobs kept across an update too
    for b in new.iter() {
        muts.push(Mutation {
            key: state::blob_ref_key(&st.did, b, path).into(),
            val: Some(Bytes::copy_from_slice(&rev.to_be_bytes())),
        });
    }
    if !new.is_empty() {
        st.blob_refs.insert(path.to_string(), new);
    }
}

fn send_entry(st: &RepoState, entry: LogEntry) -> anyhow::Result<()> {
    st.pending.fetch_add(1, Ordering::AcqRel);
    enqueue(st, entry)
}

fn head_ack(
    reply: oneshot::Sender<Result<Head, WriteError>>,
    head: Head,
) -> crate::partition::AckFn {
    Box::new(move |r| {
        let _ = reply.send(
            r.map(|_| head)
                .map_err(|e| WriteError::Internal(e.to_string())),
        );
    })
}

/// Mutations deleting every record (and its index entries) currently in the repo.
fn clear_repo_mutations(st: &mut RepoState, muts: &mut Vec<Mutation>) {
    let did = st.did.clone();
    st.tree.walk(&mut |k, cid| {
        if let Ok(path) = std::str::from_utf8(k) {
            muts.push(Mutation {
                key: state::record_key(&did, path).into(),
                val: None,
            });
            muts.push(Mutation {
                key: state::record_cid_key(&did, &cid, path).into(),
                val: None,
            });
        }
    });
    for coll in st.collections.keys() {
        muts.push(Mutation {
            key: state::collection_key(coll, &did).into(),
            val: None,
        });
    }
    for (path, blobs) in &st.blob_refs {
        for b in blobs {
            muts.push(Mutation {
                key: state::blob_ref_key(&did, b, path).into(),
                val: None,
            });
        }
    }
    st.collections.clear();
    st.blob_refs.clear();
}

/// Runs `mutate` on a copy of the current account: Some(next) = persist it,
/// None = a no-op (the mutation said nothing changed).
fn mutate_account(st: &RepoState, mutate: AccountMutation) -> Result<Option<state::Account>, WriteError> {
    if st.account.status.as_deref() == Some("deleted") {
        return Err(WriteError::RepoNotFound);
    }
    let mut next = st.account.clone();
    Ok(mutate(&mut next)?.then_some(next))
}

fn apply_account(st: &mut RepoState, req: AccountReq, clock_id: u64) -> anyhow::Result<()> {
    let time = events::now_rfc3339();
    let mut frames = Vec::new();
    let mut muts = Vec::new();
    match req.op {
        AccountOp::Update {
            mutate,
            identity_event,
            account_event,
        } => {
            let account = match mutate_account(st, mutate) {
                Ok(Some(a)) => a,
                // rejected, or nothing to write: ack now (a no-op logs nothing)
                r => {
                    let _ = req.reply.send(r.map(|_| st.head.clone()));
                    return Ok(());
                }
            };
            if account.handle != st.account.handle {
                muts.push(Mutation {
                    key: state::handle_key(&st.did, &st.account.handle).into(),
                    val: None,
                });
                muts.push(Mutation {
                    key: state::handle_key(&st.did, &account.handle).into(),
                    val: Some(Bytes::from(st.did.to_string())),
                });
            }
            muts.push(Mutation {
                key: state::account_key(&st.did).into(),
                val: Some(Bytes::from(serde_json::to_vec(&account)?)),
            });
            if identity_event {
                frames.push(events::identity_frame(&st.did, &account.handle, &time));
            }
            if account_event {
                frames.push(events::account_frame(
                    &st.did,
                    account.status.is_none(),
                    account.status.as_deref(),
                    &time,
                ));
            }
            // signing key rotation (admin.updateAccountSigningKey): sign later commits with the new key
            if account.signing_key != st.account.signing_key {
                st.key = Arc::new(Keypair::from_bytes(&hex::decode(&account.signing_key)?)?);
            }
            st.account = account;
        }
        AccountOp::Activate { mutate } => {
            let account = match mutate_account(st, mutate) {
                Ok(Some(a)) => a,
                // rejected, or nothing to write: ack now (a no-op logs nothing)
                r => {
                    let _ = req.reply.send(r.map(|_| st.head.clone()));
                    return Ok(());
                }
            };
            muts.push(Mutation {
                key: state::account_key(&st.did).into(),
                val: Some(Bytes::from(serde_json::to_vec(&account)?)),
            });
            frames.push(events::account_frame(
                &st.did,
                account.status.is_none(),
                account.status.as_deref(),
                &time,
            ));
            frames.push(events::identity_frame(&st.did, &account.handle, &time));
            let mut car_bytes = Vec::with_capacity(st.head.commit_block.len() + 64);
            car::write_header(&mut car_bytes, &st.head.commit);
            car::write_block(&mut car_bytes, &st.head.commit, &st.head.commit_block);
            frames.push(events::sync_frame(
                &st.did,
                &st.head.rev.to_string(),
                &car_bytes,
                &time,
            ));
            st.account = account;
        }
        AccountOp::ReplaceRepo { records } => {
            // deactivated accounts may import (the migration flow); others may not
            if let Some(status) = st.account.status.as_ref().filter(|s| *s != "deactivated") {
                let _ = req
                    .reply
                    .send(Err(WriteError::RepoInactive(status.clone())));
                return Ok(());
            }
            clear_repo_mutations(st, &mut muts);
            let rev = tid::next_rev(Some(st.head.rev), clock_id);
            let mut tree = Tree::new();
            for (path, cid, bytes, blobs) in &records {
                tree.insert_no_proof(path.as_bytes(), *cid)?;
                muts.push(Mutation {
                    key: state::record_key(&st.did, path).into(),
                    val: Some(state::record_value(cid, rev.0, bytes)),
                });
                muts.push(Mutation {
                    key: state::record_cid_key(&st.did, cid, path).into(),
                    val: Some(Bytes::new()),
                });
                index_mutations(st, rev.0, path, false, true, Some(blobs), &mut muts);
            }
            let data = tree.root_cid()?;
            let (commit, commit_block) = sign_commit(&st.did, &rev.to_string(), &data, &st.key);
            // a deactivated account (mid-migration) is announced with #sync
            // when activated (reference importRepo sequences nothing)
            if st.account.status.is_none() {
                let mut car_bytes = Vec::with_capacity(commit_block.len() + 64);
                car::write_header(&mut car_bytes, &commit);
                car::write_block(&mut car_bytes, &commit, &commit_block);
                frames.push(events::sync_frame(
                    &st.did,
                    &rev.to_string(),
                    &car_bytes,
                    &time,
                ));
            }
            st.tree = tree;
            st.head = Head {
                commit,
                data,
                rev,
                commit_block,
            };
            muts.push(Mutation {
                key: state::head_key(&st.did).into(),
                val: Some(st.head.encode()),
            });
        }
        AccountOp::Delete => {
            clear_repo_mutations(st, &mut muts);
            st.tree = Tree::new();
            muts.push(Mutation {
                key: state::head_key(&st.did).into(),
                val: None,
            });
            muts.push(Mutation {
                key: state::account_key(&st.did).into(),
                val: None,
            });
            muts.push(Mutation {
                key: state::handle_key(&st.did, &st.account.handle).into(),
                val: None,
            });
            frames.push(events::account_frame(
                &st.did,
                false,
                Some("deleted"),
                &time,
            ));
            st.account.status = Some("deleted".into());
        }
    }
    let entry = LogEntry {
        shard: st.partition.id,
        frames,
        muts,
        ack: Some({
            let inner = head_ack(req.reply, st.head.clone());
            let (view, snap) = (st.view.clone(), st.durable_view());
            let did = st.did.clone();
            Box::new(move |r| {
                if r.is_ok() {
                    *view.write() = snap;
                }
                // applied (acks follow the state apply): drop cached copies
                // of the account (status, signing key)
                crate::xrpc::proxy::account_changed(&did);
                inner(r)
            })
        }),
        pending: Some(st.pending.clone()),
        enqueued: Instant::now(),
    };
    send_entry(st, entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodelog::{NodeLog, NodeLogConfig};

    fn settle(e: LogEntry) {
        if let Some(p) = e.pending {
            p.fetch_sub(1, Ordering::AcqRel);
        }
        if let Some(ack) = e.ack {
            ack(Ok(()));
        }
    }

    fn write(did: &Arc<str>, rkey: &str) -> (WorkerMsg, oneshot::Receiver<Result<CommitAck, WriteError>>) {
        let bytes = Bytes::from(format!("record {rkey}"));
        let (reply, rx) = oneshot::channel();
        let w = Write::Create { collection: "app.test.thing".into(), rkey: rkey.into(), cid: Cid::dag_cbor(&bytes), bytes, blobs: Vec::new() };
        (WorkerMsg::Write(WriteReq { did: did.clone(), writes: vec![w], swap_commit: None, reply }), rx)
    }

    /// A commit that fails while an earlier one is still in flight must not
    /// reload the repo before that one is durable: the reload would build on
    /// a durable head without it (a fork).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_commit_waits_for_inflight_before_reload() {
        let store = crate::store::Store::memory(None);
        let db = Arc::new(crate::partition::open_db(&store, 0, None).await.unwrap());
        let (merger_tx, _merger_rx) = tokio::sync::mpsc::unbounded_channel();
        let log = NodeLog::start(
            store.clone(),
            NodeLogConfig { log_id: "t".into(), writer: 1, max_segment_bytes: 1 << 20, hedge_after: Duration::from_secs(1), lease_ok: None },
            merger_tx,
        );
        // the "sequencer" is this test: it decides when entries become durable
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LogEntry>(16);
        let part = Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone() });
        let p2 = part.clone();
        let workers = spawn(1, 100, Arc::new(move |_: &str| Some(p2.clone())), tokio::runtime::Handle::current());
        drop(part);
        let w = workers.senders[0].clone();
        let did: Arc<str> = "did:plc:test".into();
        let key = Arc::new(Keypair::generate());
        let account = serde_json::json!({
            "did": &*did, "handle": "t.test", "signing_key": hex::encode(key.to_bytes()),
            "password_hash": "", "created_at": "2026-01-01T00:00:00Z",
        });
        let (reply, created) = oneshot::channel();
        w.send(WorkerMsg::CreateRepo(CreateRepoReq {
            did: did.clone(),
            handle: "t.test".into(),
            key,
            account_json: Bytes::from(serde_json::to_vec(&account).unwrap()),
            records: Vec::new(),
            reply,
        }))
        .unwrap();
        settle(rx.recv().await.unwrap());
        created.await.unwrap().unwrap();
        // commit 1: in flight, not durable yet
        let (m, first) = write(&did, "a");
        w.send(m).unwrap();
        let inflight = rx.recv().await.unwrap();
        // commit 2 fails (the log intake is gone)
        drop(rx);
        let (m, second) = write(&did, "b");
        w.send(m).unwrap();
        assert!(second.await.is_err());
        // a later write must wait for commit 1 instead of reloading now
        let (m, mut third) = write(&did, "c");
        w.send(m).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(third.try_recv().is_err(), "repo reloaded with a commit in flight");
        settle(inflight);
        first.await.unwrap().unwrap();
        // now it reloads (from a state that never got the commits applied here)
        let r = tokio::time::timeout(Duration::from_secs(5), third).await.unwrap().unwrap();
        assert!(r.is_err());
    }

    /// The cache evicts by approximate bytes as well as count; a repo past
    /// the pin threshold is never evicted and gets an `L/` index entry, and
    /// below half the threshold it is unpinned (entry deleted) and evictable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_pins_large_repos_and_evicts_by_bytes() {
        let store = crate::store::Store::memory(None);
        let db = Arc::new(crate::partition::open_db(&store, 0, None).await.unwrap());
        let (merger_tx, _merger_rx) = tokio::sync::mpsc::unbounded_channel();
        let log = NodeLog::start(
            store.clone(),
            NodeLogConfig { log_id: "t".into(), writer: 1, max_segment_bytes: 1 << 20, hedge_after: Duration::from_secs(1), lease_ok: None },
            merger_tx,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LogEntry>(16);
        let part = Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone() });
        let repo = |name: &str, records: u32| {
            let did: Arc<str> = format!("did:plc:{name}").into();
            let key = Keypair::generate();
            let mut tree = Tree::new();
            for i in 0..records {
                tree.insert_no_proof(format!("c.x/{i:04}").as_bytes(), Cid::dag_cbor(&i.to_be_bytes())).unwrap();
            }
            let root = tree.root_cid().unwrap();
            let head = Head { commit: root, data: root, rev: Tid(1), commit_block: Bytes::new() };
            let acct: state::Account = serde_json::from_value(serde_json::json!({
                "did": &*did, "handle": "t.test", "signing_key": hex::encode(key.to_bytes()), "password_hash": "", "created_at": "",
            }))
            .unwrap();
            let mut st = finish_load(part.clone(), did.clone(), tree, head, key, acct).unwrap();
            if records > 0 {
                st.collections.insert("c.x".into(), records);
            }
            (did, st)
        };
        let small_bytes = REPO_BASE_BYTES + 10 * MST_BYTES_PER_RECORD;
        let limits = CacheLimits { entries: 100, bytes: 3 * small_bytes, pin_records: 50 };
        let (me, _rx) = crossbeam_channel::unbounded();
        let mut w = Worker::new(0, me, Arc::new(|_: &str| None), tokio::runtime::Handle::current(), limits);
        let big = repo("big", 60);
        let smalls: Vec<_> = (0..5).map(|i| repo(&format!("small{i}"), 10)).collect();
        let big_did = big.0.clone();
        let w = tokio::task::spawn_blocking(move || {
            w.cache_put(big.0.clone(), big.1);
            w.settle(&big.0);
            for (did, st) in smalls {
                w.cache_put(did.clone(), st);
                w.settle(&did);
                w.evict();
            }
            w
        })
        .await
        .unwrap();
        let entry = rx.recv().await.unwrap();
        assert_eq!(entry.muts[0].key, state::large_repo_key(&big_did));
        assert_eq!(entry.muts[0].val.as_deref(), Some(&60u64.to_be_bytes()[..]));
        settle(entry);
        let cached = |w: &Worker| w.cache.iter().map(|(d, _)| d.to_string()).collect::<std::collections::BTreeSet<_>>();
        assert!(w.cache.contains(&big_did), "the large repo stays");
        assert_eq!((w.pinned, w.cache.len()), (1, 4), "{:?}", cached(&w));
        assert!(!w.cache.contains("did:plc:small0") && !w.cache.contains("did:plc:small1"), "oldest small repos evicted");
        assert_eq!(w.bytes - w.pinned_bytes, 3 * small_bytes);
        assert_eq!(w.pinned_bytes, REPO_BASE_BYTES + 60 * MST_BYTES_PER_RECORD);
        // shrinks to 30 records: still pinned (above half); to 20: unpinned
        let w = tokio::task::spawn_blocking(move || {
            let mut w = w;
            for n in [30, 20] {
                w.cache.peek_mut(&big_did).unwrap().collections.insert("c.x".into(), n);
                w.settle(&big_did);
                assert_eq!(w.pinned, (n == 30) as usize);
            }
            (w, big_did)
        })
        .await
        .unwrap();
        let (mut w, big_did) = w;
        let entry = rx.recv().await.unwrap();
        assert!(entry.muts[0].key == state::large_repo_key(&big_did) && entry.muts[0].val.is_none());
        settle(entry);
        w.evict();
        assert!(!w.cache.contains(&big_did), "unpinned, the least recently used goes first: {:?}", cached(&w));
        assert_eq!(w.bytes, 3 * small_bytes);
    }

    /// Dropping the last `Workers` handle ends the threads (each holds a
    /// sender to its own channel, so disconnection alone never would).
    #[tokio::test]
    async fn workers_exit_when_dropped() {
        let workers = spawn(2, 10, Arc::new(|_: &str| None), tokio::runtime::Handle::current());
        let probes: Vec<_> = workers.senders.iter().cloned().collect();
        drop(workers);
        let deadline = Instant::now() + Duration::from_secs(5);
        // a worker's receiver is dropped when its thread returns
        while probes.iter().any(|p| p.send(WorkerMsg::Shutdown).is_ok()) {
            assert!(Instant::now() < deadline, "repo workers still running");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
