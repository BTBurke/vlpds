//! CPU workers. Each repo is owned by one worker thread (hash of DID), which
//! keeps the loaded paths of its MST in memory (DESIGN.md "Partial MSTs"),
//! coalesces queued writes into commits, signs them
//! and hands them to the repo's partition sequencer without waiting for
//! durability (acks are released by the partition in log order).

use crate::car;
use crate::cid::Cid;
use crate::crypto::Keypair;
use crate::events::{self, RepoOp};
use crate::metrics;
use crate::mst::Tree;
use crate::mst_lazy::{LazyTree, Source};
use crate::mst_store::{DbSource, ScanSource};
use crate::partition::{LogEntry, Partition};
use crate::secrets::Secrets;
use crate::segment::Mutation;
use crate::state::{self, Head};
use crate::stats::STATS;
use crate::tid::{self, Tid};
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use prometheus::IntCounter;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Spec limits for a single commit.
pub const MAX_COMMIT_OPS: usize = 200;
pub const MAX_COMMIT_RECORD_BYTES: usize = 1_000_000;
/// [`WriteError::Invalid`] message of a CreateRepo for a repo the worker holds.
pub const REPO_EXISTS: &str = "repo already exists";

#[derive(Debug, Clone)]
pub enum WriteError {
    RepoNotFound,
    /// Account is deactivated / taken down / deleted (the status string).
    RepoInactive(String),
    InvalidSwap(String),
    Invalid(String),
    Internal(String),
    /// The repo's shard isn't served by this node right now (moving between
    /// owners). Only raised before the request started, so it was never
    /// applied: 503 `ShardMoved`, which the entry node resends.
    Unavailable(String),
    /// The repo's signing key couldn't be unwrapped (key service down; see
    /// src/secrets.rs). Nothing was applied: 503 `KeyUnavailable`.
    KeyUnavailable(String),
    /// The commit signature failed verification twice (src/crypto.rs:
    /// suspected memory/CPU fault). Nothing was applied or emitted: 503
    /// `SignatureFault`.
    SignatureFault(String),
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
    /// Set for a forwarded write the owner may give up on before it starts
    /// (see [`Claim`]); None = always applied once queued.
    pub claim: Option<Arc<Claim>>,
}

/// Who decides a queued write's fate: its worker taking it into a commit,
/// or its handler abandoning it (a forwarded write whose repo is still
/// loading: the owner answers 503 `RepoLoading` and the forwarding node
/// retries). Exactly one wins, so an abandoned write is never applied and a
/// taken one is always answered.
#[derive(Default)]
pub struct Claim(std::sync::atomic::AtomicU8);

impl Claim {
    const PENDING: u8 = 0;
    const TAKEN: u8 = 1;
    const ABANDONED: u8 = 2;

    /// The worker starts the write: false if it was abandoned.
    pub fn take(&self) -> bool {
        self.0.compare_exchange(Self::PENDING, Self::TAKEN, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    /// The handler gives up: false if the worker took it already.
    pub fn abandon(&self) -> bool {
        self.0.compare_exchange(Self::PENDING, Self::ABANDONED, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }
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
    /// Load a recently written repo ahead of its first request (after a
    /// shard open); `done` gets whether a load was started and cached it.
    Preload { did: Arc<str>, done: oneshot::Sender<bool> },
    /// What the worker holds for a repo (None = not cached).
    CacheInfo { did: Arc<str>, reply: oneshot::Sender<Option<CachedRepo>> },
    /// The paths a repo's queued requests visit, loaded on the
    /// blocking pool (see `Worker::start_fetch`): the tree to continue
    /// with, or why it failed.
    Fetched { did: Arc<str>, res: Result<Box<LazyTree>, crate::mst::MstError> },
}

/// A cached repo, as [`WorkerMsg::CacheInfo`] reports it.
#[derive(Clone, Debug)]
pub struct CachedRepo {
    /// MST nodes loaded (the paths operations visited).
    pub loaded_nodes: usize,
    /// Bytes charged to the cache budget.
    pub charge: usize,
}

/// The repo as of its latest *durable* commit: what exports and proofs serve.
/// Published by the commit's ack (after the state apply), so it never shows a
/// commit that could still be lost.
pub struct DurableView {
    pub head: Head,
    /// The tree at `head`, only partly loaded: readers load the rest from a
    /// SlateDB snapshot taken with the view (`M/` and `R/` as of this head,
    /// see `App::repo_view`) into a private copy.
    pub tree: Tree,
    /// The repo's MST node index (getBlocks of leaves), shared with the
    /// worker, which advances it per commit.
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
    /// The repo's MST, loaded along the paths recent operations visited.
    pub mst: LazyTree,
    pub head: Head,
    /// The unwrapped signing key; None if the key service was down at load
    /// (reads are served, writes refused with `KeyUnavailable`).
    pub key: Option<Arc<Keypair>>,
    pub pending: Arc<AtomicU32>,
    pub account: state::Account,
    /// Blob refs per record path (drives the b/{did}\0{blob}\0{path} index).
    pub blob_refs: HashMap<String, Vec<Cid>>,
    pub view: ViewCell,
    pub nodes: crate::mst::SharedNodeIndex,
    /// Approximate heap charged to the worker's cache ([`repo_bytes`]).
    pub charge: usize,
    /// A repo whose paths are being loaded off the worker thread
    /// (its requests wait in `Worker::loading`; see `Worker::start_fetch`).
    pub fetching: bool,
    /// A repo rebuilt from its records on open: its interior nodes
    /// are written to `M/` once it is cached (self-healing).
    pub backfill: bool,
    /// The repo's log entries in flight that wrote MST state, oldest
    /// first: (applied, the nodes the commit wrote; None = the whole tree,
    /// an import, account delete, creation or backfill). Paths of a repo
    /// with commits in flight are unloaded only outside these sets.
    pub inflight: std::collections::VecDeque<(Arc<std::sync::atomic::AtomicBool>, Option<HashSet<Cid>>)>,
}

impl RepoState {
    fn durable_view(&self) -> Arc<DurableView> {
        Arc::new(DurableView { head: self.head.clone(), tree: self.mst.tree.clone(), nodes: self.nodes.clone() })
    }
}

/// Per-repo overhead outside the tree (account, key, head, views, maps).
const REPO_BASE_BYTES: usize = 2048;

/// Approximate heap of a cached repo: what the cache budget counts. A repo
/// is charged its loaded paths (`mst_lazy::heap_bytes`).
pub fn repo_bytes(st: &RepoState) -> usize {
    REPO_BASE_BYTES + st.mst.heap_bytes() + st.blob_refs.len() * 96
}

/// A repo charged more than this is unloaded (back to its root) as soon
/// as nothing of it is in flight, whatever the budget: a fully loaded tree
/// (a new import, a rebuild) mustn't be walked for its charge every commit.
const LAZY_REPO_MAX_BYTES: usize = 1 << 20;

/// Repo cache limits, per worker.
#[derive(Clone, Copy, Debug)]
pub struct CacheLimits {
    /// Repos held (LRU).
    pub entries: usize,
    /// Approximate heap of the repos and their loaded paths
    /// ([`repo_bytes`]): over it, idle repos drop back to their root, then
    /// the least recently used are evicted. 0 = unbounded.
    pub bytes: usize,
    /// Cold open: read up to this much of the repo's `M/` range with one
    /// scan (`--lazy-mst-prefetch-kb`; 0 = point reads only).
    pub prefetch_bytes: usize,
    /// Tests: unload every idle repo's paths after each pass.
    pub unload_idle: bool,
}

/// One read-ahead window: the whole `M/` range of repos up to ~35k records
/// (28 B/record; the average active repo's ~0.5 MB). Larger caps measured
/// no better (a 1M-record repo's 28 MB range costs more round trips than
/// its path's point reads; tests/all/mst_lazy.rs `bench_cold_write`).
pub const DEFAULT_PREFETCH_BYTES: usize = 1 << 20;

impl From<usize> for CacheLimits {
    fn from(entries: usize) -> CacheLimits {
        CacheLimits { entries, bytes: 0, prefetch_bytes: DEFAULT_PREFETCH_BYTES, unload_idle: false }
    }
}

fn new_view(head: &Head, mst: &LazyTree, nodes: &crate::mst::SharedNodeIndex) -> ViewCell {
    Arc::new(parking_lot::RwLock::new(Arc::new(DurableView {
        head: head.clone(),
        tree: mst.tree.clone(),
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
    spawn_with_secrets(n, limits, partitions, rt, Secrets::dev())
}

/// [`spawn`] with the node's keyring (signing keys are unwrapped on load).
pub fn spawn_with_secrets(
    n: usize,
    limits: impl Into<CacheLimits>,
    partitions: PartitionLookup,
    rt: tokio::runtime::Handle,
    secrets: Arc<Secrets>,
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
        let secrets = secrets.clone();
        std::thread::Builder::new()
            .name(format!("repo-worker-{i}"))
            .spawn(move || Worker::new(i, me, partitions, rt, limits, secrets).run(rx))
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
    /// Sum of the cached repos' charges.
    bytes: usize,
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
    /// Repos charged over [`LAZY_REPO_MAX_BYTES`]: unloaded once idle.
    big: HashSet<Arc<str>>,
    /// Unwraps signing keys on cold loads.
    secrets: Arc<Secrets>,
}

impl Worker {
    fn new(
        idx: usize,
        me: Sender<WorkerMsg>,
        partitions: PartitionLookup,
        rt: tokio::runtime::Handle,
        limits: CacheLimits,
        secrets: Arc<Secrets>,
    ) -> Worker {
        Worker {
            secrets,
            label: idx.to_string(),
            me,
            partitions,
            rt,
            cache: lru::LruCache::unbounded(),
            limits,
            bytes: 0,
            loading: HashMap::new(),
            preloads: HashMap::new(),
            draining: HashMap::new(),
            clock_id: rand::random::<u64>() & 0x3ff,
            stop: false,
            big: HashSet::new(),
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
                        CACHE_LOADING.inc();
                        buf.push(q);
                    } else if self.cache.contains(&did) {
                        CACHE_HIT.inc();
                        let g = groups.entry(did.clone()).or_insert_with(|| {
                            order.push(did.clone());
                            Vec::new()
                        });
                        g.push(q);
                    } else {
                        CACHE_MISS.inc();
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
                // the requests may visit paths the repo hasn't loaded:
                // load them on the blocking pool first, not on this thread
                let reqs = match lazy_needs(st, reqs) {
                    Ok(reqs) => reqs,
                    Err((reqs, Some(need))) => {
                        self.start_fetch(did, reqs, need);
                        continue;
                    }
                    Err((reqs, None)) => {
                        tracing::error!(%did, "lazy MST walk failed, evicting repo");
                        self.loading.insert(did.clone(), reqs);
                        self.discard(did.clone());
                        if !self.draining.contains_key(&did) {
                            self.reload_buffered(&did);
                        }
                        continue;
                    }
                };
                let wrote = reqs.iter().any(|q| matches!(q, Queued::Write(_)));
                let had_key = st.key.is_some();
                if let Err(e) = process(st, reqs, self.clock_id, &self.rt) {
                    // MST errors mean in-memory state can't be trusted: drop it and
                    // reload from durable state, once nothing is in flight.
                    tracing::error!(%did, "commit failed, evicting repo: {e:#}");
                    self.discard(did);
                    continue;
                }
                // Without its signing key (key service down at load, or just
                // rotated) writes are refused: reload, which unwraps again.
                if st.key.is_none() && (wrote || had_key) {
                    self.discard(did);
                    continue;
                }
                if wrote {
                    st.partition.recent.touch(&did);
                }
                self.settle(&did);
            }
            self.release_drained();
            self.evict();
            metrics::CACHED_REPOS
                .with_label_values(&[&self.label])
                .set(self.cache.len() as i64);
            metrics::REPO_CACHE_BYTES.with_label_values(&[&self.label]).set(self.bytes as i64);
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
                        let cached = matches!(&res, Ok(Some(st)) if (self.partitions)(&did).is_some_and(|p| Arc::ptr_eq(&p, &st.partition)));
                        if let Some(done) = self.preloads.remove(&did) {
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
                                if st.backfill {
                                    backfill_nodes(&mut st);
                                }
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
                        let _ = reply.send(self.cache.peek(&did).map(|st| CachedRepo {
                            loaded_nodes: st.mst.loaded_nodes(),
                            charge: st.charge,
                        }));
                    }
                    WorkerMsg::Fetched { did, res } => {
                        let buffered = self.loading.remove(&did).unwrap_or_default();
                        match (self.cache.peek_mut(&did), res) {
                            (Some(st), Ok(mst)) if st.fetching => {
                                metrics::LAZY_MST_FETCHES.with_label_values(&["ok"]).inc();
                                st.fetching = false;
                                st.mst = *mst;
                                order.push(did.clone());
                                groups.insert(did, buffered);
                            }
                            (cached, res) => {
                                // failed (a node or rebuilt leaf didn't match
                                // its link, or the store failed), or dropped
                                // meanwhile: reload from durable state once
                                // nothing is in flight (the open falls back
                                // to a rebuild from the records)
                                if let Err(e) = &res {
                                    tracing::error!(%did, "lazy MST fetch failed, evicting repo: {e}");
                                    metrics::LAZY_MST_FETCHES.with_label_values(&["error"]).inc();
                                }
                                if let Some(st) = cached {
                                    st.fetching = false;
                                }
                                self.loading.insert(did.clone(), buffered);
                                self.discard(did.clone());
                                if !self.draining.contains_key(&did) {
                                    self.reload_buffered(&did);
                                }
                            }
                        }
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
        let need = Some(Need::of(std::slice::from_ref(&req)));
        self.loading.insert(did.clone(), vec![req]);
        self.spawn_load_with(did, need);
    }

    /// Loads `did` in the background (its `loading` entry is set); the
    /// result comes back as [`WorkerMsg::Loaded`].
    fn spawn_load(&mut self, did: Arc<str>) {
        self.spawn_load_with(did, None)
    }

    /// [`spawn_load`](Self::spawn_load), also loading what `need` (the first
    /// request's) visits.
    fn spawn_load_with(&mut self, did: Arc<str>, need: Option<Need>) {
        let opts = LoadOpts { prefetch_bytes: self.limits.prefetch_bytes, need, secrets: Some(self.secrets.clone()) };
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
            let res = load_repo_with(partition, did.clone(), opts).await;
            let _ = STATS
                .load_us
                .lock()
                .record(t.elapsed().as_micros().max(1) as u64);
            metrics::REPO_LOAD_DURATION.observe(t.elapsed().as_secs_f64());
            metrics::LOADING_REPOS.dec();
            let _ = me.send(WorkerMsg::Loaded { did, res });
        });
    }

    /// Inserts a repo, keeping the byte count.
    fn cache_put(&mut self, did: Arc<str>, st: RepoState) {
        self.bytes += st.charge;
        if let Some(old) = self.cache.put(did, st) {
            self.bytes -= old.charge;
        }
    }

    fn cache_pop(&mut self, did: &Arc<str>) -> Option<RepoState> {
        let st = self.cache.pop(did)?;
        self.bytes -= st.charge;
        Some(st)
    }

    /// Re-charges a cached repo after it changed (its loaded paths).
    fn settle(&mut self, did: &Arc<str>) {
        if self.big.contains(did) {
            return; // charged as loaded until it is unloaded (`evict`)
        }
        let Some(st) = self.cache.peek_mut(did) else { return };
        let charge = repo_bytes(st);
        if charge > LAZY_REPO_MAX_BYTES {
            self.big.insert(did.clone());
        }
        self.bytes = self.bytes + charge - st.charge;
        st.charge = charge;
    }

    /// Unloads paths ([`unload_paths`](Self::unload_paths)), then evicts
    /// least recently used repos while the cache exceeds its entry or byte
    /// budget. Repos with commits in flight (durable state lags memory) or
    /// a fetch running are skipped.
    fn evict(&mut self) {
        self.unload_paths();
        let over = |n: usize, b: usize, l: &CacheLimits| n > l.entries || (l.bytes > 0 && b > l.bytes);
        let (mut n, mut b) = (self.cache.len(), self.bytes);
        if !over(n, b, &self.limits) {
            return;
        }
        let mut victims = Vec::new();
        for (scanned, (did, st)) in self.cache.iter().rev().enumerate() {
            if !over(n, b, &self.limits) || scanned > 1024 {
                break;
            }
            if st.fetching || st.pending.load(Ordering::Acquire) > 0 {
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

    /// Loads what `reqs` of a cached repo visit on the blocking pool
    /// (the paths of their keys and neighbours, the collection probes, or
    /// the whole tree for an account delete / repo import), so the worker
    /// thread never waits on the store. The repo's requests wait in
    /// `loading` meanwhile, so its tree doesn't change; the loaded copy
    /// replaces it on [`WorkerMsg::Fetched`] and the requests run then.
    fn start_fetch(&mut self, did: Arc<str>, reqs: Vec<Queued>, need: Need) {
        let Some(st) = self.cache.peek_mut(&did) else {
            self.loading.insert(did.clone(), reqs);
            self.reload_buffered(&did);
            return;
        };
        st.fetching = true;
        let mut mst = st.mst.clone();
        let db = st.partition.db.clone();
        let (rt, me, d) = (self.rt.clone(), self.me.clone(), did.clone());
        self.loading.insert(did, reqs);
        self.rt.spawn_blocking(move || {
            let res = need.load(&mut mst, &*db, &d, &rt).map(|_| Box::new(mst));
            let _ = me.send(WorkerMsg::Fetched { did: d, res });
        });
    }

    /// The path cache: drops the loaded paths (back to the
    /// root) of repos over [`LAZY_REPO_MAX_BYTES`], then of the least
    /// recently used ones while the worker is over its byte budget. Only
    /// repos with nothing in flight: their loaded nodes are then all in
    /// durable state (`M/`, `R/`), so any can be read back, and the delete
    /// check of a commit's persistence diff only needs the nodes its own
    /// walks loaded.
    fn unload_paths(&mut self) {
        let idle = |st: &RepoState| st.pending.load(Ordering::Acquire) == 0 && !st.fetching && st.view.read().head.rev == st.head.rev;
        let big: Vec<Arc<str>> = self.big.iter().cloned().collect();
        for did in big {
            let Some(st) = self.cache.peek_mut(&did) else {
                self.big.remove(&did);
                continue;
            };
            let was = st.charge;
            if idle(st) {
                unload_repo(st);
            } else if !st.fetching && !unload_settled(st) {
                continue; // a whole-tree entry in flight: wait for it
            }
            self.bytes = self.bytes - was + st.charge;
            if st.charge <= LAZY_REPO_MAX_BYTES {
                self.big.remove(&did);
            }
        }
        let all = self.limits.unload_idle;
        if !all && (self.limits.bytes == 0 || self.bytes <= self.limits.bytes) {
            return;
        }
        let mut b = self.bytes;
        let mut freed = 0;
        for (scanned, (_, st)) in self.cache.iter_mut().rev().enumerate() {
            if !all && (b <= self.limits.bytes || scanned > 4096) {
                break;
            }
            if !idle(st) || st.mst.loaded_nodes() <= 1 {
                continue;
            }
            let was = st.charge;
            unload_repo(st);
            b -= was - st.charge.min(was);
            freed += was - st.charge.min(was);
        }
        self.bytes -= freed;
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
                .send(Err(WriteError::Invalid(REPO_EXISTS.into())));
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
        let (commit, commit_block) = match sign_commit(&req.did, &rev.to_string(), &data, &req.key) {
            Ok(c) => c,
            Err(e) => {
                let _ = req.reply.send(Err(signature_fault(&e)));
                return;
            }
        };
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
        replace_nodes_mutations(&req.did, HashMap::new(), &tree, &mut muts);
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
        // a new repo's whole tree is in flight until this applies
        let applied = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let a2 = applied.clone();
        let entry = LogEntry {
            shard: partition.id,
            frames,
            muts,
            ack: Some(Box::new(move |r| {
                a2.store(true, Ordering::Release);
                let _ = reply.send(
                    r.map(|_| h2)
                        .map_err(|e| WriteError::Internal(e.to_string())),
                );
            })),
            pending: Some(pending.clone()),
            enqueued: Instant::now(),
        };
        let nodes = crate::mst::SharedNodeIndex::default();
        let mst = LazyTree::loaded(tree, 1);
        let view = new_view(&head, &mst, &nodes);
        let st = RepoState {
            did: req.did.clone(),
            partition: partition.clone(),
            mst,
            head,
            key: Some(req.key),
            pending,
            account,
            blob_refs: HashMap::new(),
            view,
            nodes,
            charge: 0,
            fetching: false,
            backfill: false,
            inflight: [(applied, None)].into(),
        };
        let did = req.did.clone();
        self.cache_put(req.did, st);
        if partition.tx.blocking_send(entry).is_err() {
            tracing::error!("partition sequencer gone");
        }
        self.settle(&did);
    }
}

/// What a repo's queued requests visit: the keys they write (their
/// paths and neighbours'), the collections they write (`coll/` probes for
/// the collection index), or everything (an account delete or import).
#[derive(Clone, Debug, Default)]
pub struct Need {
    keys: Vec<Vec<u8>>,
    probes: Vec<Vec<u8>>,
    all: bool,
}

impl Need {
    fn of(reqs: &[Queued]) -> Need {
        let mut n = Need::default();
        for q in reqs {
            match q {
                Queued::Write(r) => {
                    for w in &r.writes {
                        let p = w.path();
                        let probe = format!("{}/", collection_of(&p)).into_bytes();
                        if !n.probes.contains(&probe) {
                            n.probes.push(probe);
                        }
                        n.keys.push(p.into_bytes());
                    }
                }
                Queued::Account(AccountReq { op: AccountOp::ReplaceRepo { .. } | AccountOp::Delete, .. }) => n.all = true,
                Queued::Account(_) | Queued::Snapshot(_) => {}
            }
        }
        n
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.probes.is_empty() && !self.all
    }

    /// Loads it into `mst` from `db` (blocking: the blocking pool only).
    /// A persisted node found missing is an error too (the subtree was
    /// rebuilt from records, but `M/` lacks it): the repo is then reopened,
    /// which rebuilds the tree and backfills `M/`.
    fn load<R: slatedb::DbReadOps + Sync + ?Sized>(&self, mst: &mut LazyTree, db: &R, did: &str, rt: &tokio::runtime::Handle) -> Result<(), crate::mst::MstError> {
        let fallbacks = mst.stats.fallbacks;
        if self.all && !mst.fully_loaded() {
            // one forward scan of the records serves every unloaded leaf
            let scan = ScanSource::open(db, did, DbSource::new(db, did, rt), rt)?;
            mst.load_all(&scan)?;
        }
        let keys: Vec<&[u8]> = self.keys.iter().map(|k| &k[..]).collect();
        let probes: Vec<&[u8]> = self.probes.iter().map(|k| &k[..]).collect();
        mst.fetch(&keys, &probes, &DbSource::new(db, did, rt))?;
        match mst.stats.fallbacks > fallbacks {
            true => Err(crate::mst::MstError::Invalid("persisted MST nodes missing")),
            false => Ok(()),
        }
    }
}

/// Whether a repo's `reqs` run on its loaded paths alone: `Ok` (they do),
/// else what to load first (`Some`), or None if the walk failed (a node or
/// leaf that doesn't match its link: the repo is reloaded).
fn lazy_needs(st: &mut RepoState, reqs: Vec<Queued>) -> Result<Vec<Queued>, (Vec<Queued>, Option<Need>)> {
    let need = Need::of(&reqs);
    if need.is_empty() {
        return Ok(reqs);
    }
    if need.all && !st.mst.fully_loaded() {
        return Err((reqs, Some(need)));
    }
    let keys: Vec<&[u8]> = need.keys.iter().map(|k| &k[..]).collect();
    let probes: Vec<&[u8]> = need.probes.iter().map(|k| &k[..]).collect();
    match st.mst.fetch(&keys, &probes, &crate::mst_store::CachedOnly) {
        Ok(()) => Ok(reqs),
        Err(crate::mst::MstError::NotLoaded) => Err((reqs, Some(need))),
        Err(e) => {
            tracing::error!(did = %st.did, "lazy MST walk failed: {e}");
            Err((reqs, None))
        }
    }
}

/// Drops the loaded paths of a repo with commits in flight, except the
/// nodes those commits wrote (their state isn't applied yet). False if one
/// of them replaces the whole tree. Recharges the repo.
fn unload_settled(st: &mut RepoState) -> bool {
    while st.inflight.front().is_some_and(|(done, _)| done.load(Ordering::Acquire)) {
        st.inflight.pop_front();
    }
    let mut keep = HashSet::new();
    for (_, nodes) in &st.inflight {
        match nodes {
            Some(n) => keep.extend(n.iter().copied()),
            None => return false,
        }
    }
    st.mst.unload_except(&keep);
    metrics::LAZY_MST_UNLOADS.inc();
    st.charge = repo_bytes(st);
    true
}

/// Marks a repo's log entry that writes MST state as in flight (see
/// `RepoState::inflight`); the returned flag is set once it is applied.
fn track_inflight(st: &mut RepoState, nodes: Option<HashSet<Cid>>) -> Arc<std::sync::atomic::AtomicBool> {
    while st.inflight.front().is_some_and(|(done, _)| done.load(Ordering::Acquire)) {
        st.inflight.pop_front();
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    st.inflight.push_back((done.clone(), nodes));
    done
}

/// Drops a repo's loaded paths (nothing of it in flight) and
/// republishes its durable view without them (the view, at the same head,
/// shares the nodes).
fn unload_repo(st: &mut RepoState) {
    st.inflight.clear();
    st.mst.unload(0);
    *st.view.write() = st.durable_view();
    metrics::LAZY_MST_UNLOADS.inc();
    st.charge = repo_bytes(st);
}

/// Writes the interior nodes of a repo rebuilt from its records on open
/// (its `M/` nodes were missing or wrong) through the log, so the next open
/// reads them (replay included).
fn backfill_nodes(st: &mut RepoState) {
    st.backfill = false;
    let mut muts = Vec::new();
    replace_nodes_mutations(&st.did, HashMap::new(), &st.mst.tree, &mut muts);
    if muts.is_empty() {
        return;
    }
    let applied = track_inflight(st, None);
    let ack: crate::partition::AckFn = Box::new(move |_| applied.store(true, Ordering::Release));
    let entry = LogEntry { shard: st.partition.id, frames: Vec::new(), muts, ack: Some(ack), pending: Some(st.pending.clone()), enqueued: Instant::now() };
    if let Err(e) = send_entry(st, entry) {
        tracing::warn!(did = %st.did, "MST node backfill not logged: {e}");
    }
}

/// How a cold open loads a repo.
#[derive(Clone, Debug, Default)]
pub struct LoadOpts {
    /// Read up to this much of the repo's `M/` range with one scan first.
    pub prefetch_bytes: usize,
    /// What to load besides the root (the first request's paths).
    pub need: Option<Need>,
    /// Unwraps the account's signing key (None: the dev keyring).
    pub secrets: Option<Arc<Secrets>>,
}

/// Recently-written-repo preloads in flight per node: typical repos (a few
/// point reads and a short scan each).
const PRELOAD_CONCURRENCY: usize = 32;

/// Warms freshly opened shards in the background, so first writes don't pay
/// the cold load: their recently written repos (the set the previous owner
/// persisted with its last checkpoint, [`crate::partition::RecentRepos`]),
/// newest first and interleaved across shards, [`PRELOAD_CONCURRENCY`] at a
/// time. Each shard's set is
/// seeded with what it read, so it carries over to the next owner. Stops
/// early if the workers shut down; a shard closed meanwhile just fails its
/// loads (the worker drops stale ones).
pub fn spawn_preload(workers: &Workers, shards: Vec<(u16, Arc<slatedb::Db>, Arc<crate::partition::RecentRepos>)>) {
    use futures::StreamExt;
    let senders = Arc::downgrade(&workers.senders);
    tokio::spawn(async move {
        let t = Instant::now();
        // every shard's set at once: one at a time, a takeover's reads
        // queue behind the request-driven cold loads it is meant to spare
        let recent: Vec<Vec<Arc<str>>> = futures::stream::iter(shards)
            .map(|(shard, db, set)| async move {
                if set.cap() == 0 {
                    return Vec::new();
                }
                match db.get(crate::nodelog::META_RECENT).await {
                    Ok(Some(b)) => {
                        let recent = crate::partition::RecentRepos::decode(&b);
                        set.seed(&recent);
                        recent
                    }
                    Ok(None) => Vec::new(),
                    Err(e) => {
                        tracing::warn!(shard, "recent repos read failed: {e:#}");
                        Vec::new()
                    }
                }
            })
            .buffer_unordered(64)
            .filter(|v| std::future::ready(!v.is_empty()))
            .collect()
            .await;
        // newest first, round-robin over the shards
        let mut order = Vec::with_capacity(recent.iter().map(Vec::len).sum());
        for k in 0..recent.iter().map(Vec::len).max().unwrap_or(0) {
            order.extend(recent.iter().filter_map(|v| v.get(k).cloned()));
        }
        let n = order.len();
        let loaded = futures::stream::iter(order)
            .map(|did| {
                let senders = senders.clone();
                async move {
                    let rx = {
                        let senders = senders.upgrade()?;
                        let (tx, rx) = oneshot::channel();
                        let w = Workers { senders };
                        w.route(&did).send(WorkerMsg::Preload { did, done: tx }).ok()?;
                        rx
                    };
                    rx.await.ok()
                }
            })
            .buffer_unordered(PRELOAD_CONCURRENCY)
            .filter(|r| std::future::ready(*r == Some(true)))
            .count()
            .await;
        if n > 0 {
            tracing::info!(recent = n, recent_loaded = loaded, elapsed_ms = t.elapsed().as_millis() as u64, "repos preloaded");
        }
    });
}

/// The signed commit block: hedged nonce, and verified against the key's
/// public key before anything can sequence it (src/crypto.rs). Err: the
/// signature failed twice (suspected hardware fault; counted, and repeats
/// fail-stop the node): nothing may be emitted for this commit.
pub fn sign_commit(did: &str, rev: &str, data: &Cid, key: &Keypair) -> Result<(Cid, Bytes), crate::crypto::SignatureFault> {
    let unsigned = events::encode_commit(did, rev, data, None);
    let sig = key.sign_verified(crate::crypto::Purpose::Commit, &unsigned)?;
    let signed = events::encode_commit(did, rev, data, Some(&sig));
    Ok((Cid::dag_cbor(&signed), Bytes::from(signed)))
}

fn signature_fault(e: &crate::crypto::SignatureFault) -> WriteError {
    WriteError::SignatureFault(e.to_string())
}

/// Opens a repo cold: its root (the default `M/` prefetch), nothing else.
pub async fn load_repo(
    partition: Arc<Partition>,
    did: Arc<str>,
) -> anyhow::Result<Option<RepoState>> {
    load_repo_with(partition, did, LoadOpts { prefetch_bytes: DEFAULT_PREFETCH_BYTES, need: None, secrets: None }).await
}

pub async fn load_repo_with(
    partition: Arc<Partition>,
    did: Arc<str>,
    opts: LoadOpts,
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
    // the signing key: cached, else unwrapped (a KMS call). With the key
    // service down the repo still loads for reads; writes get a 503
    // (process_with) and the next one reloads (retries the unwrap).
    let secrets = opts.secrets.clone().unwrap_or_else(Secrets::dev);
    let key = match secrets.account_signing_key(&acct).await {
        // once per load: the (possibly long-cached) scalar still derives
        // the account's public key; if not, drop it and treat the key as
        // unavailable (writes 503 and reload, which unwraps afresh)
        Ok(k) if !k.matches_public(&acct.signing_pubkey) => {
            crate::crypto::record_fault(crate::crypto::Purpose::KeyLoad);
            secrets.forget(&did);
            None
        }
        Ok(k) => Some(k),
        Err(e) if e.retryable() => None,
        Err(e) => return Err(anyhow::anyhow!("signing key of {did}: {e}")),
    };
    let (mst, backfill) = open_lazy(&partition, &did, &head, &opts).await?;
    let blob_refs = load_blob_refs(db, &did).await?;
    let mut st = finish_load(partition.clone(), did, mst, head, key, acct)?;
    st.blob_refs = blob_refs;
    st.backfill = backfill;
    Ok(Some(st))
}

/// Opens a repo's MST lazily: its `M/` range read ahead with one scan (up to
/// `opts.prefetch_bytes`), the root, and the paths `opts.need` visits. A
/// root that isn't persisted is a small repo (a leaf root, rebuilt from its
/// few records) or one whose nodes are missing; a node or rebuilt leaf
/// that doesn't match its link means `M/` is wrong. Both rebuild the whole
/// tree from the records (no commit of the repo is in flight during a cold
/// open, so `R/` is at the head), and the caller backfills `M/` (the bool).
async fn open_lazy(partition: &Arc<Partition>, did: &Arc<str>, head: &Head, opts: &LoadOpts) -> anyhow::Result<(LazyTree, bool)> {
    let _permit = LOAD_PERMITS.acquire().await?;
    let (pre, _) = crate::mst_store::prefetch(&*partition.db, did, opts.prefetch_bytes).await?;
    let (db, did, root, need) = (partition.db.clone(), did.clone(), head.data, opts.need.clone().unwrap_or_default());
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || -> anyhow::Result<(LazyTree, bool)> {
        let src = DbSource::new(&*db, &did, &rt).with_prefetched(Some(&pre));
        let opened = LazyTree::open(root, 1, &src).and_then(|mut t| {
            need.load(&mut t, &*db, &did, &rt)?;
            Ok(t)
        });
        let err = match opened {
            Ok(t) if t.stats.node_reads == 0 && t.tree.root.height >= 1 => "missing",
            Ok(t) => return Ok((t, false)),
            Err(crate::mst::MstError::Store(e)) => anyhow::bail!("lazy MST open: {e}"),
            Err(crate::mst::MstError::Invalid("persisted MST nodes missing")) => "missing_node",
            Err(e) => {
                tracing::warn!(%did, "lazy MST open failed ({e}): rebuilding from records");
                "invalid"
            }
        };
        metrics::LAZY_MST_FALLBACKS.with_label_values(&[err]).inc();
        let mut recs = Vec::new();
        src.records(None, None, &mut recs)?;
        let mut t = LazyTree::loaded(crate::mst_lazy::build_tree(&recs)?, 1);
        let r = t.tree.root_cid()?;
        anyhow::ensure!(r == root, "MST rebuilt from records {r} != head data {root}");
        Ok((t, true))
    })
    .await?
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
/// object store with every repo's reads at once.
static LOAD_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(256);

fn finish_load(
    partition: Arc<Partition>,
    did: Arc<str>,
    mut mst: LazyTree,
    head: Head,
    key: Option<Arc<Keypair>>,
    account: state::Account,
) -> anyhow::Result<RepoState> {
    let root = mst.tree.root_cid()?;
    anyhow::ensure!(
        root == head.data,
        "rebuilt MST root {root} != head data {}",
        head.data
    );
    let nodes = crate::mst::SharedNodeIndex::default();
    let view = new_view(&head, &mst, &nodes);
    Ok(RepoState {
        did,
        partition,
        mst,
        head,
        key,
        pending: Arc::new(AtomicU32::new(0)),
        account,
        blob_refs: HashMap::new(),
        view,
        nodes,
        charge: 0,
        fetching: false,
        backfill: false,
        inflight: Default::default(),
    })
}


// Per-message / per-op counters, resolved once (`with_label_values` hashes
// the label and takes a lock on every call).
static CACHE_HIT: LazyLock<IntCounter> = LazyLock::new(|| metrics::REPO_CACHE.with_label_values(&["hit"]));
static CACHE_MISS: LazyLock<IntCounter> = LazyLock::new(|| metrics::REPO_CACHE.with_label_values(&["miss"]));
static CACHE_LOADING: LazyLock<IntCounter> = LazyLock::new(|| metrics::REPO_CACHE.with_label_values(&["loading"]));
static OPS_CREATE: LazyLock<IntCounter> = LazyLock::new(|| metrics::OPS.with_label_values(&["create"]));
static OPS_UPDATE: LazyLock<IntCounter> = LazyLock::new(|| metrics::OPS.with_label_values(&["update"]));
static OPS_DELETE: LazyLock<IntCounter> = LazyLock::new(|| metrics::OPS.with_label_values(&["delete"]));

/// Net change per path within one commit: (value before the commit, value after).
struct Batch {
    ops: BTreeMap<String, (Option<Cid>, Option<Cid>)>,
    records: HashMap<Cid, Bytes>,
    record_bytes: usize,
    /// Blob refs of each path's latest value in this batch.
    blobs: HashMap<String, Vec<Cid>>,
    waiters: Vec<(WriteReply, Vec<WriteOutcome>)>,
    /// Whether each collection the batch writes had records before it (the
    /// `C/` index changes where that differs after it).
    colls: BTreeMap<String, bool>,
}

impl Batch {
    fn new() -> Batch {
        Batch {
            ops: BTreeMap::new(),
            records: HashMap::new(),
            record_bytes: 0,
            blobs: HashMap::new(),
            waiters: Vec::new(),
            colls: BTreeMap::new(),
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
fn process(st: &mut RepoState, reqs: Vec<Queued>, clock_id: u64, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
    // the repo's paths were loaded before (`lazy_needs`); the source
    // reads only if an op still finds one missing (rare: metered)
    let (part, did) = (st.partition.clone(), st.did.clone());
    let db_src = DbSource::new(&*part.db, &did, rt);
    let r = process_with(st, reqs, clock_id, &db_src);
    if db_src.reads.get() > 0 {
        // the fetch before missed something: reads on the worker thread
        metrics::LAZY_MST_FETCHES.with_label_values(&["inline"]).inc_by(db_src.reads.get());
    }
    r
}

fn key_unavailable(did: &str) -> WriteError {
    WriteError::KeyUnavailable(format!("signing key of {did} is unavailable (key service unreachable); retry"))
}

fn process_with(st: &mut RepoState, reqs: Vec<Queued>, clock_id: u64, src: &dyn Source) -> anyhow::Result<()> {
    let mut rest = reqs.into_iter();
    let r = process_reqs(st, &mut rest, clock_id, src);
    // a commit whose signature failed: the requests not reached yet were
    // not applied either; answer them retryably rather than dropping them
    if let Err(e) = &r {
        if let Some(f) = e.downcast_ref::<crate::crypto::SignatureFault>() {
            for q in rest {
                q.fail(signature_fault(f));
            }
        }
    }
    r
}

fn process_reqs(st: &mut RepoState, reqs: &mut std::vec::IntoIter<Queued>, clock_id: u64, src: &dyn Source) -> anyhow::Result<()> {
    let mut batch = Batch::new();
    for q in reqs {
        let req = match q {
            // abandoned by its handler (answered "not started"): drop it
            Queued::Write(r) if r.claim.as_ref().is_some_and(|c| !c.take()) => {
                metrics::WRITES_ABANDONED.inc();
                continue;
            }
            Queued::Write(r) => r,
            Queued::Snapshot(r) => {
                let _ = r.reply.send(Ok(st.view.clone()));
                continue;
            }
            Queued::Account(a) => {
                if !batch.is_empty() {
                    flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id, src)?;
                }
                apply_account(st, a, clock_id, src)?;
                continue;
            }
        };
        if let Some(status) = &st.account.status {
            let _ = req
                .reply
                .send(Err(WriteError::RepoInactive(status.clone())));
            continue;
        }
        if st.key.is_none() {
            let _ = req.reply.send(Err(key_unavailable(&st.did)));
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
                flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id, src)?;
            }
            if sc != st.head.commit {
                let _ = req.reply.send(Err(WriteError::InvalidSwap(format!(
                    "commit was at {}",
                    st.head.commit
                ))));
                continue;
            }
        }
        // each write's path, formatted once for the checks and the apply
        let paths: Vec<String> = req.writes.iter().map(Write::path).collect();
        let new_paths = paths.iter().filter(|p| !batch.ops.contains_key(*p)).collect::<HashSet<_>>().len();
        let incoming_bytes: usize = req
            .writes
            .iter()
            .map(|w| match w {
                Write::Create { bytes, .. } | Write::Update { bytes, .. } => bytes.len(),
                _ => 0,
            })
            .sum();
        if !batch.is_empty()
            && (batch.ops.len() + new_paths > MAX_COMMIT_OPS
                || batch.record_bytes + incoming_bytes > MAX_COMMIT_RECORD_BYTES)
        {
            flush(st, std::mem::replace(&mut batch, Batch::new()), clock_id, src)?;
        }
        match validate(st, &req.writes, &paths, src) {
            Ok(()) => {}
            Err(e) => {
                let _ = req.reply.send(Err(e));
                continue;
            }
        }
        let mut outcomes = Vec::with_capacity(req.writes.len());
        for (w, path) in req.writes.into_iter().zip(paths) {
            if !batch.colls.contains_key(collection_of(&path)) {
                let coll = collection_of(&path);
                let had = st.mst.has_prefix(format!("{coll}/").as_bytes(), src)?;
                batch.colls.insert(coll.to_string(), had);
            }
            match w {
                Write::Create {
                    cid, bytes, blobs, ..
                }
                | Write::Update {
                    cid, bytes, blobs, ..
                } => {
                    let prev = st.mst.insert(path.as_bytes(), cid, src)?;
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
                    let prev = st.mst.remove(path.as_bytes(), src)?;
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
        flush(st, batch, clock_id, src)?;
    }
    Ok(())
}

/// Checks a request against the current tree (including earlier writes in
/// the batch) without mutating anything, so applyWrites stays atomic.
fn validate(st: &mut RepoState, writes: &[Write], paths: &[String], src: &dyn Source) -> Result<(), WriteError> {
    let mut overlay: HashMap<&str, Option<Cid>> = HashMap::new();
    for (w, path) in writes.iter().zip(paths) {
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
        let cur = match overlay.get(path.as_str()) {
            Some(v) => *v,
            None => st
                .mst
                .get(path.as_bytes(), src)
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
        overlay.insert(path.as_str(), new);
    }
    Ok(())
}

/// Builds, signs and enqueues one commit for the batch.
fn flush(st: &mut RepoState, batch: Batch, clock_id: u64, src: &dyn Source) -> anyhow::Result<()> {
    let build_start = Instant::now();
    let mut mst_blocks = Vec::with_capacity(16);
    let prev_data = st.head.data;
    // the collection index: whether each written collection still (or now)
    // has keys
    let mut coll_muts = Vec::new();
    for (coll, had) in &batch.colls {
        let has = st.mst.has_prefix(format!("{coll}/").as_bytes(), src)?;
        if has != *had {
            coll_muts.push(Mutation { key: state::collection_key(coll, &st.did).into(), val: has.then(Bytes::new) });
        }
    }
    // once getBlocks has asked for node blocks, report where written nodes
    // sit (interior nodes come from `M/`, leaves this way)
    let mut node_refs = st.nodes.lock().wanted.then(Vec::new);
    let (data, persist) = st.mst.write_diff_blocks_with_refs(&mut mst_blocks, node_refs.as_mut())?;
    // A batch that nets to no change (e.g. deleting a missing record) leaves no
    // dirty nodes; the commit's CAR must still carry the root node so it can be
    // loaded and verified.
    let mut persist = persist;
    if !mst_blocks.iter().any(|(c, _)| *c == data) {
        let root = st.mst.tree.root_block()?;
        // replay derives a put of it from the CAR (an interior root)
        if st.mst.tree.root.height >= st.mst.persist_min() && !st.mst.tree.root.entries.is_empty() {
            persist.puts.push(root.clone());
        }
        mst_blocks.push(root);
    }
    let rev = tid::next_rev(Some(st.head.rev), clock_id);
    let rev_s = rev.to_string();
    let since_s = st.head.rev.to_string();
    // process_with refuses writes to a repo without its key
    let key = st.key.as_deref().ok_or_else(|| anyhow::anyhow!("signing key unavailable"))?;
    let (commit, commit_block) = match sign_commit(&st.did, &rev_s, &data, key) {
        Ok(c) => c,
        Err(e) => {
            // nothing sequenced; the tree already holds the batch, so the
            // caller evicts the repo (it reloads from durable state)
            for (reply, _) in batch.waiters {
                let _ = reply.send(Err(signature_fault(&e)));
            }
            return Err(e.into());
        }
    };

    let mut ops = Vec::with_capacity(batch.ops.len());
    let mut muts = Vec::with_capacity(batch.ops.len() + 1);
    // exact up to the varints: header ~60, each block varint + 36-byte CID
    let mut car_bytes = Vec::with_capacity(
        96 + commit_block.len()
            + mst_blocks.iter().map(|(_, b)| b.len() + 40).sum::<usize>()
            + batch.record_bytes
            + batch.records.len() * 40,
    );
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
    // `M/` holds exactly the interior nodes of the tree at `h/`: put the
    // commit's (derived from its CAR at replay: `persisted_blocks`, the debug
    // check below), delete the replaced ones
    for (c, b) in std::mem::take(&mut persist.puts) {
        muts.push(Mutation { key: state::mst_node_key(&st.did, &c).into(), val: Some(Bytes::from(b)) });
    }
    for c in &persist.deletes {
        extra.push(Mutation { key: state::mst_node_key(&st.did, c).into(), val: None });
    }
    extra.append(&mut coll_muts);
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
        let d = crate::segment::derive_commit_muts_n(&f, derived).expect("derive commit muts");
        assert!(
            d.len() == derived && d.iter().zip(&muts).all(|(a, b)| a.key == b.key && a.val == b.val),
            "muts derived from the #commit frame differ from the commit's"
        );
    }
    STATS.commits.fetch_add(1, Ordering::Relaxed);
    STATS.ops.fetch_add(ops.len() as u64, Ordering::Relaxed);
    metrics::COMMITS.inc();
    for op in &ops {
        match op.action {
            "create" => OPS_CREATE.inc(),
            "update" => OPS_UPDATE.inc(),
            _ => OPS_DELETE.inc(),
        }
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
    let applied = track_inflight(st, Some(mst_blocks.iter().map(|(c, _)| *c).collect()));

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
            applied.store(true, Ordering::Release);
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

/// Maintains the blob-ref index for one net op (the collection index
/// changes at flush: `Batch::colls`).
fn index_mutations(
    st: &mut RepoState,
    rev: u64,
    path: &str,
    exists: bool,
    new_blobs: Option<&Vec<Cid>>,
    muts: &mut Vec<Mutation>,
) {
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

/// Mutations deleting every record (and its index entries) currently in the
/// repo. Loads the rest of its tree for it (the fetch before the op
/// normally did) and returns its persisted (interior) nodes, whose `M/`
/// keys the caller deletes or keeps.
fn clear_repo_mutations(st: &mut RepoState, muts: &mut Vec<Mutation>, src: &dyn Source) -> anyhow::Result<HashMap<Cid, Arc<[u8]>>> {
    if !st.mst.fully_loaded() {
        st.mst.load_all(src)?;
    }
    let did = st.did.clone();
    let mut colls = std::collections::BTreeSet::new();
    st.mst.tree.walk(&mut |k, cid| {
        if let Ok(path) = std::str::from_utf8(k) {
            muts.push(Mutation {
                key: state::record_key(&did, path).into(),
                val: None,
            });
            muts.push(Mutation {
                key: state::record_cid_key(&did, &cid, path).into(),
                val: None,
            });
            if !colls.contains(collection_of(path)) {
                colls.insert(collection_of(path).to_string());
            }
        }
    });
    for coll in &colls {
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
    st.blob_refs.clear();
    Ok(crate::mst_lazy::persisted_nodes(&st.mst.tree, st.mst.persist_min()))
}

/// `M/` mutations replacing a repo's persisted nodes `old` by those of
/// the (written, fully loaded) `tree`.
fn replace_nodes_mutations(did: &str, old: HashMap<Cid, Arc<[u8]>>, tree: &Tree, muts: &mut Vec<Mutation>) {
    let new = crate::mst_lazy::persisted_nodes(tree, 1);
    for c in old.keys().filter(|c| !new.contains_key(c)) {
        muts.push(Mutation { key: state::mst_node_key(did, c).into(), val: None });
    }
    for (c, b) in new {
        muts.push(Mutation { key: state::mst_node_key(did, &c).into(), val: Some(Bytes::copy_from_slice(&b)) });
    }
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

fn apply_account(st: &mut RepoState, req: AccountReq, clock_id: u64, src: &dyn Source) -> anyhow::Result<()> {
    let time = events::now_rfc3339();
    let mut frames = Vec::new();
    let mut muts = Vec::new();
    let whole_tree = matches!(req.op, AccountOp::ReplaceRepo { .. } | AccountOp::Delete);
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
            // (only its wrapped form is in the row: drop the old key, and the
            // worker reloads the repo, which finds the new one in the
            // keyring's cache, put there by the rotation). A rewrap under a
            // new KEK keeps the key.
            if account.signing_pubkey != st.account.signing_pubkey {
                st.key = None;
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
            let Some(key) = st.key.clone() else {
                let _ = req.reply.send(Err(key_unavailable(&st.did)));
                return Ok(());
            };
            let old_nodes = clear_repo_mutations(st, &mut muts, src)?;
            let rev = tid::next_rev(Some(st.head.rev), clock_id);
            let mut tree = Tree::new();
            let mut colls = HashSet::new();
            for (path, cid, bytes, blobs) in &records {
                if colls.insert(collection_of(path)) {
                    muts.push(Mutation { key: state::collection_key(collection_of(path), &st.did).into(), val: Some(Bytes::new()) });
                }
                tree.insert_no_proof(path.as_bytes(), *cid)?;
                muts.push(Mutation {
                    key: state::record_key(&st.did, path).into(),
                    val: Some(state::record_value(cid, rev.0, bytes)),
                });
                muts.push(Mutation {
                    key: state::record_cid_key(&st.did, cid, path).into(),
                    val: Some(Bytes::new()),
                });
                index_mutations(st, rev.0, path, true, Some(blobs), &mut muts);
            }
            let data = tree.root_cid()?;
            replace_nodes_mutations(&st.did, old_nodes, &tree, &mut muts);
            let (commit, commit_block) = match sign_commit(&st.did, &rev.to_string(), &data, &key) {
                Ok(c) => c,
                Err(e) => {
                    // nothing sequenced; evict (the import's reads loaded the tree)
                    let _ = req.reply.send(Err(signature_fault(&e)));
                    return Err(e.into());
                }
            };
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
            st.mst = LazyTree::loaded(tree, 1);
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
            let old_nodes = clear_repo_mutations(st, &mut muts, src)?;
            for c in old_nodes.keys() {
                muts.push(Mutation { key: state::mst_node_key(&st.did, c).into(), val: None });
            }
            st.mst = LazyTree::loaded(Tree::new(), 1);
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
    let applied = whole_tree.then(|| track_inflight(st, None));
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
                if let Some(a) = applied {
                    a.store(true, Ordering::Release);
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
        (WorkerMsg::Write(WriteReq { did: did.clone(), writes: vec![w], swap_commit: None, reply, claim: None }), rx)
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
        let part = Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone(), recent: Default::default() });
        let p2 = part.clone();
        let workers = spawn(1, 100, Arc::new(move |_: &str| Some(p2.clone())), tokio::runtime::Handle::current());
        drop(part);
        let w = workers.senders[0].clone();
        let did: Arc<str> = "did:plc:test".into();
        let key = Arc::new(Keypair::generate());
        let account = serde_json::json!({
            "did": &*did, "handle": "t.test", "wrapped_signing_key": Secrets::dev().wrap_signing_key(&did, &key).await.unwrap().0, "signing_pubkey": key.public_multibase(),
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

    /// A write its handler abandoned before the worker took it (a forwarded
    /// write answered RepoLoading) is never applied; one with a live claim is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_writes_are_not_applied() {
        let store = crate::store::Store::memory(None);
        let db = Arc::new(crate::partition::open_db(&store, 0, None).await.unwrap());
        let (merger_tx, _merger_rx) = tokio::sync::mpsc::unbounded_channel();
        let log = NodeLog::start(
            store.clone(),
            NodeLogConfig { log_id: "t".into(), writer: 1, max_segment_bytes: 1 << 20, hedge_after: Duration::from_secs(1), lease_ok: None },
            merger_tx,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LogEntry>(16);
        let part = Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone(), recent: Default::default() });
        let p2 = part.clone();
        let workers = spawn(1, 100, Arc::new(move |_: &str| Some(p2.clone())), tokio::runtime::Handle::current());
        let w = workers.senders[0].clone();
        let did: Arc<str> = "did:plc:claims".into();
        let key = Arc::new(Keypair::generate());
        let account = serde_json::json!({
            "did": &*did, "handle": "t.test", "wrapped_signing_key": Secrets::dev().wrap_signing_key(&did, &key).await.unwrap().0, "signing_pubkey": key.public_multibase(),
            "password_hash": "", "created_at": "2026-01-01T00:00:00Z",
        });
        let (reply, created) = oneshot::channel();
        w.send(WorkerMsg::CreateRepo(CreateRepoReq { did: did.clone(), handle: "t.test".into(), key, account_json: Bytes::from(serde_json::to_vec(&account).unwrap()), records: Vec::new(), reply })).unwrap();
        settle(rx.recv().await.unwrap());
        created.await.unwrap().unwrap();
        let with_claim = |rkey: &str, abandoned: bool| {
            let (m, r) = write(&did, rkey);
            let WorkerMsg::Write(mut req) = m else { unreachable!() };
            let c = Arc::new(Claim::default());
            if abandoned {
                assert!(c.abandon());
            }
            req.claim = Some(c.clone());
            (WorkerMsg::Write(req), r, c)
        };
        let (m1, r1, _) = with_claim("a", true);
        let (m2, r2, c2) = with_claim("b", false);
        w.send(m1).unwrap();
        w.send(m2).unwrap();
        settle(rx.recv().await.unwrap());
        assert!(r1.await.is_err(), "abandoned write answered");
        let ack = r2.await.unwrap().unwrap();
        assert!(matches!(&ack.results[..], [WriteOutcome::Create { path, .. }] if path == "app.test.thing/b"), "{:?}", ack.results);
        assert!(!c2.abandon(), "taken by the worker");
        let (reply, info) = oneshot::channel();
        w.send(WorkerMsg::CacheInfo { did: did.clone(), reply }).unwrap();
        assert!(info.await.unwrap().unwrap().loaded_nodes >= 1);
        assert!(part.recent.take_dirty().is_some_and(|b| b.as_ref() == b"did:plc:claims\n"), "written repo tracked as recent");
    }

    /// The path cache: over the byte budget, idle repos drop back to their
    /// root, least recently used first; if the roots alone are still over
    /// it, the least recently used repos are evicted. A repo charged over
    /// [`LAZY_REPO_MAX_BYTES`] drops its paths as soon as it is idle,
    /// whatever the budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn path_cache_unloads_then_evicts_by_bytes() {
        let store = crate::store::Store::memory(None);
        let db = Arc::new(crate::partition::open_db(&store, 0, None).await.unwrap());
        let (merger_tx, _merger_rx) = tokio::sync::mpsc::unbounded_channel();
        let log = NodeLog::start(
            store.clone(),
            NodeLogConfig { log_id: "t".into(), writer: 1, max_segment_bytes: 1 << 20, hedge_after: Duration::from_secs(1), lease_ok: None },
            merger_tx,
        );
        let (tx, _rx) = tokio::sync::mpsc::channel::<LogEntry>(16);
        let part = Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone(), recent: Default::default() });
        // a fully loaded repo (as after an import or a rebuild)
        let repo = |name: &str, records: u32| {
            let did: Arc<str> = format!("did:plc:{name}").into();
            let key = Keypair::generate();
            let mut tree = Tree::new();
            for i in 0..records {
                tree.insert_no_proof(format!("c.x/{i:05}").as_bytes(), Cid::dag_cbor(&i.to_be_bytes())).unwrap();
            }
            let root = tree.root_cid().unwrap();
            let head = Head { commit: root, data: root, rev: Tid(1), commit_block: Bytes::new() };
            let acct: state::Account = serde_json::from_value(serde_json::json!({
                "did": &*did, "handle": "t.test", "wrapped_signing_key": "", "signing_pubkey": key.public_multibase(), "password_hash": "", "created_at": "",
            }))
            .unwrap();
            (did.clone(), finish_load(part.clone(), did, LazyTree::loaded(tree, 1), head, Some(Arc::new(key)), acct).unwrap())
        };
        // a repo's charge fully loaded, and with its root only
        let charges = |records: u32| {
            let (_, mut st) = repo("probe", records);
            let full = repo_bytes(&st);
            st.mst.unload(0);
            (full, repo_bytes(&st))
        };
        let (full, root) = charges(400);
        assert!(full > 3 * root, "{full} vs {root}");
        let limits = CacheLimits { entries: 100, bytes: full + root + root / 2, ..CacheLimits::from(0) };
        let (me, _me_rx) = crossbeam_channel::unbounded();
        let mut w = Worker::new(0, me, Arc::new(|_: &str| None), tokio::runtime::Handle::current(), limits, Secrets::dev());
        let put = |w: &mut Worker, (did, st): (Arc<str>, RepoState)| {
            w.cache_put(did.clone(), st);
            w.settle(&did);
            w.evict();
            did
        };
        let loaded = |w: &Worker, did: &Arc<str>| w.cache.peek(did).map(|st| st.mst.loaded_nodes());
        // the second loaded repo puts the worker over: the older one unloads
        let a = put(&mut w, repo("a", 400));
        let b = put(&mut w, repo("b", 400));
        assert_eq!(loaded(&w, &a), Some(1));
        assert!(loaded(&w, &b).unwrap() > 1);
        assert_eq!(w.bytes, full + root);
        // a third: b unloads, then c itself
        let c = put(&mut w, repo("c", 400));
        assert_eq!((loaded(&w, &b), loaded(&w, &c)), (Some(1), Some(1)));
        assert_eq!((w.cache.len(), w.bytes), (3, 3 * root));
        // roots alone over the budget: the least recently used is evicted
        w.limits.bytes = 2 * root + root / 2;
        w.evict();
        assert!(!w.cache.contains(&a) && w.cache.contains(&b) && w.cache.contains(&c));
        assert_eq!(w.bytes, 2 * root);
        // an unbounded cache still unloads a repo over 1 MiB once idle
        w.limits.bytes = 0;
        let (big_full, big_root) = charges(8_000);
        assert!(big_full > LAZY_REPO_MAX_BYTES, "{big_full}");
        let big = put(&mut w, repo("big", 8_000));
        assert_eq!(loaded(&w, &big), Some(1));
        assert!(!w.big.contains(&big));
        assert_eq!(w.bytes, 2 * root + big_root);
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

    /// Thread CPU seconds (CLOCK_THREAD_CPUTIME_ID).
    fn thread_cpu() -> f64 {
        #[repr(C)]
        struct Ts(i64, i64);
        unsafe extern "C" {
            fn clock_gettime(clk: i32, ts: *mut Ts) -> i32;
        }
        let clk = if cfg!(target_os = "macos") { 16 } else { 3 };
        let mut t = Ts(0, 0);
        unsafe { clock_gettime(clk, &mut t) };
        t.0 as f64 + t.1 as f64 * 1e-9
    }

    /// CPU per commit of the worker's commit path (validate, MST insert,
    /// diff blocks + node refs, sign, CAR, #commit frame, mutations) plus
    /// its ack (durable view swap), one createRecord per commit, on repos
    /// of 20 / 5000 TID-keyed posts (the no-I/O fetch pass, the neighbour
    /// walks, `M/` puts/deletes and collection probes included, on a tree
    /// whose paths are loaded), with the state bytes
    /// (keys + values written to SlateDB) and the segment bytes (frame +
    /// stored muts) per commit. Measurement only:
    /// `cargo test --release --lib bench_commit_cpu -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_commit_cpu() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let (part, mut rx) = rt.block_on(async {
            let store = crate::store::Store::memory(None);
            let db = Arc::new(crate::partition::open_db(&store, 0, None).await.unwrap());
            let (merger_tx, _merger_rx) = tokio::sync::mpsc::unbounded_channel();
            let log = NodeLog::start(
                store.clone(),
                NodeLogConfig { log_id: "t".into(), writer: 1, max_segment_bytes: 1 << 20, hedge_after: Duration::from_secs(1), lease_ok: None },
                merger_tx,
            );
            std::mem::forget(_merger_rx);
            let (tx, rx) = tokio::sync::mpsc::channel::<LogEntry>(1 << 16);
            (Arc::new(Partition { id: 0, epoch: 1, db, apply_lock: Default::default(), tx, wm: log.wm.clone(), log: log.clone(), recent: Default::default() }), rx)
        });
        let tid = |i: u64| Tid::from_parts(1_700_000_000_000_000 + i * 1_000_003, i % 1024).to_string();
        for records in [20u64, 5000] {
            let did: Arc<str> = format!("did:plc:bench{records}").into();
            let key = Keypair::generate();
            let mut tree = Tree::new();
            for i in 0..records {
                tree.insert_no_proof(format!("app.bsky.feed.post/{}", tid(i)).as_bytes(), Cid::dag_cbor(&i.to_be_bytes())).unwrap();
            }
            let root = tree.root_cid().unwrap();
            let head = Head { commit: root, data: root, rev: Tid(1), commit_block: Bytes::new() };
            let acct: state::Account = serde_json::from_value(serde_json::json!({
                "did": &*did, "handle": "t.test", "wrapped_signing_key": "", "signing_pubkey": key.public_multibase(), "password_hash": "", "created_at": "",
            }))
            .unwrap();
            let mut st = finish_load(part.clone(), did.clone(), LazyTree::loaded(tree, 1), head, Some(Arc::new(key)), acct).unwrap();
            let mut next = records;
            let (mut state_bytes, mut seg_bytes, mut commits) = (0usize, 0usize, 0usize);
            let mut one = |st: &mut RepoState| {
                let rkey = tid(next);
                next += 1;
                let bytes = Bytes::from(format!("{{\"$type\":\"app.bsky.feed.post\",\"text\":\"post {rkey} {}\",\"createdAt\":\"2026-10-01T00:00:00.000Z\"}}", "x".repeat(120)));
                let (reply, _rx) = oneshot::channel();
                let w = Write::Create { collection: "app.bsky.feed.post".into(), rkey, cid: Cid::dag_cbor(&bytes), bytes, blobs: Vec::new() };
                let reqs = vec![Queued::Write(WriteReq { did: did.clone(), writes: vec![w], swap_commit: None, reply, claim: None })];
                let Ok(reqs) = lazy_needs(st, reqs) else { panic!("paths not loaded") };
                process(st, reqs, 7, rt.handle()).unwrap();
                while let Ok(e) = rx.try_recv() {
                    commits += 1;
                    state_bytes += e.muts.iter().map(|m| m.key.len() + m.val.as_ref().map_or(0, |v| v.len())).sum::<usize>();
                    let derived = e.frames.first().map_or(0, |f| f.derived_muts);
                    seg_bytes += e.frames.iter().map(|f| f.len_hint()).sum::<usize>() + e.muts[derived..].iter().map(|m| 6 + m.key.len() + m.val.as_ref().map_or(0, |v| v.len())).sum::<usize>();
                    settle(e);
                }
            };
            for _ in 0..2000 {
                one(&mut st);
            }
            let mut best = f64::MAX;
            // BENCH_ROUNDS: more rounds, e.g. to attach a sampler
            let rounds = std::env::var("BENCH_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(7);
            for _ in 0..rounds {
                let n = 3000;
                let t = thread_cpu();
                for _ in 0..n {
                    one(&mut st);
                }
                best = best.min((thread_cpu() - t) / n as f64 * 1e6);
            }
            println!(
                "bench_commit_cpu records={records}: {best:.2} us/commit (thread CPU, best of {rounds}); {} state B/commit, {} segment B/commit",
                state_bytes / commits.max(1),
                seg_bytes / commits.max(1)
            );
        }
    }
}
