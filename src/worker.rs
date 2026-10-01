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
use std::time::Instant;
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

pub enum AccountOp {
    /// Persist a new account record. `old_handle` set = handle changed (index
    /// moved, #identity emitted). `account_event` emits #account with the
    /// account's status (status None = active). Writes are rejected while
    /// status is Some.
    Update {
        account: state::Account,
        old_handle: Option<String>,
        identity_event: bool,
        account_event: bool,
    },
    /// Replace the whole repo contents (importRepo / reset): new commit, #sync.
    /// Records are (path, cid, bytes, blob refs).
    ReplaceRepo {
        records: Vec<(String, Cid, Bytes, Vec<Cid>)>,
    },
    /// Delete the account and repo: #account(active=false, status=deleted).
    Delete,
    /// Persist a reactivated account: #account, #identity and #sync of the
    /// current commit, as the reference's sequenceAccountActivation.
    Activate { account: state::Account },
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
}

/// The repo as of its latest *durable* commit: what exports and proofs serve.
/// Published by the commit's ack (after the state apply), so it never shows a
/// commit that could still be lost.
pub struct DurableView {
    pub head: Head,
    pub tree: Tree,
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
}

fn new_view(head: &Head, tree: &Tree) -> ViewCell {
    Arc::new(parking_lot::RwLock::new(Arc::new(DurableView { head: head.clone(), tree: tree.clone() })))
}

#[derive(Clone)]
pub struct Workers {
    pub senders: Arc<Vec<Sender<WorkerMsg>>>,
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
    cache_per_worker: usize,
    partitions: PartitionLookup,
    rt: tokio::runtime::Handle,
) -> Workers {
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
            .spawn(move || Worker::new(i, me, partitions, rt, cache_per_worker).run(rx))
            .unwrap();
    }
    Workers {
        senders: Arc::new(senders),
    }
}

struct Worker {
    label: String,
    me: Sender<WorkerMsg>,
    partitions: PartitionLookup,
    rt: tokio::runtime::Handle,
    cache: lru::LruCache<Arc<str>, RepoState>,
    cap: usize,
    loading: HashMap<Arc<str>, Vec<Queued>>,
    clock_id: u64,
}

impl Worker {
    fn new(
        idx: usize,
        me: Sender<WorkerMsg>,
        partitions: PartitionLookup,
        rt: tokio::runtime::Handle,
        cap: usize,
    ) -> Worker {
        Worker {
            label: idx.to_string(),
            me,
            partitions,
            rt,
            cache: lru::LruCache::unbounded(),
            cap,
            loading: HashMap::new(),
            clock_id: rand::random::<u64>() & 0x3ff,
        }
    }

    fn run(mut self, rx: Receiver<WorkerMsg>) {
        let mut msgs = Vec::with_capacity(8192);
        while let Ok(first) = rx.recv() {
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
                        buf.push(q);
                    } else if self.cache.contains(&did) {
                        let g = groups.entry(did.clone()).or_insert_with(|| {
                            order.push(did.clone());
                            Vec::new()
                        });
                        g.push(q);
                    } else {
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
                    continue;
                };
                if let Err(e) = process(st, reqs, self.clock_id) {
                    // MST errors mean in-memory state can't be trusted: drop it and
                    // reload from durable state on the next write.
                    tracing::error!(%did, "commit failed, evicting repo: {e:#}");
                    self.cache.pop(&did);
                }
            }
            self.evict();
            metrics::CACHED_REPOS
                .with_label_values(&[&self.label])
                .set(self.cache.len() as i64);
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
                        match res {
                            Ok(Some(st)) => {
                                STATS.repo_loads.fetch_add(1, Ordering::Relaxed);
                                metrics::REPO_LOADS.with_label_values(&["ok"]).inc();
                                self.cache.put(did.clone(), st);
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
                    WorkerMsg::DropPartition(p, done) => {
                        let drop: Vec<Arc<str>> = self
                            .cache
                            .iter()
                            .filter(|(_, st)| st.partition.id == p)
                            .map(|(d, _)| d.clone())
                            .collect();
                        for d in drop {
                            self.cache.pop(&d);
                        }
                        let _ = done.send(());
                    }
                }
            }
        }
    }

    fn start_load(&mut self, req: Queued) {
        let did = req.did().clone();
        self.loading.insert(did.clone(), vec![req]);
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
            metrics::LOADING_REPOS.dec();
            let _ = me.send(WorkerMsg::Loaded { did, res });
        });
    }

    fn evict(&mut self) {
        let mut skipped = 0;
        while self.cache.len() > self.cap && skipped < 64 {
            let Some((did, st)) = self.cache.pop_lru() else {
                break;
            };
            if st.pending.load(Ordering::Acquire) > 0 {
                // still has commits in flight: durable state lags memory; keep it
                self.cache.put(did, st);
                skipped += 1;
            } else {
                metrics::REPO_EVICTIONS.inc();
            }
        }
    }

    fn create_repo(&mut self, req: CreateRepoReq) {
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
        let frames = vec![
            events::identity_frame(&req.did, &req.handle, &time),
            events::account_frame(&req.did, true, None, &time),
            events::sync_frame(&req.did, &rev.to_string(), &car_bytes, &time),
        ];
        let mut muts = Vec::with_capacity(3 + req.records.len());
        let mut colls = HashSet::new();
        for (path, cid, bytes) in &req.records {
            muts.push(Mutation {
                key: state::record_key(&req.did, path).into(),
                val: Some(state::record_value(cid, rev.0, bytes)),
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
                key: state::handle_key(&req.handle).into(),
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
        let account: state::Account = match serde_json::from_slice(&req.account_json) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("bad account json: {e}");
                return;
            }
        };
        let mut collections = HashMap::new();
        for (path, _, _) in &req.records {
            *collections
                .entry(collection_of(path).to_string())
                .or_insert(0) += 1;
        }
        let view = new_view(&head, &tree);
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
        };
        self.cache.put(req.did, st);
        if partition.tx.blocking_send(entry).is_err() {
            tracing::error!("partition sequencer gone");
        }
    }
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
    let view = new_view(&head, &tree);
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
    })
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
    let data = st.tree.write_diff_blocks(&mut mst_blocks)?;
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
            &mut muts,
        );
        let key = Bytes::from(state::record_key(&st.did, path));
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

    let time = events::now_rfc3339();
    let frame = events::commit_frame(&events::CommitFrame {
        repo: &st.did,
        rev: &rev_s,
        since: Some(&since_s),
        commit,
        prev_data: Some(prev_data),
        blocks: &car_bytes,
        ops: &ops,
        time: &time,
    });
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
    st.head = head;
    st.pending.fetch_add(1, Ordering::AcqRel);

    let waiters = batch.waiters;
    let (view, snap) = (st.view.clone(), Arc::new(DurableView { head: st.head.clone(), tree: st.tree.clone() }));
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
    st.partition
        .tx
        .blocking_send(entry)
        .map_err(|_| anyhow::anyhow!("partition sequencer gone"))
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
    st.partition
        .tx
        .blocking_send(entry)
        .map_err(|_| anyhow::anyhow!("partition sequencer gone"))
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
    st.tree.walk(&mut |k, _| {
        if let Ok(path) = std::str::from_utf8(k) {
            muts.push(Mutation {
                key: state::record_key(&did, path).into(),
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

fn apply_account(st: &mut RepoState, req: AccountReq, clock_id: u64) -> anyhow::Result<()> {
    let time = events::now_rfc3339();
    let mut frames = Vec::new();
    let mut muts = Vec::new();
    match req.op {
        AccountOp::Update {
            account,
            old_handle,
            identity_event,
            account_event,
        } => {
            if let Some(old) = &old_handle {
                if *old != account.handle {
                    muts.push(Mutation {
                        key: state::handle_key(old).into(),
                        val: None,
                    });
                    muts.push(Mutation {
                        key: state::handle_key(&account.handle).into(),
                        val: Some(Bytes::from(st.did.to_string())),
                    });
                }
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
        AccountOp::Activate { account } => {
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
                index_mutations(st, rev.0, path, false, true, Some(blobs), &mut muts);
            }
            let data = tree.root_cid()?;
            let (commit, commit_block) = sign_commit(&st.did, &rev.to_string(), &data, &st.key);
            let mut car_bytes = Vec::with_capacity(commit_block.len() + 64);
            car::write_header(&mut car_bytes, &commit);
            car::write_block(&mut car_bytes, &commit, &commit_block);
            frames.push(events::sync_frame(
                &st.did,
                &rev.to_string(),
                &car_bytes,
                &time,
            ));
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
                key: state::handle_key(&st.account.handle).into(),
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
            let (view, snap) = (st.view.clone(), Arc::new(DurableView { head: st.head.clone(), tree: st.tree.clone() }));
            Box::new(move |r| {
                if r.is_ok() {
                    *view.write() = snap;
                }
                inner(r)
            })
        }),
        pending: Some(st.pending.clone()),
        enqueued: Instant::now(),
    };
    send_entry(st, entry)
}
