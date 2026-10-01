//! Cluster control plane: node leases, writer ids, shard assignments and log
//! fencing (see DESIGN.md "HA" and "Planet scale"). A single node is a
//! one-node cluster.
//!
//! Objects (all CAS via ETag or If-None-Match):
//!   nodes/{node_id}     NodeLease {log_id, addr, writer, renewals, ..}  renewed by the node
//!   writers/{w:03}      WriterClaim {node_id, log_id, confirmed}  unique seq low byte among live nodes
//!   assign/{shard:03}   Assignment {owner, log_id, addr, epoch, seq_floor, history[Span]}
//!                       changes only when a shard moves
//!   log/{log}/{ord}.seg a fence object at a dead log's next ordinal closes it
//!
//! Liveness never compares wall clocks across nodes. A peer is presumed dead
//! once its lease object has not changed for TTL + skew of *our* monotonic
//! time since we last saw it change (a lease first seen gets a full TTL). A
//! node's own validity runs on its own monotonic clock from the send time of
//! its last successful renewal (TTL - skew).
//!
//! Safety does not rest on clocks (DESIGN.md "Why safety needs no clocks"):
//! - A node acks only after its segment PUT (If-None-Match at the next
//!   ordinal) succeeded, and only while its lease is valid.
//! - A dead node's log is fenced before its shards are reassigned, so the
//!   span end used for replay is final and a zombie can never extend it.
//! - Assignments move by CAS; a new owner replays previous spans first.
//! - A node fail-stops when its lease lapses or a shard it holds is
//!   reassigned under it.

use crate::nodelog::Span;
use crate::store::Store;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeLease {
    pub node_id: String,
    pub log_id: String,
    pub addr: String,
    pub writer: u8,
    /// The writer's wall clock + TTL. Informational (status pages); peers
    /// never compare it with their own clocks.
    pub expires_ms: u64,
    /// Bumped on every write, so every renewal changes the object (and its
    /// ETag): peers judge liveness by seeing it change.
    pub renewals: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Assignment {
    pub owner: Option<String>,
    pub log_id: Option<String>,
    pub addr: Option<String>,
    pub epoch: u64,
    /// Every seq a previous owner assigned for this shard is <= this. A new
    /// owner's seqs start above it, so a repo's commits keep their firehose
    /// order across a handoff whatever the two nodes' wall clocks say.
    pub seq_floor: i64,
    /// Chronological ownership spans (last may be open).
    pub history: Vec<Span>,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub node_id: String,
    pub addr: String,
    pub shards: u16,
    pub ttl: Duration,
    pub renew_every: Duration,
    pub skew: Duration,
    /// Tests only: offsets this node's wall clock as the control plane sees
    /// it (the `expires_ms` it publishes), to simulate clock skew in-process.
    pub clock_offset_ms: i64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            node_id: format!("node-{}", hex::encode(rand::random::<[u8; 4]>())),
            addr: "http://127.0.0.1:2583".into(),
            shards: 256,
            ttl: Duration::from_secs(10),
            renew_every: Duration::from_secs(2),
            skew: Duration::from_secs(2),
            clock_offset_ms: 0,
        }
    }
}

/// What the cluster asks the node to do with shards.
#[async_trait::async_trait]
pub trait ShardHost: Send + Sync + 'static {
    /// Next ordinal of our log (start of our span when we take a shard).
    fn next_ordinal(&self) -> u64;
    /// Last durable ordinal of our log (end of our span when we release).
    fn durable_end(&self) -> u64;
    /// Every seq this node has assigned so far is <= this (after a close,
    /// the closed shards' seqs are covered).
    fn seq_high(&self) -> i64 {
        0
    }
    /// Returns once every seq this node assigns from now on exceeds `seq`
    /// (a shard taken from a node whose clock runs ahead of ours).
    async fn wait_seq_floor(&self, _seq: i64) {}
    /// Replay each shard's `history` (previous owners' spans) and start
    /// serving them. Batched so a takeover reads a dead log once for all shards.
    async fn open_many(&self, shards: Vec<(u16, u64, Vec<Span>)>) -> Vec<(u16, anyhow::Result<()>)>;
    /// Stop accepting writes for `shards`, drain (one barrier segment for all
    /// of them), checkpoint, close. A shard may be released only if its close
    /// succeeded: otherwise entries of it may still be in flight.
    async fn close_many(&self, shards: Vec<u16>) -> Vec<(u16, anyhow::Result<()>)>;
    /// Waits until nothing is queued or in flight on our log (before fencing
    /// it on shutdown). False if it did not quiesce in time.
    async fn quiesce(&self) -> bool {
        true
    }
    /// Our node lease was lost (CAS failed): must stop acking immediately.
    fn lost(&self);
    /// Called after every membership refresh (e.g. to follow peers' logs).
    fn on_membership(&self) {}
}

/// What we last saw of a peer's lease, timed on our monotonic clock.
struct Seen {
    etag: Option<String>,
    lease: NodeLease,
    changed_at: Instant,
}

/// Re-read every assignment (not only those whose ETag changed in the
/// LIST) every this many steps: a safety net, ~5 min at the default TTL.
const FULL_RESYNC_STEPS: u64 = 150;

/// An object with the ETag it was read at.
type Versioned<T> = (T, Option<String>);
type Cached = Option<Versioned<Assignment>>;

pub struct Cluster {
    pub cfg: ClusterConfig,
    pub log_id: String,
    pub writer: u8,
    store: Store,
    lease_etag: RwLock<Option<String>>,
    lease: RwLock<NodeLease>,
    /// Our lease expiry on our own (unoffset) wall clock: the watermark cap.
    expires_local_ms: AtomicU64,
    valid_until: RwLock<Instant>,
    /// Routing table: shard -> (owner node, addr)
    table: RwLock<Vec<Option<(String, String)>>>,
    owned: RwLock<HashSet<u16>>,
    /// Live peers (node_id -> lease), refreshed every step.
    peers: RwLock<HashMap<String, NodeLease>>,
    /// Peer leases as last observed (node_id -> etag, lease, when it changed).
    seen: RwLock<HashMap<String, Seen>>,
    /// Assignment cache (with ETags), refreshed from a LIST every step.
    assigns: RwLock<Vec<Cached>>,
    steps: AtomicU64,
    /// Control-plane object-store requests this node made (also exported
    /// as vlpds_cluster_store_requests_total).
    requests: AtomicU64,
    /// Dead logs we know are fenced: log_id -> (fence ordinal, last seq in it).
    fenced: RwLock<HashMap<String, (u64, i64)>>,
    joined_at: Instant,
    /// Set by shutdown(); held across each step so a step can't re-acquire
    /// shards while (or after) shutdown releases them.
    stopping: AtomicBool,
    step_lock: tokio::sync::Mutex<()>,
    /// Set once shutdown is about to delete our lease: the renew loop and the
    /// watchdog stop (they keep running during the shutdown drain itself).
    gone: AtomicBool,
    /// Held across each renewal so shutdown can't delete the lease under one.
    renew_lock: tokio::sync::Mutex<()>,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn is_conflict(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
}

impl Cluster {
    /// Registers this node: claims a writer id and creates its node lease.
    pub async fn join(cfg: ClusterConfig, store: Store) -> anyhow::Result<Arc<Cluster>> {
        let log_id = format!("{}.{}", cfg.node_id, crate::tid::now_micros());
        let mut c = Cluster {
            log_id: log_id.clone(),
            writer: 0,
            store,
            lease_etag: RwLock::new(None),
            lease: RwLock::new(NodeLease { node_id: cfg.node_id.clone(), log_id, addr: cfg.addr.clone(), writer: 0, expires_ms: 0, renewals: 0 }),
            expires_local_ms: AtomicU64::new(0),
            valid_until: RwLock::new(Instant::now()),
            table: RwLock::new(vec![None; cfg.shards as usize]),
            owned: RwLock::new(HashSet::new()),
            peers: RwLock::new(HashMap::new()),
            seen: RwLock::new(HashMap::new()),
            assigns: RwLock::new(vec![None; cfg.shards as usize]),
            steps: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            fenced: RwLock::new(HashMap::new()),
            joined_at: Instant::now(),
            stopping: AtomicBool::new(false),
            step_lock: tokio::sync::Mutex::new(()),
            gone: AtomicBool::new(false),
            renew_lock: tokio::sync::Mutex::new(()),
            cfg,
        };
        // create (or take over our own stale) node lease
        let path = c.path(&format!("nodes/{}", c.cfg.node_id));
        let existing = c.get_json::<NodeLease>(&path).await?;
        let mut mode = match existing {
            None => PutMode::Create,
            Some((l, etag)) => {
                // Our previous incarnation (same node id). Fence its log now:
                // if it is somehow still running, its next PUT collides and it
                // fail-stops; everything it acked is before the fence and gets
                // replayed by whoever takes its shards (us).
                if l.log_id != c.log_id {
                    c.fence(&l.log_id).await?;
                }
                c.lease.write().renewals = l.renewals;
                PutMode::Update(UpdateVersion { e_tag: etag, version: None })
            }
        };
        loop {
            let (writer, claim_etag) = c.claim_writer().await?;
            c.writer = writer;
            c.lease.write().writer = writer;
            c.write_lease(mode).await?;
            mode = PutMode::Update(UpdateVersion { e_tag: c.lease_etag.read().clone(), version: None });
            // Confirm the claim now that our lease exists (a claim is taken
            // over only while its holder has no lease). Rewriting it changes
            // its ETag, so a joiner that read it before our lease existed
            // fails its CAS instead of sharing our writer id.
            let wpath = c.path(&format!("writers/{writer:03}"));
            let confirmed = serde_json::json!({"node_id": c.cfg.node_id, "log_id": c.log_id, "confirmed": true});
            match c.put_json(&wpath, &confirmed, PutMode::Update(UpdateVersion { e_tag: claim_etag, version: None })).await {
                Ok(_) => break,
                Err(e) if is_conflict(&e) => {
                    tracing::warn!(writer, "writer id taken over before our lease existed; claiming another");
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(Arc::new(c))
    }

    fn path(&self, rel: &str) -> Path {
        Path::from(format!("{}/{}", self.store.prefix, rel))
    }

    /// Counts a control-plane object-store request.
    fn count(&self, op: &str) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        crate::metrics::CLUSTER_STORE_REQUESTS.with_label_values(&[op]).inc();
    }

    /// Control-plane object-store requests made so far.
    pub fn store_requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Our wall clock as published to peers (offset in tests only).
    fn wall_ms(&self) -> u64 {
        (now_ms() as i64 + self.cfg.clock_offset_ms).max(0) as u64
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> anyhow::Result<Option<(T, Option<String>)>> {
        self.count("get");
        match self.store.raw.get(path).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                Ok(Some((serde_json::from_slice(&r.bytes().await?)?, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_json<T: Serialize>(&self, path: &Path, v: &T, mode: PutMode) -> Result<Option<String>, object_store::Error> {
        self.count("put");
        let body = PutPayload::from(serde_json::to_vec(v).unwrap());
        self.store.raw.put_opts(path, body, PutOptions { mode, ..Default::default() }).await.map(|r| r.e_tag)
    }

    /// (file name, ETag) of every object under `rel`: one LIST per 1000.
    async fn list(&self, rel: &str) -> anyhow::Result<Vec<(String, Option<String>)>> {
        use futures::StreamExt;
        self.count("list");
        let mut out = Vec::new();
        let mut list = self.store.raw.list(Some(&self.path(rel)));
        while let Some(m) = list.next().await {
            let m = m?;
            if let Some(name) = m.location.filename() {
                out.push((name.to_string(), m.e_tag.clone()));
            }
        }
        Ok(out)
    }

    async fn delete(&self, rel: &str) {
        self.count("delete");
        let _ = self.store.raw.delete(&self.path(rel)).await;
    }

    /// Claims a writer id (CAS); returns it and the claim's ETag. A claim is
    /// free when unclaimed, ours (a previous incarnation), or its holder has
    /// no node lease at all (gone, or not yet created: `join` confirms). A
    /// holder with a lease is never judged dead here: that takes observation
    /// over time, and there are 256 ids to choose from.
    async fn claim_writer(&self) -> anyhow::Result<(u8, Option<String>)> {
        let start = (crate::state::did_hash(&self.cfg.node_id) % 256) as u16;
        let claim = serde_json::json!({"node_id": self.cfg.node_id, "log_id": self.log_id, "confirmed": false});
        for i in 0..256u16 {
            let w = ((start + i) % 256) as u8;
            let path = self.path(&format!("writers/{w:03}"));
            let mode = match self.get_json::<serde_json::Value>(&path).await? {
                None => PutMode::Create,
                Some((v, etag)) => {
                    let holder = v["node_id"].as_str().unwrap_or_default().to_string();
                    if holder != self.cfg.node_id && self.get_json::<NodeLease>(&self.path(&format!("nodes/{holder}"))).await?.is_some() {
                        continue;
                    }
                    PutMode::Update(UpdateVersion { e_tag: etag, version: None })
                }
            };
            match self.put_json(&path, &claim, mode).await {
                Ok(etag) => return Ok((w, etag)),
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("no free writer id (256 nodes with leases?)")
    }

    async fn write_lease(&self, mode: PutMode) -> anyhow::Result<()> {
        let sent = Instant::now();
        let mut l = self.lease.read().clone();
        l.expires_ms = self.wall_ms() + self.cfg.ttl.as_millis() as u64;
        l.renewals += 1;
        let etag = self.put_json(&self.path(&format!("nodes/{}", self.cfg.node_id)), &l, mode).await?;
        *self.lease_etag.write() = etag;
        *self.lease.write() = l;
        self.expires_local_ms.store(now_ms() + self.cfg.ttl.as_millis() as u64, Ordering::Release);
        *self.valid_until.write() = sent + self.cfg.ttl - self.cfg.skew;
        Ok(())
    }

    /// True while we may acknowledge writes / PUT segments.
    pub fn lease_valid(&self) -> bool {
        Instant::now() < *self.valid_until.read()
    }

    /// Our lease expiry on our own wall clock (caps our announced watermark).
    pub fn lease_expiry_us(&self) -> u64 {
        self.expires_local_ms.load(Ordering::Acquire) * 1000
    }

    pub fn is_owner(&self, shard: u16) -> bool {
        self.owned.read().contains(&shard)
    }

    pub fn owned(&self) -> Vec<u16> {
        let mut v: Vec<u16> = self.owned.read().iter().copied().collect();
        v.sort();
        v
    }

    /// (node_id, addr) owning `shard`, from the routing table.
    pub fn owner_of(&self, shard: u16) -> Option<(String, String)> {
        self.table.read().get(shard as usize).cloned().flatten()
    }

    pub fn peers(&self) -> Vec<NodeLease> {
        self.peers.read().values().cloned().collect()
    }

    pub fn fenced_logs(&self) -> HashMap<String, u64> {
        self.fenced.read().iter().map(|(k, (o, _))| (k.clone(), *o)).collect()
    }

    /// Closes a dead node's log: writes a fence object at its first free
    /// ordinal. Returns that ordinal (the log's final end) and the last seq
    /// in the log before it.
    pub async fn fence(&self, log_id: &str) -> anyhow::Result<(u64, i64)> {
        if let Some(f) = self.fenced.read().get(log_id) {
            return Ok(*f);
        }
        loop {
            let mut next = 0u64;
            for (name, _) in self.list(&format!("log/{log_id}")).await? {
                if let Some(ord) = name.strip_suffix(".seg").and_then(|f| f.parse::<u64>().ok()) {
                    next = next.max(ord + 1);
                }
            }
            // HA fix: if the last object is already a fence (another survivor
            // fenced first), that ordinal is the log's end. Without this check
            // every survivor stacked another fence after it, so spans for the same
            // dead log ended at different ordinals.
            if next > 0 {
                let last = crate::nodelog::segment_path(&self.store, log_id, next - 1);
                self.count("get");
                if let Ok(r) = self.store.raw.get(&last).await {
                    if matches!(crate::segment::parse(r.bytes().await?, false, None), Ok(crate::segment::LogObject::Fence { .. })) {
                        let seq = self.last_seq_before(log_id, next - 1).await?;
                        self.fenced.write().insert(log_id.to_string(), (next - 1, seq));
                        return Ok((next - 1, seq));
                    }
                }
            }
            let path = crate::nodelog::segment_path(&self.store, log_id, next);
            self.count("put");
            match self.store.raw.put_opts(&path, PutPayload::from_bytes(crate::segment::fence_object(&self.cfg.node_id)), PutOptions { mode: PutMode::Create, ..Default::default() }).await {
                Ok(_) => {}
                Err(e) if is_conflict(&e) => {
                    // a zombie got a segment in, or another node fenced first
                    self.count("get");
                    let b = self.store.raw.get(&path).await?.bytes().await?;
                    if !matches!(crate::segment::parse(b, false, None)?, crate::segment::LogObject::Fence { .. }) {
                        continue; // re-scan: the log grew
                    }
                }
                Err(e) => return Err(e.into()),
            }
            let seq = self.last_seq_before(log_id, next).await?;
            self.fenced.write().insert(log_id.to_string(), (next, seq));
            tracing::info!(log_id, fence_ordinal = next, last_seq = seq, "fenced dead node's log");
            return Ok((next, seq));
        }
    }

    /// The last seq in `log_id` below ordinal `end` (0 for an empty log).
    async fn last_seq_before(&self, log_id: &str, end: u64) -> anyhow::Result<i64> {
        let mut ord = end;
        while ord > 0 {
            ord -= 1;
            self.count("get");
            let b = self.store.raw.get(&crate::nodelog::segment_path(&self.store, log_id, ord)).await?.bytes().await?;
            if let crate::segment::LogObject::Segment(h, _) = crate::segment::parse(b, false, None)? {
                return Ok(h.last_seq);
            }
        }
        Ok(0)
    }

    pub fn spawn(self: &Arc<Self>, host: Arc<dyn ShardHost>) {
        // HA fix: renew the node lease on its own loop. A step reads every
        // node lease and every shard assignment (O(shards) S3 GETs); renewing
        // only at the top of a step meant one slow step (S3 at ~400 ms per
        // call: 64 GETs = ~25 s) let the lease lapse and the node fail-stop
        // within ~2 s of a mild S3 brownout (bench/ha s3-slow).
        let me = self.clone();
        let h = host.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.renew_every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                // keeps renewing through a graceful shutdown's drain (closing
                // 100+ shards takes longer than a TTL); stops when the lease goes
                if me.gone.load(Ordering::Acquire) {
                    return;
                }
                me.renew(&h).await;
            }
        });
        // HA fix: lease watchdog. Lease validity was only checked before a
        // segment PUT or an ack, so a node whose S3 calls hang (the PUT in
        // flight never returns) stayed up as a zombie. It held forwarded and
        // client requests open until the network healed (12 s in bench/ha
        // s3-partition and full-partition), long after peers had fenced its
        // log. Peers presume us dead TTL + skew after they last saw our lease
        // change, i.e. no earlier than 2 x skew after our validity ends: by
        // then we can never ack again, so fail-stop.
        let me = self.clone();
        let h = host.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.renew_every / 2);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if me.gone.load(Ordering::Acquire) {
                    return;
                }
                let lapsed_for = Instant::now().saturating_duration_since(*me.valid_until.read());
                if lapsed_for > me.cfg.skew * 2 {
                    tracing::error!(lapsed_ms = lapsed_for.as_millis() as u64, "node lease lapsed past takeover: fail-stop");
                    h.lost();
                    return;
                }
            }
        });
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.renew_every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = me.step_inner(&host, false).await {
                    tracing::warn!("cluster step failed: {e:#}");
                }
            }
        });
    }

    /// Renews our node lease (CAS on its ETag); a conflict means someone
    /// else rewrote it: fail-stop via `host.lost()`.
    async fn renew(&self, host: &Arc<dyn ShardHost>) {
        let _g = self.renew_lock.lock().await;
        if self.gone.load(Ordering::Acquire) {
            return;
        }
        // HA fix: never resurrect a lapsed lease. Peers may already have
        // declared us dead and fenced our log; a renewal that lands afterwards
        // makes them count us live again, so they shrink their fair share and
        // release shards they just took (unowned until our lease expires once
        // more). Seen with a SIGSTOP zombie waking (bench/ha zombie-short).
        if !self.lease_valid() {
            tracing::error!("node lease lapsed before renewal: fail-stop");
            host.lost();
            return;
        }
        let etag = self.lease_etag.read().clone();
        if let Err(e) = self.write_lease(PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
            match e.downcast_ref::<object_store::Error>() {
                Some(oe) if is_conflict(oe) => {
                    tracing::error!("node lease lost (CAS conflict)");
                    host.lost();
                }
                _ => tracing::warn!("node lease renew error (will retry): {e:#}"),
            }
        }
    }

    /// Membership: (live, dead) node leases. One LIST; a lease is fetched
    /// only when its ETag changed (or the store reports none). A peer is live
    /// until its lease has gone unchanged for TTL + skew of our monotonic
    /// time, or until we fence its log (it can never ack again, even if a
    /// renewal it sent before lapsing lands late).
    async fn read_nodes(&self) -> anyhow::Result<(Vec<NodeLease>, Vec<NodeLease>)> {
        use futures::StreamExt;
        // judge staleness as of before the LIST: a slow LIST can't make a
        // lease look older than it is
        let t0 = Instant::now();
        let listed = self.list("nodes").await?;
        let stale: Vec<String> = {
            let seen = self.seen.read();
            listed
                .iter()
                .filter(|(id, etag)| *id != self.cfg.node_id && (etag.is_none() || seen.get(id).is_none_or(|s| s.etag != *etag)))
                .map(|(id, _)| id.clone())
                .collect()
        };
        let fetched: Vec<(String, Option<Versioned<NodeLease>>)> = futures::stream::iter(stale)
            .map(|id| async move {
                let r = self.get_json::<NodeLease>(&self.path(&format!("nodes/{id}"))).await;
                r.map(|v| (id, v))
            })
            .buffered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()?;
        let now = Instant::now();
        let mut seen = self.seen.write();
        for (id, got) in fetched {
            match got {
                None => {
                    seen.remove(&id);
                }
                Some((lease, etag)) => {
                    let prev = seen.get(&id).filter(|s| s.lease.renewals == lease.renewals && s.lease.log_id == lease.log_id);
                    let changed_at = prev.map_or(now, |s| s.changed_at);
                    seen.insert(id, Seen { etag, lease, changed_at });
                }
            }
        }
        let names: HashSet<&String> = listed.iter().map(|(id, _)| id).collect();
        seen.retain(|id, _| names.contains(id));
        let fenced = self.fenced.read();
        let (mut live, mut dead) = (vec![self.lease.read().clone()], Vec::new());
        for s in seen.values() {
            let quiet = t0.saturating_duration_since(s.changed_at);
            if fenced.contains_key(&s.lease.log_id) || quiet > self.cfg.ttl + self.cfg.skew {
                dead.push(s.lease.clone());
            } else {
                live.push(s.lease.clone());
            }
        }
        Ok((live, dead))
    }

    /// Refreshes the assignment cache: one LIST, then a GET for each shard
    /// whose ETag changed (every shard every FULL_RESYNC_STEPS steps).
    async fn read_assignments(&self) -> anyhow::Result<()> {
        use futures::StreamExt;
        let n = self.cfg.shards as usize;
        let full = self.steps.fetch_add(1, Ordering::Relaxed).is_multiple_of(FULL_RESYNC_STEPS);
        let mut listed: Vec<Option<Option<String>>> = vec![None; n];
        for (name, etag) in self.list("assign").await? {
            if let Some(s) = name.parse::<usize>().ok().filter(|s| *s < n) {
                listed[s] = Some(etag);
            }
        }
        let stale: Vec<usize> = {
            let cache = self.assigns.read();
            (0..n)
                .filter(|&s| match (&listed[s], &cache[s]) {
                    (None, _) => false,
                    (Some(Some(e)), Some((_, Some(ce)))) => full || e != ce,
                    _ => true,
                })
                .collect()
        };
        let fetched: Vec<(usize, Cached)> = futures::stream::iter(stale)
            .map(|s| async move { self.get_json::<Assignment>(&self.path(&format!("assign/{s:03}"))).await.map(|a| (s, a)) })
            .buffered(32)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()?;
        let mut cache = self.assigns.write();
        for (s, l) in listed.iter().enumerate() {
            if l.is_none() {
                cache[s] = None;
            }
        }
        for (s, a) in fetched {
            cache[s] = a;
        }
        Ok(())
    }

    /// Runs `fut` (shard opens/closes, which can take many seconds for 100+
    /// shards) while renewing our lease every renew interval, when no
    /// independent renew loop is running (the inline first step at startup,
    /// tests). Renewals are never cancelled mid-flight: a dropped CAS PUT could
    /// land with an ETag we never learn, and our next renew would "lose" the lease.
    async fn with_keepalive<T>(&self, host: &Arc<dyn ShardHost>, renew: bool, fut: impl std::future::Future<Output = T>) -> T {
        if !renew {
            return fut.await;
        }
        let done = tokio::sync::Notify::new();
        let work = async {
            let r = fut.await;
            done.notify_one();
            r
        };
        let keepalive = async {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(self.cfg.renew_every) => {}
                    _ = done.notified() => return,
                }
                self.renew(host).await;
            }
        };
        let (r, ()) = tokio::join!(work, keepalive);
        r
    }

    /// One control-plane round: renew, membership, routing, acquire/release.
    pub async fn step(&self, host: &Arc<dyn ShardHost>) -> anyhow::Result<()> {
        self.step_inner(host, true).await
    }

    async fn step_inner(&self, host: &Arc<dyn ShardHost>, renew: bool) -> anyhow::Result<()> {
        let _step = self.step_lock.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        // 1. renew our node lease (the spawned loop renews on its own task)
        if !renew {
            return self.step_body(host).await;
        }
        self.renew(host).await;
        // HA fix: the first step runs inline at startup, before the renew loop
        // exists. On a fresh prefix a lone node acquires its whole share there
        // (one CAS PUT per shard, then opening them all: 181 shards took 19 s
        // for the UI agent), which outlived the lease, so the node fail-stopped
        // on first start. Keep renewing for as long as the inline step runs.
        self.with_keepalive(host, true, self.step_body(host)).await
    }

    async fn step_body(&self, host: &Arc<dyn ShardHost>) -> anyhow::Result<()> {
        // 2. membership
        let (live, dead) = self.read_nodes().await?;
        *self.peers.write() = live.iter().filter(|l| l.node_id != self.cfg.node_id).map(|l| (l.node_id.clone(), l.clone())).collect();
        let live_ids: HashSet<String> = live.iter().map(|l| l.node_id.clone()).collect();
        let dead_logs: HashMap<String, String> = dead.iter().map(|l| (l.node_id.clone(), l.log_id.clone())).collect();
        // 3. assignments -> routing table
        let n = self.cfg.shards;
        self.read_assignments().await?;
        let assigns = self.assigns.read().clone();
        {
            let mut t = self.table.write();
            for (s, a) in assigns.iter().enumerate() {
                t[s] = a.as_ref().and_then(|(a, _)| {
                    let o = a.owner.clone()?;
                    live_ids.contains(&o).then(|| (o, a.addr.clone().unwrap_or_default()))
                });
            }
        }
        host.on_membership();
        // A shard we hold was reassigned under us: a peer presumed us dead
        // and fenced our log, so we can never ack again. Fail-stop now rather
        // than serve stale reads until our next segment PUT collides.
        if let Some(s) = self.owned().into_iter().find(|&s| {
            assigns[s as usize].as_ref().is_none_or(|(a, _)| a.owner.as_deref() != Some(&self.cfg.node_id) || a.log_id.as_deref() != Some(&self.log_id))
        }) {
            tracing::error!(shard = s, "a shard we hold was reassigned: fail-stop");
            host.lost();
            return Ok(());
        }
        let fair = (n as usize).div_ceil(live.len().max(1));
        let owned = self.owned();
        // Join grace: give every peer a membership refresh to discover us (and
        // start following our log) before we produce events, so no peer's
        // merged firehose has already moved past our first seqs.
        let has_peers = live.iter().any(|l| l.node_id != self.cfg.node_id);
        if has_peers && self.joined_at.elapsed() < self.cfg.renew_every * 2 {
            return Ok(());
        }
        // HA fix: never take (or juggle) shards without a valid lease, e.g. a
        // zombie that woke after its peers fenced it, or while renewals fail.
        if !self.lease_valid() {
            return Ok(());
        }
        // 4. acquire free / orphaned shards up to our fair share
        if owned.len() < fair {
            let mut want = fair - owned.len();
            let mut acquired = Vec::new();
            let mut floor = 0i64;
            for s in 0..n {
                if want == 0 {
                    break;
                }
                if self.is_owner(s) {
                    continue;
                }
                let (cur, etag) = match &assigns[s as usize] {
                    None => (Assignment::default(), None),
                    Some((a, e)) => (a.clone(), e.clone()),
                };
                let mut history = cur.history.clone();
                let mut seq_floor = cur.seq_floor;
                let stale_self = cur.owner.as_deref() == Some(&self.cfg.node_id) && cur.log_id.as_deref() != Some(&self.log_id);
                match &cur.owner {
                    Some(o) if live_ids.contains(o) && !stale_self => continue, // healthy owner
                    Some(o) => {
                        // orphaned (dead owner, or our own previous incarnation):
                        // fence its log first so the span end is final
                        let Some(log) = cur.log_id.clone().or_else(|| dead_logs.get(o).cloned()) else { continue };
                        let (end, last_seq) = self.fence(&log).await?;
                        seq_floor = seq_floor.max(last_seq);
                        if let Some(last) = history.last_mut() {
                            if last.end.is_none() {
                                last.end = Some(end);
                            }
                        }
                    }
                    None => {}
                }
                let epoch = cur.epoch + 1;
                let mut next = history.clone();
                next.push(Span { log_id: self.log_id.clone(), epoch, start: host.next_ordinal(), end: None });
                if next.len() > 16 {
                    next.drain(..next.len() - 16);
                }
                let newa = Assignment {
                    owner: Some(self.cfg.node_id.clone()),
                    log_id: Some(self.log_id.clone()),
                    addr: Some(self.cfg.addr.clone()),
                    epoch,
                    seq_floor,
                    history: next,
                };
                let mode = match etag {
                    None => PutMode::Create,
                    Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
                };
                match self.put_json(&self.path(&format!("assign/{s:03}")), &newa, mode).await {
                    Ok(e) => self.assigns.write()[s as usize] = Some((newa, e)),
                    Err(e) if is_conflict(&e) => {
                        // someone else moved it: re-read it next step
                        if let Some((_, etag)) = self.assigns.write()[s as usize].as_mut() {
                            *etag = None;
                        }
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
                self.owned.write().insert(s);
                floor = floor.max(seq_floor);
                acquired.push((s, epoch, history));
                want -= 1;
            }
            if !acquired.is_empty() {
                tracing::info!(shards = ?acquired.iter().map(|a| a.0).collect::<Vec<_>>(), owned = self.owned().len(), fair, live = live.len(), "acquired shards");
                host.wait_seq_floor(floor).await;
            }
            for (s, res) in host.open_many(acquired).await {
                match res {
                    Ok(()) => {
                        self.table.write()[s as usize] = Some((self.cfg.node_id.clone(), self.cfg.addr.clone()));
                    }
                    Err(e) => {
                        // nothing was logged for it: release with an empty span
                        tracing::error!(shard = s, "open failed: {e:#}; releasing");
                        self.owned.write().remove(&s);
                        self.release(s, host.next_ordinal(), host.seq_high()).await?;
                    }
                }
            }
        } else if owned.len() > fair {
            // 5. hand back extras so newcomers get work
            tracing::info!(owned = owned.len(), fair, live = live.len(), "releasing extra shards");
            let extras: Vec<u16> = owned.iter().rev().take(owned.len() - fair).copied().collect();
            if !self.close_and_release(host, extras).await {
                return Ok(());
            }
        }
        // 6. dead nodes whose shards have all moved can be forgotten
        for d in &dead {
            if !assigns.iter().any(|a| a.as_ref().is_some_and(|(a, _)| a.owner.as_deref() == Some(&d.node_id))) && self.fenced.read().contains_key(&d.log_id) {
                self.delete(&format!("nodes/{}", d.node_id)).await;
            }
        }
        Ok(())
    }

    /// Closes `shards` together (one barrier segment for all of them) and
    /// releases each one whose close succeeded. A failed close means entries
    /// of that shard may still be in flight past the span end we would
    /// publish, so it is never released: we fail-stop instead, and a
    /// successor fences our log and replays it to the fence. False if we
    /// fail-stopped.
    async fn close_and_release(&self, host: &Arc<dyn ShardHost>, shards: Vec<u16>) -> bool {
        use futures::StreamExt;
        if shards.is_empty() {
            return true;
        }
        let n = shards.len();
        let started = Instant::now();
        let closed = host.close_many(shards).await;
        let (end, floor) = (host.durable_end(), host.seq_high());
        let mut ok = true;
        let mut done = Vec::new();
        for (s, r) in closed {
            match r {
                Ok(()) => {
                    self.owned.write().remove(&s);
                    done.push(s);
                }
                Err(e) => {
                    tracing::error!(shard = s, "close failed: {e:#}");
                    ok = false;
                }
            }
        }
        let released: Vec<anyhow::Result<()>> = futures::stream::iter(done).map(|s| self.release(s, end, floor)).buffer_unordered(32).collect().await;
        for r in released {
            if let Err(e) = r {
                // the assignment still names us with an open span: whoever
                // takes the shard once our lease lapses fences and replays
                tracing::warn!("release failed: {e:#}");
            }
        }
        tracing::info!(shards = n, elapsed_ms = started.elapsed().as_millis() as u64, "closed and released shards");
        if !ok {
            tracing::error!("a shard failed to close cleanly: fail-stop (a successor fences and replays our log)");
            host.lost();
        }
        ok
    }

    /// Releases `shard`, closing our span at `end` (exclusive) and raising its
    /// seq floor to `seq_floor`. CAS against the cached assignment; re-read
    /// once on a conflict.
    async fn release(&self, shard: u16, end: u64, seq_floor: i64) -> anyhow::Result<()> {
        let path = self.path(&format!("assign/{shard:03}"));
        let mut cur = self.assigns.read()[shard as usize].clone().filter(|(_, e)| e.is_some());
        for attempt in 0..2 {
            let (mut a, etag) = match cur.take() {
                Some(c) => c,
                None => match self.get_json::<Assignment>(&path).await? {
                    Some(c) => c,
                    None => return Ok(()),
                },
            };
            if a.owner.as_deref() != Some(&self.cfg.node_id) {
                return Ok(());
            }
            if let Some(last) = a.history.last_mut() {
                if last.log_id == self.log_id && last.end.is_none() {
                    last.end = Some(end);
                }
            }
            a.owner = None;
            a.addr = None;
            a.log_id = None;
            a.seq_floor = a.seq_floor.max(seq_floor);
            match self.put_json(&path, &a, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
                Ok(e) => {
                    self.assigns.write()[shard as usize] = Some((a, e));
                    self.table.write()[shard as usize] = None;
                    return Ok(());
                }
                Err(e) if is_conflict(&e) && attempt == 0 => continue, // stale cache: re-read
                Err(e) => return Err(e.into()),
            }
        }
        unreachable!("second attempt returns")
    }

    /// Graceful shutdown: close and release every shard, drop our lease.
    pub async fn shutdown(&self, host: &Arc<dyn ShardHost>) {
        // HA fix: stop the step loop first (it would re-acquire the shards we
        // are releasing), and wait out a step already in flight.
        self.stopping.store(true, Ordering::Release);
        let _step = self.step_lock.lock().await;
        if !self.close_and_release(host, self.owned()).await {
            return;
        }
        // Nothing may still be in flight on our log when we fence it: a
        // fence below an in-flight segment would cut entries out of a span.
        if !host.quiesce().await {
            tracing::error!("our log did not quiesce: fail-stop without fencing it (peers will)");
            host.lost();
            return;
        }
        // HA fix: fence our own (now idle) log before dropping the lease.
        // Peers following it drain it from S3 up to a fence and only then drop
        // its firehose source; without a fence they wait forever and every
        // peer's merged firehose stalls at our watermark (bench/ha sigterm).
        if let Err(e) = self.fence(&self.log_id).await {
            tracing::warn!("fencing our log on shutdown failed: {e:#}");
        }
        {
            // stop renewing (waiting out a renewal in flight) before the delete
            let _r = self.renew_lock.lock().await;
            self.gone.store(true, Ordering::Release);
        }
        self.delete(&format!("nodes/{}", self.cfg.node_id)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::AtomicI64;

    #[derive(Default)]
    struct Host {
        opened: Mutex<Vec<(u16, u64, Vec<Span>)>>,
        closed: Mutex<Vec<Vec<u16>>>,
        ord: AtomicU64,
        seq: AtomicI64,
        floors: Mutex<Vec<i64>>,
        lost: AtomicU64,
        fail_close: Mutex<HashSet<u16>>,
    }

    #[async_trait::async_trait]
    impl ShardHost for Host {
        fn next_ordinal(&self) -> u64 {
            self.ord.load(Ordering::SeqCst)
        }
        fn durable_end(&self) -> u64 {
            self.ord.load(Ordering::SeqCst)
        }
        fn seq_high(&self) -> i64 {
            self.seq.load(Ordering::SeqCst)
        }
        async fn wait_seq_floor(&self, seq: i64) {
            self.floors.lock().push(seq);
        }
        async fn open_many(&self, v: Vec<(u16, u64, Vec<Span>)>) -> Vec<(u16, anyhow::Result<()>)> {
            v.into_iter()
                .map(|(s, e, h)| {
                    self.opened.lock().push((s, e, h));
                    (s, Ok(()))
                })
                .collect()
        }
        async fn close_many(&self, v: Vec<u16>) -> Vec<(u16, anyhow::Result<()>)> {
            self.closed.lock().push(v.clone());
            let fail = self.fail_close.lock().clone();
            v.into_iter().map(|s| (s, if fail.contains(&s) { Err(anyhow::anyhow!("barrier timed out")) } else { Ok(()) })).collect()
        }
        fn lost(&self) {
            self.lost.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn cfg(id: &str) -> ClusterConfig {
        ClusterConfig {
            node_id: id.into(),
            addr: format!("http://{id}"),
            shards: 8,
            ttl: Duration::from_millis(600),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(100),
            clock_offset_ms: 0,
        }
    }

    fn skewed(id: &str, offset_ms: i64) -> ClusterConfig {
        ClusterConfig { clock_offset_ms: offset_ms, ..cfg(id) }
    }

    fn host() -> (Arc<Host>, Arc<dyn ShardHost>) {
        let h = Arc::new(Host::default());
        let d: Arc<dyn ShardHost> = h.clone();
        (h, d)
    }

    /// A real segment holding one entry at `seq` (fence/replay parse it).
    fn segment(log_id: &str, ord: u64, seq: i64) -> PutPayload {
        let mut b = crate::segment::SegmentBuilder::new();
        b.push(seq, 0, 1, |_| {}, &[]);
        let mut data = b.header(log_id, ord);
        data.extend_from_slice(&b.body);
        PutPayload::from(data)
    }

    #[tokio::test]
    async fn assignment_handoff_fencing() {
        let store = Store::memory(None);
        let a = Cluster::join(cfg("a"), store.clone()).await.unwrap();
        let (ha, ha_dyn) = host();
        a.step(&ha_dyn).await.unwrap();
        assert_eq!(a.owned().len(), 8);
        ha.ord.store(5, Ordering::SeqCst); // a wrote 5 segments
        ha.seq.store(1000, Ordering::SeqCst);

        let b = Cluster::join(cfg("b"), store.clone()).await.unwrap();
        assert_ne!(a.writer, b.writer, "writer ids unique among live nodes");
        let (hb, hb_dyn) = host();
        b.step(&hb_dyn).await.unwrap();
        assert!(b.owned().is_empty(), "join grace: no shards before peers can discover us");
        tokio::time::sleep(Duration::from_millis(250)).await;
        a.step(&ha_dyn).await.unwrap(); // a releases 4
        assert_eq!(ha.closed.lock().len(), 1, "the 4 extras are closed in one batch (one barrier segment)");
        b.step(&hb_dyn).await.unwrap(); // b takes them, with a's closed span as history
        assert_eq!((a.owned().len(), b.owned().len()), (4, 4));
        for (_, epoch, hist) in hb.opened.lock().iter() {
            assert_eq!(*epoch, 2);
            assert_eq!(hist.len(), 1);
            assert_eq!((hist[0].log_id.as_str(), hist[0].start, hist[0].end), (a.log_id.as_str(), 0, Some(5)));
        }
        assert_eq!(*hb.floors.lock(), vec![1000], "the releaser's seqs bound the new owner's");

        // a dies with a segment in flight; b fences a's log and takes over
        store.raw.put(&crate::nodelog::segment_path(&store, &a.log_id, 7), segment(&a.log_id, 7, 5000)).await.unwrap();
        // b keeps renewing (as its renew loop would) while a's lease goes
        // stale; it must not take a's shards before ttl + skew of b's time
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            b.step(&hb_dyn).await.unwrap();
        }
        assert_eq!(b.owned().len(), 4, "a presumed alive for ttl + skew after its lease last changed");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!a.lease_valid(), "a must stop acking after ttl - skew");
        b.step(&hb_dyn).await.unwrap();
        assert_eq!(b.owned().len(), 8, "b takes over all shards");
        let fence = *b.fenced_logs().get(&a.log_id).unwrap();
        assert_eq!(fence, 8, "fence lands after the highest existing segment");
        let last = hb.opened.lock().last().cloned().unwrap();
        assert_eq!(last.2.last().unwrap().end, Some(8), "dead span ends at the fence");
        assert_eq!(*hb.floors.lock().last().unwrap(), 5000, "a dead log's last seq bounds the new owner's");
        // a zombie write at the fence ordinal now collides
        let r = store
            .raw
            .put_opts(&crate::nodelog::segment_path(&store, &a.log_id, 8), PutPayload::from_static(b"zombie"), PutOptions { mode: PutMode::Create, ..Default::default() })
            .await;
        assert!(r.is_err(), "zombie append must fail");
        // and a stepping again (it has no valid lease) fail-stops: its
        // shards were reassigned under it
        a.step(&ha_dyn).await.unwrap();
        assert!(ha.lost.load(Ordering::SeqCst) > 0, "a zombie whose shards moved fail-stops");
    }

    /// A shard whose close failed (its barrier never became durable) is
    /// never released: entries of it may still be in flight past the span
    /// end we'd publish. The node fail-stops; a successor fences and replays.
    #[tokio::test]
    async fn failed_close_is_not_released() {
        let store = Store::memory(None);
        let a = Cluster::join(cfg("a"), store.clone()).await.unwrap();
        let (ha, ha_dyn) = host();
        a.step(&ha_dyn).await.unwrap();
        let b = Cluster::join(cfg("b"), store.clone()).await.unwrap();
        let (_hb, hb_dyn) = host();
        b.step(&hb_dyn).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        ha.fail_close.lock().insert(7); // a releases 7..4 (highest first)
        a.step(&ha_dyn).await.unwrap();
        assert_eq!(ha.lost.load(Ordering::SeqCst), 1, "fail-stop on a failed close");
        assert_eq!(a.owned(), vec![0, 1, 2, 3, 7]);
        let get = |s: u16| {
            let store = store.clone();
            async move {
                let r = store.raw.get(&Path::from(format!("{}/assign/{s:03}", store.prefix))).await.unwrap();
                serde_json::from_slice::<Assignment>(&r.bytes().await.unwrap()).unwrap()
            }
        };
        let s7 = get(7).await;
        assert_eq!(s7.owner.as_deref(), Some("a"), "not released");
        assert_eq!(s7.history.last().unwrap().end, None, "span left open: a successor ends it at the fence");
        assert_eq!(get(6).await.owner, None, "the closed ones are released");
    }

    /// Wall clocks minutes apart: liveness doesn't care (O3). With the old
    /// rule (peer live while its expires_ms > my now - skew) `slow` looked
    /// dead to `fast` immediately and was fenced over and over.
    #[tokio::test]
    async fn skewed_clocks_stay_live() {
        let store = Store::memory(None);
        let slow = Cluster::join(skewed("slow", -120_000), store.clone()).await.unwrap();
        let fast = Cluster::join(skewed("fast", 120_000), store.clone()).await.unwrap();
        let (hs, hs_dyn) = host();
        let (hf, hf_dyn) = host();
        for _ in 0..25 {
            slow.step(&hs_dyn).await.unwrap();
            fast.step(&hf_dyn).await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        assert_eq!((slow.owned().len(), fast.owned().len()), (4, 4));
        assert!(slow.fenced_logs().is_empty() && fast.fenced_logs().is_empty(), "nobody was fenced");
        assert_eq!((slow.peers().len(), fast.peers().len()), (1, 1));
        assert_eq!(hs.lost.load(Ordering::SeqCst) + hf.lost.load(Ordering::SeqCst), 0);
    }

    /// A dead node whose clock ran an hour ahead (its expires_ms is far in
    /// the future) is still taken over after ttl + skew of the observer's
    /// time, and a lease first seen gets a full ttl from first sight.
    #[tokio::test]
    async fn dead_peer_with_future_clock_is_taken_over() {
        let store = Store::memory(None);
        let a = Cluster::join(skewed("a", 3_600_000), store.clone()).await.unwrap();
        let (_ha, ha_dyn) = host();
        a.step(&ha_dyn).await.unwrap();
        assert_eq!(a.owned().len(), 8);
        // a dies now; c joins later and has never seen a's lease change
        tokio::time::sleep(Duration::from_millis(500)).await;
        let c = Cluster::join(cfg("c"), store.clone()).await.unwrap();
        let (hc, hc_dyn) = host();
        let first_seen = Instant::now();
        while c.owned().len() < 8 {
            c.step(&hc_dyn).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(first_seen.elapsed() < Duration::from_secs(3), "never took over");
        }
        assert!(first_seen.elapsed() >= Duration::from_millis(700), "full ttl + skew from first sight: {:?}", first_seen.elapsed());
        assert!(c.fenced_logs().contains_key(&a.log_id));
        assert_eq!(hc.lost.load(Ordering::SeqCst), 0);
    }

    /// Steady state reads only what changed: one LIST of leases, one of
    /// assignments, and a GET per peer renewal (O5).
    #[tokio::test]
    async fn steady_state_reads_are_cheap() {
        let store = Store::memory(None);
        let mut c = cfg("a");
        c.shards = 256;
        let a = Cluster::join(c.clone(), store.clone()).await.unwrap();
        let b = Cluster::join(ClusterConfig { node_id: "b".into(), ..c }, store.clone()).await.unwrap();
        let ((_, ha), (_, hb)) = (host(), host());
        for _ in 0..6 {
            a.step(&ha).await.unwrap();
            b.step(&hb).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert_eq!((a.owned().len(), b.owned().len()), (128, 128));
        // 10 more rounds (none a full resync), each node renewing in its step
        let before = a.store_requests();
        for _ in 0..10 {
            a.step(&ha).await.unwrap();
            b.step(&hb).await.unwrap();
        }
        let per_step = (a.store_requests() - before) as f64 / 10.0;
        // renew PUT + 2 LISTs + 1 GET (b's renewed lease)
        assert!(per_step <= 4.0, "{per_step} requests per step at 256 shards");
    }
}
