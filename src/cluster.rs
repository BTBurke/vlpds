//! Cluster control plane: node leases, writer ids, shard assignments and log
//! fencing (see DESIGN.md "HA" and "Planet scale"). A single node is a
//! one-node cluster.
//!
//! Objects (all CAS via ETag or If-None-Match):
//!   nodes/{node_id}     NodeLease {log_id, addr, writer, renewals, ..}  renewed by the node
//!   writers/{w:03}      WriterClaim {node_id, log_id, confirmed}  unique seq low byte among live nodes
//!   assign/{shard:03}   Assignment {owner, log_id, addr, epoch, seq_floor, history[Span]}
//!                       changes only when a shard moves: taken by CAS, or
//!                       handed by its owner straight to a joiner (see `Handoff`)
//!   log/{log}/{ord}.seg a fence object at a dead log's first hole closes it
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
    /// Our log's next ordinal as of this renewal. Ordinals only grow, so a
    /// peer handing us a shard starts our span here: a lower bound on our
    /// first entry for it.
    pub next_ordinal: u64,
    /// Set by a graceful shutdown before it hands its shards out: peers stop
    /// counting it toward fair shares and never hand it shards.
    pub draining: bool,
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

/// A shard handed straight to a joiner: the releaser closed it and CASed its
/// assignment to name the joiner (epoch + 1, the releaser's span closed at
/// its barrier, a new open span for the joiner), then POSTs this to the
/// joiner's /internal/v1/cluster/nudge. The joiner adopts it without a
/// control-plane read: replays `history` minus its own span and serves once
/// its seqs pass `seq_floor`. If the nudge is lost, the joiner's next step
/// finds the same assignment and adopts it then.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Handoff {
    pub shard: u16,
    pub assignment: Assignment,
    /// ETag of the handed assignment (our release CAS later starts from it).
    pub etag: Option<String>,
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
    /// POSTs /internal/v1/cluster/nudge to each `(addr, handoffs)`: the node
    /// adopts the shards handed to it, and steps at once instead of on its
    /// next tick. Best effort: a missed nudge costs a step interval.
    async fn nudge(&self, _nudges: Vec<(String, Vec<Handoff>)>) {}
}

/// What we last saw of a peer's lease, timed on our monotonic clock.
struct Seen {
    etag: Option<String>,
    lease: NodeLease,
    changed_at: Instant,
    /// When we first saw this incarnation (log_id) of the peer.
    first_seen: Instant,
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
    /// Wakes the step loop early (a peer released shards for us).
    nudged: tokio::sync::Notify,
    /// Shards peers handed us that we haven't adopted yet.
    handed: parking_lot::Mutex<Vec<Handoff>>,
    /// Highest epoch of each shard this incarnation has opened. An
    /// assignment naming us at a newer epoch was handed to us; at an epoch we
    /// already opened it is ours from before (e.g. a release CAS that failed)
    /// and must not be adopted again: its history minus our span would
    /// replay older owners' writes over ours.
    opened: RwLock<HashMap<u16, u64>>,
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
            lease: RwLock::new(NodeLease { node_id: cfg.node_id.clone(), log_id, addr: cfg.addr.clone(), writer: 0, expires_ms: 0, renewals: 0, next_ordinal: 0, draining: false }),
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
            nudged: tokio::sync::Notify::new(),
            handed: parking_lot::Mutex::new(Vec::new()),
            opened: RwLock::new(HashMap::new()),
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

    /// Closes a dead node's log: writes a fence object at the end of its
    /// durable prefix, its first ordinal that isn't a segment (with K PUTs in
    /// flight a crash can leave segments past a hole: those were never acked
    /// and the fence cuts them off). Returns that ordinal (the log's final
    /// end) and the last seq in the log before it.
    pub async fn fence(&self, log_id: &str) -> anyhow::Result<(u64, i64)> {
        if let Some(f) = self.fenced.read().get(log_id) {
            return Ok(*f);
        }
        loop {
            // HA fix: if a fence is already there (another survivor fenced
            // first), it is the log's end. Without this check every survivor
            // stacked another fence after it, so spans for the same dead log
            // ended at different ordinals. Fencers agree because the end is
            // the first non-segment ordinal, which never changes once fenced.
            self.count("list");
            let (next, fenced) = crate::nodelog::first_free(&self.store, log_id).await?;
            if !fenced {
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
            }
            let seq = self.last_seq_before(log_id, next).await?;
            self.fenced.write().insert(log_id.to_string(), (next, seq));
            tracing::info!(log_id, fence_ordinal = next, last_seq = seq, "fenced dead node's log");
            return Ok((next, seq));
        }
    }

    /// The last seq in `log_id` below ordinal `end` (0 for an empty log).
    /// Everything below a fence is a segment, so this reads one header.
    async fn last_seq_before(&self, log_id: &str, end: u64) -> anyhow::Result<i64> {
        let mut ord = end;
        while ord > 0 {
            ord -= 1;
            self.count("get");
            if let crate::nodelog::Head::Segment(h) = crate::nodelog::read_head(&self.store, log_id, ord).await? {
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
                // Step every tick, when a peer nudges us (it released shards
                // for us: take them now, not up to a tick later), and once
                // more as our join grace ends (a nudge during it is a no-op).
                let grace_end = tokio::time::Instant::from_std(me.joined_at + me.join_grace());
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = me.nudged.notified() => {
                        crate::metrics::CLUSTER_NUDGES.with_label_values(&["received"]).inc();
                    }
                    _ = tokio::time::sleep_until(grace_end), if tokio::time::Instant::now() < grace_end => {}
                }
                if let Err(e) = me.adopt_handed(&host).await {
                    tracing::warn!("adopting handed shards failed: {e:#}");
                }
                if let Err(e) = me.step_inner(&host, false).await {
                    tracing::warn!("cluster step failed: {e:#}");
                }
            }
        });
    }

    /// A peer handed us shards or released some: adopt / step now
    /// (coalesced; one queued while a step runs starts right after it).
    pub fn nudge(&self, handoffs: Vec<Handoff>) {
        self.handed.lock().extend(handoffs);
        self.nudged.notify_one();
    }

    /// Whether `a` names this incarnation at an epoch it hasn't opened yet:
    /// a peer handed the shard to us.
    fn handed_to_us(&self, shard: u16, a: &Assignment) -> bool {
        a.owner.as_deref() == Some(&self.cfg.node_id)
            && a.log_id.as_deref() == Some(&self.log_id)
            && !self.is_owner(shard)
            && self.opened.read().get(&shard).is_none_or(|&e| e < a.epoch)
    }

    /// Adopts the shards peers handed us in nudges: no control-plane read,
    /// the nudge carries the assignment the releaser wrote.
    async fn adopt_handed(&self, host: &Arc<dyn ShardHost>) -> anyhow::Result<()> {
        if self.handed.lock().is_empty() {
            return Ok(());
        }
        let _step = self.step_lock.lock().await;
        let handed = std::mem::take(&mut *self.handed.lock());
        // in our join grace (or without a lease) the step adopts them later
        if self.stopping.load(Ordering::Acquire) || !self.lease_valid() || self.joined_at.elapsed() < self.join_grace() {
            return Ok(());
        }
        let mut adopt = Vec::new();
        for h in handed {
            if h.shard >= self.cfg.shards || !self.handed_to_us(h.shard, &h.assignment) {
                continue;
            }
            self.assigns.write()[h.shard as usize] = Some((h.assignment.clone(), h.etag));
            adopt.push((h.shard, h.assignment));
        }
        self.adopt(host, adopt).await
    }

    /// Starts serving shards whose assignment already names us (handed by
    /// their previous owner): replay the history before our own span.
    async fn adopt(&self, host: &Arc<dyn ShardHost>, shards: Vec<(u16, Assignment)>) -> anyhow::Result<()> {
        if shards.is_empty() {
            return Ok(());
        }
        let mut floor = 0i64;
        let mut open = Vec::with_capacity(shards.len());
        for (s, a) in shards {
            self.owned.write().insert(s);
            floor = floor.max(a.seq_floor);
            let mut history = a.history;
            history.pop(); // our own, open span
            open.push((s, a.epoch, history));
        }
        tracing::info!(shards = ?open.iter().map(|a| a.0).collect::<Vec<_>>(), owned = self.owned().len(), "adopting shards handed to us");
        host.wait_seq_floor(floor).await;
        self.open_acquired(host, open).await
    }

    /// Opens shards we now own and routes them to us; one that fails to open
    /// is released (nothing was logged for it).
    async fn open_acquired(&self, host: &Arc<dyn ShardHost>, shards: Vec<(u16, u64, Vec<Span>)>) -> anyhow::Result<()> {
        {
            let mut opened = self.opened.write();
            for (s, epoch, _) in &shards {
                opened.insert(*s, *epoch);
            }
        }
        for (s, res) in host.open_many(shards).await {
            match res {
                Ok(()) => {
                    self.table.write()[s as usize] = Some((self.cfg.node_id.clone(), self.cfg.addr.clone()));
                }
                Err(e) => {
                    // nothing was logged for it: release with an empty span
                    tracing::error!(shard = s, "open failed: {e:#}; releasing");
                    self.owned.write().remove(&s);
                    self.release(s, host.next_ordinal(), host.seq_high(), None).await?;
                }
            }
        }
        Ok(())
    }

    /// After joining, give every peer a membership refresh to discover us
    /// (and start following our log) before we produce events, so no peer's
    /// merged firehose has already moved past our first seqs.
    fn join_grace(&self) -> Duration {
        self.cfg.renew_every * 2
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
        self.lease.write().next_ordinal = host.next_ordinal();
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
                    let same = seen.get(&id).filter(|s| s.lease.log_id == lease.log_id);
                    let first_seen = same.map_or(now, |s| s.first_seen);
                    let changed_at = same.filter(|s| s.lease.renewals == lease.renewals).map_or(now, |s| s.changed_at);
                    seen.insert(id, Seen { etag, lease, changed_at, first_seen });
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
        use futures::StreamExt;
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
        let fair = (n as usize).div_ceil(live.iter().filter(|l| !l.draining).count().max(1));
        // Join grace (see `join_grace`).
        let has_peers = live.iter().any(|l| l.node_id != self.cfg.node_id);
        if has_peers && self.joined_at.elapsed() < self.join_grace() {
            return Ok(());
        }
        // HA fix: never take (or juggle) shards without a valid lease, e.g. a
        // zombie that woke after its peers fenced it, or while renewals fail.
        if !self.lease_valid() {
            return Ok(());
        }
        // 4a. shards a peer handed us whose nudge we missed
        let handed: Vec<(u16, Assignment)> = (0..n)
            .filter_map(|s| assigns[s as usize].as_ref().filter(|(a, _)| self.handed_to_us(s, a)).map(|(a, _)| (s, a.clone())))
            .collect();
        self.adopt(host, handed).await?;
        let owned = self.owned();
        // 4b. acquire free / orphaned shards up to our fair share
        if owned.len() < fair {
            let want = fair - owned.len();
            // pick them (fencing an orphan's log first so its span end is final)
            let mut picked = Vec::new();
            for s in 0..n {
                if picked.len() == want {
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
                picked.push((s, newa, mode, history));
            }
            // CAS them concurrently: a handback of ~85 shards is one round
            // trip, not one per shard (~2 s at 25 ms PUTs, bench 2026-10-02 §2)
            let cas: Vec<_> = futures::stream::iter(picked)
                .map(|(s, newa, mode, history)| async move {
                    let r = self.put_json(&self.path(&format!("assign/{s:03}")), &newa, mode).await;
                    (s, newa, history, r)
                })
                .buffer_unordered(32)
                .collect()
                .await;
            let (mut acquired, mut floor, mut failed) = (Vec::new(), 0i64, None);
            for (s, newa, history, r) in cas {
                match r {
                    Ok(e) => {
                        floor = floor.max(newa.seq_floor);
                        acquired.push((s, newa.epoch, history));
                        self.assigns.write()[s as usize] = Some((newa, e));
                        self.owned.write().insert(s);
                    }
                    Err(e) if is_conflict(&e) => {
                        // someone else moved it: re-read it next step
                        if let Some((_, etag)) = self.assigns.write()[s as usize].as_mut() {
                            *etag = None;
                        }
                    }
                    Err(e) => failed = Some(e),
                }
            }
            acquired.sort_by_key(|a| a.0);
            if !acquired.is_empty() {
                tracing::info!(shards = ?acquired.iter().map(|a| a.0).collect::<Vec<_>>(), owned = self.owned().len(), fair, live = live.len(), "acquired shards");
                host.wait_seq_floor(floor).await;
            }
            self.open_acquired(host, acquired).await?;
            if let Some(e) = failed {
                return Err(e.into());
            }
        } else {
            // 5. hand extras straight to the peers short of their share. A
            //    peer counts only once it is past its join grace (as we time
            //    it): it can't adopt anything before that.
            let settled = self.settled_peers();
            let keep = (n as usize).div_ceil(settled.len() + 1);
            if owned.len() > keep {
                tracing::info!(owned = owned.len(), keep, live = live.len(), "handing back extra shards");
                let extras: Vec<u16> = owned.iter().rev().take(owned.len() - keep).copied().collect();
                let to = self.short_of(&settled, fair);
                if !self.close_and_release(host, extras, to).await {
                    return Ok(());
                }
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

    /// Live peers we've seen for at least a join grace (past theirs).
    fn settled_peers(&self) -> Vec<NodeLease> {
        let seen = self.seen.read();
        self.peers
            .read()
            .values()
            .filter(|l| !l.draining && seen.get(&l.node_id).is_some_and(|s| s.first_seen.elapsed() >= self.join_grace()))
            .cloned()
            .collect()
    }

    /// `peers` owning fewer than `share` shards (per our assignment cache),
    /// each with how many it is short.
    fn short_of(&self, peers: &[NodeLease], share: usize) -> Vec<(NodeLease, usize)> {
        let mut count: HashMap<&str, usize> = HashMap::new();
        let assigns = self.assigns.read();
        for (a, _) in assigns.iter().flatten() {
            if let Some(o) = a.owner.as_deref() {
                *count.entry(o).or_default() += 1;
            }
        }
        peers
            .iter()
            .filter_map(|l| {
                let have = count.get(l.node_id.as_str()).copied().unwrap_or(0);
                (have < share).then(|| (l.clone(), share - have))
            })
            .collect()
    }

    /// Closes `shards` together (one barrier segment for all of them) and
    /// releases each one whose close succeeded: handed straight to a peer in
    /// `to` while it is short (up to its count), else unowned. Then nudges
    /// every peer: those in `to` with their handoffs, so they serve the
    /// shards at once; the rest so their routing follows.
    /// A failed close means entries of that shard may still be in flight
    /// past the span end we would publish, so it is never released: we
    /// fail-stop instead, and a successor fences our log and replays it to
    /// the fence. False if we fail-stopped.
    async fn close_and_release(&self, host: &Arc<dyn ShardHost>, shards: Vec<u16>, mut to: Vec<(NodeLease, usize)>) -> bool {
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
        // round-robin over the peers still short, so each gets a fair slice
        let mut plan = Vec::with_capacity(done.len());
        let mut i = 0;
        for s in done {
            let mut dest = None;
            for _ in 0..to.len() {
                let k = i % to.len();
                i += 1;
                if to[k].1 > 0 {
                    to[k].1 -= 1;
                    dest = Some(to[k].0.clone());
                    break;
                }
            }
            plan.push((s, dest));
        }
        let released: Vec<(Option<NodeLease>, anyhow::Result<Option<Handoff>>)> = futures::stream::iter(plan)
            .map(|(s, dest)| async move {
                let r = self.release(s, end, floor, dest.as_ref()).await;
                (dest, r)
            })
            .buffer_unordered(32)
            .collect()
            .await;
        // every peer: recipients adopt, the rest refresh their routing now
        // (a stale route forwards to us, and we no longer own the shard)
        let mut nudges: HashMap<String, Vec<Handoff>> = self.peers.read().values().map(|l| (l.addr.clone(), Vec::new())).collect();
        for (dest, r) in released {
            match r {
                Ok(Some(h)) => nudges.entry(dest.map(|l| l.addr).unwrap_or_default()).or_default().push(h),
                Ok(None) => {}
                // the assignment still names us with an open span: whoever
                // takes the shard once our lease lapses fences and replays
                Err(e) => tracing::warn!("release failed: {e:#}"),
            }
        }
        let handed: usize = nudges.values().map(|v| v.len()).sum();
        tracing::info!(shards = n, handed, elapsed_ms = started.elapsed().as_millis() as u64, "closed and released shards");
        // wake them instead of leaving the shards idle (and misrouted) until
        // their next step
        let nudges: Vec<(String, Vec<Handoff>)> = nudges.into_iter().filter(|(a, _)| !a.is_empty()).collect();
        crate::metrics::CLUSTER_NUDGES.with_label_values(&["sent"]).inc_by(nudges.len() as u64);
        host.nudge(nudges).await;
        if !ok {
            tracing::error!("a shard failed to close cleanly: fail-stop (a successor fences and replays our log)");
            host.lost();
        }
        ok
    }

    /// Releases `shard`, closing our span at `end` (exclusive) and raising its
    /// seq floor to `seq_floor`: unowned, or handed to `to` (the next epoch,
    /// with an open span for it starting at the log ordinal its lease last
    /// published). CAS against the cached assignment; re-read once on a
    /// conflict. Returns the handoff for `to`.
    async fn release(&self, shard: u16, end: u64, seq_floor: i64, to: Option<&NodeLease>) -> anyhow::Result<Option<Handoff>> {
        let path = self.path(&format!("assign/{shard:03}"));
        let mut cur = self.assigns.read()[shard as usize].clone().filter(|(_, e)| e.is_some());
        for attempt in 0..2 {
            let (mut a, etag) = match cur.take() {
                Some(c) => c,
                None => match self.get_json::<Assignment>(&path).await? {
                    Some(c) => c,
                    None => return Ok(None),
                },
            };
            if a.owner.as_deref() != Some(&self.cfg.node_id) {
                return Ok(None);
            }
            if let Some(last) = a.history.last_mut() {
                if last.log_id == self.log_id && last.end.is_none() {
                    last.end = Some(end);
                }
            }
            a.seq_floor = a.seq_floor.max(seq_floor);
            match to {
                None => {
                    a.owner = None;
                    a.addr = None;
                    a.log_id = None;
                }
                Some(l) => {
                    // Its span starts at the ordinal its lease published (a
                    // lower bound: ordinals only grow), and never inside an
                    // earlier span of the same log: replay markers must map
                    // to one span.
                    let start = a.history.iter().filter(|sp| sp.log_id == l.log_id).filter_map(|sp| sp.end).fold(l.next_ordinal, u64::max);
                    a.epoch += 1;
                    a.owner = Some(l.node_id.clone());
                    a.addr = Some(l.addr.clone());
                    a.log_id = Some(l.log_id.clone());
                    a.history.push(Span { log_id: l.log_id.clone(), epoch: a.epoch, start, end: None });
                    if a.history.len() > 16 {
                        a.history.drain(..a.history.len() - 16);
                    }
                }
            }
            match self.put_json(&path, &a, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
                Ok(e) => {
                    self.assigns.write()[shard as usize] = Some((a.clone(), e.clone()));
                    self.table.write()[shard as usize] = to.map(|l| (l.node_id.clone(), l.addr.clone()));
                    return Ok(to.map(|_| Handoff { shard, assignment: a, etag: e }));
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
        // Announce the drain first: otherwise a peer stepping meanwhile still
        // counts us toward the fair share and hands shards back to us, which
        // we'd never adopt (they'd wait out our lease and a fence).
        self.lease.write().draining = true;
        self.renew(host).await;
        // hand our shards straight to the settled peers, evenly
        let settled = self.settled_peers();
        let to = self.short_of(&settled, (self.cfg.shards as usize).div_ceil(settled.len().max(1)));
        // plus any a peer handed us before it saw the drain (never opened:
        // closing them is a no-op, so they are just handed on)
        let mut shards = self.owned();
        let pending = std::mem::take(&mut *self.handed.lock());
        for h in pending {
            if h.shard < self.cfg.shards && self.handed_to_us(h.shard, &h.assignment) {
                self.assigns.write()[h.shard as usize] = Some((h.assignment, h.etag));
            }
        }
        let assigns = self.assigns.read().clone();
        shards.extend((0..self.cfg.shards).filter(|&s| assigns[s as usize].as_ref().is_some_and(|(a, _)| self.handed_to_us(s, a))));
        if !self.close_and_release(host, shards, to).await {
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
        // Our lease is gone, so peers' next step counts us out and takes
        // whatever we didn't hand them: run it now, not a step interval later.
        let nudges: Vec<(String, Vec<Handoff>)> = self.peers().into_iter().map(|l| (l.addr, Vec::new())).collect();
        crate::metrics::CLUSTER_NUDGES.with_label_values(&["sent"]).inc_by(nudges.len() as u64);
        host.nudge(nudges).await;
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
        nudged: Mutex<Vec<(String, Vec<Handoff>)>>,
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
        async fn nudge(&self, nudges: Vec<(String, Vec<Handoff>)>) {
            self.nudged.lock().extend(nudges);
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

    /// With K PUTs in flight a crash leaves holes: 0..=2 durable, 3 never
    /// landed, 4 and 5 (sealed while 3 was in flight) did. The fence goes at
    /// the hole, every fencer agrees on it, and the garbage stays cut off.
    #[tokio::test]
    async fn fence_lands_at_the_first_hole() {
        let store = Store::memory(None);
        let log = "dead.1";
        for ord in 0..3 {
            store.raw.put(&crate::nodelog::segment_path(&store, log, ord), segment(log, ord, 100 + ord as i64)).await.unwrap();
        }
        for ord in 4..6u64 {
            let mut b = crate::segment::SegmentBuilder::new();
            b.push(200 + ord as i64, 0, 1, |_| {}, &[]);
            let mut data = b.sealed_header(log, ord, 3);
            data.extend_from_slice(&b.body);
            store.raw.put(&crate::nodelog::segment_path(&store, log, ord), PutPayload::from(data)).await.unwrap();
        }
        let a = Cluster::join(cfg("a"), store.clone()).await.unwrap();
        assert_eq!(a.fence(log).await.unwrap(), (3, 102), "fence at the hole; last seq from the durable prefix");
        let b = Cluster::join(cfg("b"), store.clone()).await.unwrap();
        assert_eq!(b.fence(log).await.unwrap(), (3, 102), "a second fencer finds the same end instead of stacking one");
        let r = store
            .raw
            .put_opts(&crate::nodelog::segment_path(&store, log, 3), segment(log, 3, 103), PutOptions { mode: PutMode::Create, ..Default::default() })
            .await;
        assert!(r.is_err(), "the zombie's in-flight segment collides with the fence");
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
        a.step(&ha_dyn).await.unwrap(); // a first sees b
        assert!(ha.closed.lock().is_empty(), "a keeps its shards while b is in its join grace");
        tokio::time::sleep(Duration::from_millis(250)).await;
        a.step(&ha_dyn).await.unwrap(); // a releases 4
        assert_eq!(ha.closed.lock().len(), 1, "the 4 extras are closed in one batch (one barrier segment)");
        let nudged = ha.nudged.lock().clone();
        assert_eq!(nudged.len(), 1, "one nudge, to the node short of its share");
        assert_eq!((nudged[0].0.as_str(), nudged[0].1.len()), ("http://b", 4), "carrying the 4 handoffs");
        for h in &nudged[0].1 {
            let a = &h.assignment;
            assert_eq!((a.owner.as_deref(), a.log_id.as_deref(), a.epoch), (Some("b"), Some(b.log_id.as_str()), 2), "handed straight to b");
            assert_eq!(a.history.len(), 2);
            assert_eq!(a.history[1], Span { log_id: b.log_id.clone(), epoch: 2, start: 0, end: None }, "b's span opens at b's published ordinal");
        }
        // b adopts them from the nudge, with no control-plane read
        let before = b.store_requests();
        b.nudge(nudged[0].1.clone());
        b.adopt_handed(&hb_dyn).await.unwrap();
        assert_eq!(b.store_requests(), before, "adopting a handoff reads nothing");
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

    /// A handoff whose nudge never arrived is adopted by the joiner's next
    /// step; a shard still naming us at an epoch we already opened (our
    /// release CAS failed) is never adopted again.
    #[tokio::test]
    async fn missed_nudge_is_adopted_by_the_next_step_once() {
        let store = Store::memory(None);
        let a = Cluster::join(cfg("a"), store.clone()).await.unwrap();
        let (ha, ha_dyn) = host();
        a.step(&ha_dyn).await.unwrap();
        ha.ord.store(3, Ordering::SeqCst);
        let b = Cluster::join(cfg("b"), store.clone()).await.unwrap();
        let (hb, hb_dyn) = host();
        hb.ord.store(7, Ordering::SeqCst);
        b.step(&hb_dyn).await.unwrap(); // renews: publishes ordinal 7
        a.step(&ha_dyn).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        a.step(&ha_dyn).await.unwrap(); // hands 4 to b; the nudge is dropped
        assert_eq!(ha.nudged.lock()[0].1.len(), 4);
        b.step(&hb_dyn).await.unwrap();
        assert_eq!(b.owned(), vec![4, 5, 6, 7]);
        for (_, epoch, hist) in hb.opened.lock().iter() {
            assert_eq!((*epoch, hist.len(), hist[0].end), (2, 1, Some(3)), "replays a's closed span only");
        }
        // b's release of 7 "fails" (the assignment still names b): b must not
        // reopen it with a's span as its history
        b.owned.write().remove(&7);
        b.step(&hb_dyn).await.unwrap();
        assert_eq!(hb.opened.lock().len(), 4, "not adopted twice");
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
        a.step(&ha_dyn).await.unwrap(); // a first sees b
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
        assert_eq!(get(6).await.owner.as_deref(), Some("b"), "the closed ones are handed to b");
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
        for _ in 0..10 {
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
