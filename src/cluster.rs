//! Cluster control plane: node leases, writer ids, shard assignments and log
//! fencing (see DESIGN.md "Planet scale"). A single node is a one-node cluster.
//!
//! Objects (all CAS via ETag or If-None-Match):
//!   nodes/{node_id}     NodeLease {log_id, addr, writer, expires_ms}  renewed by the node
//!   writers/{w:03}      WriterClaim {node_id}   unique seq low byte among live nodes
//!   assign/{shard:03}   Assignment {owner, log_id, addr, epoch, history[Span]}
//!                       changes only when a shard moves
//!   log/{log}/{ord}.seg a fence object at a dead log's next ordinal closes it
//!
//! Safety:
//! - A node acks/PUTs only while its node lease is valid (measured locally
//!   from before the renewing PUT, minus skew).
//! - A dead node's log is fenced before its shards are reassigned, so the
//!   span end used for replay is final and a zombie can never extend it.
//! - A new owner replays its shards' previous spans before serving them.

use crate::nodelog::Span;
use crate::store::Store;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeLease {
    pub node_id: String,
    pub log_id: String,
    pub addr: String,
    pub writer: u8,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Assignment {
    pub owner: Option<String>,
    pub log_id: Option<String>,
    pub addr: Option<String>,
    pub epoch: u64,
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
    /// Replay each shard's `history` (previous owners' spans) and start
    /// serving them. Batched so a takeover reads a dead log once for all shards.
    async fn open_many(&self, shards: Vec<(u16, u64, Vec<Span>)>) -> Vec<(u16, anyhow::Result<()>)>;
    /// Stop accepting writes, drain, checkpoint, close.
    async fn close(&self, shard: u16) -> anyhow::Result<()>;
    /// Our node lease was lost (CAS failed): must stop acking immediately.
    fn lost(&self);
    /// Called after every membership refresh (e.g. to follow peers' logs).
    fn on_membership(&self) {}
}

pub struct Cluster {
    pub cfg: ClusterConfig,
    pub log_id: String,
    pub writer: u8,
    store: Store,
    lease_etag: RwLock<Option<String>>,
    lease: RwLock<NodeLease>,
    valid_until: RwLock<Instant>,
    /// Routing table: shard -> (owner node, addr)
    table: RwLock<Vec<Option<(String, String)>>>,
    owned: RwLock<HashSet<u16>>,
    /// Live peers (node_id -> lease), refreshed every step.
    peers: RwLock<HashMap<String, NodeLease>>,
    /// Dead logs we know are fenced: log_id -> fence ordinal.
    fenced: RwLock<HashMap<String, u64>>,
    joined_at: Instant,
    /// Set by shutdown(); held across each step so a step can't re-acquire
    /// shards while (or after) shutdown releases them.
    stopping: std::sync::atomic::AtomicBool,
    step_lock: tokio::sync::Mutex<()>,
    /// Set once shutdown is about to delete our lease: the renew loop and the
    /// watchdog stop (they keep running during the shutdown drain itself).
    gone: std::sync::atomic::AtomicBool,
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
        let c = Cluster {
            log_id: log_id.clone(),
            writer: 0,
            store,
            lease_etag: RwLock::new(None),
            lease: RwLock::new(NodeLease { node_id: cfg.node_id.clone(), log_id, addr: cfg.addr.clone(), writer: 0, expires_ms: 0 }),
            valid_until: RwLock::new(Instant::now()),
            table: RwLock::new(vec![None; cfg.shards as usize]),
            owned: RwLock::new(HashSet::new()),
            peers: RwLock::new(HashMap::new()),
            fenced: RwLock::new(HashMap::new()),
            joined_at: Instant::now(),
            stopping: std::sync::atomic::AtomicBool::new(false),
            step_lock: tokio::sync::Mutex::new(()),
            gone: std::sync::atomic::AtomicBool::new(false),
            renew_lock: tokio::sync::Mutex::new(()),
            cfg,
        };
        let writer = c.claim_writer().await?;
        let mut c = c;
        c.writer = writer;
        c.lease.write().writer = writer;
        // create (or take over our own stale) node lease
        let path = c.path(&format!("nodes/{}", c.cfg.node_id));
        let existing = c.get_json::<NodeLease>(&path).await?;
        let mode = match existing {
            None => PutMode::Create,
            Some((l, etag)) => {
                // Our previous incarnation (same node id). Fence its log now:
                // if it is somehow still running, its next PUT collides and it
                // fail-stops; everything it acked is before the fence and gets
                // replayed by whoever takes its shards (us).
                if l.log_id != c.log_id {
                    c.fence(&l.log_id).await?;
                }
                PutMode::Update(UpdateVersion { e_tag: etag, version: None })
            }
        };
        c.write_lease(mode).await?;
        Ok(Arc::new(c))
    }

    fn path(&self, rel: &str) -> Path {
        Path::from(format!("{}/{}", self.store.prefix, rel))
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> anyhow::Result<Option<(T, Option<String>)>> {
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
        let body = PutPayload::from(serde_json::to_vec(v).unwrap());
        self.store.raw.put_opts(path, body, PutOptions { mode, ..Default::default() }).await.map(|r| r.e_tag)
    }

    async fn claim_writer(&self) -> anyhow::Result<u8> {
        let start = (crate::state::did_hash(&self.cfg.node_id) % 256) as u16;
        for i in 0..256u16 {
            let w = ((start + i) % 256) as u8;
            let path = self.path(&format!("writers/{w:03}"));
            let claim = serde_json::json!({"node_id": self.cfg.node_id});
            match self.get_json::<serde_json::Value>(&path).await? {
                None => {
                    if self.put_json(&path, &claim, PutMode::Create).await.is_ok() {
                        return Ok(w);
                    }
                }
                Some((v, etag)) => {
                    let holder = v["node_id"].as_str().unwrap_or_default().to_string();
                    let holder_alive = holder != self.cfg.node_id
                        && self
                            .get_json::<NodeLease>(&self.path(&format!("nodes/{holder}")))
                            .await?
                            .is_some_and(|(l, _)| now_ms() <= l.expires_ms + self.cfg.skew.as_millis() as u64);
                    if !holder_alive
                        && (holder == self.cfg.node_id
                            || self.put_json(&path, &claim, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await.is_ok())
                    {
                        return Ok(w);
                    }
                }
            }
        }
        anyhow::bail!("no free writer id (256 live nodes?)")
    }

    async fn write_lease(&self, mode: PutMode) -> anyhow::Result<()> {
        let sent = Instant::now();
        let mut l = self.lease.read().clone();
        l.expires_ms = now_ms() + self.cfg.ttl.as_millis() as u64;
        let etag = self.put_json(&self.path(&format!("nodes/{}", self.cfg.node_id)), &l, mode).await?;
        *self.lease_etag.write() = etag;
        *self.lease.write() = l;
        *self.valid_until.write() = sent + self.cfg.ttl - self.cfg.skew;
        Ok(())
    }

    /// True while we may acknowledge writes / PUT segments.
    pub fn lease_valid(&self) -> bool {
        Instant::now() < *self.valid_until.read()
    }

    pub fn lease_expiry_us(&self) -> u64 {
        self.lease.read().expires_ms * 1000
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
        self.fenced.read().clone()
    }

    /// Closes a dead node's log: writes a fence object at its first free
    /// ordinal. Returns that ordinal (the log's final end).
    pub async fn fence(&self, log_id: &str) -> anyhow::Result<u64> {
        if let Some(o) = self.fenced.read().get(log_id) {
            return Ok(*o);
        }
        use futures::StreamExt;
        loop {
            let prefix = self.path(&format!("log/{log_id}"));
            let mut next = 0u64;
            let mut list = self.store.raw.list(Some(&prefix));
            while let Some(m) = list.next().await {
                let m = m?;
                if let Some(ord) = m.location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse::<u64>().ok()) {
                    next = next.max(ord + 1);
                }
            }
            // HA fix: if the last object is already a fence (another survivor
            // fenced first), that ordinal is the log's end. Without this check
            // every survivor stacked another fence after it, so spans for the same
            // dead log ended at different ordinals.
            if next > 0 {
                let last = crate::nodelog::segment_path(&self.store, log_id, next - 1);
                if let Ok(r) = self.store.raw.get(&last).await {
                    if matches!(crate::segment::parse(r.bytes().await?, false, None), Ok(crate::segment::LogObject::Fence { .. })) {
                        self.fenced.write().insert(log_id.to_string(), next - 1);
                        return Ok(next - 1);
                    }
                }
            }
            let path = crate::nodelog::segment_path(&self.store, log_id, next);
            match self.store.raw.put_opts(&path, PutPayload::from_bytes(crate::segment::fence_object(&self.cfg.node_id)), PutOptions { mode: PutMode::Create, ..Default::default() }).await {
                Ok(_) => {}
                Err(e) if is_conflict(&e) => {
                    // a zombie got a segment in, or another node fenced first
                    let b = self.store.raw.get(&path).await?.bytes().await?;
                    if !matches!(crate::segment::parse(b, false, None)?, crate::segment::LogObject::Fence { .. }) {
                        continue; // re-scan: the log grew
                    }
                }
                Err(e) => return Err(e.into()),
            }
            self.fenced.write().insert(log_id.to_string(), next);
            tracing::info!(log_id, fence_ordinal = next, "fenced dead node's log");
            return Ok(next);
        }
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
                if me.gone.load(std::sync::atomic::Ordering::Acquire) {
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
        // log. Once our lease is invalid for longer than peers need to declare
        // us dead (2 x skew), we can never ack again, so fail-stop.
        let me = self.clone();
        let h = host.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.renew_every / 2);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if me.gone.load(std::sync::atomic::Ordering::Acquire) {
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
        if self.gone.load(std::sync::atomic::Ordering::Acquire) {
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

    async fn read_nodes(&self) -> anyhow::Result<(Vec<NodeLease>, Vec<NodeLease>)> {
        use futures::StreamExt;
        let mut names = Vec::new();
        let mut list = self.store.raw.list(Some(&self.path("nodes")));
        while let Some(m) = list.next().await {
            names.push(m?.location);
        }
        let (mut live, mut dead) = (Vec::new(), Vec::new());
        for p in names {
            if let Some((l, _)) = self.get_json::<NodeLease>(&p).await? {
                if now_ms() <= l.expires_ms + self.cfg.skew.as_millis() as u64 {
                    live.push(l);
                } else {
                    dead.push(l);
                }
            }
        }
        Ok((live, dead))
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
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
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
        // HA fix: fetch assignments concurrently (was one GET at a time).
        let assigns: Vec<Option<(Assignment, Option<String>)>> = {
            use futures::StreamExt;
            futures::stream::iter(0..n)
                .map(|s| async move { self.get_json::<Assignment>(&self.path(&format!("assign/{s:03}"))).await })
                .buffered(32)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<anyhow::Result<_>>()?
        };
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
                let stale_self = cur.owner.as_deref() == Some(&self.cfg.node_id) && cur.log_id.as_deref() != Some(&self.log_id);
                match &cur.owner {
                    Some(o) if live_ids.contains(o) && !stale_self => continue, // healthy owner
                    Some(o) => {
                        // orphaned (dead owner, or our own previous incarnation):
                        // fence its log first so the span end is final
                        let Some(log) = cur.log_id.clone().or_else(|| dead_logs.get(o).cloned()) else { continue };
                        let end = self.fence(&log).await?;
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
                    history: next,
                };
                let mode = match etag {
                    None => PutMode::Create,
                    Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
                };
                match self.put_json(&self.path(&format!("assign/{s:03}")), &newa, mode).await {
                    Ok(_) => {}
                    Err(e) if is_conflict(&e) => continue,
                    Err(e) => return Err(e.into()),
                }
                self.owned.write().insert(s);
                acquired.push((s, epoch, history));
                want -= 1;
            }
            if !acquired.is_empty() {
                tracing::info!(shards = ?acquired.iter().map(|a| a.0).collect::<Vec<_>>(), owned = self.owned().len(), fair, live = live.len(), "acquired shards");
            }
            for (s, res) in host.open_many(acquired).await {
                match res {
                    Ok(()) => {
                        self.table.write()[s as usize] = Some((self.cfg.node_id.clone(), self.cfg.addr.clone()));
                    }
                    Err(e) => {
                        tracing::error!(shard = s, "open failed: {e:#}; releasing");
                        self.owned.write().remove(&s);
                        self.release(s, host.next_ordinal()).await?;
                    }
                }
            }
        } else if owned.len() > fair {
            // 5. hand back extras so newcomers get work
            tracing::info!(owned = owned.len(), fair, live = live.len(), "releasing extra shards");
            for s in owned.into_iter().rev().take(self.owned().len() - fair) {
                host.close(s).await?;
                self.owned.write().remove(&s);
                self.release(s, host.durable_end()).await?;
            }
        }
        // 6. dead nodes whose shards have all moved can be forgotten
        for d in &dead {
            if !assigns.iter().any(|a| a.as_ref().is_some_and(|(a, _)| a.owner.as_deref() == Some(&d.node_id))) && self.fenced.read().contains_key(&d.log_id) {
                let _ = self.store.raw.delete(&self.path(&format!("nodes/{}", d.node_id))).await;
            }
        }
        Ok(())
    }

    /// Releases `shard`, closing our span at `end` (exclusive).
    async fn release(&self, shard: u16, end: u64) -> anyhow::Result<()> {
        let path = self.path(&format!("assign/{shard:03}"));
        let Some((mut a, etag)) = self.get_json::<Assignment>(&path).await? else { return Ok(()) };
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
        self.put_json(&path, &a, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await?;
        self.table.write()[shard as usize] = None;
        Ok(())
    }

    /// Graceful shutdown: close and release every shard, drop our lease.
    pub async fn shutdown(&self, host: &Arc<dyn ShardHost>) {
        // HA fix: stop the step loop first (it would re-acquire the shards we
        // are releasing), and wait out a step already in flight.
        self.stopping.store(true, std::sync::atomic::Ordering::Release);
        let _step = self.step_lock.lock().await;
        for s in self.owned() {
            let _ = host.close(s).await;
            self.owned.write().remove(&s);
            let _ = self.release(s, host.durable_end()).await;
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
            self.gone.store(true, std::sync::atomic::Ordering::Release);
        }
        let _ = self.store.raw.delete(&self.path(&format!("nodes/{}", self.cfg.node_id))).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct Host {
        opened: Mutex<Vec<(u16, u64, Vec<Span>)>>,
        closed: Mutex<Vec<u16>>,
        ord: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl ShardHost for Host {
        fn next_ordinal(&self) -> u64 {
            self.ord.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn durable_end(&self) -> u64 {
            self.ord.load(std::sync::atomic::Ordering::SeqCst)
        }
        async fn open_many(&self, v: Vec<(u16, u64, Vec<Span>)>) -> Vec<(u16, anyhow::Result<()>)> {
            v.into_iter()
                .map(|(s, e, h)| {
                    self.opened.lock().push((s, e, h));
                    (s, Ok(()))
                })
                .collect()
        }
        async fn close(&self, s: u16) -> anyhow::Result<()> {
            self.closed.lock().push(s);
            Ok(())
        }
        fn lost(&self) {}
    }

    fn cfg(id: &str) -> ClusterConfig {
        ClusterConfig {
            node_id: id.into(),
            addr: format!("http://{id}"),
            shards: 8,
            ttl: Duration::from_millis(600),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(100),
        }
    }

    #[tokio::test]
    async fn assignment_handoff_fencing() {
        let store = Store::memory(None);
        let a = Cluster::join(cfg("a"), store.clone()).await.unwrap();
        let ha = Arc::new(Host::default());
        let ha_dyn: Arc<dyn ShardHost> = ha.clone();
        a.step(&ha_dyn).await.unwrap();
        assert_eq!(a.owned().len(), 8);
        ha.ord.store(5, std::sync::atomic::Ordering::SeqCst); // a wrote 5 segments

        let b = Cluster::join(cfg("b"), store.clone()).await.unwrap();
        assert_ne!(a.writer, b.writer, "writer ids unique among live nodes");
        let hb = Arc::new(Host::default());
        let hb_dyn: Arc<dyn ShardHost> = hb.clone();
        b.step(&hb_dyn).await.unwrap();
        assert!(b.owned().is_empty(), "join grace: no shards before peers can discover us");
        tokio::time::sleep(Duration::from_millis(250)).await;
        a.step(&ha_dyn).await.unwrap(); // a releases 4
        b.step(&hb_dyn).await.unwrap(); // b takes them, with a's closed span as history
        assert_eq!((a.owned().len(), b.owned().len()), (4, 4));
        for (_, epoch, hist) in hb.opened.lock().iter() {
            assert_eq!(*epoch, 2);
            assert_eq!(hist.len(), 1);
            assert_eq!((hist[0].log_id.as_str(), hist[0].start, hist[0].end), (a.log_id.as_str(), 0, Some(5)));
        }

        // a dies with a segment in flight; b fences a's log and takes over
        store
            .raw
            .put(&crate::nodelog::segment_path(&store, &a.log_id, 7), PutPayload::from_static(b"seg"))
            .await
            .unwrap();
        // b keeps renewing (as its renew loop would) while a's lease runs out;
        // it must not take a's shards before a is dead (ttl + skew)
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            b.step(&hb_dyn).await.unwrap();
        }
        assert!(!a.lease_valid(), "a must stop acking after ttl - skew");
        b.step(&hb_dyn).await.unwrap();
        assert_eq!(b.owned().len(), 8, "b takes over all shards");
        let fence = *b.fenced_logs().get(&a.log_id).unwrap();
        assert_eq!(fence, 8, "fence lands after the highest existing segment");
        let last = hb.opened.lock().last().cloned().unwrap();
        assert_eq!(last.2.last().unwrap().end, Some(8), "dead span ends at the fence");
        // a zombie write at the fence ordinal now collides
        let r = store
            .raw
            .put_opts(
                &crate::nodelog::segment_path(&store, &a.log_id, 8),
                PutPayload::from_static(b"zombie"),
                PutOptions { mode: PutMode::Create, ..Default::default() },
            )
            .await;
        assert!(r.is_err(), "zombie append must fail");
    }
}
