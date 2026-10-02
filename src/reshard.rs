//! Online shard split/merge: the control-plane half (DESIGN.md "Online
//! shard split/merge"). The layout (`assign/layout`) carries at most one op;
//! each parent's owner freezes it (close + CAS `frozen`), then the op's
//! driver clones the children from the frozen parents, writes their
//! assignments and flips the layout (the commit point), then takes the
//! children. Every step is idempotent and resumable from object-store
//! state, and anything before the flip can be aborted.

use crate::cluster::{Assignment, Cluster, ShardHost, LAYOUT};
use crate::slots::{Layout, Reshard, ShardId};
use object_store::{PutMode, UpdateVersion};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// What an admin (or the policy) asks for.
#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
    /// Split `shard` at slot `at` (default: the midpoint of its range).
    Split { shard: ShardId, at: Option<u32> },
    /// Merge two adjacent shards (`left` holds the lower slots).
    Merge { left: ShardId, right: ShardId },
}

/// Policy hook (off by default): split a shard whose state or write rate
/// is past a threshold. Evaluated by every node for the shards it holds,
/// at most one plan per node per `POLICY_EVERY`.
#[derive(Clone, Debug, Default)]
pub struct Policy {
    /// Split shards with more SST bytes than this.
    pub split_bytes: Option<u64>,
    /// Split shards applying more log entries per second than this.
    pub split_writes_per_sec: Option<f64>,
}

const POLICY_EVERY: Duration = Duration::from_secs(60);

fn is_conflict(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
}

/// Test hook: asked at each phase of a reshard on a node ("planned",
/// "closed", "frozen", "cloned", "children", "flipped"); returning true
/// makes that node's reshard work stop dead there, as a crash would (the
/// test halts the node in the hook).
pub type CrashHook = Arc<dyn Fn(&str) -> bool + Send + Sync>;

static CRASH_HOOKS: parking_lot::RwLock<Option<HashMap<String, CrashHook>>> = parking_lot::RwLock::new(None);

/// Installs (Some) or removes (None) the crash hook of node `node_id`.
pub fn set_crash_hook(node_id: &str, h: Option<CrashHook>) {
    let mut g = CRASH_HOOKS.write();
    let m = g.get_or_insert_with(HashMap::new);
    match h {
        Some(h) => m.insert(node_id.to_string(), h),
        None => m.remove(node_id),
    };
}

pub(crate) fn crash_at(node: &str, phase: &str) -> bool {
    let h = CRASH_HOOKS.read().as_ref().and_then(|m| m.get(node).cloned());
    h.is_some_and(|h| h(phase))
}

/// A phase's crash hook fired: this node stops reshard work as if it died.
#[derive(Debug)]
pub struct Crashed(pub String);

impl std::fmt::Display for Crashed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "crash hook at {}", self.0)
    }
}

impl std::error::Error for Crashed {}

impl Cluster {
    /// Plans a split or merge: CAS the layout to carry the op, driven by the
    /// parents' owner when they share one (it can then freeze, clone and
    /// flip in one step), else by us. Returns the op.
    pub async fn plan_reshard(&self, host: &Arc<dyn ShardHost>, plan: Plan) -> anyhow::Result<Reshard> {
        for _ in 0..5 {
            let path = self.path(LAYOUT);
            let Some((cur, etag)) = self.get_json::<Layout>(&path).await? else { anyhow::bail!("shard layout missing") };
            let parents = match &plan {
                Plan::Split { shard, .. } => vec![*shard],
                Plan::Merge { left, right } => vec![*left, *right],
            };
            let owners: HashSet<Option<String>> = parents.iter().map(|p| self.owner_of(*p).map(|o| o.0)).collect();
            let driver = match owners.into_iter().collect::<Vec<_>>().as_slice() {
                [Some(o)] => o.clone(),
                _ => self.cfg.node_id.clone(),
            };
            let op = match &plan {
                Plan::Split { shard, at } => cur.plan_split(*shard, *at, &driver)?,
                Plan::Merge { left, right } => cur.plan_merge(*left, *right, &driver)?,
            };
            let next = cur.with_op(op.clone());
            match self.put_json(&path, &next, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
                Ok(e) => {
                    tracing::info!(op = op.id, parents = ?op.parents, children = ?op.children, driver = %op.driver, "planned reshard");
                    crate::metrics::RESHARD_EVENTS.with_label_values(&["planned"]).inc();
                    self.install_layout(host, next, e);
                    if crash_at(&self.cfg.node_id, "planned") {
                        return Ok(op);
                    }
                    self.nudge_all(host, true).await;
                    return Ok(op);
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("layout kept changing under the plan; retry")
    }

    /// Aborts the op in flight (before its flip): clears it from the layout
    /// and unfreezes its parents. Returns the aborted op (None: nothing to
    /// abort, e.g. it already flipped).
    pub async fn abort_reshard(&self, host: &Arc<dyn ShardHost>) -> anyhow::Result<Option<Reshard>> {
        let path = self.path(LAYOUT);
        let op = loop {
            let Some((cur, etag)) = self.get_json::<Layout>(&path).await? else { anyhow::bail!("shard layout missing") };
            let Some(op) = cur.op.clone() else { return Ok(None) };
            let next = Layout { op: None, ..cur };
            match self.put_json(&path, &next, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
                Ok(e) => {
                    self.install_layout(host, next, e);
                    break op;
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        };
        tracing::warn!(op = op.id, parents = ?op.parents, "aborted reshard");
        crate::metrics::RESHARD_EVENTS.with_label_values(&["aborted"]).inc();
        for &p in &op.parents {
            self.unfreeze(p, op.id).await?;
        }
        self.nudge_all(host, true).await;
        Ok(Some(op))
    }

    /// Clears `frozen == op` from shard `p`'s assignment (CAS on a fresh read).
    async fn unfreeze(&self, p: ShardId, op: u64) -> anyhow::Result<()> {
        let path = self.path(&format!("assign/{}", p.key()));
        loop {
            let Some((mut a, etag)) = self.get_json::<Assignment>(&path).await? else { return Ok(()) };
            if a.frozen != Some(op) {
                return Ok(());
            }
            a.frozen = None;
            match self.put_json(&path, &a, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
                Ok(e) => {
                    tracing::info!(shard = p.0, op, "unfroze shard");
                    self.assigns.write().insert(p, (a, e));
                    return Ok(());
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Nudges every peer (and, with `me`, ourselves): they step now.
    async fn nudge_all(&self, host: &Arc<dyn ShardHost>, me: bool) {
        let nudges: Vec<(String, Vec<crate::cluster::Handoff>)> = self.peers().into_iter().map(|l| (l.addr, Vec::new())).collect();
        crate::metrics::CLUSTER_NUDGES.with_label_values(&["sent"]).inc_by(nudges.len() as u64);
        if me {
            self.nudge(Vec::new());
        }
        host.nudge(nudges).await;
    }

    /// One step of reshard work (inside the cluster step): freeze parents
    /// we hold, clean up stale freezes, drive the op if it is ours, and run
    /// the policy hook.
    pub(crate) async fn reshard_step(&self, host: &Arc<dyn ShardHost>, live: &HashSet<String>) -> anyhow::Result<()> {
        let layout = self.layout();
        self.unfreeze_stale(host, &layout).await?;
        let Some(op) = layout.op.clone() else {
            return self.run_policy(host).await;
        };
        // 1. freeze the parents we hold (one barrier for all of them)
        let mine: Vec<ShardId> = op.parents.iter().copied().filter(|p| self.is_owner(*p)).collect();
        if !mine.is_empty() {
            tracing::info!(op = op.id, shards = ?mine, "freezing reshard parents");
            if !self.close_and_release(host, mine, Vec::new(), Some(op.id)).await {
                return Ok(()); // fail-stopped
            }
            crate::metrics::RESHARD_EVENTS.with_label_values(&["frozen"]).inc();
            if crash_at(&self.cfg.node_id, "frozen") {
                return Err(Crashed("frozen".into()).into());
            }
        }
        // 2. drive it if we are its driver, or its driver is gone and we are
        //    the live node with the lowest id
        let mut op = op;
        if op.driver != self.cfg.node_id {
            let lowest = live.iter().min().is_some_and(|m| *m == self.cfg.node_id);
            if live.contains(&op.driver) || !lowest {
                return Ok(());
            }
            match self.take_over_driver(host, &op).await? {
                Some(o) => op = o,
                None => return Ok(()),
            }
        }
        self.drive(host, &layout, &op).await
    }

    /// CASes ourselves in as the driver of `op` (its driver is gone).
    async fn take_over_driver(&self, host: &Arc<dyn ShardHost>, op: &Reshard) -> anyhow::Result<Option<Reshard>> {
        let path = self.path(LAYOUT);
        let Some((cur, etag)) = self.get_json::<Layout>(&path).await? else { return Ok(None) };
        let Some(mut o) = cur.op.clone().filter(|o| o.id == op.id) else { return Ok(None) };
        tracing::warn!(op = o.id, dead = %o.driver, "taking over a reshard whose driver is gone");
        o.driver = self.cfg.node_id.clone();
        let next = Layout { op: Some(o.clone()), ..cur };
        match self.put_json(&path, &next, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
            Ok(e) => {
                self.install_layout(host, next, e);
                Ok(Some(o))
            }
            Err(e) if is_conflict(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Clone, children's assignments, flip, take the children. Waits (Ok)
    /// until every parent is frozen for `op`.
    async fn drive(&self, host: &Arc<dyn ShardHost>, layout: &Layout, op: &Reshard) -> anyhow::Result<()> {
        let started = Instant::now();
        let mut floor = 0i64;
        for &p in &op.parents {
            // a fresh read: the parent's owner froze it on its own step
            let cached = self.assigns.read().get(&p).cloned();
            let a = match cached {
                Some((a, _)) if a.frozen == Some(op.id) => a,
                _ => match self.get_json::<Assignment>(&self.path(&format!("assign/{}", p.key()))).await? {
                    Some((a, e)) => {
                        self.assigns.write().insert(p, (a.clone(), e));
                        a
                    }
                    None => return Ok(()),
                },
            };
            if a.frozen != Some(op.id) || a.owner.is_some() {
                return Ok(()); // not frozen yet
            }
            floor = floor.max(a.seq_floor);
        }
        // 3. clone the children (idempotent) and write their assignments
        host.clone_shards(layout, op).await?;
        if crash_at(&self.cfg.node_id, "cloned") {
            return Err(Crashed("cloned".into()).into());
        }
        for c in &op.children {
            self.write_child(c.id, floor).await?;
        }
        if crash_at(&self.cfg.node_id, "children") {
            return Err(Crashed("children".into()).into());
        }
        // 4. flip: the commit point
        let path = self.path(LAYOUT);
        let Some((cur, etag)) = self.get_json::<Layout>(&path).await? else { anyhow::bail!("shard layout missing") };
        if cur.op.as_ref().is_none_or(|o| o.id != op.id) {
            return Ok(()); // aborted (or flipped by a previous driver) meanwhile
        }
        let next = cur.flipped(op)?;
        match self.put_json(&path, &next, PutMode::Update(UpdateVersion { e_tag: etag, version: None })).await {
            Ok(e) => {
                tracing::info!(op = op.id, version = next.version, parents = ?op.parents, children = ?op.children.iter().map(|c| c.id).collect::<Vec<_>>(), "reshard flipped");
                crate::metrics::RESHARD_EVENTS.with_label_values(&[if op.is_split() { "split" } else { "merged" }]).inc();
                self.install_layout(host, next, e);
            }
            Err(e) if is_conflict(&e) => return Ok(()), // re-read next step
            Err(e) => return Err(e.into()),
        }
        if crash_at(&self.cfg.node_id, "flipped") {
            return Err(Crashed("flipped".into()).into());
        }
        // 5. take the children now (they are free shards in the new layout)
        let live_ids: HashSet<String> = self.peers().into_iter().map(|l| l.node_id).chain([self.cfg.node_id.clone()]).collect();
        let ids: Vec<ShardId> = op.children.iter().map(|c| c.id).collect();
        let n = ids.len();
        self.acquire(host, ids, n, &live_ids, &HashMap::new(), 0, live_ids.len()).await?;
        crate::metrics::RESHARD_SECONDS.observe(started.elapsed().as_secs_f64());
        self.nudge_all(host, false).await;
        Ok(())
    }

    /// Writes child `id`'s fresh assignment (no owner, epoch 0, no history,
    /// `seq_floor`). Never blindly: a retry (or a driver presumed dead that
    /// wakes up late) may only replace a fresh one, by CAS, so an assignment
    /// a node already took after the flip is never reset.
    async fn write_child(&self, id: ShardId, floor: i64) -> anyhow::Result<()> {
        let path = self.path(&format!("assign/{}", id.key()));
        let fresh = Assignment { seq_floor: floor, ..Default::default() };
        loop {
            let mode = match self.get_json::<Assignment>(&path).await? {
                None => PutMode::Create,
                Some((a, _)) if a.owner.is_some() || a.epoch > 0 || !a.history.is_empty() => return Ok(()),
                Some((a, _)) if a == fresh => return Ok(()),
                Some((_, etag)) => PutMode::Update(UpdateVersion { e_tag: etag, version: None }),
            };
            match self.put_json(&path, &fresh, mode).await {
                Ok(e) => {
                    self.assigns.write().insert(id, (fresh, e));
                    return Ok(());
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// A shard still in the layout but frozen for an op that isn't the
    /// layout's (an abort that crashed half-way) is unfrozen, after a fresh
    /// read of the layout confirms it.
    async fn unfreeze_stale(&self, host: &Arc<dyn ShardHost>, layout: &Layout) -> anyhow::Result<()> {
        let cur = layout.op.as_ref().map(|o| o.id);
        let stale: Vec<(ShardId, u64)> = {
            let assigns = self.assigns.read();
            layout.ids().into_iter().filter_map(|s| assigns.get(&s).and_then(|(a, _)| a.frozen).filter(|f| Some(*f) != cur).map(|f| (s, f))).collect()
        };
        if stale.is_empty() {
            return Ok(());
        }
        let fresh = self.refresh_layout(host).await?;
        for (s, f) in stale {
            if fresh.contains(s) && fresh.op.as_ref().is_none_or(|o| o.id != f) {
                tracing::warn!(shard = s.0, op = f, "unfreezing a shard left frozen by an aborted reshard");
                self.unfreeze(s, f).await?;
            }
        }
        Ok(())
    }

    /// Plans a split of one of our shards past the policy's thresholds.
    async fn run_policy(&self, host: &Arc<dyn ShardHost>) -> anyhow::Result<()> {
        let policy = self.policy.read().clone();
        if policy.split_bytes.is_none() && policy.split_writes_per_sec.is_none() {
            return Ok(());
        }
        let now = Instant::now();
        let stats = host.shard_stats();
        let pick = {
            let mut last = self.policy_state.lock();
            let prev = std::mem::replace(&mut last.1, stats.iter().map(|(s, _, n)| (*s, (*n, now))).collect());
            if last.0.is_some_and(|t| now.duration_since(t) < POLICY_EVERY) {
                return Ok(());
            }
            let pick = stats.into_iter().find_map(|(s, bytes, entries)| {
                let rate = prev.get(&s).map(|(n0, t0)| entries.saturating_sub(*n0) as f64 / now.duration_since(*t0).as_secs_f64().max(1e-3));
                let big = policy.split_bytes.is_some_and(|b| bytes > b);
                let hot = policy.split_writes_per_sec.zip(rate).is_some_and(|(w, r)| r > w);
                (big || hot).then_some((s, bytes, rate))
            });
            if pick.is_some() {
                last.0 = Some(now);
            }
            pick
        };
        let Some((shard, bytes, rate)) = pick else { return Ok(()) };
        tracing::info!(shard = shard.0, bytes, rate, "reshard policy: splitting");
        if let Err(e) = self.plan_reshard(host, Plan::Split { shard, at: None }).await {
            tracing::warn!(shard = shard.0, "reshard policy: plan failed: {e:#}");
        }
        Ok(())
    }
}
