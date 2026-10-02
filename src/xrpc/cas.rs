//! Conditional writes of private (`p/{routing}\0...`) state, correct across
//! nodes: [`App::private_cas`].
//!
//! `put_private` is a blind write. Read-modify-write sequences whose
//! correctness depends on what they read (a refresh-token rotation, a
//! session created from a credential check, a TOTP `last_step` or lockout
//! counter) use this instead: the conditions are checked and the write made
//! at the routing key's owner, under a lock there that every conditional
//! write of that routing key takes, with the write applied before the lock
//! is released. So two conditional writes of one key never interleave, on
//! whichever nodes they started (requests on other nodes are forwarded to
//! the owner, as `put_private` does).
//!
//! Only conditional writes serialize with each other: a blind `put_private`
//! of the same rows can still slip between a check and its write. Rows that
//! need the guarantee are written only through here (the OAuth store, the
//! legacy `sess/` rows, `auth_epoch`, TOTP state, the email-factor lockout).
//!
//! An ownership move between the check and the write is safe: the old
//! owner's log refuses an entry for a shard it no longer holds (nodelog
//! `Open::push`), so the write fails rather than landing after the new
//! owner's writes.

use super::*;
use crate::segment::Mutation;
use std::collections::HashMap;

/// A precondition, on the current (applied) value of one private row.
#[derive(Clone, Debug)]
pub enum Cond {
    /// The row `name` holds exactly `val` (None = absent).
    Eq { name: String, val: Option<Bytes> },
}

impl Cond {
    pub fn eq(name: impl Into<String>, val: Option<Bytes>) -> Cond {
        Cond::Eq { name: name.into(), val }
    }
}

/// A write, made if every [`Cond`] holds.
#[derive(Clone, Debug)]
pub enum Op {
    /// Set (Some) or delete (None) the row `name`.
    Put { name: String, val: Option<Bytes> },
    /// Delete every row whose name starts with `prefix` (read at the owner
    /// under the lock, so no row created by an earlier conditional write is
    /// missed).
    DeletePrefix { prefix: String },
}

impl Op {
    pub fn put(name: impl Into<String>, val: Option<Bytes>) -> Op {
        Op::Put { name: name.into(), val }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Outcome {
    /// Whether every condition held (and so the write was made).
    pub applied: bool,
    /// Rows removed by [`Op::DeletePrefix`] (name, value).
    pub deleted: Vec<(String, Bytes)>,
}

/// Per-routing-key locks of one node (per `App`, so in-process test
/// clusters behave like separate machines). Unused entries are pruned.
#[derive(Default)]
struct Locks {
    m: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

const LOCKS_PRUNE_AT: usize = 4096;

impl Locks {
    fn get(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut m = self.m.lock();
        if m.len() >= LOCKS_PRUNE_AT {
            // a held lock (or one being waited on) has another reference
            m.retain(|_, l| Arc::strong_count(l) > 1);
        }
        m.entry(key.to_string()).or_default().clone()
    }
}

static LOCKS: parking_lot::RwLock<Vec<(usize, Arc<Locks>)>> = parking_lot::RwLock::new(Vec::new());

fn locks(app: &App) -> Arc<Locks> {
    let id = app as *const App as usize;
    if let Some((_, l)) = LOCKS.read().iter().find(|(k, _)| *k == id) {
        return l.clone();
    }
    let mut w = LOCKS.write();
    if let Some((_, l)) = w.iter().find(|(k, _)| *k == id) {
        return l.clone();
    }
    let l = Arc::new(Locks::default());
    w.push((id, l.clone()));
    l
}

/// Test hook: awaited at a named point of a read-modify-write (e.g.
/// `oauth_refresh`, right before its conditional write) for one routing key,
/// so a test can hold a request there while it races something against it.
pub type PauseHook = Arc<dyn Fn(&str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

static PAUSE_HOOKS: parking_lot::Mutex<Option<HashMap<String, PauseHook>>> = parking_lot::Mutex::new(None);
static ANY_PAUSE_HOOK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Installs (Some) or removes (None) the pause hook of `routing` (tests).
pub fn set_pause_hook(routing: &str, h: Option<PauseHook>) {
    let mut g = PAUSE_HOOKS.lock();
    let m = g.get_or_insert_with(HashMap::new);
    match h {
        Some(h) => m.insert(routing.to_string(), h),
        None => m.remove(routing),
    };
    ANY_PAUSE_HOOK.store(!m.is_empty(), std::sync::atomic::Ordering::Release);
}

/// Runs `routing`'s pause hook at `point`, if a test installed one.
pub async fn pause_point(point: &str, routing: &str) {
    if !ANY_PAUSE_HOOK.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    let h = PAUSE_HOOKS.lock().as_ref().and_then(|m| m.get(routing).cloned());
    if let Some(h) = h {
        h(point).await;
    }
}

impl App {
    /// Checks `conds` and, if they all hold, applies `ops` in one log write,
    /// at the owner of `routing` and serialized with every other
    /// conditional write of `routing` (module docs). `applied: false` =
    /// a condition failed and nothing was written.
    pub async fn private_cas(&self, routing: &str, conds: Vec<Cond>, ops: Vec<Op>) -> Result<Outcome, XrpcError> {
        if let Some(owner) = self.remote_owner(routing) {
            let touches_sec = ops.iter().any(|o| matches!(o, Op::Put { name, .. } if name.starts_with(super::server::SEC)));
            let r = internal::forward_private_cas(self, &owner, routing, conds, ops).await;
            if touches_sec {
                // the owner dropped its view; so does this node (as put_sec)
                super::server::ctl_changed(self, routing);
            }
            return r;
        }
        private_cas_local(self, routing, conds, ops).await
    }
}

/// [`App::private_cas`] on the owner (the internal endpoint calls this: never
/// forwarded again).
pub(super) async fn private_cas_local(app: &App, routing: &str, conds: Vec<Cond>, ops: Vec<Op>) -> Result<Outcome, XrpcError> {
    let lock = locks(app).get(routing);
    let _g = lock.lock().await;
    let p = app.partition(routing)?;
    for c in &conds {
        let Cond::Eq { name, val } = c;
        let cur = p.db.get(state::private_key(routing, name)).await.map_err(XrpcError::from_err)?;
        if cur.as_deref() != val.as_deref() {
            return Ok(Outcome::default());
        }
    }
    let mut muts: Vec<Mutation> = Vec::new();
    let mut deleted = Vec::new();
    let mut put_names: Vec<&str> = Vec::new();
    for op in &ops {
        if let Op::Put { name, .. } = op {
            put_names.push(name);
        }
    }
    for op in &ops {
        match op {
            Op::Put { name, val } => muts.push(Mutation { key: state::private_key(routing, name).into(), val: val.clone() }),
            Op::DeletePrefix { prefix } => {
                for (name, v) in super::server::scan_private(app, routing, prefix).await? {
                    if !put_names.contains(&name.as_str()) {
                        muts.push(Mutation { key: state::private_key(routing, &name).into(), val: None });
                    }
                    deleted.push((name, v));
                }
            }
        }
    }
    if !muts.is_empty() {
        let sec = state::private_key(routing, super::server::SEC);
        let touches_sec = muts.iter().any(|m| m.key.starts_with(&sec));
        let r = write_local(&p, muts).await;
        if touches_sec {
            super::server::ctl_changed(app, routing);
        }
        r?;
    }
    Ok(Outcome { applied: true, deleted })
}

/// `put_private` into a partition held here, never forwarded: if the shard
/// moved since the checks, the log refuses the entry (module docs).
async fn write_local(p: &crate::partition::Partition, muts: Vec<Mutation>) -> Result<(), XrpcError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let entry = crate::partition::LogEntry {
        shard: p.id,
        frames: Vec::new(),
        muts,
        ack: Some(Box::new(move |r| {
            let _ = tx.send(r);
        })),
        pending: None,
        enqueued: std::time::Instant::now(),
    };
    p.tx.send(entry).await.map_err(|_| XrpcError::internal("partition sequencer gone"))?;
    rx.await
        .map_err(|_| XrpcError::internal("log dropped write"))?
        .map_err(|e| XrpcError::internal(e.to_string()))
}
