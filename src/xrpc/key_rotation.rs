//! Signing-key rotation (DESIGN.md "Signing-key rotation"):
//! `admin.updateAccountSigningKey` and its recovery.
//!
//! The reference's rotate-keys updates the DID document, then writes an
//! empty commit signed with the new key and sequences `#identity` and
//! `#sync`. Here the repo's worker orders the steps with the repo's commits
//! (`worker::KeyStep`):
//!
//! 1. `Begin`: the new key, wrapped, is recorded as the account's pending
//!    key (durably, with a `K/` marker) and repo writes are refused from
//!    then on. The key is persisted before any directory can name it, so a
//!    crash never leaves a DID document listing a key nobody holds, and no
//!    commit is signed with the old key once the document may list the new
//!    one.
//! 2. The PLC directory's `atproto` key is set to it (did:plc with PLC
//!    registration on; otherwise there is nothing to update here).
//! 3. `Finish`: the account switches keys and the head is re-signed (same
//!    data root, new rev), `#identity` then `#sync`, in one log entry. The
//!    call is acknowledged once that entry is durable, so every read after
//!    the acknowledgement serves the re-signed head.
//!
//! A rotation that stops between 1 and 3 (a crash, a directory outage, the
//! shard moving) stays pending; [`complete`] drives a pending rotation to
//! its end from durable state alone and is idempotent: it sets the
//! directory's key to the pending one (a no-op if it already is) and
//! finishes. Only a definite refusal by the directory, with the directory
//! still not naming the key, abandons it (`Abort`). The node runs it for
//! every marker in its shards at start and every [`RECOVERY_INTERVAL`]
//! ([`spawn_recovery`]), and in the background after a failed attempt.

use super::*;
use crate::plc::PlcError;
use crate::state::PendingSigningKey;
use crate::worker::{AccountOp, KeyStep};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// How often each node looks for rotations left pending in its shards.
pub const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

/// Error name of a rotation stopped by a test crash hook.
const INTERRUPTED: &str = "KeyRotationInterrupted";

/// Test hook: asked at each phase of a rotation of a DID ("begun": the
/// pending key is durable; "plc_updated": the directory names it, the repo
/// isn't re-signed yet). Returning true stops the rotation there, as a crash
/// would (the test halts the node in the hook): no retry is scheduled.
pub type CrashHook = Arc<dyn Fn(&str) -> bool + Send + Sync>;

static CRASH_HOOKS: parking_lot::Mutex<Option<HashMap<String, CrashHook>>> = parking_lot::Mutex::new(None);

/// Installs (Some) or removes (None) the crash hook of rotations of `did`.
pub fn set_crash_hook(did: &str, h: Option<CrashHook>) {
    let mut g = CRASH_HOOKS.lock();
    let m = g.get_or_insert_with(HashMap::new);
    match h {
        Some(h) => m.insert(did.to_string(), h),
        None => m.remove(did),
    };
}

fn crash_at(did: &str, phase: &str) -> Result<(), XrpcError> {
    let h = CRASH_HOOKS.lock().as_ref().and_then(|m| m.get(did).cloned());
    match h.is_some_and(|h| h(phase)) {
        true => Err(XrpcError::bad(INTERRUPTED, format!("key rotation of {did} stopped at {phase} (crash hook)"))),
        false => Ok(()),
    }
}

/// DIDs whose rotation this node is driving (a handler or a resolver), so
/// the sweep doesn't start a second driver beside it. Only an economy:
/// every step is safe to repeat or race.
static DRIVING: parking_lot::Mutex<Option<HashSet<String>>> = parking_lot::Mutex::new(None);

struct Driving(String);

impl Driving {
    fn take(did: &str) -> Option<Driving> {
        DRIVING.lock().get_or_insert_with(HashSet::new).insert(did.to_string()).then(|| Driving(did.to_string()))
    }
}

impl Drop for Driving {
    fn drop(&mut self) {
        if let Some(s) = DRIVING.lock().as_mut() {
            s.remove(&self.0);
        }
    }
}

/// How a pending rotation ended.
pub enum Done {
    /// The repo is re-signed with the new key (its new head).
    Finished(Head),
    /// The directory refused the new key and doesn't name it: the rotation
    /// was dropped (why).
    Aborted(XrpcError),
}

/// Whether the directory's document of `did` names `did_key` as its
/// `atproto` key (a tombstoned DID names none).
async fn plc_names(plc: &crate::plc::Plc, did: &str, did_key: &str) -> Result<bool, PlcError> {
    match plc.last_op(did).await {
        Ok(last) => Ok(crate::plc::normalize(&last)["verificationMethods"]["atproto"] == did_key),
        Err(PlcError::Tombstoned) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Drives the pending rotation `p` of `did` to its end (steps 2 and 3).
/// `key`: the new key, if the caller holds it (else it is unwrapped from
/// `p`). Err: undecided (directory or key service unreachable, shard moved,
/// crash hook); the rotation is still pending.
pub async fn complete(app: &App, did: &str, p: &PendingSigningKey, key: Option<Arc<Keypair>>) -> Result<Done, XrpcError> {
    let did_key = format!("did:key:{}", p.pubkey);
    if let (Some(plc), true) = (&app.plc, did.starts_with("did:plc:")) {
        match plc.update_signing_key(did, &did_key).await {
            Ok(_) => {}
            // maybe applied: decided on a later attempt
            Err(e @ PlcError::Unavailable(_)) => return Err(e.into()),
            // refused; the key stays unless an earlier attempt (a racing
            // driver) already made it the document's
            Err(e) => {
                if !plc_names(plc, did, &did_key).await? {
                    tracing::warn!(%did, key = %did_key, "signing key rotation abandoned: PLC refused the new key: {e}");
                    app.account_op(did, AccountOp::SigningKey(KeyStep::Abort { pubkey: p.pubkey.clone() })).await?;
                    return Ok(Done::Aborted(e.into()));
                }
            }
        }
    }
    crash_at(did, "plc_updated")?;
    let key = match key {
        Some(k) => k,
        None => app.secrets.signing_key(did, &p.wrapped, &p.pubkey).await?,
    };
    let head = app.account_op(did, AccountOp::SigningKey(KeyStep::Finish { key })).await?;
    app.did_resolver.invalidate(did);
    tracing::info!(%did, key = %did_key, rev = %head.rev, "signing key rotated, repo re-signed");
    Ok(Done::Finished(head))
}

/// Rotates `did`'s signing key to `key` (all three steps). The new key's
/// did:key once the repo is re-signed with it. On an undecided failure the
/// rotation stays pending and is finished in the background.
pub(super) async fn rotate(app: &Arc<App>, did: &str, key: Keypair) -> XResult<String> {
    let key = Arc::new(key);
    let did_key = key.did_key();
    // wrapped for the row; cached unwrapped too (the re-sign needs no unwrap)
    let (wrapped, pubkey) = app.secrets.wrap_signing_key(did, &key).await?;
    let p = PendingSigningKey { wrapped, pubkey };
    let driving = Driving::take(did);
    app.account_op(did, AccountOp::SigningKey(KeyStep::Begin(p.clone()))).await?;
    let r = match crash_at(did, "begun") {
        Ok(()) => complete(app, did, &p, Some(key)).await,
        Err(e) => Err(e),
    };
    match r {
        Ok(Done::Finished(_)) => Ok(did_key),
        Ok(Done::Aborted(e)) => Err(e),
        Err(e) => {
            drop(driving);
            if retryable(app, did, &e) {
                tracing::warn!(%did, key = %did_key, "signing key rotation pending, retrying in the background: {}", e.message);
                spawn_resolver(app.clone(), did.to_string());
            }
            Err(e)
        }
    }
}

/// Whether an undecided rotation is worth retrying here: a server-side
/// failure on a repo this node still owns (a moved repo is the new owner's
/// to finish), not a crash hook.
fn retryable(app: &App, did: &str, e: &XrpcError) -> bool {
    e.status.is_server_error() && e.error != INTERRUPTED && app.partition(did).is_ok()
}

/// One attempt at `did`'s pending rotation, if it has one (None: nothing
/// pending).
async fn resolve_once(app: &App, did: &str) -> Result<Option<Done>, XrpcError> {
    let acct = app.account(did).await?;
    let Some(p) = acct.pending_signing_key else { return Ok(None) };
    complete(app, did, &p, None).await.map(Some)
}

/// Retries `did`'s pending rotation with backoff until it ends (or can't
/// be finished here), unless this node is driving it already.
fn spawn_resolver(app: Arc<App>, did: String) {
    let Some(driving) = Driving::take(&did) else { return };
    tokio::spawn(async move {
        let _driving = driving;
        let mut wait = Duration::from_secs(1);
        loop {
            tokio::time::sleep(wait).await;
            match resolve_once(&app, &did).await {
                Ok(_) => return,
                Err(e) if retryable(&app, &did, &e) => {
                    tracing::warn!(%did, "pending signing key rotation: {}", e.message);
                    wait = (wait * 2).min(RECOVERY_INTERVAL);
                }
                Err(e) => {
                    tracing::warn!(%did, "pending signing key rotation left to its next owner or sweep: {}", e.message);
                    return;
                }
            }
        }
    });
}

/// What one [`recover_pending`] pass found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    /// Rotations found pending in this node's shards.
    pub pending: usize,
    pub finished: usize,
    pub aborted: usize,
}

/// Finishes (or abandons) every rotation left pending in the shards this
/// node owns: one attempt each, undecided ones retried in the background.
pub async fn recover_pending(app: &Arc<App>) -> Recovered {
    let mut dids = Vec::new();
    for p in app.partitions.owned() {
        let fam = state::KEY_ROTATION_FAMILY;
        let scan = async {
            let mut it = state::FamilyScan::new(p.db.as_ref(), fam, None, &Default::default()).await?;
            while let Some(kv) = it.next().await? {
                dids.push(String::from_utf8_lossy(&state::key_body(&kv.key)[fam.len()..]).into_owned());
            }
            Ok::<_, slatedb::Error>(())
        };
        if let Err(e) = scan.await {
            tracing::warn!(shard = %p.id, "pending signing key rotations: scan failed: {e}");
        }
    }
    let mut out = Recovered { pending: dids.len(), ..Default::default() };
    for did in dids {
        let Some(driving) = Driving::take(&did) else { continue };
        match resolve_once(app, &did).await {
            Ok(Some(Done::Finished(_))) => out.finished += 1,
            Ok(Some(Done::Aborted(_))) => out.aborted += 1,
            Ok(None) => {}
            Err(e) => {
                drop(driving);
                if retryable(app, &did, &e) {
                    spawn_resolver(app.clone(), did.clone());
                }
                tracing::warn!(%did, "pending signing key rotation: {}", e.message);
            }
        }
    }
    if out.pending > 0 {
        tracing::info!(pending = out.pending, finished = out.finished, aborted = out.aborted, "pending signing key rotations recovered");
    }
    out
}

/// [`recover_pending`] now and every [`RECOVERY_INTERVAL`] (a crash or a
/// shard move leaves a rotation to the shard's next owner).
pub fn spawn_recovery(app: Arc<App>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(RECOVERY_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            recover_pending(&app).await;
        }
    })
}
