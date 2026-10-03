//! Signing-key rotation (DESIGN.md "Signing-key rotation"): `Begin` records
//! the new key as pending (before any directory can name it, and repo
//! writes are refused from then on), the PLC directory is updated, `Finish`
//! re-signs the head. A rotation stopped in between stays pending and
//! [`complete`] drives it to its end from durable state alone; only a
//! definite refusal by a directory that doesn't name the key abandons it.

use super::*;
use crate::plc::PlcError;
use crate::state::PendingSigningKey;
use crate::worker::{AccountOp, KeyStep};
use std::collections::HashSet;
use std::time::Duration;

pub const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

const INTERRUPTED: &str = "KeyRotationInterrupted";

pub use crate::lifecycle::CrashHook;

/// Phases of a DID's rotation: "begun" (the pending key is durable) and
/// "plc_updated" (the directory names it, the repo isn't re-signed yet). A
/// firing hook schedules no retry; the test halts the node in the hook.
static CRASH_HOOKS: crate::lifecycle::CrashHooks = crate::lifecycle::CrashHooks::new();

pub fn set_crash_hook(did: &str, h: Option<CrashHook>) {
    CRASH_HOOKS.set(did, h)
}

fn crash_at(did: &str, phase: &str) -> Result<(), XrpcError> {
    match CRASH_HOOKS.fires(did, phase) {
        true => Err(XrpcError::bad(INTERRUPTED, format!("key rotation of {did} stopped at {phase} (crash hook)"))),
        false => Ok(()),
    }
}

/// DIDs whose rotation this node is driving, so the sweep doesn't start a
/// second driver. Only an economy: every step is safe to repeat or race.
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

pub enum Done {
    Finished(Head),
    /// The directory refused the new key and doesn't name it.
    Aborted(XrpcError),
}

/// Whether the directory names `did_key` as the `atproto` key.
async fn plc_names(plc: &crate::plc::Plc, did: &str, did_key: &str) -> Result<bool, PlcError> {
    match plc.last_op(did).await {
        Ok(last) => Ok(crate::plc::normalize(&last)["verificationMethods"]["atproto"] == did_key),
        Err(PlcError::Tombstoned) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Idempotent. `key`: the new key, if the caller holds it (else unwrapped
/// from `p`). Err: undecided; the rotation is still pending.
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

/// The new key's did:key once the repo is re-signed with it. On an
/// undecided failure the rotation is finished in the background.
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

/// A moved repo is the new owner's to finish.
fn retryable(app: &App, did: &str, e: &XrpcError) -> bool {
    e.status.is_server_error() && e.error != INTERRUPTED && app.partition(did).is_ok()
}

/// None: nothing pending.
async fn resolve_once(app: &App, did: &str) -> Result<Option<Done>, XrpcError> {
    let acct = app.account(did).await?;
    let Some(p) = acct.pending_signing_key else { return Ok(None) };
    complete(app, did, &p, None).await.map(Some)
}

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    pub pending: usize,
    pub finished: usize,
    pub aborted: usize,
}

/// One attempt at each rotation pending in this node's shards; undecided
/// ones are retried in the background.
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

/// A crash or a shard move leaves a rotation to the shard's next owner.
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
