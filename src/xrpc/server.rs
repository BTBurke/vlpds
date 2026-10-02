//! com.atproto.server.* (sessions, accounts, app passwords, email flows,
//! invites, service auth), com.atproto.temp.{checkSignupQueue,
//! checkHandleAvailability} and the vlpds.server.*Totp second factor.
//!
//! Durable state (all through the partition log via `put_private` /
//! `account_op`):
//!   p/{did}\0sess/{refresh id}     refresh-token session (rotated on refresh)
//!   p/{did}\0apppass/{name}        app password metadata
//!   p/{did}\0apphash/{hash}        app password hash -> name (login lookup)
//!   p/{did}\0etok/{purpose}        current email token per purpose (keyed digest)
//!   p/{did}\0totp                  TOTP state (src/totp.rs; secrets wrapped)
//!   p/_reset:{digest}\0t           password-reset token digest -> did
//!   p/_invite:{code}\0c            invite code (+ p/{account}\0invite/{code} index)
//!   p/{did}\0sec/rvk/f/{family}    revoked session family (access tokens), TTL'd
//!   p/{did}\0sec/rvk/d             all sessions of the DID revoked before a time
//!   p/{did}\0sec/td/rec/{coll}/{rkey}, p/{did}\0sec/td/blob/{cid}
//!                                  record / blob takedowns (admin.rs)
//!   {prefix}/email/{sha256(email)} global email claim -> did (object store,
//!                                  conditional create, like handle claims)
//! Account fields owned here live in `Account.extra`: deactivatedAt,
//! deleteAfter, takedownRef, emailConfirmedAt, invitesDisabled, invitedBy,
//! totpEnabled.
//!
//! Access tokens carry `jti` = session family id (`{issue micros:016x}{rand}`),
//! refresh tokens `jti` = refresh id. Revocations and takedowns live in the
//! account's own partition (`sec/`), so the node that owns the DID (where
//! forwarding sends its requests) enforces them, and a successor owner reads
//! them back after a failover. `verify_bearer` and the takedown checks use a
//! per-DID in-memory view ([`ctl`]): loaded once from the owned partition and
//! kept until a change or an ownership move (re-read from the owner every
//! few seconds on other nodes), so the hot path does no storage reads.

use super::authn::Credentials;
use super::*;
use crate::segment::Mutation;
use crate::worker::AccountOp;
use parking_lot::{Mutex as PMutex, RwLock};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;

pub fn routes() -> Router<Arc<App>> {
    let r = Router::new()
        .route(
            "/xrpc/com.atproto.server.describeServer",
            get(describe_server),
        )
        .route(
            "/xrpc/com.atproto.server.createAccount",
            post(create_account),
        )
        .route(
            "/xrpc/com.atproto.server.createSession",
            post(create_session),
        )
        .route("/xrpc/com.atproto.server.getSession", get(get_session))
        .route(
            "/xrpc/com.atproto.server.refreshSession",
            post(refresh_session),
        )
        .route(
            "/xrpc/com.atproto.server.deleteSession",
            post(delete_session),
        )
        .route(
            "/xrpc/com.atproto.server.createAppPassword",
            post(create_app_password),
        )
        .route(
            "/xrpc/com.atproto.server.listAppPasswords",
            get(list_app_passwords),
        )
        .route(
            "/xrpc/com.atproto.server.revokeAppPassword",
            post(revoke_app_password),
        )
        .route(
            "/xrpc/com.atproto.server.deactivateAccount",
            post(deactivate_account),
        )
        .route(
            "/xrpc/com.atproto.server.activateAccount",
            post(activate_account),
        )
        .route(
            "/xrpc/com.atproto.server.checkAccountStatus",
            get(check_account_status),
        )
        .route(
            "/xrpc/com.atproto.server.requestAccountDelete",
            post(request_account_delete),
        )
        .route(
            "/xrpc/com.atproto.server.deleteAccount",
            post(delete_account),
        )
        .route(
            "/xrpc/com.atproto.server.reserveSigningKey",
            post(reserve_signing_key),
        )
        .route(
            "/xrpc/com.atproto.server.requestEmailConfirmation",
            post(request_email_confirmation),
        )
        .route("/xrpc/com.atproto.server.confirmEmail", post(confirm_email))
        .route(
            "/xrpc/com.atproto.server.requestEmailUpdate",
            post(request_email_update),
        )
        .route("/xrpc/com.atproto.server.updateEmail", post(update_email))
        .route(
            "/xrpc/com.atproto.server.requestPasswordReset",
            post(request_password_reset),
        )
        .route(
            "/xrpc/com.atproto.server.resetPassword",
            post(reset_password),
        )
        .route(
            "/xrpc/com.atproto.server.createInviteCode",
            post(create_invite_code),
        )
        .route(
            "/xrpc/com.atproto.server.createInviteCodes",
            post(create_invite_codes),
        )
        .route(
            "/xrpc/com.atproto.server.getAccountInviteCodes",
            get(get_account_invite_codes),
        )
        .route(
            "/xrpc/com.atproto.server.getServiceAuth",
            get(get_service_auth),
        )
        .route(
            "/xrpc/com.atproto.temp.checkSignupQueue",
            get(check_signup_queue),
        )
        .route(
            "/xrpc/com.atproto.temp.checkHandleAvailability",
            get(check_handle_availability),
        )
        .route("/xrpc/vlpds.server.setupTotp", post(setup_totp))
        .route("/xrpc/vlpds.server.confirmTotp", post(confirm_totp))
        .route("/xrpc/vlpds.server.disableTotp", post(disable_totp))
        .route("/xrpc/vlpds.server.getTotpStatus", get(get_totp_status));
    r
}

// ---------------------------------------------------------------------------
// constants, small helpers
// ---------------------------------------------------------------------------

const ACCESS_TTL: u64 = 2 * 3600;
const REFRESH_TTL: u64 = 90 * 86400;
/// A rotated refresh token stays usable this long (reference REFRESH_GRACE_MS).
const REFRESH_GRACE: u64 = 2 * 3600;
/// How long a revocation must be remembered for access tokens: their lifetime + slack.
const REVOKE_TTL: u64 = ACCESS_TTL + 600;
/// A DID's revocations/takedowns read from another node (its owner) are
/// re-read this often.
const RELOAD_SECS: u64 = 10;
/// ... and when read from a partition this node owns (changes made here or
/// forwarded here invalidate it at once; this only bounds a missed one).
const LOCAL_RELOAD_SECS: u64 = 60;
const EMAIL_TOKEN_TTL_MS: u64 = 15 * 60 * 1000;
pub(super) const NEW_PASSWORD_MAX_LENGTH: usize = 256;
pub(super) const OLD_PASSWORD_MAX_LENGTH: usize = 512;

/// Private-name prefix of an account's revocations and takedowns.
pub(super) const SEC: &str = "sec/";
/// `sec/rvk/d/{before:016x}`: every session issued at or before `before`
/// (micros) is revoked until `exp`. One immutable row per revocation (the
/// newest dominates), so the GC can delete an expired one without racing a
/// newer revocation.
const REVOKED_ALL: &str = "sec/rvk/d/";
const REVOKED_FAMILY: &str = "sec/rvk/f/";
/// Takedown entries: `sec/td/rec/{collection}/{rkey}`, `sec/td/blob/{cid}`.
pub(super) const TAKEDOWN: &str = "sec/td/";

pub(super) const SCOPE_ACCESS: &str = "com.atproto.access";
pub(super) const SCOPE_APP_PASS: &str = "com.atproto.appPass";
pub(super) const SCOPE_APP_PASS_PRIVILEGED: &str = "com.atproto.appPassPrivileged";
pub(super) const SCOPE_REFRESH: &str = "com.atproto.refresh";
/// Access scope of a taken-down account's restricted session.
pub(super) const SCOPE_TAKENDOWN: &str = "com.atproto.takendown";

pub(super) fn now_secs() -> u64 {
    crate::tid::now_micros() / 1_000_000
}

pub(super) fn now_ms() -> u64 {
    crate::tid::now_micros() / 1000
}

fn err(status: StatusCode, error: &str, message: impl Into<String>) -> XrpcError {
    XrpcError {
        status,
        error: error.into(),
        message: message.into(),
    }
}

pub(super) fn invalid_request(message: impl Into<String>) -> XrpcError {
    XrpcError::bad("InvalidRequest", message)
}

fn invalid_token(message: &str) -> XrpcError {
    XrpcError::bad("InvalidToken", message)
}

fn expired_token(message: &str) -> XrpcError {
    XrpcError::bad("ExpiredToken", message)
}

fn auth_required(message: &str) -> XrpcError {
    err(StatusCode::UNAUTHORIZED, "AuthenticationRequired", message)
}

pub(super) fn takedown_error() -> XrpcError {
    err(
        StatusCode::UNAUTHORIZED,
        "AccountTakedown",
        "Account has been taken down",
    )
}

fn oauth_forbidden() -> XrpcError {
    err(
        StatusCode::FORBIDDEN,
        "Forbidden",
        "OAuth credentials are not supported for this endpoint",
    )
}

fn bad_scope() -> XrpcError {
    invalid_token("Bad token scope")
}

fn random_hex(n: usize) -> String {
    let b: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    hex::encode(b)
}

/// TS getRandomToken(): `xxxxx-xxxxx` in base32.
pub(super) fn random_token() -> String {
    let s = crate::cid::base32_encode(&rand::random::<[u8; 8]>());
    format!("{}-{}", &s[..5], &s[5..10])
}

pub(super) fn pmut(routing: &str, name: &str, val: Option<Vec<u8>>) -> Mutation {
    Mutation {
        key: state::private_key(routing, name).into(),
        val: val.map(Bytes::from),
    }
}

pub(super) fn to_json_bytes<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("serializable")
}

pub(super) async fn get_json<T: serde::de::DeserializeOwned>(
    app: &App,
    routing: &str,
    name: &str,
) -> XResult<Option<T>> {
    match app.get_private(routing, name).await? {
        Some(v) => Ok(Some(
            serde_json::from_slice(&v).map_err(XrpcError::from_err)?,
        )),
        None => Ok(None),
    }
}

/// All private entries of `routing` whose name starts with `name_prefix`:
/// (name, value) pairs.
pub(super) async fn scan_private(
    app: &App,
    routing: &str,
    name_prefix: &str,
) -> XResult<Vec<(String, Bytes)>> {
    let p = app.partition(routing)?;
    let base = state::private_prefix(routing);
    let lo = [base.as_slice(), name_prefix.as_bytes()].concat();
    let hi = state::prefix_end(&lo);
    let mut iter = p.db.scan(lo..hi).await.map_err(XrpcError::from_err)?;
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        out.push((
            String::from_utf8_lossy(&kv.key[base.len()..]).to_string(),
            kv.value,
        ));
    }
    Ok(out)
}

/// Every private entry whose routing key starts with `routing_prefix`,
/// across all partitions this node owns: (routing key, name, value).
pub(super) async fn scan_private_routing(
    app: &App,
    routing_prefix: &str,
) -> XResult<Vec<(String, String, Bytes)>> {
    // keys are slot-major: walk each slot's run of p/{routing_prefix}
    let fam = [state::PRIVATE_FAMILY, routing_prefix.as_bytes()].concat();
    let mut out = Vec::new();
    for p in app.partitions.owned() {
        let mut iter = state::FamilyScan::new(p.db.as_ref(), &fam, None, &Default::default())
            .await
            .map_err(XrpcError::from_err)?;
        while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
            let rest = String::from_utf8_lossy(&state::key_body(&kv.key)[state::PRIVATE_FAMILY.len()..]).to_string();
            if let Some((routing, name)) = rest.split_once('\0') {
                out.push((routing.to_string(), name.to_string(), kv.value));
            }
        }
    }
    Ok(out)
}

pub(super) fn set_extra(a: &mut Account, k: &str, v: J) {
    if v.is_null() {
        a.extra.remove(k);
    } else {
        a.extra.insert(k.to_string(), v);
    }
}

/// Derives `Account.status` from the flags owned here: a takedown wins over a
/// deactivation; other statuses (e.g. "suspended") set elsewhere are kept.
pub(super) fn recompute_status(a: &mut Account) {
    if a.extra.get("takedownRef").is_some_and(|v| !v.is_null()) {
        a.status = Some("takendown".into());
    } else if a.extra.get("deactivatedAt").is_some_and(|v| !v.is_null()) {
        a.status = Some("deactivated".into());
    } else if matches!(a.status.as_deref(), Some("takendown") | Some("deactivated")) {
        a.status = None;
    }
}

pub(super) fn is_takendown_account(a: &Account) -> bool {
    matches!(a.status.as_deref(), Some("takendown") | Some("suspended"))
}

/// (active, status) as in the reference's formatAccountStatus.
fn account_status(a: &Account) -> (bool, Option<String>) {
    (a.status.is_none(), a.status.clone())
}

pub(super) async fn verify_password(a: &Account, password: &str) -> bool {
    if password.len() > OLD_PASSWORD_MAX_LENGTH || a.password_hash.is_empty() {
        return false;
    }
    state::verify_password_hash(&a.password_hash, password).await
}

pub(super) fn valid_email(e: &str) -> bool {
    let Some((local, domain)) = e.rsplit_once('@') else {
        return false;
    };
    e.len() <= 254
        && !local.is_empty()
        && local.len() <= 64
        && !e.chars().any(|c| c.is_whitespace() || c.is_control())
        && !local.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && domain
            .split('.')
            .all(|l| !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
}

fn user_did(creds: &Credentials) -> XResult<String> {
    creds
        .did()
        .map(str::to_string)
        .ok_or_else(|| auth_required("user credentials required"))
}

/// Full-access session only (the reference's ACCESS_FULL; OAuth refused).
fn full_access(creds: &Credentials) -> XResult<String> {
    match creds {
        // (takendown tokens only reach the methods that accept them)
        Credentials::Session { did } | Credentials::Takendown { did } => Ok(did.clone()),
        Credentials::AppPassword { .. } => Err(bad_scope()),
        Credentials::OAuth { .. } => Err(oauth_forbidden()),
        Credentials::Admin => Err(auth_required("user credentials required")),
    }
}

/// Session or app password (the reference's ACCESS_STANDARD; OAuth refused).
fn standard_no_oauth(creds: &Credentials) -> XResult<String> {
    match creds {
        Credentials::Session { did }
        | Credentials::AppPassword { did, .. }
        | Credentials::Takendown { did } => Ok(did.clone()),
        Credentials::OAuth { .. } => Err(oauth_forbidden()),
        Credentials::Admin => Err(auth_required("user credentials required")),
    }
}

/// Full session, or OAuth holding `account:{attr}?action={action}`.
fn full_or_oauth_account(creds: &Credentials, attr: &str, action: &str) -> XResult<String> {
    match creds {
        Credentials::OAuth { did, .. } => {
            creds.require(creds.allows_account(attr, action))?;
            Ok(did.clone())
        }
        _ => full_access(creds),
    }
}

/// Session/app password, or OAuth holding `account:{attr}?action={action}`.
fn standard_or_oauth_account(creds: &Credentials, attr: &str, action: &str) -> XResult<String> {
    match creds {
        Credentials::OAuth { did, .. } => {
            creds.require(creds.allows_account(attr, action))?;
            Ok(did.clone())
        }
        _ => standard_no_oauth(creds),
    }
}

// ---------------------------------------------------------------------------
// per-App in-memory state: revocations, takedowns, dev mailbox, locks
// ---------------------------------------------------------------------------

pub(super) struct Ext {
    /// did -> its revocations and takedowns ([`ctl`]), at most the
    /// `security_controls` cap (src/caches.rs)
    ctl: Arc<RwLock<HashMap<String, Arc<Ctl>>>>,
    /// bumped by every change, so a load racing one is not cached
    gen: AtomicU64,
    pub(super) dev_mail: PMutex<HashMap<String, Vec<Mail>>>,
    locks: Vec<tokio::sync::Mutex<()>>,
}

static EXTS: RwLock<Vec<(usize, Arc<Ext>)>> = RwLock::new(Vec::new());

pub(super) fn ext(app: &App) -> Arc<Ext> {
    let id = app as *const App as usize;
    if let Some((_, e)) = EXTS.read().iter().find(|(k, _)| *k == id) {
        return e.clone();
    }
    let mut w = EXTS.write();
    if let Some((_, e)) = w.iter().find(|(k, _)| *k == id) {
        return e.clone();
    }
    let e = Arc::new(Ext {
        ctl: crate::caches::track(crate::caches::Cache::SecurityControls, Default::default()),
        gen: AtomicU64::new(0),
        dev_mail: PMutex::new(HashMap::new()),
        locks: (0..64).map(|_| tokio::sync::Mutex::new(())).collect(),
    });
    w.push((id, e.clone()));
    e
}

impl Ext {
    /// Serializes read-modify-write of one account/key on this node.
    pub(super) async fn lock(&self, key: &str) -> tokio::sync::MutexGuard<'_, ()> {
        self.locks[(state::did_hash(key) % self.locks.len() as u64) as usize]
            .lock()
            .await
    }
}

/// One account's session revocations and record/blob takedowns, as read from
/// its partition (`p/{did}\0sec/...`).
#[derive(Default)]
pub(super) struct Ctl {
    /// when it was read (unix secs)
    at: u64,
    /// (partition, epoch) when read from a partition this node owns; None =
    /// read from the owning node
    local: Option<(u16, u64)>,
    /// sessions issued at or before these micros are revoked (until exp secs)
    before: Option<(u64, u64)>,
    /// revoked session family -> expiry (unix secs)
    families: HashMap<String, u64>,
    /// takedown names below [`TAKEDOWN`]: `rec/{collection}/{rkey}`, `blob/{cid}`
    takedowns: HashSet<String>,
}

impl Ctl {
    fn is_revoked(&self, jti: Option<&str>, iat: u64) -> bool {
        let now = now_secs();
        let issued_us = jti
            .and_then(family_micros)
            .unwrap_or(iat.saturating_mul(1_000_000));
        if self.before.is_some_and(|(before, exp)| exp >= now && issued_us <= before) {
            return true;
        }
        jti.is_some_and(|j| self.families.get(j).is_some_and(|exp| *exp >= now))
    }

    /// `name` relative to [`TAKEDOWN`] (`rec/{collection}/{rkey}` or `blob/{cid}`).
    pub(super) fn has_takedown(&self, name: &str) -> bool {
        !self.takedowns.is_empty() && self.takedowns.contains(name)
    }
}

/// Issue time (micros) encoded in a session family id.
fn family_micros(jti: &str) -> Option<u64> {
    if jti.len() < 16 {
        return None;
    }
    u64::from_str_radix(&jti[..16], 16).ok()
}

fn new_family_id() -> String {
    format!("{:016x}{}", crate::tid::now_micros(), random_hex(8))
}

async fn load_sets(app: &App, did: &str, local: Option<(u16, u64)>) -> XResult<Ctl> {
    let rows = if local.is_some() {
        scan_private(app, did, SEC).await?
    } else {
        super::internal::scan_private_anywhere(app, did, SEC).await?
    };
    let now = now_secs();
    let mut c = Ctl { at: now, local, ..Default::default() };
    for (name, v) in rows {
        if let Some(td) = name.strip_prefix(TAKEDOWN) {
            c.takedowns.insert(td.to_string());
            continue;
        }
        let Ok(j) = serde_json::from_slice::<J>(&v) else {
            continue;
        };
        let exp = j["exp"].as_u64().unwrap_or(0);
        if exp < now {
            continue;
        }
        if name.starts_with(REVOKED_ALL) {
            let before = j["before"].as_u64().unwrap_or(0);
            if c.before.is_none_or(|(b, _)| before > b) {
                c.before = Some((before, exp));
            }
        } else if let Some(f) = name.strip_prefix(REVOKED_FAMILY) {
            c.families.insert(f.to_string(), exp);
        }
    }
    Ok(c)
}

/// `did`'s revocations and takedowns. Read from its partition and cached:
/// on the owning node until a change ([`ctl_changed`], also called for
/// changes forwarded here) or an ownership move; elsewhere re-read from the
/// owner every [`RELOAD_SECS`]. If the owner can't be reached the last view
/// (or none) is used, as before for the cluster-wide sets.
pub(super) async fn ctl(app: &App, did: &str) -> Arc<Ctl> {
    let e = ext(app);
    let now = now_secs();
    let local = app.partitions.for_key(did).map(|p| (p.id, p.epoch));
    let cached = e.ctl.read().get(did).cloned();
    if let Some(c) = &cached {
        let fresh = match (c.local, local) {
            (Some(a), Some(b)) => a == b && now.saturating_sub(c.at) < LOCAL_RELOAD_SECS,
            (None, None) => now.saturating_sub(c.at) < RELOAD_SECS,
            _ => false,
        };
        if fresh {
            return c.clone();
        }
    }
    let gen0 = e.gen.load(Ordering::SeqCst);
    match load_sets(app, did, local).await {
        Ok(c) => {
            let c = Arc::new(c);
            let mut m = e.ctl.write();
            // a change since the read began: use it for this check only
            if e.gen.load(Ordering::SeqCst) == gen0 {
                // full: evict the views older than RELOAD_SECS, else all of
                // them (a dropped view only costs a re-read)
                let cap = crate::caches::cap(crate::caches::Cache::SecurityControls);
                if m.len() >= cap && !m.contains_key(did) {
                    m.retain(|_, v| now.saturating_sub(v.at) < RELOAD_SECS);
                    if m.len() >= cap {
                        m.clear();
                    }
                }
                m.insert(did.to_string(), c.clone());
            }
            c
        }
        Err(err) => {
            tracing::warn!(%did, "loading session revocations/takedowns failed: {}", err.message);
            cached.unwrap_or_default()
        }
    }
}

/// Drops the cached view of `did` after a change to its `sec/` entries.
pub(super) fn ctl_changed(app: &App, did: &str) {
    let e = ext(app);
    e.gen.fetch_add(1, Ordering::SeqCst);
    e.ctl.write().remove(did);
}

/// Writes `did`'s `sec/` entries (in its partition, forwarded to the owner if
/// need be) and drops the cached view.
pub(super) async fn put_sec(app: &App, did: &str, muts: Vec<Mutation>) -> XResult<()> {
    let r = app.put_private(did, muts).await;
    ctl_changed(app, did);
    r
}

// ---------------------------------------------------------------------------
// mail
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub body: String,
    /// confirm_email | update_email | reset_password | delete_account | plc_operation | admin
    pub purpose: String,
    pub token: Option<String>,
    pub sent_at: String,
}

/// Outbound email. The default logs; a real SMTP/API mailer can be installed
/// once at startup with `set_mailer`.
pub trait Mailer: Send + Sync {
    fn send(&self, mail: &Mail);
}

pub struct LogMailer;

impl Mailer for LogMailer {
    fn send(&self, m: &Mail) {
        // Never the token or body at info: they are credentials. Dev mode
        // keeps them in the dev mailbox (vlpds.admin.getDevMail).
        tracing::info!(to = %m.to, subject = %m.subject, purpose = %m.purpose, "mail (log mailer: email disabled, not sent)");
        tracing::debug!(to = %m.to, purpose = %m.purpose, token = ?m.token, body = %m.body, "mail (log mailer) contents");
    }
}

static MAILER: std::sync::OnceLock<Box<dyn Mailer>> = std::sync::OnceLock::new();

#[allow(dead_code)]
pub fn set_mailer(m: Box<dyn Mailer>) -> bool {
    MAILER.set(m).is_ok()
}

/// Sends through the mailer; in dev mode also keeps it in the per-address
/// dev mailbox (vlpds.admin.getDevMail).
pub(super) fn deliver(
    app: &App,
    to: &str,
    subject: &str,
    body: &str,
    purpose: &str,
    token: Option<&str>,
) {
    let mail = Mail {
        to: to.to_string(),
        subject: subject.to_string(),
        body: body.to_string(),
        purpose: purpose.to_string(),
        token: token.map(str::to_string),
        sent_at: crate::events::now_rfc3339(),
    };
    // The node's own mailer (--email-smtp-url, crate::mail; queues, never
    // blocks), else the process-wide one (LogMailer unless `set_mailer`).
    match &app.config.mailer {
        Some(m) => m.send(&mail),
        None => MAILER.get_or_init(|| Box::new(LogMailer)).send(&mail),
    }
    if app.config.dev_mode {
        let e = ext(app);
        let mut box_ = e.dev_mail.lock();
        let v = box_.entry(to.to_ascii_lowercase()).or_default();
        v.push(mail);
        if v.len() > 50 {
            v.remove(0);
        }
    }
}

// ---------------------------------------------------------------------------
// email tokens
// ---------------------------------------------------------------------------

/// Only a keyed digest of the token is stored (bucket readers can't use a
/// live one): `token_hash` = [`email_token_digest`].
#[derive(serde::Serialize, serde::Deserialize)]
struct EmailToken {
    token_hash: String,
    requested_at: u64,
}

/// HMAC-SHA256 of an (uppercased) email token under a key derived from the
/// server secret, which isn't in the bucket. Tokens have ~50 bits and live
/// 15 minutes; an unkeyed hash of one could be brute-forced offline.
fn email_token_digest(app: &App, token: &str) -> String {
    let key = crate::oauth::util::derive_secret(&app.config.jwt_secret, "email-token");
    hex::encode(crate::oauth::util::hmac_sha256(&key, &[token.trim().to_ascii_uppercase().as_bytes()]))
}

pub(super) const EMAIL_PURPOSES: &[&str] = &[
    "confirm_email",
    "update_email",
    "reset_password",
    "delete_account",
    "plc_operation",
];

/// Creates (replacing any previous) the account's token for `purpose`.
pub(super) async fn create_email_token(app: &App, did: &str, purpose: &str) -> XResult<String> {
    let token = random_token().to_ascii_uppercase();
    let digest = email_token_digest(app, &token);
    let rec = EmailToken {
        token_hash: digest.clone(),
        requested_at: now_ms(),
    };
    app.put_private(
        did,
        vec![pmut(
            did,
            &format!("etok/{purpose}"),
            Some(to_json_bytes(&rec)),
        )],
    )
    .await?;
    if purpose == "reset_password" {
        let routing = format!("_reset:{digest}");
        app.put_private(
            &routing,
            vec![pmut(&routing, "t", Some(did.as_bytes().to_vec()))],
        )
        .await?;
    }
    Ok(token)
}

pub(super) async fn assert_email_token(
    app: &App,
    did: &str,
    purpose: &str,
    token: &str,
) -> XResult<()> {
    let rec: Option<EmailToken> = get_json(app, did, &format!("etok/{purpose}")).await?;
    let Some(rec) = rec else {
        return Err(invalid_token("Token is invalid"));
    };
    if !crate::auth::token_eq(&rec.token_hash, &email_token_digest(app, token)) {
        return Err(invalid_token("Token is invalid"));
    }
    if now_ms().saturating_sub(rec.requested_at) > EMAIL_TOKEN_TTL_MS {
        return Err(expired_token("Token is expired"));
    }
    Ok(())
}

pub(super) async fn delete_email_tokens(app: &App, did: &str, purposes: &[&str]) -> XResult<()> {
    let muts = purposes
        .iter()
        .map(|p| pmut(did, &format!("etok/{p}"), None))
        .collect();
    app.put_private(did, muts).await
}

// ---------------------------------------------------------------------------
// email claims (global uniqueness via conditional create, like handles)
// ---------------------------------------------------------------------------

fn email_path(app: &App, email: &str) -> object_store::path::Path {
    let h = hex::encode(Sha256::digest(email.to_ascii_lowercase().as_bytes()));
    object_store::path::Path::from(format!("{}/email/{}", app.store.prefix, h))
}

pub(super) async fn did_by_email(app: &App, email: &str) -> XResult<Option<String>> {
    match app.store.raw.get(&email_path(app, email)).await {
        Ok(r) => Ok(Some(
            String::from_utf8_lossy(&r.bytes().await.map_err(XrpcError::from_err)?).to_string(),
        )),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(XrpcError::from_err(e)),
    }
}

/// Claims `email` for `did`. Ok(false) when another account holds it.
pub(super) async fn claim_email(app: &App, email: &str, did: &str) -> XResult<bool> {
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match app
        .store
        .raw
        .put_opts(
            &email_path(app, email),
            PutPayload::from(did.as_bytes().to_vec()),
            opts,
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let holder = did_by_email(app, email).await?;
            if holder.as_deref() == Some(did) {
                return Ok(true);
            }
            // stale claim from a deleted account
            if let Some(h) = &holder {
                if app.partition(h).is_ok() && app.account(h).await.is_err() {
                    app.store
                        .raw
                        .put(
                            &email_path(app, email),
                            PutPayload::from(did.as_bytes().to_vec()),
                        )
                        .await
                        .map_err(XrpcError::from_err)?;
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(e) => Err(XrpcError::from_err(e)),
    }
}

/// Releases `email` if `did` holds it.
pub(super) async fn release_email(app: &App, email: &str, did: &str) {
    if did_by_email(app, email).await.ok().flatten().as_deref() == Some(did) {
        if let Err(e) = app.store.raw.delete(&email_path(app, email)).await {
            tracing::warn!(%did, "failed to release email claim: {e}");
        }
    }
}

fn handle_path(app: &App, handle: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/handle/{}", app.store.prefix, handle))
}

/// Claims `handle` for `did` (conditional create). Ok(false) when taken.
pub(super) async fn claim_handle(app: &App, handle: &str, did: &str) -> XResult<bool> {
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match app
        .store
        .raw
        .put_opts(
            &handle_path(app, handle),
            PutPayload::from(did.as_bytes().to_vec()),
            opts,
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => {
            Ok(app.resolve_handle(handle).await?.as_deref() == Some(did))
        }
        Err(e) => Err(XrpcError::from_err(e)),
    }
}

pub(super) async fn release_handle(app: &App, handle: &str, did: &str) {
    if app.resolve_handle(handle).await.ok().flatten().as_deref() == Some(did) {
        if let Err(e) = app.store.raw.delete(&handle_path(app, handle)).await {
            tracing::warn!(%did, %handle, "failed to release handle claim: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// handles
// ---------------------------------------------------------------------------

pub(super) fn normalize_handle(h: &str) -> XResult<String> {
    let h = h.trim().to_ascii_lowercase();
    if !super::syntax::valid_handle(&h) {
        return Err(XrpcError::bad(
            "InvalidHandle",
            "Input/handle must be a valid handle",
        ));
    }
    const DISALLOWED_TLDS: &[&str] = &[
        ".local",
        ".arpa",
        ".invalid",
        ".localhost",
        ".internal",
        ".example",
        ".alt",
        ".onion",
    ];
    if DISALLOWED_TLDS.iter().any(|t| h.ends_with(t)) {
        return Err(XrpcError::bad(
            "InvalidHandle",
            "Handle TLD is invalid or disallowed",
        ));
    }
    Ok(h)
}

fn is_reserved(front: &str) -> bool {
    RESERVED_HANDLES
        .split_ascii_whitespace()
        .any(|w| w == front)
}

/// Constraints for a handle under our service domain (reference
/// ensureHandleServiceConstraints). Non-service domains are refused
/// (UnsupportedDomain) for new accounts.
pub(super) fn ensure_service_handle(app: &App, handle: &str, allow_reserved: bool) -> XResult<()> {
    let suffix = format!(".{}", app.handle_domain);
    let Some(front) = handle.strip_suffix(&suffix) else {
        return Err(XrpcError::bad(
            "UnsupportedDomain",
            "Not a supported handle domain",
        ));
    };
    if front.contains('.') {
        return Err(XrpcError::bad(
            "InvalidHandle",
            "Invalid characters in handle",
        ));
    }
    if front.len() < 3 {
        return Err(XrpcError::bad("InvalidHandle", "Handle too short"));
    }
    if front.len() > 18 {
        return Err(XrpcError::bad("InvalidHandle", "Handle too long"));
    }
    if !allow_reserved && is_reserved(front) {
        return Err(XrpcError::bad("HandleNotAvailable", "Reserved handle"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct AppPassRef {
    pub name: String,
    pub privileged: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct RefreshState {
    family: String,
    exp: u64,
    #[serde(default)]
    app_password: Option<AppPassRef>,
    created_at: u64,
    /// Set once rotated: reuse within the grace period re-issues this id.
    #[serde(default)]
    next_id: Option<String>,
}

fn access_scope(ap: &Option<AppPassRef>) -> &'static str {
    match ap {
        None => SCOPE_ACCESS,
        Some(a) if a.privileged => SCOPE_APP_PASS_PRIVILEGED,
        Some(_) => SCOPE_APP_PASS,
    }
}

fn issue_pair(
    app: &App,
    did: &str,
    family: &str,
    refresh_id: &str,
    ap: &Option<AppPassRef>,
) -> (String, String) {
    (
        app.jwt
            .issue_with_jti(did, access_scope(ap), ACCESS_TTL, "at+jwt", Some(family)),
        app.jwt.issue_with_jti(
            did,
            SCOPE_REFRESH,
            REFRESH_TTL,
            "refresh+jwt",
            Some(refresh_id),
        ),
    )
}

/// Starts a new session family and returns (accessJwt, refreshJwt).
pub(super) async fn create_session_tokens(
    app: &App,
    did: &str,
    ap: Option<AppPassRef>,
) -> XResult<(String, String)> {
    create_session_tokens_scoped(app, did, ap, false).await
}

/// `takendown`: the access token gets the restricted `com.atproto.takendown`
/// scope (reference createSession for a soft-deleted account).
async fn create_session_tokens_scoped(
    app: &App,
    did: &str,
    ap: Option<AppPassRef>,
    takendown: bool,
) -> XResult<(String, String)> {
    let family = new_family_id();
    let rid = random_hex(24);
    let st = RefreshState {
        family: family.clone(),
        exp: now_secs() + REFRESH_TTL,
        app_password: ap.clone(),
        created_at: now_secs(),
        next_id: None,
    };
    app.put_private(
        did,
        vec![pmut(did, &format!("sess/{rid}"), Some(to_json_bytes(&st)))],
    )
    .await?;
    let (access, refresh) = issue_pair(app, did, &family, &rid, &ap);
    if takendown {
        let access =
            app.jwt
                .issue_with_jti(did, SCOPE_TAKENDOWN, ACCESS_TTL, "at+jwt", Some(&family));
        return Ok((access, refresh));
    }
    Ok((access, refresh))
}

/// Revokes session families of `did` (their access tokens), durably in its
/// partition.
async fn revoke_families(app: &App, did: &str, families: &[String]) -> XResult<()> {
    if families.is_empty() {
        return Ok(());
    }
    let exp = now_secs() + REVOKE_TTL;
    let muts = families
        .iter()
        .map(|fam| {
            pmut(
                did,
                &format!("{REVOKED_FAMILY}{fam}"),
                Some(to_json_bytes(&json!({"exp": exp}))),
            )
        })
        .collect();
    put_sec(app, did, muts).await
}

/// Revokes every session of `did` (refresh tokens deleted, outstanding access
/// tokens rejected). Used on password change, takedown and deletion.
pub(super) async fn revoke_all_sessions(app: &App, did: &str) -> XResult<()> {
    let before = crate::tid::now_micros();
    let exp = now_secs() + REVOKE_TTL;
    put_sec(
        app,
        did,
        vec![pmut(
            did,
            &format!("{REVOKED_ALL}{before:016x}"),
            Some(to_json_bytes(&json!({"before": before, "exp": exp}))),
        )],
    )
    .await?;
    revoke_refresh_tokens(app, did).await
}

/// For the private-row GC (`crate::oauth::gc`, which sweeps every `p/` row
/// of the partitions this node owns): Some(expired) for a session
/// revocation row (`sec/rvk/...`) of `routing`, None for any other row. A
/// revocation is expired once every token it revokes has (`exp`: issued
/// before it, so access-token lifetime + slack); unparseable rows count as
/// expired. Deleted accounts keep these rows until then, so a DID that comes
/// back can't revive old tokens; past it they are dropped like any other.
pub fn revocation_expired(routing: &str, name: &str, val: &[u8], now: u64) -> Option<bool> {
    if !routing.starts_with("did:") || !(name.starts_with(REVOKED_ALL) || name.starts_with(REVOKED_FAMILY)) {
        return None;
    }
    Some(serde_json::from_slice::<J>(val).ok().and_then(|j| j["exp"].as_u64()).is_none_or(|exp| exp < now))
}

/// Deletes an expired revocation row (see [`revocation_expired`]). Rows are
/// never rewritten once expired (a new revocation is a new `d/` row; a family
/// is revoked once, after its sessions are gone), so no lock is needed.
pub async fn drop_revocation(app: &App, did: &str, name: &str) -> XResult<()> {
    put_sec(app, did, vec![pmut(did, name, None)]).await
}

/// Deletes every refresh token (session) of `did`; outstanding access tokens
/// stay valid until they expire. What the reference does on takedown
/// (`revokeRefreshTokensByDid`), so the owner can still use the access token
/// for what a taken-down account may do (e.g. sync its own repo).
pub(super) async fn revoke_refresh_tokens(app: &App, did: &str) -> XResult<()> {
    let sessions = scan_private(app, did, "sess/").await?;
    if !sessions.is_empty() {
        app.put_private(
            did,
            sessions
                .iter()
                .map(|(name, _)| pmut(did, name, None))
                .collect(),
        )
        .await?;
    }
    Ok(())
}

/// Revokes the sessions created with app password `name`.
async fn revoke_app_password_sessions(app: &App, did: &str, name: &str) -> XResult<()> {
    let mut dels = Vec::new();
    let mut fams = Vec::new();
    for (k, v) in scan_private(app, did, "sess/").await? {
        let Ok(st) = serde_json::from_slice::<RefreshState>(&v) else {
            continue;
        };
        if st.app_password.as_ref().is_some_and(|a| a.name == name) {
            dels.push(pmut(did, &k, None));
            fams.push(st.family);
        }
    }
    if !dels.is_empty() {
        app.put_private(did, dels).await?;
    }
    revoke_families(app, did, &fams).await
}

/// Verifies a legacy Bearer token (session or app-password access JWT).
/// Hot path: cached signature check + claims ([`crate::auth::TokenCache`])
/// and the in-memory revocation check on every request, no storage reads.
pub async fn verify_bearer(app: &App, token: &str) -> XResult<Credentials> {
    let c = app
        .jwt
        .verify_signature_cached(token)
        .ok_or_else(|| invalid_token("Token could not be verified"))?;
    if c.aud != app.jwt.service_did || !c.sub.starts_with("did:") {
        return Err(invalid_token("Malformed token"));
    }
    if c.exp < now_secs() {
        return Err(expired_token("Token has expired"));
    }
    let creds = match c.scope.as_str() {
        SCOPE_ACCESS => Credentials::Session { did: c.sub.clone() },
        SCOPE_APP_PASS => Credentials::AppPassword {
            did: c.sub.clone(),
            privileged: false,
        },
        SCOPE_APP_PASS_PRIVILEGED => Credentials::AppPassword {
            did: c.sub.clone(),
            privileged: true,
        },
        SCOPE_TAKENDOWN => Credentials::Takendown { did: c.sub.clone() },
        _ => return Err(bad_scope()),
    };
    if ctl(app, &c.sub).await.is_revoked(c.jti.as_deref(), c.iat) {
        return Err(expired_token("Token has been revoked"));
    }
    Ok(creds)
}

/// Verifies the refresh token in the Authorization header.
fn refresh_claims(
    app: &App,
    headers: &HeaderMap,
    allow_expired: bool,
) -> XResult<crate::auth::Claims> {
    let tok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            err(
                StatusCode::UNAUTHORIZED,
                "AuthMissing",
                "Authentication Required",
            )
        })?;
    let c = app
        .jwt
        .verify_signature(tok.trim())
        .ok_or_else(|| invalid_token("Token could not be verified"))?;
    if c.scope != SCOPE_REFRESH {
        return Err(bad_scope());
    }
    if c.aud != app.jwt.service_did || c.jti.is_none() {
        return Err(invalid_token("Malformed token"));
    }
    if !allow_expired && c.exp < now_secs() {
        return Err(expired_token("Token has expired"));
    }
    Ok(c)
}

fn session_info(app: &App, a: &Account, include_email: bool) -> J {
    let (active, status) = account_status(a);
    let mut out = json!({
        "did": a.did,
        "handle": a.handle,
        "active": active,
    });
    if let Ok(doc) = super::identity::did_doc(app, a) {
        out["didDoc"] = doc;
    }
    if let Some(s) = status {
        out["status"] = json!(s);
    }
    if include_email {
        if let Some(e) = &a.email {
            out["email"] = json!(e);
        }
        out["emailConfirmed"] = json!(a.email_confirmed);
        out["emailAuthFactor"] = json!(false);
    }
    out
}

// ---------------------------------------------------------------------------
// describeServer / createAccount
// ---------------------------------------------------------------------------

pub(super) fn invites_required(app: &App) -> bool {
    app.config.invite_required
}

async fn describe_server(State(app): AppState) -> Json<J> {
    Json(json!({
        "did": app.jwt.service_did,
        "availableUserDomains": [format!(".{}", app.handle_domain)],
        "inviteCodeRequired": invites_required(&app),
        "links": {},
        "contact": {},
    }))
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(super) struct CreateAccountIn {
    pub handle: String,
    pub email: Option<String>,
    pub password: Option<String>,
    pub invite_code: Option<String>,
    pub did: Option<String>,
    pub plc_op: Option<J>,
}

/// Account extension flag: the DID was brought from elsewhere (migration
/// in), so its document is not ours to generate: activation and
/// checkAccountStatus check the resolved one.
pub(super) const EXTERNAL_DID: &str = "externalDid";

pub(super) fn has_external_did(a: &Account) -> bool {
    a.extra.get(EXTERNAL_DID).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// createAccount, with the reference's optional service auth
/// (`userServiceAuthOptional`): a Bearer token must be a service JWT for
/// this method, and its issuer may then bring its own DID (migration in).
async fn create_account(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<CreateAccountIn>,
) -> XResult<Json<J>> {
    const LXM: &str = "com.atproto.server.createAccount";
    let requester = super::authn::optional_service_auth(&app, &headers, LXM).await?;
    let acct = create_account_inner(&app, inp, requester.as_ref().map(|r| r.did())).await?;
    let (access, refresh) = create_session_tokens(&app, &acct.did, None).await?;
    let mut out = json!({
        "handle": acct.handle,
        "did": acct.did,
        "accessJwt": access,
        "refreshJwt": refresh,
    });
    if let Some(doc) = account_did_doc(&app, &acct).await {
        out["didDoc"] = doc;
    }
    Ok(Json(out))
}

/// The DID document to report for an account (reference safeResolveDidDoc
/// with a forced refresh): the resolved one for a DID brought here, ours
/// otherwise.
async fn account_did_doc(app: &App, a: &Account) -> Option<J> {
    if has_external_did(a) {
        app.did_resolver.invalidate(&a.did);
        return app.did_resolver.resolve(&a.did).await.ok().map(|d| (*d).clone());
    }
    super::identity::did_doc(app, a).ok()
}

/// Account creation (createAccount, which then starts a session, and the
/// OAuth sign-up page), as the reference's local-PDS path
/// (validateInputsForLocalPds + createAccount):
/// - `plcOp` is refused; there is no PLC registration here.
/// - Without `did`, a DID is minted and the handle must be under our domain.
/// - With `did` (migration in), `requester` (the verified service-auth
///   issuer) must be that DID. The account starts deactivated with an empty
///   repo, a fresh signing key and no firehose events; the user then imports
///   the repo and blobs, points the DID document here and activates it
///   (activateAccount checks the document). The handle may be external
///   (checked to resolve to the DID).
/// - An invite code, when given, must be available and its use is recorded,
///   whether or not invites are required.
pub(super) async fn create_account_inner(
    app: &App,
    inp: CreateAccountIn,
    requester: Option<&str>,
) -> XResult<Account> {
    if inp.plc_op.is_some() {
        return Err(invalid_request("Unsupported input: \"plcOp\""));
    }
    let password = inp
        .password
        .ok_or_else(|| invalid_request("Password is required"))?;
    if password.len() > NEW_PASSWORD_MAX_LENGTH {
        return Err(invalid_request(format!(
            "Password too long. Maximum length is {NEW_PASSWORD_MAX_LENGTH} characters."
        )));
    }
    let invite = inp
        .invite_code
        .as_deref()
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_string);
    if invites_required(app) && invite.is_none() {
        return Err(XrpcError::bad(
            "InvalidInviteCode",
            "No invite code provided",
        ));
    }
    let email = match inp
        .email
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty())
    {
        // required, as in the reference (no self-delete / password reset without one)
        None => return Err(invalid_request("Email is required")),
        Some(e) => {
            let e = e.to_ascii_lowercase();
            if !valid_email(&e) {
                return Err(invalid_request(
                    "This email address is not supported, please use a different email.",
                ));
            }
            e
        }
    };
    let handle = normalize_handle(&inp.handle)?;
    let (did, external) = match inp.did.as_deref() {
        Some(d) => {
            // (checked before the handle, whose proof may be fetched)
            if requester != Some(d) {
                return Err(auth_required(&format!(
                    "Missing auth to create account with did: {d}"
                )));
            }
            if !is_atproto_did(d) {
                return Err(invalid_request("Invalid DID"));
            }
            // the handle may be external if it resolves to the DID
            super::identity::check_new_handle(app, &handle, d).await?;
            if account_if_exists(app, d).await?.is_some() {
                return Err(invalid_request("Account already exists"));
            }
            (d.to_string(), true)
        }
        None => {
            ensure_service_handle(app, &handle, false)?;
            (app.mint_local_did()?, false)
        }
    };
    // Atomic use of the invite code (a conditional-create claim per use).
    let claim = match &invite {
        Some(code) => Some(super::admin::claim_invite_use(app, code, &did).await?),
        None => None,
    };
    let release = |claim: Option<super::admin::InviteClaim>| async move {
        if let Some(c) = claim {
            super::admin::release_invite_use(app, c).await;
        }
    };
    // Global handle and email uniqueness across nodes: conditional creates
    // of handle/{handle} and the email claim. Both store round trips and the
    // password hash (~20 ms of CPU on the blocking pool) run concurrently:
    // one after the other they were most of createAccount's latency, and at
    // a fixed concurrency its rate.
    // The signing key is wrapped (a KMS call in production) alongside.
    let key = Arc::new(Keypair::generate());
    let (h, e, password_hash, wrapped) = tokio::join!(
        claim_handle(app, &handle, &did),
        claim_email(app, &email, &did),
        state::hash_password(&password),
        app.secrets.wrap_signing_key(&did, &key)
    );
    let (h_ok, e_ok) = (matches!(h, Ok(true)), matches!(e, Ok(true)));
    let (wrapped_signing_key, signing_pubkey) = match wrapped {
        Ok(w) => w,
        Err(err) => {
            if h_ok {
                release_handle(app, &handle, &did).await;
            }
            if e_ok {
                release_email(app, &email, &did).await;
            }
            release(claim).await;
            return Err(err.into());
        }
    };
    if !(h_ok && e_ok) {
        if h_ok {
            release_handle(app, &handle, &did).await;
        }
        if e_ok {
            release_email(app, &email, &did).await;
        }
        release(claim).await;
        if !h? {
            return Err(XrpcError::bad("HandleNotAvailable", format!("Handle already taken: {handle}")));
        }
        e?;
        return Err(invalid_request(format!("Email already taken: {email}")));
    }
    let mut acct = Account {
        did: did.clone(),
        handle: handle.clone(),
        wrapped_signing_key,
        signing_pubkey,
        created_at: crate::events::now_rfc3339(),
        email: Some(email.clone()),
        ..Default::default()
    };
    acct.password_hash = password_hash;
    set_extra(&mut acct, "totpEnabled", json!(false));
    if let Some(code) = &invite {
        set_extra(&mut acct, "invitedBy", json!(code));
    }
    if external {
        // deactivated until the migration completes (activateAccount); the
        // worker sequences no events for an account created inactive
        set_extra(&mut acct, EXTERNAL_DID, json!(true));
        set_extra(&mut acct, "deactivatedAt", json!(crate::events::now_rfc3339()));
        recompute_status(&mut acct);
    }
    let did_arc: Arc<str> = did.clone().into();
    let (tx, rx) = oneshot::channel();
    let sent = app
        .workers
        .route(&did)
        .send(WorkerMsg::CreateRepo(CreateRepoReq {
            did: did_arc,
            handle: handle.clone(),
            key,
            account_json: Bytes::from(serde_json::to_vec(&acct).unwrap()),
            records: Vec::new(),
            reply: tx,
        }));
    let created = match sent {
        Ok(()) => rx
            .await
            .map_err(|_| XrpcError::internal("worker dropped request"))
            .and_then(|r| r.map_err(XrpcError::from)),
        Err(e) => Err(XrpcError::from_err(e)),
    };
    if let Err(e) = created {
        release_handle(app, &handle, &did).await;
        release_email(app, &email, &did).await;
        release(claim).await;
        return Err(e);
    }
    if let Some(c) = &claim {
        if let Err(e) = super::admin::record_invite_use(app, c, &did).await {
            tracing::warn!(%did, "recording invite use failed: {}", e.message);
        }
    }
    Ok(acct)
}

// ---------------------------------------------------------------------------
// createSession / getSession / refreshSession / deleteSession
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSessionIn {
    identifier: String,
    password: String,
    auth_factor_token: Option<String>,
    #[serde(default)]
    allow_takendown: bool,
}

/// Resolves a login identifier (handle, DID or email) to a local account.
/// The account a login identifier names; Ok(None) if there is none.
/// Unavailability (the account's shard moving between nodes) is an error,
/// not "no such account", so a client retries instead of being told its
/// credentials are wrong.
pub(super) async fn login_account(app: &App, identifier: &str) -> XResult<Option<Account>> {
    let ident = identifier.trim().to_ascii_lowercase();
    let did = if ident.contains('@') {
        match did_by_email(app, &ident).await? {
            Some(d) => d,
            None => return Ok(None),
        }
    } else {
        match app.resolve_repo(&ident).await {
            Ok(d) => d.to_string(),
            Err(e) if e.status.is_client_error() => return Ok(None),
            Err(e) => return Err(e),
        }
    };
    let Some(a) = account_if_exists(app, &did).await? else {
        return Ok(None);
    };
    if ident.contains('@') && a.email.as_deref() != Some(ident.as_str()) {
        return Ok(None);
    }
    Ok(Some(a))
}

/// `app.account`, with "no such account" as Ok(None) and every other error
/// (e.g. 503 while the shard moves) passed on.
pub(super) async fn account_if_exists(app: &App, did: &str) -> XResult<Option<Account>> {
    match app.account(did).await {
        Ok(a) => Ok(Some(a)),
        Err(e) if e.error == "AccountNotFound" => Ok(None),
        Err(e) => Err(e),
    }
}

/// App passwords are server-generated with ~80 bits of randomness, so a fast
/// deterministic hash is safe here (no dictionary to attack) and lets us look
/// them up by hash. User-chosen account passwords use Argon2id instead.
fn app_password_hash(did: &str, password: &str) -> String {
    hex::encode(Sha256::digest(
        format!("vlpds-app-password:{did}:{password}").as_bytes(),
    ))
}

async fn verify_app_password(app: &App, did: &str, password: &str) -> XResult<Option<AppPassRef>> {
    let h = app_password_hash(did, password.trim());
    let Some(name) = app.get_private(did, &format!("apphash/{h}")).await? else {
        return Ok(None);
    };
    let name = String::from_utf8_lossy(&name).to_string();
    let meta: Option<J> = get_json(app, did, &format!("apppass/{name}")).await?;
    Ok(meta.map(|m| AppPassRef {
        name,
        privileged: m["privileged"].as_bool().unwrap_or(false),
    }))
}

async fn create_session(
    State(app): AppState,
    Json(inp): Json<CreateSessionIn>,
) -> XResult<Json<J>> {
    if inp.password.len() > OLD_PASSWORD_MAX_LENGTH {
        return Err(auth_required(
            "Password too long. Consider resetting your password.",
        ));
    }
    // reference: 300/day and 30/5min per `${identifier}-${ip}`, normalized
    // like the OAuth sign-in's key (whose buckets these are too), so case
    // variants of one handle or email share a bucket instead of each
    // getting a fresh one
    {
        use crate::ratelimit::*;
        let key = inp.identifier.trim().trim_start_matches('@').to_lowercase();
        check_with_ip(&[&CREATE_SESSION_DAY, &CREATE_SESSION_5MIN], &key, 1)?;
    }
    let invalid = || auth_required("Invalid identifier or password");
    let acct = login_account(&app, &inp.identifier)
        .await?
        .ok_or_else(invalid)?;
    let soft_deleted = is_takendown_account(&acct);
    let mut app_pass = None;
    if !verify_password(&acct, &inp.password).await {
        // takendown/suspended accounts cannot log in with an app password
        if soft_deleted {
            return Err(invalid());
        }
        app_pass = Some(
            verify_app_password(&app, &acct.did, &inp.password)
                .await?
                .ok_or_else(invalid)?,
        );
    }
    if soft_deleted && !inp.allow_takendown {
        return Err(takedown_error());
    }
    // second factor for password logins (app passwords bypass it)
    if app_pass.is_none() {
        crate::totp::check_second_factor(&app, &acct, inp.auth_factor_token.as_deref()).await?;
    }
    let (access, refresh) =
        create_session_tokens_scoped(&app, &acct.did, app_pass, soft_deleted).await?;
    let mut out = session_info(&app, &acct, true);
    out["accessJwt"] = json!(access);
    out["refreshJwt"] = json!(refresh);
    Ok(Json(out))
}

async fn get_session(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = user_did(&creds)?;
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request(format!("Could not find user info for account: {did}")))?;
    let include_email =
        !matches!(creds, Credentials::OAuth { .. }) || creds.allows_account("email", "read");
    Ok(Json(session_info(&app, &acct, include_email)))
}

async fn refresh_session(State(app): AppState, headers: HeaderMap) -> XResult<Json<J>> {
    let c = refresh_claims(&app, &headers, false)?;
    let did = c.sub.clone();
    let rid = c.jti.clone().unwrap_or_default();
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request(format!("Could not find user info for account: {did}")))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let e = ext(&app);
    // serializes racing refreshes of one account on this node
    let _g = e.lock(&did).await;
    let st: Option<RefreshState> = get_json(&app, &did, &format!("sess/{rid}")).await?;
    // revoked (deleteSession, password change, ...) or past its grace period
    let now = now_secs();
    let st = st.filter(|s| s.exp >= now).ok_or_else(|| expired_token("Token has been revoked"))?;
    if ctl(&app, &did).await.is_revoked(Some(&st.family), 0) {
        return Err(expired_token("Token has been revoked"));
    }
    // Rotation as in the reference: the old token stays usable for a grace
    // period (min(2h, its expiry)) and reuse yields the same next token id;
    // after that it is rejected.
    let next = st.next_id.clone().unwrap_or_else(|| random_hex(24));
    let mut muts = vec![pmut(
        &did,
        &format!("sess/{rid}"),
        Some(to_json_bytes(&RefreshState { exp: st.exp.min(now + REFRESH_GRACE), next_id: Some(next.clone()), ..st.clone() })),
    )];
    if app.get_private(&did, &format!("sess/{next}")).await?.is_none() {
        let next_st = RefreshState { exp: now + REFRESH_TTL, created_at: now, next_id: None, ..st.clone() };
        muts.push(pmut(&did, &format!("sess/{next}"), Some(to_json_bytes(&next_st))));
    }
    app.put_private(&did, muts).await?;
    let (access, refresh) = issue_pair(&app, &did, &st.family, &next, &st.app_password);
    let mut out = session_info(&app, &acct, true);
    out["accessJwt"] = json!(access);
    out["refreshJwt"] = json!(refresh);
    Ok(Json(out))
}

async fn delete_session(State(app): AppState, headers: HeaderMap) -> XResult<StatusCode> {
    let c = refresh_claims(&app, &headers, true)?;
    let did = c.sub.clone();
    let rid = c.jti.clone().unwrap_or_default();
    let e = ext(&app);
    let _g = e.lock(&did).await;
    if let Some(st) = get_json::<RefreshState>(&app, &did, &format!("sess/{rid}")).await? {
        // the whole session: this token, its rotations and their access tokens
        let mut dels = vec![pmut(&did, &format!("sess/{rid}"), None)];
        for (name, v) in scan_private(&app, &did, "sess/").await? {
            if serde_json::from_slice::<RefreshState>(&v).is_ok_and(|o| o.family == st.family) {
                dels.push(pmut(&did, &name, None));
            }
        }
        app.put_private(&did, dels).await?;
        revoke_families(&app, &did, &[st.family]).await?;
    }
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// app passwords
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateAppPasswordIn {
    name: String,
    #[serde(default)]
    privileged: Option<bool>,
}

async fn create_app_password(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<CreateAppPasswordIn>,
) -> XResult<Json<J>> {
    let did = full_access(&creds)?;
    let acct = app.account(&did).await?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let name = inp.name.trim().to_string();
    if name.is_empty() || name.len() > 256 || name.contains('\0') {
        return Err(invalid_request("Invalid app password name"));
    }
    let privileged = inp.privileged.unwrap_or(false);
    let e = ext(&app);
    let _g = e.lock(&did).await;
    if app
        .get_private(&did, &format!("apppass/{name}"))
        .await?
        .is_some()
    {
        return Err(invalid_request("could not create app-specific password"));
    }
    let s = crate::cid::base32_encode(&rand::random::<[u8; 10]>());
    let password = format!("{}-{}-{}-{}", &s[0..4], &s[4..8], &s[8..12], &s[12..16]);
    let created_at = crate::events::now_rfc3339();
    let h = app_password_hash(&did, &password);
    let meta = json!({"name": name, "createdAt": created_at, "privileged": privileged, "hash": h});
    app.put_private(
        &did,
        vec![
            pmut(&did, &format!("apppass/{name}"), Some(to_json_bytes(&meta))),
            pmut(
                &did,
                &format!("apphash/{h}"),
                Some(name.as_bytes().to_vec()),
            ),
        ],
    )
    .await?;
    Ok(Json(
        json!({"name": name, "password": password, "createdAt": created_at, "privileged": privileged}),
    ))
}

async fn list_app_passwords(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = standard_no_oauth(&creds)?;
    let mut out: Vec<J> = scan_private(&app, &did, "apppass/")
        .await?
        .into_iter()
        .filter_map(|(_, v)| serde_json::from_slice::<J>(&v).ok())
        .map(|m| json!({"name": m["name"], "createdAt": m["createdAt"], "privileged": m["privileged"].as_bool().unwrap_or(false)}))
        .collect();
    out.sort_by(|a, b| b["createdAt"].as_str().cmp(&a["createdAt"].as_str()));
    Ok(Json(json!({"passwords": out})))
}

#[derive(Deserialize)]
struct RevokeAppPasswordIn {
    name: String,
}

async fn revoke_app_password(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<RevokeAppPasswordIn>,
) -> XResult<StatusCode> {
    // app passwords can't revoke app passwords (stricter than the reference)
    let did = full_access(&creds)?;
    let e = ext(&app);
    let _g = e.lock(&did).await;
    let name = inp.name.trim().to_string();
    if let Some(meta) = get_json::<J>(&app, &did, &format!("apppass/{name}")).await? {
        let mut muts = vec![pmut(&did, &format!("apppass/{name}"), None)];
        if let Some(h) = meta["hash"].as_str() {
            muts.push(pmut(&did, &format!("apphash/{h}"), None));
        }
        app.put_private(&did, muts).await?;
    }
    revoke_app_password_sessions(&app, &did, &name).await?;
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// account lifecycle
// ---------------------------------------------------------------------------

/// Read-modify-write of an account, applied by the repo's worker to its
/// current state (App::mutate_account). Returns the account as written.
pub(super) async fn update_account<F>(
    app: &App,
    did: &str,
    identity_event: bool,
    account_event: bool,
    f: F,
) -> XResult<Account>
where
    F: FnOnce(&mut Account) -> XResult<()> + Send + 'static,
{
    let (_, a) = app
        .mutate_account(did, identity_event, account_event, false, |a| f(a).map(|_| true))
        .await?;
    Ok(a)
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DeactivateIn {
    delete_after: Option<String>,
}

pub(super) async fn set_deactivated(
    app: &App,
    did: &str,
    deactivated: bool,
    delete_after: Option<String>,
) -> XResult<Account> {
    update_account(app, did, !deactivated, true, move |a| {
        if deactivated {
            if a.extra.get("deactivatedAt").is_none_or(|v| v.is_null()) {
                set_extra(a, "deactivatedAt", json!(crate::events::now_rfc3339()));
            }
            set_extra(
                a,
                "deleteAfter",
                delete_after.map(J::String).unwrap_or(J::Null),
            );
        } else {
            if a.extra.get("takedownRef").is_some_and(|v| !v.is_null()) {
                return Err(XrpcError::bad("AccountNotFound", "user not found"));
            }
            set_extra(a, "deactivatedAt", J::Null);
            set_extra(a, "deleteAfter", J::Null);
        }
        recompute_status(a);
        Ok(())
    })
    .await
}

async fn deactivate_account(
    State(app): AppState,
    Auth(creds): Auth,
    body: Option<Json<DeactivateIn>>,
) -> XResult<StatusCode> {
    let did = full_or_oauth_account(&creds, "status", "manage")?;
    let inp = body.map(|Json(b)| b).unwrap_or_default();
    if let Some(d) = &inp.delete_after {
        if chrono::DateTime::parse_from_rfc3339(d).is_err() {
            return Err(invalid_request("deleteAfter must be a valid datetime"));
        }
    }
    app.account(&did)
        .await
        .map_err(|_| invalid_request("Account not found"))?;
    set_deactivated(&app, &did, true, inp.delete_after).await?;
    Ok(StatusCode::OK)
}

async fn activate_account(State(app): AppState, Auth(creds): Auth) -> XResult<StatusCode> {
    let did = match &creds {
        Credentials::OAuth { .. } => {
            return Err(err(
                StatusCode::FORBIDDEN,
                "Forbidden",
                "Account reactivation is not available with OAuth credentials. Sign in to your account management page to reactivate.",
            ))
        }
        _ => full_access(&creds)?,
    };
    let acct = app
        .account(&did)
        .await
        .map_err(|_| XrpcError::bad("AccountNotFound", "user not found"))?;
    assert_valid_did_doc(&app, &acct).await?;
    // #account, #identity and #sync (reference sequenceAccountActivation)
    app.mutate_account(&did, true, true, true, |a| {
        // a taken-down account can't be activated
        if a.extra.get("takedownRef").is_some_and(|v| !v.is_null()) {
            return Err(XrpcError::bad("AccountNotFound", "user not found"));
        }
        set_extra(a, "deactivatedAt", J::Null);
        set_extra(a, "deleteAfter", J::Null);
        recompute_status(a);
        Ok(true)
    })
    .await?;
    Ok(StatusCode::OK)
}

/// Reference assertValidDidDocumentForService: the account's DID document
/// names this PDS and the account's signing key. A DID minted here is
/// documented by this server (it is never registered elsewhere), so only a
/// DID brought here (migration in) is resolved and checked. There is no
/// rotation-key check: this PDS holds no PLC rotation key.
async fn assert_valid_did_doc(app: &App, a: &Account) -> XResult<()> {
    if !has_external_did(a) {
        return Ok(());
    }
    app.did_resolver.invalidate(&a.did);
    let doc = app
        .did_resolver
        .resolve(&a.did)
        .await
        .map_err(|_| invalid_request("Could not resolve DID"))?;
    let pds = crate::did_resolver::service_endpoint(&doc, "atproto_pds");
    if pds.as_deref().map(|p| p.trim_end_matches('/')) != Some(app.public_url.trim_end_matches('/')) {
        return Err(invalid_request(
            "DID document atproto_pds service endpoint does not match PDS public url",
        ));
    }
    if crate::did_resolver::signing_key_multibase(&doc).as_deref() != Some(a.signing_pubkey.as_str()) {
        return Err(invalid_request(
            "DID document verification method does not match expected signing key",
        ));
    }
    Ok(())
}

/// Migration progress (reference checkAccountStatus): `repoBlocks` counts
/// the commit, the MST nodes and the distinct record blocks; `expectedBlobs`
/// the distinct blobs the records reference; `importedBlobs` the blobs
/// stored for the account (uploaded, referenced or not yet).
async fn check_account_status(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = user_did(&creds)?;
    let acct = app.account(&did).await?;
    let (view, snap) = app.repo_view(&did).await?;
    // the whole tree, streamed from the snapshot (`M/` + one `R/` scan)
    let (d, root) = (did.clone(), view.head.data);
    let nodes = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let mut nodes = HashSet::new();
        let scan = crate::mst_store::ScanSource::open(&*snap, &d, crate::mst_store::DbSource::new(&*snap, &d, &rt), &rt)?;
        crate::mst_lazy::export_blocks(root, 1, &scan, &mut |c, b| {
            // the empty tree's root isn't counted
            if crate::mst::decode_node(b, c).is_ok_and(|n| !n.entries.is_empty()) {
                nodes.insert(c);
            }
        })?;
        Ok::<_, crate::mst::MstError>(nodes)
    })
    .await
    .map_err(XrpcError::from_err)?
    .map_err(XrpcError::from_err)?;
    let p = app.partition(&did)?;
    let prefix = state::record_prefix(&did);
    let mut iter =
        p.db.scan(prefix.clone()..state::prefix_end(&prefix))
            .await
            .map_err(XrpcError::from_err)?;
    let mut records = 0u64;
    let mut record_blocks = HashSet::new();
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        records += 1;
        if let Ok((cid, _)) = state::decode_record_value(&kv.value) {
            record_blocks.insert(cid);
        }
    }
    let bprefix = state::blob_ref_prefix(&did);
    let mut iter =
        p.db.scan(bprefix.clone()..state::prefix_end(&bprefix))
            .await
            .map_err(XrpcError::from_err)?;
    let mut expected = HashSet::new();
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        let rest = String::from_utf8_lossy(&kv.key[bprefix.len()..]).to_string();
        expected.insert(rest.split('\0').next().unwrap_or("").to_string());
    }
    let blob_dir = object_store::path::Path::from(format!("{}/blob/{}", app.store.prefix, did));
    let mut imported = 0u64;
    let mut list = app.store.raw.list(Some(&blob_dir));
    while let Some(meta) = futures::StreamExt::next(&mut list).await {
        meta.map_err(XrpcError::from_err)?;
        imported += 1;
    }
    let valid_did = assert_valid_did_doc(&app, &acct).await.is_ok();
    Ok(Json(json!({
        "activated": acct.status.is_none(),
        "validDid": valid_did,
        "repoCommit": view.head.commit.to_string(),
        "repoRev": view.head.rev.to_string(),
        "repoBlocks": 1 + nodes.len() + record_blocks.len(),
        "indexedRecords": records,
        "privateStateValues": 0,
        "expectedBlobs": expected.len(),
        "importedBlobs": imported,
    })))
}

async fn request_account_delete(State(app): AppState, Auth(creds): Auth) -> XResult<StatusCode> {
    let did = full_access(&creds)?;
    {
        use crate::ratelimit::*;
        check(&[&REQUEST_ACCOUNT_DELETE_DAY, &REQUEST_ACCOUNT_DELETE_HOUR], &did, 1)?;
    }
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request("account not found"))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let email = acct
        .email
        .clone()
        .ok_or_else(|| invalid_request("account does not have an email address"))?;
    let token = create_email_token(&app, &did, "delete_account").await?;
    deliver(
        &app,
        &email,
        "Account Deletion Request",
        &format!("Your account deletion code is {token}"),
        "delete_account",
        Some(&token),
    );
    Ok(StatusCode::OK)
}

/// Deletes an account entirely: sessions, repo + account (#account deleted
/// event), handle and email claims, private state.
pub(super) async fn delete_account_fully(app: &App, did: &str) -> XResult<()> {
    let acct = app.account(did).await.ok();
    revoke_all_sessions(app, did).await?;
    app.account_op(did, AccountOp::Delete).await?;
    if let Some(a) = &acct {
        release_handle(app, &a.handle, did).await;
        if let Some(e) = &a.email {
            release_email(app, e, did).await;
        }
    }
    // revocations stay (TTL'd), so a DID that comes back (migration) doesn't
    // revive access tokens issued before
    let mut private = scan_private(app, did, "").await?;
    private.retain(|(name, _)| !name.starts_with(REVOKED_ALL) && !name.starts_with(REVOKED_FAMILY));
    for chunk in private.chunks(500) {
        app.put_private(
            did,
            chunk
                .iter()
                .map(|(name, _)| pmut(did, name, None))
                .collect(),
        )
        .await?;
    }
    ctl_changed(app, did);
    Ok(())
}

#[derive(Deserialize)]
struct DeleteAccountIn {
    did: String,
    password: String,
    token: String,
}

async fn delete_account(
    State(app): AppState,
    Json(inp): Json<DeleteAccountIn>,
) -> XResult<StatusCode> {
    if inp.password.len() > OLD_PASSWORD_MAX_LENGTH {
        return Err(auth_required(
            "Password too long. Consider resetting your password.",
        ));
    }
    let acct = app
        .account(&inp.did)
        .await
        .map_err(|_| invalid_request("account not found"))?;
    if !verify_password(&acct, &inp.password).await {
        return Err(auth_required("Invalid did or password"));
    }
    assert_email_token(&app, &acct.did, "delete_account", &inp.token).await?;
    delete_account_fully(&app, &acct.did).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize, Default)]
struct ReserveSigningKeyIn {
    did: Option<String>,
}

/// Unclaimed reserved signing keys expire after this long (swept by
/// [`spawn_reserved_key_gc`]; an expired reservation can't be taken).
pub const RESERVED_KEY_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

/// Private-state routing key of a reservation: by did:key for the key
/// itself, by DID for the per-DID index (`{"signingKey", "createdAt"}`).
fn reserved_routing(id: &str) -> String {
    format!("_reserved:{id}")
}

fn reservation_expired(rec: &J, ttl: std::time::Duration) -> bool {
    let created = rec["createdAt"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
    match created {
        Some(t) => chrono::Utc::now().signed_duration_since(t).to_std().unwrap_or_default() >= ttl,
        None => true,
    }
}

/// Reserves a signing key; `admin.updateAccountSigningKey` can later
/// install it (the PDS must hold the private key to sign commits). With
/// `did`, the same key comes back while its reservation is live (reference
/// actorStore.reserveKeypair keys the reservation by DID).
async fn reserve_signing_key(
    State(app): AppState,
    body: Option<Json<ReserveSigningKeyIn>>,
) -> XResult<Json<J>> {
    let inp = body.map(|Json(b)| b).unwrap_or_default();
    let did = inp.did.filter(|d| !d.is_empty());
    if let Some(did) = &did {
        if !did.starts_with("did:") {
            return Err(invalid_request("did must be a DID"));
        }
        let idx = reserved_routing(did);
        if let Some(rec) = get_json::<J>(&app, &idx, "k").await? {
            let dk = rec["signingKey"].as_str().unwrap_or("").to_string();
            let live = !reservation_expired(&rec, RESERVED_KEY_TTL)
                && get_json::<J>(&app, &reserved_routing(&dk), "k").await?.is_some();
            if live {
                return Ok(Json(json!({"signingKey": dk})));
            }
        }
    }
    let key = Keypair::generate();
    let did_key = key.did_key();
    let routing = reserved_routing(&did_key);
    let now = crate::events::now_rfc3339();
    // wrapped, bound to the did:key it is reserved under
    let wrapped = app.secrets.wrap(crate::secrets::Purpose::ReservedKey, &did_key, &key.to_bytes()).await?;
    let rec = json!({"key": wrapped, "did": did, "createdAt": now});
    app.put_private(
        &routing,
        vec![pmut(&routing, "k", Some(to_json_bytes(&rec)))],
    )
    .await?;
    if let Some(did) = &did {
        let idx = reserved_routing(did);
        let rec = json!({"signingKey": did_key, "createdAt": now});
        app.put_private(&idx, vec![pmut(&idx, "k", Some(to_json_bytes(&rec)))])
            .await?;
    }
    Ok(Json(json!({"signingKey": did_key})))
}

/// Takes a key reserved with reserveSigningKey (by its did:key), clearing
/// the reservation and its per-DID index. Expired reservations are gone.
pub(super) async fn take_reserved_key(app: &App, did_key: &str) -> XResult<Option<Keypair>> {
    let routing = reserved_routing(did_key);
    let Some(rec) = get_json::<J>(app, &routing, "k").await? else {
        return Ok(None);
    };
    // unwrap before consuming the reservation: with the key service down
    // the caller retries and the reservation is still there
    let raw = if reservation_expired(&rec, RESERVED_KEY_TTL) {
        None
    } else {
        let blob = rec["key"].as_str().unwrap_or("");
        Some(app.secrets.unwrap(crate::secrets::Purpose::ReservedKey, did_key, blob).await?.plaintext)
    };
    app.put_private(&routing, vec![pmut(&routing, "k", None)])
        .await?;
    if let Some(did) = rec["did"].as_str() {
        let idx = reserved_routing(did);
        app.put_private(&idx, vec![pmut(&idx, "k", None)]).await?;
    }
    let Some(raw) = raw else {
        return Ok(None);
    };
    let key = Keypair::from_bytes(&raw).map_err(XrpcError::from_err)?;
    Ok(Some(key))
}

/// Deletes reservations (keys and per-DID indexes) older than `ttl` in the
/// partitions this node owns. Returns how many were removed.
pub async fn sweep_reserved_keys(app: &App, ttl: std::time::Duration) -> Result<usize, XrpcError> {
    let mut n = 0;
    for (routing, name, val) in scan_private_routing(app, "_reserved:").await? {
        let expired = serde_json::from_slice::<J>(&val)
            .map(|rec| reservation_expired(&rec, ttl))
            .unwrap_or(true);
        if expired {
            app.put_private(&routing, vec![pmut(&routing, &name, None)])
                .await?;
            n += 1;
        }
    }
    Ok(n)
}

/// Background sweep of expired signing-key reservations (hourly).
pub fn spawn_reserved_key_gc(app: Arc<App>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match sweep_reserved_keys(&app, RESERVED_KEY_TTL).await {
                Ok(n) if n > 0 => tracing::info!(removed = n, "reserved signing key gc"),
                Ok(_) => {}
                Err(e) => tracing::warn!("reserved signing key gc: {}", e.message),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// email flows
// ---------------------------------------------------------------------------

async fn request_email_confirmation(
    State(app): AppState,
    Auth(creds): Auth,
) -> XResult<StatusCode> {
    let did = standard_or_oauth_account(&creds, "email", "manage")?;
    {
        use crate::ratelimit::*;
        check(&[&REQUEST_EMAIL_CONFIRMATION_DAY, &REQUEST_EMAIL_CONFIRMATION_HOUR], &did, 1)?;
    }
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request("account not found"))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let email = acct
        .email
        .clone()
        .ok_or_else(|| invalid_request("account does not have an email address"))?;
    let token = create_email_token(&app, &did, "confirm_email").await?;
    deliver(
        &app,
        &email,
        "Confirm your email",
        &format!("Your email confirmation code is {token}"),
        "confirm_email",
        Some(&token),
    );
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct ConfirmEmailIn {
    email: String,
    token: String,
}

async fn confirm_email(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<ConfirmEmailIn>,
) -> XResult<StatusCode> {
    let did = standard_or_oauth_account(&creds, "email", "manage")?;
    let acct = app
        .account(&did)
        .await
        .map_err(|_| XrpcError::bad("AccountNotFound", "user not found"))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    if acct.email.as_deref() != Some(inp.email.trim().to_ascii_lowercase().as_str()) {
        return Err(XrpcError::bad("InvalidEmail", "invalid email"));
    }
    assert_email_token(&app, &did, "confirm_email", &inp.token).await?;
    delete_email_tokens(&app, &did, &["confirm_email"]).await?;
    update_account(&app, &did, false, false, move |a| {
        a.email_confirmed = true;
        set_extra(a, "emailConfirmedAt", json!(crate::events::now_rfc3339()));
        Ok(())
    })
    .await?;
    Ok(StatusCode::OK)
}

async fn request_email_update(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = full_or_oauth_account(&creds, "email", "manage")?;
    {
        use crate::ratelimit::*;
        check(&[&REQUEST_EMAIL_UPDATE_DAY, &REQUEST_EMAIL_UPDATE_HOUR], &did, 1)?;
    }
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request("account not found"))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let email = acct
        .email
        .clone()
        .ok_or_else(|| invalid_request("account does not have an email address"))?;
    let token_required = acct.email_confirmed;
    if token_required {
        let token = create_email_token(&app, &did, "update_email").await?;
        deliver(
            &app,
            &email,
            "Update your email",
            &format!("Your email update code is {token}"),
            "update_email",
            Some(&token),
        );
    }
    Ok(Json(json!({"tokenRequired": token_required})))
}

/// Sets a new (unconfirmed) email: claims it globally, releases the old one,
/// clears email tokens.
pub(super) async fn set_email(app: &App, did: &str, email: &str) -> XResult<()> {
    let email = email.trim().to_ascii_lowercase();
    if !valid_email(&email) {
        return Err(invalid_request(
            "This email address is not supported, please use a different email.",
        ));
    }
    let acct = app.account(did).await?;
    if acct.email.as_deref() == Some(email.as_str()) {
        return Ok(());
    }
    if !claim_email(app, &email, did).await? {
        return Err(invalid_request(
            "This email address is already in use, please use a different email.",
        ));
    }
    let new = email.clone();
    let res = app
        .mutate_account(did, false, false, false, move |a| {
            if a.email.as_deref() == Some(new.as_str()) {
                return Ok(false);
            }
            a.email = Some(new);
            a.email_confirmed = false;
            set_extra(a, "emailConfirmedAt", J::Null);
            Ok(true)
        })
        .await;
    let before = match res {
        Ok((before, _)) => before,
        Err(e) => {
            release_email(app, &email, did).await;
            return Err(e);
        }
    };
    // the email replaced is the one the worker saw, not the one read above
    if let Some(o) = before.email.filter(|o| *o != email) {
        release_email(app, &o, did).await;
    }
    delete_email_tokens(app, did, EMAIL_PURPOSES).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateEmailIn {
    email: String,
    token: Option<String>,
    email_auth_factor: Option<bool>,
}

async fn update_email(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateEmailIn>,
) -> XResult<StatusCode> {
    // app passwords can't change the email (stricter than the reference)
    let did = full_or_oauth_account(&creds, "email", "manage")?;
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request(format!("Could not find user info for account: {did}")))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let email = inp.email.trim().to_ascii_lowercase();
    if inp.email_auth_factor == Some(true) {
        return Err(invalid_request(
            "Email two-factor authentication is not supported by this server; use TOTP",
        ));
    }
    if !valid_email(&email) {
        return Err(invalid_request(
            "This email address is not supported, please use a different email.",
        ));
    }
    match inp.token.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => assert_email_token(&app, &did, "update_email", t).await?,
        None if acct.email_confirmed => {
            return Err(XrpcError::bad(
                "TokenRequired",
                "confirmation token required",
            ))
        }
        None => {}
    }
    set_email(&app, &did, &email).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct RequestPasswordResetIn {
    email: String,
}

async fn request_password_reset(
    State(app): AppState,
    Json(inp): Json<RequestPasswordResetIn>,
) -> XResult<StatusCode> {
    let email = inp.email.trim().to_ascii_lowercase();
    let acct = match did_by_email(&app, &email).await? {
        Some(did) => account_if_exists(&app, &did)
            .await?
            .filter(|a| a.email.as_deref() == Some(email.as_str())),
        None => None,
    };
    let Some(acct) = acct else {
        return Err(invalid_request("account does not have an email address"));
    };
    let token = create_email_token(&app, &acct.did, "reset_password").await?;
    deliver(
        &app,
        &email,
        "Password Reset Requested",
        &format!("Hi {}, your password reset code is {token}", acct.handle),
        "reset_password",
        Some(&token),
    );
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct ResetPasswordIn {
    token: String,
    password: String,
}

/// The account a password-reset token was issued for (a global lookup; HA
/// routing sends resetPassword to that account's owner).
pub async fn reset_token_did(app: &App, token: &str) -> XResult<Option<String>> {
    let routing = format!("_reset:{}", email_token_digest(app, token));
    Ok(app
        .get_private(&routing, "t")
        .await?
        .map(|v| String::from_utf8_lossy(&v).to_string()))
}

/// Sets a new password and revokes every session, OAuth grants included.
pub(super) async fn change_password(app: &App, did: &str, password: &str) -> XResult<()> {
    let hash = state::hash_password(password).await;
    update_account(app, did, false, false, move |a| {
        a.password_hash = hash.clone();
        Ok(())
    })
    .await?;
    delete_email_tokens(app, did, &["reset_password"]).await?;
    crate::oauth::store::revoke_all_sessions(app, did)
        .await
        .map_err(|e| XrpcError::from_err(e.description))?;
    revoke_all_sessions(app, did).await
}

async fn reset_password(
    State(app): AppState,
    Json(inp): Json<ResetPasswordIn>,
) -> XResult<StatusCode> {
    if inp.password.len() > NEW_PASSWORD_MAX_LENGTH {
        return Err(invalid_request("Invalid password length."));
    }
    let token = inp.token.trim().to_ascii_uppercase();
    let routing = format!("_reset:{}", email_token_digest(&app, &token));
    let did = reset_token_did(&app, &token)
        .await?
        .ok_or_else(|| invalid_token("Token is invalid"))?;
    assert_email_token(&app, &did, "reset_password", &token).await?;
    change_password(&app, &did, &inp.password).await?;
    app.put_private(&routing, vec![pmut(&routing, "t", None)])
        .await?;
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// invites (user side + admin creation)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateInviteCodeIn {
    use_count: i64,
    for_account: Option<String>,
}

fn require_admin(creds: &Credentials) -> XResult<()> {
    match creds {
        Credentials::Admin => Ok(()),
        _ => Err(auth_required("admin credentials required")),
    }
}

async fn create_invite_code(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<CreateInviteCodeIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let account = inp.for_account.unwrap_or_else(|| "admin".into());
    let code = super::admin::gen_invite_code(&app);
    super::admin::create_invites(
        &app,
        &account,
        std::slice::from_ref(&code),
        inp.use_count,
        false,
    )
    .await?;
    Ok(Json(json!({"code": code})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateInviteCodesIn {
    #[serde(default = "one")]
    code_count: usize,
    use_count: i64,
    for_accounts: Option<Vec<String>>,
}

fn one() -> usize {
    1
}

async fn create_invite_codes(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<CreateInviteCodesIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let accounts = inp.for_accounts.unwrap_or_else(|| vec!["admin".into()]);
    let mut out = Vec::new();
    for account in accounts {
        let codes: Vec<String> = (0..inp.code_count.min(1000))
            .map(|_| super::admin::gen_invite_code(&app))
            .collect();
        super::admin::create_invites(&app, &account, &codes, inp.use_count, false).await?;
        out.push(json!({"account": account, "codes": codes}));
    }
    Ok(Json(json!({"codes": out})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountInviteCodesQ {
    include_used: Option<bool>,
    #[allow(dead_code)]
    create_available: Option<bool>,
}

async fn get_account_invite_codes(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<AccountInviteCodesQ>,
) -> XResult<Json<J>> {
    let did = full_access(&creds)?;
    let acct = app
        .account(&did)
        .await
        .map_err(|_| XrpcError::bad("NotFound", "Account not found"))?;
    if is_takendown_account(&acct) {
        return Err(takedown_error());
    }
    let include_used = q.include_used.unwrap_or(true);
    let codes: Vec<J> = super::admin::account_invites(&app, &did)
        .await?
        .into_iter()
        .filter(|c| !c.disabled && (include_used || (c.uses.len() as i64) < c.available))
        .map(|c| serde_json::to_value(c).unwrap())
        .collect();
    Ok(Json(json!({"codes": codes})))
}

// ---------------------------------------------------------------------------
// getServiceAuth
// ---------------------------------------------------------------------------

/// Methods that must be called directly, never via service auth (reference PROTECTED_METHODS).
const PROTECTED_METHODS: &[&str] = &[
    "com.atproto.admin.sendEmail",
    "com.atproto.identity.requestPlcOperationSignature",
    "com.atproto.identity.signPlcOperation",
    "com.atproto.identity.updateHandle",
    "com.atproto.server.activateAccount",
    "com.atproto.server.confirmEmail",
    "com.atproto.server.createAppPassword",
    "com.atproto.server.deactivateAccount",
    "com.atproto.server.getAccountInviteCodes",
    "com.atproto.server.getSession",
    "com.atproto.server.listAppPasswords",
    "com.atproto.server.requestAccountDelete",
    "com.atproto.server.requestEmailConfirmation",
    "com.atproto.server.requestEmailUpdate",
    "com.atproto.server.revokeAppPassword",
    "com.atproto.server.updateEmail",
];

/// Methods that need a privileged credential (reference PRIVILEGED_METHODS:
/// chat.bsky.* and createAccount).
/// (Matched case-insensitively, like the reference's LxmSet.)
fn privileged_method(lxm: &str) -> bool {
    let l = lxm.to_ascii_lowercase();
    l.starts_with("chat.bsky.") || l == "com.atproto.server.createaccount"
}

fn protected_method(lxm: &str) -> bool {
    PROTECTED_METHODS.iter().any(|m| m.eq_ignore_ascii_case(lxm))
}

#[derive(Deserialize)]
struct ServiceAuthQ {
    aud: String,
    exp: Option<i64>,
    lxm: Option<String>,
}

async fn get_service_auth(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<ServiceAuthQ>,
) -> XResult<Json<J>> {
    let did = user_did(&creds)?;
    let lxm = q.lxm.as_deref().filter(|l| !l.is_empty());
    let (aud_did, fragment) = match q.aud.split_once('#') {
        Some((d, f)) => (d, Some(f)),
        None => (q.aud.as_str(), None),
    };
    if !is_atproto_did(aud_did) || fragment.is_some_and(|f| f.is_empty()) {
        return Err(invalid_request(
            "aud must be a valid atproto DID or did#serviceId reference",
        ));
    }
    match &creds {
        Credentials::OAuth { .. } => creds.require(creds.allows_rpc(lxm.unwrap_or("*"), &q.aud))?,
        Credentials::AppPassword {
            privileged: false, ..
        } => {
            if let Some(l) = lxm.filter(|l| privileged_method(l)) {
                return Err(invalid_request(format!("insufficient access to request a service auth token for the following method: {l}")));
            }
        }
        _ => {}
    }
    let acct = app.account(&did).await?;
    if is_takendown_account(&acct) && lxm != Some("com.atproto.server.createAccount") {
        return Err(bad_scope());
    }
    let now = now_secs() as i64;
    let ttl = match q.exp {
        Some(exp) => {
            let diff = exp - now;
            if diff < 0 {
                return Err(XrpcError::bad("BadExpiration", "expiration is in past"));
            } else if diff > 3600 {
                return Err(XrpcError::bad(
                    "BadExpiration",
                    "cannot request a token with an expiration more than an hour in the future",
                ));
            } else if lxm.is_none() && diff > 60 {
                return Err(XrpcError::bad("BadExpiration", "cannot request a method-less token with an expiration more than a minute in the future"));
            }
            diff as u64
        }
        None => 60,
    };
    if let Some(l) = lxm.filter(|l| protected_method(l)) {
        return Err(invalid_request(format!(
            "cannot request a service auth token for the following protected method: {l}"
        )));
    }
    let key = app.secrets.account_signing_key(&acct).await?;
    let token = crate::auth::service_auth_jwt(&key, &did, &q.aud, lxm, ttl);
    Ok(Json(json!({"token": token})))
}

/// did:plc (24 base32 chars) or did:web without a path; a port only for
/// localhost (reference @atproto/did isAtprotoDid).
pub(super) fn is_atproto_did(s: &str) -> bool {
    if let Some(id) = s.strip_prefix("did:plc:") {
        return id.len() == 24 && id.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'));
    }
    if let Some(host) = s.strip_prefix("did:web:") {
        return super::syntax::valid_did(s)
            && !host.contains(':')
            && (!host.contains("%3A") || host.starts_with("localhost%3A"));
    }
    false
}

// ---------------------------------------------------------------------------
// temp.*
// ---------------------------------------------------------------------------

async fn check_signup_queue(Auth(creds): Auth) -> XResult<Json<J>> {
    standard_no_oauth(&creds)?;
    Ok(Json(json!({"activated": true})))
}

#[derive(Deserialize)]
struct HandleAvailabilityQ {
    handle: String,
    email: Option<String>,
}

async fn handle_available(app: &App, handle: &str) -> XResult<bool> {
    if ensure_service_handle(app, handle, false).is_err() {
        return Ok(false);
    }
    Ok(app.resolve_handle(handle).await?.is_none())
}

async fn check_handle_availability(
    State(app): AppState,
    Query(q): Query<HandleAvailabilityQ>,
) -> XResult<Json<J>> {
    if let Some(e) = q.email.as_deref().filter(|e| !e.is_empty()) {
        if !valid_email(&e.to_ascii_lowercase()) {
            return Err(XrpcError::bad(
                "InvalidEmail",
                "An invalid email was provided.",
            ));
        }
    }
    let handle = normalize_handle(&q.handle)?;
    if handle_available(&app, &handle).await? {
        return Ok(Json(json!({
            "handle": handle,
            "result": {"$type": "com.atproto.temp.checkHandleAvailability#resultAvailable"},
        })));
    }
    // suggestions: the first label plus random digits, under our domain
    let suffix = format!(".{}", app.handle_domain);
    let base: String = handle
        .split('.')
        .next()
        .unwrap_or("user")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(14)
        .collect();
    let base = if base.len() < 3 {
        format!("{base}user")
    } else {
        base
    };
    let mut suggestions = Vec::new();
    for _ in 0..12 {
        if suggestions.len() >= 3 {
            break;
        }
        let cand = format!("{base}{}{suffix}", rand::random::<u16>() % 10_000);
        if handle_available(&app, &cand).await?
            && !suggestions.iter().any(|s: &J| s["handle"] == cand)
        {
            suggestions.push(json!({"handle": cand, "method": "random_digits"}));
        }
    }
    Ok(Json(json!({
        "handle": handle,
        "result": {"$type": "com.atproto.temp.checkHandleAvailability#resultUnavailable", "suggestions": suggestions},
    })))
}

// ---------------------------------------------------------------------------
// TOTP (vlpds.server.*Totp)
// ---------------------------------------------------------------------------

fn session_only(creds: &Credentials) -> XResult<String> {
    full_access(creds)
}

async fn set_totp_flag(app: &App, did: &str, enabled: bool) -> XResult<()> {
    update_account(app, did, false, false, move |a| {
        set_extra(a, "totpEnabled", json!(enabled));
        Ok(())
    })
    .await
    .map(|_| ())
}

async fn setup_totp(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = session_only(&creds)?;
    let acct = app.account(&did).await?;
    let _g = crate::totp::lock(&did).await;
    let mut st = crate::totp::load(&app, &did).await?;
    if st.enabled() {
        return Err(invalid_request("TOTP is already enabled; disable it first"));
    }
    let secret = crate::totp::base32_encode(&crate::totp::generate_secret());
    st.pending = Some(secret.clone());
    crate::totp::save(&app, &did, &st).await?;
    let issuer = app
        .public_url
        .split("://")
        .nth(1)
        .unwrap_or(&app.public_url)
        .split(['/', ':'])
        .next()
        .unwrap_or("vlpds")
        .to_string();
    let uri = crate::totp::otpauth_uri(&secret, &issuer, &acct.handle);
    Ok(Json(json!({"secret": secret, "uri": uri})))
}

#[derive(Deserialize)]
struct ConfirmTotpIn {
    code: String,
}

async fn confirm_totp(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<ConfirmTotpIn>,
) -> XResult<Json<J>> {
    let did = session_only(&creds)?;
    let codes = {
        let _g = crate::totp::lock(&did).await;
        let mut st = crate::totp::load(&app, &did).await?;
        if st.enabled() {
            return Err(invalid_request("TOTP is already enabled"));
        }
        let pending = st.pending.clone().ok_or_else(|| {
            invalid_request("No pending TOTP setup; call vlpds.server.setupTotp first")
        })?;
        let secret = crate::totp::base32_decode(&pending)
            .ok_or_else(|| XrpcError::internal("corrupt pending TOTP secret"))?;
        let step = crate::totp::verify_code(&secret, &inp.code, crate::totp::now_secs(), 0)
            .ok_or_else(|| invalid_token("Token is invalid"))?;
        let codes = crate::totp::generate_recovery_codes();
        st.secret = Some(pending);
        st.pending = None;
        st.last_step = step;
        st.recovery = codes
            .iter()
            .map(|c| crate::totp::hash_recovery_code(c))
            .collect();
        st.enabled_at = Some(crate::events::now_rfc3339());
        // flag first: a crash between the two writes must not leave TOTP
        // enabled with the login fast path (totpEnabled=false) skipping it
        set_totp_flag(&app, &did, true).await?;
        crate::totp::save(&app, &did, &st).await?;
        codes
    };
    Ok(Json(json!({"enabled": true, "recoveryCodes": codes})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DisableTotpIn {
    code: Option<String>,
    recovery_code: Option<String>,
    password: String,
}

async fn disable_totp(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<DisableTotpIn>,
) -> XResult<StatusCode> {
    let did = session_only(&creds)?;
    let acct = app.account(&did).await?;
    if !verify_password(&acct, &inp.password).await {
        return Err(auth_required("Invalid password"));
    }
    {
        let _g = crate::totp::lock(&did).await;
        let mut st = crate::totp::load(&app, &did).await?;
        if !st.enabled() {
            return Err(invalid_request("TOTP is not enabled"));
        }
        let code = inp
            .code
            .as_deref()
            .or(inp.recovery_code.as_deref())
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| invalid_request("code or recoveryCode is required"))?;
        // counts toward the lockout like a sign-in attempt
        if let Err(e) = crate::totp::attempt(&mut st, code, crate::totp::now_secs()) {
            crate::totp::save(&app, &did, &st).await?;
            return Err(e);
        }
        crate::totp::save(&app, &did, &crate::totp::TotpState::default()).await?;
    }
    set_totp_flag(&app, &did, false).await?;
    Ok(StatusCode::OK)
}

async fn get_totp_status(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = standard_no_oauth(&creds)?;
    let st = crate::totp::load(&app, &did).await?;
    let mut out = json!({
        "enabled": st.enabled(),
        "pending": st.pending.is_some(),
        "recoveryCodesRemaining": st.recovery.len(),
    });
    if let Some(at) = &st.enabled_at {
        out["enabledAt"] = json!(at);
    }
    Ok(Json(out))
}

/// Reserved service-domain handle labels (from the reference PDS).
const RESERVED_HANDLES: &str = concat!(
    "10downingstreet 10ronaldinho 3gerardpique about abuse access account accounts aclu acme activate activities ",
    "activity ad add address adele adm admanager admin administration administrator administrators admins ads ",
    "adsense adult advertising adwords affiliate affiliatepage affiliates afp ajax akiko_lawson akshaykumar aliaa08 ",
    "aliciakeys all alpha amitshah analysis analytics andresiniesta8 android anon anonymous answer answers ",
    "anushkasharma aoc ap api apis app appengine appnews apps archive archives arianagrande ariyoshihiroiki ",
    "arrahman article arvindkejriwal asahi asdf asset assets at atp auth authentication avatar avrillavigne backup ",
    "bank banner banners barackobama base bbcbreaking bbcworld beginners beingsalmankhan beta beyonce billgates ",
    "billieeilish billing bin binaries binary blackberry blog blogs blogsearch bluesky board book bookmark ",
    "bookmarks books bot bots brasildefato britneyspears brunomars bsky bts_bighit bts_twt bug bugs business buy ",
    "buzz cache calendar call campaign cancel captcha career careers cart carterjwm catalog catalogs categories ",
    "category cdn cgi cgi-bin championsleague changelog chart charts chat check checked checking checkout ",
    "chrisbrown claudialeitte client cliente clients clients1 cnarne cnnbrk code coldplay comercial comment ",
    "comments communities community company compare compras conanobrien config configuration confirm confirmation ",
    "connect contact contact-us contact_us contacts contactus content contest contribute contributor contributors ",
    "coppa copyright copyrights core corp correio countries country cpanel create cristiano css cssproxy customise ",
    "customize danieltosh dashboard data davidguetta db ddlovato deepikapadukone default delete demo design ",
    "designer desktop destroy dev devel developer developers devs diagram diary dict dictionary did die dir ",
    "direct-messages direct_messages directory dist diversity dl dmca doc docs documentation documentations ",
    "documents domain domains donate download downloads dozle_official drake dril e e-mail earth ecommerce edit ",
    "editor edits edu education elisapie ellendegeneres elonmusk em_com email embed embedded eminem emmawatson ",
    "employment employments empty enable encrypted end engine enterprise enterprises entries entry error errorlog ",
    "errors estadao eval event example examplecommunity exampleopenid examplesyn examplesyndicated exampleusername ",
    "exchange exit explore famima_now faq faqs favorite favorites favourite favourites fcbarcelona feature features ",
    "feed feedback feedburner feedproxy feeds ff_xiv_jp file files finance first folder folders folha following ",
    "forgot form forms forum forums founder foxnews free friend friends ftp fuck fujitv fun fusion gadget gadgets ",
    "game games gazetadopovo gears general geographic get gettingstarted gift gifts gigazine gist git github gmail ",
    "go golang goto gov graph graphs gretathunberg group groups guest guests guide guides hack hacks hajimesyacho ",
    "handle harry_styles head help hikakin hillaryclinton home homepage host hosting hostmaster hostname how-to ",
    "how_to howto html htrnl http httpd https i iamges iamsrk icon icons id idea ideas ihrithik im imac image ",
    "images imap img imvkohli inbox inboxes index indexes info information inquiry instagram intranet investor ",
    "investors invitation invitations invite invoice invoices ios ipad iphone irc irnages irng is issue issues it ",
    "item items ivetesangalo jairbolsonaro java javascript jimmyfallon jlo job jobs jocx joebiden join ",
    "jornaldobrasil jornaloglobo jotx js json jtimberlake jump justinbieber kaka kamalaharris kanyewest katyperry ",
    "kb kendalljenner kevinhart4real khloekardashian kimkardashian kingjames kiyo_saiore knowledge-base ",
    "knowledgebase kourtneykardash kremlinrussia_e kyliejenner lab labs ladygaga language languages last ",
    "ldap-status ldap_status ldapstatus legal leomessi lex lexicon liampayne license licenses liltunechi link links ",
    "linux list lists livejournal lj local locale location log log-in log-out log_in log_out login logout logs ",
    "lucianohuck lulaoficial m mac mac-os mac-os-x mac_os_x macos macosx mail mailer mailing main mainichi ",
    "maintenance manage manager manual manutd map maps marcosmion mariahcarey marketing master matsu_bouzu me media ",
    "member members memories memory merchandise message messages messenger mg microblog microblogs mileycyrus mine ",
    "mis misc mms mob mobile model models mohamadalarefe money movie movies mp3 mp4 msg msn music mx my mymme mysql ",
    "name named nan naomiosaka narendramodi nasa natgeo navi navigation nba net network networks new news ",
    "newsletter neymarjr nfl nhk niallofficial nick nickiminaj nickname nike nikkei nil nintendo none notes ",
    "noticias notification notifications notify npr ns ns1 ns2 ns3 ns4 ns5 nsid ntv null nytimes oauth ",
    "oauth-clients oauth_clients ocsp offer offers official old onedirection online oowareware1945 openid operator ",
    "oprah option options order orders org organization organizations other overview owner owners p0rn pack page ",
    "pager pages paid pamyurin panel partner partnerpage partners password patch paulocoelho pay payment pds people ",
    "perl person phone photo photoalbum photos php phpmyadmin phppgadmin phpredisadmin pic pics picture pictures ",
    "ping pink pitbull pixel places plan plans playstation plc plugin plugins pmoindia podcasts poke_times policies ",
    "policy pop pop3 popular porn portal portalr7 portals post postfix postmaster posts potus pr pr0n premierleague ",
    "premium press price pricing principles print privacy privacy-policy privacy_policy privacypolicy private ",
    "priyankachopra prod product production products profile profiles project projects promo promotions proxies ",
    "proxy pub public purchase purpose put python queries query radio random ranking read reader readme ",
    "realdonaldtrump recent recruit recruitment rede_globo redirect register registration release remove replies ",
    "repo report reports repositories repository req request requests research reset resolve resolver review ",
    "ricky_martin rihanna rnail rnicrosoft roc rolaworld rondesantisfl root rss ruby rule sachin_rt sag sale sales ",
    "sample samples sandbox save scholar school schools script scripts search secure security seikintv selenagomez ",
    "self seminars send server server-info server-status server_info server_status servers service services session ",
    "sessions setting settings setup shakira share shawnmendes shop shopping shortcut shortcuts show sign-in ",
    "sign-up sign_in sign_up signin signout signup site sitemap sitemaps sitenews sites sketchup sky slash ",
    "slashinvoice slut smartphone sms smtp snoopdogg soap software sorry source spec special sportscenter ",
    "spreadsheet spreadsheets sql srbachchan src srntp ssh ssl ssladmin ssladministrator sslwebmaster ssytem staff ",
    "stage staging starbucksjapan start stat state static statistics stats status store stores stories style ",
    "styleguide styles stylesheet stylesheets subdomain subhisharma100 subscribe subscription subscriptions suggest ",
    "suggestqueries support survey surveys surveytool svn swf syn sync syndicated sys sysadmin sysadministrator ",
    "sysadmins system tablet tablets tag tags talk talkgadget task tasks taylorswift taylorswift13 tbs tbs_pr team ",
    "teams tech telnet term terms terms-of-service terms_of_service termsofservice test testing tests text ",
    "theeconomist theme themes therock thread threads ticket tickets tid tmp to-do to_do todo toml tool toolbar ",
    "toolbars tools top topic topics tos tour trac trace translate translation translations translator trends ",
    "tutorial tux tv tvasahi tvtokyo twitter txt ukraine ul undef unfollow unsubscribe update updates upgrade ",
    "upgrades upi upload uploads url usage user username usernames users uuid validation validations ver version ",
    "video video-stats videos virendersehwag visitor visitors voice volunteer volunteers w washingtonpost watch ",
    "wave weather web webdisk webhook webhooks webmail webmaster webmasters webrnail website websites welcome ",
    "whitehouse45 whm whois widget widgets wifi wiki wikis win windows wizkhalifa word work works workshop wpad ww ",
    "wws www wwws wwww xfn xhtml xhtrnl xml xmpp xpg xrpc xxx yaml year yml yokoono yomiuri_online you yourdomain ",
    "yourname yoursite yourusername yousuck2020 youtube zaynmalik zelenskyyua zerohora ",
);
