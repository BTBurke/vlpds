//! Service proxying (atproto-proxy) and app-level endpoints served by the PDS.
//!
//! Mirrors the reference PDS's `pipethrough.ts`:
//! - Unknown `/xrpc/{nsid}` requests (router fallback) are forwarded to the
//!   service named by the `atproto-proxy: <did>#<service id>` header, or by
//!   default `app.bsky.*` / `tools.ozone.*` go to the configured AppView.
//!   `chat.bsky.*` requires the header. Anything else is 501.
//! - The forwarded request carries an ES256K service-auth JWT signed with the
//!   user's repo key (iss = user DID, aud = bare service DID, lxm = nsid).
//!   Scope checks use the `did#service_id` form, like TS.
//! - Request and response bodies stream through; headers are allow-listed.
//! - Upstream >= 400 responses are re-raised as XRPC errors (500 becomes 502
//!   UpstreamFailure); connection failures and timeouts are 502 UpstreamFailure.
//!
//! Also serves `app.bsky.actor.{get,put}Preferences` from private account
//! state and proxies `com.atproto.moderation.createReport`.

use super::authn::Credentials;
use super::*;
use crate::did_resolver;
use axum::extract::Request;
use axum::http::{Method, Uri};
use std::borrow::Cow;
use std::time::Duration;

/// Private-state name of the stored `app.bsky` preferences (JSON array).
const PREFS_KEY: &str = "prefs:app.bsky";
const PREFS_NAMESPACE: &str = "app.bsky";
const PERSONAL_DETAILS_PREF: &str = "app.bsky.actor.defs#personalDetailsPref";
const DECLARED_AGE_PREF: &str = "app.bsky.actor.defs#declaredAgePref";

const GET_PREFERENCES: &str = "app.bsky.actor.getPreferences";
const PUT_PREFERENCES: &str = "app.bsky.actor.putPreferences";
const CREATE_REPORT: &str = "com.atproto.moderation.createReport";
const APPEAL_ACTIONED_SUBJECT: &str = "tools.ozone.inbox.appealActionedSubject";

/// TS proxy defaults: headersTimeout 10s, bodyTimeout 30s, maxResponseSize 10MB.
const HEADERS_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 10 << 20;
const SERVICE_JWT_TTL_SECS: u64 = 60;

/// Account-management methods that must be called directly, never proxied.
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

/// Methods a non-privileged app password may not call (DMs + createAccount).
const PRIVILEGED_METHODS: &[&str] = &[
    "chat.bsky.actor.deleteAccount",
    "chat.bsky.actor.exportAccountData",
    "chat.bsky.convo.deleteMessageForSelf",
    "chat.bsky.convo.getConvo",
    "chat.bsky.convo.getConvoForMembers",
    "chat.bsky.convo.getLog",
    "chat.bsky.convo.getMessages",
    "chat.bsky.convo.leaveConvo",
    "chat.bsky.convo.listConvos",
    "chat.bsky.convo.muteConvo",
    "chat.bsky.convo.sendMessage",
    "chat.bsky.convo.sendMessageBatch",
    "chat.bsky.convo.unmuteConvo",
    "chat.bsky.convo.updateRead",
    "com.atproto.server.createAccount",
];

/// Response headers forwarded from upstream (besides content-* headers).
const RES_HEADERS_TO_FORWARD: [header::HeaderName; 3] = [
    header::HeaderName::from_static("atproto-repo-rev"),
    header::HeaderName::from_static("atproto-content-labelers"),
    header::RETRY_AFTER,
];

/// Response headers of a successful upstream response that are passed on.
const RES_HEADERS: [header::HeaderName; 7] = [
    header::CONTENT_LENGTH,
    header::CONTENT_ENCODING,
    header::CONTENT_TYPE,
    header::CONTENT_LANGUAGE,
    header::HeaderName::from_static("atproto-repo-rev"),
    header::HeaderName::from_static("atproto-content-labelers"),
    header::RETRY_AFTER,
];

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/app.bsky.actor.getPreferences", get(get_preferences))
        .route("/xrpc/app.bsky.actor.putPreferences", post(put_preferences))
        .route(
            "/xrpc/com.atproto.moderation.createReport",
            post(create_report),
        )
}

fn xerr(status: StatusCode, error: &str, message: impl Into<String>) -> XrpcError {
    XrpcError {
        status,
        error: error.into(),
        message: message.into(),
    }
}

fn lxm_in(set: &[&str], lxm: &str) -> bool {
    set.iter().any(|m| m.eq_ignore_ascii_case(lxm))
}

fn valid_nsid(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    s.len() <= 317
        && parts.len() >= 3
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.len() <= 63
                && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

// Target selection
// ----------------

/// A resolved proxy target (configured ones borrow the config).
struct Target<'a> {
    /// Service endpoint (only its origin is used; the path comes from the request).
    url: Cow<'a, str>,
    /// Bare service DID: the service-auth JWT audience.
    did: Cow<'a, str>,
    service_id: Cow<'a, str>,
    /// Operator-configured (AppView / report service): exempt from SSRF checks.
    trusted: bool,
}

impl Target<'_> {
    /// `did#service_id`, the audience used for scope checks.
    fn scope_aud(&self) -> String {
        format!("{}#{}", self.did, self.service_id)
    }
}

fn proxy_header(headers: &HeaderMap) -> XResult<Option<&str>> {
    match headers.get("atproto-proxy") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(Some)
            .map_err(|_| XrpcError::bad("InvalidRequest", "invalid proxy header format")),
    }
}

fn configured<'a>(svc: &'a Option<(String, String)>, service_id: &'static str) -> Option<Target<'a>> {
    svc.as_ref().map(|(url, did)| Target {
        url: Cow::Borrowed(url),
        did: Cow::Borrowed(did),
        service_id: Cow::Borrowed(service_id),
        trusted: true,
    })
}

/// Default service for a method without an atproto-proxy header:
/// Ok(None) = not proxyable (501).
fn default_target<'a>(app: &'a App, lxm: &str) -> XResult<Option<Target<'a>>> {
    let no_service =
        || XrpcError::bad("InvalidRequest", format!("No service configured for {lxm}"));
    if lxm == CREATE_REPORT {
        return configured(&app.config.report_service, "atproto_labeler")
            .map(Some)
            .ok_or_else(no_service);
    }
    if lxm.starts_with("chat.bsky.") {
        // DMs live on a separate service; clients must name it.
        return Err(no_service());
    }
    if lxm.starts_with("app.bsky.") || lxm.starts_with("tools.ozone.") {
        return configured(&app.config.appview, "bsky_appview")
            .map(Some)
            .ok_or_else(no_service);
    }
    Ok(None)
}

/// The `did#service_id` a request targets (header or default), without
/// resolving anything. Used for scope checks on locally served methods.
fn compute_proxy_to(app: &App, headers: &HeaderMap, lxm: &str) -> XResult<String> {
    if let Some(h) = proxy_header(headers)? {
        return Ok(h.to_string());
    }
    match default_target(app, lxm)? {
        Some(t) => Ok(t.scope_aud()),
        None => Err(XrpcError::bad(
            "InvalidRequest",
            format!("No service configured for {lxm}"),
        )),
    }
}

/// Parses and resolves `atproto-proxy: <did>#<service id>`.
async fn parse_proxy_header<'a>(app: &'a App, proxy_to: &str) -> XResult<Target<'a>> {
    let bad = |m: &str| XrpcError::bad("InvalidRequest", m);
    let hash = match proxy_to.find('#') {
        Some(0) => return Err(bad("no did specified in proxy header")),
        Some(i) if i == proxy_to.len() - 1 => {
            return Err(bad("no service id specified in proxy header"))
        }
        None => return Err(bad("no service id specified in proxy header")),
        Some(i) => i,
    };
    if proxy_to[hash + 1..].contains('#') {
        return Err(bad("invalid proxy header format"));
    }
    if proxy_to.contains(' ') {
        return Err(bad("proxy header cannot contain spaces"));
    }
    let (did, service_id) = (&proxy_to[..hash], &proxy_to[hash + 1..]);
    // The configured AppView is used without resolution.
    if let Some((url, av_did)) = &app.config.appview {
        if did == av_did && service_id == "bsky_appview" {
            return Ok(Target {
                url: Cow::Borrowed(url),
                did: Cow::Borrowed(av_did),
                service_id: Cow::Borrowed("bsky_appview"),
                trusted: true,
            });
        }
    }
    let doc = resolve_did(app, did)
        .await
        .map_err(|_| bad("could not resolve proxy did"))?;
    let url = did_resolver::service_endpoint(&doc, service_id)
        .ok_or_else(|| bad("could not resolve proxy did service url"))?;
    Ok(Target {
        url: Cow::Owned(url),
        did: Cow::Owned(did.into()),
        service_id: Cow::Owned(service_id.into()),
        trusted: false,
    })
}

/// DID document for `did`: built locally for accounts hosted here, otherwise
/// resolved over the network (cached).
pub async fn resolve_did(app: &App, did: &str) -> Result<Arc<J>, did_resolver::ResolveError> {
    if !did.starts_with("did:") {
        return Err(did_resolver::ResolveError::BadDid(did.into()));
    }
    if let Ok(acct) = app.account(did).await {
        if let Some(doc) = local_did_doc(app, &acct) {
            return Ok(Arc::new(doc));
        }
    }
    app.did_resolver.resolve(did).await
}

fn local_did_doc(app: &App, acct: &Account) -> Option<J> {
    let key = Keypair::from_bytes(&hex::decode(&acct.signing_key).ok()?).ok()?;
    Some(json!({
        "@context": ["https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1"],
        "id": acct.did,
        "alsoKnownAs": [format!("at://{}", acct.handle)],
        "verificationMethod": [{
            "id": format!("{}#atproto", acct.did),
            "type": "Multikey",
            "controller": acct.did,
            "publicKeyMultibase": key.public_multibase(),
        }],
        "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": app.public_url}],
    }))
}

// Forwarding
// ----------

/// Configured upstreams (AppView, report service) use the shared public
/// client; endpoints taken from DID documents use the SSRF-guarded one.
fn proxy_http(app: &App, trusted: bool) -> &'static reqwest::Client {
    if trusted {
        crate::http::proxy()
    } else {
        crate::http::guarded(app.config.dev_mode)
    }
}

fn upstream_failure(message: &str) -> XrpcError {
    xerr(StatusCode::BAD_GATEWAY, "UpstreamFailure", message)
}

/// Request headers passed to the upstream service (TS allow-list).
fn forward_headers(src: &HeaderMap, with_body: bool, authorization: Option<&str>) -> HeaderMap {
    const ACCEPT_LANGUAGE: header::HeaderName = header::ACCEPT_LANGUAGE;
    const ACCEPT_LABELERS: header::HeaderName = header::HeaderName::from_static("atproto-accept-labelers");
    const BSKY_TOPICS: header::HeaderName = header::HeaderName::from_static("x-bsky-topics");
    let mut out = HeaderMap::with_capacity(8);
    let copy = |out: &mut HeaderMap, name: &header::HeaderName| {
        for v in src.get_all(name) {
            out.append(name.clone(), v.clone());
        }
    };
    let ae = src.get(header::ACCEPT_ENCODING).cloned();
    out.insert(header::ACCEPT_ENCODING, ae.unwrap_or(header::HeaderValue::from_static("identity")));
    copy(&mut out, &ACCEPT_LANGUAGE);
    copy(&mut out, &ACCEPT_LABELERS);
    for name in src.keys() {
        if name.as_str().starts_with("x-atproto-") {
            copy(&mut out, name);
        }
    }
    copy(&mut out, &BSKY_TOPICS);
    if with_body {
        copy(&mut out, &header::CONTENT_TYPE);
        copy(&mut out, &header::CONTENT_ENCODING);
        copy(&mut out, &header::CONTENT_LENGTH);
    }
    if let Some(Ok(v)) = authorization.map(|a| header::HeaderValue::from_str(&format!("Bearer {a}"))) {
        out.insert(header::AUTHORIZATION, v);
    }
    out
}

fn is_json_content_type(ct: &str) -> bool {
    let ct = ct.to_ascii_lowercase();
    let Some(rest) = ct.split_once("application/").map(|(_, r)| r) else {
        return false;
    };
    let sub: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '+')
        .collect();
    sub == "json" || sub.ends_with("+json")
}

fn response_type_name(status: u16) -> Option<&'static str> {
    Some(match status {
        400 => "InvalidRequest",
        401 => "AuthenticationRequired",
        403 => "Forbidden",
        404 => "XRPCNotSupported",
        406 => "NotAcceptable",
        413 => "PayloadTooLarge",
        415 => "UnsupportedMediaType",
        429 => "RateLimitExceeded",
        500 => "InternalServerError",
        501 => "MethodNotImplemented",
        502 => "UpstreamFailure",
        503 => "NotEnoughResources",
        504 => "UpstreamTimeout",
        _ => return None,
    })
}

fn response_type_str(status: u16) -> Option<&'static str> {
    Some(match status {
        400 => "Invalid Request",
        401 => "Authentication Required",
        403 => "Forbidden",
        404 => "XRPC Not Supported",
        406 => "Not Acceptable",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        429 => "Rate Limit Exceeded",
        500 => "Internal Server Error",
        501 => "Method Not Implemented",
        502 => "Upstream Failure",
        503 => "Not Enough Resources",
        504 => "Upstream Timeout",
        _ => return None,
    })
}

/// Re-raises an upstream error response (TS PipethroughUpstreamError):
/// status passes through except 500 -> 502; error/message come from the
/// upstream JSON body when present; only the forwardable headers are kept.
async fn upstream_error(resp: axum::http::response::Parts, body: Body) -> Response {
    let upstream_status = resp.status.as_u16();
    let status = if upstream_status == 500 {
        502
    } else {
        upstream_status
    };
    let mut fwd = HeaderMap::new();
    for name in RES_HEADERS_TO_FORWARD {
        if let Some(v) = resp.headers.get(&name) {
            fwd.insert(name, v.clone());
        }
    }
    let json_body = resp
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(is_json_content_type);
    let encoded = resp
        .headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|e| !e.trim().is_empty() && !e.trim().eq_ignore_ascii_case("identity"));
    let (mut error, mut message) = (None, None);
    if json_body && !encoded {
        let buf = axum::body::to_bytes(body, usize::MAX).await;
        if let Ok(buf) = buf {
            if let Ok(v) = serde_json::from_slice::<J>(&buf) {
                error = v.get("error").and_then(|e| e.as_str()).map(String::from);
                message = v.get("message").and_then(|e| e.as_str()).map(String::from);
            }
        }
    }
    let error = error.or_else(|| response_type_name(status).map(String::from));
    let message = message
        .filter(|m| !m.is_empty())
        .or_else(|| response_type_str(status).map(String::from));
    let mut body = serde_json::Map::new();
    if let Some(e) = error {
        body.insert("error".into(), J::String(e));
    }
    if let Some(m) = message {
        body.insert("message".into(), J::String(m));
    }
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (code, fwd, Json(J::Object(body))).into_response()
}

struct Forward<'a> {
    method: Method,
    /// Path and query, forwarded verbatim (TS uses req.originalUrl).
    path_and_query: &'a str,
    headers: &'a HeaderMap,
    body: Option<Body>,
    /// Service-auth issuer; None forwards without credentials.
    iss: Option<&'a str>,
    lxm: &'a str,
}

// ---- fast path caches ------------------------------------------------------
//
// Proxying is the hottest path a PDS serves (every AppView read goes through
// it). Per request it would otherwise cost two account reads + JSON parse, a
// key parse, and a ~25 µs ES256K signature. Instead:
// - account signing key + status are cached, read once per request. Only
//   the DID's owner caches (requests are routed to it), and an entry is
//   valid only in the partition epoch it was read in, so changes another
//   node made while it owned the DID never show through a stale entry.
//   Every account change goes through the owner's worker, which drops the
//   entry once the change is applied ([`account_changed`]): takedowns and
//   key rotations apply to the next request. ACCT_TTL only bounds an
//   entry's life (memory, and a backstop);
// - minted service JWTs are reused per (iss, aud, lxm, signing key) until
//   half their lifetime has passed, so an active account signs ~2×/min per
//   method. The key is part of the cache key: after a rotation or migration
//   the account reload brings the new key, and with it fresh tokens,
//   instead of reusing ones signed by the old key.
// Lookups don't allocate: the account cache is keyed by DID (borrowed
// lookups), the JWT cache by a hash of (iss, aud, lxm, key id) with the full
// key stored and compared on every hit.

const ACCT_TTL: Duration = Duration::from_secs(60);
const JWT_REUSE: Duration = Duration::from_secs(SERVICE_JWT_TTL_SECS / 2);
const CACHE_SHARDS: usize = 64;

type Shard<K, V> = parking_lot::Mutex<std::collections::HashMap<K, (V, std::time::Instant)>>;

/// Capped by `kind`'s [`crate::caches`] cap (sized from the memory budget).
struct TtlCache<K, V> {
    shards: Vec<Shard<K, V>>,
    kind: crate::caches::Cache,
    /// per shard, bumped (under its lock) by every [`TtlCache::invalidate`]:
    /// a load that raced one is not cached
    gens: Vec<std::sync::atomic::AtomicU64>,
}

/// Fixed-key hash (the same key always picks the same shard).
fn fixed_hash<Q: std::hash::Hash + ?Sized>(k: &Q) -> u64 {
    use std::hash::Hasher;
    let mut h = std::hash::DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

impl<K: Send, V: Send> crate::caches::Len for TtlCache<K, V> {
    fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }
}

impl<K: std::hash::Hash + Eq, V: Clone> TtlCache<K, V> {
    fn new(kind: crate::caches::Cache) -> Self {
        TtlCache {
            shards: (0..CACHE_SHARDS).map(|_| parking_lot::Mutex::new(Default::default())).collect(),
            gens: (0..CACHE_SHARDS).map(|_| Default::default()).collect(),
            kind,
        }
    }
    fn shard_of<Q: std::hash::Hash + ?Sized>(k: &Q) -> usize {
        (fixed_hash(k) as usize) % CACHE_SHARDS
    }
    fn shard<Q: std::hash::Hash + ?Sized>(&self, k: &Q) -> &Shard<K, V> {
        &self.shards[Self::shard_of(k)]
    }
    /// Taken before reading what will be cached under `k`; pass to
    /// [`TtlCache::put_unless_changed`].
    fn generation<Q: std::hash::Hash + ?Sized>(&self, k: &Q) -> u64 {
        self.gens[Self::shard_of(k)].load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Drops `k`, and keeps loads that began before this from caching what
    /// they read.
    fn invalidate<Q>(&self, k: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        let i = Self::shard_of(k);
        let mut m = self.shards[i].lock();
        self.gens[i].fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        m.remove(k);
    }
    /// Value if inserted less than `max_age` ago.
    fn get<Q>(&self, k: &Q, max_age: Duration) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        self.get_aged(k).filter(|(_, age)| *age < max_age).map(|(v, _)| v)
    }
    /// Value and age, however old.
    fn get_aged<Q>(&self, k: &Q) -> Option<(V, Duration)>
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        let m = self.shard(k).lock();
        m.get(k).map(|(v, t)| (v.clone(), t.elapsed()))
    }
    fn put(&self, k: K, v: V, max_age: Duration) {
        self.put_unless_changed(k, v, max_age, None)
    }
    /// [`TtlCache::put`], skipped if `k`'s shard was invalidated since `gen`
    /// was taken.
    fn put_unless_changed(&self, k: K, v: V, max_age: Duration, gen: Option<u64>) {
        let i = Self::shard_of(&k);
        let cap = crate::caches::cap(self.kind).div_ceil(CACHE_SHARDS).max(1);
        let mut m = self.shards[i].lock();
        if gen.is_some_and(|g| g != self.gens[i].load(std::sync::atomic::Ordering::SeqCst)) {
            return;
        }
        if m.len() >= cap {
            m.retain(|_, (_, t)| t.elapsed() < max_age);
            if m.len() >= cap {
                m.clear();
            }
        }
        m.insert(k, (v, std::time::Instant::now()));
    }
}

#[derive(Clone)]
struct CachedAcct {
    key: Arc<Keypair>,
    /// Identifies the signing key (a hash of it): part of the JWT cache key.
    key_id: u64,
    status: Option<String>,
    /// (partition, epoch) it was read in: valid only while still owned in it
    part: (u16, u64),
}

/// A minted service JWT and the (iss, aud, lxm, key id) it was minted for.
type CachedJwt = Arc<(String, String, String, u64, Arc<str>)>;

static ACCTS: std::sync::LazyLock<Arc<TtlCache<String, CachedAcct>>> = std::sync::LazyLock::new(|| {
    use crate::caches::{track, Cache};
    track(Cache::ProxyAccounts, Arc::new(TtlCache::new(Cache::ProxyAccounts)))
});
static JWTS: std::sync::LazyLock<Arc<TtlCache<u64, CachedJwt>>> = std::sync::LazyLock::new(|| {
    use crate::caches::{track, Cache};
    track(Cache::ProxyJwts, Arc::new(TtlCache::new(Cache::ProxyJwts)))
});

/// Drops `did`'s cached account. Its worker calls this once an account
/// change is applied (before acking it).
pub(crate) fn account_changed(did: &str) {
    ACCTS.invalidate(did);
}

async fn cached_account(app: &App, did: &str) -> XResult<CachedAcct> {
    let part = app.partition(did)?;
    let prev = match ACCTS.get_aged(did) {
        Some((a, age)) if age < ACCT_TTL && a.part == (part.id, part.epoch) => {
            crate::metrics::PROXY_CACHE.with_label_values(&["account_hit"]).inc();
            return Ok(a);
        }
        prev => prev.map(|(a, _)| a),
    };
    crate::metrics::PROXY_CACHE.with_label_values(&["account_miss"]).inc();
    // the two fields used here, borrowed: a full `Account` parse (its
    // flattened extension map buffers the whole document) cost more than
    // the read at 1M active accounts, where ~half the lookups miss
    #[derive(serde::Deserialize)]
    struct KeyAndStatus<'a> {
        #[serde(borrow)]
        signing_key: std::borrow::Cow<'a, str>,
        #[serde(default, borrow)]
        status: Option<std::borrow::Cow<'a, str>>,
    }
    let gen = ACCTS.generation(did);
    let raw = part
        .db
        .get(state::account_key(did))
        .await
        .map_err(XrpcError::from_err)?
        .ok_or_else(|| XrpcError::bad("AccountNotFound", format!("no account {did}")))?;
    let acct: KeyAndStatus = serde_json::from_slice(&raw).map_err(XrpcError::from_err)?;
    let key_id = fixed_hash(&*acct.signing_key);
    // an unchanged key keeps its parsed form
    let key = match prev.filter(|p| p.key_id == key_id) {
        Some(p) => p.key,
        None => Arc::new(
            Keypair::from_bytes(&hex::decode(&*acct.signing_key).map_err(XrpcError::from_err)?)
                .map_err(XrpcError::from_err)?,
        ),
    };
    let c = CachedAcct { key, key_id, status: acct.status.map(Into::into), part: (part.id, part.epoch) };
    ACCTS.put_unless_changed(did.to_string(), c.clone(), ACCT_TTL, Some(gen));
    Ok(c)
}

fn service_jwt(acct: &CachedAcct, iss: &str, aud: &str, lxm: &str) -> Arc<str> {
    let h = fixed_hash(&(iss, aud, lxm, acct.key_id));
    let hit = |j: &CachedJwt| j.0 == iss && j.1 == aud && j.2 == lxm && j.3 == acct.key_id;
    if let Some(j) = JWTS.get(&h, JWT_REUSE).filter(hit) {
        crate::metrics::PROXY_CACHE.with_label_values(&["jwt_hit"]).inc();
        return j.4.clone();
    }
    crate::metrics::PROXY_CACHE.with_label_values(&["jwt_miss"]).inc();
    let j: Arc<str> = crate::auth::service_auth_jwt(&acct.key, iss, aud, Some(lxm), SERVICE_JWT_TTL_SECS).into();
    JWTS.put(h, Arc::new((iss.into(), aud.into(), lxm.into(), acct.key_id, j.clone())), JWT_REUSE);
    j
}

/// A service endpoint as the proxy uses it.
#[derive(Clone)]
struct Endpoint {
    /// `scheme://host[:port]`
    origin: Arc<str>,
    /// `host:port` of a plain `http://` endpoint (the HTTP/1.1 fast path).
    h1: Option<Arc<str>>,
}

/// The parsed form of a service endpoint URL. The last one per thread is
/// kept: proxied calls nearly always go to the one AppView.
fn endpoint(url: &str) -> XResult<Endpoint> {
    thread_local! {
        static LAST: std::cell::RefCell<Option<(String, Endpoint)>> = const { std::cell::RefCell::new(None) };
    }
    if let Some(e) = LAST.with_borrow(|l| l.as_ref().filter(|(u, _)| u == url).map(|(_, e)| e.clone())) {
        return Ok(e);
    }
    let base = reqwest::Url::parse(url).map_err(|_| XrpcError::bad("InvalidRequest", "invalid service endpoint"))?;
    let h1 = match (base.scheme(), base.host_str(), base.port_or_known_default()) {
        ("http", Some(host), Some(port)) => Some(format!("{host}:{port}").into()),
        _ => None,
    };
    let e = Endpoint { origin: base.origin().ascii_serialization().into(), h1 };
    LAST.set(Some((url.to_string(), e.clone())));
    Ok(e)
}

/// Upstream response body, passed through with the reference's limits: at
/// most [`MAX_RESPONSE_BYTES`], and [`BODY_TIMEOUT`] without progress fails
/// it. The idle timer is armed only while the upstream keeps us waiting, so
/// a response that arrived with its head (the common case) costs no timer
/// (each tokio timer operation takes the runtime's one timer-wheel lock).
struct UpstreamBody<B> {
    inner: B,
    seen: usize,
    idle: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    progressed: bool,
}

impl<B> UpstreamBody<B> {
    fn new(inner: B) -> Self {
        UpstreamBody { inner, seen: 0, idle: None, progressed: false }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl<B> hyper::body::Body for UpstreamBody<B>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
        use std::task::Poll;
        let this = &mut *self;
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(f))) => {
                if let Some(d) = f.data_ref() {
                    this.seen += d.len();
                    if this.seen > MAX_RESPONSE_BYTES {
                        return Poll::Ready(Some(Err("upstream response too large".into())));
                    }
                }
                this.progressed = true;
                Poll::Ready(Some(Ok(f)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                let deadline = tokio::time::Instant::now() + BODY_TIMEOUT;
                let progressed = std::mem::take(&mut this.progressed);
                let idle = match &mut this.idle {
                    Some(s) => {
                        if progressed {
                            s.as_mut().reset(deadline);
                        }
                        s
                    }
                    None => this.idle.insert(Box::pin(tokio::time::sleep_until(deadline))),
                };
                if std::future::Future::poll(idle.as_mut(), cx).is_ready() {
                    return Poll::Ready(Some(Err("upstream body timeout".into())));
                }
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// Sends the request to `target` with a (cached) service-auth token (when
/// there is an issuer, whose account `acct` is) and streams the response
/// back.
async fn forward(app: &App, target: &Target<'_>, f: Forward<'_>, acct: Option<&CachedAcct>) -> XResult<Response> {
    let authorization = match f.iss {
        Some(iss) => {
            let fetched;
            let acct = match acct {
                Some(a) => a,
                None => {
                    fetched = cached_account(app, iss).await?;
                    &fetched
                }
            };
            // Phase 1 of service-auth updates: the outbound JWT aud is the bare DID.
            Some(service_jwt(acct, iss, &target.did, f.lxm))
        }
        None => None,
    };

    if !target.trusted {
        let base = reqwest::Url::parse(&target.url)
            .map_err(|_| XrpcError::bad("InvalidRequest", "invalid service endpoint"))?;
        if let Err(e) = did_resolver::check_outbound_url(&base, app.config.dev_mode) {
            tracing::warn!(endpoint = %target.url, "proxy target refused: {e}");
            return Err(upstream_failure("Upstream service unreachable"));
        }
    }
    let ep = endpoint(&target.url)?;
    let with_body = f.body.is_some();
    let headers = forward_headers(f.headers, with_body, authorization.as_deref());
    let sent = match ep.h1.as_ref().filter(|_| target.trusted) {
        // operator-configured plain-HTTP upstream: the HTTP/1.1 fast path
        Some(authority) => {
            let mut req = axum::http::Request::new(f.body.unwrap_or_default());
            *req.method_mut() = f.method;
            *req.uri_mut() = axum::http::Uri::try_from(f.path_and_query)
                .map_err(|_| XrpcError::bad("InvalidRequest", "invalid xrpc path"))?;
            *req.headers_mut() = headers;
            let send = crate::http::h1::send("public", authority, req);
            match tokio::time::timeout(HEADERS_TIMEOUT, send).await {
                Ok(Ok(r)) => {
                    let (parts, body) = r.into_parts();
                    Ok((parts, Body::new(UpstreamBody::new(body))))
                }
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("headers timeout".to_string()),
            }
        }
        None => {
            let mut url = String::with_capacity(ep.origin.len() + f.path_and_query.len());
            url.push_str(&ep.origin);
            url.push_str(f.path_and_query);
            let mut rb = proxy_http(app, target.trusted).request(f.method, &url).headers(headers);
            if let Some(b) = f.body {
                rb = rb.body(reqwest::Body::wrap_stream(b.into_data_stream()));
            }
            match tokio::time::timeout(HEADERS_TIMEOUT, rb.send()).await {
                Ok(Ok(r)) => {
                    let (parts, body) = axum::http::Response::from(r).into_parts();
                    Ok((parts, Body::new(UpstreamBody::new(body))))
                }
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("headers timeout".to_string()),
            }
        }
    };
    let (parts, body) = match sent {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(endpoint = %target.url, path = f.path_and_query, "proxy upstream error: {e}");
            return Err(upstream_failure("Upstream service unreachable"));
        }
    };
    if parts.status.as_u16() >= 400 {
        return Ok(upstream_error(parts, body).await);
    }
    let mut out = Response::new(body);
    *out.status_mut() = parts.status;
    let headers = out.headers_mut();
    for name in RES_HEADERS {
        for v in parts.headers.get_all(&name) {
            headers.append(name.clone(), v.clone());
        }
    }
    Ok(out)
}

/// Unauthenticated pipethrough of a GET to the `atproto-proxy` target or the
/// method's default service (reference `pipethrough(ctx, req)` without an
/// issuer), e.g. repo.getRecord for repos not hosted here.
pub(super) async fn pipethrough_unauthed(
    app: &App,
    headers: &HeaderMap,
    uri: &Uri,
    lxm: &str,
) -> XResult<Response> {
    let target = match proxy_header(headers)? {
        Some(h) => parse_proxy_header(app, h).await?,
        None => default_target(app, lxm)?
            .or_else(|| configured(&app.config.appview, "bsky_appview"))
            .ok_or_else(|| XrpcError::bad("InvalidRequest", format!("No service configured for {lxm}")))?,
    };
    let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or(uri.path());
    forward(
        app,
        &target,
        Forward {
            method: Method::GET,
            path_and_query: pq,
            headers,
            body: None,
            iss: None,
            lxm,
        },
        None,
    )
    .await
}

/// Account checks shared by every proxied call: loads the account and
/// rejects taken-down accounts unless the method allows them.
async fn check_takedown(app: &App, did: &str, allow_takendown: bool) -> XResult<CachedAcct> {
    let acct = cached_account(app, did).await.map_err(|_| {
        xerr(
            StatusCode::FORBIDDEN,
            "AccountNotFound",
            "Account not found",
        )
    })?;
    if !allow_takendown && matches!(acct.status.as_deref(), Some("takendown") | Some("suspended")) {
        return Err(xerr(
            StatusCode::UNAUTHORIZED,
            "AccountTakedown",
            "Account has been taken down",
        ));
    }
    Ok(acct)
}

fn user_did(creds: &Credentials) -> XResult<&str> {
    creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))
}

/// Router fallback: catch-all proxy for XRPC methods not served locally.
pub async fn fallback(State(app): AppState, req: Request) -> Response {
    match proxy_request(&app, req).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn proxy_request(app: &App, req: Request) -> XResult<Response> {
    let Some(nsid) = req.uri().path().strip_prefix("/xrpc/") else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let lxm = nsid.to_string();
    if !valid_nsid(&lxm) {
        return Err(XrpcError::bad("InvalidRequest", "invalid xrpc path"));
    }
    let method = req.method().clone();
    if method != Method::GET && method != Method::HEAD && method != Method::POST {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "XRPC requests only supports GET and POST",
        ));
    }
    if lxm_in(PROTECTED_METHODS, &lxm) {
        return Err(XrpcError::bad("InvalidToken", "Bad token method"));
    }
    let header = proxy_header(req.headers())?.map(String::from);
    // Decide whether there is anything to proxy to before authenticating or
    // touching the network.
    let default = match &header {
        Some(_) => None,
        None => match default_target(app, &lxm)? {
            Some(t) => Some(t),
            None => {
                return Err(xerr(
                    StatusCode::NOT_IMPLEMENTED,
                    "MethodNotImplemented",
                    "Method Not Implemented",
                ))
            }
        },
    };

    let (parts, body) = req.into_parts();
    let creds = super::authn::authenticate(app, &parts).await?;
    let did = user_did(&creds)?;

    let target = match (header, default) {
        (Some(h), _) => parse_proxy_header(app, &h).await?,
        (None, Some(t)) => t,
        (None, None) => unreachable!(),
    };
    creds.require(creds.allows_rpc(&lxm, &target.scope_aud()))?;
    if matches!(
        creds,
        Credentials::AppPassword {
            privileged: false,
            ..
        }
    ) && lxm_in(PRIVILEGED_METHODS, &lxm)
    {
        return Err(XrpcError::bad("InvalidToken", "Bad token method"));
    }
    let acct = check_takedown(app, did, lxm == APPEAL_ACTIONED_SUBJECT).await?;

    let body = (method == Method::POST).then_some(body);
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or(parts.uri.path());
    forward(
        app,
        &target,
        Forward {
            method,
            path_and_query: pq,
            headers: &parts.headers,
            body,
            iss: Some(did),
            lxm: &lxm,
        },
        Some(&acct),
    )
    .await
}

// app.bsky.actor.{get,put}Preferences
// -----------------------------------

/// The AppView audience whose preferences this PDS stores locally.
fn local_prefs_aud(app: &App) -> String {
    match &app.config.appview {
        Some((_, did)) => format!("{did}#bsky_appview"),
        None => format!("{}#bsky_appview", app.jwt.service_did),
    }
}

/// Scope audience and, when the request names a different AppView, the
/// target to pipe through to instead of serving locally.
async fn prefs_target<'a>(
    app: &'a App,
    creds: &Credentials,
    headers: &HeaderMap,
    lxm: &str,
) -> XResult<Option<Target<'a>>> {
    let local = local_prefs_aud(app);
    let aud = match proxy_header(headers)? {
        Some(h) => h.to_string(),
        None => local.clone(),
    };
    creds.require(creds.allows_rpc(lxm, &aud))?;
    if aud == local {
        return Ok(None);
    }
    Ok(Some(parse_proxy_header(app, &aud).await?))
}

/// Legacy full-access session (not app password / OAuth): may see and set
/// personalDetailsPref.
fn has_access_full(creds: &Credentials) -> bool {
    matches!(creds, Credentials::Session { .. })
}

fn pref_type(p: &J) -> Option<&str> {
    p.get("$type").and_then(|t| t.as_str())
}

fn pref_allowed(ty: &str, full: bool) -> bool {
    full || ty != PERSONAL_DETAILS_PREF
}

fn pref_in_namespace(ty: &str) -> bool {
    ty == PREFS_NAMESPACE || ty.starts_with("app.bsky.")
}

/// Age in whole years at `today` (UTC); None if the date can't be parsed
/// (JS `new Date()` gives NaN, so every age comparison is false).
fn age_from_datestring(birth: &str, today: chrono::NaiveDate) -> Option<i32> {
    use chrono::Datelike;
    let bday = chrono::DateTime::parse_from_rfc3339(birth)
        .map(|d| d.with_timezone(&chrono::Utc).date_naive())
        .ok()
        .or_else(|| chrono::NaiveDate::parse_from_str(birth.get(..10)?, "%Y-%m-%d").ok())?;
    let mut age = today.year() - bday.year();
    if (today.month(), today.day()) < (bday.month(), bday.day()) {
        age -= 1;
    }
    Some(age)
}

async fn load_prefs(app: &App, did: &str) -> XResult<Vec<J>> {
    match app.get_private(did, PREFS_KEY).await? {
        Some(b) => serde_json::from_slice(&b).map_err(XrpcError::from_err),
        None => Ok(Vec::new()),
    }
}

async fn get_preferences(
    State(app): AppState,
    Auth(creds): Auth,
    headers: HeaderMap,
    uri: Uri,
) -> XResult<Response> {
    let did = user_did(&creds)?.to_string();
    if let Some(target) = prefs_target(&app, &creds, &headers, GET_PREFERENCES).await? {
        let pq = uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or(uri.path());
        return forward(
            &app,
            &target,
            Forward {
                method: Method::GET,
                path_and_query: pq,
                headers: &headers,
                body: None,
                iss: Some(&did),
                lxm: GET_PREFERENCES,
            },
            None,
        )
        .await;
    }
    let full = has_access_full(&creds);
    let mut prefs = load_prefs(&app, &did).await?;
    let birth = prefs
        .iter()
        .find(|p| pref_type(p) == Some(PERSONAL_DETAILS_PREF))
        .and_then(|p| p.get("birthDate").and_then(|b| b.as_str()))
        .filter(|b| !b.is_empty())
        .map(String::from);
    if let Some(birth) = birth {
        let age = age_from_datestring(&birth, chrono::Utc::now().date_naive());
        let over = |n: i32| age.is_some_and(|a| a >= n);
        prefs.push(json!({"$type": DECLARED_AGE_PREF, "isOverAge13": over(13), "isOverAge16": over(16), "isOverAge18": over(18)}));
    }
    prefs.retain(|p| pref_type(p).is_some_and(|t| pref_allowed(t, full)));
    Ok(Json(json!({"preferences": prefs})).into_response())
}

async fn put_preferences(
    State(app): AppState,
    Auth(creds): Auth,
    headers: HeaderMap,
    uri: Uri,
    body: AxBytes,
) -> XResult<Response> {
    let did = user_did(&creds)?.to_string();
    if let Some(target) = prefs_target(&app, &creds, &headers, PUT_PREFERENCES).await? {
        let pq = uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or(uri.path());
        let fwd = Forward {
            method: Method::POST,
            path_and_query: pq,
            headers: &headers,
            body: Some(Body::from(body)),
            iss: Some(&did),
            lxm: PUT_PREFERENCES,
        };
        return forward(&app, &target, fwd, None).await;
    }
    check_takedown(&app, &did, false).await?;

    let input: J = serde_json::from_slice(&body)
        .map_err(|_| XrpcError::bad("InvalidRequest", "Request body must be a JSON object"))?;
    let Some(input) = input.as_object() else {
        return Err(XrpcError::bad("InvalidRequest", "Input must be an object"));
    };
    let values = match input.get("preferences") {
        None => {
            return Err(XrpcError::bad(
                "InvalidRequest",
                "Input must have the property \"preferences\"",
            ))
        }
        Some(J::Array(a)) => a,
        Some(_) => {
            return Err(XrpcError::bad(
                "InvalidRequest",
                "Input/preferences must be an array",
            ))
        }
    };
    let mut checked = Vec::with_capacity(values.len());
    for v in values {
        match (v.is_object(), pref_type(v)) {
            (true, Some(_)) => checked.push(v.clone()),
            _ => {
                return Err(XrpcError::bad(
                    "InvalidRequest",
                    "Preference is missing a $type",
                ))
            }
        }
    }
    if !checked
        .iter()
        .all(|p| pref_in_namespace(pref_type(p).unwrap()))
    {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!("Some preferences are not in the {PREFS_NAMESPACE} namespace"),
        ));
    }
    let full = has_access_full(&creds);
    let forbidden: Vec<&str> = checked
        .iter()
        .filter_map(|p| pref_type(p))
        .filter(|t| !pref_allowed(t, full))
        .collect();
    if !forbidden.is_empty() {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!(
                "Do not have authorization to set preferences: {}",
                forbidden.join(", ")
            ),
        ));
    }
    // Replace every stored pref this caller may set; keep the ones it can't
    // (personalDetailsPref for non-full-access sessions). Read-only prefs
    // (declaredAgePref) are derived and never stored.
    // Serialized per account so concurrent puts can't lose each other's
    // kept prefs (requests for a DID are served by its owning node).
    let ext = super::server::ext(&app);
    let _g = ext.lock(&format!("prefs:{did}")).await;
    let mut stored: Vec<J> = load_prefs(&app, &did)
        .await?
        .into_iter()
        .filter(|p| pref_type(p).is_some_and(|t| !(pref_in_namespace(t) && pref_allowed(t, full))))
        .collect();
    stored.extend(
        checked
            .into_iter()
            .filter(|p| pref_type(p) != Some(DECLARED_AGE_PREF)),
    );
    let val = Bytes::from(serde_json::to_vec(&stored).map_err(XrpcError::from_err)?);
    let m = crate::segment::Mutation {
        key: Bytes::from(state::private_key(&did, PREFS_KEY)),
        val: Some(val),
    };
    app.put_private(&did, vec![m]).await?;
    Ok(StatusCode::OK.into_response())
}

// com.atproto.moderation.createReport
// -----------------------------------

async fn create_report(
    State(app): AppState,
    Auth(creds): Auth,
    headers: HeaderMap,
    body: AxBytes,
) -> XResult<Response> {
    let did = user_did(&creds)?.to_string();
    let aud = compute_proxy_to(&app, &headers, CREATE_REPORT)?;
    creds.require(creds.allows_rpc(CREATE_REPORT, &aud))?;

    let input: J = serde_json::from_slice(&body)
        .map_err(|_| XrpcError::bad("InvalidRequest", "Request body must be a JSON object"))?;
    if !input.is_object() {
        return Err(XrpcError::bad("InvalidRequest", "Input must be an object"));
    }
    if !input.get("reasonType").is_some_and(|v| v.is_string()) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Input must have the property \"reasonType\"",
        ));
    }
    if !input.get("subject").is_some_and(|v| v.is_object()) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Input must have the property \"subject\"",
        ));
    }
    // Taken-down accounts may still report (appeals).
    let acct = check_takedown(&app, &did, true).await?;

    let target = match proxy_header(&headers)? {
        Some(h) => parse_proxy_header(&app, h).await?,
        None => default_target(&app, CREATE_REPORT)?.expect("createReport has a default target"),
    };
    let mut fwd_headers = HeaderMap::new();
    for name in ["accept-language", "atproto-accept-labelers"] {
        if let Some(v) = headers.get(name) {
            fwd_headers.insert(name, v.clone());
        }
    }
    fwd_headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    let body = Bytes::from(serde_json::to_vec(&input).map_err(XrpcError::from_err)?);
    fwd_headers.insert(header::CONTENT_LENGTH, body.len().into());
    let path = format!("/xrpc/{CREATE_REPORT}");
    forward(
        &app,
        &target,
        Forward {
            method: Method::POST,
            path_and_query: &path,
            headers: &fwd_headers,
            body: Some(Body::from(body)),
            iss: Some(&did),
            lxm: CREATE_REPORT,
        },
        Some(&acct),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
        assert_eq!(
            age_from_datestring("2010-06-15T00:00:00.000Z", today),
            Some(16)
        );
        assert_eq!(age_from_datestring("2010-06-16", today), Some(15));
        assert_eq!(age_from_datestring("garbage", today), None);
    }

    #[test]
    fn nsids_and_content_types() {
        assert!(valid_nsid("app.bsky.feed.getTimeline"));
        assert!(!valid_nsid("app.bsky"));
        assert!(!valid_nsid("app..bsky.x"));
        assert!(!valid_nsid("app.bsky.x/y"));
        assert!(is_json_content_type("application/json; charset=utf-8"));
        assert!(is_json_content_type("application/problem+json"));
        assert!(!is_json_content_type("text/plain"));
    }
}
