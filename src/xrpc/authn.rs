//! Request authentication: `Auth` dispatches on the Authorization scheme
//! (Bearer session or service JWT, DPoP OAuth, Basic admin) and yields
//! `Credentials`, whose `allows_*` methods are the one place permission
//! checks (OAuth scopes, app-password limits) happen.

use super::*;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

#[derive(Clone, Debug)]
pub enum Credentials {
    Session {
        did: String,
    },
    /// Privileged app passwords may use DMs.
    AppPassword {
        did: String,
        privileged: bool,
    },
    OAuth {
        did: String,
        client_id: String,
        scopes: super::oauth::ScopeSet,
    },
    Admin,
    /// A taken-down account's session (`allowTakendown`), accepted only by
    /// [`TAKENDOWN_METHODS`].
    Takendown {
        did: String,
    },
    /// The `--mod-service-did` service on [`MODERATOR_METHODS`] or
    /// getPreferences. `iss` is the DID or `DID#atproto_labeler`.
    ModService {
        iss: String,
    },
    /// A local user's own service JWT (reference `userServiceAuth`), only on
    /// [`USER_SERVICE_AUTH_METHODS`].
    UserServiceAuth {
        did: String,
    },
}

/// Methods that also take a user's service JWT (reference
/// `authorizationOrUserServiceAuth`): the Bluesky app hands an uploadBlob
/// token from getServiceAuth to the video service, which uploads the
/// processed video here. createAccount verifies its own service auth.
pub const USER_SERVICE_AUTH_METHODS: &[&str] = &["com.atproto.repo.uploadBlob"];

/// Admin methods the moderation service may call (reference
/// `authVerifier.moderator`): a Bearer token on these is only ever a
/// moderation-service JWT. Other admin methods take Basic auth only.
pub const MODERATOR_METHODS: &[&str] = &[
    "com.atproto.admin.disableAccountInvites",
    "com.atproto.admin.disableInviteCodes",
    "com.atproto.admin.enableAccountInvites",
    "com.atproto.admin.getAccountInfo",
    "com.atproto.admin.getAccountInfos",
    "com.atproto.admin.getInviteCodes",
    "com.atproto.admin.getSubjectStatus",
    "com.atproto.admin.sendEmail",
    "com.atproto.admin.updateSubjectStatus",
];

/// Reference `authorizationOrModService`.
const MOD_SERVICE_OR_USER_METHODS: &[&str] = &["app.bsky.actor.getPreferences"];

/// Reference `additional: [AuthScope.Takendown]`.
pub const TAKENDOWN_METHODS: &[&str] = &[
    "app.bsky.actor.getPreferences",
    "com.atproto.identity.requestPlcOperationSignature",
    "com.atproto.identity.signPlcOperation",
    "com.atproto.moderation.createReport",
    "com.atproto.server.deactivateAccount",
    "com.atproto.server.getServiceAuth",
    "com.atproto.sync.getBlob",
    "com.atproto.sync.getRepo",
    "com.atproto.sync.listBlobs",
    "tools.ozone.inbox.appealActionedSubject",
];

impl Credentials {
    pub fn did(&self) -> Option<&str> {
        match self {
            Credentials::Session { did }
            | Credentials::AppPassword { did, .. }
            | Credentials::OAuth { did, .. }
            | Credentials::Takendown { did }
            | Credentials::UserServiceAuth { did } => Some(did),
            Credentials::Admin | Credentials::ModService { .. } => None,
        }
    }

    pub fn user_did(&self) -> XResult<&str> {
        self.did().ok_or_else(|| XrpcError::auth("user credentials required"))
    }

    /// action: "create" | "update" | "delete"
    pub fn allows_repo(&self, collection: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_repo(collection, action),
            Credentials::Takendown { .. } | Credentials::ModService { .. } | Credentials::UserServiceAuth { .. } => {
                false
            }
            _ => true,
        }
    }

    pub fn allows_rpc(&self, lxm: &str, aud: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_rpc(lxm, aud),
            Credentials::AppPassword { privileged, .. } => {
                *privileged || !lxm.get(..10).is_some_and(|p| p.eq_ignore_ascii_case("chat.bsky."))
            }
            Credentials::ModService { .. } | Credentials::UserServiceAuth { .. } => false,
            _ => true,
        }
    }

    pub fn allows_blob(&self, mime: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_blob(mime),
            Credentials::Takendown { .. } | Credentials::ModService { .. } => false,
            _ => true,
        }
    }

    /// action: "read" | "manage"
    pub fn allows_account(&self, attr: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_account(attr, action),
            Credentials::AppPassword { .. } => action == "read",
            Credentials::Takendown { .. } | Credentials::ModService { .. } | Credentials::UserServiceAuth { .. } => {
                false
            }
            _ => true,
        }
    }

    pub fn allows_identity(&self, attr: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_identity(attr),
            Credentials::AppPassword { .. }
            | Credentials::Takendown { .. }
            | Credentials::ModService { .. }
            | Credentials::UserServiceAuth { .. } => false,
            _ => true,
        }
    }

    /// OAuth refusals name the scope that would grant it (reference
    /// `ScopeMissingError`).
    fn require_scope(&self, ok: bool, scope: impl FnOnce() -> String) -> XResult<()> {
        if ok {
            return Ok(());
        }
        match self {
            Credentials::OAuth { .. } => Err(XrpcError {
                status: StatusCode::FORBIDDEN,
                error: "ScopeMissingError".into(),
                message: format!("Missing required scope \"{}\"", scope()),
            }),
            _ => self.require(false),
        }
    }

    pub fn need_repo(&self, collection: &str, action: &str) -> XResult<()> {
        self.require_scope(self.allows_repo(collection, action), || format!("repo:{collection}?action={action}"))
    }

    pub fn need_rpc(&self, lxm: &str, aud: &str) -> XResult<()> {
        // the reference pipethrough's answer to a non-privileged app password
        if matches!(self, Credentials::AppPassword { .. }) && !self.allows_rpc(lxm, aud) {
            return Err(XrpcError::bad("InvalidToken", "Bad token method"));
        }
        self.require_scope(self.allows_rpc(lxm, aud), || format!("rpc:{lxm}?aud={}", aud.replace('#', "%23")))
    }

    pub fn need_blob(&self, mime: &str) -> XResult<()> {
        self.require_scope(self.allows_blob(mime), || format!("blob:{mime}"))
    }

    pub fn need_account(&self, attr: &str, action: &str) -> XResult<()> {
        self.require_scope(self.allows_account(attr, action), || match action {
            "read" => format!("account:{attr}"),
            _ => format!("account:{attr}?action={action}"),
        })
    }

    pub fn need_identity(&self, attr: &str) -> XResult<()> {
        self.require_scope(self.allows_identity(attr), || format!("identity:{attr}"))
    }

    fn require(&self, ok: bool) -> XResult<()> {
        if ok {
            Ok(())
        } else {
            Err(XrpcError {
                status: StatusCode::FORBIDDEN,
                error: "InsufficientScope".into(),
                message: "credentials do not grant this action".into(),
            })
        }
    }
}

pub async fn authenticate(app: &App, parts: &Parts) -> XResult<Credentials> {
    let h = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| XrpcError::auth("Authentication Required"))?;
    if let Some(tok) = h.strip_prefix("Bearer ") {
        let nsid = parts.uri.path().strip_prefix("/xrpc/").unwrap_or("");
        if MODERATOR_METHODS.contains(&nsid)
            || (MOD_SERVICE_OR_USER_METHODS.contains(&nsid) && is_mod_service_token(app, tok))
        {
            return verify_mod_service(app, tok.trim(), nsid).await;
        }
        if USER_SERVICE_AUTH_METHODS.contains(&nsid) && has_lxm(tok.trim()) {
            return verify_user_service_auth(app, tok.trim(), nsid).await;
        }
        let creds = super::server::verify_bearer(app, tok).await?;
        if matches!(creds, Credentials::Takendown { .. }) && !TAKENDOWN_METHODS.contains(&nsid) {
            return Err(XrpcError::bad("InvalidToken", "Bad token scope"));
        }
        return Ok(creds);
    }
    if let Some(tok) = h.strip_prefix("DPoP ") {
        return super::oauth::verify_dpop(app, tok, parts).await;
    }
    if let Some(b) = h.strip_prefix("Basic ") {
        if crate::auth::basic_admin_ok(b, &app.admin_token) {
            return Ok(Credentials::Admin);
        }
        return Err(XrpcError::auth("invalid admin credentials"));
    }
    Err(XrpcError::auth("unsupported authorization scheme"))
}

/// Reference `authVerifier.modService`.
async fn verify_mod_service(app: &App, tok: &str, nsid: &str) -> XResult<Credentials> {
    let Some(m) = app.config.mod_service_did.as_deref() else {
        return Err(service_auth_err("UntrustedIss", "Untrusted issuer"));
    };
    let trusted = [m.to_string(), format!("{m}#atproto_labeler")];
    let sa = verify_jwt(app, tok, Some(nsid), Some(&trusted), false).await?;
    Ok(Credentials::ModService { iss: sa.iss })
}

/// Reference `userServiceAuth`: no `jti` replay check and no `iat` bound,
/// as there (getServiceAuth tokens live at most an hour). Account status is
/// the handler's business.
async fn verify_user_service_auth(app: &App, tok: &str, nsid: &str) -> XResult<Credentials> {
    let sa = verify_jwt(app, tok, Some(nsid), None, true).await?;
    Ok(Credentials::UserServiceAuth { did: sa.iss })
}

/// Anything but an account hosted here is the reference's actor-store miss.
async fn local_account_iss(app: &App, iss: &str) -> XResult<()> {
    if iss.contains('#') || super::server::account_if_exists(app, iss).await?.is_none() {
        return Err(XrpcError::bad("NotFound", "Repo not found"));
    }
    Ok(())
}

/// A JWT's payload, unverified: only for routing a token to its verifier.
fn unverified_claims(tok: &str) -> Option<J> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use base64::Engine;
    let b = B64.decode(tok.split('.').nth(1)?).ok()?;
    serde_json::from_slice(&b).ok()
}

/// Reference `isDefinitelyServiceAuth`: session and OAuth tokens never carry
/// `lxm`.
fn has_lxm(tok: &str) -> bool {
    unverified_claims(tok).is_some_and(|c| c.get("lxm").is_some_and(|l| !l.is_null()))
}

fn is_mod_service_token(app: &App, tok: &str) -> bool {
    let Some(m) = app.config.mod_service_did.as_deref() else {
        return false;
    };
    unverified_claims(tok).is_some_and(|c| c["iss"].as_str().is_some_and(|i| i.split('#').next() == Some(m)))
}

/// Signature and `typ` checked once per token until its `exp`; claims, the
/// DPoP proof and the session are still checked per request. Entries
/// remember the verifying key: one process can run several servers.
pub fn verify_access_token(
    server: &crate::oauth::jose::ServerKey,
    token: &str,
) -> Result<Arc<crate::oauth::jose::DecodedJwt>, String> {
    type Verified = (Arc<str>, Arc<crate::oauth::jose::DecodedJwt>);
    static CACHE: std::sync::LazyLock<Arc<crate::auth::TokenCache<Verified>>> =
        std::sync::LazyLock::new(|| crate::auth::TokenCache::tracked(crate::caches::Cache::OAuthTokens));
    let now = crate::tid::now_micros() / 1_000_000;
    if let Some((kid, jwt)) = CACHE.get(token, now) {
        if *kid == *server.kid {
            return Ok(jwt);
        }
    }
    let jwt = Arc::new(server.verify(token, "at+jwt")?);
    if let Some(exp) = jwt.claim_i64("exp").and_then(|e| u64::try_from(e).ok()) {
        CACHE.put(token, (server.kid.as_str().into(), jwt.clone()), exp, now);
    }
    Ok(jwt)
}

/// A forwarded request's authentication waits at most this long, leaving
/// the owner's write start (`--forwarded-write-start-ms`) its time before
/// the entry node's [`crate::forward::TTFB_FAST`].
const FORWARDED_AUTH_WAIT: std::time::Duration = std::time::Duration::from_millis(1500);

/// [`authenticate`]; on a forwarded Bearer request (with not-applied answers
/// on), past [`FORWARDED_AUTH_WAIT`] it answers 503 `RepoLoading` instead:
/// nothing was done, so the entry node resends it rather than failing it at
/// its deadline. That wait is the account's security controls loading cold
/// (every account of a shard after a takeover); the load goes on in the
/// background, so the resend finds it cached. A DPoP proof claimed by then
/// is given back with that answer (`oauth::dpop_layer`), so the resend's
/// same proof is accepted; one cancelled while its claim was in flight
/// stays claimed, and its resend is refused.
async fn authenticate_within(app: &Arc<App>, parts: &Parts) -> XResult<Credentials> {
    let h = parts.headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    let (bearer, dpop) = (h.and_then(|h| h.strip_prefix("Bearer ")), h.and_then(|h| h.strip_prefix("DPoP ")));
    if (bearer.is_none() && dpop.is_none())
        || app.config.forwarded_write_start.is_none()
        || !crate::forward::is_forwarded()
    {
        return authenticate(app, parts).await;
    }
    match tokio::time::timeout(FORWARDED_AUTH_WAIT, authenticate(app, parts)).await {
        Ok(r) => r,
        Err(_) => {
            let sub = match (bearer, dpop) {
                (Some(t), _) => app.jwt.verify_signature_cached(t.trim()).map(|c| c.sub.clone()),
                (_, Some(t)) => super::oauth::access_token_sub(app, t.trim()),
                _ => None,
            };
            if let Some(did) = sub.filter(|d| d.starts_with("did:")) {
                let app = app.clone();
                tokio::spawn(async move {
                    let _ = super::server::ctl(&app, &did).await;
                });
            }
            Err(XrpcError::unavailable(
                crate::forward::REPO_LOADING,
                format!("account state still loading after {} ms; not applied, retry", FORWARDED_AUTH_WAIT.as_millis()),
            ))
        }
    }
}

pub struct Auth(pub Credentials);

impl FromRequestParts<Arc<App>> for Auth {
    type Rejection = XrpcError;
    async fn from_request_parts(parts: &mut Parts, app: &Arc<App>) -> Result<Self, Self::Rejection> {
        authenticate_within(app, parts).await.map(Auth)
    }
}

pub struct MaybeAuth(pub Option<Credentials>);

impl FromRequestParts<Arc<App>> for MaybeAuth {
    type Rejection = XrpcError;
    async fn from_request_parts(parts: &mut Parts, app: &Arc<App>) -> Result<Self, Self::Rejection> {
        if parts.headers.get(header::AUTHORIZATION).is_none() {
            return Ok(MaybeAuth(None));
        }
        authenticate_within(app, parts).await.map(|c| MaybeAuth(Some(c)))
    }
}

/// The caller's DID, which `repo` (handle or DID) must name.
pub async fn authed_repo(app: &App, creds: &Credentials, repo: &str) -> XResult<Arc<str>> {
    let did = creds.user_did()?;
    let target = app.resolve_repo(repo).await?;
    if *target != *did {
        // reference createRecord/putRecord/deleteRecord/applyWrites:
        // `if (did !== auth.credentials.did) throw new AuthRequiredError()`
        return Err(XrpcError::auth("Authentication Required"));
    }
    Ok(target)
}

/// A verified inter-service JWT: `iss` is a DID, optionally `#service`.
#[derive(Clone, Debug)]
pub struct ServiceAuth {
    pub iss: String,
}

fn service_auth_err(error: &str, message: &str) -> XrpcError {
    XrpcError { status: StatusCode::UNAUTHORIZED, error: error.into(), message: message.into() }
}

/// The `#atproto` (`#atproto_label` for a `#atproto_labeler` issuer) key of
/// `iss`, as multibase. `fresh` skips the resolver's cache.
async fn issuer_key(app: &App, iss: &str, fresh: bool) -> XResult<String> {
    let (did, service) = iss.split_once('#').unwrap_or((iss, ""));
    let key_id = if service == "atproto_labeler" { "atproto_label" } else { "atproto" };
    if key_id == "atproto" {
        if let Ok(a) = app.account(did).await {
            if super::identity::serves_local_doc(app, &a) {
                return Ok(a.signing_pubkey);
            }
        }
    }
    if fresh && !app.did_resolver.refresh(did) {
        // refreshed recently: the cached key is the current one as far as
        // we may know (no re-fetch per forged token)
        return Err(service_auth_err("BadJwtSignature", "jwt signature does not match jwt issuer"));
    }
    let doc = app
        .did_resolver
        .resolve(did)
        .await
        .map_err(|_| service_auth_err("AuthenticationRequired", "could not resolve iss did"))?;
    let full = format!("{did}#{key_id}");
    let short = format!("#{key_id}");
    doc.get("verificationMethod")
        .and_then(|v| v.as_array())
        .and_then(|ms| {
            ms.iter().find_map(|m| {
                let id = m.get("id")?.as_str()?;
                (id == full || id == short).then(|| m.get("publicKeyMultibase")?.as_str().map(String::from))?
            })
        })
        .ok_or_else(|| service_auth_err("AuthenticationRequired", "missing or bad key in did doc"))
}

/// An inter-service JWT addressed to this PDS, with the reference's errors
/// (xrpc-server verifyJwt). High-S signatures are accepted, as the
/// reference does for service JWTs (`allowMalleableSig`).
pub async fn verify_service_jwt(app: &App, token: &str, lxm: Option<&str>) -> XResult<ServiceAuth> {
    verify_jwt(app, token, lxm, None, false).await
}

/// `trusted` (exact `iss`) and `local_iss` are checked before the issuer's
/// key is resolved, so a forged token naming a foreign DID costs no
/// outbound DID fetch.
async fn verify_jwt(
    app: &App,
    token: &str,
    lxm: Option<&str>,
    trusted: Option<&[String]>,
    local_iss: bool,
) -> XResult<ServiceAuth> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use base64::Engine;
    let parts: Vec<&str> = token.split('.').collect();
    let [h, p, s] = parts[..] else {
        return Err(service_auth_err("BadJwt", "poorly formatted jwt"));
    };
    let decode = |x: &str| -> XResult<J> {
        let b = B64.decode(x).map_err(|_| service_auth_err("BadJwt", "poorly formatted jwt"))?;
        serde_json::from_slice(&b).map_err(|_| service_auth_err("BadJwt", "poorly formatted jwt"))
    };
    let header = decode(h)?;
    let payload = decode(p)?;
    if let Some(t @ ("at+jwt" | "refresh+jwt" | "dpop+jwt")) = header["typ"].as_str() {
        return Err(service_auth_err("BadJwtType", &format!("Invalid jwt type \"{t}\"")));
    }
    let exp = payload["exp"].as_f64().ok_or_else(|| service_auth_err("BadJwt", "poorly formatted jwt"))?;
    if (crate::tid::now_micros() as f64) / 1e6 > exp {
        return Err(service_auth_err("JwtExpired", "jwt expired"));
    }
    if payload["aud"].as_str() != Some(app.jwt.service_did.as_str()) {
        return Err(service_auth_err("BadJwtAudience", "jwt audience does not match service did"));
    }
    if let Some(lxm) = lxm {
        let got = payload["lxm"].as_str();
        if got != Some(lxm) {
            let what = if got.is_some() { "bad" } else { "missing" };
            return Err(service_auth_err(
                "BadJwtLexiconMethod",
                &format!("{what} jwt lexicon method (\"lxm\"). must match: {lxm}"),
            ));
        }
    }
    let iss = payload["iss"].as_str().unwrap_or("");
    if !super::syntax::valid_did(iss.split('#').next().unwrap_or("")) {
        return Err(service_auth_err("BadJwtIss", "jwt iss is not a valid did"));
    }
    if trusted.is_some_and(|t| !t.iter().any(|x| x == iss)) {
        return Err(service_auth_err("UntrustedIss", "Untrusted issuer"));
    }
    if local_iss {
        local_account_iss(app, iss).await?;
    }
    let msg = format!("{h}.{p}");
    let sig = B64.decode(s).map_err(|_| service_auth_err("BadJwtSignature", "could not verify jwt signature"))?;
    let check = |key: &str| {
        crate::oauth::lexicon::verify_sig_malleable(key, msg.as_bytes(), &sig)
            .map_err(|_| service_auth_err("BadJwtSignature", "could not verify jwt signature"))
    };
    let key = issuer_key(app, iss, false).await?;
    if !check(&key)? {
        // the key may have just been rotated
        let fresh = issuer_key(app, iss, true).await?;
        if fresh == key || !check(&fresh)? {
            return Err(service_auth_err("BadJwtSignature", "jwt signature does not match jwt issuer"));
        }
    }
    Ok(ServiceAuth { iss: iss.to_string() })
}

/// Reference `userServiceAuthOptional`: a Bearer token must be a valid
/// service JWT for `lxm`; no header or another scheme is unauthenticated.
pub async fn optional_service_auth(app: &App, headers: &HeaderMap, lxm: &str) -> XResult<Option<ServiceAuth>> {
    let bearer =
        headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    match bearer {
        Some(tok) => verify_jwt(app, tok.trim(), Some(lxm), None, false).await.map(Some),
        None => Ok(None),
    }
}
