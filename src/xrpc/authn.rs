//! Request authentication. Every authenticated handler takes `Auth`, which
//! dispatches on the Authorization scheme:
//!   Bearer <jwt>  -> legacy session / app-password tokens   (server.rs)
//!   DPoP <token>  -> OAuth access tokens bound to a DPoP key (oauth.rs)
//!   Basic admin:<token> -> admin
//!   Bearer <service jwt> on a moderator method -> the moderation service
//!     (`--mod-service-did`; [`MODERATOR_METHODS`])
//!   Bearer <service jwt with `lxm`> on uploadBlob -> a user's own service
//!     JWT ([`USER_SERVICE_AUTH_METHODS`]; e.g. the video service uploading
//!     a processed video for the user)
//! and yields `Credentials`, whose `allows_*` methods are the single place
//! permission checks happen (OAuth scopes, app-password restrictions).

use super::*;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

#[derive(Clone, Debug)]
pub enum Credentials {
    /// Full-access session from createSession with the account password.
    Session {
        did: String,
    },
    /// App-password session. Privileged app passwords may use DMs.
    AppPassword {
        did: String,
        privileged: bool,
    },
    /// OAuth access token (DPoP-bound) with its granted scopes.
    OAuth {
        did: String,
        client_id: String,
        scopes: super::oauth::ScopeSet,
    },
    Admin,
    /// Restricted session of a taken-down account (createSession with
    /// `allowTakendown`; scope `com.atproto.takendown`). Accepted only by
    /// the methods in [`TAKENDOWN_METHODS`]; grants no repo, blob, account
    /// or identity actions.
    Takendown {
        did: String,
    },
    /// A service JWT from the configured moderation service
    /// (`--mod-service-did`), on one of [`MODERATOR_METHODS`] (or
    /// getPreferences, for any account's preferences). `iss` is the token's
    /// issuer (the DID, or `DID#atproto_labeler`). Grants nothing else.
    ModService {
        iss: String,
    },
    /// A service JWT issued by a user hosted here, with its own `#atproto`
    /// key, addressed to this PDS for the method being called (the
    /// reference's `userServiceAuth`; from getServiceAuth). Accepted only on
    /// [`USER_SERVICE_AUTH_METHODS`]; grants blob uploads and nothing else.
    UserServiceAuth {
        did: String,
    },
}

/// Methods that also take a user's service JWT (the reference's
/// `authorizationOrUserServiceAuth`): a Bearer token carrying an `lxm`
/// claim is verified as one ([`verify_user_service_auth`]), anything else
/// as a session. The Bluesky app's video upload depends on it: the app gets
/// a token (`aud` = this PDS, `lxm` = uploadBlob) from getServiceAuth and
/// hands it to the video service, which uploads the processed video here.
/// (createAccount takes service auth too, `userServiceAuthOptional`; it is
/// verified in its handler: `super::server::create_account`.)
pub const USER_SERVICE_AUTH_METHODS: &[&str] = &["com.atproto.repo.uploadBlob"];

/// Admin methods the moderation service may call with service auth (the
/// reference's `authVerifier.moderator`): a Bearer token on these is only
/// ever a moderation-service JWT. The other admin methods (deleteAccount,
/// updateAccountEmail/Handle/Password, createInviteCode(s), ...) take admin
/// Basic auth only (`adminToken`).
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

/// The reference's `authorizationOrModService`: user auth, or the
/// moderation service reading an account's preferences (`?did=`).
const MOD_SERVICE_OR_USER_METHODS: &[&str] = &["app.bsky.actor.getPreferences"];

/// Methods that accept the `com.atproto.takendown` scope (the reference's
/// `additional: [AuthScope.Takendown]`).
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

    /// action: "create" | "update" | "delete"
    pub fn allows_repo(&self, collection: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_repo(collection, action),
            Credentials::Takendown { .. } | Credentials::ModService { .. } | Credentials::UserServiceAuth { .. } => false,
            _ => true,
        }
    }

    /// Proxied / service-auth calls to method `lxm` at service `aud`.
    pub fn allows_rpc(&self, lxm: &str, aud: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_rpc(lxm, aud),
            Credentials::AppPassword { privileged, .. } => {
                *privileged || !lxm.starts_with("chat.bsky.")
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

    /// Account management (attr e.g. "email", "repo", "status"; action "read" | "manage").
    pub fn allows_account(&self, attr: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_account(attr, action),
            // app passwords can't manage the account (see server.rs for specifics)
            Credentials::AppPassword { .. } => action == "read",
            Credentials::Takendown { .. } | Credentials::ModService { .. } | Credentials::UserServiceAuth { .. } => false,
            _ => true,
        }
    }

    /// Identity changes (attr "handle" | "*").
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

    /// `ok`, else the refusal: for OAuth the reference's
    /// `ScopeMissingError` (403, `Missing required scope "<scope>"`, naming
    /// the scope that would grant it), else [`Self::require`]'s.
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
        self.require_scope(self.allows_repo(collection, action), || {
            format!("repo:{collection}?action={action}")
        })
    }

    pub fn need_rpc(&self, lxm: &str, aud: &str) -> XResult<()> {
        // a non-privileged app password calling a privileged (chat) method:
        // the reference's pipethrough "Bad token method"
        if matches!(self, Credentials::AppPassword { .. }) && !self.allows_rpc(lxm, aud) {
            return Err(XrpcError::bad("InvalidToken", "Bad token method"));
        }
        self.require_scope(self.allows_rpc(lxm, aud), || {
            format!("rpc:{lxm}?aud={}", aud.replace('#', "%23"))
        })
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

    pub fn require(&self, ok: bool) -> XResult<()> {
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

/// The reference's `authVerifier.modService`: a service JWT for `nsid`
/// issued by the configured moderation service (its DID, or
/// `DID#atproto_labeler` signed with the `#atproto_label` key). No
/// moderation service configured, or another issuer: 401 UntrustedIss
/// "Untrusted issuer".
async fn verify_mod_service(app: &App, tok: &str, nsid: &str) -> XResult<Credentials> {
    let Some(m) = app.config.mod_service_did.as_deref() else {
        return Err(service_auth_err("UntrustedIss", "Untrusted issuer"));
    };
    let trusted = [m.to_string(), format!("{m}#atproto_labeler")];
    let sa = verify_service_jwt_from(app, tok, Some(nsid), Some(&trusted)).await?;
    Ok(Credentials::ModService { iss: sa.iss })
}

/// The reference's `userServiceAuth`: a service JWT for `nsid` (`lxm` must
/// match), `aud` exactly our service DID (no `#fragment`; there is no
/// entryway), signed with the issuer's current `#atproto` key (an older,
/// rotated key is refused), not expired. No `jti` replay check and no
/// `iat` bound, as in the reference (the token lives at most an hour:
/// getServiceAuth). The issuer must be an account hosted here: anything else
/// (including a `did#service` issuer) is the reference's actor-store miss,
/// 400 NotFound "Repo not found". Account status is the handler's business.
async fn verify_user_service_auth(app: &App, tok: &str, nsid: &str) -> XResult<Credentials> {
    let sa = verify_service_jwt(app, tok, Some(nsid)).await?;
    let repo_not_found = || XrpcError::bad("NotFound", "Repo not found");
    if sa.iss.contains('#') {
        return Err(repo_not_found());
    }
    match app.account(&sa.iss).await {
        Ok(_) => Ok(Credentials::UserServiceAuth { did: sa.iss }),
        Err(e) if e.error == "AccountNotFound" => Err(repo_not_found()),
        Err(e) => Err(e),
    }
}

/// Does `tok` (unverified) carry an `lxm` claim? The reference's
/// `isDefinitelyServiceAuth`: session and OAuth tokens never do, so such a
/// token is a service JWT. Undecodable tokens are not (they fail as
/// sessions).
fn has_lxm(tok: &str) -> bool {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use base64::Engine;
    tok.split('.')
        .nth(1)
        .and_then(|p| B64.decode(p).ok())
        .and_then(|b| serde_json::from_slice::<J>(&b).ok())
        .is_some_and(|c| c.get("lxm").is_some_and(|l| !l.is_null()))
}

/// Is `tok` (unverified) a JWT issued by the configured moderation service?
/// Only routes getPreferences between user and moderation-service auth; the
/// token is then verified as such.
fn is_mod_service_token(app: &App, tok: &str) -> bool {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use base64::Engine;
    let Some(m) = app.config.mod_service_did.as_deref() else {
        return false;
    };
    let iss = tok
        .split('.')
        .nth(1)
        .and_then(|p| B64.decode(p).ok())
        .and_then(|b| serde_json::from_slice::<J>(&b).ok())
        .and_then(|c| c["iss"].as_str().map(String::from));
    iss.is_some_and(|i| i.split('#').next() == Some(m))
}

/// OAuth access tokens verified by the server's ES256 key: signature and
/// `typ` checked and the token decoded once per token (until its `exp`), as
/// [`crate::auth::Jwt::verify_signature_cached`] does for legacy tokens.
/// Entries remember the key that verified them (one process can run several
/// servers, each with its own key). Claims, the DPoP proof and the session
/// are still checked per request by `oauth::verify_dpop`.
pub fn verify_access_token(
    server: &crate::oauth::jose::ServerKey,
    token: &str,
) -> Result<Arc<crate::oauth::jose::DecodedJwt>, String> {
    type Verified = (Arc<str>, Arc<crate::oauth::jose::DecodedJwt>);
    // capped by the memory budget (crate::caches; ~1.5 KB each)
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

/// Extractor: authenticated request.
pub struct Auth(pub Credentials);

impl FromRequestParts<Arc<App>> for Auth {
    type Rejection = XrpcError;
    async fn from_request_parts(
        parts: &mut Parts,
        app: &Arc<App>,
    ) -> Result<Self, Self::Rejection> {
        authenticate(app, parts).await.map(Auth)
    }
}

/// Extractor: authentication if present (for endpoints with optional auth).
pub struct MaybeAuth(pub Option<Credentials>);

impl FromRequestParts<Arc<App>> for MaybeAuth {
    type Rejection = XrpcError;
    async fn from_request_parts(
        parts: &mut Parts,
        app: &Arc<App>,
    ) -> Result<Self, Self::Rejection> {
        if parts.headers.get(header::AUTHORIZATION).is_none() {
            return Ok(MaybeAuth(None));
        }
        authenticate(app, parts).await.map(|c| MaybeAuth(Some(c)))
    }
}

/// The authenticated user's DID must be `repo` (handle or DID); returns the DID.
pub async fn authed_repo(app: &App, creds: &Credentials, repo: &str) -> XResult<Arc<str>> {
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?;
    let target = app.resolve_repo(repo).await?;
    if *target != *did {
        // reference createRecord/putRecord/deleteRecord/applyWrites:
        // `if (did !== auth.credentials.did) throw new AuthRequiredError()`
        return Err(XrpcError::auth("Authentication Required"));
    }
    Ok(target)
}

// ---------------------------------------------------------------------------
// inbound service auth (reference xrpc-server verifyJwt + AuthVerifier
// verifyServiceJwt)
// ---------------------------------------------------------------------------

/// A verified inter-service JWT: `iss` is a DID (optionally `#service`).
#[derive(Clone, Debug)]
pub struct ServiceAuth {
    pub iss: String,
}

impl ServiceAuth {
    /// The issuing DID, without a `#service` fragment.
    pub fn did(&self) -> &str {
        self.iss.split('#').next().unwrap_or("")
    }
}

fn service_auth_err(error: &str, message: &str) -> XrpcError {
    XrpcError {
        status: StatusCode::UNAUTHORIZED,
        error: error.into(),
        message: message.into(),
    }
}

/// The `#atproto` (or `#atproto_label` for a `#atproto_labeler` issuer)
/// signing key of `iss`, as multibase. Accounts hosted here use their local
/// document; others resolve through the DID resolver (`fresh` skips its cache).
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
    if fresh {
        app.did_resolver.invalidate(did);
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

/// Verifies an inter-service JWT addressed to this PDS (`aud` = our service
/// DID) for method `lxm` (required to match when given), signed by its
/// issuer's current key (retried once with a fresh DID document, for a
/// recent key rotation). High-S signatures are accepted, as the reference
/// does for service JWTs (`allowMalleableSig`). Errors are the reference's
/// (BadJwt, JwtExpired, BadJwtAudience, BadJwtLexiconMethod, BadJwtIss,
/// BadJwtSignature).
pub async fn verify_service_jwt(app: &App, token: &str, lxm: Option<&str>) -> XResult<ServiceAuth> {
    verify_service_jwt_from(app, token, lxm, None).await
}

/// [`verify_service_jwt`], accepting only the issuers in `trusted` (when
/// given; exact `iss`, fragment included): any other is 401 UntrustedIss
/// "Untrusted issuer", checked before the issuer's key is resolved.
pub async fn verify_service_jwt_from(
    app: &App,
    token: &str,
    lxm: Option<&str>,
    trusted: Option<&[String]>,
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
        match payload["lxm"].as_str() {
            Some(l) if l == lxm => {}
            Some(_) => {
                return Err(service_auth_err(
                    "BadJwtLexiconMethod",
                    &format!("bad jwt lexicon method (\"lxm\"). must match: {lxm}"),
                ))
            }
            None => {
                return Err(service_auth_err(
                    "BadJwtLexiconMethod",
                    &format!("missing jwt lexicon method (\"lxm\"). must match: {lxm}"),
                ))
            }
        }
    }
    let iss = payload["iss"].as_str().unwrap_or("");
    let did_ok = {
        let did = iss.split('#').next().unwrap_or("");
        super::syntax::valid_did(did)
    };
    if !did_ok {
        return Err(service_auth_err("BadJwtIss", "jwt iss is not a valid did"));
    }
    if trusted.is_some_and(|t| !t.iter().any(|x| x == iss)) {
        return Err(service_auth_err("UntrustedIss", "Untrusted issuer"));
    }
    let msg = format!("{h}.{p}");
    let sig = B64.decode(s).map_err(|_| service_auth_err("BadJwtSignature", "could not verify jwt signature"))?;
    let check = |key: &str| {
        crate::oauth::lexicon::verify_sig_malleable(key, msg.as_bytes(), &sig)
            .map_err(|_| service_auth_err("BadJwtSignature", "could not verify jwt signature"))
    };
    let key = issuer_key(app, iss, false).await?;
    if !check(&key)? {
        // a fresh document, in case the key was just rotated
        let fresh = issuer_key(app, iss, true).await?;
        if fresh == key || !check(&fresh)? {
            return Err(service_auth_err("BadJwtSignature", "jwt signature does not match jwt issuer"));
        }
    }
    Ok(ServiceAuth { iss: iss.to_string() })
}

/// Optional service auth (reference `userServiceAuthOptional`): a Bearer
/// token must be a valid service JWT for `lxm`; anything else (no
/// Authorization header, another scheme) is unauthenticated.
pub async fn optional_service_auth(app: &App, headers: &HeaderMap, lxm: &str) -> XResult<Option<ServiceAuth>> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match bearer {
        Some(tok) => verify_service_jwt(app, tok.trim(), Some(lxm)).await.map(Some),
        None => Ok(None),
    }
}
