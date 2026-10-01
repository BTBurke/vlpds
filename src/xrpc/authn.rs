//! Request authentication. Every authenticated handler takes `Auth`, which
//! dispatches on the Authorization scheme:
//!   Bearer <jwt>  -> legacy session / app-password tokens   (server.rs)
//!   DPoP <token>  -> OAuth access tokens bound to a DPoP key (oauth.rs)
//!   Basic admin:<token> -> admin
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
}

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
            | Credentials::Takendown { did } => Some(did),
            Credentials::Admin => None,
        }
    }

    /// action: "create" | "update" | "delete"
    pub fn allows_repo(&self, collection: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_repo(collection, action),
            Credentials::Takendown { .. } => false,
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
            _ => true,
        }
    }

    pub fn allows_blob(&self, mime: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_blob(mime),
            Credentials::Takendown { .. } => false,
            _ => true,
        }
    }

    /// Account management (attr e.g. "email", "repo", "status"; action "read" | "manage").
    pub fn allows_account(&self, attr: &str, action: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_account(attr, action),
            // app passwords can't manage the account (see server.rs for specifics)
            Credentials::AppPassword { .. } => action == "read",
            Credentials::Takendown { .. } => false,
            _ => true,
        }
    }

    /// Identity changes (attr "handle" | "*").
    pub fn allows_identity(&self, attr: &str) -> bool {
        match self {
            Credentials::OAuth { scopes, .. } => scopes.allows_identity(attr),
            Credentials::AppPassword { .. } | Credentials::Takendown { .. } => false,
            _ => true,
        }
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
        .ok_or_else(|| XrpcError::auth("authentication required"))?;
    if let Some(tok) = h.strip_prefix("Bearer ") {
        let creds = super::server::verify_bearer(app, tok).await?;
        if matches!(creds, Credentials::Takendown { .. }) {
            let nsid = parts.uri.path().strip_prefix("/xrpc/").unwrap_or("");
            if !TAKENDOWN_METHODS.contains(&nsid) {
                return Err(XrpcError::bad("InvalidToken", "Bad token scope"));
            }
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
        return Err(XrpcError {
            status: StatusCode::FORBIDDEN,
            error: "Forbidden".into(),
            message: "token does not match repo".into(),
        });
    }
    Ok(target)
}
