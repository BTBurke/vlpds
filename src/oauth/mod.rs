//! atproto OAuth authorization server building blocks. The HTTP surface
//! (metadata, PAR, authorize UI, token, revoke, session management and DPoP
//! verification of resource requests) lives in `xrpc/oauth.rs`.
//!
//! ## Keys and secrets
//! Access tokens are ES256 JWTs signed with a P-256 key derived from the
//! server secret (`Config::jwt_secret`); its public JWK is served at
//! `/oauth/jwks`. The DPoP nonce secret, CSRF key and refresh-token MAC key
//! are derived from the same secret. All nodes of a deployment share it, so
//! any node can issue and verify tokens with no shared mutable state. Rotating
//! `jwt_secret` invalidates every outstanding OAuth token.
//!
//! ## Durable state
//! Everything persistent goes through the partition log via
//! `App::put_private` (never SlateDB directly), so any node can read it once
//! applied:
//! - `oauth:req:{id}` / `oauth/req`: pending authorization (PAR) requests,
//!   later the issued code; a consumed request is kept as a tombstone so code
//!   reuse can revoke the session it created.
//! - `oauth:cc:{hash}` / `oauth/cc`: PKCE `code_challenge` reuse index (24 h).
//! - `oauth:dev:{id}` / `oauth/dev`: browser device sessions (account chooser).
//! - `{did}` / `oauth/ses/{session id}`: OAuth sessions (one per grant),
//!   listed by prefix scan for the session-management UI and XRPC.
//! - `{did}` / `oauth/authz/{hash(client_id)}`: remembered consent.
//! - `oauth:lex:{nsid}` / `oauth/lex`: last good permission-set lexicons.
//!
//! Expired requests, code-challenge markers, idle devices and sessions past
//! their lifetime are deleted by a periodic, bounded sweep (`gc.rs`).
//!
//! Lookups by token value need no index: refresh tokens and codes embed the
//! routing information (DID + session id, request id) next to their secret.
//!
//! ## HA caveats
//! - DPoP proof, client-assertion and request-object (JAR) `jti`s are
//!   tracked in an in-memory TTL cache per process. Behind a load balancer
//!   a proof could be replayed once against a different node within its
//!   ~3-minute window (~1 minute for request objects); nonces bound that
//!   window. A shared cache would close it.
//! - Single-use operations (code exchange, refresh-token rotation) take a
//!   process-local lock before their read-modify-write. Requests for the same
//!   code/session that hit two different nodes concurrently are not
//!   serialized against each other.

pub mod client;
pub mod gc;
pub mod jose;
pub mod lexicon;
pub mod scopes;
pub mod store;
pub mod ui;
pub mod util;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;

pub use scopes::ScopeSet;

/// Access tokens live 15 minutes (spec: < 30 min when individually
/// revocable; ours are checked against the session store on every request).
pub const ACCESS_TOKEN_TTL: i64 = 15 * 60;
/// PAR request_uri lifetime.
pub const PAR_EXPIRES_IN: i64 = 5 * 60;
/// Inactivity timeout while the user is on the authorization page, and the
/// lifetime of an issued authorization code.
pub const AUTHORIZATION_INACTIVITY_TIMEOUT: i64 = 5 * 60;
/// A device login older than this must re-enter credentials.
pub const AUTHENTICATION_MAX_AGE: i64 = 7 * 86_400;
/// PKCE code_challenge values may not be reused within this window.
pub const CODE_CHALLENGE_REPLAY_TIMEFRAME: i64 = 86_400;

/// OAuth error response (`{"error", "error_description"}`).
#[derive(Debug, Clone)]
pub struct OAuthError {
    pub status: StatusCode,
    pub error: String,
    pub description: String,
}

impl OAuthError {
    pub fn new(status: StatusCode, error: &str, description: &str) -> OAuthError {
        OAuthError {
            status,
            error: error.into(),
            description: description.into(),
        }
    }
    pub fn invalid_request(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", d)
    }
    pub fn invalid_client(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_client", d)
    }
    pub fn invalid_client_metadata(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_client_metadata", d)
    }
    pub fn invalid_redirect_uri(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_redirect_uri", d)
    }
    pub fn invalid_grant(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_grant", d)
    }
    pub fn invalid_scope(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_scope", d)
    }
    pub fn unauthorized_client(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "unauthorized_client", d)
    }
    pub fn unsupported_grant_type(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "unsupported_grant_type", d)
    }
    pub fn use_dpop_nonce(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "use_dpop_nonce", d)
    }
    pub fn invalid_dpop_proof(d: &str) -> OAuthError {
        Self::new(StatusCode::BAD_REQUEST, "invalid_dpop_proof", d)
    }
    pub fn server_error(d: &str) -> OAuthError {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "server_error", d)
    }
}

impl From<crate::xrpc::XrpcError> for OAuthError {
    fn from(e: crate::xrpc::XrpcError) -> OAuthError {
        OAuthError::server_error(&e.message)
    }
}

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        let mut r = (
            self.status,
            Json(serde_json::json!({"error": self.error, "error_description": self.description})),
        )
            .into_response();
        r.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        r
    }
}
