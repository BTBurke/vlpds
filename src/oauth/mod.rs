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
//!   reuse can revoke the session it created. The id is minted so that this
//!   row lands on a partition of the node that ran PAR.
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
//! ## HA: which node does what
//! Every row has a routing key (above) and so one owning node at a time.
//! Reads and writes from other nodes go through `put_private` /
//! `get_private` (forwarded to the owner), so any node *can* serve any
//! step; what needs one node is single use. `crate::forward` routes
//! `/oauth/*` by `xrpc::oauth::route_key` (stateless: the key comes from the
//! request itself):
//! - `/oauth/par`: the `login_hint` account's owner if there is one, else
//!   the receiving node; either way the request id is minted local to the
//!   node that runs it, so the flow tends to stay on one node.
//! - `/oauth/authorize` (GET), `.../select`, `.../consent`: the request
//!   row's owner (`request_uri` -> `oauth:req:{id}`).
//! - `.../sign-in` and `/oauth/account/sign-in`: the account's owner, from
//!   the identifier (handle / DID / email, resolved through the global
//!   handle and email claims); the authenticator-code step names no account
//!   and goes to the owner of the device's pending one. Per-account rate
//!   limits, the TOTP lockout and the account record are then local.
//! - `/oauth/token`: a code -> its request row's owner (codes embed the
//!   request id); a refresh token -> its session's account owner (refresh
//!   tokens embed the DID). `/oauth/revoke` likewise (access tokens by `sub`).
//! - `/oauth/account` pages and the rest: any node (account records and
//!   session lists are read from their owners).
//!
//! Single use, cluster-wide:
//! - Code exchange and refresh rotation run only on the owner of the code's
//!   request row / the session's account (`require_owner`; 503
//!   `temporarily_unavailable` mid-handoff, so clients retry), under a lock
//!   on that node (`store::lock`), so concurrent uses that hit different
//!   nodes are serialized there: exactly one wins. What makes them correct
//!   against revocations from any node is the write itself: a session is
//!   written conditionally at its owner (`store::put_session_if`,
//!   src/xrpc/cas.rs; DESIGN.md "Auth state under concurrency"), on the row
//!   it read (refresh) or on the account's credential epoch of the
//!   approving login (code exchange), and every other OAuth row write goes
//!   through the same per-account lock, so a revoke-all (password change,
//!   takedown) is never undone by a refresh or exchange in flight.
//! - DPoP proof, client-assertion and request-object (JAR) `jti`s are claimed
//!   at the owner of a routing key (`xrpc::internal::claim_replay_anywhere`):
//!   a resource request's proof under the access token's DID (`ath` binds it
//!   to that token, and the request was routed there, so this is normally
//!   local), an authorization-server proof under its key (`oauth:jkt:{jkt}`),
//!   assertions and request objects under the client (`oauth:client:{hash}`).
//!   The owner's in-memory TTL sets (one per claim kind, bounded per routing
//!   key; full, they evict instead of refusing: `util::ReplayCache`) settle
//!   concurrent claims. Claims at the
//!   authorization server (token endpoint and PAR proofs, client assertions,
//!   request objects) are also persisted as `oauth/replay/{hash}` in that
//!   partition (awaited before the claim counts) and a claim missing from
//!   memory is checked there, so the owner after a failover or handoff,
//!   starting with an empty set, still refuses a proof its predecessor
//!   accepted. The rows are dropped by the GC once past their validity
//!   window. PKCE `code_challenge` reuse: the durable 24 h marker plus a
//!   short, memory-only claim at its owner for concurrent PARs.
//! - Resource-request proofs are claimed in the owner's memory only, like
//!   the reference's in-memory replay store: a durable claim would put a log
//!   write (an S3 segment PUT) on every authenticated request, AppView
//!   proxying included. Residual risk: right after a failover or handoff the
//!   new owner starts with an empty set, so a proof captured from a request
//!   the old owner served could be replayed once, within the proof's `iat`
//!   window and only while its nonce is still accepted (nonce rotation
//!   bounds it), and only together with the access token it is bound to
//!   (`ath`), which itself stays revocable and short-lived.

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
        // transient 503s (shard moving, Argon2 shed, KMS down) stay
        // retryable: RFC 6749 temporarily_unavailable, not server_error
        if e.status == StatusCode::SERVICE_UNAVAILABLE {
            return OAuthError::new(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable", &e.message);
        }
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
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        r
    }
}
