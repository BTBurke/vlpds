//! atproto OAuth authorization server (PAR, PKCE, DPoP, client metadata,
//! token/refresh/revocation, consent UI, granular permission scopes) and
//! DPoP verification of resource requests. Building blocks live in
//! `crate::oauth`; see its module docs for keys, storage layout and HA notes.
//!
//! Endpoints:
//! - `GET /.well-known/oauth-protected-resource`, `/.well-known/oauth-authorization-server`
//! - `GET /oauth/jwks` (access-token verification key)
//! - `POST /oauth/par`, `GET /oauth/authorize` (+ form posts under it),
//!   `POST /oauth/token`, `POST /oauth/revoke`
//! - `GET /oauth/account` (+ form posts): signed-in user's connected apps
//!   (no-JS, cookie based; `/account/*` is the embedded web UI)
//! - `vlpds.oauth.listSessions` / `vlpds.oauth.revokeSession` (XRPC, full
//!   account session required)

use super::authn::Credentials;
use super::*;
use crate::oauth::client::{self, Client, ClientAuth, ClientCredentials};
use crate::oauth::jose::{self, DpopError, DpopNonces, DpopProof, ServerKey};
use crate::oauth::scopes::{is_atproto_did, is_atproto_oauth_scope};
use crate::oauth::store::{self, AuthParams, Device, DeviceAccount, RequestData, Session};
use crate::oauth::util::{self as ou, now_secs};
use crate::oauth::{
    lexicon, ui, OAuthError, ACCESS_TOKEN_TTL, AUTHENTICATION_MAX_AGE,
    AUTHORIZATION_INACTIVITY_TIMEOUT, PAR_EXPIRES_IN,
};
use axum::http::request::Parts;
use axum::http::{HeaderName, HeaderValue};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::LazyLock;

pub use crate::oauth::ScopeSet;

const DEVICE_COOKIE: &str = "vlpds-device";
const PENDING_2FA_TTL: i64 = 5 * 60;
/// Wrong authenticator codes per pending sign-in before the password step
/// must be redone (the per-account lockout in `crate::totp` still applies).
const PENDING_2FA_MAX_FAILURES: u32 = 3;

// ---------- per-secret keys ----------

struct Keys {
    server: ServerKey,
    nonces: DpopNonces,
    csrf: [u8; 32],
    refresh: [u8; 32],
}

/// Keys of the first secret seen: a production process has one, so the
/// per-request lookup (every DPoP request) is a compare against it,
/// lock-free and without cloning the secret.
static FIRST_KEYS: std::sync::OnceLock<(Box<str>, Keys)> = std::sync::OnceLock::new();
/// Further secrets (in-process tests run servers with several); their keys
/// are leaked once each, to live as long as the first's.
static OTHER_KEYS: LazyLock<parking_lot::Mutex<HashMap<Box<str>, &'static Keys>>> =
    LazyLock::new(Default::default);

fn derive_keys(secret: &str) -> Keys {
    Keys {
        server: ServerKey::derive(secret),
        nonces: DpopNonces::new(secret),
        csrf: ou::derive_secret(secret, "csrf"),
        refresh: ou::derive_secret(secret, "refresh-token"),
    }
}

fn keys(app: &App) -> &'static Keys {
    let secret = app.config.jwt_secret.as_str();
    let (first, k) = FIRST_KEYS.get_or_init(|| (secret.into(), derive_keys(secret)));
    if **first == *secret {
        return k;
    }
    let mut m = OTHER_KEYS.lock();
    if let Some(k) = m.get(secret) {
        return k;
    }
    let k: &'static Keys = Box::leak(Box::new(derive_keys(secret)));
    m.insert(secret.into(), k);
    k
}

fn issuer(app: &App) -> String {
    app.public_url.trim_end_matches('/').to_string()
}

fn is_https(app: &App) -> bool {
    app.public_url.starts_with("https://")
}

// ---------- routes ----------

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource_metadata).options(preflight),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server_metadata).options(preflight),
        )
        .route("/oauth/jwks", get(jwks).options(preflight))
        .route("/oauth/par", post(par).options(preflight))
        .route("/oauth/token", post(token).options(preflight))
        .route("/oauth/revoke", post(revoke).options(preflight))
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/authorize/sign-in", post(authorize_sign_in))
        .route("/oauth/authorize/sign-up", post(authorize_sign_up))
        .route("/oauth/authorize/select", post(authorize_select))
        .route("/oauth/authorize/consent", post(authorize_consent))
        .route("/oauth/account", get(account_page))
        .route("/oauth/account/sign-in", post(account_sign_in))
        .route("/oauth/account/sign-out", post(account_sign_out))
        .route("/oauth/account/revoke", post(account_revoke))
        .route("/xrpc/vlpds.oauth.listSessions", get(xrpc_list_sessions))
        .route("/xrpc/vlpds.oauth.revokeSession", post(xrpc_revoke_session))
}

// ---------- CORS / common headers ----------

// ---------- HA routing ----------

/// Login identifier (handle, DID or email; `@` prefix and case ignored) ->
/// DID. Global lookups (handle / email claims), so any node can resolve.
pub async fn resolve_identifier(app: &App, ident: &str) -> Option<String> {
    let ident = ident.trim().trim_start_matches('@').to_ascii_lowercase();
    if ident.starts_with("did:") {
        return Some(ident);
    }
    if ident.contains('@') {
        return super::server::did_by_email(app, &ident).await.ok().flatten();
    }
    if !ident.contains('.') {
        return None;
    }
    app.resolve_handle(&ident).await.ok().flatten()
}

/// The routing key an `/oauth/*` request is served by, for the forwarding
/// layer (`crate::forward`); None = any node. See the HA notes in
/// `crate::oauth`. `body` is the (form or JSON) request body.
pub async fn route_key(
    app: &App,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<String> {
    let params = || parse_params(headers, body).ok().unwrap_or_default();
    let request = |uri: Option<&String>| {
        store::request_id_from_uri(uri?).map(store::req_routing)
    };
    match path {
        // the account owner (so the whole flow tends to stay there: the
        // request id is minted local to whichever node runs PAR)
        "/oauth/par" => {
            let hint = params().remove("login_hint")?;
            resolve_identifier(app, &hint).await
        }
        "/oauth/authorize" => {
            let q: HashMap<String, String> =
                ou::parse_form(query.unwrap_or("")).into_iter().collect();
            request(q.get("request_uri"))
        }
        // sign-up mints the DID on the node that runs it; the request row's
        // owner, like the steps after it
        "/oauth/authorize/select" | "/oauth/authorize/consent" | "/oauth/authorize/sign-up" => {
            request(params().get("request_uri"))
        }
        // sign-in: the account's owner (its rate limits, 2FA lockout and
        // account record are there); the code step names no account, so the
        // device's pending one
        "/oauth/authorize/sign-in" | "/oauth/account/sign-in" => {
            let p = params();
            let did = if p.get("step").map(String::as_str) == Some("totp") {
                let id = cookie(headers, DEVICE_COOKIE).filter(|i| store::valid_device_id(i))?;
                store::get_device(app, &id).await.ok()??.pending_2fa.map(|(did, _)| did)
            } else {
                match p.get("identifier") {
                    Some(i) => resolve_identifier(app, i).await,
                    None => None,
                }
            };
            did.or_else(|| request(p.get("request_uri")))
        }
        "/oauth/account/revoke" => params().remove("did").filter(|d| d.starts_with("did:")),
        // code -> its request row; refresh token -> its session's account
        "/oauth/token" => {
            let p = params();
            match p.get("grant_type").map(String::as_str) {
                Some("authorization_code") => {
                    store::code_request_id(p.get("code")?).map(|id| store::req_routing(&id))
                }
                Some("refresh_token") => {
                    store::parse_refresh_token(p.get("refresh_token")?).map(|r| r.did)
                }
                _ => None,
            }
        }
        "/oauth/revoke" => {
            let p = params();
            let tok = p.get("token")?;
            if let Some(r) = store::parse_refresh_token(tok) {
                Some(r.did)
            } else if let Some(id) = store::code_request_id(tok) {
                Some(store::req_routing(&id))
            } else {
                // access token: its (unverified) sub; the owner verifies
                let payload = ou::b64u_decode(tok.split('.').nth(1)?)?;
                #[derive(Deserialize)]
                struct Sub {
                    sub: String,
                }
                serde_json::from_slice::<Sub>(&payload).ok().map(|s| s.sub)
            }
        }
        _ => None,
    }
}

fn cors(h: &mut HeaderMap) {
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("DPoP-Nonce, WWW-Authenticate"),
    );
}

async fn preflight() -> Response {
    let mut r = StatusCode::NO_CONTENT.into_response();
    let h = r.headers_mut();
    cors(h);
    h.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, DPoP, Authorization"),
    );
    h.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("86400"),
    );
    r
}

/// JSON response for the AS endpoints: CORS, no-store, fresh DPoP nonce.
fn as_json(app: &App, status: StatusCode, body: J) -> Response {
    let mut r = (status, Json(body)).into_response();
    finish_as(app, &mut r);
    r
}

fn finish_as(app: &App, r: &mut Response) {
    let h = r.headers_mut();
    cors(h);
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    if let Ok(v) = HeaderValue::from_str(&keys(app).nonces.next()) {
        h.insert(HeaderName::from_static("dpop-nonce"), v);
    }
}

fn as_error(app: &App, e: OAuthError) -> Response {
    let mut r = e.into_response();
    finish_as(app, &mut r);
    r
}

// ---------- metadata ----------

async fn protected_resource_metadata(State(app): AppState) -> Response {
    let iss = issuer(&app);
    let mut r = Json(json!({
        "resource": iss,
        "authorization_servers": [iss],
        "scopes_supported": [],
        "bearer_methods_supported": ["header"],
        "resource_documentation": "https://atproto.com",
    }))
    .into_response();
    cors(r.headers_mut());
    r
}

async fn authorization_server_metadata(State(app): AppState) -> Response {
    let iss = issuer(&app);
    let mut r = Json(json!({
        "issuer": iss,
        "scopes_supported": ["atproto", "transition:email", "transition:generic", "transition:chat.bsky"],
        "subject_types_supported": ["public"],
        "response_types_supported": ["code"],
        "response_modes_supported": ["query", "fragment", "form_post"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "ui_locales_supported": ["en-US"],
        "display_values_supported": ["page", "popup", "touch"],
        "prompt_values_supported": ["none", "login", "consent", "select_account", "create"],
        "authorization_response_iss_parameter_supported": true,
        "request_object_signing_alg_values_supported": [jose::VERIFY_ALGS[0], "none"],
        "request_object_encryption_alg_values_supported": [],
        "request_object_encryption_enc_values_supported": [],
        "request_parameter_supported": true,
        "request_uri_parameter_supported": true,
        "require_request_uri_registration": true,
        "jwks_uri": format!("{iss}/oauth/jwks"),
        "authorization_endpoint": format!("{iss}/oauth/authorize"),
        "token_endpoint": format!("{iss}/oauth/token"),
        "token_endpoint_auth_methods_supported": client::AUTH_METHODS_SUPPORTED,
        "token_endpoint_auth_signing_alg_values_supported": jose::VERIFY_ALGS,
        "revocation_endpoint": format!("{iss}/oauth/revoke"),
        "pushed_authorization_request_endpoint": format!("{iss}/oauth/par"),
        "require_pushed_authorization_requests": true,
        "dpop_signing_alg_values_supported": jose::VERIFY_ALGS,
        "protected_resources": [iss],
        "client_id_metadata_document_supported": true,
    }))
    .into_response();
    cors(r.headers_mut());
    r
}

async fn jwks(State(app): AppState) -> Response {
    let mut r = Json(json!({"keys": [keys(&app).server.public_jwk()]})).into_response();
    cors(r.headers_mut());
    r
}

// ---------- request parsing ----------

/// Parses an urlencoded (or JSON) request body. Repeated parameters are an
/// error (RFC 6749 §3.1).
fn parse_params(headers: &HeaderMap, body: &[u8]) -> Result<HashMap<String, String>, OAuthError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let mut out = HashMap::new();
    if ct.starts_with("application/json") {
        let j: J = serde_json::from_slice(body)
            .map_err(|_| OAuthError::invalid_request("Invalid JSON body"))?;
        let obj = j
            .as_object()
            .ok_or_else(|| OAuthError::invalid_request("Invalid JSON body"))?;
        for (k, v) in obj {
            let s = match v {
                J::String(s) => s.clone(),
                J::Number(n) => n.to_string(),
                J::Bool(b) => b.to_string(),
                J::Null => continue,
                _ => {
                    return Err(OAuthError::invalid_request(&format!(
                        "Invalid \"{k}\" parameter"
                    )))
                }
            };
            out.insert(k.clone(), s);
        }
        return Ok(out);
    }
    let s = std::str::from_utf8(body)
        .map_err(|_| OAuthError::invalid_request("Invalid request body"))?;
    for (k, v) in ou::parse_form(s) {
        if out.insert(k.clone(), v).is_some() {
            return Err(OAuthError::invalid_request(&format!(
                "Duplicate \"{k}\" parameter"
            )));
        }
    }
    Ok(out)
}

fn dpop_header(headers: &HeaderMap) -> Result<Option<String>, String> {
    let mut it = headers.get_all("dpop").iter();
    match (it.next(), it.next()) {
        (None, _) => Ok(None),
        (Some(v), None) => {
            let s = v.to_str().map_err(|_| "Invalid DPoP header".to_string())?;
            if s.is_empty() {
                Err("DPoP header cannot be empty".into())
            } else {
                Ok(Some(s.to_string()))
            }
        }
        _ => Err("DPoP header must contain a single proof".into()),
    }
}

/// DPoP proof at the authorization server (PAR / token): required, and
/// single use cluster-wide (claimed at the owner of the key's routing).
async fn check_as_dpop(app: &App, headers: &HeaderMap, path: &str) -> Result<DpopProof, OAuthError> {
    let proof = dpop_header(headers)
        .map_err(|e| OAuthError::invalid_dpop_proof(&e))?
        .ok_or_else(|| OAuthError::invalid_dpop_proof("DPoP proof required"))?;
    let htu = jose::normalize_htu(&format!("{}{path}", issuer(app)))
        .ok_or_else(|| OAuthError::server_error("bad public_url"))?;
    let proof = jose::check_proof(&proof, "POST", &htu, None, &keys(app).nonces).map_err(|e| match e {
        DpopError::UseNonce(m) => OAuthError::use_dpop_nonce(&m),
        DpopError::Invalid(m) => OAuthError::invalid_dpop_proof(&m),
    })?;
    let replay = proof.replay(ou::jkt_routing(&proof.jkt));
    claim(app, &replay, OAuthError::invalid_dpop_proof("DPoP proof replayed")).await?;
    Ok(proof)
}

/// Claims a single-use value at the owner of its routing key (see the HA
/// notes in `crate::oauth`). `replayed` is the error for a second use.
async fn claim(app: &App, r: &ou::Replay, replayed: OAuthError) -> Result<(), OAuthError> {
    match super::internal::claim_replay_anywhere(app, &r.routing, &r.key, r.until).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(replayed),
        Err(e) => Err(unavailable(&e.message)),
    }
}

fn unavailable(msg: &str) -> OAuthError {
    OAuthError::new(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable", msg)
}

/// Single-use state (a request row's code, a session's refresh rotation) is
/// read-modify-written only by the node owning its routing key, under a
/// process-local lock there. Forwarding sends the request to that node; this
/// refuses (retryably) if it isn't us, e.g. mid-handoff.
fn require_owner(app: &App, routing: &str) -> Result<(), OAuthError> {
    if app.remote_owner(routing).is_some() || app.partition(routing).is_err() {
        return Err(unavailable("this grant's partition is moving; retry"));
    }
    Ok(())
}

/// Account record of `did`, from its owner if that is another node.
async fn account_any(app: &App, did: &str) -> XResult<Account> {
    super::internal::account_anywhere(app, did).await
}

/// [`App::ensure_active`] wherever the account lives.
async fn ensure_active_any(app: &App, did: &str) -> XResult<Account> {
    let a = account_any(app, did)
        .await
        .map_err(|_| XrpcError::bad("RepoNotFound", format!("could not find repo: {did}")))?;
    match &a.status {
        Some(st) => Err(super::inactive_account_error(st)),
        None => Ok(a),
    }
}

// ---------- PAR ----------

async fn par(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    match par_inner(&app, &headers, &body).await {
        Ok(j) => as_json(&app, StatusCode::CREATED, j),
        Err(e) => as_error(&app, e),
    }
}

async fn par_inner(app: &App, headers: &HeaderMap, body: &[u8]) -> Result<J, OAuthError> {
    let p = parse_params(headers, body)?;
    let proof = check_as_dpop(app, headers, "/oauth/par").await?;
    let creds = ClientCredentials::from_params(&p)?;
    let client = client::get_client(&creds.client_id, app.config.dev_mode).await?;
    let (client_auth, assertion) = client.authenticate(&creds, &issuer(app))?;
    if let Some(r) = &assertion {
        claim(app, r, OAuthError::invalid_client("client assertion replayed")).await?;
    }
    if p.contains_key("request_uri") {
        return Err(OAuthError::invalid_request(
            "\"request_uri\" is not supported in pushed authorization requests",
        ));
    }
    // JAR (RFC 9101): only the request object's parameters are used.
    let p = match p.get("request") {
        Some(jar) => {
            let (payload, r) = client.decode_request_object(jar, &issuer(app))?;
            claim(app, &r, OAuthError::invalid_request("Request object was replayed")).await?;
            request_object_params(&payload)?
        }
        None => p,
    };
    let params = validate_authorization_request(app, &client, &p, &proof).await?;
    if !store::claim_code_challenge(app, &params.code_challenge).await? {
        return Err(OAuthError::invalid_request(
            "code_challenge was already used",
        ));
    }
    let id = store::new_local_request_id(app);
    let now = now_secs();
    let req = RequestData {
        client_id: client.id.clone(),
        client_auth,
        params,
        created_at: now,
        expires_at: now + PAR_EXPIRES_IN,
        device_id: None,
        did: None,
        code_hash: None,
        consumed: None,
    };
    store::put_request(app, &id, Some(&req)).await?;
    Ok(json!({"request_uri": store::request_uri(&id), "expires_in": PAR_EXPIRES_IN - 1}))
}

/// Authorization request parameters from a verified request object payload
/// (`oauthAuthorizationRequestParametersSchema` in the reference): string
/// values as-is, scalars stringified, registered JWT claims dropped.
fn request_object_params(payload: &J) -> Result<HashMap<String, String>, OAuthError> {
    let bad = |k: &str| {
        OAuthError::invalid_request(&format!("Invalid parameters in JAR: invalid \"{k}\""))
    };
    let obj = payload
        .as_object()
        .ok_or_else(|| OAuthError::invalid_request("Invalid parameters in JAR"))?;
    let mut out = HashMap::new();
    for (k, v) in obj {
        if matches!(
            k.as_str(),
            "iss" | "aud" | "sub" | "iat" | "exp" | "nbf" | "jti"
        ) {
            continue;
        }
        let s = match v {
            J::Null => continue,
            J::String(s) => s.clone(),
            J::Number(n) => n.to_string(),
            J::Bool(b) => b.to_string(),
            // rejected with its own error by validate_authorization_request
            J::Array(_) if k == "authorization_details" => v.to_string(),
            _ => return Err(bad(k)),
        };
        out.insert(k.clone(), s);
    }
    if !out.contains_key("client_id") {
        return Err(bad("client_id"));
    }
    Ok(out)
}

fn is_valid_handle(h: &str) -> bool {
    h.len() <= 253
        && h.split('.').count() >= 2
        && h.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        && !h
            .rsplit('.')
            .next()
            .unwrap_or("")
            .starts_with(|c: char| c.is_ascii_digit())
}

/// Authorization request validation (`RequestManager.validate` + the
/// client-side checks of `Client.validateRequest`).
async fn validate_authorization_request(
    app: &App,
    client: &Client,
    p: &HashMap<String, String>,
    proof: &DpopProof,
) -> Result<AuthParams, OAuthError> {
    let g = |k: &str| p.get(k).filter(|v| !v.is_empty()).cloned();
    if g("client_id").is_some_and(|c| c != client.id) {
        return Err(OAuthError::invalid_request(
            "The \"client_id\" parameter field does not match the value used to authenticate the client",
        ));
    }
    for k in ["request", "request_uri"] {
        if p.contains_key(k) {
            return Err(OAuthError::invalid_request(&format!(
                "\"{k}\" is not supported in pushed authorization requests"
            )));
        }
    }
    for k in ["claims", "id_token_hint", "nonce"] {
        if p.contains_key(k) {
            return Err(OAuthError::invalid_request(&format!(
                "Unsupported \"{k}\" parameter"
            )));
        }
    }
    if p.contains_key("authorization_details") {
        return Err(OAuthError::new(
            StatusCode::BAD_REQUEST,
            "invalid_authorization_details",
            "Unsupported \"authorization_details\"",
        ));
    }
    let response_type = g("response_type")
        .ok_or_else(|| OAuthError::invalid_request("Missing \"response_type\""))?;
    if response_type != "code" {
        return Err(OAuthError::new(
            StatusCode::BAD_REQUEST,
            "unsupported_response_type",
            &format!("Unsupported response_type \"{response_type}\""),
        ));
    }
    // client checks
    if !client.response_types.contains(&response_type) {
        return Err(OAuthError::invalid_request(&format!(
            "Invalid response_type \"{response_type}\" requested by the client"
        )));
    }
    if !client.grant_types.iter().any(|g| g == "authorization_code") {
        return Err(OAuthError::unauthorized_client(
            "This client is not allowed to use the \"authorization_code\" grant type",
        ));
    }
    let redirect_uri = match g("redirect_uri") {
        Some(r) => {
            if !client.allows_redirect_uri(&r) {
                return Err(OAuthError::invalid_request(&format!(
                    "Invalid redirect_uri {r}"
                )));
            }
            r
        }
        None => client
            .default_redirect_uri()
            .map(String::from)
            .ok_or_else(|| OAuthError::invalid_request("redirect_uri is required"))?,
    };
    let requested = g("scope").unwrap_or_default();
    for s in requested.split(' ').filter(|s| !s.is_empty()) {
        if !client.scopes.iter().any(|c| c == s) {
            return Err(OAuthError::invalid_scope(&format!(
                "Scope \"{s}\" is not declared in the client metadata"
            )));
        }
    }
    let mut scopes: Vec<String> = Vec::new();
    for s in requested.split(' ').filter(|s| !s.is_empty()) {
        if s == "openid" {
            return Err(OAuthError::invalid_scope(
                "OpenID Connect is not compatible with atproto",
            ));
        }
        if is_atproto_oauth_scope(s) && !scopes.iter().any(|x| x == s) {
            scopes.push(s.to_string());
        }
    }
    if !scopes.iter().any(|s| s == "atproto") {
        return Err(OAuthError::invalid_scope(
            "The \"atproto\" scope is required",
        ));
    }
    let scope = scopes.join(" ");
    let code_challenge = g("code_challenge").ok_or_else(|| {
        if p.contains_key("code_challenge_method") {
            OAuthError::invalid_request(
                "code_challenge is required when code_challenge_method is provided",
            )
        } else {
            OAuthError::invalid_request("Use of PKCE is required")
        }
    })?;
    let method = g("code_challenge_method").unwrap_or_else(|| "plain".into());
    if method != "S256" {
        return Err(OAuthError::invalid_request(
            "atproto requires use of \"S256\" code_challenge_method",
        ));
    }
    if code_challenge.len() != 43 || ou::b64u_decode(&code_challenge).map(|b| b.len()) != Some(32) {
        return Err(OAuthError::invalid_request("Invalid code_challenge"));
    }
    let response_mode = g("response_mode");
    match response_mode.as_deref() {
        None | Some("query") | Some("fragment") | Some("form_post") => {}
        Some(m) => {
            return Err(OAuthError::invalid_request(&format!(
                "Unsupported response_mode \"{m}\""
            )))
        }
    }
    let mut prompt = g("prompt");
    match prompt.as_deref() {
        None
        | Some("none")
        | Some("login")
        | Some("consent")
        | Some("select_account")
        | Some("create") => {}
        Some(v) => {
            return Err(OAuthError::invalid_request(&format!(
                "Unsupported prompt \"{v}\""
            )))
        }
    }
    // atproto: public (unauthenticated) clients may not sign in silently and
    // always get the consent screen (unless they ask for account creation,
    // which keeps its prompt; consent_required still holds for them).
    if !client.is_confidential() {
        if prompt.as_deref() == Some("none") {
            return Err(OAuthError::new(
                StatusCode::BAD_REQUEST,
                "consent_required",
                "Public clients are not allowed to use silent-sign-on",
            ));
        }
        if prompt.as_deref() != Some("create") {
            prompt = Some("consent".into());
        }
    }
    let login_hint = match g("login_hint") {
        Some(h) => {
            let h = h.to_lowercase();
            let h = h.strip_prefix('@').unwrap_or(&h).to_string();
            if !is_atproto_did(&h) && !is_valid_handle(&h) {
                return Err(OAuthError::invalid_request(&format!(
                    "Invalid login_hint \"{h}\""
                )));
            }
            Some(h)
        }
        None => None,
    };
    if let Some(jkt) = g("dpop_jkt") {
        if jkt != proof.jkt {
            return Err(OAuthError::invalid_dpop_proof(
                "DPoP proof does not match the dpop_jkt parameter",
            ));
        }
    }
    // Every include: scope must resolve to a permission set.
    lexicon::permission_sets_for_scope(app, &scope)
        .await
        .map_err(|e| OAuthError::invalid_scope(&e))?;
    Ok(AuthParams {
        client_id: client.id.clone(),
        response_type,
        redirect_uri,
        scope,
        state: g("state"),
        code_challenge,
        code_challenge_method: method,
        response_mode,
        prompt,
        login_hint,
        dpop_jkt: proof.jkt.clone(),
        display: g("display"),
        ui_locales: g("ui_locales"),
    })
}

// ---------- authorization endpoint (UI) ----------

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for v in headers.get_all(header::COOKIE) {
        for part in v.to_str().ok()?.split(';') {
            if let Some((k, val)) = part.trim().split_once('=') {
                if k == name {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

/// Loads (or starts) the browser device session. Returns the device and
/// whether a cookie must be set.
async fn device_for(app: &App, headers: &HeaderMap) -> Result<(Device, bool), OAuthError> {
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(256).collect::<String>());
    if let Some(id) = cookie(headers, DEVICE_COOKIE).filter(|i| store::valid_device_id(i)) {
        if let Some(mut d) = store::get_device(app, &id).await? {
            let now = now_secs();
            if now - d.last_seen_at > 3600 {
                d.last_seen_at = now;
                store::put_device(app, &d).await?;
            }
            return Ok((d, false));
        }
    }
    let now = now_secs();
    let d = Device {
        id: store::new_device_id(),
        created_at: now,
        last_seen_at: now,
        user_agent: ua,
        accounts: vec![],
        pending_2fa: None,
        pending_2fa_failures: 0,
    };
    store::put_device(app, &d).await?;
    Ok((d, true))
}

fn device_cookie(app: &App, d: &Device) -> HeaderValue {
    let secure = if is_https(app) { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{DEVICE_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000{secure}",
        d.id
    ))
    .unwrap()
}

fn csrf_token(app: &App, device_id: &str, scope: &str) -> String {
    ou::b64u(ou::hmac_sha256(
        &keys(app).csrf,
        &[device_id.as_bytes(), scope.as_bytes()],
    ))
}

/// CSRF defence for form posts: a token bound to the device cookie and the
/// request, plus Fetch-Metadata / Origin checks when the browser sends them.
fn check_csrf(
    app: &App,
    headers: &HeaderMap,
    device: &Device,
    scope: &str,
    token: Option<&String>,
) -> bool {
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if site != "same-origin" && site != "none" {
            return false;
        }
    }
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        if origin != "null"
            && reqwest::Url::parse(&issuer(app))
                .map(|u| u.origin().ascii_serialization())
                .ok()
                .as_deref()
                != Some(origin)
        {
            return false;
        }
    }
    match token {
        Some(t) => ou::ct_eq(t.as_bytes(), csrf_token(app, &device.id, scope).as_bytes()),
        None => false,
    }
}

/// form-action source allowing the post-consent redirect to the client.
fn redirect_source(redirect_uri: &str) -> Option<String> {
    let u = reqwest::Url::parse(redirect_uri).ok()?;
    match u.scheme() {
        "http" | "https" => Some(u.origin().ascii_serialization()),
        s => Some(format!("{s}:")),
    }
}

fn html(
    app: &App,
    status: StatusCode,
    body: String,
    form_action: &[String],
    set_cookie: Option<&Device>,
) -> Response {
    let mut r = (status, body).into_response();
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_str(&ui::csp(form_action)).unwrap(),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    if let Some(d) = set_cookie {
        h.insert(header::SET_COOKIE, device_cookie(app, d));
    }
    r
}

fn error_page(app: &App, status: StatusCode, title: &str, msg: &str) -> Response {
    html(app, status, ui::error(title, msg), &[], None)
}

/// Builds the redirect back to the client (RFC 6749 §4.1.2 + RFC 9207 `iss`)
/// in the request's response mode: query (default), fragment, or form_post
/// (an auto-submitting form page).
fn client_redirect(app: &App, params: &AuthParams, mut pairs: Vec<(String, String)>) -> Response {
    if let Some(s) = &params.state {
        pairs.push(("state".into(), s.clone()));
    }
    pairs.push(("iss".into(), issuer(app)));
    if params.response_mode.as_deref() == Some("form_post") {
        let form_action: Vec<String> = redirect_source(&params.redirect_uri).into_iter().collect();
        let mut r = html(
            app,
            StatusCode::OK,
            ui::form_post(&params.redirect_uri, &pairs),
            &form_action,
            None,
        );
        let h = r.headers_mut();
        h.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_str(&ui::csp_form_post(&form_action)).unwrap(),
        );
        // Keep the page out of the back/forward cache, so going "back" never
        // re-posts the response (as the reference does).
        h.append(
            header::SET_COOKIE,
            HeaderValue::from_static("bfCacheBypass=1; Path=/; Max-Age=1; SameSite=Lax"),
        );
        return r;
    }
    let enc = ou::form_encode(&pairs);
    let url = if params.response_mode.as_deref() == Some("fragment") {
        format!(
            "{}#{enc}",
            params.redirect_uri.split('#').next().unwrap_or("")
        )
    } else if params.redirect_uri.contains('?') {
        format!("{}&{enc}", params.redirect_uri)
    } else {
        format!("{}?{enc}", params.redirect_uri)
    };
    let mut r = StatusCode::SEE_OTHER.into_response();
    let h = r.headers_mut();
    h.insert(
        header::LOCATION,
        HeaderValue::from_str(&url).unwrap_or(HeaderValue::from_static("/")),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    r
}

fn redirect_error(app: &App, params: &AuthParams, error: &str, desc: &str) -> Response {
    client_redirect(
        app,
        params,
        vec![
            ("error".into(), error.into()),
            ("error_description".into(), desc.into()),
        ],
    )
}

/// A loaded authorization request in the context of a browser device.
struct Flow {
    id: String,
    uri: String,
    req: RequestData,
    client: Arc<Client>,
    device: Device,
    new_cookie: bool,
}

enum FlowError {
    /// Show an error page (redirect target not trustworthy / unknown).
    Page(StatusCode, String),
    /// Redirect to the client with an OAuth error.
    Redirect(Box<AuthParams>, &'static str, String),
}

impl FlowError {
    fn into_response(self, app: &App) -> Response {
        match self {
            FlowError::Page(s, m) => error_page(app, s, "Authorization failed", &m),
            FlowError::Redirect(p, e, m) => redirect_error(app, &p, e, &m),
        }
    }
}

impl From<OAuthError> for FlowError {
    fn from(e: OAuthError) -> FlowError {
        FlowError::Page(StatusCode::INTERNAL_SERVER_ERROR, e.description)
    }
}

/// Loads the request named by `request_uri` and binds it to this device
/// (`RequestManager.get`). Failed requests are deleted.
async fn load_flow(
    app: &App,
    headers: &HeaderMap,
    request_uri: Option<&str>,
    client_id: Option<&str>,
) -> Result<Flow, FlowError> {
    let uri = request_uri.ok_or_else(|| {
        FlowError::Page(
            StatusCode::BAD_REQUEST,
            "Pushed Authorization Request (PAR) is required: missing request_uri".into(),
        )
    })?;
    let id = store::request_id_from_uri(uri)
        .ok_or_else(|| FlowError::Page(StatusCode::BAD_REQUEST, "Invalid request_uri".into()))?
        .to_string();
    let (device, new_cookie) = device_for(app, headers).await?;
    let mut req = store::get_request(app, &id)
        .await?
        .ok_or_else(|| FlowError::Page(StatusCode::BAD_REQUEST, "Unknown request_uri".into()))?;
    let fail =
        |m: &str| FlowError::Redirect(Box::new(req.params.clone()), "access_denied", m.to_string());
    let now = now_secs();
    let err = if req.did.is_some() || req.code_hash.is_some() || req.consumed.is_some() {
        Some(fail("This request was already authorized"))
    } else if req.expires_at < now {
        Some(fail("This request has expired"))
    } else if client_id.is_some_and(|c| c != req.client_id) {
        Some(fail("This request was initiated for another client"))
    } else if req.device_id.as_ref().is_some_and(|d| *d != device.id) {
        Some(fail("This request was initiated from another device"))
    } else {
        None
    };
    if let Some(e) = err {
        if req.consumed.is_none() {
            store::put_request(app, &id, None).await?;
        }
        return Err(e);
    }
    req.device_id = Some(device.id.clone());
    req.expires_at = now + AUTHORIZATION_INACTIVITY_TIMEOUT;
    store::put_request(app, &id, Some(&req)).await?;
    let client = client::get_client(&req.client_id, app.config.dev_mode)
        .await
        .map_err(|e| {
            FlowError::Redirect(
                Box::new(req.params.clone()),
                "invalid_client",
                e.description,
            )
        })?;
    Ok(Flow {
        id,
        uri: uri.to_string(),
        req,
        client,
        device,
        new_cookie,
    })
}

impl Flow {
    fn csrf(&self, app: &App) -> String {
        csrf_token(app, &self.device.id, &self.id)
    }

    fn form_action(&self) -> Vec<String> {
        redirect_source(&self.req.params.redirect_uri)
            .into_iter()
            .collect()
    }

    fn page(&self, app: &App, body: String) -> Response {
        html(
            app,
            StatusCode::OK,
            body,
            &self.form_action(),
            self.new_cookie.then_some(&self.device),
        )
    }

    fn ctx<'a>(&'a self, csrf: &'a str, server_name: &'a str) -> ui::Ctx<'a> {
        ui::Ctx {
            request_uri: &self.uri,
            csrf,
            client_id: &self.client.id,
            loopback: self.client.loopback,
            server_name,
        }
    }
}

fn server_name(app: &App) -> String {
    reqwest::Url::parse(&app.public_url)
        .ok()
        .and_then(|u| u.host_str().map(String::from))
        .unwrap_or_else(|| app.public_url.clone())
}

/// Accounts signed in on this device whose login is still fresh, with handles.
async fn device_accounts(app: &App, d: &Device) -> Vec<(String, String)> {
    let now = now_secs();
    let mut out = Vec::new();
    for a in &d.accounts {
        if now - a.authenticated_at > AUTHENTICATION_MAX_AGE {
            continue;
        }
        if let Ok(acct) = account_any(app, &a.did).await {
            if acct.status.is_none() {
                out.push((acct.did.clone(), acct.handle.clone()));
            }
        }
    }
    out
}

fn hint_matches(hint: &str, did: &str, handle: &str) -> bool {
    hint == did || hint == handle
}

async fn consent_required(app: &App, flow: &Flow, did: &str) -> Result<bool, OAuthError> {
    if flow.req.params.prompt.as_deref() == Some("consent") || !flow.client.is_confidential() {
        return Ok(true);
    }
    let Some(a) = store::get_authorization(app, did, &flow.client.id).await? else {
        return Ok(true);
    };
    Ok(!flow
        .req
        .params
        .scope
        .split(' ')
        .all(|s| a.scopes.iter().any(|x| x == s)))
}

fn login_page(
    app: &App,
    flow: &Flow,
    identifier: &str,
    error: Option<&str>,
    totp: bool,
    status: StatusCode,
) -> Response {
    let csrf = flow.csrf(app);
    let name = server_name(app);
    let body = ui::login(
        Some(&flow.ctx(&csrf, &name)),
        &ui::LoginForm {
            action: "/oauth/authorize/sign-in",
            identifier,
            error,
            totp,
            email_hint: None,
        },
        "",
    );
    let mut r = flow.page(app, body);
    *r.status_mut() = status;
    r
}

/// After an account is chosen: show consent, or approve directly when the
/// user already granted these scopes to this (confidential) client.
async fn consent_step(app: &App, flow: Flow, did: &str) -> Response {
    let acct = match account_any(app, did).await {
        Ok(a) => a,
        Err(_) => {
            return login_page(
                app,
                &flow,
                "",
                Some("Account not found"),
                false,
                StatusCode::OK,
            )
        }
    };
    match consent_required(app, &flow, did).await {
        Ok(false) => return issue_code(app, flow, did).await,
        Ok(true) => {}
        Err(e) => {
            return error_page(
                app,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Authorization failed",
                &e.description,
            )
        }
    }
    let sets = lexicon::permission_sets_for_scope(app, &flow.req.params.scope)
        .await
        .unwrap_or_default();
    let perms = ui::describe_scopes(&flow.req.params.scope, &sets);
    let csrf = flow.csrf(app);
    let name = server_name(app);
    let email_choice = can_withhold_email(&flow.req.params.scope);
    let body = ui::consent(
        &flow.ctx(&csrf, &name),
        did,
        &acct.handle,
        &perms,
        email_choice,
    );
    flow.page(app, body)
}

/// Binds the request to the account and redirects with the code
/// (`RequestManager.setAuthorized`).
async fn issue_code(app: &App, mut flow: Flow, did: &str) -> Response {
    if let Err(e) = ensure_active_any(app, did).await {
        let _ = store::put_request(app, &flow.id, None).await;
        return redirect_error(
            app,
            &flow.req.params,
            "access_denied",
            &format!("Account unavailable: {}", e.message),
        );
    }
    let code = store::new_code(&flow.id);
    flow.req.did = Some(did.to_string());
    flow.req.code_hash = Some(store::hash_secret(&code));
    flow.req.expires_at = now_secs() + AUTHORIZATION_INACTIVITY_TIMEOUT;
    if let Err(e) = store::put_request(app, &flow.id, Some(&flow.req)).await {
        return error_page(
            app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Authorization failed",
            &e.description,
        );
    }
    // Remember consent (union with earlier grants).
    let mut scopes: Vec<String> = flow.req.params.scope.split(' ').map(String::from).collect();
    if let Ok(Some(prev)) = store::get_authorization(app, did, &flow.client.id).await {
        for s in prev.scopes {
            if !scopes.contains(&s) {
                scopes.push(s);
            }
        }
    }
    let _ = store::put_authorization(
        app,
        did,
        &store::Authorization {
            client_id: flow.client.id.clone(),
            scopes,
            updated_at: now_secs(),
        },
    )
    .await;
    let mut r = client_redirect(app, &flow.req.params, vec![("code".into(), code)]);
    if flow.new_cookie {
        r.headers_mut()
            .append(header::SET_COOKIE, device_cookie(app, &flow.device));
    }
    r
}

async fn authorize(
    State(app): AppState,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if !q.contains_key("client_id") {
        return error_page(
            &app,
            StatusCode::BAD_REQUEST,
            "Authorization failed",
            "Missing client_id",
        );
    }
    let flow = match load_flow(
        &app,
        &headers,
        q.get("request_uri").map(String::as_str),
        q.get("client_id").map(String::as_str),
    )
    .await
    {
        Ok(f) => f,
        Err(e) => return e.into_response(&app),
    };
    let accounts = device_accounts(&app, &flow.device).await;
    let params = flow.req.params.clone();
    let hint = params.login_hint.clone().unwrap_or_default();
    let hinted = accounts
        .iter()
        .find(|(d, h)| !hint.is_empty() && hint_matches(&hint, d, h))
        .cloned();
    // the sign-in <-> sign-up links between the two pages
    match q.get("screen").map(String::as_str) {
        Some("sign-up") => return signup_page(&app, &flow, &SignupValues::default(), None, StatusCode::OK),
        Some("sign-in") => return login_page(&app, &flow, &hint, None, false, StatusCode::OK),
        _ => {}
    }
    match params.prompt.as_deref() {
        Some("none") => {
            let chosen = match (&hinted, accounts.len()) {
                (Some(a), _) => a.clone(),
                (None, 1) if hint.is_empty() => accounts[0].clone(),
                (None, 0) => {
                    return redirect_error(&app, &params, "login_required", "Login is required")
                }
                (None, _) if !hint.is_empty() => {
                    return redirect_error(&app, &params, "login_required", "Login is required")
                }
                (None, _) => {
                    return redirect_error(
                        &app,
                        &params,
                        "account_selection_required",
                        "Account selection is required",
                    )
                }
            };
            match consent_required(&app, &flow, &chosen.0).await {
                Ok(false) => issue_code(&app, flow, &chosen.0).await,
                Ok(true) => {
                    redirect_error(&app, &params, "consent_required", "Consent is required")
                }
                Err(e) => redirect_error(&app, &params, "server_error", &e.description),
            }
        }
        Some("login") => login_page(&app, &flow, &hint, None, false, StatusCode::OK),
        // prompt=create: the sign-up page (which links to sign-in)
        Some("create") => signup_page(&app, &flow, &SignupValues::default(), None, StatusCode::OK),
        Some("select_account") if !accounts.is_empty() => chooser_page(&app, &flow, &accounts),
        _ => {
            if let Some((did, _)) = hinted {
                consent_step(&app, flow, &did).await
            } else if !hint.is_empty() || accounts.is_empty() {
                login_page(&app, &flow, &hint, None, false, StatusCode::OK)
            } else {
                chooser_page(&app, &flow, &accounts)
            }
        }
    }
}

fn chooser_page(app: &App, flow: &Flow, accounts: &[(String, String)]) -> Response {
    let csrf = flow.csrf(app);
    let name = server_name(app);
    flow.page(app, ui::chooser(&flow.ctx(&csrf, &name), accounts))
}

/// Loads a form-post flow and checks CSRF.
#[allow(clippy::result_large_err)]
async fn form_flow(
    app: &App,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(Flow, HashMap<String, String>), Response> {
    let f: HashMap<String, String> = ou::parse_form(std::str::from_utf8(body).unwrap_or(""))
        .into_iter()
        .collect();
    let flow = load_flow(app, headers, f.get("request_uri").map(String::as_str), None)
        .await
        .map_err(|e| e.into_response(app))?;
    if flow.new_cookie || !check_csrf(app, headers, &flow.device, &flow.id, f.get("csrf")) {
        return Err(error_page(
            app,
            StatusCode::FORBIDDEN,
            "Authorization failed",
            "Invalid or missing CSRF token; please restart the sign-in from the app.",
        ));
    }
    Ok((flow, f))
}

enum SignIn {
    Ok(String),
    /// Password accepted; a second-factor code is needed (handle; the
    /// obfuscated address when it is an emailed code).
    NeedTotp(String, Option<String>),
    /// Wrong code; still pending (handle, email hint).
    NeedTotpErr(String, Option<String>),
    /// (identifier to pre-fill, why)
    Failed(String, LoginError),
}

/// Sign-in failures. Pages show only these fixed messages, and
/// `/oauth/account?error=` carries the code, never request-supplied text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoginError {
    Invalid,
    Timeout,
    BadCode,
    /// Too many wrong codes: password step again (or the account's factor
    /// is locked out).
    TooManyCodes,
    RateLimited,
    Inactive,
}

impl LoginError {
    const ALL: [LoginError; 6] = [
        LoginError::Invalid,
        LoginError::Timeout,
        LoginError::BadCode,
        LoginError::TooManyCodes,
        LoginError::RateLimited,
        LoginError::Inactive,
    ];

    pub(crate) fn code(self) -> &'static str {
        match self {
            LoginError::Invalid => "invalid",
            LoginError::Timeout => "timeout",
            LoginError::BadCode => "bad_code",
            LoginError::TooManyCodes => "too_many_codes",
            LoginError::RateLimited => "rate_limited",
            LoginError::Inactive => "inactive",
        }
    }

    pub(crate) fn from_code(c: &str) -> Option<LoginError> {
        Self::ALL.into_iter().find(|e| e.code() == c)
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            LoginError::Invalid => "Invalid handle or password",
            LoginError::Timeout => "Your sign-in timed out. Please enter your password again.",
            LoginError::BadCode => "Invalid authenticator code",
            LoginError::TooManyCodes => {
                "Too many invalid authenticator codes. Please sign in again later."
            }
            LoginError::RateLimited => "Too many sign-in attempts. Please try again later.",
            LoginError::Inactive => "This account is deactivated or suspended",
        }
    }

    fn status(self) -> StatusCode {
        match self {
            LoginError::TooManyCodes | LoginError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::UNAUTHORIZED,
        }
    }
}

/// Password (+ TOTP) sign-in; records the login on the device. Rate limited
/// per IP, per identifier + IP and per account (src/ratelimit.rs); wrong
/// codes count against the account's TOTP lockout and, past
/// [`PENDING_2FA_MAX_FAILURES`], drop the pending sign-in.
async fn sign_in(
    app: &App,
    device: &mut Device,
    f: &HashMap<String, String>,
) -> Result<SignIn, OAuthError> {
    use crate::ratelimit as rl;
    let now = now_secs();
    let code = f.get("code").map(|c| c.trim()).filter(|c| !c.is_empty());
    let ident = f
        .get("identifier")
        .map(|s| s.trim().trim_start_matches('@').to_lowercase())
        .unwrap_or_default();
    let limited = |ident: String| Ok(SignIn::Failed(ident, LoginError::RateLimited));
    if rl::check_ip(&[&rl::GLOBAL_IP, &rl::OAUTH_SIGN_IN_IP], 1).is_err() {
        return limited(ident);
    }
    let password_step = f.get("step").map(String::as_str) != Some("totp");
    let (acct, ident) = if !password_step {
        // Second step: password already verified for the pending account.
        let Some((did, _)) = device
            .pending_2fa
            .clone()
            .filter(|(_, at)| now - at < PENDING_2FA_TTL)
        else {
            return Ok(SignIn::Failed(String::new(), LoginError::Timeout));
        };
        if rl::check_with_ip(
            &[&rl::CREATE_SESSION_DAY, &rl::CREATE_SESSION_5MIN],
            &did,
            1,
        )
        .is_err()
            || rl::check(&[&rl::SIGN_IN_ACCOUNT], &did, 1).is_err()
        {
            return limited(String::new());
        }
        (account_any(app, &did).await?, String::new())
    } else {
        let invalid = || Ok(SignIn::Failed(ident.clone(), LoginError::Invalid));
        let password = f.get("password").cloned().unwrap_or_default();
        if ident.is_empty() || password.is_empty() {
            return invalid();
        }
        // createSession's buckets (shared with it), before any Argon2 work
        if rl::check_with_ip(
            &[&rl::CREATE_SESSION_DAY, &rl::CREATE_SESSION_5MIN],
            &ident,
            1,
        )
        .is_err()
        {
            return limited(ident);
        }
        let Some(did) = resolve_identifier(app, &ident).await else {
            return invalid();
        };
        if rl::check(&[&rl::SIGN_IN_ACCOUNT], &did, 1).is_err() {
            return limited(ident);
        }
        let Ok(acct) = account_any(app, &did).await else {
            return invalid();
        };
        if ident.contains('@') && acct.email.as_deref() != Some(ident.as_str()) {
            return invalid();
        }
        if !state::verify_password_hash(&acct.password_hash, &password).await {
            return invalid();
        }
        if acct.status.is_some() {
            return Ok(SignIn::Failed(ident, LoginError::Inactive));
        }
        (acct, ident)
    };
    // TOTP, else the email factor (which mails the code on the password step)
    match super::email2fa::check_second_factor(app, &acct, code, false).await {
        Ok(()) => {}
        Err(fe) if fe.err.error == "AuthFactorTokenRequired" => {
            device.pending_2fa = Some((acct.did.clone(), now));
            device.pending_2fa_failures = 0;
            store::put_device(app, device).await?;
            return Ok(SignIn::NeedTotp(acct.handle, email_hint(fe.factor)));
        }
        Err(fe) if fe.err.status.is_server_error() => return Err(fe.err.into()),
        Err(fe) => {
            let (e, hint) = (fe.err, email_hint(fe.factor));
            // a password step starts a new pending sign-in
            if password_step {
                device.pending_2fa = Some((acct.did.clone(), now));
                device.pending_2fa_failures = 0;
            }
            device.pending_2fa_failures += 1;
            if crate::totp::is_lockout(&e)
                || device.pending_2fa_failures >= PENDING_2FA_MAX_FAILURES
            {
                device.pending_2fa = None;
                device.pending_2fa_failures = 0;
                store::put_device(app, device).await?;
                return Ok(SignIn::Failed(ident, LoginError::TooManyCodes));
            }
            store::put_device(app, device).await?;
            return Ok(SignIn::NeedTotpErr(acct.handle, hint));
        }
    }
    let did = acct.did;
    device.pending_2fa = None;
    device.pending_2fa_failures = 0;
    device.accounts.retain(|a| a.did != did);
    device.accounts.push(DeviceAccount {
        did: did.clone(),
        authenticated_at: now,
    });
    device.last_seen_at = now;
    store::put_device(app, device).await?;
    Ok(SignIn::Ok(did))
}

fn email_hint(f: super::email2fa::Factor) -> Option<String> {
    match f {
        super::email2fa::Factor::Email { hint } => Some(hint),
        super::email2fa::Factor::Totp => None,
    }
}

/// The second-factor step of the sign-in page: an authenticator code, or
/// (`email_hint`) the code just mailed.
fn code_page(app: &App, flow: &Flow, handle: &str, email_hint: Option<&str>, bad_code: bool) -> Response {
    let csrf = flow.csrf(app);
    let name = server_name(app);
    let error = bad_code.then(|| match email_hint {
        Some(_) => "Invalid sign-in code",
        None => LoginError::BadCode.message(),
    });
    let body = ui::login(
        Some(&flow.ctx(&csrf, &name)),
        &ui::LoginForm {
            action: "/oauth/authorize/sign-in",
            identifier: handle,
            error,
            totp: true,
            email_hint,
        },
        "",
    );
    let mut r = flow.page(app, body);
    if bad_code {
        *r.status_mut() = StatusCode::UNAUTHORIZED;
    }
    r
}

async fn authorize_sign_in(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (mut flow, f) = match form_flow(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    if f.get("action").map(String::as_str) == Some("deny") {
        let _ = store::put_request(&app, &flow.id, None).await;
        return redirect_error(&app, &flow.req.params, "access_denied", "Access denied");
    }
    match sign_in(&app, &mut flow.device, &f).await {
        Ok(SignIn::Ok(did)) => consent_step(&app, flow, &did).await,
        Ok(SignIn::NeedTotp(handle, hint)) => code_page(&app, &flow, &handle, hint.as_deref(), false),
        Ok(SignIn::NeedTotpErr(handle, hint)) => code_page(&app, &flow, &handle, hint.as_deref(), true),
        Ok(SignIn::Failed(ident, e)) => {
            login_page(&app, &flow, &ident, Some(e.message()), false, e.status())
        }
        Err(e) => error_page(
            &app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Sign-in failed",
            &e.description,
        ),
    }
}

/// What the sign-up form keeps when it is shown again after an error.
#[derive(Default)]
struct SignupValues {
    handle: String,
    email: String,
    invite_code: String,
}

fn signup_page(app: &App, flow: &Flow, v: &SignupValues, error: Option<&str>, status: StatusCode) -> Response {
    let csrf = flow.csrf(app);
    let name = server_name(app);
    let body = ui::signup(
        &flow.ctx(&csrf, &name),
        &ui::SignupForm {
            handle: &v.handle,
            domain: &app.handle_domain,
            email: &v.email,
            invite_code: &v.invite_code,
            invite_required: app.config.invite_required,
            error,
        },
    );
    let mut r = flow.page(app, body);
    *r.status_mut() = status;
    r
}

/// The sign-up form: creates the account (as createAccount does, without a
/// legacy session), signs it in on this device and continues to consent.
/// Rate limited like createAccount (per IP).
async fn authorize_sign_up(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    use crate::ratelimit as rl;
    let (mut flow, f) = match form_flow(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    if f.get("action").map(String::as_str) == Some("deny") {
        let _ = store::put_request(&app, &flow.id, None).await;
        return redirect_error(&app, &flow.req.params, "access_denied", "Access denied");
    }
    let field = |k: &str| f.get(k).map(|v| v.trim().to_string()).unwrap_or_default();
    let v = SignupValues {
        handle: field("handle").trim_start_matches('@').to_ascii_lowercase(),
        email: field("email"),
        invite_code: field("invite_code"),
    };
    if rl::check_ip(&[&rl::CREATE_ACCOUNT], 1).is_err() {
        let msg = "Too many sign-up attempts. Please try again later.";
        return signup_page(&app, &flow, &v, Some(msg), StatusCode::TOO_MANY_REQUESTS);
    }
    // the form asks for the first label; a full handle under our domain is fine too
    let suffix = format!(".{}", app.handle_domain);
    let handle = if v.handle.ends_with(&suffix) { v.handle.clone() } else { format!("{}{suffix}", v.handle) };
    let inp = super::server::CreateAccountIn {
        handle,
        email: Some(v.email.clone()),
        password: f.get("password").cloned().filter(|p| !p.is_empty()),
        invite_code: Some(v.invite_code.clone()).filter(|c| !c.is_empty()),
        ..Default::default()
    };
    let acct = match super::server::create_account_inner(&app, inp, None).await {
        Ok(a) => a,
        Err(e) if e.status.is_server_error() => {
            return error_page(&app, StatusCode::INTERNAL_SERVER_ERROR, "Sign-up failed", &e.message)
        }
        Err(e) => return signup_page(&app, &flow, &v, Some(&e.message), StatusCode::BAD_REQUEST),
    };
    let now = now_secs();
    flow.device.accounts.retain(|a| a.did != acct.did);
    flow.device.accounts.push(DeviceAccount { did: acct.did.clone(), authenticated_at: now });
    flow.device.last_seen_at = now;
    if let Err(e) = store::put_device(&app, &flow.device).await {
        return error_page(&app, StatusCode::INTERNAL_SERVER_ERROR, "Sign-up failed", &e.description);
    }
    consent_step(&app, flow, &acct.did).await
}

async fn authorize_select(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (flow, f) = match form_flow(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    let did = f.get("did").cloned().unwrap_or_default();
    if did.is_empty() {
        return login_page(&app, &flow, "", None, false, StatusCode::OK);
    }
    let accounts = device_accounts(&app, &flow.device).await;
    match accounts.iter().find(|(d, _)| *d == did) {
        Some(_) if flow.req.params.prompt.as_deref() != Some("login") => {
            consent_step(&app, flow, &did).await
        }
        Some((_, handle)) => {
            let h = handle.clone();
            login_page(&app, &flow, &h, None, false, StatusCode::OK)
        }
        None => login_page(
            &app,
            &flow,
            "",
            Some("Please sign in again"),
            false,
            StatusCode::OK,
        ),
    }
}

async fn authorize_consent(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (flow, f) = match form_flow(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    if f.get("action").map(String::as_str) != Some("allow") {
        let _ = store::put_request(&app, &flow.id, None).await;
        return redirect_error(&app, &flow.req.params, "access_denied", "Access denied");
    }
    let did = f.get("did").cloned().unwrap_or_default();
    let accounts = device_accounts(&app, &flow.device).await;
    if !accounts.iter().any(|(d, _)| *d == did) {
        return login_page(
            &app,
            &flow,
            "",
            Some("Please sign in again"),
            false,
            StatusCode::UNAUTHORIZED,
        );
    }
    let mut flow = flow;
    match granted_scope(&flow.req.params.scope, &f) {
        Some(scope) => flow.req.params.scope = scope,
        None => {
            let _ = store::put_request(&app, &flow.id, None).await;
            return redirect_error(
                &app,
                &flow.req.params,
                "access_denied",
                "The \"atproto\" scope is required",
            );
        }
    }
    issue_code(&app, flow, &did).await
}

fn is_email_read_scope(s: &str) -> bool {
    crate::oauth::scopes::Permission::parse(s).is_some_and(|p| p.matches_account("email", "read"))
}

/// The consent form may withhold the email address when it is requested
/// through a granular `account:email` scope (transition scopes cannot be
/// narrowed).
fn can_withhold_email(scope: &str) -> bool {
    !scope.split(' ').any(|s| s.starts_with("transition:"))
        && scope.split(' ').any(is_email_read_scope)
}

/// Scope the user granted on the consent form (`setAuthorized` with a scope
/// override in the reference): the requested scope, narrowed to the form's
/// `scope` field when present (scopes can be removed, never added) and
/// without the `account:email` scopes when the email checkbox was cleared.
/// None if the result lacks `atproto`.
fn granted_scope(requested: &str, f: &HashMap<String, String>) -> Option<String> {
    let allowed: Option<Vec<&str>> = f.get("scope").map(|s| s.split(' ').collect());
    let withhold_email = f.contains_key("email_choice")
        && !f.contains_key("allow_email")
        && can_withhold_email(requested);
    let granted: Vec<&str> = requested
        .split(' ')
        .filter(|s| !s.is_empty())
        .filter(|s| allowed.as_ref().is_none_or(|a| a.contains(s)))
        .filter(|s| !(withhold_email && is_email_read_scope(s)))
        .collect();
    granted.contains(&"atproto").then(|| granted.join(" "))
}

// ---------- token endpoint ----------

async fn token(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    match token_inner(&app, &headers, &body).await {
        Ok(j) => as_json(&app, StatusCode::OK, j),
        Err(e) => as_error(&app, e),
    }
}

fn same_client_auth(a: &ClientAuth, b: &ClientAuth) -> bool {
    match (a, b) {
        (ClientAuth::None, ClientAuth::None) => true,
        (
            ClientAuth::PrivateKeyJwt {
                alg: a1,
                kid: k1,
                jkt: j1,
            },
            ClientAuth::PrivateKeyJwt {
                alg: a2,
                kid: k2,
                jkt: j2,
            },
        ) => a1 == a2 && k1 == k2 && j1 == j2,
        _ => false,
    }
}

async fn token_inner(app: &App, headers: &HeaderMap, body: &[u8]) -> Result<J, OAuthError> {
    let p = parse_params(headers, body)?;
    let proof = check_as_dpop(app, headers, "/oauth/token").await?;
    let creds = ClientCredentials::from_params(&p)?;
    let client = client::get_client(&creds.client_id, app.config.dev_mode).await?;
    let (client_auth, assertion) = client.authenticate(&creds, &issuer(app))?;
    if let Some(r) = &assertion {
        claim(app, r, OAuthError::invalid_client("client assertion replayed")).await?;
    }
    let grant_type = p.get("grant_type").map(String::as_str).unwrap_or("");
    if !client.grant_types.iter().any(|g| g == grant_type)
        && matches!(grant_type, "authorization_code" | "refresh_token")
    {
        return Err(OAuthError::unauthorized_client(&format!(
            "This client is not allowed to use the \"{grant_type}\" grant type"
        )));
    }
    match grant_type {
        "authorization_code" => code_grant(app, &client, client_auth, &p, &proof).await,
        "refresh_token" => refresh_grant(app, &client, client_auth, &p, &proof).await,
        "" => Err(OAuthError::invalid_request("Missing \"grant_type\"")),
        g => Err(OAuthError::unsupported_grant_type(&format!(
            "Unsupported grant_type \"{g}\""
        ))),
    }
}

fn verify_pkce(verifier: &str, challenge: &str) -> bool {
    let ok_chars = verifier
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b));
    (43..=128).contains(&verifier.len())
        && ok_chars
        && ou::ct_eq(ou::sha256_b64u(verifier).as_bytes(), challenge.as_bytes())
}

async fn code_grant(
    app: &App,
    client: &Client,
    client_auth: ClientAuth,
    p: &HashMap<String, String>,
    proof: &DpopProof,
) -> Result<J, OAuthError> {
    let code = p
        .get("code")
        .filter(|c| !c.is_empty())
        .ok_or_else(|| OAuthError::invalid_request("Missing \"code\""))?;
    let rid =
        store::code_request_id(code).ok_or_else(|| OAuthError::invalid_grant("Invalid code"))?;
    require_owner(app, &store::req_routing(&rid))?;
    let _g = store::lock(app, &format!("req:{rid}")).await;
    let mut req = store::get_request(app, &rid)
        .await?
        .ok_or_else(|| OAuthError::invalid_grant("Invalid code"))?;
    let code_ok = req
        .code_hash
        .as_deref()
        .is_some_and(|h| ou::ct_eq(h.as_bytes(), store::hash_secret(code).as_bytes()));
    if !code_ok {
        return Err(OAuthError::invalid_grant("Invalid code"));
    }
    if let Some((did, sid)) = &req.consumed {
        // Code reuse: revoke everything issued from the first use.
        store::delete_session(app, did, sid).await?;
        return Err(OAuthError::invalid_grant("Code replayed"));
    }
    let did = req
        .did
        .clone()
        .ok_or_else(|| OAuthError::invalid_grant("Invalid code"))?;
    let params = req.params.clone();
    let fail = |m: &str| OAuthError::invalid_grant(m);
    if req.expires_at < now_secs() {
        store::put_request(app, &rid, None).await?;
        return Err(fail("This code has expired"));
    }
    if req.client_id != client.id {
        return Err(fail("The code was not issued to this client"));
    }
    if !same_client_auth(&req.client_auth, &client_auth) {
        return Err(fail("Client authentication mismatch"));
    }
    if p.get("redirect_uri").map(String::as_str) != Some(params.redirect_uri.as_str()) {
        return Err(fail("Invalid redirect_uri"));
    }
    let verifier = p
        .get("code_verifier")
        .ok_or_else(|| OAuthError::invalid_grant("Missing code_verifier"))?;
    if !verify_pkce(verifier, &params.code_challenge) {
        return Err(fail("Invalid code_verifier"));
    }
    if proof.jkt != params.dpop_jkt {
        return Err(OAuthError::invalid_dpop_proof(
            "DPoP proof does not match the expected JKT",
        ));
    }
    ensure_active_any(app, &did)
        .await
        .map_err(|e| OAuthError::invalid_grant(&e.message))?;
    let token_scope = lexicon::build_token_scope(app, &params.scope)
        .await
        .map_err(|e| OAuthError::invalid_request(&e))?;
    let now = now_secs();
    let mut s = Session {
        id: ou::random_id("ses-", 16),
        did: did.clone(),
        client_id: client.id.clone(),
        client_auth,
        dpop_jkt: params.dpop_jkt.clone(),
        scope: params.scope.clone(),
        token_scope,
        created_at: now,
        updated_at: now,
        expires_at: 0,
        token_id: String::new(),
        refresh_gen: 0,
        refresh_salt: ou::random_id("", 16),
        device_id: req.device_id.clone(),
        request_id: Some(rid.clone()),
    };
    req.consumed = Some((did.clone(), s.id.clone()));
    store::put_request(app, &rid, Some(&req)).await?;
    issue_tokens(app, client, &mut s).await
}

async fn issue_tokens(app: &App, client: &Client, s: &mut Session) -> Result<J, OAuthError> {
    let now = now_secs();
    let lifetime = ACCESS_TOKEN_TTL.min(s.created_at + client.session_lifetime() - now);
    if lifetime <= 1 {
        store::delete_session(app, &s.did, &s.id).await?;
        return Err(OAuthError::invalid_grant("Session expired"));
    }
    s.token_id = ou::random_id("tok-", 16);
    s.updated_at = now;
    s.expires_at = now + lifetime;
    let claims = json!({
        "iss": issuer(app),
        "aud": app.jwt.service_did,
        "sub": s.did,
        "iat": now,
        "exp": s.expires_at,
        "jti": s.token_id,
        "scope": s.token_scope,
        "client_id": s.client_id,
        "cnf": {"jkt": s.dpop_jkt},
        "sid": s.id,
    });
    // signed and verified (src/crypto.rs) before the session row names the
    // new token: a signature fault (503) never stores a token id that no
    // client received
    let access = keys(app).server.sign("at+jwt", &claims).map_err(|e| unavailable(&e.to_string()))?;
    store::put_session(app, s).await?;
    let mut out = json!({
        "access_token": access,
        "token_type": "DPoP",
        "expires_in": lifetime,
        "scope": s.token_scope,
        "sub": s.did,
    });
    if client.grant_types.iter().any(|g| g == "refresh_token") {
        out["refresh_token"] = J::String(store::refresh_token(&keys(app).refresh, s));
    }
    Ok(out)
}

async fn refresh_grant(
    app: &App,
    client: &Client,
    client_auth: ClientAuth,
    p: &HashMap<String, String>,
    proof: &DpopProof,
) -> Result<J, OAuthError> {
    let tok = p
        .get("refresh_token")
        .filter(|t| !t.is_empty())
        .ok_or_else(|| OAuthError::invalid_request("Missing \"refresh_token\""))?;
    let invalid = || OAuthError::invalid_grant("Invalid refresh token");
    let parsed = store::parse_refresh_token(tok).ok_or_else(invalid)?;
    require_owner(app, &parsed.did)?;
    let _g = store::lock(app, &format!("ses:{}", parsed.session_id)).await;
    let mut s = store::get_session(app, &parsed.did, &parsed.session_id)
        .await?
        .ok_or_else(invalid)?;
    let k = keys(app);
    if !parsed.authentic(&k.refresh, &s) || parsed.generation > s.refresh_gen {
        return Err(invalid());
    }
    if parsed.generation < s.refresh_gen {
        // A rotated-out token was presented again: assume theft, kill the session.
        store::delete_session(app, &s.did, &s.id).await?;
        return Err(OAuthError::invalid_grant("Refresh token replayed"));
    }
    if s.client_id != client.id {
        return Err(OAuthError::invalid_grant(
            "Refresh token was issued to another client",
        ));
    }
    if !client.has_key(&s.client_auth) {
        // The client's authentication key is gone from its metadata: the
        // session must be revoked.
        store::delete_session(app, &s.did, &s.id).await?;
        return Err(OAuthError::invalid_grant(
            "Client authentication key no longer available",
        ));
    }
    if !same_client_auth(&s.client_auth, &client_auth) {
        return Err(OAuthError::invalid_grant("Client authentication mismatch"));
    }
    if proof.jkt != s.dpop_jkt {
        return Err(OAuthError::invalid_dpop_proof(
            "DPoP proof does not match the expected JKT",
        ));
    }
    let now = now_secs();
    if now - s.created_at > client.session_lifetime() {
        store::delete_session(app, &s.did, &s.id).await?;
        return Err(OAuthError::invalid_grant("Session expired"));
    }
    if now - s.updated_at > client.refresh_lifetime() {
        store::delete_session(app, &s.did, &s.id).await?;
        return Err(OAuthError::invalid_grant("Refresh token expired"));
    }
    ensure_active_any(app, &s.did)
        .await
        .map_err(|e| OAuthError::invalid_grant(&e.message))?;
    s.token_scope = lexicon::build_token_scope(app, &s.scope)
        .await
        .map_err(|e| OAuthError::server_error(&e))?;
    s.refresh_gen += 1;
    issue_tokens(app, client, &mut s).await
}

// ---------- revocation (RFC 7009) ----------

async fn revoke(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    match revoke_inner(&app, &headers, &body).await {
        Ok(()) => as_json(&app, StatusCode::OK, json!({})),
        Err(e) => as_error(&app, e),
    }
}

async fn revoke_inner(app: &App, headers: &HeaderMap, body: &[u8]) -> Result<(), OAuthError> {
    let p = parse_params(headers, body)?;
    let tok = p
        .get("token")
        .filter(|t| !t.is_empty())
        .ok_or_else(|| OAuthError::invalid_request("Missing \"token\""))?;
    let creds = ClientCredentials::from_params(&p)?;
    let client = client::get_client(&creds.client_id, app.config.dev_mode).await?;
    if let (_, Some(r)) = client.authenticate(&creds, &issuer(app))? {
        claim(app, &r, OAuthError::invalid_client("client assertion replayed")).await?;
    }
    let k = keys(app);
    // Invalid or unknown tokens are not an error (RFC 7009 §2.2).
    if let Some(r) = store::parse_refresh_token(tok) {
        if let Some(s) = store::get_session(app, &r.did, &r.session_id).await? {
            if r.authentic(&k.refresh, &s) && s.client_id == client.id {
                store::delete_session(app, &s.did, &s.id).await?;
            }
        }
    } else if let Ok(jwt) = k.server.verify(tok, "at+jwt") {
        if let (Some(did), Some(sid)) = (jwt.claim_str("sub"), jwt.claim_str("sid")) {
            if let Some(s) = store::get_session(app, did, sid).await? {
                if s.client_id == client.id {
                    store::delete_session(app, did, sid).await?;
                }
            }
        }
    } else if let Some(rid) = store::code_request_id(tok) {
        require_owner(app, &store::req_routing(&rid))?;
        let _g = store::lock(app, &format!("req:{rid}")).await;
        if let Some(req) = store::get_request(app, &rid).await? {
            let ok = req
                .code_hash
                .as_deref()
                .is_some_and(|h| ou::ct_eq(h.as_bytes(), store::hash_secret(tok).as_bytes()));
            if ok && req.client_id == client.id {
                if let Some((did, sid)) = &req.consumed {
                    store::delete_session(app, did, sid).await?;
                }
                store::put_request(app, &rid, None).await?;
            }
        }
    }
    Ok(())
}

// ---------- resource requests (DPoP-bound access tokens) ----------

tokio::task_local! {
    /// WWW-Authenticate challenge recorded by `verify_dpop` for the response
    /// layer (XrpcError can't carry headers).
    static DPOP_CHALLENGE: RefCell<Option<String>>;
}

fn dpop_fail(error: &str, desc: &str) -> XrpcError {
    let challenge = format!(
        "DPoP algs=\"ES256\", error=\"{error}\", error_description=\"{}\"",
        desc.replace('"', "'")
    );
    let _ = DPOP_CHALLENGE.try_with(|c| *c.borrow_mut() = Some(challenge));
    XrpcError {
        status: StatusCode::UNAUTHORIZED,
        error: error.into(),
        message: desc.into(),
    }
}

/// Response layer for requests authenticated with `Authorization: DPoP`:
/// adds a fresh `DPoP-Nonce` (RFC 9449 §8.2/§9) and, when verification
/// failed, the `WWW-Authenticate: DPoP error=...` challenge.
/// Installed with [`with_dpop_layer`], which clones the app only for DPoP
/// requests (not one `Arc<App>` refcount round trip per request).
async fn dpop_layer(app: Option<Arc<App>>, req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let Some(app) = app else {
        return next.run(req).await;
    };
    DPOP_CHALLENGE
        .scope(RefCell::new(None), async move {
            let mut r = next.run(req).await;
            let challenge = DPOP_CHALLENGE.with(|c| c.borrow_mut().take());
            let h = r.headers_mut();
            if let Ok(v) = HeaderValue::from_str(&keys(&app).nonces.next()) {
                h.insert(HeaderName::from_static("dpop-nonce"), v);
            }
            let mut expose = "DPoP-Nonce".to_string();
            if let Some(c) = challenge.filter(|_| r.status() == StatusCode::UNAUTHORIZED) {
                if let Ok(v) = HeaderValue::from_str(&c) {
                    r.headers_mut().insert(header::WWW_AUTHENTICATE, v);
                    expose.push_str(", WWW-Authenticate");
                }
            }
            if let Ok(v) = HeaderValue::from_str(&expose) {
                r.headers_mut()
                    .append(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
            }
            r
        })
        .await
}

/// Adds [`dpop_layer`] to `r`.
pub fn with_dpop_layer(r: axum::Router<Arc<App>>, app: &Arc<App>) -> axum::Router<Arc<App>> {
    let app = app.clone();
    r.layer(axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
        let is_dpop = req.headers().get(header::AUTHORIZATION).is_some_and(|v| v.as_bytes().starts_with(b"DPoP "));
        dpop_layer(is_dpop.then(|| app.clone()), req, next)
    }))
}

/// Verifies `Authorization: DPoP <token>` on a resource request.
pub async fn verify_dpop(app: &App, token: &str, parts: &Parts) -> XResult<Credentials> {
    let k = keys(app);
    let jwt = super::authn::verify_access_token(&k.server, token)
        .map_err(|e| dpop_fail("invalid_token", &e))?;
    let now = now_secs();
    let claims_ok = jwt.claim_str("iss") == Some(issuer(app).as_str())
        && jwt.claim_str("aud") == Some(app.jwt.service_did.as_str());
    if !claims_ok {
        return Err(dpop_fail(
            "invalid_token",
            "Invalid token audience or issuer",
        ));
    }
    if jwt.claim_i64("exp").is_none_or(|e| e <= now) {
        return Err(dpop_fail("invalid_token", "Token expired"));
    }
    let (Some(did), Some(sid), Some(jti), Some(client_id)) = (
        jwt.claim_str("sub"),
        jwt.claim_str("sid"),
        jwt.claim_str("jti"),
        jwt.claim_str("client_id"),
    ) else {
        return Err(dpop_fail("invalid_token", "Malformed token"));
    };
    let jkt = jwt
        .payload
        .get("cnf")
        .and_then(|c| c.get("jkt"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| dpop_fail("invalid_token", "Token is not DPoP-bound"))?;
    let proof = dpop_header(&parts.headers)
        .map_err(|e| dpop_fail("invalid_dpop_proof", &e))?
        .ok_or_else(|| dpop_fail("invalid_dpop_proof", "DPoP proof required"))?;
    let htu = jose::normalize_htu(&format!("{}{}", issuer(app), parts.uri.path()))
        .ok_or_else(|| XrpcError::internal("bad public_url"))?;
    let checked = jose::check_proof(&proof, parts.method.as_str(), &htu, Some(token), &k.nonces)
        .map_err(|e| match e {
            DpopError::UseNonce(m) => dpop_fail("use_dpop_nonce", &m),
            DpopError::Invalid(m) => dpop_fail("invalid_dpop_proof", &m),
        })?;
    if checked.jkt != jkt {
        return Err(dpop_fail(
            "invalid_token",
            "Access token is bound to another DPoP key",
        ));
    }
    // single use, claimed at the token DID's owner (normally this node: the
    // request was routed by that DID); `ath` binds the proof to this token.
    // In the owner's memory only, like the reference's replay store: a
    // durable claim would put a log write on every resource request (HA
    // notes in crate::oauth for the residual risk)
    let replay = checked.replay(did.to_string());
    match super::internal::claim_transient_anywhere(app, &replay.routing, &replay.key, replay.until).await {
        Ok(true) => {}
        Ok(false) => return Err(dpop_fail("invalid_dpop_proof", "DPoP proof replayed")),
        Err(e) => return Err(e),
    }
    // Stateful check: the session must still exist and this must be its
    // current token (rotation and revocation take effect immediately).
    let s = store::get_session(app, did, sid)
        .await
        .map_err(|e| XrpcError::internal(e.description))?;
    match s {
        Some(s) if s.token_id == jti && s.client_id == client_id => {}
        _ => return Err(dpop_fail("invalid_token", "Token has been revoked")),
    }
    let scopes = ScopeSet::new(jwt.claim_str("scope").unwrap_or(""));
    if !scopes.has("atproto") {
        return Err(dpop_fail(
            "invalid_token",
            "OAuth token does not have \"atproto\" scope",
        ));
    }
    Ok(Credentials::OAuth {
        did: did.to_string(),
        client_id: client_id.to_string(),
        scopes,
    })
}

// ---------- session management: /oauth/account UI ----------

fn redirect_to(path: &str) -> Response {
    let mut r = StatusCode::SEE_OTHER.into_response();
    r.headers_mut()
        .insert(header::LOCATION, HeaderValue::from_str(path).unwrap());
    r
}

fn rfc3339(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_default()
}

fn fmt_time(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default()
}

async fn account_page(
    State(app): AppState,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let (device, new_cookie) = match device_for(&app, &headers).await {
        Ok(d) => d,
        Err(e) => {
            return error_page(
                &app,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error",
                &e.description,
            )
        }
    };
    let csrf = csrf_token(&app, &device.id, "account");
    let accounts = device_accounts(&app, &device).await;
    let cookie = new_cookie.then_some(&device);
    if accounts.is_empty() || q.contains_key("add") {
        let pending = device
            .pending_2fa
            .as_ref()
            .filter(|(_, at)| now_secs() - at < PENDING_2FA_TTL)
            .is_some()
            && q.contains_key("totp");
        let body = ui::login(
            None,
            &ui::LoginForm {
                action: "/oauth/account/sign-in",
                identifier: "",
                // fixed messages by code only (never echo the query text)
                error: q
                    .get("error")
                    .and_then(|c| LoginError::from_code(c))
                    .map(LoginError::message),
                totp: pending,
                // the address isn't put in the URL; the page says "your email"
                email_hint: (pending && q.contains_key("email")).then_some("your email address"),
            },
            &csrf,
        );
        return html(&app, StatusCode::OK, body, &[], cookie);
    }
    let mut rows = Vec::new();
    for (did, handle) in accounts {
        let mut sessions = store::list_sessions(&app, &did).await.unwrap_or_default();
        sessions.sort_by_key(|s| -s.updated_at);
        let list = sessions
            .into_iter()
            .map(|s| ui::SessionRow {
                id: s.id,
                client_id: s.client_id,
                scope: s.scope,
                created_at: fmt_time(s.created_at),
                updated_at: fmt_time(s.updated_at),
            })
            .collect();
        rows.push((did, handle, list));
    }
    html(
        &app,
        StatusCode::OK,
        ui::account_page(&csrf, &rows),
        &[],
        cookie,
    )
}

#[allow(clippy::result_large_err)]
async fn account_form(
    app: &App,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(Device, HashMap<String, String>), Response> {
    let f: HashMap<String, String> = ou::parse_form(std::str::from_utf8(body).unwrap_or(""))
        .into_iter()
        .collect();
    let (device, new_cookie) = device_for(app, headers).await.map_err(|e| {
        error_page(
            app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error",
            &e.description,
        )
    })?;
    if new_cookie || !check_csrf(app, headers, &device, "account", f.get("csrf")) {
        return Err(error_page(
            app,
            StatusCode::FORBIDDEN,
            "Request failed",
            "Invalid or missing CSRF token.",
        ));
    }
    Ok((device, f))
}

async fn account_sign_in(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (mut device, f) = match account_form(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    match sign_in(&app, &mut device, &f).await {
        Ok(SignIn::Ok(_)) => redirect_to("/oauth/account"),
        Ok(SignIn::NeedTotp(_, hint)) => {
            redirect_to(&format!("/oauth/account?add=1&totp=1{}", if hint.is_some() { "&email=1" } else { "" }))
        }
        Ok(SignIn::NeedTotpErr(_, hint)) => redirect_to(&format!(
            "/oauth/account?add=1&totp=1{}&error={}",
            if hint.is_some() { "&email=1" } else { "" },
            LoginError::BadCode.code()
        )),
        Ok(SignIn::Failed(_, e)) => {
            redirect_to(&format!("/oauth/account?add=1&error={}", e.code()))
        }
        Err(e) => error_page(
            &app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Sign-in failed",
            &e.description,
        ),
    }
}

async fn account_sign_out(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (mut device, f) = match account_form(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    let did = f.get("did").cloned().unwrap_or_default();
    device.accounts.retain(|a| a.did != did);
    if let Err(e) = store::put_device(&app, &device).await {
        return error_page(
            &app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error",
            &e.description,
        );
    }
    redirect_to("/oauth/account")
}

async fn account_revoke(State(app): AppState, headers: HeaderMap, body: AxBytes) -> Response {
    let (device, f) = match account_form(&app, &headers, &body).await {
        Ok(x) => x,
        Err(r) => return r,
    };
    let did = f.get("did").cloned().unwrap_or_default();
    let sid = f.get("session").cloned().unwrap_or_default();
    let signed_in = device_accounts(&app, &device)
        .await
        .iter()
        .any(|(d, _)| *d == did);
    if !signed_in {
        return error_page(
            &app,
            StatusCode::FORBIDDEN,
            "Request failed",
            "You are not signed in to that account on this device.",
        );
    }
    if let Err(e) = store::delete_session(&app, &did, &sid).await {
        return error_page(
            &app,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error",
            &e.description,
        );
    }
    redirect_to("/oauth/account")
}

// ---------- session management: XRPC ----------

/// Only full account sessions (password login) may manage OAuth grants.
fn full_session(creds: &Credentials) -> XResult<String> {
    match creds {
        Credentials::Session { did } => Ok(did.clone()),
        _ => Err(XrpcError {
            status: StatusCode::FORBIDDEN,
            error: "InsufficientScope".into(),
            message: "a full account session is required".into(),
        }),
    }
}

async fn xrpc_list_sessions(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    let did = full_session(&creds)?;
    let mut sessions = store::list_sessions(&app, &did)
        .await
        .map_err(|e| XrpcError::internal(e.description))?;
    sessions.sort_by_key(|s| -s.updated_at);
    let out: Vec<J> = sessions
        .iter()
        .map(|s| {
            json!({
                "id": s.id,
                "clientId": s.client_id,
                "scope": s.scope,
                "createdAt": rfc3339(s.created_at),
                "updatedAt": rfc3339(s.updated_at),
                "accessExpiresAt": rfc3339(s.expires_at),
            })
        })
        .collect();
    Ok(Json(json!({"sessions": out})))
}

#[derive(Deserialize)]
struct RevokeSessionIn {
    id: String,
}

async fn xrpc_revoke_session(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<RevokeSessionIn>,
) -> XResult<Json<J>> {
    let did = full_session(&creds)?;
    if store::get_session(&app, &did, &inp.id)
        .await
        .map_err(|e| XrpcError::internal(e.description))?
        .is_none()
    {
        return Err(XrpcError::bad("SessionNotFound", "no such OAuth session"));
    }
    store::delete_session(&app, &did, &inp.id)
        .await
        .map_err(|e| XrpcError::internal(e.description))?;
    Ok(Json(json!({})))
}
