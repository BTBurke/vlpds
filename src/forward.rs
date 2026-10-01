//! HA request routing: any node accepts any request; requests for a DID whose
//! partition is owned by another node are proxied to that owner.
//!
//! XRPC: methods outside `com.atproto.*` / `vlpds.*` (proxied to the
//! AppView and other services, or the app.bsky preferences) route by the
//! bearer token's `sub` alone. Otherwise the routing DID comes from (in
//! order) the `repo` / `did` /
//! `handle` / `identifier` query parameter (handles resolved), the bearer
//! token's `sub` (when that is ours the body is never parsed), then the
//! `repo` / `did` / `identifier` field of a JSON body (handles and emails
//! resolved) and, for `com.atproto.admin.*`, the moderation `subject` (`did`,
//! or the DID of its `uri`; also the `uri` query parameter) or the `account`
//! / `recipientDid`, then the token `sub` (requestPasswordReset: its
//! `email`'s account; resetPassword: the account its token was issued for).
//! Requests without one (describeServer, subscribeRepos, ...) are served
//! locally.
//! Bodies are only buffered for JSON requests (bounded), so blob uploads
//! stream straight through; routing reads them with a borrowed struct that
//! skips every other field.
//!
//! OAuth (`/oauth/*`): routed by `xrpc::oauth::route_key` (the account, the
//! pushed request or the grant's owner; see the HA notes in `crate::oauth`).
//!
//! A forwarded request carries `x-vlpds-forwarded: <internal token>` and is
//! served by the receiver whatever its routing table says (no loops). The
//! marker is honored only with a valid internal token, and stripped either
//! way (a client's copy just routes normally). It is not `x-vlpds-internal`,
//! which would exempt forwarded requests from the owner's rate limits.
//!
//! Forwards fail fast: if the owner hasn't started answering within a
//! time-to-first-byte deadline (counted once the request body is sent) the
//! client gets 503 `PartitionUnavailable` + `Retry-After`, and the owner's
//! lease expiry moves the shard. Response bodies then stream without limit.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde::de::{Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Forwarded-request marker; its value is the internal token.
pub const FORWARDED_HEADER: &str = "x-vlpds-forwarded";
const MAX_JSON_BODY: usize = 4 << 20;
/// Owner time-to-first-byte for quick local work (repo/sync reads and
/// writes, sessions, admin, OAuth).
pub const TTFB_FAST: Duration = Duration::from_millis(3000);
/// ... for calls whose owner waits on something else (AppView proxying, PLC,
/// mail) or does bulk work (repo exports/imports, blobs).
pub const TTFB_SLOW: Duration = Duration::from_secs(30);
/// An upload whose body the owner stops reading for this long fails too.
const BODY_STALL: Duration = Duration::from_secs(10);
/// Ceiling on one forwarded exchange (response streaming included);
/// overrides the internal client's short default.
const FORWARD_MAX: Duration = Duration::from_secs(3600);

#[async_trait::async_trait]
pub trait Router: Send + Sync + 'static {
    /// Owner base URL for `did` if a *different* node owns it; None = handle here.
    fn remote_owner(&self, did: &str) -> Option<String>;
    /// Handle -> DID (for routing createSession by identifier).
    async fn resolve_handle(&self, handle: &str) -> Option<String>;
    /// The node itself: its internal token (forwarded markers are trusted
    /// and sent only with it), OAuth routing and email lookups. None (tests)
    /// = XRPC routing only, and no forwarded marker is trusted.
    fn app(&self) -> Option<Arc<crate::xrpc::App>> {
        None
    }
}

/// Routing key from the query string: a DID in `repo`/`did`, else a handle in
/// `repo`/`did`/`handle`/`identifier` (resolved to its DID by the caller). `admin` also
/// takes the DID of an at:// `uri` (getSubjectStatus).
fn query_target(query: Option<&str>, admin: bool) -> (Option<String>, Option<String>) {
    let mut handle = None;
    for kv in query.unwrap_or("").split('&') {
        let Some((k, v)) = kv.split_once('=') else { continue };
        if admin && k == "uri" {
            if let Some(d) = uri_did(&percent_decode(v)) {
                return (Some(d.to_string()), None);
            }
            continue;
        }
        if !matches!(k, "repo" | "did" | "handle" | "identifier") {
            continue;
        }
        let v = percent_decode(v);
        if v.starts_with("did:") {
            return (Some(v), None);
        }
        if handle.is_none() && v.contains('.') && !v.contains('@') {
            handle = Some(v);
        }
    }
    (None, handle)
}

/// The DID authority of an at:// URI.
fn uri_did(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://")?.split('/').next().filter(|d| d.starts_with("did:"))
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `sub` of a JWT (unverified: routing only; the owner verifies).
fn token_sub(req: &Request) -> Option<String> {
    let h = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let tok = h
        .strip_prefix("Bearer ")
        .or_else(|| h.strip_prefix("DPoP "))?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tok.split('.').nth(1)?)
        .ok()?;
    #[derive(Deserialize)]
    struct Claims<'a> {
        #[serde(borrow, default)]
        sub: Str<'a>,
    }
    let c: Claims = serde_json::from_slice(&payload).ok()?;
    c.sub.0.filter(|s| s.starts_with("did:")).map(Cow::into_owned)
}

// ---------- borrowed body routing ----------

/// A string field, or None for anything else (skipped without allocating).
#[derive(Default, Debug)]
struct Str<'a>(Option<Cow<'a, str>>);

impl<'de: 'a, 'a> Deserialize<'de> for Str<'a> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Str<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(Str(Some(Cow::Borrowed(v))))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Str(Some(Cow::Owned(v.to_string()))))
            }
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Str(Some(Cow::Owned(v))))
            }
            fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(Str(None))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                while a.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Str(None))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                while a.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(Str(None))
            }
        }
        d.deserialize_any(V)
    }
}

/// The routing fields of a moderation `subject` (any other shape: none).
#[derive(Default, Debug)]
struct Subject<'a> {
    did: Str<'a>,
    uri: Str<'a>,
}

impl<'de: 'a, 'a> Deserialize<'de> for Subject<'a> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Subject<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut s = Subject::default();
                while let Some(k) = a.next_key::<Str<'de>>()? {
                    match k.0.as_deref() {
                        Some("did") => s.did = a.next_value()?,
                        Some("uri") => s.uri = a.next_value()?,
                        _ => {
                            a.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(s)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                while a.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Subject::default())
            }
            fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
            fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
            fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
            fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
            fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(Subject::default())
            }
        }
        d.deserialize_any(V)
    }
}

/// Only the fields routing looks at; serde skips the rest of the body.
#[derive(Deserialize, Default, Debug)]
struct BodyKeys<'a> {
    #[serde(borrow, default)]
    repo: Str<'a>,
    #[serde(borrow, default)]
    did: Str<'a>,
    #[serde(borrow, default)]
    identifier: Str<'a>,
    #[serde(borrow, default)]
    subject: Subject<'a>,
    /// admin updateAccountEmail / {enable,disable}AccountInvites (DID or handle)
    #[serde(borrow, default)]
    account: Str<'a>,
    /// admin sendEmail
    #[serde(borrow, default, rename = "recipientDid")]
    recipient_did: Str<'a>,
}

#[derive(Debug, PartialEq)]
enum BodyTarget {
    Did(String),
    /// handle, or (identifier only) an email
    Ident(String),
}

/// Routing target of a JSON body: a DID in `repo` / `did` / `identifier`
/// (`admin`: also the moderation subject's), else a handle (or an email
/// `identifier`) to resolve.
fn body_target(body: &[u8], admin: bool) -> Option<BodyTarget> {
    let k: BodyKeys = serde_json::from_slice(body).ok()?;
    let is_did = |s: &Str| s.0.as_deref().filter(|v| v.starts_with("did:")).map(String::from);
    if let Some(d) = is_did(&k.repo).or_else(|| is_did(&k.did)).or_else(|| is_did(&k.identifier)) {
        return Some(BodyTarget::Did(d));
    }
    if admin {
        if let Some(d) = is_did(&k.subject.did)
            .or_else(|| k.subject.uri.0.as_deref().and_then(uri_did).map(String::from))
            .or_else(|| is_did(&k.account))
            .or_else(|| is_did(&k.recipient_did))
        {
            return Some(BodyTarget::Did(d));
        }
        if let Some(h) = k.account.0.as_deref().filter(|s| s.contains('.') && !s.contains('@')) {
            return Some(BodyTarget::Ident(h.to_string()));
        }
    }
    if let Some(i) = k.identifier.0.as_deref().filter(|s| s.contains('.') || s.contains('@')) {
        return Some(BodyTarget::Ident(i.to_string()));
    }
    k.repo
        .0
        .as_deref()
        .filter(|s| s.contains('.') && !s.contains('@'))
        .map(|s| BodyTarget::Ident(s.to_string()))
}

// ---------- the layer ----------

/// Strips the forwarded marker from a request; true when it carried a valid
/// internal token (a peer forwarded it).
fn take_forwarded(req: &mut Request, app: Option<&crate::xrpc::App>) -> bool {
    let Some(token) = req.headers_mut().remove(FORWARDED_HEADER) else {
        return false;
    };
    app.is_some_and(|a| {
        token
            .to_str()
            .is_ok_and(|t| crate::xrpc::internal::internal_token_ok(&a.config, t))
    })
}

#[allow(clippy::result_large_err)]
async fn buffer(req: Request) -> Result<(Request, bytes::Bytes), Response> {
    let (parts, body) = req.into_parts();
    match axum::body::to_bytes(body, MAX_JSON_BODY).await {
        Ok(b) => Ok((Request::from_parts(parts, Body::from(b.clone())), b)),
        Err(_) => Err((StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response()),
    }
}

async fn resolve_ident(router: &dyn Router, app: Option<&crate::xrpc::App>, ident: &str) -> Option<String> {
    match app {
        Some(app) => crate::xrpc::oauth::resolve_identifier(app, ident).await,
        None if !ident.contains('@') => router.resolve_handle(&ident.to_ascii_lowercase()).await,
        None => None,
    }
}

/// Unauthenticated calls that name their account another way:
/// requestPasswordReset by `email`, resetPassword by its token (the account
/// it was issued for). Both run through that account's owner.
async fn named_account(router: &dyn Router, app: Option<&crate::xrpc::App>, path: &str, body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct Named<'a> {
        #[serde(borrow, default)]
        email: Str<'a>,
        #[serde(borrow, default)]
        token: Str<'a>,
    }
    let n: Named = match path {
        "/xrpc/com.atproto.server.requestPasswordReset" | "/xrpc/com.atproto.server.resetPassword" => {
            serde_json::from_slice(body).ok()?
        }
        _ => return None,
    };
    if path.ends_with("requestPasswordReset") {
        let email = n.email.0.filter(|e| e.contains('@'))?;
        return resolve_ident(router, app, &email).await;
    }
    crate::xrpc::reset_token_did(app?, n.token.0.as_deref()?).await.ok()?
}

/// XRPC routing DID (None = serve here).
#[allow(clippy::result_large_err)]
async fn xrpc_target(
    router: &dyn Router,
    app: Option<&crate::xrpc::App>,
    req: Request,
) -> Result<(Request, Option<String>), Response> {
    let nsid = req.uri().path().strip_prefix("/xrpc/").unwrap_or("");
    if !nsid.starts_with("com.atproto.") && !nsid.starts_with("vlpds.") {
        // app.bsky.* / chat.bsky.* / tools.ozone.* / ...: proxied (or the
        // app.bsky preferences), on behalf of the caller, whose account (and
        // signing key) is at its owner, whatever DIDs the parameters name
        // (e.g. tools.ozone.moderation.getRepo?did=). Nothing else to parse.
        let sub = token_sub(&req);
        return Ok((req, sub));
    }
    let admin = nsid.starts_with("com.atproto.admin.");
    match query_target(req.uri().query(), admin) {
        (Some(d), _) => return Ok((req, Some(d))),
        (None, Some(h)) => {
            let did = router.resolve_handle(&h.to_ascii_lowercase()).await;
            return Ok((req, did));
        }
        (None, None) => {}
    }
    let sub = token_sub(&req);
    if sub.as_deref().is_some_and(|s| router.remote_owner(s).is_none()) {
        // the caller's own account is ours: no body parse
        return Ok((req, None));
    }
    let is_json = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if !is_json {
        return Ok((req, sub));
    }
    let (req, b) = buffer(req).await?;
    let did = match body_target(&b, admin) {
        Some(BodyTarget::Did(d)) => Some(d),
        Some(BodyTarget::Ident(i)) => resolve_ident(router, app, &i).await,
        None => named_account(router, app, req.uri().path(), &b).await,
    };
    Ok((req, did.or(sub)))
}

/// OAuth routing key (None = serve here).
#[allow(clippy::result_large_err)]
async fn oauth_target(
    app: Option<&crate::xrpc::App>,
    req: Request,
) -> Result<(Request, Option<String>), Response> {
    let Some(app) = app else {
        return Ok((req, None));
    };
    let (req, body) = if req.method() == Method::POST {
        buffer(req).await?
    } else {
        (req, bytes::Bytes::new())
    };
    let key = crate::xrpc::oauth::route_key(
        app,
        req.uri().path(),
        req.uri().query(),
        req.headers(),
        &body,
    )
    .await;
    Ok((req, key))
}

pub async fn route(
    router: Arc<dyn Router>,
    client: crate::http::PeerClient,
    mut req: Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    let (xrpc, oauth) = (path.starts_with("/xrpc/"), path.starts_with("/oauth/"));
    if !xrpc && !oauth {
        return next.run(req).await;
    }
    let app = router.app();
    if take_forwarded(&mut req, app.as_deref()) {
        return next.run(req).await;
    }
    let target = if xrpc {
        xrpc_target(&*router, app.as_deref(), req).await
    } else {
        oauth_target(app.as_deref(), req).await
    };
    let (req, key) = match target {
        Ok(t) => t,
        Err(r) => return r,
    };
    let Some(owner) = key.as_deref().and_then(|k| router.remote_owner(k)) else {
        return next.run(req).await;
    };
    crate::metrics::FORWARDED.inc();
    let token = app.as_ref().map(|a| a.config.internal_token.clone());
    let ttfb = ttfb_for(&req);
    let t = Instant::now();
    let resp = forward(client.pick(), &owner, req, token.as_deref(), ttfb).await;
    crate::metrics::observe_forward(resp.status().as_u16(), t);
    resp
}

/// Time-to-first-byte deadline for forwarding `req` (see [`TTFB_FAST`]).
fn ttfb_for(req: &Request) -> Duration {
    let path = req.uri().path();
    if path.starts_with("/oauth/") {
        return TTFB_FAST;
    }
    if req.headers().contains_key("atproto-proxy") {
        return TTFB_SLOW;
    }
    let nsid = path.trim_start_matches("/xrpc/");
    let slow = matches!(
        nsid,
        "com.atproto.repo.uploadBlob"
            | "com.atproto.repo.importRepo"
            | "com.atproto.sync.getRepo"
            | "com.atproto.sync.getBlob"
            | "com.atproto.sync.getBlocks"
            | "com.atproto.server.createAccount"
    ) || nsid.starts_with("com.atproto.server.request");
    let fast = ["com.atproto.repo.", "com.atproto.sync.", "com.atproto.server.", "com.atproto.admin.", "vlpds."]
        .iter()
        .any(|p| nsid.starts_with(p));
    if fast && !slow {
        TTFB_FAST
    } else {
        TTFB_SLOW
    }
}

/// Request body progress, for the time-to-first-byte deadline.
struct Progress {
    start: Instant,
    /// millis since `start` of the last chunk handed to the owner
    last_ms: AtomicU64,
    done: AtomicBool,
    done_notify: tokio::sync::Notify,
}

impl Progress {
    fn finish(&self) {
        if !self.done.swap(true, Ordering::AcqRel) {
            self.done_notify.notify_one();
        }
    }

    /// Resolves when the owner is late: `ttfb` after the body was fully
    /// sent, or once the owner stopped reading it for [`BODY_STALL`].
    async fn deadline(&self, ttfb: Duration) {
        loop {
            if self.done.load(Ordering::Acquire) {
                tokio::time::sleep(ttfb).await;
                return;
            }
            tokio::select! {
                _ = self.done_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let last = Duration::from_millis(self.last_ms.load(Ordering::Relaxed));
                    if self.start.elapsed().saturating_sub(last) > BODY_STALL {
                        return;
                    }
                }
            }
        }
    }
}

/// The request body as a stream that records progress.
fn tracked_body(body: Body, p: Arc<Progress>) -> reqwest::Body {
    use futures::StreamExt;
    let p2 = p.clone();
    let s = body
        .into_data_stream()
        .map(move |chunk| {
            p.last_ms.store(p.start.elapsed().as_millis() as u64, Ordering::Relaxed);
            chunk
        })
        .chain(futures::stream::poll_fn(move |_| {
            p2.finish();
            std::task::Poll::<Option<Result<bytes::Bytes, axum::Error>>>::Ready(None)
        }));
    reqwest::Body::wrap_stream(s)
}

fn unavailable(message: String) -> Response {
    let mut r = (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({"error": "PartitionUnavailable", "message": message})),
    )
        .into_response();
    r.headers_mut().insert(axum::http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    r
}

async fn forward(
    client: &reqwest::Client,
    owner: &str,
    req: Request,
    internal_token: Option<&str>,
    ttfb: Duration,
) -> Response {
    let (parts, body) = req.into_parts();
    let url = format!(
        "{}{}",
        owner.trim_end_matches('/'),
        parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
    );
    let mut rb = client.request(parts.method.clone(), &url).timeout(FORWARD_MAX);
    for (k, v) in parts.headers.iter() {
        if k != axum::http::header::HOST && k != axum::http::header::CONTENT_LENGTH {
            rb = rb.header(k, v);
        }
    }
    // the marker (dropped by the receiver unless the token is valid)
    rb = rb.header(FORWARDED_HEADER, internal_token.unwrap_or("-"));
    // client address for the owner's per-IP rate limits (used there when
    // this node is one of its trusted_proxies)
    if let Some(peer) = parts
        .extensions
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        rb = rb.header("x-forwarded-for", peer.0.ip().to_string());
    }
    let progress = Arc::new(Progress {
        start: Instant::now(),
        last_ms: AtomicU64::new(0),
        done: AtomicBool::new(false),
        done_notify: tokio::sync::Notify::new(),
    });
    if axum::body::HttpBody::size_hint(&body).exact() == Some(0) {
        progress.finish();
    } else {
        rb = rb.body(tracked_body(body, progress.clone()));
    }
    let send = rb.send();
    let resp = tokio::select! {
        r = send => match r {
            Ok(r) => r,
            Err(e) => return unavailable(format!("owner unreachable: {e}")),
        },
        _ = progress.deadline(ttfb) => {
            tracing::warn!(owner, path = parts.uri.path(), "forward: owner did not answer in time");
            return unavailable(format!("owner did not answer within {} ms", ttfb.as_millis()));
        }
    };
    let mut out = Response::builder().status(resp.status().as_u16());
    for (k, v) in resp.headers() {
        out = out.header(k, v);
    }
    out.body(Body::from_stream(resp.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing() {
        assert_eq!(
            query_target(Some("repo=did%3Aplc%3Aabc&collection=x"), false),
            (Some("did:plc:abc".into()), None)
        );
        assert_eq!(
            query_target(Some("did=did:web:example.com"), false),
            (Some("did:web:example.com".into()), None)
        );
        assert_eq!(query_target(Some("repo=alice.test"), false), (None, Some("alice.test".into())));
        assert_eq!(query_target(Some("flag&handle=bob.test"), false), (None, Some("bob.test".into())));
        assert_eq!(query_target(Some("cursor=a.b&limit=5"), false), (None, None));
        assert_eq!(query_target(None, false), (None, None));
        let uri = "uri=at%3A%2F%2Fdid%3Aplc%3Axyz%2Fapp.bsky.feed.post%2F3k";
        assert_eq!(query_target(Some(uri), true), (Some("did:plc:xyz".into()), None));
        assert_eq!(query_target(Some(uri), false), (None, None), "uri only routes admin calls");
    }

    #[test]
    fn body_parsing() {
        let t = |s: &str, admin| body_target(s.as_bytes(), admin);
        let did = |d: &str| Some(BodyTarget::Did(d.into()));
        assert_eq!(t(r#"{"repo":"did:plc:a","collection":"x","record":{"text":"hi","did":"did:plc:z"}}"#, false), did("did:plc:a"));
        assert_eq!(t(r#"{"record":{"repo":"did:plc:z"},"did":"did:plc:b"}"#, false), did("did:plc:b"));
        assert_eq!(t(r#"{"identifier":"did:plc:c","password":"p"}"#, false), did("did:plc:c"));
        assert_eq!(t(r#"{"identifier":"Alice.Test","password":"p"}"#, false), Some(BodyTarget::Ident("Alice.Test".into())));
        assert_eq!(t(r#"{"identifier":"a@b.c"}"#, false), Some(BodyTarget::Ident("a@b.c".into())));
        assert_eq!(t(r#"{"repo":"alice.test"}"#, false), Some(BodyTarget::Ident("alice.test".into())));
        // escaped strings (owned), non-string fields, other shapes
        assert_eq!(t(r#"{"repo":"did:plc:ab"}"#, false), did("did:plc:ab"));
        assert_eq!(t(r#"{"repo":{"x":[1,2]},"did":7,"identifier":null}"#, false), None);
        assert_eq!(t(r#"[1,2]"#, false), None);
        assert_eq!(t(r#"not json"#, false), None);
        // moderation subjects: admin calls only
        let td = r#"{"subject":{"$type":"com.atproto.admin.defs#repoRef","did":"did:plc:s"},"takedown":{"applied":true}}"#;
        assert_eq!(t(td, true), did("did:plc:s"));
        assert_eq!(t(td, false), None);
        let rec = r#"{"subject":{"$type":"com.atproto.repo.strongRef","uri":"at://did:plc:r/app.bsky.feed.post/1","cid":"bafy"}}"#;
        assert_eq!(t(rec, true), did("did:plc:r"));
        assert_eq!(t(r#"{"subject":"x"}"#, true), None);
        // admin account updates name the account as `account` / `recipientDid`
        assert_eq!(t(r#"{"account":"did:plc:e","email":"a@b.c"}"#, true), did("did:plc:e"));
        assert_eq!(t(r#"{"account":"alice.test","email":"a@b.c"}"#, true), Some(BodyTarget::Ident("alice.test".into())));
        assert_eq!(t(r#"{"account":"did:plc:e"}"#, false), None);
        assert_eq!(t(r#"{"recipientDid":"did:plc:m","content":"hi"}"#, true), did("did:plc:m"));
    }

    #[test]
    fn token_sub_parsing() {
        let b64 = |j: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(j);
        let req = |auth: String| Request::builder().header("authorization", auth).body(Body::empty()).unwrap();
        let tok = format!("h.{}.s", b64(r#"{"scope":"x","sub":"did:plc:me","aud":["a"]}"#));
        assert_eq!(token_sub(&req(format!("Bearer {tok}"))).as_deref(), Some("did:plc:me"));
        assert_eq!(token_sub(&req(format!("DPoP {tok}"))).as_deref(), Some("did:plc:me"));
        let tok = format!("h.{}.s", b64(r#"{"sub":5}"#));
        assert_eq!(token_sub(&req(format!("Bearer {tok}"))), None);
    }

    #[tokio::test]
    async fn proxied_methods_route_by_the_caller() {
        let b64 = |j: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(j);
        let tok = format!("Bearer h.{}.s", b64(r#"{"sub":"did:plc:me"}"#));
        let target = |uri: &str, auth: bool| {
            let mut b = Request::builder().uri(uri);
            if auth {
                b = b.header("authorization", tok.clone());
            }
            let req = b.body(Body::empty()).unwrap();
            async move { xrpc_target(&Fixed(None), None, req).await.ok().unwrap().1 }
        };
        let me = Some("did:plc:me".to_string());
        assert_eq!(target("/xrpc/tools.ozone.moderation.getRepo?did=did:plc:subject", true).await, me);
        assert_eq!(target("/xrpc/app.bsky.feed.getTimeline?limit=5&repo=did:plc:x", true).await, me);
        assert_eq!(target("/xrpc/app.bsky.feed.getTimeline", false).await, None);
        // com.atproto.* still routes by the repo it names
        let subject = Some("did:plc:subject".to_string());
        assert_eq!(target("/xrpc/com.atproto.repo.getRecord?repo=did:plc:subject", true).await, subject);
    }

    struct Fixed(Option<String>);

    #[async_trait::async_trait]
    impl Router for Fixed {
        fn remote_owner(&self, _: &str) -> Option<String> {
            self.0.clone()
        }
        async fn resolve_handle(&self, _: &str) -> Option<String> {
            None
        }
    }

    /// A node whose routing sends every DID to `owner`, serving "local" for
    /// whatever it keeps; returns its base URL.
    async fn spawn_node(name: &'static str, owner: Option<String>) -> String {
        let r: Arc<dyn Router> = Arc::new(Fixed(owner));
        let client = crate::http::PeerClient::single(reqwest::Client::new());
        let app = axum::Router::new()
            .route(
                "/xrpc/{nsid}",
                axum::routing::any(move |req: Request| async move {
                    let fwd = req.headers().contains_key(FORWARDED_HEADER);
                    format!("{name} forwarded={fwd}")
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let (r, client) = (r.clone(), client.clone());
                async move { route(r, client, req, next).await }
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        url
    }

    /// An owner that accepts connections and reads, but never answers.
    async fn frozen_owner() -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = l.accept().await {
                held.push(s);
            }
        });
        url
    }

    #[tokio::test]
    async fn frozen_owner_fails_fast_with_503() {
        let owner = frozen_owner().await;
        let req = Request::builder()
            .method("POST")
            .uri("/xrpc/com.atproto.repo.createRecord")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"repo":"did:plc:x"}"#))
            .unwrap();
        let t = Instant::now();
        let r = forward(&reqwest::Client::new(), &owner, req, None, Duration::from_millis(300)).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.headers().get("retry-after").unwrap(), "1");
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        let b = axum::body::to_bytes(r.into_body(), 1 << 16).await.unwrap();
        assert!(String::from_utf8_lossy(&b).contains("PartitionUnavailable"));

        // through the layer, with the production deadline for a quick call
        let node = spawn_node("n", Some(owner)).await;
        let t = Instant::now();
        let r = reqwest::Client::new()
            .get(format!("{node}/xrpc/com.atproto.repo.getRecord?repo=did:plc:x"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 503);
        assert!(t.elapsed() < TTFB_FAST + Duration::from_secs(2), "{:?}", t.elapsed());
    }

    #[tokio::test]
    async fn client_forwarded_marker_is_stripped() {
        // b owns nothing locally (everything routes to a); a serves locally
        let a = spawn_node("a", None).await;
        let b = spawn_node("b", Some(a.clone())).await;
        let http = reqwest::Client::new();
        // a forged marker does not make b serve locally, nor reach a's handler
        let r = http
            .get(format!("{b}/xrpc/com.atproto.repo.getRecord?repo=did:plc:x"))
            .header(FORWARDED_HEADER, "guess")
            .send()
            .await
            .unwrap();
        // b forwarded it (no app = no trusted marker); a, also untrusting,
        // served it locally after stripping the client's copy
        assert_eq!(r.text().await.unwrap(), "a forwarded=false");
    }

    /// Routing-parse cost, borrowed struct vs serde_json::Value (run with
    /// `--release -- --ignored --nocapture`).
    #[test]
    #[ignore]
    fn routing_parse_bench() {
        let body = serde_json::json!({
            "repo": "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
            "collection": "app.bsky.feed.post",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "a fairly ordinary post with some text in it, a link and a mention".repeat(3),
                "createdAt": "2026-10-01T00:00:00.000Z",
                "langs": ["en"],
                "facets": [{"index": {"byteStart": 0, "byteEnd": 10}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:abc"}]}],
                "embed": {"$type": "app.bsky.embed.external", "external": {"uri": "https://example.com/x", "title": "t", "description": "d"}}
            }
        })
        .to_string();
        let n = 200_000;
        let t = Instant::now();
        for _ in 0..n {
            let v: serde_json::Value = serde_json::from_slice(body.as_bytes()).unwrap();
            std::hint::black_box(["repo", "did", "identifier"].iter().find_map(|k| v.get(*k)?.as_str().map(String::from)));
        }
        let value = t.elapsed() / n;
        let t = Instant::now();
        for _ in 0..n {
            std::hint::black_box(body_target(body.as_bytes(), false));
        }
        let borrowed = t.elapsed() / n;
        println!("routing parse ({} B body): Value {value:?}, borrowed {borrowed:?}", body.len());
    }
}
