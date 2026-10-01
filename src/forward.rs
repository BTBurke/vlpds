//! HA request routing: any node accepts any request; requests for a DID whose
//! partition is owned by another node are proxied to that owner.
//!
//! The routing DID comes from (in order): the `repo` / `did` query parameter,
//! the `repo` / `did` field of a JSON body, or the `sub` of the bearer token.
//! Requests without a routing DID (describeServer, subscribeRepos, ...) are
//! served locally. Bodies are only buffered for JSON requests (bounded), so
//! blob uploads stream straight through.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use std::sync::Arc;

pub const FORWARDED_HEADER: &str = "x-vlpds-forwarded";
const MAX_JSON_BODY: usize = 4 << 20;

#[async_trait::async_trait]
pub trait Router: Send + Sync + 'static {
    /// Owner base URL for `did` if a *different* node owns it; None = handle here.
    fn remote_owner(&self, did: &str) -> Option<String>;
    /// Handle -> DID (for routing createSession by identifier).
    async fn resolve_handle(&self, handle: &str) -> Option<String>;
}

/// Routing key from the query string: a DID in `repo`/`did`, else a handle in
/// `repo`/`did`/`handle` (resolved to its DID by the caller).
fn query_target(query: Option<&str>) -> (Option<String>, Option<String>) {
    let mut handle = None;
    for kv in query.unwrap_or("").split('&') {
        let Some((k, v)) = kv.split_once('=') else { continue };
        if !matches!(k, "repo" | "did" | "handle") {
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
    let payload = tok.split('.').nth(1)?;
    let claims: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .ok()?,
    )
    .ok()?;
    claims.get("sub")?.as_str().map(String::from)
}

pub async fn route(
    router: Arc<dyn Router>,
    client: reqwest::Client,
    req: Request,
    next: axum::middleware::Next,
) -> Response {
    if req.headers().contains_key(FORWARDED_HEADER) || !req.uri().path().starts_with("/xrpc/") {
        return next.run(req).await;
    }
    let (mut did, mut handle) = query_target(req.uri().query());
    let is_json = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    let (parts, body) = req.into_parts();
    let body = if did.is_none() && handle.is_none() && is_json {
        match axum::body::to_bytes(body, MAX_JSON_BODY).await {
            Ok(b) => {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                    did = ["repo", "did", "identifier"].iter().find_map(|k| {
                        v.get(*k)?
                            .as_str()
                            .filter(|s| s.starts_with("did:"))
                            .map(String::from)
                    });
                    if did.is_none() {
                        handle = ["identifier", "repo"].iter().find_map(|k| {
                            v.get(*k)?
                                .as_str()
                                .filter(|s| s.contains('.') && !s.contains('@'))
                                .map(String::from)
                        });
                    }
                }
                Body::from(b)
            }
            Err(_) => {
                return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response()
            }
        }
    } else {
        body
    };
    let req = Request::from_parts(parts, body);
    let did = match (did, handle) {
        (Some(d), _) => Some(d),
        (None, Some(h)) => router.resolve_handle(&h.to_ascii_lowercase()).await,
        (None, None) => None,
    };
    let did = did.or_else(|| token_sub(&req));
    let Some(owner) = did.as_deref().and_then(|d| router.remote_owner(d)) else {
        return next.run(req).await;
    };
    crate::metrics::FORWARDED.inc();
    forward(&client, &owner, req).await
}

async fn forward(client: &reqwest::Client, owner: &str, req: Request) -> Response {
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
    let mut rb = client.request(parts.method.clone(), &url);
    for (k, v) in parts.headers.iter() {
        if k != axum::http::header::HOST && k != axum::http::header::CONTENT_LENGTH {
            rb = rb.header(k, v);
        }
    }
    rb = rb.header(FORWARDED_HEADER, HeaderValue::from_static("1"));
    // client address for the owner's per-IP rate limits (used there when
    // this node is one of its trusted_proxies)
    if let Some(peer) = parts
        .extensions
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        rb = rb.header("x-forwarded-for", peer.0.ip().to_string());
    }
    let stream = body.into_data_stream();
    let resp = match rb.body(reqwest::Body::wrap_stream(stream)).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({"error": "PartitionUnavailable", "message": format!("owner unreachable: {e}")})),
            )
                .into_response()
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
            query_target(Some("repo=did%3Aplc%3Aabc&collection=x")),
            (Some("did:plc:abc".into()), None)
        );
        assert_eq!(
            query_target(Some("did=did:web:example.com")),
            (Some("did:web:example.com".into()), None)
        );
        assert_eq!(query_target(Some("repo=alice.test")), (None, Some("alice.test".into())));
        assert_eq!(query_target(Some("flag&handle=bob.test")), (None, Some("bob.test".into())));
        assert_eq!(query_target(Some("cursor=a.b&limit=5")), (None, None));
        assert_eq!(query_target(None), (None, None));
    }
}
