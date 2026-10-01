//! XRPC-flavoured `Json` and `Query` extractors. They replace axum's (via the
//! prelude) so that every rejection is an XRPC error envelope
//! (`{error, message}`) with the reference's status codes instead of axum's
//! plain-text 4xx bodies:
//!
//! - malformed / mistyped JSON or query params: 400 `InvalidRequest`
//!   (axum would answer 400/415/422 in plain text);
//! - JSON bodies over 150 KiB: 413 `PayloadTooLarge` (reference
//!   `jsonLimit: 150 * 1024`, packages/pds/src/index.ts);
//! - params whose lexicon format is fixed everywhere they appear (`did`,
//!   `repo` (at-identifier), `cid`, `handle`) are syntax-checked, so a bad
//!   value is a 400 `InvalidRequest` rather than a lookup miss, as the
//!   reference's lexicon param validation does.

use super::syntax;
use super::XrpcError;
use axum::extract::{FromRequest, FromRequestParts, OptionalFromRequest, Request};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::de::DeserializeOwned;

/// Largest JSON request body (reference: 150kb).
pub const JSON_LIMIT: usize = 150 * 1024;

fn invalid(message: impl Into<String>) -> XrpcError {
    XrpcError::bad("InvalidRequest", message)
}

pub fn too_large() -> XrpcError {
    XrpcError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        error: "PayloadTooLarge".into(),
        message: "request entity too large".into(),
    }
}

// ---------------------------------------------------------------------------
// Json
// ---------------------------------------------------------------------------

pub struct Json<T>(pub T);

impl<T: serde::Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

/// Reads at most `limit` bytes of body (413 past it).
pub async fn read_body(req: Request, limit: usize) -> Result<Vec<u8>, XrpcError> {
    if req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > limit)
    {
        return Err(too_large());
    }
    let mut stream = req.into_body().into_data_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| invalid(format!("error reading body: {e}")))?;
        if out.len() + chunk.len() > limit {
            return Err(too_large());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn check_content_type(req: &Request) -> Result<(), XrpcError> {
    match req.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
        // Missing content type: let the JSON parse decide (clients that omit
        // it for JSON bodies are common).
        None => Ok(()),
        Some(ct) => {
            let mime = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
            if mime == "application/json" || (mime.starts_with("application/") && mime.ends_with("+json")) {
                Ok(())
            } else {
                Err(invalid(format!(
                    "Wrong request encoding (Content-Type): {mime}"
                )))
            }
        }
    }
}

fn parse<T: DeserializeOwned>(body: &[u8]) -> Result<T, XrpcError> {
    serde_json::from_slice(body).map_err(|e| {
        if e.is_eof() && body.iter().all(|b| b.is_ascii_whitespace()) {
            invalid("Request body is required")
        } else {
            invalid(format!("Invalid JSON body: {e}"))
        }
    })
}

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for Json<T> {
    type Rejection = XrpcError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        check_content_type(&req)?;
        let body = read_body(req, JSON_LIMIT).await?;
        parse(&body).map(Json)
    }
}

/// `Option<Json<T>>`: an empty body is `None`; anything else must parse.
impl<T: DeserializeOwned, S: Send + Sync> OptionalFromRequest<S> for Json<T> {
    type Rejection = XrpcError;

    async fn from_request(req: Request, _state: &S) -> Result<Option<Self>, Self::Rejection> {
        check_content_type(&req)?;
        let body = read_body(req, JSON_LIMIT).await?;
        if body.iter().all(|b| b.is_ascii_whitespace()) {
            return Ok(None);
        }
        parse(&body).map(|v| Some(Json(v)))
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

pub struct Query<T>(pub T);

/// CID string syntax (lexicon format `cid`): a multibase CIDv1 string. CIDv0
/// (`Qm...`) is not supported by atproto.
pub fn valid_cid_syntax(s: &str) -> bool {
    (8..=256).contains(&s.len())
        && !s.starts_with("Qm")
        && s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+/=-_".contains(&b))
}

/// at-identifier: a DID or a handle.
pub fn valid_at_identifier(s: &str) -> bool {
    if s.starts_with("did:") {
        syntax::valid_did(s)
    } else {
        syntax::valid_handle(s)
    }
}

/// Syntax checks for params whose lexicon format is the same in every
/// com.atproto method that takes them.
fn validate_params(parts: &Parts) -> Result<(), XrpcError> {
    if parts.uri.query().is_none() || !parts.uri.path().starts_with("/xrpc/com.atproto.") {
        return Ok(());
    }
    let pairs: Vec<(String, String)> =
        axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&parts.uri)
            .map(|q| q.0)
            .map_err(|e| invalid(e.body_text()))?;
    for (k, v) in &pairs {
        let ok = match k.as_str() {
            "did" => syntax::valid_did(v),
            "repo" => valid_at_identifier(v),
            "cid" => valid_cid_syntax(v),
            "handle" => syntax::valid_handle(v),
            _ => true,
        };
        if !ok {
            let what = match k.as_str() {
                "did" => "DID",
                "repo" => "at-identifier",
                "cid" => "CID",
                _ => "handle",
            };
            return Err(invalid(format!("Invalid {what} in param {k}: {v}")));
        }
    }
    Ok(())
}

impl<T: DeserializeOwned, S: Send + Sync> FromRequestParts<S> for Query<T> {
    type Rejection = XrpcError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        validate_params(parts)?;
        axum::extract::Query::<T>::try_from_uri(&parts.uri)
            .map(|q| Query(q.0))
            .map_err(|e| invalid(e.body_text()))
    }
}
