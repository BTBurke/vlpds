//! XRPC-flavoured `Json` and `Query` extractors. They replace axum's (via the
//! prelude) so that every rejection is an XRPC error envelope
//! (`{error, message}`) with the reference's status codes instead of axum's
//! plain-text 4xx bodies:
//!
//! - malformed / mistyped JSON or query params: 400 `InvalidRequest`
//!   (axum would answer 400/415/422 in plain text);
//! - JSON bodies over 150 KiB: 413 `PayloadTooLarge` (reference
//!   `jsonLimit: 150 * 1024`, packages/pds/src/index.ts); record writes
//!   (`RecordBody`) allow 1,000,000 bytes like the reference's
//!   createRecord/putRecord/applyWrites;
//! - params whose lexicon format is fixed everywhere they appear (`did`,
//!   `repo` (at-identifier), `cid`, `handle`) are syntax-checked, so a bad
//!   value is a 400 `InvalidRequest` rather than a lookup miss, as the
//!   reference's lexicon param validation does;
//! - params and JSON bodies of the bundled com.atproto.* methods are then
//!   validated against their lexicons (src/lexicon.rs), with the
//!   reference's `Params ...` / `Input ...` messages. A checked body is
//!   parsed once into a JSON value, validated, then converted to `T`.
//!   Record writes ([`RecordBody`]) parse into a borrowed [`JsonValue`]
//!   instead, which the handler validates and encodes records from.
//!   [`debug_output_layer`] checks responses in debug builds.

use super::syntax;
use super::XrpcError;
use axum::extract::{FromRequest, FromRequestParts, OptionalFromRequest, Request};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use crate::cbor::JsonValue;
use serde::de::DeserializeOwned;

/// Largest JSON request body (reference: 150kb).
pub const JSON_LIMIT: usize = 150 * 1024;
/// Largest record-write JSON body (reference createRecord/putRecord/
/// applyWrites `jsonLimit: 1_000_000`).
pub const RECORD_JSON_LIMIT: usize = 1_000_000;

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
        #[allow(unused_mut)]
        let mut r = axum::Json(self.0).into_response();
        #[cfg(debug_assertions)]
        r.extensions_mut().insert(HandlerJson);
        r
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

fn parse<'a, T: serde::Deserialize<'a>>(body: &'a [u8]) -> Result<T, XrpcError> {
    serde_json::from_slice(body).map_err(|e| {
        if e.is_eof() && body.iter().all(|b| b.is_ascii_whitespace()) {
            invalid("Request body is required")
        } else {
            invalid(format!("Invalid JSON body: {e}"))
        }
    })
}

/// The method NSID of a request whose JSON input has a bundled schema.
fn input_nsid(req: &Request) -> Option<String> {
    req.uri()
        .path()
        .strip_prefix("/xrpc/")
        .filter(|n| crate::lexicon::has_input_schema(n))
        .map(String::from)
}

/// [`parse`], validating against the method's input schema if it has one.
/// (Validating a borrowed [`JsonValue`] and then parsing `T` from the bytes
/// measured no faster for these small bodies: 1.08 us either way.)
fn parse_input<T: DeserializeOwned>(nsid: Option<&str>, body: &[u8]) -> Result<T, XrpcError> {
    let Some(nsid) = nsid else {
        return parse(body);
    };
    let v: serde_json::Value = parse(body)?;
    crate::lexicon::validate_input(nsid, &v).map_err(invalid)?;
    serde_json::from_value(v).map_err(|e| invalid(format!("Invalid JSON body: {e}")))
}

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for Json<T> {
    type Rejection = XrpcError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        check_content_type(&req)?;
        let nsid = input_nsid(&req);
        let body = read_body(req, JSON_LIMIT).await?;
        parse_input(nsid.as_deref(), &body).map(Json)
    }
}

/// A record-write JSON body (createRecord / putRecord / applyWrites) with
/// the record-write limit ([`RECORD_JSON_LIMIT`]). The handler parses it
/// once into a [`JsonValue`] borrowing from the body ([`RecordBody::parse`])
/// and encodes records straight from that tree.
pub struct RecordBody {
    body: Vec<u8>,
    nsid: Option<String>,
}

impl RecordBody {
    /// Parses the body, validated against the method's input schema.
    pub fn parse(&self) -> Result<JsonValue<'_>, XrpcError> {
        let v: JsonValue = parse(&self.body)?;
        if let Some(nsid) = &self.nsid {
            crate::lexicon::validate_input(nsid, &v).map_err(invalid)?;
        }
        Ok(v)
    }

    #[cfg(test)]
    pub fn new(nsid: &str, body: Vec<u8>) -> RecordBody {
        let nsid = crate::lexicon::has_input_schema(nsid).then(|| nsid.to_string());
        RecordBody { body, nsid }
    }
}

impl<S: Send + Sync> FromRequest<S> for RecordBody {
    type Rejection = XrpcError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        check_content_type(&req)?;
        let nsid = input_nsid(&req);
        let body = read_body(req, RECORD_JSON_LIMIT).await?;
        Ok(RecordBody { body, nsid })
    }
}

/// `Option<Json<T>>`: an empty body is `None`; anything else must parse.
impl<T: DeserializeOwned, S: Send + Sync> OptionalFromRequest<S> for Json<T> {
    type Rejection = XrpcError;

    async fn from_request(req: Request, _state: &S) -> Result<Option<Self>, Self::Rejection> {
        check_content_type(&req)?;
        let nsid = input_nsid(&req);
        let body = read_body(req, JSON_LIMIT).await?;
        if body.iter().all(|b| b.is_ascii_whitespace()) {
            return Ok(None);
        }
        parse_input(nsid.as_deref(), &body).map(|v| Some(Json(v)))
    }
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

pub struct Query<T>(pub T);

/// A `limit` param checked against its lexicon range like the reference's
/// param validation (400 InvalidRequest when out of range), else `default`.
pub fn limit_param(v: Option<i64>, default: usize, min: i64, max: i64) -> Result<usize, XrpcError> {
    match v {
        None => Ok(default),
        Some(n) if n < min => Err(invalid(format!("Params/limit can not be less than {min}"))),
        Some(n) if n > max => Err(invalid(format!("Params/limit can not be greater than {max}"))),
        Some(n) => Ok(n as usize),
    }
}

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
/// com.atproto method that takes them, then the method's lexicon params.
fn validate_params(parts: &Parts) -> Result<(), XrpcError> {
    let Some(nsid) = parts.uri.path().strip_prefix("/xrpc/") else {
        return Ok(());
    };
    let schema = crate::lexicon::has_params(nsid);
    if !nsid.starts_with("com.atproto.") || (parts.uri.query().is_none() && !schema) {
        return Ok(());
    }
    let pairs: Vec<(String, String)> = match parts.uri.query() {
        None => Vec::new(),
        Some(_) => axum::extract::Query::<Vec<(String, String)>>::try_from_uri(&parts.uri)
            .map(|q| q.0)
            .map_err(|e| invalid(e.body_text()))?,
    };
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
    if schema {
        crate::lexicon::validate_params(nsid, &pairs).map_err(invalid)?;
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

// ---------------------------------------------------------------------------
// output validation (debug builds)
// ---------------------------------------------------------------------------

/// Marks a response body built by a handler's [`Json`] (not piped through
/// from another service), so [`debug_output_layer`] checks it.
#[cfg(debug_assertions)]
#[derive(Clone, Copy)]
struct HandlerJson;

/// Debug builds: validates 200 [`Json`] responses of bundled methods against
/// their output schema and turns a mismatch into a 500 (like the
/// reference's output verifier), so handler bugs fail the test suite.
/// Release builds: no layer at all.
pub fn debug_output_layer<S: Clone + Send + Sync + 'static>(
    r: axum::Router<S>,
) -> axum::Router<S> {
    #[cfg(debug_assertions)]
    let r = r.layer(axum::middleware::from_fn(check_output));
    r
}

#[cfg(debug_assertions)]
async fn check_output(req: Request, next: axum::middleware::Next) -> Response {
    let nsid = req.uri().path().strip_prefix("/xrpc/").map(String::from);
    let resp = next.run(req).await;
    let ours = resp.extensions().get::<HandlerJson>().is_some();
    let Some(nsid) = nsid.filter(|_| resp.status() == StatusCode::OK && ours) else {
        return resp;
    };
    let (parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => return XrpcError::internal(format!("reading response: {e}")).into_response(),
    };
    let checked = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|e| e.to_string())
        .and_then(|v| crate::lexicon::validate_output(&nsid, &v));
    if let Err(e) = checked {
        tracing::error!(nsid, "invalid handler output: {e}");
        return XrpcError::internal(format!("Invalid {nsid} output: {e}")).into_response();
    }
    Response::from_parts(parts, axum::body::Body::from(bytes))
}
