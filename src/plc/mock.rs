//! An in-process PLC directory for tests and local runs: accepts operations
//! with the directory's own checks (`assert_valid_incoming`, then
//! [`PlcLog::apply`]: genesis hash, signature chain, recovery forks,
//! tombstones) and serves `GET /{did}`, `/{did}/data`, `/{did}/log`,
//! `/{did}/log/audit`, `/{did}/log/last`. Not the real thing: no rate
//! limits, no export, state in memory. Failures can be injected.

use super::{assert_valid_incoming, format_did_doc, PlcLog};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use parking_lot::Mutex;
use serde_json::{json, Value as J};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Inner {
    logs: Mutex<HashMap<String, PlcLog>>,
    /// POSTs answered with this status (count, status) before validation.
    fail_posts: Mutex<(u32, u16)>,
    /// Every request answered 503.
    down: AtomicBool,
    posts: AtomicU64,
    accepted: AtomicU64,
}

/// A running mock directory (served until the process exits).
#[derive(Clone)]
pub struct MockPlc {
    pub url: String,
    inner: Arc<Inner>,
}

fn err(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"message": message.into()}))).into_response()
}

impl MockPlc {
    /// Binds 127.0.0.1:0 and serves in a background task.
    pub async fn start() -> MockPlc {
        let inner = Arc::new(Inner::default());
        let app = axum::Router::new()
            .route("/{did}", get(doc).post(post_op))
            .route("/{did}/data", get(data))
            .route("/{did}/log", get(log))
            .route("/{did}/log/audit", get(audit))
            .route("/{did}/log/last", get(last))
            .with_state(inner.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock PLC");
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
        MockPlc { url, inner }
    }

    /// The next `n` POSTs fail with `status` (and change nothing).
    pub fn fail_posts(&self, n: u32, status: u16) {
        *self.inner.fail_posts.lock() = (n, status);
    }

    /// While set, every request is answered 503.
    pub fn set_down(&self, down: bool) {
        self.inner.down.store(down, Ordering::SeqCst);
    }

    /// POSTs received / accepted.
    pub fn posts(&self) -> u64 {
        self.inner.posts.load(Ordering::SeqCst)
    }

    pub fn accepted(&self) -> u64 {
        self.inner.accepted.load(Ordering::SeqCst)
    }

    /// The DID's accepted ops, oldest first (nullified ones included).
    pub fn ops(&self, did: &str) -> Vec<J> {
        self.inner.logs.lock().get(did).map(|l| l.entries.iter().map(|e| e.op.clone()).collect()).unwrap_or_default()
    }

    pub fn last_op(&self, did: &str) -> Option<J> {
        self.inner.logs.lock().get(did).and_then(|l| l.last().map(|e| e.op.clone()))
    }

    /// `/data` of the DID (None: unknown or tombstoned).
    pub fn data(&self, did: &str) -> Option<J> {
        self.inner.logs.lock().get(did).and_then(PlcLog::data)
    }

    pub fn dids(&self) -> Vec<String> {
        self.inner.logs.lock().keys().cloned().collect()
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[allow(clippy::result_large_err)]
fn known(s: &Inner, did: &str) -> Result<PlcLog, Response> {
    if s.down.load(Ordering::SeqCst) {
        return Err(err(StatusCode::SERVICE_UNAVAILABLE, "mock PLC down"));
    }
    match s.logs.lock().get(did) {
        Some(l) if !l.entries.is_empty() => Ok(l.clone()),
        _ => Err(err(StatusCode::NOT_FOUND, format!("DID not registered: {did}"))),
    }
}

async fn doc(State(s): State<Arc<Inner>>, Path(did): Path<String>) -> Response {
    match known(&s, &did) {
        Ok(l) => match l.data() {
            Some(d) => ([(axum::http::header::CONTENT_TYPE, "application/did+ld+json")], format_did_doc(&d).to_string()).into_response(),
            None => err(StatusCode::NOT_FOUND, format!("DID not available: {did}")),
        },
        Err(r) => r,
    }
}

async fn data(State(s): State<Arc<Inner>>, Path(did): Path<String>) -> Response {
    match known(&s, &did) {
        Ok(l) => match l.data() {
            Some(d) => Json(d).into_response(),
            None => err(StatusCode::NOT_FOUND, format!("DID not available: {did}")),
        },
        Err(r) => r,
    }
}

async fn log(State(s): State<Arc<Inner>>, Path(did): Path<String>) -> Response {
    match known(&s, &did) {
        Ok(l) => Json(l.entries.iter().filter(|e| !e.nullified).map(|e| e.op.clone()).collect::<Vec<_>>()).into_response(),
        Err(r) => r,
    }
}

async fn audit(State(s): State<Arc<Inner>>, Path(did): Path<String>) -> Response {
    match known(&s, &did) {
        Ok(l) => Json(
            l.entries
                .iter()
                .map(|e| {
                    let at = chrono::DateTime::from_timestamp_millis(e.created_at_ms).unwrap_or_default();
                    json!({"did": did, "operation": e.op, "cid": e.cid, "nullified": e.nullified,
                        "createdAt": at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)})
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(r) => r,
    }
}

async fn last(State(s): State<Arc<Inner>>, Path(did): Path<String>) -> Response {
    match known(&s, &did) {
        Ok(l) => Json(l.last().map(|e| e.op.clone()).unwrap_or(J::Null)).into_response(),
        Err(r) => r,
    }
}

async fn post_op(State(s): State<Arc<Inner>>, Path(did): Path<String>, body: axum::body::Bytes) -> Response {
    s.posts.fetch_add(1, Ordering::SeqCst);
    if s.down.load(Ordering::SeqCst) {
        return err(StatusCode::SERVICE_UNAVAILABLE, "mock PLC down");
    }
    {
        let mut f = s.fail_posts.lock();
        if f.0 > 0 {
            f.0 -= 1;
            return err(StatusCode::from_u16(f.1).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), "injected failure");
        }
    }
    if !super::valid_plc_did(&did) {
        return err(StatusCode::BAD_REQUEST, format!("Invalid DID: {did}"));
    }
    let op: J = match serde_json::from_slice(&body) {
        Ok(o) => o,
        Err(e) => return err(StatusCode::BAD_REQUEST, format!("bad JSON: {e}")),
    };
    if let Err(e) = assert_valid_incoming(&op) {
        return err(StatusCode::BAD_REQUEST, e.to_string());
    }
    let mut logs = s.logs.lock();
    let mut l = logs.get(&did).cloned().unwrap_or_else(|| PlcLog::new(&did));
    // strictly increasing timestamps, as the directory's clock would give
    let at = now_ms().max(l.entries.last().map_or(0, |e| e.created_at_ms + 1));
    match l.apply(op, at) {
        Ok(()) => {
            logs.insert(did, l);
            s.accepted.fetch_add(1, Ordering::SeqCst);
            // the directory answers `res.sendStatus(200)`: text/plain "OK"
            ([(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")], "OK").into_response()
        }
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::Keypair;
    use crate::plc::Plc;

    #[tokio::test]
    async fn mock_directory_round_trip() {
        let m = MockPlc::start().await;
        let plc = Plc::new(&m.url, Arc::new(Keypair::generate()), None);
        let signing = Keypair::generate().did_key();
        let (did, op) = plc.genesis(&signing, "alice.test", "https://pds.example", None).unwrap();
        // a genesis op posted under another DID is refused
        let other = crate::crypto::random_plc_did();
        assert!(matches!(plc.client.send(&other, &op, "create").await, Err(crate::plc::PlcError::Rejected { status: 400, .. })));
        plc.create(&did, &op).await.unwrap();
        assert_eq!(m.last_op(&did).unwrap(), op);
        assert!(plc.update_handle(&did, "bob.test").await.unwrap());
        assert!(!plc.update_handle(&did, "bob.test").await.unwrap(), "unchanged: nothing submitted");
        let new_key = Keypair::generate().did_key();
        assert!(plc.update_signing_key(&did, &new_key).await.unwrap());
        let d = plc.client.document_data(&did).await.unwrap();
        assert_eq!(d["alsoKnownAs"], json!(["at://bob.test"]));
        assert_eq!(d["verificationMethods"]["atproto"], json!(new_key));
        assert_eq!(m.ops(&did).len(), 3);
        // injected failures and outages
        m.fail_posts(1, 500);
        assert!(matches!(plc.update_handle(&did, "carol.test").await, Err(crate::plc::PlcError::Unavailable(_))));
        m.set_down(true);
        assert!(matches!(plc.client.last_op(&did).await, Err(crate::plc::PlcError::Unavailable(_))));
        m.set_down(false);
        assert!(matches!(plc.client.last_op(&other).await, Err(crate::plc::PlcError::NotFound(_))));
        plc.tombstone(&did).await.unwrap();
        assert!(m.data(&did).is_none());
        assert!(matches!(plc.update_handle(&did, "dave.test").await, Err(crate::plc::PlcError::Tombstoned)));
        // a doc served like the directory's
        let doc: J = reqwest::get(format!("{}/{did}", m.url)).await.unwrap().json().await.unwrap();
        assert_eq!(doc["message"], json!(format!("DID not available: {did}")));
    }
}
