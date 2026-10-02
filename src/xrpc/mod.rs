//! XRPC HTTP surface (axum). One module per lexicon namespace; each exposes
//! `routes()`. Shared state, errors and auth helpers live here.

mod admin;
mod admin_tools;
mod email2fa;
mod feature_level;
pub mod extract;
pub mod internal;
pub mod authn;
pub mod blobs;
mod identity;
#[doc(hidden)]
pub mod private_rows;
pub mod key_rotation;
pub mod oauth;
pub(crate) mod proxy;
mod ratelimits;
mod repo;
mod server;
mod sync;
pub mod syntax;
mod webui;
pub use blobs::spawn_blob_gc;
pub use server::{drop_revocation, reset_token_did, revocation_expired, spawn_reserved_key_gc, sweep_reserved_keys};
pub use sync::{request_crawl, set_export_buffer_max_mb};
pub use server::{set_mailer, LogMailer, Mail, Mailer};

/// Imports shared by every XRPC module (they `use super::*`).
#[allow(unused_imports)]
mod prelude {
    pub(crate) use crate::auth::Jwt;
    pub(crate) use crate::car;
    pub(crate) use crate::cbor::Value;
    pub(crate) use crate::cid::Cid;
    pub(crate) use crate::crypto::{self, Keypair};
    pub(crate) use crate::firehose::Firehose;
    pub(crate) use crate::metrics;
    pub(crate) use crate::partition::Partition;
    pub(crate) use crate::state::{self, Account, Head};
    pub(crate) use crate::stats::STATS;
    pub(crate) use crate::store::Store;
    pub(crate) use crate::tid::TidClock;
    pub(crate) use crate::worker::{
        CommitAck, CreateRepoReq, WorkerMsg, Workers, Write, WriteError, WriteOutcome, WriteReq,
    };
    pub(crate) use axum::body::{Body, Bytes as AxBytes};
    pub(crate) use axum::extract::{State, WebSocketUpgrade};
    // XRPC-envelope rejections (400 InvalidRequest / 413) instead of axum's.
    pub(crate) use super::extract::{Json, Query};
    pub(crate) use axum::http::{header, HeaderMap, StatusCode};
    pub(crate) use axum::response::{IntoResponse, Response};
    pub(crate) use axum::routing::{get, post};
    pub(crate) use axum::Router;
    pub(crate) use bytes::Bytes;
    pub(crate) use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
    pub(crate) use serde::Deserialize;
    pub(crate) use serde_json::{json, Value as J};
    pub(crate) use std::sync::atomic::Ordering;
    pub(crate) use std::sync::Arc;
    pub(crate) use std::time::Instant;
    pub(crate) use tokio::sync::oneshot;
}
pub(crate) use prelude::*;
pub struct App {
    pub jwt: Jwt,
    pub store: Store,
    pub workers: Workers,
    pub partitions: Arc<crate::partitions::PartitionTable>,
    pub firehose: Arc<Firehose>,
    pub tids: TidClock,
    pub public_url: String,
    pub handle_domain: String,
    /// Admission control: write requests beyond this many in flight get a fast 503.
    pub write_permits: tokio::sync::Semaphore,
    pub admin_token: String,
    pub config: Arc<crate::server::Config>,
    pub did_resolver: Arc<crate::did_resolver::DidResolver>,
    /// Cluster membership (None = single node owning every partition).
    pub cluster: Option<Arc<crate::cluster::Cluster>>,
    /// Internal node-to-node HTTP client (h2c, a few connections per peer;
    /// derefs to the next `reqwest::Client` round-robin).
    pub http: crate::http::PeerClient,
    /// This node's commit log (shared by its shards; peers stream it).
    pub log: Arc<crate::nodelog::NodeLog>,
    /// Shard host (graceful shutdown).
    pub node: Arc<crate::node::Node>,
    /// Rate limits: counters, the policy in force and its runtime config.
    pub ratelimit: Arc<crate::ratelimit::Limiter>,
    /// Key-encryption keys and the unwrapped signing-key cache (src/secrets.rs).
    pub secrets: Arc<crate::secrets::Secrets>,
    /// PLC registration (src/plc): the server rotation key and the
    /// directory. None = DIDs minted locally and never registered (dev only).
    pub plc: Option<Arc<crate::plc::Plc>>,
}

type AppState = State<Arc<App>>;

impl App {
    pub fn partition(&self, did: &str) -> Result<Arc<Partition>, XrpcError> {
        let p = self.partitions.shard_of(did);
        // not here (moving, or not reopened yet after a restart or takeover):
        // nothing was done, so the entry node resends writes (crate::forward)
        self.partitions.get(p).ok_or_else(|| XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: crate::forward::SHARD_MOVED.into(),
            message: format!("partition {p} is not owned by this node"),
        })
    }

    pub async fn resolve_handle(&self, handle: &str) -> Result<Option<String>, XrpcError> {
        let path =
            object_store::path::Path::from(format!("{}/handle/{}", self.store.prefix, handle));
        match self.store.raw.get(&path).await {
            Ok(r) => Ok(Some(
                String::from_utf8_lossy(&r.bytes().await.map_err(XrpcError::from_err)?).to_string(),
            )),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(XrpcError::from_err(e)),
        }
    }

    pub async fn resolve_repo(&self, repo: &str) -> Result<Arc<str>, XrpcError> {
        if repo.starts_with("did:") {
            return Ok(repo.into());
        }
        self.resolve_handle(&repo.to_ascii_lowercase())
            .await?
            .map(Into::into)
            .ok_or_else(|| XrpcError::bad("RepoNotFound", format!("could not find repo: {repo}")))
    }

    pub async fn head(&self, did: &str) -> Result<Head, XrpcError> {
        let p = self.partition(did)?;
        let v =
            p.db.get(state::head_key(did))
                .await
                .map_err(XrpcError::from_err)?;
        let v =
            v.ok_or_else(|| XrpcError::bad("RepoNotFound", format!("could not find repo: {did}")))?;
        Head::decode(&v).map_err(XrpcError::from_err)
    }

    pub async fn account(&self, did: &str) -> Result<Account, XrpcError> {
        let p = self.partition(did)?;
        let v =
            p.db.get(state::account_key(did))
                .await
                .map_err(XrpcError::from_err)?;
        let v = v.ok_or_else(|| XrpcError::bad("AccountNotFound", format!("no account {did}")))?;
        serde_json::from_slice(&v).map_err(XrpcError::from_err)
    }
}

pub struct XrpcError {
    pub status: StatusCode,
    pub error: String,
    pub message: String,
}

impl XrpcError {
    pub fn bad(error: &str, message: impl Into<String>) -> XrpcError {
        XrpcError {
            status: StatusCode::BAD_REQUEST,
            error: error.into(),
            message: message.into(),
        }
    }
    pub fn auth(message: &str) -> XrpcError {
        XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AuthenticationRequired".into(),
            message: message.into(),
        }
    }
    pub fn internal(message: impl Into<String>) -> XrpcError {
        XrpcError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: "InternalServerError".into(),
            message: message.into(),
        }
    }
    pub fn from_err(e: impl std::fmt::Display) -> XrpcError {
        XrpcError::internal(e.to_string())
    }
}

impl From<WriteError> for XrpcError {
    fn from(e: WriteError) -> XrpcError {
        match e {
            WriteError::RepoNotFound => XrpcError::bad("RepoNotFound", "repo not found"),
            WriteError::RepoInactive(status) => inactive_account_error(&status),
            WriteError::InvalidSwap(m) => XrpcError::bad("InvalidSwap", m),
            WriteError::Invalid(m) => XrpcError::bad("InvalidRequest", m),
            WriteError::Internal(m) => XrpcError::internal(m),
            // not applied (the shard left before the write started): the
            // entry node resends repo writes (crate::forward)
            WriteError::Unavailable(m) => XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: crate::forward::SHARD_MOVED.into(), message: m },
            // the repo's signing key couldn't be unwrapped (KMS down): nothing applied
            WriteError::KeyUnavailable(m) => XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: KEY_UNAVAILABLE.into(), message: m },
            // the signature failed verification twice: nothing applied
            WriteError::SignatureFault(m) => XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: SIGNATURE_FAULT.into(), message: m },
        }
    }
}

/// 503 error of a write whose signing key can't be unwrapped right now.
pub const KEY_UNAVAILABLE: &str = "KeyUnavailable";

/// 503 error of a signature that failed verification after signing
/// (src/crypto.rs): nothing was emitted, retry.
pub const SIGNATURE_FAULT: &str = "SignatureFault";

impl From<crate::crypto::SignatureFault> for XrpcError {
    fn from(e: crate::crypto::SignatureFault) -> XrpcError {
        XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: SIGNATURE_FAULT.into(), message: e.to_string() }
    }
}

impl From<crate::secrets::SecretError> for XrpcError {
    fn from(e: crate::secrets::SecretError) -> XrpcError {
        if e.retryable() {
            XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: KEY_UNAVAILABLE.into(), message: e.to_string() }
        } else {
            tracing::error!("secret unwrap failed: {e}");
            XrpcError::internal(e.to_string())
        }
    }
}

impl IntoResponse for XrpcError {
    fn into_response(self) -> Response {
        let unavailable = self.status == StatusCode::SERVICE_UNAVAILABLE;
        let mut r = (
            self.status,
            Json(json!({"error": self.error, "message": self.message})),
        )
            .into_response();
        // every 503 here is transient (shard moving, shedding, repo loading)
        if unavailable {
            r.headers_mut().insert(header::RETRY_AFTER, axum::http::HeaderValue::from_static("1"));
        }
        r
    }
}

type XResult<T> = Result<T, XrpcError>;

pub fn router(app: Arc<App>) -> Router {
    let r = Router::new()
        .route(
            "/xrpc/_health",
            get(|| async { Json(json!({"version": "vlpds"})) }),
        )
        .route("/metrics", get(|| async { metrics::render() }))
        // locally served XRPC methods; debug builds check their output schemas
        .merge(extract::debug_output_layer(
            Router::new()
                .merge(server::routes())
                .merge(identity::routes())
                .merge(repo::routes())
                .merge(sync::routes())
                .merge(blobs::routes())
                .merge(admin::routes())
                .merge(admin_tools::routes()),
        ))
        .merge(proxy::routes())
        .fallback(proxy::fallback)
        .merge(oauth::routes())
        .merge(internal::routes())
        .merge(crate::profiling::routes())
        .merge(ratelimits::routes())
        .merge(feature_level::routes())
        .merge(webui::routes());
    ratelimits::start(&app);
    // DPoP-Nonce / WWW-Authenticate on DPoP-authenticated requests
    let r = oauth::with_dpop_layer(r, &app);
    let r = if app.config.rate_limits_enabled {
        let limiter = app.ratelimit.clone();
        r.layer(axum::middleware::from_fn_with_state(limiter, crate::ratelimit::layer))
    } else {
        r
    };
    r.layer(axum::middleware::from_fn(incorrect_method))
        .layer(axum::middleware::from_fn(track_http))
        // request bodies: Content-Encoding gzip/deflate decoded (415 otherwise)
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
        // responses: gzip for JSON and CAR bodies over 1 KiB (reference
        // `compression()` with its CAR filter, packages/pds/src/util/compression.ts)
        .layer(tower_http::compression::CompressionLayer::new().compress_when(
            tower_http::compression::Predicate::and(
                tower_http::compression::predicate::SizeAbove::new(1024),
                JsonOrCar,
            ),
        ))
        .layer(axum::middleware::from_fn(cors))
        .with_state(app)
}

/// Compress only JSON and CAR bodies (reference `compression()` filter with
/// its CAR special case, packages/pds/src/util/compression.ts).
#[derive(Clone, Copy)]
struct JsonOrCar;

impl tower_http::compression::predicate::Predicate for JsonOrCar {
    fn should_compress<B>(&self, response: &axum::http::Response<B>) -> bool
    where
        B: axum::body::HttpBody,
    {
        let ct = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let ct = ct.split(';').next().unwrap_or("").trim();
        ct == "application/json" || ct == "application/vnd.ipld.car"
    }
}

/// Headers browser clients may read (DPoP, auth challenges, repo rev,
/// labelers, rate limits).
const CORS_EXPOSE: &str = "DPoP-Nonce, WWW-Authenticate, atproto-repo-rev, atproto-content-labelers, \
RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset, RateLimit-Policy, Retry-After";
/// Allowed request headers when a preflight doesn't name any.
const CORS_ALLOW_HEADERS: &str = "Authorization, Content-Type, DPoP, atproto-proxy, \
atproto-accept-labelers, atproto-content-labelers";

/// Server-wide CORS, like the reference's `cors({ maxAge: DAY / SECOND })`:
/// any origin; preflights (for routes without their own OPTIONS handler)
/// allow any method and mirror the requested headers; every response
/// exposes [`CORS_EXPOSE`] (added next to any value a handler set).
async fn cors(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    use header::HeaderValue;
    let preflight = req.method() == axum::http::Method::OPTIONS;
    let req_headers = req
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .cloned();
    // XRPC preflights (incl. proxied methods) are answered here; other
    // routes (OAuth endpoints) may have their own OPTIONS handlers.
    let mut resp = if preflight && req.uri().path().starts_with("/xrpc/") {
        StatusCode::NOT_FOUND.into_response()
    } else {
        next.run(req).await
    };
    if preflight
        && matches!(
            resp.status(),
            StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_FOUND
        )
    {
        resp = StatusCode::NO_CONTENT.into_response();
        let h = resp.headers_mut();
        h.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET,HEAD,PUT,PATCH,POST,DELETE"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            req_headers.unwrap_or(HeaderValue::from_static(CORS_ALLOW_HEADERS)),
        );
        h.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    }
    let h = resp.headers_mut();
    h.entry(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .or_insert(HeaderValue::from_static("*"));
    h.append(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(CORS_EXPOSE),
    );
    resp
}

/// A local XRPC route called with the wrong HTTP method: 400 InvalidRequest
/// as the reference's xrpc-server ("Incorrect HTTP method (POST) expected
/// GET") instead of axum's bare 405. Local routes are GET or POST only, so
/// the expected method is the other one (axum adds its Allow header outside
/// route layers, too late to read here). Preflights are left to [`cors`].
async fn incorrect_method(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    use axum::http::Method;
    let method = req.method().clone();
    let xrpc = req.uri().path().starts_with("/xrpc/");
    let resp = next.run(req).await;
    if !xrpc || method == Method::OPTIONS || resp.status() != StatusCode::METHOD_NOT_ALLOWED {
        return resp;
    }
    let message = match method {
        Method::POST => "Incorrect HTTP method (POST) expected GET".to_string(),
        Method::GET | Method::HEAD => format!("Incorrect HTTP method ({method}) expected POST"),
        _ => "XRPC requests only supports GET and POST".to_string(),
    };
    XrpcError::bad("InvalidRequest", message).into_response()
}

async fn track_http(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    // Label by matched route only: proxied/fallback paths are attacker-chosen,
    // so they share one label to keep metric cardinality bounded.
    let method = match req.extensions().get::<axum::extract::MatchedPath>() {
        Some(m) => metrics::method_label(m.as_str()).to_string(),
        None => "_proxy_or_unmatched".to_string(),
    };
    let start = Instant::now();
    metrics::HTTP_INFLIGHT.inc();
    let resp = next.run(req).await;
    metrics::HTTP_INFLIGHT.dec();
    metrics::HTTP_DURATION
        .with_label_values(&[&method])
        .observe(start.elapsed().as_secs_f64());
    metrics::HTTP_REQUESTS
        .with_label_values(&[&method, resp.status().as_str()])
        .inc();
    resp
}

#[allow(unused_imports)]
pub(crate) use authn::{authed_repo, Auth, Credentials, MaybeAuth};

/// Error for a write to an inactive account, as the reference's findAccount
/// with checkTakedown/checkDeactivated: 401 AccountTakedown (taken down or
/// suspended) or 401 AccountDeactivated; other statuses keep the
/// Repo{Status} name.
pub fn inactive_account_error(status: &str) -> XrpcError {
    match status {
        "takendown" | "suspended" => XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AccountTakedown".into(),
            message: "Account has been taken down".into(),
        },
        "deactivated" => XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AccountDeactivated".into(),
            message: "Account is deactivated".into(),
        },
        st => XrpcError::bad(&inactive_error(st), format!("repo is {st}")),
    }
}

/// XRPC error name for an inactive account status (RepoDeactivated, RepoTakendown, ...).
pub fn inactive_error(status: &str) -> String {
    let mut c = status.chars();
    match c.next() {
        Some(f) => format!("Repo{}{}", f.to_ascii_uppercase(), c.as_str()),
        None => "RepoInactive".into(),
    }
}

impl App {
    /// Base URL of the node owning `routing_key`'s partition, if that is not us.
    pub fn remote_owner(&self, routing_key: &str) -> Option<String> {
        let cluster = self.cluster.as_ref()?;
        let p = self.partitions.shard_of(routing_key);
        if self.partitions.get(p).is_some() {
            return None;
        }
        cluster.owner_of(p).filter(|(id, _)| *id != cluster.cfg.node_id).map(|(_, addr)| addr)
    }

    /// A fresh did:plc-shaped DID in a partition this node owns (cluster mode
    /// mints locally so account creation never needs forwarding).
    pub fn mint_local_did(&self) -> Result<String, XrpcError> {
        if self.cluster.is_none() {
            return Ok(crypto::random_plc_did());
        }
        for _ in 0..10_000 {
            let did = crypto::random_plc_did();
            if self.partitions.for_key(&did).is_some() {
                return Ok(did);
            }
        }
        Err(XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: "PartitionUnavailable".into(),
            message: "this node owns no partitions yet".into(),
        })
    }

    /// A new account's DID with PLC registration on: the genesis op (signed
    /// with the server rotation key; its hedged signature makes every
    /// attempt a new DID) re-signed until its DID lands in a partition this
    /// node owns, as [`mint_local_did`](Self::mint_local_did). (did, op).
    pub fn mint_plc_did(
        &self,
        plc: &crate::plc::Plc,
        signing_did_key: &str,
        handle: &str,
        recovery_key: Option<&str>,
    ) -> Result<(String, serde_json::Value), XrpcError> {
        for _ in 0..10_000 {
            let (did, op) = plc.genesis(signing_did_key, handle, &self.public_url, recovery_key)?;
            if self.cluster.is_none() || self.partitions.for_key(&did).is_some() {
                return Ok((did, op));
            }
        }
        Err(XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: "PartitionUnavailable".into(),
            message: "this node owns no partitions yet".into(),
        })
    }

    /// A repo's latest durable (head, MST) plus a SlateDB snapshot consistent
    /// with it, for exports and proofs. The MST comes from the repo worker's
    /// in-memory copy-on-write tree, so this is O(1) instead of an O(n) rebuild.
    pub async fn repo_view(
        &self,
        did: &str,
    ) -> Result<(Arc<crate::worker::DurableView>, Arc<slatedb::DbSnapshot>), XrpcError> {
        let (tx, rx) = oneshot::channel();
        self.workers
            .route(did)
            .send(WorkerMsg::Snapshot(crate::worker::SnapshotReq { did: did.into(), reply: tx }))
            .map_err(XrpcError::from_err)?;
        let cell = rx.await.map_err(|_| XrpcError::internal("worker dropped request"))??;
        let p = self.partition(did)?;
        let _g = p.apply_lock.read().await;
        let view = cell.read().clone();
        let snap = p.db.snapshot().await.map_err(XrpcError::from_err)?;
        Ok((view, snap))
    }

    /// Loads the account and fails unless it is active, with the
    /// reference findAccount errors ([`inactive_account_error`]).
    pub async fn ensure_active(&self, did: &str) -> Result<Account, XrpcError> {
        let a = self
            .account(did)
            .await
            .map_err(|_| XrpcError::bad("RepoNotFound", format!("could not find repo: {did}")))?;
        match &a.status {
            Some(st) => Err(inactive_account_error(st)),
            None => Ok(a),
        }
    }

    /// Durably writes private (non-repo) per-account state through the
    /// partition log (replayed on recovery), without firehose events.
    pub async fn put_private(
        &self,
        did: &str,
        muts: Vec<crate::segment::Mutation>,
    ) -> Result<(), XrpcError> {
        if let Some(owner) = self.remote_owner(did) {
            return internal::forward_put_private(self, &owner, did, muts).await;
        }
        let p = self.partition(did)?;
        let (tx, rx) = oneshot::channel();
        let entry = crate::partition::LogEntry {
            shard: p.id,
            frames: Vec::new(),
            muts,
            ack: Some(Box::new(move |r| {
                let _ = tx.send(r);
            })),
            pending: None,
            enqueued: Instant::now(),
        };
        p.tx.send(entry)
            .await
            .map_err(|_| XrpcError::internal("partition sequencer gone"))?;
        rx.await
            .map_err(|_| XrpcError::internal("log dropped write"))?
            .map_err(|e| XrpcError::internal(e.to_string()))
    }

    pub async fn get_private(&self, did: &str, name: &str) -> Result<Option<Bytes>, XrpcError> {
        if let Some(owner) = self.remote_owner(did) {
            return internal::forward_get_private(self, &owner, did, name).await;
        }
        let p = self.partition(did)?;
        p.db.get(state::private_key(did, name))
            .await
            .map_err(XrpcError::from_err)
    }

    /// Applies an account-level change through the repo's worker, ordered with
    /// its commits.
    pub async fn account_op(
        &self,
        did: &str,
        op: crate::worker::AccountOp,
    ) -> Result<Head, XrpcError> {
        let (tx, rx) = oneshot::channel();
        self.workers
            .route(did)
            .send(WorkerMsg::Account(crate::worker::AccountReq {
                did: did.into(),
                op,
                reply: tx,
            }))
            .map_err(XrpcError::from_err)?;
        Ok(rx
            .await
            .map_err(|_| XrpcError::internal("worker dropped request"))??)
    }

    /// Read-modify-write of an account, run by the repo's worker on its
    /// current state (never on a snapshot read here, which a concurrent change
    /// could have outdated). `f` checks its preconditions on that state and
    /// returns whether anything changed (false = no write, no events).
    /// `activate` sends it as AccountOp::Activate. Returns (before, after).
    pub async fn mutate_account<F>(
        &self,
        did: &str,
        identity_event: bool,
        account_event: bool,
        activate: bool,
        f: F,
    ) -> Result<(Account, Account), XrpcError>
    where
        F: FnOnce(&mut Account) -> Result<bool, XrpcError> + Send + 'static,
    {
        // f's own error and the accounts come back through `out`; the worker
        // only learns that the op was rejected
        let (out_tx, mut out_rx) = oneshot::channel();
        let mutate: crate::worker::AccountMutation = Box::new(move |a: &mut Account| {
            let before = a.clone();
            match f(a) {
                Ok(changed) => {
                    let _ = out_tx.send(Ok((before, a.clone())));
                    Ok(changed)
                }
                Err(e) => {
                    let msg = e.message.clone();
                    let _ = out_tx.send(Err(e));
                    Err(WriteError::Invalid(msg))
                }
            }
        });
        let op = if activate {
            crate::worker::AccountOp::Activate { mutate }
        } else {
            crate::worker::AccountOp::Update { mutate, identity_event, account_event }
        };
        let res = self.account_op(did, op).await;
        match (out_rx.try_recv(), res) {
            (Ok(Err(e)), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(Ok(accts)), Ok(_)) => Ok(accts),
            (Err(_), Ok(_)) => Err(XrpcError::internal("account mutation did not run")),
        }
    }
}

/// Blob CIDs referenced by a record ({"$type": "blob", "ref": {"$link": ...}}).
pub fn blob_refs(v: &Value, out: &mut Vec<Cid>) {
    match v {
        Value::Map(m) => {
            if v.get("$type").and_then(|t| t.as_str()) == Some("blob") {
                if let Some(Value::Link(c)) = v.get("ref") {
                    if !out.contains(c) {
                        out.push(*c);
                    }
                }
            }
            for (_, child) in m {
                blob_refs(child, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|c| blob_refs(c, out)),
        _ => {}
    }
}
