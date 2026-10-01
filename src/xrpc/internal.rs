//! Node-to-node endpoints (cluster mode). Authenticated with the shared
//! internal token (`Config::internal_token`, not the admin token) in
//! `x-vlpds-internal`; never exposed publicly in production.

use super::*;
use crate::segment::Mutation;
use base64::Engine;

const HDR: &str = "x-vlpds-internal";
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/internal/v1/log/stream", get(stream))
        .route("/internal/v1/private/put", post(put_private))
        .route("/internal/v1/private/get", get(get_private))
        .route("/internal/v1/cluster", get(cluster_status))
        .route("/internal/v1/admin/searchAccounts", get(admin_search_accounts))
        .route("/internal/v1/admin/inviteCodes", get(admin_invite_codes))
}

/// Cluster view of this node (HA tests / ops): shards it owns, the routing
/// table it forwards by, its log, peers, and where its firehose merger stands.
async fn cluster_status(State(app): AppState, headers: HeaderMap) -> XResult<Json<J>> {
    check(&app, &headers)?;
    let owned: Vec<u16> = app.partitions.owned().iter().map(|p| p.id).collect();
    let (node, table, peers, lease_valid) = match &app.cluster {
        Some(c) => (
            c.cfg.node_id.clone(),
            (0..c.cfg.shards).map(|p| c.owner_of(p).map(|(id, _)| id)).collect::<Vec<_>>(),
            c.peers().into_iter().map(|l| json!({"node": l.node_id, "log": l.log_id, "addr": l.addr})).collect::<Vec<_>>(),
            c.lease_valid(),
        ),
        None => (String::new(), Vec::new(), Vec::new(), false),
    };
    Ok(Json(json!({
        "node": node,
        "log": app.log.log_id.to_string(),
        "log_durable_ordinal": app.log.durable_ordinal.load(std::sync::atomic::Ordering::Acquire),
        "owned": owned,
        "table": table,
        "peers": peers,
        "lease_valid": lease_valid,
        "firehose_last_emitted": app.firehose.last_emitted.load(std::sync::atomic::Ordering::Acquire),
        "firehose_min_watermark": app.firehose.min_watermark(),
    })))
}

fn check(app: &App, headers: &HeaderMap) -> XResult<()> {
    let t = headers.get(HDR).and_then(|v| v.to_str().ok()).unwrap_or("");
    if internal_token_ok(&app.config, t) {
        Ok(())
    } else {
        Err(XrpcError::auth("internal endpoint"))
    }
}

/// Whether `t` is the node-to-node token. Dev mode also accepts the admin
/// token, for senders not yet switched to the internal token.
pub fn internal_token_ok(cfg: &crate::server::Config, t: &str) -> bool {
    crate::auth::token_eq(&cfg.internal_token, t)
        || (cfg.dev_mode && crate::auth::token_eq(&cfg.admin_token, t))
}

/// Streams this node's log (durable batches + watermark heartbeats) to a peer.
async fn stream(State(app): AppState, headers: HeaderMap, ws: WebSocketUpgrade) -> XResult<Response> {
    check(&app, &headers)?;
    let log = app.log.clone();
    Ok(ws.on_upgrade(move |socket| crate::remote::serve_stream(socket, log)))
}

#[derive(serde::Serialize, Deserialize)]
struct PutIn {
    routing: String,
    muts: Vec<(String, Option<String>)>,
}

// node-to-node: axum's 2 MB body limit, not the 150 KiB XRPC one
async fn put_private(State(app): AppState, headers: HeaderMap, axum::Json(inp): axum::Json<PutIn>) -> XResult<Json<J>> {
    check(&app, &headers)?;
    let muts = inp
        .muts
        .into_iter()
        .map(|(k, v)| {
            let key = B64.decode(k).map_err(XrpcError::from_err)?;
            // only the routing key's own private state (p/{routing}\0...),
            // never repo data, heads or another account's state
            if !key.starts_with(&state::private_prefix(&inp.routing)) {
                return Err(XrpcError::bad("InvalidRequest", "key outside the routing key's private state"));
            }
            Ok(Mutation {
                key: key.into(),
                val: v.map(|v| B64.decode(v).map(Bytes::from)).transpose().map_err(XrpcError::from_err)?,
            })
        })
        .collect::<XResult<Vec<_>>>()?;
    // must be local now (no forwarding loops)
    app.partition(&inp.routing)?;
    app.put_private(&inp.routing, muts).await?;
    Ok(Json(json!({})))
}

#[derive(Deserialize)]
struct GetQ {
    routing: String,
    name: String,
}

async fn get_private(State(app): AppState, headers: HeaderMap, Query(q): Query<GetQ>) -> XResult<Json<J>> {
    check(&app, &headers)?;
    app.partition(&q.routing)?;
    let v = app.get_private(&q.routing, &q.name).await?;
    Ok(Json(json!({"value": v.map(|b| B64.encode(b))})))
}

fn upstream(e: impl std::fmt::Display) -> XrpcError {
    XrpcError { status: StatusCode::SERVICE_UNAVAILABLE, error: "PartitionUnavailable".into(), message: format!("partition owner: {e}") }
}

pub async fn forward_put_private(app: &App, owner: &str, routing: &str, muts: Vec<Mutation>) -> XResult<()> {
    let body = PutIn {
        routing: routing.to_string(),
        muts: muts.into_iter().map(|m| (B64.encode(&m.key), m.val.map(|v| B64.encode(v)))).collect(),
    };
    let r = app
        .http
        .post(format!("{owner}/internal/v1/private/put"))
        .header(HDR, &app.config.internal_token)
        .json(&body)
        .send()
        .await
        .map_err(upstream)?;
    if !r.status().is_success() {
        return Err(upstream(format!("{}: {}", r.status(), r.text().await.unwrap_or_default())));
    }
    Ok(())
}

pub async fn forward_get_private(app: &App, owner: &str, routing: &str, name: &str) -> XResult<Option<Bytes>> {
    let r = app
        .http
        .get(format!("{owner}/internal/v1/private/get"))
        .header(HDR, &app.config.internal_token)
        .query(&[("routing", routing), ("name", name)])
        .send()
        .await
        .map_err(upstream)?;
    if !r.status().is_success() {
        return Err(upstream(format!("{}: {}", r.status(), r.text().await.unwrap_or_default())));
    }
    let v: J = r.json().await.map_err(upstream)?;
    match v["value"].as_str() {
        Some(s) => Ok(Some(B64.decode(s).map_err(upstream)?.into())),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// admin scatter-gather (cluster-wide listings; see admin.rs)
// ---------------------------------------------------------------------------

/// Per-peer deadline for a scatter-gather leg: a slow or dead peer costs the
/// admin call at most this, and is reported as unreachable.
const GATHER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// The local half of searchAccounts on this node's shards, for a peer's merge.
async fn admin_search_accounts(
    State(app): AppState,
    headers: HeaderMap,
    Query(q): Query<super::admin::SearchQ>,
) -> XResult<Json<J>> {
    check(&app, &headers)?;
    let (hits, owned) = super::admin::search_accounts_local(&app, &q).await?;
    Ok(Json(json!({"owned": owned, "accounts": hits})))
}

/// The local half of getInviteCodes on this node's shards, for a peer's merge.
async fn admin_invite_codes(
    State(app): AppState,
    headers: HeaderMap,
    Query(q): Query<super::admin::InviteCodesQ>,
) -> XResult<Json<J>> {
    check(&app, &headers)?;
    let (codes, owned) = super::admin::invite_codes_local(&app, &q).await?;
    Ok(Json(json!({"owned": owned, "codes": codes})))
}

pub struct PeerReply {
    pub node: String,
    /// Shards the peer scanned (owned at the time).
    pub owned: Vec<u16>,
    pub body: J,
}

#[derive(Default)]
pub struct Gathered {
    pub replies: Vec<PeerReply>,
    /// Node ids of live peers that failed or timed out.
    pub unreachable: Vec<String>,
}

/// GETs `path?query` on every live peer (not this node) concurrently, each
/// bounded by [`GATHER_TIMEOUT`]. No peers (a single node) = nothing to do.
pub async fn gather(app: &App, path: &str, query: &[(&str, String)]) -> Gathered {
    let Some(c) = &app.cluster else {
        return Gathered::default();
    };
    let me = c.cfg.node_id.clone();
    let mut peers: Vec<_> = c.peers().into_iter().filter(|l| l.node_id != me).collect();
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let legs = peers.into_iter().map(|l| async move {
        let r = async {
            let r = app
                .http
                .get(format!("{}{path}", l.addr.trim_end_matches('/')))
                .header(HDR, &app.config.internal_token)
                .query(query)
                .timeout(GATHER_TIMEOUT)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if !r.status().is_success() {
                return Err(format!("{}: {}", r.status(), r.text().await.unwrap_or_default()));
            }
            r.json::<J>().await.map_err(|e| e.to_string())
        }
        .await;
        (l.node_id, r)
    });
    let mut out = Gathered::default();
    for (node, r) in futures::future::join_all(legs).await {
        match r {
            Ok(body) => {
                let owned = serde_json::from_value(body["owned"].clone()).unwrap_or_default();
                out.replies.push(PeerReply { node, owned, body });
            }
            Err(e) => {
                tracing::warn!(peer = %node, path, "admin scatter-gather: peer unreachable: {e}");
                out.unreachable.push(node);
            }
        }
    }
    out
}
