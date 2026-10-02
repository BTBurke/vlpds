//! The embedded web UI (React + Vite, built from `ui/` into `ui/dist`):
//! public landing `/`, the account app `/account/*` and the operator console
//! `/admin/*`, all client-side routed and served as one `index.html`, plus
//! hashed assets and the bundled fonts. Also `vlpds.admin.getClusterStatus`,
//! the admin-authenticated (Basic) view of `/internal/v1/cluster` the console
//! polls, with peers' own status fetched server-side.
//!
//! When the UI hasn't been built, `build.rs` leaves a placeholder page in
//! `ui/dist`, so the binary always builds.

use super::*;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "ui/dist"]
struct Assets;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/", get(shell))
        .route("/account", get(shell))
        .route("/account/", get(shell))
        .route("/account/{*rest}", get(shell))
        .route("/admin", get(shell))
        .route("/admin/", get(shell))
        .route("/admin/{*rest}", get(shell))
        .route("/assets/{*path}", get(asset))
        .route("/fonts/{*path}", get(asset))
        .route("/favicon.svg", get(asset))
        .route("/xrpc/vlpds.admin.getClusterStatus", get(cluster_status))
}

/// Same-origin only: scripts and styles come from the bundle, API calls go
/// to this server. Inline style *attributes* set through the CSSOM (React
/// `style`, uPlot) are not governed by style-src.
const SPA_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self'; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";

async fn shell() -> Response {
    let Some(f) = Assets::get("index.html") else {
        return (StatusCode::NOT_FOUND, "UI not built: run `just ui`").into_response();
    };
    let mut r = (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        f.data,
    )
        .into_response();
    let h = r.headers_mut();
    h.insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(SPA_CSP));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, header::HeaderValue::from_static("nosniff"));
    h.insert(header::REFERRER_POLICY, header::HeaderValue::from_static("no-referrer"));
    h.insert(header::X_FRAME_OPTIONS, header::HeaderValue::from_static("DENY"));
    r
}

async fn asset(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let Some(f) = Assets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // hashed bundle files never change; fonts and the icon keep stable names
    let cache = if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "public, max-age=86400"
    };
    let mime = f.metadata.mimetype().to_string();
    (
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, cache.to_string()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
        ],
        f.data,
    )
        .into_response()
}

/// The cluster as this node sees it, for the operator console. Admin only.
/// Peers' own durable ordinal and lease state come from their
/// `/internal/v1/cluster` (shared admin token), fetched concurrently with a
/// short timeout; an unreachable peer is reported with `"reachable": false`.
async fn cluster_status(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    if !matches!(creds, Credentials::Admin) {
        return Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AuthenticationRequired".into(),
            message: "admin credentials required".into(),
        });
    }
    let owned: Vec<crate::slots::ShardId> = app.partitions.owned().iter().map(|p| p.id).collect();
    let durable = app.log.durable_ordinal.load(Ordering::Acquire);
    let durable = (durable != u64::MAX).then_some(durable);
    let sources: Vec<J> = {
        let s = app.firehose.sources.read();
        let mut v: Vec<J> = s
            .iter()
            .map(|(log, src)| {
                let (wm, local) = match src {
                    crate::firehose::Source::Local(w) => (w.get(), true),
                    crate::firehose::Source::Remote(a) => (a.load(Ordering::Acquire), false),
                };
                json!({"log": log.to_string(), "watermark": wm.to_string(), "local": local})
            })
            .collect();
        v.sort_by(|a, b| a["log"].as_str().cmp(&b["log"].as_str()));
        v
    };
    let mut out = json!({
        "node": "",
        "log": app.log.log_id.to_string(),
        "logDurableOrdinal": durable,
        "owned": owned,
        "shards": app.partitions.len(),
        "table": [],
        "nodes": [],
        "leaseValid": false,
        "firehose": {
            // seqs are unix_micros × 256 + writer: beyond JS's 2^53, so strings
            "lastEmitted": app.firehose.last_emitted.load(Ordering::Acquire).to_string(),
            "minWatermark": app.firehose.min_watermark().map(|w| w.to_string()),
            "sources": sources,
        },
        "fencedLogs": {},
        "time": crate::tid::now_micros() / 1000,
    });
    let Some(c) = &app.cluster else {
        return Ok(Json(out));
    };
    let me = c.cfg.node_id.clone();
    out["node"] = json!(me);
    out["leaseValid"] = json!(c.lease_valid());
    out["leaseExpiresMs"] = json!(c.lease_expiry_us() / 1000);
    // in slot order (shard ids are stable names since splits and merges)
    let layout = c.layout();
    out["table"] = json!(layout.shards.iter().map(|r| c.owner_of(r.id).map(|(id, _)| id)).collect::<Vec<_>>());
    out["layout"] = json!({"version": layout.version, "shards": layout.shards, "op": layout.op});
    out["fencedLogs"] = json!(c.fenced_logs());
    let mut peers = c.peers();
    if !peers.iter().any(|l| l.node_id == me) {
        peers.push(crate::cluster::NodeLease {
            node_id: me.clone(),
            log_id: app.log.log_id.to_string(),
            addr: c.cfg.addr.clone(),
            writer: c.writer,
            expires_ms: c.lease_expiry_us() / 1000,
            renewals: 0,
            next_ordinal: app.log.next_ordinal(),
            draining: false,
            joined: c.joined(),
            follows: Default::default(),
            wm_cap: 0,
        });
    }
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let fetches = peers.iter().map(|l| {
        let (app, l, me) = (app.clone(), l.clone(), me.clone());
        async move {
            let mut n = json!({
                "node": l.node_id, "log": l.log_id, "addr": l.addr,
                "writer": l.writer, "expiresMs": l.expires_ms, "self": l.node_id == me,
            });
            if l.node_id == me {
                n["reachable"] = json!(true);
                n["leaseValid"] = json!(app.cluster.as_ref().is_some_and(|c| c.lease_valid()));
                n["logDurableOrdinal"] = json!(durable);
                n["owned"] = json!(app.partitions.owned().len());
                return n;
            }
            let r = app
                .http
                .get(format!("{}/internal/v1/cluster", l.addr.trim_end_matches('/')))
                .header("x-vlpds-internal", &app.config.internal_token)
                .timeout(std::time::Duration::from_millis(1500))
                .send()
                .await;
            match r {
                Ok(r) if r.status().is_success() => {
                    let v: J = r.json().await.unwrap_or(J::Null);
                    n["reachable"] = json!(true);
                    n["leaseValid"] = v["lease_valid"].clone();
                    let o = v["log_durable_ordinal"].as_u64().filter(|o| *o != u64::MAX);
                    n["logDurableOrdinal"] = json!(o);
                    n["owned"] = json!(v["owned"].as_array().map(|a| a.len()).unwrap_or(0));
                }
                _ => n["reachable"] = json!(false),
            }
            n
        }
    });
    out["nodes"] = json!(futures::future::join_all(fetches).await);
    Ok(Json(out))
}
