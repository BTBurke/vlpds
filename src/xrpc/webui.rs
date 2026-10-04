//! The embedded web UI (built from `ui/` into `ui/dist`; `build.rs` leaves a
//! placeholder when it isn't built) and `vlpds.admin.getClusterStatus`, the
//! view the operator console polls.

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
        .route("/docs", get(shell))
        .route("/docs/", get(shell))
        .route("/docs/{*rest}", get(shell))
        .route("/migrate", get(migrate_shell))
        .route("/migrate/", get(migrate_shell))
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

/// The migration page talks to the account's current PDS, which can be any
/// host; a dev server's (and a local e2e's) old PDS may be plain http.
const MIGRATE_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self' https:; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";
const MIGRATE_CSP_DEV: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data: blob:; connect-src 'self' https: http:; manifest-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";

async fn shell() -> Response {
    shell_with(SPA_CSP)
}

async fn migrate_shell(State(app): AppState) -> Response {
    shell_with(if app.config.dev_mode { MIGRATE_CSP_DEV } else { MIGRATE_CSP })
}

fn shell_with(csp: &'static str) -> Response {
    let Some(f) = Assets::get("index.html") else {
        return (StatusCode::NOT_FOUND, "UI not built: run `just ui`").into_response();
    };
    let mut r = ([(header::CONTENT_TYPE, "text/html; charset=utf-8"), (header::CACHE_CONTROL, "no-cache")], f.data).into_response();
    let h = r.headers_mut();
    h.insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(csp));
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
    let cache = if path.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "public, max-age=86400" };
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

/// Peers' own durable ordinal and lease state come from their
/// `/internal/v1/cluster`.
async fn cluster_status(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    super::admin::require_admin(&creds)?;
    let owned: Vec<crate::slots::ShardId> = app.partitions.owned().iter().map(|p| p.id).collect();
    let durable = app.log.durable_ordinal.load(Ordering::Acquire);
    let durable = (durable != u64::MAX).then_some(durable);
    let sources: Vec<J> = {
        let mut v: Vec<J> = app
            .firehose
            .sources
            .read()
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
    // in slot order
    let layout = c.layout();
    out["table"] = json!(layout.shards.iter().map(|r| c.owner_of(r.id).map(|(id, _)| id)).collect::<Vec<_>>());
    out["layout"] = json!({"version": layout.version, "shards": layout.shards, "op": layout.op});
    out["fencedLogs"] = json!(c.fenced_logs());
    let mut peers = c.peers();
    if !peers.iter().any(|l| l.node_id == me) {
        let mut own = c.own_lease();
        own.expires_ms = c.lease_expiry_us() / 1000;
        own.next_ordinal = app.log.next_ordinal();
        own.joined = c.joined();
        peers.push(own);
    }
    out["version"] = feature_levels(c, &peers).await;
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let fetches = peers.iter().map(|l| {
        let (app, l, me) = (app.clone(), l.clone(), me.clone());
        async move {
            let mut n = json!({
                "node": l.node_id, "log": l.log_id, "addr": l.addr,
                "writer": l.writer, "expiresMs": l.expires_ms, "self": l.node_id == me,
                "rev": l.rev, "minLevel": l.min_level, "maxLevel": l.max_level, "seenLevel": l.seen_level,
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
                .header(super::internal::HDR, &app.config.internal_token)
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

/// `finalizable`: the highest level every live node can run, when above the
/// active one. `finalizedAt`: when the active level was raised (older builds
/// can no longer join).
async fn feature_levels(c: &crate::cluster::Cluster, nodes: &[crate::cluster::NodeLease]) -> J {
    let (v, error) = match c.read_version().await {
        Ok(Some((v, _))) => (Some(v), None),
        Ok(None) => (None, Some(format!("{} is missing", crate::version::OBJECT))),
        Err(e) => (c.cluster_version(), Some(format!("{e:#}"))),
    };
    let revs: std::collections::BTreeSet<&str> = nodes.iter().map(|l| l.rev.as_str()).collect();
    let common_max = nodes.iter().map(|l| l.max_level).min();
    let active = v.as_ref().map(|v| v.active);
    let finalizable = common_max.filter(|m| active.is_some_and(|a| *m > a));
    let finalized_at = v.as_ref().and_then(|v| v.history.iter().rev().find(|h| h.level == v.active && v.history.len() > 1).map(|h| h.at.clone()));
    let mut out = json!({
        "active": active,
        "target": v.as_ref().and_then(|v| v.target),
        "history": v.as_ref().map(|v| v.history.clone()).unwrap_or_default(),
        "binary": {"min": c.cfg.levels.min, "max": c.cfg.levels.max, "rev": crate::version::build_rev()},
        "mixedBuilds": revs.len() > 1,
        "revs": revs,
        "finalizable": finalizable,
        "finalizedAt": finalized_at,
    });
    if let Some(e) = error {
        out["error"] = json!(e);
    }
    out
}
