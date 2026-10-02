//! Rate-limit observability and runtime configuration (src/ratelimit.rs,
//! DESIGN.md "Rate limits: observability and runtime config"):
//!
//! - `GET vlpds.admin.getRateLimits?top=N&local=bool` (admin): the limits in
//!   force, the stored config with its history, and per node and cluster-wide
//!   the heaviest keys per bucket and recent 429s by bucket and route. Peers
//!   are asked over `/internal/v1/ratelimits` with a short deadline; a peer
//!   that doesn't answer is listed in `unreachableNodes`.
//! - `POST vlpds.admin.updateRateLimits` (admin) `{config, ifVersion,
//!   actor?, note?}`: validates and stores the next config version (CAS),
//!   installs it here, then nudges every peer to reload and reports which
//!   version each one now runs.
//! - `/internal/v1/ratelimits` (GET: this node's snapshot) and
//!   `/internal/v1/ratelimits/reload` (POST: re-read the object now).

use super::authn::Credentials;
use super::*;
use crate::ratelimit::config::Doc;
use crate::ratelimit::runtime::{self, SaveError, SaveReq};
use crate::ratelimit::{Consumer, NodeSnapshot, RejectionCount};
use std::collections::BTreeMap;
use std::time::Duration;

const INTERNAL_HDR: &str = "x-vlpds-internal";
/// Deadline for a peer's reload after a change.
const RELOAD_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_TOP: usize = 10;
const MAX_TOP: usize = 50;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/vlpds.admin.getRateLimits", get(get_rate_limits))
        .route("/xrpc/vlpds.admin.updateRateLimits", post(update_rate_limits))
        .route("/internal/v1/ratelimits", get(internal_snapshot))
        .route("/internal/v1/ratelimits/reload", post(internal_reload))
}

/// Starts the config refresher (called when the router is built;
/// idempotent). It runs under `--no-rate-limits` too, so the console shows
/// and edits the cluster's config from any node: one conditional GET per
/// [`runtime::REFRESH_EVERY`].
pub fn start(app: &Arc<App>) {
    runtime::spawn_refresher(&app.ratelimit, app.store.clone());
}

fn require_admin(creds: &Credentials) -> XResult<()> {
    match creds {
        Credentials::Admin => Ok(()),
        _ => Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AuthenticationRequired".into(),
            message: "admin credentials required".into(),
        }),
    }
}

fn internal_ok(app: &App, headers: &HeaderMap) -> XResult<()> {
    let t = headers.get(INTERNAL_HDR).and_then(|v| v.to_str().ok()).unwrap_or("");
    if internal::internal_token_ok(&app.config, t) {
        Ok(())
    } else {
        Err(XrpcError::auth("internal endpoint"))
    }
}

fn node_id(app: &App) -> String {
    app.cluster.as_ref().map(|c| c.cfg.node_id.clone()).unwrap_or_else(|| "single".into())
}

#[derive(Deserialize)]
struct TopQ {
    top: Option<usize>,
    #[serde(default)]
    local: bool,
}

/// One bucket as in force on this node, with its default.
fn limiter_rows(p: &crate::ratelimit::Policy) -> Vec<J> {
    p.specs()
        .map(|s| {
            let def = crate::ratelimit::BUILTIN.iter().find(|l| *l.name == *s.name);
            json!({
                "name": &*s.name,
                "key": s.key,
                "scope": &*s.scope,
                "windowSecs": s.window_ms / 1000,
                "points": s.points,
                "enabled": s.enabled,
                "custom": def.is_none(),
                "default": def.map(|l| json!({"windowSecs": l.window_ms / 1000, "points": l.points})),
            })
        })
        .collect()
}

fn node_row(s: &NodeSnapshot, me: &str, reachable: bool) -> J {
    json!({
        "node": s.node,
        "self": s.node == me,
        "reachable": reachable,
        "enabledByFlag": s.enabled_by_flag,
        "configVersion": s.config_version,
        "configError": s.config_error,
        "loadedAtMs": s.loaded_at_ms,
        "checkedAtMs": s.checked_at_ms,
        "liveWindows": s.live_windows,
    })
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterConsumer {
    key: String,
    /// Summed over nodes (per-IP counters are per node).
    used: u32,
    /// The most any one node counted (what its limit is checked against).
    max_node_used: u32,
    limit: Option<u32>,
    reset_ms: u64,
    nodes: Vec<String>,
}

/// Merges nodes' heavy hitters per bucket: summed by key, heaviest first.
fn merge_top(snaps: &[NodeSnapshot], n: usize) -> BTreeMap<String, Vec<ClusterConsumer>> {
    let mut by: BTreeMap<String, BTreeMap<String, ClusterConsumer>> = BTreeMap::new();
    for s in snaps {
        for (bucket, list) in &s.top {
            let m = by.entry(bucket.clone()).or_default();
            for Consumer { key, used, limit, reset_ms } in list {
                let c = m.entry(key.clone()).or_insert_with(|| ClusterConsumer {
                    key: key.clone(),
                    used: 0,
                    max_node_used: 0,
                    limit: *limit,
                    reset_ms: *reset_ms,
                    nodes: Vec::new(),
                });
                c.used = c.used.saturating_add(*used);
                c.max_node_used = c.max_node_used.max(*used);
                c.reset_ms = c.reset_ms.max(*reset_ms);
                c.nodes.push(s.node.clone());
            }
        }
    }
    by.into_iter()
        .map(|(b, m)| {
            let mut v: Vec<ClusterConsumer> = m.into_values().collect();
            v.sort_by(|a, b| b.used.cmp(&a.used).then_with(|| a.key.cmp(&b.key)));
            v.truncate(n);
            (b, v)
        })
        .collect()
}

/// Sums nodes' 429 tallies by (bucket, route).
fn merge_rejections(snaps: &[NodeSnapshot]) -> Vec<RejectionCount> {
    let mut m: BTreeMap<(String, String), RejectionCount> = BTreeMap::new();
    for s in snaps {
        for r in &s.rejections {
            let e = m.entry((r.limiter.clone(), r.route.clone())).or_insert_with(|| RejectionCount {
                limiter: r.limiter.clone(),
                route: r.route.clone(),
                ..Default::default()
            });
            e.last1m += r.last1m;
            e.last5m += r.last5m;
            e.last15m += r.last15m;
            e.total += r.total;
        }
    }
    let mut v: Vec<RejectionCount> = m.into_values().collect();
    v.sort_by(|a, b| b.last5m.cmp(&a.last5m).then(b.total.cmp(&a.total)));
    v
}

async fn get_rate_limits(State(app): AppState, Auth(creds): Auth, Query(q): Query<TopQ>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let n = q.top.unwrap_or(DEFAULT_TOP).clamp(1, MAX_TOP);
    let me = node_id(&app);
    let limiter = &app.ratelimit;
    let mine = limiter.snapshot(&me, n);
    let mut snaps = vec![mine];
    let mut nodes = vec![node_row(&snaps[0], &me, true)];
    let mut unreachable = Vec::new();
    if !q.local {
        let g = internal::gather(&app, "/internal/v1/ratelimits", &[("top", n.to_string())]).await;
        for r in g.replies {
            match serde_json::from_value::<NodeSnapshot>(r.body) {
                Ok(s) => {
                    nodes.push(node_row(&s, &me, true));
                    snaps.push(s);
                }
                Err(e) => {
                    tracing::warn!(peer = %r.node, "rate-limit snapshot unreadable: {e}");
                    unreachable.push(r.node);
                }
            }
        }
        unreachable.extend(g.unreachable);
        for u in &unreachable {
            nodes.push(json!({"node": u, "self": false, "reachable": false}));
        }
    }
    nodes.sort_by(|a, b| a["node"].as_str().cmp(&b["node"].as_str()));
    let policy = limiter.policy();
    let st = limiter.runtime.status();
    let mut out = json!({
        "node": me,
        "enabledByFlag": limiter.enabled_by_flag,
        "enabled": policy.enabled,
        "configVersion": policy.version,
        "config": st.doc,
        "configError": st.error,
        "refreshSecs": runtime::REFRESH_EVERY.as_secs(),
        "limiters": limiter_rows(&policy),
        "nodes": nodes,
        "top": merge_top(&snaps, n),
        "rejections": merge_rejections(&snaps),
        "time": crate::ratelimit::now_ms(),
    });
    if !unreachable.is_empty() {
        out["unreachableNodes"] = json!(unreachable);
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateIn {
    config: J,
    if_version: u64,
    actor: Option<String>,
    note: Option<String>,
}

/// The caller's address for the audit entry (TCP peer, or the client behind
/// a trusted proxy, or the client a forwarding peer vouched for).
pub struct PeerIp(Option<std::net::IpAddr>);

impl axum::extract::FromRequestParts<Arc<App>> for PeerIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, app: &Arc<App>) -> Result<Self, Self::Rejection> {
        Ok(PeerIp(crate::ratelimit::request_client_ip(&parts.headers, &parts.extensions, &app.ratelimit.trusted)))
    }
}

fn save_error(e: SaveError) -> XrpcError {
    let message = e.to_string();
    match e {
        SaveError::Invalid(_) => XrpcError::bad("InvalidConfig", message),
        SaveError::Conflict { .. } => XrpcError { status: StatusCode::CONFLICT, error: "ConfigConflict".into(), message },
        SaveError::Store(_) => XrpcError { status: StatusCode::BAD_GATEWAY, error: "UpstreamFailure".into(), message },
    }
}

async fn update_rate_limits(
    State(app): AppState,
    Auth(creds): Auth,
    PeerIp(peer): PeerIp,
    Json(inp): Json<UpdateIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let doc: Doc = serde_json::from_value(inp.config).map_err(|e| XrpcError::bad("InvalidConfig", format!("invalid config: {e}")))?;
    let actor = inp.actor.map(|a| a.trim().chars().take(64).collect::<String>()).filter(|a| !a.is_empty()).unwrap_or_else(|| "admin".into());
    let ip = peer.map(|ip| ip.to_string());
    let me = node_id(&app);
    let req = SaveReq { doc, if_version: inp.if_version, actor, ip, node: me.clone(), note: inp.note };
    let saved = runtime::save(&app.ratelimit, &app.store, req).await.map_err(save_error)?;
    // every peer reloads now (each also re-reads within REFRESH_EVERY anyway)
    let mut applied = vec![json!({"node": me, "configVersion": app.ratelimit.policy().version, "ok": true})];
    if let Some(c) = &app.cluster {
        let peers: Vec<_> = c.peers().into_iter().filter(|l| l.node_id != me).collect();
        let sends = peers.into_iter().map(|l| {
            let app = app.clone();
            async move {
                let r = app
                    .http
                    .post(format!("{}/internal/v1/ratelimits/reload", l.addr.trim_end_matches('/')))
                    .header(INTERNAL_HDR, &app.config.internal_token)
                    .timeout(RELOAD_TIMEOUT)
                    .send()
                    .await
                    .and_then(|r| r.error_for_status());
                match r {
                    Ok(r) => {
                        let v: J = r.json().await.unwrap_or(J::Null);
                        json!({"node": l.node_id, "configVersion": v["configVersion"], "configError": v["configError"], "ok": true})
                    }
                    Err(e) => {
                        tracing::warn!(peer = %l.node_id, "rate-limit reload nudge failed (it re-reads on its own): {e}");
                        json!({"node": l.node_id, "ok": false, "error": e.to_string()})
                    }
                }
            }
        });
        applied.extend(futures::future::join_all(sends).await);
    }
    Ok(Json(json!({"version": saved.version, "config": saved, "nodes": applied})))
}

async fn internal_snapshot(State(app): AppState, headers: HeaderMap, Query(q): Query<TopQ>) -> XResult<Json<J>> {
    internal_ok(&app, &headers)?;
    let n = q.top.unwrap_or(DEFAULT_TOP).clamp(1, MAX_TOP);
    let s = app.ratelimit.snapshot(&node_id(&app), n);
    Ok(Json(serde_json::to_value(s).map_err(XrpcError::from_err)?))
}

async fn internal_reload(State(app): AppState, headers: HeaderMap) -> XResult<Json<J>> {
    internal_ok(&app, &headers)?;
    let l = &app.ratelimit;
    if let Err(e) = runtime::refresh(l, &app.store).await {
        return Err(XrpcError { status: StatusCode::BAD_GATEWAY, error: "UpstreamFailure".into(), message: format!("{e:#}") });
    }
    let st = l.runtime.status();
    Ok(Json(json!({"node": node_id(&app), "configVersion": l.policy().version, "configError": st.error})))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(node: &str, top: &[(&str, &str, u32)], rej: &[(&str, &str, u64)]) -> NodeSnapshot {
        let mut s = NodeSnapshot { node: node.into(), ..Default::default() };
        for (b, k, used) in top {
            s.top.entry(b.to_string()).or_default().push(Consumer { key: k.to_string(), used: *used, limit: Some(100), reset_ms: 5 });
        }
        for (l, r, n) in rej {
            s.rejections.push(RejectionCount { limiter: l.to_string(), route: r.to_string(), last1m: *n, last5m: *n, last15m: *n, total: *n });
        }
        s
    }

    #[test]
    fn merges_nodes() {
        let a = snap("a", &[("global-ip", "1.1.1.1", 50), ("global-ip", "2.2.2.2", 40)], &[("global-ip", "x.y.z", 3)]);
        let b = snap("b", &[("global-ip", "2.2.2.2", 30), ("repo-write-hour", "did:plc:x", 9)], &[("global-ip", "x.y.z", 2), ("repo-write-hour", "x.y.w", 1)]);
        let t = merge_top(&[a.clone(), b.clone()], 10);
        let g = &t["global-ip"];
        assert_eq!((g[0].key.as_str(), g[0].used, g[0].max_node_used, g[0].nodes.len()), ("2.2.2.2", 70, 40, 2));
        assert_eq!((g[1].key.as_str(), g[1].used), ("1.1.1.1", 50));
        assert_eq!(t["repo-write-hour"][0].used, 9);
        assert_eq!(merge_top(&[a.clone(), b.clone()], 1)["global-ip"].len(), 1);
        let r = merge_rejections(&[a, b]);
        assert_eq!((r[0].limiter.as_str(), r[0].route.as_str(), r[0].total), ("global-ip", "x.y.z", 5));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn rows_list_builtins_then_routes() {
        let d: Doc = serde_json::from_value(json!({"routes": [{"nsid": "a.b.c", "points": 1, "windowSecs": 2}]})).unwrap();
        let p = crate::ratelimit::config::compile(Some(&d)).unwrap();
        let rows = limiter_rows(&p);
        assert_eq!(rows.len(), crate::ratelimit::BUILTIN.len() + 1);
        assert_eq!(rows[0]["name"], "global-ip");
        assert_eq!(rows[0]["default"]["points"], 3000);
        let last = rows.last().unwrap();
        assert_eq!((last["name"].as_str(), last["custom"].as_bool(), last["key"].as_str()), (Some("route:a.b.c"), Some(true), Some("ip")));
    }
}
