//! The space host's calls out (reference `simplespace/manager.ts` and
//! `api/com/atproto/space/util.ts`): resolving a service identifier,
//! asking a managing app whether a user may read or write, forwarding a
//! sequenced write to a registered service, and telling registered services
//! a space is gone. Every call goes through the SSRF-guarded client with
//! service auth from the authority (`aud` = the service identifier as
//! published), a 10 s timeout and a small response cap.

use super::rows::NotifyRow;
use crate::state::{self, SpaceId};
use crate::tid::Tid;
use crate::xrpc::App;
use serde_json::{json, Value as J};
use std::time::Duration;

pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE: usize = 64 << 10;
/// Endpoints are kept in `sN` rows (u16-length strings).
const MAX_ENDPOINT: usize = 2048;

/// How long a notify registration lasts (the reference's
/// `REGISTRATION_TTL_MS`).
pub const REGISTRATION_TTL: Duration = Duration::from_secs(24 * 3600);

/// Reference `resolveServiceEndpoint`: `did#fragment` names that service
/// entry; a bare DID its PDS. `#atproto_space_host` falls back to the PDS
/// when the document has no dedicated entry.
pub async fn resolve_service_endpoint(app: &App, service: &str) -> Option<String> {
    let (did, fragment) = match service.split_once('#') {
        Some((d, f)) => (d, Some(f)),
        None => (service, None),
    };
    if !crate::xrpc::syntax::valid_did(did) {
        return None;
    }
    let doc = match app.did_resolver.resolve(did).await {
        Ok(d) => d,
        Err(e) => {
            tracing::info!(service, "could not resolve service did: {e:?}");
            return None;
        }
    };
    let ep = match fragment {
        Some("atproto_space_host") => crate::did_resolver::service_endpoint(&doc, "atproto_space_host")
            .or_else(|| crate::did_resolver::service_endpoint(&doc, "atproto_pds")),
        Some(f) => crate::did_resolver::service_endpoint(&doc, f),
        None => crate::did_resolver::service_endpoint(&doc, "atproto_pds"),
    };
    ep.filter(|e| e.len() <= MAX_ENDPOINT)
}

/// A call to `service` at `endpoint` as `iss` (an account hosted here):
/// Ok(status, JSON body) once it answered.
#[allow(clippy::too_many_arguments)]
async fn call(
    app: &App,
    client: crate::http::Guarded,
    iss: &str,
    service: &str,
    endpoint: &str,
    lxm: &str,
    method: reqwest::Method,
    query: &[(&str, &str)],
    body: Option<&J>,
) -> Result<(u16, J), String> {
    let (key, _) = crate::xrpc::proxy::account_key_status(app, iss).await.map_err(|e| e.message)?;
    let jwt = crate::auth::service_auth_jwt(&key, iss, service, Some(lxm), 60).map_err(|e| e.to_string())?;
    let url = format!("{}/xrpc/{lxm}", endpoint.trim_end_matches('/'));
    let mut rb = client.request(method, &url)?.bearer_auth(jwt).timeout(CALL_TIMEOUT);
    if !query.is_empty() {
        rb = rb.query(query);
    }
    if let Some(b) = body {
        rb = rb.json(b);
    }
    let mut r = rb.send().await.map_err(|e| format!("{url}: {e}"))?;
    let status = r.status().as_u16();
    let mut buf = Vec::new();
    while let Some(c) = r.chunk().await.map_err(|e| format!("{url}: {e}"))? {
        if buf.len() + c.len() > MAX_RESPONSE {
            return Err(format!("{url}: response too large"));
        }
        buf.extend_from_slice(&c);
    }
    Ok((status, serde_json::from_slice(&buf).unwrap_or(J::Null)))
}

/// Reference `checkManagingApp`: the managing app's answer, and a denial
/// whenever it can't be had (unresolvable, unreachable, any error), since
/// failing open would hand credentials out for the spaces that asked for
/// the strictest gate.
pub async fn check_user_access(
    app: &App,
    space: &str,
    authority: &str,
    managing_app: &str,
    user: &str,
    access: &str,
    client_id: Option<&str>,
) -> bool {
    let lxm = "com.atproto.simplespace.checkUserAccess";
    let Some(endpoint) = resolve_service_endpoint(app, managing_app).await else {
        tracing::info!(space, managing_app, "could not resolve managing app");
        return false;
    };
    let mut q = vec![("space", space), ("user", user), ("access", access)];
    if let Some(c) = client_id {
        q.push(("clientId", c));
    }
    let client = crate::http::guarded(app.config.dev_mode);
    match call(app, client, authority, managing_app, &endpoint, lxm, reqwest::Method::GET, &q, None).await {
        Ok((200, body)) => body["authorized"] == J::Bool(true),
        Ok((status, _)) => {
            tracing::info!(space, managing_app, user, status, "managing app check failed");
            false
        }
        Err(e) => {
            tracing::info!(space, managing_app, user, "managing app check failed: {e}");
            false
        }
    }
}

/// A space's registrations (`sN`) from the authority's shard: the live
/// ones, and the services of expired ones.
pub async fn registrations(
    app: &App,
    authority: &str,
    sid: &SpaceId,
) -> anyhow::Result<(Vec<(String, NotifyRow)>, Vec<String>)> {
    let p = app.partition(authority).map_err(|e| anyhow::anyhow!("{}", e.message))?;
    let prefix = state::space_prefix(state::SPACE_NOTIFY_FAMILY, authority, sid);
    let opts = slatedb::config::ScanOptions::default();
    let mut it = p.db.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await?;
    let now = crate::tid::now_micros();
    let (mut live, mut expired) = (Vec::new(), Vec::new());
    while let Some(kv) = it.next().await? {
        let service = std::str::from_utf8(&kv.key[prefix.len()..])?.to_string();
        let row = NotifyRow::decode(&kv.value)?;
        match row.expires > now {
            true => live.push((service, row)),
            false => expired.push(service),
        }
    }
    Ok((live, expired))
}

/// One forward of a sequenced write to a registered service.
pub struct Forward {
    pub authority: std::sync::Arc<str>,
    pub uri: std::sync::Arc<str>,
    pub service: String,
    pub endpoint: String,
    pub writer: String,
    pub repo_rev: Tid,
    pub hash: [u8; 32],
    pub space_rev: Tid,
    pub prev_space_rev: Option<Tid>,
    /// The registration's expiry (unix µs).
    pub expires: u64,
}

fn b64(b: &[u8]) -> J {
    use base64::Engine;
    json!({"$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(b)})
}

/// notifyWrite to a registered service, as the reference forwards it (the
/// writer's notify plus the spaceRevs), naming `prev` as the spaceRev
/// before it: "ok", "refused" (a status not worth retrying) or "error".
pub async fn forward(app: &App, f: &Forward, prev: Option<Tid>) -> &'static str {
    let lxm = "com.atproto.space.notifyWrite";
    let mut body = json!({
        "space": &*f.uri,
        "repo": f.writer,
        "repoRev": f.repo_rev.to_string(),
        "hash": b64(&f.hash),
        "spaceRev": f.space_rev.to_string(),
    });
    if let Some(p) = prev {
        body["prevSpaceRev"] = json!(p.to_string());
    }
    let client = crate::http::guarded_fanout(app.config.dev_mode);
    let post = reqwest::Method::POST;
    match call(app, client, &f.authority, &f.service, &f.endpoint, lxm, post, &[], Some(&body)).await {
        Ok((s, _)) if (200..300).contains(&s) => "ok",
        Ok((s, _)) if crate::xrpc::space::retryable_status(s) => {
            tracing::info!(space = %f.uri, service = f.service, status = s, "space notify forward failed");
            "error"
        }
        Ok((s, _)) => {
            tracing::info!(space = %f.uri, service = f.service, status = s, "space notify forward refused");
            "refused"
        }
        Err(e) => {
            tracing::info!(space = %f.uri, service = f.service, "space notify forward failed: {e}");
            "error"
        }
    }
}

/// Reference deleteSpace's notifySpaceDeleted: best effort, one service at a
/// time; one that misses it learns of it from `SpaceDeleted` on its next
/// credential renewal.
pub async fn notify_space_deleted(app: &App, authority: &str, uri: &str, services: Vec<(String, NotifyRow)>) {
    let lxm = "com.atproto.space.notifySpaceDeleted";
    let body = json!({"space": uri});
    for (service, row) in services {
        let client = crate::http::guarded_fanout(app.config.dev_mode);
        let r =
            call(app, client, authority, &service, &row.endpoint, lxm, reqwest::Method::POST, &[], Some(&body)).await;
        match r {
            Ok((s, _)) if (200..300).contains(&s) => {}
            Ok((s, _)) => tracing::info!(space = uri, service, status = s, "notifySpaceDeleted refused"),
            Err(e) => tracing::info!(space = uri, service, "notifySpaceDeleted failed: {e}"),
        }
    }
}
