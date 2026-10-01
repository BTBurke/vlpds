//! Hot-path caches and their invalidation: verified access tokens
//! (`vlpds::auth::TokenCache`) keep revocation per request, and the proxy's
//! account cache (status + signing key, src/xrpc/proxy.rs) is dropped by the
//! owner's worker on every account change, so takedowns, reactivations and
//! key rotations apply to the very next proxied request, through any node.

use crate::common::*;
use axum::extract::{Request, State};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use k256::ecdsa::signature::Verifier;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const APPVIEW_DID: &str = "did:web:appview.test";
const SHARDS: u16 = 4;

/// A fake AppView that records the Authorization header of each request.
async fn spawn_appview() -> (Arc<Mutex<Vec<String>>>, String) {
    let seen: Arc<Mutex<Vec<String>>> = Default::default();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let router = axum::Router::new()
        .fallback(|State(seen): State<Arc<Mutex<Vec<String>>>>, req: Request| async move {
            let auth = req.headers().get("authorization").map(|v| v.to_str().unwrap().to_string());
            seen.lock().push(auth.unwrap_or_default());
            axum::Json(json!({"feed": []}))
        })
        .with_state(seen.clone());
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    (seen, url)
}

async fn timeline(s: &TestServer, auth: &Auth) -> Resp {
    s.xrpc.get("app.bsky.feed.getTimeline", &[("limit", "1")], auth).await
}

async fn set_takedown(s: &TestServer, did: &str, applied: bool) {
    let body = json!({
        "subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did},
        "takedown": {"applied": applied, "ref": "test"},
    });
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::Admin).await.ok();
}

fn verifies(bearer: &str, key: &k256::ecdsa::VerifyingKey) -> bool {
    let tok = bearer.strip_prefix("Bearer ").expect("bearer token");
    let (signing_input, sig) = tok.rsplit_once('.').unwrap();
    let sig = k256::ecdsa::Signature::from_slice(&B64.decode(sig).unwrap()).unwrap();
    key.verify(signing_input.as_bytes(), &sig).is_ok()
}

/// Tokens verified (and cached) before a revocation are refused after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revocation_applies_to_cached_tokens() {
    let s = TestServer::spawn().await;
    let a = s.create_account("ct").await;
    for _ in 0..3 {
        s.xrpc.get("com.atproto.server.getSession", &[], &a.auth()).await.ok();
    }
    // a token differing only in its signature is not the cached one
    let (head, sig) = a.access.rsplit_once('.').unwrap();
    let flipped = if sig.starts_with('A') { "B" } else { "A" };
    let forged = format!("{head}.{flipped}{}", &sig[1..]);
    s.xrpc.get("com.atproto.server.getSession", &[], &Auth::Bearer(forged)).await.err_status(400);

    // app-password session: used, then its password revoked
    let pw = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "c"}), &a.auth()).await.ok();
    let sess = s.create_session(&a.handle, pw["password"].as_str().unwrap()).await.ok();
    let app_auth = Auth::Bearer(sess["accessJwt"].as_str().unwrap().into());
    s.xrpc.get("com.atproto.server.getSession", &[], &app_auth).await.ok();
    s.xrpc.post("com.atproto.server.revokeAppPassword", &json!({"name": "c"}), &a.auth()).await.ok();
    let r = s.xrpc.get("com.atproto.server.getSession", &[], &app_auth).await;
    assert!(matches!(r.status, 400 | 401), "revoked app-password token: {}", r.text());

    // full session: logged out
    s.xrpc.post_empty("com.atproto.server.deleteSession", &a.refresh_auth()).await.ok();
    let r = s.xrpc.get("com.atproto.server.getSession", &[], &a.auth()).await;
    assert!(matches!(r.status, 400 | 401), "access token of a deleted session: {}", r.text());
}

/// Single node: status and key changes apply to the next proxied request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_changes_apply_to_the_next_proxied_request() {
    let (seen, av_url) = spawn_appview().await;
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((av_url, APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("ac").await;
    for _ in 0..3 {
        assert_eq!(timeline(&s, &a.auth()).await.status, 200);
    }
    set_takedown(&s, &a.did, true).await;
    timeline(&s, &a.auth()).await.err(401, "AccountTakedown");
    set_takedown(&s, &a.did, false).await;
    assert_eq!(timeline(&s, &a.auth()).await.status, 200);

    let k1 = s.signing_key(&a.did).await;
    assert!(verifies(seen.lock().last().unwrap(), &k1));
    let r = s
        .xrpc
        .post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin)
        .await
        .ok();
    assert!(r["signingKey"].is_string(), "{r}");
    let k2 = s.signing_key(&a.did).await;
    assert_ne!(k1, k2);
    assert_eq!(timeline(&s, &a.auth()).await.status, 200);
    assert!(verifies(seen.lock().last().unwrap(), &k2), "the first request after a rotation is signed with the new key");
}

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>, appview: &str) -> TestServer {
    let (id, store, appview) = (id.to_string(), store.clone(), appview.to_string());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.appview = Some((appview, APPVIEW_DID.into()));
        c.dev_mode = true;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await
}

/// Waits until both nodes own shards and agree on who owns what.
async fn balanced(nodes: &[&TestServer]) {
    for _ in 0..400 {
        let owned: Vec<Vec<u16>> =
            nodes.iter().map(|n| n.app.partitions.owned().iter().map(|p| p.id).collect()).collect();
        let all: HashSet<u16> = owned.iter().flatten().copied().collect();
        let complete = owned.iter().all(|o| !o.is_empty())
            && all.len() == SHARDS as usize
            && owned.iter().map(|o| o.len()).sum::<usize>() == SHARDS as usize;
        let routed = complete
            && nodes.iter().all(|n| {
                let c = n.app.cluster.as_ref().unwrap();
                (0..SHARDS).all(|p| {
                    let owner = nodes.iter().position(|m| m.app.partitions.get(p as usize).is_some()).unwrap();
                    c.owner_of(p).map(|(id, _)| id) == Some(nodes[owner].app.cluster.as_ref().unwrap().cfg.node_id.clone())
                })
            });
        if routed {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cluster never balanced");
}

/// Retries `f` while it answers 503 (a shard handoff in progress, e.g.
/// under a loaded test run): anything else is the answer.
async fn past_handoffs<F, Fut>(mut f: F) -> Resp
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Resp>,
{
    for _ in 0..200 {
        let r = f().await;
        if r.status != 503 {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("still 503 after 10 s");
}

/// Cluster: a takedown made through either node applies at once to proxied
/// requests sent to either node (non-owners forward them to the owner, whose
/// cache the change dropped).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedown_applies_at_once_through_any_node() {
    let (_seen, av_url) = spawn_appview().await;
    let store = Arc::new(object_store::memory::InMemory::new());
    let n1 = node("n1", &store, &av_url).await;
    let n2 = node("n2", &store, &av_url).await;
    balanced(&[&n1, &n2]).await;
    let nodes = [&n1, &n2];
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name("cc"));
    let email = format!("{}@example.com", handle.replace('.', "-"));
    let body = json!({"handle": handle, "password": PASSWORD, "email": email});
    let j = past_handoffs(|| n1.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None)).await.ok();
    let (did, auth) = (j["did"].as_str().unwrap().to_string(), Auth::Bearer(j["accessJwt"].as_str().unwrap().into()));
    let p = vlpds::state::partition_of(&did, SHARDS) as usize;
    let owner = nodes.iter().position(|n| n.app.partitions.get(p).is_some()).expect("owned");
    let (own, other) = (nodes[owner], nodes[1 - owner]);
    let status = |s: bool| {
        json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did}, "takedown": {"applied": s, "ref": "test"}})
    };
    for n in nodes {
        assert_eq!(past_handoffs(|| timeline(n, &auth)).await.status, 200);
    }
    // through the non-owner (forwarded by subject), then read through both
    let (on, off) = (status(true), status(false));
    past_handoffs(|| other.xrpc.post("com.atproto.admin.updateSubjectStatus", &on, &Auth::Admin)).await.ok();
    for n in nodes {
        past_handoffs(|| timeline(n, &auth)).await.err(401, "AccountTakedown");
    }
    past_handoffs(|| own.xrpc.post("com.atproto.admin.updateSubjectStatus", &off, &Auth::Admin)).await.ok();
    for n in nodes {
        assert_eq!(past_handoffs(|| timeline(n, &auth)).await.status, 200);
    }
}
