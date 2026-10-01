//! Proxy fast path (src/xrpc/proxy.rs caches): service JWTs are reused per
//! (iss, aud, lxm, signing key), so a key rotation stops the reuse of tokens
//! signed by the old key once the account cache refreshes.

use crate::common::*;
use axum::extract::{Request, State};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use k256::ecdsa::signature::Verifier;
use parking_lot::Mutex;
use serde_json::{json, Value as J};
use std::sync::Arc;
use std::time::{Duration, Instant};

const APPVIEW_DID: &str = "did:web:appview.test";

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

/// The account's current #atproto key, from describeRepo's DID document.
async fn repo_key(s: &TestServer, did: &str) -> k256::ecdsa::VerifyingKey {
    let r = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None).await.ok();
    let mb = r["didDoc"]["verificationMethod"][0]["publicKeyMultibase"].as_str().unwrap().to_string();
    let raw = bs58::decode(mb.strip_prefix('z').unwrap()).into_vec().unwrap();
    assert_eq!(&raw[..2], &[0xe7, 0x01], "secp256k1-pub multicodec");
    k256::ecdsa::VerifyingKey::from_sec1_bytes(&raw[2..]).unwrap()
}

fn verifies(bearer: &str, key: &k256::ecdsa::VerifyingKey) -> bool {
    let tok = bearer.strip_prefix("Bearer ").expect("bearer token");
    let (signing_input, sig) = tok.rsplit_once('.').unwrap();
    let sig = k256::ecdsa::Signature::from_slice(&B64.decode(sig).unwrap()).unwrap();
    key.verify(signing_input.as_bytes(), &sig).is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotated_signing_key_stops_service_jwt_reuse() {
    let (seen, av_url) = spawn_appview().await;
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((av_url, APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("rotator").await;
    let timeline = || async {
        let r = s.xrpc.get("app.bsky.feed.getTimeline", &[("limit", "1")], &a.auth()).await;
        assert_eq!(r.status, 200, "{r:?}");
        seen.lock().last().cloned().expect("appview saw the request")
    };

    let k1 = repo_key(&s, &a.did).await;
    let t1 = timeline().await;
    assert!(verifies(&t1, &k1));
    assert_eq!(timeline().await, t1, "the minted token is reused");

    let r = s
        .xrpc
        .post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin)
        .await
        .ok();
    assert!(r["signingKey"].is_string(), "{r}");
    let k2 = repo_key(&s, &a.did).await;
    assert_ne!(k1, k2);

    // the new key signs (at once: the rotation drops the cached account)
    let t0 = Instant::now();
    loop {
        let t = timeline().await;
        if verifies(&t, &k2) {
            assert_ne!(t, t1);
            break;
        }
        assert!(verifies(&t, &k1), "a token signed by neither key");
        assert!(t0.elapsed() < Duration::from_secs(5), "still signing with the rotated-out key");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // and it stays on the new key
    let t = timeline().await;
    assert!(verifies(&t, &k2));
    let claims: J = serde_json::from_slice(&B64.decode(t.split('.').nth(1).unwrap()).unwrap()).unwrap();
    assert_eq!(claims["iss"], a.did.as_str());
    assert_eq!(claims["aud"], APPVIEW_DID);
}

/// The plain-HTTP/1.1 upstream path keeps connections: many proxied
/// requests, sequential and concurrent, open a handful of connections, not
/// one each, and the upstream closing one doesn't fail the next request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_connections_are_reused() {
    use axum::serve::ListenerExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let accepted = Arc::new(AtomicUsize::new(0));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let n = accepted.clone();
    let l = l.tap_io(move |_| {
        n.fetch_add(1, Ordering::Relaxed);
    });
    let router = axum::Router::new().fallback(|req: Request| async move {
        // `close=1`: the upstream drops this connection after answering
        let close = req.uri().query().is_some_and(|q| q.contains("close=1"));
        let mut r = axum::response::IntoResponse::into_response(axum::Json(json!({"feed": [], "pad": "x".repeat(3000)})));
        if close {
            r.headers_mut().insert("connection", "close".parse().unwrap());
        }
        r
    });
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((url, APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("reuser").await;
    let get = |q: &'static str| {
        let (xrpc, auth) = (&s.xrpc, a.auth());
        async move {
            let r = xrpc.get("app.bsky.feed.getTimeline", &[("limit", "1"), ("q", q)], &auth).await;
            assert_eq!(r.status, 200, "{r:?}");
            assert_eq!(r.json["pad"].as_str().map(str::len), Some(3000));
        }
    };
    for _ in 0..50 {
        get("seq").await;
    }
    let seq = accepted.load(Ordering::Relaxed);
    assert!(seq <= 8, "{seq} connections for 50 sequential requests");
    for _ in 0..20 {
        futures::future::join_all((0..32).map(|_| get("par"))).await;
    }
    // bounded by the concurrency plus what each thread keeps idle
    // (vlpds::http::h1::LOCAL_IDLE per IO thread), not one per request
    let par = accepted.load(Ordering::Relaxed) - seq;
    assert!(par <= 32 + 4 * vlpds::http::h1::LOCAL_IDLE, "{par} connections for 640 requests, 32 in flight");
    // connections the upstream closes are not reused
    for _ in 0..5 {
        get("close=1").await;
        get("after").await;
    }
}
