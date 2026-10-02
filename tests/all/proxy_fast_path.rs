//! Proxy fast path (src/xrpc/proxy.rs caches): service JWTs are reused per
//! (iss, aud, lxm, signing key), so a key rotation stops the reuse of tokens
//! signed by the old key once the account cache refreshes. Also the
//! proxy's limits: per-account requests in flight, clients that stop
//! reading, account-load failures.

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
    // One: a connection goes back to the slot of the thread that read its
    // response to the end, and a request on a thread whose slot is empty
    // takes it from there instead of connecting (per-thread pools drifted
    // to 2-10 here when tasks hopped threads)
    let seq = accepted.load(Ordering::Relaxed);
    assert!(seq <= 2, "{seq} connections for 50 sequential requests");
    for _ in 0..20 {
        futures::future::join_all((0..32).map(|_| get("par"))).await;
    }
    // bounded by the concurrency, not one per request or per thread
    let par = accepted.load(Ordering::Relaxed) - seq;
    assert!(par <= 32, "{par} connections for 640 requests, 32 in flight");
    // connections the upstream closes are not reused
    for _ in 0..5 {
        get("close=1").await;
        get("after").await;
    }
}

/// Compressed AppView responses pass through as they are: same bytes, same
/// Content-Encoding and Content-Length (no decode/re-encode, no buffering),
/// and the client's Accept-Encoding reaches the upstream (for methods with
/// read-after-write, only its decodable codings: tests/all/read_after_write.rs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_responses_pass_through() {
    // not valid gzip on purpose: the PDS must not look inside
    let payload: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    let seen_ae: Arc<Mutex<Vec<String>>> = Default::default();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let (body, seen) = (payload.clone(), seen_ae.clone());
    let router = axum::Router::new().fallback(move |req: Request| {
        let (body, seen) = (body.clone(), seen.clone());
        async move {
            let ae = req.headers().get("accept-encoding").map(|v| v.to_str().unwrap().to_string());
            seen.lock().push(ae.unwrap_or_default());
            ([("content-type", "application/json"), ("content-encoding", "gzip")], body)
        }
    });
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((url, APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("gzipper").await;
    let r = reqwest::Client::new()
        .get(format!("{}/xrpc/app.bsky.feed.getLikes?uri=x", s.url))
        .header("authorization", format!("Bearer {}", a.access))
        .header("accept-encoding", "gzip, br")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-encoding").unwrap(), "gzip");
    assert_eq!(r.headers().get("content-length").unwrap(), "5000");
    assert_eq!(r.bytes().await.unwrap().as_ref(), payload.as_slice());
    assert_eq!(seen_ae.lock().last().unwrap(), "gzip, br");
}

/// CORS preflights for proxied methods are answered by the PDS itself: no
/// auth, no upstream request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflights_stay_local() {
    let (seen, av_url) = spawn_appview().await;
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((av_url, APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let r = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("{}/xrpc/app.bsky.actor.getProfile?actor=x", s.url))
        .header("origin", "https://bsky.app")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization,atproto-accept-labelers")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let h = r.headers();
    assert_eq!(h.get("access-control-allow-origin").unwrap(), "*");
    assert_eq!(h.get("access-control-allow-headers").unwrap(), "authorization,atproto-accept-labelers");
    assert_eq!(h.get("access-control-max-age").unwrap(), "86400");
    assert!(seen.lock().is_empty(), "a preflight reached the AppView");
}

/// An upstream for the limit tests: `/xrpc/app.bsky.test.slow` streams a
/// chunk every 50 ms forever, `big` is 9 MiB and `small` 64 KiB (both with
/// a Content-Length). Returns its `host:port`.
async fn limits_upstream() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = l.local_addr().unwrap().to_string();
    let router = axum::Router::new().fallback(|req: Request| async move {
        match req.uri().path() {
            "/xrpc/app.bsky.test.slow" => {
                let s = futures::stream::unfold((), |_| async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"chunk ")), ()))
                });
                axum::body::Body::from_stream(s)
            }
            "/xrpc/app.bsky.test.big" => axum::body::Body::from(vec![b'b'; 9 << 20]),
            _ => axum::body::Body::from(vec![b's'; 64 << 10]),
        }
    });
    tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
    authority
}

/// One account can't hold more than MAX_IN_FLIGHT_PER_ACCOUNT proxied
/// requests (bodies included): past it 429, and finished ones free their
/// slots. (Without it one account's unread responses could hold most of
/// the AppView pool.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxied_requests_in_flight_are_capped_per_account() {
    let authority = limits_upstream().await;
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((format!("http://{authority}"), APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("capped").await;
    let b = s.create_account("uncapped").await;
    let http = reqwest::Client::new();
    let get = |tok: String, nsid: &'static str| {
        let (http, url) = (http.clone(), format!("{}/xrpc/{nsid}", s.url));
        async move { http.get(url).bearer_auth(tok).send().await.unwrap() }
    };
    let max = 64; // proxy::MAX_IN_FLIGHT_PER_ACCOUNT
    let held = futures::future::join_all((0..max).map(|_| get(a.access.clone(), "app.bsky.test.slow"))).await;
    assert!(held.iter().all(|r| r.status() == 200));
    let r = get(a.access.clone(), "app.bsky.test.small").await;
    assert_eq!(r.status(), 429);
    assert_eq!(r.json::<J>().await.unwrap()["error"], "RateLimitExceeded");
    // other accounts are not affected
    assert_eq!(get(b.access.clone(), "app.bsky.test.small").await.status(), 200);
    // done (the clients went away): the slots come back
    drop(held);
    let t = Instant::now();
    loop {
        let r = get(a.access.clone(), "app.bsky.test.small").await;
        if r.status() == 200 {
            assert_eq!(r.bytes().await.unwrap().len(), 64 << 10);
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(10), "slots not freed: {}", r.status());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A client that sends a proxied request and never reads the response
/// must not keep the upstream connection: a small response is read whole
/// and its connection pooled at once; a large one is dropped (and its
/// connection closed) once the client hasn't taken any of it for the
/// write-stall deadline. Before, both held a pooled AppView connection for
/// as long as the client kept the socket open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unread_proxied_responses_free_upstream_connections() {
    use tokio::io::AsyncWriteExt;
    let authority = limits_upstream().await;
    let s = TestServer::spawn_with(|c| {
        c.appview = Some((format!("http://{authority}"), APPVIEW_DID.into()));
        c.dev_mode = true;
    })
    .await;
    let a = s.create_account("staller").await;
    let host = vlpds::http::h1::host(&authority);
    let stall = |nsid: &'static str| {
        let (addr, tok) = (s.addr, a.access.clone());
        async move {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            let req = format!("GET /xrpc/{nsid} HTTP/1.1\r\nhost: pds.test\r\nauthorization: Bearer {tok}\r\n\r\n");
            sock.write_all(req.as_bytes()).await.unwrap();
            sock // never read
        }
    };
    let wait = |what: &str, ok: &dyn Fn() -> bool| {
        let t = Instant::now();
        while !ok() {
            assert!(t.elapsed() < Duration::from_secs(10), "{what}: open {} idle {}", host.open_connections(), host.idle_connections());
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let _small = stall("app.bsky.test.small").await;
    wait("small response pooled", &|| host.open_connections() == 1 && host.idle_connections() == 1);
    let _big = stall("app.bsky.test.big").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(host.idle_connections(), 0, "the unread response holds its connection");
    assert_eq!(host.open_connections(), 1);
    let limit = Duration::from_millis(1200);
    vlpds::http::stall::set_limit(limit);
    let dropped = vlpds::http::stall::sweep_now();
    vlpds::http::stall::set_limit(vlpds::http::stall::WRITE_STALL);
    assert!(dropped >= 1, "{dropped}");
    wait("stalled response's connection closed", &|| host.open_connections() == 0);
}

/// Only a missing account is 403 AccountNotFound: an account whose signing
/// key can't be unwrapped (here: a node without its KEK) is a server error
/// the client may retry, not "account not found".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_load_failures_are_not_account_not_found() {
    use vlpds::secrets::{KekBytes, KekConfig};
    let (_seen, av_url) = spawn_appview().await;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let node = |kek: KekBytes| {
        let (store, av_url) = (store.clone(), av_url.clone());
        TestServer::spawn_with(move |c| {
            c.memory_store = Some(store);
            c.shards = 4;
            c.kek = KekConfig { local: Some(kek), ..Default::default() };
            c.appview = Some((av_url, APPVIEW_DID.into()));
            c.dev_mode = true;
            c.cluster = Some(vlpds::cluster::ClusterConfig {
                node_id: "kek".into(),
                addr: c.public_url.clone(),
                shards: 4,
                ttl: Duration::from_millis(1500),
                renew_every: Duration::from_millis(100),
                skew: Duration::from_millis(300),
                ..Default::default()
            });
        })
    };
    let a = node(KekBytes::random()).await;
    let u = a.create_account("kekless").await;
    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;
    let b = node(KekBytes::random()).await;
    let r = b.xrpc.get("app.bsky.feed.getTimeline", &[], &u.auth()).await;
    assert!(r.status >= 500, "{r:?}");
    assert_ne!(r.json["error"], "AccountNotFound", "{r:?}");
    vlpds::server::shutdown(&b.app).await;
}
