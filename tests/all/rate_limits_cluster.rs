//! Rate limits keyed on the real client across forwards (src/forward.rs,
//! src/ratelimit.rs): a node forwarding a request to the owner of its
//! account sends the client address it resolved, so the owner's per-IP
//! buckets never key on the forwarding node (one client can't lock others
//! out of an account through another node); a client can't set that address
//! itself; createSession's per-account cap is enforced on the account's
//! owner whichever node is called and whatever query or token is added;
//! IPv6 clients share their /64; reserveSigningKey and the OAuth endpoints
//! have their own buckets.
//!
//! Requests from several client addresses go through each node's router in
//! process (`tower::ServiceExt::oneshot` with the TCP peer set as the
//! server sets it); forwards between nodes are real HTTP.

use crate::common::*;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tower::ServiceExt;

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> TestServer {
    cluster_node(id, store.clone(), SHARDS, |c| c.rate_limits_enabled = true).await
}

/// An account owned by each node (a node mints DIDs in its own shards):
/// `[owned by a, owned by b]`.
async fn one_each(a: &TestServer, b: &TestServer) -> [TestAccount; 2] {
    let (x, y) = (a.create_account("rlc").await, b.create_account("rlc").await);
    assert!(a.app.remote_owner(&x.did).is_none() && b.app.remote_owner(&x.did).is_some());
    assert!(b.app.remote_owner(&y.did).is_none() && a.app.remote_owner(&y.did).is_some());
    [x, y]
}

/// Installs a rate-limit config on each node directly (as a saved config
/// object would be).
fn set_limits(nodes: &[&TestServer], doc: J) {
    for n in nodes {
        let d: vlpds::ratelimit::config::Doc = serde_json::from_value(doc.clone()).unwrap();
        n.app.ratelimit.install(vlpds::ratelimit::config::compile(Some(&d)).unwrap());
    }
}

/// One request into `n`'s router from client `ip` (the TCP peer).
async fn call(n: &TestServer, ip: &str, method: &str, uri: &str, headers: &[(&str, &str)], body: Option<J>) -> (u16, J) {
    let peer = SocketAddr::new(ip.parse::<IpAddr>().unwrap(), 40000);
    let mut b = axum::http::Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let body = match body {
        Some(j) => {
            b = b.header("content-type", "application/json");
            axum::body::Body::from(j.to_string())
        }
        None => axum::body::Body::empty(),
    };
    let mut req = b.body(body).unwrap();
    req.extensions_mut().insert(axum::extract::ConnectInfo(peer));
    let r = vlpds::server::router(&n.app).oneshot(req).await.unwrap();
    let status = r.status().as_u16();
    let bytes = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(J::Null))
}

async fn sign_in(n: &TestServer, ip: &str, uri: &str, headers: &[(&str, &str)], ident: &str, password: &str) -> (u16, J) {
    call(n, ip, "POST", uri, headers, Some(json!({"identifier": ident, "password": password}))).await
}

const CREATE_SESSION: &str = "/xrpc/com.atproto.server.createSession";

/// The owner keys a forwarded request's per-IP buckets on the client the
/// entry node saw, not on the entry node: one client spending an account's
/// createSession bucket through another node no longer locks every other
/// client of that node out of the account. A client can't choose the
/// address the owner uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_requests_keep_the_client_ip() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let (a, b) = (node("a", &store).await, node("b", &store).await);
    balanced(&[&a, &b]).await;
    let [victim, _] = one_each(&a, &b).await;
    // the victim's account lives on a; everyone below calls b
    let attacker = "203.0.113.1";
    for i in 0..30 {
        let (st, j) = sign_in(&b, attacker, CREATE_SESSION, &[], &victim.handle, "wrong").await;
        assert_eq!(st, 401, "attempt {i}: {j}");
    }
    let (st, j) = sign_in(&b, attacker, CREATE_SESSION, &[], &victim.handle, "wrong").await;
    assert_eq!((st, j["error"].as_str()), (429, Some("RateLimitExceeded")), "{j}");
    // the owner counted the attacker's address, not b's
    let top = a.app.ratelimit.snapshot("a", 50).top;
    let keys: Vec<&str> = top["com.atproto.server.createSession-1"].iter().map(|c| c.key.as_str()).collect();
    assert!(keys.contains(&format!("{}-{attacker}", victim.handle).as_str()), "{keys:?}");
    assert!(!keys.iter().any(|k| k.ends_with("-127.0.0.1")), "{keys:?}");
    // the victim, also entering through b, signs in
    let (st, j) = sign_in(&b, "203.0.113.2", CREATE_SESSION, &[], &victim.handle, PASSWORD).await;
    assert_eq!(st, 200, "victim locked out through b: {j}");
    // the attacker can't pick a fresh address: b drops a client's copy of
    // the header and sends the one it saw
    for fake in ["198.51.100.1", "198.51.100.2"] {
        let h = [("x-vlpds-client-ip", fake), ("x-vlpds-forwarded", "guess")];
        let (st, j) = sign_in(&b, attacker, CREATE_SESSION, &h, &victim.handle, "wrong").await;
        assert_eq!(st, 429, "spoofed {fake} through b: {j}");
    }
    // nor straight at the owner: without the internal token the header is
    // ignored, and the TCP peer (here 127.0.0.1) is the client
    let ghost = format!("ghost{}.vlpds.test", unique_name("g"));
    for i in 0..30 {
        let rb = a
            .xrpc
            .http
            .post(format!("{}{CREATE_SESSION}", a.url))
            .header("x-vlpds-forwarded", "guess")
            .header("x-vlpds-client-ip", format!("198.51.100.{i}"))
            .json(&json!({"identifier": ghost, "password": "x"}));
        assert_eq!(a.xrpc.send(rb).await.status, 401, "attempt {i}");
    }
    let rb = a
        .xrpc
        .http
        .post(format!("{}{CREATE_SESSION}", a.url))
        .header("x-vlpds-forwarded", "guess")
        .header("x-vlpds-client-ip", "198.51.100.200")
        .json(&json!({"identifier": ghost, "password": "x"}));
    a.xrpc.send(rb).await.err(429, "RateLimitExceeded");
}

/// createSession's per-account cap (sign-in-account, any IP) is counted on
/// the account's owner, whichever node each attempt enters and whatever
/// `?did=` or (forged) bearer token is added to send it elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_session_account_cap_across_nodes() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let (a, b) = (node("a", &store).await, node("b", &store).await);
    balanced(&[&a, &b]).await;
    let [victim, other] = one_each(&a, &b).await;
    set_limits(&[&a, &b], json!({"limiters": {"sign-in-account": {"points": 5}}}));
    // five guesses from five addresses, through both nodes
    for i in 0..5 {
        let n = if i % 2 == 0 { &a } else { &b };
        let (st, j) = sign_in(n, &format!("203.0.113.{}", 10 + i), CREATE_SESSION, &[], &victim.handle, "wrong").await;
        assert_eq!(st, 401, "attempt {i}: {j}");
    }
    // the account is capped for everyone, the right password included
    for n in [&a, &b] {
        let (st, j) = sign_in(n, "203.0.113.99", CREATE_SESSION, &[], &victim.handle, PASSWORD).await;
        assert_eq!((st, j["error"].as_str()), (429, Some("RateLimitExceeded")), "{j}");
    }
    // ?did= naming an account of b no longer serves it on b (fresh counters)
    let uri = format!("{CREATE_SESSION}?did={}", other.did);
    let (st, j) = sign_in(&b, "203.0.113.98", &uri, &[], &victim.handle, PASSWORD).await;
    assert_eq!(st, 429, "?did= rerouted createSession: {j}");
    // nor does an (unverified) token whose sub is b's
    use base64::Engine;
    let sub = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json!({"sub": other.did}).to_string());
    let auth = format!("Bearer x.{sub}.y");
    let (st, j) = sign_in(&b, "203.0.113.97", CREATE_SESSION, &[("authorization", &auth)], &victim.handle, PASSWORD).await;
    assert_eq!(st, 429, "a token's sub rerouted createSession: {j}");
    // only a's counters know the victim
    let has = |n: &TestServer, id: &str| {
        n.app.ratelimit.snapshot(id, 50).top.get("sign-in-account").is_some_and(|l| l.iter().any(|c| c.key == victim.did))
    };
    assert!(has(&a, "a") && !has(&b, "b"));
    // another account is unaffected
    let (st, j) = sign_in(&b, "203.0.113.99", CREATE_SESSION, &[], &other.handle, PASSWORD).await;
    assert_eq!(st, 200, "{j}");
}

/// IPv6 clients are counted by /64: fresh addresses in one allocation share
/// a bucket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipv6_clients_share_their_64() {
    let s = TestServer::spawn_with(|c| c.rate_limits_enabled = true).await;
    let ident = format!("{}.vlpds.test", unique_name("v6"));
    for i in 0..30 {
        let ip = format!("2001:db8:1:2::{:x}", i + 1);
        let (st, j) = sign_in(&s, &ip, CREATE_SESSION, &[], &ident, "x").await;
        assert_eq!(st, 401, "attempt {i}: {j}");
    }
    let (st, _) = sign_in(&s, "2001:db8:1:2:ffff:ffff:ffff:ffff", CREATE_SESSION, &[], &ident, "x").await;
    assert_eq!(st, 429);
    let (st, _) = sign_in(&s, "2001:db8:1:3::1", CREATE_SESSION, &[], &ident, "x").await;
    assert_eq!(st, 401, "another /64 has its own bucket");
    // IPv4 (and IPv4-mapped) clients stay per address
    let (st, _) = sign_in(&s, "::ffff:192.0.2.1", CREATE_SESSION, &[], &ident, "x").await;
    assert_eq!(st, 401);
}

/// reserveSigningKey: per IP, plus a per-node cap on new reservations (a
/// live reservation for a DID is still answered).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserve_signing_key_limits() {
    let s = TestServer::spawn_with(|c| c.rate_limits_enabled = true).await;
    const RESERVE: &str = "/xrpc/com.atproto.server.reserveSigningKey";
    set_limits(&[&s], json!({"limiters": {"com.atproto.server.reserveSigningKey-0": {"points": 3}}}));
    for i in 0..3 {
        let (st, j) = call(&s, "192.0.2.1", "POST", RESERVE, &[], Some(json!({}))).await;
        assert_eq!(st, 200, "call {i}: {j}");
    }
    let (st, j) = call(&s, "192.0.2.1", "POST", RESERVE, &[], Some(json!({}))).await;
    assert_eq!((st, j["error"].as_str()), (429, Some("RateLimitExceeded")), "{j}");
    // the node cap counts only new reservations (3 made above)
    set_limits(&[&s], json!({"limiters": {"reserve-signing-key-node": {"points": 5}}}));
    let did = "did:plc:reservelimittest00000000";
    let (st, j) = call(&s, "192.0.2.2", "POST", RESERVE, &[], Some(json!({"did": did}))).await;
    assert_eq!(st, 200, "{j}");
    let key = j["signingKey"].as_str().unwrap().to_string();
    let (st, j) = call(&s, "192.0.2.3", "POST", RESERVE, &[], Some(json!({}))).await;
    assert_eq!(st, 200, "{j}");
    let (st, j) = call(&s, "192.0.2.4", "POST", RESERVE, &[], Some(json!({}))).await;
    assert_eq!((st, j["error"].as_str()), (429, Some("RateLimitExceeded")), "node cap: {j}");
    let (st, j) = call(&s, "192.0.2.4", "POST", RESERVE, &[], Some(json!({"did": did}))).await;
    assert_eq!((st, j["signingKey"].as_str()), (200, Some(key.as_str())), "live reservation: {j}");
    // admin bypasses
    let r = s.xrpc.post("com.atproto.server.reserveSigningKey", &json!({}), &Auth::Admin).await;
    r.ok();
}

/// /oauth/par, /oauth/token and /oauth/revoke share a per-IP bucket and
/// answer 429 in OAuth's error shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_endpoints_are_limited_per_ip() {
    let s = TestServer::spawn_with(|c| c.rate_limits_enabled = true).await;
    set_limits(&[&s], json!({"limiters": {"oauth-ip": {"points": 3}}}));
    for path in ["/oauth/par", "/oauth/token", "/oauth/revoke"] {
        let (st, j) = call(&s, "192.0.2.10", "POST", path, &[("content-type", "application/x-www-form-urlencoded")], None).await;
        assert_ne!(st, 429, "{path}: {j}");
    }
    let (st, j) = call(&s, "192.0.2.10", "POST", "/oauth/token", &[], None).await;
    assert_eq!((st, j["error"].as_str()), (429, Some("rate_limit_exceeded")), "{j}");
    let (st, j) = call(&s, "192.0.2.11", "POST", "/oauth/token", &[], None).await;
    assert_ne!(st, 429, "another address: {j}");
    // XRPC's global bucket is not spent by them
    let (st, _) = call(&s, "192.0.2.10", "GET", "/xrpc/com.atproto.server.describeServer", &[], None).await;
    assert_eq!(st, 200);
}
