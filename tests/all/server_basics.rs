//! Port of atproto/packages/pds/tests/server.test.ts: 404s, JSON input size
//! limit, response compression, health check; plus XRPC error envelope basics.
use crate::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preserves_404s() {
    let s = TestServer::spawn().await;
    let r = s.xrpc.send(s.xrpc.http.get(format!("{}/unknown", s.url))).await;
    assert_eq!(r.status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_xrpc_method_without_appview() {
    // no AppView configured: an unknown method is not implemented (not a 500)
    let s = TestServer::spawn().await;
    let r = s.xrpc.get("com.example.doesNotExist", &[], &Auth::None).await;
    assert!(
        r.status == 501 || r.status == 404 || r.status == 400,
        "unknown XRPC method should be a clean 4xx/501, got {}",
        r.text()
    );
    assert!(r.error_name().is_some(), "XRPC error envelope expected: {}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits_size_of_json_input() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let body = format!("\"{}\"", "x".repeat(150 * 1024));
    let r = s
        .xrpc
        .post_bytes("com.atproto.identity.updateHandle", body.into_bytes(), "application/json", &a.auth())
        .await;
    assert_eq!(r.status, 413, "150 KiB JSON body to updateHandle: {}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compresses_large_json_and_car_responses_only() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut rec = serde_json::Map::new();
    rec.insert("$type".into(), json!("app.bsky.feed.post"));
    rec.insert("text".into(), json!("blah"));
    rec.insert("createdAt".into(), json!(now_iso()));
    for i in 0..100 {
        rec.insert(format!("k{i:03}{}", unique_name("f")), json!(unique_name("v").repeat(4)));
    }
    let p = s.create_record(&a, "app.bsky.feed.post", J::Object(rec)).await;

    let get = |url: String| {
        let x = s.xrpc.clone();
        async move { x.send(x.http.get(url).header("accept-encoding", "gzip")).await }
    };
    let r = get(format!(
        "{}/xrpc/com.atproto.repo.getRecord?repo={}&collection={}&rkey={}",
        s.url,
        a.did,
        p.collection(),
        p.rkey()
    ))
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding").as_deref(), Some("gzip"), "large getRecord JSON not compressed");

    let r = get(format!("{}/xrpc/com.atproto.sync.getRepo?did={}", s.url, a.did)).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding").as_deref(), Some("gzip"), "getRepo CAR not compressed");

    let r = get(format!("{}/xrpc/_health", s.url)).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-encoding"), None, "tiny payload should not be compressed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthcheck() {
    let s = TestServer::spawn().await;
    let r = s.xrpc.get("_health", &[], &Auth::None).await;
    assert_eq!(r.status, 200);
    assert!(r.json["version"].is_string(), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_json_and_missing_params_are_400() {
    let s = TestServer::spawn().await;
    let a = s.create_account("bob").await;
    let r = s
        .xrpc
        .post_bytes("com.atproto.repo.createRecord", b"{not json".to_vec(), "application/json", &a.auth())
        .await;
    assert_eq!(r.status, 400, "malformed JSON: {}", r.text());
    let r = s.xrpc.get("com.atproto.repo.getRecord", &[("repo", &a.did)], &Auth::None).await;
    assert_eq!(r.status, 400, "missing required params: {}", r.text());
    assert!(r.error_name().is_some(), "XRPC error envelope expected: {}", r.text());
}

/// Wrong HTTP method on a local XRPC route: 400 InvalidRequest with an XRPC
/// body (reference "Incorrect HTTP method (X) expected Y"), not a bare 405.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incorrect_http_method_is_400() {
    let s = TestServer::spawn().await;
    let r = s.xrpc.post("com.atproto.repo.getRecord", &json!({}), &Auth::None).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("Incorrect HTTP method (POST) expected GET"), "{}", r.text());
    let r = s.xrpc.get("com.atproto.repo.createRecord", &[], &Auth::None).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("Incorrect HTTP method (GET) expected POST"), "{}", r.text());
}
