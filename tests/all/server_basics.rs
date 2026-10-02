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

/// `/.well-known/did.json` resolves a did:web service DID to this PDS; any
/// other service DID has no document here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_did_web_document() {
    let s = TestServer::spawn_with(|c| c.service_did = "did:web:pds.example.com".into()).await;
    let r = s.xrpc.send(s.xrpc.http.get(format!("{}/.well-known/did.json", s.url))).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        r.json,
        json!({
            "@context": ["https://www.w3.org/ns/did/v1"],
            "id": "did:web:pds.example.com",
            "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": s.url}],
        })
    );
    let s = TestServer::spawn_with(|c| c.service_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into()).await;
    let r = s.xrpc.send(s.xrpc.http.get(format!("{}/.well-known/did.json", s.url))).await;
    assert_eq!(r.status, 404);
}

/// `--disk-cache-mb` is split over the layout's shards and reaches every
/// shard's SlateDB disk cache (partition.rs tests check the settings).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_cache_budget_per_shard() {
    let dir = std::env::temp_dir().join(unique_name("vlpds-disk-cache"));
    let d = dir.clone();
    let s = TestServer::spawn_with(move |c| {
        c.cache_dir = Some(d);
        c.shards = 4;
        c.disk_cache_bytes = Some(1 << 30);
    })
    .await;
    let cache = s.app.node.shard_disk_cache().expect("disk cache configured");
    assert_eq!((cache.dir.as_path(), cache.shard_bytes), (dir.as_path(), 256 << 20));
    let a = s.create_account("dc").await;
    s.xrpc
        .post("com.atproto.repo.createRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("cached")}), &a.auth())
        .await
        .ok();
    vlpds::server::shutdown(&s.app).await;
    std::fs::remove_dir_all(&dir).ok();
    // unset: SlateDB's 16 GiB per shard; no cache dir, no cache
    let s = TestServer::spawn_with(|c| c.cache_dir = Some(std::env::temp_dir().join(unique_name("vlpds-disk-cache")))).await;
    assert_eq!(s.app.node.shard_disk_cache().unwrap().shard_bytes, 16 << 30);
    std::fs::remove_dir_all(&s.app.node.shard_disk_cache().unwrap().dir).ok();
    assert!(TestServer::spawn().await.app.node.shard_disk_cache().is_none());
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
