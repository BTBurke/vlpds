//! Rate limits (src/ratelimit.rs): the reference PDS's buckets and values
//! (packages/pds/src/rate-limits.ts + the handlers' `rateLimit` configs),
//! 429 `RateLimitExceeded` in the XRPC envelope, the `RateLimit-*` headers
//! of xrpc-server's HttpRateLimiter, repo-write points, and the bypasses.
use crate::common::*;

async fn limited() -> TestServer {
    TestServer::spawn_with(|c| c.rate_limits_enabled = true).await
}

fn num(r: &Resp, h: &str) -> i64 {
    r.header(h).unwrap_or_else(|| panic!("missing {h}: {:?}", r.headers)).parse().unwrap()
}

fn create(coll: &str, rkey: String) -> J {
    json!({"$type": "com.atproto.repo.applyWrites#create", "collection": coll, "rkey": rkey, "value": {"$type": coll, "i": 1}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn headers_on_every_xrpc_response() {
    let s = limited().await;
    let r1 = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
    r1.ok();
    // global-ip: 3000 points per 5 minutes
    assert_eq!(num(&r1, "ratelimit-limit"), 3000);
    assert_eq!(r1.header("ratelimit-policy").as_deref(), Some("3000;w=300"));
    let rem1 = num(&r1, "ratelimit-remaining");
    assert!(rem1 < 3000, "{rem1}");
    let now = chrono::Utc::now().timestamp();
    let reset = num(&r1, "ratelimit-reset");
    assert!(reset > now && reset <= now + 301, "reset {reset} vs now {now}");
    let r2 = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
    assert_eq!(num(&r2, "ratelimit-remaining"), rem1 - 1);
    // errors carry them too
    let r3 = s.get_record("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "app.bsky.feed.post", "x").await;
    r3.client_err();
    assert_eq!(num(&r3, "ratelimit-remaining"), rem1 - 2);
    // browsers can read them (CORS)
    let r = s.xrpc.send(s.xrpc.http.get(format!("{}/xrpc/com.atproto.server.describeServer", s.url)).header("origin", "https://example.com")).await;
    let expose = r.header("access-control-expose-headers").unwrap_or_default().to_ascii_lowercase();
    for h in ["ratelimit-limit", "ratelimit-remaining", "ratelimit-reset", "ratelimit-policy", "retry-after"] {
        assert!(expose.contains(h), "{h} not exposed: {expose}");
    }
    // non-XRPC paths are not limited
    let r = s.xrpc.send(s.xrpc.http.get(format!("{}/xrpc/_health", s.url))).await;
    assert!(r.header("ratelimit-limit").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_means_no_limits_or_headers() {
    let s = TestServer::spawn().await; // harness default: off
    let r = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
    r.ok();
    assert!(r.header("ratelimit-limit").is_none());
    for _ in 0..40 {
        let r = s.create_session("nobody.vlpds.test", "wrong").await;
        assert_eq!(r.status, 401, "{}", r.text());
    }
}

/// Case variants of one identifier share its createSession bucket (the key
/// is normalized as the OAuth sign-in's is); before, each variant of a
/// handle got a fresh 30 guesses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_session_identifier_variants_share_a_bucket() {
    let s = limited().await;
    let a = s.create_account("rlv").await;
    let variants = [a.handle.clone(), a.handle.to_uppercase(), format!(" {} ", a.handle), format!("@{}", a.handle)];
    for i in 0..30 {
        let r = s.create_session(&variants[i % variants.len()], "wrong").await;
        assert_eq!(r.status, 401, "attempt {i}: {}", r.text());
    }
    for v in &variants {
        s.create_session(v, "wrong").await.err(429, "RateLimitExceeded");
    }
    // another account's identifier is unaffected
    let b = s.create_account("rlv").await;
    s.create_session(&b.handle, PASSWORD).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_session_per_identifier_and_ip() {
    let s = limited().await;
    // 30 per 5 minutes per `${identifier}-${ip}` (plus 300/day)
    for i in 0..30 {
        let r = s.create_session("ghost.vlpds.test", "wrong").await;
        assert_eq!(r.status, 401, "attempt {i}: {}", r.text());
    }
    let r = s.create_session("ghost.vlpds.test", "wrong").await;
    r.err(429, "RateLimitExceeded");
    assert_eq!(num(&r, "ratelimit-limit"), 30);
    assert_eq!(num(&r, "ratelimit-remaining"), 0);
    assert_eq!(r.header("ratelimit-policy").as_deref(), Some("30;w=300"));
    let retry = num(&r, "retry-after");
    assert!((1..=300).contains(&retry), "retry-after {retry}");
    // another identifier has its own bucket
    let a = s.create_account("rl").await;
    s.create_session(&a.handle, PASSWORD).await.ok();
    // admin and internal (a peer's, on the peer listener) requests bypass
    let body = json!({"identifier": "ghost.vlpds.test", "password": "wrong"});
    let internal = peer_client().post(format!("{}/xrpc/com.atproto.server.createSession", s.peer_url)).json(&body);
    let r = s.xrpc.send(internal.header("x-vlpds-internal", ADMIN_TOKEN)).await;
    assert_eq!(r.status, 401, "internal bypass: {}", r.text());
    assert!(r.header("ratelimit-limit").is_none());
    let rb = s.xrpc.http.post(format!("{}/xrpc/com.atproto.server.createSession", s.url)).json(&body);
    // a client's copy of the header counts for nothing
    let r = s.xrpc.send(rb.try_clone().unwrap().header("x-vlpds-internal", ADMIN_TOKEN)).await;
    r.err(429, "RateLimitExceeded");
    use base64::Engine;
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("admin:{ADMIN_TOKEN}"));
    let r = s.xrpc.send(rb.try_clone().unwrap().header("authorization", format!("Basic {basic}"))).await;
    assert_eq!(r.status, 401, "admin bypass: {}", r.text());
    // a wrong internal token does not
    let r = s.xrpc.send(rb.header("x-vlpds-internal", "nope")).await;
    r.err(429, "RateLimitExceeded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repo_write_points_per_did() {
    let s = limited().await;
    let a = s.create_account("writer").await;
    let b = s.create_account("other").await;
    let coll = "com.example.rl";
    // repo-write-hour: 5000 points; create=3, update=2, delete=1
    for batch in 0..8 {
        let writes: Vec<J> = (0..200).map(|i| create(coll, format!("b{batch}k{i}"))).collect();
        s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "writes": writes}), &a.auth()).await.ok(); // 600 points each -> 4800
    }
    s.xrpc
        .post("com.atproto.repo.putRecord", &json!({"repo": a.did, "collection": coll, "rkey": "b0k0", "record": {"$type": coll, "i": 2}}), &a.auth())
        .await
        .ok(); // 4802
    s.xrpc.post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": coll, "rkey": "b0k1"}), &a.auth()).await.ok(); // 4803
    let writes: Vec<J> = (0..65).map(|i| create(coll, format!("c{i}"))).collect();
    let r = s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "writes": writes}), &a.auth()).await;
    r.ok(); // 4998
    assert_eq!(num(&r, "ratelimit-remaining"), 2, "tightest bucket is repo-write-hour: {:?}", r.headers);
    assert_eq!(r.header("ratelimit-policy").as_deref(), Some("5000;w=3600"));
    let before = s.latest_commit(&a.did).await;
    let r = s.xrpc.post("com.atproto.repo.createRecord", &json!({"repo": a.did, "collection": coll, "record": {"$type": coll}}), &a.auth()).await; // 5001
    r.err(429, "RateLimitExceeded");
    assert!(r.header("retry-after").is_some());
    assert_eq!(s.latest_commit(&a.did).await, before, "a rate-limited write must not commit");
    // another DID is unaffected
    s.create_record(&b, coll, json!({"$type": coll})).await;
    // reads are not write-limited
    s.list_records(&a.did, coll, &[]).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_ip_limit() {
    let s = limited().await;
    let a = s.create_account("glob").await;
    // spend the 3000-point global bucket from this IP
    let mut handles = Vec::new();
    for _ in 0..16 {
        let x = s.xrpc.clone();
        handles.push(tokio::spawn(async move {
            let mut last = 0u16;
            for _ in 0..200 {
                last = x.get("com.atproto.server.describeServer", &[], &Auth::None).await.status;
                if last == 429 {
                    break;
                }
            }
            last
        }));
    }
    let mut saw_429 = false;
    for h in handles {
        saw_429 |= h.await.unwrap() == 429;
    }
    assert!(saw_429, "3200 requests should exceed the 3000/5min global limit");
    let r = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
    r.err(429, "RateLimitExceeded");
    assert_eq!(num(&r, "ratelimit-limit"), 3000);
    assert_eq!(num(&r, "ratelimit-remaining"), 0);
    // proxied / unknown methods count too
    let r = s.xrpc.get("app.bsky.feed.getTimeline", &[], &a.auth()).await;
    r.err(429, "RateLimitExceeded");
    // sync.getRepo has its own 6000/5min bucket instead of the global one
    let r = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(num(&r, "ratelimit-limit"), 6000);
    // admin bypasses
    s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &a.did)], &Auth::Admin).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_for_only_from_trusted_proxies() {
    // untrusted peer: X-Forwarded-For is ignored, so spoofing it doesn't
    // get a fresh createSession bucket
    let s = limited().await;
    for i in 0..30 {
        let rb = s
            .xrpc
            .http
            .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
            .header("x-forwarded-for", format!("10.9.0.{i}"))
            .json(&json!({"identifier": "spoof.vlpds.test", "password": "x"}));
        assert_eq!(s.xrpc.send(rb).await.status, 401);
    }
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
        .header("x-forwarded-for", "10.9.1.1")
        .json(&json!({"identifier": "spoof.vlpds.test", "password": "x"}));
    s.xrpc.send(rb).await.err(429, "RateLimitExceeded");

    // trusted proxy (loopback): each forwarded client gets its own bucket
    let s = TestServer::spawn_with(|c| {
        c.rate_limits_enabled = true;
        c.trusted_proxies = vec!["127.0.0.1".into(), "::1".into()];
    })
    .await;
    for client in ["10.9.0.1", "10.9.0.2"] {
        for _ in 0..30 {
            let rb = s
                .xrpc
                .http
                .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
                .header("x-forwarded-for", client)
                .json(&json!({"identifier": "fwd.vlpds.test", "password": "x"}));
            assert_eq!(s.xrpc.send(rb).await.status, 401, "client {client}");
        }
    }
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
        .header("x-forwarded-for", "10.9.0.1")
        .json(&json!({"identifier": "fwd.vlpds.test", "password": "x"}));
    s.xrpc.send(rb).await.err(429, "RateLimitExceeded");
}

/// uploadBlob's per-IP daily budget (1000, as the reference) would stop an
/// account with more images than that from moving in on the same day: blobs
/// a deactivated account's repo references don't count; anything else does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_blob_budget_spares_blobs_an_arriving_repo_references() {
    let s = limited().await;
    let cfg = json!({"limiters": {"com.atproto.repo.uploadBlob-0": {"points": 3}}});
    s.xrpc.post("vlpds.admin.updateRateLimits", &json!({"config": cfg, "ifVersion": 0, "actor": "it-test"}), &Auth::Admin).await.ok();
    let up = |a: &TestAccount, bytes: Vec<u8>| {
        let (x, auth) = (s.xrpc.clone(), a.auth());
        async move { x.post_bytes("com.atproto.repo.uploadBlob", bytes, "image/png", &auth).await }
    };
    let img = |i: u8| [b"\x89PNG\r\n\x1a\n".as_slice(), &[i; 64]].concat();

    // an active account spends the budget on three images it posts
    let a = s.create_account("rlb").await;
    for i in 0..3 {
        let blob = up(&a, img(i)).await.ok()["blob"].clone();
        s.create_record(&a, "app.bsky.feed.post", image_post("pic", &blob)).await;
    }
    up(&a, img(9)).await.err(429, "RateLimitExceeded");

    // another account takes that repo in while deactivated, as a migration does
    let b = s.create_account("rlb").await;
    s.import_repo(&b.auth(), s.get_repo_car(&a.did).await).await.ok();
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &b.auth()).await.ok();
    for i in 0..3 {
        up(&b, img(i)).await.ok();
    }
    let missing = s.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &b.auth()).await.ok();
    assert_eq!(missing["blobs"], json!([]));
    // a blob its repo doesn't reference still counts
    up(&b, img(8)).await.err(429, "RateLimitExceeded");
}

/// The global per-IP limit (3000 per 5 minutes) would cap a migration's
/// blob copy: an arriving account's uploads of blobs its repo references
/// give their point back, so they never use it up; any other upload counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_ip_limit_spares_blobs_an_arriving_repo_references() {
    let s = limited().await;
    let up = |a: &TestAccount, bytes: Vec<u8>| {
        let (x, auth) = (s.xrpc.clone(), a.auth());
        async move { x.post_bytes("com.atproto.repo.uploadBlob", bytes, "image/png", &auth).await }
    };
    let img = |i: u8| [b"\x89PNG\r\n\x1a\n".as_slice(), &[i; 64]].concat();
    let a = s.create_account("rlg").await;
    for i in 0..3 {
        let blob = up(&a, img(i)).await.ok()["blob"].clone();
        s.create_record(&a, "app.bsky.feed.post", image_post("pic", &blob)).await;
    }
    let b = s.create_account("rlg").await;
    s.import_repo(&b.auth(), s.get_repo_car(&a.did).await).await.ok();
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &b.auth()).await.ok();

    // a new window length starts this IP's global window afresh
    const POINTS: i64 = 8;
    let cfg = json!({"limiters": {"global-ip": {"points": POINTS, "windowSecs": 3600}}});
    s.xrpc.post("vlpds.admin.updateRateLimits", &json!({"config": cfg, "ifVersion": 0, "actor": "it-test"}), &Auth::Admin).await.ok();

    for n in 0..3 * POINTS {
        let r = up(&b, img((n % 3) as u8)).await;
        assert_eq!(r.status, 200, "upload {n}: {}", r.text());
        assert_eq!(num(&r, "ratelimit-remaining"), POINTS, "upload {n}");
    }
    // a blob its repo doesn't reference counts
    let r = up(&b, img(8)).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(num(&r, "ratelimit-remaining"), POINTS - 1);
    // so does every upload by an active account, until the limit
    for n in 1..POINTS {
        assert_eq!(up(&a, img(100 + n as u8)).await.status, 200, "active upload {n}");
    }
    up(&a, img(120)).await.err(429, "RateLimitExceeded");
    // over it, nothing reaches a handler: the arriving account waits too
    up(&b, img(0)).await.err(429, "RateLimitExceeded");
}
