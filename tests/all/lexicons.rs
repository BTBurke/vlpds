//! Lexicon validation beyond bundled records (src/lexicon.rs): opt-in
//! dynamic resolution of third-party record lexicons, com.atproto.* params
//! and input validation with the reference's messages, and the debug-build
//! output check. Resolution goes through the real resolver; the DNS step is
//! replaced by `override_authority` and the lexicon is published by an
//! account on the test server (so it is read locally, no network).
use crate::common::*;
use std::time::{Duration, Instant};
use vlpds::oauth::lexicon::{nsid_authority, override_authority};

/// A fresh record NSID whose authority is unique to this test run, so the
/// process-wide resolution cache never leaks between tests.
fn fresh_nsid() -> String {
    format!("test.vlpds.{}.thing", unique_name("lx"))
}

fn thing_lexicon(nsid: &str) -> J {
    json!({
        "$type": "com.atproto.lexicon.schema",
        "lexicon": 1,
        "id": nsid,
        "defs": {
            "main": {"type": "record", "key": "tid", "record": {
                "type": "object",
                "required": ["text"],
                "properties": {
                    "text": {"type": "string", "maxLength": 10},
                    "sub": {"type": "ref", "ref": "#sub"},
                },
            }},
            "sub": {"type": "object", "required": ["n"], "properties": {"n": {"type": "integer", "minimum": 1}}},
        },
    })
}

/// Publishes `nsid`'s lexicon from a new account and points its authority
/// at that account.
async fn publish(s: &TestServer, nsid: &str) {
    let publisher = s.create_account("lexpub").await;
    s.xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": publisher.did, "collection": "com.atproto.lexicon.schema", "rkey": nsid,
                    "record": thing_lexicon(nsid), "validate": false}),
            &publisher.auth(),
        )
        .await
        .ok();
    override_authority(&nsid_authority(nsid), &publisher.did);
}

async fn create(s: &TestServer, a: &TestAccount, nsid: &str, record: J, validate: Option<bool>) -> Resp {
    let mut body = json!({"repo": a.did, "collection": nsid, "record": record});
    if let Some(v) = validate {
        body["validate"] = json!(v);
    }
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolved_lexicons_validate_third_party_records() {
    let s = TestServer::spawn_with(|c| c.resolve_lexicons = Some(Duration::from_secs(5))).await;
    let nsid = fresh_nsid();
    publish(&s, &nsid).await;
    let a = s.create_account("alice").await;

    let r = create(&s, &a, &nsid, json!({"text": "hi", "sub": {"n": 2}}), None).await;
    assert_eq!(r.ok()["validationStatus"], "valid");

    let r = create(&s, &a, &nsid, json!({"text": "far too long here"}), None).await;
    r.err(400, "InvalidRequest");
    assert_eq!(r.json["message"], format!("Invalid {nsid} record: record/text must not be longer than 10 characters"));
    // refs inside the resolved document are followed
    let r = create(&s, &a, &nsid, json!({"text": "hi", "sub": {"n": 0}}), None).await;
    r.err(400, "InvalidRequest");
    assert_eq!(r.json["message"], format!("Invalid {nsid} record: record/sub/n can not be less than 1"));
    // validate: false still skips validation entirely
    let r = create(&s, &a, &nsid, json!({"text": "far too long here"}), Some(false)).await;
    assert!(r.ok().get("validationStatus").is_none());

    // applyWrites uses the same (cached) schema
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.applyWrites",
            &json!({"repo": a.did, "writes": [
                {"$type": "com.atproto.repo.applyWrites#create", "collection": nsid, "value": {"text": "ok"}},
                {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post_record("x")},
            ]}),
            &a.auth(),
        )
        .await
        .ok();
    assert_eq!(r["results"][0]["validationStatus"], "valid");
    assert_eq!(r["results"][1]["validationStatus"], "valid");

    // without the flag the same type stays "unknown"
    let off = TestServer::spawn().await;
    let b = off.create_account("bob").await;
    let r = create(&off, &b, &nsid, json!({"text": "far too long here"}), None).await;
    assert_eq!(r.ok()["validationStatus"], "unknown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unresolvable_lexicons_are_unknown() {
    let s = TestServer::spawn_with(|c| c.resolve_lexicons = Some(Duration::from_secs(5))).await;
    let a = s.create_account("alice").await;
    // authority points at a local account that never published the lexicon
    let nsid = fresh_nsid();
    override_authority(&nsid_authority(&nsid), &a.did);
    for _ in 0..2 {
        // second round: served from the negative cache
        let r = create(&s, &a, &nsid, json!({"anything": 1}), None).await;
        assert_eq!(r.ok()["validationStatus"], "unknown");
    }
    let r = create(&s, &a, &nsid, json!({"anything": 1}), Some(true)).await;
    r.err(400, "InvalidRequest");
    assert_eq!(r.json["message"], format!("Unknown lexicon type: {nsid}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_resolution_does_not_stall_writes() {
    // A PLC directory that accepts connections and never answers.
    let plc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plc_url = format!("http://{}", plc.local_addr().unwrap());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((c, _)) = plc.accept().await {
            held.push(c);
        }
    });
    let s = TestServer::spawn_with(|c| {
        c.resolve_lexicons = Some(Duration::from_millis(200));
        c.plc_url = plc_url;
    })
    .await;
    let a = s.create_account("alice").await;
    let nsid = fresh_nsid();
    override_authority(&nsid_authority(&nsid), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
    let t = Instant::now();
    let r = create(&s, &a, &nsid, json!({"anything": 1}), None).await;
    assert_eq!(r.ok()["validationStatus"], "unknown");
    let r = create(&s, &a, &nsid, json!({"anything": 1}), Some(true)).await;
    r.err(400, "InvalidRequest");
    // both writes waited out the 200 ms bound (the resolution is still in
    // flight), not the resolver's own multi-second timeouts
    let waited = t.elapsed();
    assert!(waited >= Duration::from_millis(400) && waited < Duration::from_secs(2), "writes waited {waited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn procedure_inputs_are_validated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for (nsid, body, msg) in [
        ("com.atproto.repo.createRecord", json!({"repo": a.did, "record": {}}), "Input must have the property \"collection\""),
        ("com.atproto.repo.createRecord", json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": "x"}), "Input/record must be an object"),
        (
            "com.atproto.repo.createRecord",
            json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": {}, "validate": "yes"}),
            "Input/validate must be a boolean",
        ),
        ("com.atproto.repo.deleteRecord", json!({"repo": a.did, "collection": "not an nsid", "rkey": "x"}), "Input/collection must be a valid nsid"),
        (
            "com.atproto.repo.applyWrites",
            json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#nope"}]}),
            "Input/writes/0 $type must be one of #create, #update, #delete",
        ),
        (
            "com.atproto.repo.applyWrites",
            json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "value": {}}]}),
            "Input/writes/0 must have the property \"collection\"",
        ),
        ("com.atproto.server.createSession", json!({"identifier": a.handle}), "Input must have the property \"password\""),
        ("com.atproto.server.createSession", json!([1]), "Input must be an object"),
    ] {
        let r = s.xrpc.post(nsid, &body, &a.auth()).await;
        r.err(400, "InvalidRequest");
        assert_eq!(r.json["message"], msg, "{nsid} {body}");
    }
    // valid inputs still go through
    s.post(&a, "still fine").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_params_are_validated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for (q, msg) in [
        (vec![("repo", a.did.as_str())], "Params must have the property \"collection\""),
        (vec![("repo", a.did.as_str()), ("collection", "app.bsky.feed.post"), ("limit", "500")], "limit can not be greater than 100"),
        (vec![("repo", a.did.as_str()), ("collection", "app.bsky.feed.post"), ("reverse", "maybe")], "reverse must be a boolean"),
        (vec![("repo", a.did.as_str()), ("collection", "nope")], "collection must be a valid nsid"),
    ] {
        let r = s.xrpc.get("com.atproto.repo.listRecords", &q, &Auth::None).await;
        r.err(400, "InvalidRequest");
        assert_eq!(r.json["message"], msg, "{q:?}");
    }
    let r = s.xrpc.get("com.atproto.repo.describeRepo", &[], &Auth::None).await;
    r.err(400, "InvalidRequest");
    assert_eq!(r.json["message"], "Params must have the property \"repo\"");
    s.xrpc
        .get("com.atproto.repo.listRecords", &[("repo", &a.did), ("collection", "app.bsky.feed.post"), ("reverse", "true"), ("limit", "2")], &Auth::None)
        .await
        .ok();
}

/// Debug builds check handler outputs against the method's output schema.
#[cfg(debug_assertions)]
#[tokio::test]
async fn handler_outputs_are_checked_in_debug_builds() {
    use axum::routing::get;
    use tower::ServiceExt;
    use vlpds::xrpc::extract::{Json, debug_output_layer};

    let router = debug_output_layer(
        axum::Router::new()
            .route("/xrpc/com.atproto.repo.describeRepo", get(|| async { Json(json!({"handle": "x.test"})) }))
            .route("/xrpc/com.atproto.server.describeServer", get(|| async { Json(json!({"did": "did:web:x", "availableUserDomains": []})) }))
            // piped-through bodies (not built by a handler's Json) are not checked
            .route("/xrpc/com.atproto.repo.getRecord", get(|| async { axum::Json(json!({})) })),
    );
    let call = |path: &'static str| {
        let router = router.clone();
        async move {
            let req = axum::http::Request::get(path).body(axum::body::Body::empty()).unwrap();
            let resp = router.oneshot(req).await.unwrap();
            let status = resp.status().as_u16();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice::<J>(&body).unwrap())
        }
    };
    let (status, body) = call("/xrpc/com.atproto.repo.describeRepo").await;
    assert_eq!(status, 500);
    assert_eq!(body["message"], "Invalid com.atproto.repo.describeRepo output: Output must have the property \"did\"");
    assert_eq!(call("/xrpc/com.atproto.server.describeServer").await.0, 200);
    assert_eq!(call("/xrpc/com.atproto.repo.getRecord").await.0, 200);
}
