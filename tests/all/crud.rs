//! Port of atproto/packages/pds/tests/crud.test.ts: record CRUD, listRecords
//! pagination, putRecord semantics, compare-and-swap, applyWrites atomicity,
//! rkey/collection/$type rules and data-model round trips.
//! (bsky-specific duplicate-like/follow pruning and takedowns live elsewhere.)
use crate::common::*;

async fn setup() -> (TestServer, TestAccount) {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    (s, a)
}

async fn create(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    let mut body = body;
    body["repo"] = json!(a.did);
    s.xrpc
        .post("com.atproto.repo.createRecord", &body, &a.auth())
        .await
}

async fn put(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    let mut body = body;
    if body.get("repo").is_none() {
        body["repo"] = json!(a.did);
    }
    s.xrpc
        .post("com.atproto.repo.putRecord", &body, &a.auth())
        .await
}

async fn delete(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    let mut body = body;
    body["repo"] = json!(a.did);
    s.xrpc
        .post("com.atproto.repo.deleteRecord", &body, &a.auth())
        .await
}

async fn apply(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    let mut body = body;
    body["repo"] = json!(a.did);
    s.xrpc
        .post("com.atproto.repo.applyWrites", &body, &a.auth())
        .await
}

fn profile(n: &str) -> J {
    json!({"$type": "app.bsky.actor.profile", "displayName": n})
}

/// Every record in a collection, following cursors.
async fn list_all(s: &TestServer, did: &str, coll: &str, limit: usize, reverse: bool) -> Vec<J> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let lim = limit.to_string();
    for _ in 0..100 {
        let mut q = vec![("limit", lim.as_str())];
        if reverse {
            q.push(("reverse", "true"));
        }
        if let Some(c) = &cursor {
            q.push(("cursor", c.as_str()));
        }
        let j = s.list_records(did, coll, &q).await.ok();
        let recs = j["records"].as_array().unwrap().clone();
        assert!(recs.len() <= limit, "page larger than limit");
        let n = recs.len();
        if let Some(c) = j["cursor"].as_str() {
            // cursor, when present, is the last record's rkey
            assert_eq!(
                Some(c),
                recs.last()
                    .and_then(|r| r["uri"].as_str())
                    .map(|u| u.rsplit('/').next().unwrap())
            );
        }
        out.extend(recs);
        cursor = j["cursor"].as_str().map(String::from);
        if cursor.is_none() || n == 0 {
            return out;
        }
    }
    panic!("pagination did not terminate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registers_and_describes_repo() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    assert_ne!(a.did, b.did);
    assert!(a.did.starts_with("did:"));
    assert!(!a.access.is_empty());
    let d = s
        .xrpc
        .get(
            "com.atproto.repo.describeRepo",
            &[("repo", &a.did)],
            &Auth::None,
        )
        .await
        .ok();
    assert_eq!(d["handle"], json!(a.handle));
    assert_eq!(d["did"], json!(a.did));
    assert_eq!(d["handleIsCorrect"], json!(true));
    assert_eq!(d["didDoc"]["id"], json!(a.did));
    // by handle
    let d = s
        .xrpc
        .get(
            "com.atproto.repo.describeRepo",
            &[("repo", &b.handle)],
            &Auth::None,
        )
        .await
        .ok();
    assert_eq!(d["did"], json!(b.did));
    // collections reflect the repo's contents
    s.post(&a, "hi").await;
    let d = s
        .xrpc
        .get(
            "com.atproto.repo.describeRepo",
            &[("repo", &a.did)],
            &Auth::None,
        )
        .await
        .ok();
    assert_eq!(
        d["collections"],
        json!(["app.bsky.feed.post"]),
        "describeRepo.collections"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_gets_lists_and_deletes_records() {
    let (s, a) = setup().await;
    let r = create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "record": post_record("Hello, world!")}),
    )
    .await
    .ok();
    let rec = RecordRef::from_json(&r);
    assert!(
        rec.uri
            .starts_with(&format!("at://{}/app.bsky.feed.post/", a.did)),
        "{}",
        rec.uri
    );
    assert!(
        is_tid(rec.rkey()),
        "generated rkey should be a TID: {}",
        rec.rkey()
    );
    assert!(Cid::parse(&rec.cid).is_ok());
    let (head, rev) = s.latest_commit(&a.did).await;
    assert_eq!(
        rec.commit_cid.as_deref(),
        Some(head.to_string().as_str()),
        "createRecord commit.cid"
    );
    assert_eq!(
        rec.rev.as_deref(),
        Some(rev.as_str()),
        "createRecord commit.rev"
    );

    let l = s.list_records(&a.did, "app.bsky.feed.post", &[]).await.ok();
    let recs = l["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["uri"], json!(rec.uri));
    assert_eq!(recs[0]["cid"], json!(rec.cid));
    assert_eq!(recs[0]["value"]["text"], json!("Hello, world!"));

    let g = s
        .get_record(&a.did, "app.bsky.feed.post", rec.rkey())
        .await
        .ok();
    assert_eq!(g["uri"], json!(rec.uri));
    assert_eq!(g["cid"], json!(rec.cid));
    assert_eq!(g["value"]["text"], json!("Hello, world!"));
    assert_eq!(g["value"]["$type"], json!("app.bsky.feed.post"));

    // repo by handle works for reads
    let g2 = s
        .get_record(&a.handle, "app.bsky.feed.post", rec.rkey())
        .await
        .ok();
    assert_eq!(g2["cid"], json!(rec.cid));

    // getRecord pinned to the right cid works, a wrong cid is RecordNotFound
    let ok = s
        .xrpc
        .get(
            "com.atproto.repo.getRecord",
            &[
                ("repo", &a.did),
                ("collection", "app.bsky.feed.post"),
                ("rkey", rec.rkey()),
                ("cid", &rec.cid),
            ],
            &Auth::None,
        )
        .await;
    ok.ok();
    let other = Cid::dag_cbor(b"nope").to_string();
    let bad = s
        .xrpc
        .get(
            "com.atproto.repo.getRecord",
            &[
                ("repo", &a.did),
                ("collection", "app.bsky.feed.post"),
                ("rkey", rec.rkey()),
                ("cid", &other),
            ],
            &Auth::None,
        )
        .await;
    bad.err(400, "RecordNotFound");

    let d = delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": rec.rkey()}),
    )
    .await
    .ok();
    assert!(
        d["commit"]["cid"].is_string(),
        "deleteRecord returns commit: {d}"
    );
    let l = s.list_records(&a.did, "app.bsky.feed.post", &[]).await.ok();
    assert_eq!(l["records"].as_array().unwrap().len(), 0);
    let g = s.get_record(&a.did, "app.bsky.feed.post", rec.rkey()).await;
    g.err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_requires_auth_and_matching_repo() {
    let (s, a) = setup().await;
    let b = s.create_account("bob").await;
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x")}),
            &Auth::None,
        )
        .await;
    r.err(401, "AuthenticationRequired");
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x")}),
            &Auth::Bearer("garbage.token.here".into()),
        )
        .await;
    // reference auth-verifier: an unverifiable JWT is InvalidRequestError('Token could not be verified', 'InvalidToken') -> 400
    assert!(
        (r.status, r.error_name()) == (400, Some("InvalidToken")) || r.status == 401,
        "garbage bearer token: {}",
        r.text()
    );
    // putRecord into someone else's repo fails, and leaves it untouched
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.putRecord",
            &json!({"repo": b.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": profile("evil")}),
            &a.auth(),
        )
        .await;
    r.client_err();
    s.get_record(&b.did, "app.bsky.actor.profile", "self")
        .await
        .err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_missing_record_is_a_noop() {
    let (s, a) = setup().await;
    let rec = s.post(&a, "post").await;
    delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": rec.rkey()}),
    )
    .await
    .ok();
    s.get_record(&a.did, "app.bsky.feed.post", rec.rkey())
        .await
        .err(400, "RecordNotFound");
    let before = s.latest_commit(&a.did).await;
    let r = delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": rec.rkey()}),
    )
    .await;
    r.ok();
    assert_eq!(
        s.latest_commit(&a.did).await,
        before,
        "deleting a missing record must not create a commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_keeps_block_referenced_elsewhere() {
    let (s, a) = setup().await;
    let record = json!({"$type": "app.bsky.feed.post", "text": "post", "createdAt": "2026-01-01T00:00:00.000Z"});
    let p1 = RecordRef::from_json(
        &create(
            &s,
            &a,
            json!({"collection": "app.bsky.feed.post", "record": record}),
        )
        .await
        .ok(),
    );
    let p2 = RecordRef::from_json(
        &create(
            &s,
            &a,
            json!({"collection": "app.bsky.feed.post", "record": record}),
        )
        .await
        .ok(),
    );
    assert_eq!(p1.cid, p2.cid, "identical records share a cid");
    delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": p1.rkey()}),
    )
    .await
    .ok();
    let g = s
        .get_record(&a.did, "app.bsky.feed.post", p2.rkey())
        .await
        .ok();
    assert_eq!(g["value"], record);
    // and the exported repo still contains the block
    let repo = s.get_repo(&a.did).await;
    assert_eq!(
        repo.record(&format!("app.bsky.feed.post/{}", p2.rkey())),
        Some(record)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_creates_then_updates() {
    let (s, a) = setup().await;
    s.get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .err(400, "RecordNotFound");
    let p = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "record": {"displayName": "Robert"}})).await.ok();
    assert_eq!(
        p["uri"],
        json!(format!("at://{}/app.bsky.actor.profile/self", a.did))
    );
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(
        g["value"],
        json!({"$type": "app.bsky.actor.profile", "displayName": "Robert"}),
        "putRecord should default $type"
    );

    let p2 = put(
        &s,
        &a,
        json!({"collection": "app.bsky.actor.profile", "rkey": "self", "record": {"$type": "app.bsky.actor.profile", "displayName": "Robert", "description": "Dog lover"}}),
    )
    .await
    .ok();
    assert_ne!(p2["cid"], p["cid"]);
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(
        g["value"],
        json!({"$type": "app.bsky.actor.profile", "displayName": "Robert", "description": "Dog lover"})
    );
    assert_eq!(g["cid"], p2["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_by_handle() {
    let (s, a) = setup().await;
    let rkey = "3jzfcijpj2z2a";
    put(
        &s,
        &a,
        json!({"repo": a.handle, "collection": "app.bsky.graph.follow", "rkey": rkey, "record": {"$type": "app.bsky.graph.follow", "subject": "did:plc:abc", "createdAt": now_iso()}}),
    )
    .await
    .ok();
    s.get_record(&a.did, "app.bsky.graph.follow", rkey)
        .await
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_noop_does_not_commit() {
    let (s, a) = setup().await;
    let body =
        json!({"collection": "app.bsky.actor.profile", "rkey": "self", "record": profile("same")});
    let p1 = put(&s, &a, body.clone()).await.ok();
    let before = s.latest_commit(&a.did).await;
    let p2 = put(&s, &a, body).await.ok();
    assert_eq!(p1["uri"], p2["uri"]);
    assert_eq!(p1["cid"], p2["cid"]);
    assert_eq!(
        s.latest_commit(&a.did).await,
        before,
        "no-op putRecord must not produce a commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn defaults_undefined_type() {
    let (s, a) = setup().await;
    let r = RecordRef::from_json(
        &create(&s, &a, json!({"collection": "app.bsky.feed.post", "record": {"text": "no type", "createdAt": now_iso()}})).await.ok(),
    );
    let g = s
        .get_record(&a.did, "app.bsky.feed.post", r.rkey())
        .await
        .ok();
    assert_eq!(g["value"]["$type"], json!("app.bsky.feed.post"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profile_gets_self_rkey() {
    // app.bsky.actor.profile has key literal:self. In the reference test
    // (crud.test.ts "creates records with the correct key described by the
    // schema") the *client* (`agent.app.bsky.actor.profile.create`) fills in
    // rkey "self"; the server validates the key against the schema
    // (repo/prepare.ts validateRecord: `schema.keySchema.safeValidate(rkey)`),
    // so a createRecord without an rkey gets a TID and is rejected.
    let (s, a) = setup().await;
    let r = create(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "record": {"displayName": "alice", "createdAt": now_iso()}})).await.ok();
    assert_eq!(
        RecordRef::from_json(&r).rkey(),
        "self",
        "app.bsky.actor.profile has key literal:self"
    );
    assert_eq!(r["validationStatus"], json!("valid"));
    create(&s, &a, json!({"collection": "app.bsky.actor.profile", "record": {"displayName": "alice"}}))
        .await
        .err(400, "InvalidRequest");
    put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "other", "record": {"displayName": "alice"}}))
        .await
        .err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_type_to_match_collection() {
    let (s, a) = setup().await;
    let r = create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.like"}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // also when unvalidated / unknown lexicon
    let r = create(&s, &a, json!({"collection": "com.example.record", "record": {"$type": "com.example.other", "blah": "thing"}})).await;
    r.err(400, "InvalidRequest");
    let r = put(&s, &a, json!({"collection": "com.example.record", "rkey": "x", "record": {"$type": "com.example.other"}})).await;
    r.err(400, "InvalidRequest");
    let r = apply(
        &s,
        &a,
        json!({"writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "value": {"$type": "com.example.other"}}]}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // $type must be a non-empty string when present
    for t in [json!(null), json!(123), json!("")] {
        let r = create(
            &s,
            &a,
            json!({"collection": "com.example.record", "record": {"$type": t, "a": 1}}),
        )
        .await;
        r.err(400, "InvalidRequest");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_valid_rkey() {
    let (s, a) = setup().await;
    let long = "o".repeat(513);
    let bad = [
        ".",
        "..",
        "a/b",
        "with space",
        "#extra",
        "@handle",
        "number[3]",
        "number(3)",
        "\"quote\"",
        "dHJ1ZQ==",
        long.as_str(),
    ];
    for rk in bad {
        let r = create(
            &s,
            &a,
            json!({"collection": "com.example.record", "rkey": rk, "record": {"a": 1}}),
        )
        .await;
        r.err(400, "InvalidRequest");
        let r = put(
            &s,
            &a,
            json!({"collection": "com.example.record", "rkey": rk, "record": {"a": 1}}),
        )
        .await;
        r.err(400, "InvalidRequest");
        let r = apply(
            &s,
            &a,
            json!({"writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": rk, "value": {"a": 1}}]}),
        )
        .await;
        r.err(400, "InvalidRequest");
    }
    // nothing was written
    let l = s.list_records(&a.did, "com.example.record", &[]).await.ok();
    assert_eq!(l["records"].as_array().unwrap().len(), 0);
    // valid record keys from the interop fixtures are accepted
    for rk in fixture_lines("interop/syntax/recordkey_syntax_valid.txt") {
        let r = put(
            &s,
            &a,
            json!({"collection": "com.example.record", "rkey": rk, "record": {"a": 1}}),
        )
        .await;
        assert!(r.is_ok(), "valid rkey {rk:?} rejected: {}", r.text());
        s.get_record(&a.did, "com.example.record", &rk).await.ok();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_valid_collection_nsid() {
    let (s, a) = setup().await;
    for c in [
        "app.bsky",
        "example.com",
        "one.two..three",
        "com.example.foo.*",
        "not an nsid",
        "com.atproto.feed.p@st",
        "a/b.c.d",
    ] {
        let r = create(&s, &a, json!({"collection": c, "record": {"a": 1}})).await;
        assert_eq!(
            (r.status, r.error_name()),
            (400, Some("InvalidRequest")),
            "collection {c:?}: {}",
            r.text()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unvalidated_writes_of_unknown_lexicons() {
    let (s, a) = setup().await;
    // validate unset: allowed, status unknown
    let r = create(&s, &a, json!({"collection": "com.example.record", "record": {"$type": "com.example.record", "blah": "thing"}})).await.ok();
    assert_eq!(r["validationStatus"], json!("unknown"));
    let g = s
        .get_record(
            &a.did,
            "com.example.record",
            RecordRef::from_json(&r).rkey(),
        )
        .await
        .ok();
    assert_eq!(
        g["value"],
        json!({"$type": "com.example.record", "blah": "thing"})
    );
    // validate=false: allowed, no validation status
    let r = create(&s, &a, json!({"collection": "com.example.record", "validate": false, "record": {"$type": "com.example.record", "blah": "thing2"}})).await.ok();
    assert!(
        r.get("validationStatus")
            .map(|v| v.is_null())
            .unwrap_or(true),
        "validate=false => no validationStatus: {r}"
    );
    // validate=true on an unknown lexicon: rejected, mentioning the NSID
    let r = create(&s, &a, json!({"collection": "com.example.foobar", "validate": true, "record": {"$type": "com.example.foobar"}})).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("com.example.foobar"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validates_known_records_on_write() {
    let (s, a) = setup().await;
    // missing required "text"
    let r = create(&s, &a, json!({"collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "createdAt": now_iso()}})).await;
    r.err(400, "InvalidRequest");
    // datetimes are validated rigorously
    let r = create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "text": "test", "createdAt": "0000-00-12T23:20:50.123Z"}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // valid post: validationStatus valid
    let r = create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "record": post_record("ok")}),
    )
    .await
    .ok();
    assert_eq!(r["validationStatus"], json!("valid"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_legacy_blob_refs_and_bad_values() {
    let (s, a) = setup().await;
    let up = s
        .xrpc
        .post_bytes(
            "com.atproto.repo.uploadBlob",
            PNG_1X1.to_vec(),
            "image/png",
            &a.auth(),
        )
        .await
        .ok();
    let cid = up["blob"]["ref"]["$link"].as_str().unwrap().to_string();
    let r = create(
        &s,
        &a,
        json!({"collection": "com.example.record", "validate": false, "record": {"blah": "thing", "image": {"cid": cid, "mimeType": "image/png"}}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // floats are not part of the data model
    let r = create(
        &s,
        &a,
        json!({"collection": "com.example.record", "record": {"a": 1.5}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // blob with a string size is malformed
    let r = create(
        &s,
        &a,
        json!({"collection": "com.example.record", "record": {"b": {"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/png", "size": "10"}}}),
    )
    .await;
    r.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_values_too_deep_for_cbor() {
    let (s, a) = setup().await;
    // 4000 levels of nesting (built as raw JSON text; serde_json can't build it)
    let mut deep = String::new();
    for _ in 0..4000 {
        deep.push_str("{\"x\":");
    }
    deep.push('1');
    for _ in 0..4000 {
        deep.push('}');
    }
    let body = format!(
        r#"{{"repo":"{}","collection":"app.bsky.feed.post","record":{{"text":"x","createdAt":"{}","deepObject":{deep}}}}}"#,
        a.did,
        now_iso()
    );
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.repo.createRecord", s.url))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", a.access))
        .body(body);
    let r = s.xrpc.send(rb).await;
    r.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_model_values_round_trip() {
    let (s, a) = setup().await;
    let link = Cid::dag_cbor(b"something").to_string();
    let record = json!({
        "$type": "com.example.kitchen",
        "bytes": {"$bytes": "nFERjvLLiw9qm45JrqH9QTzyC2Lu1Xb4ne6+sBrCzI0"},
        "link": {"$link": link},
        "nested": {"arr": [1, -2, true, null, "s", {"k": []}], "empty": {}},
        "unicode": "a~öñ©⽘☎𓋓😀👨‍👩‍👧‍👧",
        "big": 9007199254740991i64,
        "neg": -9007199254740991i64,
    });
    let r = create(
        &s,
        &a,
        json!({"collection": "com.example.kitchen", "record": record}),
    )
    .await
    .ok();
    let rec = RecordRef::from_json(&r);
    let g = s
        .get_record(&a.did, "com.example.kitchen", rec.rkey())
        .await
        .ok();
    assert_eq!(g["value"], record);
    // the CID is the DAG-CBOR hash of the record
    let v = Value::from_json(&record).unwrap();
    assert_eq!(rec.cid, Cid::dag_cbor(&v.to_cbor()).to_string());
    // and the exported repo block equals that encoding
    let repo = s.get_repo(&a.did).await;
    assert_eq!(
        repo.blocks.get(&Cid::parse(&rec.cid).unwrap()),
        Some(&v.to_cbor())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_repo_errors() {
    let (s, a) = setup().await;
    let ghost = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    s.get_record(ghost, "app.bsky.feed.post", "3jzfcijpj2z2a")
        .await
        .err_status(400);
    s.list_records(ghost, "app.bsky.feed.post", &[])
        .await
        .err_status(400);
    s.xrpc
        .get(
            "com.atproto.repo.describeRepo",
            &[("repo", ghost)],
            &Auth::None,
        )
        .await
        .err_status(400);
    s.get_record("nobody.vlpds.test", "app.bsky.feed.post", "3jzfcijpj2z2a")
        .await
        .err_status(400);
    // writing to another (nonexistent) repo with your token
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": ghost, "collection": "app.bsky.feed.post", "record": post_record("x")}),
            &a.auth(),
        )
        .await;
    r.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_create_fails() {
    let (s, a) = setup().await;
    let rk = "3jzfcijpj2z2a";
    create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": rk, "record": post_record("one")}),
    )
    .await
    .ok();
    let r = create(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": rk, "record": post_record("two")}),
    )
    .await;
    r.client_err();
    let g = s.get_record(&a.did, "app.bsky.feed.post", rk).await.ok();
    assert_eq!(g["value"]["text"], json!("one"));
}

// ---------------------------------------------------------------------------
// pagination
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paginates_list_records() {
    let (s, a) = setup().await;
    let mut uris = Vec::new();
    for i in 0..5 {
        uris.push(s.post(&a, &format!("post {i}")).await.uri);
    }
    let full = s.list_records(&a.did, "app.bsky.feed.post", &[]).await.ok();
    let full_recs = full["records"].as_array().unwrap().clone();
    assert_eq!(full_recs.len(), 5);
    // default order is newest-first (descending rkey)
    let got: Vec<&str> = full_recs
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    let mut want: Vec<&str> = uris.iter().map(|s| s.as_str()).collect();
    want.reverse();
    assert_eq!(got, want);

    // forwards, 2 at a time
    let paged = list_all(&s, &a.did, "app.bsky.feed.post", 2, false).await;
    assert_eq!(paged, full_recs);

    // reverse, 2 at a time
    let paged_rev = list_all(&s, &a.did, "app.bsky.feed.post", 2, true).await;
    let mut full_rev = full_recs.clone();
    full_rev.reverse();
    assert_eq!(paged_rev, full_rev);

    // reverse=true on one page is the exact reverse
    let rev = s
        .list_records(&a.did, "app.bsky.feed.post", &[("reverse", "true")])
        .await
        .ok();
    assert_eq!(rev["records"].as_array().unwrap(), &full_rev);
    if let Some(c) = full["cursor"].as_str() {
        assert_eq!(c, uris[0].rsplit('/').next().unwrap());
    }
    if let Some(c) = rev["cursor"].as_str() {
        assert_eq!(c, uris[4].rsplit('/').next().unwrap());
    }

    // other collections are not included; an empty collection lists nothing
    s.create_record(&a, "app.bsky.feed.like", json!({"$type": "app.bsky.feed.like", "subject": {"uri": uris[0], "cid": full_recs[0]["cid"]}, "createdAt": now_iso()}))
        .await;
    assert_eq!(
        s.list_records(&a.did, "app.bsky.feed.post", &[]).await.ok()["records"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        s.list_records(&a.did, "app.bsky.feed.repost", &[])
            .await
            .ok()["records"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_records_limit_bounds() {
    let (s, a) = setup().await;
    for i in 0..3 {
        s.post(&a, &format!("p{i}")).await;
    }
    // lexicon: limit 1..=100
    s.list_records(&a.did, "app.bsky.feed.post", &[("limit", "0")])
        .await
        .err(400, "InvalidRequest");
    s.list_records(&a.did, "app.bsky.feed.post", &[("limit", "101")])
        .await
        .err(400, "InvalidRequest");
    let one = s
        .list_records(&a.did, "app.bsky.feed.post", &[("limit", "1")])
        .await
        .ok();
    assert_eq!(one["records"].as_array().unwrap().len(), 1);
    assert!(one["cursor"].is_string());
}

// ---------------------------------------------------------------------------
// compare-and-swap
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_record_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    let r = create(&s, &a, json!({"collection": "app.bsky.feed.post", "swapCommit": head.to_string(), "record": post_record("cas ok")})).await.ok();
    s.get_record(
        &a.did,
        "app.bsky.feed.post",
        RecordRef::from_json(&r).rkey(),
    )
    .await
    .ok();
    // head is now stale
    let rk = "3jzfcijpj2z2b";
    let r = create(&s, &a, json!({"collection": "app.bsky.feed.post", "rkey": rk, "swapCommit": head.to_string(), "record": post_record("cas bad")})).await;
    r.err(400, "InvalidSwap");
    s.get_record(&a.did, "app.bsky.feed.post", rk)
        .await
        .err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_record_swap_commit_and_record() {
    let (s, a) = setup().await;
    // proper commit cas
    let p = s.post(&a, "p1").await;
    let (head, _) = s.latest_commit(&a.did).await;
    delete(&s, &a, json!({"collection": "app.bsky.feed.post", "rkey": p.rkey(), "swapCommit": head.to_string()})).await.ok();
    s.get_record(&a.did, "app.bsky.feed.post", p.rkey())
        .await
        .err(400, "RecordNotFound");
    // bad commit cas
    let (stale, _) = s.latest_commit(&a.did).await;
    let p = s.post(&a, "p2").await;
    let r = delete(&s, &a, json!({"collection": "app.bsky.feed.post", "rkey": p.rkey(), "swapCommit": stale.to_string()})).await;
    r.err(400, "InvalidSwap");
    s.get_record(&a.did, "app.bsky.feed.post", p.rkey())
        .await
        .ok();
    // proper record cas
    delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": p.rkey(), "swapRecord": p.cid}),
    )
    .await
    .ok();
    s.get_record(&a.did, "app.bsky.feed.post", p.rkey())
        .await
        .err(400, "RecordNotFound");
    // bad record cas
    let p = s.post(&a, "p3").await;
    let wrong = Cid::dag_cbor(&Value::Map(vec![]).to_cbor()).to_string();
    let r = delete(
        &s,
        &a,
        json!({"collection": "app.bsky.feed.post", "rkey": p.rkey(), "swapRecord": wrong}),
    )
    .await;
    r.err(400, "InvalidSwap");
    s.get_record(&a.did, "app.bsky.feed.post", p.rkey())
        .await
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    let p = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapCommit": head.to_string(), "record": profile("a1")})).await.ok();
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(g["cid"], p["cid"]);
    let r = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapCommit": head.to_string(), "record": profile("a2")})).await;
    r.err(400, "InvalidSwap");
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(g["cid"], p["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_swap_record() {
    let (s, a) = setup().await;
    // swapRecord: null => the record must not exist (create)
    let p1 = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": null, "record": profile("b1")})).await.ok();
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(g["cid"], p1["cid"]);
    // swapRecord: cid => update
    let p2 = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": p1["cid"], "record": profile("b2")})).await.ok();
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(g["cid"], p2["cid"]);
    // swapRecord: null now fails (record exists)
    let r = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": null, "record": profile("b3")})).await;
    r.err(400, "InvalidSwap");
    // swapRecord: wrong cid fails
    let wrong = Cid::dag_cbor(&Value::Map(vec![]).to_cbor()).to_string();
    let r = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": wrong, "record": profile("b4")})).await;
    r.err(400, "InvalidSwap");
    // stale (previous) cid fails
    let r = put(&s, &a, json!({"collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": p1["cid"], "record": profile("b5")})).await;
    r.err(400, "InvalidSwap");
    let g = s
        .get_record(&a.did, "app.bsky.actor.profile", "self")
        .await
        .ok();
    assert_eq!(g["cid"], p2["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    apply(
        &s,
        &a,
        json!({"swapCommit": head.to_string(), "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post_record("aw")}]}),
    )
    .await
    .ok();
    let r = apply(
        &s,
        &a,
        json!({"swapCommit": head.to_string(), "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2c", "value": post_record("aw2")}]}),
    )
    .await;
    r.err(400, "InvalidSwap");
    s.get_record(&a.did, "app.bsky.feed.post", "3jzfcijpj2z2c")
        .await
        .err(400, "RecordNotFound");
}

// ---------------------------------------------------------------------------
// applyWrites
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_mixed_batch_is_one_commit() {
    let (s, a) = setup().await;
    let p = s.post(&a, "to update").await;
    let d = s.post(&a, "to delete").await;
    let r = apply(
        &s,
        &a,
        json!({"writes": [
            {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2d", "value": post_record("new")},
            {"$type": "com.atproto.repo.applyWrites#update", "collection": "app.bsky.feed.post", "rkey": p.rkey(), "value": post_record("updated")},
            {"$type": "com.atproto.repo.applyWrites#delete", "collection": "app.bsky.feed.post", "rkey": d.rkey()},
        ]}),
    )
    .await
    .ok();
    let res = r["results"].as_array().expect("results");
    assert_eq!(res.len(), 3);
    assert_eq!(
        res[0]["$type"],
        json!("com.atproto.repo.applyWrites#createResult")
    );
    assert_eq!(
        res[0]["uri"],
        json!(format!("at://{}/app.bsky.feed.post/3jzfcijpj2z2d", a.did))
    );
    assert_eq!(
        res[1]["$type"],
        json!("com.atproto.repo.applyWrites#updateResult")
    );
    assert_eq!(
        res[2]["$type"],
        json!("com.atproto.repo.applyWrites#deleteResult")
    );
    let (head, rev) = s.latest_commit(&a.did).await;
    assert_eq!(r["commit"]["cid"], json!(head.to_string()));
    assert_eq!(r["commit"]["rev"], json!(rev));
    assert_eq!(
        s.get_record(&a.did, "app.bsky.feed.post", "3jzfcijpj2z2d")
            .await
            .ok()["cid"],
        res[0]["cid"]
    );
    assert_eq!(
        s.get_record(&a.did, "app.bsky.feed.post", p.rkey())
            .await
            .ok()["value"]["text"],
        json!("updated")
    );
    s.get_record(&a.did, "app.bsky.feed.post", d.rkey())
        .await
        .err(400, "RecordNotFound");
    // all three ops landed in one #commit
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub
        .until(FH_TIMEOUT, |fs| {
            fs.iter()
                .any(|f| f.commit().map(|c| c.rev == rev).unwrap_or(false))
        })
        .await;
    let c = frames
        .iter()
        .filter_map(|f| f.commit())
        .find(|c| c.rev == rev)
        .unwrap();
    assert_eq!(c.ops.len(), 3);
    let mut actions: Vec<_> = c.ops.iter().map(|o| o.action.as_str()).collect();
    actions.sort();
    assert_eq!(actions, vec!["create", "delete", "update"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_is_atomic() {
    let (s, a) = setup().await;
    let existing = s.post(&a, "exists").await;
    let before = s.latest_commit(&a.did).await;
    let good = json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2e", "value": post_record("good")});
    let bad_batches = [
        // duplicate create of an existing key
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "rkey": existing.rkey(), "value": post_record("dupe")}),
        // invalid rkey
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "rkey": "..", "value": post_record("x")}),
        // $type mismatch
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": {"$type": "app.bsky.feed.like"}}),
        // update of a missing record
        json!({"$type": "com.atproto.repo.applyWrites#update", "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2f", "value": post_record("x")}),
        // unknown write type
        json!({"$type": "com.atproto.repo.applyWrites#explode", "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2g"}),
    ];
    for bad in bad_batches {
        let r = apply(&s, &a, json!({"writes": [good.clone(), bad.clone()]})).await;
        assert!(
            (400..500).contains(&r.status),
            "batch with {bad} should fail with 4xx: {}",
            r.text()
        );
        s.get_record(&a.did, "app.bsky.feed.post", "3jzfcijpj2z2e")
            .await
            .err(400, "RecordNotFound");
        assert_eq!(
            s.latest_commit(&a.did).await,
            before,
            "failed applyWrites must not commit ({bad})"
        );
    }
    assert_eq!(
        s.get_record(&a.did, "app.bsky.feed.post", existing.rkey())
            .await
            .ok()["value"]["text"],
        json!("exists")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_limits() {
    let (s, a) = setup().await;
    let writes: Vec<J> = (0..201)
        .map(|i| json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": format!("k{i}"), "value": {"i": i}}))
        .collect();
    let r = apply(&s, &a, json!({"writes": writes})).await;
    r.err(400, "InvalidRequest");
    let writes: Vec<J> = (0..200)
        .map(|i| json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": format!("k{i}"), "value": {"i": i}}))
        .collect();
    let r = apply(&s, &a, json!({"writes": writes})).await.ok();
    assert_eq!(r["results"].as_array().unwrap().len(), 200);
    assert_eq!(s.get_repo(&a.did).await.entries().len(), 200);
}

/// Record writes take JSON bodies up to 1,000,000 bytes (reference
/// createRecord/putRecord/applyWrites `jsonLimit`), past the 150 KiB default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_writes_accept_large_json_bodies() {
    let (s, a) = setup().await;
    let big = "x".repeat(600_000);
    let rec = json!({"$type": "com.example.big", "data": big});
    create(&s, &a, json!({"collection": "com.example.big", "rkey": "a", "record": rec})).await.ok();
    put(&s, &a, json!({"collection": "com.example.big", "rkey": "b", "record": rec})).await.ok();
    apply(
        &s,
        &a,
        json!({"writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.big", "rkey": "c", "value": rec}]}),
    )
    .await
    .ok();
    let got = s.get_record(&a.did, "com.example.big", "b").await.ok();
    assert_eq!(got["value"]["data"].as_str().map(str::len), Some(600_000));
    // past 1,000,000 bytes: 413
    let huge = format!(
        "{{\"repo\":\"{}\",\"collection\":\"com.example.big\",\"record\":{{\"data\":\"{}\"}}}}",
        a.did,
        "x".repeat(1_000_000)
    );
    s.xrpc
        .post_bytes("com.atproto.repo.createRecord", huge.into_bytes(), "application/json", &a.auth())
        .await
        .err(413, "PayloadTooLarge");
}

/// Without an AppView, getRecord for a repo not hosted here is the
/// reference's 400 InvalidRequest "Could not locate record".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_record_unhosted_without_appview() {
    let (s, _) = setup().await;
    let r = s.get_record("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "app.bsky.feed.post", "3jzfcijpj2z2a").await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("Could not locate record"), "{}", r.text());
}
