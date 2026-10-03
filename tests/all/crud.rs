//! Port of atproto/packages/pds/tests/crud.test.ts: record CRUD, listRecords
//! pagination, putRecord semantics, compare-and-swap, applyWrites atomicity,
//! rkey/collection/$type rules and data-model round trips.
//! (bsky-specific duplicate-like/follow pruning and takedowns live elsewhere.)
use crate::common::*;

const POST: &str = "app.bsky.feed.post";
const PROFILE: &str = "app.bsky.actor.profile";

async fn setup() -> (TestServer, TestAccount) {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    (s, a)
}

/// `nsid` as `a`, with `repo` defaulting to `a`'s DID.
async fn write(s: &TestServer, a: &TestAccount, nsid: &str, mut body: J) -> Resp {
    if body.get("repo").is_none() {
        body["repo"] = json!(a.did);
    }
    s.xrpc.post(&format!("com.atproto.repo.{nsid}"), &body, &a.auth()).await
}

async fn create(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    write(s, a, "createRecord", body).await
}

async fn put(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    write(s, a, "putRecord", body).await
}

async fn delete(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    write(s, a, "deleteRecord", body).await
}

async fn apply(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    write(s, a, "applyWrites", body).await
}

fn profile(n: &str) -> J {
    json!({"$type": PROFILE, "displayName": n})
}

async fn count(s: &TestServer, did: &str, coll: &str) -> usize {
    s.list_records(did, coll, &[]).await.ok()["records"].as_array().unwrap().len()
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
            assert_eq!(Some(c), recs.last().and_then(|r| r["uri"].as_str()).map(|u| u.rsplit('/').next().unwrap()));
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
    let d = s.describe_repo(&a.did).await.ok();
    assert_eq!(d["handle"], json!(a.handle));
    assert_eq!(d["did"], json!(a.did));
    assert_eq!(d["handleIsCorrect"], json!(true));
    assert_eq!(d["didDoc"]["id"], json!(a.did));
    assert_eq!(s.describe_repo(&b.handle).await.ok()["did"], json!(b.did));
    // collections reflect the repo's contents
    s.post(&a, "hi").await;
    assert_eq!(s.describe_repo(&a.did).await.ok()["collections"], json!([POST]), "describeRepo.collections");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_gets_lists_and_deletes_records() {
    let (s, a) = setup().await;
    let rec = RecordRef::from_json(&create(&s, &a, json!({"collection": POST, "record": post_record("Hello, world!")})).await.ok());
    assert!(rec.uri.starts_with(&format!("at://{}/{POST}/", a.did)), "{}", rec.uri);
    assert!(is_tid(rec.rkey()), "generated rkey should be a TID: {}", rec.rkey());
    assert!(Cid::parse(&rec.cid).is_ok());
    let (head, rev) = s.latest_commit(&a.did).await;
    assert_eq!(rec.commit_cid.as_deref(), Some(head.to_string().as_str()), "createRecord commit.cid");
    assert_eq!(rec.rev.as_deref(), Some(rev.as_str()), "createRecord commit.rev");

    let l = s.list_records(&a.did, POST, &[]).await.ok();
    let recs = l["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["uri"], json!(rec.uri));
    assert_eq!(recs[0]["cid"], json!(rec.cid));
    assert_eq!(recs[0]["value"]["text"], json!("Hello, world!"));

    let g = s.get_record(&a.did, POST, rec.rkey()).await.ok();
    assert_eq!(g["uri"], json!(rec.uri));
    assert_eq!(g["cid"], json!(rec.cid));
    assert_eq!(g["value"]["text"], json!("Hello, world!"));
    assert_eq!(g["value"]["$type"], json!(POST));

    // repo by handle works for reads
    assert_eq!(s.get_record(&a.handle, POST, rec.rkey()).await.ok()["cid"], json!(rec.cid));

    // getRecord pinned to the right cid works, a wrong cid is RecordNotFound
    let pinned = |cid: String| {
        let (x, did, rkey) = (s.xrpc.clone(), a.did.clone(), rec.rkey().to_string());
        async move { x.get("com.atproto.repo.getRecord", &[("repo", &did), ("collection", POST), ("rkey", &rkey), ("cid", &cid)], &Auth::None).await }
    };
    pinned(rec.cid.clone()).await.ok();
    pinned(Cid::dag_cbor(b"nope").to_string()).await.err(400, "RecordNotFound");

    let d = delete(&s, &a, json!({"collection": POST, "rkey": rec.rkey()})).await.ok();
    assert!(d["commit"]["cid"].is_string(), "deleteRecord returns commit: {d}");
    assert_eq!(count(&s, &a.did, POST).await, 0);
    s.get_record(&a.did, POST, rec.rkey()).await.err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_requires_auth_and_matching_repo() {
    let (s, a) = setup().await;
    let b = s.create_account("bob").await;
    let body = json!({"repo": a.did, "collection": POST, "record": post_record("x")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &Auth::None).await.err(401, "AuthenticationRequired");
    let r = s.xrpc.post("com.atproto.repo.createRecord", &body, &Auth::Bearer("garbage.token.here".into())).await;
    // reference auth-verifier: an unverifiable JWT is InvalidRequestError('Token could not be verified', 'InvalidToken') -> 400
    assert!((r.status, r.error_name()) == (400, Some("InvalidToken")) || r.status == 401, "garbage bearer token: {}", r.text());
    // putRecord into someone else's repo fails, and leaves it untouched
    put(&s, &a, json!({"repo": b.did, "collection": PROFILE, "rkey": "self", "record": profile("evil")})).await.client_err();
    s.get_record(&b.did, PROFILE, "self").await.err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_missing_record_is_a_noop() {
    let (s, a) = setup().await;
    let rec = s.post(&a, "post").await;
    let body = json!({"collection": POST, "rkey": rec.rkey()});
    delete(&s, &a, body.clone()).await.ok();
    s.get_record(&a.did, POST, rec.rkey()).await.err(400, "RecordNotFound");
    let before = s.latest_commit(&a.did).await;
    delete(&s, &a, body).await.ok();
    assert_eq!(s.latest_commit(&a.did).await, before, "deleting a missing record must not create a commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_keeps_block_referenced_elsewhere() {
    let (s, a) = setup().await;
    let record = json!({"$type": POST, "text": "post", "createdAt": "2026-01-01T00:00:00.000Z"});
    let p1 = RecordRef::from_json(&create(&s, &a, json!({"collection": POST, "record": record})).await.ok());
    let p2 = RecordRef::from_json(&create(&s, &a, json!({"collection": POST, "record": record})).await.ok());
    assert_eq!(p1.cid, p2.cid, "identical records share a cid");
    delete(&s, &a, json!({"collection": POST, "rkey": p1.rkey()})).await.ok();
    assert_eq!(s.get_record(&a.did, POST, p2.rkey()).await.ok()["value"], record);
    // and the exported repo still contains the block
    let repo = s.get_repo(&a.did).await;
    assert_eq!(repo.record(&format!("{POST}/{}", p2.rkey())), Some(record));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_creates_then_updates() {
    let (s, a) = setup().await;
    s.get_record(&a.did, PROFILE, "self").await.err(400, "RecordNotFound");
    let p = put(&s, &a, json!({"collection": PROFILE, "rkey": "self", "record": {"displayName": "Robert"}})).await.ok();
    assert_eq!(p["uri"], json!(format!("at://{}/{PROFILE}/self", a.did)));
    let g = s.get_record(&a.did, PROFILE, "self").await.ok();
    assert_eq!(g["value"], json!({"$type": PROFILE, "displayName": "Robert"}), "putRecord should default $type");

    let value = json!({"$type": PROFILE, "displayName": "Robert", "description": "Dog lover"});
    let p2 = put(&s, &a, json!({"collection": PROFILE, "rkey": "self", "record": value})).await.ok();
    assert_ne!(p2["cid"], p["cid"]);
    let g = s.get_record(&a.did, PROFILE, "self").await.ok();
    assert_eq!(g["value"], value);
    assert_eq!(g["cid"], p2["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_by_handle() {
    let (s, a) = setup().await;
    let rkey = "3jzfcijpj2z2a";
    let follow = json!({"$type": "app.bsky.graph.follow", "subject": "did:plc:abc", "createdAt": now_iso()});
    put(&s, &a, json!({"repo": a.handle, "collection": "app.bsky.graph.follow", "rkey": rkey, "record": follow})).await.ok();
    s.get_record(&a.did, "app.bsky.graph.follow", rkey).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_noop_does_not_commit() {
    let (s, a) = setup().await;
    let body = json!({"collection": PROFILE, "rkey": "self", "record": profile("same")});
    let p1 = put(&s, &a, body.clone()).await.ok();
    let before = s.latest_commit(&a.did).await;
    let p2 = put(&s, &a, body).await.ok();
    assert_eq!(p1["uri"], p2["uri"]);
    assert_eq!(p1["cid"], p2["cid"]);
    assert_eq!(s.latest_commit(&a.did).await, before, "no-op putRecord must not produce a commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn defaults_undefined_type() {
    let (s, a) = setup().await;
    let r = RecordRef::from_json(&create(&s, &a, json!({"collection": POST, "record": {"text": "no type", "createdAt": now_iso()}})).await.ok());
    assert_eq!(s.get_record(&a.did, POST, r.rkey()).await.ok()["value"]["$type"], json!(POST));
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
    let r = create(&s, &a, json!({"collection": PROFILE, "rkey": "self", "record": {"displayName": "alice", "createdAt": now_iso()}})).await.ok();
    assert_eq!(RecordRef::from_json(&r).rkey(), "self", "app.bsky.actor.profile has key literal:self");
    assert_eq!(r["validationStatus"], json!("valid"));
    create(&s, &a, json!({"collection": PROFILE, "record": {"displayName": "alice"}})).await.err(400, "InvalidRequest");
    put(&s, &a, json!({"collection": PROFILE, "rkey": "other", "record": {"displayName": "alice"}})).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_type_to_match_collection() {
    let (s, a) = setup().await;
    create(&s, &a, json!({"collection": POST, "record": {"$type": "app.bsky.feed.like"}})).await.err(400, "InvalidRequest");
    // also when unvalidated / unknown lexicon
    create(&s, &a, json!({"collection": "com.example.record", "record": {"$type": "com.example.other", "blah": "thing"}})).await.err(400, "InvalidRequest");
    put(&s, &a, json!({"collection": "com.example.record", "rkey": "x", "record": {"$type": "com.example.other"}})).await.err(400, "InvalidRequest");
    let writes = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "value": {"$type": "com.example.other"}}]);
    apply(&s, &a, json!({"writes": writes})).await.err(400, "InvalidRequest");
    // $type must be a non-empty string when present
    for t in [json!(null), json!(123), json!("")] {
        create(&s, &a, json!({"collection": "com.example.record", "record": {"$type": t, "a": 1}})).await.err(400, "InvalidRequest");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_valid_rkey() {
    let (s, a) = setup().await;
    let long = "o".repeat(513);
    let bad = [".", "..", "a/b", "with space", "#extra", "@handle", "number[3]", "number(3)", "\"quote\"", "dHJ1ZQ==", long.as_str()];
    for rk in bad {
        create(&s, &a, json!({"collection": "com.example.record", "rkey": rk, "record": {"a": 1}})).await.err(400, "InvalidRequest");
        put(&s, &a, json!({"collection": "com.example.record", "rkey": rk, "record": {"a": 1}})).await.err(400, "InvalidRequest");
        let writes = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": rk, "value": {"a": 1}}]);
        apply(&s, &a, json!({"writes": writes})).await.err(400, "InvalidRequest");
    }
    // nothing was written
    assert_eq!(count(&s, &a.did, "com.example.record").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_valid_collection_nsid() {
    let (s, a) = setup().await;
    for c in ["app.bsky", "example.com", "one.two..three", "com.example.foo.*", "not an nsid", "com.atproto.feed.p@st", "a/b.c.d"] {
        let r = create(&s, &a, json!({"collection": c, "record": {"a": 1}})).await;
        assert_eq!((r.status, r.error_name()), (400, Some("InvalidRequest")), "collection {c:?}: {}", r.text());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unvalidated_writes_of_unknown_lexicons() {
    let (s, a) = setup().await;
    // validate unset: allowed, status unknown
    let r = create(&s, &a, json!({"collection": "com.example.record", "record": {"$type": "com.example.record", "blah": "thing"}})).await.ok();
    assert_eq!(r["validationStatus"], json!("unknown"));
    let g = s.get_record(&a.did, "com.example.record", RecordRef::from_json(&r).rkey()).await.ok();
    assert_eq!(g["value"], json!({"$type": "com.example.record", "blah": "thing"}));
    // validate=false: allowed, no validation status
    let r = create(&s, &a, json!({"collection": "com.example.record", "validate": false, "record": {"$type": "com.example.record", "blah": "thing2"}})).await.ok();
    assert!(r.get("validationStatus").is_none_or(|v| v.is_null()), "validate=false => no validationStatus: {r}");
    // validate=true on an unknown lexicon: rejected, mentioning the NSID
    let r = create(&s, &a, json!({"collection": "com.example.foobar", "validate": true, "record": {"$type": "com.example.foobar"}})).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("com.example.foobar"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validates_known_records_on_write() {
    let (s, a) = setup().await;
    // missing required "text"
    create(&s, &a, json!({"collection": POST, "record": {"$type": POST, "createdAt": now_iso()}})).await.err(400, "InvalidRequest");
    // datetimes are validated rigorously
    let bad_date = json!({"$type": POST, "text": "test", "createdAt": "0000-00-12T23:20:50.123Z"});
    create(&s, &a, json!({"collection": POST, "record": bad_date})).await.err(400, "InvalidRequest");
    let r = create(&s, &a, json!({"collection": POST, "record": post_record("ok")})).await.ok();
    assert_eq!(r["validationStatus"], json!("valid"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_legacy_blob_refs_and_bad_values() {
    let (s, a) = setup().await;
    let cid = s.upload_blob(&a, PNG_1X1, "image/png").await["ref"]["$link"].as_str().unwrap().to_string();
    let legacy = json!({"blah": "thing", "image": {"cid": cid, "mimeType": "image/png"}});
    create(&s, &a, json!({"collection": "com.example.record", "validate": false, "record": legacy})).await.err(400, "InvalidRequest");
    // floats are not part of the data model
    create(&s, &a, json!({"collection": "com.example.record", "record": {"a": 1.5}})).await.err(400, "InvalidRequest");
    // blob with a string size is malformed
    let blob = json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/png", "size": "10"});
    create(&s, &a, json!({"collection": "com.example.record", "record": {"b": blob}})).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_values_too_deep_for_cbor() {
    let (s, a) = setup().await;
    // 4000 levels of nesting (built as raw JSON text; serde_json can't build it)
    let deep = format!("{}1{}", "{\"x\":".repeat(4000), "}".repeat(4000));
    let body = format!(r#"{{"repo":"{}","collection":"{POST}","record":{{"text":"x","createdAt":"{}","deepObject":{deep}}}}}"#, a.did, now_iso());
    s.xrpc.post_bytes("com.atproto.repo.createRecord", body.into_bytes(), "application/json", &a.auth()).await.err(400, "InvalidRequest");
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
    let rec = RecordRef::from_json(&create(&s, &a, json!({"collection": "com.example.kitchen", "record": record})).await.ok());
    assert_eq!(s.get_record(&a.did, "com.example.kitchen", rec.rkey()).await.ok()["value"], record);
    // the CID is the DAG-CBOR hash of the record, and the exported block
    let v = Value::from_json(&record).unwrap();
    assert_eq!(rec.cid, Cid::dag_cbor(&v.to_cbor()).to_string());
    let repo = s.get_repo(&a.did).await;
    assert_eq!(repo.blocks.get(&Cid::parse(&rec.cid).unwrap()), Some(&v.to_cbor()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_repo_errors() {
    let (s, a) = setup().await;
    let ghost = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    s.get_record(ghost, POST, "3jzfcijpj2z2a").await.err_status(400);
    s.list_records(ghost, POST, &[]).await.err_status(400);
    s.describe_repo(ghost).await.err_status(400);
    s.get_record("nobody.vlpds.test", POST, "3jzfcijpj2z2a").await.err_status(400);
    // writing to another (nonexistent) repo with your token
    create(&s, &a, json!({"repo": ghost, "collection": POST, "record": post_record("x")})).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_create_fails() {
    let (s, a) = setup().await;
    let rk = "3jzfcijpj2z2a";
    create(&s, &a, json!({"collection": POST, "rkey": rk, "record": post_record("one")})).await.ok();
    create(&s, &a, json!({"collection": POST, "rkey": rk, "record": post_record("two")})).await.client_err();
    assert_eq!(s.get_record(&a.did, POST, rk).await.ok()["value"]["text"], json!("one"));
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
    let full = s.list_records(&a.did, POST, &[]).await.ok();
    let full_recs = full["records"].as_array().unwrap().clone();
    assert_eq!(full_recs.len(), 5);
    // default order is newest-first (descending rkey)
    let got: Vec<&str> = full_recs.iter().map(|r| r["uri"].as_str().unwrap()).collect();
    let want: Vec<&str> = uris.iter().rev().map(|s| s.as_str()).collect();
    assert_eq!(got, want);

    // forwards and reverse, 2 at a time
    assert_eq!(list_all(&s, &a.did, POST, 2, false).await, full_recs);
    let mut full_rev = full_recs.clone();
    full_rev.reverse();
    assert_eq!(list_all(&s, &a.did, POST, 2, true).await, full_rev);

    // reverse=true on one page is the exact reverse
    let rev = s.list_records(&a.did, POST, &[("reverse", "true")]).await.ok();
    assert_eq!(rev["records"].as_array().unwrap(), &full_rev);
    if let Some(c) = full["cursor"].as_str() {
        assert_eq!(c, uris[0].rsplit('/').next().unwrap());
    }
    if let Some(c) = rev["cursor"].as_str() {
        assert_eq!(c, uris[4].rsplit('/').next().unwrap());
    }

    // other collections are not included; an empty collection lists nothing
    s.create_record(&a, "app.bsky.feed.like", json!({"$type": "app.bsky.feed.like", "subject": {"uri": uris[0], "cid": full_recs[0]["cid"]}, "createdAt": now_iso()})).await;
    assert_eq!(count(&s, &a.did, POST).await, 5);
    assert_eq!(count(&s, &a.did, "app.bsky.feed.repost").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_records_limit_bounds() {
    let (s, a) = setup().await;
    for i in 0..3 {
        s.post(&a, &format!("p{i}")).await;
    }
    // lexicon: limit 1..=100
    s.list_records(&a.did, POST, &[("limit", "0")]).await.err(400, "InvalidRequest");
    s.list_records(&a.did, POST, &[("limit", "101")]).await.err(400, "InvalidRequest");
    let one = s.list_records(&a.did, POST, &[("limit", "1")]).await.ok();
    assert_eq!(one["records"].as_array().unwrap().len(), 1);
    assert!(one["cursor"].is_string());
}

// ---------------------------------------------------------------------------
// compare-and-swap
// ---------------------------------------------------------------------------

/// A CID no record has.
fn wrong_cid() -> String {
    Cid::dag_cbor(&Value::Map(vec![]).to_cbor()).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_record_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    let r = create(&s, &a, json!({"collection": POST, "swapCommit": head.to_string(), "record": post_record("cas ok")})).await.ok();
    s.get_record(&a.did, POST, RecordRef::from_json(&r).rkey()).await.ok();
    // head is now stale
    let rk = "3jzfcijpj2z2b";
    create(&s, &a, json!({"collection": POST, "rkey": rk, "swapCommit": head.to_string(), "record": post_record("cas bad")})).await.err(400, "InvalidSwap");
    s.get_record(&a.did, POST, rk).await.err(400, "RecordNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_record_swap_commit_and_record() {
    let (s, a) = setup().await;
    // proper commit cas
    let p = s.post(&a, "p1").await;
    let (head, _) = s.latest_commit(&a.did).await;
    delete(&s, &a, json!({"collection": POST, "rkey": p.rkey(), "swapCommit": head.to_string()})).await.ok();
    s.get_record(&a.did, POST, p.rkey()).await.err(400, "RecordNotFound");
    // bad commit cas
    let (stale, _) = s.latest_commit(&a.did).await;
    let p = s.post(&a, "p2").await;
    delete(&s, &a, json!({"collection": POST, "rkey": p.rkey(), "swapCommit": stale.to_string()})).await.err(400, "InvalidSwap");
    s.get_record(&a.did, POST, p.rkey()).await.ok();
    // proper record cas
    delete(&s, &a, json!({"collection": POST, "rkey": p.rkey(), "swapRecord": p.cid})).await.ok();
    s.get_record(&a.did, POST, p.rkey()).await.err(400, "RecordNotFound");
    // bad record cas
    let p = s.post(&a, "p3").await;
    delete(&s, &a, json!({"collection": POST, "rkey": p.rkey(), "swapRecord": wrong_cid()})).await.err(400, "InvalidSwap");
    s.get_record(&a.did, POST, p.rkey()).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    let p = put(&s, &a, json!({"collection": PROFILE, "rkey": "self", "swapCommit": head.to_string(), "record": profile("a1")})).await.ok();
    assert_eq!(s.get_record(&a.did, PROFILE, "self").await.ok()["cid"], p["cid"]);
    put(&s, &a, json!({"collection": PROFILE, "rkey": "self", "swapCommit": head.to_string(), "record": profile("a2")})).await.err(400, "InvalidSwap");
    assert_eq!(s.get_record(&a.did, PROFILE, "self").await.ok()["cid"], p["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_record_swap_record() {
    let (s, a) = setup().await;
    let put_swap = |swap: J, name: &str| put(&s, &a, json!({"collection": PROFILE, "rkey": "self", "swapRecord": swap, "record": profile(name)}));
    // swapRecord: null => the record must not exist (create)
    let p1 = put_swap(J::Null, "b1").await.ok();
    assert_eq!(s.get_record(&a.did, PROFILE, "self").await.ok()["cid"], p1["cid"]);
    // swapRecord: cid => update
    let p2 = put_swap(p1["cid"].clone(), "b2").await.ok();
    assert_eq!(s.get_record(&a.did, PROFILE, "self").await.ok()["cid"], p2["cid"]);
    // null now fails (record exists), as do a wrong and a stale (previous) cid
    put_swap(J::Null, "b3").await.err(400, "InvalidSwap");
    put_swap(json!(wrong_cid()), "b4").await.err(400, "InvalidSwap");
    put_swap(p1["cid"].clone(), "b5").await.err(400, "InvalidSwap");
    assert_eq!(s.get_record(&a.did, PROFILE, "self").await.ok()["cid"], p2["cid"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_swap_commit() {
    let (s, a) = setup().await;
    let (head, _) = s.latest_commit(&a.did).await;
    let ok = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "value": post_record("aw")}]);
    apply(&s, &a, json!({"swapCommit": head.to_string(), "writes": ok})).await.ok();
    let stale = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "3jzfcijpj2z2c", "value": post_record("aw2")}]);
    apply(&s, &a, json!({"swapCommit": head.to_string(), "writes": stale})).await.err(400, "InvalidSwap");
    s.get_record(&a.did, POST, "3jzfcijpj2z2c").await.err(400, "RecordNotFound");
}

// ---------------------------------------------------------------------------
// applyWrites
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_mixed_batch_is_one_commit() {
    let (s, a) = setup().await;
    let p = s.post(&a, "to update").await;
    let d = s.post(&a, "to delete").await;
    let writes = json!([
        {"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "3jzfcijpj2z2d", "value": post_record("new")},
        {"$type": "com.atproto.repo.applyWrites#update", "collection": POST, "rkey": p.rkey(), "value": post_record("updated")},
        {"$type": "com.atproto.repo.applyWrites#delete", "collection": POST, "rkey": d.rkey()},
    ]);
    let r = apply(&s, &a, json!({"writes": writes})).await.ok();
    let res = r["results"].as_array().expect("results");
    assert_eq!(res.len(), 3);
    assert_eq!(res[0]["$type"], json!("com.atproto.repo.applyWrites#createResult"));
    assert_eq!(res[0]["uri"], json!(format!("at://{}/{POST}/3jzfcijpj2z2d", a.did)));
    assert_eq!(res[1]["$type"], json!("com.atproto.repo.applyWrites#updateResult"));
    assert_eq!(res[2]["$type"], json!("com.atproto.repo.applyWrites#deleteResult"));
    let (head, rev) = s.latest_commit(&a.did).await;
    assert_eq!(r["commit"]["cid"], json!(head.to_string()));
    assert_eq!(r["commit"]["rev"], json!(rev));
    assert_eq!(s.get_record(&a.did, POST, "3jzfcijpj2z2d").await.ok()["cid"], res[0]["cid"]);
    assert_eq!(s.get_record(&a.did, POST, p.rkey()).await.ok()["value"]["text"], json!("updated"));
    s.get_record(&a.did, POST, d.rkey()).await.err(400, "RecordNotFound");
    // all three ops landed in one #commit
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub.until(FH_TIMEOUT, |fs| fs.iter().any(|f| f.commit().is_some_and(|c| c.rev == rev))).await;
    let c = frames.iter().filter_map(|f| f.commit()).find(|c| c.rev == rev).unwrap();
    let mut actions: Vec<_> = c.ops.iter().map(|o| o.action.as_str()).collect();
    actions.sort();
    assert_eq!(actions, vec!["create", "delete", "update"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_is_atomic() {
    let (s, a) = setup().await;
    let existing = s.post(&a, "exists").await;
    let before = s.latest_commit(&a.did).await;
    let good = json!({"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "3jzfcijpj2z2e", "value": post_record("good")});
    let bad_batches = [
        // duplicate create of an existing key
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": existing.rkey(), "value": post_record("dupe")}),
        // invalid rkey
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "..", "value": post_record("x")}),
        // $type mismatch
        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "value": {"$type": "app.bsky.feed.like"}}),
        // update of a missing record
        json!({"$type": "com.atproto.repo.applyWrites#update", "collection": POST, "rkey": "3jzfcijpj2z2f", "value": post_record("x")}),
        // unknown write type
        json!({"$type": "com.atproto.repo.applyWrites#explode", "collection": POST, "rkey": "3jzfcijpj2z2g"}),
    ];
    for bad in bad_batches {
        let r = apply(&s, &a, json!({"writes": [good.clone(), bad.clone()]})).await;
        assert!((400..500).contains(&r.status), "batch with {bad} should fail with 4xx: {}", r.text());
        s.get_record(&a.did, POST, "3jzfcijpj2z2e").await.err(400, "RecordNotFound");
        assert_eq!(s.latest_commit(&a.did).await, before, "failed applyWrites must not commit ({bad})");
    }
    assert_eq!(s.get_record(&a.did, POST, existing.rkey()).await.ok()["value"]["text"], json!("exists"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_writes_limits() {
    let (s, a) = setup().await;
    let writes = |n: usize| -> J { (0..n).map(|i| json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": format!("k{i}"), "value": {"i": i}})).collect() };
    apply(&s, &a, json!({"writes": writes(201)})).await.err(400, "InvalidRequest");
    let r = apply(&s, &a, json!({"writes": writes(200)})).await.ok();
    assert_eq!(r["results"].as_array().unwrap().len(), 200);
    assert_eq!(s.get_repo(&a.did).await.entries().len(), 200);
}

/// Record writes take JSON bodies up to 1,000,000 bytes (reference
/// createRecord/putRecord/applyWrites `jsonLimit`), past the 150 KiB default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_writes_accept_large_json_bodies() {
    let (s, a) = setup().await;
    let rec = json!({"$type": "com.example.big", "data": "x".repeat(600_000)});
    create(&s, &a, json!({"collection": "com.example.big", "rkey": "a", "record": rec})).await.ok();
    put(&s, &a, json!({"collection": "com.example.big", "rkey": "b", "record": rec})).await.ok();
    let writes = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.big", "rkey": "c", "value": rec}]);
    apply(&s, &a, json!({"writes": writes})).await.ok();
    let got = s.get_record(&a.did, "com.example.big", "b").await.ok();
    assert_eq!(got["value"]["data"].as_str().map(str::len), Some(600_000));
    // past 1,000,000 bytes: 413
    let huge = format!("{{\"repo\":\"{}\",\"collection\":\"com.example.big\",\"record\":{{\"data\":\"{}\"}}}}", a.did, "x".repeat(1_000_000));
    // The server rejects from Content-Length without reading the body, so it may
    // close before the client finishes writing; either outcome is a rejection.
    match s.xrpc.try_post_bytes("com.atproto.repo.createRecord", huge.into_bytes(), "application/json", &a.auth()).await {
        Ok(r) => r.err(413, "PayloadTooLarge"),
        Err(e) => assert!(e.is_request() || e.is_body(), "unexpected error: {e}"),
    }
}

/// Without an AppView, getRecord for a repo not hosted here is the
/// reference's 400 InvalidRequest "Could not locate record".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_record_unhosted_without_appview() {
    let (s, _) = setup().await;
    let r = s.get_record("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", POST, "3jzfcijpj2z2a").await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("Could not locate record"), "{}", r.text());
}
