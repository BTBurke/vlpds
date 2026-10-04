//! Reference-coverage ports (tests/REFERENCE_COVERAGE.md) of cases from
//! packages/pds/tests/{crud,file-uploads,preferences}.test.ts that the
//! original ports (crud, file_uploads, preferences) did not assert.
use crate::common::*;
use std::time::Duration;

async fn call(s: &TestServer, nsid: &str, a: &TestAccount, body: J) -> Resp {
    s.xrpc.post(nsid, &body, &a.auth()).await
}

async fn upload(s: &TestServer, a: &TestAccount, bytes: &[u8], mime: &str) -> J {
    s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.to_vec(), mime, &a.auth()).await.ok()["blob"].clone()
}

fn random_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|_| rand::random::<u8>()).collect()
}

fn link(blob: &J) -> String {
    blob["ref"]["$link"].as_str().unwrap().to_string()
}

async fn get_blob(s: &TestServer, did: &str, cid: &str) -> Resp {
    s.xrpc.get("com.atproto.sync.getBlob", &[("did", did), ("cid", cid)], &Auth::None).await
}

async fn list_blobs(s: &TestServer, did: &str) -> Vec<String> {
    let j = s.xrpc.get("com.atproto.sync.listBlobs", &[("did", did)], &Auth::None).await.ok();
    j["cids"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_string()).collect()
}

/// crud.test.ts "putRecord > fails on user mismatch": writing into another
/// account's repo is the reference's `AuthRequiredError` (401
/// AuthenticationRequired, "Authentication Required") for every write route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_writes_to_another_repo_are_auth_required() {
    let s = TestServer::spawn().await;
    let alice = s.create_account("alice").await;
    let bob = s.create_account("bob").await;
    let follow = json!({"$type": "app.bsky.graph.follow", "subject": alice.did, "createdAt": now_iso()});
    let bobs = s.post(&bob, "bob's").await;
    for (nsid, body) in [
        (
            "com.atproto.repo.putRecord",
            json!({"repo": bob.did, "collection": "app.bsky.graph.follow", "rkey": "3jzfcijpj2z2a", "record": follow}),
        ),
        (
            "com.atproto.repo.createRecord",
            json!({"repo": bob.did, "collection": "app.bsky.graph.follow", "record": follow}),
        ),
        (
            "com.atproto.repo.deleteRecord",
            json!({"repo": bob.did, "collection": "app.bsky.feed.post", "rkey": bobs.rkey()}),
        ),
        (
            "com.atproto.repo.applyWrites",
            json!({"repo": bob.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.graph.follow", "value": follow}]}),
        ),
    ] {
        let r = call(&s, nsid, &alice, body).await;
        r.err(401, "AuthenticationRequired");
        assert!(r.text().contains("Authentication Required"), "{nsid}: {}", r.text());
    }
    // by handle too
    let r = call(
        &s,
        "com.atproto.repo.createRecord",
        &alice,
        json!({"repo": bob.handle, "collection": "app.bsky.graph.follow", "record": follow}),
    )
    .await;
    r.err(401, "AuthenticationRequired");
    // bob's repo is untouched
    s.get_record(&bob.did, "app.bsky.feed.post", bobs.rkey()).await.ok();
    let l = s.list_records(&bob.did, "app.bsky.graph.follow", &[]).await.ok();
    assert_eq!(l["records"], json!([]));
}

/// crud.test.ts "putRecord > fails on invalid record": an invalid update is
/// 400 InvalidRequest naming the bad field, and the stored record is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_put_record_invalid_update_leaves_record() {
    let s = TestServer::spawn().await;
    let bob = s.create_account("bob").await;
    let good = json!({"$type": "app.bsky.actor.profile", "displayName": "Robert", "description": "Dog lover"});
    call(
        &s,
        "com.atproto.repo.putRecord",
        &bob,
        json!({"repo": bob.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": good}),
    )
    .await
    .ok();
    // a float (not in the data model; vlpds's message doesn't name the field)
    let r = call(
        &s,
        "com.atproto.repo.putRecord",
        &bob,
        json!({"repo": bob.did, "collection": "app.bsky.actor.profile", "rkey": "self",
               "record": {"displayName": "Robert", "description": 2.5}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    // an integer (wrong lexicon type) names the field
    let r = call(
        &s,
        "com.atproto.repo.putRecord",
        &bob,
        json!({"repo": bob.did, "collection": "app.bsky.actor.profile", "rkey": "self",
               "record": {"displayName": "Robert", "description": 3}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("description"), "{}", r.text());
    let g = s.get_record(&bob.did, "app.bsky.actor.profile", "self").await.ok();
    assert_eq!(g["value"], good);
}

/// crud.test.ts "unvalidated writes > allows update of unknown lexicons when
/// validate is set to false": putRecord updates an unknown-lexicon record
/// with validate unset (status `unknown`) and with `validate: false` (no
/// status), defaulting `$type` both times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_updates_unknown_lexicon_records() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let c = call(
        &s,
        "com.atproto.repo.createRecord",
        &a,
        json!({"repo": a.did, "collection": "com.example.record", "validate": false, "record": {"blah": "thing"}}),
    )
    .await
    .ok();
    assert!(c.get("validationStatus").is_none(), "{c}");
    let rkey = RecordRef::from_json(&c).rkey().to_string();
    let want = json!({"$type": "com.example.record", "blah": "something else"});

    let u1 = call(
        &s,
        "com.atproto.repo.putRecord",
        &a,
        json!({"repo": a.did, "collection": "com.example.record", "rkey": rkey, "record": {"blah": "something else"}}),
    )
    .await
    .ok();
    assert_eq!(u1["validationStatus"], json!("unknown"));
    let g = s.get_record(&a.did, "com.example.record", &rkey).await.ok();
    assert_eq!((g["value"].clone(), g["cid"].clone()), (want.clone(), u1["cid"].clone()));

    let u2 = call(
        &s,
        "com.atproto.repo.putRecord",
        &a,
        json!({"repo": a.did, "collection": "com.example.record", "rkey": rkey, "validate": false, "record": {"blah": "something else"}}),
    )
    .await
    .ok();
    assert!(u2.get("validationStatus").is_none(), "{u2}");
    assert_eq!(u2["cid"], u1["cid"]);
    let g = s.get_record(&a.did, "com.example.record", &rkey).await.ok();
    assert_eq!(g["value"], want);
}

/// crud.test.ts "unvalidated writes > applyWrites returns results with
/// validation status": exact result shapes per write type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_apply_writes_results_carry_validation_status() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut existing = Vec::new();
    for blah in ["thing1", "thing2"] {
        let r = call(
            &s,
            "com.atproto.repo.createRecord",
            &a,
            json!({"repo": a.did, "collection": "com.example.record", "validate": false, "record": {"blah": blah}}),
        )
        .await
        .ok();
        existing.push(RecordRef::from_json(&r));
    }
    let r = call(
        &s,
        "com.atproto.repo.applyWrites",
        &a,
        json!({"repo": a.did, "writes": [
            {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post",
             "value": {"$type": "app.bsky.feed.post", "text": "👋", "createdAt": now_iso()}},
            {"$type": "com.atproto.repo.applyWrites#update", "collection": "com.example.record",
             "rkey": existing[0].rkey(), "value": {}},
            {"$type": "com.atproto.repo.applyWrites#delete", "collection": "com.example.record",
             "rkey": existing[1].rkey()},
        ]}),
    )
    .await
    .ok();
    let res = r["results"].as_array().unwrap();
    assert_eq!(res.len(), 3);
    let keys = |v: &J| {
        let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
        k.sort();
        k
    };
    assert_eq!(res[0]["$type"], json!("com.atproto.repo.applyWrites#createResult"));
    assert_eq!(res[0]["validationStatus"], json!("valid"));
    assert_eq!(keys(&res[0]), ["$type", "cid", "uri", "validationStatus"]);
    assert_eq!(res[1]["$type"], json!("com.atproto.repo.applyWrites#updateResult"));
    assert_eq!(res[1]["validationStatus"], json!("unknown"));
    assert_eq!(res[1]["uri"], json!(existing[0].uri));
    assert_eq!(keys(&res[1]), ["$type", "cid", "uri", "validationStatus"]);
    assert_eq!(res[2], json!({"$type": "com.atproto.repo.applyWrites#deleteResult"}));
    // the update's `{}` got its $type defaulted
    let g = s.get_record(&a.did, "com.example.record", existing[0].rkey()).await.ok();
    assert_eq!(g["value"], json!({"$type": "com.example.record"}));
    assert_eq!(g["cid"], res[1]["cid"]);
}

/// crud.test.ts "attaches images to a post": an uploaded blob is not listed
/// until a record references it; after the post it is listed and served, and
/// the record holds the same blob ref. (The reference also refuses getBlob
/// before the reference, from its temp store; vlpds has no temp store and
/// serves an upload to anyone holding its CID: divergent, see
/// REFERENCE_COVERAGE.md.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_attaches_images_to_a_post() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut png = PNG_1X1.to_vec();
    png.extend_from_slice(&random_bytes(32));
    let blob = upload(&s, &a, &png, "image/png").await;
    assert!(list_blobs(&s, &a.did).await.is_empty());
    let post = s
        .create_record(
            &a,
            "app.bsky.feed.post",
            json!({"$type": "app.bsky.feed.post", "text": "Here's a key!", "createdAt": now_iso(),
                   "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}}),
        )
        .await;
    let g = s.get_record(&a.did, "app.bsky.feed.post", post.rkey()).await.ok();
    let images = g["value"]["embed"]["images"].as_array().unwrap();
    assert_eq!(images.len(), 1);
    assert_eq!(images[0]["image"]["ref"], blob["ref"]);
    let r = get_blob(&s, &a.did, &link(&blob)).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(&r.body[..], &png[..]);
    assert_eq!(list_blobs(&s, &a.did).await, vec![link(&blob)]);
}

/// crud.test.ts "unvalidated writes > correctly associates images with
/// unknown record types": a blob in an unvalidated, unknown-lexicon record
/// is associated with it (listed, served) and released when it is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_images_in_unknown_record_types_are_associated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let blob = upload(&s, &a, &random_bytes(3000), "image/jpeg").await;
    let r = call(
        &s,
        "com.atproto.repo.createRecord",
        &a,
        json!({"repo": a.did, "collection": "com.example.record", "validate": false,
               "record": {"blah": "thing", "image": blob}}),
    )
    .await
    .ok();
    let rec = RecordRef::from_json(&r);
    let g = s.get_record(&a.did, "com.example.record", rec.rkey()).await.ok();
    assert_eq!(g["value"]["blah"], json!("thing"));
    assert_eq!(g["value"]["$type"], json!("com.example.record"));
    assert_eq!(list_blobs(&s, &a.did).await, vec![link(&blob)]);
    assert_eq!(get_blob(&s, &a.did, &link(&blob)).await.status, 200);
    call(
        &s,
        "com.atproto.repo.deleteRecord",
        &a,
        json!({"repo": a.did, "collection": "com.example.record", "rkey": rec.rkey()}),
    )
    .await
    .ok();
    assert!(list_blobs(&s, &a.did).await.is_empty());
}

/// file-uploads.test.ts "does not make a blob permanent if referencing
/// failed": the blob stays unlisted (no record references it) and can still
/// be referenced later by a record whose schema allows it. (vlpds has no
/// temp/permanent split, so "not permanent" is observed as "not referenced".)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_failed_reference_does_not_make_blob_permanent() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    // over app.bsky.actor.profile#avatar's 1,000,000-byte maxSize
    let big = upload(&s, &a, &random_bytes(1_100_000), "image/jpeg").await;
    let r = call(
        &s,
        "com.atproto.repo.putRecord",
        &a,
        json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self",
               "record": {"$type": "app.bsky.actor.profile", "avatar": big}}),
    )
    .await;
    r.err(400, "InvalidRequest");
    assert!(list_blobs(&s, &a.did).await.is_empty());
    s.get_record(&a.did, "app.bsky.actor.profile", "self").await.err(400, "RecordNotFound");
    // the upload is still there to reference
    s.create_record(&a, "com.example.media", json!({"$type": "com.example.media", "file": big})).await;
    assert_eq!(list_blobs(&s, &a.did).await, vec![link(&big)]);
    assert_eq!(get_blob(&s, &a.did, &link(&big)).await.body.len(), 1_100_000);
}

/// file-uploads.test.ts "handles client abort": a client that drops an
/// upload mid-body leaves the server healthy and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_upload_client_abort() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    // 5 MB announced, ~1 MB sent, then the body stream fails
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = (0..16)
        .map(|_| Ok(bytes::Bytes::from(vec![0u8; 64 * 1024])))
        .chain(std::iter::once(Err(std::io::Error::other("client aborted"))))
        .collect();
    let body = reqwest::Body::wrap_stream(futures::stream::iter(chunks));
    let res = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.repo.uploadBlob", s.url))
        .header("authorization", format!("Bearer {}", a.access))
        .header("content-type", "image/jpeg")
        .header("content-length", "5000000")
        .body(body)
        .send()
        .await;
    assert!(res.is_err(), "aborted upload must not complete: {res:?}");
    // the server keeps serving, and the aborted bytes were not stored
    tokio::time::sleep(Duration::from_millis(10)).await;
    let blob = upload(&s, &a, &random_bytes(5000), "image/jpeg").await;
    s.create_record(&a, "com.example.media", json!({"$type": "com.example.media", "file": blob})).await;
    assert_eq!(list_blobs(&s, &a.did).await, vec![link(&blob)]);
    let zeros = Cid::raw(&vec![0u8; 16 * 64 * 1024]).to_string();
    get_blob(&s, &a.did, &zeros).await.err(400, "BlobNotFound");
}

/// crud.test.ts "doesn't serve taken-down actor": listRecords for a
/// taken-down repo is the reference's 400 "Could not find repo".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_taken_down_actor_records_not_served() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let p = s.post(&a, "hello").await;
    set_repo_takedown(&s, &a.did, true).await;
    let r = s.list_records(&a.did, "app.bsky.feed.post", &[]).await;
    r.err_status(400);
    assert!(r.text().contains("Could not find repo"), "{}", r.text());
    s.get_record(&a.did, "app.bsky.feed.post", p.rkey()).await.err_status(400);
    // restored
    set_repo_takedown(&s, &a.did, false).await;
    let l = s.list_records(&a.did, "app.bsky.feed.post", &[]).await.ok();
    assert_eq!(l["records"].as_array().unwrap().len(), 1);
}

/// crud.test.ts "prevents duplicate likes / reposts / blocks / follows":
/// creating a like/repost (same subject.uri) or follow/block (same subject
/// DID) deletes the account's earlier record with that subject in the same
/// commit (reference createRecord `getBacklinkConflicts`). Other accounts'
/// records are unaffected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_prevents_duplicate_backlinks() {
    let s = TestServer::spawn().await;
    let alice = s.create_account("alice").await;
    let bob = s.create_account("bob").await;
    let now = now_iso();
    let post_a = format!("at://{}/app.bsky.feed.post/3jzfcijpj2z2a", bob.did);
    let post_b = format!("at://{}/app.bsky.feed.post/3jzfcijpj2z2b", bob.did);
    let cid_a = Cid::dag_cbor(b"a").to_string();
    let cid_b = Cid::dag_cbor(b"b").to_string();
    for coll in ["app.bsky.feed.like", "app.bsky.feed.repost"] {
        let rec = |uri: &str, cid: &str| json!({"$type": coll, "subject": {"uri": uri, "cid": cid}, "createdAt": now});
        let one = s.create_record(&alice, coll, rec(&post_a, &cid_a)).await;
        let two = s.create_record(&alice, coll, rec(&post_b, &cid_b)).await;
        let three = s.create_record(&alice, coll, rec(&post_a, &cid_a)).await;
        let r = s.get_record(&alice.did, coll, one.rkey()).await;
        r.err_status(400);
        assert!(r.text().contains("Could not locate record"), "{coll}: {}", r.text());
        s.get_record(&alice.did, coll, two.rkey()).await.ok();
        s.get_record(&alice.did, coll, three.rkey()).await.ok();
    }
    for coll in ["app.bsky.graph.block", "app.bsky.graph.follow"] {
        let one = s.create_record(&alice, coll, json!({"$type": coll, "subject": bob.did, "createdAt": now})).await;
        let two = s.create_record(&bob, coll, json!({"$type": coll, "subject": alice.did, "createdAt": now})).await;
        let three = s.create_record(&alice, coll, json!({"$type": coll, "subject": bob.did, "createdAt": now})).await;
        let r = s.get_record(&alice.did, coll, one.rkey()).await;
        r.err_status(400);
        assert!(r.text().contains("Could not locate record"), "{coll}: {}", r.text());
        s.get_record(&bob.did, coll, two.rkey()).await.ok();
        s.get_record(&alice.did, coll, three.rkey()).await.ok();
    }
}

/// preferences.test.ts error messages: auth required, namespace, missing
/// `$type`, app-password writes of permissioned prefs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_preferences_error_messages() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let put = |auth: Auth, prefs: J| {
        let s = &s;
        async move { s.xrpc.post("app.bsky.actor.putPreferences", &json!({"preferences": prefs}), &auth).await }
    };
    let adult = json!({"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false});
    let r = put(Auth::None, json!([adult])).await;
    r.err(401, "AuthenticationRequired");
    assert!(r.text().contains("Authentication Required"), "{}", r.text());
    let r = s.xrpc.get("app.bsky.actor.getPreferences", &[], &Auth::None).await;
    r.err(401, "AuthenticationRequired");
    assert!(r.text().contains("Authentication Required"), "{}", r.text());

    let r = put(a.auth(), json!([adult, {"$type": "com.atproto.server.defs#unknown", "hello": "world"}])).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("Some preferences are not in the app.bsky namespace"), "{}", r.text());

    let r = put(a.auth(), json!([adult, {"label": "dogs", "visibility": "warn"}])).await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("$type"), "{}", r.text());
    let r = s.xrpc.get("app.bsky.actor.getPreferences", &[], &a.auth()).await;
    assert_eq!(r.ok(), json!({"preferences": []}), "failed puts must not change prefs");

    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "ap"}), &a.auth()).await.ok();
    let sess = s.create_session(&a.handle, ap["password"].as_str().unwrap()).await.ok();
    let ap_auth = Auth::Bearer(sess["accessJwt"].as_str().unwrap().to_string());
    let r = put(ap_auth, json!([{"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": now_iso()}])).await;
    r.client_err();
    assert!(r.text().contains("Do not have authorization to set preferences"), "{}", r.text());
}
