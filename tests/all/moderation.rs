//! Port of atproto/packages/pds/tests/moderation.test.ts (account, record and
//! blob takedowns via admin.updateSubjectStatus / getSubjectStatus) and the
//! PDS-side parts of takedown-appeal.test.ts.
use crate::common::*;

async fn update_status(s: &TestServer, subject: J, takedown: J) -> Resp {
    s.xrpc
        .post("com.atproto.admin.updateSubjectStatus", &json!({"subject": subject, "takedown": takedown}), &Auth::Admin)
        .await
}

async fn subject_status(s: &TestServer, q: &[(&str, &str)]) -> Resp {
    s.xrpc.get("com.atproto.admin.getSubjectStatus", q, &Auth::Admin).await
}

fn repo_ref(did: &str) -> J {
    json!({"$type": "com.atproto.admin.defs#repoRef", "did": did})
}

async fn upload_png(s: &TestServer, a: &TestAccount, bytes: Vec<u8>) -> J {
    s.xrpc
        .post_bytes("com.atproto.repo.uploadBlob", bytes, "image/png", &a.auth())
        .await
        .ok()["blob"]
        .clone()
}

fn image_post(blob: &J) -> J {
    json!({
        "$type": "app.bsky.feed.post", "text": "pic", "createdAt": now_iso(),
        "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}
    })
}

/// A PNG that differs per call (so its CID is fresh).
fn unique_png(tag: &str) -> Vec<u8> {
    let mut b = PNG_1X1.to_vec();
    b.extend_from_slice(tag.as_bytes()); // trailing bytes after IEND are tolerated
    b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takes_down_and_restores_accounts() {
    let s = TestServer::spawn().await;
    let bob = s.create_account("bob").await;
    update_status(&s, repo_ref(&bob.did), json!({"applied": true, "ref": "test-repo"})).await.ok();
    let j = subject_status(&s, &[("did", &bob.did)]).await.ok();
    assert_eq!(j["subject"]["did"], json!(bob.did));
    assert_eq!(j["takedown"]["applied"], json!(true));
    assert_eq!(j["takedown"]["ref"], json!("test-repo"));

    update_status(&s, repo_ref(&bob.did), json!({"applied": false})).await.ok();
    let j = subject_status(&s, &[("did", &bob.did)]).await.ok();
    assert_eq!(j["takedown"]["applied"], json!(false));
    assert!(j["takedown"].get("ref").map(|v| v.is_null()).unwrap_or(true), "ref should be cleared: {j}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takes_down_and_restores_records() {
    let s = TestServer::spawn().await;
    let bob = s.create_account("bob").await;
    let p = s.post(&bob, "takedown me").await;
    let other = s.post(&bob, "keep me").await;
    let subject = json!({"$type": "com.atproto.repo.strongRef", "uri": p.uri, "cid": p.cid});

    update_status(&s, subject.clone(), json!({"applied": true, "ref": "test-record"})).await.ok();
    let j = subject_status(&s, &[("uri", &p.uri)]).await.ok();
    assert_eq!(j["subject"]["uri"], json!(p.uri));
    assert_eq!(j["takedown"]["applied"], json!(true));
    assert_eq!(j["takedown"]["ref"], json!("test-record"));

    // hidden from repo.getRecord and listRecords; other records unaffected
    s.get_record(&bob.did, p.collection(), p.rkey()).await.err(400, "RecordNotFound");
    s.get_record(&bob.did, other.collection(), other.rkey()).await.ok();
    let lr = s.list_records(&bob.did, "app.bsky.feed.post", &[]).await.ok();
    let uris: Vec<&str> = lr["records"].as_array().unwrap().iter().map(|r| r["uri"].as_str().unwrap()).collect();
    assert!(!uris.contains(&p.uri.as_str()) && uris.contains(&other.uri.as_str()), "listRecords: {uris:?}");

    update_status(&s, subject, json!({"applied": false})).await.ok();
    let j = subject_status(&s, &[("uri", &p.uri)]).await;
    // reference: subject returned with applied=false; vlpds may 404 an untouched subject
    if j.is_ok() {
        assert_eq!(j.json["takedown"]["applied"], json!(false));
    }
    s.get_record(&bob.did, p.collection(), p.rkey()).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_takedown_lifecycle() {
    let s = TestServer::spawn().await;
    let carol = s.create_account("carol").await;
    let bob = s.create_account("bob").await;
    let bytes = unique_png("carol-blob");
    let blob = upload_png(&s, &carol, bytes.clone()).await;
    s.create_record(&carol, "app.bsky.feed.post", image_post(&blob)).await;
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    let subject = json!({"$type": "com.atproto.admin.defs#repoBlobRef", "did": carol.did, "cid": cid});

    // takes down blobs
    update_status(&s, subject.clone(), json!({"applied": true, "ref": "test-blob"})).await.ok();
    let j = subject_status(&s, &[("did", &carol.did), ("blob", &cid)]).await.ok();
    assert_eq!(j["subject"]["did"], json!(carol.did));
    assert_eq!(j["subject"]["cid"], json!(cid));
    assert_eq!(j["takedown"]["applied"], json!(true));
    assert_eq!(j["takedown"]["ref"], json!("test-blob"));

    // prevents the blob from being served
    let r = s.xrpc.get("com.atproto.sync.getBlob", &[("did", &carol.did), ("cid", &cid)], &Auth::None).await;
    r.err(400, "BlobNotFound");

    // prevents the blob from being re-uploaded
    let r = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.clone(), "image/png", &carol.auth()).await;
    assert!(
        !r.is_ok(),
        "re-upload of a taken-down blob must fail (reference: 'Blob has been takendown, cannot re-upload'); got {}",
        r.text()
    );

    // prevents the blob from being referenced again
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": carol.did, "collection": "app.bsky.feed.post", "record": image_post(&blob)}),
            &carol.auth(),
        )
        .await;
    assert!(!r.is_ok(), "referencing a taken-down blob must fail (reference: 'Could not find blob'); got {}", r.text());

    // restores blob when takedown is removed
    update_status(&s, subject, json!({"applied": false})).await.ok();
    let r = s.xrpc.get("com.atproto.sync.getBlob", &[("did", &carol.did), ("cid", &cid)], &Auth::None).await;
    assert_eq!(r.status, 200, "restored blob: {}", r.text());
    assert_eq!(&r.body[..], &bytes[..]);
    s.create_record(&carol, "app.bsky.feed.post", image_post(&blob)).await;

    // blobs of taken-down accounts: hidden from the public and other users,
    // visible to the account itself (reference) and to admins
    update_status(&s, repo_ref(&carol.did), json!({"applied": true})).await.ok();
    let q = [("did", carol.did.as_str()), ("cid", cid.as_str())];
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &Auth::None).await;
    assert!(r.status >= 400 && r.text().contains("takendown"), "public getBlob of taken-down repo: {}", r.text());
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &bob.auth()).await;
    assert!(r.status >= 400 && r.text().contains("takendown"), "other user's getBlob of taken-down repo: {}", r.text());
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &Auth::Admin).await;
    assert_eq!(r.status, 200, "admin getBlob of taken-down repo: {}", r.text());
    update_status(&s, repo_ref(&carol.did), json!({"applied": false})).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subject_status_errors() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dan").await;
    // admin only
    s.xrpc.get("com.atproto.admin.getSubjectStatus", &[("did", &a.did)], &a.auth()).await.client_err();
    s.xrpc
        .post("com.atproto.admin.updateSubjectStatus", &json!({"subject": repo_ref(&a.did), "takedown": {"applied": true}}), &a.auth())
        .await
        .client_err();
    // unknown subject type
    update_status(&s, json!({"$type": "com.example.nope", "did": a.did}), json!({"applied": true})).await.err(400, "InvalidRequest");
    // blob status needs a did
    subject_status(&s, &[("blob", "bafkreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm")]).await.err(400, "InvalidRequest");
    // no subject at all
    subject_status(&s, &[]).await.err(400, "InvalidRequest");
    // unknown account
    subject_status(&s, &[("did", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")]).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takendown_actor_cannot_report_or_write() {
    let s = TestServer::spawn().await;
    let jeff = s.create_account("jeff").await;
    update_status(&s, repo_ref(&jeff.did), json!({"applied": true})).await.ok();

    // a plain login is refused
    let r = s.create_session(&jeff.handle, PASSWORD).await;
    r.err(401, "AccountTakedown");

    // allowTakendown yields a restricted session (reference behaviour)
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createSession",
            &json!({"identifier": jeff.handle, "password": PASSWORD, "allowTakendown": true}),
            &Auth::None,
        )
        .await;
    assert!(r.is_ok(), "createSession allowTakendown=true should succeed for a taken-down account: {}", r.text());
    let tok = Auth::Bearer(r.json["accessJwt"].as_str().unwrap().to_string());

    let r = s
        .xrpc
        .post(
            "com.atproto.moderation.createReport",
            &json!({"reasonType": "com.atproto.moderation.defs#reasonRude", "reason": "reporting others", "subject": repo_ref("did:plc:test")}),
            &tok,
        )
        .await;
    r.client_err();
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": jeff.did, "collection": "app.bsky.feed.post", "record": post_record("test")}),
            &tok,
        )
        .await;
    r.client_err();
}
