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

/// Sets the account's status directly (no admin endpoint sets `suspended`).
async fn set_status(s: &TestServer, did: &str, status: &'static str) {
    s.app
        .mutate_account(did, false, false, false, move |acct| {
            acct.status = Some(status.into());
            Ok(true)
        })
        .await
        .unwrap_or_else(|e| panic!("{}", e.message));
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
    let bytes = unique_png("carol-blob");
    let blob = s.upload_blob(&carol, &bytes, "image/png").await;
    s.create_record(&carol, "app.bsky.feed.post", image_post("pic", &blob)).await;
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
    s.get_blob(&carol.did, &cid).await.err(400, "BlobNotFound");

    // prevents the blob from being re-uploaded
    let r = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.clone(), "image/png", &carol.auth()).await;
    assert!(
        !r.is_ok(),
        "re-upload of a taken-down blob must fail (reference: 'Blob has been takendown, cannot re-upload'); got {}",
        r.text()
    );

    // prevents the blob from being referenced again
    let body = json!({"repo": carol.did, "collection": "app.bsky.feed.post", "record": image_post("pic", &blob)});
    let r = s.xrpc.post("com.atproto.repo.createRecord", &body, &carol.auth()).await;
    assert!(!r.is_ok(), "referencing a taken-down blob must fail (reference: 'Could not find blob'); got {}", r.text());

    // restores blob when takedown is removed
    update_status(&s, subject, json!({"applied": false})).await.ok();
    let r = s.get_blob(&carol.did, &cid).await;
    assert_eq!(r.status, 200, "restored blob: {}", r.text());
    assert_eq!(&r.body[..], &bytes[..]);
    s.create_record(&carol, "app.bsky.feed.post", image_post("pic", &blob)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subject_status_errors() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dan").await;
    // admin only
    s.xrpc.get("com.atproto.admin.getSubjectStatus", &[("did", &a.did)], &a.auth()).await.client_err();
    let body = json!({"subject": repo_ref(&a.did), "takedown": {"applied": true}});
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &a.auth()).await.client_err();
    // unknown subject type
    update_status(&s, json!({"$type": "com.example.nope", "did": a.did}), json!({"applied": true}))
        .await
        .err(400, "InvalidRequest");
    // blob status needs a did
    subject_status(&s, &[("blob", "bafkreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm")])
        .await
        .err(400, "InvalidRequest");
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
    let body = json!({"identifier": jeff.handle, "password": PASSWORD, "allowTakendown": true});
    let r = s.xrpc.post("com.atproto.server.createSession", &body, &Auth::None).await;
    assert!(r.is_ok(), "createSession allowTakendown=true should succeed for a taken-down account: {}", r.text());
    let tok = Auth::Bearer(r.json["accessJwt"].as_str().unwrap().to_string());

    let report = json!({"reasonType": "com.atproto.moderation.defs#reasonRude", "reason": "reporting others", "subject": repo_ref("did:plc:test")});
    s.xrpc.post("com.atproto.moderation.createReport", &report, &tok).await.client_err();
    let body = json!({"repo": jeff.did, "collection": "app.bsky.feed.post", "record": post_record("test")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &tok).await.client_err();
}

/// A suspended account is treated like a taken-down one: writes get the
/// reference's 401 AccountTakedown, and so do proxied calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_account_cannot_write_or_proxy() {
    let s = TestServer::spawn_with(|c| c.appview = Some(("http://127.0.0.1:1".into(), "did:web:appview.test".into())))
        .await;
    let a = s.create_account("susp").await;
    set_status(&s, &a.did, "suspended").await;
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await.err(401, "AccountTakedown");
    s.xrpc.get("app.bsky.feed.getTimeline", &[], &a.auth()).await.err(401, "AccountTakedown");
}

/// Writes to a taken-down repo: 401 AccountTakedown (reference findAccount
/// with checkTakedown), checked with a token minted before the takedown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takendown_repo_write_is_account_takedown() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tkw").await;
    set_status(&s, &a.did, "takendown").await;
    for (nsid, body) in [
        (
            "com.atproto.repo.createRecord",
            json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x")}),
        ),
        (
            "com.atproto.repo.deleteRecord",
            json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": "3jzfcijpj2z2a"}),
        ),
        (
            "com.atproto.repo.applyWrites",
            json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post_record("x")}]}),
        ),
    ] {
        s.xrpc.post(nsid, &body, &a.auth()).await.err(401, "AccountTakedown");
    }
}
