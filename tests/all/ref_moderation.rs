//! Reference PDS moderation tests (packages/pds/tests/moderation.test.ts) not
//! covered by `moderation`; see tests/REFERENCE_COVERAGE.md.
use crate::common::*;

/// moderation.test.ts "blob takedown > prevents blobs of takendown accounts
/// from being served": the public and other users get RepoTakendown; the
/// account itself and admins may still fetch the blob.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takendown_account_blobs_are_served_to_owner_and_admin_only() {
    let s = TestServer::spawn().await;
    let carol = s.create_account("carol").await;
    let bob = s.create_account("bob").await;
    let mut bytes = PNG_1X1.to_vec();
    bytes.extend_from_slice(unique_name("blob").as_bytes());
    let blob = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.clone(), "image/png", &carol.auth()).await.ok()["blob"].clone();
    s.create_record(
        &carol,
        "app.bsky.feed.post",
        json!({"$type": "app.bsky.feed.post", "text": "pic", "createdAt": now_iso(),
               "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}}),
    )
    .await;
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    set_repo_takedown(&s, &carol.did, true).await;
    let q = [("did", carol.did.as_str()), ("cid", cid.as_str())];
    s.xrpc.get("com.atproto.sync.getBlob", &q, &Auth::None).await.err(400, "RepoTakendown");
    s.xrpc.get("com.atproto.sync.getBlob", &q, &bob.auth()).await.err(400, "RepoTakendown");
    // the owner's still-live access token (takedown keeps those valid, as in the reference)
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &carol.auth()).await;
    assert_eq!(r.status, 200, "owner getBlob of its taken-down repo: {}", r.text());
    assert_eq!(&r.body[..], &bytes[..]);
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &Auth::Admin).await;
    assert_eq!(r.status, 200, "admin getBlob: {}", r.text());

    set_repo_takedown(&s, &carol.did, false).await;
    let r = s.xrpc.get("com.atproto.sync.getBlob", &q, &Auth::None).await;
    assert_eq!(r.status, 200, "public getBlob after the takedown is lifted: {}", r.text());
}
