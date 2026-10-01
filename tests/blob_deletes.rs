//! Port of atproto/packages/pds/tests/blob-deletes.test.ts. vlpds deletes
//! unreferenced blobs with a GC sweep (`blobs::sweep_blobs`) rather than in
//! the write transaction, so each test drops references, runs a sweep with
//! zero grace, and checks listBlobs/getBlob from the outside.
mod common;
use common::*;
use std::time::Duration;

async fn upload(s: &TestServer, a: &TestAccount, bytes: &[u8]) -> J {
    s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.to_vec(), "image/jpeg", &a.auth()).await.ok()["blob"].clone()
}

fn file(tag: u8) -> Vec<u8> {
    let mut v: Vec<u8> = (0..4000).map(|_| rand::random::<u8>()).collect();
    v[0] = tag;
    v
}

fn link(blob: &J) -> String {
    blob["ref"]["$link"].as_str().unwrap().to_string()
}

async fn post_with_image(s: &TestServer, a: &TestAccount, blob: &J) -> RecordRef {
    s.create_record(
        a,
        "app.bsky.feed.post",
        json!({"$type": "app.bsky.feed.post", "text": "img", "createdAt": now_iso(),
               "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": "alt"}]}}),
    )
    .await
}

async fn update_profile(s: &TestServer, a: &TestAccount, avatar: Option<&J>, banner: Option<&J>) {
    let mut rec = json!({"$type": "app.bsky.actor.profile"});
    if let Some(b) = avatar {
        rec["avatar"] = b.clone();
    }
    if let Some(b) = banner {
        rec["banner"] = b.clone();
    }
    s.xrpc
        .post("com.atproto.repo.putRecord", &json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": rec}), &a.auth())
        .await
        .ok();
}

async fn listed(s: &TestServer, did: &str) -> Vec<String> {
    let j = s.xrpc.get("com.atproto.sync.listBlobs", &[("did", did)], &Auth::None).await.ok();
    let mut v: Vec<String> = j["cids"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_string()).collect();
    v.sort();
    v
}

async fn stored(s: &TestServer, did: &str, cid: &str) -> bool {
    let r = s.xrpc.get("com.atproto.sync.getBlob", &[("did", did), ("cid", cid)], &Auth::None).await;
    match r.status {
        200 => true,
        _ => {
            r.err(400, "BlobNotFound");
            false
        }
    }
}

async fn gc(s: &TestServer) {
    vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::ZERO).await.expect("blob gc sweep");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_blob_when_record_is_deleted() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let post = post_with_image(&s, &a, &img).await;
    assert_eq!(listed(&s, &a.did).await, vec![link(&img)]);
    s.xrpc
        .post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": post.rkey()}), &a.auth())
        .await
        .ok();
    assert!(listed(&s, &a.did).await.is_empty(), "listBlobs must drop the blob once unreferenced");
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&img)).await, "unreferenced blob must be deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_blob_when_ref_is_updated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let img2 = upload(&s, &a, &file(2)).await;
    update_profile(&s, &a, Some(&img), Some(&img)).await;
    update_profile(&s, &a, Some(&img2), Some(&img2)).await;
    assert_eq!(listed(&s, &a.did).await, vec![link(&img2)]);
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&img)).await);
    assert!(stored(&s, &a.did, &link(&img2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_blob_when_ref_is_not_updated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let img2 = upload(&s, &a, &file(2)).await;
    update_profile(&s, &a, Some(&img), Some(&img)).await;
    update_profile(&s, &a, Some(&img), Some(&img2)).await;
    let mut want = vec![link(&img), link(&img2)];
    want.sort();
    assert_eq!(listed(&s, &a.did).await, want);
    gc(&s).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
    assert!(stored(&s, &a.did, &link(&img2)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_blob_reused_by_another_record_in_same_commit() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(1)).await;
    let post = post_with_image(&s, &a, &img).await;
    s.xrpc
        .post(
            "com.atproto.repo.applyWrites",
            &json!({"repo": a.did, "writes": [
                {"$type": "com.atproto.repo.applyWrites#delete", "collection": "app.bsky.feed.post", "rkey": post.rkey()},
                {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": {
                    "$type": "app.bsky.feed.post", "text": "post2", "createdAt": now_iso(),
                    "embed": {"$type": "app.bsky.embed.images", "images": [{"image": img, "alt": "alt"}]}}},
            ]}),
            &a.auth(),
        )
        .await
        .ok();
    assert_eq!(listed(&s, &a.did).await, vec![link(&img)]);
    gc(&s).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_from_own_store_even_if_another_user_uses_it() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let bytes = file(9);
    let ia = upload(&s, &a, &bytes).await;
    let ib = upload(&s, &b, &bytes).await;
    assert_eq!(link(&ia), link(&ib));
    let pa = post_with_image(&s, &a, &ia).await;
    post_with_image(&s, &b, &ib).await;
    s.xrpc
        .post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": pa.rkey()}), &a.auth())
        .await
        .ok();
    gc(&s).await;
    assert!(!stored(&s, &a.did, &link(&ia)).await, "alice's copy is gone");
    assert!(stored(&s, &b.did, &link(&ib)).await, "bob's copy remains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_grace_protects_recent_uploads() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let img = upload(&s, &a, &file(3)).await;
    // nothing references it yet, but it's within the grace period
    vlpds::xrpc::blobs::sweep_blobs(&s.app, Duration::from_secs(3600)).await.unwrap();
    // so it can still be referenced and served
    post_with_image(&s, &a, &img).await;
    assert!(stored(&s, &a.did, &link(&img)).await);
}
