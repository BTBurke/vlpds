//! The blob GC's check-then-delete race: a write that checked its blob just
//! before a sweep collected it, and applies its reference just after, must
//! not lose the blob. The sweep quarantines instead of deleting, re-checks
//! the references after a settle time and moves the blob back if one
//! appeared (`blobs::sweep_blobs_settle`).
use crate::common::*;
use std::time::Duration;

const HOUR: Duration = Duration::from_secs(3600);

fn png(tag: u8) -> Vec<u8> {
    let mut v = b"\x89PNG\r\n\x1a\n".to_vec();
    v.extend((0..2000).map(|_| rand::random::<u8>()));
    v.push(tag);
    v
}

async fn blob_status(s: &TestServer, did: &str, cid: &str) -> u16 {
    s.xrpc.get("com.atproto.sync.getBlob", &[("did", did), ("cid", cid)], &Auth::None).await.status
}

async fn sweep(s: &TestServer, settle: Duration) -> usize {
    vlpds::xrpc::blobs::sweep_blobs_settle(&s.app, Duration::ZERO, settle).await.expect("sweep").1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reference_applied_after_collection_restores_the_blob() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gcr").await;
    let bytes = png(1);
    let blob = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", bytes.clone(), "image/png", &a.auth()).await.ok()["blob"].clone();
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    let post = s
        .create_record(
            &a,
            "app.bsky.feed.post",
            json!({"$type": "app.bsky.feed.post", "text": "img", "createdAt": now_iso(),
                   "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}}),
        )
        .await;
    // a repo version that references the blob, for later
    let car = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await.body.to_vec();
    s.xrpc
        .post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": post.rkey()}), &a.auth())
        .await
        .ok();

    // collected: no longer served, and a write checking it now fails
    assert_eq!(sweep(&s, HOUR).await, 1);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "text": "late", "createdAt": now_iso(),
                     "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}}}),
            &a.auth(),
        )
        .await;
    r.err(400, "BlobNotFound");
    // still quarantined until the settle time has passed
    assert_eq!(sweep(&s, HOUR).await, 0);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);

    // the reference lands after the collection (importRepo doesn't check
    // blobs, so it stands in for a write that checked before the move)
    s.xrpc.post_bytes("com.atproto.repo.importRepo", car, "application/vnd.ipld.car", &a.auth()).await.ok();
    sweep(&s, Duration::ZERO).await;
    let g = s.xrpc.get("com.atproto.sync.getBlob", &[("did", &a.did), ("cid", &cid)], &Auth::None).await;
    assert_eq!(g.status, 200, "restored");
    assert_eq!(&g.body[..], &bytes[..]);
    assert_eq!(g.header("content-type").as_deref(), Some("image/png"));
    let missing = s.xrpc.get("com.atproto.repo.listMissingBlobs", &[], &a.auth()).await.ok();
    assert_eq!(missing["blobs"], json!([]));
    // and later sweeps leave it alone
    assert_eq!(sweep(&s, Duration::ZERO).await, 0);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreferenced_blob_is_purged_after_settling() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gcp").await;
    let blob = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", png(2), "image/png", &a.auth()).await.ok()["blob"].clone();
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    assert_eq!(sweep(&s, HOUR).await, 1);
    let quarantined = || async {
        let mut n = 0;
        let root = object_store::path::Path::from(format!("{}/blob-gc", s.app.store.prefix));
        let mut l = object_store::ObjectStore::list(&*s.app.store.raw, Some(&root));
        while futures::StreamExt::next(&mut l).await.is_some() {
            n += 1;
        }
        n
    };
    assert_eq!(quarantined().await, 1);
    sweep(&s, Duration::ZERO).await;
    assert_eq!(quarantined().await, 0);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);
}
