//! The blob GC's check-then-delete race: a write that checked its blob just
//! before a sweep collected it, and applies its reference just after, must
//! not lose the blob. The sweep quarantines instead of deleting, re-checks
//! the references after a settle time and moves the blob back if one
//! appeared (`blobs::sweep_blobs_settle`).
use crate::common::*;
use std::time::Duration;

const HOUR: Duration = Duration::from_secs(3600);

async fn blob_status(s: &TestServer, did: &str, cid: &str) -> u16 {
    s.get_blob(did, cid).await.status
}

async fn sweep(s: &TestServer, settle: Duration) -> usize {
    vlpds::xrpc::blobs::sweep_blobs_settle(&s.app, Duration::ZERO, settle).await.expect("sweep").1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reference_applied_after_collection_restores_the_blob() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gcr").await;
    let bytes = random_png(1);
    let blob = s.upload_blob(&a, &bytes, "image/png").await;
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    let post = s.create_record(&a, "app.bsky.feed.post", image_post("img", &blob)).await;
    // a repo version that references the blob, for later
    let car = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await.body.to_vec();
    s.delete_record(&a, "app.bsky.feed.post", post.rkey()).await.ok();

    // collected: no longer served, and a write checking it now fails
    assert_eq!(sweep(&s, HOUR).await, 1);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": image_post("late", &blob)});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await.err(400, "BlobNotFound");
    // still quarantined until the settle time has passed
    assert_eq!(sweep(&s, HOUR).await, 0);
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);

    // the reference lands after the collection (importRepo doesn't check
    // blobs, so it stands in for a write that checked before the move)
    s.xrpc.post_bytes("com.atproto.repo.importRepo", car, "application/vnd.ipld.car", &a.auth()).await.ok();
    sweep(&s, Duration::ZERO).await;
    let g = s.get_blob(&a.did, &cid).await;
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
    let blob = s.upload_blob(&a, &random_png(2), "image/png").await;
    let cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    assert_eq!(sweep(&s, HOUR).await, 1);
    let root = object_store::path::Path::from(format!("{}/blob-gc", s.app.store.prefix));
    assert_eq!(objects(&s, &root).await, 1, "quarantined");
    sweep(&s, Duration::ZERO).await;
    assert_eq!(objects(&s, &root).await, 0, "purged");
    assert_eq!(blob_status(&s, &a.did, &cid).await, 400);
}

async fn objects(s: &TestServer, path: &object_store::path::Path) -> usize {
    let l = object_store::ObjectStore::list(&*s.app.store.raw, Some(path));
    futures::StreamExt::count(l).await
}

/// A write that checked a blob and hasn't applied yet (a slow apply: cold
/// repo load, store brownout) holds it: past the settle time the
/// quarantined copy is kept while the write is in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_write_holds_a_quarantined_blob() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gch").await;
    let blob = s.upload_blob(&a, &random_png(3), "image/png").await;
    let cid = Cid::parse(blob["ref"]["$link"].as_str().unwrap()).unwrap();
    assert_eq!(sweep(&s, HOUR).await, 1);
    let held = vlpds::xrpc::blobs::HeldBlobs::hold(&a.did, [cid]);
    let path = object_store::path::Path::from(format!("{}/blob-gc/{}", s.app.store.prefix, a.did));
    sweep(&s, Duration::ZERO).await;
    assert_ne!(objects(&s, &path).await, 0, "purged under an in-flight write");
    drop(held);
    sweep(&s, Duration::ZERO).await;
    assert_eq!(objects(&s, &path).await, 0, "purged once the write is gone");
}
