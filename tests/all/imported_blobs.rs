//! checkAccountStatus `importedBlobs` (every blob the account stores,
//! referenced or not) follows uploads, re-uploads, the GC's quarantine and
//! restore, and equals a LIST of the account's blob objects throughout.
use crate::common::*;
use std::time::Duration;

const HOUR: Duration = Duration::from_secs(3600);

async fn imported(s: &TestServer, a: &TestAccount) -> (u64, u64) {
    let st = s.xrpc.get("com.atproto.server.checkAccountStatus", &[], &a.auth()).await.ok();
    let listed = futures::StreamExt::count(object_store::ObjectStore::list(&*s.app.store.raw, Some(&object_store::path::Path::from(format!("{}/blob/{}", s.app.store.prefix, a.did))))).await as u64;
    (st["importedBlobs"].as_u64().unwrap(), listed)
}

async fn sweep(s: &TestServer, settle: Duration) {
    vlpds::xrpc::blobs::sweep_blobs_settle(&s.app, Duration::ZERO, settle).await.expect("sweep");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imported_blobs_follow_uploads_and_the_gc() {
    let s = TestServer::spawn().await;
    let a = s.create_account("imb").await;
    let other = s.create_account("imb").await;
    assert_eq!(imported(&s, &a).await, (0, 0));

    let pngs: Vec<Vec<u8>> = (0..4).map(|i| random_png(100 + i)).collect();
    let blobs: Vec<J> = futures::future::join_all(pngs.iter().map(|p| s.upload_blob(&a, p, "image/png"))).await;
    // the same bytes again, and by another account: counted once, per account
    s.upload_blob(&a, &pngs[0], "image/png").await;
    s.upload_blob(&other, &pngs[0], "image/png").await;
    assert_eq!(imported(&s, &a).await, (4, 4));
    assert_eq!(imported(&s, &other).await, (1, 1));

    // two referenced (one of them later only by a CAR import), two not
    s.create_record(&a, "app.bsky.feed.post", image_post("kept", &blobs[0])).await;
    let late = s.create_record(&a, "app.bsky.feed.post", image_post("late", &blobs[1])).await;
    let car = s.get_repo_car(&a.did).await;
    s.delete_record(&a, "app.bsky.feed.post", late.rkey()).await.ok();

    // quarantined: no longer stored
    sweep(&s, HOUR).await;
    assert_eq!(imported(&s, &a).await, (1, 1));
    // a reference that lands during the quarantine restores its blob
    s.import_repo(&a.auth(), car).await.ok();
    sweep(&s, Duration::ZERO).await;
    assert_eq!(imported(&s, &a).await, (2, 2));
    // a quarantined blob uploaded again is stored again
    s.upload_blob(&a, &pngs[3], "image/png").await;
    assert_eq!(imported(&s, &a).await, (3, 3));
    let st = s.xrpc.get("com.atproto.server.checkAccountStatus", &[], &a.auth()).await.ok();
    assert_eq!(st["expectedBlobs"], json!(2), "{st}");
    assert_eq!(imported(&s, &other).await, (0, 0), "unreferenced: collected");
}
