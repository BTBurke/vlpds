//! Smoke tests for the harness itself: boot, account, write, firehose, repo export.
mod common;
use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_write_and_observe() {
    let s = TestServer::spawn().await;
    let health = s.xrpc.get("_health", &[], &Auth::None).await;
    assert_eq!(health.status, 200);

    let mut sub = s.subscribe(Some(0)).await;
    let a = s.create_account("smoke").await;
    let r = s.post(&a, "hello").await;
    assert!(r
        .uri
        .starts_with(&format!("at://{}/app.bsky.feed.post/", a.did)));

    let frames = sub.wait_for(FH_TIMEOUT, &a.did, "#commit").await;
    let c = frames.last().unwrap().commit().unwrap();
    assert_eq!(c.ops.len(), 1);
    assert_eq!(c.invert().unwrap(), c.prev_data.unwrap());
    let key = s.signing_key(&a.did).await;
    c.commit_obj().verify(&key).unwrap();

    let repo = s.get_repo(&a.did).await;
    repo.commit().verify(&key).unwrap();
    assert_eq!(repo.entries().len(), 1);
}
