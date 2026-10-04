//! vlpds.admin.bulkCreate (the capacity test's population path): resuming a
//! range is idempotent (existing accounts are found in storage, not just in
//! the repo cache), per-account record counts, explicit index lists, and
//! accounts outside this node's shards reported as `notOwned`.

use crate::common::*;
use std::sync::Arc;
use vlpds::state::bulk_did;

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>) -> TestServer {
    cluster_node(id, store.clone(), 4, |_| {}).await
}

async fn bulk(s: &TestServer, body: J) -> J {
    let r = s.xrpc.post("vlpds.admin.bulkCreate", &body, &Auth::Bearer(ADMIN_TOKEN.into())).await;
    assert_eq!(r.status, 200, "{}", r.text());
    r.json
}

/// (head commit, signing key, records) of bulk account `i`.
async fn fingerprint(s: &TestServer, i: u64) -> (String, String, usize) {
    let did = bulk_did(i);
    let (head, _) = s.latest_commit(&did).await;
    let p = s.app.partitions.for_key(&did).expect("owned");
    let acct = p.db.get(vlpds::state::account_key(&did)).await.unwrap().expect("account");
    let acct: J = serde_json::from_slice(&acct).unwrap();
    let recs = s.list_records(&did, "app.bsky.feed.post", &[("limit", "100")]).await.ok();
    (
        head.to_string(),
        acct["wrapped_signing_key"].as_str().unwrap_or_default().to_string(),
        recs["records"].as_array().unwrap().len(),
    )
}

/// A resumed chunk creates nothing: the second run finds every account
/// (the worker still holds them), and so does a third after a restart (cold
/// cache: only storage knows). Keys, heads and records are untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_is_idempotent() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("bk", &store).await;
    let records: Vec<u32> = (0..60).map(|i| i % 4).collect();
    let req = json!({"start": 1000, "count": 60, "records": records});
    let v = bulk(&a, req.clone()).await;
    assert_eq!(
        (v["created"].as_u64(), v["existing"].as_u64(), v["failed"].as_u64()),
        (Some(60), Some(0), Some(0)),
        "{v}"
    );
    assert_eq!(v["records"].as_u64(), Some(records.iter().map(|&n| n as u64).sum()));
    let before: Vec<_> = futures::future::join_all((1000..1060).step_by(7).map(|i| fingerprint(&a, i))).await;
    for (k, f) in before.iter().enumerate() {
        assert_eq!(f.2, records[k * 7] as usize, "per-account record counts");
    }

    let v = bulk(&a, req.clone()).await;
    assert_eq!(
        (v["created"].as_u64(), v["existing"].as_u64(), v["failed"].as_u64()),
        (Some(0), Some(60), Some(0)),
        "{v}"
    );

    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;
    let b = node("bk", &store).await;
    // an overlapping resume: 60 exist, 20 are new
    let v = bulk(&b, json!({"start": 1000, "count": 80, "records": 1})).await;
    assert_eq!(
        (v["created"].as_u64(), v["existing"].as_u64(), v["failed"].as_u64()),
        (Some(20), Some(60), Some(0)),
        "{v}"
    );
    let after: Vec<_> = futures::future::join_all((1000..1060).step_by(7).map(|i| fingerprint(&b, i))).await;
    assert_eq!(before, after, "existing accounts untouched");
    assert_eq!(fingerprint(&b, 1070).await.2, 1);
}

/// Explicit index lists with per-account counts; DIDs in shards this node
/// doesn't serve come back as `notOwned` and are not created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indices_and_ownership() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("ix-a", &store).await;
    let b = node("ix-b", &store).await;
    balanced(&[&a, &b]).await;
    let idx: Vec<u64> = (0..40).map(|i| 5000 + i * 3).collect();
    let (mine, theirs): (Vec<u64>, Vec<u64>) =
        idx.iter().partition(|&&i| a.app.partitions.for_key(&bulk_did(i)).is_some());
    assert!(!mine.is_empty() && !theirs.is_empty());
    let recs: Vec<u32> = idx.iter().map(|i| (i % 5) as u32).collect();
    let v = bulk(&a, json!({"indices": idx, "records": recs})).await;
    assert_eq!(v["created"].as_u64(), Some(mine.len() as u64), "{v}");
    assert_eq!(v["notOwned"].as_u64(), Some(theirs.len() as u64), "{v}");
    // b gets only its own
    let recs_b: Vec<u32> = theirs.iter().map(|i| (i % 5) as u32).collect();
    let v = bulk(&b, json!({"indices": theirs, "records": recs_b})).await;
    assert_eq!((v["created"].as_u64(), v["notOwned"].as_u64()), (Some(theirs.len() as u64), Some(0)), "{v}");
    for &i in idx.iter().step_by(5) {
        let r = a.list_records(&bulk_did(i), "app.bsky.feed.post", &[]).await.ok();
        assert_eq!(r["records"].as_array().unwrap().len() as u64, i % 5, "account {i}");
    }
    let bad = a
        .xrpc
        .post("vlpds.admin.bulkCreate", &json!({"indices": [1, 2], "records": [1]}), &Auth::Bearer(ADMIN_TOKEN.into()))
        .await;
    assert_eq!(bad.status, 400, "{}", bad.text());
}

/// bulkCreate is dev-mode (or `--allow-bulk-create`) only, and its accounts
/// get the request's password, else a random one nobody can log in with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gated_and_password() {
    let prod = TestServer::spawn_with(|c| c.dev_mode = false).await;
    let r = prod
        .xrpc
        .post(
            "vlpds.admin.bulkCreate",
            &json!({"start": 0, "count": 1, "records": 0}),
            &Auth::Bearer(ADMIN_TOKEN.into()),
        )
        .await;
    r.err(404, "MethodNotImplemented");
    let allowed = TestServer::spawn_with(|c| {
        c.dev_mode = false;
        c.allow_bulk_create = true;
    })
    .await;
    bulk(&allowed, json!({"start": 0, "count": 1, "records": 0})).await;

    let s = TestServer::spawn().await;
    let v = bulk(&s, json!({"start": 10, "count": 1, "records": 0, "password": "bulk-pw-1"})).await;
    assert_eq!(v["created"].as_u64(), Some(1), "{v}");
    assert!(s.create_session(&bulk_did(10), "bulk-pw-1").await.is_ok());
    let v = bulk(&s, json!({"start": 11, "count": 1, "records": 0})).await;
    assert_eq!(v["created"].as_u64(), Some(1), "{v}");
    for pw in ["hunter2", "bulk-pw-1"] {
        assert!(!s.create_session(&bulk_did(11), pw).await.is_ok(), "no known password: {pw}");
    }
}
