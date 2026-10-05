//! deactivateAccount's `deleteAfter` (src/xrpc/scheduled_deletion.rs): the
//! sweep deletes a deactivated account once it has passed and the minimum
//! hold is over, skips taken-down ones, and runs only on the shard's owner.
//! Reactivating clears it.

use crate::common::*;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use vlpds::xrpc::scheduled_deletion::{sweep, Swept, MAX_PER_PASS};

const PAST: &str = "2000-01-01T00:00:00Z";

/// Past the default 3-day hold of an account deactivated now.
fn after_hold() -> DateTime<Utc> {
    Utc::now() + chrono::Duration::days(4)
}

async fn deactivate(s: &TestServer, a: &TestAccount, delete_after: &str) {
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({"deleteAfter": delete_after}), &a.auth()).await.ok();
}

async fn exists(s: &TestServer, did: &str) -> bool {
    s.app.account(did).await.is_ok()
}

fn deleted(n: usize) -> Swept {
    Swept { deleted: n, ..Default::default() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_delete_after_is_deleted_by_one_sweep() {
    let s = TestServer::spawn().await;
    let a = s.create_account("sdpast").await;
    s.post(&a, "hello").await;
    deactivate(&s, &a, PAST).await;
    let info = s.account_info(&a.did).await.ok();
    assert_eq!(info["deleteAfter"], json!(PAST));
    let due = info["deletionScheduledAt"].as_str().expect("scheduled").to_string();
    let due = DateTime::parse_from_rfc3339(&due).unwrap().to_utc();
    assert!(due > Utc::now() + chrono::Duration::days(2), "the hold, not the past deleteAfter: {due}");
    assert_eq!(s.get_session(&a.auth()).await.ok()["deletionScheduledAt"], info["deletionScheduledAt"]);

    // within the hold: kept
    assert_eq!(sweep(&s.app, Utc::now(), MAX_PER_PASS).await, Swept::default());
    assert!(exists(&s, &a.did).await);

    let before = vlpds::metrics::ACCOUNT_DELETIONS.with_label_values(&["delete_after"]).get();
    let mut sub = s.subscribe_from_now().await;
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, deleted(1));
    assert!(!exists(&s, &a.did).await);
    assert_eq!(s.app.resolve_handle(&a.handle).await.ok().unwrap(), None, "handle claim released");
    let ev = sub.wait_for(FH_TIMEOUT, &a.did, "#account").await.pop().unwrap();
    assert_eq!(ev.str("status"), Some("deleted"));
    assert!(vlpds::metrics::ACCOUNT_DELETIONS.with_label_values(&["delete_after"]).get() > before);
    // the D/ row went with it: nothing left to do
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_delete_after_is_kept() {
    let s = TestServer::spawn().await;
    let a = s.create_account("sdfut").await;
    let later = (Utc::now() + chrono::Duration::days(30)).to_rfc3339();
    deactivate(&s, &a, &later).await;
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept::default());
    assert!(exists(&s, &a.did).await);
    assert_eq!(sweep(&s.app, Utc::now() + chrono::Duration::days(31), MAX_PER_PASS).await, deleted(1));
    assert!(!exists(&s, &a.did).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivating_clears_delete_after() {
    let s = TestServer::spawn().await;
    let a = s.create_account("sdreact").await;
    deactivate(&s, &a, PAST).await;
    s.xrpc.post_empty("com.atproto.server.activateAccount", &a.auth()).await.ok();
    let info = s.account_info(&a.did).await.ok();
    assert!(info.get("deleteAfter").is_none() && info.get("deletionScheduledAt").is_none(), "{info}");
    assert!(s.get_session(&a.auth()).await.ok().get("deletionScheduledAt").is_none());
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept::default());
    assert!(exists(&s, &a.did).await);
    // deactivated again without one: still kept
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept::default());
    assert!(exists(&s, &a.did).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn taken_down_accounts_are_held() {
    let s = TestServer::spawn().await;
    let a = s.create_account("sdtd").await;
    deactivate(&s, &a, PAST).await;
    let body = json!({"subject": repo_ref(&a.did), "takedown": {"applied": true, "ref": "mod-1"}});
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::Admin).await.ok();
    assert!(s.account_info(&a.did).await.ok().get("deletionScheduledAt").is_none());
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept { held: 1, ..Default::default() });
    assert!(exists(&s, &a.did).await);
    // the takedown reversed, still deactivated: deleted as scheduled
    let body = json!({"subject": repo_ref(&a.did), "takedown": {"applied": false}});
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::Admin).await.ok();
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, deleted(1));
    assert!(!exists(&s, &a.did).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deletion_stopped_partway_is_finished_by_the_next_pass() {
    let s = TestServer::spawn().await;
    let a = s.create_account("sdcrash").await;
    deactivate(&s, &a, PAST).await;
    vlpds::xrpc::set_delete_crash_hook(&a.did, Some(Arc::new(|p: &str| p == "deleted")));
    let first = sweep(&s.app, after_hold(), MAX_PER_PASS).await;
    vlpds::xrpc::set_delete_crash_hook(&a.did, None);
    assert_eq!(first, Swept { failed: 1, ..Default::default() });
    assert!(!exists(&s, &a.did).await);
    assert_eq!(s.app.resolve_handle(&a.handle).await.ok().unwrap().as_deref(), Some(a.did.as_str()));
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept { finished: 1, ..Default::default() });
    assert_eq!(s.app.resolve_handle(&a.handle).await.ok().unwrap(), None);
    assert_eq!(sweep(&s.app, after_hold(), MAX_PER_PASS).await, Swept::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passes_are_bounded() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for i in 0..3 {
        let a = s.create_account(&format!("sdmax{i}")).await;
        deactivate(&s, &a, PAST).await;
        accts.push(a);
    }
    assert_eq!(sweep(&s.app, after_hold(), 2).await, deleted(2));
    assert_eq!(sweep(&s.app, after_hold(), 2).await, deleted(1));
    for a in &accts {
        assert!(!exists(&s, &a.did).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn only_the_shard_owner_deletes() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let tag = unique_name("sdc");
    let a = cluster_node(&format!("{tag}-a"), store.clone(), 4, |_| {}).await;
    let b = cluster_node(&format!("{tag}-b"), store.clone(), 4, |_| {}).await;
    wait_until("both own shards", Duration::from_secs(15), || {
        owned(&a) > 0 && owned(&b) > 0 && owned(&a) + owned(&b) == 4
    })
    .await;
    let x = a.create_account("sdcx").await;
    deactivate(&a, &x, PAST).await;
    let (owner, other) = if a.app.partition(&x.did).is_ok() { (&a, &b) } else { (&b, &a) };
    assert!(other.app.partition(&x.did).is_err());
    assert_eq!(sweep(&other.app, after_hold(), MAX_PER_PASS).await, Swept::default());
    assert!(exists(owner, &x.did).await);
    assert_eq!(sweep(&owner.app, after_hold(), MAX_PER_PASS).await, deleted(1));
    assert!(!exists(owner, &x.did).await);
    // a second sweep anywhere finds nothing to do
    assert_eq!(sweep(&owner.app, after_hold(), MAX_PER_PASS).await, Swept::default());
    assert_eq!(sweep(&other.app, after_hold(), MAX_PER_PASS).await, Swept::default());
}
