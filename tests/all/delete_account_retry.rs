//! deleteAccount is retry-safe (src/xrpc/server.rs `delete_account_fully`):
//! a deletion that fails after the repo delete (the account row gone)
//! leaves its `deleting` row, and a retry, by the user with the password or
//! by the admin, releases the handle and email claims and drops the private
//! rows. The user's retry needs the account's password.

use crate::common::*;
use std::sync::Arc;

async fn create_app_password(s: &TestServer, a: &TestAccount) {
    s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "leftover"}), &a.auth()).await.ok();
}

async fn private(s: &TestServer, did: &str, name: &str) -> bool {
    s.app.get_private(did, name).await.ok().unwrap().is_some()
}

async fn recreate(s: &TestServer, a: &TestAccount) -> Resp {
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": a.handle, "password": PASSWORD, "email": a.email}),
            &Auth::None,
        )
        .await
}

/// Fails the deletion once, right after the repo delete; checks what is
/// left behind.
async fn fail_after_repo_delete(s: &TestServer, a: &TestAccount, delete: impl std::future::Future<Output = Resp>) {
    vlpds::xrpc::set_delete_crash_hook(&a.did, Some(Arc::new(|p: &str| p == "deleted")));
    let r = delete.await;
    vlpds::xrpc::set_delete_crash_hook(&a.did, None);
    r.err(500, "InternalServerError");
    assert!(s.app.account(&a.did).await.is_err(), "the account row is gone");
    assert_eq!(
        s.app.resolve_handle(&a.handle).await.ok().unwrap().as_deref(),
        Some(a.did.as_str()),
        "handle claim left behind"
    );
    assert!(private(s, &a.did, "apppass/leftover").await, "private rows left behind");
    assert!(private(s, &a.did, "deleting").await);
    let r = recreate(s, a).await;
    assert!(!r.is_ok(), "claims still held: {}", r.text());
}

async fn assert_cleaned(s: &TestServer, a: &TestAccount) {
    assert_eq!(s.app.resolve_handle(&a.handle).await.ok().unwrap(), None, "handle claim released");
    assert!(!private(s, &a.did, "apppass/leftover").await);
    assert!(!private(s, &a.did, "deleting").await);
    // the handle and email are free at once (no stale-claim grace)
    let j = recreate(s, a).await.ok();
    assert_ne!(j["did"], json!(a.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_retry_finishes_a_deletion() {
    let s = TestServer::spawn().await;
    let a = s.create_account("delr").await;
    s.post(&a, "hello").await;
    create_app_password(&s, &a).await;
    s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &a.auth()).await.ok();
    let token = s.mail_token(&a.email).await.unwrap();
    let del = |password: &str| {
        let body = json!({"did": a.did, "password": password, "token": token});
        s.xrpc.post_owned("com.atproto.server.deleteAccount", body, Auth::None)
    };
    fail_after_repo_delete(&s, &a, del(&a.password)).await;

    // the leftovers are the account's: the password is still required
    let r = del("not-the-password").await;
    assert_eq!(r.status, 401, "{}", r.text());
    assert!(private(&s, &a.did, "deleting").await);

    del(&a.password).await.ok();
    assert_cleaned(&s, &a).await;
    // nothing left: a further retry finds no account
    del(&a.password).await.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_retry_finishes_a_deletion() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dela").await;
    s.post(&a, "hello").await;
    create_app_password(&s, &a).await;
    let del = || s.xrpc.post_owned("com.atproto.admin.deleteAccount", json!({"did": a.did}), Auth::Admin);
    fail_after_repo_delete(&s, &a, del()).await;
    del().await.ok();
    assert_cleaned(&s, &a).await;
    del().await.err(400, "InvalidRequest");
}
