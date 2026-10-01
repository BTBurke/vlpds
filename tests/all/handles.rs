//! Port of packages/pds/tests/handles.test.ts: resolveHandle, updateHandle
//! (uniqueness, idempotence, validation, reserved names), DID doc updates,
//! admin updateAccountHandle, and #identity events.
use crate::common::*;
use std::time::Duration;

fn h(label: &str) -> String {
    format!("{label}.{HANDLE_DOMAIN}")
}

async fn update_handle(s: &TestServer, a: &TestAccount, handle: &str) -> Resp {
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": handle}), &a.auth()).await
}

async fn resolve(s: &TestServer, handle: &str) -> Resp {
    s.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", handle)], &Auth::None).await
}

async fn current_handle(s: &TestServer, did: &str) -> String {
    let d = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None).await.ok();
    d["handle"].as_str().unwrap().to_string()
}

async fn did_doc_handle(s: &TestServer, did: &str) -> Option<String> {
    let d = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None).await.ok();
    d["didDoc"]["alsoKnownAs"].as_array()?.iter().filter_map(|x| x.as_str()).find_map(|x| x.strip_prefix("at://")).map(String::from)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolves_handles() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    assert_eq!(resolve(&s, &a.handle).await.ok()["did"], json!(a.did));
    // non-normalized input
    assert_eq!(resolve(&s, &a.handle.to_uppercase()).await.ok()["did"], json!(a.did));
    let r = resolve(&s, &h("john")).await;
    r.err(400, "HandleNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_changes_handle() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let new = h(&unique_name("alic"));
    let mut sub = s.subscribe(Some(s.current_seq().await)).await;
    update_handle(&s, &a, &new).await.ok();

    resolve(&s, &a.handle).await.err(400, "HandleNotFound");
    assert_eq!(resolve(&s, &new).await.ok()["did"], json!(a.did));
    assert_eq!(current_handle(&s, &a.did).await, new);
    assert_eq!(did_doc_handle(&s, &a.did).await.as_deref(), Some(new.as_str()), "DID doc alsoKnownAs updated");

    // login with the new handle
    let sess = s.create_session(&new, &a.password).await.ok();
    assert_eq!(sess["did"], json!(a.did));
    assert_eq!(sess["handle"], json!(new));
    // the old handle no longer logs in
    s.create_session(&a.handle, &a.password).await.client_err();

    let frames = sub.wait_for(FH_TIMEOUT, &a.did, "#identity").await;
    assert_eq!(frames.last().unwrap().str("handle"), Some(new.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cannot_take_existing_handle() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let r = update_handle(&s, &a, &b.handle.to_uppercase()).await;
    r.err_status(400);
    assert!(r.text().to_lowercase().contains("taken") || r.error_name() == Some("HandleNotAvailable"), "{}", r.text());
    // failure leaves alice's handle (and DID doc) unchanged
    assert_eq!(current_handle(&s, &a.did).await, a.handle);
    assert_eq!(did_doc_handle(&s, &a.did).await.as_deref(), Some(a.handle.as_str()));
    assert_eq!(resolve(&s, &b.handle).await.ok()["did"], json!(b.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_updates_are_idempotent() {
    let s = TestServer::spawn().await;
    let b = s.create_account("bob").await;
    let mut sub = s.subscribe(Some(s.current_seq().await)).await;
    update_handle(&s, &b, &b.handle.to_uppercase()).await.ok();
    assert_eq!(current_handle(&s, &b.did).await, b.handle);
    assert_eq!(resolve(&s, &b.handle).await.ok()["did"], json!(b.did));
    // re-sends #identity even though nothing changed
    let frames = sub.wait_for(FH_TIMEOUT, &b.did, "#identity").await;
    assert_eq!(frames.last().unwrap().str("handle"), Some(b.handle.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validates_input_handle_syntax() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for bad in ["did:john", "jo_hn.test", "jo!hn.test", "jo%hn.test", "jo&hn.test", "jo*hn.test", "jo|hn.test", "jo:hn.test", "jo/hn.test"] {
        let r = update_handle(&s, &a, bad).await;
        assert_eq!(r.status, 400, "{bad}: {}", r.text());
        assert!(matches!(r.error_name(), Some("InvalidRequest") | Some("InvalidHandle")), "{bad}: {}", r.text());
    }
    assert_eq!(current_handle(&s, &a.did).await, a.handle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applies_pds_length_constraints() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let r = update_handle(&s, &a, &h("j")).await;
    r.err(400, "InvalidHandle");
    assert!(r.text().contains("too short"), "{}", r.text());
    let r = update_handle(&s, &a, &h("jayromy-johnber12345678910")).await;
    r.err(400, "InvalidHandle");
    assert!(r.text().contains("too long"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_reserved_handles() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for r in ["about", "atp"] {
        let resp = update_handle(&s, &a, &h(r)).await;
        resp.err(400, "HandleNotAvailable");
        assert!(resp.text().contains("Reserved"), "{}", resp.text());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_handle_must_resolve() {
    // dev mode deliberately skips external-handle verification, so run this one without it
    let s = TestServer::spawn_with(|c| c.dev_mode = false).await;
    let a = s.create_account("alice").await;
    // nothing serves _atproto TXT / .well-known for this name
    let r = tokio::time::timeout(Duration::from_secs(20), update_handle(&s, &a, "noexist-vlpds-test.example.com")).await.expect("bounded");
    r.client_err();
    assert_eq!(current_handle(&s, &a.did).await, a.handle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_overrides_handles() {
    let s = TestServer::spawn().await;
    let b = s.create_account("bob").await;
    let alt = h(&unique_name("balt"));
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &json!({"did": b.did, "handle": alt}), &Auth::Admin).await.ok();
    assert_eq!(current_handle(&s, &b.did).await, alt);
    assert_eq!(resolve(&s, &alt).await.ok()["did"], json!(b.did));
    // admins may assign reserved names
    let reserved = h("dril");
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &json!({"did": b.did, "handle": reserved}), &Auth::Admin).await.ok();
    assert_eq!(current_handle(&s, &b.did).await, reserved);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_update_requires_admin_auth() {
    let s = TestServer::spawn().await;
    let b = s.create_account("bob").await;
    let body = json!({"did": b.did, "handle": h(&unique_name("balt"))});
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &body, &b.auth()).await.err_status(401);
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &body, &Auth::None).await.err_status(401);
    s.xrpc.post("com.atproto.admin.updateAccountHandle", &body, &Auth::Basic("admin".into(), "wrong".into())).await.err_status(401);
    assert_eq!(current_handle(&s, &b.did).await, b.handle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_account_handle_uniqueness_is_case_insensitive() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let r = s
        .xrpc
        .post("com.atproto.server.createAccount", &json!({"handle": a.handle.to_uppercase(), "password": "x-password", "email": "dupe@example.com"}), &Auth::None)
        .await;
    r.err(400, "HandleNotAvailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_of_one_handle() {
    let s = TestServer::spawn().await;
    let accts = futures::future::join_all((0..5).map(|_| s.create_account("racer"))).await;
    let target = h(&unique_name("prize"));
    let results = futures::future::join_all(accts.iter().map(|a| update_handle(&s, a, &target))).await;
    let winners: Vec<usize> = results.iter().enumerate().filter(|(_, r)| r.is_ok()).map(|(i, _)| i).collect();
    assert_eq!(winners.len(), 1, "exactly one account gets the handle: {results:?}");
    let did = &accts[winners[0]].did;
    assert_eq!(resolve(&s, &target).await.ok()["did"], json!(did));
}
