//! Ports of atproto/packages/pds/tests/email-confirmation.test.ts and
//! account-deletion.test.ts. Mail is read back through the dev-mode mailbox
//! (vlpds.admin.getDevMail).
use crate::common::*;

async fn session(s: &TestServer, a: &TestAccount) -> J {
    s.get_session(&a.auth()).await.ok()
}

async fn confirm_email(s: &TestServer, a: &TestAccount, email: &str, token: &str) -> Resp {
    s.xrpc.post("com.atproto.server.confirmEmail", &json!({"email": email, "token": token}), &a.auth()).await
}

async fn update_email(s: &TestServer, auth: &Auth, email: &str, token: Option<&str>) -> Resp {
    let mut body = json!({"email": email});
    if let Some(t) = token {
        body["token"] = json!(t);
    }
    s.xrpc.post("com.atproto.server.updateEmail", &body, auth).await
}

async fn delete_account(s: &TestServer, did: &str, password: &str, token: &str) -> Resp {
    s.xrpc.post("com.atproto.server.deleteAccount", &json!({"did": did, "password": password, "token": token}), &Auth::None).await
}

// ---------------------------------------------------------------------------
// email confirmation / update
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_confirmation_and_update_flow() {
    let s = TestServer::spawn().await;
    let mut alice = s.create_account("alice").await;
    let bob = s.create_account("bob").await;

    // starts a user out unverified
    assert_eq!(session(&s, &alice).await["emailConfirmed"], json!(false));

    // allows email update without token when unverified
    let r = s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &alice.auth()).await.ok();
    assert_eq!(r["tokenRequired"], json!(false));
    let new1 = format!("new-{}", alice.email);
    update_email(&s, &alice.auth(), &new1, None).await.ok();
    let sess = session(&s, &alice).await;
    assert_eq!(sess["email"], json!(new1));
    assert_eq!(sess["emailConfirmed"], json!(false));
    alice.email = new1.clone();

    // requests email confirmation: one mail to the current address
    let (confirm, _, mail) = mailed(&s, &alice.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &alice.auth())).await;
    assert_eq!(mail["to"].as_str().map(str::to_ascii_lowercase), Some(alice.email.to_ascii_lowercase()));

    confirm_email(&s, &alice, &alice.email, "123456").await.err(400, "InvalidToken");
    // right token, wrong email
    confirm_email(&s, &alice, "fake-alice@example.com", &confirm).await.err(400, "InvalidEmail");
    confirm_email(&s, &alice, &alice.email, &confirm).await.ok();
    assert_eq!(session(&s, &alice).await["emailConfirmed"], json!(true));
    // the token is single-use
    confirm_email(&s, &alice, &alice.email, &confirm).await.client_err();

    // disallows email update without token when verified
    let new2 = "new-alice-2@example.com";
    update_email(&s, &alice.auth(), new2, None).await.err(400, "TokenRequired");

    // requests email update: token mailed to the *current* address
    let (update, r, _) = mailed(&s, &alice.email, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &alice.auth())).await;
    assert_eq!(r.ok()["tokenRequired"], json!(true));

    update_email(&s, &alice.auth(), new2, Some("123456")).await.err(400, "InvalidToken");
    update_email(&s, &alice.auth(), "not an email", Some(&update)).await.client_err();
    // in-use email (bob's), case-insensitively
    let r = update_email(&s, &alice.auth(), &bob.email, Some(&update)).await;
    r.client_err();
    assert!(r.text().to_lowercase().contains("in use"), "in-use email: {}", r.text());
    update_email(&s, &alice.auth(), &bob.email.to_uppercase(), Some(&update)).await.client_err();

    // updates email; confirmation resets
    update_email(&s, &alice.auth(), new2, Some(&update)).await.ok();
    let sess = session(&s, &alice).await;
    assert_eq!(sess["email"], json!(new2));
    assert_eq!(sess["emailConfirmed"], json!(false));

    // can log in with the new email, not the old one
    s.create_session(new2, PASSWORD).await.ok();
    s.create_session(&alice.email, PASSWORD).await.client_err();
    // the old address is free again
    let h = format!("{}.{HANDLE_DOMAIN}", unique_name("carol"));
    let body = json!({"handle": h, "password": PASSWORD, "email": alice.email});
    s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_tokens_are_purpose_bound() {
    // a confirmation token cannot be used to update the email and vice versa
    let s = TestServer::spawn().await;
    let a = s.create_account("dana").await;
    let (confirm, _, _) = mailed(&s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    confirm_email(&s, &a, &a.email, &confirm).await.ok();
    let (conf2, _, _) = mailed(&s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    update_email(&s, &a.auth(), "x-dana@example.com", Some(&conf2)).await.err(400, "InvalidToken");
    // a password-reset token cannot delete the account
    let (reset, _, _) = mailed(&s, &a.email, s.xrpc.post("com.atproto.server.requestPasswordReset", &json!({"email": a.email}), &Auth::None)).await;
    delete_account(&s, &a.did, PASSWORD, &reset).await.client_err();
    s.get_repo(&a.did).await; // still there
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_endpoints_require_full_access() {
    let s = TestServer::spawn().await;
    let a = s.create_account("erin").await;
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "ap"}), &a.auth()).await.ok();
    let sess = s.create_session(&a.handle, ap["password"].as_str().unwrap()).await.ok();
    let app_auth = Auth::Bearer(sess["accessJwt"].as_str().unwrap().to_string());
    // reference: requestEmailConfirmation/confirmEmail accept any access token
    // (app passwords included); requestEmailUpdate/updateEmail and
    // requestAccountDelete need full access.
    s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &app_auth).await.ok();
    for nsid in ["com.atproto.server.requestEmailUpdate", "com.atproto.server.requestAccountDelete"] {
        s.xrpc.post_empty(nsid, &app_auth).await.client_err();
        s.xrpc.post_empty(nsid, &Auth::None).await.err_status(401);
    }
    update_email(&s, &app_auth, "y-erin@example.com", None).await.client_err();
}

// ---------------------------------------------------------------------------
// account deletion
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_deletion_flow() {
    let s = TestServer::spawn().await;
    let carol = s.create_account("carol").await;
    let other = s.create_account("other").await;
    // give carol some records and a blob, and the other user a copy of the same blob
    let blob = s.upload_blob(&carol, PNG_1X1, "image/png").await;
    let blob_cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    s.create_record(&carol, "app.bsky.feed.post", image_post("img", &blob)).await;
    for i in 0..5 {
        s.post(&carol, &format!("post {i}")).await;
    }
    let other_blob = s.upload_blob(&other, PNG_1X1, "image/png").await;
    s.create_record(&other, "app.bsky.feed.post", image_post("img", &other_blob)).await;

    // requests account deletion
    let (token, r, _) = mailed(&s, &carol.email, s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &carol.auth())).await;
    r.ok();

    delete_account(&s, &carol.did, PASSWORD, "123456").await.err(400, "InvalidToken");
    delete_account(&s, &carol.did, "wrong-password", &token).await.err_status(401);

    let mut sub = s.subscribe_from_now().await;
    delete_account(&s, &carol.did, PASSWORD, &token).await.ok();
    // #account event with status deleted
    let ev = sub.wait_for(FH_TIMEOUT, &carol.did, "#account").await.pop().unwrap();
    assert_eq!((ev.bool("active"), ev.str("status")), (Some(false), Some("deleted")));

    // no longer lets the user log in or use old tokens
    s.create_session(&carol.handle, PASSWORD).await.client_err();
    s.get_session(&carol.auth()).await.client_err();
    s.xrpc.post("com.atproto.server.refreshSession", &json!({}), &carol.refresh_auth()).await.client_err();

    // no longer stores the account or repo
    s.xrpc.get("com.atproto.sync.getRepo", &[("did", &carol.did)], &Auth::None).await.client_err();
    s.xrpc.get("com.atproto.sync.getLatestCommit", &[("did", &carol.did)], &Auth::None).await.client_err();
    s.repo_status(&carol.did).await.client_err();
    let lr = s.list_records(&carol.did, "app.bsky.feed.post", &[]).await;
    assert!(!lr.is_ok() || lr.json["records"].as_array().map(|a| a.is_empty()).unwrap_or(true), "records still listed after deletion: {}", lr.text());
    s.resolve_handle(&carol.handle).await.client_err();
    let lrs = s.xrpc.get("com.atproto.sync.listRepos", &[("limit", "1000")], &Auth::None).await.ok();
    assert!(!lrs["repos"].as_array().unwrap().iter().any(|r| r["did"] == json!(carol.did)), "deleted repo still in listRepos");
    s.account_info(&carol.did).await.client_err();

    // deletes the user's blobs, keeps the other user's copy
    s.get_blob(&carol.did, &blob_cid).await.client_err();
    let gb = s.get_blob(&other.did, other_blob["ref"]["$link"].as_str().unwrap()).await;
    assert_eq!(gb.status, 200, "other user's blob must survive: {}", gb.text());
    assert_eq!(&gb.body[..], PNG_1X1);

    // handle and email are reusable
    let body = json!({"handle": carol.handle, "email": carol.email, "password": PASSWORD});
    let r = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await.ok();
    assert_ne!(r["did"], json!(carol.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn can_delete_an_empty_user() {
    let s = TestServer::spawn().await;
    let eve = s.create_account("eve").await;
    let (token, _, _) = mailed(&s, &eve.email, s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &eve.auth())).await;
    delete_account(&s, &eve.did, PASSWORD, &token).await.ok();
    s.xrpc.get("com.atproto.sync.getRepo", &[("did", &eve.did)], &Auth::None).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_can_delete_account() {
    let s = TestServer::spawn().await;
    let a = s.create_account("frank").await;
    s.post(&a, "hi").await;
    s.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": a.did}), &Auth::None).await.err_status(401);
    s.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": a.did}), &a.auth()).await.client_err();
    s.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": a.did}), &Auth::Admin).await.ok();
    s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await.client_err();
    s.create_session(&a.handle, PASSWORD).await.client_err();
    s.get_record(&a.did, "app.bsky.feed.post", "x").await.client_err();
}
