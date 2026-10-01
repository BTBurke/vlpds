//! Ports of atproto/packages/pds/tests/email-confirmation.test.ts and
//! account-deletion.test.ts. Mail is read back through the dev-mode mailbox
//! (vlpds.admin.getDevMail).
mod common;
use common::*;

async fn session(s: &TestServer, a: &TestAccount) -> J {
    s.xrpc.get("com.atproto.server.getSession", &[], &a.auth()).await.ok()
}

/// Mail messages for `email` (newest last).
async fn messages(s: &TestServer, email: &str) -> Vec<J> {
    s.dev_mail(email).await.ok()["messages"].as_array().cloned().unwrap_or_default()
}

/// Runs `f` and returns the token of the single new mail it sent to `email`.
async fn mailed<F: std::future::Future<Output = Resp>>(s: &TestServer, email: &str, f: F) -> (String, Resp, J) {
    let before = messages(s, email).await.len();
    let r = f.await;
    let after = messages(s, email).await;
    assert_eq!(after.len(), before + 1, "expected exactly one mail to {email} (response {})", r.text());
    let m = after.last().unwrap().clone();
    let tok = m["token"].as_str().map(String::from).or_else(|| s_find_code(&m)).unwrap_or_else(|| panic!("mail without token: {m}"));
    (tok, r, m)
}

fn s_find_code(m: &J) -> Option<String> {
    let b = m["body"].as_str()?;
    b.split_whitespace().find(|w| w.len() == 11 && w.as_bytes()[5] == b'-').map(String::from)
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
    s.xrpc.post("com.atproto.server.updateEmail", &json!({"email": new1}), &alice.auth()).await.ok();
    let sess = session(&s, &alice).await;
    assert_eq!(sess["email"], json!(new1));
    assert_eq!(sess["emailConfirmed"], json!(false));
    alice.email = new1.clone();

    // requests email confirmation: one mail to the current address
    let (confirm, _, mail) = mailed(&s, &alice.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &alice.auth())).await;
    assert_eq!(mail["to"].as_str().map(str::to_ascii_lowercase), Some(alice.email.to_ascii_lowercase()));

    // bad token
    s.xrpc
        .post("com.atproto.server.confirmEmail", &json!({"email": alice.email, "token": "123456"}), &alice.auth())
        .await
        .err(400, "InvalidToken");
    // right token, wrong email
    s.xrpc
        .post("com.atproto.server.confirmEmail", &json!({"email": "fake-alice@example.com", "token": confirm}), &alice.auth())
        .await
        .err(400, "InvalidEmail");
    // confirms
    s.xrpc
        .post("com.atproto.server.confirmEmail", &json!({"email": alice.email, "token": confirm}), &alice.auth())
        .await
        .ok();
    assert_eq!(session(&s, &alice).await["emailConfirmed"], json!(true));
    // the token is single-use
    s.xrpc
        .post("com.atproto.server.confirmEmail", &json!({"email": alice.email, "token": confirm}), &alice.auth())
        .await
        .client_err();

    // disallows email update without token when verified
    s.xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "new-alice-2@example.com"}), &alice.auth())
        .await
        .err(400, "TokenRequired");

    // requests email update: token mailed to the *current* address
    let (update, r, _) = mailed(&s, &alice.email, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &alice.auth())).await;
    assert_eq!(r.ok()["tokenRequired"], json!(true));

    // bad token
    s.xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "new-alice-2@example.com", "token": "123456"}), &alice.auth())
        .await
        .err(400, "InvalidToken");
    // badly formatted email
    let r = s
        .xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "not an email", "token": update}), &alice.auth())
        .await;
    r.client_err();
    // in-use email (bob's)
    let r = s
        .xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": bob.email, "token": update}), &alice.auth())
        .await;
    r.client_err();
    assert!(r.text().to_lowercase().contains("in use"), "in-use email: {}", r.text());
    // in-use check is case-insensitive
    let r = s
        .xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": bob.email.to_uppercase(), "token": update}), &alice.auth())
        .await;
    r.client_err();

    // updates email; confirmation resets
    s.xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "new-alice-2@example.com", "token": update}), &alice.auth())
        .await
        .ok();
    let sess = session(&s, &alice).await;
    assert_eq!(sess["email"], json!("new-alice-2@example.com"));
    assert_eq!(sess["emailConfirmed"], json!(false));

    // can log in with the new email, not the old one
    s.create_session("new-alice-2@example.com", PASSWORD).await.ok();
    s.create_session(&alice.email, PASSWORD).await.client_err();
    // the old address is free again
    let h = format!("{}.{HANDLE_DOMAIN}", unique_name("carol"));
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": h, "password": PASSWORD, "email": alice.email}),
            &Auth::None,
        )
        .await
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_tokens_are_purpose_bound() {
    // a confirmation token cannot be used to update the email and vice versa
    let s = TestServer::spawn().await;
    let a = s.create_account("dana").await;
    let (confirm, _, _) = mailed(&s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    s.xrpc.post("com.atproto.server.confirmEmail", &json!({"email": a.email, "token": confirm}), &a.auth()).await.ok();
    let (conf2, _, _) = mailed(&s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    s.xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "x-dana@example.com", "token": conf2}), &a.auth())
        .await
        .err(400, "InvalidToken");
    // a password-reset token cannot delete the account
    let (reset, _, _) = mailed(
        &s,
        &a.email,
        s.xrpc.post("com.atproto.server.requestPasswordReset", &json!({"email": a.email}), &Auth::None),
    )
    .await;
    let r = s
        .xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": a.did, "password": PASSWORD, "token": reset}), &Auth::None)
        .await;
    r.client_err();
    s.get_repo(&a.did).await; // still there
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_endpoints_require_full_access() {
    let s = TestServer::spawn().await;
    let a = s.create_account("erin").await;
    let ap = s
        .xrpc
        .post("com.atproto.server.createAppPassword", &json!({"name": "ap"}), &a.auth())
        .await
        .ok();
    let sess = s.create_session(&a.handle, ap["password"].as_str().unwrap()).await.ok();
    let app_auth = Auth::Bearer(sess["accessJwt"].as_str().unwrap().to_string());
    // reference: requestEmailConfirmation/confirmEmail accept any access token
    // (app passwords included); requestEmailUpdate/updateEmail and
    // requestAccountDelete need full access.
    s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &app_auth).await.ok();
    for nsid in ["com.atproto.server.requestEmailUpdate", "com.atproto.server.requestAccountDelete"] {
        let r = s.xrpc.post_empty(nsid, &app_auth).await;
        r.client_err();
        let r = s.xrpc.post_empty(nsid, &Auth::None).await;
        r.err_status(401);
    }
    s.xrpc
        .post("com.atproto.server.updateEmail", &json!({"email": "y-erin@example.com"}), &app_auth)
        .await
        .client_err();
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
    let blob = s
        .xrpc
        .post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &carol.auth())
        .await
        .ok()["blob"]
        .clone();
    let blob_cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    s.create_record(
        &carol,
        "app.bsky.feed.post",
        json!({"$type": "app.bsky.feed.post", "text": "img", "createdAt": now_iso(), "embed": {"$type": "app.bsky.embed.images", "images": [{"image": blob, "alt": ""}]}}),
    )
    .await;
    for i in 0..5 {
        s.post(&carol, &format!("post {i}")).await;
    }
    let other_blob = s
        .xrpc
        .post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &other.auth())
        .await
        .ok()["blob"]
        .clone();
    s.create_record(
        &other,
        "app.bsky.feed.post",
        json!({"$type": "app.bsky.feed.post", "text": "img", "createdAt": now_iso(), "embed": {"$type": "app.bsky.embed.images", "images": [{"image": other_blob, "alt": ""}]}}),
    )
    .await;

    // requests account deletion
    let (token, r, _) = mailed(&s, &carol.email, s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &carol.auth())).await;
    r.ok();

    // bad token
    let r = s
        .xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": carol.did, "password": PASSWORD, "token": "123456"}), &Auth::None)
        .await;
    r.err(400, "InvalidToken");
    // bad password
    let r = s
        .xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": carol.did, "password": "wrong-password", "token": token}), &Auth::None)
        .await;
    r.err_status(401);

    let mut sub = s.subscribe_from_now().await;
    s.xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": carol.did, "password": PASSWORD, "token": token}), &Auth::None)
        .await
        .ok();
    // #account event with status deleted
    let ev = sub.wait_for(FH_TIMEOUT, &carol.did, "#account").await.pop().unwrap();
    assert_eq!((ev.bool("active"), ev.str("status")), (Some(false), Some("deleted")));

    // no longer lets the user log in or use old tokens
    s.create_session(&carol.handle, PASSWORD).await.client_err();
    s.xrpc.get("com.atproto.server.getSession", &[], &carol.auth()).await.client_err();
    s.xrpc
        .post("com.atproto.server.refreshSession", &json!({}), &carol.refresh_auth())
        .await
        .client_err();

    // no longer stores the account or repo
    s.xrpc.get("com.atproto.sync.getRepo", &[("did", &carol.did)], &Auth::None).await.client_err();
    s.xrpc.get("com.atproto.sync.getLatestCommit", &[("did", &carol.did)], &Auth::None).await.client_err();
    let st = s.xrpc.get("com.atproto.sync.getRepoStatus", &[("did", &carol.did)], &Auth::None).await;
    st.client_err();
    let lr = s.list_records(&carol.did, "app.bsky.feed.post", &[]).await;
    assert!(!lr.is_ok() || lr.json["records"].as_array().map(|a| a.is_empty()).unwrap_or(true), "records still listed after deletion: {}", lr.text());
    s.xrpc
        .get("com.atproto.identity.resolveHandle", &[("handle", &carol.handle)], &Auth::None)
        .await
        .client_err();
    let lrs = s.xrpc.get("com.atproto.sync.listRepos", &[("limit", "1000")], &Auth::None).await.ok();
    assert!(
        !lrs["repos"].as_array().unwrap().iter().any(|r| r["did"] == json!(carol.did)),
        "deleted repo still in listRepos"
    );
    let ai = s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &carol.did)], &Auth::Admin).await;
    ai.client_err();

    // deletes the user's blobs, keeps the other user's copy
    let gb = s.xrpc.get("com.atproto.sync.getBlob", &[("did", &carol.did), ("cid", &blob_cid)], &Auth::None).await;
    gb.client_err();
    let gb = s
        .xrpc
        .get("com.atproto.sync.getBlob", &[("did", &other.did), ("cid", other_blob["ref"]["$link"].as_str().unwrap())], &Auth::None)
        .await;
    assert_eq!(gb.status, 200, "other user's blob must survive: {}", gb.text());
    assert_eq!(&gb.body[..], PNG_1X1);

    // handle and email are reusable
    let r = s
        .xrpc
        .post("com.atproto.server.createAccount", &json!({"handle": carol.handle, "email": carol.email, "password": PASSWORD}), &Auth::None)
        .await
        .ok();
    assert_ne!(r["did"], json!(carol.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn can_delete_an_empty_user() {
    let s = TestServer::spawn().await;
    let eve = s.create_account("eve").await;
    let (token, _, _) = mailed(&s, &eve.email, s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &eve.auth())).await;
    s.xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": eve.did, "password": PASSWORD, "token": token}), &Auth::None)
        .await
        .ok();
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
