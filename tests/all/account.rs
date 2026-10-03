//! Port of atproto/packages/pds/tests/account.test.ts: account creation,
//! handle rules, uniqueness, login, password reset and admin account updates.
use crate::common::*;

/// The reference PDS answers a taken handle with InvalidRequest "Handle already
/// taken"; the lexicon names HandleNotAvailable. Accept either.
#[track_caller]
fn handle_taken(r: &Resp) {
    assert_eq!(r.status, 400, "{}", r.text());
    let ok = r.error_name() == Some("HandleNotAvailable")
        || (r.error_name() == Some("InvalidRequest") && r.text().to_lowercase().contains("taken"));
    assert!(ok, "expected handle-taken error, got {}", r.text());
}

async fn signup(s: &TestServer, handle: &str, email: &str) -> Resp {
    s.xrpc.post("com.atproto.server.createAccount", &json!({"handle": handle, "email": email, "password": "asdf"}), &Auth::None).await
}

async fn try_handle(s: &TestServer, handle: &str) -> Resp {
    signup(s, handle, &format!("{}@example.com", unique_name("th"))).await
}

fn fresh_handle(prefix: &str) -> String {
    format!("{}.{HANDLE_DOMAIN}", unique_name(prefix))
}

async fn request_reset(s: &TestServer, email: &str) -> String {
    let (token, r, _) = mailed(s, email, s.xrpc.post("com.atproto.server.requestPasswordReset", &json!({"email": email}), &Auth::None)).await;
    r.ok();
    token
}

async fn reset_password(s: &TestServer, token: &str, password: &str) -> Resp {
    s.xrpc.post("com.atproto.server.resetPassword", &json!({"token": token, "password": password}), &Auth::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_the_accounts_system_config() {
    let s = TestServer::spawn().await;
    let j = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok();
    assert_eq!(j["inviteCodeRequired"], json!(false));
    assert_eq!(j["availableUserDomains"][0], json!(format!(".{HANDLE_DOMAIN}")));
    assert!(j["did"].as_str().unwrap().starts_with("did:"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_an_account_with_plc_shaped_did_and_did_doc() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    assert!(a.did.starts_with("did:plc:"), "{}", a.did);
    let id = &a.did["did:plc:".len()..];
    assert_eq!(id.len(), 24, "did:plc identifier must be 24 chars: {}", a.did);
    assert!(id.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7')), "{}", a.did);
    assert!(!a.access.is_empty() && !a.refresh.is_empty());

    // DID document via describeRepo: handle, PDS endpoint and the repo signing key.
    let j = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", &a.did)], &Auth::None).await.ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["handle"], json!(a.handle));
    let doc = &j["didDoc"];
    assert_eq!(doc["id"], json!(a.did));
    assert!(doc["alsoKnownAs"].as_array().unwrap().contains(&json!(format!("at://{}", a.handle))));
    let svc = doc["service"].as_array().unwrap();
    assert!(svc.iter().any(|x| x["id"].as_str().is_some_and(|i| i.ends_with("#atproto_pds")) && x["serviceEndpoint"] == json!(s.url)));
    // signing key in the doc verifies the repo's commits
    let key = s.signing_key(&a.did).await;
    s.get_repo(&a.did).await.commit().verify(&key).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_on_invalid_handles() {
    let s = TestServer::spawn().await;
    for h in [
        "did:bad-handle.vlpds.test",
        "did:john",
        "jo_hn.vlpds.test",
        "jo!hn.vlpds.test",
        "jo%hn.vlpds.test",
        "jo&hn.vlpds.test",
        "jo*hn.vlpds.test",
        "jo|hn.vlpds.test",
        "jo:hn.vlpds.test",
        "jo/hn.vlpds.test",
        "",
        "nodots",
        ".vlpds.test",
        "a..vlpds.test",
    ] {
        let r = try_handle(&s, h).await;
        assert_eq!(r.status, 400, "handle {h:?}: {}", r.text());
        assert!(matches!(r.error_name(), Some("InvalidHandle" | "InvalidRequest")), "handle {h:?}: {}", r.text());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_improperly_formatted_handles() {
    let s = TestServer::spawn().await;
    // too short / too long first label on the service domain
    try_handle(&s, &format!("j.{HANDLE_DOMAIN}")).await.err(400, "InvalidHandle");
    try_handle(&s, &format!("jayromy-johnber12345678910.{HANDLE_DOMAIN}")).await.err(400, "InvalidHandle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_unsupported_domains() {
    let s = TestServer::spawn().await;
    try_handle(&s, "john.bsky.io").await.err(400, "UnsupportedDomain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_reserved_handles() {
    let s = TestServer::spawn().await;
    try_handle(&s, &format!("about.{HANDLE_DOMAIN}")).await.err(400, "HandleNotAvailable");
    try_handle(&s, &format!("atp.{HANDLE_DOMAIN}")).await.err(400, "HandleNotAvailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_duplicate_email_addresses_and_handles() {
    let s = TestServer::spawn().await;
    let name = unique_name("bob");
    let handle = format!("{name}.{HANDLE_DOMAIN}");
    let email = format!("{name}@test.com");
    signup(&s, &handle, &email).await.ok();
    // same email, different case
    let r = signup(&s, &fresh_handle("carol"), &email.to_uppercase()).await;
    r.client_err();
    assert!(r.text().to_lowercase().contains("email"), "{}", r.text());
    // same handle, different case
    handle_taken(&signup(&s, &handle.to_uppercase(), &format!("{}@test.com", unique_name("carol"))).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_the_email_and_handle_of_a_deactivated_account() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dan").await;
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    handle_taken(&signup(&s, &a.handle, &format!("{}@test.com", unique_name("erin"))).await);
    signup(&s, &fresh_handle("erin"), &a.email).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handles_racing_signups_for_same_handle() {
    let s = std::sync::Arc::new(TestServer::spawn().await);
    let handle = fresh_handle("match");
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let (s, handle) = (s.clone(), handle.clone());
            tokio::spawn(async move { signup(&s, &handle, &format!("matching{i}@test.com")).await.is_ok() })
        })
        .collect();
    let mut ok = 0;
    for t in tasks {
        ok += t.await.unwrap() as usize;
    }
    assert_eq!(ok, 1, "exactly one racing signup must win");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn login_and_authenticated_requests() {
    let s = TestServer::spawn().await;
    // unauthenticated
    s.get_session(&Auth::None).await.err_status(401);

    let a = s.create_account("alice").await;
    for ident in [a.handle.clone(), a.did.clone(), a.handle.to_uppercase()] {
        let j = s.create_session(&ident, &a.password).await.ok();
        assert_eq!(j["did"], json!(a.did), "identifier {ident}");
        assert_eq!(j["handle"], json!(a.handle));
        assert_eq!(j["email"], json!(a.email));
        let sess = s.get_session(&Auth::Bearer(j["accessJwt"].as_str().unwrap().into())).await.ok();
        assert_eq!(sess["did"], json!(a.did));
        assert_eq!(sess["handle"], json!(a.handle));
        assert_eq!(sess["email"], json!(a.email));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn can_reset_account_password() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let alt = "the-alt-password";

    let token = request_reset(&s, &a.email).await;
    reset_password(&s, &token, alt).await.ok();
    s.create_session(&a.handle, &a.password).await.err(401, "AuthenticationRequired");
    s.create_session(&a.handle, alt).await.ok();

    // single use: reuse of the same token fails
    let r = reset_password(&s, &token, &a.password).await;
    assert_eq!(r.status, 400, "{}", r.text());
    assert!(matches!(r.error_name(), Some("InvalidToken" | "ExpiredToken")), "{}", r.text());
    s.create_session(&a.handle, alt).await.ok();

    // reset back (token is case-insensitive) and it revokes existing refresh tokens
    let sess = s.create_session(&a.handle, alt).await.ok();
    let token = request_reset(&s, &a.email).await;
    reset_password(&s, &token.to_lowercase(), &a.password).await.ok();
    s.create_session(&a.handle, alt).await.err(401, "AuthenticationRequired");
    s.create_session(&a.handle, &a.password).await.ok();
    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(sess["refreshJwt"].as_str().unwrap().into())).await;
    assert_eq!(r.status, 400, "refresh with pre-reset token must fail: {}", r.text());
    assert!(matches!(r.error_name(), Some("ExpiredToken" | "InvalidToken")), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_bogus_password_reset_token() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    reset_password(&s, "AAAAA-AAAAA", "x").await.err(400, "InvalidToken");
    s.create_session(&a.handle, &a.password).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allows_an_admin_to_update_password() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let update = |pw: &str, auth: Auth| s.xrpc.post_owned("com.atproto.admin.updateAccountPassword", json!({"did": a.did, "password": pw}), auth);
    update("new-admin-pass", Auth::None).await.err_status(401);
    update("new-admin-pass", a.auth()).await.client_err();
    update("new-admin-password", Auth::Admin).await.ok();
    s.create_session(&a.did, &a.password).await.err(401, "AuthenticationRequired");
    s.create_session(&a.did, "new-admin-password").await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allows_administrative_email_updates() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let update = |account: &str, email: &str, auth: Auth| s.xrpc.post_owned("com.atproto.admin.updateAccountEmail", json!({"account": account, "email": email}), auth);
    update(&a.handle, "alIce-NEw@teST.com", Auth::Admin).await.ok();
    let info = s.account_info(&a.did).await.ok();
    assert_eq!(info["email"], json!("alice-new@test.com"));
    assert_eq!(info["did"], json!(a.did));
    assert_eq!(info["handle"], json!(a.handle));

    update(&a.did, &a.email, Auth::Admin).await.ok();
    assert_eq!(s.account_info(&a.did).await.ok()["email"], json!(a.email));
    assert_eq!(s.get_session(&a.auth()).await.ok()["email"], json!(a.email));
    update(&a.did, "x@test.com", a.auth()).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_account_emits_identity_and_account_events() {
    let s = TestServer::spawn().await;
    let mut sub = s.subscribe(None).await;
    let a = s.create_account("fh").await;
    let did = a.did.clone();
    let frames = sub
        .until(FH_TIMEOUT, move |fs| ["#identity", "#account"].iter().all(|k| fs.iter().any(|f| f.did() == Some(did.as_str()) && f.kind() == *k)))
        .await;
    let mine: Vec<_> = frames.iter().filter(|f| f.did() == Some(a.did.as_str())).collect();
    let ident = mine.iter().find(|f| f.kind() == "#identity").expect("#identity event for new account");
    assert_eq!(ident.str("handle"), Some(a.handle.as_str()));
    let acct = mine.iter().find(|f| f.kind() == "#account").unwrap();
    assert_eq!(acct.bool("active"), Some(true));
    assert!(acct.str("status").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_an_email() {
    let s = TestServer::spawn().await;
    let handle = fresh_handle("noemail");
    for body in [
        json!({"handle": handle, "password": PASSWORD}),
        json!({"handle": handle, "password": PASSWORD, "email": ""}),
    ] {
        let r = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await;
        r.err(400, "InvalidRequest");
        assert!(r.text().contains("Email is required"), "{}", r.text());
    }
}

/// reserveSigningKey with a DID returns the same key while the reservation
/// is live (reference reserveKeypair); unclaimed reservations expire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserve_signing_key_reuse_and_expiry() {
    let s = TestServer::spawn().await;
    let reserve = |body: J| {
        let r = s.xrpc.post_owned("com.atproto.server.reserveSigningKey", body, Auth::None);
        async move { r.await.ok()["signingKey"].as_str().unwrap().to_string() }
    };
    let did = "did:plc:migratingaaaaaaaaaaaaaaa";
    let k1 = reserve(json!({"did": did})).await;
    assert_eq!(reserve(json!({"did": did})).await, k1, "same DID, same reserved key");
    assert_ne!(reserve(json!({})).await, reserve(json!({})).await);

    // once taken, the DID gets a fresh key
    let a = s.create_account("rsk").await;
    let k2 = reserve(json!({"did": a.did})).await;
    let r = s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did, "signingKey": k2}), &Auth::Admin).await.ok();
    assert_eq!(r["signingKey"], json!(k2));
    assert_ne!(reserve(json!({"did": a.did})).await, k2);

    // expired reservations are swept and can't be installed
    let swept = vlpds::xrpc::sweep_reserved_keys(&s.app, std::time::Duration::ZERO).await.unwrap_or_else(|e| panic!("{}", e.message));
    assert!(swept >= 5, "swept {swept}");
    assert_ne!(reserve(json!({"did": did})).await, k1, "expired reservation was reused");
    let body = json!({"did": a.did, "signingKey": k1});
    s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &body, &Auth::Admin).await.err(400, "InvalidRequest");
}
