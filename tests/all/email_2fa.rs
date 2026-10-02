//! Email second factor (`emailAuthFactor`): port of the reference's
//! packages/pds/tests/email-auth-factor.test.ts (toggling through
//! updateEmail), plus sign-in with an emailed code on createSession, code
//! expiry and single use, the wrong-code lockout (TOTP's schedule) and the
//! TOTP-over-email precedence. The OAuth sign-in page is in oauth.rs.
use crate::common::*;

async fn session(s: &TestServer, a: &TestAccount) -> J {
    s.xrpc.get("com.atproto.server.getSession", &[], &a.auth()).await.ok()
}

async fn messages(s: &TestServer, email: &str) -> Vec<J> {
    s.dev_mail(email).await.ok()["messages"].as_array().cloned().unwrap_or_default()
}

/// Runs `f`, asserting it mailed exactly `n` messages to `email`; returns
/// the response and the newest message (if any).
async fn mails<F: std::future::Future<Output = Resp>>(s: &TestServer, email: &str, n: usize, f: F) -> (Resp, Option<J>) {
    let before = messages(s, email).await.len();
    let r = f.await;
    let after = messages(s, email).await;
    assert_eq!(after.len(), before + n, "mails to {email} (response {})", r.text());
    (r, (n > 0).then(|| after.last().unwrap().clone()))
}

fn token_of(m: &J) -> String {
    m["token"].as_str().expect("mail token").to_string()
}

async fn update_email(s: &TestServer, a: &TestAccount, body: J) -> Resp {
    s.xrpc.post("com.atproto.server.updateEmail", &body, &a.auth()).await
}

/// An account whose email is confirmed (the factor needs that).
async fn confirmed_account(s: &TestServer, prefix: &str) -> TestAccount {
    let a = s.create_account(prefix).await;
    let (_, m) = mails(s, &a.email, 1, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    let tok = token_of(&m.unwrap());
    s.xrpc.post("com.atproto.server.confirmEmail", &json!({"email": a.email, "token": tok}), &a.auth()).await.ok();
    assert_eq!(session(s, &a).await["emailConfirmed"], json!(true));
    a
}

async fn enable(s: &TestServer, a: &TestAccount) {
    update_email(s, a, json!({"email": a.email, "emailAuthFactor": true})).await.ok();
    assert_eq!(session(s, a).await["emailAuthFactor"], json!(true));
}

async fn login(s: &TestServer, ident: &str, password: &str, code: Option<&str>) -> Resp {
    let mut body = json!({"identifier": ident, "password": password});
    if let Some(c) = code {
        body["authFactorToken"] = json!(c);
    }
    s.xrpc.post("com.atproto.server.createSession", &body, &Auth::None).await
}

/// A password login that mails a sign-in code; returns the code.
async fn request_code(s: &TestServer, a: &TestAccount) -> String {
    let (r, m) = mails(s, &a.email, 1, login(s, &a.handle, &a.password, None)).await;
    r.err(401, "AuthFactorTokenRequired");
    assert!(r.text().contains("sign in code has been sent"), "{}", r.text());
    let m = m.unwrap();
    assert_eq!(m["purpose"], json!("auth_factor"));
    assert_eq!(m["subject"], json!("Sign-in Confirmation"));
    token_of(&m)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toggles_like_the_reference() {
    let s = TestServer::spawn().await;
    let faye = confirmed_account(&s, "faye").await;
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(false));

    // enables the auth factor without a token, and without mailing anything
    let (r, _) = mails(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": true}))).await;
    r.ok();
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));
    // no-ops when already enabled
    let (r, _) = mails(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": true}))).await;
    r.ok();

    // omitting emailAuthFactor is a plain email update: token required
    let (r, _) = mails(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email}))).await;
    r.err(400, "TokenRequired");
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));

    // enabling while changing the address is refused
    let r = update_email(&s, &faye, json!({"email": format!("new-{}", faye.email), "emailAuthFactor": true})).await;
    r.err_status(400);
    assert!(r.text().contains("Please change and verify your email before enabling OTP"), "{}", r.text());
    let sess = session(&s, &faye).await;
    assert_eq!((sess["emailAuthFactor"].clone(), sess["email"].clone()), (json!(true), json!(faye.email)));

    // disabling needs a confirmation token: the first call mails one
    let (r, m) = mails(&s, &faye.email, 1, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false}))).await;
    r.err(400, "TokenRequired");
    let m = m.unwrap();
    assert_eq!(m["purpose"], json!("update_email"));
    let disable_token = token_of(&m);
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));

    // an invalid token has no side effects
    assert_ne!(disable_token, "AAAAA-AAAAA");
    update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false, "token": "AAAAA-AAAAA"}))
        .await
        .err(400, "InvalidToken");
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));

    // disables with the token; address and confirmation untouched
    update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false, "token": disable_token})).await.ok();
    let sess = session(&s, &faye).await;
    assert_eq!(sess["emailAuthFactor"], json!(false));
    assert_eq!(sess["email"], json!(faye.email));
    assert_eq!(sess["emailConfirmed"], json!(true));

    // a requestEmailUpdate token works too (what the Bluesky app sends)
    enable(&s, &faye).await;
    let (r, m) = mails(&s, &faye.email, 1, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &faye.auth())).await;
    assert_eq!(r.ok()["tokenRequired"], json!(true));
    update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false, "token": token_of(&m.unwrap())})).await.ok();
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(false));

    // no-op (and no mail) when already disabled
    let (r, _) = mails(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false}))).await;
    r.ok();
    assert_eq!(session(&s, &faye).await["emailConfirmed"], json!(true));

    // the address is matched case-insensitively (a toggle, not a change)
    let (r, _) = mails(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email.to_uppercase(), "emailAuthFactor": true}))).await;
    r.ok();
    let sess = session(&s, &faye).await;
    assert_eq!(sess["emailAuthFactor"], json!(true));
    assert_eq!(sess["email"], json!(faye.email));
    assert_eq!(sess["emailConfirmed"], json!(true));

    // changing the address drops the factor (codes must not go to an
    // unconfirmed inbox)
    let (_, m) = mails(&s, &faye.email, 1, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &faye.auth())).await;
    let new = format!("moved-{}", faye.email);
    update_email(&s, &faye, json!({"email": new, "token": token_of(&m.unwrap())})).await.ok();
    let sess = session(&s, &faye).await;
    assert_eq!(sess["email"], json!(new));
    assert_eq!(sess["emailAuthFactor"], json!(false));
    // and it can't come back until the new address is confirmed
    let r = update_email(&s, &faye, json!({"email": new, "emailAuthFactor": true})).await;
    r.err_status(400);
    assert!(r.text().contains("Please change and verify your email before enabling OTP"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconfirmed_email_cannot_enable() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gil").await;
    let r = update_email(&s, &a, json!({"email": a.email, "emailAuthFactor": true})).await;
    r.err_status(400);
    assert!(r.text().contains("Please change and verify your email before enabling OTP"), "{}", r.text());
    assert_eq!(session(&s, &a).await["emailAuthFactor"], json!(false));
    // logins are unaffected
    login(&s, &a.handle, &a.password, None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_in_with_emailed_code() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "hal").await;
    enable(&s, &a).await;

    let code = request_code(&s, &a).await;
    // wrong code
    login(&s, &a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    // wrong password with the right code: still the password error
    login(&s, &a.handle, "not-the-password", Some(&code)).await.err_status(401);
    // right code (case-insensitive, as tokens are uppercased)
    let j = login(&s, &a.handle, &a.password, Some(&code.to_lowercase())).await.ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["emailAuthFactor"], json!(true));
    assert!(j["accessJwt"].is_string());
    // single use
    login(&s, &a.handle, &a.password, Some(&code)).await.err(400, "InvalidToken");

    // a newer code replaces the older one
    let old = request_code(&s, &a).await;
    let new = request_code(&s, &a).await;
    assert_ne!(old, new);
    login(&s, &a.handle, &a.password, Some(&old)).await.err(400, "InvalidToken");
    login(&s, &a.handle, &a.password, Some(&new)).await.ok();

    // login by email address asks too
    let (r, _) = mails(&s, &a.email, 1, login(&s, &a.email, &a.password, None)).await;
    r.err(401, "AuthFactorTokenRequired");

    // app passwords bypass the factor (as in the reference)
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "phone"}), &a.auth()).await.ok();
    let (r, _) = mails(&s, &a.email, 0, login(&s, &a.handle, ap["password"].as_str().unwrap(), None)).await;
    r.ok();

    // refreshSession/getSession report the flag
    let sess = session(&s, &a).await;
    assert_eq!(sess["emailAuthFactor"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_code_is_refused() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "ida").await;
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    // age the stored token past its 15 minutes
    let name = "etok/auth_factor";
    let raw = s.app.get_private(&a.did, name).await.ok().flatten().expect("stored token");
    let mut rec: J = serde_json::from_slice(&raw).unwrap();
    rec["requested_at"] = json!(rec["requested_at"].as_u64().unwrap() - 16 * 60 * 1000);
    s.app
        .put_private(
            &a.did,
            vec![vlpds::segment::Mutation {
                key: vlpds::state::private_key(&a.did, name).into(),
                val: Some(serde_json::to_vec(&rec).unwrap().into()),
            }],
        )
        .await
        .unwrap_or_else(|e| panic!("put_private: {}", e.message));
    login(&s, &a.handle, &a.password, Some(&code)).await.err(400, "ExpiredToken");
    // a fresh one works
    let code = request_code(&s, &a).await;
    login(&s, &a.handle, &a.password, Some(&code)).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_codes_lock_the_factor() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "jo").await;
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    // MAX_FAILURES - 1 wrong codes are plain InvalidToken
    for _ in 1..vlpds::totp::MAX_FAILURES {
        login(&s, &a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    }
    // the next one locks
    login(&s, &a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(429, "RateLimitExceeded");
    // while locked even the right code is refused, and no code is mailed
    login(&s, &a.handle, &a.password, Some(&code)).await.err(429, "RateLimitExceeded");
    let (r, _) = mails(&s, &a.email, 0, login(&s, &a.handle, &a.password, None)).await;
    r.err(429, "RateLimitExceeded");
    // the account still works through an app password
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "x"}), &a.auth()).await.ok();
    login(&s, &a.handle, ap["password"].as_str().unwrap(), None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_takes_precedence_over_email() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "kim").await;
    enable(&s, &a).await;
    // enable TOTP as well
    let j = s.xrpc.post_empty("vlpds.server.setupTotp", &a.auth()).await.ok();
    let secret = vlpds::totp::base32_decode(j["secret"].as_str().unwrap()).unwrap();
    let step = vlpds::totp::step_at(vlpds::totp::now_secs());
    s.xrpc
        .post("vlpds.server.confirmTotp", &json!({"code": vlpds::totp::code_for_step(&secret, step)}), &a.auth())
        .await
        .ok();
    // no email code: TOTP is asked for
    let (r, _) = mails(&s, &a.email, 0, login(&s, &a.handle, &a.password, None)).await;
    r.err(401, "AuthFactorTokenRequired");
    assert!(r.text().contains("two-factor authentication code"), "{}", r.text());
    // a TOTP code signs in
    let code = vlpds::totp::code_for_step(&secret, step + 1);
    login(&s, &a.handle, &a.password, Some(&code)).await.ok();
    // both stay enabled
    assert_eq!(session(&s, &a).await["emailAuthFactor"], json!(true));
}
