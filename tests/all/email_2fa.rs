//! Email second factor (`emailAuthFactor`): port of the reference's
//! packages/pds/tests/email-auth-factor.test.ts (toggling through
//! updateEmail), plus sign-in with an emailed code on createSession, code
//! expiry and single use, the wrong-code lockout (TOTP's schedule) and the
//! TOTP-over-email precedence. The OAuth sign-in page is in oauth.rs.
use crate::common::*;

async fn session(s: &TestServer, a: &TestAccount) -> J {
    s.get_session(&a.auth()).await.ok()
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
    let (tok, _, _) =
        mailed(s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    s.xrpc.post("com.atproto.server.confirmEmail", &json!({"email": a.email, "token": tok}), &a.auth()).await.ok();
    assert_eq!(session(s, &a).await["emailConfirmed"], json!(true));
    a
}

async fn enable(s: &TestServer, a: &TestAccount) {
    update_email(s, a, json!({"email": a.email, "emailAuthFactor": true})).await.ok();
    assert_eq!(session(s, a).await["emailAuthFactor"], json!(true));
}

/// A password login that mails a sign-in code; returns the code.
async fn request_code(s: &TestServer, a: &TestAccount) -> String {
    let (r, m) = mailed_n(s, &a.email, 1, s.login(&a.handle, &a.password, None)).await;
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
    let (r, _) =
        mailed_n(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": true})))
            .await;
    r.ok();
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));
    // no-ops when already enabled
    let (r, _) =
        mailed_n(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": true})))
            .await;
    r.ok();

    // omitting emailAuthFactor is a plain email update: token required
    let (r, _) = mailed_n(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email}))).await;
    r.err(400, "TokenRequired");
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(true));

    // enabling while changing the address is refused
    let r = update_email(&s, &faye, json!({"email": format!("new-{}", faye.email), "emailAuthFactor": true})).await;
    r.err_status(400);
    assert!(r.text().contains("Please change and verify your email before enabling OTP"), "{}", r.text());
    let sess = session(&s, &faye).await;
    assert_eq!((sess["emailAuthFactor"].clone(), sess["email"].clone()), (json!(true), json!(faye.email)));

    // disabling needs a confirmation token: the first call mails one
    let (r, m) =
        mailed_n(&s, &faye.email, 1, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false})))
            .await;
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
    let (r, m) =
        mailed_n(&s, &faye.email, 1, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &faye.auth())).await;
    assert_eq!(r.ok()["tokenRequired"], json!(true));
    update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false, "token": token_of(&m.unwrap())}))
        .await
        .ok();
    assert_eq!(session(&s, &faye).await["emailAuthFactor"], json!(false));

    // no-op (and no mail) when already disabled
    let (r, _) =
        mailed_n(&s, &faye.email, 0, update_email(&s, &faye, json!({"email": faye.email, "emailAuthFactor": false})))
            .await;
    r.ok();
    assert_eq!(session(&s, &faye).await["emailConfirmed"], json!(true));

    // the address is matched case-insensitively (a toggle, not a change)
    let (r, _) = mailed_n(
        &s,
        &faye.email,
        0,
        update_email(&s, &faye, json!({"email": faye.email.to_uppercase(), "emailAuthFactor": true})),
    )
    .await;
    r.ok();
    let sess = session(&s, &faye).await;
    assert_eq!(sess["emailAuthFactor"], json!(true));
    assert_eq!(sess["email"], json!(faye.email));
    assert_eq!(sess["emailConfirmed"], json!(true));

    // changing the address drops the factor (codes must not go to an
    // unconfirmed inbox)
    let (_, m) =
        mailed_n(&s, &faye.email, 1, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &faye.auth())).await;
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
    s.login(&a.handle, &a.password, None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_in_with_emailed_code() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "hal").await;
    enable(&s, &a).await;

    let code = request_code(&s, &a).await;
    // wrong code
    s.login(&a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    // wrong password with the right code: still the password error
    s.login(&a.handle, "not-the-password", Some(&code)).await.err_status(401);
    // right code (case-insensitive, as tokens are uppercased)
    let j = s.login(&a.handle, &a.password, Some(&code.to_lowercase())).await.ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["emailAuthFactor"], json!(true));
    assert!(j["accessJwt"].is_string());
    // single use
    s.login(&a.handle, &a.password, Some(&code)).await.err(400, "InvalidToken");

    // a newer code (a minute on) replaces the older one
    let old = request_code(&s, &a).await;
    age_email_token(&s, &a.did, "auth_factor", 61_000).await;
    let new = request_code(&s, &a).await;
    assert_ne!(old, new);
    s.login(&a.handle, &a.password, Some(&old)).await.err(400, "InvalidToken");
    s.login(&a.handle, &a.password, Some(&new)).await.ok();

    // login by email address asks too
    let (r, _) = mailed_n(&s, &a.email, 1, s.login(&a.email, &a.password, None)).await;
    r.err(401, "AuthFactorTokenRequired");

    // app passwords bypass the factor (as in the reference)
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "phone"}), &a.auth()).await.ok();
    let (r, _) = mailed_n(&s, &a.email, 0, s.login(&a.handle, ap["password"].as_str().unwrap(), None)).await;
    r.ok();

    // refreshSession/getSession report the flag
    let sess = session(&s, &a).await;
    assert_eq!(sess["emailAuthFactor"], json!(true));
}

/// The reference's `login()` checks an `authFactorToken` whenever one is
/// sent (`if (authFactorToken) assertValidEmailTokenAndCleanup(...)`), not
/// only when the factor is on: with no factor (and so no code mailed) it is
/// refused, and app passwords, which skip the factor, check a sent code too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn code_without_a_factor_is_still_checked() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "lou").await;
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "phone"}), &a.auth()).await.ok();
    let ap = ap["password"].as_str().unwrap().to_string();
    // no factor: a code is refused, no code is a plain sign-in
    let (r, _) = mailed_n(&s, &a.email, 0, s.login(&a.handle, &a.password, Some("AAAAA-AAAAA"))).await;
    r.err(400, "InvalidToken");
    assert!(r.text().contains("Token is invalid"), "{}", r.text());
    s.login(&a.handle, &ap, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    // empty is no code (JS truthiness)
    s.login(&a.handle, &a.password, Some("")).await.ok();
    s.login(&a.handle, &a.password, None).await.ok();
    s.login(&a.handle, &ap, None).await.ok();

    // factor on: an app password with no code still bypasses it; with a
    // mailed code, the code is checked and spent
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    let (r, _) = mailed_n(&s, &a.email, 0, s.login(&a.handle, &ap, None)).await;
    r.ok();
    s.login(&a.handle, &ap, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    s.login(&a.handle, &ap, Some(&code)).await.ok();
    s.login(&a.handle, &a.password, Some(&code)).await.err(400, "InvalidToken");
}

/// A sign-in within a minute of the last code mails none (rate limits on or
/// off) and still asks for the code, which still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_code_is_not_resent_within_a_minute() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "kit").await;
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    for ident in [&a.handle, &a.email] {
        let (r, _) = mailed_n(&s, &a.email, 0, s.login(ident, &a.password, None)).await;
        r.err(401, "AuthFactorTokenRequired");
    }
    // a minute on, a fresh one goes out and replaces it
    age_email_token(&s, &a.did, "auth_factor", 61_000).await;
    let new = request_code(&s, &a).await;
    s.login(&a.handle, &a.password, Some(&code)).await.err(400, "InvalidToken");
    s.login(&a.handle, &a.password, Some(&new)).await.ok();
    // once used, the next sign-in mails a code at once
    request_code(&s, &a).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_code_is_refused() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "ida").await;
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    // age the stored token past its 15 minutes
    age_email_token(&s, &a.did, "auth_factor", 16 * 60 * 1000).await;
    s.login(&a.handle, &a.password, Some(&code)).await.err(400, "ExpiredToken");
    // a fresh one works
    let code = request_code(&s, &a).await;
    s.login(&a.handle, &a.password, Some(&code)).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_codes_lock_the_factor() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "jo").await;
    enable(&s, &a).await;
    let code = request_code(&s, &a).await;
    // MAX_FAILURES - 1 wrong codes are plain InvalidToken
    for _ in 1..vlpds::totp::MAX_FAILURES {
        s.login(&a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(400, "InvalidToken");
    }
    // the next one locks
    s.login(&a.handle, &a.password, Some("AAAAA-AAAAA")).await.err(429, "RateLimitExceeded");
    // while locked even the right code is refused, and no code is mailed
    s.login(&a.handle, &a.password, Some(&code)).await.err(429, "RateLimitExceeded");
    let (r, _) = mailed_n(&s, &a.email, 0, s.login(&a.handle, &a.password, None)).await;
    r.err(429, "RateLimitExceeded");
    // the account still works through an app password
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "x"}), &a.auth()).await.ok();
    s.login(&a.handle, ap["password"].as_str().unwrap(), None).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_takes_precedence_over_email() {
    let s = TestServer::spawn().await;
    let a = confirmed_account(&s, "kim").await;
    enable(&s, &a).await;
    // enable TOTP as well
    let (secret, step) = s.enable_totp(&a).await;
    // no email code: TOTP is asked for
    let (r, _) = mailed_n(&s, &a.email, 0, s.login(&a.handle, &a.password, None)).await;
    r.err(401, "AuthFactorTokenRequired");
    assert!(r.text().contains("two-factor authentication code"), "{}", r.text());
    // a TOTP code signs in
    let code = vlpds::totp::code_for_step(&secret, step + 1);
    s.login(&a.handle, &a.password, Some(&code)).await.ok();
    // both stay enabled
    assert_eq!(session(&s, &a).await["emailAuthFactor"], json!(true));
}
