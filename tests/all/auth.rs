//! Port of atproto/packages/pds/tests/auth.test.ts: session creation, refresh
//! (rotation, grace period, revocation), token types, expiry, takedowns and
//! the email sign-in second factor.
use crate::common::*;
use std::collections::HashSet;
use std::sync::Arc;

const JWT_SECRET: &str = "dev-secret-change-me";
const SERVICE_DID: &str = "did:web:localhost";

/// HS256 JWT signed with the test server's session secret.
fn forge(claims: &J, typ: &str, secret: &str) -> String {
    use hmac::{Hmac, Mac};
    let header = b64url(json!({"alg": "HS256", "typ": typ}).to_string());
    let payload = b64url(claims.to_string());
    let input = format!("{header}.{payload}");
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(input.as_bytes());
    format!("{input}.{}", b64url(mac.finalize().into_bytes()))
}

async fn get_session(s: &TestServer, tok: &str) -> Resp {
    s.xrpc.get("com.atproto.server.getSession", &[], &Auth::Bearer(tok.into())).await
}

async fn refresh(s: &TestServer, tok: &str) -> Resp {
    s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(tok.into())).await
}

async fn delete_session(s: &TestServer, tok: &str) -> Resp {
    s.xrpc.post_empty("com.atproto.server.deleteSession", &Auth::Bearer(tok.into())).await
}

#[track_caller]
fn assert_session_info(j: &J, a: &TestAccount) {
    assert_eq!(j["did"], json!(a.did), "{j}");
    assert_eq!(j["handle"], json!(a.handle), "{j}");
    assert_eq!(j["email"], json!(a.email), "{j}");
    assert_eq!(j["emailConfirmed"], json!(false), "{j}");
    assert_eq!(j["emailAuthFactor"], json!(false), "{j}");
    assert_eq!(j["active"], json!(true), "{j}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_tokens_on_account_creation() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    assert_session_info(&get_session(&s, &a.access).await.ok(), &a);
    let next = refresh(&s, &a.refresh).await.ok();
    assert_eq!(next["did"], json!(a.did));
    assert_eq!(next["handle"], json!(a.handle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_tokens_on_session_creation() {
    let s = TestServer::spawn().await;
    let a = s.create_account("bob").await;
    let sess = s.create_session(&a.handle, &a.password).await.ok();
    assert_session_info(&get_session(&s, sess["accessJwt"].as_str().unwrap()).await.ok(), &a);
    let next = refresh(&s, sess["refreshJwt"].as_str().unwrap()).await.ok();
    assert_eq!(next["did"], json!(a.did));
    assert_eq!(next["handle"], json!(a.handle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_creation_using_email_address() {
    let s = TestServer::spawn().await;
    let a = s.create_account("bob").await;
    let j = s.create_session(&a.email.to_uppercase(), &a.password).await.ok();
    assert_eq!(j["handle"], json!(a.handle));
    assert_eq!(j["did"], json!(a.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_password_and_unknown_identifier_are_indistinguishable() {
    let s = TestServer::spawn().await;
    let a = s.create_account("bob").await;
    let wrong = s.create_session(&a.handle, "wrong-password").await;
    let unknown = s.create_session(&format!("no-such-user.{HANDLE_DOMAIN}"), "any-password").await;
    wrong.err(401, "AuthenticationRequired");
    assert_eq!((wrong.status, &wrong.json), (unknown.status, &unknown.json), "responses must be identical");
    assert_eq!(wrong.json["message"], json!("Invalid identifier or password"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_tokens_on_session_refresh_and_chained_refresh() {
    let s = TestServer::spawn().await;
    let a = s.create_account("carol").await;
    let sess = refresh(&s, &a.refresh).await.ok();
    assert_session_info(&get_session(&s, sess["accessJwt"].as_str().unwrap()).await.ok(), &a);
    let next = refresh(&s, sess["refreshJwt"].as_str().unwrap()).await.ok();
    assert_eq!(next["did"], json!(a.did));
    // token types
    assert_eq!(jwt_claims(&a.access)["scope"], json!("com.atproto.access"));
    assert_eq!(jwt_claims(&a.refresh)["scope"], json!("com.atproto.refresh"));
    assert_eq!(jwt_claims(&a.access)["sub"], json!(a.did));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handles_racing_refreshes() {
    let s = Arc::new(TestServer::spawn().await);
    let a = s.create_account("dan").await;
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let s = s.clone();
        let tok = a.refresh.clone();
        tasks.push(tokio::spawn(async move {
            let j = refresh(&s, &tok).await.ok();
            jwt_claims(j["refreshJwt"].as_str().unwrap())["jti"].as_str().expect("jti on refresh token").to_string()
        }));
    }
    let mut ids = HashSet::new();
    for t in tasks {
        ids.insert(t.await.unwrap());
    }
    assert_eq!(ids.len(), 1, "racing refreshes must yield the same next token id: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_reuse_within_grace_period_yields_same_token_id() {
    let s = TestServer::spawn().await;
    let a = s.create_account("eve").await;
    let r1 = refresh(&s, &a.refresh).await.ok();
    let r2 = refresh(&s, &a.refresh).await.ok();
    let t0 = jwt_claims(&a.refresh);
    let t1 = jwt_claims(r1["refreshJwt"].as_str().unwrap());
    let t2 = jwt_claims(r2["refreshJwt"].as_str().unwrap());
    assert!(t1["jti"].is_string());
    assert_eq!(t1["jti"], t2["jti"]);
    assert_ne!(t1["jti"], t0["jti"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_token_revoked_when_session_deleted() {
    let s = TestServer::spawn().await;
    let a = s.create_account("finn").await;
    delete_session(&s, &a.refresh).await.ok();
    let r = refresh(&s, &a.refresh).await;
    assert_eq!(r.status, 400, "{}", r.text());
    assert!(matches!(r.error_name(), Some("ExpiredToken" | "InvalidToken")), "{}", r.text());
    // double revoke is fine
    delete_session(&s, &a.refresh).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn access_token_cannot_refresh_and_refresh_token_cannot_access() {
    let s = TestServer::spawn().await;
    let a = s.create_account("gordon").await;
    refresh(&s, &a.access).await.err(400, "InvalidToken");
    let r = get_session(&s, &a.refresh).await;
    assert!(matches!(r.status, 400 | 401), "refresh token used as access token: {}", r.text());
    // a refresh token cannot write either
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x")});
    let r = s.xrpc.post("com.atproto.repo.createRecord", &body, &a.refresh_auth()).await;
    assert!(matches!(r.status, 400 | 401), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_refresh_token_cannot_be_used() {
    let s = TestServer::spawn().await;
    let a = s.create_account("holga").await;
    let t = now_secs();
    let tok = forge(
        &json!({"scope": "com.atproto.refresh", "sub": a.did, "aud": SERVICE_DID, "iat": t - 100, "exp": t - 1, "jti": "expired-test-jti"}),
        "refresh+jwt",
        JWT_SECRET,
    );
    refresh(&s, &tok).await.err(400, "ExpiredToken");
    // revoking an expired token is not an error
    delete_session(&s, &tok).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_and_expired_access_tokens_are_rejected() {
    let s = TestServer::spawn().await;
    let a = s.create_account("ivan").await;
    let t = now_secs();
    let expired = forge(&json!({"scope": "com.atproto.access", "sub": a.did, "aud": SERVICE_DID, "iat": t - 100, "exp": t - 1}), "at+jwt", JWT_SECRET);
    let r = get_session(&s, &expired).await;
    assert!(matches!(r.status, 400 | 401), "{}", r.text());
    assert!(matches!(r.error_name(), Some("ExpiredToken" | "InvalidToken" | "AuthenticationRequired")), "{}", r.text());

    let wrong_key = forge(&json!({"scope": "com.atproto.access", "sub": a.did, "aud": SERVICE_DID, "iat": t, "exp": t + 600}), "at+jwt", "not-the-secret");
    for tok in [wrong_key.as_str(), "garbage", "a.b.c", ""] {
        let r = get_session(&s, tok).await;
        assert!(matches!(r.status, 400 | 401), "token {tok:?}: {}", r.text());
    }
    // alg=none must never be accepted
    let claims = json!({"scope": "com.atproto.access", "sub": a.did, "aud": SERVICE_DID, "iat": t, "exp": t + 600});
    let none = format!("{}.{}.", b64url(r#"{"alg":"none","typ":"at+jwt"}"#), b64url(claims.to_string()));
    let r = get_session(&s, &none).await;
    assert!(matches!(r.status, 400 | 401), "alg=none accepted: {}", r.text());
    // wrong scheme
    let r = s.get_session(&Auth::Raw(format!("Token {}", a.access))).await;
    assert!(matches!(r.status, 400 | 401), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_takedown_disallows_fresh_session() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    set_repo_takedown(&s, &a.did, true).await;
    let r = s.create_session(&a.handle, &a.password).await;
    r.client_err();
    assert_eq!(r.error_name(), Some("AccountTakedown"), "{}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_takedown_disallows_refresh_session() {
    let s = TestServer::spawn().await;
    let a = s.create_account("jared").await;
    set_repo_takedown(&s, &a.did, true).await;
    let r = refresh(&s, &a.refresh).await;
    r.client_err();
    assert_eq!(r.error_name(), Some("AccountTakedown"), "{}", r.text());
}

// ---- email sign-in second factor ----

async fn enable_email_auth_factor(s: &TestServer, a: &TestAccount) {
    let (tok, _, _) = mailed(s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &a.auth())).await;
    s.xrpc.post("com.atproto.server.confirmEmail", &json!({"email": a.email, "token": tok}), &a.auth()).await.ok();
    let (tok, r, _) = mailed(s, &a.email, s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &a.auth())).await;
    assert_eq!(r.ok()["tokenRequired"], json!(true));
    let body = json!({"email": a.email, "emailAuthFactor": true, "token": tok});
    s.xrpc.post("com.atproto.server.updateEmail", &body, &a.auth()).await.ok();
    let sess = get_session(s, &a.access).await.ok();
    assert_eq!(sess["emailAuthFactor"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_2fa_challenges_and_accepts_token() {
    let s = TestServer::spawn().await;
    let a = s.create_account("jane").await;
    enable_email_auth_factor(&s, &a).await;

    let (tok, r, _) = mailed(&s, &a.email, s.create_session(&a.handle, &a.password)).await;
    r.err(401, "AuthFactorTokenRequired");

    let j = s.login(&a.handle, &a.password, Some(&tok)).await.ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["email"], json!(a.email));
    assert_eq!(j["emailConfirmed"], json!(true));
    assert_eq!(j["emailAuthFactor"], json!(true));
    assert_eq!(j["active"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn email_2fa_rejects_invalid_token() {
    let s = TestServer::spawn().await;
    let a = s.create_account("jane").await;
    enable_email_auth_factor(&s, &a).await;
    let (tok, r, _) = mailed(&s, &a.email, s.create_session(&a.handle, &a.password)).await;
    r.err(401, "AuthFactorTokenRequired");
    assert_ne!(tok, "AAAAA-AAAAA");
    let r = s.login(&a.handle, &a.password, Some("AAAAA-AAAAA")).await;
    r.client_err();
    assert!(r.text().contains("Token is invalid") || r.error_name() == Some("InvalidToken"), "{}", r.text());
    s.login(&a.handle, &a.password, Some(&tok)).await.ok();
}
