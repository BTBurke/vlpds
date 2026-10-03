//! Ported from the reference PDS's oauth-deactivation.test.ts and the OAuth
//! case of auth.test.ts (see tests/REFERENCE_COVERAGE.md). A child module of
//! `oauth` (declared there with `#[path]`) so it reuses that file's client
//! and browser simulation.

use super::*;

async fn create_app_password(s: &Srv, acct: &Account, name: &str) -> String {
    let (st, j) = s.bearer(&acct.jwt, "com.atproto.server.createAppPassword", true, Some(json!({"name": name}))).await;
    assert_eq!(st, 200, "{j}");
    j["password"].as_str().unwrap().to_string()
}

/// oauth-deactivation.test.ts, all five cases in order: the status scope
/// is required, reactivation is refused over OAuth, and an OAuth
/// deactivation drops the account's app passwords and OAuth sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_account_deactivation_over_oauth() {
    let s = spawn().await;
    let acct = create_account(&s, "deact").await;
    let (ukey, unscoped) = login(&s, &acct, "atproto").await;
    let (skey, scoped) = login(&s, &acct, "atproto account:status?action=manage").await;

    // rejects deactivation when the session lacks the status scope
    let r = xrpc_dpop(&s, &ukey, &unscoped, "POST", "com.atproto.server.deactivateAccount", Some(json!({}))).await;
    assert_eq!(r.status, 403, "{}", r.body);
    assert_eq!(r.body["error"], "ScopeMissingError", "{}", r.body);
    assert!(r.body["message"].as_str().unwrap().contains("account:status?action=manage"), "{}", r.body);

    // rejects reactivation over OAuth with a message pointing at the account page
    let r = xrpc_dpop(&s, &skey, &scoped, "POST", "com.atproto.server.activateAccount", None).await;
    assert!(r.status >= 400, "{}", r.body);
    assert!(r.body["message"].as_str().unwrap().contains("account management page"), "{}", r.body);

    // deactivates the account when the status scope is granted
    let ap_password = create_app_password(&s, &acct, "before-deactivation").await;
    let r = xrpc_dpop(&s, &skey, &scoped, "POST", "com.atproto.server.deactivateAccount", Some(json!({}))).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let status = s.get_json(&format!("/xrpc/com.atproto.sync.getRepoStatus?did={}", acct.did)).await;
    assert_eq!(status, json!({"did": acct.did, "active": false, "status": "deactivated"}));

    // revokes app passwords on OAuth deactivation
    let (_, list) = s.bearer(&acct.jwt, "com.atproto.server.listAppPasswords", false, None).await;
    assert_eq!(list["passwords"], json!([]), "{list}");
    let login = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.base))
        .json(&json!({"identifier": acct.handle, "password": ap_password}))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 401);

    // revokes the OAuth session that performed the deactivation (and every other one)
    for (key, tok) in [(&skey, &scoped), (&ukey, &unscoped)] {
        let r = xrpc_dpop(&s, key, tok, "GET", "com.atproto.server.getSession", None).await;
        assert_eq!(r.status, 401, "{}", r.body);
    }
    // the password session is kept (the reference deletes only OAuth/app-password credentials)
    assert_eq!(s.bearer(&acct.jwt, "com.atproto.server.getSession", false, None).await.0, 200);
}

/// A password-session deactivation keeps OAuth sessions and app passwords
/// (the reference passes `deleteCredentials` only for OAuth callers).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_password_session_deactivation_keeps_credentials() {
    let s = spawn().await;
    let acct = create_account(&s, "deact2").await;
    let (key, tok) = login(&s, &acct, "atproto").await;
    create_app_password(&s, &acct, "kept").await;
    assert_eq!(s.bearer(&acct.jwt, "com.atproto.server.deactivateAccount", true, Some(json!({}))).await.0, 200);
    let (_, list) = s.bearer(&acct.jwt, "com.atproto.server.listAppPasswords", false, None).await;
    assert_eq!(list["passwords"].as_array().map(Vec::len), Some(1), "{list}");
    let r = xrpc_dpop(&s, &key, &tok, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.status, 200, "{}", r.body);
}

/// auth.test.ts: "returns identical error responses for unknown identifier
/// and known-identifier-with-wrong-password in the OAuth sign-in flow". vlpds
/// signs in with HTML form posts rather than the reference's JSON
/// `~api/sign-in`, so this compares those: the account page's redirect and
/// the authorization page's status and message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_oauth_sign_in_errors_are_indistinguishable() {
    let s = spawn().await;
    let acct = create_account(&s, "bob").await;
    let unknown = format!("no-such-user{}.vlpds.test", rand::random::<u32>() % 100000);

    // the account page
    let mut outcomes = Vec::new();
    for ident in [unknown.as_str(), acct.handle.as_str()] {
        let mut b = Browser::default();
        let (_, _, html) = b.get(&s, &format!("{}/oauth/account?add=1", s.base)).await;
        let (st, h, _) = b.post(&s, "/oauth/account/sign-in", &[("csrf", &csrf_of(&html)), ("identifier", ident), ("password", "wrong-password")]).await;
        outcomes.push((st, h.get("location").map(|v| v.to_str().unwrap().to_string())));
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0], (303, Some("/oauth/account?add=1&error=invalid".to_string())));

    // the authorization page
    let key = DpopKey::new();
    let f = Flow::loopback("atproto", &key);
    let mut outcomes = Vec::new();
    for ident in [unknown.as_str(), acct.handle.as_str()] {
        let ru = f.request_uri(&s, &pkce(), "st").await;
        let mut b = Browser::default();
        let csrf = csrf_of(&b.authorize(&s, &f, &ru).await.2);
        let (st, _, html) = b.sign_in(&s, &ru, &csrf, ident, "wrong-password").await;
        assert!(html.contains("Invalid handle or password"), "{html}");
        outcomes.push(st);
    }
    assert_eq!(outcomes[0], outcomes[1]);
}
