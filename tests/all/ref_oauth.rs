//! Ported from the reference PDS's oauth-deactivation.test.ts and the OAuth
//! case of auth.test.ts (see tests/REFERENCE_COVERAGE.md). A child module of
//! `oauth` (declared there with `#[path]`) so it reuses that file's client
//! and browser simulation.

use super::*;

/// A loopback client authorized for `scope` by `acct`; returns the DPoP key
/// and the access token.
async fn authorize(s: &Srv, acct: &Account, scope: &str) -> (DpopKey, String) {
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id(scope, redirect);
    let t = {
        let f = Flow::new(&cid, redirect, scope, &key);
        let mut b = Browser::default();
        let p = pkce();
        let code = authorize_interactive(s, &mut b, &f, acct, &p).await;
        tokens(&exchange(s, &f, &code, &p, &[]).await)
    };
    assert_eq!(t.scope, scope);
    (key, t.access)
}

/// oauth-deactivation.test.ts, all five cases in order: the status scope
/// is required, reactivation is refused over OAuth, and an OAuth
/// deactivation drops the account's app passwords and OAuth sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_account_deactivation_over_oauth() {
    let s = spawn().await;
    let acct = create_account(&s, "deact").await;
    let (ukey, unscoped) = authorize(&s, &acct, "atproto").await;
    let (skey, scoped) = authorize(&s, &acct, "atproto account:status?action=manage").await;

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
    let ap = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createAppPassword", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({"name": "before-deactivation"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ap.status(), 200);
    let ap_password = ap.json::<J>().await.unwrap()["password"].as_str().unwrap().to_string();
    let r = xrpc_dpop(&s, &skey, &scoped, "POST", "com.atproto.server.deactivateAccount", Some(json!({}))).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let status: J = s
        .http
        .get(format!("{}/xrpc/com.atproto.sync.getRepoStatus?did={}", s.base, acct.did))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status, json!({"did": acct.did, "active": false, "status": "deactivated"}));

    // revokes app passwords on OAuth deactivation
    let list: J = s
        .http
        .get(format!("{}/xrpc/com.atproto.server.listAppPasswords", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
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
    let r = xrpc_dpop(&s, &skey, &scoped, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.status, 401, "{}", r.body);
    let r = xrpc_dpop(&s, &ukey, &unscoped, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.status, 401, "{}", r.body);
    // the password session is kept (the reference deletes only OAuth/app-password credentials)
    let r = s
        .http
        .get(format!("{}/xrpc/com.atproto.server.getSession", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

/// A password-session deactivation keeps OAuth sessions and app passwords
/// (the reference passes `deleteCredentials` only for OAuth callers).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_password_session_deactivation_keeps_credentials() {
    let s = spawn().await;
    let acct = create_account(&s, "deact2").await;
    let (key, tok) = authorize(&s, &acct, "atproto").await;
    let ap = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.createAppPassword", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({"name": "kept"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ap.status(), 200);
    let r = s
        .http
        .post(format!("{}/xrpc/com.atproto.server.deactivateAccount", s.base))
        .bearer_auth(&acct.jwt)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let list: J = s
        .http
        .get(format!("{}/xrpc/com.atproto.server.listAppPasswords", s.base))
        .bearer_auth(&acct.jwt)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
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
        let csrf = csrf_of(&html);
        let (st, h, _) = b
            .post(&s, "/oauth/account/sign-in", &[("csrf", &csrf), ("identifier", ident), ("password", "wrong-password")])
            .await;
        outcomes.push((st, h.get("location").map(|v| v.to_str().unwrap().to_string())));
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0], (303, Some("/oauth/account?add=1&error=invalid".to_string())));

    // the authorization page
    let key = DpopKey::new();
    let redirect = "http://127.0.0.1/cb";
    let cid = loopback_client_id("atproto", redirect);
    let f = Flow::new(&cid, redirect, "atproto", &key);
    let mut outcomes = Vec::new();
    for ident in [unknown.as_str(), acct.handle.as_str()] {
        let par = f.par(&s, &pkce(), "st").await;
        assert_eq!(par.status, 201, "{}", par.body);
        let ru = par.body["request_uri"].as_str().unwrap().to_string();
        let mut b = Browser::default();
        let (_, _, html) = b.get(&s, &f.authorize_url(&s, &ru)).await;
        let csrf = csrf_of(&html);
        let (st, _, html) = b
            .post(
                &s,
                "/oauth/authorize/sign-in",
                &[("request_uri", &ru), ("csrf", &csrf), ("identifier", ident), ("password", "wrong-password"), ("action", "sign-in")],
            )
            .await;
        assert!(html.contains("Invalid handle or password"), "{html}");
        outcomes.push(st);
    }
    assert_eq!(outcomes[0], outcomes[1]);
}
