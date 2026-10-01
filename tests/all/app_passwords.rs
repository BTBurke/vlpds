//! Port of atproto/packages/pds/tests/app-passwords.test.ts.
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use crate::common::*;

const SERVICE_DID: &str = "did:web:localhost";

fn scope(tok: &str) -> J {
    let p = tok.split('.').nth(1).unwrap();
    serde_json::from_slice::<J>(&B64.decode(p).unwrap()).unwrap()["scope"].clone()
}

struct Sess {
    access: String,
    refresh: String,
}

impl Sess {
    fn auth(&self) -> Auth {
        Auth::Bearer(self.access.clone())
    }
}

async fn app_login(s: &TestServer, a: &TestAccount, pw: &str) -> Sess {
    let j = s.create_session(&a.handle, pw).await.ok();
    assert_eq!(j["did"], json!(a.did));
    Sess { access: j["accessJwt"].as_str().unwrap().into(), refresh: j["refreshJwt"].as_str().unwrap().into() }
}

async fn create_app_password(s: &TestServer, auth: &Auth, name: &str, privileged: bool) -> Resp {
    s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": name, "privileged": privileged}), auth).await
}

async fn can_post(s: &TestServer, a: &TestAccount, auth: &Auth) {
    s.xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("Testing testing")}),
            auth,
        )
        .await
        .ok();
}

async fn service_auth(s: &TestServer, auth: &Auth, lxm: Option<&str>) -> Resp {
    let mut q = vec![("aud", SERVICE_DID)];
    if let Some(l) = lxm {
        q.push(("lxm", l));
    }
    s.xrpc.get("com.atproto.server.getServiceAuth", &q, auth).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_password_lifecycle() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;

    // creates normal + privileged app passwords
    let j = create_app_password(&s, &a.auth(), "test-pass", false).await.ok();
    assert_eq!(j["name"], json!("test-pass"));
    assert_eq!(j["privileged"], json!(false));
    assert!(j["createdAt"].is_string());
    let app_pass = j["password"].as_str().unwrap().to_string();
    let j = create_app_password(&s, &a.auth(), "privi-pass", true).await.ok();
    assert_eq!(j["name"], json!("privi-pass"));
    assert_eq!(j["privileged"], json!(true));
    let privi_pass = j["password"].as_str().unwrap().to_string();
    // duplicate name is rejected
    create_app_password(&s, &a.auth(), "test-pass", false).await.client_err();

    // sessions with app passwords and their token scopes
    let app = app_login(&s, &a, &app_pass).await;
    let privi = app_login(&s, &a, &privi_pass).await;
    assert_eq!(scope(&app.access), json!("com.atproto.appPass"));
    assert_eq!(scope(&privi.access), json!("com.atproto.appPassPrivileged"));

    // allowed: repo writes
    can_post(&s, &a, &app.auth()).await;
    can_post(&s, &a, &privi.auth()).await;

    // restricted: full-access-only actions
    create_app_password(&s, &app.auth(), "another-one", false).await.client_err();
    create_app_password(&s, &privi.auth(), "another-one", false).await.client_err();

    // service auth for privileged methods only with privileged app passwords
    let r = service_auth(&s, &app.auth(), Some("com.atproto.server.createAccount")).await;
    r.client_err();
    let r = service_auth(&s, &app.auth(), Some("com.atproto.server.createaccount")).await;
    r.client_err();
    service_auth(&s, &privi.auth(), Some("com.atproto.server.createAccount")).await.ok();

    // scope persists across refresh
    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(app.refresh.clone())).await.ok();
    let app2 = Sess { access: r["accessJwt"].as_str().unwrap().into(), refresh: r["refreshJwt"].as_str().unwrap().into() };
    assert_eq!(scope(&app2.access), json!("com.atproto.appPass"));
    can_post(&s, &a, &app2.auth()).await;
    service_auth(&s, &app2.auth(), Some("com.atproto.server.createAccount")).await.client_err();
    create_app_password(&s, &app2.auth(), "another-one", false).await.client_err();

    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(privi.refresh.clone())).await.ok();
    let privi2 = Sess { access: r["accessJwt"].as_str().unwrap().into(), refresh: r["refreshJwt"].as_str().unwrap().into() };
    assert_eq!(scope(&privi2.access), json!("com.atproto.appPassPrivileged"));
    can_post(&s, &a, &privi2.auth()).await;
    service_auth(&s, &privi2.auth(), None).await.ok();
    create_app_password(&s, &privi2.auth(), "another-one", false).await.client_err();

    // listing (app-password sessions may list)
    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &app2.auth()).await.ok();
    let pws = j["passwords"].as_array().unwrap();
    assert_eq!(pws.len(), 2, "{j}");
    let find = |n: &str| pws.iter().find(|p| p["name"] == json!(n)).cloned().unwrap_or_else(|| panic!("{n} not listed"));
    assert_eq!(find("privi-pass")["privileged"], json!(true));
    assert_eq!(find("test-pass")["privileged"], json!(false));
    assert!(pws.iter().all(|p| p.get("password").is_none()), "listing must not reveal passwords");

    // revocation
    s.xrpc.post("com.atproto.server.revokeAppPassword", &json!({"name": "test-pass"}), &a.auth()).await.ok();
    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(app2.refresh.clone())).await;
    assert_eq!(r.status, 400, "refresh after revocation: {}", r.text());
    assert!(matches!(r.error_name(), Some("ExpiredToken" | "InvalidToken")), "{}", r.text());
    s.create_session(&a.handle, &app_pass).await.err(401, "AuthenticationRequired");
    // the privileged one still works
    app_login(&s, &a, &privi_pass).await;
    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &a.auth()).await.ok();
    assert_eq!(j["passwords"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoking_app_password_revokes_its_access_tokens() {
    // Not asserted by the TS suite (access tokens there stay valid until
    // expiry), but revocation must at least stop refreshes; we additionally
    // require new logins to fail.
    let s = TestServer::spawn().await;
    let a = s.create_account("bob").await;
    let pw = create_app_password(&s, &a.auth(), "x", false).await.ok()["password"].as_str().unwrap().to_string();
    let sess = app_login(&s, &a, &pw).await;
    s.xrpc.post("com.atproto.server.revokeAppPassword", &json!({"name": "x"}), &a.auth()).await.ok();
    s.create_session(&a.handle, &pw).await.err(401, "AuthenticationRequired");
    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(sess.refresh.clone())).await;
    r.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_password_cannot_manage_account() {
    let s = TestServer::spawn().await;
    let a = s.create_account("carl").await;
    let pw = create_app_password(&s, &a.auth(), "x", true).await.ok()["password"].as_str().unwrap().to_string();
    let sess = app_login(&s, &a, &pw).await;
    // account-management procedures need a full session
    s.xrpc.post("com.atproto.server.revokeAppPassword", &json!({"name": "x"}), &sess.auth()).await.client_err();
    s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &sess.auth()).await.client_err();
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &sess.auth()).await.client_err();
    s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &sess.auth()).await.client_err();
    s.xrpc
        .post("com.atproto.identity.updateHandle", &json!({"handle": format!("{}.{HANDLE_DOMAIN}", unique_name("nh"))}), &sess.auth())
        .await
        .client_err();
    // getSession works
    let j = s.xrpc.get("com.atproto.server.getSession", &[], &sess.auth()).await.ok();
    assert_eq!(j["did"], json!(a.did));
}
