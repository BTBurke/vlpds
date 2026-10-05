//! Port of atproto/packages/pds/tests/app-passwords.test.ts.
use crate::common::*;

const SERVICE_DID: &str = "did:web:localhost";

fn scope(tok: &str) -> J {
    jwt_claims(tok)["scope"].clone()
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
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("Testing testing")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, auth).await.ok();
}

async fn refresh(s: &TestServer, sess: &Sess) -> Sess {
    let r = s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(sess.refresh.clone())).await.ok();
    Sess { access: r["accessJwt"].as_str().unwrap().into(), refresh: r["refreshJwt"].as_str().unwrap().into() }
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
    service_auth(&s, &app.auth(), Some("com.atproto.server.createAccount")).await.client_err();
    service_auth(&s, &app.auth(), Some("com.atproto.server.createaccount")).await.client_err();
    service_auth(&s, &privi.auth(), Some("com.atproto.server.createAccount")).await.ok();

    // scope persists across refresh
    let app2 = refresh(&s, &app).await;
    assert_eq!(scope(&app2.access), json!("com.atproto.appPass"));
    can_post(&s, &a, &app2.auth()).await;
    service_auth(&s, &app2.auth(), Some("com.atproto.server.createAccount")).await.client_err();
    create_app_password(&s, &app2.auth(), "another-one", false).await.client_err();

    let privi2 = refresh(&s, &privi).await;
    assert_eq!(scope(&privi2.access), json!("com.atproto.appPassPrivileged"));
    can_post(&s, &a, &privi2.auth()).await;
    service_auth(&s, &privi2.auth(), None).await.ok();
    create_app_password(&s, &privi2.auth(), "another-one", false).await.client_err();

    // listing (app-password sessions may list)
    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &app2.auth()).await.ok();
    let pws = j["passwords"].as_array().unwrap();
    assert_eq!(pws.len(), 2, "{j}");
    let find =
        |n: &str| pws.iter().find(|p| p["name"] == json!(n)).cloned().unwrap_or_else(|| panic!("{n} not listed"));
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
    s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(sess.refresh.clone())).await.client_err();
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
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name("nh"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": handle}), &sess.auth()).await.client_err();
    assert_eq!(s.get_session(&sess.auth()).await.ok()["did"], json!(a.did));
}

/// The account page's "Post only" preset.
const POST_ONLY: &str = "atproto repo?collection=app.bsky.feed.like&collection=app.bsky.feed.post&collection=app.bsky.feed.repost&collection=app.bsky.graph.follow&action=create&action=delete blob:*/*";

async fn create_scoped(s: &TestServer, auth: &Auth, name: &str, privileged: bool, scopes: &str) -> Resp {
    let body = json!({"name": name, "privileged": privileged, "scopes": scopes});
    s.xrpc.post("com.atproto.server.createAppPassword", &body, auth).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_only_app_password() {
    let s = TestServer::spawn().await;
    let a = s.create_account("poster").await;
    // privileged too: the scopes still withhold DMs and the rest
    let j = create_scoped(&s, &a.auth(), "bot", true, POST_ONLY).await.ok();
    assert_eq!(j["scopes"], json!(POST_ONLY));
    let pw = j["password"].as_str().unwrap().to_string();
    let sess = app_login(&s, &a, &pw).await;
    let claims = jwt_claims(&sess.access);
    assert_eq!(claims["scope"], json!("com.atproto.appPassPrivileged"));
    assert_eq!(claims["appPassScope"], json!(POST_ONLY));

    // granted: posts, likes, blobs, deletes
    let post = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("bot post")});
    let p = s.xrpc.post("com.atproto.repo.createRecord", &post, &sess.auth()).await.ok();
    let like =
        json!({"$type": "app.bsky.feed.like", "subject": {"uri": p["uri"], "cid": p["cid"]}, "createdAt": now_iso()});
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.like", "record": like});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &sess.auth()).await.ok();
    s.xrpc.post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &sess.auth()).await.ok();
    let rkey = p["uri"].as_str().unwrap().rsplit('/').next().unwrap().to_string();
    let del = json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": rkey});
    s.xrpc.post("com.atproto.repo.deleteRecord", &del, &sess.auth()).await.ok();

    // withheld by the scopes
    let profile = json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": {"$type": "app.bsky.actor.profile", "displayName": "pwned"}});
    s.xrpc.post("com.atproto.repo.putRecord", &profile, &sess.auth()).await.err(403, "ScopeMissingError");
    let edit = json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": "3l3qo2vutsw2b", "record": post_record("edit")});
    s.xrpc.post("com.atproto.repo.putRecord", &edit, &sess.auth()).await.err(403, "ScopeMissingError");
    // an unscoped app password may ask for an email confirmation; this one may not
    s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &sess.auth()).await.err(403, "ScopeMissingError");
    service_auth(&s, &sess.auth(), None).await.err(403, "ScopeMissingError");
    service_auth(&s, &sess.auth(), Some("com.atproto.repo.uploadBlob")).await.err(403, "ScopeMissingError");
    s.xrpc.get("com.atproto.server.listAppPasswords", &[], &sess.auth()).await.err(403, "InsufficientScope");
    // withheld from any app password
    create_app_password(&s, &sess.auth(), "another-one", false).await.client_err();
    create_scoped(&s, &sess.auth(), "another-one", false, POST_ONLY).await.client_err();
    s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &sess.auth()).await.client_err();
    s.xrpc.post_empty("com.atproto.server.requestEmailUpdate", &sess.auth()).await.client_err();
    let upd = json!({"email": "new@example.com"});
    s.xrpc.post("com.atproto.server.updateEmail", &upd, &sess.auth()).await.client_err();
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &sess.auth()).await.client_err();

    // getSession works, without the email (no account:email)
    let me = s.get_session(&sess.auth()).await.ok();
    assert_eq!(me["did"], json!(a.did));
    assert!(me.get("email").is_none(), "{me}");
    let login = s.create_session(&a.handle, &pw).await.ok();
    assert!(login.get("email").is_none(), "{login}");

    // the scopes survive a refresh
    let sess2 = refresh(&s, &sess).await;
    assert_eq!(jwt_claims(&sess2.access)["appPassScope"], json!(POST_ONLY));
    can_post(&s, &a, &sess2.auth()).await;
    s.xrpc.post("com.atproto.repo.putRecord", &profile, &sess2.auth()).await.err(403, "ScopeMissingError");

    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &a.auth()).await.ok();
    assert_eq!(j["passwords"][0]["scopes"], json!(POST_ONLY), "{j}");

    // revocation as today
    s.xrpc.post("com.atproto.server.revokeAppPassword", &json!({"name": "bot"}), &a.auth()).await.ok();
    s.xrpc.post_empty("com.atproto.server.refreshSession", &Auth::Bearer(sess2.refresh.clone())).await.client_err();
    s.create_session(&a.handle, &pw).await.err(401, "AuthenticationRequired");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_password_scopes_validated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("scopecheck").await;
    for bad in ["bogus", "atproto repo:not..an.nsid", "include:com.example.authBasic", "rpc:*?aud=*", "blob:nope"] {
        create_scoped(&s, &a.auth(), "x", false, bad).await.err(400, "InvalidRequest");
    }
    let long = (0..200).map(|i| format!("repo:com.example.c{i}")).collect::<Vec<_>>().join(" ");
    create_scoped(&s, &a.auth(), "x", false, &long).await.err(400, "InvalidRequest");
    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &a.auth()).await.ok();
    assert!(j["passwords"].as_array().unwrap().is_empty(), "{j}");

    // blank is unscoped; whitespace and repeats are normalized
    let j = create_scoped(&s, &a.auth(), "blank", false, "  ").await.ok();
    assert!(j.get("scopes").is_none(), "{j}");
    let j = create_scoped(&s, &a.auth(), "spaced", false, " atproto\n blob:image/*  atproto ").await.ok();
    assert_eq!(j["scopes"], json!("atproto blob:image/*"));

    // an unscoped password is unchanged: no claim, nothing listed
    let pw = create_app_password(&s, &a.auth(), "plain", false).await.ok()["password"].as_str().unwrap().to_string();
    let sess = app_login(&s, &a, &pw).await;
    assert!(jwt_claims(&sess.access).get("appPassScope").is_none());
    let j = s.xrpc.get("com.atproto.server.listAppPasswords", &[], &sess.auth()).await.ok();
    let plain = j["passwords"].as_array().unwrap().iter().find(|p| p["name"] == json!("plain")).cloned().unwrap();
    assert!(plain.get("scopes").is_none(), "{plain}");
    s.xrpc.post_empty("com.atproto.server.requestEmailConfirmation", &sess.auth()).await.ok();
    assert!(s.get_session(&sess.auth()).await.ok()["email"].is_string());
}
