//! Per-route status checks for legacy (Bearer) tokens of a taken-down
//! account: the routes the reference marks `checkTakedown` refuse tokens
//! issued before the takedown with 401 AccountTakedown (takedown itself only
//! deletes refresh tokens and OAuth sessions, as in the reference), while
//! the rest keep working.
use crate::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn check_takedown_routes_refuse_legacy_tokens() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tdr").await;
    let p = s.post(&a, "before").await;
    let app_pw = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "client"}), &a.auth()).await.ok();
    let app_pw = s.create_session(&a.handle, app_pw["password"].as_str().unwrap()).await.ok();
    let app_auth = Auth::Bearer(app_pw["accessJwt"].as_str().unwrap().to_string());
    let car = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await.body.to_vec();
    set_repo_takedown(&s, &a.did, true).await;

    let post = |text: &str| json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": now_iso()});
    let posts: Vec<(&str, J)> = vec![
        ("app.bsky.actor.putPreferences", json!({"preferences": []})),
        ("com.atproto.identity.updateHandle", json!({"handle": a.handle})),
        ("com.atproto.server.createAppPassword", json!({"name": "another"})),
        ("com.atproto.server.requestEmailConfirmation", json!({})),
        ("com.atproto.server.requestEmailUpdate", json!({})),
        ("com.atproto.server.requestAccountDelete", json!({})),
        ("com.atproto.repo.createRecord", json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post("x")})),
        ("com.atproto.repo.putRecord", json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": p.rkey(), "record": post("y")})),
        ("com.atproto.repo.deleteRecord", json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": p.rkey()})),
        (
            "com.atproto.repo.applyWrites",
            json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post("z")}]}),
        ),
    ];
    // Every route refuses the session token for the takedown. App-password
    // tokens are refused by the routes that take them for the takedown too;
    // the others refuse them for their scope first (as in the reference).
    let app_pw_routes = [
        "app.bsky.actor.putPreferences",
        "com.atproto.server.requestEmailConfirmation",
        "com.atproto.repo.createRecord",
        "com.atproto.repo.putRecord",
        "com.atproto.repo.deleteRecord",
        "com.atproto.repo.applyWrites",
        "com.atproto.repo.uploadBlob",
    ];
    let mut wrong = Vec::new();
    for (who, auth) in [("session", a.auth()), ("app password", app_auth.clone())] {
        let mut check = |nsid: &str, r: Resp| {
            let takedown = (r.status, r.error_name()) == (401, Some("AccountTakedown"));
            let ok = if who == "session" || app_pw_routes.contains(&nsid) { takedown } else { r.status >= 400 && r.status < 500 };
            if !ok {
                wrong.push(format!("{who} {nsid}: {} {}", r.status, r.text()));
            }
        };
        for (nsid, body) in &posts {
            check(nsid, s.xrpc.post(nsid, body, &auth).await);
        }
        let nsid = "com.atproto.server.getAccountInviteCodes";
        check(nsid, s.xrpc.get(nsid, &[], &auth).await);
        let nsid = "com.atproto.repo.uploadBlob";
        check(nsid, s.xrpc.post_bytes(nsid, b"bytes".to_vec(), "image/png", &auth).await);
        let nsid = "com.atproto.repo.importRepo";
        check(nsid, s.xrpc.post_bytes(nsid, car.clone(), "application/vnd.ipld.car", &auth).await);
    }
    assert!(wrong.is_empty(), "not refused for the takedown:\n{}", wrong.join("\n"));

    // routes without the check still accept the token (reference: getSession
    // reports the status; getServiceAuth for createAccount, to migrate away)
    let j = s.xrpc.get("com.atproto.server.getSession", &[], &a.auth()).await.ok();
    assert_eq!((j["active"].clone(), j["status"].clone()), (json!(false), json!("takendown")), "{j}");
    s.xrpc.get("com.atproto.server.getServiceAuth", &[("aud", "did:web:elsewhere.test"), ("lxm", "com.atproto.server.createAccount")], &a.auth()).await.ok();
    // refresh tokens are gone
    s.xrpc.post_empty("com.atproto.server.refreshSession", &a.refresh_auth()).await.client_err();

    // the checks follow the account's status: lifting the takedown makes the
    // same (unexpired, unrevoked) access token work again
    set_repo_takedown(&s, &a.did, false).await;
    s.post(&a, "after").await;
    // (the preferences route reads a status cached for up to 2 s)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let r = s.xrpc.post("app.bsky.actor.putPreferences", &json!({"preferences": []}), &a.auth()).await;
        if r.status == 200 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "putPreferences after the takedown was lifted: {}", r.text());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
