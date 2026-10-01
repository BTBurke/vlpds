//! Port of atproto/packages/pds/tests/preferences.test.ts:
//! app.bsky.actor.getPreferences / putPreferences, namespace rules, and the
//! app-password restrictions on personalDetailsPref / declaredAgePref.
mod common;
use common::*;

async fn get_prefs(s: &TestServer, auth: &Auth) -> Resp {
    s.xrpc.get("app.bsky.actor.getPreferences", &[], auth).await
}

async fn put_prefs(s: &TestServer, auth: &Auth, prefs: J) -> Resp {
    s.xrpc
        .post(
            "app.bsky.actor.putPreferences",
            &json!({"preferences": prefs}),
            auth,
        )
        .await
}

/// Creates an app password and logs in with it.
async fn app_password_auth(s: &TestServer, a: &TestAccount) -> Auth {
    let ap = s
        .xrpc
        .post(
            "com.atproto.server.createAppPassword",
            &json!({"name": "test-app-pass"}),
            &a.auth(),
        )
        .await
        .ok();
    let sess = s
        .create_session(&a.handle, ap["password"].as_str().unwrap())
        .await
        .ok();
    Auth::Bearer(sess["accessJwt"].as_str().unwrap().to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requires_auth() {
    let s = TestServer::spawn().await;
    put_prefs(
        &s,
        &Auth::None,
        json!([{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false}]),
    )
    .await
    .err(401, "AuthenticationRequired");
    get_prefs(&s, &Auth::None)
        .await
        .err(401, "AuthenticationRequired");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_update_and_clear() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    assert_eq!(
        get_prefs(&s, &a.auth()).await.ok(),
        json!({"preferences": []})
    );

    let prefs = json!([
        {"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false},
        {"$type": "app.bsky.actor.defs#contentLabelPref", "label": "dogs", "visibility": "show"},
        {"$type": "app.bsky.actor.defs#contentLabelPref", "label": "cats", "visibility": "warn"},
    ]);
    put_prefs(&s, &a.auth(), prefs.clone()).await.ok();
    assert_eq!(
        get_prefs(&s, &a.auth()).await.ok(),
        json!({"preferences": prefs})
    );

    let prefs = json!([
        {"$type": "app.bsky.actor.defs#adultContentPref", "enabled": true},
        {"$type": "app.bsky.actor.defs#contentLabelPref", "label": "dogs", "visibility": "warn"},
    ]);
    put_prefs(&s, &a.auth(), prefs.clone()).await.ok();
    assert_eq!(
        get_prefs(&s, &a.auth()).await.ok(),
        json!({"preferences": prefs})
    );

    put_prefs(&s, &a.auth(), json!([])).await.ok();
    assert_eq!(
        get_prefs(&s, &a.auth()).await.ok(),
        json!({"preferences": []})
    );

    // preferences are per account
    let b = s.create_account("bob").await;
    put_prefs(
        &s,
        &a.auth(),
        json!([{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": true}]),
    )
    .await
    .ok();
    assert_eq!(
        get_prefs(&s, &b.auth()).await.ok(),
        json!({"preferences": []})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_outside_namespace() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let r = put_prefs(
        &s,
        &a.auth(),
        json!([
            {"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false},
            {"$type": "com.atproto.server.defs#unknown", "hello": "world"},
        ]),
    )
    .await;
    r.err(400, "InvalidRequest");
    assert_eq!(
        get_prefs(&s, &a.auth()).await.ok(),
        json!({"preferences": []}),
        "failed put must not change prefs"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_without_type() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let r = put_prefs(
        &s,
        &a.auth(),
        json!([
            {"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false},
            {"label": "dogs", "visibility": "warn"},
        ]),
    )
    .await;
    r.err(400, "InvalidRequest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_password_cannot_read_write_or_remove_personal_details() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let ap = app_password_auth(&s, &a).await;
    let birth = "2020-06-01T00:00:00.000Z"; // a minor
    put_prefs(
        &s,
        &a.auth(),
        json!([{"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": birth}]),
    )
    .await
    .ok();

    // read with app password: personal details hidden, declared age computed
    let got = get_prefs(&s, &ap).await.ok();
    assert_eq!(
        got["preferences"],
        json!([{"$type": "app.bsky.actor.defs#declaredAgePref", "isOverAge13": false, "isOverAge16": false, "isOverAge18": false}])
    );

    // write with app password: rejected
    let r = put_prefs(
        &s,
        &ap,
        json!([{"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": now_iso()}]),
    )
    .await;
    r.client_err();

    // clearing with an app password keeps the permissioned pref
    put_prefs(&s, &ap, json!([])).await.ok();
    let full = get_prefs(&s, &a.auth()).await.ok();
    assert!(
        full["preferences"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["$type"] == json!("app.bsky.actor.defs#personalDetailsPref")),
        "app password must not remove personalDetailsPref: {full}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_age_pref_is_computed_and_not_settable() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let ap = app_password_auth(&s, &a).await;
    let birth = "1970-01-01T00:00:00.000Z";
    let over = json!({"$type": "app.bsky.actor.defs#declaredAgePref", "isOverAge13": true, "isOverAge16": true, "isOverAge18": true});
    put_prefs(
        &s,
        &a.auth(),
        json!([{"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": birth}]),
    )
    .await
    .ok();
    for auth in [&a.auth(), &ap] {
        let got = get_prefs(&s, auth).await.ok();
        assert!(
            got["preferences"].as_array().unwrap().contains(&over),
            "declaredAgePref missing: {got}"
        );
    }
    // the user cannot set declaredAgePref themselves
    put_prefs(
        &s,
        &a.auth(),
        json!([
            {"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": birth},
            {"$type": "app.bsky.actor.defs#declaredAgePref", "isOverAge13": false, "isOverAge16": false, "isOverAge18": false},
        ]),
    )
    .await
    .ok();
    let got = get_prefs(&s, &a.auth()).await.ok();
    assert_eq!(
        got["preferences"],
        json!([{"$type": "app.bsky.actor.defs#personalDetailsPref", "birthDate": birth}, over])
    );
}
