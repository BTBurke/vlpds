//! Port of atproto/packages/pds/tests/account.test.ts: account creation,
//! handle rules, uniqueness, login, password reset and admin account updates.
use crate::common::*;
use std::collections::HashSet;

async fn get_session(s: &TestServer, tok: &str) -> Resp {
    s.xrpc
        .get(
            "com.atproto.server.getSession",
            &[],
            &Auth::Bearer(tok.into()),
        )
        .await
}

/// The reference PDS answers a taken handle with InvalidRequest "Handle already
/// taken"; the lexicon names HandleNotAvailable. Accept either.
#[track_caller]
fn handle_taken(r: &Resp) {
    assert_eq!(r.status, 400, "{}", r.text());
    let ok = r.error_name() == Some("HandleNotAvailable")
        || (r.error_name() == Some("InvalidRequest") && r.text().to_lowercase().contains("taken"));
    assert!(ok, "expected handle-taken error, got {}", r.text());
}

async fn try_handle(s: &TestServer, handle: &str) -> Resp {
    let email = format!("{}@example.com", unique_name("th"));
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": handle, "email": email, "password": "asdf"}),
            &Auth::None,
        )
        .await
}

/// All emailed tokens currently visible for `email`.
async fn all_tokens(s: &TestServer, email: &str) -> HashSet<String> {
    let r = s.dev_mail(email).await;
    let mut out = HashSet::new();
    collect_tokens(&r.json, &mut out);
    out
}

fn collect_tokens(j: &J, out: &mut HashSet<String>) {
    match j {
        J::Object(o) => {
            for (k, v) in o {
                if (k == "token" || k == "code") && v.is_string() {
                    out.insert(v.as_str().unwrap().to_string());
                } else {
                    collect_tokens(v, out);
                }
            }
        }
        J::Array(a) => a.iter().for_each(|v| collect_tokens(v, out)),
        _ => {}
    }
}

/// Runs `f` (which should trigger an email) and returns the newly mailed token.
async fn new_token<F: std::future::Future<Output = Resp>>(
    s: &TestServer,
    email: &str,
    f: F,
) -> String {
    let before = all_tokens(s, email).await;
    f.await.ok();
    let after = all_tokens(s, email).await;
    let mut fresh: Vec<_> = after.difference(&before).cloned().collect();
    if fresh.is_empty() {
        // fall back to the harness heuristic (latest token)
        return s
            .mail_token(email)
            .await
            .expect("no emailed token found via vlpds.admin.getDevMail");
    }
    assert_eq!(
        fresh.len(),
        1,
        "expected exactly one new token, got {fresh:?}"
    );
    fresh.pop().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_the_accounts_system_config() {
    let s = TestServer::spawn().await;
    let j = s
        .xrpc
        .get("com.atproto.server.describeServer", &[], &Auth::None)
        .await
        .ok();
    assert_eq!(j["inviteCodeRequired"], json!(false));
    assert_eq!(
        j["availableUserDomains"][0],
        json!(format!(".{HANDLE_DOMAIN}"))
    );
    assert!(j["did"].as_str().unwrap().starts_with("did:"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_an_account_with_plc_shaped_did_and_did_doc() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    assert!(a.did.starts_with("did:plc:"), "{}", a.did);
    let id = &a.did["did:plc:".len()..];
    assert_eq!(
        id.len(),
        24,
        "did:plc identifier must be 24 chars: {}",
        a.did
    );
    assert!(
        id.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7')),
        "{}",
        a.did
    );
    assert!(!a.access.is_empty() && !a.refresh.is_empty());

    // DID document via describeRepo: handle, PDS endpoint and the repo signing key.
    let j = s
        .xrpc
        .get(
            "com.atproto.repo.describeRepo",
            &[("repo", &a.did)],
            &Auth::None,
        )
        .await
        .ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["handle"], json!(a.handle));
    let doc = &j["didDoc"];
    assert_eq!(doc["id"], json!(a.did));
    assert!(doc["alsoKnownAs"]
        .as_array()
        .unwrap()
        .contains(&json!(format!("at://{}", a.handle))));
    let svc = doc["service"].as_array().unwrap();
    assert!(svc.iter().any(|x| x["id"]
        .as_str()
        .map(|i| i.ends_with("#atproto_pds"))
        .unwrap_or(false)
        && x["serviceEndpoint"] == json!(s.url)));
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
        assert!(
            matches!(r.error_name(), Some("InvalidHandle" | "InvalidRequest")),
            "handle {h:?}: {}",
            r.text()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_improperly_formatted_handles() {
    let s = TestServer::spawn().await;
    // too short / too long first label on the service domain
    try_handle(&s, &format!("j.{HANDLE_DOMAIN}"))
        .await
        .err(400, "InvalidHandle");
    try_handle(&s, &format!("jayromy-johnber12345678910.{HANDLE_DOMAIN}"))
        .await
        .err(400, "InvalidHandle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_unsupported_domains() {
    let s = TestServer::spawn().await;
    try_handle(&s, "john.bsky.io")
        .await
        .err(400, "UnsupportedDomain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_reserved_handles() {
    let s = TestServer::spawn().await;
    try_handle(&s, &format!("about.{HANDLE_DOMAIN}"))
        .await
        .err(400, "HandleNotAvailable");
    try_handle(&s, &format!("atp.{HANDLE_DOMAIN}"))
        .await
        .err(400, "HandleNotAvailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_duplicate_email_addresses_and_handles() {
    let s = TestServer::spawn().await;
    let name = unique_name("bob");
    let handle = format!("{name}.{HANDLE_DOMAIN}");
    let email = format!("{name}@test.com");
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": handle, "email": email, "password": "test123"}),
            &Auth::None,
        )
        .await
        .ok();
    // same email, different case
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": format!("{}.{HANDLE_DOMAIN}", unique_name("carol")), "email": email.to_uppercase(), "password": "test123"}),
            &Auth::None,
        )
        .await;
    r.client_err();
    assert!(r.text().to_lowercase().contains("email"), "{}", r.text());
    // same handle, different case
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": handle.to_uppercase(), "email": format!("{}@test.com", unique_name("carol")), "password": "test123"}),
            &Auth::None,
        )
        .await;
    handle_taken(&r);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disallows_the_email_and_handle_of_a_deactivated_account() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dan").await;
    s.xrpc
        .post(
            "com.atproto.server.deactivateAccount",
            &json!({}),
            &a.auth(),
        )
        .await
        .ok();
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": a.handle, "email": format!("{}@test.com", unique_name("erin")), "password": "x"}),
            &Auth::None,
        )
        .await;
    handle_taken(&r);
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": format!("{}.{HANDLE_DOMAIN}", unique_name("erin")), "email": a.email, "password": "x"}),
            &Auth::None,
        )
        .await;
    r.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn handles_racing_signups_for_same_handle() {
    let s = std::sync::Arc::new(TestServer::spawn().await);
    let handle = format!("{}.{HANDLE_DOMAIN}", unique_name("match"));
    let mut tasks = Vec::new();
    for i in 0..10 {
        let s = s.clone();
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            s.xrpc
                .post(
                    "com.atproto.server.createAccount",
                    &json!({"handle": handle, "email": format!("matching{i}@test.com"), "password": "password"}),
                    &Auth::None,
                )
                .await
                .is_ok()
        }));
    }
    let mut ok = 0;
    for t in tasks {
        if t.await.unwrap() {
            ok += 1;
        }
    }
    assert_eq!(ok, 1, "exactly one racing signup must win");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn login_and_authenticated_requests() {
    let s = TestServer::spawn().await;
    // unauthenticated
    s.xrpc
        .get("com.atproto.server.getSession", &[], &Auth::None)
        .await
        .err_status(401);

    let a = s.create_account("alice").await;
    for ident in [a.handle.clone(), a.did.clone(), a.handle.to_uppercase()] {
        let j = s.create_session(&ident, &a.password).await.ok();
        assert_eq!(j["did"], json!(a.did), "identifier {ident}");
        assert_eq!(j["handle"], json!(a.handle));
        assert_eq!(j["email"], json!(a.email));
        let sess = get_session(&s, j["accessJwt"].as_str().unwrap()).await.ok();
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

    let token = new_token(
        &s,
        &a.email,
        s.xrpc.post(
            "com.atproto.server.requestPasswordReset",
            &json!({"email": a.email}),
            &Auth::None,
        ),
    )
    .await;
    s.xrpc
        .post(
            "com.atproto.server.resetPassword",
            &json!({"token": token, "password": alt}),
            &Auth::None,
        )
        .await
        .ok();
    s.create_session(&a.handle, &a.password)
        .await
        .err(401, "AuthenticationRequired");
    s.create_session(&a.handle, alt).await.ok();

    // single use: reuse of the same token fails
    let r = s
        .xrpc
        .post(
            "com.atproto.server.resetPassword",
            &json!({"token": token, "password": a.password}),
            &Auth::None,
        )
        .await;
    assert_eq!(r.status, 400, "{}", r.text());
    assert!(
        matches!(r.error_name(), Some("InvalidToken" | "ExpiredToken")),
        "{}",
        r.text()
    );
    s.create_session(&a.handle, alt).await.ok();

    // reset back (token is case-insensitive) and it revokes existing refresh tokens
    let sess = s.create_session(&a.handle, alt).await.ok();
    let token = new_token(
        &s,
        &a.email,
        s.xrpc.post(
            "com.atproto.server.requestPasswordReset",
            &json!({"email": a.email}),
            &Auth::None,
        ),
    )
    .await;
    s.xrpc
        .post(
            "com.atproto.server.resetPassword",
            &json!({"token": token.to_lowercase(), "password": a.password}),
            &Auth::None,
        )
        .await
        .ok();
    s.create_session(&a.handle, alt)
        .await
        .err(401, "AuthenticationRequired");
    s.create_session(&a.handle, &a.password).await.ok();
    let r = s
        .xrpc
        .post_empty(
            "com.atproto.server.refreshSession",
            &Auth::Bearer(sess["refreshJwt"].as_str().unwrap().into()),
        )
        .await;
    assert_eq!(
        r.status,
        400,
        "refresh with pre-reset token must fail: {}",
        r.text()
    );
    assert!(
        matches!(r.error_name(), Some("ExpiredToken" | "InvalidToken")),
        "{}",
        r.text()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_bogus_password_reset_token() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let r = s
        .xrpc
        .post(
            "com.atproto.server.resetPassword",
            &json!({"token": "AAAAA-AAAAA", "password": "x"}),
            &Auth::None,
        )
        .await;
    r.err(400, "InvalidToken");
    s.create_session(&a.handle, &a.password).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allows_an_admin_to_update_password() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.xrpc
        .post(
            "com.atproto.admin.updateAccountPassword",
            &json!({"did": a.did, "password": "new-admin-pass"}),
            &Auth::None,
        )
        .await
        .err_status(401);
    s.xrpc
        .post(
            "com.atproto.admin.updateAccountPassword",
            &json!({"did": a.did, "password": "new-admin-pass"}),
            &a.auth(),
        )
        .await
        .client_err();
    s.xrpc
        .post(
            "com.atproto.admin.updateAccountPassword",
            &json!({"did": a.did, "password": "new-admin-password"}),
            &Auth::Admin,
        )
        .await
        .ok();
    s.create_session(&a.did, &a.password)
        .await
        .err(401, "AuthenticationRequired");
    s.create_session(&a.did, "new-admin-password").await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allows_administrative_email_updates() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.xrpc
        .post(
            "com.atproto.admin.updateAccountEmail",
            &json!({"account": a.handle, "email": "alIce-NEw@teST.com"}),
            &Auth::Admin,
        )
        .await
        .ok();
    let info = s
        .xrpc
        .get(
            "com.atproto.admin.getAccountInfo",
            &[("did", &a.did)],
            &Auth::Admin,
        )
        .await
        .ok();
    assert_eq!(info["email"], json!("alice-new@test.com"));
    assert_eq!(info["did"], json!(a.did));
    assert_eq!(info["handle"], json!(a.handle));

    s.xrpc
        .post(
            "com.atproto.admin.updateAccountEmail",
            &json!({"account": a.did, "email": a.email}),
            &Auth::Admin,
        )
        .await
        .ok();
    let info = s
        .xrpc
        .get(
            "com.atproto.admin.getAccountInfo",
            &[("did", &a.did)],
            &Auth::Admin,
        )
        .await
        .ok();
    assert_eq!(info["email"], json!(a.email));
    // session reflects it too
    let sess = get_session(&s, &a.access).await.ok();
    assert_eq!(sess["email"], json!(a.email));
    // non-admin is rejected
    s.xrpc
        .post(
            "com.atproto.admin.updateAccountEmail",
            &json!({"account": a.did, "email": "x@test.com"}),
            &a.auth(),
        )
        .await
        .client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_account_emits_identity_and_account_events() {
    let s = TestServer::spawn().await;
    let mut sub = s.subscribe(None).await;
    let a = s.create_account("fh").await;
    let did = a.did.clone();
    let frames = sub
        .until(FH_TIMEOUT, move |fs| {
            ["#identity", "#account"].iter().all(|k| {
                fs.iter()
                    .any(|f| f.did() == Some(did.as_str()) && f.kind() == *k)
            })
        })
        .await;
    let mine: Vec<_> = frames
        .iter()
        .filter(|f| f.did() == Some(a.did.as_str()))
        .collect();
    let ident = mine
        .iter()
        .find(|f| f.kind() == "#identity")
        .expect("#identity event for new account");
    assert_eq!(ident.str("handle"), Some(a.handle.as_str()));
    let acct = mine.iter().find(|f| f.kind() == "#account").unwrap();
    assert_eq!(acct.bool("active"), Some(true));
    assert!(acct.str("status").is_none());
}
