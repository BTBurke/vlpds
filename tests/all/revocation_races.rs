//! Revocations racing the read-modify-writes they must stop (src/xrpc/cas.rs):
//! an OAuth refresh or code exchange, a legacy refreshSession or
//! createSession held mid-flight (a pause hook right before its write)
//! while a password change or takedown revokes the account's sessions from
//! another node must fail and leave no session behind; a TOTP code or
//! wrong-code count written concurrently on two nodes is neither accepted
//! twice nor lost. Two in-process nodes on one in-memory object store.

use crate::common::*;
use crate::ha_auth::{as_post, balanced, csrf_of, node, owner_of, Browser, Client, PUBLIC};
use std::sync::Arc;

/// Holds requests of `did` reaching pause point `point` until released.
struct Gate {
    did: String,
    reached: tokio::sync::mpsc::UnboundedReceiver<()>,
    open: Arc<tokio::sync::Semaphore>,
}

impl Gate {
    fn new(did: &str, point: &'static str) -> Gate {
        let (tx, reached) = tokio::sync::mpsc::unbounded_channel();
        let open = Arc::new(tokio::sync::Semaphore::new(0));
        let o = open.clone();
        vlpds::xrpc::cas::set_pause_hook(
            did,
            Some(Arc::new(move |p: &str| {
                let (hit, tx, o) = (p == point, tx.clone(), o.clone());
                Box::pin(async move {
                    if hit {
                        let _ = tx.send(());
                        o.acquire().await.expect("gate").forget();
                    }
                })
            })),
        );
        Gate { did: did.to_string(), reached, open }
    }

    async fn reached(&mut self) {
        tokio::time::timeout(std::time::Duration::from_secs(20), self.reached.recv()).await.expect("request never reached the pause point");
    }

    /// Lets the held request (and any retry of it) through.
    fn release(&self) {
        vlpds::xrpc::cas::set_pause_hook(&self.did, None);
        self.open.add_permits(1_000);
    }
}

/// An admin password change (to the same password, so the tests' logins
/// keep working: the revocation is what matters).
async fn change_password(s: &TestServer, did: &str) {
    s.xrpc
        .post("com.atproto.admin.updateAccountPassword", &json!({"did": did, "password": PASSWORD}), &Auth::Admin)
        .await
        .ok();
}

async fn take_down(s: &TestServer, did: &str) {
    let body = json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did}, "takedown": {"applied": true, "ref": "race"}});
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::Admin).await.ok();
}

async fn oauth_sessions(s: &TestServer, did: &str) -> usize {
    vlpds::oauth::store::list_sessions(&s.app, did).await.expect("list sessions").len()
}

async fn legacy_sessions(s: &TestServer, did: &str) -> usize {
    vlpds::xrpc::internal::scan_private_anywhere(&s.app, did, "sess/").await.map_err(|e| e.message).expect("scan").len()
}

/// Two nodes; accounts are created on `a` (so `a` owns them) and the
/// revocations are sent to `b` (forwarded to the owner).
async fn cluster(tag: &str) -> (TestServer, TestServer) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node(&format!("{tag}-a"), &store, Some(PUBLIC)).await;
    let b = node(&format!("{tag}-b"), &store, Some(PUBLIC)).await;
    balanced(&[&a, &b]).await;
    (a, b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_refresh_racing_a_revocation_does_not_resurrect_the_session() {
    let (a, b) = cluster("rr-refresh").await;
    for takedown in [false, true] {
        let acct = a.create_account("rrr").await;
        assert!(std::ptr::eq(owner_of(&[&a, &b], &acct.did), &a));
        let client = Client::new();
        let mut browser = Browser::default();
        let (code, verifier) = client.authorize(&mut browser, [&a, &b, &a, &b], &acct.handle, &acct.did).await;
        let (st, t) = client.exchange(&b, &code, &verifier).await;
        assert_eq!(st, 200, "{t}");
        let access = t["access_token"].as_str().unwrap().to_string();
        let rt = t["refresh_token"].as_str().unwrap().to_string();
        assert_eq!(oauth_sessions(&b, &acct.did).await, 1);

        // the refresh passes every check, then waits right before its write
        // while the account's sessions are revoked from the other node
        let mut gate = Gate::new(&acct.did, "oauth_refresh");
        let ((st, j), ()) = tokio::join!(client.refresh(&b, &rt), async {
            gate.reached().await;
            if takedown {
                take_down(&b, &acct.did).await;
            } else {
                change_password(&b, &acct.did).await;
            }
            assert_eq!(oauth_sessions(&b, &acct.did).await, 0, "revoked");
            gate.release();
        });
        assert_eq!((st, j["error"].as_str()), (400, Some("invalid_grant")), "refresh raced a revocation (takedown {takedown}): {j}");
        assert_eq!(oauth_sessions(&a, &acct.did).await, 0, "no session survives (takedown {takedown})");
        let (st, _) = client.refresh(&a, &rt).await;
        assert_eq!(st, 400);
        let (st, _) = client.create_post(&a, &access, &acct.did).await;
        assert_eq!(st, 401, "access token of the revoked session");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_code_exchange_racing_a_password_change_fails() {
    let (a, b) = cluster("rr-code").await;
    let acct = a.create_account("rrc").await;
    let client = Client::new();
    let mut browser = Browser::default();
    let (code, verifier) = client.authorize(&mut browser, [&a, &b, &a, &b], &acct.handle, &acct.did).await;
    let mut gate = Gate::new(&acct.did, "oauth_code");
    let ((st, j), ()) = tokio::join!(client.exchange(&b, &code, &verifier), async {
        gate.reached().await;
        change_password(&b, &acct.did).await;
        gate.release();
    });
    assert_eq!((st, j["error"].as_str()), (400, Some("invalid_grant")), "{j}");
    assert_eq!(oauth_sessions(&a, &acct.did).await, 0);

    // a code approved before the change (the exchange comes after it) is void too
    let client = Client::new();
    let mut browser = Browser::default();
    let (code, verifier) = client.authorize(&mut browser, [&b, &a, &b, &a], &acct.handle, &acct.did).await;
    change_password(&b, &acct.did).await;
    let (st, j) = client.exchange(&a, &code, &verifier).await;
    assert_eq!((st, j["error"].as_str()), (400, Some("invalid_grant")), "{j}");
    assert_eq!(oauth_sessions(&a, &acct.did).await, 0);

    // and the device signed in before it no longer counts as signed in:
    // approving needs the password again
    let enc = vlpds::oauth::util::form_encode_component;
    let challenge = vlpds::oauth::util::sha256_b64u("x".repeat(43));
    let (st, j) = as_post(
        &a,
        &client.key,
        "/oauth/par",
        &[
            ("client_id", &client.id),
            ("response_type", "code"),
            ("redirect_uri", "http://127.0.0.1/callback"),
            ("scope", "atproto transition:generic"),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
        ],
        None,
    )
    .await;
    assert_eq!(st, 201, "{j}");
    let request_uri = j["request_uri"].as_str().unwrap().to_string();
    let (st, _, html) = browser.get(&b, &format!("/oauth/authorize?client_id={}&request_uri={}", enc(&client.id), enc(&request_uri))).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("name=\"password\""), "the stale device login is not offered: {html}");
    let (st, h, html) = browser
        .post(&b, "/oauth/authorize/consent", &[("request_uri", &request_uri), ("csrf", &csrf_of(&html)), ("did", &acct.did), ("action", "allow")])
        .await;
    assert_eq!(st, 401, "consent with a login from before the password change: {h:?} {html}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_refresh_and_login_racing_a_password_change_fail() {
    let (a, b) = cluster("rr-legacy").await;
    let acct = a.create_account("rrl").await;
    assert_eq!(legacy_sessions(&b, &acct.did).await, 1);

    // refreshSession held right before its rotation write
    let mut gate = Gate::new(&acct.did, "legacy_refresh");
    let refresh_auth = acct.refresh_auth();
    let (r, ()) = tokio::join!(b.xrpc.post_empty("com.atproto.server.refreshSession", &refresh_auth), async {
        gate.reached().await;
        change_password(&b, &acct.did).await;
        gate.release();
    });
    r.err(400, "ExpiredToken");
    assert_eq!(legacy_sessions(&a, &acct.did).await, 0, "no refresh row survives");
    b.xrpc.post_empty("com.atproto.server.refreshSession", &acct.refresh_auth()).await.err(400, "ExpiredToken");

    // createSession checked the old password, then the password changed
    // before its session was written
    let mut gate = Gate::new(&acct.did, "legacy_login");
    let (r, ()) = tokio::join!(b.create_session(&acct.handle, PASSWORD), async {
        gate.reached().await;
        b.xrpc
            .post("com.atproto.admin.updateAccountPassword", &json!({"did": acct.did, "password": "the-third-password"}), &Auth::Admin)
            .await
            .ok();
        gate.release();
    });
    r.err_status(401);
    assert_eq!(legacy_sessions(&a, &acct.did).await, 0, "no session from the old password");
    b.create_session(&acct.handle, "the-third-password").await.ok();
}

fn totp_code(secret: &[u8], step: u64) -> String {
    vlpds::totp::code_for_step(secret, step)
}

/// Enables TOTP for `acct` (on `s`); returns the secret and the step used.
async fn enable_totp(s: &TestServer, acct: &TestAccount) -> (Vec<u8>, u64) {
    let j = s.xrpc.post_empty("vlpds.server.setupTotp", &acct.auth()).await.ok();
    let secret = vlpds::totp::base32_decode(j["secret"].as_str().unwrap()).unwrap();
    let step = vlpds::totp::step_at(vlpds::totp::now_secs());
    s.xrpc.post("vlpds.server.confirmTotp", &json!({"code": totp_code(&secret, step)}), &acct.auth()).await.ok();
    (secret, step)
}

/// What another node does with a code (the same steps as a login there):
/// attempt on the state it reads, written only if unchanged.
async fn attempt_elsewhere(s: &TestServer, did: &str, code: &str) -> Result<(), String> {
    loop {
        let (mut st, raw) = vlpds::totp::load_raw(&s.app, did).await.map_err(|e| e.message).unwrap();
        let r = vlpds::totp::attempt(&mut st, code, vlpds::totp::now_secs());
        if vlpds::totp::save_if(&s.app, did, &st, raw).await.map_err(|e| e.message).unwrap() {
            return r.map_err(|e| e.error);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totp_codes_and_failures_are_counted_once_across_nodes() {
    let (a, b) = cluster("rr-totp").await;
    let acct = a.create_account("rrt").await;
    let (secret, step) = enable_totp(&a, &acct).await;
    let login = |code: String| {
        let body = json!({"identifier": acct.handle, "password": acct.password, "authFactorToken": code});
        let a = &a;
        async move { a.xrpc.post("com.atproto.server.createSession", &body, &Auth::None).await }
    };

    // one code, accepted on two nodes at once: once
    let code = totp_code(&secret, step + 1);
    let mut gate = Gate::new(&acct.did, "totp");
    let (r, other) = tokio::join!(login(code.clone()), async {
        gate.reached().await;
        // the login on node a has verified the code; node b accepts it first
        let other = attempt_elsewhere(&b, &acct.did, &code).await;
        gate.release();
        other
    });
    assert_eq!(other, Ok(()), "node b accepted the code");
    assert!(!r.is_ok(), "the same code accepted twice: {}", r.text());

    // wrong codes on two nodes at once: every one counts toward the lockout
    let mut gate = Gate::new(&acct.did, "totp");
    let (r, ()) = tokio::join!(login("000000".into()), async {
        gate.reached().await;
        for _ in 0..3 {
            assert_eq!(attempt_elsewhere(&b, &acct.did, "111111").await, Err("InvalidToken".to_string()));
        }
        gate.release();
    });
    let st = vlpds::totp::load(&b.app, &acct.did).await.map_err(|e| e.message).unwrap();
    // 1 wrong code on the first round above, + 3 elsewhere + 1 here
    assert_eq!(st.failures, 5, "no failure lost");
    assert!(st.locked_until > vlpds::totp::now_secs(), "five wrong codes lock the factor");
    r.err(429, "RateLimitExceeded");
}
