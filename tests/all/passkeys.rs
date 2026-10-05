//! Passkeys (src/xrpc/passkeys.rs, src/webauthn.rs) driven by a software
//! authenticator (common/webauthn.rs): registration on the Security page,
//! the account page's passkey sign-in, removal ending what a passkey signed
//! in, recovery codes, the operator's reset, and how passkeys meet trusted
//! browsers, alerts and the sign-in log. The OAuth page's passkey steps are
//! in oauth.rs.

use crate::common::webauthn::*;
use crate::common::*;

fn count(v: &prometheus::IntCounterVec, labels: &[&str]) -> u64 {
    v.with_label_values(labels).get()
}

async fn list(s: &TestServer, a: &TestAccount) -> J {
    s.xrpc.get("vlpds.server.listPasskeys", &[], &a.auth()).await.ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_list_rename_remove() {
    let registered = count(&vlpds::metrics::PASSKEYS, &["registered"]);
    let s = TestServer::spawn().await;
    let a = s.create_account("pkreg").await;
    let l = list(&s, &a).await;
    assert_eq!(l["passkeys"], json!([]));
    assert_eq!(l["origin"], json!(s.url));
    assert_eq!(l["passwordlessAvailable"], json!(true));

    // the password is checked before any challenge is handed out
    s.xrpc
        .post("vlpds.server.startPasskeyRegistration", &json!({"password": "wrong"}), &a.auth())
        .await
        .err(401, "AuthenticationRequired");
    let opts =
        s.xrpc.post("vlpds.server.startPasskeyRegistration", &json!({"password": a.password}), &a.auth()).await.ok();
    assert_eq!(opts["user"]["id"], json!(b64u(&a.did)), "the user handle is the DID");
    assert_eq!(opts["attestation"], json!("none"));
    assert_eq!(
        opts["pubKeyCredParams"],
        json!([{"type": "public-key", "alg": -7}, {"type": "public-key", "alg": -8}, {"type": "public-key", "alg": -257}])
    );
    let mut k = SoftKey::new(&s.url);
    let cred = k.register(&opts, &Lie::default());
    let done = s
        .xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "Laptop", "credential": cred}), &a.auth())
        .await
        .ok();
    assert_eq!(done["name"], json!("Laptop"));
    // the same challenge again is refused
    s.xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "Again", "credential": cred}), &a.auth())
        .await
        .err(400, "PasskeyRefused");
    let l = list(&s, &a).await;
    let pk = &l["passkeys"][0];
    assert_eq!((pk["name"].as_str(), pk["passwordless"].as_bool()), (Some("Laptop"), Some(true)));
    assert!(count(&vlpds::metrics::PASSKEYS, &["registered"]) > registered);

    // the second factor is now on, and the owner was mailed
    let sec = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    assert_eq!(sec["secondFactor"], json!(true));
    let mail = s.dev_mail(&a.email).await.ok();
    assert!(mail.to_string().contains("was added to your account"), "{mail}");

    s.xrpc.post("vlpds.server.renamePasskey", &json!({"id": k.id_b64(), "name": "Work laptop"}), &a.auth()).await.ok();
    assert_eq!(list(&s, &a).await["passkeys"][0]["name"], json!("Work laptop"));

    // app passwords and OAuth can't manage passkeys
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "bot"}), &a.auth()).await.ok();
    let ap_sess = s.create_session(&a.handle, ap["password"].as_str().unwrap()).await.ok();
    let ap_auth = Auth::Bearer(ap_sess["accessJwt"].as_str().unwrap().into());
    s.xrpc.get("vlpds.server.listPasskeys", &[], &ap_auth).await.err_status(400);
    s.xrpc
        .post("vlpds.server.startPasskeyRegistration", &json!({"password": a.password}), &ap_auth)
        .await
        .err_status(400);

    // removing needs the password
    let rm = json!({"id": k.id_b64(), "password": "wrong"});
    s.xrpc.post("vlpds.server.removePasskey", &rm, &a.auth()).await.err(401, "AuthenticationRequired");
    let rm = json!({"id": k.id_b64(), "password": a.password});
    s.xrpc.post("vlpds.server.removePasskey", &rm, &a.auth()).await.ok();
    assert_eq!(list(&s, &a).await["passkeys"], json!([]));
    let sec = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    assert_eq!(sec["secondFactor"], json!(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_ceremony_checks() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkcer").await;
    let start = || async {
        s.xrpc.post("vlpds.server.startPasskeyRegistration", &json!({"password": a.password}), &a.auth()).await.ok()
    };
    let lies = [
        Lie { origin: Some("https://evil.example".into()), ..Default::default() },
        Lie { rp_id: Some("evil.example".into()), ..Default::default() },
        Lie { ty: Some("webauthn.get"), ..Default::default() },
        Lie { no_up: true, ..Default::default() },
        Lie { cross_origin: true, ..Default::default() },
        Lie { challenge: Some(b64u(b"made up")), ..Default::default() },
    ];
    for l in lies {
        let opts = start().await;
        let cred = SoftKey::new(&s.url).register(&opts, &l);
        s.xrpc
            .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "x", "credential": cred}), &a.auth())
            .await
            .err(400, "PasskeyRefused");
    }
    // a challenge minted for another account
    let b = s.create_account("pkcer2").await;
    let opts =
        s.xrpc.post("vlpds.server.startPasskeyRegistration", &json!({"password": b.password}), &b.auth()).await.ok();
    let cred = SoftKey::new(&s.url).register(&opts, &Lie::default());
    s.xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "x", "credential": cred}), &a.auth())
        .await
        .err(400, "PasskeyRefused");
    assert_eq!(list(&s, &a).await["passkeys"], json!([]));
    // a key without user verification can still be a second factor
    let opts = start().await;
    let cred = SoftKey::new(&s.url).register(&opts, &Lie { no_uv: true, ..Default::default() });
    s.xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "no pin", "credential": cred}), &a.auth())
        .await
        .ok();
    assert_eq!(list(&s, &a).await["passkeys"][0]["passwordless"], json!(false));
    // oversized input is refused
    let opts = start().await;
    let mut cred = SoftKey::new(&s.url).register(&opts, &Lie::default());
    cred["response"]["attestationObject"] = json!("A".repeat(30_000));
    s.xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "x", "credential": cred}), &a.auth())
        .await
        .err(400, "PasskeyRefused");
}

// ---------------------------------------------------------------- the OAuth page

use crate::oauth as o;

/// A `data-*` attribute of the passkey form.
fn attr(html: &str, name: &str) -> String {
    let pat = format!("{name}=\"");
    let i = html.find(&pat).unwrap_or_else(|| panic!("no {name}: {html}")) + pat.len();
    html[i..i + html[i..].find('"').unwrap()].replace("&quot;", "\"").replace("&amp;", "&")
}

async fn oauth_register(s: &o::Srv, acct: &o::Account, key: &mut SoftKey) {
    let (st, opts) = s
        .bearer(&acct.jwt, "vlpds.server.startPasskeyRegistration", true, Some(json!({"password": o::PASSWORD})))
        .await;
    assert_eq!(st, 200, "{opts}");
    let cred = key.register(&opts, &Lie::default());
    let body = json!({"name": "key", "credential": cred});
    let (st, j) = s.bearer(&acct.jwt, "vlpds.server.finishPasskeyRegistration", true, Some(body)).await;
    assert_eq!(st, 200, "{j}");
}

/// Posts an assertion the way the page's script does.
async fn post_assertion(
    s: &o::Srv,
    b: &mut o::Browser,
    path: &str,
    hidden: &[(&str, &str)],
    step: &str,
    a: &J,
) -> o::Page {
    let mut pairs: Vec<(&str, &str)> = hidden.to_vec();
    let g = |k: &str| a[k].as_str().unwrap_or_default().to_string();
    let (id, cdj, ad, sig, uh) =
        (g("id"), g("clientDataJSON"), g("authenticatorData"), g("signature"), g("userHandle"));
    pairs.extend([
        ("step", step),
        ("action", "sign-in"),
        ("passkey_id", id.as_str()),
        ("client_data", cdj.as_str()),
        ("auth_data", ad.as_str()),
        ("signature", sig.as_str()),
        ("user_handle", uh.as_str()),
    ]);
    b.post(s, path, &pairs).await
}

/// A pushed request, its page, and the password step: (request_uri, csrf,
/// the page after the password).
async fn to_second_step(
    s: &o::Srv,
    b: &mut o::Browser,
    f: &o::Flow<'_>,
    acct: &o::Account,
) -> (String, String, String) {
    let ru = f.request_uri(s, &o::pkce(), "st").await;
    let (_, _, html) = b.authorize(s, f, &ru).await;
    let csrf = o::csrf_of(&html);
    let (st, _, html) = b.sign_in(s, &ru, &csrf, &acct.handle, o::PASSWORD).await;
    assert_eq!(st, 200, "{html}");
    (ru, csrf, html)
}

async fn dev_mail(s: &o::Srv, email: &str) -> String {
    s.http
        .get(format!("{}/xrpc/vlpds.admin.getDevMail?email={}", s.base, o::enc(email)))
        .basic_auth("admin", Some("dev-admin-token"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// The OAuth page's second-factor step with a passkey, through the code
/// exchange; refusals one check at a time; no emailed code in its place;
/// removing the passkey ends the session it signed in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_second_factor() {
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pk2fa").await;
    let mut key = SoftKey::new(&s.base);
    oauth_register(&s, &acct, &mut key).await;
    let dk = o::DpopKey::new();
    let f = o::Flow::loopback("atproto", &dk);
    let mut b = o::Browser::default();
    let p = o::pkce();
    let ru = f.request_uri(&s, &p, "st").await;
    let (_, _, html) = b.authorize(&s, &f, &ru).await;
    let csrf = o::csrf_of(&html);
    let (st, h, html) = b.sign_in(&s, &ru, &csrf, &acct.handle, o::PASSWORD).await;
    assert_eq!(st, 200, "{html}");
    // the passkey step: the script allowed by hash, no emailed code
    assert!(html.contains("Use your passkey") && html.contains("data-mode=\"2fa\""), "{html}");
    assert!(html.contains("Use a recovery code instead") && !html.contains("Sign-in code from your email"), "{html}");
    let csp = h.get("content-security-policy").unwrap().to_str().unwrap();
    assert!(csp.contains("script-src 'sha256-") && csp.contains("default-src 'none'"), "{csp}");
    assert!(!csp.contains("unsafe"), "{csp}");
    assert!(attr(&html, "data-allow").contains(&key.id_b64()), "allowCredentials lists the key");
    assert_eq!(attr(&html, "data-rp"), "127.0.0.1");
    let challenge = attr(&html, "data-challenge");
    let hidden = [("request_uri", ru.as_str()), ("csrf", csrf.as_str())];

    // each lie is refused with the same message, and the sign-in stays pending
    for lie in [
        Lie { origin: Some("https://phish.example".into()), ..Default::default() },
        Lie { rp_id: Some("phish.example".into()), ..Default::default() },
    ] {
        let a = key.assert(&challenge, &lie);
        let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await;
        assert_eq!(st, 401, "{html}");
        assert!(html.contains("Passkey not recognized"), "{html}");
    }
    // the good one; no user verification needed for a second factor
    let a = key.assert(&challenge, &Lie { no_uv: true, ..Default::default() });
    let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "{html}");
    let csrf2 = o::csrf_of(&html);
    let consent = [("request_uri", ru.as_str()), ("csrf", &csrf2), ("did", &acct.did), ("action", "allow")];
    let (st, h, _) = b.post(&s, "/oauth/authorize/consent", &consent).await;
    assert_eq!(st, 303);
    let (_, q) = o::location_params(&h);
    let t = o::tokens(&o::exchange(&s, &f, &q["code"], &p, &[]).await);

    // the log names the factor
    let (_, j) = s.bearer(&acct.jwt, "vlpds.server.getSignInSecurity", false, None).await;
    assert_eq!(j["recentSignIns"][0]["factor"], json!("passkey"), "{j}");

    // that assertion again, in a new flow: its challenge was for the old one
    let (ru2, csrf, _) = to_second_step(&s, &mut b, &f, &acct).await;
    let hidden = [("request_uri", ru2.as_str()), ("csrf", csrf.as_str())];
    let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await;
    assert_eq!(st, 401, "{html}");

    // removing the passkey ends the OAuth session it signed in
    let r = o::xrpc_dpop(&s, &dk, &t.access, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let rm = json!({"id": key.id_b64(), "password": o::PASSWORD});
    let (st, j) = s.bearer(&acct.jwt, "vlpds.server.removePasskey", true, Some(rm)).await;
    assert_eq!(st, 200, "{j}");
    let r = o::xrpc_dpop(&s, &dk, &t.access, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.status, 401, "{}", r.body);
}

/// The same assertion twice in the flow and browser it was minted for: the
/// first use claimed its challenge, so the second is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn challenge_replay_is_refused() {
    let replays = count(&vlpds::metrics::PASSKEY_FAILURES, &["replay"]);
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pkrep").await;
    let mut key = SoftKey::synced(&s.base);
    oauth_register(&s, &acct, &mut key).await;
    let dk = o::DpopKey::new();
    let f = o::Flow::loopback("atproto", &dk);
    let mut b = o::Browser::default();
    let (ru, csrf, html) = to_second_step(&s, &mut b, &f, &acct).await;
    let a = key.assert(&attr(&html, "data-challenge"), &Lie::default());
    let hidden = [("request_uri", ru.as_str()), ("csrf", csrf.as_str())];
    let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "{html}");
    // the password again on the same request, then the same assertion
    let (st, _, html) = b.sign_in(&s, &ru, &csrf, &acct.handle, o::PASSWORD).await;
    assert!(st == 200 && html.contains("Use your passkey"), "{html}");
    let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await;
    assert_eq!(st, 401, "{html}");
    assert!(html.contains("Passkey not recognized"), "{html}");
    assert!(count(&vlpds::metrics::PASSKEY_FAILURES, &["replay"]) > replays);
}

/// A hardware key whose counter goes backwards is refused and flagged (and
/// its owner mailed); a synced passkey's copies are fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counter_regression() {
    let refused = count(&vlpds::metrics::PASSKEY_COUNTER_REGRESSIONS, &["refused"]);
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pkctr").await;
    let mut hw = SoftKey::new(&s.base);
    oauth_register(&s, &acct, &mut hw).await;
    let mut synced = SoftKey::synced(&s.base);
    oauth_register(&s, &acct, &mut synced).await;
    let dk = o::DpopKey::new();
    let f = o::Flow::loopback("atproto", &dk);
    async fn go(s: &o::Srv, f: &o::Flow<'_>, acct: &o::Account, key: &mut SoftKey, lie: Lie) -> u16 {
        let mut b = o::Browser::default();
        let (ru, csrf, html) = to_second_step(s, &mut b, f, acct).await;
        let a = key.assert(&attr(&html, "data-challenge"), &lie);
        let hidden = [("request_uri", ru.as_str()), ("csrf", csrf.as_str())];
        post_assertion(s, &mut b, "/oauth/authorize/sign-in", &hidden, "2fa", &a).await.0
    }
    assert_eq!(go(&s, &f, &acct, &mut hw, Lie::default()).await, 200);
    assert_eq!(go(&s, &f, &acct, &mut synced, Lie::default()).await, 200);
    assert_eq!(go(&s, &f, &acct, &mut synced, Lie::default()).await, 200, "synced passkeys stay at 0");
    // the hardware key's count (2 by now) goes back to 1
    assert_eq!(go(&s, &f, &acct, &mut hw, Lie { count: Some(1), ..Default::default() }).await, 401);
    assert!(count(&vlpds::metrics::PASSKEY_COUNTER_REGRESSIONS, &["refused"]) > refused);
    // flagged: refused even with a good count from now on
    assert_eq!(go(&s, &f, &acct, &mut hw, Lie { count: Some(100), ..Default::default() }).await, 401);
    let (_, l) = s.bearer(&acct.jwt, "vlpds.server.listPasskeys", false, None).await;
    let flagged = l["passkeys"].as_array().unwrap().iter().filter(|k| !k["suspectAt"].is_null()).count();
    assert_eq!(flagged, 1, "{l}");
    let mail = dev_mail(&s, "pkctr@example.com").await;
    assert!(mail.contains("looks like it was copied"), "{mail}");
    // the synced one still works
    assert_eq!(go(&s, &f, &acct, &mut synced, Lie::default()).await, 200);
}

/// Passwordless on the OAuth sign-in page: autofill and a button, through
/// the code exchange; one message for every refusal (no UV, an unknown or
/// another account's credential, a forged user handle); the log and the
/// new-device alert say "with a passkey".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_passwordless() {
    let before = count(&vlpds::metrics::LOGINS, &["passkey", "success"]);
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pkpwl").await;
    let other = o::create_account(&s, "pkpwl2").await;
    let mut key = SoftKey::synced(&s.base);
    oauth_register(&s, &acct, &mut key).await;
    let mut other_key = SoftKey::synced(&s.base);
    oauth_register(&s, &other, &mut other_key).await;
    let dk = o::DpopKey::new();
    let f = o::Flow::loopback("atproto", &dk);

    let mut b = o::Browser::default();
    let p = o::pkce();
    let ru = f.request_uri(&s, &p, "st").await;
    let (_, h, html) = b.authorize(&s, &f, &ru).await;
    assert!(html.contains("autocomplete=\"username webauthn\""), "{html}");
    assert!(html.contains("Sign in with a passkey") && html.contains("data-mode=\"signin\""), "{html}");
    assert_eq!(attr(&html, "data-uv"), "required");
    assert!(h.get("content-security-policy").unwrap().to_str().unwrap().contains("script-src 'sha256-"));
    // the page lists no credentials: it names no account
    assert_eq!(attr(&html, "data-allow"), "[]");
    let csrf = o::csrf_of(&html);
    let challenge = attr(&html, "data-challenge");
    let hidden = [("request_uri", ru.as_str()), ("csrf", csrf.as_str())];
    let refusals = [
        // no PIN or biometric: a second factor at most
        key.assert(&challenge, &Lie { no_uv: true, ..Default::default() }),
        // the user handle of another account: the key isn't in its row
        key.assert(&challenge, &Lie { user_handle: Some(other.did.as_bytes().to_vec()), ..Default::default() }),
        // an account with no passkeys at all, and a DID with no account
        key.assert(
            &challenge,
            &Lie { user_handle: Some(b"did:plc:nobodyhereatall2345678".to_vec()), ..Default::default() },
        ),
        // a key this server never saw
        SoftKey::synced(&s.base)
            .assert(&challenge, &Lie { user_handle: Some(acct.did.as_bytes().to_vec()), ..Default::default() }),
        // another account's own key, sent as this one
        other_key.assert(&challenge, &Lie { user_handle: Some(acct.did.as_bytes().to_vec()), ..Default::default() }),
    ];
    for a in &refusals {
        let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "passkey", a).await;
        assert_eq!(st, 401, "{html}");
        assert!(html.contains("Passkey not recognized"), "{html}");
    }
    let a = key.assert(&challenge, &Lie::default());
    let (st, _, html) = post_assertion(&s, &mut b, "/oauth/authorize/sign-in", &hidden, "passkey", &a).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access") && html.contains(&acct.handle), "{html}");
    let csrf2 = o::csrf_of(&html);
    let consent = [("request_uri", ru.as_str()), ("csrf", &csrf2), ("did", &acct.did), ("action", "allow")];
    let (st, h, _) = b.post(&s, "/oauth/authorize/consent", &consent).await;
    assert_eq!(st, 303);
    let (_, q) = o::location_params(&h);
    let t = o::tokens(&o::exchange(&s, &f, &q["code"], &p, &[]).await);
    let r = o::xrpc_dpop(&s, &dk, &t.access, "GET", "com.atproto.server.getSession", None).await;
    assert_eq!(r.body["did"], json!(acct.did));
    assert!(count(&vlpds::metrics::LOGINS, &["passkey", "success"]) > before);

    let (_, j) = s.bearer(&acct.jwt, "vlpds.server.getSignInSecurity", false, None).await;
    let e = &j["recentSignIns"][0];
    assert_eq!((e["method"].as_str(), e["factor"].as_str()), (Some("passkey"), Some("passkey")), "{j}");
    assert_eq!(e["clientId"], json!(f.client_id));

    // a code approved by a passkey that's removed before the exchange fails
    let p2 = o::pkce();
    let ru = f.request_uri(&s, &p2, "st").await;
    let mut b2 = o::Browser::default();
    let (_, _, html) = b2.authorize(&s, &f, &ru).await;
    let csrf = o::csrf_of(&html);
    let a = key.assert(&attr(&html, "data-challenge"), &Lie::default());
    let hidden = [("request_uri", ru.as_str()), ("csrf", csrf.as_str())];
    let (_, _, html) = post_assertion(&s, &mut b2, "/oauth/authorize/sign-in", &hidden, "passkey", &a).await;
    let csrf2 = o::csrf_of(&html);
    let consent = [("request_uri", ru.as_str()), ("csrf", &csrf2), ("did", &acct.did), ("action", "allow")];
    let (_, h, _) = b2.post(&s, "/oauth/authorize/consent", &consent).await;
    let (_, q) = o::location_params(&h);
    let rm = json!({"id": key.id_b64(), "password": o::PASSWORD});
    let (st, _) = s.bearer(&acct.jwt, "vlpds.server.removePasskey", true, Some(rm)).await;
    assert_eq!(st, 200);
    let r = o::exchange(&s, &f, &q["code"], &p2, &[]).await;
    assert_eq!(r.status, 400, "{}", r.body);
    // and the browser's sign-in is gone: the next request asks again
    let ru = f.request_uri(&s, &o::pkce(), "st").await;
    let (_, _, html) = b2.authorize(&s, &f, &ru).await;
    assert!(html.contains("name=\"password\""), "{html}");
}

/// `/oauth/account` signs in with a passkey too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_page_passwordless() {
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pkacc").await;
    let mut key = SoftKey::synced(&s.base);
    oauth_register(&s, &acct, &mut key).await;
    let mut b = o::Browser::default();
    let (_, h, html) = b.get(&s, &format!("{}/oauth/account", s.base)).await;
    assert!(html.contains("Sign in with a passkey"), "{html}");
    assert!(h.get("content-security-policy").unwrap().to_str().unwrap().contains("script-src 'sha256-"));
    let csrf = o::csrf_of(&html);
    let a = key.assert(&attr(&html, "data-challenge"), &Lie::default());
    let (st, h, _) = post_assertion(&s, &mut b, "/oauth/account/sign-in", &[("csrf", &csrf)], "passkey", &a).await;
    assert_eq!(st, 303);
    assert_eq!(h.get("location").unwrap(), "/oauth/account");
    let (_, _, html) = b.get(&s, &format!("{}/oauth/account", s.base)).await;
    assert!(html.contains("Connected apps") && html.contains(&acct.handle), "{html}");
    // a challenge from the authorize page doesn't work here, nor the reverse
    let mut b2 = o::Browser::default();
    let (_, _, html) = b2.get(&s, &format!("{}/oauth/account", s.base)).await;
    let csrf = o::csrf_of(&html);
    let mut b3 = o::Browser::default();
    let (_, _, other_page) = b3.get(&s, &format!("{}/oauth/account", s.base)).await;
    let a = key.assert(&attr(&other_page, "data-challenge"), &Lie::default());
    let (st, h, _) = post_assertion(&s, &mut b2, "/oauth/account/sign-in", &[("csrf", &csrf)], "passkey", &a).await;
    assert_eq!(st, 303);
    assert_eq!(h.get("location").unwrap(), "/oauth/account?add=1&error=passkey", "another browser's challenge");
}

// ---------------------------------------------------------------- the account page (SPA)

async fn start_sign_in(s: &TestServer, body: J) -> Resp {
    s.xrpc.post("vlpds.server.startPasskeySignIn", &body, &Auth::None).await
}

async fn passkey_session(s: &TestServer, did: &str, credential: J) -> Resp {
    s.xrpc.post("vlpds.server.createPasskeySession", &json!({"did": did, "credential": credential}), &Auth::None).await
}

/// Own-page createSession (same-origin fetch metadata), with a code.
async fn own_login(s: &TestServer, a: &TestAccount, code: Option<&str>) -> Resp {
    let mut body = json!({"identifier": a.handle, "password": a.password});
    if let Some(c) = code {
        body["authFactorToken"] = json!(c);
    }
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
        .header("sec-fetch-site", "same-origin")
        .json(&body);
    s.xrpc.send(rb).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_page_sign_in() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkspa").await;
    let mut key = SoftKey::synced(&s.url);
    register_passkey(&s, &a, &mut key, "phone").await;

    // passwordless: the options name no account
    let opts = start_sign_in(&s, json!({})).await.ok();
    assert_eq!((opts["userVerification"].as_str(), opts["allowCredentials"].clone()), (Some("required"), json!([])));
    let cred = key.assert(opts["challenge"].as_str().unwrap(), &Lie::default());
    let out = passkey_session(&s, &a.did, cred.clone()).await.ok();
    assert_eq!(out["did"], json!(a.did));
    let auth = Auth::Bearer(out["accessJwt"].as_str().unwrap().into());
    s.get_session(&auth).await.ok();
    // once only
    passkey_session(&s, &a.did, cred).await.err(400, "PasskeyRefused");
    // UV is required, and the user handle has to be the DID asked for
    let opts = start_sign_in(&s, json!({})).await.ok();
    let ch = opts["challenge"].as_str().unwrap();
    passkey_session(&s, &a.did, key.assert(ch, &Lie { no_uv: true, ..Default::default() }))
        .await
        .err(400, "PasskeyRefused");
    let b = s.create_account("pkspa2").await;
    passkey_session(&s, &b.did, key.assert(ch, &Lie::default())).await.err(400, "PasskeyRefused");
    let l = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    let e = &l["recentSignIns"][0];
    assert_eq!((e["method"].as_str(), e["factor"].as_str()), (Some("passkey"), Some("passkey")), "{l}");

    // the password alone no longer makes a session: not for other apps,
    // and not here without the passkey or a recovery code
    s.login(&a.handle, &a.password, None).await.err(401, "PasskeyRequired");
    own_login(&s, &a, None).await.err(401, "PasskeyRequired");
    // app passwords keep working
    let ap = s.xrpc.post("com.atproto.server.createAppPassword", &json!({"name": "bot"}), &a.auth()).await.ok();
    s.login(&a.handle, ap["password"].as_str().unwrap(), None).await.ok();

    // the second step after the password: wrong password, then right
    start_sign_in(&s, json!({"identifier": a.handle, "password": "nope"})).await.err(401, "AuthenticationRequired");
    let opts = start_sign_in(&s, json!({"identifier": a.handle, "password": a.password})).await.ok();
    assert_eq!(opts["did"], json!(a.did));
    assert_eq!(opts["allowCredentials"][0]["id"], json!(key.id_b64()));
    let cred = key.assert(opts["challenge"].as_str().unwrap(), &Lie { no_uv: true, ..Default::default() });
    let out = passkey_session(&s, &a.did, cred).await.ok();
    let l = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    let e = &l["recentSignIns"][0];
    assert_eq!((e["method"].as_str(), e["factor"].as_str()), (Some("password"), Some("passkey")), "{l}");

    // a password change voids a second-step challenge minted before it
    let opts = start_sign_in(&s, json!({"identifier": a.handle, "password": a.password})).await.ok();
    s.xrpc
        .post("com.atproto.admin.updateAccountPassword", &json!({"did": a.did, "password": a.password}), &Auth::Admin)
        .await
        .ok();
    let cred = key.assert(opts["challenge"].as_str().unwrap(), &Lie { no_uv: true, ..Default::default() });
    passkey_session(&s, &a.did, cred).await.err(400, "PasskeyRefused");
    // ... which also signed out the session above
    s.get_session(&Auth::Bearer(out["accessJwt"].as_str().unwrap().into())).await.err_status(400);
}

/// Removing a passkey signs out the account-page sessions it made, and
/// leaves the rest; "sign out everywhere" ends everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removal_ends_what_it_signed_in() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkrm").await;
    let mut k1 = SoftKey::synced(&s.url);
    let mut k2 = SoftKey::synced(&s.url);
    register_passkey(&s, &a, &mut k1, "one").await;
    register_passkey(&s, &a, &mut k2, "two").await;
    let sign_in = async |k: &mut SoftKey| {
        let opts = start_sign_in(&s, json!({})).await.ok();
        let out =
            passkey_session(&s, &a.did, k.assert(opts["challenge"].as_str().unwrap(), &Lie::default())).await.ok();
        (
            Auth::Bearer(out["accessJwt"].as_str().unwrap().into()),
            Auth::Bearer(out["refreshJwt"].as_str().unwrap().into()),
        )
    };
    let (s1, r1) = sign_in(&mut k1).await;
    let (s2, _) = sign_in(&mut k2).await;
    let rm = json!({"id": k1.id_b64(), "password": a.password});
    s.xrpc.post("vlpds.server.removePasskey", &rm, &a.auth()).await.ok();
    s.get_session(&s1).await.err_status(400);
    s.xrpc.post_empty("com.atproto.server.refreshSession", &r1).await.err_status(400);
    s.get_session(&s2).await.ok();
    s.get_session(&a.auth()).await.ok();
    let rm = json!({"id": k2.id_b64(), "password": a.password, "signOutEverywhere": true});
    s.xrpc.post("vlpds.server.removePasskey", &rm, &a.auth()).await.ok();
    s.get_session(&s2).await.err_status(400);
    s.get_session(&a.auth()).await.err_status(400);
    // no passkeys left: the password works on its own again
    s.login(&a.handle, &a.password, None).await.ok();
}

/// Passkeys join the trusted-browser fingerprint: a browser trusted before
/// a passkey was added or removed is asked again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passkey_changes_void_trusted_browsers() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pktrust").await;
    let (secret, step) = s.enable_totp(&a).await;
    let code = vlpds::totp::code_for_step(&secret, step + 1);
    let mut body =
        json!({"identifier": a.handle, "password": a.password, "authFactorToken": code, "trustDevice": true});
    let rb = s
        .xrpc
        .http
        .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
        .header("sec-fetch-site", "same-origin");
    let r = s.xrpc.send(rb.json(&body)).await;
    r.ok();
    let cookie = r.header("set-cookie").unwrap().split(';').next().unwrap().to_string();
    body = json!({"identifier": a.handle, "password": a.password});
    let trusted = async || {
        let rb = s
            .xrpc
            .http
            .post(format!("{}/xrpc/com.atproto.server.createSession", s.url))
            .header("sec-fetch-site", "same-origin")
            .header("cookie", &cookie)
            .json(&body);
        s.xrpc.send(rb).await
    };
    trusted().await.ok();
    let mut k = SoftKey::synced(&s.url);
    register_passkey(&s, &a, &mut k, "k").await;
    trusted().await.err(401, "AuthFactorTokenRequired");
    let sec = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    assert_eq!(sec["trustedBrowsers"], json!([]));
}

/// Start on one node, finish on another: the challenge is stateless and the
/// finish is routed to the account's owner by its DID.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn across_nodes() {
    use std::sync::Arc;
    let store = Arc::new(object_store::memory::InMemory::new());
    let public = "https://pds.passkeys.test".to_string();
    let p1 = public.clone();
    let p2 = public.clone();
    let n1 = cluster_node("pk-a", store.clone(), 8, move |c| c.public_url = p1).await;
    let n2 = cluster_node("pk-b", store.clone(), 8, move |c| c.public_url = p2).await;
    balanced(&[&n1, &n2]).await;
    let a = n1.create_account("pkx").await;
    let owner = owner_of(&[&n1, &n2], &a.did);
    let other = if std::ptr::eq(owner, &n1) { &n2 } else { &n1 };
    let mut k = SoftKey::synced(&public);
    register_passkey(other, &a, &mut k, "k").await;
    for (start, finish) in [(other, owner), (owner, other), (other, other)] {
        let opts = start_sign_in(start, json!({})).await.ok();
        let cred = k.assert(opts["challenge"].as_str().unwrap(), &Lie::default());
        passkey_session(finish, &a.did, cred.clone()).await.ok();
        // the claim is cluster-wide
        passkey_session(owner, &a.did, cred.clone()).await.err(400, "PasskeyRefused");
        passkey_session(other, &a.did, cred).await.err(400, "PasskeyRefused");
    }
}

// ---------------------------------------------------------------- recovery codes

/// One shared set of codes: issued with the first strong factor, good in
/// place of a passkey (the OAuth page, the account page) or a TOTP code,
/// spent once, regenerated behind the password, dropped with the last
/// strong factor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_recovery_codes() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkrc").await;
    let mut k1 = SoftKey::synced(&s.url);
    let first = register_passkey(&s, &a, &mut k1, "one").await;
    let codes: Vec<String> =
        first["recoveryCodes"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_string()).collect();
    assert_eq!(codes.len(), 10);
    assert_eq!(codes[0].len(), 19, "80 bits: {}", codes[0]);
    let mut k2 = SoftKey::synced(&s.url);
    let second = register_passkey(&s, &a, &mut k2, "two").await;
    assert_eq!(second["recoveryCodes"], json!([]), "one set for every factor");
    assert_eq!(list(&s, &a).await["recoveryCodesRemaining"], json!(10));

    // the account page: the password plus a recovery code; other apps still can't
    s.login(&a.handle, &a.password, Some(&codes[0])).await.err(401, "PasskeyRequired");
    let out = own_login(&s, &a, Some(&codes[0])).await.ok();
    assert!(out["accessJwt"].is_string());
    own_login(&s, &a, Some(&codes[0])).await.err(400, "InvalidToken");
    let sec = s.xrpc.get("vlpds.server.getSignInSecurity", &[], &a.auth()).await.ok();
    assert_eq!(sec["recentSignIns"][0]["factor"], json!("recovery"), "{sec}");

    // TOTP joins the same set; its code or a recovery code both work
    let (secret, step) = s.enable_totp(&a).await;
    let st = s.xrpc.get("vlpds.server.getTotpStatus", &[], &a.auth()).await.ok();
    assert_eq!(st["recoveryCodesRemaining"], json!(9));
    s.login(&a.handle, &a.password, Some(&codes[1])).await.ok();
    s.login(&a.handle, &a.password, Some(&vlpds::totp::code_for_step(&secret, step + 1))).await.ok();

    // a new set, behind the password
    s.xrpc
        .post("vlpds.server.regenerateRecoveryCodes", &json!({"password": "wrong"}), &a.auth())
        .await
        .err(401, "AuthenticationRequired");
    let fresh =
        s.xrpc.post("vlpds.server.regenerateRecoveryCodes", &json!({"password": a.password}), &a.auth()).await.ok();
    let fresh: Vec<String> =
        fresh["recoveryCodes"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_string()).collect();
    s.login(&a.handle, &a.password, Some(&codes[2])).await.err(400, "InvalidToken");
    s.login(&a.handle, &a.password, Some(&fresh[0])).await.ok();

    // TOTP off keeps the codes (passkeys are still on) ...
    let off = json!({"password": a.password, "recoveryCode": fresh[1]});
    s.xrpc.post("vlpds.server.disableTotp", &off, &a.auth()).await.ok();
    assert_eq!(list(&s, &a).await["recoveryCodesRemaining"], json!(8));
    // ... and the last passkey takes them
    for k in [&k1, &k2] {
        let rm = json!({"id": k.id_b64(), "password": a.password});
        s.xrpc.post("vlpds.server.removePasskey", &rm, &a.auth()).await.ok();
    }
    assert_eq!(list(&s, &a).await["recoveryCodesRemaining"], json!(0));
    s.xrpc
        .post("vlpds.server.regenerateRecoveryCodes", &json!({"password": a.password}), &a.auth())
        .await
        .err(400, "InvalidRequest");
}

/// The OAuth page takes a recovery code in place of the passkey.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oauth_recovery_code() {
    let s = o::spawn().await;
    let acct = o::create_account(&s, "pkorc").await;
    let mut key = SoftKey::synced(&s.base);
    let (_, opts) = s
        .bearer(&acct.jwt, "vlpds.server.startPasskeyRegistration", true, Some(json!({"password": o::PASSWORD})))
        .await;
    let cred = key.register(&opts, &Lie::default());
    let body = json!({"name": "key", "credential": cred});
    let (_, done) = s.bearer(&acct.jwt, "vlpds.server.finishPasskeyRegistration", true, Some(body)).await;
    let code = done["recoveryCodes"][0].as_str().unwrap().to_string();
    let dk = o::DpopKey::new();
    let f = o::Flow::loopback("atproto", &dk);
    let mut b = o::Browser::default();
    let (ru, csrf, _) = to_second_step(&s, &mut b, &f, &acct).await;
    let (st, html) = b.second_factor(&s, &ru, &csrf, "aaaa-bbbb-cccc-dddd").await;
    assert_eq!(st, 401, "{html}");
    let (st, html) = b.second_factor(&s, &ru, &csrf, &code).await;
    assert_eq!(st, 200, "{html}");
    assert!(html.contains("Authorize access"), "{html}");
}

// ---------------------------------------------------------------- the operator's reset

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_reset() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkreset").await;
    let mut k = SoftKey::synced(&s.url);
    register_passkey(&s, &a, &mut k, "lost").await;
    s.enable_totp(&a).await;
    // a session the passkey signed in, which the reset ends
    let opts = start_sign_in(&s, json!({})).await.ok();
    let out = passkey_session(&s, &a.did, k.assert(opts["challenge"].as_str().unwrap(), &Lie::default())).await.ok();
    let pk_session = Auth::Bearer(out["accessJwt"].as_str().unwrap().into());
    s.login(&a.handle, &a.password, None).await.err(401, "AuthFactorTokenRequired");

    // admin only, and a reason is required
    let body = json!({"did": a.did, "reason": "verified by a video call", "actor": "jaz"});
    s.xrpc.post("vlpds.admin.resetSecondFactors", &body, &a.auth()).await.err_status(401);
    s.xrpc
        .post("vlpds.admin.resetSecondFactors", &json!({"did": a.did, "reason": " "}), &Auth::Admin)
        .await
        .err(400, "InvalidRequest");
    let r = s.xrpc.post("vlpds.admin.resetSecondFactors", &body, &Auth::Admin).await.ok();
    assert_eq!(r["result"], json!({"passkeys": 1, "totp": true, "trustedBrowsers": 0}), "{r}");

    // the password alone works again, the passkey doesn't, its session is gone
    s.login(&a.handle, &a.password, None).await.ok();
    assert_eq!(list(&s, &a).await["passkeys"], json!([]));
    assert_eq!(list(&s, &a).await["recoveryCodesRemaining"], json!(0));
    s.get_session(&pk_session).await.err_status(400);
    let opts = start_sign_in(&s, json!({})).await.ok();
    passkey_session(&s, &a.did, k.assert(opts["challenge"].as_str().unwrap(), &Lie::default()))
        .await
        .err(400, "PasskeyRefused");
    let st = s.xrpc.get("vlpds.server.getTotpStatus", &[], &a.auth()).await.ok();
    assert_eq!(st["enabled"], json!(false));

    // audited, and the user was told
    let log = s.xrpc.get("vlpds.admin.getAuditLog", &[], &Auth::Admin).await.ok();
    let e = log["entries"].as_array().unwrap().iter().find(|e| e["action"] == "second_factors.reset").cloned();
    let e = e.unwrap_or_else(|| panic!("{log}"));
    assert_eq!((e["actor"].as_str(), e["reason"].as_str()), (Some("jaz"), Some("verified by a video call")));
    assert_eq!(e["subject"]["did"], json!(a.did));
    let mail = s.dev_mail(&a.email).await.ok();
    assert!(mail.to_string().contains("operator reset your two-factor sign-in"), "{mail}");
}

/// A passkey sign-in from a new device sends the new-device alert, saying
/// how it signed in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_device_alert_names_the_passkey() {
    let s = TestServer::spawn().await;
    let a = s.create_account("pkalert").await;
    let mut k = SoftKey::synced(&s.url);
    register_passkey(&s, &a, &mut k, "phone").await;
    let go = async |k: &mut SoftKey, ua: &str| {
        let opts = start_sign_in(&s, json!({})).await.ok();
        let cred = k.assert(opts["challenge"].as_str().unwrap(), &Lie::default());
        let rb = s
            .xrpc
            .http
            .post(format!("{}/xrpc/vlpds.server.createPasskeySession", s.url))
            .header("user-agent", ua)
            .json(&json!({"did": a.did, "credential": cred}));
        s.xrpc.send(rb).await.ok();
    };
    go(&mut k, "first-device/1.0").await;
    go(&mut k, "second-device/1.0").await;
    let mail = s.dev_mail(&a.email).await.ok();
    let alerts: Vec<&J> =
        mail["messages"].as_array().unwrap().iter().filter(|m| m["purpose"] == "sign_in_alert").collect();
    assert_eq!(alerts.len(), 1, "the first sign-in is the baseline: {mail}");
    let body = alerts[0]["body"].as_str().unwrap();
    assert!(body.contains("Signed in with a passkey") && body.contains("second-device/1.0"), "{body}");
}
