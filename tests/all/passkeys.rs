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
