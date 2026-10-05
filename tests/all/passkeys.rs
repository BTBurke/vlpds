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
