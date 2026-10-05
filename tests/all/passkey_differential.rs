//! Differential: 1Password's passkey-rs (an independent WebAuthn client and
//! software authenticator) against our relying party. It reads the options
//! our XRPCs hand out as any client would, builds its own clientDataJSON,
//! authenticator data and COSE key, and our verifier has to accept the
//! result: registration, passwordless sign-in and the second step, through
//! the real endpoints. Nothing here shares code with src/webauthn.rs or the
//! suite's own software authenticator.

use crate::common::*;
use passkey::authenticator::{Authenticator, UiHint, UserCheck, UserValidationMethod};
use passkey::client::{Client, DefaultClientData};
use passkey::crypto::rust_crypto::RustCryptoBackend;
use passkey::types::ctap2::{Aaguid, Ctap2Error};
use passkey::types::webauthn::{CredentialCreationOptions, CredentialRequestOptions};
use passkey::types::Passkey;

/// A user who's always there and always verified (a PIN or biometric).
struct Present;

#[async_trait::async_trait]
impl UserValidationMethod for Present {
    type PasskeyItem = Passkey;

    async fn check_user<'a>(
        &self,
        _hint: UiHint<'a, Passkey>,
        presence: bool,
        verification: bool,
    ) -> Result<UserCheck, Ctap2Error> {
        Ok(UserCheck { presence, verification })
    }

    fn is_presence_enabled(&self) -> bool {
        true
    }

    fn is_verification_enabled(&self) -> Option<bool> {
        Some(true)
    }
}

/// Its JSON, with field names as a browser's `toJSON()` would give them.
fn to_json<T: serde::Serialize>(v: &T) -> J {
    serde_json::to_value(v).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passkey_rs_registers_and_signs_in() {
    // the client only allows an http origin for localhost
    let s = TestServer::spawn_with(|c| c.public_url = c.public_url.replace("127.0.0.1", "localhost")).await;
    let origin = reqwest::Url::parse(&s.app.public_url).unwrap();
    let a = s.create_account("pkrs").await;
    let store: Option<Passkey> = None;
    let auth = Authenticator::new(Aaguid::new_empty(), store, Present, RustCryptoBackend);
    let mut client = Client::new(auth).allows_insecure_localhost(true);

    // registration: our options, its credential
    let opts =
        s.xrpc.post("vlpds.server.startPasskeyRegistration", &json!({"password": a.password}), &a.auth()).await.ok();
    let req: CredentialCreationOptions = serde_json::from_value(json!({"publicKey": opts})).expect("our options parse");
    let created = client.register(&origin, req, DefaultClientData).await.expect("passkey-rs registers");
    let cred = to_json(&created);
    let done = s
        .xrpc
        .post("vlpds.server.finishPasskeyRegistration", &json!({"name": "passkey-rs", "credential": cred}), &a.auth())
        .await
        .ok();
    assert_eq!(done["name"], json!("passkey-rs"));
    let listed = s.xrpc.get("vlpds.server.listPasskeys", &[], &a.auth()).await.ok();
    assert_eq!(listed["passkeys"][0]["id"], cred["id"], "{listed}");

    // passwordless: its assertion over our challenge
    for _ in 0..2 {
        let opts = s.xrpc.post("vlpds.server.startPasskeySignIn", &json!({}), &Auth::None).await.ok();
        let req: CredentialRequestOptions =
            serde_json::from_value(json!({"publicKey": opts})).expect("our options parse");
        let got = to_json(&client.authenticate(&origin, req, DefaultClientData).await.expect("passkey-rs signs"));
        let r = &got["response"];
        let assertion = json!({
            "id": got["rawId"],
            "clientDataJSON": r["clientDataJSON"],
            "authenticatorData": r["authenticatorData"],
            "signature": r["signature"],
            "userHandle": r["userHandle"],
        });
        let out = s
            .xrpc
            .post("vlpds.server.createPasskeySession", &json!({"did": a.did, "credential": assertion}), &Auth::None)
            .await
            .ok();
        assert_eq!(out["did"], json!(a.did));
    }

    // the second step after the password, with our allowCredentials
    let opts = s
        .xrpc
        .post("vlpds.server.startPasskeySignIn", &json!({"identifier": a.handle, "password": a.password}), &Auth::None)
        .await
        .ok();
    let mut body = opts.clone();
    body.as_object_mut().unwrap().remove("did");
    let req: CredentialRequestOptions = serde_json::from_value(json!({"publicKey": body})).expect("our options parse");
    let got = to_json(&client.authenticate(&origin, req, DefaultClientData).await.expect("passkey-rs signs"));
    let r = &got["response"];
    let assertion = json!({
        "id": got["rawId"],
        "clientDataJSON": r["clientDataJSON"],
        "authenticatorData": r["authenticatorData"],
        "signature": r["signature"],
    });
    s.xrpc
        .post("vlpds.server.createPasskeySession", &json!({"did": a.did, "credential": assertion}), &Auth::None)
        .await
        .ok();

    // a sign-in it makes on another site, over our challenge, is refused
    let evil = reqwest::Url::parse("https://evil.example").unwrap();
    let opts = s.xrpc.post("vlpds.server.startPasskeySignIn", &json!({}), &Auth::None).await.ok();
    let mut o2 = opts.clone();
    o2["rpId"] = json!("evil.example");
    let req: CredentialRequestOptions = serde_json::from_value(json!({"publicKey": o2})).unwrap();
    if let Ok(got) = client.authenticate(&evil, req, DefaultClientData).await {
        let got = to_json(&got);
        let r = &got["response"];
        let assertion = json!({
            "id": got["rawId"],
            "clientDataJSON": r["clientDataJSON"],
            "authenticatorData": r["authenticatorData"],
            "signature": r["signature"],
            "userHandle": r["userHandle"],
        });
        s.xrpc
            .post("vlpds.server.createPasskeySession", &json!({"did": a.did, "credential": assertion}), &Auth::None)
            .await
            .err(400, "PasskeyRefused");
    }
}
