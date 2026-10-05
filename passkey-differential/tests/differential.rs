//! passkey-rs (1Password's WebAuthn client and software authenticator, an
//! independent implementation) answers options shaped like the ones vlpds
//! hands out, and vlpds's verifier has to accept what it produces, and
//! refuse it where a check says so.

use passkey::authenticator::{Authenticator, UiHint, UserCheck, UserValidationMethod};
use passkey::client::{Client, DefaultClientData};
use passkey::crypto::rust_crypto::RustCryptoBackend;
use passkey::types::ctap2::{Aaguid, Ctap2Error};
use passkey::types::webauthn::{CredentialCreationOptions, CredentialRequestOptions};
use passkey::types::Passkey;
use serde_json::{json, Value as J};
use vlpds_passkey_differential::oauth::util::{b64u, b64u_decode};
use vlpds_passkey_differential::webauthn::{self, Fail, PublicKey, Rp};

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

const ORIGIN: &str = "http://localhost:2790";
const DID: &str = "did:plc:abcdefghijklmnopqrstuvwx";
const KEY: [u8; 32] = [9; 32];

fn rp() -> Rp {
    Rp::from_public_url(ORIGIN).unwrap()
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

fn field(j: &J, k: &str) -> Vec<u8> {
    b64u_decode(j[k].as_str().unwrap_or_else(|| panic!("no {k}: {j}"))).unwrap()
}

/// `vlpds.server.startPasskeyRegistration`'s options for DID.
fn creation_options(challenge: &[u8]) -> J {
    json!({
        "rp": {"id": "localhost", "name": "localhost"},
        "user": {"id": b64u(DID), "name": "alice.test", "displayName": "alice.test"},
        "challenge": b64u(challenge),
        "pubKeyCredParams": webauthn::ALGS.iter().map(|a| json!({"type": "public-key", "alg": a})).collect::<Vec<_>>(),
        "timeout": 300000,
        "attestation": "none",
        "authenticatorSelection": {"residentKey": "preferred", "requireResidentKey": false, "userVerification": "preferred"},
        "excludeCredentials": [],
        "extensions": {"credProps": true},
    })
}

fn request_options(challenge: &[u8], allow: J) -> J {
    json!({
        "challenge": b64u(challenge),
        "rpId": "localhost",
        "timeout": 300000,
        "userVerification": "required",
        "allowCredentials": allow,
    })
}

#[tokio::test]
async fn passkey_rs_against_our_verifier() {
    let origin = url::Url::parse(ORIGIN).unwrap();
    let store: Option<Passkey> = None;
    let auth = Authenticator::new(Aaguid::new_empty(), store, Present, RustCryptoBackend);
    let mut client = Client::new(auth).allows_insecure_localhost(true);

    // registration, over one of our MAC'd challenges
    let ch = webauthn::mint_challenge(&KEY, "register", DID, now());
    let req: CredentialCreationOptions =
        serde_json::from_value(json!({"publicKey": creation_options(&ch)})).expect("our options parse");
    let created = serde_json::to_value(client.register(&origin, req, DefaultClientData).await.unwrap()).unwrap();
    let r = &created["response"];
    let cdj = field(r, "clientDataJSON");
    let got = webauthn::client_data_challenge(&cdj).unwrap();
    webauthn::open_challenge(&KEY, "register", DID, &got, now()).expect("its client data carries our challenge");
    let reg = webauthn::verify_registration(
        &rp(),
        &got,
        &field(&created, "rawId"),
        &cdj,
        &field(r, "attestationObject"),
        true,
    )
    .expect("our verifier accepts passkey-rs's registration");
    assert!(webauthn::ALGS.contains(&reg.alg));
    let key = PublicKey::from_cose(&reg.cose_key).unwrap();
    let mut stored = reg.sign_count;

    // sign-ins: passwordless (discoverable, UV) and with allowCredentials
    for allow in [json!([]), json!([{"type": "public-key", "id": b64u(&reg.credential_id)}])] {
        let ch = webauthn::mint_challenge(&KEY, "signin", "", now());
        let req: CredentialRequestOptions =
            serde_json::from_value(json!({"publicKey": request_options(&ch, allow)})).expect("our options parse");
        let a = serde_json::to_value(client.authenticate(&origin, req, DefaultClientData).await.unwrap()).unwrap();
        let r = &a["response"];
        assert_eq!(field(&a, "rawId"), reg.credential_id);
        assert_eq!(field(r, "userHandle"), DID.as_bytes(), "the user handle is the DID");
        let (cdj, ad, sig) = (field(r, "clientDataJSON"), field(r, "authenticatorData"), field(r, "signature"));
        let got = webauthn::verify_assertion(&rp(), &ch, &key, &cdj, &ad, &sig, true)
            .expect("our verifier accepts passkey-rs's assertion");
        assert!(got.uv);
        assert!(!webauthn::counter_regressed(stored, got.sign_count), "{stored} -> {}", got.sign_count);
        stored = got.sign_count;
        // the same assertion over another challenge, or from another RP's view, fails
        assert_eq!(
            webauthn::verify_assertion(&rp(), b"other", &key, &cdj, &ad, &sig, true).unwrap_err(),
            Fail::Challenge
        );
        let other = Rp { id: "localhost".into(), origin: "https://localhost".into() };
        assert_eq!(webauthn::verify_assertion(&other, &ch, &key, &cdj, &ad, &sig, true).unwrap_err(), Fail::Origin);
    }

    // made on another origin (a lookalike's page asking for our RP ID is
    // refused by the client; one asking for its own is refused by us)
    let evil = url::Url::parse("http://localhost:9999").unwrap();
    let ch = webauthn::mint_challenge(&KEY, "signin", "", now());
    let req: CredentialRequestOptions =
        serde_json::from_value(json!({"publicKey": request_options(&ch, json!([]))})).unwrap();
    let a = serde_json::to_value(client.authenticate(&evil, req, DefaultClientData).await.unwrap()).unwrap();
    let r = &a["response"];
    let e = webauthn::verify_assertion(
        &rp(),
        &ch,
        &key,
        &field(r, "clientDataJSON"),
        &field(r, "authenticatorData"),
        &field(r, "signature"),
        true,
    )
    .unwrap_err();
    assert_eq!(e, Fail::Origin);
}
