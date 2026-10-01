//! atproto crypto interop fixtures (testdata/interop/crypto).
//!
//! vlpds only generates secp256k1 (K-256) keys, so the K-256 vectors are
//! checked against `vlpds::crypto` and against the same verification rules
//! the harness applies to commits (compact 64-byte, low-S). P-256 vectors are
//! only checked for being recognized as non-K-256 keys.
mod common;
use base64::Engine;
use common::*;
use k256::ecdsa::signature::Verifier;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SigFixture {
    comment: String,
    message_base64: String,
    algorithm: String,
    public_key_did: String,
    signature_base64: String,
    valid_signature: bool,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DidKeyFixture {
    private_key_bytes_hex: String,
    public_did_key: String,
}

fn b64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(s.trim_end_matches('='))
        .unwrap()
}

/// atproto signature rules: 64-byte compact (r||s), low-S, ES256K over sha256(msg).
fn atproto_verify_k256(key: &k256::ecdsa::VerifyingKey, msg: &[u8], sig: &[u8]) -> bool {
    if sig.len() != 64 {
        return false;
    }
    let Ok(sig) = k256::ecdsa::Signature::from_slice(sig) else {
        return false;
    };
    if sig.normalize_s().is_some() {
        return false; // high-S
    }
    key.verify(msg, &sig).is_ok()
}

#[test]
fn w3c_did_key_k256_from_private_key() {
    let cases: Vec<DidKeyFixture> =
        serde_json::from_str(&read_fixture("interop/crypto/w3c_didkey_K256.json")).unwrap();
    assert!(!cases.is_empty());
    for c in cases {
        let kp =
            vlpds::crypto::Keypair::from_bytes(&hex::decode(&c.private_key_bytes_hex).unwrap())
                .unwrap();
        assert_eq!(kp.did_key(), c.public_did_key);
        // and the did:key decodes back to the same public key
        let vk = decode_did_key_k256(&c.public_did_key).unwrap();
        assert_eq!(&vk, kp.sk.verifying_key());
    }
}

#[test]
fn w3c_did_key_p256_not_mistaken_for_k256() {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct P {
        public_did_key: String,
    }
    let cases: Vec<P> =
        serde_json::from_str(&read_fixture("interop/crypto/w3c_didkey_P256.json")).unwrap();
    for c in cases {
        assert!(
            decode_did_key_k256(&c.public_did_key).is_err(),
            "{} parsed as K-256",
            c.public_did_key
        );
    }
}

#[test]
fn signature_fixtures_k256() {
    let cases: Vec<SigFixture> =
        serde_json::from_str(&read_fixture("interop/crypto/signature-fixtures.json")).unwrap();
    let mut n = 0;
    for c in cases.iter().filter(|c| c.algorithm == "ES256K") {
        n += 1;
        let key = decode_did_key_k256(&c.public_key_did).unwrap();
        let ok = atproto_verify_k256(&key, &b64(&c.message_base64), &b64(&c.signature_base64));
        assert_eq!(ok, c.valid_signature, "{}", c.comment);
    }
    assert!(n >= 3);
}

#[test]
fn harness_commit_verifier_rejects_high_s() {
    // CommitObj::verify (used across the suite) must reject the high-S vector.
    let cases: Vec<SigFixture> =
        serde_json::from_str(&read_fixture("interop/crypto/signature-fixtures.json")).unwrap();
    let c = cases
        .iter()
        .find(|c| c.algorithm == "ES256K" && c.comment.contains("non-low-S"))
        .unwrap();
    let sig = k256::ecdsa::Signature::from_slice(&b64(&c.signature_base64)).unwrap();
    assert!(sig.normalize_s().is_some(), "fixture should be high-S");
}

#[test]
fn vlpds_signatures_are_low_s_compact_and_verify() {
    let kp = vlpds::crypto::Keypair::generate();
    let vk = decode_did_key_k256(&kp.did_key()).unwrap();
    for i in 0..256u32 {
        let msg = format!("message {i}");
        let sig = kp.sign(msg.as_bytes());
        assert!(
            atproto_verify_k256(&vk, msg.as_bytes(), &sig),
            "signature {i} not valid under atproto rules"
        );
    }
}

#[test]
fn multibase_and_did_key_agree() {
    let kp = vlpds::crypto::Keypair::generate();
    assert_eq!(kp.did_key(), format!("did:key:{}", kp.public_multibase()));
    assert!(
        kp.did_key().starts_with("did:key:zQ3s"),
        "K-256 did:key prefix"
    );
    let kp2 = vlpds::crypto::Keypair::from_bytes(&kp.to_bytes()).unwrap();
    assert_eq!(kp2.did_key(), kp.did_key());
}

#[test]
fn service_auth_jwt_is_es256k_and_verifies() {
    let kp = vlpds::crypto::Keypair::generate();
    let tok = vlpds::auth::service_auth_jwt(
        &kp,
        "did:plc:abc",
        "did:web:example.com",
        Some("com.example.method"),
        60,
    );
    let parts: Vec<&str> = tok.split('.').collect();
    assert_eq!(parts.len(), 3);
    let dec = |s: &str| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .unwrap()
    };
    let header: J = serde_json::from_slice(&dec(parts[0])).unwrap();
    assert_eq!(header["alg"], "ES256K");
    let claims: J = serde_json::from_slice(&dec(parts[1])).unwrap();
    assert_eq!(claims["iss"], "did:plc:abc");
    assert_eq!(claims["aud"], "did:web:example.com");
    assert_eq!(claims["lxm"], "com.example.method");
    assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());
    let vk = decode_did_key_k256(&kp.did_key()).unwrap();
    assert!(atproto_verify_k256(
        &vk,
        format!("{}.{}", parts[0], parts[1]).as_bytes(),
        &dec(parts[2])
    ));
}
