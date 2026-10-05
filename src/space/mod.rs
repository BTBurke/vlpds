//! AT Protocol Spaces (permissioned data), tracking the reference's alpha
//! (bluesky-social/atproto PR #5187; pinned commit in
//! testdata/spaces-alpha/SOURCE). This holds the protocol primitives only:
//! pure, no I/O, each checked against vectors from the reference.
//!
//! - [`lthash`]: the space repo's homomorphic set hash.
//! - [`commit`]: the deniable commit (ctx, MAC, signature).
//! - [`httpsig`]: the RFC 9421 subset requests carry with a delegation
//!   token or a space credential.
//! - [`token`]: delegation tokens, space credentials, client attestations.
//!
//! Serving is behind `--spaces` (`Config::spaces`), off by default. No
//! method is implemented yet: with the flag on, every Spaces NSID answers
//! 501 here rather than reaching the `atproto-proxy` fallback.

pub mod commit;
pub mod httpsig;
pub mod lthash;
mod sfv;
pub mod token;

/// The XRPC methods of the Spaces lexicons. NSID authorities are
/// case-insensitive, as the proxy's method lists match them.
pub fn is_space_nsid(nsid: &str) -> bool {
    ["com.atproto.space.", "com.atproto.simplespace."]
        .iter()
        .any(|p| nsid.get(..p.len()).is_some_and(|h| h.eq_ignore_ascii_case(p)))
}

/// A did:key's JWT `alg` (@atproto/crypto `parseDidKey`): ES256K for
/// secp256k1, ES256 for P-256.
pub fn did_key_alg(did_key: &str) -> Option<&'static str> {
    let raw = bs58::decode(did_key.strip_prefix("did:key:z")?).into_vec().ok()?;
    match raw.as_slice() {
        [0xe7, 0x01, ..] => Some("ES256K"),
        [0x80, 0x24, ..] => Some("ES256"),
        _ => None,
    }
}

/// A compact (r‖s, 64-byte) ECDSA signature over sha256(msg) by `did_key`;
/// DER is refused. High-S only with `allow_high_s` (@atproto/crypto
/// `allowMalleableSig`). Err: not a usable did:key or signature encoding.
fn verify_did_key(did_key: &str, msg: &[u8], sig: &[u8], allow_high_s: bool) -> Result<bool, String> {
    if sig.len() != 64 {
        return Err("signature must be 64 bytes".into());
    }
    let multibase = did_key.strip_prefix("did:key:").ok_or("not a did:key")?;
    // libsecp256k1 also parses hybrid (0x06/0x07) points; the reference doesn't
    let raw = bs58::decode(multibase.strip_prefix('z').ok_or("unsupported multibase")?)
        .into_vec()
        .map_err(|e| e.to_string())?;
    let key = raw.get(2..).unwrap_or_default();
    if !matches!((key.len(), key.first()), (33, Some(2 | 3)) | (65, Some(4))) {
        return Err("unsupported public key encoding".into());
    }
    if allow_high_s {
        crate::oauth::lexicon::verify_sig_malleable(multibase, msg, sig)
    } else {
        crate::oauth::lexicon::verify_sig(multibase, msg, sig)
    }
}

#[cfg(test)]
pub(crate) mod vectors {
    use std::sync::LazyLock;

    pub static VECTORS: LazyLock<serde_json::Value> = LazyLock::new(|| {
        serde_json::from_str(include_str!("../../testdata/spaces-alpha/vectors.json")).expect("vectors.json")
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_nsids() {
        for yes in ["com.atproto.space.getRecord", "com.atproto.simplespace.createSpace", "COM.ATPROTO.Space.x"] {
            assert!(is_space_nsid(yes), "{yes}");
        }
        for no in ["com.atproto.repo.getRecord", "com.atproto.spaces.x", "com.atproto.space", "app.bsky.space.x", ""] {
            assert!(!is_space_nsid(no), "{no}");
        }
    }

    #[test]
    fn did_key_algs() {
        let k256 = crate::crypto::Keypair::generate().did_key();
        assert_eq!(did_key_alg(&k256), Some("ES256K"));
        let p256 = vectors::VECTORS["httpsig"][0]["keyDid"].as_str().unwrap();
        assert_eq!(did_key_alg(p256), Some("ES256"));
        assert_eq!(did_key_alg("did:key:zQ3"), None);
        assert_eq!(did_key_alg("did:plc:abc"), None);
        assert!(verify_did_key("did:plc:abc", b"x", &[0; 64], false).is_err());
        assert!(verify_did_key(&k256, b"x", &[0; 70], false).is_err());
        // a hybrid-encoded point (0x06/0x07 prefix) of a real key
        let key = crate::crypto::Keypair::generate();
        let sig = key.sign(b"x");
        let pk = secp256k1::PublicKey::from_slice(&key.public_key_sec1()).unwrap().serialize_uncompressed();
        let mut hybrid = vec![0xe7, 0x01, 6 | (pk[64] & 1)];
        hybrid.extend_from_slice(&pk[1..]);
        let hybrid = format!("did:key:z{}", bs58::encode(hybrid).into_string());
        assert!(
            crate::oauth::lexicon::verify_sig(hybrid.strip_prefix("did:key:").unwrap(), b"x", &sig).is_ok_and(|v| v)
        );
        assert!(verify_did_key(&hybrid, b"x", &sig, false).is_err());
        assert_eq!(verify_did_key(&key.did_key(), b"x", &sig, false), Ok(true));
    }
}
