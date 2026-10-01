//! secp256k1 (K-256) signing keys and did:key / multikey encodings.
//!
//! Backed by libsecp256k1 (the `secp256k1` crate, vendored C built by `cc`):
//! on the M4 Pro it signs in ~13.4 µs vs ~22.5 µs for RustCrypto `k256`, and
//! verifies in ~13 µs vs ~35 µs. Signatures are byte-identical to `k256`
//! (both RFC 6979 deterministic; we normalize to low-S). P-256 stays on
//! RustCrypto (`p256`), used by OAuth/JOSE and P-256 did:keys.

use secp256k1::{ecdsa::Signature, Message, PublicKey, SecretKey, SECP256K1};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub struct Keypair {
    sk: SecretKey,
    /// derived lazily: deriving costs ~9 µs and most loads only sign
    pk: OnceLock<PublicKey>,
}

impl Keypair {
    fn new(sk: SecretKey) -> Keypair {
        Keypair { sk, pk: OnceLock::new() }
    }

    pub fn generate() -> Keypair {
        loop {
            // a uniformly random 32-byte string is a valid scalar except
            // with negligible (~2^-128) probability
            let b: [u8; 32] = rand::random();
            if let Ok(sk) = SecretKey::from_byte_array(&b) {
                return Keypair::new(sk);
            }
        }
    }

    pub fn from_bytes(b: &[u8]) -> anyhow::Result<Keypair> {
        Ok(Keypair::new(SecretKey::from_slice(b)?))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.sk.secret_bytes().to_vec()
    }

    /// Low-S ECDSA signature over sha256(data), 64-byte compact form.
    pub fn sign(&self, data: &[u8]) -> [u8; 64] {
        let digest: [u8; 32] = Sha256::digest(data).into();
        let mut sig = SECP256K1.sign_ecdsa(&Message::from_digest(digest), &self.sk);
        sig.normalize_s();
        sig.serialize_compact()
    }

    fn public_key(&self) -> &PublicKey {
        self.pk
            .get_or_init(|| PublicKey::from_secret_key(SECP256K1, &self.sk))
    }

    /// Compressed SEC1 public key (33 bytes).
    pub fn public_key_sec1(&self) -> [u8; 33] {
        self.public_key().serialize()
    }

    /// Multibase (base58btc) multikey: secp256k1-pub multicodec + compressed point.
    pub fn public_multibase(&self) -> String {
        let mut b = vec![0xe7, 0x01];
        b.extend_from_slice(&self.public_key_sec1());
        format!("z{}", bs58::encode(b).into_string())
    }

    pub fn did_key(&self) -> String {
        format!("did:key:{}", self.public_multibase())
    }
}

/// atproto ES256K check: `sig` is compact 64-byte (r||s) over sha256(msg),
/// and must be low-S (high-S is rejected, as `k256` and the reference do).
/// Err for a malformed key or signature encoding, Ok(false) for a bad signature.
pub fn verify_k256(pubkey_sec1: &[u8], msg: &[u8], sig: &[u8]) -> anyhow::Result<bool> {
    let pk = PublicKey::from_slice(pubkey_sec1)?;
    let sig = Signature::from_compact(sig)?;
    let digest: [u8; 32] = Sha256::digest(msg).into();
    Ok(SECP256K1
        .verify_ecdsa(&Message::from_digest(digest), &sig, &pk)
        .is_ok())
}

/// [`verify_k256`], also accepting the high-S form of a signature: the
/// reference's `allowMalleableSig`, used for inter-service JWTs only (commits
/// and records stay low-S).
pub fn verify_k256_malleable(pubkey_sec1: &[u8], msg: &[u8], sig: &[u8]) -> anyhow::Result<bool> {
    let pk = PublicKey::from_slice(pubkey_sec1)?;
    let mut sig = Signature::from_compact(sig)?;
    sig.normalize_s();
    let digest: [u8; 32] = Sha256::digest(msg).into();
    Ok(SECP256K1
        .verify_ecdsa(&Message::from_digest(digest), &sig, &pk)
        .is_ok())
}

pub fn random_plc_did() -> String {
    let b: [u8; 15] = rand::random();
    format!("did:plc:{}", crate::cid::base32_encode(&b))
}
