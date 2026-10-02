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
        let mut sig = self.sign_digest(&digest);
        sig.normalize_s();
        sig.serialize_compact()
    }

    /// libsecp256k1's `secp256k1_ecdsa_sign` with [`rfc6979_nonce`]: the
    /// same signature as `SECP256K1.sign_ecdsa`, with the nonce's
    /// HMAC-SHA256 on the `sha2` crate's hardware SHA-256 (SHA-NI / ARMv8
    /// SHA2) instead of libsecp256k1's portable C SHA-256, which was ~2.5 µs
    /// (~17%) of a ~14.5 µs signature.
    fn sign_digest(&self, digest: &[u8; 32]) -> Signature {
        use secp256k1::ffi::{self, CPtr};
        // SAFETY: an all-zero signature buffer for libsecp256k1 to fill; the
        // global context can sign; `digest` and the secret key are 32-byte
        // buffers; no nonce data is passed.
        let mut sig = unsafe { ffi::Signature::new() };
        let ok = unsafe {
            ffi::secp256k1_ecdsa_sign(
                SECP256K1.ctx().as_ptr(),
                &mut sig,
                digest.as_ptr(),
                self.sk.as_c_ptr(),
                Some(rfc6979_nonce),
                std::ptr::null(),
            )
        };
        // fails only for an invalid secret key, which SecretKey rules out
        assert_eq!(ok, 1, "secp256k1_ecdsa_sign");
        Signature::from(sig)
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

/// The secp256k1 group order n, big-endian.
const ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
];

/// `m mod n` for a 32-byte big-endian value (m < 2^256 < 2n: at most one
/// subtraction). Constant time.
fn reduce_mod_order(m: &[u8; 32]) -> [u8; 32] {
    let mut diff = [0u8; 32];
    let mut borrow = 0u16;
    for i in (0..32).rev() {
        let d = (m[i] as u16).wrapping_sub(ORDER[i] as u16).wrapping_sub(borrow);
        diff[i] = d as u8;
        borrow = (d >> 8) & 1;
    }
    // borrow == 1: m < n, keep m
    let keep = 0u8.wrapping_sub(borrow as u8);
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = (m[i] & keep) | (diff[i] & !keep);
    }
    out
}

/// HMAC-SHA256 with a 32-byte key over the concatenation of `parts`.
fn hmac_sha256(key: &[u8; 32], parts: &[&[u8]]) -> [u8; 32] {
    let mut pad = [0x36u8; 64];
    for (p, k) in pad.iter_mut().zip(key) {
        *p ^= k;
    }
    let mut h = Sha256::new();
    h.update(pad);
    for p in parts {
        h.update(p);
    }
    let inner = h.finalize();
    for p in pad.iter_mut() {
        *p ^= 0x36 ^ 0x5c;
    }
    let mut h = Sha256::new();
    h.update(pad);
    h.update(inner);
    pad.fill(0);
    h.finalize().into()
}

/// libsecp256k1's `nonce_function_rfc6979` (its default ECDSA nonce, see
/// secp256k1.c and hash_impl.h `rfc6979_hmac_sha256_*`) step for step:
/// HMAC-DRBG seeded with key32 || (msg32 mod n), output number `counter`.
/// Calls with extra nonce data or an algorithm tag (never made here) go
/// to the C function.
unsafe extern "C" fn rfc6979_nonce(
    nonce32: *mut std::os::raw::c_uchar,
    msg32: *const std::os::raw::c_uchar,
    key32: *const std::os::raw::c_uchar,
    algo16: *const std::os::raw::c_uchar,
    data: *mut std::os::raw::c_void,
    counter: std::os::raw::c_uint,
) -> std::os::raw::c_int {
    use secp256k1::ffi;
    if !algo16.is_null() || !data.is_null() {
        // SAFETY: forwards the caller's arguments unchanged
        return unsafe {
            match ffi::secp256k1_nonce_function_rfc6979 {
                Some(f) => f(nonce32, msg32, key32, algo16, data, counter),
                None => 0,
            }
        };
    }
    // SAFETY: libsecp256k1 passes 32-byte buffers for nonce32, msg32, key32
    let (out, msg, key) = unsafe {
        (
            &mut *(nonce32 as *mut [u8; 32]),
            &*(msg32 as *const [u8; 32]),
            &*(key32 as *const [u8; 32]),
        )
    };
    let mut seed = [0u8; 64];
    seed[..32].copy_from_slice(key);
    seed[32..].copy_from_slice(&reduce_mod_order(msg));
    // RFC 6979 3.2 b-g
    let mut v = [0x01u8; 32];
    let mut k = [0x00u8; 32];
    k = hmac_sha256(&k, &[&v, &[0x00], &seed]);
    v = hmac_sha256(&k, &[&v]);
    k = hmac_sha256(&k, &[&v, &[0x01], &seed]);
    v = hmac_sha256(&k, &[&v]);
    seed.fill(0);
    // 3.2 h: output `counter` (each retry first reseeds K and V)
    for i in 0..=counter {
        if i > 0 {
            k = hmac_sha256(&k, &[&v, &[0x00]]);
            v = hmac_sha256(&k, &[&v]);
        }
        v = hmac_sha256(&k, &[&v]);
    }
    *out = v;
    k.fill(0);
    v.fill(0);
    std::hint::black_box((&k, &v, &seed));
    1
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use secp256k1::ffi;

    /// The C nonce function, called directly.
    fn c_nonce(msg: &[u8; 32], key: &[u8; 32], counter: u32) -> [u8; 32] {
        let mut out = [0u8; 32];
        let f = unsafe { ffi::secp256k1_nonce_function_rfc6979 }.unwrap();
        let r = unsafe {
            f(out.as_mut_ptr(), msg.as_ptr(), key.as_ptr(), std::ptr::null(), std::ptr::null_mut(), counter)
        };
        assert_eq!(r, 1);
        out
    }

    fn rust_nonce(msg: &[u8; 32], key: &[u8; 32], counter: u32) -> [u8; 32] {
        let mut out = [0u8; 32];
        let r = unsafe {
            rfc6979_nonce(out.as_mut_ptr(), msg.as_ptr(), key.as_ptr(), std::ptr::null(), std::ptr::null_mut(), counter)
        };
        assert_eq!(r, 1);
        out
    }

    fn edge_messages() -> Vec<[u8; 32]> {
        let mut n_minus_1 = ORDER;
        n_minus_1[31] -= 1;
        let mut n_plus_1 = ORDER;
        n_plus_1[31] += 1;
        vec![[0u8; 32], [0xff; 32], ORDER, n_minus_1, n_plus_1, {
            let mut m = [0u8; 32];
            m[31] = 1;
            m
        }]
    }

    #[test]
    fn reduce_mod_order_matches_big_subtraction() {
        let mut n_minus_1 = ORDER;
        n_minus_1[31] -= 1;
        assert_eq!(reduce_mod_order(&ORDER), [0u8; 32]);
        assert_eq!(reduce_mod_order(&n_minus_1), n_minus_1);
        // 2^256 - 1 - n = 0x14551231950b75fc4402da1732fc9bebe
        let r = reduce_mod_order(&[0xff; 32]);
        assert_eq!(hex::encode(r), "000000000000000000000000000000014551231950b75fc4402da1732fc9bebe");
    }

    #[test]
    fn rfc6979_nonce_matches_libsecp256k1() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(6979);
        let mut msgs = edge_messages();
        for _ in 0..2000 {
            msgs.push(rng.r#gen());
        }
        for (i, msg) in msgs.iter().enumerate() {
            let key: [u8; 32] = if i % 3 == 0 { [0xff; 32] } else { rng.r#gen() };
            for counter in [0, 1, 2, 7] {
                assert_eq!(rust_nonce(msg, &key, counter), c_nonce(msg, &key, counter), "msg {i} counter {counter}");
            }
        }
    }

    #[test]
    fn signatures_match_libsecp256k1_default_nonce() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(256);
        for i in 0..3000 {
            let kp = Keypair::generate();
            let mut data = vec![0u8; rng.gen_range(0..400)];
            rng.fill(&mut data[..]);
            let digest: [u8; 32] = Sha256::digest(&data).into();
            let mut want = SECP256K1.sign_ecdsa(&Message::from_digest(digest), &kp.sk);
            want.normalize_s();
            assert_eq!(kp.sign(&data), want.serialize_compact(), "signature {i}");
        }
        // digests at and above the group order
        let kp = Keypair::generate();
        for d in edge_messages() {
            let mut want = SECP256K1.sign_ecdsa(&Message::from_digest(d), &kp.sk);
            want.normalize_s();
            let mut got = kp.sign_digest(&d);
            got.normalize_s();
            assert_eq!(got, want);
        }
    }
}
