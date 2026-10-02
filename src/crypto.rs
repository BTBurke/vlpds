//! secp256k1 (K-256) signing keys and did:key / multikey encodings.
//!
//! Backed by libsecp256k1 (the `secp256k1` crate, vendored C built by `cc`):
//! on the M4 Pro it signs in ~13.4 µs vs ~22.5 µs for RustCrypto `k256`, and
//! verifies in ~13 µs vs ~35 µs. P-256 stays on RustCrypto (`p256`), used by
//! OAuth/JOSE and P-256 did:keys.
//!
//! **Signing hardening** (fault attacks: Rowhammer, voltage/clock glitches,
//! bad RAM). With purely deterministic ECDSA (RFC 6979), one fault while
//! signing a message that is also signed correctly leaks the private key
//! from the two signatures, and commit signatures go straight to the public
//! firehose. So:
//! - every signature is *hedged*: 32 fresh bytes from a thread-local CSPRNG
//!   (`rand::thread_rng`, ChaCha12 seeded from the OS) go into the nonce
//!   derivation as RFC 6979 §3.6 additional data (libsecp256k1's `ndata`),
//!   so no two signatures share a nonce, even over the same message, and a
//!   broken RNG still leaves RFC 6979's guarantee;
//! - signatures that leave the node ([`Keypair::sign_verified`]: commits,
//!   service-auth JWTs; `oauth::jose::ServerKey::sign` for access tokens)
//!   are verified against the key's cached public key, over a freshly
//!   hashed message, before they are used. A failure is never emitted: it
//!   counts `vlpds_signature_verify_failures_total{purpose}`, logs an
//!   error and signs once more (fresh nonce); a second failure is a
//!   retryable error. [`FAULT_FAIL_STOP`] failures within [`FAULT_WINDOW`]
//!   fail-stop the node (`signature_fault`, exit code 6): a host that flips
//!   bits must not keep signing.
//!
//! The deterministic path stays for tests and tools that compare against
//! other RFC 6979 implementations ([`Keypair::sign_deterministic`]).

use secp256k1::{ecdsa::Signature, Message, PublicKey, SecretKey, SECP256K1};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

pub struct Keypair {
    sk: SecretKey,
    /// derived lazily: deriving costs ~9 µs and most loads only sign
    pk: OnceLock<PublicKey>,
}

/// Unwrapped signing keys live in caches (`secrets::Secrets`, repo state):
/// overwrite the scalar when the last holder lets go (best effort; see
/// `SecretKey::non_secure_erase`).
impl Drop for Keypair {
    fn drop(&mut self) {
        self.sk.non_secure_erase();
    }
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

    /// The raw secret (callers wrap it at once: `secrets::Secrets`).
    pub fn to_bytes(&self) -> zeroize::Zeroizing<Vec<u8>> {
        let mut b = self.sk.secret_bytes();
        let v = zeroize::Zeroizing::new(b.to_vec());
        zeroize::Zeroize::zeroize(&mut b);
        v
    }

    /// Low-S ECDSA signature over sha256(data), 64-byte compact form, with a
    /// hedged nonce. Not verified: what leaves the node goes through
    /// [`sign_verified`](Self::sign_verified).
    pub fn sign(&self, data: &[u8]) -> [u8; 64] {
        let digest: [u8; 32] = Sha256::digest(data).into();
        sign_hedged(&self.sk, &digest)
    }

    /// RFC 6979 without additional data: the signature `k256`, shrike or
    /// `SECP256K1.sign_ecdsa` make for the same key and message. For tests
    /// and tools that compare signatures byte for byte, never for anything a
    /// node emits.
    pub fn sign_deterministic(&self, data: &[u8]) -> [u8; 64] {
        let digest: [u8; 32] = Sha256::digest(data).into();
        let mut sig = sign_digest(&self.sk, &digest, None);
        sig.normalize_s();
        sig.serialize_compact()
    }

    /// [`sign`](Self::sign), then checked against this key's cached public
    /// key over a freshly computed hash of `data` before it is returned. A
    /// signature that fails is never returned: it is recorded
    /// ([`record_fault`]: metric, error log, fail-stop on repeats) and
    /// signing is retried once with a fresh nonce; a second failure is a
    /// [`SignatureFault`] (retryable: nothing was emitted).
    pub fn sign_verified(&self, purpose: Purpose, data: &[u8]) -> Result<[u8; 64], SignatureFault> {
        let pk = self.public_key();
        for _ in 0..2 {
            let digest: [u8; 32] = Sha256::digest(data).into();
            let injected = if fault::armed() { fault::take(&pk.serialize()) } else { None };
            let mut sig = match injected {
                None => sign_hedged(&self.sk, &digest),
                Some(fault::Fault::Signature) => {
                    let mut s = sign_hedged(&self.sk, &digest);
                    s[40] ^= 0x04;
                    s
                }
                Some(fault::Fault::Secret) => {
                    // a bit of the scalar flipped while signing
                    let mut b = self.sk.secret_bytes();
                    b[31] ^= 0x10;
                    let flipped = SecretKey::from_byte_array(&b).expect("flipped scalar");
                    b.fill(0);
                    sign_hedged(&flipped, &digest)
                }
            };
            if verify_compact(pk, data, &sig) {
                return Ok(sig);
            }
            sig.fill(0);
            record_fault(purpose);
        }
        Err(SignatureFault { purpose: purpose.as_str() })
    }

    /// Whether the secret scalar still derives the cached public key and
    /// `multibase` (the account's stored public key; empty: the cached key
    /// only). One public-key derivation (~9 µs): once per repo load.
    pub fn matches_public(&self, multibase: &str) -> bool {
        let fresh = PublicKey::from_secret_key(SECP256K1, &self.sk);
        fresh == *self.public_key() && (multibase.is_empty() || self.public_multibase() == multibase)
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

/// A low-S compact signature with 32 fresh random bytes as the nonce's
/// additional data.
fn sign_hedged(sk: &SecretKey, digest: &[u8; 32]) -> [u8; 64] {
    let mut extra = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut extra);
    let mut sig = sign_digest(sk, digest, Some(&extra));
    extra.fill(0);
    sig.normalize_s();
    sig.serialize_compact()
}

/// libsecp256k1's `secp256k1_ecdsa_sign` with [`rfc6979_nonce`]: the same
/// signature as `SECP256K1.sign_ecdsa_with_noncedata` (`sign_ecdsa` without
/// `extra`), with the nonce's HMAC-SHA256 on the `sha2` crate's hardware
/// SHA-256 (SHA-NI / ARMv8 SHA2) instead of libsecp256k1's portable C
/// SHA-256, which was ~2.5 µs (~17%) of a ~14.5 µs signature.
fn sign_digest(sk: &SecretKey, digest: &[u8; 32], extra: Option<&[u8; 32]>) -> Signature {
    use secp256k1::ffi::{self, CPtr};
    let ndata = extra.map_or(std::ptr::null(), |e| e.as_ptr() as *const std::os::raw::c_void);
    // SAFETY: an all-zero signature buffer for libsecp256k1 to fill; the
    // global context can sign; `digest`, the secret key and the nonce data
    // (if any) are 32-byte buffers that outlive the call.
    let mut sig = unsafe { ffi::Signature::new() };
    let ok = unsafe {
        ffi::secp256k1_ecdsa_sign(
            SECP256K1.ctx().as_ptr(),
            &mut sig,
            digest.as_ptr(),
            sk.as_c_ptr(),
            Some(rfc6979_nonce),
            ndata,
        )
    };
    // fails only for an invalid secret key, which SecretKey rules out
    assert_eq!(ok, 1, "secp256k1_ecdsa_sign");
    Signature::from(sig)
}

/// `sig` (compact, as emitted) is a valid low-S signature by `pk` over
/// sha256(`data`), hashed here again rather than reusing the signer's digest.
fn verify_compact(pk: &PublicKey, data: &[u8], sig: &[u8; 64]) -> bool {
    let Ok(sig) = Signature::from_compact(sig) else { return false };
    let digest: [u8; 32] = Sha256::digest(data).into();
    SECP256K1.verify_ecdsa(&Message::from_digest(digest), &sig, pk).is_ok()
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
/// HMAC-DRBG seeded with key32 || (msg32 mod n) [|| data32], output number
/// `counter`. `data` is the hedge (RFC 6979 §3.6 additional data), appended
/// to the seed as the C function does. Calls with an algorithm tag (never
/// made here) go to the C function.
unsafe extern "C" fn rfc6979_nonce(
    nonce32: *mut std::os::raw::c_uchar,
    msg32: *const std::os::raw::c_uchar,
    key32: *const std::os::raw::c_uchar,
    algo16: *const std::os::raw::c_uchar,
    data: *mut std::os::raw::c_void,
    counter: std::os::raw::c_uint,
) -> std::os::raw::c_int {
    use secp256k1::ffi;
    if !algo16.is_null() {
        // SAFETY: forwards the caller's arguments unchanged
        return unsafe {
            match ffi::secp256k1_nonce_function_rfc6979 {
                Some(f) => f(nonce32, msg32, key32, algo16, data, counter),
                None => 0,
            }
        };
    }
    // SAFETY: libsecp256k1 passes 32-byte buffers for nonce32, msg32 and
    // key32; `data` is the signer's 32-byte nonce data (ndata) or null
    let (out, msg, key, extra) = unsafe {
        (
            &mut *(nonce32 as *mut [u8; 32]),
            &*(msg32 as *const [u8; 32]),
            &*(key32 as *const [u8; 32]),
            (data as *const [u8; 32]).as_ref(),
        )
    };
    let mut buf = [0u8; 96];
    buf[..32].copy_from_slice(key);
    buf[32..64].copy_from_slice(&reduce_mod_order(msg));
    let len = match extra {
        Some(e) => {
            buf[64..].copy_from_slice(e);
            96
        }
        None => 64,
    };
    // RFC 6979 3.2 b-g
    let mut v = [0x01u8; 32];
    let mut k = [0x00u8; 32];
    k = hmac_sha256(&k, &[&v, &[0x00], &buf[..len]]);
    v = hmac_sha256(&k, &[&v]);
    k = hmac_sha256(&k, &[&v, &[0x01], &buf[..len]]);
    v = hmac_sha256(&k, &[&v]);
    buf.fill(0);
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
    std::hint::black_box((&k, &v, &buf));
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

/// What a verified signature is for: the `purpose` label of
/// `vlpds_signature_verify_failures_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    /// A repo commit (firehose, getRepo, sync).
    Commit,
    /// An inter-service auth JWT (proxy, getServiceAuth).
    ServiceAuth,
    /// An OAuth access token (the server's ES256 key).
    OAuthToken,
    /// A loaded signing key whose scalar no longer derives its public key.
    KeyLoad,
}

impl Purpose {
    pub const ALL: [Purpose; 4] = [Purpose::Commit, Purpose::ServiceAuth, Purpose::OAuthToken, Purpose::KeyLoad];

    pub fn as_str(self) -> &'static str {
        match self {
            Purpose::Commit => "commit",
            Purpose::ServiceAuth => "service_auth",
            Purpose::OAuthToken => "oauth_token",
            Purpose::KeyLoad => "key_load",
        }
    }
}

/// A signature failed verification twice in a row and was not emitted.
/// Retryable: nothing was written or sent.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{purpose} signature failed verification after signing (suspected hardware fault); nothing was emitted, retry")]
pub struct SignatureFault {
    pub purpose: &'static str,
}

/// Verify failures within [`FAULT_WINDOW`] that fail-stop the node.
pub const FAULT_FAIL_STOP: usize = 3;
pub const FAULT_WINDOW: Duration = Duration::from_secs(60);
/// Exit code and reason of the signature-fault fail-stop (lifecycle.rs).
pub const FAULT_EXIT_CODE: i32 = 6;
pub const FAULT_REASON: &str = "signature_fault";

/// Recent verify failures (a sliding window).
#[derive(Default)]
pub struct FaultWindow {
    times: parking_lot::Mutex<VecDeque<Instant>>,
}

impl FaultWindow {
    /// Records a failure at `now`; returns the failures within [`FAULT_WINDOW`].
    pub fn record(&self, now: Instant) -> usize {
        let mut t = self.times.lock();
        while t.front().is_some_and(|&f| now.duration_since(f) >= FAULT_WINDOW) {
            t.pop_front();
        }
        t.push_back(now);
        t.len()
    }
}

static FAULTS: LazyLock<FaultWindow> = LazyLock::new(FaultWindow::default);

type FailStopHook = Arc<dyn Fn(&'static str) + Send + Sync>;
static FAIL_STOP_HOOK: parking_lot::RwLock<Option<FailStopHook>> = parking_lot::RwLock::new(None);

/// Tests: replaces the signature-fault fail-stop (process exit) with `f`.
#[doc(hidden)]
pub fn set_fail_stop_hook(f: Option<FailStopHook>) {
    *FAIL_STOP_HOOK.write() = f;
}

/// One signature that failed verification (never emitted): counts
/// `vlpds_signature_verify_failures_total{purpose}`, logs at error, and
/// fail-stops the node once [`FAULT_FAIL_STOP`] happened within
/// [`FAULT_WINDOW`].
pub fn record_fault(purpose: Purpose) {
    crate::metrics::SIGNATURE_VERIFY_FAILURES.with_label_values(&[purpose.as_str()]).inc();
    let recent = FAULTS.record(Instant::now());
    tracing::error!(
        purpose = purpose.as_str(),
        recent,
        "signature failed verification against the signing key's public key: suspected memory/CPU fault (not emitted)"
    );
    if recent >= FAULT_FAIL_STOP {
        tracing::error!(recent, "repeated signature faults: fail-stop (suspect this host's memory or CPU)");
        let hook = FAIL_STOP_HOOK.read().clone();
        match hook {
            Some(h) => h(FAULT_REASON),
            // unit tests inject faults in parallel: never exit the test binary
            None if cfg!(test) => {}
            None => crate::lifecycle::fail_stop(FAULT_EXIT_CODE, FAULT_REASON),
        }
    }
}

/// Exports every purpose's failure counter at 0 (so `increase()` sees the
/// first failure).
pub fn touch_metrics() {
    for p in Purpose::ALL {
        crate::metrics::SIGNATURE_VERIFY_FAILURES.with_label_values(&[p.as_str()]);
    }
}

/// Test-only fault injection: corrupts the next signatures made by one key
/// (named by its compressed SEC1 public key), so tests can prove a faulty
/// signature is never emitted. Costs one relaxed load per signature unless
/// a test armed it.
#[doc(hidden)]
pub mod fault {
    use super::*;

    /// Where the fault lands.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Fault {
        /// A bit of the finished signature flips.
        Signature,
        /// A bit of the secret scalar flips while signing (secp256k1 keys;
        /// P-256 treats it as `Signature`).
        Secret,
    }

    type Pending = std::collections::HashMap<Vec<u8>, (Fault, u32)>;
    static ARMED: AtomicBool = AtomicBool::new(false);
    static PENDING: LazyLock<parking_lot::Mutex<Pending>> = LazyLock::new(Default::default);

    /// Corrupts the next `n` signatures by the key `key_id` with `f`.
    pub fn inject(key_id: &[u8], f: Fault, n: u32) {
        let mut p = PENDING.lock();
        if n == 0 {
            p.remove(key_id);
        } else {
            p.insert(key_id.to_vec(), (f, n));
        }
        ARMED.store(!p.is_empty(), Ordering::Release);
    }

    /// Whether any fault is pending (lets callers skip computing a key id).
    pub fn armed() -> bool {
        ARMED.load(Ordering::Relaxed)
    }

    /// Whether (and how) to corrupt this signature by `key_id`.
    pub fn take(key_id: &[u8]) -> Option<Fault> {
        if !armed() {
            return None;
        }
        let mut p = PENDING.lock();
        let e = p.get_mut(key_id)?;
        let f = e.0;
        e.1 -= 1;
        if e.1 == 0 {
            p.remove(key_id);
        }
        ARMED.store(!p.is_empty(), Ordering::Release);
        Some(f)
    }
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
    fn c_nonce(msg: &[u8; 32], key: &[u8; 32], data: Option<&[u8; 32]>, counter: u32) -> [u8; 32] {
        let mut out = [0u8; 32];
        let f = unsafe { ffi::secp256k1_nonce_function_rfc6979 }.unwrap();
        let d = data.map_or(std::ptr::null_mut(), |d| d.as_ptr() as *mut std::os::raw::c_void);
        let r = unsafe { f(out.as_mut_ptr(), msg.as_ptr(), key.as_ptr(), std::ptr::null(), d, counter) };
        assert_eq!(r, 1);
        out
    }

    fn rust_nonce(msg: &[u8; 32], key: &[u8; 32], data: Option<&[u8; 32]>, counter: u32) -> [u8; 32] {
        let mut out = [0u8; 32];
        let d = data.map_or(std::ptr::null_mut(), |d| d.as_ptr() as *mut std::os::raw::c_void);
        let r = unsafe { rfc6979_nonce(out.as_mut_ptr(), msg.as_ptr(), key.as_ptr(), std::ptr::null(), d, counter) };
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

    /// Null and non-null nonce data (the hedge) alike.
    #[test]
    fn rfc6979_nonce_matches_libsecp256k1() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(6979);
        let mut msgs = edge_messages();
        for _ in 0..2000 {
            msgs.push(rng.r#gen());
        }
        for (i, msg) in msgs.iter().enumerate() {
            let key: [u8; 32] = if i % 3 == 0 { [0xff; 32] } else { rng.r#gen() };
            let data: [u8; 32] = if i % 5 == 0 { [0u8; 32] } else { rng.r#gen() };
            for counter in [0, 1, 2, 7] {
                assert_eq!(rust_nonce(msg, &key, None, counter), c_nonce(msg, &key, None, counter), "msg {i} counter {counter}");
                assert_eq!(
                    rust_nonce(msg, &key, Some(&data), counter),
                    c_nonce(msg, &key, Some(&data), counter),
                    "msg {i} counter {counter} with data"
                );
            }
            // the hedge changes the nonce
            assert_ne!(rust_nonce(msg, &key, None, 0), rust_nonce(msg, &key, Some(&data), 0));
        }
    }

    #[test]
    fn signatures_match_libsecp256k1_with_and_without_nonce_data() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(256);
        for i in 0..3000 {
            let kp = Keypair::generate();
            let mut data = vec![0u8; rng.gen_range(0..400)];
            rng.fill(&mut data[..]);
            let digest: [u8; 32] = Sha256::digest(&data).into();
            let mut want = SECP256K1.sign_ecdsa(&Message::from_digest(digest), &kp.sk);
            want.normalize_s();
            assert_eq!(kp.sign_deterministic(&data), want.serialize_compact(), "signature {i}");
            let nd: [u8; 32] = rng.r#gen();
            let mut want = SECP256K1.sign_ecdsa_with_noncedata(&Message::from_digest(digest), &kp.sk, &nd);
            want.normalize_s();
            let mut got = sign_digest(&kp.sk, &digest, Some(&nd));
            got.normalize_s();
            assert_eq!(got, want, "hedged signature {i}");
        }
        // digests at and above the group order
        let kp = Keypair::generate();
        for d in edge_messages() {
            let mut want = SECP256K1.sign_ecdsa(&Message::from_digest(d), &kp.sk);
            want.normalize_s();
            let mut got = sign_digest(&kp.sk, &d, None);
            got.normalize_s();
            assert_eq!(got, want);
        }
    }

    /// Hedged signatures differ every time, are low-S and verify.
    #[test]
    fn hedged_signatures_are_fresh_low_s_and_valid() {
        let kp = Keypair::generate();
        let pk = kp.public_key_sec1();
        let mut seen = std::collections::HashSet::new();
        for i in 0..500u32 {
            let msg = format!("same message {}", i % 5);
            for sig in [kp.sign(msg.as_bytes()), kp.sign_verified(Purpose::Commit, msg.as_bytes()).unwrap()] {
                assert!(seen.insert(sig), "signature repeated");
                assert!(verify_k256(&pk, msg.as_bytes(), &sig).unwrap());
                let mut s = Signature::from_compact(&sig).unwrap();
                let before = s;
                s.normalize_s();
                assert_eq!(s, before, "high-S signature");
            }
            assert_ne!(kp.sign(msg.as_bytes()), kp.sign_deterministic(msg.as_bytes()));
        }
    }

    fn failures(p: Purpose) -> u64 {
        crate::metrics::SIGNATURE_VERIFY_FAILURES.with_label_values(&[p.as_str()]).get()
    }

    /// A corrupted signature (or one made with a flipped scalar) is never
    /// returned: one fault is retried with a fresh nonce, two fail.
    #[test]
    fn injected_faults_are_caught_and_retried_once() {
        for f in [fault::Fault::Signature, fault::Fault::Secret] {
            let kp = Keypair::generate();
            let pk = kp.public_key_sec1();
            let before = failures(Purpose::ServiceAuth);
            fault::inject(&pk, f, 1);
            let sig = kp.sign_verified(Purpose::ServiceAuth, b"payload").unwrap();
            assert!(verify_k256(&pk, b"payload", &sig).unwrap());
            fault::inject(&pk, f, 2);
            let e = kp.sign_verified(Purpose::ServiceAuth, b"payload").unwrap_err();
            assert_eq!(e.purpose, "service_auth");
            // other tests may count too, never fewer than ours
            assert!(failures(Purpose::ServiceAuth) >= before + 3, "{f:?}");
            assert!(fault::take(&pk).is_none());
            kp.sign_verified(Purpose::ServiceAuth, b"payload").unwrap();
        }
    }

    #[test]
    fn fault_window_counts_the_last_minute() {
        let w = FaultWindow::default();
        let t = Instant::now();
        assert_eq!(w.record(t), 1);
        assert_eq!(w.record(t + Duration::from_secs(30)), 2);
        assert_eq!(w.record(t + Duration::from_secs(59)), 3);
        // the first has aged out
        assert_eq!(w.record(t + Duration::from_secs(61)), 3);
        assert_eq!(w.record(t + Duration::from_secs(200)), 1);
    }

    #[test]
    fn matches_public_checks_the_scalar() {
        let kp = Keypair::generate();
        let mb = kp.public_multibase();
        assert!(kp.matches_public(&mb));
        assert!(kp.matches_public(""));
        assert!(!kp.matches_public(&Keypair::generate().public_multibase()));
        // a scalar that changed after its public key was cached
        let mut b = kp.sk.secret_bytes();
        b[0] ^= 1;
        let flipped = Keypair { sk: SecretKey::from_byte_array(&b).unwrap(), pk: OnceLock::from(*kp.public_key()) };
        assert!(!flipped.matches_public(&mb));
        assert!(!flipped.matches_public(""));
    }

    /// Per-signature cost: the deterministic signature (before), hedged
    /// (+ CSPRNG bytes, longer HMAC seed), hedged + verify-after-sign (what
    /// commits and service-auth JWTs pay), and the CSPRNG and verify alone.
    /// Interleaved rounds; median and range per variant.
    /// `cargo test --release --lib bench_sign -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_sign() {
        let kp = Keypair::generate();
        let _ = kp.public_key();
        // a commit object's size
        let msg: Vec<u8> = (0..150u8).collect();
        let n = 20_000u32;
        let rounds: usize = std::env::var("BENCH_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(9);
        let names = ["deterministic sign", "hedged sign", "hedged sign + verify", "CSPRNG 32 B", "verify alone"];
        let sig = kp.sign(&msg);
        let mut res: Vec<Vec<f64>> = vec![Vec::new(); names.len()];
        let mut m = msg.clone();
        for _ in 0..rounds {
            for (v, r) in res.iter_mut().enumerate() {
                let t = Instant::now();
                for i in 0..n {
                    m[0] = i as u8;
                    match v {
                        0 => drop(std::hint::black_box(kp.sign_deterministic(&m))),
                        1 => drop(std::hint::black_box(kp.sign(&m))),
                        2 => drop(std::hint::black_box(kp.sign_verified(Purpose::Commit, &m).unwrap())),
                        3 => {
                            let mut b = [0u8; 32];
                            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut b);
                            std::hint::black_box(b);
                        }
                        _ => drop(std::hint::black_box(verify_compact(kp.public_key(), &msg, &sig))),
                    }
                }
                r.push(t.elapsed().as_nanos() as f64 / n as f64);
            }
        }
        for (name, mut r) in names.iter().zip(res) {
            r.sort_by(f64::total_cmp);
            println!("bench_sign {name:>22}: median {:>8.0} ns  (min {:.0}, max {:.0}, {rounds} rounds x {n})", r[r.len() / 2], r[0], r[r.len() - 1]);
        }
    }
}
