//! Secrets at rest (DESIGN.md "Secrets at rest"): key material that must be
//! recoverable (repo signing keys, reserved signing keys, TOTP secrets) is
//! stored in the bucket only wrapped under a key-encryption key (KEK).
//!
//! A [`KeyWrapper`] holds one KEK and wraps/unwraps a secret with
//! authenticated data binding it to its purpose and subject (a DID or
//! did:key), so a wrapped blob copied into another account's row, or used
//! for another purpose, does not unwrap. Backends:
//! - [`LocalKek`]: 32 random bytes from `--kek-file` / `VLPDS_KEK`,
//!   XChaCha20-Poly1305 with a random 192-bit nonce per wrap. Dev mode
//!   falls back to a well-known [`dev_kek`] (refused outside dev mode).
//! - [`GcpKms`]: Google Cloud KMS `encrypt`/`decrypt` over its REST API
//!   with the node's service-account token (metadata server). The secret
//!   itself is the KMS plaintext (32 bytes), so a bucket copy is useless
//!   without KMS decrypt permission, and every unwrap is in KMS audit logs.
//!
//! Wrapped form (a string, so it sits in JSON rows): `vw1.{kid}.{b64url}`.
//! `kid` names the KEK (`L` + 16 hex for a local key: a hash of it; `G` +
//! 16 hex for a Cloud KMS CryptoKey: a hash of its resource name). The
//! [`Secrets`] keyring wraps under its first (current) KEK and unwraps under
//! any configured one, so a KEK rotates by adding the new key as current,
//! keeping the old one for unwrap, and rewrapping (`vlpds.admin.rewrapSecrets`).
//!
//! Unwrapped signing keys are cached per DID ([`Secrets::signing_key`];
//! bounded by the `signing_keys` cache cap, validated by the account's
//! public key, zeroized when the last reference drops), so a KMS round trip
//! happens at most once per account per cache lifetime and never on the
//! commit path of a loaded repo. Remote unwraps are limited
//! (`--kms-concurrency`), coalesced per DID, time out after
//! [`KMS_TIMEOUT`], and fail fast for [`KMS_BACKOFF`] after an outage is
//! seen: the caller gets [`SecretError::Unavailable`] (a retryable 503).

use crate::crypto::Keypair;
use async_trait::async_trait;
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use zeroize::{Zeroize, Zeroizing};

/// Version tag of the wrapped form.
const WRAP_VERSION: &str = "vw1";
/// One KMS request (token fetch included) gives up after this.
pub const KMS_TIMEOUT: Duration = Duration::from_secs(5);
/// After a KMS request fails as unavailable, remote unwraps fail at once for
/// this long (one probe per interval goes through), so an outage doesn't
/// queue every cold write behind a timeout.
pub const KMS_BACKOFF: Duration = Duration::from_secs(1);
/// Default remote (KMS) calls in flight per node (`--kms-concurrency`).
pub const DEFAULT_KMS_CONCURRENCY: usize = 64;

static KMS_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "vlpds_kms_requests_total",
        "Key-encryption-key operations by backend (local, gcpkms), op (wrap, unwrap) and result (ok, unavailable, rejected)",
        &["backend", "op", "result"]
    )
    .unwrap()
});
static KMS_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "vlpds_kms_request_seconds",
        "Key-encryption-key operation latency by backend and op",
        &["backend", "op"],
        prometheus::exponential_buckets(0.00001, 2.0, 20).unwrap()
    )
    .unwrap()
});
static KEY_CACHE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "vlpds_signing_key_cache_total",
        "Unwrapped signing-key cache lookups (hit, miss) and unwraps that failed (unavailable, rejected)",
        &["result"]
    )
    .unwrap()
});

#[derive(Debug, Clone, thiserror::Error)]
pub enum SecretError {
    /// The KEK's service can't be reached (timeout, 5xx, auth): retry later.
    #[error("key service unavailable: {0}")]
    Unavailable(String),
    /// Authentication failed: wrong KEK, wrong purpose/subject, or corrupt.
    #[error("wrapped secret rejected: {0}")]
    Rejected(String),
    /// Wrapped under a KEK this node isn't configured with.
    #[error("wrapped under unknown key-encryption key {0}")]
    UnknownKek(String),
    #[error("malformed wrapped secret")]
    Malformed,
}

impl SecretError {
    pub fn retryable(&self) -> bool {
        matches!(self, SecretError::Unavailable(_))
    }
}

/// What a wrapped secret is for; part of its authenticated data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    /// An account's repo signing key (subject: the account DID).
    SigningKey,
    /// A key reserved with `server.reserveSigningKey` (subject: its did:key).
    ReservedKey,
    /// A TOTP shared secret (subject: the account DID).
    Totp,
}

impl Purpose {
    pub fn label(self) -> &'static str {
        match self {
            Purpose::SigningKey => "repo-signing-key",
            Purpose::ReservedKey => "reserved-signing-key",
            Purpose::Totp => "totp-secret",
        }
    }
}

/// Authenticated data of a wrapped secret: version, purpose and subject.
pub fn aad(purpose: Purpose, subject: &str) -> Vec<u8> {
    [b"vlpds-secret-v1\0", purpose.label().as_bytes(), b"\0", subject.as_bytes()].concat()
}

/// An unwrapped secret. `stale`: wrapped under a KEK (or KEK version) other
/// than the current one, so a rewrap would change it.
pub struct Unwrapped {
    pub plaintext: Zeroizing<Vec<u8>>,
    pub stale: bool,
}

/// One key-encryption key.
#[async_trait]
pub trait KeyWrapper: Send + Sync {
    /// Short id stored with every blob it wraps.
    fn kid(&self) -> &str;
    /// Metrics label.
    fn backend(&self) -> &'static str;
    /// Whether wrap/unwrap is a network round trip (limited and cached).
    fn remote(&self) -> bool;
    async fn wrap(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError>;
    /// `stale` = the KEK has a newer primary version than the one used.
    async fn unwrap(&self, aad: &[u8], ciphertext: &[u8]) -> Result<Unwrapped, SecretError>;
}

// ---------------------------------------------------------------------------
// local KEK
// ---------------------------------------------------------------------------

/// A 32-byte key-encryption key, zeroized on drop; never printed.
#[derive(Clone, zeroize::ZeroizeOnDrop)]
pub struct KekBytes([u8; 32]);

impl std::fmt::Debug for KekBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KekBytes({})", local_kid(&self.0))
    }
}

impl PartialEq for KekBytes {
    fn eq(&self, o: &KekBytes) -> bool {
        self.0.iter().zip(o.0.iter()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0
    }
}

impl KekBytes {
    pub fn new(b: [u8; 32]) -> KekBytes {
        KekBytes(b)
    }

    pub fn random() -> KekBytes {
        KekBytes(rand::random())
    }

    /// 64 hex chars or base64 (standard or url-safe, padded or not) of 32
    /// bytes; surrounding whitespace ignored.
    pub fn parse(s: &str) -> anyhow::Result<KekBytes> {
        let s = Zeroizing::new(s.trim().to_string());
        let mut raw = Zeroizing::new(
            if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
                hex::decode(s.as_bytes())?
            } else {
                let b64 = s.trim_end_matches('=');
                base64::engine::general_purpose::STANDARD_NO_PAD
                    .decode(b64)
                    .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(b64))
                    .map_err(|_| anyhow::anyhow!("KEK must be 32 bytes as 64 hex chars or base64"))?
            },
        );
        anyhow::ensure!(raw.len() == 32, "KEK must be 32 bytes (got {})", raw.len());
        let mut k = [0u8; 32];
        k.copy_from_slice(&raw);
        raw.zeroize();
        Ok(KekBytes(k))
    }

    /// A KEK file: exactly 32 raw bytes, or the text forms of [`parse`](Self::parse).
    pub fn from_file(path: &std::path::Path) -> anyhow::Result<KekBytes> {
        let b = Zeroizing::new(std::fs::read(path).map_err(|e| anyhow::anyhow!("reading KEK file {}: {e}", path.display()))?);
        if b.len() == 32 {
            let mut k = [0u8; 32];
            k.copy_from_slice(&b);
            return Ok(KekBytes(k));
        }
        let s = std::str::from_utf8(&b).map_err(|_| anyhow::anyhow!("KEK file {} is neither 32 raw bytes nor text", path.display()))?;
        KekBytes::parse(s)
    }

    pub fn kid(&self) -> String {
        local_kid(&self.0)
    }
}

fn local_kid(k: &[u8; 32]) -> String {
    let h = Sha256::digest([b"vlpds-kek-id\0".as_slice(), k].concat());
    format!("L{}", hex::encode(&h[..8]))
}

/// The well-known dev-mode KEK (derived from a public string: it protects
/// nothing, and is refused outside dev mode).
pub fn dev_kek() -> KekBytes {
    KekBytes(Sha256::digest(b"vlpds dev-mode KEK: not a secret").into())
}

/// XChaCha20-Poly1305 under a local KEK: `nonce (24) ‖ ciphertext ‖ tag`.
pub struct LocalKek {
    kid: String,
    aead: XChaCha20Poly1305,
}

impl LocalKek {
    pub fn new(k: &KekBytes) -> LocalKek {
        LocalKek { kid: k.kid(), aead: XChaCha20Poly1305::new((&k.0).into()) }
    }

    pub fn wrap_sync(&self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let nonce: [u8; 24] = rand::random();
        let ct = self
            .aead
            .encrypt(&XNonce::from(nonce), Payload { msg: plaintext, aad })
            .expect("xchacha20poly1305 encrypt");
        [nonce.as_slice(), &ct].concat()
    }

    pub fn unwrap_sync(&self, aad: &[u8], ct: &[u8]) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        if ct.len() < 24 + 16 {
            return Err(SecretError::Malformed);
        }
        let (nonce, body) = ct.split_at(24);
        let nonce: [u8; 24] = nonce.try_into().expect("24 bytes");
        self.aead
            .decrypt(&XNonce::from(nonce), Payload { msg: body, aad })
            .map(Zeroizing::new)
            .map_err(|_| SecretError::Rejected(format!("authentication failed under {}", self.kid)))
    }
}

#[async_trait]
impl KeyWrapper for LocalKek {
    fn kid(&self) -> &str {
        &self.kid
    }
    fn backend(&self) -> &'static str {
        "local"
    }
    fn remote(&self) -> bool {
        false
    }
    async fn wrap(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        Ok(self.wrap_sync(aad, plaintext))
    }
    async fn unwrap(&self, aad: &[u8], ct: &[u8]) -> Result<Unwrapped, SecretError> {
        Ok(Unwrapped { plaintext: self.unwrap_sync(aad, ct)?, stale: false })
    }
}

// ---------------------------------------------------------------------------
// Google Cloud KMS
// ---------------------------------------------------------------------------

/// Where [`GcpKms`] gets OAuth access tokens.
#[derive(Clone, Debug)]
pub enum GcpToken {
    /// The GCE/GKE metadata server (the node's service account). The URL is
    /// the token endpoint; `GCE_METADATA_HOST` overrides its host.
    Metadata(String),
    /// A fixed bearer token (tests, or a short-lived token for an operator
    /// running an admin task off-cluster).
    Static(String),
}

impl Default for GcpToken {
    fn default() -> GcpToken {
        let host = std::env::var("GCE_METADATA_HOST").unwrap_or_else(|_| "metadata.google.internal".into());
        GcpToken::Metadata(format!("http://{host}/computeMetadata/v1/instance/service-accounts/default/token"))
    }
}

pub const GCP_KMS_ENDPOINT: &str = "https://cloudkms.googleapis.com";

/// Cloud KMS symmetric `encrypt`/`decrypt` on one CryptoKey
/// (`projects/P/locations/L/keyRings/R/cryptoKeys/K`). KMS picks the
/// primary version to encrypt and finds the version from the ciphertext to
/// decrypt, so rotating versions inside the CryptoKey needs no config
/// change; `usedPrimary: false` on decrypt marks the blob stale.
pub struct GcpKms {
    kid: String,
    name: String,
    endpoint: String,
    token: GcpToken,
    http: reqwest::Client,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl GcpKms {
    pub fn new(name: &str, endpoint: &str, token: GcpToken) -> anyhow::Result<GcpKms> {
        anyhow::ensure!(
            name.starts_with("projects/") && name.contains("/cryptoKeys/") && !name.contains("/cryptoKeyVersions/"),
            "Cloud KMS key must be projects/P/locations/L/keyRings/R/cryptoKeys/K (no version): {name}"
        );
        let h = Sha256::digest(name.as_bytes());
        Ok(GcpKms {
            kid: format!("G{}", hex::encode(&h[..8])),
            name: name.to_string(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token,
            http: crate::http::public().clone(),
            cached: tokio::sync::Mutex::new(None),
        })
    }

    async fn access_token(&self, refresh: bool) -> Result<String, SecretError> {
        let url = match &self.token {
            GcpToken::Static(t) => return Ok(t.clone()),
            GcpToken::Metadata(url) => url,
        };
        let mut g = self.cached.lock().await;
        if let Some((t, exp)) = g.as_ref() {
            if !refresh && Instant::now() < *exp {
                return Ok(t.clone());
            }
        }
        #[derive(serde::Deserialize)]
        struct Tok {
            access_token: String,
            expires_in: u64,
        }
        let r = self
            .http
            .get(url)
            .header("Metadata-Flavor", "Google")
            .timeout(KMS_TIMEOUT)
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("metadata token: {e}")))?;
        if !r.status().is_success() {
            return Err(SecretError::Unavailable(format!("metadata token: HTTP {}", r.status())));
        }
        let t: Tok = r.json().await.map_err(|e| SecretError::Unavailable(format!("metadata token: {e}")))?;
        // refresh a minute early (tokens last ~1 h)
        let exp = Instant::now() + Duration::from_secs(t.expires_in.saturating_sub(60).max(1));
        *g = Some((t.access_token.clone(), exp));
        Ok(t.access_token)
    }

    async fn call(&self, op: &str, body: serde_json::Value) -> Result<serde_json::Value, SecretError> {
        let url = format!("{}/v1/{}:{op}", self.endpoint, self.name);
        for attempt in 0..2 {
            let token = self.access_token(attempt > 0).await?;
            let r = self
                .http
                .post(&url)
                .bearer_auth(token)
                .json(&body)
                .timeout(KMS_TIMEOUT)
                .send()
                .await
                .map_err(|e| SecretError::Unavailable(format!("cloud kms {op}: {e}")))?;
            let status = r.status();
            if status.is_success() {
                return r.json().await.map_err(|e| SecretError::Unavailable(format!("cloud kms {op}: {e}")));
            }
            let text = r.text().await.unwrap_or_default();
            match status.as_u16() {
                // an expired token: refresh once
                401 if attempt == 0 && matches!(self.token, GcpToken::Metadata(_)) => continue,
                // wrong AAD, corrupt ciphertext, or a ciphertext of another key
                400 => return Err(SecretError::Rejected(format!("cloud kms {op}: {}", truncate(&text)))),
                _ => return Err(SecretError::Unavailable(format!("cloud kms {op}: HTTP {status}: {}", truncate(&text)))),
            }
        }
        Err(SecretError::Unavailable(format!("cloud kms {op}: unauthorized")))
    }
}

fn truncate(s: &str) -> &str {
    &s[..s.floor_char_boundary(300)]
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

fn crc32c(b: &[u8]) -> u32 {
    // Castagnoli, bitwise (tiny inputs only)
    let mut c = !0u32;
    for &x in b {
        c ^= x as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 { (c >> 1) ^ 0x82F6_3B78 } else { c >> 1 };
        }
    }
    !c
}

#[async_trait]
impl KeyWrapper for GcpKms {
    fn kid(&self) -> &str {
        &self.kid
    }
    fn backend(&self) -> &'static str {
        "gcpkms"
    }
    fn remote(&self) -> bool {
        true
    }
    async fn wrap(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        let pt = Zeroizing::new(b64(plaintext));
        let body = serde_json::json!({
            "plaintext": &*pt,
            "additionalAuthenticatedData": b64(aad),
            "plaintextCrc32c": crc32c(plaintext).to_string(),
            "additionalAuthenticatedDataCrc32c": crc32c(aad).to_string(),
        });
        let r = self.call("encrypt", body).await?;
        if r.get("verifiedPlaintextCrc32c").and_then(|v| v.as_bool()) == Some(false) {
            return Err(SecretError::Unavailable("cloud kms encrypt: plaintext checksum not verified".into()));
        }
        let ct = r["ciphertext"].as_str().ok_or_else(|| SecretError::Unavailable("cloud kms encrypt: no ciphertext".into()))?;
        base64::engine::general_purpose::STANDARD
            .decode(ct)
            .map_err(|_| SecretError::Unavailable("cloud kms encrypt: bad ciphertext".into()))
    }
    async fn unwrap(&self, aad: &[u8], ct: &[u8]) -> Result<Unwrapped, SecretError> {
        let body = serde_json::json!({
            "ciphertext": b64(ct),
            "additionalAuthenticatedData": b64(aad),
            "ciphertextCrc32c": crc32c(ct).to_string(),
            "additionalAuthenticatedDataCrc32c": crc32c(aad).to_string(),
        });
        let mut r = self.call("decrypt", body).await?;
        let stale = r.get("usedPrimary").and_then(|v| v.as_bool()) == Some(false);
        let pt = match r.get_mut("plaintext").map(serde_json::Value::take) {
            Some(serde_json::Value::String(s)) => Zeroizing::new(s),
            // an empty plaintext is omitted from the JSON
            _ => Zeroizing::new(String::new()),
        };
        let plaintext = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(pt.as_bytes())
                .map_err(|_| SecretError::Unavailable("cloud kms decrypt: bad plaintext".into()))?,
        );
        Ok(Unwrapped { plaintext, stale })
    }
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

/// KEK configuration (flags in main.rs). The current KEK wraps; every
/// configured one unwraps. Current = the Cloud KMS key if set, else the
/// local KEK, else (dev mode only) [`dev_kek`].
#[derive(Clone, Debug, Default)]
pub struct KekConfig {
    /// `--kek-file` / `VLPDS_KEK`.
    pub local: Option<KekBytes>,
    /// `--kek-old-file` / `VLPDS_KEK_OLD`: unwrap only (rotation).
    pub local_old: Vec<KekBytes>,
    /// `--gcp-kms-key`: a CryptoKey resource name.
    pub gcp_key: Option<String>,
    /// `--gcp-kms-old-key`: unwrap only (moving to another CryptoKey).
    pub gcp_old_keys: Vec<String>,
    /// Cloud KMS API base (`--gcp-kms-endpoint`; tests point it at a mock).
    pub gcp_endpoint: Option<String>,
    pub gcp_token: Option<GcpToken>,
    /// Remote KEK calls in flight (`--kms-concurrency`; 0 = default).
    pub kms_concurrency: usize,
}

impl KekConfig {
    /// Startup check: outside dev mode a real KEK must be configured.
    pub fn check(&self, dev_mode: bool) -> anyhow::Result<()> {
        if dev_mode {
            return Ok(());
        }
        anyhow::ensure!(
            self.local.is_some() || self.gcp_key.is_some(),
            "a key-encryption key is required outside --dev-mode: set --kek-file / VLPDS_KEK (32 random bytes) or --gcp-kms-key"
        );
        let dev = dev_kek();
        anyhow::ensure!(
            self.local.as_ref() != Some(&dev) && !self.local_old.contains(&dev),
            "the dev-mode KEK is not accepted outside --dev-mode"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// keyring + signing-key cache
// ---------------------------------------------------------------------------

const CACHE_SHARDS: usize = 16;

/// Unwrapped signing keys by DID, with the public key they were validated
/// against. Bounded by `caches::cap(Cache::SigningKeys)` (LRU per shard).
struct KeyCache {
    shards: Vec<KeyShard>,
}

/// DID -> (public multibase, unwrapped key).
type KeyShard = parking_lot::Mutex<lru::LruCache<Arc<str>, (Arc<str>, Arc<Keypair>)>>;

impl KeyCache {
    fn new() -> KeyCache {
        KeyCache { shards: (0..CACHE_SHARDS).map(|_| parking_lot::Mutex::new(lru::LruCache::unbounded())).collect() }
    }

    fn shard(&self, did: &str) -> &KeyShard {
        &self.shards[(crate::state::did_hash(did) % CACHE_SHARDS as u64) as usize]
    }

    fn get(&self, did: &str, pubkey: &str) -> Option<Arc<Keypair>> {
        let mut s = self.shard(did).lock();
        match s.get(did) {
            Some((pk, k)) if &**pk == pubkey => Some(k.clone()),
            _ => None,
        }
    }

    fn put(&self, did: &str, pubkey: &str, key: Arc<Keypair>) {
        let cap = (crate::caches::cap(crate::caches::Cache::SigningKeys) / CACHE_SHARDS).max(1);
        let mut s = self.shard(did).lock();
        s.put(did.into(), (pubkey.into(), key));
        while s.len() > cap {
            s.pop_lru();
        }
    }

    fn remove(&self, did: &str) {
        self.shard(did).lock().pop(did);
    }

    fn clear(&self) {
        for s in &self.shards {
            s.lock().clear();
        }
    }
}

impl crate::caches::Len for KeyCache {
    fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }
}

/// The node's keyring: the KEKs, the signing-key cache, and the limits on
/// remote calls. One per server (`App::secrets`, the repo workers).
pub struct Secrets {
    /// `[0]` wraps; all unwrap (by kid).
    wrappers: Vec<Arc<dyn KeyWrapper>>,
    dev_kek: bool,
    keys: Arc<KeyCache>,
    permits: tokio::sync::Semaphore,
    /// Coalesces concurrent cold unwraps of one DID (striped).
    stripes: Vec<tokio::sync::Mutex<()>>,
    /// Remote unwraps fail fast until this (micros since `epoch`).
    down_until: AtomicU64,
    epoch: Instant,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Secrets").field("kids", &self.wrappers.iter().map(|w| w.kid()).collect::<Vec<_>>()).finish()
    }
}

impl Secrets {
    /// `wrappers[0]` is the current KEK.
    pub fn new(wrappers: Vec<Arc<dyn KeyWrapper>>, kms_concurrency: usize) -> anyhow::Result<Secrets> {
        anyhow::ensure!(!wrappers.is_empty(), "no key-encryption key");
        let n = if kms_concurrency == 0 { DEFAULT_KMS_CONCURRENCY } else { kms_concurrency };
        let keys = crate::caches::track(crate::caches::Cache::SigningKeys, Arc::new(KeyCache::new()));
        Ok(Secrets {
            wrappers,
            dev_kek: false,
            keys,
            permits: tokio::sync::Semaphore::new(n),
            stripes: (0..256).map(|_| tokio::sync::Mutex::new(())).collect(),
            down_until: AtomicU64::new(0),
            epoch: Instant::now(),
        })
    }

    /// The keyring `cfg` describes (see [`KekConfig`]). With no KEK
    /// configured it wraps under [`dev_kek`]: the binary refuses that
    /// outside dev mode first (`Config::check_secrets` -> [`KekConfig::check`]);
    /// in-process tests may run non-dev servers without a KEK.
    pub fn from_config(cfg: &KekConfig, dev_mode: bool) -> anyhow::Result<Secrets> {
        let endpoint = cfg.gcp_endpoint.as_deref().unwrap_or(GCP_KMS_ENDPOINT);
        let token = cfg.gcp_token.clone().unwrap_or_default();
        let mut ws: Vec<Arc<dyn KeyWrapper>> = Vec::new();
        if let Some(k) = &cfg.gcp_key {
            ws.push(Arc::new(GcpKms::new(k, endpoint, token.clone())?));
        }
        let mut dev = false;
        match &cfg.local {
            Some(k) => ws.push(Arc::new(LocalKek::new(k))),
            None if ws.is_empty() => {
                dev = true;
                ws.push(Arc::new(LocalKek::new(&dev_kek())));
            }
            None => {}
        }
        for k in &cfg.gcp_old_keys {
            ws.push(Arc::new(GcpKms::new(k, endpoint, token.clone())?));
        }
        for k in &cfg.local_old {
            ws.push(Arc::new(LocalKek::new(k)));
        }
        if dev_mode && !dev {
            // dev clusters keep reading state written under the dev KEK
            ws.push(Arc::new(LocalKek::new(&dev_kek())));
        }
        let mut seen = std::collections::HashSet::new();
        ws.retain(|w| seen.insert(w.kid().to_string()));
        let mut s = Secrets::new(ws, cfg.kms_concurrency)?;
        s.dev_kek = dev;
        Ok(s)
    }

    /// A keyring with only the dev KEK (tests, tools).
    pub fn dev() -> Arc<Secrets> {
        static DEV: LazyLock<Arc<Secrets>> = LazyLock::new(|| {
            let mut s = Secrets::new(vec![Arc::new(LocalKek::new(&dev_kek()))], 0).expect("dev keyring");
            s.dev_kek = true;
            Arc::new(s)
        });
        DEV.clone()
    }

    /// Whether this keyring wraps under the well-known dev KEK.
    pub fn is_dev(&self) -> bool {
        self.dev_kek
    }

    pub fn current_kid(&self) -> &str {
        self.wrappers[0].kid()
    }

    pub fn kids(&self) -> Vec<String> {
        self.wrappers.iter().map(|w| w.kid().to_string()).collect()
    }

    fn wrapper(&self, kid: &str) -> Option<&Arc<dyn KeyWrapper>> {
        self.wrappers.iter().find(|w| w.kid() == kid)
    }

    fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    /// Runs one KEK operation with metrics; remote ones under the permit
    /// limit and the outage backoff.
    async fn run<T>(
        &self,
        w: &Arc<dyn KeyWrapper>,
        op: &'static str,
        f: impl std::future::Future<Output = Result<T, SecretError>>,
    ) -> Result<T, SecretError> {
        let t = Instant::now();
        // whether the key service itself answered (or timed out): only that
        // starts a backoff window, never a fast-fail or a full queue
        let mut called = false;
        let r = if w.remote() {
            let now = self.now_us();
            let until = self.down_until.load(Ordering::Relaxed);
            if now < until {
                Err(SecretError::Unavailable("key service recently unavailable; backing off".into()))
            } else {
                match tokio::time::timeout(KMS_TIMEOUT, self.permits.acquire()).await {
                    Err(_) => Err(SecretError::Unavailable("too many key service calls queued".into())),
                    Ok(p) => {
                        let _p = p.expect("permits never closed");
                        called = true;
                        match tokio::time::timeout(KMS_TIMEOUT, f).await {
                            Ok(r) => r,
                            Err(_) => Err(SecretError::Unavailable(format!("{} {op} timed out", w.backend()))),
                        }
                    }
                }
            }
        } else {
            f.await
        };
        let result = match &r {
            Ok(_) => "ok",
            Err(SecretError::Unavailable(_)) => "unavailable",
            Err(_) => "rejected",
        };
        KMS_REQUESTS.with_label_values(&[w.backend(), op, result]).inc();
        KMS_SECONDS.with_label_values(&[w.backend(), op]).observe(t.elapsed().as_secs_f64());
        if let Err(SecretError::Unavailable(e)) = &r {
            if called {
                let now = self.now_us();
                let prev = self.down_until.swap(now + KMS_BACKOFF.as_micros() as u64, Ordering::Relaxed);
                // one log line per backoff window, not per request
                if prev <= now {
                    tracing::warn!(kid = w.kid(), backend = w.backend(), op, "key service unavailable: {e}");
                }
            }
        }
        r
    }

    /// Wraps `plaintext` for `purpose`/`subject` under the current KEK.
    pub async fn wrap(&self, purpose: Purpose, subject: &str, plaintext: &[u8]) -> Result<String, SecretError> {
        let w = &self.wrappers[0];
        let a = aad(purpose, subject);
        let ct = self.run(w, "wrap", w.wrap(&a, plaintext)).await?;
        Ok(format!("{WRAP_VERSION}.{}.{}", w.kid(), base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ct)))
    }

    /// Unwraps a blob from [`wrap`](Self::wrap) under whichever configured
    /// KEK made it.
    pub async fn unwrap(&self, purpose: Purpose, subject: &str, blob: &str) -> Result<Unwrapped, SecretError> {
        let (kid, ct) = parse_blob(blob)?;
        let w = self.wrapper(kid).ok_or_else(|| SecretError::UnknownKek(kid.to_string()))?;
        let a = aad(purpose, subject);
        let mut u = self.run(w, "unwrap", w.unwrap(&a, &ct)).await?;
        u.stale |= kid != self.current_kid();
        Ok(u)
    }

    /// Whether `blob` is under the current KEK's id (a cheap pre-filter for
    /// rewraps; a Cloud KMS version rotation only shows on unwrap).
    pub fn is_current(&self, blob: &str) -> bool {
        parse_blob(blob).is_ok_and(|(kid, _)| kid == self.current_kid())
    }

    /// Unwrap + wrap under the current KEK. None if already current
    /// (and, for Cloud KMS, under the primary version).
    pub async fn rewrap(&self, purpose: Purpose, subject: &str, blob: &str) -> Result<Option<String>, SecretError> {
        let u = self.unwrap(purpose, subject, blob).await?;
        if !u.stale {
            return Ok(None);
        }
        Ok(Some(self.wrap(purpose, subject, &u.plaintext).await?))
    }

    // ---- signing keys ----

    /// Wraps a new or rotated signing key of `did` and caches it: (wrapped,
    /// public multibase).
    pub async fn wrap_signing_key(&self, did: &str, key: &Arc<Keypair>) -> Result<(String, String), SecretError> {
        let raw = Zeroizing::new(key.to_bytes());
        let wrapped = self.wrap(Purpose::SigningKey, did, &raw).await?;
        let pubkey = key.public_multibase();
        self.keys.put(did, &pubkey, key.clone());
        Ok((wrapped, pubkey))
    }

    /// The cached signing key of `did` if it matches `pubkey` (no unwrap).
    pub fn cached_signing_key(&self, did: &str, pubkey: &str) -> Option<Arc<Keypair>> {
        self.keys.get(did, pubkey)
    }

    /// `did`'s signing key: from the cache, else unwrapped (one call per DID
    /// at a time) and checked against `pubkey`.
    pub async fn signing_key(&self, did: &str, wrapped: &str, pubkey: &str) -> Result<Arc<Keypair>, SecretError> {
        if let Some(k) = self.keys.get(did, pubkey) {
            KEY_CACHE.with_label_values(&["hit"]).inc();
            return Ok(k);
        }
        let _g = self.stripes[(crate::state::did_hash(did) % self.stripes.len() as u64) as usize].lock().await;
        if let Some(k) = self.keys.get(did, pubkey) {
            KEY_CACHE.with_label_values(&["hit"]).inc();
            return Ok(k);
        }
        KEY_CACHE.with_label_values(&["miss"]).inc();
        let u = match self.unwrap(Purpose::SigningKey, did, wrapped).await {
            Ok(u) => u,
            Err(e) => {
                KEY_CACHE.with_label_values(&[if e.retryable() { "unavailable" } else { "rejected" }]).inc();
                return Err(e);
            }
        };
        let key = Arc::new(Keypair::from_bytes(&u.plaintext).map_err(|e| SecretError::Rejected(format!("signing key of {did}: {e}")))?);
        if !pubkey.is_empty() && key.public_multibase() != pubkey {
            KEY_CACHE.with_label_values(&["rejected"]).inc();
            return Err(SecretError::Rejected(format!("signing key of {did} does not match its public key")));
        }
        self.keys.put(did, &key.public_multibase(), key.clone());
        Ok(key)
    }

    /// [`signing_key`](Self::signing_key) of an account row.
    pub async fn account_signing_key(&self, a: &crate::state::Account) -> Result<Arc<Keypair>, SecretError> {
        self.signing_key(&a.did, &a.wrapped_signing_key, &a.signing_pubkey).await
    }

    /// Drops `did`'s cached key (account deleted).
    pub fn forget(&self, did: &str) {
        self.keys.remove(did);
    }

    /// Drops every cached key (tests: simulate a restart).
    pub fn clear_cache(&self) {
        self.keys.clear();
    }

    /// Cached signing keys.
    pub fn cached_keys(&self) -> usize {
        use crate::caches::Len;
        self.keys.len()
    }
}

fn parse_blob(blob: &str) -> Result<(&str, Vec<u8>), SecretError> {
    let mut it = blob.splitn(3, '.');
    match (it.next(), it.next(), it.next()) {
        (Some(WRAP_VERSION), Some(kid), Some(b)) if !kid.is_empty() => Ok((
            kid,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(b).map_err(|_| SecretError::Malformed)?,
        )),
        _ => Err(SecretError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(keys: &[&KekBytes]) -> Secrets {
        Secrets::new(keys.iter().map(|k| Arc::new(LocalKek::new(k)) as Arc<dyn KeyWrapper>).collect(), 0).unwrap()
    }

    #[tokio::test]
    async fn roundtrip_and_binding() {
        let k = KekBytes::random();
        let s = ring(&[&k]);
        let secret = [7u8; 32];
        let w = s.wrap(Purpose::SigningKey, "did:plc:a", &secret).await.unwrap();
        assert!(w.starts_with(&format!("vw1.{}.", k.kid())));
        assert!(!w.contains(&hex::encode(secret)));
        let u = s.unwrap(Purpose::SigningKey, "did:plc:a", &w).await.unwrap();
        assert_eq!(&u.plaintext[..], &secret);
        assert!(!u.stale);
        // two wraps of one secret differ (random nonces)
        assert_ne!(w, s.wrap(Purpose::SigningKey, "did:plc:a", &secret).await.unwrap());
        // another subject or purpose: rejected
        assert!(matches!(s.unwrap(Purpose::SigningKey, "did:plc:b", &w).await, Err(SecretError::Rejected(_))));
        assert!(matches!(s.unwrap(Purpose::Totp, "did:plc:a", &w).await, Err(SecretError::Rejected(_))));
        // another KEK: unknown kid; the same kid with a different key can't happen
        // (the kid is the key's hash), but a forged kid is rejected by the tag
        let other = ring(&[&KekBytes::random()]);
        assert!(matches!(other.unwrap(Purpose::SigningKey, "did:plc:a", &w).await, Err(SecretError::UnknownKek(_))));
        let forged = w.replacen(&k.kid(), other.current_kid(), 1);
        assert!(matches!(other.unwrap(Purpose::SigningKey, "did:plc:a", &forged).await, Err(SecretError::Rejected(_))));
        // tampered ciphertext
        let mut bad = w.clone().into_bytes();
        let n = bad.len();
        bad[n - 3] = if bad[n - 3] == b'A' { b'B' } else { b'A' };
        assert!(s.unwrap(Purpose::SigningKey, "did:plc:a", std::str::from_utf8(&bad).unwrap()).await.is_err());
        assert!(matches!(s.unwrap(Purpose::SigningKey, "did:plc:a", "hex-or-whatever").await, Err(SecretError::Malformed)));
    }

    #[tokio::test]
    async fn rotation_and_rewrap() {
        let (old, new) = (KekBytes::random(), KekBytes::random());
        let before = ring(&[&old]);
        let w = before.wrap(Purpose::Totp, "did:plc:x", b"JBSWY3DPEHPK3PXP").await.unwrap();
        // new current, old kept for unwrap
        let during = ring(&[&new, &old]);
        assert!(!during.is_current(&w));
        let u = during.unwrap(Purpose::Totp, "did:plc:x", &w).await.unwrap();
        assert!(u.stale);
        let w2 = during.rewrap(Purpose::Totp, "did:plc:x", &w).await.unwrap().expect("stale blob rewrapped");
        assert!(during.is_current(&w2));
        assert_eq!(during.rewrap(Purpose::Totp, "did:plc:x", &w2).await.unwrap(), None);
        // after the old KEK is retired only the rewrapped blob opens
        let after = ring(&[&new]);
        assert_eq!(&after.unwrap(Purpose::Totp, "did:plc:x", &w2).await.unwrap().plaintext[..], b"JBSWY3DPEHPK3PXP");
        assert!(matches!(after.unwrap(Purpose::Totp, "did:plc:x", &w).await, Err(SecretError::UnknownKek(_))));
    }

    #[tokio::test]
    async fn signing_key_cache() {
        let s = ring(&[&KekBytes::random()]);
        let key = Arc::new(Keypair::generate());
        let (w, pk) = s.wrap_signing_key("did:plc:c", &key).await.unwrap();
        assert_eq!(pk, key.public_multibase());
        // cached by wrap: same Arc, no unwrap
        assert!(Arc::ptr_eq(&s.cached_signing_key("did:plc:c", &pk).unwrap(), &key));
        s.clear_cache();
        assert!(s.cached_signing_key("did:plc:c", &pk).is_none());
        let k1 = s.signing_key("did:plc:c", &w, &pk).await.unwrap();
        assert_eq!(k1.to_bytes(), key.to_bytes());
        let k2 = s.signing_key("did:plc:c", &w, &pk).await.unwrap();
        assert!(Arc::ptr_eq(&k1, &k2), "second lookup is a cache hit");
        // a row whose public key doesn't match the wrapped secret
        s.clear_cache();
        let other = Keypair::generate().public_multibase();
        assert!(matches!(s.signing_key("did:plc:c", &w, &other).await, Err(SecretError::Rejected(_))));
        // a cached key isn't served for another public key (rotated)
        let _ = s.signing_key("did:plc:c", &w, &pk).await.unwrap();
        assert!(s.cached_signing_key("did:plc:c", &other).is_none());
    }

    #[test]
    fn kek_parsing_and_dev_check() {
        let k = KekBytes::random();
        assert_eq!(KekBytes::parse(&hex::encode(k.0)).unwrap(), k);
        assert_eq!(KekBytes::parse(&format!(" {}\n", base64::engine::general_purpose::STANDARD.encode(k.0))).unwrap(), k);
        assert_eq!(KekBytes::parse(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(k.0)).unwrap(), k);
        assert!(KekBytes::parse("abcd").is_err());
        assert!(!format!("{k:?}").contains(&hex::encode(k.0)));
        // dev mode: no KEK needed, the dev KEK wraps
        let s = Secrets::from_config(&KekConfig::default(), true).unwrap();
        assert!(s.is_dev());
        assert_eq!(s.current_kid(), dev_kek().kid());
        // production: a KEK is required, and not the dev one
        assert!(KekConfig::default().check(false).is_err());
        assert!(KekConfig { local: Some(dev_kek()), ..Default::default() }.check(false).is_err());
        assert!(KekConfig { local: Some(k.clone()), ..Default::default() }.check(false).is_ok());
        let s = Secrets::from_config(&KekConfig { local: Some(k.clone()), ..Default::default() }, false).unwrap();
        assert!(!s.is_dev());
        assert_eq!(s.kids(), vec![k.kid()]);
        // dev mode with a real KEK still reads dev-KEK blobs
        let s = Secrets::from_config(&KekConfig { local: Some(k.clone()), ..Default::default() }, true).unwrap();
        assert_eq!(s.kids(), vec![k.kid(), dev_kek().kid()]);
    }

    /// Cost of the keyring on the write path: a cache hit (every load of a
    /// warm account, the proxy's misses) and a local-KEK unwrap (a cold
    /// load with `--kek-file`), next to one commit signature for scale.
    /// `cargo test --profile dev-release --lib bench_keyring -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn bench_keyring() {
        let s = ring(&[&KekBytes::random()]);
        let dids: Vec<String> = (0..10_000).map(|i| format!("did:plc:bench{i:019}")).collect();
        let mut rows = Vec::new();
        for d in &dids {
            let k = Arc::new(Keypair::generate());
            let (w, pk) = s.wrap_signing_key(d, &k).await.unwrap();
            rows.push((w, pk));
        }
        let t = Instant::now();
        for (d, (w, pk)) in dids.iter().zip(&rows) {
            std::hint::black_box(s.signing_key(d, w, pk).await.unwrap());
        }
        let hit = t.elapsed().as_nanos() as f64 / dids.len() as f64;
        s.clear_cache();
        let t = Instant::now();
        for (d, (w, pk)) in dids.iter().zip(&rows) {
            std::hint::black_box(s.signing_key(d, w, pk).await.unwrap());
        }
        let miss = t.elapsed().as_nanos() as f64 / dids.len() as f64;
        let k = Keypair::generate();
        let t = Instant::now();
        for i in 0..10_000u32 {
            std::hint::black_box(k.sign(&i.to_be_bytes()));
        }
        let sign = t.elapsed().as_nanos() as f64 / 10_000.0;
        println!("bench_keyring: cache hit {hit:.0} ns, local unwrap + parse + pubkey check {miss:.0} ns, one signature {sign:.0} ns");
    }

    #[test]
    fn crc32c_known_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}
