//! HashiCorp Vault (or OpenBao) Transit as the KEK: `encrypt`/`decrypt` on
//! one named key, the purpose and subject as Transit's `associated_data`.
//! Vault before 1.13 ignores that parameter (with only a warning), so the
//! current key is checked once, before its first use, to reject a wrong AAD;
//! every response is also checked for Vault's "ignored parameters" warning.
//!
//! The stored ciphertext is Transit's own `vault:vN:…` string. Transit
//! picks the version, so `rotate` needs no config change; a blob under an
//! older version than the latest one seen is stale.

use super::{b64, truncate, KeyWrapper, SecretError, Unwrapped, KMS_TIMEOUT};
use async_trait::async_trait;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

pub const DEFAULT_K8S_JWT_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
pub const DEFAULT_APPROLE_MOUNT: &str = "approle";
pub const DEFAULT_K8S_MOUNT: &str = "kubernetes";
/// A Vault Agent sidecar rewrites its token file without telling anyone.
const TOKEN_FILE_RELOAD: Duration = Duration::from_secs(60);
/// A token without a TTL (`lease_duration` 0) is still re-checked this often.
const NO_TTL_RECHECK: Duration = Duration::from_secs(3600);
/// How old the latest known key version may get before an unwrap asks
/// Transit (one encrypt of a dummy value). Bounds how long after a `rotate`
/// a dry-run rewrap still reports blobs under the old version as current.
const VERSION_RECHECK: Duration = Duration::from_secs(60);
const AEAD_TYPES: [&str; 3] = ["aes256-gcm96", "aes128-gcm96", "chacha20-poly1305"];

/// How a node gets its Vault token.
#[derive(Clone)]
pub enum VaultAuth {
    /// Read at first use, then again every minute and after a 403, so a
    /// Vault Agent sidecar can rotate it.
    TokenFile(PathBuf),
    /// The secret ID is read from its file at every login.
    AppRole { mount: String, role_id: String, secret_id_file: PathBuf },
    /// The service-account JWT is read at every login: projected tokens rotate.
    Kubernetes { mount: String, role: String, jwt_file: PathBuf },
    /// Tests.
    Static(String),
}

impl VaultAuth {
    pub fn method(&self) -> &'static str {
        match self {
            VaultAuth::TokenFile(_) => "token",
            VaultAuth::AppRole { .. } => "approle",
            VaultAuth::Kubernetes { .. } => "kubernetes",
            VaultAuth::Static(_) => "static",
        }
    }
}

impl std::fmt::Debug for VaultAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultAuth::TokenFile(p) => f.debug_tuple("TokenFile").field(p).finish(),
            VaultAuth::AppRole { mount, secret_id_file, .. } => f
                .debug_struct("AppRole")
                .field("mount", mount)
                .field("role_id", &"<redacted>")
                .field("secret_id_file", secret_id_file)
                .finish(),
            VaultAuth::Kubernetes { mount, role, jwt_file } => f
                .debug_struct("Kubernetes")
                .field("mount", mount)
                .field("role", role)
                .field("jwt_file", jwt_file)
                .finish(),
            VaultAuth::Static(_) => f.debug_tuple("Static").field(&"<redacted>").finish(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VaultConfig {
    /// `https://vault.example:8200`.
    pub addr: String,
    pub namespace: Option<String>,
    /// PEM certificates trusted on top of the public roots.
    pub ca_pem: Option<Vec<u8>>,
    pub auth: VaultAuth,
}

struct Session {
    token: Zeroizing<String>,
    refresh_at: Instant,
    /// The next refresh may renew this token instead of logging in.
    renew: bool,
    /// The TTL the login granted: a renewal granting less has hit max_ttl.
    login_ttl: u64,
}

impl Session {
    fn new(token: Zeroizing<String>, ttl: u64, renewable: bool, login_ttl: u64) -> Session {
        if ttl == 0 {
            return Session { token, refresh_at: Instant::now() + NO_TTL_RECHECK, renew: false, login_ttl };
        }
        let ttl_d = Duration::from_secs(ttl);
        Session { token, refresh_at: Instant::now() + ttl_d - refresh_margin(ttl_d), renew: renewable, login_ttl }
    }
}

/// Before a token expires: a third of its TTL, at most a minute.
fn refresh_margin(ttl: Duration) -> Duration {
    (ttl / 3).min(Duration::from_secs(60))
}

#[derive(serde::Deserialize)]
struct AuthResp {
    auth: Option<AuthData>,
}

#[derive(serde::Deserialize)]
struct AuthData {
    client_token: String,
    #[serde(default)]
    lease_duration: u64,
    #[serde(default)]
    renewable: bool,
}

/// One Vault server and identity; its token is shared by every Transit key
/// using it.
pub struct VaultClient {
    addr: String,
    host: String,
    namespace: Option<String>,
    auth: VaultAuth,
    http: reqwest::Client,
    session: tokio::sync::Mutex<Option<Session>>,
    logins: AtomicU64,
    renewals: AtomicU64,
}

impl std::fmt::Debug for VaultClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultClient")
            .field("addr", &self.addr)
            .field("namespace", &self.namespace)
            .field("auth", &self.auth)
            .finish_non_exhaustive()
    }
}

impl VaultClient {
    pub fn new(cfg: &VaultConfig) -> anyhow::Result<Arc<VaultClient>> {
        let addr = cfg.addr.trim().trim_end_matches('/').to_string();
        let u = reqwest::Url::parse(&addr).map_err(|e| anyhow::anyhow!("--vault-addr {addr:?}: {e}"))?;
        anyhow::ensure!(
            matches!(u.scheme(), "http" | "https") && u.path() == "/" && u.query().is_none(),
            "--vault-addr must be http(s)://host[:port] with no path: {addr}"
        );
        let host = u.host_str().ok_or_else(|| anyhow::anyhow!("--vault-addr has no host: {addr}"))?.to_string();
        let namespace = cfg.namespace.as_deref().map(|n| n.trim().trim_matches('/')).filter(|n| !n.is_empty());
        let http = crate::http::public_own(cfg.ca_pem.as_deref()).map_err(|e| e.context("--vault-ca-file"))?;
        match &cfg.auth {
            VaultAuth::AppRole { mount, role_id, .. } => {
                check_mount(mount, "--vault-approle-mount")?;
                anyhow::ensure!(!role_id.trim().is_empty(), "the Vault AppRole role ID is empty");
            }
            VaultAuth::Kubernetes { mount, role, .. } => {
                check_mount(mount, "--vault-k8s-mount")?;
                anyhow::ensure!(!role.trim().is_empty(), "--vault-k8s-role is empty");
            }
            VaultAuth::TokenFile(_) | VaultAuth::Static(_) => {}
        }
        Ok(Arc::new(VaultClient {
            addr,
            host,
            namespace: namespace.map(str::to_string),
            auth: cfg.auth.clone(),
            http,
            session: tokio::sync::Mutex::new(None),
            logins: AtomicU64::new(0),
            renewals: AtomicU64::new(0),
        }))
    }

    /// Logins so far (tests, and the startup line).
    pub fn logins(&self) -> u64 {
        self.logins.load(Ordering::Relaxed)
    }

    pub fn renewals(&self) -> u64 {
        self.renewals.load(Ordering::Relaxed)
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut r = self.http.request(method, format!("{}/v1/{path}", self.addr)).timeout(KMS_TIMEOUT);
        if let Some(ns) = &self.namespace {
            r = r.header("X-Vault-Namespace", ns);
        }
        r
    }

    /// `force`: the last one got a 403 (expired, revoked or rotated away).
    async fn token(&self, force: bool) -> Result<Zeroizing<String>, SecretError> {
        let mut g = self.session.lock().await;
        if let Some(s) = g.as_ref() {
            if !force && Instant::now() < s.refresh_at {
                return Ok(s.token.clone());
            }
        }
        let next = match &self.auth {
            VaultAuth::Static(t) => return Ok(Zeroizing::new(t.clone())),
            VaultAuth::TokenFile(p) => Session {
                token: read_secret(p, "vault token file")?,
                refresh_at: Instant::now() + TOKEN_FILE_RELOAD,
                renew: false,
                login_ttl: 0,
            },
            _ => {
                let renewed = match g.as_ref() {
                    Some(s) if !force && s.renew => match self.renew(s).await {
                        Ok(n) => Some(n),
                        Err(e) => {
                            tracing::warn!(method = self.auth.method(), "vault token renewal failed, logging in: {e}");
                            None
                        }
                    },
                    _ => None,
                };
                match renewed {
                    Some(s) => s,
                    None => self.login().await?,
                }
            }
        };
        let t = next.token.clone();
        *g = Some(next);
        Ok(t)
    }

    async fn renew(&self, s: &Session) -> Result<Session, SecretError> {
        let r = self
            .request(reqwest::Method::POST, "auth/token/renew-self")
            .header("X-Vault-Token", s.token.as_str())
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault token renewal: {}", chain(&e))))?;
        let a = auth_response(r, "token renewal").await?;
        self.renewals.fetch_add(1, Ordering::Relaxed);
        let ttl = a.lease_duration;
        // a shorter TTL than the login's means max_ttl caps it: log in
        // again before it runs out rather than renew a dying token
        let renew = a.renewable && ttl >= s.login_ttl;
        Ok(Session::new(Zeroizing::new(a.client_token), ttl, renew, s.login_ttl))
    }

    async fn login(&self) -> Result<Session, SecretError> {
        let (mount, body) = match &self.auth {
            VaultAuth::AppRole { mount, role_id, secret_id_file } => {
                let secret_id = read_secret(secret_id_file, "vault AppRole secret ID file")?;
                let body = Zeroizing::new(
                    serde_json::to_vec(&serde_json::json!({"role_id": role_id, "secret_id": &*secret_id}))
                        .expect("json"),
                );
                (mount, body)
            }
            VaultAuth::Kubernetes { mount, role, jwt_file } => {
                let jwt = read_secret(jwt_file, "kubernetes service-account token file")?;
                let body =
                    Zeroizing::new(serde_json::to_vec(&serde_json::json!({"role": role, "jwt": &*jwt})).expect("json"));
                (mount, body)
            }
            VaultAuth::TokenFile(_) | VaultAuth::Static(_) => unreachable!("no login for a given token"),
        };
        let what = format!("{} login", self.auth.method());
        let r = self
            .request(reqwest::Method::POST, &format!("auth/{mount}/login"))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|e| SecretError::Unavailable(format!("vault {what}: {}", chain(&e))))?;
        let a = auth_response(r, &what).await?;
        self.logins.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(method = self.auth.method(), ttl = a.lease_duration, renewable = a.renewable, "vault login");
        Ok(Session::new(Zeroizing::new(a.client_token), a.lease_duration, a.renewable, a.lease_duration))
    }

    /// A Vault API call with the token, logging in again once on a 403.
    /// Returns the response's `data`.
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        op: &str,
        body: Option<&[u8]>,
        reauth: bool,
    ) -> Result<serde_json::Value, SecretError> {
        for attempt in 0..2 {
            let token = self.token(attempt > 0).await?;
            let mut req = self.request(method.clone(), path).header("X-Vault-Token", token.as_str());
            if let Some(b) = body {
                req = req.header(reqwest::header::CONTENT_TYPE, "application/json").body(b.to_vec());
            }
            let r = req.send().await.map_err(|e| SecretError::Unavailable(format!("vault {op}: {}", chain(&e))))?;
            let status = r.status();
            if status.is_success() {
                let mut v: serde_json::Value =
                    r.json().await.map_err(|e| SecretError::Unavailable(format!("vault {op}: {e}")))?;
                check_warnings(&v, op)?;
                return Ok(v.get_mut("data").map(serde_json::Value::take).unwrap_or_default());
            }
            let text = r.text().await.unwrap_or_default();
            match status.as_u16() {
                403 if reauth && attempt == 0 && !matches!(self.auth, VaultAuth::Static(_)) => continue,
                // wrong AAD, a ciphertext of another key, one below
                // min_decryption_version, or a missing key
                400 => return Err(SecretError::Rejected(format!("vault {op}: {}", errors(&text)))),
                _ => return Err(SecretError::Unavailable(format!("vault {op}: HTTP {status}: {}", errors(&text)))),
            }
        }
        Err(SecretError::Unavailable(format!("vault {op}: permission denied")))
    }
}

fn check_mount(m: &str, flag: &str) -> anyhow::Result<()> {
    anyhow::ensure!(valid_path(m), "{flag} must be a Vault path like `approle` or `team/approle`: {m:?}");
    Ok(())
}

fn valid_path(p: &str) -> bool {
    !p.is_empty()
        && p.split('/').all(|s| {
            !s.is_empty()
                && s != "."
                && s != ".."
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
}

/// Trimmed. Errors name the file, never its contents.
fn read_secret(p: &std::path::Path, what: &str) -> Result<Zeroizing<String>, SecretError> {
    let raw = Zeroizing::new(
        std::fs::read_to_string(p).map_err(|e| SecretError::Unavailable(format!("{what} {}: {e}", p.display())))?,
    );
    let t = Zeroizing::new(raw.trim().to_string());
    if t.is_empty() {
        return Err(SecretError::Unavailable(format!("{what} {} is empty", p.display())));
    }
    Ok(t)
}

/// Every login or renewal failure is retryable: wrong credentials look the
/// same as a revoked identity, which an operator fixes without a restart.
async fn auth_response(r: reqwest::Response, what: &str) -> Result<AuthData, SecretError> {
    let status = r.status();
    if !status.is_success() {
        let text = r.text().await.unwrap_or_default();
        return Err(SecretError::Unavailable(format!("vault {what}: HTTP {status}: {}", errors(&text))));
    }
    let a: AuthResp = r.json().await.map_err(|e| SecretError::Unavailable(format!("vault {what}: {e}")))?;
    a.auth
        .filter(|a| !a.client_token.is_empty())
        .ok_or_else(|| SecretError::Unavailable(format!("vault {what}: no token in the response")))
}

/// reqwest's own message stops at "error sending request".
fn chain(e: &reqwest::Error) -> String {
    let mut s = e.to_string();
    let mut src = std::error::Error::source(e);
    while let Some(x) = src {
        s.push_str(": ");
        s.push_str(&x.to_string());
        src = x.source();
    }
    s
}

/// Vault's `{"errors": [...]}`, or the raw body.
fn errors(text: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(text).ok().and_then(|v| v.get("errors").cloned()) {
        Some(serde_json::Value::Array(es)) => {
            let s = es.iter().filter_map(|e| e.as_str()).collect::<Vec<_>>().join("; ");
            truncate(s.trim()).to_string()
        }
        _ => truncate(text.trim()).to_string(),
    }
}

/// Vault before 1.13 accepts `associated_data` and drops it, saying so only
/// in `warnings`. Neither a wrap nor an unwrap may be trusted then.
fn check_warnings(v: &serde_json::Value, op: &str) -> Result<(), SecretError> {
    let ignored = v["warnings"]
        .as_array()
        .is_some_and(|ws| ws.iter().any(|w| w.as_str().is_some_and(|w| w.contains("associated_data"))));
    if ignored {
        return Err(SecretError::Rejected(format!(
            "vault {op}: the server ignored associated_data (it needs Vault 1.13 or later, or OpenBao)"
        )));
    }
    Ok(())
}

/// `vault:v{N}:…`, N from 1.
fn ciphertext_version(ct: &str) -> Option<u64> {
    let rest = ct.strip_prefix("vault:v")?;
    let (n, body) = rest.split_once(':')?;
    let ok = !body.is_empty() && body.bytes().all(|b| b.is_ascii_alphanumeric() || b"+/=".contains(&b));
    n.parse::<u64>().ok().filter(|&n| n >= 1 && ok)
}

/// One Transit key. Its kid hashes the server's host name, namespace, mount
/// and key name but no version, so `rotate` keeps the kid.
pub struct VaultTransit {
    kid: String,
    name: String,
    mount: String,
    key: String,
    client: Arc<VaultClient>,
    /// Self-tested before first use, and asked for its latest version.
    current: bool,
    verified: tokio::sync::OnceCell<()>,
    latest: AtomicU64,
    latest_checked: parking_lot::Mutex<Option<Instant>>,
}

impl std::fmt::Debug for VaultTransit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultTransit").field("kid", &self.kid).field("name", &self.name).finish_non_exhaustive()
    }
}

impl VaultTransit {
    /// `mount_key`: `<mount>/<key>`, the mount possibly nested (`a/b/key`).
    pub fn new(client: Arc<VaultClient>, mount_key: &str, current: bool) -> anyhow::Result<VaultTransit> {
        let mk = mount_key.trim().trim_matches('/');
        let (mount, key) = mk.rsplit_once('/').filter(|(m, k)| valid_path(m) && valid_path(k)).ok_or_else(|| {
            anyhow::anyhow!("Vault Transit key must be <mount>/<key> (e.g. transit/vlpds): {mount_key:?}")
        })?;
        let name = match &client.namespace {
            Some(ns) => format!("vault:{}/{ns}/{mount}/{key}", client.host),
            None => format!("vault:{}/{mount}/{key}", client.host),
        };
        let h = Sha256::digest(name.as_bytes());
        Ok(VaultTransit {
            kid: format!("V{}", hex::encode(&h[..8])),
            name,
            mount: mount.to_string(),
            key: key.to_string(),
            client,
            current,
            verified: tokio::sync::OnceCell::new(),
            latest: AtomicU64::new(0),
            latest_checked: parking_lot::Mutex::new(None),
        })
    }

    /// `vault:{host}/[{namespace}/]{mount}/{key}`, what the kid hashes.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn client(&self) -> &Arc<VaultClient> {
        &self.client
    }

    /// The newest key version this node has seen (0: none yet).
    pub fn latest_version(&self) -> u64 {
        self.latest.load(Ordering::Relaxed)
    }

    async fn encrypt(&self, aad: &[u8], plaintext: &[u8]) -> Result<String, SecretError> {
        let pt = Zeroizing::new(b64(plaintext));
        // base64 needs no JSON escaping
        let body = Zeroizing::new(
            format!(r#"{{"plaintext":"{}","associated_data":"{}"}}"#, pt.as_str(), b64(aad)).into_bytes(),
        );
        let path = format!("{}/encrypt/{}", self.mount, self.key);
        let data = self.client.call(reqwest::Method::POST, &path, "encrypt", Some(&body), true).await?;
        let ct = data["ciphertext"]
            .as_str()
            .ok_or_else(|| SecretError::Unavailable("vault encrypt: no ciphertext".into()))?;
        let v = ciphertext_version(ct)
            .ok_or_else(|| SecretError::Unavailable("vault encrypt: ciphertext isn't vault:vN:…".into()))?;
        self.latest.fetch_max(v, Ordering::Relaxed);
        *self.latest_checked.lock() = Some(Instant::now());
        Ok(ct.to_string())
    }

    async fn decrypt(&self, aad: &[u8], ct: &str) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        let body =
            serde_json::to_vec(&serde_json::json!({"ciphertext": ct, "associated_data": b64(aad)})).expect("json");
        let path = format!("{}/decrypt/{}", self.mount, self.key);
        let mut data = self.client.call(reqwest::Method::POST, &path, "decrypt", Some(&body), true).await?;
        let pt = match data.get_mut("plaintext").map(serde_json::Value::take) {
            Some(serde_json::Value::String(s)) => Zeroizing::new(s),
            _ => Zeroizing::new(String::new()),
        };
        Ok(Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(pt.as_bytes())
                .map_err(|_| SecretError::Unavailable("vault decrypt: bad plaintext".into()))?,
        ))
    }

    async fn verify(&self) -> Result<(), SecretError> {
        if !self.current {
            return Ok(());
        }
        self.verified.get_or_try_init(|| self.self_test_inner()).await.map(|_| ())
    }

    /// The key's type, where the policy allows reading it; the AAD check
    /// catches the rest.
    async fn check_key_type(&self) -> Result<(), SecretError> {
        let path = format!("{}/keys/{}", self.mount, self.key);
        let data = match self.client.call(reqwest::Method::GET, &path, "read key", None, false).await {
            Ok(d) => d,
            Err(_) => return Ok(()),
        };
        let ty = data["type"].as_str().unwrap_or("");
        if !AEAD_TYPES.contains(&ty) {
            return Err(SecretError::Rejected(format!(
                "Vault Transit key {} is {ty:?}: vlpds needs an AEAD key that takes associated_data ({})",
                self.name,
                AEAD_TYPES.join(", ")
            )));
        }
        if data["derived"].as_bool() == Some(true) {
            return Err(SecretError::Rejected(format!(
                "Vault Transit key {} is derived: vlpds needs a plain (non-derived) key",
                self.name
            )));
        }
        Ok(())
    }

    /// Wraps a random value under AAD A, unwraps it under A, and requires
    /// the unwrap under AAD B to be refused.
    async fn self_test_inner(&self) -> Result<(), SecretError> {
        self.check_key_type().await?;
        let pt = Zeroizing::new(rand::random::<[u8; 32]>().to_vec());
        let (a, b) = (b"vlpds-vault-aad-check\0a".as_slice(), b"vlpds-vault-aad-check\0b".as_slice());
        let ct = self.encrypt(a, &pt).await?;
        if self.decrypt(a, &ct).await?.as_slice() != pt.as_slice() {
            return Err(SecretError::Rejected(format!(
                "Vault Transit key {}: a decrypt returned other bytes",
                self.name
            )));
        }
        match self.decrypt(b, &ct).await {
            Err(SecretError::Rejected(_)) => Ok(()),
            Err(e) => Err(e),
            Ok(_) => Err(SecretError::Rejected(format!(
                "Vault Transit key {} decrypted with the wrong associated_data: the server or key type doesn't \
                 enforce AAD (needs Vault 1.13+ or OpenBao, and an {} key)",
                self.name,
                AEAD_TYPES.join(" / ")
            ))),
        }
    }

    /// The latest version, asking Transit (one dummy encrypt) when the last
    /// answer is older than [`VERSION_RECHECK`]. A failed ask keeps the old
    /// answer: staleness is advisory.
    async fn refresh_latest(&self) -> u64 {
        let due = {
            let mut g = self.latest_checked.lock();
            let due = g.is_none_or(|t| t.elapsed() >= VERSION_RECHECK);
            if due {
                *g = Some(Instant::now());
            }
            due
        };
        if due {
            if let Err(e) = self.encrypt(b"vlpds-vault-version-probe", &[0]).await {
                tracing::debug!(kid = self.kid, "vault key version probe: {e}");
            }
        }
        self.latest.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl KeyWrapper for VaultTransit {
    fn kid(&self) -> &str {
        &self.kid
    }
    fn backend(&self) -> &'static str {
        "vault"
    }
    fn remote(&self) -> bool {
        true
    }
    async fn self_test(&self) -> Result<(), SecretError> {
        self.verify().await
    }
    async fn wrap(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        self.verify().await?;
        Ok(self.encrypt(aad, plaintext).await?.into_bytes())
    }
    async fn unwrap(&self, aad: &[u8], ct: &[u8]) -> Result<Unwrapped, SecretError> {
        let ct = std::str::from_utf8(ct).map_err(|_| SecretError::Malformed)?;
        let v = ciphertext_version(ct).ok_or(SecretError::Malformed)?;
        self.verify().await?;
        let plaintext = self.decrypt(aad, ct).await?;
        self.latest.fetch_max(v, Ordering::Relaxed);
        let stale = self.current && v < self.refresh_latest().await;
        Ok(Unwrapped { plaintext, stale })
    }
}

#[cfg(test)]
mod tests;
