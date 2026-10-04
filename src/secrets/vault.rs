//! HashiCorp Vault (or OpenBao) Transit as the KEK: `encrypt`/`decrypt` on
//! one named key, the purpose and subject as Transit's `associated_data`.
//! Vault before 1.13 drops that parameter (with a warning, or silently for
//! some key types), so each key is checked before its first use: the current
//! one wraps under AAD A and must refuse to unwrap under AAD B, an old one
//! must refuse its first blob under a wrong AAD. Every response is also
//! checked for Vault's "ignored parameters" warning.
//!
//! The stored ciphertext is Transit's own `vault:vN:…` string. Transit
//! picks the version, so `rotate` needs no config change; a blob under an
//! older version than the latest one seen is stale.

use super::{b64, truncate, KeyWrapper, SecretError, Unwrapped, KMS_TIMEOUT};
use async_trait::async_trait;
use base64::Engine;
use futures::future::{BoxFuture, FutureExt, Shared};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
/// Vault's own ceiling is 32 days; this only keeps `Instant` math in range.
const MAX_TTL: u64 = 365 * 86_400;
/// After a 403 forced a new login, the next forced one waits this long: a
/// policy that refuses every token would otherwise log in once per request.
const FORCED_RELOGIN_GAP: Duration = if cfg!(test) { Duration::from_millis(300) } else { Duration::from_secs(5) };
/// A refresh that failed while the token is still valid is retried this
/// often, the old token serving meanwhile.
const REFRESH_RETRY: Duration = Duration::from_secs(5);
/// How old the latest known key version may get before an unwrap asks
/// Transit again (one encrypt of a dummy value, in the background).
const VERSION_RECHECK: Duration = Duration::from_secs(60);
/// The self-test runs in its own task with this deadline, so a slow Vault
/// can pass it although each caller gives up after [`KMS_TIMEOUT`].
const SELF_TEST_DEADLINE: Duration = Duration::from_secs(15);
/// A failed self-test is reused for this long before it runs again.
const REJECTED_FOR: Duration = Duration::from_secs(30);
const UNAVAILABLE_FOR: Duration = Duration::from_secs(1);
/// A passed self-test is repeated in the background this often, in case
/// Vault was swapped for an older one.
const RETEST_EVERY: Duration = Duration::from_secs(3600);
/// A token file refused at startup is re-read this many times: a sink on a
/// persistent volume can hold the last run's token until the Agent rewrites it.
const STARTUP_403_RETRIES: u32 = 3;
const STARTUP_403_DELAY: Duration = if cfg!(test) { Duration::from_millis(300) } else { Duration::from_secs(2) };
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
    #[cfg(test)]
    Static(String),
}

impl VaultAuth {
    pub fn method(&self) -> &'static str {
        match self {
            VaultAuth::TokenFile(_) => "token",
            VaultAuth::AppRole { .. } => "approle",
            VaultAuth::Kubernetes { .. } => "kubernetes",
            #[cfg(test)]
            VaultAuth::Static(_) => "static",
        }
    }

    fn logs_in(&self) -> bool {
        matches!(self, VaultAuth::AppRole { .. } | VaultAuth::Kubernetes { .. })
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
            #[cfg(test)]
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
    /// Trust only `ca_pem`.
    pub ca_only: bool,
    pub auth: VaultAuth,
}

struct Session {
    token: Zeroizing<String>,
    refresh_at: Instant,
    /// None: unknown (a token file) or no TTL.
    expires_at: Option<Instant>,
    /// The next refresh may renew this token instead of logging in.
    renew: bool,
    /// The TTL the login granted: a renewal granting less has hit max_ttl.
    login_ttl: u64,
}

impl Session {
    fn new(token: Zeroizing<String>, ttl: u64, renewable: bool, login_ttl: u64) -> Session {
        let now = Instant::now();
        if ttl == 0 {
            return Session { token, refresh_at: now + NO_TTL_RECHECK, expires_at: None, renew: false, login_ttl };
        }
        let ttl_d = Duration::from_secs(ttl.min(MAX_TTL));
        Session {
            token,
            refresh_at: now + ttl_d - refresh_margin(ttl_d),
            expires_at: Some(now + ttl_d),
            renew: renewable,
            login_ttl,
        }
    }
}

/// Before a token expires: a third of its TTL, at most a minute.
fn refresh_margin(ttl: Duration) -> Duration {
    (ttl / 3).min(Duration::from_secs(60))
}

#[derive(Default)]
struct SessionState {
    cur: Option<Session>,
    last_forced: Option<Instant>,
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

/// A failed Vault request. At startup, an answer from Vault (`status`) or a
/// missing credential file (`config`) is a setup mistake and stops the
/// node; no answer is an outage.
#[derive(Clone)]
struct Fail {
    e: SecretError,
    status: Option<u16>,
    path: String,
    config: bool,
}

impl From<Fail> for SecretError {
    fn from(f: Fail) -> SecretError {
        f.e
    }
}

/// No HTTP answer: a connection error, a bad body.
fn local(e: SecretError) -> Fail {
    Fail { e, status: None, path: String::new(), config: false }
}

/// Mid-run the same answers stay retryable: an operator can fix a policy
/// without a restart.
fn startup_error(f: Fail) -> SecretError {
    let msg = f.e.to_string();
    let hint = match (f.status, f.path.ends_with("/login")) {
        _ if f.config => "vlpds can't read it: check the path and the file's owner and mode".to_string(),
        (Some(400 | 401 | 403), true) => "the auth role or its credentials are wrong".to_string(),
        (Some(401 | 403), false) => format!(
            "the token's policy lacks update on {}, or the key doesn't exist (the policy may not create it)",
            f.path
        ),
        (Some(404), _) => format!("nothing at {}: check the mount and key names", f.path),
        (Some(400), false) if msg.contains("not found") => format!("no such key at {}", f.path),
        _ => return f.e,
    };
    SecretError::Rejected(format!("{msg} ({hint})"))
}

/// One Vault server and identity; its token is shared by every Transit key
/// using it.
pub struct VaultClient {
    addr: String,
    namespace: Option<String>,
    auth: VaultAuth,
    http: reqwest::Client,
    session: tokio::sync::Mutex<SessionState>,
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

fn is_loopback(u: &reqwest::Url) -> bool {
    let h = u.host_str().unwrap_or("");
    h.eq_ignore_ascii_case("localhost")
        || h.trim_start_matches('[').trim_end_matches(']').parse::<std::net::IpAddr>().is_ok_and(|a| a.is_loopback())
}

impl VaultClient {
    /// `http://` only in dev mode or to a loopback address. `max_idle`: the
    /// node's Vault calls in flight at most.
    pub fn new(cfg: &VaultConfig, dev_mode: bool, max_idle: usize) -> anyhow::Result<Arc<VaultClient>> {
        let addr = cfg.addr.trim().trim_end_matches('/').to_string();
        let u = reqwest::Url::parse(&addr).map_err(|e| anyhow::anyhow!("--vault-addr {addr:?}: {e}"))?;
        anyhow::ensure!(
            matches!(u.scheme(), "http" | "https") && u.path() == "/" && u.query().is_none(),
            "--vault-addr must be http(s)://host[:port] with no path: {addr}"
        );
        anyhow::ensure!(u.host_str().is_some(), "--vault-addr has no host: {addr}");
        anyhow::ensure!(
            u.username().is_empty() && u.password().is_none(),
            "--vault-addr must not carry credentials (user:pass@)"
        );
        anyhow::ensure!(
            u.scheme() == "https" || dev_mode || is_loopback(&u),
            "--vault-addr must be https:// (plain http only to a loopback address or in --dev-mode): {addr}"
        );
        let namespace = cfg.namespace.as_deref().map(|n| n.trim().trim_matches('/')).filter(|n| !n.is_empty());
        let http = crate::http::dedicated("vault", max_idle.max(1), cfg.ca_pem.as_deref(), cfg.ca_only)
            .map_err(|e| e.context("--vault-ca-file"))?;
        match &cfg.auth {
            VaultAuth::AppRole { mount, role_id, .. } => {
                check_mount(mount, "--vault-approle-mount")?;
                anyhow::ensure!(!role_id.trim().is_empty(), "the Vault AppRole role ID is empty");
            }
            VaultAuth::Kubernetes { mount, role, .. } => {
                check_mount(mount, "--vault-k8s-mount")?;
                anyhow::ensure!(!role.trim().is_empty(), "--vault-k8s-role is empty");
            }
            VaultAuth::TokenFile(_) => {}
            #[cfg(test)]
            VaultAuth::Static(_) => {}
        }
        Ok(Arc::new(VaultClient {
            addr,
            namespace: namespace.map(str::to_string),
            auth: cfg.auth.clone(),
            http,
            session: tokio::sync::Mutex::new(SessionState::default()),
            logins: AtomicU64::new(0),
            renewals: AtomicU64::new(0),
        }))
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Logins so far (tests).
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

    /// `refused`: the token a 403 just came back for. Another caller may
    /// already have replaced it; only if not is there a new login. None: a
    /// new login was refused moments ago, so this 403 stands.
    async fn token(&self, refused: Option<&str>) -> Result<Option<Zeroizing<String>>, Fail> {
        #[cfg(test)]
        if let VaultAuth::Static(t) = &self.auth {
            return Ok(Some(Zeroizing::new(t.clone())));
        }
        let mut g = self.session.lock().await;
        let now = Instant::now();
        if let Some(refused) = refused {
            if let Some(s) = g.cur.as_ref().filter(|s| s.token.as_str() != refused) {
                return Ok(Some(s.token.clone()));
            }
            if g.last_forced.is_some_and(|t| t.elapsed() < FORCED_RELOGIN_GAP) {
                return Ok(None);
            }
            g.last_forced = Some(now);
            let next = self.fresh(None).await?;
            return Ok(Some(self.replace(&mut g, next)));
        }
        if let Some(s) = g.cur.as_ref().filter(|s| now < s.refresh_at) {
            return Ok(Some(s.token.clone()));
        }
        match self.fresh(g.cur.as_ref()).await {
            Ok(next) => Ok(Some(self.replace(&mut g, next))),
            Err(e) => match g.cur.as_mut().filter(|s| s.expires_at.is_none_or(|x| now < x)) {
                Some(s) => {
                    tracing::warn!(
                        method = self.auth.method(),
                        "vault token refresh failed, the current one serves until it expires: {}",
                        e.e
                    );
                    s.refresh_at = s.expires_at.map_or(now + REFRESH_RETRY, |x| x.min(now + REFRESH_RETRY));
                    Ok(Some(s.token.clone()))
                }
                None => Err(e),
            },
        }
    }

    /// A token read or minted now: a renewal of `prev` if it allows one.
    async fn fresh(&self, prev: Option<&Session>) -> Result<Session, Fail> {
        if let VaultAuth::TokenFile(p) = &self.auth {
            return Ok(Session {
                token: read_secret(p, "vault token file").map_err(local)?,
                refresh_at: Instant::now() + TOKEN_FILE_RELOAD,
                expires_at: None,
                renew: false,
                login_ttl: 0,
            });
        }
        if let Some(s) = prev.filter(|s| s.renew) {
            match self.renew(s).await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    tracing::warn!(method = self.auth.method(), "vault token renewal failed, logging in: {}", e.e)
                }
            }
        }
        self.login().await
    }

    /// Installs `next`; a token it replaces is revoked in the background
    /// (only the ones vlpds minted: a token file's belongs to the Agent).
    fn replace(&self, g: &mut SessionState, next: Session) -> Zeroizing<String> {
        let t = next.token.clone();
        if let Some(old) = g.cur.replace(next) {
            if self.auth.logs_in() && old.token.as_str() != t.as_str() {
                let req = self
                    .request(reqwest::Method::POST, "auth/token/revoke-self")
                    .header("X-Vault-Token", old.token.as_str());
                tokio::spawn(async move {
                    if let Err(e) = req.send().await {
                        tracing::debug!("vault revoke-self of a replaced token: {}", chain(&e));
                    }
                });
            }
        }
        t
    }

    async fn renew(&self, s: &Session) -> Result<Session, Fail> {
        let path = "auth/token/renew-self";
        let r = self
            .request(reqwest::Method::POST, path)
            .header("X-Vault-Token", s.token.as_str())
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| local(SecretError::Unavailable(format!("vault token renewal: {}", chain(&e)))))?;
        let a = auth_response(r, "token renewal", path).await?;
        self.renewals.fetch_add(1, Ordering::Relaxed);
        let ttl = a.lease_duration;
        // a shorter TTL than the login's means max_ttl caps it: log in
        // again before it runs out rather than renew a dying token
        let renew = a.renewable && ttl >= s.login_ttl;
        Ok(Session::new(Zeroizing::new(a.client_token), ttl, renew, s.login_ttl))
    }

    async fn login(&self) -> Result<Session, Fail> {
        let credential = |p: &Path, what: &str| {
            read_secret(p, what).map_err(|e| Fail { e, status: None, path: p.display().to_string(), config: true })
        };
        let (mount, body) = match &self.auth {
            VaultAuth::AppRole { mount, role_id, secret_id_file } => {
                let secret_id = credential(secret_id_file, "vault AppRole secret ID file")?;
                let body = Zeroizing::new(
                    serde_json::to_vec(&serde_json::json!({"role_id": role_id, "secret_id": &*secret_id}))
                        .expect("json"),
                );
                (mount, body)
            }
            VaultAuth::Kubernetes { mount, role, jwt_file } => {
                let jwt = credential(jwt_file, "kubernetes service-account token file")?;
                let body =
                    Zeroizing::new(serde_json::to_vec(&serde_json::json!({"role": role, "jwt": &*jwt})).expect("json"));
                (mount, body)
            }
            _ => unreachable!("no login for a given token"),
        };
        let what = format!("{} login", self.auth.method());
        let path = format!("auth/{mount}/login");
        let r = self
            .request(reqwest::Method::POST, &path)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|e| local(SecretError::Unavailable(format!("vault {what}: {}", chain(&e)))))?;
        let a = auth_response(r, &what, &path).await?;
        self.logins.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(method = self.auth.method(), ttl = a.lease_duration, renewable = a.renewable, "vault login");
        Ok(Session::new(Zeroizing::new(a.client_token), a.lease_duration, a.renewable, a.lease_duration))
    }

    /// A Vault API call with the token, with a new one once on a 403.
    /// Returns the response's `data` (Null if it has none).
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        op: &str,
        body: Option<&[u8]>,
        reauth: bool,
    ) -> Result<serde_json::Value, Fail> {
        let fail = |e, status| Fail { e, status: Some(status), path: path.to_string(), config: false };
        let mut refused: Option<(Zeroizing<String>, Fail)> = None;
        for attempt in 0..2 {
            let token = match self.token(refused.as_ref().map(|(t, _)| t.as_str())).await? {
                Some(t) => t,
                None => return Err(refused.expect("only after a 403").1),
            };
            let mut req = self.request(method.clone(), path).header("X-Vault-Token", token.as_str());
            if let Some(b) = body {
                req = req.header(reqwest::header::CONTENT_TYPE, "application/json").body(b.to_vec());
            }
            let r =
                req.send().await.map_err(|e| local(SecretError::Unavailable(format!("vault {op}: {}", chain(&e)))))?;
            let status = r.status();
            if status.is_success() {
                let mut v: serde_json::Value =
                    r.json().await.map_err(|e| local(SecretError::Unavailable(format!("vault {op}: {e}"))))?;
                check_warnings(&v, op).map_err(local)?;
                return Ok(v.get_mut("data").map(serde_json::Value::take).unwrap_or_default());
            }
            let text = r.text().await.unwrap_or_default();
            match status.as_u16() {
                403 if reauth && attempt == 0 && self.can_reauth() => {
                    let e = SecretError::Unavailable(format!("vault {op}: HTTP {status}: {}", errors(&text)));
                    refused = Some((token, fail(e, 403)));
                }
                // wrong AAD, a ciphertext of another key, one below
                // min_decryption_version, or a missing key
                400 => return Err(fail(SecretError::Rejected(format!("vault {op}: {}", errors(&text))), 400)),
                s => {
                    let e = SecretError::Unavailable(format!("vault {op}: HTTP {status}: {}", errors(&text)));
                    return Err(fail(e, s));
                }
            }
        }
        unreachable!("the second attempt returns")
    }

    fn can_reauth(&self) -> bool {
        #[cfg(test)]
        if matches!(self.auth, VaultAuth::Static(_)) {
            return false;
        }
        true
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
fn read_secret(p: &Path, what: &str) -> Result<Zeroizing<String>, SecretError> {
    let raw = Zeroizing::new(
        std::fs::read_to_string(p).map_err(|e| SecretError::Unavailable(format!("{what} {}: {e}", p.display())))?,
    );
    warn_if_world_readable(p);
    let t = Zeroizing::new(raw.trim().to_string());
    if t.is_empty() {
        return Err(SecretError::Unavailable(format!("{what} {} is empty", p.display())));
    }
    Ok(t)
}

/// Once per file.
fn warn_if_world_readable(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    static WARNED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());
    let Ok(m) = std::fs::metadata(p) else { return };
    if m.permissions().mode() & 0o004 == 0 {
        return;
    }
    let mut w = WARNED.lock();
    if !w.iter().any(|x| x == p) {
        w.push(p.to_path_buf());
        tracing::warn!(path = %p.display(), "a Vault credential file is world-readable; make it 0400");
    }
}

/// Every login or renewal failure is retryable mid-run: wrong credentials
/// look the same as a revoked identity, which an operator fixes without a
/// restart.
async fn auth_response(r: reqwest::Response, what: &str, path: &str) -> Result<AuthData, Fail> {
    let status = r.status();
    if !status.is_success() {
        let text = r.text().await.unwrap_or_default();
        return Err(Fail {
            e: SecretError::Unavailable(format!("vault {what}: HTTP {status}: {}", errors(&text))),
            status: Some(status.as_u16()),
            path: path.to_string(),
            config: false,
        });
    }
    let a: AuthResp = r.json().await.map_err(|e| local(SecretError::Unavailable(format!("vault {what}: {e}"))))?;
    a.auth
        .filter(|a| !a.client_token.is_empty())
        .ok_or_else(|| local(SecretError::Unavailable(format!("vault {what}: no token in the response"))))
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
    let ignored = v["warnings"].as_array().is_some_and(|ws| {
        ws.iter().filter_map(|w| w.as_str()).any(|w| {
            let w = w.to_ascii_lowercase();
            w.contains("associated_data") && (w.contains("ignored") || w.contains("unrecognized"))
        })
    });
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

/// What a key's background tasks need: everything but the self-test state.
struct KeyCore {
    kid: String,
    name: String,
    mount: String,
    key: String,
    client: Arc<VaultClient>,
    latest: AtomicU64,
    /// Set only by a successful encrypt.
    latest_checked: parking_lot::Mutex<Option<Instant>>,
    probing: AtomicBool,
}

impl KeyCore {
    fn path(&self, op: &str) -> String {
        format!("{}/{op}/{}", self.mount, self.key)
    }

    async fn encrypt(&self, aad: &[u8], plaintext: &[u8]) -> Result<String, Fail> {
        let pt = Zeroizing::new(b64(plaintext));
        // base64 needs no JSON escaping
        let body = Zeroizing::new(
            format!(r#"{{"plaintext":"{}","associated_data":"{}"}}"#, pt.as_str(), b64(aad)).into_bytes(),
        );
        let data = self.client.call(reqwest::Method::POST, &self.path("encrypt"), "encrypt", Some(&body), true).await?;
        let ct = data["ciphertext"]
            .as_str()
            .ok_or_else(|| local(SecretError::Unavailable("vault encrypt: no ciphertext".into())))?;
        let v = ciphertext_version(ct)
            .ok_or_else(|| local(SecretError::Unavailable("vault encrypt: ciphertext isn't vault:vN:…".into())))?;
        self.latest.fetch_max(v, Ordering::Relaxed);
        *self.latest_checked.lock() = Some(Instant::now());
        Ok(ct.to_string())
    }

    async fn decrypt(&self, aad: &[u8], ct: &str) -> Result<Zeroizing<Vec<u8>>, Fail> {
        let body =
            serde_json::to_vec(&serde_json::json!({"ciphertext": ct, "associated_data": b64(aad)})).expect("json");
        let mut data =
            self.client.call(reqwest::Method::POST, &self.path("decrypt"), "decrypt", Some(&body), true).await?;
        let pt = match data.get_mut("plaintext").map(serde_json::Value::take) {
            Some(serde_json::Value::String(s)) => Zeroizing::new(s),
            _ => return Err(local(SecretError::Unavailable("vault decrypt: no plaintext in the response".into()))),
        };
        Ok(Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(pt.as_bytes())
                .map_err(|_| local(SecretError::Unavailable("vault decrypt: bad plaintext".into())))?,
        ))
    }

    /// The key's type, where the policy allows reading it; the AAD check
    /// catches the rest.
    async fn check_key_type(&self) -> Result<(), Fail> {
        let data = match self.client.call(reqwest::Method::GET, &self.path("keys"), "read key", None, false).await {
            Ok(d) => d,
            Err(_) => return Ok(()),
        };
        let ty = data["type"].as_str().unwrap_or("");
        if !AEAD_TYPES.contains(&ty) {
            return Err(local(SecretError::Rejected(format!(
                "Vault Transit key {} is {ty:?}: vlpds needs an AEAD key that takes associated_data ({})",
                self.name,
                AEAD_TYPES.join(", ")
            ))));
        }
        if data["derived"].as_bool() == Some(true) {
            return Err(local(SecretError::Rejected(format!(
                "Vault Transit key {} is derived: vlpds needs a plain (non-derived) key",
                self.name
            ))));
        }
        Ok(())
    }

    fn not_enforced(&self) -> Fail {
        local(SecretError::Rejected(format!(
            "Vault Transit key {} decrypted with the wrong associated_data: the server or key type doesn't enforce \
             AAD (needs Vault 1.13+ or OpenBao, and an {} key)",
            self.name,
            AEAD_TYPES.join(" / ")
        )))
    }

    /// Wraps a random value under AAD A, unwraps it under A, and requires
    /// the unwrap under AAD B to be refused.
    async fn self_test(&self) -> Result<(), Fail> {
        self.check_key_type().await?;
        let pt = Zeroizing::new(rand::random::<[u8; 32]>().to_vec());
        let (a, b) = (b"vlpds-vault-aad-check\0a".as_slice(), b"vlpds-vault-aad-check\0b".as_slice());
        let ct = self.encrypt(a, &pt).await?;
        if self.decrypt(a, &ct).await?.as_slice() != pt.as_slice() {
            return Err(local(SecretError::Rejected(format!(
                "Vault Transit key {}: a decrypt returned other bytes",
                self.name
            ))));
        }
        match self.decrypt(b, &ct).await {
            Err(Fail { e: SecretError::Rejected(_), .. }) => Ok(()),
            Err(e) => Err(e),
            Ok(_) => Err(self.not_enforced()),
        }
    }

    /// An unwrap-only key at startup: the policy allows decrypt (a junk
    /// ciphertext is refused by Transit with a 400) and the key exists.
    async fn probe_decrypt(&self) -> Result<(), Fail> {
        self.check_key_type().await?;
        let junk = format!("vault:v1:{}", b64(&rand::random::<[u8; 32]>()));
        match self.decrypt(b"vlpds-vault-decrypt-probe", &junk).await {
            Err(f @ Fail { status: Some(400), .. }) if f.e.to_string().contains("not found") => Err(f),
            Err(Fail { status: Some(400), .. }) => Ok(()),
            Err(f) => Err(f),
            Ok(_) => Ok(()),
        }
    }

    /// One dummy encrypt; `latest_checked` moves only if it worked.
    async fn probe_version(&self) -> Result<(), Fail> {
        self.encrypt(b"vlpds-vault-version-probe", &[0]).await.map(|_| ())
    }

    fn refresh_in_background(self: &Arc<Self>) {
        let due = self.latest_checked.lock().is_none_or(|t| t.elapsed() >= VERSION_RECHECK);
        if !due || self.probing.swap(true, Ordering::AcqRel) {
            return;
        }
        let core = self.clone();
        tokio::spawn(async move {
            if let Err(e) = core.probe_version().await {
                tracing::debug!(kid = core.kid, "vault key version probe: {}", e.e);
            }
            core.probing.store(false, Ordering::Release);
        });
    }
}

type TestRun = Shared<BoxFuture<'static, Result<(), Fail>>>;

enum Check {
    Idle,
    Running(TestRun),
    Passed(Instant),
    Failed(Instant, Fail),
}

/// The current key's self-test: one run at a time in its own task, which
/// records its result, so a caller that gives up doesn't cancel it.
struct Verifier {
    state: parking_lot::Mutex<Check>,
}

impl Verifier {
    /// `retest`: a background re-test of a passed key, which only a
    /// rejection overrides (a blip of Vault mustn't fail calls).
    fn start(self: &Arc<Self>, core: &Arc<KeyCore>, retest: bool) -> TestRun {
        let (core, me) = (core.clone(), self.clone());
        let task = tokio::spawn(async move {
            let r = match tokio::time::timeout(SELF_TEST_DEADLINE, core.self_test()).await {
                Ok(r) => r,
                Err(_) => Err(local(SecretError::Unavailable(format!(
                    "vault self-test of {} timed out after {SELF_TEST_DEADLINE:?}",
                    core.name
                )))),
            };
            if !retest || r.as_ref().is_err_and(|f| !f.e.retryable()) {
                me.record(&r);
            }
            r
        });
        async move {
            task.await.unwrap_or_else(|e| Err(local(SecretError::Unavailable(format!("vault self-test: {e}")))))
        }
        .boxed()
        .shared()
    }

    fn record(&self, r: &Result<(), Fail>) {
        *self.state.lock() = match r {
            Ok(()) => Check::Passed(Instant::now()),
            Err(f) => Check::Failed(Instant::now(), f.clone()),
        };
    }

    fn reset(&self) {
        *self.state.lock() = Check::Idle;
    }

    async fn verify(self: &Arc<Self>, core: &Arc<KeyCore>) -> Result<(), Fail> {
        let run = {
            let mut g = self.state.lock();
            match &*g {
                Check::Passed(at) => {
                    if at.elapsed() >= RETEST_EVERY {
                        // keep serving meanwhile; the task runs on without its handle
                        *g = Check::Passed(Instant::now());
                        drop(g);
                        drop(self.start(core, true));
                    }
                    return Ok(());
                }
                Check::Failed(at, f) => {
                    let ttl = if f.e.retryable() { UNAVAILABLE_FOR } else { REJECTED_FOR };
                    if at.elapsed() < ttl {
                        return Err(f.clone());
                    }
                    let run = self.start(core, false);
                    *g = Check::Running(run.clone());
                    run
                }
                Check::Running(run) => run.clone(),
                Check::Idle => {
                    let run = self.start(core, false);
                    *g = Check::Running(run.clone());
                    run
                }
            }
        };
        run.await
    }
}

enum OldCheck {
    Unchecked,
    Passed,
    Failed(Instant, Fail),
}

/// One Transit key. Its kid hashes the namespace, mount and key name: no
/// version, so `rotate` keeps the kid, and no address, so the same Vault
/// under another name (a new DNS name, a load balancer) keeps it too.
pub struct VaultTransit {
    core: Arc<KeyCore>,
    current: bool,
    check: Arc<Verifier>,
    old_check: parking_lot::Mutex<OldCheck>,
}

impl std::fmt::Debug for VaultTransit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultTransit")
            .field("kid", &self.core.kid)
            .field("name", &self.core.name)
            .field("current", &self.current)
            .finish_non_exhaustive()
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
            Some(ns) => format!("vault:{ns}/{mount}/{key}"),
            None => format!("vault:{mount}/{key}"),
        };
        let h = Sha256::digest(name.as_bytes());
        Ok(VaultTransit {
            core: Arc::new(KeyCore {
                kid: format!("V{}", hex::encode(&h[..8])),
                name,
                mount: mount.to_string(),
                key: key.to_string(),
                client,
                latest: AtomicU64::new(0),
                latest_checked: parking_lot::Mutex::new(None),
                probing: AtomicBool::new(false),
            }),
            current,
            check: Arc::new(Verifier { state: parking_lot::Mutex::new(Check::Idle) }),
            old_check: parking_lot::Mutex::new(OldCheck::Unchecked),
        })
    }

    /// `vault:[{namespace}/]{mount}/{key}`, what the kid hashes.
    pub fn name(&self) -> &str {
        &self.core.name
    }

    /// The newest key version this node has seen (0: none yet).
    pub fn latest_version(&self) -> u64 {
        self.core.latest.load(Ordering::Relaxed)
    }

    fn is_token_file(&self) -> bool {
        matches!(self.core.client.auth, VaultAuth::TokenFile(_))
    }

    /// An old key's first unwrap also decrypts under a wrong AAD, which must
    /// fail while the right one works.
    async fn unwrap_old(&self, aad: &[u8], ct: &str) -> Result<Zeroizing<Vec<u8>>, Fail> {
        let passed = match &*self.old_check.lock() {
            OldCheck::Passed => true,
            OldCheck::Failed(at, f) if at.elapsed() < REJECTED_FOR => return Err(f.clone()),
            _ => false,
        };
        if passed {
            return self.core.decrypt(aad, ct).await;
        }
        let failed = |f: Fail| {
            *self.old_check.lock() = OldCheck::Failed(Instant::now(), f.clone());
            Err(f)
        };
        if let Err(f) = self.core.check_key_type().await {
            return failed(f);
        }
        let wrong = [aad, b"\0vlpds-vault-aad-check"].concat();
        let wrong = self.core.decrypt(&wrong, ct).await;
        // a bad blob fails both and proves nothing: the check waits for a good one
        let plaintext = self.core.decrypt(aad, ct).await?;
        match wrong {
            Err(Fail { e: SecretError::Rejected(_), .. }) => {
                *self.old_check.lock() = OldCheck::Passed;
                Ok(plaintext)
            }
            Err(f) => Err(f),
            Ok(_) => failed(self.core.not_enforced()),
        }
    }
}

#[async_trait]
impl KeyWrapper for VaultTransit {
    fn kid(&self) -> &str {
        &self.core.kid
    }
    fn backend(&self) -> &'static str {
        "vault"
    }
    fn remote(&self) -> bool {
        true
    }
    async fn self_test(&self) -> Result<(), SecretError> {
        if !self.current {
            return self.core.probe_decrypt().await.map_err(startup_error);
        }
        let mut attempt = 0;
        loop {
            match self.check.verify(&self.core).await {
                Ok(()) => return Ok(()),
                Err(f) if f.status == Some(403) && self.is_token_file() && attempt < STARTUP_403_RETRIES => {
                    attempt += 1;
                    tracing::warn!(
                        kid = self.core.kid,
                        attempt,
                        "vault refused the token file's token, reading it again shortly: {}",
                        f.e
                    );
                    tokio::time::sleep(STARTUP_403_DELAY).await;
                    self.check.reset();
                }
                Err(f) => return Err(startup_error(f)),
            }
        }
    }
    fn ciphertext_version(&self, ct: &[u8]) -> Option<u64> {
        ciphertext_version(std::str::from_utf8(ct).ok()?)
    }
    async fn refresh_version(&self) -> Result<(), SecretError> {
        if !self.current {
            return Ok(());
        }
        self.check.verify(&self.core).await?;
        Ok(self.core.probe_version().await?)
    }
    async fn wrap(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
        self.check.verify(&self.core).await?;
        Ok(self.core.encrypt(aad, plaintext).await?.into_bytes())
    }
    async fn unwrap(&self, aad: &[u8], ct: &[u8]) -> Result<Unwrapped, SecretError> {
        let ct = std::str::from_utf8(ct).map_err(|_| SecretError::Malformed)?;
        let v = ciphertext_version(ct).ok_or(SecretError::Malformed)?;
        if !self.current {
            let plaintext = self.unwrap_old(aad, ct).await?;
            return Ok(Unwrapped { plaintext, stale: true });
        }
        self.check.verify(&self.core).await?;
        let plaintext = self.core.decrypt(aad, ct).await?;
        self.core.latest.fetch_max(v, Ordering::Relaxed);
        self.core.refresh_in_background();
        Ok(Unwrapped { plaintext, stale: v < self.core.latest.load(Ordering::Relaxed) })
    }
}

#[cfg(test)]
mod tests;
