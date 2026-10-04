use super::*;
use crate::secrets::{KekBytes, KekConfig, LocalKek, Purpose, Secrets};
use std::collections::{HashMap, HashSet};

/// A Transit stand-in: `encrypt`/`decrypt`/`keys` on any key (the key name
/// bound into a local AEAD's AAD, so another key's ciphertext fails),
/// AppRole and Kubernetes logins minting numbered tokens, `renew-self`,
/// `revoke-self`, and switches for the failure modes a real server has.
struct MockVault {
    url: String,
    aead: LocalKek,
    tokens: parking_lot::Mutex<HashSet<String>>,
    minted: AtomicU64,
    logins: AtomicU64,
    renews: AtomicU64,
    revokes: AtomicU64,
    encrypts: AtomicU64,
    ttl: AtomicU64,
    /// 0: the login TTL.
    renew_ttl: AtomicU64,
    renewable: AtomicBool,
    secret_id: parking_lot::Mutex<String>,
    jwt: parking_lot::Mutex<String>,
    version: AtomicU64,
    /// Vault before 1.13: drops associated_data with a warning.
    ignore_aad: AtomicBool,
    /// Drops it silently (a non-AEAD key, a broken fork), on every key or
    /// on the keys (`mount/key`) listed.
    silent_ignore_aad: AtomicBool,
    silent_ignore_keys: parking_lot::Mutex<HashSet<String>>,
    /// `mount/key`s that don't exist: encrypt is a 403 (the policy can't
    /// create), decrypt a 400.
    missing: parking_lot::Mutex<HashSet<String>>,
    /// `keys/{key}`'s type; empty: 403.
    key_type: parking_lot::Mutex<String>,
    namespaces: parking_lot::Mutex<Vec<Option<String>>>,
    /// Path -> status, after the token check (a policy without that path).
    deny: parking_lot::Mutex<HashMap<String, u16>>,
    sealed: AtomicBool,
    omit_plaintext: AtomicBool,
    /// Per request.
    delay_ms: AtomicU64,
    last_encrypt: parking_lot::Mutex<(String, serde_json::Value)>,
}

const ROLE_ID: &str = "role-id-1234";

async fn mock_vault() -> Arc<MockVault> {
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, Method, StatusCode};
    use axum::response::IntoResponse;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let m = Arc::new(MockVault {
        url: format!("http://{}", listener.local_addr().unwrap()),
        aead: LocalKek::new(&KekBytes::random()),
        tokens: parking_lot::Mutex::new(HashSet::new()),
        minted: AtomicU64::new(0),
        logins: AtomicU64::new(0),
        renews: AtomicU64::new(0),
        revokes: AtomicU64::new(0),
        encrypts: AtomicU64::new(0),
        ttl: AtomicU64::new(3600),
        renew_ttl: AtomicU64::new(0),
        renewable: AtomicBool::new(true),
        secret_id: parking_lot::Mutex::new("secret-id-5678".into()),
        jwt: parking_lot::Mutex::new("jwt-1".into()),
        version: AtomicU64::new(1),
        ignore_aad: AtomicBool::new(false),
        silent_ignore_aad: AtomicBool::new(false),
        silent_ignore_keys: parking_lot::Mutex::new(HashSet::new()),
        missing: parking_lot::Mutex::new(HashSet::new()),
        key_type: parking_lot::Mutex::new("aes256-gcm96".into()),
        namespaces: parking_lot::Mutex::new(Vec::new()),
        deny: parking_lot::Mutex::new(HashMap::new()),
        sealed: AtomicBool::new(false),
        omit_plaintext: AtomicBool::new(false),
        delay_ms: AtomicU64::new(0),
        last_encrypt: parking_lot::Mutex::new((String::new(), serde_json::Value::Null)),
    });
    fn err(status: StatusCode, msg: &str) -> axum::response::Response {
        (status, axum::Json(serde_json::json!({"errors": [msg]}))).into_response()
    }
    fn mint(m: &MockVault, ttl: u64) -> axum::response::Response {
        let t = format!("hvs.token-{}", m.minted.fetch_add(1, Ordering::SeqCst) + 1);
        m.tokens.lock().insert(t.clone());
        axum::Json(serde_json::json!({"auth": {"client_token": t, "lease_duration": ttl, "renewable": m.renewable.load(Ordering::SeqCst)}})).into_response()
    }
    async fn handle(
        State(m): State<Arc<MockVault>>,
        method: Method,
        Path(rest): Path<String>,
        headers: HeaderMap,
        body: String,
    ) -> axum::response::Response {
        let b64 = base64::engine::general_purpose::STANDARD;
        let delay = m.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        m.namespaces.lock().push(headers.get("x-vault-namespace").map(|v| v.to_str().unwrap().to_string()));
        if m.sealed.load(Ordering::SeqCst) {
            return err(StatusCode::SERVICE_UNAVAILABLE, "Vault is sealed");
        }
        let body: serde_json::Value =
            if body.is_empty() { serde_json::Value::Null } else { serde_json::from_str(&body).unwrap() };
        if let Some(mount) = rest.strip_prefix("auth/").and_then(|r| r.strip_suffix("/login")) {
            let ok = match mount {
                "approle" | "ar2" => body["role_id"] == ROLE_ID && body["secret_id"] == *m.secret_id.lock(),
                "kubernetes" => body["role"] == "vlpds" && body["jwt"] == *m.jwt.lock(),
                _ => return err(StatusCode::NOT_FOUND, "no handler for route"),
            };
            if !ok {
                return err(StatusCode::BAD_REQUEST, "invalid role or secret ID");
            }
            m.logins.fetch_add(1, Ordering::SeqCst);
            return mint(&m, m.ttl.load(Ordering::SeqCst));
        }
        let token = headers.get("x-vault-token").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        if !m.tokens.lock().contains(&token) {
            return err(StatusCode::FORBIDDEN, "permission denied");
        }
        if let Some(&st) = m.deny.lock().get(&rest) {
            return err(StatusCode::from_u16(st).unwrap(), "permission denied");
        }
        if rest == "auth/token/revoke-self" {
            m.tokens.lock().remove(&token);
            m.revokes.fetch_add(1, Ordering::SeqCst);
            return StatusCode::NO_CONTENT.into_response();
        }
        if rest == "auth/token/renew-self" {
            m.renews.fetch_add(1, Ordering::SeqCst);
            let ttl = match m.renew_ttl.load(Ordering::SeqCst) {
                0 => m.ttl.load(Ordering::SeqCst),
                t => t,
            };
            return axum::Json(
                serde_json::json!({"auth": {"client_token": token, "lease_duration": ttl, "renewable": true}}),
            )
            .into_response();
        }
        let parts: Vec<&str> = rest.rsplitn(3, '/').collect();
        let [key, op, mount] = parts[..] else { return err(StatusCode::NOT_FOUND, "no handler for route") };
        let mk = format!("{mount}/{key}");
        if m.missing.lock().contains(&mk) {
            return match op {
                "encrypt" => err(StatusCode::FORBIDDEN, "permission denied"),
                "decrypt" => err(StatusCode::BAD_REQUEST, "encryption key not found"),
                _ => err(StatusCode::NOT_FOUND, ""),
            };
        }
        let ignore = m.ignore_aad.load(Ordering::SeqCst)
            || m.silent_ignore_aad.load(Ordering::SeqCst)
            || m.silent_ignore_keys.lock().contains(&mk);
        let aad = [
            format!("{mk}\0").into_bytes(),
            if ignore { vec![] } else { b64.decode(body["associated_data"].as_str().unwrap_or("")).unwrap() },
        ]
        .concat();
        let warnings = if m.ignore_aad.load(Ordering::SeqCst) {
            serde_json::json!(["Endpoint ignored these unrecognized parameters: [associated_data]"])
        } else {
            serde_json::Value::Null
        };
        match (method, op) {
            (Method::GET, "keys") => {
                let ty = m.key_type.lock().clone();
                if ty.is_empty() {
                    return err(StatusCode::FORBIDDEN, "permission denied");
                }
                axum::Json(serde_json::json!({"data": {"type": ty, "derived": false, "latest_version": m.version.load(Ordering::SeqCst)}})).into_response()
            }
            (Method::POST, "encrypt") => {
                m.encrypts.fetch_add(1, Ordering::SeqCst);
                *m.last_encrypt.lock() = (mk.clone(), body.clone());
                let pt = b64.decode(body["plaintext"].as_str().unwrap()).unwrap();
                let v = m.version.load(Ordering::SeqCst);
                let ct = format!("vault:v{v}:{}", b64.encode(m.aead.wrap_sync(&aad, &pt)));
                axum::Json(serde_json::json!({"data": {"ciphertext": ct, "key_version": v}, "warnings": warnings}))
                    .into_response()
            }
            (Method::POST, "decrypt") => {
                let ct = body["ciphertext"].as_str().unwrap();
                let Some(raw) = ct.splitn(3, ':').nth(2).and_then(|b| b64.decode(b).ok()) else {
                    return err(StatusCode::BAD_REQUEST, "invalid ciphertext");
                };
                match m.aead.unwrap_sync(&aad, &raw) {
                    Ok(_) if m.omit_plaintext.load(Ordering::SeqCst) => {
                        axum::Json(serde_json::json!({"data": {}})).into_response()
                    }
                    Ok(pt) => {
                        axum::Json(serde_json::json!({"data": {"plaintext": b64.encode(&*pt)}, "warnings": warnings}))
                            .into_response()
                    }
                    Err(_) => err(StatusCode::BAD_REQUEST, "cipher: message authentication failed"),
                }
            }
            _ => err(StatusCode::NOT_FOUND, "no handler for route"),
        }
    }
    let app = axum::Router::new().route("/v1/{*rest}", axum::routing::post(handle).get(handle)).with_state(m.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    m
}

impl MockVault {
    fn static_token(&self) -> VaultAuth {
        self.tokens.lock().insert("hvs.static".into());
        VaultAuth::Static("hvs.static".into())
    }

    fn cfg(&self, auth: VaultAuth) -> VaultConfig {
        VaultConfig { addr: self.url.clone(), namespace: None, ca_pem: None, ca_only: false, auth }
    }

    fn approle(&self, secret_id_file: &Path) -> VaultAuth {
        VaultAuth::AppRole { mount: "approle".into(), role_id: ROLE_ID.into(), secret_id_file: secret_id_file.into() }
    }

    fn revoke_all(&self) {
        self.tokens.lock().clear();
    }
}

fn kek(v: VaultConfig, key: &str, old: &[&str]) -> KekConfig {
    KekConfig {
        vault: Some(v),
        vault_key: Some(key.into()),
        vault_old_keys: old.iter().map(|k| k.to_string()).collect(),
        ..Default::default()
    }
}

fn client(addr: &str, dev_mode: bool) -> anyhow::Result<Arc<VaultClient>> {
    VaultClient::new(
        &VaultConfig {
            addr: addr.into(),
            namespace: None,
            ca_pem: None,
            ca_only: false,
            auth: VaultAuth::Static("x".into()),
        },
        dev_mode,
        4,
    )
}

fn tmp_file(contents: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("vlpds-vault-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::write(&p, contents).unwrap();
    p
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("never: {what}");
}

#[test]
fn ciphertext_versions() {
    assert_eq!(ciphertext_version("vault:v1:AAAA"), Some(1));
    assert_eq!(ciphertext_version("vault:v12:ab+/cd=="), Some(12));
    for bad in ["vault:v0:AAAA", "vault:v1:", "vault:vx:AAAA", "vault:v1:AA\"AA", "v1:AAAA", "vault:1:AAAA", ""] {
        assert_eq!(ciphertext_version(bad), None, "{bad}");
    }
}

/// Only Vault's "ignored / unrecognized parameters" warning about
/// associated_data refuses a response, not any warning that names it.
#[test]
fn aad_warning_match() {
    let w = |s: &str| serde_json::json!({"warnings": [s]});
    assert!(check_warnings(&w("Endpoint ignored these unrecognized parameters: [associated_data]"), "x").is_err());
    assert!(check_warnings(&w("associated_data was ignored for this key type"), "x").is_err());
    assert!(check_warnings(&w("associated_data is deprecated in favour of aad"), "x").is_ok());
    assert!(check_warnings(&w("Endpoint ignored these unrecognized parameters: [batch_input]"), "x").is_ok());
    assert!(check_warnings(&serde_json::json!({"warnings": null}), "x").is_ok());
}

/// A huge or zero lease doesn't panic the `Instant` math.
#[test]
fn lease_durations_are_clamped() {
    let s = Session::new(Zeroizing::new("t".into()), u64::MAX, true, u64::MAX);
    assert!(s.expires_at.is_some() && s.refresh_at > Instant::now());
    let s = Session::new(Zeroizing::new("t".into()), 0, true, 0);
    assert!(s.expires_at.is_none() && !s.renew);
}

/// https only, except to loopback or in dev mode; no credentials in the URL.
#[test]
fn vault_addr_rules() {
    assert!(client("https://vault.example:8200", false).is_ok());
    let e = client("http://vault.example:8200", false).unwrap_err().to_string();
    assert!(e.contains("https"), "{e}");
    assert!(client("http://vault.example:8200", true).is_ok());
    for lo in ["http://127.0.0.1:8200", "http://localhost:8200", "http://[::1]:8200", "http://127.9.9.9"] {
        assert!(client(lo, false).is_ok(), "{lo}");
    }
    for bad in [
        "https://user:pw@vault.example",
        "https://user@vault.example",
        "vault.example",
        "ftp://vault.example",
        "https://vault.example/v1",
        "https://vault.example/?x=1",
    ] {
        assert!(client(bad, true).is_err(), "{bad}");
    }
    let only = VaultConfig {
        addr: "https://vault.example".into(),
        namespace: None,
        ca_pem: None,
        ca_only: true,
        auth: VaultAuth::Static("x".into()),
    };
    assert!(VaultClient::new(&only, false, 4).is_err(), "ca_only without a CA");
}

/// The wire format: base64 plaintext and AAD, the stored blob Transit's
/// own string, the namespace header on every call, a kid stable across
/// versions and addresses, different per namespace, mount and key.
#[tokio::test]
async fn request_shape_aad_and_namespace() {
    let m = mock_vault().await;
    let mut cfg = m.cfg(m.static_token());
    cfg.namespace = Some("/team-a/".into());
    let s = Secrets::from_config(&kek(cfg.clone(), "transit/vlpds", &[]), false).unwrap();
    assert!(s.current_kid().starts_with('V'));
    let secret = [7u8; 32];
    let blob = s.wrap(Purpose::SigningKey, "did:plc:a", &secret).await.unwrap();
    let (path, body) = m.last_encrypt.lock().clone();
    assert_eq!(path, "transit/vlpds");
    assert_eq!(body["plaintext"], b64(&secret));
    assert_eq!(body["associated_data"], b64(&crate::secrets::aad(Purpose::SigningKey, "did:plc:a")));
    let (kid, ct) = crate::secrets::parse_blob(&blob).unwrap();
    assert_eq!(kid, s.current_kid());
    assert!(std::str::from_utf8(&ct).unwrap().starts_with("vault:v1:"));
    assert_eq!(s.blob_version(&blob), Some((kid.to_string(), 1)));
    assert!(m.namespaces.lock().iter().all(|n| n.as_deref() == Some("team-a")), "{:?}", m.namespaces.lock());
    let u = s.unwrap(Purpose::SigningKey, "did:plc:a", &blob).await.unwrap();
    assert_eq!(&u.plaintext[..], &secret);
    assert!(!u.stale);
    // another subject or purpose: Transit refuses (400), a Rejected
    assert!(matches!(s.unwrap(Purpose::SigningKey, "did:plc:b", &blob).await, Err(SecretError::Rejected(_))));
    assert!(matches!(s.unwrap(Purpose::Totp, "did:plc:a", &blob).await, Err(SecretError::Rejected(_))));
    // the kid: namespace, mount and key count, the address doesn't
    let kid = |addr: &str, ns: Option<&str>, key: &str| {
        let c = VaultClient::new(
            &VaultConfig {
                addr: addr.into(),
                namespace: ns.map(str::to_string),
                ca_pem: None,
                ca_only: false,
                auth: VaultAuth::Static("x".into()),
            },
            true,
            4,
        )
        .unwrap();
        VaultTransit::new(c, key, true).unwrap().kid().to_string()
    };
    let k = kid("https://vault.example:8200", None, "transit/vlpds");
    assert_eq!(k, kid("http://vault.example/", None, "/transit/vlpds/"));
    assert_eq!(k, kid("https://vault2.example:8200", None, "transit/vlpds"));
    assert_eq!(k, kid("http://10.0.0.7:8200", None, "transit/vlpds"));
    assert_ne!(k, kid("https://vault.example:8200", Some("ns"), "transit/vlpds"));
    assert_ne!(k, kid("https://vault.example:8200", None, "transit/other"));
    assert_ne!(k, kid("https://vault.example:8200", None, "transit2/vlpds"));
    let c = client(&m.url, false).unwrap();
    for bad in ["vlpds", "transit/", "/vlpds", "transit/../vlpds", "transit/vl pds", "transit//vlpds"] {
        assert!(VaultTransit::new(c.clone(), bad, true).is_err(), "{bad}");
    }
    assert_eq!(VaultTransit::new(c.clone(), "a/b/vlpds", true).unwrap().core.mount, "a/b");
}

/// Before its first use, the current key wraps under AAD A and must refuse
/// the unwrap under AAD B. A server that drops associated_data (Vault
/// before 1.13 says so in a warning; a non-AEAD key doesn't) and a key of
/// the wrong type are refused at startup; an unreachable one is checked
/// again at first use.
#[tokio::test]
async fn startup_aad_self_test() {
    let m = mock_vault().await;
    let auth = m.static_token();
    let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    let n = m.encrypts.load(Ordering::SeqCst);
    // verified once: wraps don't repeat it
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!(m.encrypts.load(Ordering::SeqCst), n + 2);

    m.ignore_aad.store(true, Ordering::SeqCst);
    let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/vlpds", &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("ignored associated_data") && e.contains("1.13"), "{e}");
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Rejected(_))));
    m.ignore_aad.store(false, Ordering::SeqCst);

    m.silent_ignore_aad.store(true, Ordering::SeqCst);
    let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/vlpds", &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("wrong associated_data"), "{e}");
    m.silent_ignore_aad.store(false, Ordering::SeqCst);

    *m.key_type.lock() = "rsa-2048".into();
    let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/vlpds", &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("rsa-2048") && e.contains("AEAD"), "{e}");
    // a policy without read on the key: only the AAD check runs
    *m.key_type.lock() = String::new();
    let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();

    // unreachable: starts, and the first wrap is a retryable failure
    let dead = VaultConfig { addr: "http://127.0.0.1:9".into(), ..m.cfg(auth) };
    let s = Secrets::from_config(&kek(dead, "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Unavailable(_))));
}

/// A self-test that found a server not enforcing AAD isn't rerun on every
/// call: its rejection is reused for a while.
#[tokio::test]
async fn rejected_self_test_is_cached() {
    let m = mock_vault().await;
    m.sealed.store(true, Ordering::SeqCst);
    let s = Secrets::from_config(&kek(m.cfg(m.static_token()), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    m.sealed.store(false, Ordering::SeqCst);
    m.silent_ignore_aad.store(true, Ordering::SeqCst);
    tokio::time::sleep(UNAVAILABLE_FOR).await;
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Rejected(_))));
    let n = m.encrypts.load(Ordering::SeqCst);
    for _ in 0..5 {
        assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Rejected(_))));
    }
    assert_eq!(m.encrypts.load(Ordering::SeqCst), n, "no new self-test within REJECTED_FOR");
}

/// With Vault ~1 s per round trip, the self-test (4 calls) and a wrap don't
/// fit one caller's 5 s, but the test runs on in its own task: a node that
/// started while Vault was down serves once it's back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_vault_after_a_deferred_start_serves() {
    let m = mock_vault().await;
    m.sealed.store(true, Ordering::SeqCst);
    let s = Secrets::from_config(&kek(m.cfg(m.static_token()), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    m.sealed.store(false, Ordering::SeqCst);
    m.delay_ms.store(1100, Ordering::SeqCst);
    let start = Instant::now();
    let mut failures = 0;
    loop {
        match s.wrap(Purpose::Totp, "did:plc:a", b"x").await {
            Ok(_) => break,
            Err(e) => {
                assert!(e.retryable(), "{e}");
                failures += 1;
            }
        }
        assert!(start.elapsed() < Duration::from_secs(40), "never served");
    }
    assert!(failures >= 1, "the first call can't fit the self-test and a wrap");
}

/// After `rotate`, blobs under older versions unwrap as stale. An unwrap
/// never waits on the version check: it uses the latest version seen and
/// refreshes it in the background once a minute. A rewrap asks first, and
/// a failed ask leaves the old answer marked old.
#[tokio::test]
async fn transit_rotate_marks_old_versions_stale() {
    let m = mock_vault().await;
    let s = Secrets::from_config(&kek(m.cfg(m.static_token()), "transit/vlpds", &[]), false).unwrap();
    let kid = s.current_kid().to_string();
    let v1 = s.wrap(Purpose::Totp, "did:plc:a", b"JBSWY3DP").await.unwrap();
    m.version.store(2, Ordering::SeqCst);
    let n = m.encrypts.load(Ordering::SeqCst);
    // the node last asked under a minute ago: still v1 as far as it knows
    assert!(!s.unwrap(Purpose::Totp, "did:plc:a", &v1).await.unwrap().stale);
    assert_eq!(m.encrypts.load(Ordering::SeqCst), n, "no probe on the unwrap path");
    // the rewrap's synchronous ask sees v2
    s.refresh_versions().await.unwrap();
    assert!(s.unwrap(Purpose::Totp, "did:plc:a", &v1).await.unwrap().stale);
    let v2 = s.rewrap(Purpose::Totp, "did:plc:a", &v1).await.unwrap().expect("stale");
    assert_eq!(s.current_kid(), kid, "same kid across versions");
    assert!(s.is_current(&v1), "the kid pre-filter can't see versions: --check-versions");
    assert_eq!(s.blob_version(&v2), Some((kid.clone(), 2)));
    assert_eq!(s.rewrap(Purpose::Totp, "did:plc:a", &v2).await.unwrap(), None);
    // a failed ask fails the rewrap and doesn't count as having asked
    m.version.store(3, Ordering::SeqCst);
    let c = VaultClient::new(&m.cfg(m.static_token()), false, 4).unwrap();
    let vt = VaultTransit::new(c, "transit/vlpds", true).unwrap();
    vt.refresh_version().await.unwrap();
    assert_eq!(vt.latest_version(), 3);
    let asked = *vt.core.latest_checked.lock();
    m.deny.lock().insert("transit/encrypt/vlpds".into(), 403);
    m.version.store(4, Ordering::SeqCst);
    assert!(vt.refresh_version().await.is_err());
    assert_eq!(*vt.core.latest_checked.lock(), asked);
    m.deny.lock().clear();
    // the background refresh: due once VERSION_RECHECK is up, the unwrap answers at once
    *vt.core.latest_checked.lock() = Some(Instant::now() - VERSION_RECHECK);
    let ct = crate::secrets::parse_blob(&v2).unwrap().1;
    let aad = crate::secrets::aad(Purpose::Totp, "did:plc:a");
    assert!(vt.unwrap(&aad, &ct).await.unwrap().stale, "v2 < 3, known already");
    eventually("background probe", || vt.latest_version() == 4).await;
}

/// Moving keys: B current, A unwrap-only; A's blobs are stale and rewrap
/// to B, and A's ciphertext is refused by B.
#[tokio::test]
async fn old_key_flag_moves_between_keys() {
    let m = mock_vault().await;
    let auth = m.static_token();
    let a = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/a", &[]), false).unwrap();
    let blob = a.wrap(Purpose::SigningKey, "did:plc:x", &[1u8; 32]).await.unwrap();
    let both = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit-b/b", &["transit/a"]), false).unwrap();
    both.check_key_service().await.unwrap();
    assert_eq!(both.kids(), vec![both.current_kid().to_string(), a.current_kid().to_string()]);
    let u = both.unwrap(Purpose::SigningKey, "did:plc:x", &blob).await.unwrap();
    assert!(u.stale);
    let moved = both.rewrap(Purpose::SigningKey, "did:plc:x", &blob).await.unwrap().unwrap();
    assert!(both.is_current(&moved));
    // A's ciphertext under B's kid: B refuses it
    let forged = blob.replacen(a.current_kid(), both.current_kid(), 1);
    assert!(matches!(both.unwrap(Purpose::SigningKey, "did:plc:x", &forged).await, Err(SecretError::Rejected(_))));
    let b_only = Secrets::from_config(&kek(m.cfg(auth), "transit-b/b", &[]), false).unwrap();
    assert!(matches!(b_only.unwrap(Purpose::SigningKey, "did:plc:x", &blob).await, Err(SecretError::UnknownKek(_))));
    assert_eq!(&b_only.unwrap(Purpose::SigningKey, "did:plc:x", &moved).await.unwrap().plaintext[..], &[1u8; 32]);
}

/// Unwrap-only keys are checked too: at startup the policy must allow
/// decrypt (a junk ciphertext gets Transit's 400) and the key must exist;
/// before the first unwrap, a wrong AAD must fail where the right one works.
#[tokio::test]
async fn old_keys_are_checked() {
    let m = mock_vault().await;
    let auth = m.static_token();
    let start = |old: &str| {
        let s = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/new", &[old]), false).unwrap();
        async move { (s.check_key_service().await, s) }
    };
    start("transit/old").await.0.unwrap();
    m.deny.lock().insert("transit/decrypt/old".into(), 403);
    let e = start("transit/old").await.0.unwrap_err().to_string();
    assert!(e.contains("lacks update on transit/decrypt/old"), "{e}");
    m.deny.lock().clear();
    m.missing.lock().insert("transit/gone".into());
    let e = start("transit/gone").await.0.unwrap_err().to_string();
    assert!(e.contains("no such key at transit/decrypt/gone"), "{e}");

    // an old key on which Transit doesn't enforce AAD: its blobs, bound to
    // nothing, are refused instead of unwrapped
    m.silent_ignore_keys.lock().insert("transit/old".into());
    let old = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/old", &[]), false).unwrap();
    let unbound = {
        let vt =
            VaultTransit::new(VaultClient::new(&m.cfg(auth.clone()), false, 4).unwrap(), "transit/old", false).unwrap();
        let aad = crate::secrets::aad(Purpose::Totp, "did:plc:a");
        let ct = vt.core.encrypt(&aad, b"x").await.ok().unwrap();
        format!("vw1.{}.{}", old.current_kid(), base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(ct))
    };
    let (r, s) = start("transit/old").await;
    r.unwrap();
    match s.unwrap(Purpose::Totp, "did:plc:a", &unbound).await {
        Err(SecretError::Rejected(e)) => assert!(e.contains("wrong associated_data"), "{e}"),
        r => panic!("{:?}", r.map(|_| ())),
    }
    // an enforcing old key passes once and isn't re-checked
    m.silent_ignore_keys.lock().clear();
    let a = Secrets::from_config(&kek(m.cfg(auth.clone()), "transit/old", &[]), false).unwrap();
    let good = a.wrap(Purpose::Totp, "did:plc:a", b"y").await.unwrap();
    let (r, s) = start("transit/old").await;
    r.unwrap();
    assert_eq!(&s.unwrap(Purpose::Totp, "did:plc:a", &good).await.unwrap().plaintext[..], b"y");
    assert!(s.unwrap(Purpose::Totp, "did:plc:a", &good).await.unwrap().stale);
}

/// A passed key is re-tested in the background once an hour: calls keep
/// being served meanwhile, and a Vault that stopped enforcing AAD (swapped
/// for an older one) is refused once the re-test lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hourly_retest_catches_a_downgrade() {
    let m = mock_vault().await;
    let c = VaultClient::new(&m.cfg(m.static_token()), false, 4).unwrap();
    let vt = VaultTransit::new(c, "transit/vlpds", true).unwrap();
    vt.wrap(b"aad", b"x").await.unwrap();
    *vt.check.state.lock() = Check::Passed(Instant::now() - RETEST_EVERY);
    m.silent_ignore_aad.store(true, Ordering::SeqCst);
    vt.wrap(b"aad", b"x").await.unwrap();
    eventually("the re-test lands", || matches!(*vt.check.state.lock(), Check::Failed(..))).await;
    assert!(matches!(vt.wrap(b"aad", b"x").await, Err(SecretError::Rejected(_))));
}

/// A decrypt answer without a plaintext is an error, never empty bytes.
#[tokio::test]
async fn decrypt_without_plaintext_is_an_error() {
    let m = mock_vault().await;
    let s = Secrets::from_config(&kek(m.cfg(m.static_token()), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    let b = s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    m.omit_plaintext.store(true, Ordering::SeqCst);
    match s.unwrap(Purpose::Totp, "did:plc:a", &b).await {
        Err(SecretError::Unavailable(e)) => assert!(e.contains("no plaintext"), "{e}"),
        r => panic!("{:?}", r.map(|u| u.plaintext.len())),
    }
}

/// A token file is read once, then again after a 403 (a Vault Agent wrote
/// a new token): the call succeeds on the retry. A file holding a dead
/// token is a retryable failure after one re-read.
#[tokio::test]
async fn token_file_reloaded_on_403() {
    let m = mock_vault().await;
    m.tokens.lock().insert("hvs.one".into());
    let f = tmp_file("hvs.one\n");
    let s = Secrets::from_config(&kek(m.cfg(VaultAuth::TokenFile(f.clone())), "transit/vlpds", &[]), false).unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    m.revoke_all();
    m.tokens.lock().insert("hvs.two".into());
    std::fs::write(&f, "hvs.two").unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    m.revoke_all();
    tokio::time::sleep(FORCED_RELOGIN_GAP).await;
    match s.wrap(Purpose::Totp, "did:plc:a", b"x").await {
        Err(SecretError::Unavailable(e)) => {
            assert!(e.contains("403") && !e.contains("hvs.two"), "{e}")
        }
        r => panic!("{:?}", r.map(|_| ())),
    }
    std::fs::remove_file(&f).unwrap();
    tokio::time::sleep(FORCED_RELOGIN_GAP).await;
    match s.wrap(Purpose::Totp, "did:plc:a", b"x").await {
        Err(SecretError::Unavailable(e)) => assert!(e.contains("token file"), "{e}"),
        r => panic!("{:?}", r.map(|_| ())),
    }
}

/// At startup a token file whose token Vault refuses is read again a few
/// times (a sink on a persistent volume can hold the last run's token)
/// before the node gives up; a missing token file only defers the check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_token_file_403_is_retried() {
    let m = mock_vault().await;
    m.tokens.lock().insert("hvs.fresh".into());
    let f = tmp_file("hvs.stale");
    let s = Secrets::from_config(&kek(m.cfg(VaultAuth::TokenFile(f.clone())), "transit/vlpds", &[]), false).unwrap();
    let (f2, delay) = (f.clone(), STARTUP_403_DELAY);
    tokio::spawn(async move {
        tokio::time::sleep(delay + delay / 2).await;
        std::fs::write(&f2, "hvs.fresh").unwrap();
    });
    s.check_key_service().await.unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    std::fs::write(&f, "hvs.stale").unwrap();
    let s = Secrets::from_config(&kek(m.cfg(VaultAuth::TokenFile(f.clone())), "transit/vlpds", &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("lacks update on transit/encrypt/vlpds"), "{e}");
    std::fs::remove_file(&f).unwrap();
    let s = Secrets::from_config(&kek(m.cfg(VaultAuth::TokenFile(f.clone())), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
}

/// A missing secret-ID or service-account token file is a setup mistake:
/// fatal at startup, naming the file.
#[tokio::test]
async fn missing_credential_files_are_fatal_at_startup() {
    let m = mock_vault().await;
    let gone = std::env::temp_dir().join("vlpds-vault-no-such-file");
    for auth in [
        m.approle(&gone),
        VaultAuth::Kubernetes { mount: "kubernetes".into(), role: "vlpds".into(), jwt_file: gone.clone() },
    ] {
        let s = Secrets::from_config(&kek(m.cfg(auth), "transit/vlpds", &[]), false).unwrap();
        let e = s.check_key_service().await.unwrap_err().to_string();
        assert!(e.contains("vlpds-vault-no-such-file") && e.contains("can't read it"), "{e}");
    }
}

/// AppRole: one login serves every call and every key; the token is
/// renewed before it expires, a renewal capped by max_ttl leads to a fresh
/// login before the token dies (and the replaced token is revoked), a 403
/// logs in again once, and a login the server refuses is a retryable outage
/// that names no credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approle_login_renew_and_relogin() {
    let m = mock_vault().await;
    m.ttl.store(3, Ordering::SeqCst);
    let sid = tmp_file("secret-id-5678\n");
    let s = Secrets::from_config(&kek(m.cfg(m.approle(&sid)), "transit/vlpds", &["transit/old"]), false).unwrap();
    for i in 0..5 {
        let w = s.wrap(Purpose::Totp, &format!("did:plc:{i}"), b"x").await.unwrap();
        s.unwrap(Purpose::Totp, &format!("did:plc:{i}"), &w).await.unwrap();
    }
    assert_eq!(m.logins.load(Ordering::SeqCst), 1, "one login for every call");
    // TTL 3 s: refreshed a second before expiry, by renewal
    tokio::time::sleep(Duration::from_millis(2100)).await;
    s.wrap(Purpose::Totp, "did:plc:r", b"x").await.unwrap();
    assert_eq!((m.logins.load(Ordering::SeqCst), m.renews.load(Ordering::SeqCst)), (1, 1));
    // max_ttl caps the next renewal (2 s < 3 s): kept, then a login instead
    m.renew_ttl.store(2, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    s.wrap(Purpose::Totp, "did:plc:r", b"x").await.unwrap();
    assert_eq!((m.logins.load(Ordering::SeqCst), m.renews.load(Ordering::SeqCst)), (1, 2));
    tokio::time::sleep(Duration::from_millis(1400)).await;
    s.wrap(Purpose::Totp, "did:plc:r", b"x").await.unwrap();
    assert_eq!((m.logins.load(Ordering::SeqCst), m.renews.load(Ordering::SeqCst)), (2, 2), "re-login near max_ttl");
    eventually("the replaced token revoked", || m.revokes.load(Ordering::SeqCst) == 1).await;
    assert!(!m.tokens.lock().contains("hvs.token-1"));
    // revoked mid-run: a 403, one login, and the call succeeds
    m.revoke_all();
    s.wrap(Purpose::Totp, "did:plc:r", b"x").await.unwrap();
    assert_eq!(m.logins.load(Ordering::SeqCst), 3);
    // the secret ID was rotated on the server but not the file: unavailable
    *m.secret_id.lock() = "secret-id-new".into();
    m.revoke_all();
    tokio::time::sleep(FORCED_RELOGIN_GAP).await;
    match s.wrap(Purpose::Totp, "did:plc:r", b"x").await {
        Err(e @ SecretError::Unavailable(_)) => {
            let e = e.to_string();
            assert!(e.contains("approle login") && !e.contains("secret-id") && !e.contains(ROLE_ID), "{e}");
        }
        r => panic!("{:?}", r.map(|_| ())),
    }
    // the file is read at each login: a new secret ID works without a restart
    std::fs::write(&sid, "secret-id-new").unwrap();
    tokio::time::sleep(FORCED_RELOGIN_GAP).await;
    s.wrap(Purpose::Totp, "did:plc:r", b"x").await.unwrap();
    std::fs::remove_file(&sid).unwrap();
}

/// Many calls getting a 403 at once (the token was revoked) log in once:
/// the others use the token that login got. A token that keeps getting
/// 403s isn't replaced more than once per FORCED_RELOGIN_GAP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_403s_log_in_once() {
    let m = mock_vault().await;
    let sid = tmp_file("secret-id-5678");
    let s = Arc::new(Secrets::from_config(&kek(m.cfg(m.approle(&sid)), "transit/vlpds", &[]), false).unwrap());
    s.check_key_service().await.unwrap();
    let blob = s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!(m.logins.load(Ordering::SeqCst), 1);
    m.revoke_all();
    let calls = (0..32).map(|i| {
        let (s, blob) = (s.clone(), blob.clone());
        tokio::spawn(async move {
            if i % 2 == 0 {
                s.wrap(Purpose::Totp, "did:plc:a", b"x").await.map(|_| ())
            } else {
                s.unwrap(Purpose::Totp, "did:plc:a", &blob).await.map(|_| ())
            }
        })
    });
    for r in futures::future::join_all(calls).await {
        r.unwrap().unwrap();
    }
    assert_eq!(m.logins.load(Ordering::SeqCst), 2, "one login for 32 refused calls");
    // a policy refusing every token: no login per request
    m.deny.lock().insert("transit/encrypt/vlpds".into(), 403);
    tokio::time::sleep(FORCED_RELOGIN_GAP).await;
    let calls = (0..16).map(|_| {
        let s = s.clone();
        tokio::spawn(async move { s.wrap(Purpose::Totp, "did:plc:a", b"x").await })
    });
    for r in futures::future::join_all(calls).await {
        assert!(matches!(r.unwrap(), Err(SecretError::Unavailable(_))));
    }
    assert_eq!(m.logins.load(Ordering::SeqCst), 3);
    std::fs::remove_file(&sid).unwrap();
}

/// A refresh that fails while the token is still valid keeps the old token
/// serving until it really expires; then calls fail as unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_refresh_keeps_a_valid_token() {
    let m = mock_vault().await;
    m.ttl.store(3, Ordering::SeqCst);
    m.renewable.store(false, Ordering::SeqCst);
    let sid = tmp_file("secret-id-5678");
    let s = Secrets::from_config(&kek(m.cfg(m.approle(&sid)), "transit/vlpds", &[]), false).unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    *m.secret_id.lock() = "rotated-elsewhere".into();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!(m.logins.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Unavailable(_))));
    std::fs::remove_file(&sid).unwrap();
}

/// Kubernetes: a non-renewable token is replaced by a fresh login, reading
/// the projected service-account token again (it rotates), and the old
/// token is revoked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kubernetes_login_rereads_the_jwt() {
    let m = mock_vault().await;
    m.ttl.store(3, Ordering::SeqCst);
    m.renewable.store(false, Ordering::SeqCst);
    let jwt = tmp_file("jwt-1\n");
    let auth = VaultAuth::Kubernetes { mount: "kubernetes".into(), role: "vlpds".into(), jwt_file: jwt.clone() };
    let s = Secrets::from_config(&kek(m.cfg(auth), "transit/vlpds", &[]), false).unwrap();
    s.check_key_service().await.unwrap();
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!(m.logins.load(Ordering::SeqCst), 1);
    *m.jwt.lock() = "jwt-2".into();
    std::fs::write(&jwt, "jwt-2").unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!((m.logins.load(Ordering::SeqCst), m.renews.load(Ordering::SeqCst)), (2, 0));
    eventually("the replaced token revoked", || m.revokes.load(Ordering::SeqCst) == 1).await;
    std::fs::remove_file(&jwt).unwrap();
}

/// Debug output and errors never carry a token, role ID, secret ID or JWT.
#[test]
fn redaction() {
    let cfg = VaultConfig {
        addr: "https://vault.example".into(),
        namespace: None,
        ca_pem: None,
        ca_only: false,
        auth: VaultAuth::Static("hvs.SECRET-TOKEN".into()),
    };
    let ar = VaultAuth::AppRole {
        mount: "approle".into(),
        role_id: "ROLE-ID-VALUE".into(),
        secret_id_file: "/run/vlpds/secret-id".into(),
    };
    let k = VaultAuth::Kubernetes { mount: "kubernetes".into(), role: "vlpds".into(), jwt_file: "/var/run/t".into() };
    let c = VaultClient::new(&cfg, false, 4).unwrap();
    for d in [format!("{cfg:?}"), format!("{ar:?}"), format!("{k:?}"), format!("{c:?}")] {
        assert!(!d.contains("SECRET-TOKEN") && !d.contains("ROLE-ID-VALUE"), "{d}");
    }
    let kc = KekConfig { vault: Some(VaultConfig { auth: ar, ..cfg }), ..Default::default() };
    assert!(!format!("{kc:?}").contains("ROLE-ID-VALUE"));
}

/// One current key service; Vault satisfies the KEK requirement; a Vault
/// key needs a server and an auth method.
#[test]
fn config_checks() {
    let v = VaultConfig {
        addr: "https://vault.example".into(),
        namespace: None,
        ca_pem: None,
        ca_only: false,
        auth: VaultAuth::Static("t".into()),
    };
    assert!(kek(v.clone(), "transit/vlpds", &[]).check(false).is_ok());
    let both = KekConfig {
        gcp_key: Some("projects/p/locations/l/keyRings/r/cryptoKeys/k".into()),
        ..kek(v.clone(), "transit/vlpds", &[])
    };
    assert!(format!("{:#}", both.check(false).unwrap_err()).contains("not both"));
    assert!(Secrets::from_config(&both, false).is_err());
    // Vault -> Cloud KMS: the Vault key stays for unwrap
    let moving = KekConfig {
        gcp_key: Some("projects/p/locations/l/keyRings/r/cryptoKeys/k".into()),
        vault: Some(v.clone()),
        vault_old_keys: vec!["transit/vlpds".into()],
        gcp_token: Some(crate::secrets::GcpToken::Static("x".into())),
        ..Default::default()
    };
    moving.check(false).unwrap();
    let s = Secrets::from_config(&moving, false).unwrap();
    assert!(s.current_kid().starts_with('G') && s.kids()[1].starts_with('V'), "{:?}", s.kids());
    let no_server = KekConfig { vault_key: Some("transit/vlpds".into()), ..Default::default() };
    assert!(no_server.check(false).is_err());
    assert!(no_server.check(true).is_err());
    // a local KEK stays unwrap-only next to it
    let k = KekBytes::random();
    let s = Secrets::from_config(&KekConfig { local: Some(k.clone()), ..kek(v.clone(), "transit/vlpds", &[]) }, false)
        .unwrap();
    assert_eq!(s.kids()[1], k.kid());
    // plain http to a remote Vault is refused outside dev mode
    let http = VaultConfig { addr: "http://vault.example".into(), ..v.clone() };
    assert!(Secrets::from_config(&kek(http.clone(), "transit/vlpds", &[]), false).is_err());
    assert!(Secrets::from_config(&kek(http, "transit/vlpds", &[]), true).is_ok());
    // a CA bundle that isn't PEM is refused at startup
    let bad = VaultConfig { ca_pem: Some(b"not a certificate".to_vec()), ..v };
    assert!(VaultClient::new(&bad, false, 4).is_err());
}

/// At startup, an answer from Vault that retrying won't fix stops the node
/// with the path and the likely cause: a policy without encrypt or decrypt,
/// a missing mount or key, wrong AppRole credentials. Mid-run the same
/// answers stay retryable, and a sealed or unreachable Vault never stops a
/// start.
#[tokio::test]
async fn startup_refuses_config_mistakes_but_not_outages() {
    let m = mock_vault().await;
    let auth = m.static_token();
    let start = |key: &str| {
        let s = Secrets::from_config(&kek(m.cfg(auth.clone()), key, &[]), false).unwrap();
        async move { (s.check_key_service().await, s) }
    };
    for (path, status, want) in [
        ("transit/encrypt/vlpds", 403, "lacks update on transit/encrypt/vlpds"),
        ("transit/decrypt/vlpds", 403, "lacks update on transit/decrypt/vlpds"),
        ("transit/encrypt/vlpds", 404, "nothing at transit/encrypt/vlpds"),
    ] {
        m.deny.lock().insert(path.into(), status);
        let e = start("transit/vlpds").await.0.unwrap_err().to_string();
        assert!(e.contains(want) && e.contains("unusable"), "{path} {status}: {e}");
        m.deny.lock().clear();
    }
    m.missing.lock().insert("transit/gone".into());
    let e = start("transit/gone").await.0.unwrap_err().to_string();
    assert!(e.contains("doesn't exist"), "{e}");
    // wrong AppRole credentials
    let sid = tmp_file("not-the-secret-id");
    let s = Secrets::from_config(&kek(m.cfg(m.approle(&sid)), "transit/vlpds", &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("credentials are wrong") && !e.contains("not-the-secret-id"), "{e}");
    std::fs::remove_file(sid).unwrap();
    // sealed: starts, and the first use is a retryable failure, then works once unsealed
    m.sealed.store(true, Ordering::SeqCst);
    let (r, s) = start("transit/vlpds").await;
    r.unwrap();
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Unavailable(_))));
    m.sealed.store(false, Ordering::SeqCst);
    tokio::time::sleep(crate::secrets::KMS_BACKOFF).await;
    s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    // mid-run, a policy losing decrypt is an outage (503), not a rejection
    let b = s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    m.deny.lock().insert("transit/decrypt/vlpds".into(), 403);
    assert!(matches!(s.unwrap(Purpose::Totp, "did:plc:a", &b).await, Err(SecretError::Unavailable(_))));
    // a check deferred at startup that then finds a 403 is retryable too
    m.deny.lock().clear();
    m.sealed.store(true, Ordering::SeqCst);
    let (r, s) = start("transit/vlpds").await;
    r.unwrap();
    m.sealed.store(false, Ordering::SeqCst);
    m.deny.lock().insert("transit/decrypt/vlpds".into(), 403);
    tokio::time::sleep(crate::secrets::KMS_BACKOFF).await;
    assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Unavailable(_))));
}
