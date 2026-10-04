//! The Vault Transit KEK (src/secrets/vault.rs) against a real Vault dev
//! server: `just vault-test` starts one in Docker and sets
//! VLPDS_TEST_VAULT_ADDR; without it these tests skip. Each test mounts its
//! own Transit engine and auth methods, and logs in with a token limited to
//! the policy the docs give operators (encrypt and decrypt on one key).

use crate::common::*;
use std::sync::Arc;
use std::time::Duration;
use vlpds::secrets::{KekBytes, KekConfig, Purpose, SecretError, Secrets, VaultAuth, VaultConfig};

struct Vault {
    addr: String,
    root: String,
    http: reqwest::Client,
}

fn vault() -> Option<Vault> {
    let Some(addr) = std::env::var("VLPDS_TEST_VAULT_ADDR").ok().filter(|a| !a.is_empty()) else {
        eprintln!("skipping: VLPDS_TEST_VAULT_ADDR unset (`just vault-test` runs these against a dev server)");
        return None;
    };
    let root = std::env::var("VLPDS_TEST_VAULT_TOKEN").unwrap_or_else(|_| "root".into());
    Some(Vault { addr, root, http: reqwest::Client::new() })
}

impl Vault {
    async fn call(&self, method: reqwest::Method, path: &str, body: serde_json::Value) -> serde_json::Value {
        let r = self
            .http
            .request(method, format!("{}/v1/{path}", self.addr))
            .header("X-Vault-Token", &self.root)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status();
        let text = r.text().await.unwrap();
        assert!(status.is_success(), "{path}: {status} {text}");
        if text.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&text).unwrap()
        }
    }

    async fn post(&self, path: &str, body: serde_json::Value) -> serde_json::Value {
        self.call(reqwest::Method::POST, path, body).await
    }

    /// A Transit mount with one key, and a policy allowing encrypt and
    /// decrypt on it only. Returns (mount, policy).
    async fn transit(&self, key_type: &str) -> (String, String) {
        let mount = unique_name("transit");
        self.post(&format!("sys/mounts/{mount}"), json!({"type": "transit"})).await;
        self.key(&mount, "vlpds", key_type).await;
        let policy = unique_name("vlpds");
        self.allow(&policy, &mount, "vlpds").await;
        (mount, policy)
    }

    async fn key(&self, mount: &str, key: &str, key_type: &str) {
        self.post(&format!("{mount}/keys/{key}"), json!({"type": key_type})).await;
    }

    /// The operator policy from docs/operations/kek-and-key-rotation.md.
    async fn allow(&self, policy: &str, mount: &str, key: &str) {
        let hcl = format!(
            "path \"{mount}/encrypt/{key}\" {{ capabilities = [\"update\"] }}\n\
             path \"{mount}/decrypt/{key}\" {{ capabilities = [\"update\"] }}\n"
        );
        self.post(&format!("sys/policies/acl/{policy}"), json!({"policy": hcl})).await;
    }

    /// A token with `policies`, in a file (what a Vault Agent sink leaves).
    async fn token_file(&self, policies: &[&str]) -> std::path::PathBuf {
        let r = self.post("auth/token/create", json!({"policies": policies, "ttl": "1h"})).await;
        tmp_file(r["auth"]["client_token"].as_str().unwrap())
    }

    fn cfg(&self, auth: VaultAuth) -> VaultConfig {
        VaultConfig { addr: self.addr.clone(), namespace: None, ca_pem: None, auth }
    }
}

fn tmp_file(contents: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(unique_name("vlpds-vault"));
    std::fs::write(&p, contents).unwrap();
    p
}

fn kek(v: VaultConfig, key: &str, old: &[&str]) -> KekConfig {
    KekConfig {
        vault: Some(v),
        vault_key: Some(key.into()),
        vault_old_keys: old.iter().map(|k| k.to_string()).collect(),
        ..Default::default()
    }
}

fn ct_of(blob: &str) -> String {
    use base64::Engine;
    let b = blob.splitn(3, '.').nth(2).unwrap();
    String::from_utf8(base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(b).unwrap()).unwrap()
}

/// Wrap and unwrap through the documented least-privilege policy; the
/// stored blob is Transit's `vault:v1:` string; a wrong purpose or subject
/// is refused by Vault (AAD); the startup self-test passes on an AEAD key
/// and refuses keys that can't take associated_data.
#[tokio::test]
async fn vault_roundtrip_wrong_aad_and_self_test() {
    let Some(v) = vault() else { return };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let tf = v.token_file(&[&policy]).await;
    let s = Secrets::from_config(&kek(v.cfg(VaultAuth::TokenFile(tf.clone())), &format!("{mount}/vlpds"), &[]), false)
        .unwrap();
    s.check_key_service().await.unwrap();
    let secret = [3u8; 32];
    let blob = s.wrap(Purpose::SigningKey, "did:plc:a", &secret).await.unwrap();
    assert!(blob.starts_with(&format!("vw1.{}.", s.current_kid())), "{blob}");
    assert!(ct_of(&blob).starts_with("vault:v1:"));
    let u = s.unwrap(Purpose::SigningKey, "did:plc:a", &blob).await.unwrap();
    assert_eq!(&u.plaintext[..], &secret);
    assert!(!u.stale);
    for (p, subj) in [(Purpose::SigningKey, "did:plc:b"), (Purpose::Totp, "did:plc:a")] {
        match s.unwrap(p, subj, &blob).await {
            Err(SecretError::Rejected(e)) => assert!(e.contains("message authentication failed"), "{e}"),
            r => panic!("{:?}", r.map(|_| ())),
        }
    }
    // chacha20-poly1305 and aes128-gcm96 work too
    for ty in ["chacha20-poly1305", "aes128-gcm96"] {
        let (m, p) = v.transit(ty).await;
        let tf = v.token_file(&[&p]).await;
        let s = Secrets::from_config(&kek(v.cfg(VaultAuth::TokenFile(tf)), &format!("{m}/vlpds"), &[]), false).unwrap();
        s.check_key_service().await.unwrap();
        let b = s.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
        assert!(s.unwrap(Purpose::Totp, "did:plc:b", &b).await.is_err(), "{ty}");
    }
    // keys that can't bind AAD are refused before any wrap: Vault refuses
    // associated_data for RSA, and a derived key wants a context
    for (ty, derived) in [("rsa-2048", false), ("aes256-gcm96", true)] {
        let m = unique_name("transit");
        v.post(&format!("sys/mounts/{m}"), json!({"type": "transit"})).await;
        v.post(&format!("{m}/keys/vlpds"), json!({"type": ty, "derived": derived})).await;
        let p = unique_name("vlpds");
        v.allow(&p, &m, "vlpds").await;
        let tf = v.token_file(&[&p]).await;
        let s = Secrets::from_config(&kek(v.cfg(VaultAuth::TokenFile(tf)), &format!("{m}/vlpds"), &[]), false).unwrap();
        let e = s.check_key_service().await.unwrap_err().to_string();
        assert!(e.contains("unusable"), "{ty} derived={derived}: {e}");
        assert!(matches!(s.wrap(Purpose::Totp, "did:plc:a", b"x").await, Err(SecretError::Rejected(_))));
    }
    // with read on the key, its type is named
    let m = unique_name("transit");
    v.post(&format!("sys/mounts/{m}"), json!({"type": "transit"})).await;
    v.key(&m, "vlpds", "rsa-2048").await;
    let tf = v.token_file(&["root"]).await;
    let s =
        Secrets::from_config(&kek(v.cfg(VaultAuth::TokenFile(tf.clone())), &format!("{m}/vlpds"), &[]), false).unwrap();
    let e = s.check_key_service().await.unwrap_err().to_string();
    assert!(e.contains("rsa-2048") && e.contains("AEAD"), "{e}");
    std::fs::remove_file(tf).ok();
}

/// A Vault on a private CA (`server -dev-tls`): refused without
/// --vault-ca-file (a retryable failure, the check deferred), served with
/// it. `just vault-test` sets VLPDS_TEST_VAULT_TLS_ADDR and _CA.
#[tokio::test]
async fn vault_private_ca() {
    let (Ok(addr), Ok(ca)) = (std::env::var("VLPDS_TEST_VAULT_TLS_ADDR"), std::env::var("VLPDS_TEST_VAULT_TLS_CA"))
    else {
        eprintln!("skipping: VLPDS_TEST_VAULT_TLS_ADDR / _CA unset");
        return;
    };
    let v = Vault {
        addr: addr.clone(),
        root: "root".into(),
        http: reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()).unwrap())
            .build()
            .unwrap(),
    };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let tf = v.token_file(&[&policy]).await;
    let mut cfg = v.cfg(VaultAuth::TokenFile(tf.clone()));
    let without = Secrets::from_config(&kek(cfg.clone(), &format!("{mount}/vlpds"), &[]), false).unwrap();
    without.check_key_service().await.unwrap();
    match without.wrap(Purpose::Totp, "did:plc:a", b"x").await {
        Err(SecretError::Unavailable(e)) => assert!(e.to_lowercase().contains("certificate"), "{e}"),
        r => panic!("{:?}", r.map(|_| ())),
    }
    cfg.ca_pem = Some(std::fs::read(&ca).unwrap());
    let with = Secrets::from_config(&kek(cfg, &format!("{mount}/vlpds"), &[]), false).unwrap();
    with.check_key_service().await.unwrap();
    let b = with.wrap(Purpose::Totp, "did:plc:a", b"x").await.unwrap();
    assert_eq!(&with.unwrap(Purpose::Totp, "did:plc:a", &b).await.unwrap().plaintext[..], b"x");
    std::fs::remove_file(tf).ok();
}

/// Transit `rotate`: new wraps use v2, old blobs still unwrap and show as
/// stale, rewrap moves them; then min_decryption_version retires v1.
#[tokio::test]
async fn vault_rotate_then_old_ciphertexts() {
    let Some(v) = vault() else { return };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let tf = v.token_file(&[&policy]).await;
    let s = Secrets::from_config(&kek(v.cfg(VaultAuth::TokenFile(tf.clone())), &format!("{mount}/vlpds"), &[]), false)
        .unwrap();
    let old: Vec<String> = futures::future::join_all((0..3).map(|i| {
        let s = &s;
        async move { s.wrap(Purpose::Totp, &format!("did:plc:{i}"), b"JBSWY3DP").await.unwrap() }
    }))
    .await;
    v.post(&format!("{mount}/keys/vlpds/rotate"), json!({})).await;
    let kid = s.current_kid().to_string();
    let new = s.wrap(Purpose::Totp, "did:plc:n", b"x").await.unwrap();
    assert!(ct_of(&new).starts_with("vault:v2:"), "new wraps use the new version");
    assert_eq!(s.current_kid(), kid);
    for (i, b) in old.iter().enumerate() {
        let u = s.unwrap(Purpose::Totp, &format!("did:plc:{i}"), b).await.unwrap();
        assert_eq!(&u.plaintext[..], b"JBSWY3DP");
        assert!(u.stale, "v1 after rotate is stale");
    }
    let moved = s.rewrap(Purpose::Totp, "did:plc:0", &old[0]).await.unwrap().expect("stale");
    assert!(ct_of(&moved).starts_with("vault:v2:"));
    assert_eq!(s.rewrap(Purpose::Totp, "did:plc:0", &moved).await.unwrap(), None);
    // the operator retires v1: blobs not yet rewrapped are refused, not served
    v.post(&format!("{mount}/keys/vlpds/config"), json!({"min_decryption_version": 2})).await;
    match s.unwrap(Purpose::Totp, "did:plc:1", &old[1]).await {
        Err(SecretError::Rejected(e)) => assert!(e.contains("too old"), "{e}"),
        r => panic!("{:?}", r.map(|_| ())),
    }
    s.unwrap(Purpose::Totp, "did:plc:0", &moved).await.unwrap();
    std::fs::remove_file(tf).ok();
}

/// Moving between keys with --vault-transit-old-key: key B (another mount)
/// current, key A unwrap-only; then Vault -> Cloud KMS (mocked) with the
/// Vault key as the old one.
#[tokio::test]
async fn vault_old_key_flag_across_keys_and_to_gcp() {
    let Some(v) = vault() else { return };
    let (ma, pa) = v.transit("aes256-gcm96").await;
    let (mb, pb) = v.transit("aes256-gcm96").await;
    let (ka, kb) = (format!("{ma}/vlpds"), format!("{mb}/vlpds"));
    let tf = v.token_file(&[&pa, &pb]).await;
    let auth = VaultAuth::TokenFile(tf.clone());
    let a = Secrets::from_config(&kek(v.cfg(auth.clone()), &ka, &[]), false).unwrap();
    let blob = a.wrap(Purpose::SigningKey, "did:plc:x", &[5u8; 32]).await.unwrap();
    let both = Secrets::from_config(&kek(v.cfg(auth.clone()), &kb, &[&ka]), false).unwrap();
    both.check_key_service().await.unwrap();
    assert!(both.unwrap(Purpose::SigningKey, "did:plc:x", &blob).await.unwrap().stale);
    let on_b = both.rewrap(Purpose::SigningKey, "did:plc:x", &blob).await.unwrap().unwrap();
    assert!(both.is_current(&on_b));
    let b_only = Secrets::from_config(&kek(v.cfg(auth.clone()), &kb, &[]), false).unwrap();
    assert_eq!(&b_only.unwrap(Purpose::SigningKey, "did:plc:x", &on_b).await.unwrap().plaintext[..], &[5u8; 32]);
    assert!(matches!(b_only.unwrap(Purpose::SigningKey, "did:plc:x", &blob).await, Err(SecretError::UnknownKek(_))));
    // A's ciphertext relabelled as B's: Vault refuses it under B
    let forged = blob.replacen(a.current_kid(), b_only.current_kid(), 1);
    assert!(matches!(b_only.unwrap(Purpose::SigningKey, "did:plc:x", &forged).await, Err(SecretError::Rejected(_))));

    // Vault -> Cloud KMS
    let kms = crate::secrets_at_rest::mock_kms().await;
    let to_gcp =
        KekConfig { vault: Some(v.cfg(auth)), vault_old_keys: vec![kb.clone()], ..crate::secrets_at_rest::gcp(&kms) };
    let g = Secrets::from_config(&to_gcp, false).unwrap();
    assert!(g.current_kid().starts_with('G'));
    let on_g = g.rewrap(Purpose::SigningKey, "did:plc:x", &on_b).await.unwrap().expect("Vault blob is stale");
    let g_only = Secrets::from_config(&crate::secrets_at_rest::gcp(&kms), false).unwrap();
    assert_eq!(&g_only.unwrap(Purpose::SigningKey, "did:plc:x", &on_g).await.unwrap().plaintext[..], &[5u8; 32]);
    std::fs::remove_file(tf).ok();
}

/// `vlpds.admin.rewrapSecrets` from a local KEK to Vault and back: every
/// signing key, TOTP secret and reserved key moves, nodes with only the
/// new KEK keep signing, and the keys themselves don't change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vault_rewrap_local_to_vault_and_back() {
    let Some(v) = vault() else { return };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let tf = v.token_file(&[&policy]).await;
    let vkey = format!("{mount}/vlpds");
    let vcfg = v.cfg(VaultAuth::TokenFile(tf.clone()));
    let local = KekBytes::random();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let node = |kek: KekConfig| {
        let store = store.clone();
        async move { cluster_node("vault", store, 4, |c| c.kek = kek).await }
    };
    let rewrap = |s: &TestServer, dry: bool| {
        let x = s.xrpc.clone();
        async move { x.post("vlpds.admin.rewrapSecrets", &json!({"dryRun": dry}), &Auth::Admin).await.ok() }
    };

    let a = node(KekConfig { local: Some(local.clone()), ..Default::default() }).await;
    let mut accts = Vec::new();
    for _ in 0..3 {
        let t = a.create_account("vrw").await;
        a.post(&t, "local").await;
        accts.push(t);
    }
    let (totp, _) = a.enable_totp(&accts[0]).await;
    a.xrpc.post("com.atproto.server.reserveSigningKey", &json!({}), &Auth::None).await.ok();
    let pubkeys: Vec<String> = futures::future::join_all(
        accts.iter().map(|t| async { a.app.account(&t.did).await.ok().unwrap().signing_pubkey }),
    )
    .await;
    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;

    // local -> Vault
    let b = node(KekConfig { local_old: vec![local.clone()], ..kek(vcfg.clone(), &vkey, &[]) }).await;
    assert!(b.app.secrets.current_kid().starts_with('V'));
    let dry = rewrap(&b, true).await;
    assert_eq!((dry["stale"].clone(), dry["failed"].clone()), (json!(5), json!(0)), "{dry}");
    let done = rewrap(&b, false).await;
    assert_eq!((done["stale"].clone(), done["failed"].clone()), (json!(5), json!(0)), "{done}");
    assert_eq!(rewrap(&b, true).await["stale"], json!(0));
    b.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&b.app).await;

    let c = node(kek(vcfg.clone(), &vkey, &[])).await;
    for (t, pk) in accts.iter().zip(&pubkeys) {
        let row = c.app.account(&t.did).await.ok().unwrap();
        assert!(row.wrapped_signing_key.starts_with(&format!("vw1.{}.", c.app.secrets.current_kid())));
        assert_eq!(&row.signing_pubkey, pk, "rewrapped, not rotated");
        c.post(t, "vault only").await;
        c.get_repo(&t.did).await.commit().verify(&c.signing_key(&t.did).await).unwrap();
    }
    crate::secrets_at_rest::totp_login(&c, &accts[0], &totp, 1).await.ok();
    c.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&c.app).await;

    // Vault -> local
    let d = node(KekConfig {
        local: Some(local.clone()),
        vault: Some(vcfg.clone()),
        vault_old_keys: vec![vkey.clone()],
        ..Default::default()
    })
    .await;
    let done = rewrap(&d, false).await;
    assert_eq!((done["stale"].clone(), done["failed"].clone()), (json!(5), json!(0)), "{done}");
    d.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&d.app).await;
    let e = node(KekConfig { local: Some(local), ..Default::default() }).await;
    for t in &accts {
        e.post(t, "local again").await;
    }
    vlpds::server::shutdown(&e.app).await;
    std::fs::remove_file(tf).ok();
}

/// AppRole with a 2 s token TTL and a 5 s max TTL: wraps and unwraps keep
/// succeeding for 9 s across renewals and the max_ttl re-login; a revoked
/// token mid-run costs one login.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vault_approle_short_ttl() {
    let Some(v) = vault() else { return };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let ar = unique_name("approle");
    v.post(&format!("sys/auth/{ar}"), json!({"type": "approle"})).await;
    v.post(
        &format!("auth/{ar}/role/vlpds"),
        json!({"token_policies": [policy], "token_ttl": "2s", "token_max_ttl": "5s", "secret_id_num_uses": 0}),
    )
    .await;
    let role_id = v.call(reqwest::Method::GET, &format!("auth/{ar}/role/vlpds/role-id"), json!(null)).await["data"]
        ["role_id"]
        .as_str()
        .unwrap()
        .to_string();
    let sid = v.post(&format!("auth/{ar}/role/vlpds/secret-id"), json!({})).await["data"]["secret_id"]
        .as_str()
        .unwrap()
        .to_string();
    let sid_file = tmp_file(&format!("{sid}\n"));
    let auth = VaultAuth::AppRole { mount: ar.clone(), role_id, secret_id_file: sid_file.clone() };
    let cfg = kek(v.cfg(auth), &format!("{mount}/vlpds"), &[]);
    let s = Secrets::from_config(&cfg, false).unwrap();
    s.check_key_service().await.unwrap();
    let client = vlpds::secrets::VaultClient::new(cfg.vault.as_ref().unwrap()).unwrap();
    let t = vlpds::secrets::VaultTransit::new(client.clone(), &format!("{mount}/vlpds"), true).unwrap();
    use vlpds::secrets::KeyWrapper;
    let start = std::time::Instant::now();
    let mut n = 0;
    while start.elapsed() < Duration::from_secs(9) {
        let subj = format!("did:plc:{n}");
        let b = s.wrap(Purpose::Totp, &subj, b"x").await.unwrap_or_else(|e| panic!("at {:?}: {e}", start.elapsed()));
        s.unwrap(Purpose::Totp, &subj, &b).await.unwrap_or_else(|e| panic!("at {:?}: {e}", start.elapsed()));
        let c = t.wrap(b"aad", b"y").await.unwrap_or_else(|e| panic!("at {:?}: {e}", start.elapsed()));
        t.unwrap(b"aad", &c).await.unwrap();
        n += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(client.renewals() >= 1, "renewed before expiry: {}", client.renewals());
    assert!(client.logins() >= 2, "logged in again at max_ttl: {}", client.logins());
    assert!(client.logins() <= 5, "not a login per call: {} over {n} rounds", client.logins());
    // the role's tokens are revoked (tidy, an incident): one login, then on
    let before = client.logins();
    v.post(&format!("sys/leases/revoke-prefix/auth/{ar}"), json!({})).await;
    t.wrap(b"aad", b"y").await.unwrap();
    assert_eq!(client.logins(), before + 1);
    std::fs::remove_file(sid_file).ok();
}

/// Kubernetes auth with the TokenReview API mocked in this process: Vault
/// (in Docker) calls back to it. A non-renewable token is replaced by a
/// fresh login that reads the rotated service-account token from the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vault_kubernetes_login() {
    let Some(v) = vault() else { return };
    let (mount, policy) = v.transit("aes256-gcm96").await;
    let reviews = mock_token_review().await;
    let host = std::env::var("VLPDS_TEST_VAULT_HOST_FROM_CONTAINER").unwrap_or_else(|_| "host.docker.internal".into());
    let k8s = unique_name("k8s");
    v.post(&format!("sys/auth/{k8s}"), json!({"type": "kubernetes"})).await;
    v.post(
        &format!("auth/{k8s}/config"),
        // OpenBao wants a CA with disable_local_ca_jwt even for an http host
        json!({"kubernetes_host": format!("http://{host}:{}", reviews.port), "disable_local_ca_jwt": true,
               "kubernetes_ca_cert": unused_ca_pem()}),
    )
    .await;
    v.post(
        &format!("auth/{k8s}/role/vlpds"),
        json!({"bound_service_account_names": ["vlpds"], "bound_service_account_namespaces": ["pds"],
               "token_policies": [policy], "token_ttl": "3s", "token_type": "batch"}),
    )
    .await;
    let jwt_file = tmp_file(&sa_jwt(1));
    let auth = VaultAuth::Kubernetes { mount: k8s.clone(), role: "vlpds".into(), jwt_file: jwt_file.clone() };
    let cfg = kek(v.cfg(auth), &format!("{mount}/vlpds"), &[]);
    let client = vlpds::secrets::VaultClient::new(cfg.vault.as_ref().unwrap()).unwrap();
    let t = vlpds::secrets::VaultTransit::new(client.clone(), &format!("{mount}/vlpds"), true).unwrap();
    use vlpds::secrets::KeyWrapper;
    let c = t.wrap(b"aad", b"k8s").await.unwrap();
    assert_eq!(&t.unwrap(b"aad", &c).await.unwrap().plaintext[..], b"k8s");
    assert_eq!(client.logins(), 1);
    assert_eq!(reviews.last_token(), sa_jwt(1));
    std::fs::write(&jwt_file, sa_jwt(2)).unwrap();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    t.wrap(b"aad", b"k8s").await.unwrap();
    assert!(client.logins() >= 2, "{}", client.logins());
    assert_eq!(reviews.last_token(), sa_jwt(2), "the rotated service-account token was read");
    std::fs::remove_file(jwt_file).ok();
}

fn unused_ca_pem() -> String {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    rcgen::CertificateParams::new(vec!["k8s.test".into()]).unwrap().self_signed(&key).unwrap().pem()
}

/// An unsigned service-account JWT: Vault reads its claims and trusts the
/// TokenReview answer.
fn sa_jwt(n: u32) -> String {
    use base64::Engine;
    let b = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let claims = json!({
        "iss": "kubernetes/serviceaccount",
        "sub": "system:serviceaccount:pds:vlpds",
        "kubernetes.io/serviceaccount/namespace": "pds",
        "kubernetes.io/serviceaccount/service-account.name": "vlpds",
        "kubernetes.io/serviceaccount/service-account.uid": "uid-1",
        "kubernetes.io/serviceaccount/secret.name": format!("vlpds-token-{n}"),
    });
    format!(
        "{}.{}.{}",
        b.encode(r#"{"alg":"RS256","typ":"JWT"}"#),
        b.encode(claims.to_string()),
        b.encode(format!("sig{n}"))
    )
}

struct TokenReview {
    port: u16,
    last: parking_lot::Mutex<String>,
}

impl TokenReview {
    fn last_token(&self) -> String {
        self.last.lock().clone()
    }
}

async fn mock_token_review() -> Arc<TokenReview> {
    use axum::extract::State;
    // Docker Desktop forwards host.docker.internal to the host's loopback;
    // a Linux Docker host needs the listener on the bridge
    let bind = std::env::var("VLPDS_TEST_VAULT_CALLBACK_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let listener = tokio::net::TcpListener::bind(format!("{bind}:0")).await.unwrap();
    let tr = Arc::new(TokenReview {
        port: listener.local_addr().unwrap().port(),
        last: parking_lot::Mutex::new(String::new()),
    });
    async fn review(
        State(tr): State<Arc<TokenReview>>,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        *tr.last.lock() = body["spec"]["token"].as_str().unwrap_or("").to_string();
        axum::Json(json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {"authenticated": true, "user": {"username": "system:serviceaccount:pds:vlpds", "uid": "uid-1"}},
        }))
    }
    let app = axum::Router::new()
        .route("/apis/authentication.k8s.io/v1/tokenreviews", axum::routing::post(review))
        .with_state(tr.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    tr
}
