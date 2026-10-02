//! Peer mTLS: node-to-node traffic (forwards, `/internal/*` calls, log
//! streams) over TLS 1.3 with client certificates, both ends verified
//! against a cluster CA (DESIGN.md "Exposure", ops/RUNBOOK.md "Peer TLS").
//! It is the only node-to-node transport: a node with peers has
//! `--peer-listen`, `--peer-tls-dir` and an `https://` `--advertise-url`; a
//! lone node has none of them, no peer listener and no `/internal/*`.
//!
//! Files. A peer TLS directory holds `ca.crt` (the cluster CA, PEM; several
//! = all trusted) and this node's `<node-id>.crt` / `<node-id>.key`: what
//! `vlpds admin tls ca --out DIR` and `vlpds admin tls issue --out DIR`
//! write ([`Files::in_dir`]). In `--dev-mode` a node fills its directory
//! itself ([`dev_files`]): a CA once (under a lock, so processes sharing the
//! directory share the CA) and its own certificate from `ca.key`.
//!
//! Identity. A node certificate carries
//! - a URI SAN `vlpds://node/<node-id>` (its `--node-id`; required: a cert
//!   without one is refused on both ends), and
//! - DNS / IP SANs for the host of its `--advertise-url` (the client checks
//!   them as any TLS client checks a server name),
//!
//! with both serverAuth and clientAuth extended key usages, issued by the
//! cluster CA (`vlpds admin tls ca` / `vlpds admin tls issue`).
//!
//! Checks.
//! - Server (the `--peer-listen` listener): a client certificate is
//!   required, must chain to the CA (clientAuth) and name a node. Any node
//!   of the cluster may call any peer: who a caller should be isn't known
//!   ahead (a joiner greets peers before they have read its lease), so the
//!   server doesn't match the client's node id against the registry. The
//!   internal token stays a second factor on `/internal/*` and forwards.
//! - Client: the server certificate must chain to the CA (serverAuth), be
//!   valid for the advertise URL's host, and name the node the caller
//!   expects there: the node(s) whose lease advertises that origin
//!   ([`Expect::Lookup`], the HTTP peer client) or the log's node (log
//!   streams, [`Expect::Node`]). An origin neither a lease nor the routing
//!   table names is refused; a client that isn't a node (tests, tools: no
//!   registry) accepts any node of the cluster.
//! - At startup the node's own certificate must chain to the CA, match its
//!   key, and name `--node-id`.
//!
//! Rotation. The CA, cert and key files are re-read on SIGHUP and when
//! their size or mtime changes (checked every [`RELOAD_POLL`]); a bad set is
//! logged and counted, and the node keeps the previous one. New connections
//! use the new material; pooled ones keep theirs until they close. The CA
//! file may hold several CA certificates (all trusted): rotate a CA by
//! trusting old + new first. `vlpds_peer_tls_cert_expiry_seconds{cert}` is
//! the notAfter (Unix seconds) of the node cert and of the earliest CA.

use anyhow::{bail, ensure, Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// URI SAN prefix of a node certificate: `vlpds://node/<node-id>`.
pub const NODE_URI_PREFIX: &str = "vlpds://node/";
/// How often the cert files are checked for changes.
pub const RELOAD_POLL: Duration = Duration::from_secs(60);
/// A peer's TLS handshake must finish within this (server side).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// Metrics live here (registered with the default registry, rendered by
// crate::metrics::render) and are exported only on a node with peer TLS.
static CERT_EXPIRY: LazyLock<prometheus::GaugeVec> = LazyLock::new(|| {
    prometheus::register_gauge_vec!(
        "vlpds_peer_tls_cert_expiry_seconds",
        "notAfter of the peer TLS certificates in use (Unix seconds): cert=node (this node's) and cert=ca (the earliest-expiring trusted CA)",
        &["cert"]
    )
    .unwrap()
});
static RELOADS: LazyLock<prometheus::IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "vlpds_peer_tls_reloads_total",
        "Peer TLS cert/key/CA reloads (SIGHUP or a file change) by result; error = the previous set stays in use",
        &["result"]
    )
    .unwrap()
});
static HANDSHAKE_FAILURES: LazyLock<prometheus::IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "vlpds_peer_tls_handshake_failures_total",
        "Peer TLS handshakes refused or failed: side=server (an inbound peer connection: no/foreign client cert, timeout), side=client (a peer's server cert refused: foreign CA, wrong host or node identity)",
        &["side"]
    )
    .unwrap()
});

fn init_metrics() {
    for r in ["ok", "error"] {
        RELOADS.with_label_values(&[r]);
    }
    for s in ["server", "client"] {
        HANDSHAKE_FAILURES.with_label_values(&[s]);
    }
}

/// Counts an inbound handshake that failed (serve_with).
pub fn server_handshake_failed() {
    HANDSHAKE_FAILURES.with_label_values(&["server"]).inc();
}

/// The crypto provider of every peer TLS config (ring; reqwest and lettre
/// already link it).
pub fn provider() -> Arc<CryptoProvider> {
    static P: LazyLock<Arc<CryptoProvider>> = LazyLock::new(|| Arc::new(rustls::crypto::ring::default_provider()));
    P.clone()
}

/// What a certificate says about itself.
#[derive(Debug, Clone)]
pub struct CertInfo {
    /// From the `vlpds://node/<id>` URI SAN.
    pub node_id: Option<String>,
    /// notAfter, Unix seconds.
    pub not_after: i64,
    pub is_ca: bool,
    /// DNS and IP SANs, as text.
    pub hosts: Vec<String>,
    pub subject: String,
}

/// Parses a DER certificate.
pub fn cert_info(der: &[u8]) -> Result<CertInfo> {
    let (_, c) = x509_parser::parse_x509_certificate(der).map_err(|e| anyhow::anyhow!("parsing certificate: {e}"))?;
    let mut node_id = None;
    let mut hosts = Vec::new();
    if let Ok(Some(san)) = c.subject_alternative_name() {
        for n in &san.value.general_names {
            match n {
                x509_parser::extensions::GeneralName::URI(u) => {
                    if let Some(id) = u.strip_prefix(NODE_URI_PREFIX) {
                        node_id = Some(id.to_string());
                    }
                }
                x509_parser::extensions::GeneralName::DNSName(d) => hosts.push(d.to_string()),
                x509_parser::extensions::GeneralName::IPAddress(b) => {
                    let ip = match b.len() {
                        4 => Some(std::net::IpAddr::from(<[u8; 4]>::try_from(*b).unwrap())),
                        16 => Some(std::net::IpAddr::from(<[u8; 16]>::try_from(*b).unwrap())),
                        _ => None,
                    };
                    hosts.extend(ip.map(|i| i.to_string()));
                }
                _ => {}
            }
        }
    }
    Ok(CertInfo { node_id, not_after: c.validity().not_after.timestamp(), is_ca: c.is_ca(), hosts, subject: c.subject().to_string() })
}

/// The node id a certificate names (`vlpds://node/<id>` URI SAN).
pub fn node_id_of(der: &[u8]) -> Option<String> {
    cert_info(der).ok()?.node_id
}

/// Node ids usable in a certificate identity: what a URI path segment
/// carries unescaped.
pub fn check_node_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b)),
        "node id {id:?} can't name a peer TLS identity: use 1-128 of [A-Za-z0-9-._~]"
    );
    Ok(())
}

/// One loaded CA + node cert + key.
struct Material {
    node_id: String,
    key: Arc<CertifiedKey>,
    client_verifier: Arc<dyn ClientCertVerifier>,
    server_verifier: Arc<WebPkiServerVerifier>,
    cert_not_after: i64,
    ca_not_after: i64,
}

impl Material {
    fn from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Result<Material> {
        let p = provider();
        let cas: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(ca_pem).collect::<Result<_, _>>().context("reading the CA file (PEM)")?;
        ensure!(!cas.is_empty(), "no certificate in the CA file");
        let mut roots = rustls::RootCertStore::empty();
        let mut ca_not_after = i64::MAX;
        for c in &cas {
            let info = cert_info(c)?;
            ensure!(info.is_ca, "the CA file holds a certificate that is not a CA ({})", info.subject);
            ca_not_after = ca_not_after.min(info.not_after);
            roots.add(c.clone()).context("adding a CA certificate")?;
        }
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(cert_pem).collect::<Result<_, _>>().context("reading the node certificate (PEM)")?;
        ensure!(!chain.is_empty(), "no certificate in the node certificate file");
        let key = PrivateKeyDer::from_pem_slice(key_pem).context("reading the node key (PEM)")?;
        let ck = CertifiedKey::from_der(chain.clone(), key, &p).context("the node key doesn't match its certificate")?;
        let leaf = cert_info(&chain[0])?;
        let Some(node_id) = leaf.node_id else {
            bail!("the node certificate ({}) has no {NODE_URI_PREFIX}<node-id> URI SAN (issue it with `vlpds admin tls issue`)", leaf.subject);
        };
        let roots = Arc::new(roots);
        let client_verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), p.clone())
            .build()
            .context("client verifier")?;
        let server_verifier = WebPkiServerVerifier::builder_with_provider(roots, p).build().context("server verifier")?;
        // our cert is both our server and our client cert: it must chain to
        // the CA now (expiry included), or no peer would take it
        client_verifier
            .verify_client_cert(&chain[0], &chain[1..], UnixTime::now())
            .map_err(|e| anyhow::anyhow!("the node certificate doesn't verify against the cluster CA: {e}"))?;
        Ok(Material { node_id, key: Arc::new(ck), client_verifier, server_verifier, cert_not_after: leaf.not_after, ca_not_after })
    }
}

/// The files a [`PeerTls`] reloads.
#[derive(Clone, Debug)]
pub struct Files {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

type Stamp = Vec<Option<(std::time::SystemTime, u64)>>;

impl Files {
    /// `node_id`'s files in a peer TLS directory (`--peer-tls-dir`):
    /// `ca.crt`, `<node-id>.crt`, `<node-id>.key`.
    pub fn in_dir(dir: &Path, node_id: &str) -> Files {
        Files { ca: dir.join("ca.crt"), cert: dir.join(format!("{node_id}.crt")), key: dir.join(format!("{node_id}.key")) }
    }

    fn read(&self) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let r = |p: &Path, what: &str| std::fs::read(p).with_context(|| format!("reading the peer TLS {what} {}", p.display()));
        Ok((r(&self.ca, "CA")?, r(&self.cert, "certificate")?, r(&self.key, "key")?))
    }

    fn stamp(&self) -> Stamp {
        [&self.ca, &self.cert, &self.key]
            .iter()
            .map(|p| std::fs::metadata(p).ok().map(|m| (m.modified().unwrap_or(std::time::UNIX_EPOCH), m.len())))
            .collect()
    }
}

/// This node's peer TLS material (module docs), shared by the peer
/// listener and the peer clients; reloadable.
pub struct PeerTls {
    files: Option<Files>,
    stamp: parking_lot::Mutex<Stamp>,
    cur: parking_lot::RwLock<Arc<Material>>,
}

impl std::fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTls").field("node_id", &self.node_id()).field("files", &self.files).finish()
    }
}

impl PeerTls {
    /// Loads `files` (refused: unreadable files, a cert that doesn't chain
    /// to the CA, is expired, lacks a node identity or doesn't match the key).
    pub fn load(files: Files) -> Result<Arc<PeerTls>> {
        let stamp = files.stamp();
        let (ca, cert, key) = files.read()?;
        let m = Material::from_pem(&ca, &cert, &key)?;
        Ok(Self::with(Some(files), stamp, m))
    }

    /// From PEM text, not reloadable (tests).
    pub fn from_pem(ca: &str, cert: &str, key: &str) -> Result<Arc<PeerTls>> {
        let m = Material::from_pem(ca.as_bytes(), cert.as_bytes(), key.as_bytes())?;
        Ok(Self::with(None, Vec::new(), m))
    }

    fn with(files: Option<Files>, stamp: Stamp, m: Material) -> Arc<PeerTls> {
        init_metrics();
        let t = Arc::new(PeerTls { files, stamp: parking_lot::Mutex::new(stamp), cur: parking_lot::RwLock::new(Arc::new(m)) });
        t.export_expiry();
        t
    }

    fn current(&self) -> Arc<Material> {
        self.cur.read().clone()
    }

    /// The node id this node's certificate names.
    pub fn node_id(&self) -> String {
        self.cur.read().node_id.clone()
    }

    /// (node cert, earliest CA) notAfter, Unix seconds.
    pub fn not_after(&self) -> (i64, i64) {
        let m = self.cur.read();
        (m.cert_not_after, m.ca_not_after)
    }

    fn export_expiry(&self) {
        let (cert, ca) = self.not_after();
        CERT_EXPIRY.with_label_values(&["node"]).set(cert as f64);
        CERT_EXPIRY.with_label_values(&["ca"]).set(ca as f64);
    }

    /// Re-reads the files (`force`: even if unchanged). Ok(true) = a new set
    /// is in use. A set that fails to load (or names another node) leaves
    /// the current one in place.
    pub fn reload(&self, force: bool) -> Result<bool> {
        let Some(files) = &self.files else { return Ok(false) };
        let stamp = files.stamp();
        if !force && *self.stamp.lock() == stamp {
            return Ok(false);
        }
        let r = files.read().and_then(|(ca, cert, key)| Material::from_pem(&ca, &cert, &key)).and_then(|m| {
            let me = self.node_id();
            ensure!(m.node_id == me, "the new certificate names node {:?}, this node is {me:?}", m.node_id);
            Ok(m)
        });
        // a half-written set is retried on the next poll or SIGHUP
        *self.stamp.lock() = stamp;
        match r {
            Ok(m) => {
                *self.cur.write() = Arc::new(m);
                self.export_expiry();
                RELOADS.with_label_values(&["ok"]).inc();
                let (cert, ca) = self.not_after();
                tracing::info!(cert_not_after = cert, ca_not_after = ca, "peer TLS: reloaded the CA, certificate and key");
                Ok(true)
            }
            Err(e) => {
                RELOADS.with_label_values(&["error"]).inc();
                Err(e)
            }
        }
    }

    /// Reloads on SIGHUP and when the files change ([`RELOAD_POLL`]).
    pub fn spawn_reloader(self: &Arc<Self>) {
        if self.files.is_none() {
            return;
        }
        let t = self.clone();
        tokio::spawn(async move {
            let mut hup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("peer TLS: no SIGHUP handler ({e}); reloading on file changes only");
                    None
                }
            };
            let mut tick = tokio::time::interval(RELOAD_POLL);
            tick.tick().await;
            loop {
                let force = tokio::select! {
                    _ = tick.tick() => false,
                    Some(()) = async { match &mut hup { Some(h) => h.recv().await, None => std::future::pending().await } } => true,
                };
                if let Err(e) = t.reload(force) {
                    tracing::error!("peer TLS reload failed (keeping the previous certificate): {e:#}");
                }
            }
        });
    }

    /// The peer listener's config: TLS 1.3, client certs required, h2 and
    /// http/1.1 by ALPN (h2 for requests; http/1.1 for log stream upgrades).
    pub fn server_config(self: &Arc<Self>) -> Arc<rustls::ServerConfig> {
        let mut c = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3 with the ring provider")
            .with_client_cert_verifier(Arc::new(ClientAuth(self.clone())))
            .with_cert_resolver(Arc::new(OwnCert(self.clone())));
        c.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Arc::new(c)
    }

    /// A peer client config presenting this node's cert and checking the
    /// server's as [`Expect`] says, offering `alpn`.
    pub fn client_config(self: &Arc<Self>, expect: Expect, alpn: &[&[u8]]) -> rustls::ClientConfig {
        let mut c = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .expect("TLS 1.3 with the ring provider")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(ServerAuth { tls: self.clone(), expect }))
            .with_client_cert_resolver(Arc::new(OwnCert(self.clone())));
        c.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
        c
    }
}

/// Which node a client expects at the other end.
#[derive(Clone)]
pub enum Expect {
    /// Any node of the cluster (a client that isn't a node: tests, tools).
    Any,
    /// This node.
    Node(String),
    /// One of the nodes this returns (asked at each handshake; none =
    /// refused), or any node while it returns None (no registry set).
    Lookup(Arc<dyn Fn() -> Option<Vec<String>> + Send + Sync>),
}

impl std::fmt::Debug for Expect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expect::Any => f.write_str("Any"),
            Expect::Node(n) => write!(f, "Node({n})"),
            Expect::Lookup(_) => f.write_str("Lookup"),
        }
    }
}

fn identity_refused() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
}

fn verify12(message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
    rustls::crypto::verify_tls12_signature(message, cert, dss, &provider().signature_verification_algorithms)
}

fn verify13(message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
    rustls::crypto::verify_tls13_signature(message, cert, dss, &provider().signature_verification_algorithms)
}

fn schemes() -> Vec<SignatureScheme> {
    provider().signature_verification_algorithms.supported_schemes()
}

/// Server side: the client cert must chain to the CA and name a node.
#[derive(Debug)]
struct ClientAuth(Arc<PeerTls>);

impl ClientCertVerifier for ClientAuth {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // no hints: a peer has one certificate and sends it
        &[]
    }

    fn verify_client_cert(&self, end_entity: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], now: UnixTime) -> Result<ClientCertVerified, rustls::Error> {
        self.0.current().client_verifier.verify_client_cert(end_entity, intermediates, now)?;
        if node_id_of(end_entity).is_none() {
            tracing::warn!("peer TLS: refused a client certificate without a node identity");
            return Err(identity_refused());
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify12(message, cert, dss)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        schemes()
    }
}

/// Client side: the server cert must chain to the CA, fit the host, and
/// name the expected node.
#[derive(Debug)]
struct ServerAuth {
    tls: Arc<PeerTls>,
    expect: Expect,
}

impl ServerCertVerifier for ServerAuth {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fail = |e: rustls::Error| {
            HANDSHAKE_FAILURES.with_label_values(&["client"]).inc();
            tracing::warn!(server = ?server_name, "peer TLS: refused the peer's certificate: {e}");
            e
        };
        self.tls.current().server_verifier.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now).map_err(fail)?;
        let want = match &self.expect {
            Expect::Any => None,
            Expect::Node(n) => Some(vec![n.clone()]),
            Expect::Lookup(f) => f(),
        };
        match node_id_of(end_entity) {
            None => Err(fail(identity_refused())),
            Some(got) if want.as_ref().is_some_and(|w| !w.contains(&got)) => {
                tracing::warn!(server = ?server_name, got, ?want, "peer TLS: the peer's certificate names another node");
                Err(fail(identity_refused()))
            }
            Some(_) => Ok(ServerCertVerified::assertion()),
        }
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify12(message, cert, dss)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        schemes()
    }
}

/// This node's cert, as server and as client.
#[derive(Debug)]
struct OwnCert(Arc<PeerTls>);

impl ResolvesServerCert for OwnCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.current().key.clone())
    }
}

impl ResolvesClientCert for OwnCert {
    fn resolve(&self, _hints: &[&[u8]], _schemes: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(self.0.current().key.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

// ---------- issuing (vlpds admin tls) ----------

/// A PEM certificate and its PEM PKCS#8 key.
pub struct Issued {
    pub cert_pem: String,
    pub key_pem: String,
}

fn validity(params: &mut rcgen::CertificateParams, days: u32) {
    let now = time::OffsetDateTime::now_utc();
    // an hour back: a peer whose clock is a little behind still takes it
    params.not_before = now - time::Duration::hours(1);
    params.not_after = now + time::Duration::days(days.max(1) as i64);
}

/// A new cluster CA (ECDSA P-256, self-signed, path length 0: it signs
/// node certs only).
pub fn create_ca(name: &str, days: u32) -> Result<Issued> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut p = rcgen::CertificateParams::default();
    p.distinguished_name.push(rcgen::DnType::CommonName, name);
    p.distinguished_name.push(rcgen::DnType::OrganizationName, "vlpds cluster");
    p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    p.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign, rcgen::KeyUsagePurpose::CrlSign, rcgen::KeyUsagePurpose::DigitalSignature];
    validity(&mut p, days);
    let cert = p.self_signed(&key)?;
    Ok(Issued { cert_pem: cert.pem(), key_pem: key.serialize_pem() })
}

/// A node certificate (ECDSA P-256) for `node_id`, valid for `hosts` (DNS
/// names or IP addresses: the host of its `--advertise-url`), signed by the
/// CA (PEM cert + key).
pub fn issue_node(ca_cert_pem: &str, ca_key_pem: &str, node_id: &str, hosts: &[String], days: u32) -> Result<Issued> {
    check_node_id(node_id)?;
    ensure!(!hosts.is_empty(), "give at least one --host: the DNS name or IP address of the node's --advertise-url");
    let ca_key = rcgen::KeyPair::from_pem(ca_key_pem).context("reading the CA key")?;
    let ca = rcgen::Issuer::from_ca_cert_pem(ca_cert_pem, ca_key).context("reading the CA certificate")?;
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut p = rcgen::CertificateParams::new(hosts.to_vec()).context("--host")?;
    p.subject_alt_names.push(rcgen::SanType::URI(format!("{NODE_URI_PREFIX}{node_id}").try_into()?));
    p.distinguished_name.push(rcgen::DnType::CommonName, node_id);
    p.distinguished_name.push(rcgen::DnType::OrganizationName, "vlpds cluster");
    p.is_ca = rcgen::IsCa::ExplicitNoCa;
    p.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
    p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth, rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    p.use_authority_key_identifier_extension = true;
    validity(&mut p, days);
    let cert = p.signed_by(&key, &ca)?;
    Ok(Issued { cert_pem: cert.pem(), key_pem: key.serialize_pem() })
}

/// Writes a cert (0644) and its key (0600, never overwritten unless
/// `force`) as `<dir>/<name>.crt` / `<dir>/<name>.key`. Returns the paths.
pub fn write_pair(dir: &Path, name: &str, issued: &Issued, force: bool) -> Result<(PathBuf, PathBuf)> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let (crt, key) = (dir.join(format!("{name}.crt")), dir.join(format!("{name}.key")));
    for p in [&crt, &key] {
        ensure!(force || !p.exists(), "{} exists (--force replaces it)", p.display());
    }
    let write = |p: &Path, mode: u32, data: &str| -> Result<()> {
        // a new file with the mode from the start (no window at 0644 for
        // the key); then the mode again for a replaced one
        let tmp = p.with_extension(format!("tmp{}", std::process::id()));
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(mode).open(&tmp).with_context(|| format!("writing {}", tmp.display()))?;
        f.write_all(data.as_bytes())?;
        f.sync_all()?;
        std::fs::set_permissions(&tmp, std::os::unix::fs::PermissionsExt::from_mode(mode))?;
        std::fs::rename(&tmp, p).with_context(|| format!("writing {}", p.display()))?;
        Ok(())
    };
    write(&key, 0o600, &issued.key_pem)?;
    write(&crt, 0o644, &issued.cert_pem)?;
    Ok((crt, key))
}

// ---------- dev mode ----------

/// Days a dev-mode certificate is issued for; one expiring within
/// [`DEV_RENEW_DAYS`] is re-issued at startup.
const DEV_DAYS: u32 = 365;
const DEV_RENEW_DAYS: i64 = 7;

/// `--dev-mode` with `--peer-tls-dir`: makes `dir` usable for `node_id` and
/// returns its files. Under an exclusive lock on `dir/.lock` (processes
/// sharing the directory, e.g. a local multi-node cluster, start at once):
/// - no `ca.crt`: creates a cluster CA (`ca.crt`, `ca.key`);
/// - `<node-id>.crt` missing, not from that CA, expiring within a week, or
///   not valid for one of `hosts`: issues it from `ca.key` (an error if
///   there is none).
///
/// Hosts across machines share the CA by copying `ca.crt` and `ca.key` into
/// each one's directory before their nodes start (bench/xhost). `hosts` gets
/// `127.0.0.1` and `localhost` added: local tools may call any node's peer
/// listener by loopback.
pub fn dev_files(dir: &Path, node_id: &str, hosts: &[String]) -> Result<Files> {
    check_node_id(node_id)?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let lock = std::fs::File::create(dir.join(".lock")).with_context(|| format!("creating {}/.lock", dir.display()))?;
    lock.lock().with_context(|| format!("locking {}/.lock", dir.display()))?;
    let files = Files::in_dir(dir, node_id);
    if !files.ca.exists() {
        write_pair(dir, "ca", &create_ca("vlpds dev cluster CA", 3650)?, true)?;
        tracing::info!(dir = %dir.display(), "dev mode: created a peer TLS cluster CA");
    }
    let mut hosts = hosts.to_vec();
    for h in ["127.0.0.1", "localhost"] {
        if !hosts.iter().any(|x| x == h) {
            hosts.push(h.to_string());
        }
    }
    let current = || -> Result<()> {
        let (ca, cert, key) = files.read()?;
        let m = Material::from_pem(&ca, &cert, &key)?;
        ensure!(m.node_id == node_id, "it names node {:?}", m.node_id);
        ensure!(m.cert_not_after > chrono::Utc::now().timestamp() + DEV_RENEW_DAYS * 86400, "it expires within {DEV_RENEW_DAYS} days");
        let have = cert_info(&CertificateDer::pem_slice_iter(&cert).next().context("no certificate")??)?.hosts;
        ensure!(hosts.iter().all(|h| have.contains(h)), "it isn't valid for all of {hosts:?}");
        Ok(())
    };
    if let Err(why) = current() {
        let read = |p: &Path| std::fs::read_to_string(p).with_context(|| format!("dev mode: issuing a peer TLS certificate needs {}", p.display()));
        let n = issue_node(&read(&files.ca)?, &read(&dir.join("ca.key"))?, node_id, &hosts, DEV_DAYS)?;
        write_pair(dir, node_id, &n, true)?;
        tracing::info!(dir = %dir.display(), node_id, ?hosts, why = %format!("{why:#}"), "dev mode: issued this node's peer TLS certificate");
    }
    Ok(files)
}

/// The host of a URL (DNS name or IP address, no brackets).
pub fn url_host(url: &str) -> Result<String> {
    let u = reqwest::Url::parse(url).with_context(|| format!("parsing {url:?}"))?;
    let h = u.host_str().with_context(|| format!("{url:?} has no host"))?;
    Ok(h.trim_start_matches('[').trim_end_matches(']').to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A CA and node certs for tests (crate::http, crate::forward use it too).
    pub(crate) struct TestCa {
        pub ca: Issued,
    }

    impl TestCa {
        pub fn new() -> TestCa {
            TestCa { ca: create_ca("test CA", 30).unwrap() }
        }
        pub fn node(&self, id: &str) -> Arc<PeerTls> {
            let n = issue_node(&self.ca.cert_pem, &self.ca.key_pem, id, &["127.0.0.1".into(), "localhost".into()], 30).unwrap();
            PeerTls::from_pem(&self.ca.cert_pem, &n.cert_pem, &n.key_pem).unwrap()
        }
    }

    #[test]
    fn issued_certs_load_and_name_their_node() {
        let ca = TestCa::new();
        let t = ca.node("node-a");
        assert_eq!(t.node_id(), "node-a");
        let (cert, ca_exp) = t.not_after();
        let now = chrono::Utc::now().timestamp();
        assert!(cert > now + 29 * 86400 && cert <= now + 30 * 86400 + 60, "{cert}");
        assert!(ca_exp > now);
        // a cert from another CA, a mismatched key, a CA as node cert
        let other = TestCa::new();
        let n = issue_node(&other.ca.cert_pem, &other.ca.key_pem, "x", &["127.0.0.1".into()], 30).unwrap();
        let e = PeerTls::from_pem(&ca.ca.cert_pem, &n.cert_pem, &n.key_pem).unwrap_err().to_string();
        assert!(e.contains("doesn't verify against the cluster CA"), "{e}");
        let m = issue_node(&ca.ca.cert_pem, &ca.ca.key_pem, "y", &["127.0.0.1".into()], 30).unwrap();
        assert!(PeerTls::from_pem(&ca.ca.cert_pem, &m.cert_pem, &n.key_pem).is_err());
        assert!(PeerTls::from_pem(&ca.ca.cert_pem, &ca.ca.cert_pem, &ca.ca.key_pem).is_err());
        // node ids that can't be an identity
        assert!(issue_node(&ca.ca.cert_pem, &ca.ca.key_pem, "a/b", &["h".into()], 1).is_err());
        assert!(issue_node(&ca.ca.cert_pem, &ca.ca.key_pem, "a", &[], 1).is_err());
    }

    #[test]
    fn written_keys_are_private_and_reload_on_change() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("vlpds-peer-tls-{}", rand::random::<u64>()));
        let ca = create_ca("c", 30).unwrap();
        write_pair(&dir, "ca", &ca, false).unwrap();
        let n = issue_node(&ca.cert_pem, &ca.key_pem, "n1", &["127.0.0.1".into()], 30).unwrap();
        let (crt, key) = write_pair(&dir, "n1", &n, false).unwrap();
        assert_eq!(std::fs::metadata(&key).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::metadata(&crt).unwrap().permissions().mode() & 0o777, 0o644);
        assert!(write_pair(&dir, "n1", &n, false).is_err(), "never overwrites without --force");
        let t = PeerTls::load(Files { ca: dir.join("ca.crt"), cert: crt.clone(), key: key.clone() }).unwrap();
        assert!(!t.reload(false).unwrap(), "unchanged");
        let before = t.not_after().0;
        // a renewed cert (shorter validity, so notAfter differs)
        let n2 = issue_node(&ca.cert_pem, &ca.key_pem, "n1", &["127.0.0.1".into()], 10).unwrap();
        write_pair(&dir, "n1", &n2, true).unwrap();
        assert!(t.reload(true).unwrap());
        assert!(t.not_after().0 < before);
        // a cert for another node is refused; the current one stays
        let other = issue_node(&ca.cert_pem, &ca.key_pem, "n2", &["127.0.0.1".into()], 30).unwrap();
        write_pair(&dir, "n1", &other, true).unwrap();
        assert!(t.reload(true).is_err());
        assert_eq!(t.node_id(), "n1");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn dev_dir_shares_one_ca_and_issues_node_certs() {
        let dir = std::env::temp_dir().join(format!("vlpds-peer-tls-dev-{}", rand::random::<u64>()));
        // concurrent first starts: one CA
        let hosts = vec!["10.0.0.5".to_string()];
        let files: Vec<Files> = std::thread::scope(|s| {
            let (dir, hosts) = (&dir, &hosts);
            let hs: Vec<_> = (0..4).map(|i| s.spawn(move || dev_files(dir, &format!("n{i}"), hosts).unwrap())).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let ca = std::fs::read(&files[0].ca).unwrap();
        for (i, f) in files.iter().enumerate() {
            let t = PeerTls::load(f.clone()).unwrap();
            assert_eq!(t.node_id(), format!("n{i}"));
            let info = cert_info(&CertificateDer::pem_slice_iter(&std::fs::read(&f.cert).unwrap()).next().unwrap().unwrap()).unwrap();
            for h in ["10.0.0.5", "127.0.0.1", "localhost"] {
                assert!(info.hosts.iter().any(|x| x == h), "{:?}", info.hosts);
            }
        }
        // a restart reuses the cert; a new advertise host re-issues it
        let before = std::fs::read(&files[0].cert).unwrap();
        dev_files(&dir, "n0", &hosts).unwrap();
        assert_eq!(std::fs::read(&files[0].cert).unwrap(), before);
        dev_files(&dir, "n0", &["node0.test".to_string()]).unwrap();
        assert_ne!(std::fs::read(&files[0].cert).unwrap(), before);
        assert_eq!(std::fs::read(&files[0].ca).unwrap(), ca, "the CA stays");
        // a replaced CA (copied in from another host) re-issues against it
        let other = create_ca("other", 30).unwrap();
        write_pair(&dir, "ca", &other, true).unwrap();
        let f = dev_files(&dir, "n1", &hosts).unwrap();
        PeerTls::load(f).unwrap();
        // without the CA key nothing can be issued
        std::fs::remove_file(dir.join("ca.key")).unwrap();
        let e = dev_files(&dir, "n9", &hosts).unwrap_err().to_string();
        assert!(e.contains("ca.key"), "{e}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
