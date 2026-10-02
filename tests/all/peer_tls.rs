//! Peer mTLS (src/peer_tls.rs; DESIGN.md "Exposure"): an in-process cluster
//! whose nodes talk h2 over TLS 1.3 with client certificates on their peer
//! listeners, while clients use a separate public listener.
//!
//! - forwards, internal private put/get, the OAuth replay claim and log
//!   streams (the merged firehose) all work over mTLS;
//! - the public listener 404s `/internal/*` and serves a request carrying a
//!   forwarded marker as a client request (routed to the owner);
//! - the peer listener refuses a client without a certificate, or with one
//!   from another CA, at the handshake; a client refuses a server whose
//!   certificate names another node than the lease at that address.

use crate::common::*;
use crate::ha_auth::{balanced, owner_of};
use std::sync::Arc;
use std::time::Duration;
use vlpds::peer_tls::{self, PeerTls};

const SHARDS: u32 = 8;
const PUBLIC: &str = "http://pds.mtls.test";

struct Ca(peer_tls::Issued);

impl Ca {
    fn new() -> Ca {
        Ca(peer_tls::create_ca("test cluster CA", 30).unwrap())
    }

    fn node(&self, id: &str) -> Arc<PeerTls> {
        let n = peer_tls::issue_node(&self.0.cert_pem, &self.0.key_pem, id, &["127.0.0.1".into()], 30).unwrap();
        PeerTls::from_pem(&self.0.cert_pem, &n.cert_pem, &n.key_pem).unwrap()
    }
}

/// A node with a public listener (`TestServer::url`) and an mTLS peer
/// listener (returned), advertised as `https://`.
async fn tls_node(id: &str, store: &Arc<object_store::memory::InMemory>, ca: &Ca) -> (TestServer, String) {
    split_node(id, store, Some(ca)).await
}

/// [`tls_node`], or with `ca` None a cleartext (h2c) peer listener.
async fn split_node(id: &str, store: &Arc<object_store::memory::InMemory>, ca: Option<&Ca>) -> (TestServer, String) {
    init_tracing();
    let public = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", public.local_addr().unwrap());
    let scheme = if ca.is_some() { "https" } else { "http" };
    let peer_url = format!("{scheme}://{}", peer.local_addr().unwrap());
    let cfg = vlpds::server::Config {
        dev_mode: true,
        // one issuer for every node (OAuth)
        public_url: PUBLIC.into(),
        rate_limits_enabled: false,
        memory_store: Some(store.clone()),
        shards: SHARDS,
        peer_tls: ca.map(|ca| ca.node(id)),
        cluster: Some(vlpds::cluster::ClusterConfig {
            node_id: id.into(),
            addr: peer_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        }),
        ..Default::default()
    };
    let (app, addr, _) = vlpds::server::spawn_split(cfg, public, peer).await.expect("spawn");
    (TestServer { app, addr, url: url.clone(), xrpc: Xrpc::new(&url) }, peer_url)
}

fn forwards() -> u64 {
    vlpds::metrics::FORWARDED.get()
}

async fn cluster_status(c: &reqwest::Client, base: &str) -> reqwest::Result<reqwest::Response> {
    c.get(format!("{base}/internal/v1/cluster")).header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN).send().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_cluster_end_to_end() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let ca = Ca::new();
    let (a, a_peer) = tls_node("tls-a", &store, &ca).await;
    let (b, _) = tls_node("tls-b", &store, &ca).await;
    let (c, _) = tls_node("tls-c", &store, &ca).await;
    let nodes = [&a, &b, &c];
    balanced(&nodes).await;
    assert!(a.app.http.is_tls());
    for n in nodes {
        for p in n.app.cluster.as_ref().unwrap().peers() {
            assert!(p.addr.starts_with("https://"), "{}", p.addr);
        }
    }

    // accounts until each node owns one
    let mut owned: Vec<Option<TestAccount>> = vec![None, None, None];
    for k in 0..90 {
        let acct = nodes[k % 3].create_account("mtls").await;
        let o = owner_of(&nodes, &acct.did);
        let i = nodes.iter().position(|n| std::ptr::eq(*n, o)).unwrap();
        owned[i].get_or_insert(acct);
        if owned.iter().all(Option::is_some) {
            break;
        }
    }
    let owned: Vec<TestAccount> = owned.into_iter().enumerate().map(|(i, o)| o.unwrap_or_else(|| panic!("no account on node {i}: owned {:?}", nodes.iter().map(|n| n.app.partitions.owned().len()).collect::<Vec<_>>()))).collect();

    // a firehose on a: b's and c's commits reach it through their log streams
    let mut sub = a.subscribe_from_now().await;
    let before = forwards();
    for (i, acct) in owned.iter().enumerate() {
        // through a node that doesn't own the account: forwarded over mTLS
        let via = nodes[(i + 1) % 3];
        let r = via.post(acct, &format!("over mtls {i}")).await;
        let got = nodes[(i + 2) % 3].get_record(acct.did.as_str(), r.collection(), r.rkey()).await;
        assert_eq!(got.status, 200, "{}", got.text());
        sub.wait_for(FH_TIMEOUT, &acct.did, "#commit").await;
    }
    assert!(forwards() >= before + 6, "writes and reads were forwarded");

    // internal private put/get, from c, for an account a owns
    let did = &owned[0].did;
    let m = vlpds::segment::Mutation { key: vlpds::state::private_key(did, "mtls").into(), val: Some(bytes::Bytes::from_static(b"v")) };
    c.app.put_private(did, vec![m]).await.unwrap_or_else(|e| panic!("private put: {}", e.message));
    assert_eq!(c.app.get_private(did, "mtls").await.unwrap_or_else(|e| panic!("{}", e.message)).as_deref(), Some(&b"v"[..]));

    // the OAuth replay claim at the routing key's owner (a), from c
    let until = chrono::Utc::now().timestamp() + 60;
    let key = format!("mtls-jti-{}", rand::random::<u64>());
    assert!(vlpds::xrpc::internal::claim_replay_anywhere(&c.app, did, &key, until).await.unwrap_or_else(|e| panic!("{}", e.message)), "first claim");
    assert!(!vlpds::xrpc::internal::claim_replay_anywhere(&b.app, did, &key, until).await.unwrap_or_else(|e| panic!("{}", e.message)), "replay refused");

    // the public listener: no /internal/*, even with the token
    let plain = reqwest::Client::new();
    assert_eq!(cluster_status(&plain, &a.url).await.unwrap().status(), 404);
    assert_eq!(plain.get(format!("{}/internal/v1/ratelimits", a.url)).send().await.unwrap().status(), 404);
    // ... and a client's forwarded marker is a client request: a write for
    // b's account sent to a with the marker is routed to b, not served here
    let acct = &owned[1];
    let r = a
        .xrpc
        .send(
            a.xrpc
                .http
                .post(format!("{}/xrpc/com.atproto.repo.createRecord", a.url))
                .header("authorization", format!("Bearer {}", acct.access))
                .header("x-vlpds-forwarded", vlpds::server::DEV_INTERNAL_TOKEN)
                .json(&json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record("marker")})),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    // the peer listener: TLS only
    assert!(cluster_status(&plain, &a_peer.replace("https://", "http://")).await.is_err());
    // a peer client of this cluster gets in (and the token still applies)
    let peer = vlpds::http::PeerClient::with_tls(1, ca.node("tls-probe")).unwrap();
    let r = peer.get(format!("{a_peer}/internal/v1/cluster")).header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.version(), reqwest::Version::HTTP_2);
    let r = peer.get(format!("{a_peer}/internal/v1/cluster")).header("x-vlpds-internal", "nope").send().await.unwrap();
    assert_eq!(r.status(), 401, "the internal token is a second factor");
}

/// TLS 1.3 client config with the CA as root and no client certificate.
fn no_client_cert(ca: &Ca) -> reqwest::Client {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(ca.0.cert_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let mut cfg = rustls::ClientConfig::builder_with_provider(peer_tls::provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    reqwest::Client::builder().use_preconfigured_tls(cfg).http2_prior_knowledge().build().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_listener_refuses_foreign_and_missing_certs() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let ca = Ca::new();
    let (_a, a_peer) = tls_node("tls-solo", &store, &ca).await;
    let url = format!("{a_peer}/internal/v1/cluster");
    let token = vlpds::server::DEV_INTERNAL_TOKEN;
    // the server counts each refusal (other tests may add their own)
    let failures = || {
        vlpds::metrics::render()
            .lines()
            .find_map(|l| l.strip_prefix("vlpds_peer_tls_handshake_failures_total{side=\"server\"} "))
            .map_or(0.0, |v| v.parse::<f64>().unwrap())
    };
    let refused = |before: f64| async move {
        let t = std::time::Instant::now();
        while failures() <= before {
            assert!(t.elapsed() < Duration::from_secs(5), "refusal not counted");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    // a node of this cluster: in
    let ok = vlpds::http::PeerClient::with_tls(1, ca.node("tls-member")).unwrap();
    assert_eq!(ok.get(&url).header("x-vlpds-internal", token).send().await.unwrap().status(), 200);

    // no client certificate: refused at the handshake
    let before = failures();
    let e = no_client_cert(&ca).get(&url).header("x-vlpds-internal", token).send().await;
    assert!(e.is_err(), "{e:?}");
    refused(before).await;

    // a certificate from another CA (whose CA trusts ours, so only the
    // server's check fails)
    let other = Ca::new();
    let n = peer_tls::issue_node(&other.0.cert_pem, &other.0.key_pem, "tls-intruder", &["127.0.0.1".into()], 30).unwrap();
    let bundle = format!("{}{}", other.0.cert_pem, ca.0.cert_pem);
    let intruder = vlpds::http::PeerClient::with_tls(1, PeerTls::from_pem(&bundle, &n.cert_pem, &n.key_pem).unwrap()).unwrap();
    let before = failures();
    let e = intruder.get(&url).header("x-vlpds-internal", token).send().await;
    assert!(e.is_err(), "{e:?}");
    refused(before).await;

    // a client of another CA doesn't trust our server either
    let stranger = vlpds::http::PeerClient::with_tls(1, other.node("tls-stranger")).unwrap();
    assert!(stranger.get(&url).send().await.is_err());

    // plain http to a TLS peer client: refused before connecting
    assert!(ok.get(url.replace("https://", "http://")).send().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_identity_must_match_the_lease() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let ca = Ca::new();
    let (_a, a_peer) = tls_node("tls-x", &store, &ca).await;
    let url = format!("{a_peer}/internal/v1/cluster");
    let origin = vlpds::http::split_origin(&a_peer).0.to_string();

    // the registry says another node lives at this address: tls-x's
    // certificate (valid, same CA, right host) is refused
    let wrong = vlpds::http::PeerClient::with_tls(1, ca.node("tls-y")).unwrap();
    let o = origin.clone();
    wrong.set_registry(Arc::new(move |q: &str| if q == o { vec!["tls-y".to_string()] } else { Vec::new() }));
    let e = wrong.get(&url).header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN).send().await;
    assert!(e.is_err(), "{e:?}");

    // the right node: in
    let right = vlpds::http::PeerClient::with_tls(1, ca.node("tls-y")).unwrap();
    let o = origin.clone();
    right.set_registry(Arc::new(move |q: &str| if q == o { vec!["tls-x".to_string()] } else { Vec::new() }));
    let r = right.get(&url).header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN).send().await.unwrap();
    assert_eq!(r.status(), 200);

    // the log stream connector checks the node too
    let ws = format!("{}/internal/v1/log/stream", a_peer.replace("https://", "wss://"));
    let mut req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(ws.as_str()).unwrap();
    req.headers_mut().insert("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN.parse().unwrap());
    let bad = tokio_tungstenite::connect_async_tls_with_config(req.clone(), None, false, right.ws_connector(Some("tls-y"))).await;
    assert!(bad.is_err());
    let good = tokio_tungstenite::connect_async_tls_with_config(req, None, false, right.ws_connector(Some("tls-x"))).await;
    assert!(good.is_ok(), "{:?}", good.err());
}

/// A node no peer can reach (`serve_internal` off, the binary's lone mode)
/// has no `/internal/*` at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lone_node_mounts_no_internal_routes() {
    let s = TestServer::spawn_with(|c| c.serve_internal = false).await;
    let r = reqwest::Client::new().get(format!("{}/internal/v1/cluster", s.url)).header("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN).send().await.unwrap();
    assert_eq!(r.status(), 404);
    let a = s.create_account("lone").await;
    s.post(&a, "still serves").await;
}

/// Process CPU seconds (CLOCK_PROCESS_CPUTIME_ID).
fn process_cpu() -> f64 {
    #[repr(C)]
    struct Ts(i64, i64);
    unsafe extern "C" {
        fn clock_gettime(clk: i32, ts: *mut Ts) -> i32;
    }
    let clk = if cfg!(target_os = "macos") { 12 } else { 2 };
    let mut t = Ts(0, 0);
    unsafe { clock_gettime(clk, &mut t) };
    t.0 as f64 + t.1 as f64 * 1e-9
}

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

/// A/B of peer traffic, cleartext h2c vs mTLS, in one process (both nodes
/// in-process, so CPU is the pair's): a 2-node cluster with split
/// listeners either way; (1) forwarded getRecord reads through the
/// non-owner (`VLPDS_AB_READS`, `VLPDS_AB_CONC` in flight), (2) forwarded
/// createRecord writes, (3) log stream: sequential writes at the owner,
/// each timed until the other node's firehose emits it (its log stream).
/// Rounds alternate the modes (`VLPDS_AB_ROUNDS`).
/// `cargo test --profile dev-release --test all peer_tls::bench_ab -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn bench_ab() {
    let reads: usize = env_or("VLPDS_AB_READS", 20_000);
    let writes: usize = env_or("VLPDS_AB_WRITES", 2_000);
    let streamed: usize = env_or("VLPDS_AB_STREAM", 300);
    let conc: usize = env_or("VLPDS_AB_CONC", 64);
    let rounds: usize = env_or("VLPDS_AB_ROUNDS", 2);
    let ca = Ca::new();
    println!("mode,phase,n,secs,req_per_s,cpu_us_per_req,p50_ms,p99_ms");
    for round in 0..rounds {
        for tls in [false, true] {
            let mode = if tls { "mtls" } else { "h2c" };
            let store = Arc::new(object_store::memory::InMemory::new());
            let id = |n: &str| format!("ab{round}{}-{n}", if tls { "t" } else { "p" });
            let (a, _) = split_node(&id("a"), &store, tls.then_some(&ca)).await;
            let (b, _) = split_node(&id("b"), &store, tls.then_some(&ca)).await;
            balanced(&[&a, &b]).await;
            // an account b owns, used through a
            let acct = loop {
                let acct = b.create_account("ab").await;
                if b.app.partitions.get(vlpds::state::partition_of(&acct.did, SHARDS)).is_some() {
                    break acct;
                }
            };
            let rec = b.post(&acct, "ab").await;
            let url = format!(
                "{}/xrpc/com.atproto.repo.getRecord?repo={}&collection={}&rkey={}",
                a.url,
                acct.did,
                rec.collection(),
                rec.rkey()
            );
            let http = reqwest::Client::builder().pool_max_idle_per_host(conc).build().unwrap();
            let report = |phase: &str, n: usize, secs: f64, cpu: f64, mut lat: Vec<f64>| {
                println!(
                    "{mode},{phase},{n},{secs:.2},{:.0},{:.1},{:.3},{:.3}",
                    n as f64 / secs,
                    cpu / n as f64 * 1e6,
                    pct(&mut lat, 0.5) * 1e3,
                    pct(&mut lat, 0.99) * 1e3
                );
            };
            // (1) forwarded reads
            let run = |n: usize, write: bool| {
                let (http, url, a_url, acct) = (http.clone(), url.clone(), a.url.clone(), acct.clone());
                async move {
                    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let tasks: Vec<_> = (0..conc)
                        .map(|_| {
                            let (http, url, a_url, acct, next) = (http.clone(), url.clone(), a_url.clone(), acct.clone(), next.clone());
                            tokio::spawn(async move {
                                let mut lat = Vec::new();
                                while next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < n {
                                    let t = std::time::Instant::now();
                                    let r = if write {
                                        http.post(format!("{a_url}/xrpc/com.atproto.repo.createRecord"))
                                            .header("authorization", format!("Bearer {}", acct.access))
                                            .json(&json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record("ab")}))
                                            .send()
                                            .await
                                            .unwrap()
                                    } else {
                                        http.get(&url).send().await.unwrap()
                                    };
                                    assert!(r.status().is_success(), "{}", r.status());
                                    r.bytes().await.unwrap();
                                    lat.push(t.elapsed().as_secs_f64());
                                }
                                lat
                            })
                        })
                        .collect();
                    let mut all = Vec::new();
                    for t in tasks {
                        all.extend(t.await.unwrap());
                    }
                    all
                }
            };
            run(2_000, false).await; // warm-up
            let before = forwards();
            let (t, c) = (std::time::Instant::now(), process_cpu());
            let lat = run(reads, false).await;
            report("forward_read", reads, t.elapsed().as_secs_f64(), process_cpu() - c, lat);
            let (t, c) = (std::time::Instant::now(), process_cpu());
            let lat = run(writes, true).await;
            report("forward_write", writes, t.elapsed().as_secs_f64(), process_cpu() - c, lat);
            assert!(forwards() >= before + (reads + writes) as u64);
            // (3) log stream: b's commits reaching a's firehose
            let mut sub = a.subscribe_from_now().await;
            let (t0, c) = (std::time::Instant::now(), process_cpu());
            let mut lat = Vec::new();
            for i in 0..streamed {
                let t = std::time::Instant::now();
                b.post(&acct, &format!("s{i}")).await;
                loop {
                    let f = sub.next(FH_TIMEOUT).await.expect("event at a");
                    if f.did() == Some(acct.did.as_str()) && f.kind() == "#commit" {
                        break;
                    }
                }
                lat.push(t.elapsed().as_secs_f64());
            }
            report("write_to_peer_firehose", streamed, t0.elapsed().as_secs_f64(), process_cpu() - c, lat);
            vlpds::server::shutdown(&a.app).await;
            vlpds::server::shutdown(&b.app).await;
        }
    }
}
