//! Server assembly: storage, node log, cluster membership, shards, firehose,
//! workers, HTTP. Used by the binary and by in-process integration tests.
//! A single node is simply a one-node cluster.

use crate::cluster::{Cluster, ClusterConfig, ShardHost};
use crate::firehose::Firehose;
use crate::nodelog::{NodeLog, NodeLogConfig};
use crate::store::{S3Config, Store};
use crate::{auth, state, stats, worker, xrpc};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub public_url: String,
    pub handle_domain: String,
    pub service_did: String,
    pub jwt_secret: String,
    pub admin_token: String,
    /// Shared node-to-node secret (`x-vlpds-internal`); distinct from the
    /// admin token so a leaked node credential isn't an admin credential.
    pub internal_token: String,
    /// None = in-memory object store (tests/dev).
    pub s3: Option<S3Config>,
    pub prefix: String,
    /// (median ms, lognormal sigma) injected on segment PUTs.
    pub inject_latency: Option<(f64, f64)>,
    /// Shards (slot ranges) in the keyspace; fixed per bucket prefix.
    pub shards: u16,
    pub workers: usize,
    pub cache_per_worker: usize,
    pub max_segment_bytes: usize,
    pub firehose_ring_bytes: usize,
    pub hedge_after: Duration,
    pub max_inflight_writes: usize,
    pub cache_dir: Option<std::path::PathBuf>,
    /// Default AppView for proxied app.bsky.* / chat.bsky.* (url, service DID).
    pub appview: Option<(String, String)>,
    /// Moderation service for createReport (url, service DID).
    pub report_service: Option<(String, String)>,
    /// Relays to notify (requestCrawl) at startup.
    pub crawlers: Vec<String>,
    /// Dev mode: email/password-reset tokens are returned/logged instead of mailed.
    pub dev_mode: bool,
    /// Max uploadBlob size in bytes.
    pub max_blob_size: u64,
    /// Unreferenced blobs older than this are deleted by the blob GC.
    pub blob_gc_grace: Duration,
    /// PLC directory used to resolve did:plc documents.
    pub plc_url: String,
    /// createAccount requires an invite code (describeServer inviteCodeRequired).
    pub invite_required: bool,
    /// Membership settings (node id, advertised URL, lease timing). None =
    /// single-node defaults (node id "single", addr = public_url).
    pub cluster: Option<ClusterConfig>,
    /// Reference rate limits (src/ratelimit.rs). Benchmarks turn this off.
    pub rate_limits_enabled: bool,
    /// Proxies (IPs / CIDRs) whose X-Forwarded-For is trusted for the
    /// rate-limit client IP. Empty = always the TCP peer.
    pub trusted_proxies: Vec<String>,
    /// `x-ratelimit-bypass` header value that skips rate limits (reference
    /// PDS_RATE_LIMIT_BYPASS_KEY).
    pub rate_limit_bypass_key: Option<String>,
    /// Opt-in dynamic lexicon resolution for record validation
    /// (src/lexicon.rs): record types without a bundled schema are resolved
    /// over the network and validated; a write waits at most this long for a
    /// resolution (else `validationStatus: "unknown"`). None = off.
    pub resolve_lexicons: Option<Duration>,
    /// With `s3: None`: share this in-memory object store instead of a fresh
    /// one, so several in-process nodes form one cluster (tests).
    pub memory_store: Option<Arc<object_store::memory::InMemory>>,
}

/// Well-known secrets: only accepted with `dev_mode` (see [`Config::check_secrets`]).
pub const DEV_JWT_SECRET: &str = "dev-secret-change-me";
pub const DEV_ADMIN_TOKEN: &str = "dev-admin-token";
pub const DEV_INTERNAL_TOKEN: &str = "dev-internal-token";
/// Minimum length of each secret outside dev mode.
pub const MIN_SECRET_LEN: usize = 32;

impl Config {
    /// Startup check (the binary calls it; in-process tests may skip it):
    /// secrets are never empty, and outside dev mode they must be set,
    /// not the dev defaults, at least [`MIN_SECRET_LEN`] bytes, and
    /// pairwise distinct.
    pub fn check_secrets(&self) -> anyhow::Result<()> {
        let secrets = [
            ("VLPDS_JWT_SECRET", &self.jwt_secret, DEV_JWT_SECRET),
            ("VLPDS_ADMIN_TOKEN", &self.admin_token, DEV_ADMIN_TOKEN),
            ("VLPDS_INTERNAL_TOKEN", &self.internal_token, DEV_INTERNAL_TOKEN),
        ];
        for (name, v, dev) in secrets {
            anyhow::ensure!(!v.is_empty(), "{name} must be set (non-empty)");
            if self.dev_mode {
                continue;
            }
            anyhow::ensure!(v != dev, "{name} is the dev default; set a real secret (or run with --dev-mode)");
            anyhow::ensure!(v.len() >= MIN_SECRET_LEN, "{name} must be at least {MIN_SECRET_LEN} bytes");
        }
        if !self.dev_mode {
            for (i, (a, va, _)) in secrets.iter().enumerate() {
                for (b, vb, _) in &secrets[i + 1..] {
                    anyhow::ensure!(va != vb, "{a} and {b} must differ");
                }
            }
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            public_url: "http://localhost:2583".into(),
            handle_domain: "vlpds.test".into(),
            service_did: "did:web:localhost".into(),
            jwt_secret: DEV_JWT_SECRET.into(),
            admin_token: DEV_ADMIN_TOKEN.into(),
            internal_token: DEV_INTERNAL_TOKEN.into(),
            s3: None,
            prefix: "vlpds".into(),
            inject_latency: None,
            shards: 8,
            workers: 2,
            cache_per_worker: 10_000,
            max_segment_bytes: 8 << 20,
            firehose_ring_bytes: 64 << 20,
            hedge_after: Duration::from_millis(100),
            max_inflight_writes: 20_000,
            cache_dir: None,
            appview: None,
            report_service: None,
            crawlers: Vec::new(),
            dev_mode: true,
            cluster: None,
            max_blob_size: 100 << 20,
            blob_gc_grace: Duration::from_secs(6 * 3600),
            plc_url: "https://plc.directory".into(),
            invite_required: false,
            rate_limits_enabled: true,
            trusted_proxies: Vec::new(),
            rate_limit_bypass_key: None,
            resolve_lexicons: None,
            memory_store: None,
        }
    }
}

/// Opens storage, joins the cluster, starts the node log, acquires shards,
/// and starts the firehose merger and repo workers.
pub async fn build(cfg: Config) -> anyhow::Result<Arc<xrpc::App>> {
    // Separate clients (connection pools) for the commit log and everything else.
    let (store, state_store) = match &cfg.s3 {
        None => {
            let m = match &cfg.memory_store {
                Some(raw) => Store { raw: raw.clone(), ..Store::memory(cfg.inject_latency) },
                None => Store::memory(cfg.inject_latency),
            };
            (m.clone(), Store { latency: None, ..m })
        }
        Some(s3) => (Store::s3(s3, &cfg.prefix, cfg.inject_latency)?, Store::s3(s3, &cfg.prefix, None)?),
    };
    let firehose = Firehose::new(cfg.firehose_ring_bytes);
    let (merger_tx, merger_rx) = tokio::sync::mpsc::unbounded_channel();
    let n = cfg.shards;
    let table = crate::partitions::PartitionTable::new(n);
    let lookup_parts = table.clone();
    let lookup: worker::PartitionLookup = Arc::new(move |did: &str| lookup_parts.get(state::partition_of(did, n) as usize));
    let workers = worker::spawn(cfg.workers, cfg.cache_per_worker, lookup, tokio::runtime::Handle::current());

    let mut cc = cfg.cluster.clone().unwrap_or_else(|| ClusterConfig { node_id: "single".into(), addr: cfg.public_url.clone(), ..Default::default() });
    cc.shards = n;
    let started = std::time::Instant::now();
    let cluster = Cluster::join(cc, state_store.clone()).await?;
    let lease = cluster.clone();
    let log = NodeLog::start(
        store.clone(),
        NodeLogConfig {
            log_id: cluster.log_id.clone(),
            writer: cluster.writer,
            max_segment_bytes: cfg.max_segment_bytes,
            hedge_after: cfg.hedge_after,
            lease_ok: Some(Arc::new(move || lease.lease_valid())),
        },
        merger_tx.clone(),
    );
    firehose.set_source(&log.log_id, Some(crate::firehose::Source::Local(log.wm.clone())));
    *firehose.store.write() = Some(store.clone());
    firehose.spawn_merger(merger_rx);
    {
        // never announce a watermark beyond our node lease
        let (c, wm) = (cluster.clone(), log.wm.clone());
        tokio::spawn(async move {
            loop {
                wm.set_lease_expiry(c.lease_expiry_us());
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        // bound how much of our log a successor replays after a crash (~10 s)
        let l = log.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                l.checkpoint_all().await;
            }
        });
    }
    let node = crate::node::Node::new(
        cluster.clone(),
        log.clone(),
        store.clone(),
        state_store.clone(),
        table.clone(),
        firehose.clone(),
        merger_tx,
        workers.clone(),
        cfg.cache_dir.clone(),
        cfg.internal_token.clone(),
    );
    let host: Arc<dyn ShardHost> = node.clone();
    let node_handle = node.clone();
    // first membership step inline, so a lone node serves with all its shards
    cluster.step(&host).await?;
    cluster.spawn(host);
    tracing::info!(
        node = %cluster.cfg.node_id, log = %cluster.log_id, writer = cluster.writer, shards = n,
        owned = cluster.owned().len(), elapsed_ms = started.elapsed().as_millis() as u64, "node ready"
    );

    Ok(Arc::new(xrpc::App {
        jwt: auth::Jwt::new(&cfg.jwt_secret, &cfg.service_did),
        store: state_store,
        workers,
        partitions: table,
        firehose,
        tids: crate::tid::TidClock::new(),
        public_url: cfg.public_url.clone(),
        handle_domain: cfg.handle_domain.clone(),
        write_permits: tokio::sync::Semaphore::new(cfg.max_inflight_writes),
        admin_token: cfg.admin_token.clone(),
        did_resolver: Arc::new(crate::did_resolver::DidResolver::new(&cfg.plc_url, cfg.dev_mode)),
        config: Arc::new(cfg),
        cluster: Some(cluster),
        log,
        // node-to-node: fail fast when a peer is unreachable (forwarded
        // requests return 503 instead of hanging)
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .connect_timeout(Duration::from_millis(1000))
            .timeout(Duration::from_secs(15))
            .build()?,
        node: node_handle,
    }))
}

/// Background reporters (log line, watermark lag gauges, runtime stall detector).
pub fn spawn_reporters(app: &Arc<xrpc::App>) {
    stats::spawn_reporter(Duration::from_secs(5));
    let parts = app.partitions.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let now = crate::nodelog::seq_floor(crate::tid::now_micros());
            // one node log: report its watermark lag once
            if let Some(p) = parts.owned().first() {
                let lag_us = (now - p.wm.get()).max(0) >> 8;
                crate::metrics::WATERMARK_LAG.with_label_values(&["node"]).set(lag_us);
            }
        }
    });
    stats::spawn_stall_detector();
}

/// Builds the app and serves it on `listener` in a background task. Returns
/// the app and the bound address (tests bind 127.0.0.1:0).
pub async fn spawn(
    cfg: Config,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<(Arc<xrpc::App>, std::net::SocketAddr)> {
    let app = build(cfg).await?;
    let addr = listener.local_addr()?;
    let router = with_forwarding(&app, xrpc::router(app.clone()));
    tokio::spawn(async move {
        if let Err(e) = serve(listener, router).await {
            tracing::error!("server exited: {e:#}");
        }
    });
    Ok((app, addr))
}

/// HTTP/1.1 + HTTP/2 (h2c) server. axum::serve doesn't expose HTTP/2 settings,
/// and hyper's default 64KB connection window chops request bodies on busy
/// connections into tiny DATA frames, which trips h2's small-frame flood guard.
pub async fn serve(listener: tokio::net::TcpListener, router: axum::Router) -> anyhow::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http2()
        .initial_stream_window_size(4 << 20)
        .initial_connection_window_size(64 << 20)
        .max_frame_size(256 << 10)
        .max_concurrent_streams(16_384);
    loop {
        let (sock, peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        // peer address for rate limiting (axum ConnectInfo)
        let svc = TowerToHyperService::new(tower::ServiceExt::map_request(
            router.clone(),
            move |mut req: axum::http::Request<hyper::body::Incoming>| {
                req.extensions_mut().insert(axum::extract::ConnectInfo(peer));
                req
            },
        ));
        let builder = builder.clone();
        tokio::spawn(async move {
            let _ = builder
                .serve_connection_with_upgrades(TokioIo::new(sock), svc)
                .await;
        });
    }
}

struct ClusterRouter {
    app: Arc<xrpc::App>,
}

#[async_trait::async_trait]
impl crate::forward::Router for ClusterRouter {
    fn remote_owner(&self, did: &str) -> Option<String> {
        self.app.remote_owner(did)
    }
    async fn resolve_handle(&self, handle: &str) -> Option<String> {
        self.app.resolve_handle(handle).await.ok().flatten()
    }
}

/// In cluster mode, proxies requests for DIDs owned by other nodes.
pub fn with_forwarding(app: &Arc<xrpc::App>, router: axum::Router) -> axum::Router {
    if app.cluster.is_none() {
        return router;
    }
    let r: Arc<dyn crate::forward::Router> = Arc::new(ClusterRouter { app: app.clone() });
    let client = app.http.clone();
    router.layer(axum::middleware::from_fn(move |req, next| {
        let (r, client) = (r.clone(), client.clone());
        async move { crate::forward::route(r, client, req, next).await }
    }))
}

/// Graceful shutdown: hand every shard back (drain, checkpoint, release) and
/// drop the node lease, so successors take over immediately instead of
/// waiting out the lease TTL.
pub async fn shutdown(app: &Arc<xrpc::App>) {
    if let Some(c) = &app.cluster {
        let host: Arc<dyn ShardHost> = app.node.clone();
        tracing::info!(shards = c.owned().len(), "graceful shutdown: releasing shards");
        c.shutdown(&host).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_fail_closed_outside_dev_mode() {
        let dev = Config::default();
        assert!(dev.dev_mode);
        dev.check_secrets().expect("dev defaults are fine in dev mode");
        let empty_admin = Config { admin_token: String::new(), ..Config::default() };
        assert!(empty_admin.check_secrets().is_err(), "empty admin token refused even in dev mode");

        let prod = |jwt: &str, admin: &str, internal: &str| Config {
            dev_mode: false,
            jwt_secret: jwt.into(),
            admin_token: admin.into(),
            internal_token: internal.into(),
            ..Config::default()
        };
        let (a, b, c) = ("a".repeat(32), "b".repeat(32), "c".repeat(32));
        prod(&a, &b, &c).check_secrets().expect("strong distinct secrets");
        // dev defaults
        let e = Config { dev_mode: false, ..Config::default() }.check_secrets().unwrap_err();
        assert!(e.to_string().contains("VLPDS_JWT_SECRET"), "{e}");
        assert!(prod(&a, DEV_ADMIN_TOKEN, &c).check_secrets().is_err());
        assert!(prod(&a, &b, DEV_INTERNAL_TOKEN).check_secrets().is_err());
        // unset / short / shared
        assert!(prod("", &b, &c).check_secrets().is_err());
        assert!(prod(&a, &"b".repeat(31), &c).check_secrets().is_err());
        let e = prod(&a, &b, &b).check_secrets().unwrap_err();
        assert!(e.to_string().contains("must differ"), "{e}");
    }
}
