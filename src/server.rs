//! Server assembly: storage, node log, cluster membership, shards, firehose,
//! workers, HTTP. Used by the binary and by in-process integration tests.
//! A single node is simply a one-node cluster.

use crate::cluster::{Cluster, ClusterConfig, ShardHost};
use crate::firehose::Firehose;
use crate::nodelog::{NodeLog, NodeLogConfig};
use crate::store::{S3Config, Store};
use crate::{auth, stats, worker, xrpc};
use axum::response::IntoResponse;
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
    pub shards: u32,
    pub workers: usize,
    pub cache_per_worker: usize,
    /// Approximate heap budget of the node's cached repos and their loaded
    /// MST paths (DESIGN.md "Partial MSTs"; split across workers; 0 =
    /// bounded by count only).
    pub repo_cache_bytes: usize,
    /// Cold open: up to this much of a repo's `M/` range is read with one
    /// scan (`--lazy-mst-prefetch-kb`).
    pub lazy_mst_prefetch_bytes: usize,
    /// Tests: drop every idle repo's loaded paths after each worker pass,
    /// so every write and read walks from the root through the store.
    /// Default: `VLPDS_LAZY_MST_UNLOAD_IDLE=1` (runs the suite that way).
    pub lazy_mst_unload_idle: bool,
    /// Bytes of loaded MST nodes kept process-wide for readers and fetches
    /// (`--lazy-mst-node-cache-mb`; `mst_store::NodeCache`).
    pub lazy_mst_node_cache_bytes: usize,
    pub max_segment_bytes: usize,
    /// Segment PUTs in flight per node log (DESIGN.md "Pipelined segment PUTs").
    pub log_inflight: usize,
    /// Byte budget of the node log's live ring (sealed segments for peer
    /// followers and the merger; a follower behind it catches up from S3).
    pub live_ring_bytes: usize,
    /// Byte budget of the firehose merger's per-log queues; a log over it is
    /// read back from S3 instead.
    pub firehose_merge_queue_bytes: usize,
    pub firehose_ring_bytes: usize,
    /// Threads of the process-wide firehose runtime that serves
    /// subscribeRepos connections (0 = serve them on the request runtime).
    pub firehose_threads: usize,
    /// A live subscriber this many bytes behind the head gets ConsumerTooSlow.
    pub firehose_max_lag_bytes: usize,
    /// Cursor backfill: S3 read-ahead per subscriber, and the segment cache
    /// shared by subscribers replaying the same range.
    pub backfill_readahead_bytes: usize,
    pub backfill_cache_bytes: usize,
    pub hedge_after: Duration,
    pub max_inflight_writes: usize,
    pub cache_dir: Option<std::path::PathBuf>,
    /// The SST disk cache budget of this node (`--disk-cache-mb`), split
    /// over the layout's shards; None = SlateDB's 16 GiB per shard.
    pub disk_cache_bytes: Option<u64>,
    /// An explicit per-shard disk cache cap (`--disk-cache-shard-mb`).
    pub disk_cache_shard_bytes: Option<u64>,
    /// Default AppView for proxied app.bsky.* / chat.bsky.* (url, service DID).
    pub appview: Option<(String, String)>,
    /// Moderation service for createReport (url, service DID).
    pub report_service: Option<(String, String)>,
    /// Image URLs in read-after-write views: a printf-style pattern with
    /// three `%s` (preset such as `avatar`, DID, blob CID), like the
    /// reference's PDS_BSKY_APP_VIEW_CDN_URL_PATTERN. None: the PDS's own
    /// `com.atproto.sync.getBlob` URL.
    pub appview_cdn_url_pattern: Option<String>,
    /// Relays to notify (requestCrawl) at startup.
    pub crawlers: Vec<String>,
    /// Dev mode: email/password-reset tokens are returned/logged instead of mailed.
    pub dev_mode: bool,
    /// Key-encryption keys for secrets at rest (src/secrets.rs; `--kek-file`,
    /// `--gcp-kms-key`). Empty: the dev KEK, dev mode only.
    pub kek: crate::secrets::KekConfig,
    /// This node's outbound email (`--email-smtp-url`, crate::mail). None:
    /// the process-wide mailer (logs only, unless `xrpc::set_mailer`).
    pub mailer: Option<crate::mail::SharedMailer>,
    /// admin sendEmail's mailer (`--moderation-email-smtp-url`). None: the
    /// main mailer above.
    pub moderation_mailer: Option<crate::mail::SharedMailer>,
    /// Email branding (`--email-brand-name` etc., crate::mail::Branding).
    pub email_branding: crate::mail::Branding,
    /// Max uploadBlob size in bytes (also describeServer's blobUploadLimit).
    pub max_blob_size: u64,
    /// describeServer `links.privacyPolicy` / `links.termsOfService` /
    /// `contact.email` (reference PDS_PRIVACY_POLICY_URL,
    /// PDS_TERMS_OF_SERVICE_URL, PDS_CONTACT_EMAIL_ADDRESS).
    pub privacy_policy_url: Option<String>,
    pub terms_of_service_url: Option<String>,
    pub contact_email_address: Option<String>,
    /// Unreferenced blobs older than this are deleted by the blob GC.
    pub blob_gc_grace: Duration,
    /// PLC directory: resolves did:plc documents and, with PLC registration
    /// on (`plc`), receives new accounts' genesis ops and their updates.
    pub plc_url: String,
    /// PLC registration: mode and the server rotation key (src/plc;
    /// DESIGN.md "PLC identity"). Default: registration off (no rotation
    /// key), which only dev mode accepts (`check_secrets`).
    pub plc: crate::plc::PlcConfig,
    /// createAccount requires an invite code (describeServer inviteCodeRequired).
    pub invite_required: bool,
    /// With `invite_required`: each account earns one invite code per this
    /// interval of account age (reference PDS_INVITE_INTERVAL), created by
    /// getAccountInviteCodes. None = accounts never earn codes.
    pub invite_interval: Option<Duration>,
    /// Earned codes count only account age after this instant, in Unix ms
    /// (reference PDS_INVITE_EPOCH; 0 = from account creation).
    pub invite_epoch_ms: i64,
    /// The moderation service (Ozone) DID trusted to call the moderator
    /// admin methods with service auth (reference PDS_MOD_SERVICE_DID;
    /// `xrpc::authn::MODERATOR_METHODS`). None = admin Basic auth only.
    pub mod_service_did: Option<String>,
    /// DNS TXT lookups for external handle proofs (`_atproto.<handle>`).
    /// None = the system resolver (src/handle_resolver.rs); tests inject a stub.
    pub txt_resolver: Option<crate::handle_resolver::TxtResolverRef>,
    /// Membership settings (node id, advertised URL, lease timing). None =
    /// single-node defaults (node id "single", addr = public_url).
    pub cluster: Option<ClusterConfig>,
    /// Reference rate limits (src/ratelimit.rs). Benchmarks turn this off.
    pub rate_limits_enabled: bool,
    /// Proxies (IPs / CIDRs) whose X-Forwarded-For is trusted for the
    /// rate-limit client IP. Empty = always the TCP peer.
    pub trusted_proxies: Vec<String>,
    /// h2c connections to each peer node (crate::http::PeerClient).
    pub peer_connections: usize,
    /// `x-ratelimit-bypass` header value that skips rate limits (reference
    /// PDS_RATE_LIMIT_BYPASS_KEY).
    pub rate_limit_bypass_key: Option<String>,
    /// Opt-in dynamic lexicon resolution for record validation
    /// (src/lexicon.rs): record types without a bundled schema are resolved
    /// over the network and validated; a write waits at most this long for a
    /// resolution (else `validationStatus: "unknown"`). None = off.
    pub resolve_lexicons: Option<Duration>,
    /// With `s3: None`: share this in-memory object store instead of a fresh
    /// one, so several in-process nodes form one cluster (tests; may be
    /// wrapped, e.g. in a `ThrottledStore` for object-store latency).
    pub memory_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// Serve /metrics and /debug/pprof on a separate listener (the binary's
    /// `--metrics-listen`) and not on the app port. None = on the app port.
    pub metrics_listen: Option<String>,
    /// Log segment retention (src/retention.rs; `--log-retention`). None = keep
    /// every segment forever.
    pub log_retention: Option<crate::retention::Config>,
    /// Memory budget of the in-memory caches (verified tokens, proxy
    /// accounts, DID documents, ...; src/caches.rs), split between them by
    /// weight. None = 10% of physical RAM or the cgroup limit.
    pub cache_budget_bytes: Option<u64>,
    /// Entry caps overriding the budget's, per cache.
    pub cache_entries: Vec<(crate::caches::Cache, usize)>,
    /// Automatic shard splits (src/reshard.rs; off by default).
    pub reshard_policy: crate::reshard::Policy,
    /// Retired split/merge state GC and forced detach (src/reshard_gc.rs).
    /// None = keep retired parents' state forever.
    pub reshard_gc: Option<crate::reshard_gc::Config>,
    /// Every shard is checkpointed once per this (bounds a successor's
    /// replay; `--checkpoint-every`).
    pub checkpoint_every: Duration,
    /// Spread the checkpoints over the interval, one shard at a time, instead
    /// of all shards back to back (`--checkpoint-stagger`).
    pub checkpoint_stagger: bool,
    /// Recently written repos tracked per shard and preloaded by its next
    /// owner (`--preload-recent`; 0 = off).
    pub preload_recent: usize,
    /// A forwarded write not started within this (its repo still loading)
    /// is answered 503 `RepoLoading` and retried by the forwarding node
    /// (`--forwarded-write-start-ms`; None = wait for it).
    pub forwarded_write_start: Option<Duration>,
    /// The node a client called resends its repo writes answered "not
    /// applied" (RepoLoading, ShardMoved) for up to 20 s
    /// (`--retry-unapplied-writes`; crate::forward).
    pub retry_unapplied_writes: bool,
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
        self.kek.check(self.dev_mode)?;
        self.plc.check(self.dev_mode, &self.plc_url)?;
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
            repo_cache_bytes: 4 << 30,
            lazy_mst_prefetch_bytes: crate::worker::DEFAULT_PREFETCH_BYTES,
            lazy_mst_unload_idle: std::env::var("VLPDS_LAZY_MST_UNLOAD_IDLE").is_ok_and(|v| v == "1"),
            lazy_mst_node_cache_bytes: crate::mst_store::DEFAULT_NODE_CACHE_BYTES,
            max_segment_bytes: 8 << 20,
            log_inflight: crate::nodelog::DEFAULT_LOG_INFLIGHT,
            live_ring_bytes: crate::nodelog::DEFAULT_LIVE_RING_BYTES,
            firehose_merge_queue_bytes: crate::firehose::DEFAULT_MERGE_QUEUE_BYTES,
            firehose_ring_bytes: 64 << 20,
            firehose_threads: 2,
            firehose_max_lag_bytes: crate::firehose::DEFAULT_MAX_LAG_BYTES,
            backfill_readahead_bytes: crate::backfill::DEFAULT_READAHEAD_BYTES,
            backfill_cache_bytes: crate::backfill::DEFAULT_CACHE_BYTES,
            hedge_after: Duration::from_millis(100),
            max_inflight_writes: 20_000,
            cache_dir: None,
            disk_cache_bytes: None,
            disk_cache_shard_bytes: None,
            appview: None,
            report_service: None,
            appview_cdn_url_pattern: None,
            crawlers: Vec::new(),
            dev_mode: true,
            kek: Default::default(),
            mailer: None,
            moderation_mailer: None,
            email_branding: Default::default(),
            cluster: None,
            max_blob_size: 100 << 20,
            privacy_policy_url: None,
            terms_of_service_url: None,
            contact_email_address: None,
            blob_gc_grace: Duration::from_secs(6 * 3600),
            plc_url: crate::plc::DEFAULT_PLC_URL.into(),
            plc: Default::default(),
            invite_required: false,
            invite_interval: None,
            invite_epoch_ms: 0,
            mod_service_did: None,
            txt_resolver: None,
            rate_limits_enabled: true,
            trusted_proxies: Vec::new(),
            peer_connections: crate::http::DEFAULT_PEER_CONNECTIONS,
            rate_limit_bypass_key: None,
            resolve_lexicons: None,
            memory_store: None,
            metrics_listen: None,
            log_retention: Some(crate::retention::Config::default()),
            cache_budget_bytes: None,
            cache_entries: Vec::new(),
            reshard_policy: Default::default(),
            reshard_gc: Some(Default::default()),
            checkpoint_every: Duration::from_secs(10),
            checkpoint_stagger: true,
            preload_recent: crate::partition::DEFAULT_RECENT_REPOS,
            forwarded_write_start: Some(crate::forward::FORWARDED_WRITE_START),
            retry_unapplied_writes: true,
        }
    }
}

/// Opens storage, joins the cluster, starts the node log, acquires shards,
/// and starts the firehose merger and repo workers.
pub async fn build(cfg: Config) -> anyhow::Result<Arc<xrpc::App>> {
    let (caps, budget) = crate::caches::resolve(cfg.cache_budget_bytes, &cfg.cache_entries);
    crate::caches::apply(&caps);
    if let Some(m) = crate::caches::memory_bytes() {
        crate::metrics::MEMORY_LIMIT.set(m as i64);
    }
    crate::metrics::REPO_CACHE_CAPACITY.set(cfg.repo_cache_bytes as i64);
    tracing::info!(
        budget_mb = budget >> 20,
        memory_mb = crate::caches::memory_bytes().map(|m| m >> 20),
        full_mb = caps.total_bytes() >> 20,
        "cache caps: {caps}"
    );
    // Separate clients (connection pools) for the commit log and everything else.
    let (store, state_store) = match &cfg.s3 {
        None => {
            let m = match &cfg.memory_store {
                Some(raw) => Store { raw: raw.clone(), ..Store::memory(cfg.inject_latency) },
                None => Store::memory(cfg.inject_latency),
            };
            (m.clone().counted("log"), Store { latency: None, ..m }.counted("state"))
        }
        Some(s3) => (Store::s3(s3, &cfg.prefix, cfg.inject_latency)?.counted("log"), Store::s3(s3, &cfg.prefix, None)?.counted("state")),
    };
    let firehose = Firehose::new(crate::firehose::Options {
        ring_bytes: cfg.firehose_ring_bytes,
        max_lag_bytes: cfg.firehose_max_lag_bytes,
        readahead_bytes: cfg.backfill_readahead_bytes,
        backfill_cache_bytes: cfg.backfill_cache_bytes,
        runtime: (cfg.firehose_threads > 0).then(|| crate::firehose::runtime(cfg.firehose_threads)),
    });
    firehose.set_max_queue_bytes(cfg.firehose_merge_queue_bytes.max(1));
    let (merger_tx, merger_rx) = tokio::sync::mpsc::unbounded_channel();
    let n = cfg.shards;
    let table = crate::partitions::PartitionTable::new(n);
    let lookup_parts = table.clone();
    let lookup: worker::PartitionLookup = Arc::new(move |did: &str| lookup_parts.for_key(did));
    let limits = worker::CacheLimits { entries: cfg.cache_per_worker, bytes: cfg.repo_cache_bytes / cfg.workers.max(1), prefetch_bytes: cfg.lazy_mst_prefetch_bytes, unload_idle: cfg.lazy_mst_unload_idle };
    crate::mst_store::NODE_CACHE.set_bytes(cfg.lazy_mst_node_cache_bytes);
    let secrets = Arc::new(crate::secrets::Secrets::from_config(&cfg.kek, cfg.dev_mode)?);
    tracing::info!(kek = secrets.current_kid(), unwrap_keks = ?secrets.kids(), dev = secrets.is_dev(), "secrets at rest");
    let workers = worker::spawn_with_secrets(cfg.workers, limits, lookup, tokio::runtime::Handle::current(), secrets.clone());
    let plc = crate::plc::Plc::from_config(&cfg.plc, &cfg.plc_url, cfg.dev_mode, &secrets).await?;

    let mut cc = cfg.cluster.clone().unwrap_or_else(|| ClusterConfig { node_id: "single".into(), addr: cfg.public_url.clone(), ..Default::default() });
    cc.shards = n;
    crate::version::init_metrics();
    let started = std::time::Instant::now();
    let cluster = Cluster::join(cc, state_store.clone()).await?;
    // route by this prefix's layout (it may differ from --shards: splits,
    // merges, or a different count at creation)
    table.replace_layout(cluster.layout());
    cluster.set_reshard_policy(cfg.reshard_policy.clone());
    let lease = cluster.clone();
    let log = NodeLog::start_with_inflight(
        store.clone(),
        NodeLogConfig {
            log_id: cluster.log_id.clone(),
            writer: cluster.writer,
            max_segment_bytes: cfg.max_segment_bytes,
            hedge_after: cfg.hedge_after,
            lease_ok: Some(Arc::new(move || lease.lease_valid())),
        },
        cfg.log_inflight,
        merger_tx.clone(),
    );
    log.live.set_max_bytes(cfg.live_ring_bytes);
    firehose.set_source(&log.log_id, Some(crate::firehose::Source::Local(log.wm.clone())));
    *firehose.store.write() = Some(store.clone());
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
        log.spawn_checkpoints(cfg.checkpoint_every, cfg.checkpoint_stagger);
    }
    let http = crate::http::PeerClient::new(cfg.peer_connections)?;
    let node = crate::node::Node::new(
        cluster.clone(),
        log.clone(),
        store.clone(),
        state_store.clone(),
        table.clone(),
        firehose.clone(),
        merger_tx,
        workers.clone(),
        cfg.cache_dir.clone().map(|dir| crate::partition::DiskCacheConfig {
            dir,
            node_bytes: cfg.disk_cache_bytes,
            shard_bytes: cfg.disk_cache_shard_bytes,
        }),
        cfg.internal_token.clone(),
        http.clone(),
        cfg.preload_recent,
    );
    let host: Arc<dyn ShardHost> = node.clone();
    let node_handle = node.clone();
    // first membership step inline, so a lone node serves with all its shards
    cluster.step(&host).await?;
    // Only now merge: the step registered a follower for every live peer, so
    // the merger never settles past the start floor on our own log's
    // watermark alone (a peer followed after that would owe only its events
    // above the new position; the ones below it would be lost).
    firehose.spawn_merger(merger_rx);
    cluster.spawn(host);
    if let Some(rc) = cfg.log_retention.clone() {
        let (c, l) = (cluster.clone(), cluster.clone());
        let members = crate::retention::Membership {
            live_logs: Box::new(move || c.peers().into_iter().map(|p| p.log_id).chain([c.log_id.clone()]).collect()),
            // dead logs are pruned by the owner of the shard holding slot 0
            leader: Box::new(move || l.owner_of(l.layout().shard_of_slot(0)).is_some_and(|(o, _)| o == l.cfg.node_id)),
        };
        crate::retention::Retention::new(store.clone(), log.clone(), rc, members).spawn();
    }
    if let Some(gc) = cfg.reshard_gc.clone() {
        let (l, v, t) = (cluster.clone(), cluster.clone(), table.clone());
        let hooks = crate::reshard_gc::Hooks {
            // like dead-log retention: the owner of the shard holding slot 0
            leader: Box::new(move || l.owner_of(l.layout().shard_of_slot(0)).is_some_and(|(o, _)| o == l.cfg.node_id)),
            lease_ok: Box::new(move || v.lease_valid()),
            owned: Box::new(move || t.owned().into_iter().map(|p| (p.id, p.db.clone())).collect()),
            crash_at: None,
        };
        crate::reshard_gc::ReshardGc::new(state_store.clone(), gc, hooks).spawn();
    }
    tracing::info!(
        node = %cluster.cfg.node_id, log = %cluster.log_id, writer = cluster.writer, shards = cluster.layout().shards.len(),
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
        http,
        ratelimit: Arc::new(crate::ratelimit::Limiter::new(&cfg)),
        secrets,
        plc,
        config: Arc::new(cfg),
        cluster: Some(cluster),
        log,
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
    let router = router(&app);
    tokio::spawn(async move {
        if let Err(e) = serve(listener, router).await {
            tracing::error!("server exited: {e:#}");
        }
    });
    if let Some(m) = &app.config.metrics_listen {
        let l = tokio::net::TcpListener::bind(m).await?;
        let r = metrics_router(&app);
        tokio::spawn(async move {
            if let Err(e) = serve(l, r).await {
                tracing::error!("metrics server exited: {e:#}");
            }
        });
    }
    Ok((app, addr))
}

/// Paths served only by [`metrics_router`] when `metrics_listen` is set.
const METRICS_PATHS: [&str; 2] = ["/metrics", "/debug/pprof/"];

/// The app port's router: XRPC, OAuth, web UI and /internal, with cluster
/// forwarding; /metrics and /debug/pprof too unless `metrics_listen` moves
/// them to [`metrics_router`].
pub fn router(app: &Arc<xrpc::App>) -> axum::Router {
    let r = with_forwarding(app, xrpc::router(app.clone()));
    if app.config.metrics_listen.is_none() {
        return r;
    }
    r.layer(axum::middleware::from_fn(|req: axum::extract::Request, next: axum::middleware::Next| async move {
        let p = req.uri().path();
        if METRICS_PATHS.iter().any(|m| p == *m || (m.ends_with('/') && p.starts_with(m))) {
            return axum::http::StatusCode::NOT_FOUND.into_response();
        }
        next.run(req).await
    }))
}

/// The `--metrics-listen` router: Prometheus /metrics, and the on-demand CPU
/// profiler (/debug/pprof/profile, admin token; `--features profiling`).
pub fn metrics_router(app: &Arc<xrpc::App>) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(|| async { crate::metrics::render() }))
        .merge(crate::profiling::routes())
        .with_state(app.clone())
}

/// HTTP/1.1 + HTTP/2 (h2c) server. axum::serve doesn't expose HTTP/2 settings,
/// and hyper's default 64KB connection window chops request bodies on busy
/// connections into tiny DATA frames, which trips h2's small-frame flood guard.
/// Settings and their reasons: DESIGN.md "HTTP".
pub async fn serve(listener: tokio::net::TcpListener, router: axum::Router) -> anyhow::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        // the timer enables hyper's header read timeout (slowloris): a
        // client gets 30 s to send a request head, which also bounds an idle
        // keep-alive connection waiting for its next request
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(30))
        .keep_alive(true);
    builder
        .http2()
        .timer(TokioTimer::new())
        .initial_stream_window_size(4 << 20)
        .initial_connection_window_size(64 << 20)
        .max_frame_size(256 << 10)
        // per connection; the load generator spreads its requests over 64
        // connections, peers over --peer-connections
        .max_concurrent_streams(1024)
        // atproto heads (DPoP proof + access token) are a few KiB
        .max_header_list_size(32 << 10)
        // PING idle clients; drop the connection after 10 s without a PONG
        .keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(10))
        // rapid-reset (CVE-2023-44487) and local-error-reset floods: hyper/h2's
        // defaults, stated so a change is a decision
        .max_pending_accept_reset_streams(20)
        .max_local_error_reset_streams(1024);
    let active = ActiveRequests {
        h1: crate::metrics::HTTP_SERVER_ACTIVE.with_label_values(&["h1"]),
        h2: crate::metrics::HTTP_SERVER_ACTIVE.with_label_values(&["h2"]),
    };
    loop {
        let (sock, peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        crate::metrics::HTTP_SERVER_CONNECTIONS.inc();
        // peer address for rate limiting (axum ConnectInfo)
        let svc = TowerToHyperService::new(Track {
            inner: tower::ServiceExt::map_request(
                router.clone(),
                move |mut req: axum::http::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(axum::extract::ConnectInfo(peer));
                    req
                },
            ),
            active: active.clone(),
        });
        let builder = builder.clone();
        tokio::spawn(async move {
            crate::metrics::HTTP_SERVER_OPEN.inc();
            let _ = builder
                .serve_connection_with_upgrades(TokioIo::new(sock), svc)
                .await;
            crate::metrics::HTTP_SERVER_OPEN.dec();
        });
    }
}

#[derive(Clone)]
struct ActiveRequests {
    h1: prometheus::IntGauge,
    h2: prometheus::IntGauge,
}

/// Counts requests (h2: streams) until their response head
/// (`vlpds_http_server_active_requests`).
#[derive(Clone)]
struct Track<S> {
    inner: S,
    active: ActiveRequests,
}

/// Decrements on drop, so a reset stream (future dropped) is counted out.
struct ActiveGuard(prometheus::IntGauge);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

impl<S, B> tower::Service<axum::http::Request<B>> for Track<S>
where
    S: tower::Service<axum::http::Request<B>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::http::Request<B>) -> Self::Future {
        let g = if req.version() == axum::http::Version::HTTP_2 { &self.active.h2 } else { &self.active.h1 };
        g.inc();
        let guard = ActiveGuard(g.clone());
        let f = self.inner.call(req);
        Box::pin(async move {
            let r = f.await;
            drop(guard);
            r
        })
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
    fn app(&self) -> Option<&xrpc::App> {
        Some(&self.app)
    }
    fn alone(&self) -> bool {
        self.app.cluster.as_ref().is_some_and(|c| c.alone())
    }
}

/// In cluster mode, proxies requests for DIDs owned by other nodes.
pub fn with_forwarding(app: &Arc<xrpc::App>, router: axum::Router) -> axum::Router {
    if app.cluster.is_none() {
        return router;
    }
    // one Arc clone per request (router and client together)
    let ctx = Arc::new((ClusterRouter { app: app.clone() }, app.http.clone()));
    router.layer(axum::middleware::from_fn(move |req, next| {
        let ctx = ctx.clone();
        async move { crate::forward::route(&ctx.0, &ctx.1, req, next).await }
    }))
}

/// Graceful shutdown: hand every shard back (drain, checkpoint, release) and
/// drop the node lease, so successors take over immediately instead of
/// waiting out the lease TTL.
pub async fn shutdown(app: &Arc<xrpc::App>) {
    if let Some(c) = &app.cluster {
        let host: Arc<dyn ShardHost> = app.node.clone();
        tracing::info!(shards = c.owned().len(), "graceful shutdown: releasing shards");
        if let Err(e) = c.shutdown(&host).await {
            // Our lease stays (renewals stopped): peers presume us dead and
            // fence our log, as does our own restart (it reads our lease).
            tracing::error!("{e:#}: exiting nonzero without dropping our lease (peers or our restart fence the log)");
            crate::lifecycle::fail_stop(SHUTDOWN_FENCE_EXIT_CODE, "shutdown_fence");
        }
    }
}

/// Exit code of a graceful shutdown that could not fence its own log.
pub const SHUTDOWN_FENCE_EXIT_CODE: i32 = 8;

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
            kek: crate::secrets::KekConfig { local: Some(crate::secrets::KekBytes::random()), ..Default::default() },
            plc: crate::plc::PlcConfig {
                rotation_key: Some(crate::plc::RotationKey::Key(Arc::new(crate::crypto::Keypair::generate()))),
                ..Default::default()
            },
            ..Config::default()
        };
        let (a, b, c) = ("a".repeat(32), "b".repeat(32), "c".repeat(32));
        prod(&a, &b, &c).check_secrets().expect("strong distinct secrets");
        // a PLC rotation key is required (DIDs are registered), and the
        // unregistered dev mode is refused
        let e = Config { plc: Default::default(), ..prod(&a, &b, &c) }.check_secrets().unwrap_err();
        assert!(e.to_string().contains("PLC rotation key"), "{e}");
        let unreg = crate::plc::PlcConfig { mode: crate::plc::PlcMode::Unregistered, ..prod(&a, &b, &c).plc };
        assert!(Config { plc: unreg, ..prod(&a, &b, &c) }.check_secrets().is_err());
        // a KEK is required, and not the dev one
        let e = Config { kek: Default::default(), ..prod(&a, &b, &c) }.check_secrets().unwrap_err();
        assert!(e.to_string().contains("key-encryption key"), "{e}");
        let dev_kek = crate::secrets::KekConfig { local: Some(crate::secrets::dev_kek()), ..Default::default() };
        assert!(Config { kek: dev_kek, ..prod(&a, &b, &c) }.check_secrets().is_err());
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

    /// `metrics_listen` moves /metrics and /debug/pprof off the app port.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metrics_listen_splits_routes() {
        use tower::ServiceExt;
        let status = |r: axum::Router, path: &'static str| async move {
            let req = axum::http::Request::get(path)
                .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 1))))
                .body(axum::body::Body::empty())
                .unwrap();
            r.oneshot(req).await.unwrap().status().as_u16()
        };
        let app = build(Config::default()).await.unwrap();
        assert_eq!(status(router(&app), "/metrics").await, 200, "default: on the app port");
        let app = build(Config { metrics_listen: Some("127.0.0.1:0".into()), ..Config::default() }).await.unwrap();
        assert_eq!(status(router(&app), "/metrics").await, 404);
        assert_eq!(status(router(&app), "/debug/pprof/profile").await, 404);
        assert_eq!(status(router(&app), "/xrpc/_health").await, 200);
        assert_eq!(status(metrics_router(&app), "/metrics").await, 200);
        assert_ne!(status(metrics_router(&app), "/debug/pprof/profile").await, 404);
    }
}
