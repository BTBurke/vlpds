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

/// No Debug: it holds secrets (tokens, S3/SMTP credentials, KEK config).
#[derive(Clone)]
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
    /// Object-store requests in flight on the state client (SlateDB, blobs,
    /// account indexes; `--store-inflight`). See `objlimit`.
    pub store_inflight: usize,
    /// ... on the log client's reads (replay, backfill, followers,
    /// retention; `--log-store-inflight`); its segment PUTs have their own
    /// lane (`objlimit::log_write_permits`).
    pub log_store_inflight: usize,
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
    /// Cursor backfills running at once (more wait their turn): with
    /// `backfill_readahead_bytes` each, the process-wide read-ahead bound.
    pub firehose_max_backfills: usize,
    /// subscribeRepos connections per client IP (IPv6: per /64); 0 = no cap.
    pub firehose_max_per_ip: usize,
    pub hedge_after: Duration,
    pub max_inflight_writes: usize,
    /// Reads queued at the repo workers (repo views for getRepo, getRecord,
    /// getBlocks, ...) before shedding with 503: each holds its slot until
    /// its worker answers, so a slow repo load can't queue them unbounded.
    pub max_queued_reads: usize,
    /// getRepo / getCheckout exports streaming at once; more wait up to 10 s
    /// for a slot, then get 503.
    pub max_exports: usize,
    /// An export whose client reads nothing for this long is ended.
    pub export_stall: Duration,
    /// Connections open at once per listener (0 = no cap).
    pub max_connections: usize,
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
    /// vlpds.admin.bulkCreate (synthetic benchmark accounts) outside dev
    /// mode (`--allow-bulk-create`).
    pub allow_bulk_create: bool,
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
    /// Period of the account / repo / disk-cache count behind the operator
    /// dashboard's totals (`xrpc::spawn_account_stats`); zero = off.
    pub account_stats_interval: Duration,
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
    /// rate-limit client IP. Empty = always the TCP peer (or, on a request a
    /// peer forwarded, the client address it vouched for).
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
    /// Largest CAR importRepo accepts (reference PDS_MAX_REPO_IMPORT_SIZE).
    /// The CAR is parsed in memory and the import is one log entry, so this
    /// bounds both (DESIGN.md "Partial MSTs", untrusted block sets).
    pub max_import_bytes: usize,
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
/// The MinIO default S3 access/secret key (the CLI default): refused
/// outside dev mode.
pub const DEV_S3_CREDENTIAL: &str = "minioadmin";
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
        if let (Some(s3), false) = (&self.s3, self.dev_mode) {
            anyhow::ensure!(
                s3.access_key != DEV_S3_CREDENTIAL && s3.secret_key != DEV_S3_CREDENTIAL,
                "VLPDS_S3_ACCESS_KEY / VLPDS_S3_SECRET_KEY are the MinIO defaults ({DEV_S3_CREDENTIAL}); set real credentials (or run with --dev-mode)"
            );
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
            store_inflight: crate::objlimit::DEFAULT_STATE_INFLIGHT,
            log_store_inflight: crate::objlimit::DEFAULT_LOG_INFLIGHT,
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
            firehose_max_backfills: crate::firehose::DEFAULT_MAX_BACKFILLS,
            firehose_max_per_ip: crate::firehose::DEFAULT_MAX_PER_IP,
            hedge_after: Duration::from_millis(100),
            max_inflight_writes: 20_000,
            max_queued_reads: 20_000,
            max_exports: crate::xrpc::DEFAULT_MAX_EXPORTS,
            export_stall: crate::xrpc::DEFAULT_EXPORT_STALL,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            cache_dir: None,
            disk_cache_bytes: None,
            disk_cache_shard_bytes: None,
            appview: None,
            report_service: None,
            appview_cdn_url_pattern: None,
            crawlers: Vec::new(),
            dev_mode: true,
            allow_bulk_create: false,
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
            account_stats_interval: Duration::ZERO,
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
            max_import_bytes: crate::xrpc::DEFAULT_MAX_IMPORT_BYTES,
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
    // Separate clients (connection pools) for the commit log, the control
    // plane, and everything else, each with bounded requests in flight
    // (objlimit.rs): a takeover's burst queues for permits instead of
    // opening a connection per request (it once took the host's every
    // ephemeral port, and the lease renewals failed with the rest).
    use crate::objlimit::{Limits, Reserve};
    let log_limits = Limits::new(cfg.log_store_inflight).with_reserved(Reserve::Writes, crate::objlimit::log_write_permits(cfg.log_inflight));
    let state_limits = Limits::new(cfg.store_inflight);
    let ctl_limits = Limits::new(crate::objlimit::CTL_PERMITS).with_reserved(Reserve::LeaseWrites, crate::objlimit::LEASE_PERMITS);
    let (store, state_store, ctl_store) = match &cfg.s3 {
        None => {
            let m = match &cfg.memory_store {
                Some(raw) => Store { raw: raw.clone(), ..Store::memory(cfg.inject_latency) },
                None => Store::memory(cfg.inject_latency),
            };
            let plain = Store { latency: None, ..m.clone() };
            (m.counted("log"), plain.clone().counted("state"), plain.counted("ctl"))
        }
        Some(s3) => (
            Store::s3(s3, &cfg.prefix, cfg.inject_latency, log_limits.connections())?.counted("log"),
            Store::s3(s3, &cfg.prefix, None, state_limits.connections())?.counted("state"),
            Store::s3(s3, &cfg.prefix, None, ctl_limits.connections())?.counted("ctl"),
        ),
    };
    let (store, state_store, ctl_store) = (store.limited("log", log_limits), state_store.limited("state", state_limits), ctl_store.limited("ctl", ctl_limits));
    let firehose = Firehose::new(crate::firehose::Options {
        ring_bytes: cfg.firehose_ring_bytes,
        max_lag_bytes: cfg.firehose_max_lag_bytes,
        readahead_bytes: cfg.backfill_readahead_bytes,
        backfill_cache_bytes: cfg.backfill_cache_bytes,
        max_backfills: cfg.firehose_max_backfills,
        max_per_ip: cfg.firehose_max_per_ip,
        write_idle: crate::firehose::DEFAULT_WRITE_IDLE,
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
    // before the join, which may fence our own previous incarnation's log
    crate::metrics::init_counters();
    let started = std::time::Instant::now();
    let cluster = Cluster::join(cc, ctl_store).await?;
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
        write_permits: Arc::new(tokio::sync::Semaphore::new(cfg.max_inflight_writes)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(cfg.max_queued_reads.max(1))),
        exports: Arc::new(tokio::sync::Semaphore::new(cfg.max_exports.max(1))),
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
    let opts = ServeOptions { max_connections: app.config.max_connections, ..Default::default() };
    tokio::spawn(async move {
        if let Err(e) = serve_with(listener, router, opts).await {
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

/// HTTP/2 settings profile of a listener (DESIGN.md "HTTP").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum H2Profile {
    /// Node-to-node forwarding (and a `--listen` that peers also use):
    /// 4 MiB stream / 64 MiB connection windows, 1,024 streams.
    Peer,
    /// Clients only (`--listen` once `--peer-listen` takes the peers): 1 MiB
    /// stream / 8 MiB connection windows, 256 streams. What one connection
    /// can make the server buffer, and how many requests it can start at
    /// once, scale with these.
    Public,
}

/// How a listener is served.
#[derive(Clone, Copy, Debug)]
pub struct ServeOptions {
    pub h2: H2Profile,
    /// Most connections open at once; at the cap the listener stops
    /// accepting (new connections wait in the kernel's accept queue) until
    /// one closes. 0 = no cap.
    pub max_connections: usize,
}

impl Default for ServeOptions {
    fn default() -> Self {
        ServeOptions { h2: H2Profile::Peer, max_connections: DEFAULT_MAX_CONNECTIONS }
    }
}

/// Default `--max-connections` (per listener).
pub const DEFAULT_MAX_CONNECTIONS: usize = 50_000;

/// What `serve` accepts connections from: a TCP listener (tests inject
/// failing ones).
pub trait Accept: Send + 'static {
    fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>;
}

impl Accept for tokio::net::TcpListener {
    fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>> {
        tokio::net::TcpListener::poll_accept(self, cx)
    }
}

/// [`serve_with`] with the peer profile and the default connection cap.
pub async fn serve(listener: tokio::net::TcpListener, router: axum::Router) -> anyhow::Result<()> {
    serve_with(listener, router, ServeOptions::default()).await
}

/// HTTP/1.1 + HTTP/2 (h2c) server. axum::serve doesn't expose HTTP/2 settings,
/// and hyper's default 64KB connection window chops request bodies on busy
/// connections into tiny DATA frames, which trips h2's small-frame flood guard.
/// Settings and their reasons: DESIGN.md "HTTP".
///
/// Runs until the process ends: an accept error (EMFILE, ENFILE, ENOBUFS,
/// a connection reset while queued) is logged and retried after a short
/// pause, as axum::serve does, instead of ending the server (and with it
/// the process, without a graceful handoff).
pub async fn serve_with<A: Accept>(mut listener: A, router: axum::Router, opts: ServeOptions) -> anyhow::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;
    let (stream_window, conn_window, streams) = match opts.h2 {
        H2Profile::Peer => (4 << 20, 64 << 20, 1024),
        H2Profile::Public => (1 << 20, 8 << 20, 256),
    };
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
        .initial_stream_window_size(stream_window)
        .initial_connection_window_size(conn_window)
        .max_frame_size(256 << 10)
        // per connection; the load generator spreads its requests over 64
        // connections, peers over --peer-connections
        .max_concurrent_streams(streams)
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
    let slots = (opts.max_connections > 0).then(|| Arc::new(tokio::sync::Semaphore::new(opts.max_connections)));
    loop {
        // a slot first: at the cap, connections wait in the accept queue
        let slot = match &slots {
            Some(s) => Some(s.clone().acquire_owned().await.expect("never closed")),
            None => None,
        };
        let (sock, peer) = match std::future::poll_fn(|cx| listener.poll_accept(cx)).await {
            Ok(c) => c,
            Err(e) => {
                crate::metrics::HTTP_SERVER_ACCEPT_ERRORS.inc();
                tracing::warn!("accept failed (retrying): {e}");
                // EMFILE and friends last until something closes: don't spin
                tokio::time::sleep(ACCEPT_ERROR_PAUSE).await;
                continue;
            }
        };
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
            // an upgraded connection (subscribeRepos) lives on past this
            // without a slot: the firehose caps its subscribers itself
            drop(slot);
        });
    }
}

/// Pause after a failed accept.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(50);

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
        // the MinIO default S3 credentials, and their Debug is redacted
        let s3 = |k: &str| crate::store::S3Config {
            endpoint: "http://s3".into(),
            bucket: "b".into(),
            access_key: k.into(),
            secret_key: format!("{k}-secret"),
            region: "r".into(),
        };
        prod(&a, &b, &c).check_secrets().expect("no S3 (memory store)");
        Config { s3: Some(s3("AKIAREAL")), ..prod(&a, &b, &c) }.check_secrets().expect("real S3 credentials");
        let e = Config { s3: Some(s3(DEV_S3_CREDENTIAL)), ..prod(&a, &b, &c) }.check_secrets().unwrap_err();
        assert!(e.to_string().contains("MinIO defaults"), "{e}");
        Config { s3: Some(s3(DEV_S3_CREDENTIAL)), ..Config::default() }.check_secrets().expect("dev mode allows them");
        let dbg = format!("{:?}", s3("AKIAREAL"));
        assert!(!dbg.contains("AKIAREAL"), "{dbg}");
    }

    /// Accept errors (EMFILE and the like) are retried, not fatal: the
    /// server keeps serving; and at the connection cap a new connection
    /// waits until one closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accept_errors_are_retried_and_connections_capped() {
        struct Flaky {
            inner: tokio::net::TcpListener,
            fail: usize,
        }
        impl Accept for Flaky {
            fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>> {
                if self.fail > 0 {
                    self.fail -= 1;
                    return std::task::Poll::Ready(Err(std::io::Error::from_raw_os_error(24))); // EMFILE
                }
                self.inner.poll_accept(cx)
            }
        }
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let before = crate::metrics::HTTP_SERVER_ACCEPT_ERRORS.get();
        let router = axum::Router::new().route("/ok", axum::routing::get(|| async { "ok" }));
        let server = tokio::spawn(serve_with(Flaky { inner: l, fail: 3 }, router, ServeOptions { max_connections: 1, ..Default::default() }));
        let get = || async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /ok HTTP/1.1\r\nhost: x\r\n\r\n").await.unwrap();
            let mut buf = [0u8; 256];
            let n = c.read(&mut buf).await.unwrap();
            (String::from_utf8_lossy(&buf[..n]).into_owned(), c)
        };
        let (first, held) = tokio::time::timeout(Duration::from_secs(5), get()).await.expect("served after accept errors");
        assert!(first.starts_with("HTTP/1.1 200"), "{first}");
        assert!(crate::metrics::HTTP_SERVER_ACCEPT_ERRORS.get() >= before + 3);
        assert!(!server.is_finished(), "accept errors don't end the server");
        // the one slot is held by `held` (keep-alive): the next connection waits
        let second = tokio::spawn(get());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!second.is_finished(), "served past the connection cap");
        drop(held);
        let (r, _) = tokio::time::timeout(Duration::from_secs(5), second).await.expect("served once a slot freed").unwrap();
        assert!(r.starts_with("HTTP/1.1 200"), "{r}");
        server.abort();
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
