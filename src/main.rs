use clap::Parser;
use std::time::Duration;
use vlpds::server::{self, Config};
use vlpds::store::S3Config;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(about = "vlpds: a very large atproto PDS on object storage")]
struct Args {
    #[arg(long, env = "VLPDS_LISTEN", default_value = "0.0.0.0:2583")]
    listen: String,
    /// Serve /metrics and /debug/pprof on this address only (e.g.
    /// 127.0.0.1:9583), not on --listen. Unset = on the app port.
    #[arg(long, env = "VLPDS_METRICS_LISTEN")]
    metrics_listen: Option<String>,
    #[arg(
        long,
        env = "VLPDS_PUBLIC_URL",
        default_value = "http://localhost:2583"
    )]
    public_url: String,
    #[arg(long, env = "VLPDS_HANDLE_DOMAIN", default_value = "vlpds.test")]
    handle_domain: String,
    #[arg(long, env = "VLPDS_SERVICE_DID", default_value = "did:web:localhost")]
    service_did: String,
    /// Session JWT / OAuth key-derivation secret (>= 32 bytes; dev default
    /// only with --dev-mode).
    #[arg(long, env = "VLPDS_JWT_SECRET", hide_env_values = true)]
    jwt_secret: Option<String>,
    /// Admin Basic-auth token (>= 32 bytes; dev default only with --dev-mode).
    #[arg(long, env = "VLPDS_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: Option<String>,
    /// Node-to-node token (`x-vlpds-internal`), the same on every node of a
    /// cluster (>= 32 bytes, distinct from the admin token; dev default only
    /// with --dev-mode).
    #[arg(long, env = "VLPDS_INTERNAL_TOKEN", hide_env_values = true)]
    internal_token: Option<String>,

    #[arg(
        long,
        env = "VLPDS_S3_ENDPOINT",
        default_value = "http://localhost:9000"
    )]
    s3_endpoint: String,
    #[arg(long, env = "VLPDS_S3_BUCKET", default_value = "vlpds")]
    s3_bucket: String,
    #[arg(long, env = "VLPDS_S3_ACCESS_KEY", default_value = "minioadmin")]
    s3_access_key: String,
    #[arg(long, env = "VLPDS_S3_SECRET_KEY", default_value = "minioadmin")]
    s3_secret_key: String,
    #[arg(long, env = "VLPDS_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    /// Key prefix inside the bucket (one prefix = one PDS).
    #[arg(long, env = "VLPDS_PREFIX", default_value = "vlpds")]
    prefix: String,
    /// Use an in-memory object store (tests/dev only; nothing persists).
    #[arg(long)]
    memory: bool,

    /// Injected median latency (ms) on segment PUTs, to emulate S3 over local MinIO.
    #[arg(long, env = "VLPDS_INJECT_PUT_MS")]
    inject_put_ms: Option<f64>,
    /// Lognormal sigma for injected latency (0.5 ≈ p99 at 3.2× median).
    #[arg(long, default_value_t = 0.5)]
    inject_sigma: f64,

    /// Number of shards (hash-slot ranges); fixed for the lifetime of a bucket prefix.
    #[arg(long, env = "VLPDS_SHARDS", default_value_t = 256)]
    shards: u16,
    /// Repo worker threads (MST + signing). Default: half the available
    /// cores (min 1); the request runtime does the rest of the CPU work.
    #[arg(long, env = "VLPDS_WORKERS")]
    workers: Option<usize>,
    /// Tokio threads (HTTP, JSON/CBOR, sequencers, storage IO). Default: the
    /// available cores (cgroup/affinity aware); fewer pinned the proxy-heavy
    /// benchbox bench at 600% CPU with 6 (16 threads: 155k -> 204k req/s).
    #[arg(long, env = "VLPDS_IO_THREADS")]
    io_threads: Option<usize>,
    /// Cached repos per worker.
    #[arg(long, default_value_t = 50_000)]
    cache_per_worker: usize,
    /// Segment size cap (MiB; fractions allow small segments in HA tests, so
    /// K PUTs are in flight at modest load).
    #[arg(long, default_value_t = 8.0)]
    max_segment_mb: f64,
    /// Segment PUTs in flight per node log (finalized in ordinal order).
    #[arg(long, env = "VLPDS_LOG_INFLIGHT", default_value_t = vlpds::nodelog::DEFAULT_LOG_INFLIGHT)]
    log_inflight: usize,
    /// Byte budget of the node log's live ring of sealed segments (MiB); a
    /// peer follower that falls behind it catches up from S3.
    #[arg(long, env = "VLPDS_LIVE_RING_MB", default_value_t = 128.0)]
    live_ring_mb: f64,
    #[arg(long, default_value_t = 512)]
    firehose_ring_mb: usize,
    /// Byte budget of the firehose merger's queues (MiB) before it spills a
    /// log to S3 read-back (small values exercise spills in HA tests).
    #[arg(long, env = "VLPDS_FIREHOSE_MERGE_QUEUE_MB", default_value_t = 256.0)]
    firehose_merge_queue_mb: f64,
    /// Threads serving subscribeRepos connections, apart from the request
    /// runtime (0 = share it).
    #[arg(long, env = "VLPDS_FIREHOSE_THREADS", default_value_t = 4)]
    firehose_threads: usize,
    /// A live subscriber this far behind the stream head gets ConsumerTooSlow (MiB).
    #[arg(long, env = "VLPDS_FIREHOSE_MAX_LAG_MB", default_value_t = 128)]
    firehose_max_lag_mb: usize,
    /// Cursor backfill: segment read-ahead per subscriber (MiB, all logs together).
    #[arg(long, env = "VLPDS_BACKFILL_READAHEAD_MB", default_value_t = 64)]
    backfill_readahead_mb: usize,
    /// Cursor backfill: segment cache shared by subscribers replaying the same range (MiB).
    #[arg(long, env = "VLPDS_BACKFILL_CACHE_MB", default_value_t = 256)]
    backfill_cache_mb: usize,
    /// Start a second, identical segment PUT if the first takes longer than this.
    #[arg(long, env = "VLPDS_HEDGE_AFTER_MS", default_value_t = 100)]
    hedge_after_ms: u64,
    /// Max write requests in flight before shedding with 503.
    #[arg(long, env = "VLPDS_MAX_INFLIGHT_WRITES", default_value_t = 20000)]
    max_inflight_writes: usize,
    /// Local disk cache for SlateDB SSTs (empty = disabled).
    #[arg(long, env = "VLPDS_CACHE_DIR", default_value = "")]
    cache_dir: String,
    /// In-memory SST block cache shared by every shard DB on this node (MiB;
    /// the meta/index cache gets a quarter of this on top).
    #[arg(long, env = "VLPDS_BLOCK_CACHE_MB", default_value_t = 4096)]
    block_cache_mb: u64,
    /// SlateDB SST block compression: none, lz4 or zstd.
    #[arg(long, env = "VLPDS_SST_COMPRESSION", default_value = "zstd")]
    sst_compression: String,
    /// SlateDB GC: compacted SSTs no longer in the manifest are deleted once
    /// this old (e.g. 24h, 30m). Long scans (a 10M-record getRepo) may still
    /// be reading them.
    #[arg(long, env = "VLPDS_SLATEDB_GC_MIN_AGE", default_value = "24h")]
    slatedb_gc_min_age: String,
    /// Log segment retention: the firehose backfill window (e.g. 72h, 30m;
    /// "off" keeps every segment). Older segments no replay can need are
    /// deleted; older cursors get OutdatedCursor.
    #[arg(long, env = "VLPDS_LOG_RETENTION", default_value = "72h")]
    log_retention: String,
    /// Default AppView for proxied requests: "<url>,<service did>".
    #[arg(long, env = "VLPDS_APPVIEW")]
    appview: Option<String>,
    /// Moderation service for createReport: "<url>,<service did>".
    #[arg(long, env = "VLPDS_REPORT_SERVICE")]
    report_service: Option<String>,
    /// Relays to send requestCrawl to at startup (comma-separated hostnames/urls).
    #[arg(long, env = "VLPDS_CRAWLERS", value_delimiter = ',')]
    crawlers: Vec<String>,
    /// Dev mode: email/password tokens are logged instead of mailed, and the
    /// well-known dev secrets are accepted.
    #[arg(long, env = "VLPDS_DEV_MODE")]
    dev_mode: bool,
    /// Max uploadBlob size (MB).
    #[arg(long, env = "VLPDS_MAX_BLOB_MB", default_value_t = 100)]
    max_blob_mb: u64,
    /// Delete unreferenced blobs uploaded more than this many seconds ago.
    #[arg(long, env = "VLPDS_BLOB_GC_GRACE_SECS", default_value_t = 6 * 3600)]
    blob_gc_grace_secs: u64,
    /// PLC directory for did:plc resolution.
    #[arg(long, env = "VLPDS_PLC_URL", default_value = "https://plc.directory")]
    plc_url: String,
    /// Require an invite code for createAccount.
    #[arg(long, env = "VLPDS_INVITE_REQUIRED")]
    invite_required: bool,
    /// Resolve the lexicons of record types without a bundled schema (DNS
    /// `_lexicon` TXT -> DID -> com.atproto.lexicon.schema record) and
    /// validate those records too, instead of reporting them "unknown".
    #[arg(long, env = "VLPDS_RESOLVE_LEXICONS")]
    resolve_lexicons: bool,
    /// Accepted for script compatibility; every node runs the cluster protocol
    /// (a lone node is a one-node cluster).
    #[arg(long, env = "VLPDS_CLUSTER")]
    cluster: bool,
    /// Stable node id (keep it across restarts so a restarted node reclaims
    /// its shards immediately). Default "single".
    #[arg(long, env = "VLPDS_NODE_ID")]
    node_id: Option<String>,
    /// URL peers use to reach this node (default: public_url).
    #[arg(long, env = "VLPDS_ADVERTISE_URL")]
    advertise_url: Option<String>,
    /// Node lease TTL. Renewal and skew margin are TTL/5 each. A node's
    /// renewal (one CAS PUT) must complete within (TTL - skew)/2 = 0.4 x TTL
    /// (4 s at the default) or its validity gaps and it fail-stops; a
    /// cluster-wide S3 brownout beyond that stops every node. Keep >= 10 s in
    /// production; takeover after a crash is about TTL + skew + replay.
    #[arg(long, env = "VLPDS_LEASE_TTL_MS", default_value_t = 10_000)]
    lease_ttl_ms: u64,
    /// Disable the reference rate limits (benchmarks / load tests).
    #[arg(long, env = "VLPDS_NO_RATE_LIMITS")]
    no_rate_limits: bool,
    /// Proxies (IPs or CIDRs, comma-separated) whose X-Forwarded-For is
    /// trusted for the rate-limit client IP. In cluster mode list the nodes.
    #[arg(long, env = "VLPDS_TRUSTED_PROXIES", value_delimiter = ',')]
    trusted_proxies: Vec<String>,
    /// HTTP/2 (h2c) connections to each peer node; requests round-robin.
    #[arg(long, env = "VLPDS_PEER_CONNECTIONS", default_value_t = vlpds::http::DEFAULT_PEER_CONNECTIONS)]
    peer_connections: usize,
    /// `x-ratelimit-bypass` header value that skips rate limits.
    #[arg(long, env = "VLPDS_RATE_LIMIT_BYPASS_KEY")]
    rate_limit_bypass_key: Option<String>,
    /// Push continuous CPU profiles (100 Hz) to this Pyroscope server, tagged
    /// with the node id and git revision (needs `--features profiling`;
    /// bench/obs runs one on http://127.0.0.1:4040).
    #[arg(long, env = "VLPDS_PYROSCOPE_URL")]
    pyroscope_url: Option<String>,
}

fn url_did(v: &Option<String>) -> anyhow::Result<Option<(String, String)>> {
    v.as_ref()
        .map(|s| {
            let (u, d) = s
                .split_once(',')
                .ok_or_else(|| anyhow::anyhow!("expected <url>,<did>: {s}"))?;
            Ok((u.to_string(), d.to_string()))
        })
        .transpose()
}

/// Fractional MiB to bytes (at least `min`).
fn mib(v: f64, min: usize) -> usize {
    ((v * (1u64 << 20) as f64) as usize).max(min)
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get())
}

fn default_workers() -> usize {
    (cores() / 2).max(1)
}

/// Raises the soft open-files limit to the hard limit: every client, peer and
/// S3 connection is a descriptor, and Linux's default soft limit of 1024
/// broke benchbox runs. (macOS caps it at kern.maxfilesperproc.) No libc
/// dependency, as in metrics.rs: rlim_t is 64-bit on both targets.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn raise_nofile_limit() {
    #[repr(C)]
    struct Rlimit {
        cur: u64,
        max: u64,
    }
    extern "C" {
        fn getrlimit(resource: i32, rlim: *mut Rlimit) -> i32;
        fn setrlimit(resource: i32, rlim: *const Rlimit) -> i32;
    }
    #[cfg(target_os = "linux")]
    const RLIMIT_NOFILE: i32 = 7;
    #[cfg(target_os = "macos")]
    const RLIMIT_NOFILE: i32 = 8;
    let mut r = Rlimit { cur: 0, max: 0 };
    if unsafe { getrlimit(RLIMIT_NOFILE, &mut r) } != 0 {
        tracing::warn!("getrlimit(RLIMIT_NOFILE) failed: {}", std::io::Error::last_os_error());
        return;
    }
    let mut want = r.max;
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn sysctlbyname(name: *const std::ffi::c_char, old: *mut std::ffi::c_void, oldlen: *mut usize, new: *mut std::ffi::c_void, newlen: usize) -> i32;
        }
        let (mut v, mut len) = (0i32, std::mem::size_of::<i32>());
        let name = c"kern.maxfilesperproc";
        if unsafe { sysctlbyname(name.as_ptr(), &mut v as *mut i32 as *mut _, &mut len, std::ptr::null_mut(), 0) } == 0 && v > 0 {
            want = want.min(v as u64);
        }
    }
    if want <= r.cur {
        tracing::info!(soft = r.cur, hard = r.max, "open-files limit (RLIMIT_NOFILE)");
        return;
    }
    let from = r.cur;
    r.cur = want;
    if unsafe { setrlimit(RLIMIT_NOFILE, &r) } != 0 {
        tracing::warn!(soft = from, want, "raising RLIMIT_NOFILE failed: {}", std::io::Error::last_os_error());
    } else {
        tracing::info!(from, to = want, hard = r.max, "raised open-files soft limit (RLIMIT_NOFILE)");
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raise_nofile_limit() {}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();
    let args = Args::parse();
    raise_nofile_limit();
    let node_id = args.node_id.clone().unwrap_or_else(|| "single".into());
    let rev = vlpds::profiling::git_rev();
    vlpds::metrics::BUILD_INFO
        .with_label_values(&[node_id.as_str(), rev.as_str(), if vlpds::profiling::ENABLED { "1" } else { "0" }])
        .set(1);
    if let Some(url) = &args.pyroscope_url {
        // before the runtime: the agent's blocking HTTP client owns one
        vlpds::profiling::start_pyroscope(url, &node_id, &rev)?;
        tracing::info!(url, node_id, rev, "pushing CPU profiles to Pyroscope");
    }
    let io_threads = args.io_threads.unwrap_or_else(cores).max(1);
    tracing::info!(io_threads, workers = args.workers.unwrap_or_else(default_workers), cores = cores(), "threads");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(io_threads)
        .enable_all()
        .build()?;
    rt.block_on(run(args))
}

/// A secret flag's value; the well-known dev default only in dev mode (an
/// unset secret outside dev mode is refused by `Config::check_secrets`).
fn secret(v: &Option<String>, dev_mode: bool, dev_default: &str) -> String {
    match v {
        Some(v) => v.clone(),
        None if dev_mode => dev_default.to_string(),
        None => String::new(),
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    vlpds::partition::set_block_cache_bytes(args.block_cache_mb << 20);
    vlpds::partition::set_sst_compression(args.sst_compression.parse()?);
    vlpds::partition::set_gc_min_age(vlpds::retention::parse_duration(&args.slatedb_gc_min_age)?);
    let log_retention = match args.log_retention.as_str() {
        "off" | "none" => None,
        v => Some(vlpds::retention::Config { window: vlpds::retention::parse_duration(v)?, ..Default::default() }),
    };
    let cfg = Config {
        public_url: args.public_url.clone(),
        handle_domain: args.handle_domain.clone(),
        service_did: args.service_did.clone(),
        jwt_secret: secret(&args.jwt_secret, args.dev_mode, server::DEV_JWT_SECRET),
        admin_token: secret(&args.admin_token, args.dev_mode, server::DEV_ADMIN_TOKEN),
        internal_token: secret(&args.internal_token, args.dev_mode, server::DEV_INTERNAL_TOKEN),
        s3: (!args.memory).then(|| S3Config {
            endpoint: args.s3_endpoint.clone(),
            bucket: args.s3_bucket.clone(),
            access_key: args.s3_access_key.clone(),
            secret_key: args.s3_secret_key.clone(),
            region: args.s3_region.clone(),
        }),
        prefix: args.prefix.clone(),
        inject_latency: args.inject_put_ms.map(|m| (m, args.inject_sigma)),
        shards: args.shards,
        workers: args.workers.unwrap_or_else(default_workers).max(1),
        cache_per_worker: args.cache_per_worker,
        max_segment_bytes: mib(args.max_segment_mb, 4096),
        log_inflight: args.log_inflight.max(1),
        live_ring_bytes: mib(args.live_ring_mb, 1),
        firehose_merge_queue_bytes: mib(args.firehose_merge_queue_mb, 1),
        firehose_ring_bytes: args.firehose_ring_mb << 20,
        firehose_threads: args.firehose_threads,
        firehose_max_lag_bytes: args.firehose_max_lag_mb << 20,
        backfill_readahead_bytes: args.backfill_readahead_mb << 20,
        backfill_cache_bytes: args.backfill_cache_mb << 20,
        hedge_after: Duration::from_millis(args.hedge_after_ms),
        max_inflight_writes: args.max_inflight_writes,
        cache_dir: (!args.cache_dir.is_empty()).then(|| std::path::PathBuf::from(&args.cache_dir)),
        appview: url_did(&args.appview)?,
        report_service: url_did(&args.report_service)?,
        crawlers: args.crawlers.clone(),
        dev_mode: args.dev_mode,
        max_blob_size: args.max_blob_mb << 20,
        blob_gc_grace: Duration::from_secs(args.blob_gc_grace_secs),
        plc_url: args.plc_url.clone(),
        invite_required: args.invite_required,
        rate_limits_enabled: !args.no_rate_limits,
        resolve_lexicons: args
            .resolve_lexicons
            .then_some(vlpds::lexicon::RESOLVE_TIMEOUT),
        trusted_proxies: args.trusted_proxies.clone(),
        peer_connections: args.peer_connections,
        rate_limit_bypass_key: args.rate_limit_bypass_key.clone(),
        cluster: Some(vlpds::cluster::ClusterConfig {
            node_id: args.node_id.clone().unwrap_or_else(|| "single".into()),
            addr: args.advertise_url.clone().unwrap_or_else(|| args.public_url.clone()),
            shards: args.shards,
            ttl: Duration::from_millis(args.lease_ttl_ms),
            renew_every: Duration::from_millis(args.lease_ttl_ms / 5),
            skew: Duration::from_millis(args.lease_ttl_ms / 5),
            clock_offset_ms: 0,
        }),
        memory_store: None,
        metrics_listen: args.metrics_listen.clone(),
        log_retention,
    };
    cfg.check_secrets()?;
    if args.lease_ttl_ms < 10_000 && !args.dev_mode {
        tracing::warn!(
            lease_ttl_ms = args.lease_ttl_ms,
            "lease TTL below 10 s: a renewal slower than 0.4 x TTL fail-stops the node (see --lease-ttl-ms)"
        );
    }
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    let metrics_listener = match &args.metrics_listen {
        Some(a) => Some(tokio::net::TcpListener::bind(a).await?),
        None => None,
    };
    let app = server::build(cfg).await?;
    server::spawn_reporters(&app);
    tracing::info!(listen = %args.listen, metrics_listen = args.metrics_listen.as_deref().unwrap_or("(app port)"), "vlpds serving");
    if let Some(l) = metrics_listener {
        let r = server::metrics_router(&app);
        tokio::spawn(async move {
            if let Err(e) = server::serve(l, r).await {
                tracing::error!("metrics server exited: {e:#}");
            }
        });
    }
    // background services
    vlpds::xrpc::spawn_blob_gc(app.clone());
    vlpds::xrpc::spawn_reserved_key_gc(app.clone());
    vlpds::oauth::gc::spawn_gc(app.clone());
    tokio::spawn(vlpds::xrpc::request_crawl(app.clone()));
    let router = server::router(&app);
    tokio::select! {
        r = server::serve(listener, router) => r,
        _ = shutdown_signal() => {
            server::shutdown(&app).await;
            tracing::info!("shutdown complete");
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
