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
    /// Repo worker threads (MST + signing).
    #[arg(long, env = "VLPDS_WORKERS", default_value_t = 8)]
    workers: usize,
    /// Tokio threads (HTTP, sequencers, storage IO).
    #[arg(long, env = "VLPDS_IO_THREADS", default_value_t = 6)]
    io_threads: usize,
    /// Cached repos per worker.
    #[arg(long, default_value_t = 50_000)]
    cache_per_worker: usize,
    #[arg(long, default_value_t = 8)]
    max_segment_mb: usize,
    #[arg(long, default_value_t = 512)]
    firehose_ring_mb: usize,
    /// Start a second, identical segment PUT if the first takes longer than this.
    #[arg(long, env = "VLPDS_HEDGE_AFTER_MS", default_value_t = 100)]
    hedge_after_ms: u64,
    /// Max write requests in flight before shedding with 503.
    #[arg(long, env = "VLPDS_MAX_INFLIGHT_WRITES", default_value_t = 20000)]
    max_inflight_writes: usize,
    /// Local disk cache for SlateDB SSTs (empty = disabled).
    #[arg(long, env = "VLPDS_CACHE_DIR", default_value = "")]
    cache_dir: String,
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
    #[arg(long, env = "VLPDS_LEASE_TTL_MS", default_value_t = 10_000)]
    lease_ttl_ms: u64,
    /// Disable the reference rate limits (benchmarks / load tests).
    #[arg(long, env = "VLPDS_NO_RATE_LIMITS")]
    no_rate_limits: bool,
    /// Proxies (IPs or CIDRs, comma-separated) whose X-Forwarded-For is
    /// trusted for the rate-limit client IP. In cluster mode list the nodes.
    #[arg(long, env = "VLPDS_TRUSTED_PROXIES", value_delimiter = ',')]
    trusted_proxies: Vec<String>,
    /// `x-ratelimit-bypass` header value that skips rate limits.
    #[arg(long, env = "VLPDS_RATE_LIMIT_BYPASS_KEY")]
    rate_limit_bypass_key: Option<String>,
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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.io_threads)
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
        workers: args.workers,
        cache_per_worker: args.cache_per_worker,
        max_segment_bytes: args.max_segment_mb << 20,
        firehose_ring_bytes: args.firehose_ring_mb << 20,
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
        rate_limit_bypass_key: args.rate_limit_bypass_key.clone(),
        cluster: Some(vlpds::cluster::ClusterConfig {
            node_id: args.node_id.clone().unwrap_or_else(|| "single".into()),
            addr: args.advertise_url.clone().unwrap_or_else(|| args.public_url.clone()),
            shards: args.shards,
            ttl: Duration::from_millis(args.lease_ttl_ms),
            renew_every: Duration::from_millis(args.lease_ttl_ms / 5),
            skew: Duration::from_millis(args.lease_ttl_ms / 5),
        }),
        memory_store: None,
    };
    cfg.check_secrets()?;
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    let app = server::build(cfg).await?;
    server::spawn_reporters(&app);
    tracing::info!(listen = %args.listen, "vlpds serving");
    // background services
    vlpds::xrpc::spawn_blob_gc(app.clone());
    vlpds::xrpc::spawn_reserved_key_gc(app.clone());
    vlpds::oauth::gc::spawn_gc(app.clone());
    tokio::spawn(vlpds::xrpc::request_crawl(app.clone()));
    let router = server::with_forwarding(&app, vlpds::xrpc::router(app.clone()));
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
