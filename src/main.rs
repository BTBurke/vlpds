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
    /// Accept queue length for --listen (and --metrics-listen). The kernel
    /// clamps it to net.core.somaxconn on Linux (kern.ipc.somaxconn on
    /// macOS): raise that too, or connect bursts (1000 firehose
    /// subscribers, proxy clients at 1024 in flight) overflow the queue
    /// (TcpExtListenOverflows) and clients wait out a SYN retransmit (1 s+).
    /// Tokio's default is 1024.
    #[arg(long, env = "VLPDS_LISTEN_BACKLOG", default_value_t = 16384)]
    listen_backlog: u32,
    /// Serve /metrics and /debug/pprof on this address only, not on
    /// --listen. Unset: 127.0.0.1:9583 (on the app port with --dev-mode,
    /// so local multi-node runs don't collide). `app` = on the app port
    /// (public: only behind a proxy that blocks /metrics).
    #[arg(long, env = "VLPDS_METRICS_LISTEN")]
    metrics_listen: Option<String>,
    /// The peer listener: node-to-node traffic (forwards, /internal, log
    /// streams) over mTLS only (--peer-tls-dir), with the large peer HTTP/2
    /// windows and stream count; --advertise-url names it to peers
    /// (https://<host>:<this port>). The three come together: set for a node
    /// with peers, unset for a lone node (no peer listener, no /internal/*,
    /// no peer calls). --listen always serves clients only (DESIGN.md
    /// "Exposure").
    #[arg(long, env = "VLPDS_PEER_LISTEN", requires_all = ["peer_tls_dir", "advertise_url"])]
    peer_listen: Option<String>,
    /// Peer mTLS directory: ca.crt (the cluster CA, PEM; several = all
    /// trusted, for a CA rotation), <node-id>.crt and <node-id>.key (what
    /// `vlpds admin tls ca|issue --out DIR` write). Re-read on SIGHUP and
    /// when the files change. With --dev-mode a node creates what's
    /// missing: the CA once (nodes sharing the directory share it) and its
    /// own certificate from ca.key.
    #[arg(long, env = "VLPDS_PEER_TLS_DIR", requires = "peer_listen")]
    peer_tls_dir: Option<std::path::PathBuf>,
    /// Connections open at once per listener; at the cap new ones wait in
    /// the accept queue (0 = no cap).
    #[arg(long, env = "VLPDS_MAX_CONNECTIONS", default_value_t = vlpds::server::DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
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
    /// S3 access key (the MinIO default is refused without --dev-mode).
    #[arg(long, env = "VLPDS_S3_ACCESS_KEY", default_value = server::DEV_S3_CREDENTIAL, hide_env_values = true, hide_default_value = true)]
    s3_access_key: String,
    /// S3 secret key (the MinIO default is refused without --dev-mode).
    #[arg(long, env = "VLPDS_S3_SECRET_KEY", default_value = server::DEV_S3_CREDENTIAL, hide_env_values = true, hide_default_value = true)]
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

    /// Shards (hash-slot ranges) of a new prefix's initial layout. Later the
    /// layout stored in the prefix wins; shards split and merge online
    /// (`vlpds admin shard-split`, `--reshard-split-mb`). At most 65,536.
    #[arg(long, env = "VLPDS_SHARDS", default_value_t = 64, value_parser = clap::value_parser!(u32).range(1..=65_536))]
    shards: u32,
    /// Split policy: split a shard whose SSTs exceed this many MiB (0 = off).
    #[arg(long, env = "VLPDS_RESHARD_SPLIT_MB", default_value_t = 0)]
    reshard_split_mb: u64,
    /// Split policy: split a shard applying more state mutations per second
    /// than this (0 = off).
    #[arg(long, env = "VLPDS_RESHARD_SPLIT_WRITES", default_value_t = 0.0)]
    reshard_split_writes: f64,
    /// Repo worker threads (MST + signing). Default: half the available
    /// cores (min 1); the request runtime does the rest of the CPU work.
    #[arg(long, env = "VLPDS_WORKERS")]
    workers: Option<usize>,
    /// Tokio threads (HTTP, JSON/CBOR, sequencers, storage IO). Default: the
    /// available cores (cgroup/affinity aware); fewer cap proxy-heavy load.
    #[arg(long, env = "VLPDS_IO_THREADS")]
    io_threads: Option<usize>,
    /// Cached repos per worker.
    #[arg(long, default_value_t = 50_000)]
    cache_per_worker: usize,
    /// Approximate heap budget for cached repos on this node (MiB): their
    /// loaded MST paths (DESIGN.md "Partial MSTs": ~10-20 KB per written
    /// repo, ~3 KB once back to its root). Past it, idle repos drop back to
    /// their root, then the least recently used are evicted (as past
    /// --cache-per-worker). 0 = no byte bound.
    #[arg(long, env = "VLPDS_REPO_CACHE_MB", default_value_t = 4096)]
    repo_cache_mb: usize,
    /// Cold open: read up to this much of the repo's M/ range (its persisted
    /// interior MST nodes) with one scan (KiB; the average active repo's is
    /// ~0.5 MB), so a disk-cache miss is ~1 object-store round trip instead
    /// of 7-11 dependent ones.
    #[arg(long, env = "VLPDS_LAZY_MST_PREFETCH_KB", default_value_t = (vlpds::worker::DEFAULT_PREFETCH_BYTES >> 10) as u64)]
    lazy_mst_prefetch_kb: u64,
    /// MiB of loaded MST nodes kept for readers (getRecord proofs, getBlocks)
    /// and path fetches, process-wide, by CID.
    #[arg(long, env = "VLPDS_LAZY_MST_NODE_CACHE_MB", default_value_t = (vlpds::mst_store::DEFAULT_NODE_CACHE_BYTES >> 20) as u64)]
    lazy_mst_node_cache_mb: u64,
    /// Segment size cap (MiB; fractions allow small segments in HA tests, so
    /// K PUTs are in flight at modest load).
    #[arg(long, default_value_t = 8.0)]
    max_segment_mb: f64,
    /// Segment PUTs in flight per node log (finalized in ordinal order).
    #[arg(long, env = "VLPDS_LOG_INFLIGHT", default_value_t = vlpds::nodelog::DEFAULT_LOG_INFLIGHT)]
    log_inflight: usize,
    /// Object-store requests in flight on the state client (SlateDB, blobs,
    /// account indexes); more queue for a permit. The pool keeps as many
    /// connections idle, so they are reused, never churned.
    #[arg(long, env = "VLPDS_STORE_INFLIGHT", default_value_t = vlpds::objlimit::DEFAULT_STATE_INFLIGHT)]
    store_inflight: usize,
    /// Object-store reads in flight on the log client (replay, firehose
    /// backfill, peer followers, retention). Segment PUTs have their own
    /// permits: max(64, 4 x --log-inflight).
    #[arg(long, env = "VLPDS_LOG_STORE_INFLIGHT", default_value_t = vlpds::objlimit::DEFAULT_LOG_INFLIGHT)]
    log_store_inflight: usize,
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
    /// Cursor backfills running at once; more wait for a slot. Read-ahead
    /// memory is at most this x --backfill-readahead-mb.
    #[arg(long, env = "VLPDS_FIREHOSE_MAX_BACKFILLS", default_value_t = vlpds::firehose::DEFAULT_MAX_BACKFILLS)]
    firehose_max_backfills: usize,
    /// subscribeRepos connections per client IP (IPv6: per /64; behind
    /// --trusted-proxies, the forwarded client); more get 429 (0 = no cap).
    #[arg(long, env = "VLPDS_FIREHOSE_MAX_PER_IP", default_value_t = vlpds::firehose::DEFAULT_MAX_PER_IP)]
    firehose_max_per_ip: usize,
    /// getRepo exports streaming at once; more wait up to 10 s for a slot,
    /// then get 503.
    #[arg(long, env = "VLPDS_MAX_EXPORTS", default_value_t = vlpds::xrpc::DEFAULT_MAX_EXPORTS)]
    max_exports: usize,
    /// End a getRepo export whose client has read nothing for this long.
    #[arg(long, env = "VLPDS_EXPORT_STALL_SECS", default_value_t = vlpds::xrpc::DEFAULT_EXPORT_STALL.as_secs())]
    export_stall_secs: u64,
    /// Repo-view reads (getRepo, getRecord, getBlocks, ...) queued at the
    /// repo workers before shedding with 503.
    #[arg(long, env = "VLPDS_MAX_QUEUED_READS", default_value_t = 20000)]
    max_queued_reads: usize,
    /// Start a second, identical segment PUT if the first takes longer than this.
    #[arg(long, env = "VLPDS_HEDGE_AFTER_MS", default_value_t = 100)]
    hedge_after_ms: u64,
    /// Max write requests in flight before shedding with 503.
    #[arg(long, env = "VLPDS_MAX_INFLIGHT_WRITES", default_value_t = 20000)]
    max_inflight_writes: usize,
    /// Local disk cache for SlateDB SSTs (empty = disabled).
    #[arg(long, env = "VLPDS_CACHE_DIR", default_value = "")]
    cache_dir: String,
    /// Disk budget of --cache-dir on this node (MiB). Each shard's cache is
    /// capped at this divided by the layout's shard count (all of them, so
    /// the caps fit even if this node comes to hold every shard). Unset:
    /// SlateDB's 16 GiB per shard (64 shards = 1 TiB).
    #[arg(long, env = "VLPDS_DISK_CACHE_MB")]
    disk_cache_mb: Option<u64>,
    /// Per-shard --cache-dir cap (MiB), instead of dividing --disk-cache-mb:
    /// for an N-node cluster where each node holds ~1/N of the shards (the
    /// disk must then absorb a failover's extra shards).
    #[arg(long, env = "VLPDS_DISK_CACHE_SHARD_MB")]
    disk_cache_shard_mb: Option<u64>,
    /// Local file where a fail-stop records its reason and exit code, read
    /// by the next start for `vlpds_last_exit_reason_info` (lifecycle.rs).
    /// Default: `vlpds-exit-<node-id>.json` in --cache-dir; empty with no
    /// cache dir = not kept (the next start reports reason "none").
    #[arg(long, env = "VLPDS_EXIT_STATE_FILE", default_value = "")]
    exit_state_file: String,
    /// In-memory SST block cache shared by every shard DB on this node (MiB;
    /// the meta/index cache gets a quarter of this on top).
    #[arg(long, env = "VLPDS_BLOCK_CACHE_MB", default_value_t = 4096)]
    block_cache_mb: u64,
    /// SlateDB SST block compression: none, lz4 or zstd.
    #[arg(long, env = "VLPDS_SST_COMPRESSION", default_value = "zstd")]
    sst_compression: String,
    /// Shard compactors' polling: slow (--compaction-poll, cheapest idle), fast (500 ms)
    /// or adaptive (slow until a shard's L0 runs deep, then fast until it
    /// drains; absorbs unpaced bulk ingests without the idle cost).
    #[arg(long, env = "VLPDS_COMPACTION_POLLING", default_value = "adaptive")]
    compaction_polling: String,
    /// Shard compactor and compaction worker poll interval while L0 is
    /// shallow (adaptive's slow mode, and `slow`). Each poll is ~6 GETs per
    /// shard; a deep L0 switches to 500 ms polls regardless.
    #[arg(long, env = "VLPDS_COMPACTION_POLL", default_value = "30s")]
    compaction_poll: String,
    /// How often each shard DB re-reads its SlateDB manifest (2 GETs per
    /// shard per poll). The node is its shards' only writer, so reads see
    /// their own writes regardless; a poll only picks up compaction results,
    /// and a writer with a deep L0 refreshes every 500 ms anyway.
    #[arg(long, env = "VLPDS_SLATEDB_MANIFEST_POLL", default_value = "10s")]
    slatedb_manifest_poll: String,
    /// Log segment body compression: zstd level (0 = store segments
    /// uncompressed). Level 1 stores real commits ~2x smaller for ~4-6 µs
    /// of CPU per commit (DESIGN.md "Log compression").
    #[arg(long, env = "VLPDS_LOG_COMPRESSION", default_value_t = vlpds::segment::DEFAULT_ZSTD_LEVEL, allow_hyphen_values = true)]
    log_compression: i32,
    /// SlateDB GC: SSTs no manifest or checkpoint references are deleted
    /// once this old (from creation; e.g. 10m, 1h). Guards SSTs not yet in
    /// a manifest; reads are protected by --slatedb-checkpoint-lifetime.
    #[arg(long, env = "VLPDS_SLATEDB_GC_MIN_AGE", default_value = "10m")]
    slatedb_gc_min_age: String,
    /// How long SSTs a compaction replaced stay readable (the compactor's
    /// checkpoint lifetime): a scan or snapshot (a big getRepo to a slow
    /// client) must finish within it. Also how long a bulk import's
    /// replaced SSTs linger (DESIGN.md §4).
    #[arg(long, env = "VLPDS_SLATEDB_CHECKPOINT_LIFETIME", default_value = "1h")]
    slatedb_checkpoint_lifetime: String,
    /// Log segment retention: the firehose backfill window (e.g. 72h, 30m;
    /// "off" keeps every segment). Older segments no replay can need are
    /// deleted; older cursors get OutdatedCursor.
    #[arg(long, env = "VLPDS_LOG_RETENTION", default_value = "72h")]
    log_retention: String,
    /// Time between log retention passes (1s..=10m). A pass LISTs only what
    /// can be due (DESIGN.md "Log retention"), so on an idle node most
    /// cost nothing; a longer interval delays deletes (and a dead log's
    /// retirement) by up to this much.
    #[arg(long, env = "VLPDS_LOG_RETENTION_INTERVAL", default_value = "60s")]
    log_retention_interval: String,
    /// A dead log, once pruned to its fence, keeps the fence this long
    /// ("off" = forever). The fence is what stops a zombie of that
    /// incarnation; one paused longer than this (a suspended VM whose
    /// monotonic clock stopped) could ack writes after it goes (DESIGN.md
    /// "Log retention", "Fences").
    #[arg(long, env = "VLPDS_FENCE_RETENTION", default_value = "7d")]
    fence_retention: String,
    /// Split/merge parents' state dirs (and aborted ops' clones) are deleted
    /// once nothing references them and their manifest is this old ("off"
    /// = keep them forever; DESIGN.md "Retired state GC").
    #[arg(long, env = "VLPDS_RESHARD_GC_GRACE", default_value = "1h")]
    reshard_gc_grace: String,
    /// A shard still reading a split/merge parent's SSTs this long after it
    /// opened here gets a compaction that rewrites them ("off" = leave it to
    /// size-tiered compaction, which may never rewrite a quiet shard's
    /// bottom run, pinning the parent).
    #[arg(long, env = "VLPDS_FORCED_DETACH_AFTER", default_value = "5m")]
    forced_detach_after: String,
    /// Opt-in: every shard held here gets a full compaction once per this
    /// (drops tombstones in its bottom run; "off" by default).
    #[arg(long, env = "VLPDS_FULL_COMPACTION_EVERY", default_value = "off")]
    full_compaction_every: String,
    /// How often each shard DB checks whether it can detach from a
    /// split/merge parent it no longer reads (SlateDB's detach GC).
    #[arg(long, env = "VLPDS_SLATEDB_DETACH_INTERVAL", default_value = "10m")]
    slatedb_detach_interval: String,
    /// Every owned shard is checkpointed (applied marker + memtable flush)
    /// once per this: bounds how much log a successor replays after a crash.
    #[arg(long, env = "VLPDS_CHECKPOINT_EVERY", default_value = "10s")]
    checkpoint_every: String,
    /// Spread checkpoints over --checkpoint-every, one shard at a time
    /// (false: every shard back to back once per interval).
    #[arg(long, env = "VLPDS_CHECKPOINT_STAGGER", default_value_t = true, action = clap::ArgAction::Set)]
    checkpoint_stagger: bool,
    /// Recently written repos remembered per shard (persisted with its
    /// checkpoints) and preloaded by the shard's next owner (restart,
    /// takeover, handback). 0 = off.
    #[arg(long, env = "VLPDS_PRELOAD_RECENT", default_value_t = vlpds::partition::DEFAULT_RECENT_REPOS)]
    preload_recent: usize,
    /// A write forwarded here that hasn't started within this many ms (its
    /// repo still loading) is answered 503 RepoLoading, unapplied, and the
    /// forwarding node resends it (DESIGN.md "Forwarding deadlines"). 0 =
    /// wait for it (the forwarder's 3 s deadline then fails it).
    #[arg(long, env = "VLPDS_FORWARDED_WRITE_START_MS", default_value_t = vlpds::forward::FORWARDED_WRITE_START.as_millis() as u64)]
    forwarded_write_start_ms: u64,
    /// Resend repo writes answered "not applied" (503 RepoLoading /
    /// ShardMoved: a cold repo load, a shard moving) from the node the
    /// client called, for up to 20 s, instead of failing them.
    #[arg(long, env = "VLPDS_RETRY_UNAPPLIED_WRITES", default_value_t = true, action = clap::ArgAction::Set)]
    retry_unapplied_writes: bool,
    /// Default AppView for proxied requests: "<url>,<service did>".
    #[arg(long, env = "VLPDS_APPVIEW")]
    appview: Option<String>,
    /// Moderation service for createReport: "<url>,<service did>".
    #[arg(long, env = "VLPDS_REPORT_SERVICE")]
    report_service: Option<String>,
    /// Image URLs in read-after-write views of the requester's own records
    /// (reference PDS_BSKY_APP_VIEW_CDN_URL_PATTERN), with three `%s`:
    /// preset (avatar, banner, feed_thumbnail, feed_fullsize), DID, blob CID,
    /// e.g. "https://cdn.bsky.app/img/%s/plain/%s/%s@jpeg". Unset: this
    /// PDS's getBlob URL.
    #[arg(long, env = "VLPDS_BSKY_APP_VIEW_CDN_URL_PATTERN")]
    bsky_app_view_cdn_url_pattern: Option<String>,
    /// Relays to send requestCrawl to at startup (comma-separated hostnames/urls).
    #[arg(long, env = "VLPDS_CRAWLERS", value_delimiter = ',')]
    crawlers: Vec<String>,
    /// Dev mode: email/password tokens are logged instead of mailed, and the
    /// well-known dev secrets are accepted.
    #[arg(long, env = "VLPDS_DEV_MODE")]
    dev_mode: bool,
    /// Serve vlpds.admin.bulkCreate (synthetic benchmark accounts, admin
    /// token) without --dev-mode.
    #[arg(long, env = "VLPDS_ALLOW_BULK_CREATE")]
    allow_bulk_create: bool,
    /// Key-encryption key for secrets at rest (signing keys, TOTP secrets;
    /// DESIGN.md "Secrets at rest"): a file of 32 random bytes (raw, hex or
    /// base64), the same on every node. Required outside --dev-mode unless
    /// --gcp-kms-key is set (then it is accepted for unwrap only).
    #[arg(long, env = "VLPDS_KEK_FILE")]
    kek_file: Option<std::path::PathBuf>,
    /// The KEK itself (hex or base64), instead of --kek-file.
    #[arg(long, env = "VLPDS_KEK", hide_env_values = true, conflicts_with = "kek_file")]
    kek: Option<String>,
    /// Previous KEK files, accepted for unwrap only (KEK rotation: keep them
    /// until `vlpds.admin.rewrapSecrets` reports nothing stale).
    #[arg(long, env = "VLPDS_KEK_OLD_FILE", value_delimiter = ',')]
    kek_old_file: Vec<std::path::PathBuf>,
    /// Previous KEKs as values (hex or base64, comma-separated).
    #[arg(long, env = "VLPDS_KEK_OLD", value_delimiter = ',', hide_env_values = true)]
    kek_old: Vec<String>,
    /// Google Cloud KMS CryptoKey that wraps secrets
    /// (projects/P/locations/L/keyRings/R/cryptoKeys/K), used with the
    /// node's service account (metadata server) or --gcp-credentials-file.
    /// Takes precedence over --kek-file for new wraps.
    #[arg(long, env = "VLPDS_GCP_KMS_KEY")]
    gcp_kms_key: Option<String>,
    /// Service-account JSON key file for Cloud KMS (off GCE); falls back to
    /// GOOGLE_APPLICATION_CREDENTIALS, then the GCE metadata server.
    #[arg(long, env = "VLPDS_GCP_CREDENTIALS_FILE")]
    gcp_credentials_file: Option<std::path::PathBuf>,
    /// Previous CryptoKeys, unwrap only (moving to another key).
    #[arg(long, env = "VLPDS_GCP_KMS_OLD_KEY", value_delimiter = ',')]
    gcp_kms_old_key: Vec<String>,
    /// Cloud KMS API base URL.
    #[arg(long, env = "VLPDS_GCP_KMS_ENDPOINT", default_value = vlpds::secrets::GCP_KMS_ENDPOINT)]
    gcp_kms_endpoint: String,
    /// Remote KMS unwraps in flight per node (cold signing-key loads); wraps
    /// get a separate pool of a quarter of this.
    #[arg(long, env = "VLPDS_KMS_CONCURRENCY", default_value_t = vlpds::secrets::DEFAULT_KMS_CONCURRENCY)]
    kms_concurrency: usize,
    /// Send email over SMTP: smtp://[user:pass@]host[:port] (STARTTLS when
    /// offered; ?tls=required|none) or smtps://... (implicit TLS). Falls back
    /// to the reference PDS's PDS_EMAIL_SMTP_URL. Unset: mail is only logged
    /// (DESIGN.md "Email").
    #[arg(long, env = "VLPDS_EMAIL_SMTP_URL", hide_env_values = true)]
    email_smtp_url: Option<String>,
    /// From address for email ("addr@host" or "Name <addr@host>"); falls
    /// back to PDS_EMAIL_FROM_ADDRESS. Required with --email-smtp-url.
    #[arg(long, env = "VLPDS_EMAIL_FROM_ADDRESS")]
    email_from_address: Option<String>,
    /// Service name in email (falls back to PDS_SERVICE_NAME; default
    /// "{hostname} PDS").
    #[arg(long, env = "VLPDS_EMAIL_BRAND_NAME")]
    email_brand_name: Option<String>,
    /// Footer link in email (falls back to PDS_HOME_URL; default
    /// https://bsky.app, as the reference).
    #[arg(long, env = "VLPDS_EMAIL_HOME_URL")]
    email_home_url: Option<String>,
    /// Logo image URL in email (falls back to PDS_LOGO_URL; default the
    /// reference's Bluesky logo).
    #[arg(long, env = "VLPDS_EMAIL_LOGO_URL")]
    email_logo_url: Option<String>,
    /// Accent color in email (falls back to PDS_PRIMARY_COLOR; default #067df7).
    #[arg(long, env = "VLPDS_EMAIL_PRIMARY_COLOR")]
    email_primary_color: Option<String>,
    /// Drop the bsky.app "click here" link from email confirmation mail
    /// (or PDS_EMAIL_DISABLE_CONFIRMATION_LINK=true).
    #[arg(long, env = "VLPDS_EMAIL_DISABLE_CONFIRMATION_LINK")]
    email_disable_confirmation_link: bool,
    /// SMTP URL for admin sendEmail (moderation mail; same forms as
    /// --email-smtp-url). Falls back to PDS_MODERATION_EMAIL_SMTP_URL.
    /// Unset: admin sendEmail goes through the main mailer.
    #[arg(long, env = "VLPDS_MODERATION_EMAIL_SMTP_URL", hide_env_values = true)]
    moderation_email_smtp_url: Option<String>,
    /// From address for moderation mail (falls back to
    /// PDS_MODERATION_EMAIL_ADDRESS). Required with --moderation-email-smtp-url.
    #[arg(long, env = "VLPDS_MODERATION_EMAIL_ADDRESS")]
    moderation_email_address: Option<String>,
    /// Max uploadBlob size (MB).
    #[arg(long, env = "VLPDS_MAX_BLOB_MB", default_value_t = 100)]
    max_blob_mb: u64,
    /// describeServer links.privacyPolicy (falls back to PDS_PRIVACY_POLICY_URL).
    #[arg(long, env = "VLPDS_PRIVACY_POLICY_URL")]
    privacy_policy_url: Option<String>,
    /// describeServer links.termsOfService (falls back to PDS_TERMS_OF_SERVICE_URL).
    #[arg(long, env = "VLPDS_TERMS_OF_SERVICE_URL")]
    terms_of_service_url: Option<String>,
    /// describeServer contact.email (falls back to PDS_CONTACT_EMAIL_ADDRESS).
    #[arg(long, env = "VLPDS_CONTACT_EMAIL_ADDRESS")]
    contact_email_address: Option<String>,
    /// Delete unreferenced blobs uploaded more than this many seconds ago.
    #[arg(long, env = "VLPDS_BLOB_GC_GRACE_SECS", default_value_t = 6 * 3600)]
    blob_gc_grace_secs: u64,
    /// PLC directory: did:plc resolution and, with PLC registration on,
    /// where new accounts' genesis ops and their updates are submitted (a
    /// local did-method-plc server works for e2e runs).
    #[arg(long, env = "VLPDS_PLC_URL", default_value = vlpds::plc::DEFAULT_PLC_URL)]
    plc_url: String,
    /// PLC registration of new accounts' DIDs (DESIGN.md "PLC identity"):
    /// `auto` = `directory` when a rotation key is set, else `unregistered`
    /// (dev mode only); `directory`; `unregistered` (dev/test/bench only:
    /// DIDs minted locally, never registered, resolvable only here).
    #[arg(long, env = "VLPDS_PLC_MODE", default_value = "auto")]
    plc_mode: String,
    /// The server's PLC rotation key: a secp256k1 private key as 64 hex
    /// chars (falls back to the reference's
    /// PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX). Prefer the env var or
    /// --plc-rotation-key-file over the flag (argv is visible).
    #[arg(long, env = "VLPDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX", hide_env_values = true)]
    plc_rotation_key: Option<String>,
    /// A file holding the PLC rotation key wrapped under the KEK (`vw1.`,
    /// from --wrap-plc-rotation-key; unwrapped at startup, via Cloud KMS
    /// with --gcp-kms-key) or as 64 hex chars (a mounted secret).
    #[arg(long, env = "VLPDS_PLC_ROTATION_KEY_FILE", conflicts_with = "plc_rotation_key")]
    plc_rotation_key_file: Option<std::path::PathBuf>,
    /// Retired PLC rotation keys (files as --plc-rotation-key-file): they
    /// sign updates of DIDs that still list them, replacing them with the
    /// current key (`vlpds.admin.rotatePlcKeys`; RUNBOOK "PLC rotation key
    /// rotation").
    #[arg(long, env = "VLPDS_PLC_ROTATION_KEY_OLD_FILE", value_delimiter = ',')]
    plc_rotation_key_old_file: Vec<std::path::PathBuf>,
    /// Retired PLC rotation keys as hex (comma-separated).
    #[arg(long, env = "VLPDS_PLC_ROTATION_KEY_OLD", value_delimiter = ',', hide_env_values = true)]
    plc_rotation_key_old: Vec<String>,
    /// A did:key put ahead of the server rotation key in new accounts'
    /// rotation keys and in getRecommendedDidCredentials (falls back to the
    /// reference's PDS_RECOVERY_DID_KEY).
    #[arg(long, env = "VLPDS_PLC_RECOVERY_DID_KEY")]
    plc_recovery_did_key: Option<String>,
    /// Print the PLC rotation key read from stdin (64 hex chars; empty
    /// stdin = a new random key) wrapped under the configured KEK, in the
    /// --plc-rotation-key-file form, and exit. Its did:key goes to stderr.
    #[arg(long)]
    wrap_plc_rotation_key: bool,
    /// Require an invite code for createAccount.
    #[arg(long, env = "VLPDS_INVITE_REQUIRED")]
    invite_required: bool,
    /// With --invite-required: each account earns one invite code per this
    /// many milliseconds of account age (at most 5 unused at a time),
    /// created when it calls getAccountInviteCodes (falls back to the
    /// reference's PDS_INVITE_INTERVAL). Unset: accounts never earn codes.
    #[arg(long, env = "VLPDS_INVITE_INTERVAL_MS")]
    invite_interval_ms: Option<String>,
    /// Earned invite codes count only account age after this Unix time in
    /// milliseconds (falls back to PDS_INVITE_EPOCH; default 0).
    #[arg(long, env = "VLPDS_INVITE_EPOCH_MS")]
    invite_epoch_ms: Option<String>,
    /// The moderation service (Ozone) DID allowed to call the moderator admin
    /// methods (getAccountInfo(s), get/updateSubjectStatus, sendEmail,
    /// getInviteCodes, disableInviteCodes, enable/disableAccountInvites) and
    /// read any account's getPreferences with a service JWT (falls back to
    /// the reference's PDS_MOD_SERVICE_DID). Unset: admin Basic auth only.
    #[arg(long, env = "VLPDS_MOD_SERVICE_DID")]
    mod_service_did: Option<String>,
    /// Resolve the lexicons of record types without a bundled schema (DNS
    /// `_lexicon` TXT -> DID -> com.atproto.lexicon.schema record) and
    /// validate those records too, instead of reporting them "unknown".
    #[arg(long, env = "VLPDS_RESOLVE_LEXICONS")]
    resolve_lexicons: bool,
    /// Largest CAR importRepo accepts, in MiB (parsed in memory; the import
    /// is written as one log entry).
    #[arg(long, env = "VLPDS_MAX_IMPORT_MB", default_value_t = 1024)]
    max_import_mb: usize,
    /// Stable node id (keep it across restarts so a restarted node reclaims
    /// its shards immediately). Default "single".
    #[arg(long, env = "VLPDS_NODE_ID")]
    node_id: Option<String>,
    /// URL peers use to reach this node: https://<host>:<--peer-listen
    /// port>, a host its peer certificate names. Only with --peer-listen.
    #[arg(long, env = "VLPDS_ADVERTISE_URL", requires = "peer_listen")]
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
    /// trusted for the rate-limit client IP (load balancers; not the nodes:
    /// a forwarding node passes the client address over the internal token).
    #[arg(long, env = "VLPDS_TRUSTED_PROXIES", value_delimiter = ',')]
    trusted_proxies: Vec<String>,
    /// HTTP/2 connections (over peer mTLS) to each peer node; requests
    /// round-robin.
    #[arg(long, env = "VLPDS_PEER_CONNECTIONS", default_value_t = vlpds::http::DEFAULT_PEER_CONNECTIONS)]
    peer_connections: usize,
    /// `x-ratelimit-bypass` header value that skips rate limits.
    #[arg(long, env = "VLPDS_RATE_LIMIT_BYPASS_KEY")]
    rate_limit_bypass_key: Option<String>,
    /// Memory budget of the in-memory caches (verified tokens, proxy
    /// accounts and service JWTs, DID documents, lexicons, OAuth clients),
    /// split between them by weight (MiB). Default: 10% of physical RAM or
    /// the cgroup limit. The chosen caps are logged at startup.
    #[arg(long, env = "VLPDS_CACHE_BUDGET_MB")]
    cache_budget_mb: Option<u64>,
    /// Entry caps overriding the budget's split, comma-separated
    /// `<cache>=<entries>` (session_tokens, oauth_tokens, proxy_accounts,
    /// proxy_jwts, did_docs, lexicons, oauth_clients, permission_sets).
    #[arg(long, env = "VLPDS_CACHE_ENTRIES", value_delimiter = ',')]
    cache_entries: Vec<String>,
    /// Push continuous CPU profiles (100 Hz) to this Pyroscope server, tagged
    /// with the node id and git revision (needs `--features profiling`;
    /// bench/obs runs one on http://127.0.0.1:4040).
    #[arg(long, env = "VLPDS_PYROSCOPE_URL")]
    pyroscope_url: Option<String>,
    /// Log line format on stderr: text (ANSI colour only on a terminal,
    /// never with NO_COLOR) or json (one object per line; production). Level
    /// filter: RUST_LOG (default info,slatedb=warn).
    #[arg(long, env = "VLPDS_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
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

fn mib(v: f64, min: usize) -> usize {
    ((v * (1u64 << 20) as f64) as usize).max(min)
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get())
}

fn default_workers() -> usize {
    (cores() / 2).max(1)
}

/// Every client, peer and S3 connection is a descriptor, and Linux's default
/// soft limit of 1024 is far too low. macOS caps it at kern.maxfilesperproc.
/// No libc dependency, as in metrics.rs: rlim_t is 64-bit on both targets.
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

#[derive(Parser)]
#[command(name = "vlpds admin", about = "Operator commands against a running vlpds node (pdsadmin equivalents, shard layout)")]
struct AdminArgs {
    /// Any node of the cluster.
    #[arg(long, global = true, env = "VLPDS_URL", default_value = "http://127.0.0.1:2583")]
    url: String,
    /// Admin token (default: the dev token).
    #[arg(long, global = true, env = "VLPDS_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: Option<String>,
    /// Print raw JSON results instead of tables.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: AdminCmd,
}

#[derive(clap::Subcommand)]
enum AdminCmd {
    /// Peer mTLS certificates (local files; no node is called).
    #[command(subcommand)]
    Tls(TlsCmd),
    #[command(flatten)]
    Ops(vlpds::cli::admin::Cmd),
}

/// ops/RUNBOOK.md "Peer TLS".
#[derive(clap::Subcommand)]
enum TlsCmd {
    /// Create a cluster CA (ECDSA P-256): <out>/ca.crt and <out>/ca.key
    /// (0600). Keep ca.key offline; nodes need only ca.crt.
    Ca {
        /// Output directory.
        #[arg(long, default_value = ".")]
        out: std::path::PathBuf,
        /// The CA's common name.
        #[arg(long, default_value = "vlpds cluster CA")]
        name: String,
        /// Validity in days.
        #[arg(long, default_value_t = 3650)]
        days: u32,
        /// Replace existing files.
        #[arg(long)]
        force: bool,
    },
    /// Issue a node certificate from the CA: <out>/<node-id>.crt and
    /// <out>/<node-id>.key (0600), with SANs vlpds://node/<node-id> and
    /// each --host.
    Issue {
        /// The node's --node-id.
        #[arg(long)]
        node_id: String,
        /// DNS name or IP address of the node's --advertise-url (repeat or
        /// comma-separate for several).
        #[arg(long = "host", required = true, value_delimiter = ',')]
        hosts: Vec<String>,
        /// The CA certificate.
        #[arg(long, default_value = "ca.crt")]
        ca: std::path::PathBuf,
        /// The CA key.
        #[arg(long, default_value = "ca.key")]
        ca_key: std::path::PathBuf,
        /// Output directory.
        #[arg(long, default_value = ".")]
        out: std::path::PathBuf,
        /// Validity in days (renew before the 14-day expiry alert).
        #[arg(long, default_value_t = 365)]
        days: u32,
        /// Replace existing files (renewal).
        #[arg(long)]
        force: bool,
    },
    /// Print what a certificate names: node id, hosts, expiry.
    Show { cert: std::path::PathBuf },
}

fn tls_main(cmd: TlsCmd) -> anyhow::Result<()> {
    use vlpds::peer_tls;
    match cmd {
        TlsCmd::Ca { out, name, days, force } => {
            let ca = peer_tls::create_ca(&name, days)?;
            let (crt, key) = peer_tls::write_pair(&out, "ca", &ca, force)?;
            println!("CA certificate: {}\nCA key (0600; keep it offline): {}", crt.display(), key.display());
        }
        TlsCmd::Issue { node_id, hosts, ca, ca_key, out, days, force } => {
            let read = |p: &std::path::Path| std::fs::read_to_string(p).map_err(|e| anyhow::anyhow!("reading {}: {e}", p.display()));
            let n = peer_tls::issue_node(&read(&ca)?, &read(&ca_key)?, &node_id, &hosts, days)?;
            let (crt, key) = peer_tls::write_pair(&out, &node_id, &n, force)?;
            println!("node certificate: {}\nnode key (0600): {}", crt.display(), key.display());
            let dir_ca = out.join("ca.crt");
            if std::fs::canonicalize(&ca).ok() != std::fs::canonicalize(&dir_ca).ok() {
                println!("copy {} to {} (the node's directory holds ca.crt too)", ca.display(), dir_ca.display());
            }
            println!("run the node with --node-id {node_id} --peer-tls-dir {} (without ca.key)", out.display());
        }
        TlsCmd::Show { cert } => {
            use rustls::pki_types::pem::PemObject;
            let pem = std::fs::read(&cert)?;
            for der in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
                let i = peer_tls::cert_info(&der?)?;
                let left = (i.not_after - chrono::Utc::now().timestamp()) / 86400;
                let not_after = chrono::DateTime::from_timestamp(i.not_after, 0).map(|t| t.to_rfc3339()).unwrap_or_default();
                println!(
                    "subject: {}\nca: {}\nnode: {}\nhosts: {}\nnot after: {not_after} ({left} days left)",
                    i.subject,
                    i.is_ca,
                    i.node_id.as_deref().unwrap_or("-"),
                    i.hosts.join(", ")
                );
            }
        }
    }
    Ok(())
}

fn admin_main(args: AdminArgs) -> anyhow::Result<()> {
    let cmd = match args.cmd {
        AdminCmd::Tls(cmd) => return tls_main(cmd),
        AdminCmd::Ops(cmd) => cmd,
    };
    let token = args.admin_token.unwrap_or_else(|| server::DEV_ADMIN_TOKEN.to_string());
    let opts = vlpds::cli::admin::Opts { url: args.url, token, json: args.json };
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(vlpds::cli::admin::run(cmd, &opts, &mut std::io::stdout()))
}

fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("admin") {
        return admin_main(AdminArgs::parse_from(std::env::args().skip(1)));
    }
    let args = Args::parse();
    init_logging(args.log_format)?;
    vlpds::lifecycle::install_panic_hook();
    raise_nofile_limit();
    let node_id = args.node_id.clone().unwrap_or_else(|| "single".into());
    let exit_state = match (args.exit_state_file.as_str(), args.cache_dir.as_str()) {
        ("", "") => None,
        ("", dir) => Some(std::path::Path::new(dir).join(format!("vlpds-exit-{node_id}.json"))),
        (f, _) => Some(std::path::PathBuf::from(f)),
    };
    vlpds::lifecycle::init(exit_state);
    let rev = vlpds::version::build_rev().to_string();
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
    let r = rt.block_on(run(args));
    match &r {
        Ok(()) => vlpds::lifecycle::record_exit(0, "clean"),
        Err(_) => vlpds::lifecycle::record_exit(1, "error"),
    }
    r
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum LogFormat {
    /// Human-readable lines; ANSI colour only when stderr is a terminal and
    /// NO_COLOR is unset.
    Text,
    /// One JSON object per line (journald / log shippers).
    Json,
}

/// Logs go to stderr, so stdout carries only machine output
/// (`--wrap-plc-rotation-key`'s wrapped key, `vlpds admin` tables/--json).
fn init_logging(format: LogFormat) -> anyhow::Result<()> {
    use std::io::IsTerminal;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,slatedb=warn".into());
    let b = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let r = match format {
        LogFormat::Json => b.json().flatten_event(true).with_current_span(false).try_init(),
        LogFormat::Text => {
            let no_color = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
            b.with_ansi(!no_color && std::io::stderr().is_terminal()).try_init()
        }
    };
    r.map_err(|e| anyhow::anyhow!("logging: {e}"))
}

/// None: on the app port.
fn metrics_listen(args: &Args) -> Option<String> {
    match args.metrics_listen.as_deref() {
        Some("app") => None,
        Some(a) => Some(a.to_string()),
        None if args.dev_mode => None,
        None => Some("127.0.0.1:9583".into()),
    }
}

/// An unset secret outside dev mode is refused by `Config::check_secrets`.
fn secret(v: &Option<String>, dev_mode: bool, dev_default: &str) -> String {
    match v {
        Some(v) => v.clone(),
        None if dev_mode => dev_default.to_string(),
        None => String::new(),
    }
}

fn plc_config(args: &Args) -> anyhow::Result<vlpds::plc::PlcConfig> {
    use vlpds::plc::RotationKey;
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    let hex = |h: &str| RotationKey::Hex(zeroize::Zeroizing::new(h.trim().to_string()));
    let rotation_key = match (args.plc_rotation_key.as_deref().filter(|h| !h.trim().is_empty()), &args.plc_rotation_key_file) {
        (Some(h), _) => Some(hex(h)),
        (None, Some(p)) => Some(RotationKey::from_file(p)?),
        (None, None) => env("PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX").map(|h| hex(&zeroize::Zeroizing::new(h))),
    };
    let mut old_rotation_keys = args.plc_rotation_key_old_file.iter().map(|p| RotationKey::from_file(p)).collect::<anyhow::Result<Vec<_>>>()?;
    old_rotation_keys.extend(args.plc_rotation_key_old.iter().filter(|h| !h.trim().is_empty()).map(|h| hex(h)));
    Ok(vlpds::plc::PlcConfig {
        mode: args.plc_mode.parse()?,
        rotation_key,
        old_rotation_keys,
        recovery_did_key: args.plc_recovery_did_key.clone().filter(|k| !k.is_empty()).or_else(|| env("PDS_RECOVERY_DID_KEY")),
    })
}

async fn wrap_plc_rotation_key(args: &Args) -> anyhow::Result<()> {
    let kek = kek_config(args)?;
    kek.check(args.dev_mode)?;
    let secrets = vlpds::secrets::Secrets::from_config(&kek, args.dev_mode)?;
    let mut input = zeroize::Zeroizing::new(String::new());
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    let key = if input.trim().is_empty() {
        std::sync::Arc::new(vlpds::crypto::Keypair::generate())
    } else {
        vlpds::plc::RotationKey::Hex(zeroize::Zeroizing::new(input.trim().to_string())).load(&secrets).await?
    };
    let wrapped = vlpds::plc::wrap_rotation_key(&secrets, &key).await?;
    println!("{wrapped}");
    eprintln!("PLC rotation key {} wrapped under KEK {}", key.did_key(), secrets.current_kid());
    Ok(())
}

fn kek_config(args: &Args) -> anyhow::Result<vlpds::secrets::KekConfig> {
    use vlpds::secrets::KekBytes;
    let local = match (&args.kek_file, &args.kek) {
        (Some(p), _) => Some(KekBytes::from_file(p)?),
        (None, Some(v)) => Some(KekBytes::parse(v)?),
        (None, None) => None,
    };
    let mut local_old = args.kek_old_file.iter().map(|p| KekBytes::from_file(p)).collect::<anyhow::Result<Vec<_>>>()?;
    for v in args.kek_old.iter().filter(|v| !v.trim().is_empty()) {
        local_old.push(KekBytes::parse(v)?);
    }
    let gcp_key = args.gcp_kms_key.clone().filter(|k| !k.is_empty());
    let gcp_old_keys: Vec<String> = args.gcp_kms_old_key.iter().filter(|k| !k.is_empty()).cloned().collect();
    // read the credentials only when KMS is in use (a stray
    // GOOGLE_APPLICATION_CREDENTIALS doesn't matter otherwise)
    let gcp_token = if gcp_key.is_some() || !gcp_old_keys.is_empty() {
        Some(vlpds::secrets::GcpToken::from_credentials(args.gcp_credentials_file.as_deref())?)
    } else {
        None
    };
    Ok(vlpds::secrets::KekConfig {
        local,
        local_old,
        gcp_key,
        gcp_old_keys,
        gcp_endpoint: Some(args.gcp_kms_endpoint.clone()),
        gcp_token,
        kms_concurrency: args.kms_concurrency,
    })
}

fn peer_tls(args: &Args, node_id: &str, advertise_url: &str) -> anyhow::Result<Option<std::sync::Arc<vlpds::peer_tls::PeerTls>>> {
    let Some(dir) = &args.peer_tls_dir else {
        tracing::info!("lone node (no --peer-listen): no peer listener, no /internal/*, no peer calls");
        return Ok(None);
    };
    anyhow::ensure!(
        advertise_url.starts_with("https://"),
        "--advertise-url must be https://<host>:<--peer-listen port> (peers talk mTLS only), got {advertise_url:?}"
    );
    vlpds::peer_tls::check_node_id(node_id)?;
    let files = if args.dev_mode {
        vlpds::peer_tls::dev_files(dir, node_id, &[vlpds::peer_tls::url_host(advertise_url)?])?
    } else {
        vlpds::peer_tls::Files::in_dir(dir, node_id)
    };
    let t = vlpds::peer_tls::PeerTls::load(files).map_err(|e| e.context(format!("peer TLS (--peer-tls-dir {})", dir.display())))?;
    anyhow::ensure!(t.node_id() == node_id, "{node_id}.crt names node {:?} but --node-id is {node_id:?}", t.node_id());
    let (cert_exp, ca_exp) = t.not_after();
    tracing::info!(node = %t.node_id(), cert_not_after = cert_exp, ca_not_after = ca_exp, "peer mTLS on");
    t.spawn_reloader();
    Ok(Some(t))
}

async fn run(args: Args) -> anyhow::Result<()> {
    if args.wrap_plc_rotation_key {
        return wrap_plc_rotation_key(&args).await;
    }
    vlpds::partition::set_block_cache_bytes(args.block_cache_mb << 20);
    vlpds::partition::set_sst_compression(args.sst_compression.parse()?);
    vlpds::partition::set_compaction_polling(args.compaction_polling.parse()?);
    vlpds::partition::set_compaction_poll_interval(vlpds::retention::parse_duration(&args.compaction_poll)?);
    vlpds::partition::set_manifest_poll_interval(vlpds::retention::parse_duration(&args.slatedb_manifest_poll)?);
    vlpds::partition::set_gc_min_age(vlpds::retention::parse_duration(&args.slatedb_gc_min_age)?);
    vlpds::partition::set_checkpoint_lifetime(vlpds::retention::parse_duration(&args.slatedb_checkpoint_lifetime)?);
    vlpds::segment::set_compression_level(args.log_compression);
    vlpds::partition::set_detach_interval(vlpds::retention::parse_duration(&args.slatedb_detach_interval)?);
    let opt_duration = |v: &str| -> anyhow::Result<Option<Duration>> {
        match v {
            "off" | "none" => Ok(None),
            v => vlpds::retention::parse_duration(v).map(Some),
        }
    };
    let retention_interval = vlpds::retention::parse_duration(&args.log_retention_interval)?;
    // VlpdsRetentionNotRunning expects a pass at least every 15 min
    anyhow::ensure!(
        (Duration::from_secs(1)..=Duration::from_secs(600)).contains(&retention_interval),
        "--log-retention-interval must be within 1s..=10m (got {})",
        args.log_retention_interval
    );
    let log_retention = match args.log_retention.as_str() {
        "off" | "none" => None,
        v => Some(vlpds::retention::Config {
            window: vlpds::retention::parse_duration(v)?,
            interval: retention_interval,
            fence_retention: opt_duration(&args.fence_retention)?,
            ..Default::default()
        }),
    };
    let node_id = args.node_id.clone().unwrap_or_else(|| "single".into());
    let advertise_url = args.advertise_url.clone().unwrap_or_else(|| args.public_url.clone());
    let peer_tls = peer_tls(&args, &node_id, &advertise_url)?;
    let metrics_addr = metrics_listen(&args);
    let reshard_gc = Some(vlpds::reshard_gc::Config {
        grace: opt_duration(&args.reshard_gc_grace)?,
        detach_after: opt_duration(&args.forced_detach_after)?,
        full_every: opt_duration(&args.full_compaction_every)?,
        ..Default::default()
    });
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
        store_inflight: args.store_inflight.max(1),
        log_store_inflight: args.log_store_inflight.max(1),
        shards: args.shards,
        workers: args.workers.unwrap_or_else(default_workers).max(1),
        cache_per_worker: args.cache_per_worker,
        repo_cache_bytes: args.repo_cache_mb << 20,
        lazy_mst_prefetch_bytes: (args.lazy_mst_prefetch_kb << 10) as usize,
        lazy_mst_unload_idle: false,
        lazy_mst_node_cache_bytes: (args.lazy_mst_node_cache_mb << 20) as usize,
        max_segment_bytes: mib(args.max_segment_mb, 4096),
        log_inflight: args.log_inflight.max(1),
        live_ring_bytes: mib(args.live_ring_mb, 1),
        firehose_merge_queue_bytes: mib(args.firehose_merge_queue_mb, 1),
        firehose_ring_bytes: args.firehose_ring_mb << 20,
        firehose_threads: args.firehose_threads,
        firehose_max_lag_bytes: args.firehose_max_lag_mb << 20,
        backfill_readahead_bytes: args.backfill_readahead_mb << 20,
        backfill_cache_bytes: args.backfill_cache_mb << 20,
        firehose_max_backfills: args.firehose_max_backfills.max(1),
        firehose_max_per_ip: args.firehose_max_per_ip,
        hedge_after: Duration::from_millis(args.hedge_after_ms),
        max_inflight_writes: args.max_inflight_writes,
        max_queued_reads: args.max_queued_reads.max(1),
        max_exports: args.max_exports.max(1),
        export_stall: Duration::from_secs(args.export_stall_secs.max(1)),
        max_connections: args.max_connections,
        cache_dir: (!args.cache_dir.is_empty()).then(|| std::path::PathBuf::from(&args.cache_dir)),
        disk_cache_bytes: args.disk_cache_mb.map(|m| m << 20),
        disk_cache_shard_bytes: args.disk_cache_shard_mb.map(|m| m << 20),
        appview: url_did(&args.appview)?,
        report_service: url_did(&args.report_service)?,
        appview_cdn_url_pattern: args.bsky_app_view_cdn_url_pattern.clone().filter(|p| !p.is_empty()),
        crawlers: args.crawlers.clone(),
        dev_mode: args.dev_mode,
        allow_bulk_create: args.allow_bulk_create,
        kek: kek_config(&args)?,
        mailer: vlpds::mail::from_flags(args.email_smtp_url.clone(), args.email_from_address.clone())?,
        moderation_mailer: vlpds::mail::moderation_from_flags(
            args.moderation_email_smtp_url.clone(),
            args.moderation_email_address.clone(),
        )?,
        email_branding: vlpds::mail::Branding::from_flags(
            args.email_brand_name.clone(),
            args.email_home_url.clone(),
            args.email_logo_url.clone(),
            args.email_primary_color.clone(),
            args.email_disable_confirmation_link,
        ),
        max_blob_size: args.max_blob_mb << 20,
        privacy_policy_url: flag_or_env(&args.privacy_policy_url, "PDS_PRIVACY_POLICY_URL"),
        terms_of_service_url: flag_or_env(&args.terms_of_service_url, "PDS_TERMS_OF_SERVICE_URL"),
        contact_email_address: flag_or_env(&args.contact_email_address, "PDS_CONTACT_EMAIL_ADDRESS"),
        blob_gc_grace: Duration::from_secs(args.blob_gc_grace_secs),
        plc_url: args.plc_url.clone(),
        plc: plc_config(&args)?,
        invite_required: args.invite_required,
        invite_interval: flag_or_env(&args.invite_interval_ms, "PDS_INVITE_INTERVAL")
            .map(|v| v.parse::<u64>().map(Duration::from_millis))
            .transpose()
            .map_err(|e| anyhow::anyhow!("invite interval (ms): {e}"))?
            .filter(|d| !d.is_zero()),
        invite_epoch_ms: flag_or_env(&args.invite_epoch_ms, "PDS_INVITE_EPOCH")
            .map(|v| v.parse::<i64>())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invite epoch (ms): {e}"))?
            .unwrap_or(0),
        mod_service_did: flag_or_env(&args.mod_service_did, "PDS_MOD_SERVICE_DID"),
        txt_resolver: None,
        rate_limits_enabled: !args.no_rate_limits,
        resolve_lexicons: args
            .resolve_lexicons
            .then_some(vlpds::lexicon::RESOLVE_TIMEOUT),
        max_import_bytes: args.max_import_mb << 20,
        trusted_proxies: args.trusted_proxies.clone(),
        peer_connections: args.peer_connections,
        peer_tls,
        rate_limit_bypass_key: args.rate_limit_bypass_key.clone(),
        cluster: Some(vlpds::cluster::ClusterConfig {
            node_id,
            addr: advertise_url,
            shards: args.shards,
            ttl: Duration::from_millis(args.lease_ttl_ms),
            renew_every: Duration::from_millis(args.lease_ttl_ms / 5),
            skew: Duration::from_millis(args.lease_ttl_ms / 5),
            clock_offset_ms: 0,
            levels: vlpds::version::Window::BUILD,
        }),
        memory_store: None,
        metrics_listen: metrics_addr.clone(),
        log_retention,
        reshard_gc,
        cache_budget_bytes: args.cache_budget_mb.map(|m| m << 20),
        cache_entries: vlpds::caches::parse_overrides(&args.cache_entries)?,
        reshard_policy: vlpds::reshard::Policy {
            split_bytes: (args.reshard_split_mb > 0).then_some(args.reshard_split_mb << 20),
            split_writes_per_sec: (args.reshard_split_writes > 0.0).then_some(args.reshard_split_writes),
        },
        checkpoint_every: vlpds::retention::parse_duration(&args.checkpoint_every)?,
        checkpoint_stagger: args.checkpoint_stagger,
        preload_recent: args.preload_recent,
        forwarded_write_start: (args.forwarded_write_start_ms > 0).then(|| Duration::from_millis(args.forwarded_write_start_ms)),
        retry_unapplied_writes: args.retry_unapplied_writes,
    };
    cfg.check_secrets()?;
    if let Some(c) = &cfg.cluster {
        vlpds::metrics::export_lease_config(c.ttl, c.renew_every, c.skew);
    }
    vlpds::metrics::export_firehose_config(cfg.firehose_merge_queue_bytes, cfg.firehose_max_lag_bytes);
    if args.lease_ttl_ms < 10_000 && !args.dev_mode {
        tracing::warn!(
            lease_ttl_ms = args.lease_ttl_ms,
            "lease TTL below 10 s: a renewal slower than 0.4 x TTL fail-stops the node (see --lease-ttl-ms)"
        );
    }
    let listener = bind(&args.listen, args.listen_backlog).await?;
    let metrics_listener = match &metrics_addr {
        Some(a) => Some(bind(a, args.listen_backlog).await?),
        None => None,
    };
    let peer_listener = match &args.peer_listen {
        Some(a) => Some(bind(a, args.listen_backlog).await?),
        None => None,
    };
    let app = server::build(cfg).await?;
    if let Some(c) = app.node.shard_disk_cache() {
        tracing::info!(dir = %c.dir.display(), shard_mb = c.shard_bytes >> 20, "SST disk cache (per shard)");
    }
    server::spawn_reporters(&app);
    tracing::info!(listen = %args.listen, metrics_listen = metrics_addr.as_deref().unwrap_or("(app port)"), "vlpds serving");
    if let Some(l) = metrics_listener {
        server::spawn_metrics_listener(&app, l);
    }
    vlpds::xrpc::spawn_blob_gc(app.clone());
    vlpds::xrpc::export_account_totals(&app);
    vlpds::xrpc::spawn_reserved_key_gc(app.clone());
    vlpds::oauth::gc::spawn_gc(app.clone());
    vlpds::xrpc::key_rotation::spawn_recovery(app.clone());
    tokio::spawn(vlpds::xrpc::request_crawl(app.clone()));
    // Keep serving through a graceful shutdown: peers forward to us until
    // our handoff nudges reach them, and a forward we drop mid-request is
    // ambiguous to them (a client 503), while one we answer "not owned"
    // (ShardMoved) they resend to the new owner.
    if let Some(l) = peer_listener {
        server::spawn_peer_listener(&app, l)?;
        tracing::info!(peer_listen = args.peer_listen.as_deref().unwrap_or(""), "peer listener (mTLS, peer HTTP/2 settings)");
    }
    let router = server::public_router(&app);
    let opts = server::ServeOptions { h2: server::H2Profile::Public, max_connections: args.max_connections, tls: None };
    let mut serving = tokio::spawn(server::serve_with(listener, router, opts));
    tokio::select! {
        r = &mut serving => r?,
        _ = shutdown_signal() => {
            server::shutdown(&app).await;
            // let in-flight requests finish and peers' routing settle
            tokio::time::sleep(SHUTDOWN_DRAIN).await;
            tracing::info!("shutdown complete");
            Ok(())
        }
    }
}

async fn bind(addr: &str, backlog: u32) -> anyhow::Result<tokio::net::TcpListener> {
    let mut last = None;
    for a in tokio::net::lookup_host(addr).await? {
        let sock = if a.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
        // as std's TcpListener::bind: rebind while old connections sit in TIME_WAIT
        sock.set_reuseaddr(true)?;
        match sock.bind(a).and_then(|()| sock.listen(backlog)) {
            Ok(l) => return Ok(l),
            Err(e) => last = Some(e),
        }
    }
    Err(last.map_or_else(|| anyhow::anyhow!("{addr}: no address"), |e| anyhow::Error::from(e).context(format!("binding {addr}"))))
}

const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_millis(500);

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Else the reference PDS's environment variable.
fn flag_or_env(v: &Option<String>, k: &str) -> Option<String> {
    v.clone().filter(|v| !v.is_empty()).or_else(|| std::env::var(k).ok().filter(|v| !v.is_empty()))
}
