//! A shard handle. Shards are the unit of ownership and state: each has its
//! own SlateDB (`state/{shard:03}`), while all shards owned by a node share the
//! node's commit log (see nodelog.rs).

pub use crate::nodelog::{seq_floor, AckFn, LogEntry, Watermark};
use crate::nodelog::NodeLog;
use crate::store::Store;
use slatedb::Db;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Partition {
    pub id: u16,
    /// Ownership epoch (from the shard assignment).
    pub epoch: u64,
    pub db: Arc<Db>,
    /// Held (write) by the log finalizer across apply + ack; export readers
    /// take it (read) to pair a durable repo view with a SlateDB snapshot.
    pub apply_lock: Arc<tokio::sync::RwLock<()>>,
    /// The node log's intake (shared by every shard on this node).
    pub tx: mpsc::Sender<LogEntry>,
    /// The node log's watermark.
    pub wm: Arc<Watermark>,
    pub log: Arc<NodeLog>,
    /// Repos recently written here (preloaded by the shard's next owner).
    pub recent: Arc<RecentRepos>,
}

/// Default bound of [`RecentRepos`] per shard (`--preload-recent`).
pub const DEFAULT_RECENT_REPOS: usize = 2048;

/// The repos a shard committed to most recently, newest first, bounded.
/// Persisted with each checkpoint and at close (`nodelog::META_RECENT`,
/// only when the set's members changed), and preloaded by the shard's next
/// owner ([`crate::worker::spawn_preload`]), so the first writes after a
/// restart, takeover or handback find their repos warm (DESIGN.md §2). A
/// hint: a stale entry costs one load.
pub struct RecentRepos {
    cap: usize,
    inner: parking_lot::Mutex<(lru::LruCache<Arc<str>, ()>, bool)>,
}

impl Default for RecentRepos {
    fn default() -> Self {
        RecentRepos::new(DEFAULT_RECENT_REPOS)
    }
}

impl RecentRepos {
    /// `cap` 0 = track nothing.
    pub fn new(cap: usize) -> RecentRepos {
        RecentRepos { cap, inner: parking_lot::Mutex::new((lru::LruCache::unbounded(), false)) }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    /// `did` committed (a worker, once per commit batch).
    pub fn touch(&self, did: &Arc<str>) {
        if self.cap == 0 {
            return;
        }
        let mut g = self.inner.lock();
        if g.0.put(did.clone(), ()).is_none() {
            g.1 = true;
            if g.0.len() > self.cap {
                g.0.pop_lru();
            }
        }
    }

    /// Adds a previous owner's list (newest first) behind what's here.
    pub fn seed(&self, dids: &[Arc<str>]) {
        let mut g = self.inner.lock();
        for d in dids {
            if g.0.len() >= self.cap {
                break;
            }
            if !g.0.contains(d) {
                g.0.push(d.clone(), ());
                g.0.demote(d);
            }
        }
    }

    /// Whether the set changed since the last `take_dirty`.
    pub fn is_dirty(&self) -> bool {
        self.inner.lock().1
    }

    /// The set, newest first, if its members changed since the last call.
    pub fn take_dirty(&self) -> Option<bytes::Bytes> {
        let mut g = self.inner.lock();
        if !std::mem::take(&mut g.1) {
            return None;
        }
        let mut b = Vec::with_capacity(g.0.len() * 33);
        for (d, _) in g.0.iter() {
            b.extend_from_slice(d.as_bytes());
            b.push(b'\n');
        }
        Some(b.into())
    }

    pub fn decode(b: &[u8]) -> Vec<Arc<str>> {
        b.split(|&c| c == b'\n').filter(|d| !d.is_empty()).filter_map(|d| std::str::from_utf8(d).ok()).map(Arc::from).collect()
    }
}

/// One SST block/meta cache shared by every shard DB in the process. SlateDB's
/// default is a private 512 MiB block + 128 MiB meta cache per Db, which at
/// 256 shards per node (the old default) let the caches grow toward ~160 GiB
/// as reads touch more shards (the RSS creep seen at 1M–10M repos).
static BLOCK_CACHE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4 << 30);

/// Size of the shared block cache (the meta cache gets a quarter on top).
/// Takes effect only before the first shard DB opens.
pub fn set_block_cache_bytes(n: u64) {
    BLOCK_CACHE_BYTES.store(n.max(64 << 20), std::sync::atomic::Ordering::Relaxed);
}

static CACHE_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Benchmarks: DBs opened from now on don't see what the shared cache holds
/// for earlier opens of the same path (a restart starts cold).
pub fn bump_cache_epoch() {
    CACHE_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

fn shared_db_cache() -> Arc<dyn slatedb::db_cache::DbCache> {
    use slatedb::db_cache::{foyer::{FoyerCache, FoyerCacheOptions}, SplitCache};
    static CACHE: std::sync::OnceLock<Arc<dyn slatedb::db_cache::DbCache>> = std::sync::OnceLock::new();
    CACHE
        .get_or_init(|| {
            let mk = |cap| -> Arc<dyn slatedb::db_cache::DbCache> {
                Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: cap, ..Default::default() }))
            };
            let block = BLOCK_CACHE_BYTES.load(std::sync::atomic::Ordering::Relaxed);
            let meta: Arc<dyn slatedb::db_cache::DbCache> = Arc::new(MetaCache::new(block / 4));
            Arc::new(SplitCache::new().with_block_cache(Some(mk(block))).with_meta_cache(Some(meta)))
        })
        .clone()
}

/// The shared SST metadata cache (bloom filters, indexes, stats): read-mostly
/// shards under RwLocks with CLOCK eviction, so a hit takes a shared lock
/// and sets one bit. Foyer (the block cache) takes its shard's mutex on every
/// hit to update eviction state, and every point read of a repo checks the
/// same few SSTs' filters (one DB per repo's shard, up to 32 L0s plus the
/// sorted runs): the threads serialized on those hot keys. Point reads of one
/// 10M-record repo spent 32-66% of CPU spinning on that lock (getRecord
/// 24k/s on the laptop, 35k/s on benchbox with 32 IO threads vs 63k with 6).
pub struct MetaCache {
    shards: Vec<parking_lot::RwLock<MetaShard>>,
    shard_bytes: usize,
    hasher: std::hash::RandomState,
}

#[derive(Default)]
struct MetaShard {
    map: std::collections::HashMap<slatedb::db_cache::CachedKey, MetaSlot>,
    bytes: usize,
}

struct MetaSlot {
    entry: slatedb::db_cache::CachedEntry,
    size: usize,
    /// CLOCK bit: set on a hit, cleared by an eviction sweep that spares it
    used: std::sync::atomic::AtomicBool,
}

const META_SHARDS: usize = 64;

impl MetaCache {
    pub fn new(bytes: u64) -> MetaCache {
        MetaCache {
            shards: (0..META_SHARDS).map(|_| Default::default()).collect(),
            shard_bytes: (bytes as usize / META_SHARDS).max(1 << 20),
            hasher: Default::default(),
        }
    }

    fn shard(&self, key: &slatedb::db_cache::CachedKey) -> &parking_lot::RwLock<MetaShard> {
        use std::hash::BuildHasher;
        &self.shards[(self.hasher.hash_one(key) >> 32) as usize % META_SHARDS]
    }

    fn get(&self, key: &slatedb::db_cache::CachedKey) -> Option<slatedb::db_cache::CachedEntry> {
        use std::sync::atomic::Ordering::Relaxed;
        let s = self.shard(key).read();
        let slot = s.map.get(key)?;
        // load first: a hot entry's bit is already set, and a store would
        // bounce its cache line between threads
        if !slot.used.load(Relaxed) {
            slot.used.store(true, Relaxed);
        }
        Some(slot.entry.clone())
    }
}

#[async_trait::async_trait]
impl slatedb::db_cache::DbCache for MetaCache {
    async fn get_block(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }
    async fn get_index(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }
    async fn get_filter(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }
    async fn get_stats(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> {
        Ok(self.get(key))
    }
    async fn insert(&self, key: slatedb::db_cache::CachedKey, value: slatedb::db_cache::CachedEntry) {
        use std::sync::atomic::Ordering::Relaxed;
        let size = value.size();
        let mut s = self.shard(&key).write();
        let slot = MetaSlot { entry: value, size, used: std::sync::atomic::AtomicBool::new(false) };
        if let Some(old) = s.map.insert(key, slot) {
            s.bytes -= old.size;
        }
        s.bytes += size;
        // CLOCK over the shard: drop entries not hit since the last sweep,
        // clear the bit of the rest; a second pass evicts if all were hot
        for _ in 0..2 {
            if s.bytes <= self.shard_bytes {
                break;
            }
            let mut freed = 0;
            s.map.retain(|_, v| {
                if v.used.swap(false, Relaxed) {
                    true
                } else {
                    freed += v.size;
                    false
                }
            });
            s.bytes -= freed;
        }
    }
    async fn remove(&self, key: &slatedb::db_cache::CachedKey) {
        let mut s = self.shard(key).write();
        if let Some(old) = s.map.remove(key) {
            s.bytes -= old.size;
        }
    }
    fn entry_count(&self) -> u64 {
        self.shards.iter().map(|s| s.read().map.len() as u64).sum()
    }
}

/// SST block compression for shard DBs (`--sst-compression`). Each SST
/// records its codec, so a DB written with another one stays readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SstCompression {
    None,
    Lz4,
    Zstd,
}

impl std::str::FromStr for SstCompression {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "none" | "off" => SstCompression::None,
            "lz4" => SstCompression::Lz4,
            "zstd" => SstCompression::Zstd,
            _ => anyhow::bail!("unknown SST compression {s:?} (none, lz4, zstd)"),
        })
    }
}

impl SstCompression {
    fn codec(self) -> Option<slatedb::config::CompressionCodec> {
        match self {
            SstCompression::None => None,
            SstCompression::Lz4 => Some(slatedb::config::CompressionCodec::Lz4),
            SstCompression::Zstd => Some(slatedb::config::CompressionCodec::Zstd),
        }
    }
}

static SST_COMPRESSION: parking_lot::RwLock<SstCompression> = parking_lot::RwLock::new(SstCompression::Zstd);

/// Codec for shard DBs opened from now on (and their compaction output).
pub fn set_sst_compression(c: SstCompression) {
    *SST_COMPRESSION.write() = c;
}

/// SlateDB GC (`--slatedb-gc-min-age`): an SST no manifest or checkpoint
/// references is deleted once it is this old, counted from its *creation*.
/// It only guards SSTs written but not yet in a manifest (SlateDB also caps
/// the cutoff at the oldest running compaction and the newest L0), so it
/// doesn't protect reads: an SST created long ago and replaced now passes
/// any min age at once. Reads are protected by the compactor's checkpoint
/// lifetime below. (It was 24 h, which kept every SST a bulk import
/// replaced, ~3x the live bytes, for a day; see DESIGN.md §4.)
static GC_MIN_AGE_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(10 * 60);

pub fn set_gc_min_age(d: Duration) {
    GC_MIN_AGE_SECS.store(d.as_secs(), std::sync::atomic::Ordering::Relaxed);
}

/// Compactor checkpoint lifetime (`--slatedb-checkpoint-lifetime`). Before
/// each manifest update that replaces SSTs, the compactor writes a
/// checkpoint of the old manifest that lives this long, so GC keeps the
/// replaced SSTs for reads that started on it: a scan or snapshot (a
/// 10M-record getRepo streaming to a slow client, listRepos) must finish
/// within it. SlateDB's default is 15 min. It also bounds how long a bulk
/// import's replaced SSTs linger after each compaction.
static CHECKPOINT_LIFETIME_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(3600);

pub fn set_checkpoint_lifetime(d: Duration) {
    CHECKPOINT_LIFETIME_SECS.store(d.as_secs().max(60), std::sync::atomic::Ordering::Relaxed);
}

fn checkpoint_lifetime() -> Duration {
    Duration::from_secs(CHECKPOINT_LIFETIME_SECS.load(std::sync::atomic::Ordering::Relaxed))
}

/// How often a shard DB re-reads its manifest (`--slatedb-manifest-poll`,
/// SlateDB's default is 1 s). Each poll is two GETs (a probe of the next
/// manifest id, usually a 404, plus its GC boundary file), the largest
/// fixed per-shard request line (bench/results/cost-model-2026-10-02). The
/// node is the only writer of its shards' DBs, so reads never wait on it:
/// writes land in the memtable and the writer's own flushes update its
/// manifest in place. A poll only picks up the compactor's results, and
/// every flush's manifest CAS already reloads on a conflict. The one case
/// that waits on it, a writer whose view of L0 is full, is refreshed every
/// `FAST_POLL` while L0 runs deep instead (`spawn_compactor`).
static MANIFEST_POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(10_000);

pub fn set_manifest_poll_interval(d: Duration) {
    MANIFEST_POLL_MS.store((d.as_millis() as u64).max(100), std::sync::atomic::Ordering::Relaxed);
}

fn manifest_poll_interval() -> Duration {
    if cfg!(test) {
        return Duration::from_secs(1); // unit tests wait for compaction results
    }
    Duration::from_millis(MANIFEST_POLL_MS.load(std::sync::atomic::Ordering::Relaxed))
}

/// The compactor's and compaction worker's poll interval while L0 is
/// shallow (`--compaction-poll`; adaptive polling's slow mode and `slow`).
/// The coordinator reads two files per poll and the worker one, two GETs
/// each. Nothing waits on it while L0 stays shallow (shallow L0s cost only
/// bloom-filtered read amplification), and a deep L0 switches to
/// `FAST_POLL`.
static SLOW_POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(30_000);

pub fn set_compaction_poll_interval(d: Duration) {
    SLOW_POLL_MS.store((d.as_millis() as u64).max(100), std::sync::atomic::Ordering::Relaxed);
}

fn slow_poll() -> Duration {
    Duration::from_millis(SLOW_POLL_MS.load(std::sync::atomic::Ordering::Relaxed))
}

fn gc_options() -> slatedb::config::GarbageCollectorOptions {
    use slatedb::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions};
    let min_age = Duration::from_secs(GC_MIN_AGE_SECS.load(std::sync::atomic::Ordering::Relaxed));
    GarbageCollectorOptions {
        compacted_options: Some(GarbageCollectorDirectoryOptions { min_age, ..Default::default() }),
        ..Default::default()
    }
}

pub async fn open_db(
    store: &Store,
    partition: u16,
    cache_dir: Option<&std::path::Path>,
) -> anyhow::Result<Db> {
    // Per-shard LSM shape. A whole repo lives in one shard, so one shard must
    // absorb a bulk import (bench 2026-10-02 §4). With 8 MiB L0s and
    // SlateDB's default cap of 8, L0 filled in ~2 s at 25 MB/s and then
    // waited a full compaction cycle (coordinator poll 5 s + worker poll
    // 5 s ± 2.5 + commit 1 s + manifest poll 1 s): 4-11 s write stalls, and
    // since the finalizer awaits every shard's apply, the whole node stalled.
    // Now L0 holds 32 x 16 MiB = 512 MiB, more than a slow cycle's worth of
    // ingest, so compaction catches up without backpressure (one shard,
    // 2M records, 10 ms store calls: worst write 11.8 s -> 6 ms at 50k
    // records/s, 34 ms at 80k/s; tests/all/shard_ingest.rs). L0s live in the
    // object store and are bloom-filtered, so the cost is read
    // amplification only while compaction lags. Compactor polling is
    // adaptive (`spawn_compactor`): 30 s polls while L0 is shallow, 500 ms
    // (plus writer manifest refreshes) while it runs deep, so unpaced bursts
    // are absorbed without the idle GETs of always-fast polls
    // (tests/all/compaction_polling.rs, tests/all/cost_defaults.rs).
    //
    // Memory: the active memtable freezes at 16 MiB (the 10 s node
    // checkpoint flushes idle shards' sooner), so memtables total at most
    // min(shards x 16 MiB, ingest rate x 10 s). `max_unflushed_bytes` only
    // binds on a shard whose flushes are blocked, and while one shard is
    // blocked the node log's finalizer stops feeding every shard, so the
    // blocked shard's 128 MiB is the only excess: no node-wide budget needed.
    let mut settings = slatedb::Settings {
        wal_enabled: false,
        flush_interval: Some(Duration::from_millis(100)),
        l0_sst_size_bytes: 16 << 20,
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        // room for the active memtable plus l0_flush_parallelism (4) uploads
        max_unflushed_bytes: 128 << 20,
        manifest_poll_interval: manifest_poll_interval(),
        compression_codec: SST_COMPRESSION.read().codec(),
        garbage_collector_options: Some(gc_options()),
        // started after the open (`spawn_compactor`): half of an open's
        // sequential store calls were the embedded compactor's startup
        compactor_options: None,
        ..Default::default()
    };
    if let Some(dir) = cache_dir {
        // Local disk cache of SST parts: restarts and takeovers start warm
        // instead of turning every cold repo load into object store GETs.
        let oc = &mut settings.object_store_cache_options;
        oc.root_folder = Some(dir.join(format!("{partition:03}")));
        oc.cache_on_flush = true;
        oc.cache_on_compaction = true;
    }
    let path = db_path(store, partition);
    // cache ids only need to be distinct per DB in this process (tests open
    // several prefixes with the same shard numbers)
    let cache_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        path.hash(&mut h);
        CACHE_EPOCH.load(std::sync::atomic::Ordering::Relaxed).hash(&mut h);
        h.finish()
    };
    let codec = settings.compression_codec;
    let db = crate::metrics::with_slatedb_metrics(Db::builder(path.clone(), store.raw.clone()))
        .with_settings(settings)
        .with_db_cache(shared_db_cache(), cache_id)
        .with_sst_block_size(SST_BLOCK_SIZE)
        .build()
        .await?;
    let raw = external_sst_redirect(&db, &path, store.raw.clone());
    spawn_compactor(&db, path, raw, codec);
    Ok(db)
}

/// A shard cloned from others (split/merge, DESIGN.md "Online shard
/// split/merge") reads its ancestors' SSTs in place until compaction
/// rewrites them. The DB itself resolves them through its manifest, but a
/// standalone compactor and compaction worker only know the DB's root, so
/// they get a store that redirects those SST paths to their owners. The
/// set only shrinks after the open (compaction drops external SSTs).
fn external_sst_redirect(db: &Db, path: &str, raw: Arc<dyn object_store::ObjectStore>) -> Arc<dyn object_store::ObjectStore> {
    let m = db.manifest();
    let ext = m.external_dbs();
    if ext.is_empty() {
        return raw;
    }
    let mut map = std::collections::HashMap::new();
    for e in ext {
        let resolver = slatedb::PathResolver::new(path.to_string(), &m);
        for id in &e.sst_ids {
            let theirs = resolver.sst_path(id);
            let ours = object_store::path::Path::from(theirs.as_ref().replacen(e.path.as_str(), path, 1));
            map.insert(ours, theirs);
        }
    }
    Arc::new(Redirect { inner: raw, map })
}

#[derive(Debug)]
struct Redirect {
    inner: Arc<dyn object_store::ObjectStore>,
    map: std::collections::HashMap<object_store::path::Path, object_store::path::Path>,
}

impl std::fmt::Display for Redirect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Redirect({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Redirect {
    async fn put_opts(&self, location: &object_store::path::Path, payload: object_store::PutPayload, opts: object_store::PutOptions) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &object_store::path::Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &object_store::path::Path, options: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(self.map.get(location).unwrap_or(location), options).await
    }
    fn delete_stream(&self, locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &object_store::path::Path, to: &object_store::path::Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Where shard `id`'s SlateDB lives.
pub fn db_path(store: &Store, id: u16) -> String {
    format!("{}/state/{:03}", store.prefix, id)
}

/// Creates shard `child`'s SlateDB as a clone of `sources` (shard id, slots
/// [lo, hi)), each projected to its slots: a split clones one parent per
/// child, a merge clones both parents into one. O(manifest): the child
/// references the sources' SSTs (pinned by a checkpoint in each source)
/// until its compaction rewrites them. The sources must be closed (all
/// their state in SSTs). Idempotent: a retry finds the clone initialized.
pub async fn clone_db(store: &Store, child: u16, sources: &[(u16, u32, u32)]) -> anyhow::Result<()> {
    use std::ops::Bound;
    anyhow::ensure!(!sources.is_empty(), "clone of shard {child} without sources");
    let spec = |&(id, lo, hi): &(u16, u32, u32)| {
        let (a, b) = crate::state::slot_range_keys(lo, hi);
        slatedb::CloneSourceSpec::new(db_path(store, id)).with_projection_range((Bound::Included(a), Bound::Excluded(b)))
    };
    let admin = slatedb::admin::AdminBuilder::new(db_path(store, child), store.raw.clone()).build();
    let mut b = admin.create_clone_builder_from_source(spec(&sources[0]));
    for s in &sources[1..] {
        b = b.with_source(spec(s));
    }
    b.build().await?;
    Ok(())
}

const SST_BLOCK_SIZE: slatedb::SstBlockSize = slatedb::SstBlockSize::Block16Kib;

/// Runs a shard's compactor (coordinator + one worker writing the DB's SST
/// format) until the DB closes. Started after the open instead of inside it:
/// the embedded compactor's startup (~12 sequential store calls) doubled the
/// time from a takeover or handback to serving (tests/all/rebalance_handback.rs;
/// `open_calls` below: 465 -> 225 ms at 20 ms per call). L0 SSTs flushed
/// meanwhile wait for it, like any compaction cycle. Its outputs aren't
/// written into the local SST disk cache (`cache_on_compaction`); reads
/// cache them.
fn spawn_compactor(db: &Db, path: String, raw: Arc<dyn object_store::ObjectStore>, codec: Option<slatedb::config::CompressionCodec>) {
    spawn_deep_refresh(db);
    let mut status = db.subscribe();
    let watch = db.subscribe();
    tokio::spawn(async move {
        let polling = compaction_polling();
        let mut fast = polling == CompactionPolling::Fast;
        loop {
            let (compactor, worker) = match build_compactor(&path, &raw, codec, fast).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(%path, "compaction worker failed to start: {e}");
                    return;
                }
            };
            // SlateDB marks the DB closed *before* its final memtable flush,
            // and that flush waits for L0 room when L0 is full (a close
            // under bulk ingest: a handback, or freezing a hot shard to
            // split it). So keep compacting until the DB is gone (every
            // handle dropped), at most CLOSE_GRACE: stopping at the close
            // mark deadlocked such a close forever.
            let closed = async {
                while status.borrow_and_update().close_reason.is_none() {
                    if status.changed().await.is_err() {
                        return;
                    }
                }
                let gone = async { while status.changed().await.is_ok() {} };
                let _ = tokio::time::timeout(CLOSE_GRACE, gone).await;
            };
            let mut switch = false;
            tokio::select! {
                r = compactor.run() => if let Err(e) = r { tracing::warn!(%path, "compactor exited: {e}") },
                r = worker.run() => if let Err(e) = r { tracing::warn!(%path, "compaction worker exited: {e}") },
                _ = closed => {}
                _ = mode_change(&watch, fast), if polling == CompactionPolling::Adaptive => switch = true,
            }
            // a graceful stop hands claimed jobs back as Scheduled, so the
            // restarted worker picks them up again
            let _ = compactor.stop().await;
            let _ = worker.stop().await;
            if !switch {
                return;
            }
            fast = !fast;
            crate::metrics::COMPACTION_POLL_MODE.with_label_values(&[if fast { "fast" } else { "slow" }]).inc();
            tracing::debug!(%path, fast, "compaction polling switched");
        }
    });
}

/// While the writer's L0 runs deep (>= `DEEP_L0`), re-reads its manifest
/// every `FAST_POLL` instead of waiting for the (10 s) manifest poll. A
/// writer only learns that compaction freed L0 from a manifest read: its
/// flushes' CAS conflicts reload it, but once its view of L0 is full no
/// flush runs, and only a refresh unblocks it. Holds a handle until the
/// DB is closed (a DB dropped unclosed is fenced by its next opener, which
/// closes it too).
fn spawn_deep_refresh(db: &Db) {
    let db = db.clone();
    let mut status = db.subscribe();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(FAST_POLL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                r = status.changed() => if r.is_err() { return },
                _ = tick.tick() => {
                    if status.borrow().current_manifest.l0().len() >= DEEP_L0 {
                        let _ = db.refresh_manifest().await;
                    }
                }
            }
            if status.borrow().close_reason.is_some() {
                return;
            }
        }
    });
}

/// How long a closed shard's compactor keeps running for its final flush
/// (see `spawn_compactor`) if some handle outlives the close.
const CLOSE_GRACE: Duration = Duration::from_secs(60);

/// How a shard's compactor polls for work (`--compaction-polling`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionPolling {
    /// `--compaction-poll` (30 s) always: cheapest idle, but an unpaced bulk
    /// ingest into one shard fills L0 between cycles and backpressures.
    Slow,
    /// 500 ms polls always: ~10x the idle GETs.
    Fast,
    /// Slow while L0 is shallow, fast while it is deep (the default).
    Adaptive,
}

impl std::str::FromStr for CompactionPolling {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "slow" => CompactionPolling::Slow,
            "fast" => CompactionPolling::Fast,
            "adaptive" => CompactionPolling::Adaptive,
            _ => anyhow::bail!("unknown compaction polling {s:?} (slow, fast, adaptive)"),
        })
    }
}

static COMPACTION_POLLING: parking_lot::RwLock<CompactionPolling> = parking_lot::RwLock::new(CompactionPolling::Adaptive);

/// Compaction polling of shard DBs opened from now on.
pub fn set_compaction_polling(p: CompactionPolling) {
    *COMPACTION_POLLING.write() = p;
}

fn compaction_polling() -> CompactionPolling {
    *COMPACTION_POLLING.read()
}

/// Poll interval of the fast mode (the slow one is `slow_poll()`).
const FAST_POLL: Duration = Duration::from_millis(500);
/// Adaptive: go fast at this many L0 SSTs (a quarter of `l0_max_ssts`: the
/// writer is producing them faster than slow cycles drain them), back to
/// slow once L0 has stayed at or below `CALM_L0` for `CALM_FOR`.
const DEEP_L0: usize = 8;
const CALM_L0: usize = 2;
const CALM_FOR: Duration = Duration::from_secs(15);

/// Resolves when an adaptive compactor in mode `fast` should switch. Reads
/// L0 from the DB's status (holding no handle, so the DB can drop).
async fn mode_change(status: &tokio::sync::watch::Receiver<slatedb::DbStatus>, fast: bool) {
    let mut calm_since = None;
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let l0 = status.borrow().current_manifest.l0().len();
        if !fast {
            if l0 >= DEEP_L0 {
                return;
            }
            continue;
        }
        if l0 > CALM_L0 {
            calm_since = None;
        } else if calm_since.get_or_insert_with(std::time::Instant::now).elapsed() >= CALM_FOR {
            return;
        }
    }
}

/// A shard's compaction coordinator and worker, polling slow or fast.
async fn build_compactor(
    path: &str,
    raw: &Arc<dyn object_store::ObjectStore>,
    codec: Option<slatedb::config::CompressionCodec>,
    fast: bool,
) -> Result<(slatedb::compactor::Compactor, slatedb::CompactionWorker), slatedb::Error> {
    use slatedb::config::{CompactionWorkerOptions, CompactorOptions};
    let poll = if cfg!(test) {
        Duration::from_millis(100) // unit tests wait for compactions
    } else if fast {
        FAST_POLL
    } else {
        slow_poll()
    };
    let opts = CompactorOptions { worker: None, checkpoint_lifetime: checkpoint_lifetime(), poll_interval: poll, ..Default::default() };
    let worker_opts = CompactionWorkerOptions { compression_codec: codec, compactions_poll_interval: poll, ..Default::default() };
    let compactor = slatedb::CompactorBuilder::new(path.to_string(), raw.clone()).with_options(opts);
    let worker = slatedb::CompactionWorkerBuilder::new(path.to_string(), raw.clone()).with_options(worker_opts).with_sst_block_size(SST_BLOCK_SIZE);
    #[cfg(feature = "slatedb-metrics")]
    let (compactor, worker) = (compactor.with_metrics_recorder(crate::metrics::slatedb_recorder()), worker.with_metrics_recorder(crate::metrics::slatedb_recorder()));
    Ok((compactor.build(), worker.build().await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Process CPU time (user + system), so contention on a busy machine
    /// inflates the wall times below but not these.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn cpu() -> Duration {
        #[repr(C)]
        struct Timeval {
            sec: i64,
            #[cfg(target_os = "macos")]
            usec: i32,
            #[cfg(target_os = "linux")]
            usec: i64,
        }
        #[repr(C)]
        struct Rusage {
            utime: Timeval,
            stime: Timeval,
            rest: [i64; 14],
        }
        extern "C" {
            fn getrusage(who: i32, usage: *mut Rusage) -> i32;
        }
        let mut r: Rusage = unsafe { std::mem::zeroed() };
        unsafe { getrusage(0, &mut r) };
        let t = |v: &Timeval| Duration::from_secs(v.sec as u64) + Duration::from_micros(v.usec as u64);
        t(&r.utime) + t(&r.stime)
    }

    /// SST bytes and read/write time per codec on real records: a repo CAR
    /// (`VLPDS_BENCH_CAR`, e.g. a getRepo export) written as `VLPDS_BENCH_COPIES`
    /// repos (default 4) of state rows (R/ value + c/ index key, as the worker
    /// writes them), flushed to L0 SSTs, then reopened cold and read back.
    /// `VLPDS_BENCH_CAR=~/repo.car cargo test --lib partition::tests::compression -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn compression() {
        use crate::cid::Cid;
        use std::collections::HashMap;
        let path = std::env::var("VLPDS_BENCH_CAR").expect("VLPDS_BENCH_CAR=path/to/repo.car");
        let copies: usize = std::env::var("VLPDS_BENCH_COPIES").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
        let car = std::fs::read(path).unwrap();
        let (roots, blocks) = crate::car::read_car(&car).unwrap();
        let blocks: HashMap<Cid, Vec<u8>> = blocks.into_iter().map(|(c, b)| (c, b.to_vec())).collect();
        let commit = crate::cbor::Value::decode(&blocks[&roots[0]]).unwrap();
        let Some(crate::cbor::Value::Link(data)) = commit.get("data") else { panic!("no data in commit") };
        let tree = crate::mst::Tree::load_from_blocks(&blocks, *data).unwrap();
        let mut records = Vec::new();
        tree.walk(&mut |k, c| records.push((String::from_utf8(k.to_vec()).unwrap(), c)));
        let raw: usize = records.iter().map(|(_, c)| blocks[c].len()).sum();
        println!("{} records x {copies} repos, {:.1} MiB of record bytes per repo", records.len(), raw as f64 / (1 << 20) as f64);
        for codec in [SstCompression::None, SstCompression::Lz4, SstCompression::Zstd] {
            set_sst_compression(codec);
            // two copies: one read by a scan, one by point gets, each cold
            let mut stores = Vec::new();
            let (mut write, mut sst, mut logical) = (Duration::ZERO, 0u64, 0usize);
            for half in ["scan", "get"] {
                let store = Store { prefix: format!("bench-{codec:?}-{half}"), ..Store::memory(None) };
                let db = open_db(&store, 0, None).await.unwrap();
                let t = cpu();
                logical = 0;
                for r in 0..copies {
                    let did = crate::state::bulk_did(r as u64);
                    for chunk in records.chunks(2000) {
                        let mut wb = slatedb::WriteBatch::new();
                        for (path, cid) in chunk {
                            let v = crate::state::record_value(cid, 1, &blocks[cid]);
                            let k = crate::state::record_key(&did, path);
                            let ck = crate::state::record_cid_key(&did, cid, path);
                            logical += k.len() + v.len() + ck.len();
                            wb.put(&k, &v);
                            wb.put(&ck, b"");
                        }
                        db.write(wb).await.unwrap();
                    }
                }
                db.close().await.unwrap();
                write = cpu() - t;
                sst = 0;
                let prefix = object_store::path::Path::from(format!("{}/state/000", store.prefix));
                let mut list = store.raw.list(Some(&prefix));
                use futures::StreamExt;
                while let Some(m) = list.next().await {
                    let m = m.unwrap();
                    if m.location.as_ref().ends_with(".sst") {
                        sst += m.size;
                    }
                }
                stores.push(store);
            }
            let db = open_db(&stores[0], 0, None).await.unwrap();
            let t = cpu();
            let mut n = 0;
            let mut it = db.scan(b"R/".to_vec()..b"R0".to_vec()).await.unwrap();
            while let Some(_kv) = it.next().await.unwrap() {
                n += 1;
            }
            let scan = cpu() - t;
            drop(it);
            db.close().await.unwrap();
            let db = open_db(&stores[1], 0, None).await.unwrap();
            let step = (records.len() / 5000).max(1);
            let t = cpu();
            let mut gets = 0;
            for r in 0..copies {
                let did = crate::state::bulk_did(r as u64);
                for (path, _) in records.iter().skip(r).step_by(step * copies) {
                    assert!(db.get(crate::state::record_key(&did, path)).await.unwrap().is_some());
                    gets += 1;
                }
            }
            let get = cpu() - t;
            db.close().await.unwrap();
            println!(
                "{codec:?}: SST {:.1} MiB ({:.2}x of {:.1} MiB of rows); CPU: write+flush {:.0} ms, cold scan of {n} rows {:.0} ms, {gets} cold gets {:.1} us/get",
                sst as f64 / (1 << 20) as f64,
                logical as f64 / sst as f64,
                logical as f64 / (1 << 20) as f64,
                write.as_secs_f64() * 1e3,
                scan.as_secs_f64() * 1e3,
                get.as_secs_f64() * 1e6 / gets as f64,
            );
        }
        set_sst_compression(SstCompression::Zstd);
    }

    /// The compactor started after the open compacts L0 into sorted runs
    /// that read back (in the DB's SST format), and stops with the DB.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_compactor_compacts() {
        let store = Store { prefix: "compact".into(), ..Store::memory(None) };
        let db = open_db(&store, 0, None).await.unwrap();
        for i in 0..8u32 {
            for j in 0..200u32 {
                db.put(format!("k{j:04}"), format!("value {i} {j} {}", "x".repeat(100))).await.unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        let t = std::time::Instant::now();
        loop {
            let m = db.manifest();
            if m.l0().len() < 8 && !m.compacted().is_empty() {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(30), "no compaction: {} L0s", m.l0().len());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // the manifest update that replaced the L0s carries the compactor's
        // checkpoint of the old manifest, with our lifetime (what keeps the
        // replaced SSTs for in-flight reads, and nothing longer)
        let m = db.manifest();
        let cp = m.checkpoints().iter().filter_map(|c| Some(c.expire_time? - c.create_time)).max().expect("compactor checkpoint");
        assert_eq!(cp.num_seconds() as u64, checkpoint_lifetime().as_secs());
        db.close().await.unwrap();
        let db = open_db(&store, 0, None).await.unwrap();
        assert_eq!(db.get(b"k0123").await.unwrap().as_deref(), Some(format!("value 7 123 {}", "x".repeat(100)).as_bytes()));
        db.close().await.unwrap();
    }

    /// Object-store calls (sequential round trips) a shard open makes: a
    /// fresh DB, then a reopen of one with data, on a store that takes 20 ms
    /// per call. `cargo test --lib partition::tests::open_calls -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn open_calls() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let mem = Arc::new(object_store::memory::InMemory::new());
        let d = Duration::from_millis(20);
        let cfg = ThrottleConfig { wait_get_per_call: d, wait_put_per_call: d, wait_list_per_call: d, wait_delete_per_call: d, ..Default::default() };
        let store = Store { raw: Arc::new(ThrottledStore::new(mem, cfg)), ..Store::memory(None) };
        for round in ["fresh", "reopen"] {
            let t = std::time::Instant::now();
            let db = open_db(&store, 0, None).await.unwrap();
            let open = t.elapsed();
            db.put(b"k", b"v").await.unwrap();
            db.close().await.unwrap();
            println!("{round}: open {:.0} ms (~{:.0} calls at 20 ms)", open.as_secs_f64() * 1e3, open.as_secs_f64() / 0.02);
        }
    }

}

#[cfg(test)]
mod clone_tests {
    use super::*;

    fn k(slot: u16, rest: &str) -> Vec<u8> {
        crate::state::slot_family(slot, rest.as_bytes())
    }

    async fn count(db: &Db) -> usize {
        let mut n = 0;
        let mut it = db.scan(..).await.unwrap();
        while it.next().await.unwrap().is_some() {
            n += 1;
        }
        n
    }

    /// A split is two projected clones of the parent, a merge one clone of
    /// two sources: each child sees exactly its slots (never the parent's
    /// shard-wide keys), writes and compacts on its own (its compactor reads
    /// inherited SSTs through the redirect), and clones again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn split_and_merge_by_clone() {
        let store = Store { prefix: "clone".into(), ..Store::memory(None) };
        let db = open_db(&store, 0, None).await.unwrap();
        for s in [0u16, 100, 32767, 32768, 50000, 65535] {
            for i in 0..50 {
                db.put(k(s, &format!("h/did{i:03}")), format!("v{s}-{i}")).await.unwrap();
            }
        }
        db.put(crate::nodelog::META_APPLIED, b"m").await.unwrap();
        db.close().await.unwrap();
        clone_db(&store, 1, &[(0, 0, 32768)]).await.unwrap();
        clone_db(&store, 1, &[(0, 0, 32768)]).await.expect("a retried clone is a no-op");
        clone_db(&store, 2, &[(0, 32768, 65536)]).await.unwrap();
        // an empty parent clones too
        open_db(&store, 9, None).await.unwrap().close().await.unwrap();
        clone_db(&store, 10, &[(9, 0, 100)]).await.unwrap();
        let e = open_db(&store, 10, None).await.unwrap();
        assert_eq!(count(&e).await, 0);
        e.close().await.unwrap();

        let c1 = open_db(&store, 1, None).await.unwrap();
        let c2 = open_db(&store, 2, None).await.unwrap();
        assert!(c1.get(crate::nodelog::META_APPLIED).await.unwrap().is_none(), "shard-wide keys stay with the parent");
        assert!(c1.get(k(100, "h/did001")).await.unwrap().is_some());
        assert!(c1.get(k(50000, "h/did001")).await.unwrap().is_none());
        assert!(c2.get(k(50000, "h/did001")).await.unwrap().is_some());
        assert_eq!((count(&c1).await, count(&c2).await), (150, 150));
        for r in 0..10 {
            for i in 0..50 {
                c1.put(k(100, &format!("h/new{r}{i:03}")), "x").await.unwrap();
                c1.delete(k(0, &format!("h/did{i:03}"))).await.unwrap();
            }
            c1.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
        }
        let t = std::time::Instant::now();
        loop {
            let m = c1.manifest();
            if m.l0().len() < 4 && !m.compacted().is_empty() {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(30), "child never compacted: {} L0s", m.l0().len());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(c1.get(k(0, "h/did001")).await.unwrap().is_none());
        assert!(c1.get(k(100, "h/did001")).await.unwrap().is_some());
        c1.close().await.unwrap();
        c2.close().await.unwrap();
        clone_db(&store, 3, &[(1, 0, 32768), (2, 32768, 65536)]).await.unwrap();
        let m = open_db(&store, 3, None).await.unwrap();
        assert_eq!(count(&m).await, 600 + 150);
        m.put(k(65535, "h/zz"), "z").await.unwrap();
        m.close().await.unwrap();
        let m = open_db(&store, 3, None).await.unwrap();
        assert!(m.get(k(65535, "h/zz")).await.unwrap().is_some());
        assert!(m.get(k(100, "h/new9001")).await.unwrap().is_some());
        m.close().await.unwrap();
    }

    /// Flushes small L0s into `db` until its compactor has drained every L0
    /// SST it holds now (the inherited ones included).
    async fn compact_away_l0(db: &Db) {
        let before: Vec<_> = db.manifest().l0().iter().map(|v| v.sst.id).collect();
        let t = std::time::Instant::now();
        for i in 0.. {
            let m = db.manifest();
            if !m.l0().iter().any(|v| before.contains(&v.sst.id)) {
                return;
            }
            assert!(t.elapsed() < Duration::from_secs(60), "never compacted: {} L0s", m.l0().len());
            db.put(k(100, &format!("h/filler{i}")), "f").await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = db.refresh_manifest().await;
        }
    }

    /// Regression (split_and_merge_under_write_load's lost acked write): a
    /// split's halves both inherit the parent's L0 SSTs as views with the
    /// parent's view ids, so merging them back gave the union one view id
    /// twice (once per half). SlateDB's compactor keys L0 views by id: it
    /// compacted one half's view and dropped both, losing every key of the
    /// other half that was still in those L0s.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merging_a_splits_halves_keeps_their_shared_l0s() {
        let store = Store { prefix: "smc".into(), ..Store::memory(None) };
        let db = open_db(&store, 0, None).await.unwrap();
        let slots = [10u16, 20000, 32767, 32768, 40000, 65535];
        let mut wb = slatedb::WriteBatch::new();
        for s in slots {
            wb.put(k(s, "h/x"), format!("v{s}"));
        }
        db.write(wb).await.unwrap();
        db.close().await.unwrap(); // flushes them into one L0, spanning both halves
        clone_db(&store, 1, &[(0, 0, 32768)]).await.unwrap();
        clone_db(&store, 2, &[(0, 32768, 65536)]).await.unwrap();
        clone_db(&store, 3, &[(1, 0, 32768), (2, 32768, 65536)]).await.unwrap();
        let m = open_db(&store, 3, None).await.unwrap();
        let ids: Vec<_> = m.manifest().l0().iter().map(|v| v.id).collect();
        compact_away_l0(&m).await;
        let mut lost = Vec::new();
        for s in slots {
            if m.get(k(s, "h/x")).await.unwrap().is_none() {
                lost.push(s);
            }
        }
        assert!(lost.is_empty(), "keys of slots {lost:?} lost by the merged shard's compaction (L0 view ids {ids:?})");
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "L0 view ids repeat in the merged shard: {ids:?}");
        m.close().await.unwrap();
    }

    /// Model check: random generations of split/merge clones with writes,
    /// flushes and compactions in between; every key ever written stays
    /// readable from the shard holding its slot. Timing-dependent (whether a
    /// shard compacted its inherited L0s before the next clone), so a seed
    /// sweep, not a CI test (it found the repeated-L0-view-id loss above):
    /// `for s in $(seq 1 20); do SEED=$s cargo test --lib generations_of_clones -- --ignored; done`
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn generations_of_clones_keep_every_key() {
        use rand::{Rng, SeedableRng};
        let seed: u64 = std::env::var("SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let store = Store { prefix: format!("gen{seed}"), ..Store::memory(None) };
        let mut model: std::collections::BTreeMap<Vec<u8>, (u16, String, u16, usize)> = Default::default();
        // (id, lo, hi, db)
        let mut shards: Vec<(u16, u32, u32, Db)> = Vec::new();
        for i in 0..4u16 {
            let lo = i as u32 * 16384;
            shards.push((i, lo, lo + 16384, open_db(&store, i, None).await.unwrap()));
        }
        let mut next_id = 4u16;
        let mut n = 0u64;
        let mut log: Vec<String> = Vec::new();
        for step in 0..40 {
            // writes into every shard, some flushes, sometimes enough L0s to compact
            for (sid, lo, hi, db) in &shards {
                let flushes = if rng.gen_bool(0.3) { 6 } else { rng.gen_range(0..3) };
                for _ in 0..=flushes {
                    for _ in 0..20 {
                        let slot = rng.gen_range(*lo..*hi) as u16;
                        let key = k(slot, &format!("h/{n:08}"));
                        db.put(&key, format!("v{n}")).await.unwrap();
                        model.insert(key, (slot, format!("v{n}"), *sid, step));
                        n += 1;
                    }
                    db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
                }
            }
            if rng.gen_bool(0.5) {
                tokio::time::sleep(Duration::from_millis(rng.gen_range(0..600))).await;
            }
            check(&shards, &model, &log, seed, step, "after writes").await;
            // an op
            let split = shards.len() < 3 || (shards.len() < 8 && rng.gen_bool(0.5));
            if split {
                let i = rng.gen_range(0..shards.len());
                let (id, lo, hi, db) = shards.remove(i);
                if hi - lo < 2 {
                    shards.insert(i, (id, lo, hi, db));
                    continue;
                }
                db.close().await.unwrap();
                let mid = (lo + hi) / 2;
                let (a, b) = (next_id, next_id + 1);
                next_id += 2;
                clone_db(&store, a, &[(id, lo, mid)]).await.unwrap();
                clone_db(&store, b, &[(id, mid, hi)]).await.unwrap();
                log.push(format!("step {step}: split {id} [{lo},{hi}) -> {a} [{lo},{mid}), {b} [{mid},{hi})"));
                shards.insert(i, (b, mid, hi, open_db(&store, b, None).await.unwrap()));
                shards.insert(i, (a, lo, mid, open_db(&store, a, None).await.unwrap()));
            } else {
                let i = rng.gen_range(0..shards.len() - 1);
                let (x, xlo, xhi, xdb) = shards.remove(i);
                let (y, ylo, yhi, ydb) = shards.remove(i);
                xdb.close().await.unwrap();
                ydb.close().await.unwrap();
                let m = next_id;
                next_id += 1;
                clone_db(&store, m, &[(x, xlo, xhi), (y, ylo, yhi)]).await.unwrap();
                log.push(format!("step {step}: merge {x} [{xlo},{xhi}) + {y} [{ylo},{yhi}) -> {m}"));
                shards.insert(i, (m, xlo, yhi, open_db(&store, m, None).await.unwrap()));
            }
            check(&shards, &model, &log, seed, step, "after op").await;
        }
        for (_, _, _, db) in shards {
            db.close().await.unwrap();
        }
    }

    /// Every key of `model` reads back its value from the shard holding its slot.
    async fn check(shards:&[(u16, u32, u32, Db)], model: &std::collections::BTreeMap<Vec<u8>, (u16, String, u16, usize)>, log: &[String], seed: u64, step: usize, when: &str) {
        let mut missing = Vec::new();
        for (key, (slot, v, wid, wstep)) in model {
            let (id, _, _, db) = shards.iter().find(|(_, lo, hi, _)| (*slot as u32) >= *lo && (*slot as u32) < *hi).unwrap();
            match db.get(key).await.unwrap() {
                Some(got) if got.as_ref() == v.as_bytes() => {}
                other => missing.push((*id, *slot, v.clone(), other.is_some(), *wid, *wstep)),
            }
        }
        if !missing.is_empty() {
            for l in log {
                eprintln!("{l}");
            }
            for (id, _, _, db) in shards {
                let m = db.manifest();
                eprintln!("shard {id}: l0 {} srs {} ext {:?}", m.l0().len(), m.compacted().len(), m.external_dbs().iter().map(|e| (e.path.clone(), e.sst_ids.len())).collect::<Vec<_>>());
            }
            panic!("seed {seed} step {step} {when}: {} of {} keys missing, e.g. (shard, slot, v, present, written to, at step) {:?}", missing.len(), model.len(), &missing[..missing.len().min(8)]);
        }
    }

    /// FamilyScan walks one family across slots, skipping other families and
    /// empty slots, from any start key.
    #[tokio::test]
    async fn family_scan_skips_other_families() {
        let store = Store { prefix: "fam".into(), ..Store::memory(None) };
        let db = open_db(&store, 0, None).await.unwrap();
        for s in [3u16, 7, 9, 65535] {
            for f in ["C/x\0", "R/", "a/", "h/", "n/", "p/"] {
                db.put(k(s, &format!("{f}d{s}")), b"").await.unwrap();
            }
        }
        db.put(k(8, "R/only-records"), b"").await.unwrap();
        db.put(crate::nodelog::META_APPLIED, b"m").await.unwrap();
        let scan = |fam: &'static [u8], start: Option<Vec<u8>>| {
            let db = &db;
            async move {
                let mut it = crate::state::FamilyScan::new(db, fam, start, &Default::default()).await.unwrap();
                let mut out = Vec::new();
                while let Some(kv) = it.next().await.unwrap() {
                    out.push(String::from_utf8_lossy(crate::state::key_body(&kv.key)).into_owned());
                }
                out
            }
        };
        assert_eq!(scan(b"h/", None).await, vec!["h/d3", "h/d7", "h/d9", "h/d65535"]);
        assert_eq!(scan(b"a/", Some(k(7, "a/d7\0"))).await, vec!["a/d9", "a/d65535"]);
        assert_eq!(scan(b"p/", Some(k(9, "a/"))).await, vec!["p/d9", "p/d65535"]);
        assert_eq!(scan(b"C/x\0", None).await.len(), 4);
        assert!(scan(b"M/", None).await.is_empty());
        db.close().await.unwrap();
    }
}
