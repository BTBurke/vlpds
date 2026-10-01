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
}

/// One SST block/meta cache shared by every shard DB in the process. SlateDB's
/// default is a private 512 MiB block + 128 MiB meta cache per Db, which at
/// 256 shards per node lets the caches grow toward ~160 GiB as reads touch
/// more shards (the RSS creep seen at 1M–10M repos).
static BLOCK_CACHE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4 << 30);

/// Size of the shared block cache (the meta cache gets a quarter on top).
/// Takes effect only before the first shard DB opens.
pub fn set_block_cache_bytes(n: u64) {
    BLOCK_CACHE_BYTES.store(n.max(64 << 20), std::sync::atomic::Ordering::Relaxed);
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
            Arc::new(SplitCache::new().with_block_cache(Some(mk(block))).with_meta_cache(Some(mk(block / 4))))
        })
        .clone()
}

pub async fn open_db(
    store: &Store,
    partition: u16,
    cache_dir: Option<&std::path::Path>,
) -> anyhow::Result<Db> {
    let mut settings = slatedb::Settings {
        wal_enabled: false,
        flush_interval: Some(Duration::from_millis(100)),
        // Many shards per node: keep each memtable small (the node checkpoint
        // also flushes every shard periodically).
        l0_sst_size_bytes: 8 * 1024 * 1024,
        max_unflushed_bytes: 64 * 1024 * 1024,
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
    let path = format!("{}/state/{:03}", store.prefix, partition);
    // cache ids only need to be distinct per DB in this process (tests open
    // several prefixes with the same shard numbers)
    let cache_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        path.hash(&mut h);
        h.finish()
    };
    Ok(Db::builder(path, store.raw.clone())
        .with_settings(settings)
        .with_db_cache(shared_db_cache(), cache_id)
        .with_sst_block_size(slatedb::SstBlockSize::Block16Kib)
        .build()
        .await?)
}
