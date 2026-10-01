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
    Ok(Db::builder(path, store.raw.clone())
        .with_settings(settings)
        .with_sst_block_size(slatedb::SstBlockSize::Block16Kib)
        .build()
        .await?)
}
