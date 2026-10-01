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
    // amplification only while compaction lags. The polls stay at their
    // defaults: at 1 s they also absorb unpaced bursts, but cost ~4x the
    // idle GETs across 256 shards.
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
    let path = format!("{}/state/{:03}", store.prefix, partition);
    // cache ids only need to be distinct per DB in this process (tests open
    // several prefixes with the same shard numbers)
    let cache_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::hash::DefaultHasher::new();
        path.hash(&mut h);
        h.finish()
    };
    let codec = settings.compression_codec;
    let db = crate::metrics::with_slatedb_metrics(Db::builder(path.clone(), store.raw.clone()))
        .with_settings(settings)
        .with_db_cache(shared_db_cache(), cache_id)
        .with_sst_block_size(SST_BLOCK_SIZE)
        .build()
        .await?;
    spawn_compactor(&db, path, store.raw.clone(), codec);
    Ok(db)
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
    use slatedb::config::{CompactionWorkerOptions, CompactorOptions};
    let mut status = db.subscribe();
    tokio::spawn(async move {
        let mut opts = CompactorOptions { worker: None, checkpoint_lifetime: checkpoint_lifetime(), ..Default::default() };
        let mut worker_opts = CompactionWorkerOptions { compression_codec: codec, ..Default::default() };
        if cfg!(test) {
            // unit tests wait for compactions
            opts.poll_interval = Duration::from_millis(100);
            worker_opts.compactions_poll_interval = Duration::from_millis(100);
        }
        let compactor = slatedb::CompactorBuilder::new(path.clone(), raw.clone()).with_options(opts);
        let worker = slatedb::CompactionWorkerBuilder::new(path.clone(), raw).with_options(worker_opts).with_sst_block_size(SST_BLOCK_SIZE);
        #[cfg(feature = "slatedb-metrics")]
        let (compactor, worker) = (compactor.with_metrics_recorder(crate::metrics::slatedb_recorder()), worker.with_metrics_recorder(crate::metrics::slatedb_recorder()));
        let compactor = compactor.build();
        let worker = match worker.build().await {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(%path, "compaction worker failed to start: {e}");
                return;
            }
        };
        let closed = async {
            while status.borrow_and_update().close_reason.is_none() {
                if status.changed().await.is_err() {
                    break;
                }
            }
        };
        tokio::select! {
            r = compactor.run() => if let Err(e) = r { tracing::warn!(%path, "compactor exited: {e}") },
            r = worker.run() => if let Err(e) = r { tracing::warn!(%path, "compaction worker exited: {e}") },
            _ = closed => {}
        }
        let _ = compactor.stop().await;
        let _ = worker.stop().await;
    });
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
