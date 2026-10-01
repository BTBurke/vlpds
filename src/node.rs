//! The node host: opens/closes the shards the cluster assigns to this node and
//! follows every peer's log for the merged firehose (see cluster.rs, nodelog.rs,
//! remote.rs).

use crate::cluster::{Cluster, ShardHost};
use crate::firehose::{Firehose, Source};
use crate::nodelog::{self, LogBatch, LogEntry, NodeLog, ShardSink, Span};
use crate::partition::{self, Partition};
use crate::partitions::PartitionTable;
use crate::remote::{self, Follower};
use crate::store::Store;
use crate::worker::{WorkerMsg, Workers};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub struct Node {
    pub cluster: Arc<Cluster>,
    pub log: Arc<NodeLog>,
    pub store: Store,
    pub state_store: Store,
    pub table: Arc<PartitionTable>,
    pub firehose: Arc<Firehose>,
    pub merger_tx: mpsc::UnboundedSender<LogBatch>,
    pub workers: Workers,
    pub cache_dir: Option<std::path::PathBuf>,
    pub internal_token: String,
    followers: Mutex<HashMap<String, Follower>>,
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster: Arc<Cluster>,
        log: Arc<NodeLog>,
        store: Store,
        state_store: Store,
        table: Arc<PartitionTable>,
        firehose: Arc<Firehose>,
        merger_tx: mpsc::UnboundedSender<LogBatch>,
        workers: Workers,
        cache_dir: Option<std::path::PathBuf>,
        internal_token: String,
    ) -> Arc<Node> {
        Arc::new(Node {
            cluster,
            log,
            store,
            state_store,
            table,
            firehose,
            merger_tx,
            workers,
            cache_dir,
            internal_token,
            followers: Mutex::new(HashMap::new()),
        })
    }

    /// Follows every live peer's log; retires followers of dead logs once
    /// they've been drained to their fence.
    pub fn sync_followers(&self) {
        let peers = self.cluster.peers();
        let mut f = self.followers.lock();
        for p in &peers {
            if p.log_id == self.cluster.log_id || f.contains_key(&p.log_id) {
                continue;
            }
            let cluster = self.cluster.clone();
            let log_id = p.log_id.clone();
            let addr = Arc::new(move || cluster.peers().into_iter().find(|l| l.log_id == log_id).map(|l| l.addr));
            let fl = remote::follow_log(&p.log_id, self.store.clone(), addr, self.internal_token.clone(), self.merger_tx.clone());
            self.firehose.set_source(&p.log_id, Some(Source::Remote(fl.watermark.clone())));
            tracing::info!(log_id = %p.log_id, node = %p.node_id, "following peer log");
            f.insert(p.log_id.clone(), fl);
        }
        let done: Vec<String> = f.iter().filter(|(_, fl)| fl.done.load(Ordering::Acquire)).map(|(k, _)| k.clone()).collect();
        for log_id in done {
            self.firehose.set_source(&log_id, None);
            f.remove(&log_id);
            tracing::info!(%log_id, "dead peer log drained to its fence");
        }
    }
}

impl Node {
    /// Drops every worker's cached repos for `shards` and waits until each
    /// worker has done so.
    async fn purge_worker_caches(&self, shards: &[u16]) {
        let mut acks = Vec::new();
        for w in self.workers.senders.iter() {
            for &shard in shards {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = w.send(WorkerMsg::DropPartition(shard, tx));
                acks.push(rx);
            }
        }
        for a in acks {
            let _ = a.await;
        }
    }
}

#[async_trait::async_trait]
impl ShardHost for Node {
    fn next_ordinal(&self) -> u64 {
        self.log.next_ordinal()
    }

    fn durable_end(&self) -> u64 {
        self.log.durable_ordinal.load(Ordering::Acquire).wrapping_add(1)
    }

    async fn open_many(&self, shards: Vec<(u16, u64, Vec<Span>)>) -> Vec<(u16, anyhow::Result<()>)> {
        use futures::StreamExt;
        if shards.is_empty() {
            return Vec::new();
        }
        let started = Instant::now();
        let n = shards.len();
        // 1. open every shard's SlateDB concurrently
        let opened: Vec<(u16, u64, Vec<Span>, anyhow::Result<Arc<slatedb::Db>>)> = futures::stream::iter(shards)
            .map(|(s, e, h)| async move {
                let db = partition::open_db(&self.state_store, s, self.cache_dir.as_deref()).await.map(Arc::new);
                (s, e, h, db)
            })
            .buffer_unordered(32)
            .collect()
            .await;
        let mut results = Vec::with_capacity(n);
        let mut ready = Vec::new();
        for (s, e, h, db) in opened {
            match db {
                Ok(db) => ready.push((s, e, h, db)),
                Err(e) => results.push((s, Err(e))),
            }
        }
        // 2. one batched replay of previous owners' log tails
        let plan: Vec<(u16, &slatedb::Db, &[Span])> = ready.iter().map(|(s, _, h, db)| (*s, db.as_ref(), h.as_slice())).collect();
        let replayed = match nodelog::replay_many(&self.store, &plan).await {
            Ok(r) => r,
            Err(e) => {
                let msg = format!("{e:#}");
                for (s, ..) in ready {
                    results.push((s, Err(anyhow::anyhow!("replay failed: {msg}"))));
                }
                return results;
            }
        };
        // 3. make replayed state durable, then serve
        let flushed: Vec<(u16, u64, Arc<slatedb::Db>, anyhow::Result<()>)> = futures::stream::iter(ready)
            .map(|(s, e, _, db)| async move {
                let r = db
                    .flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable })
                    .await
                    .map_err(anyhow::Error::from);
                (s, e, db, r)
            })
            .buffer_unordered(32)
            .collect()
            .await;
        // HA fix: never serve a shard on top of repo state cached during an
        // earlier ownership. A repo load that was in flight when close() purged
        // the workers lands in the cache afterwards, still bound to the closed
        // shard. Writes built on it are rejected (nodelog), but its in-memory
        // head keeps advancing. When this node took the shard back, the next
        // durable commit chained on those never-logged commits: a firehose
        // chain break, and a head whose MST no longer matched its stored
        // records ("rebuilt MST root != head data"), so the repo could not be
        // loaded at all (bench/ha zombie-short).
        let ok_shards: Vec<u16> = flushed.iter().filter(|f| f.3.is_ok()).map(|f| f.0).collect();
        self.purge_worker_caches(&ok_shards).await;
        for (shard, epoch, db, r) in flushed {
            if let Err(e) = r {
                results.push((shard, Err(e)));
                continue;
            }
            let apply_lock = Arc::new(tokio::sync::RwLock::new(()));
            self.log.sinks.insert(Arc::new(ShardSink { id: shard, epoch, db: db.clone(), apply_lock: apply_lock.clone() }));
            self.table.set(
                shard,
                Some(Arc::new(Partition {
                    id: shard,
                    epoch,
                    db,
                    apply_lock,
                    tx: self.log.tx.clone(),
                    wm: self.log.wm.clone(),
                    log: self.log.clone(),
                })),
            );
            crate::metrics::LEASE_EVENTS.with_label_values(&["opened"]).inc();
            results.push((shard, Ok(())));
        }
        crate::metrics::OWNED_PARTITIONS.set(self.table.owned().len() as i64);
        tracing::info!(shards = n, segments_replayed = replayed, elapsed_ms = started.elapsed().as_millis() as u64, "shards opened");
        results
    }

    async fn close(&self, shard: u16) -> anyhow::Result<()> {
        let Some(part) = self.table.get(shard as usize) else { return Ok(()) };
        let started = Instant::now();
        // 1. stop routing new work here
        self.table.set(shard, None);
        // 2. no worker may keep (or start) building commits for it
        self.purge_worker_caches(&[shard]).await;
        // 3. barrier: once an empty entry for this shard is durable, every
        //    earlier entry for it is durable and applied (the log is FIFO)
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.log
            .tx
            .send(LogEntry {
                shard,
                frames: Vec::new(),
                muts: Vec::new(),
                ack: Some(Box::new(move |r| {
                    let _ = tx.send(r);
                })),
                pending: None,
                enqueued: Instant::now(),
            })
            .await
            .map_err(|_| anyhow::anyhow!("node log gone"))?;
        tokio::time::timeout(Duration::from_secs(30), rx).await??.map_err(|e| anyhow::anyhow!("{e}"))?;
        // 4. checkpoint + close so the successor replays nothing
        let ord = self.log.durable_ordinal.load(Ordering::Acquire);
        {
            let _g = part.apply_lock.write().await;
            let mut wb = slatedb::WriteBatch::new();
            wb.put(nodelog::META_APPLIED, nodelog::encode_marker(&self.log.log_id, ord));
            part.db.write(wb).await?;
        }
        part.db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await?;
        self.log.sinks.remove(shard);
        // HA fix: purge again; loads that completed during the drain re-cached
        // repos of this shard (see open_many)
        self.purge_worker_caches(&[shard]).await;
        part.db.close().await?;
        crate::metrics::OWNED_PARTITIONS.set(self.table.owned().len() as i64);
        crate::metrics::LEASE_EVENTS.with_label_values(&["closed"]).inc();
        tracing::debug!(shard, elapsed_ms = started.elapsed().as_millis() as u64, "shard closed");
        Ok(())
    }

    fn lost(&self) {
        crate::metrics::LEASE_EVENTS.with_label_values(&["lost"]).inc();
        tracing::error!("node lease lost unexpectedly: fail-stop");
        std::process::exit(5);
    }

    fn on_membership(&self) {
        self.sync_followers();
    }
}
