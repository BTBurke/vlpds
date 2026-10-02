//! Lazy MSTs over SlateDB (DESIGN.md "Partial MSTs"): the [`Source`]s a
//! [`LazyTree`](crate::mst_lazy::LazyTree) loads from.
//!
//! - `M/{did}\0{cid}` holds the interior nodes (height >= 1) of the repo's
//!   current tree, read by the CID a parent links to (hash-checked by the
//!   tree);
//! - leaves are rebuilt from `R/` range scans bounded by the separator keys
//!   above them.
//!
//! The sources are synchronous (the tree code is): they block on the tokio
//! runtime with `Handle::block_on`, so they run on a repo worker thread or
//! the blocking pool, never on a runtime thread. The worker avoids blocking
//! itself: a [`DbSource`] without `io` fails with `MstError::NotLoaded`
//! instead of reading, and the worker then loads the paths on the blocking
//! pool ([`fetch`]) before it applies the writes.

use crate::cid::{Cid, CODEC_DAG_CBOR};
use crate::metrics;
use crate::mst::{Entry, LeafEncoder, MstError, Node, MAX_DEPTH};
use crate::mst_lazy::{Key, Source};
use crate::state;
use slatedb::DbReadOps;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = std::result::Result<T, MstError>;

// ---------- node cache ----------

/// Nodes loaded from the store (interior nodes from `M/`, leaves rebuilt
/// from `R/`), shared by every lazy walk in the process: readers' proof
/// walks, getBlocks and the workers' fetches. Nodes are content-addressed,
/// so an entry is right for any repo version that links to it (no
/// invalidation); each is shallow (children unloaded). LRU per shard,
/// bounded by approximate bytes (`--lazy-mst-node-cache-mb`).
pub struct NodeCache {
    shards: Vec<parking_lot::Mutex<CacheShard>>,
    shard_bytes: std::sync::atomic::AtomicUsize,
}

/// An LRU of nodes by CID and its approximate bytes.
type CacheShard = (lru::LruCache<Cid, Arc<Node>>, usize);

const NODE_CACHE_SHARDS: usize = 32;
pub const DEFAULT_NODE_CACHE_BYTES: usize = 256 << 20;

pub static NODE_CACHE: std::sync::LazyLock<NodeCache> = std::sync::LazyLock::new(|| NodeCache {
    shards: (0..NODE_CACHE_SHARDS).map(|_| parking_lot::Mutex::new((lru::LruCache::unbounded(), 0))).collect(),
    shard_bytes: std::sync::atomic::AtomicUsize::new(DEFAULT_NODE_CACHE_BYTES / NODE_CACHE_SHARDS),
});

impl NodeCache {
    /// The budget (0 = off).
    pub fn set_bytes(&self, n: usize) {
        self.shard_bytes.store(n / NODE_CACHE_SHARDS, std::sync::atomic::Ordering::Relaxed);
    }

    fn shard(&self, c: &Cid) -> &parking_lot::Mutex<CacheShard> {
        &self.shards[c.digest[0] as usize % NODE_CACHE_SHARDS]
    }

    /// Approximate bytes held.
    pub fn bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().1).sum()
    }

    /// Drops every entry (tests).
    pub fn clear(&self) {
        for s in &self.shards {
            *s.lock() = (lru::LruCache::unbounded(), 0);
        }
    }

    pub fn get(&self, c: &Cid) -> Option<Arc<Node>> {
        self.shard(c).lock().0.get(c).cloned()
    }

    pub fn put(&self, n: &Arc<Node>) {
        let Some(c) = n.cid else { return };
        let cap = self.shard_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let size = crate::mst_lazy::heap_bytes(n);
        if size > cap / 4 {
            return;
        }
        let mut g = self.shard(&c).lock();
        let (lru, bytes) = &mut *g;
        if let Some(old) = lru.put(c, n.clone()) {
            *bytes -= crate::mst_lazy::heap_bytes(&old);
        }
        *bytes += size;
        while *bytes > cap {
            let Some((_, old)) = lru.pop_lru() else { break };
            *bytes -= crate::mst_lazy::heap_bytes(&old);
        }
    }
}

/// Persisted nodes read ahead of a walk (one scan of the repo's `M/` range).
pub type Prefetched = HashMap<Cid, Arc<[u8]>>;

fn store_err(e: impl std::fmt::Display) -> MstError {
    MstError::Store(e.to_string())
}

/// Nodes and records of one repo in a SlateDB (the live DB or a snapshot).
pub struct DbSource<'a, R: DbReadOps + Sync + ?Sized> {
    pub db: &'a R,
    pub did: &'a str,
    pub rt: &'a tokio::runtime::Handle,
    /// Nodes already read (a prefetch of the `M/` range); misses read `db`.
    pub prefetched: Option<&'a Prefetched>,
    /// false: never read (`NotLoaded` instead).
    pub io: bool,
    /// Reads made (node point reads + record scans).
    pub reads: Cell<u64>,
}

impl<'a, R: DbReadOps + Sync + ?Sized> DbSource<'a, R> {
    pub fn new(db: &'a R, did: &'a str, rt: &'a tokio::runtime::Handle) -> Self {
        DbSource { db, did, rt, prefetched: None, io: true, reads: Cell::new(0) }
    }

    pub fn with_prefetched(mut self, p: Option<&'a Prefetched>) -> Self {
        self.prefetched = p;
        self
    }

    pub fn no_io(mut self) -> Self {
        self.io = false;
        self
    }
}

/// The node cache alone: a walk that needs anything else fails with
/// `NotLoaded` (the repo worker's no-I/O pass).
pub struct CachedOnly;

impl Source for CachedOnly {
    fn cached(&self, cid: &Cid) -> Option<Arc<Node>> {
        NODE_CACHE.get(cid)
    }
    fn node(&self, _: &Cid) -> Result<Option<Arc<[u8]>>> {
        Err(MstError::NotLoaded)
    }
    fn records(&self, _: Option<&[u8]>, _: Option<&[u8]>, _: &mut Vec<(Key, Cid)>) -> Result<()> {
        Err(MstError::NotLoaded)
    }
}

/// `R/` keys of `did` strictly between `lo` and `hi` (None = open).
fn record_range(did: &str, lo: Option<&[u8]>, hi: Option<&[u8]>) -> std::ops::Range<Vec<u8>> {
    let prefix = state::record_prefix(did);
    let start = match lo {
        // the smallest key above `lo`
        Some(lo) => [&prefix[..], lo, b"\0"].concat(),
        None => prefix.clone(),
    };
    let end = match hi {
        Some(hi) => [&prefix[..], hi].concat(),
        None => state::prefix_end(&prefix),
    };
    start..end
}

fn push_record(prefix_len: usize, kv: &slatedb::KeyValue, out: &mut Vec<(Key, Cid)>) -> Result<()> {
    let (cid, _) = state::decode_record_value(&kv.value).map_err(store_err)?;
    out.push((Arc::from(&kv.key[prefix_len..]), cid));
    Ok(())
}

impl<R: DbReadOps + Sync + ?Sized> Source for DbSource<'_, R> {
    fn cached(&self, cid: &Cid) -> Option<Arc<Node>> {
        NODE_CACHE.get(cid)
    }

    fn remember(&self, n: &Arc<Node>) {
        NODE_CACHE.put(n)
    }

    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>> {
        if let Some(b) = self.prefetched.and_then(|p| p.get(cid)) {
            return Ok(Some(b.clone()));
        }
        if !self.io {
            return Err(MstError::NotLoaded);
        }
        self.reads.set(self.reads.get() + 1);
        metrics::LAZY_MST_READS.with_label_values(&["node"]).inc();
        let v = self.rt.block_on(self.db.get(state::mst_node_key(self.did, cid))).map_err(store_err)?;
        Ok(v.map(|b| Arc::from(&b[..])))
    }

    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(Key, Cid)>) -> Result<()> {
        if !self.io {
            return Err(MstError::NotLoaded);
        }
        self.reads.set(self.reads.get() + 1);
        metrics::LAZY_MST_READS.with_label_values(&["leaf"]).inc();
        let range = record_range(self.did, lo, hi);
        let plen = state::record_prefix(self.did).len();
        self.rt.block_on(async {
            let mut it = state::BatchedScan::new(self.db.scan(range).await.map_err(store_err)?);
            while let Some(kv) = it.next().await.map_err(store_err)? {
                push_record(plen, &kv, out)?;
            }
            Ok(())
        })
    }
}

/// A source whose record ranges come from one forward scan of the repo's
/// `R/` range (asked for in key order: an export's or a full load's
/// pre-order walk), nodes from `nodes`.
pub struct ScanSource<'a, N: Source> {
    pub nodes: N,
    rt: &'a tokio::runtime::Handle,
    prefix_len: usize,
    iter: RefCell<state::BatchedScan>,
    /// The record read past the last range's end.
    peeked: RefCell<Option<(Key, Cid)>>,
    done: Cell<bool>,
}

impl<'a, N: Source> ScanSource<'a, N> {
    /// Opens the scan of `did`'s records on `db` (blocking).
    pub fn open<R: DbReadOps + Sync + ?Sized>(db: &R, did: &str, nodes: N, rt: &'a tokio::runtime::Handle) -> Result<Self> {
        let prefix = state::record_prefix(did);
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, cache_blocks: true, ..Default::default() };
        let iter = rt.block_on(db.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts)).map_err(store_err)?;
        Ok(ScanSource { nodes, rt, prefix_len: prefix.len(), iter: RefCell::new(state::BatchedScan::new(iter)), peeked: RefCell::new(None), done: Cell::new(false) })
    }

    fn next(&self) -> Result<Option<(Key, Cid)>> {
        if let Some(r) = self.peeked.borrow_mut().take() {
            return Ok(Some(r));
        }
        if self.done.get() {
            return Ok(None);
        }
        let mut it = self.iter.borrow_mut();
        let next = match it.next_buffered() {
            Some(kv) => Some(kv),
            None => self.rt.block_on(it.next()).map_err(store_err)?,
        };
        match next {
            Some(kv) => {
                let mut v = Vec::with_capacity(1);
                push_record(self.prefix_len, &kv, &mut v)?;
                Ok(v.pop())
            }
            None => {
                self.done.set(true);
                Ok(None)
            }
        }
    }
}

impl<N: Source> Source for ScanSource<'_, N> {
    // an export or full load streams every node once: not worth caching
    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>> {
        self.nodes.node(cid)
    }

    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(Key, Cid)>) -> Result<()> {
        forward_range(|| self.next(), &self.peeked, lo, hi, out)
    }
}

/// The records of a forward stream in (`lo`, `hi`): those below `lo` are
/// skipped (a range the caller had loaded), the first at or above `hi` is
/// kept in `peeked` for the next range.
fn forward_range(
    mut next: impl FnMut() -> Result<Option<(Key, Cid)>>,
    peeked: &RefCell<Option<(Key, Cid)>>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    out: &mut Vec<(Key, Cid)>,
) -> Result<()> {
    while let Some((k, c)) = next()? {
        if lo.is_some_and(|lo| &k[..] <= lo) {
            continue;
        }
        if hi.is_some_and(|hi| &k[..] >= hi) {
            *peeked.borrow_mut() = Some((k, c));
            break;
        }
        out.push((k, c));
    }
    Ok(())
}

/// A batch of a repo's records in key order for a [`FedSource`]: the keys
/// back to back in one buffer (no allocation per record), each record's
/// key end and CID.
#[derive(Default)]
pub struct Records {
    keys: Vec<u8>,
    recs: Vec<(u32, Cid)>,
}

impl Records {
    pub fn with_capacity(n: usize) -> Self {
        Records { keys: Vec::with_capacity(n * 32), recs: Vec::with_capacity(n) }
    }

    pub fn push(&mut self, key: &[u8], cid: Cid) {
        self.keys.extend_from_slice(key);
        self.recs.push((self.keys.len() as u32, cid));
    }

    pub fn len(&self) -> usize {
        self.recs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recs.is_empty()
    }

    fn get(&self, i: usize) -> (&[u8], &Cid) {
        let start = match i {
            0 => 0,
            _ => self.recs[i - 1].0 as usize,
        };
        let (end, cid) = &self.recs[i];
        (&self.keys[start..*end as usize], cid)
    }
}

/// A batch of records for a [`FedSource`], or the scan's error.
pub type RecordBatch = std::result::Result<Records, String>;

/// [`ScanSource`] fed by a producer on the runtime (one forward `R/` scan
/// in batches, `export_repo`): the scan runs ahead of the walk instead of
/// one `block_on` per record on the walking thread.
pub struct FedSource<N: Source> {
    pub nodes: N,
    rx: RefCell<tokio::sync::mpsc::Receiver<RecordBatch>>,
    /// The batch being read, and its next record.
    cur: RefCell<(Records, usize)>,
}

impl<N: Source> FedSource<N> {
    pub fn new(nodes: N, rx: tokio::sync::mpsc::Receiver<RecordBatch>) -> Self {
        FedSource { nodes, rx: RefCell::new(rx), cur: RefCell::new((Records::default(), 0)) }
    }

    /// Hands `f` the records in (`lo`, `hi`) in key order: those at or
    /// below `lo` are skipped (a range the caller had, or the parent's own
    /// keys), the first at or above `hi` stays for the next range. Blocking
    /// (the blocking pool only).
    fn range(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, mut f: impl FnMut(&[u8], &Cid)) -> Result<()> {
        let mut cur = self.cur.borrow_mut();
        let (batch, pos) = &mut *cur;
        loop {
            while *pos < batch.len() {
                let (k, c) = batch.get(*pos);
                if hi.is_some_and(|hi| k >= hi) {
                    return Ok(());
                }
                if lo.is_none_or(|lo| k > lo) {
                    f(k, c);
                }
                *pos += 1;
            }
            match self.rx.borrow_mut().blocking_recv() {
                Some(Ok(b)) => (*batch, *pos) = (b, 0),
                Some(Err(e)) => return Err(MstError::Store(e)),
                None => return Ok(()),
            }
        }
    }
}

impl<N: Source> Source for FedSource<N> {
    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>> {
        self.nodes.node(cid)
    }

    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(Key, Cid)>) -> Result<()> {
        self.range(lo, hi, |k, c| out.push((Arc::from(k), *c)))
    }

    fn leaf_records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, enc: &mut LeafEncoder) -> Result<()> {
        enc.clear();
        self.range(lo, hi, |k, c| enc.push(k, c))
    }
}

/// Reads `did`'s persisted nodes with one scan of its contiguous `M/`
/// range, up to `max_bytes` (0 = none). The whole range of an average
/// active repo is ~0.5 MB: one read instead of 7-11 dependent ones. Returns
/// the nodes and whether the range was read to its end.
pub async fn prefetch<R: DbReadOps + Sync + ?Sized>(db: &R, did: &str, max_bytes: usize) -> anyhow::Result<(Prefetched, bool)> {
    let mut out = Prefetched::new();
    if max_bytes == 0 {
        return Ok((out, false));
    }
    let prefix = state::mst_node_prefix(did);
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, cache_blocks: true, ..Default::default() };
    let mut it = state::BatchedScan::new(db.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await?);
    let mut bytes = 0;
    while let Some(kv) = it.next().await? {
        let Ok(digest) = <[u8; 32]>::try_from(&kv.key[prefix.len()..]) else { continue };
        bytes += kv.value.len();
        out.insert(Cid { codec: CODEC_DAG_CBOR, digest }, Arc::from(&kv.value[..]));
        if bytes >= max_bytes {
            metrics::LAZY_MST_PREFETCH_BYTES.observe(bytes as f64);
            return Ok((out, false));
        }
    }
    metrics::LAZY_MST_PREFETCH_BYTES.observe(bytes as f64);
    Ok((out, true))
}

/// `Tree::proof_blocks` of `key` on a lazy tree at `root`, without blocking:
/// unloaded children are read from `db` (a snapshot at the tree's version:
/// `M/` nodes, leaves rebuilt from an `R/` range), each checked against its
/// link, into nothing shared (the tree isn't touched). The node at the end
/// of the path holds `key`, if any node does.
pub async fn proof_blocks<R: DbReadOps + Sync + ?Sized>(root: &Arc<Node>, db: &R, did: &str, key: &[u8]) -> Result<Vec<(Cid, Vec<u8>)>> {
    let mut out = Vec::new();
    walk_path(root, db, did, key, &mut |n| {
        out.push((n.cid.ok_or(MstError::Invalid("unwritten node"))?, crate::mst_lazy::node_block(n)?));
        Ok(())
    })
    .await?;
    Ok(out)
}

/// The node at the end of `key`'s path (see [`proof_blocks`]): the leaf, or
/// the node holding `key`.
pub async fn path_end<R: DbReadOps + Sync + ?Sized>(root: &Arc<Node>, db: &R, did: &str, key: &[u8]) -> Result<Arc<Node>> {
    walk_path(root, db, did, key, &mut |_| Ok(())).await
}

/// Walks `key`'s path from `root` (see [`proof_blocks`]), visiting each
/// node; returns the last.
async fn walk_path<R: DbReadOps + Sync + ?Sized>(
    root: &Arc<Node>,
    db: &R,
    did: &str,
    key: &[u8],
    visit: &mut (dyn FnMut(&Node) -> Result<()> + Send),
) -> Result<Arc<Node>> {
    let mut n = root.clone();
    let (mut lo, mut hi): (Option<Key>, Option<Key>) = (None, None);
    for _ in 0..=MAX_DEPTH {
        if n.stub {
            return Err(MstError::Partial);
        }
        visit(&n)?;
        let Some((i, clo, chi)) = crate::mst_lazy::proof_child(&n, key, &lo, &hi) else { return Ok(n) };
        let child = match &n.entries[i] {
            Entry::Child { node: Some(c), .. } => c.clone(),
            Entry::Child { node: None, cid: Some(c) } if n.height == 1 && NODE_CACHE.get(c).is_none() => {
                load_leaves(db, did, &n, lo.as_deref(), hi.as_deref(), i).await?
            }
            Entry::Child { node: None, cid: Some(c) } => load_child(db, did, c, n.height - 1, clo.as_deref(), chi.as_deref()).await?,
            _ => return Err(MstError::Partial),
        };
        (n, lo, hi) = (child, clo, chi);
    }
    Err(MstError::Invalid("tree too deep"))
}

/// The leaf at entry `want` of the height-1 node `n` (keys in (`lo`,
/// `hi`)), with its unloaded siblings: one scan of `n`'s record range
/// rebuilds them all (a range scan costs mostly its setup), and each that
/// matches its link is cached. Proofs and getBlocks of nearby keys then
/// find their leaves cached.
async fn load_leaves<R: DbReadOps + Sync + ?Sized>(db: &R, did: &str, n: &Node, lo: Option<&[u8]>, hi: Option<&[u8]>, want: usize) -> Result<Arc<Node>> {
    metrics::LAZY_MST_READS.with_label_values(&["leaf"]).inc();
    let plen = state::record_prefix(did).len();
    let mut recs = Vec::new();
    let mut it = state::BatchedScan::new(db.scan(record_range(did, lo, hi)).await.map_err(store_err)?);
    while let Some(kv) = it.next().await.map_err(store_err)? {
        push_record(plen, &kv, &mut recs)?;
    }
    let mut pos = 0;
    let mut found = None;
    for (i, e) in n.entries.iter().enumerate() {
        match e {
            // the node's own keys are records too: skip them
            Entry::Value { key, .. } => {
                while pos < recs.len() && recs[pos].0[..] <= key[..] {
                    pos += 1;
                }
            }
            Entry::Child { node, cid: Some(c) } => {
                let end = match n.entries.get(i + 1) {
                    Some(Entry::Value { key, .. }) => recs[pos..].iter().position(|(k, _)| k[..] >= key[..]).map_or(recs.len(), |p| pos + p),
                    _ => recs.len(),
                };
                let group = &recs[pos..end];
                pos = end;
                if i == want {
                    let leaf = crate::mst_lazy::rebuilt_subtree(group, 0, c)?;
                    NODE_CACHE.put(&leaf);
                    found = Some(leaf);
                } else if node.is_none() && NODE_CACHE.get(c).is_none() {
                    if let Ok(leaf) = crate::mst_lazy::rebuilt_subtree(group, 0, c) {
                        NODE_CACHE.put(&leaf);
                    }
                }
            }
            Entry::Child { cid: None, .. } => {}
        }
    }
    found.ok_or(MstError::Partial)
}

/// An unloaded child (async [`DbSource`] + `load_subtree`): interior from
/// `M/`, a leaf (or a missing interior node) rebuilt from its record range.
async fn load_child<R: DbReadOps + Sync + ?Sized>(db: &R, did: &str, cid: &Cid, height: i32, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Arc<Node>> {
    if let Some(n) = NODE_CACHE.get(cid).filter(|n| n.height == height) {
        return Ok(n);
    }
    let n = load_child_uncached(db, did, cid, height, lo, hi).await?;
    NODE_CACHE.put(&n);
    Ok(n)
}

async fn load_child_uncached<R: DbReadOps + Sync + ?Sized>(db: &R, did: &str, cid: &Cid, height: i32, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Arc<Node>> {
    if height >= 1 {
        metrics::LAZY_MST_READS.with_label_values(&["node"]).inc();
        if let Some(b) = db.get(state::mst_node_key(did, cid)).await.map_err(store_err)? {
            if let Some(n) = crate::mst_lazy::persisted_node(Arc::from(&b[..]), cid, Some(height))? {
                return Ok(n);
            }
        }
    }
    metrics::LAZY_MST_READS.with_label_values(&["leaf"]).inc();
    let plen = state::record_prefix(did).len();
    let mut recs = Vec::new();
    let mut it = state::BatchedScan::new(db.scan(record_range(did, lo, hi)).await.map_err(store_err)?);
    while let Some(kv) = it.next().await.map_err(store_err)? {
        push_record(plen, &kv, &mut recs)?;
    }
    crate::mst_lazy::rebuilt_subtree(&recs, height, cid)
}
