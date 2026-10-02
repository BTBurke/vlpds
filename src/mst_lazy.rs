//! Partial, lazily loaded MSTs (DESIGN.md "Partial MSTs"). The repo worker
//! keeps every repo's tree as a [`LazyTree`]: only the visited paths are
//! loaded (from `M/` and `R/`, `crate::mst_store`).
//!
//! A repo's tree is kept as an [`mst::Tree`](crate::mst::Tree) whose
//! unvisited subtrees stay unloaded (`Entry::Child { node: None, cid }`).
//! Nodes are loaded on demand from a [`Source`]:
//! - nodes of height >= `persist_min` are persisted content-addressed (in
//!   the real layout `M/{did}\0{cid}` -> node block, written in the same
//!   state batch as the commit's `R/` and `h/` rows) and read by the CID
//!   their parent links to, hash-checked;
//! - lower subtrees (the leaves, at `persist_min = 1`) are rebuilt from the
//!   records in their key range (an `R/` range scan bounded by the parent's
//!   separator keys: the MST layout is a pure function of the keys) and
//!   checked against the parent's link.
//!
//! Mutations, CIDs and proof blocks are `mst::Tree`'s own code: this module
//! only makes sure every node an operation visits is loaded first, namely
//! the search paths of the key and of its two neighbours in key order (the
//! predecessor's path is the right spine a delete merges, the successor's the
//! left spine, and `prove_mutation` only walks the key's own path). It then
//! derives the persistence diff: new persisted nodes to put, and replaced
//! ones to delete (every replaced node lies on those paths, so a check of
//! the nodes seen there against the new tree finds them all).
//!
//! `persist_min` = 0 is option (a) (every node persisted), 1 option (b)
//! (interior nodes), 2 the hybrid (height-1 subtrees rebuilt from records).

use crate::cid::Cid;
use crate::mst::{decode_node, encode_node, height_for_key, Entry, MstError, Node, Tree, MAX_DEPTH};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

type Result<T> = std::result::Result<T, MstError>;

/// A record key (`collection/rkey`).
pub type Key = Arc<[u8]>;

/// Where the lazy tree reads from: SlateDB in the real thing
/// (`crate::mst_store`). A source that may not do I/O right now fails with
/// [`MstError::NotLoaded`].
pub trait Source {
    /// A node loaded before (`mst_store::NODE_CACHE`): content-addressed,
    /// so right wherever its CID is linked.
    fn cached(&self, _cid: &Cid) -> Option<Arc<Node>> {
        None
    }
    /// Offers a node this source loaded to that cache.
    fn remember(&self, _n: &Arc<Node>) {}
    /// A persisted node block by CID (`M/{did}\0{cid}` point read).
    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>>;
    /// Records with `lo < key < hi` in key order (`R/` range scan; `None`
    /// bounds are open).
    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(Key, Cid)>) -> Result<()>;
}

/// What loading cost (cumulative per [`LazyTree`] or export).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LoadStats {
    /// Persisted node point reads, and their bytes.
    pub node_reads: u64,
    pub node_bytes: u64,
    /// Record range scans, and the records they returned.
    pub scans: u64,
    pub scanned_records: u64,
    /// Persisted nodes that were missing and rebuilt from records instead.
    pub fallbacks: u64,
}

/// The persisted-node changes of one commit.
#[derive(Clone, Debug, Default)]
pub struct Persist {
    /// The written nodes at persisted heights, in write order (re-puts of
    /// unchanged proof neighbours included: idempotent).
    pub puts: Vec<(Cid, Vec<u8>)>,
    pub deletes: Vec<Cid>,
}

impl Persist {
    pub fn put_bytes(&self) -> usize {
        self.puts.iter().map(|(_, b)| b.len()).sum()
    }
}

// ---------- building subtrees from records ----------

/// Finishes a node: encodes it and computes its CID (internal nodes keep
/// their block, as `mst::Tree` does after a write).
fn finish(height: i32, entries: Vec<Entry>) -> Result<Arc<Node>> {
    let mut n = Node { height, entries, cid: None, dirty: false, stub: false, bytes: None };
    let mut buf = Vec::with_capacity(64 + n.entries.len() * 80);
    encode_node(&n, &mut buf)?;
    n.cid = Some(Cid::dag_cbor(&buf));
    if height >= 1 {
        n.bytes = Some(Arc::from(buf));
    }
    Ok(Arc::new(n))
}

/// The canonical subtree at `height` holding exactly `recs` (sorted, with
/// `heights[i] = height_for_key(recs[i].0)`, all <= `height`), CIDs computed.
/// A key range of an MST is a pure function of its keys: every key of
/// height `height` is an entry, every non-empty gap between them a child.
pub fn build(recs: &[(Key, Cid)], heights: &[i32], height: i32) -> Result<Arc<Node>> {
    if height < 0 || height as usize > 4 * MAX_DEPTH {
        return Err(MstError::Invalid("bad subtree height"));
    }
    let mut entries = Vec::new();
    let mut start = 0;
    for i in 0..recs.len() {
        match heights[i].cmp(&height) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Greater => return Err(MstError::Invalid("key above its subtree")),
            std::cmp::Ordering::Equal => {}
        }
        if start < i {
            let c = build(&recs[start..i], &heights[start..i], height - 1)?;
            entries.push(Entry::Child { cid: c.cid, node: Some(c) });
        }
        entries.push(Entry::Value { key: recs[i].0.clone(), val: recs[i].1 });
        start = i + 1;
    }
    if start < recs.len() {
        let c = build(&recs[start..], &heights[start..], height - 1)?;
        entries.push(Entry::Child { cid: c.cid, node: Some(c) });
    }
    finish(height, entries)
}

/// A whole tree built from all of a repo's records (sorted): the cold-load
/// fallback, and the backfill of a repo without persisted nodes.
pub fn build_tree(recs: &[(Key, Cid)]) -> Result<Tree> {
    let heights: Vec<i32> = recs.iter().map(|(k, _)| height_for_key(k)).collect();
    let h = heights.iter().copied().max().unwrap_or(0);
    let mut t = Tree::new();
    t.root = build(recs, &heights, h)?;
    Ok(t)
}

// ---------- loading ----------

/// A persisted node, hash-checked, with its height fixed (a node without
/// keys of its own takes it from its parent).
fn read_node(src: &dyn Source, cid: &Cid, height: Option<i32>, stats: &mut LoadStats) -> Result<Option<Arc<Node>>> {
    let Some(b) = src.node(cid)? else { return Ok(None) };
    stats.node_reads += 1;
    stats.node_bytes += b.len() as u64;
    persisted_node(b, cid, height)
}

/// A persisted node's block `b` (read by `cid`), hash-checked, with its
/// height fixed (`height`: the parent's minus one; a node without keys
/// takes it). None for a key-less root (the empty tree's).
pub fn persisted_node(b: Arc<[u8]>, cid: &Cid, height: Option<i32>) -> Result<Option<Arc<Node>>> {
    if Cid::dag_cbor(&b) != *cid {
        return Err(MstError::Invalid("persisted node doesn't hash to its cid"));
    }
    let mut n = decode_node(&b, *cid)?;
    match height {
        Some(h) if n.height < 0 => n.height = h,
        Some(h) if n.height != h => return Err(MstError::Invalid("persisted node at the wrong height")),
        _ => {}
    }
    if n.height < 0 {
        // a key-less root: only the empty tree, rebuilt from records
        return Ok(None);
    }
    if n.height >= 1 {
        n.bytes = Some(b);
    }
    Ok(Some(Arc::new(n)))
}

/// The subtree at `height` whose CID is `cid` and whose keys lie strictly
/// between `lo` and `hi`: read if persisted, else rebuilt from records.
fn load_subtree(
    src: &dyn Source,
    persist_min: i32,
    cid: Cid,
    height: i32,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    stats: &mut LoadStats,
) -> Result<Arc<Node>> {
    if let Some(n) = src.cached(&cid).filter(|n| n.height == height) {
        return Ok(n);
    }
    let n = load_subtree_uncached(src, persist_min, cid, height, lo, hi, stats)?;
    src.remember(&n);
    Ok(n)
}

fn load_subtree_uncached(
    src: &dyn Source,
    persist_min: i32,
    cid: Cid,
    height: i32,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    stats: &mut LoadStats,
) -> Result<Arc<Node>> {
    if height >= persist_min {
        if let Some(n) = read_node(src, &cid, Some(height), stats)? {
            return Ok(n);
        }
        stats.fallbacks += 1;
    }
    let mut recs = Vec::new();
    src.records(lo, hi, &mut recs)?;
    stats.scans += 1;
    stats.scanned_records += recs.len() as u64;
    rebuilt_subtree(&recs, height, &cid)
}

/// The subtree at `height` holding exactly `recs` (a record range scan
/// bounded by the separators above it), checked against its link `cid`.
pub fn rebuilt_subtree(recs: &[(Key, Cid)], height: i32, cid: &Cid) -> Result<Arc<Node>> {
    let heights: Vec<i32> = recs.iter().map(|(k, _)| height_for_key(k)).collect();
    let n = build(recs, &heights, height)?;
    if n.cid != Some(*cid) {
        return Err(MstError::Invalid("subtree rebuilt from records doesn't match its link"));
    }
    Ok(n)
}

/// Where a proof walk (`Tree::proof_blocks`) goes from `n`, whose keys lie
/// in (`lo`, `hi`): the child entry to descend to and its key bounds, or
/// None where the path ends (`key` is here, or would be).
pub fn proof_child(n: &Node, key: &[u8], lo: &Option<Key>, hi: &Option<Key>) -> Option<(usize, Option<Key>, Option<Key>)> {
    let Loc::Gap(Some(i)) = locate(n, key) else { return None };
    let clo = if i > 0 { value_key(n.entries.get(i - 1)) } else { lo.clone() };
    let chi = value_key(n.entries.get(i + 1)).or_else(|| hi.clone());
    Some((i, clo, chi))
}

/// A written node's block.
pub fn node_block(n: &Node) -> Result<Vec<u8>> {
    match &n.bytes {
        Some(b) if !n.dirty => Ok(b.to_vec()),
        _ => {
            let mut buf = Vec::with_capacity(64 + n.entries.len() * 80);
            encode_node(n, &mut buf)?;
            Ok(buf)
        }
    }
}

enum Loc {
    /// The key is entry `i` of this node.
    Found(usize),
    /// The key falls in the gap at child entry `i` (None: an empty gap).
    Gap(Option<usize>),
}

fn locate(n: &Node, key: &[u8]) -> Loc {
    let mut gap = None;
    for (i, e) in n.entries.iter().enumerate() {
        match e {
            Entry::Value { key: k, .. } => match key.cmp(k) {
                std::cmp::Ordering::Equal => return Loc::Found(i),
                std::cmp::Ordering::Less => return Loc::Gap(gap),
                std::cmp::Ordering::Greater => gap = None,
            },
            Entry::Child { .. } => gap = Some(i),
        }
    }
    Loc::Gap(gap)
}

fn is_child(e: Option<&Entry>) -> bool {
    matches!(e, Some(Entry::Child { .. }))
}

fn value_key(e: Option<&Entry>) -> Option<Key> {
    match e {
        Some(Entry::Value { key, .. }) => Some(key.clone()),
        _ => None,
    }
}

/// A key in the subtree of `n` (to find it again by position).
fn any_key(n: &Node) -> Option<Key> {
    for e in &n.entries {
        match e {
            Entry::Value { key, .. } => return Some(key.clone()),
            Entry::Child { node: Some(c), .. } => {
                if let Some(k) = any_key(c) {
                    return Some(k);
                }
            }
            Entry::Child { node: None, .. } => {}
        }
    }
    None
}

/// The child a walk in `mode` goes on to from `n` (None: the path ends at
/// `n`); a neighbour walk turns into a spine walk below the key.
fn step(n: &Node, key: &[u8], mode: &mut Mode) -> Option<usize> {
    let last = n.entries.len().wrapping_sub(1);
    match *mode {
        Mode::Min => is_child(n.entries.first()).then_some(0),
        Mode::Max => is_child(n.entries.last()).then_some(last),
        Mode::Key | Mode::Before | Mode::After => match locate(n, key) {
            Loc::Gap(g) => g,
            Loc::Found(i) => match *mode {
                Mode::Before if i > 0 && is_child(n.entries.get(i - 1)) => {
                    *mode = Mode::Max;
                    Some(i - 1)
                }
                Mode::After if is_child(n.entries.get(i + 1)) => {
                    *mode = Mode::Min;
                    Some(i + 1)
                }
                _ => None,
            },
        },
    }
}

/// Which path a walk loads.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// The key's search path (to the node holding it, or the bottom).
    Key,
    /// The path to the key's predecessor (right spine below the key).
    Before,
    /// The path to the key's successor (left spine below the key).
    After,
    Min,
    Max,
}

/// Approximate heap of the loaded part of a subtree: the node structs, entry
/// vectors, key allocations and cached blocks (same accounting for full and
/// lazy trees, so the two compare; not allocator-exact).
pub fn heap_bytes(n: &Node) -> usize {
    const ARC: usize = 16;
    let mut b = ARC + std::mem::size_of::<Node>() + n.entries.capacity() * std::mem::size_of::<Entry>();
    if let Some(bytes) = &n.bytes {
        b += ARC + bytes.len();
    }
    for e in &n.entries {
        match e {
            Entry::Value { key, .. } => b += (ARC + key.len()).next_multiple_of(16),
            Entry::Child { node: Some(c), .. } => b += heap_bytes(c),
            Entry::Child { node: None, .. } => {}
        }
    }
    b
}

/// Blocks of the loaded (written) nodes of `n`'s subtree whose CIDs are in
/// `want`.
pub fn loaded_blocks(n: &Node, want: &HashSet<Cid>, out: &mut Vec<(Cid, Vec<u8>)>) -> Result<()> {
    if let Some(c) = n.cid.filter(|c| want.contains(c)) {
        if !out.iter().any(|(o, _)| *o == c) {
            let b = match &n.bytes {
                Some(b) if !n.dirty => b.to_vec(),
                _ => {
                    let mut buf = Vec::new();
                    encode_node(n, &mut buf)?;
                    buf
                }
            };
            out.push((c, b));
        }
    }
    for e in &n.entries {
        if let Entry::Child { node: Some(ch), .. } = e {
            loaded_blocks(ch, want, out)?;
        }
    }
    Ok(())
}

/// Loaded nodes of a subtree.
pub fn loaded_nodes(n: &Node) -> usize {
    1 + n
        .entries
        .iter()
        .map(|e| match e {
            Entry::Child { node: Some(c), .. } => loaded_nodes(c),
            _ => 0,
        })
        .sum::<usize>()
}

/// A repo's MST, loaded only along the paths operations have visited.
#[derive(Clone)]
pub struct LazyTree {
    /// The partial tree (`mst::Tree` does every mutation and encoding).
    pub tree: Tree,
    persist_min: i32,
    /// Persisted nodes (height >= persist_min, by their last written cid)
    /// on this batch's mutation walks:
    /// (cid, a key in the node's subtree, height). Replaced ones are deleted.
    seen: Vec<(Cid, Key, i32)>,
    pub stats: LoadStats,
}

impl LazyTree {
    /// Opens the tree at `root` with only its root node loaded (a repo
    /// whose root isn't persisted is small, or lost its nodes: rebuilt from
    /// all its records, and checked against `root`).
    pub fn open(root: Cid, persist_min: i32, src: &dyn Source) -> Result<LazyTree> {
        let mut stats = LoadStats::default();
        let tree = match read_node(src, &root, None, &mut stats)? {
            Some(n) => {
                let mut t = Tree::new();
                t.root = n;
                t
            }
            None => {
                let mut recs = Vec::new();
                src.records(None, None, &mut recs)?;
                stats.scans += 1;
                stats.scanned_records += recs.len() as u64;
                let t = build_tree(&recs)?;
                if t.root.cid != Some(root) {
                    return Err(MstError::Invalid("tree rebuilt from records doesn't match its root"));
                }
                t
            }
        };
        Ok(LazyTree { tree, persist_min, seen: Vec::new(), stats })
    }

    /// A fully loaded tree whose nodes of height >= `persist_min` are
    /// persisted (a new or imported repo, or one rebuilt from its records):
    /// it can be unloaded and walked lazily from now on.
    pub fn loaded(tree: Tree, persist_min: i32) -> LazyTree {
        LazyTree { tree, persist_min, seen: Vec::new(), stats: LoadStats::default() }
    }

    pub fn persist_min(&self) -> i32 {
        self.persist_min
    }

    /// Loads one path (see [`Mode`]); with `note` (mutation walks), notes
    /// the persisted nodes it passes as candidates for deletion.
    fn walk(&mut self, key: &[u8], mut mode: Mode, note: bool, src: &dyn Source) -> Result<()> {
        // most walks find their path loaded: check that without
        // `Arc::make_mut`, which copies every node shared with a view
        if let Some(path) = self.loaded_path(key, mode, note) {
            self.seen.extend(path);
            return Ok(());
        }
        let persist_min = self.persist_min;
        let stats = &mut self.stats;
        let seen = &mut self.seen;
        let mut n: &mut Arc<Node> = &mut self.tree.root;
        let (mut lo, mut hi): (Option<Key>, Option<Key>) = (None, None);
        let mut path = Vec::new();
        for _ in 0..=MAX_DEPTH {
            if n.stub {
                return Err(MstError::Partial);
            }
            let idx = step(n, key, &mut mode);
            // a dirty node's cid is its last written (persisted) version's:
            // an earlier op of the batch changed it
            if note && n.height >= persist_min {
                if let Some(c) = n.cid {
                    path.push((c, n.height));
                }
            }
            let Some(idx) = idx else {
                // the path ends in a node with keys (one without has a child
                // to go on to): any of them is in every path node's subtree
                if let Some(k) = any_key(n) {
                    seen.extend(path.into_iter().map(|(c, h)| (c, k.clone(), h)));
                }
                return Ok(());
            };
            let clo = if idx > 0 { value_key(n.entries.get(idx - 1)) } else { lo.clone() };
            let chi = value_key(n.entries.get(idx + 1)).or_else(|| hi.clone());
            if let Entry::Child { node: None, cid } = &n.entries[idx] {
                let c = cid.ok_or(MstError::Partial)?;
                let child = load_subtree(src, persist_min, c, n.height - 1, clo.as_deref(), chi.as_deref(), stats)?;
                Arc::make_mut(n).entries[idx] = Entry::Child { node: Some(child), cid: Some(c) };
            }
            (lo, hi) = (clo, chi);
            let Entry::Child { node: Some(c), .. } = &mut Arc::make_mut(n).entries[idx] else {
                return Err(MstError::Partial);
            };
            n = c;
        }
        Err(MstError::Invalid("tree too deep"))
    }

    /// The walk of [`walk`](Self::walk) if every node on it is loaded:
    /// the persisted nodes it notes (empty without `note`).
    fn loaded_path(&self, key: &[u8], mut mode: Mode, note: bool) -> Option<Vec<(Cid, Key, i32)>> {
        let mut n: &Node = &self.tree.root;
        let mut path = Vec::new();
        for _ in 0..=MAX_DEPTH {
            if n.stub {
                return None;
            }
            let idx = step(n, key, &mut mode);
            if note && n.height >= self.persist_min {
                if let Some(c) = n.cid {
                    path.push((c, n.height));
                }
            }
            let Some(idx) = idx else {
                return Some(match any_key(n) {
                    Some(k) => path.into_iter().map(|(c, h)| (c, k.clone(), h)).collect(),
                    None => Vec::new(),
                });
            };
            match &n.entries[idx] {
                Entry::Child { node: Some(c), .. } => n = c,
                _ => return None,
            }
        }
        None
    }

    /// Loads everything a mutation at `key` visits: its own search path and
    /// those of its neighbours.
    fn prepare(&mut self, key: &[u8], src: &dyn Source) -> Result<()> {
        self.walk(key, Mode::Key, true, src)?;
        self.walk(key, Mode::Before, true, src)?;
        self.walk(key, Mode::After, true, src)
    }

    /// Loads what operations at `keys` will visit, without noting anything
    /// (with a source that may not do I/O, the pass that finds what an
    /// asynchronous fetch has to load first: [`MstError::NotLoaded`]).
    /// A later batch op re-walks its paths for free.
    pub fn fetch(&mut self, keys: &[&[u8]], probes: &[&[u8]], src: &dyn Source) -> Result<()> {
        for k in keys {
            self.walk(k, Mode::Key, false, src)?;
            self.walk(k, Mode::Before, false, src)?;
            self.walk(k, Mode::After, false, src)?;
        }
        for p in probes {
            self.walk(p, Mode::Key, false, src)?;
        }
        Ok(())
    }

    /// Whether any key starts with `prefix` (a collection's `coll/`): the
    /// smallest key >= `prefix`, found on its loaded search path.
    pub fn has_prefix(&mut self, prefix: &[u8], src: &dyn Source) -> Result<bool> {
        self.walk(prefix, Mode::Key, false, src)?;
        let mut n: &Node = &self.tree.root;
        let mut next: Option<&Key> = None;
        for _ in 0..=MAX_DEPTH {
            if n.stub {
                return Err(MstError::Partial);
            }
            // the first value above `prefix` here bounds every key below it
            let mut gap = None;
            let mut nearer = None;
            for (i, e) in n.entries.iter().enumerate() {
                match e {
                    Entry::Value { key, .. } if &key[..] >= prefix => {
                        nearer = Some(key);
                        break;
                    }
                    Entry::Value { .. } => gap = None,
                    Entry::Child { .. } => gap = Some(i),
                }
            }
            if let Some(k) = nearer {
                if &k[..] == prefix {
                    return Ok(true);
                }
                next = Some(k);
            }
            match gap.map(|i| &n.entries[i]) {
                Some(Entry::Child { node: Some(c), .. }) => n = c,
                Some(_) => return Err(MstError::Partial),
                None => return Ok(next.is_some_and(|k| k.starts_with(prefix))),
            }
        }
        Err(MstError::Invalid("tree too deep"))
    }

    /// Loads the whole tree (an account delete or repo import needs every
    /// key and node of the current one). Unloaded subtrees come from `src` in
    /// key order, so a [`Source`] over one forward record scan serves it.
    pub fn load_all(&mut self, src: &dyn Source) -> Result<()> {
        #[allow(clippy::too_many_arguments)]
        fn rec(n: &mut Arc<Node>, lo: Option<Key>, hi: Option<Key>, pm: i32, src: &dyn Source, stats: &mut LoadStats, depth: usize) -> Result<()> {
            if depth > MAX_DEPTH {
                return Err(MstError::Invalid("tree too deep"));
            }
            if n.stub {
                return Err(MstError::Partial);
            }
            if !n.entries.iter().any(|e| matches!(e, Entry::Child { .. })) {
                return Ok(());
            }
            let nm = Arc::make_mut(n);
            for i in 0..nm.entries.len() {
                let Entry::Child { node, cid } = &nm.entries[i] else { continue };
                let clo = if i > 0 { value_key(nm.entries.get(i - 1)) } else { lo.clone() };
                let chi = value_key(nm.entries.get(i + 1)).or_else(|| hi.clone());
                if node.is_none() {
                    let c = cid.ok_or(MstError::Partial)?;
                    let child = load_subtree(src, pm, c, nm.height - 1, clo.as_deref(), chi.as_deref(), stats)?;
                    nm.entries[i] = Entry::Child { node: Some(child), cid: Some(c) };
                }
                let Entry::Child { node: Some(c), .. } = &mut nm.entries[i] else { unreachable!() };
                rec(c, clo, chi, pm, src, stats, depth + 1)?;
            }
            Ok(())
        }
        rec(&mut self.tree.root, None, None, self.persist_min, src, &mut self.stats, 0)
    }

    /// Whether every node is loaded.
    pub fn fully_loaded(&self) -> bool {
        fn rec(n: &Node) -> bool {
            n.entries.iter().all(|e| match e {
                Entry::Child { node: Some(c), .. } => rec(c),
                Entry::Child { node: None, .. } => false,
                Entry::Value { .. } => true,
            })
        }
        rec(&self.tree.root)
    }

    pub fn get(&mut self, key: &[u8], src: &dyn Source) -> Result<Option<Cid>> {
        self.walk(key, Mode::Key, false, src)?;
        self.tree.get(key)
    }

    /// Inserts or updates (proof nodes marked, as `Tree::insert`).
    pub fn insert(&mut self, key: &[u8], val: Cid, src: &dyn Source) -> Result<Option<Cid>> {
        self.prepare(key, src)?;
        self.tree.insert(key, val)
    }

    pub fn remove(&mut self, key: &[u8], src: &dyn Source) -> Result<Option<Cid>> {
        self.prepare(key, src)?;
        self.tree.remove(key)
    }

    /// The commit's MST blocks and root (exactly `Tree::write_diff_blocks`),
    /// plus the persisted-node changes for the state batch.
    pub fn write_diff_blocks(&mut self, out: &mut Vec<(Cid, Vec<u8>)>) -> Result<(Cid, Persist)> {
        self.write_diff_blocks_with_refs(out, None)
    }

    /// [`write_diff_blocks`](Self::write_diff_blocks), also reporting where
    /// the written nodes sit (`Tree::write_diff_blocks_with_refs`: nodes
    /// whose first key is in an unloaded subtree are left out; all have
    /// keys of their own but interior ones, which `M/` finds by CID).
    pub fn write_diff_blocks_with_refs(&mut self, out: &mut Vec<(Cid, Vec<u8>)>, report: Option<&mut Vec<(Cid, crate::mst::NodeRef)>>) -> Result<(Cid, Persist)> {
        let start = out.len();
        let mut refs = Vec::new();
        let root = self.tree.write_diff_blocks_with_refs(out, &mut refs)?;
        if let Some(r) = report {
            r.extend(refs.iter().cloned());
        }
        let mut heights: HashMap<Cid, i32> = refs.iter().map(|(c, (_, h))| (*c, *h)).collect();
        if heights.len() < out.len() - start {
            // refs leave out nodes whose first key is in an unloaded subtree:
            // written nodes hang together from the root, so find them there
            let written: HashSet<Cid> = out[start..].iter().map(|(c, _)| *c).collect();
            fn rec(n: &Node, written: &HashSet<Cid>, heights: &mut HashMap<Cid, i32>) {
                // the empty tree's root isn't persisted (it has no height)
                let Some(c) = n.cid.filter(|c| written.contains(c) && !n.entries.is_empty()) else { return };
                heights.insert(c, n.height);
                for e in &n.entries {
                    if let Entry::Child { node: Some(ch), .. } = e {
                        rec(ch, written, heights);
                    }
                }
            }
            rec(&self.tree.root, &written, &mut heights);
        }
        let mut kept = HashSet::new();
        let mut deletes = HashSet::new();
        for (c, k, h) in std::mem::take(&mut self.seen) {
            if kept.contains(&c) || deletes.contains(&c) {
                continue;
            }
            match self.present(&c, &k, h) {
                true => kept.insert(c),
                false => deletes.insert(c),
            };
        }
        // every written node at a persisted height, proof-only neighbours
        // (already stored) included: exactly what replay derives from the
        // commit's CAR (`persisted_blocks`), in the same order
        let puts: Vec<(Cid, Vec<u8>)> = out[start..]
            .iter()
            .filter(|(c, _)| heights.get(c).is_some_and(|h| *h >= self.persist_min))
            .cloned()
            .collect();
        Ok((root, Persist { puts, deletes: deletes.into_iter().collect() }))
    }

    /// Whether the written tree holds node `cid` (at `height`, on the path
    /// to `key`, the one place it can be). Nodes seen by a batch stay
    /// loaded until its write (unloading is between commits), so an
    /// unloaded subtree on the way doesn't hold it.
    fn present(&self, cid: &Cid, key: &[u8], height: i32) -> bool {
        let mut n: &Node = &self.tree.root;
        loop {
            if n.height <= height {
                return n.height == height && n.cid == Some(*cid);
            }
            match locate(n, key) {
                Loc::Gap(Some(i)) => match &n.entries[i] {
                    Entry::Child { node: Some(c), .. } => n = c,
                    _ => return false,
                },
                _ => return false,
            }
        }
    }

    /// Inclusion / exclusion proof of `key` (exactly `Tree::proof_blocks`).
    pub fn proof_blocks(&mut self, key: &[u8], src: &dyn Source) -> Result<Vec<(Cid, Vec<u8>)>> {
        self.walk(key, Mode::Key, false, src)?;
        self.tree.proof_blocks(key)
    }

    /// Drops every loaded subtree whose CID isn't in `keep` (call between
    /// writes), keeping the root: with `keep` = the nodes written by
    /// commits still in flight, what remains to read back is durable. A
    /// subtree changed by such a commit has its (new) CID in `keep`, and so
    /// does every node above it, which are kept and descended into.
    pub fn unload_except(&mut self, keep: &HashSet<Cid>) {
        fn rec(n: &mut Arc<Node>, keep: &HashSet<Cid>) {
            if !n.entries.iter().any(|e| matches!(e, Entry::Child { node: Some(_), .. })) {
                return;
            }
            let nm = Arc::make_mut(n);
            for e in nm.entries.iter_mut() {
                if let Entry::Child { node, cid } = e {
                    let Some(c) = node else { continue };
                    match c.cid {
                        Some(cc) if !c.dirty && !keep.contains(&cc) => {
                            *cid = Some(cc);
                            *node = None;
                        }
                        _ => rec(c, keep),
                    }
                }
            }
        }
        rec(&mut self.tree.root, keep);
    }

    /// Drops loaded subtrees more than `depth` levels below the root (clean
    /// ones: call after a write). `unload(0)` keeps just the root.
    pub fn unload(&mut self, depth: usize) {
        fn rec(n: &mut Arc<Node>, depth: usize) {
            if !n.entries.iter().any(|e| matches!(e, Entry::Child { node: Some(_), .. })) {
                return;
            }
            let nm = Arc::make_mut(n);
            for e in nm.entries.iter_mut() {
                if let Entry::Child { node, cid } = e {
                    let Some(c) = node else { continue };
                    if depth == 0 && !c.dirty && c.cid.is_some() {
                        *cid = c.cid;
                        *node = None;
                    } else if depth > 0 {
                        rec(c, depth - 1);
                    }
                }
            }
        }
        rec(&mut self.tree.root, depth);
    }

    pub fn heap_bytes(&self) -> usize {
        heap_bytes(&self.tree.root)
    }

    pub fn loaded_nodes(&self) -> usize {
        loaded_nodes(&self.tree.root)
    }
}

// ---------- persistence of a commit's nodes ----------

/// The blocks of a commit (`blocks`, in CAR order) that are nodes of the
/// tree at `root` with height >= `persist_min`, in CAR order: a commit's
/// `M/` puts. The worker and replay (`segment::derive_commit_muts_n`) derive
/// them with this one function from the same blocks, so they agree. Nodes
/// are found from the root through the links whose blocks the commit
/// carries (every written node hangs from the root; record blocks are only
/// ever values), and a node's height is its keys' (a node without keys
/// takes its parent's minus one).
pub fn persisted_blocks<'a>(root: &Cid, blocks: &[(Cid, &'a [u8])], persist_min: i32) -> Result<Vec<(Cid, &'a [u8])>> {
    let by: HashMap<Cid, &[u8]> = blocks.iter().map(|(c, b)| (*c, *b)).collect();
    let mut seen = HashSet::new();
    let mut keep = HashSet::new();
    let mut stack = vec![(*root, None::<i32>, 0usize)];
    while let Some((c, parent, depth)) = stack.pop() {
        if depth > MAX_DEPTH {
            return Err(MstError::Invalid("tree too deep"));
        }
        let Some(b) = by.get(&c) else { continue };
        if !seen.insert(c) {
            continue;
        }
        let n = decode_node(b, c)?;
        if n.entries.is_empty() {
            continue; // the empty tree's root: no height, never persisted
        }
        let height = match (n.height, parent) {
            (h, None) if h >= 0 => h,
            (h, Some(p)) if h < 0 || h == p - 1 => p - 1,
            _ => return Err(MstError::Invalid("node at the wrong height")),
        };
        if height >= persist_min {
            keep.insert(c);
        }
        for e in &n.entries {
            if let Entry::Child { cid: Some(cc), .. } = e {
                stack.push((*cc, Some(height), depth + 1));
            }
        }
    }
    Ok(blocks.iter().filter(|(c, _)| keep.contains(c)).map(|(c, b)| (*c, *b)).collect())
}

// ---------- export (getRepo) ----------

/// Every node block of the tree at `root`, in `Tree::walk_blocks` order
/// (pre-order), streamed from the store without a resident tree: persisted
/// nodes by CID, lower subtrees rebuilt from their record ranges (which a
/// real export reads with the one forward `R/` scan that also yields the
/// records). Memory: one root-to-leaf path of nodes.
pub fn export_blocks(
    root: Cid,
    persist_min: i32,
    src: &dyn Source,
    f: &mut dyn FnMut(Cid, &[u8]),
) -> Result<LoadStats> {
    let mut stats = LoadStats::default();
    let Some(r) = read_node(src, &root, None, &mut stats)? else {
        let t = LazyTree::open(root, persist_min, src)?;
        t.tree.walk_blocks(f)?;
        return Ok(t.stats);
    };
    fn emit(n: &Node, f: &mut dyn FnMut(Cid, &[u8])) -> Result<()> {
        let c = n.cid.ok_or(MstError::Invalid("unwritten node"))?;
        match &n.bytes {
            Some(b) => f(c, b),
            None => {
                let mut buf = Vec::new();
                encode_node(n, &mut buf)?;
                f(c, &buf)
            }
        }
        Ok(())
    }
    fn visit(
        n: &Node,
        lo: Option<Key>,
        hi: Option<Key>,
        persist_min: i32,
        src: &dyn Source,
        f: &mut dyn FnMut(Cid, &[u8]),
        stats: &mut LoadStats,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(MstError::Invalid("tree too deep"));
        }
        emit(n, f)?;
        for (i, e) in n.entries.iter().enumerate() {
            let Entry::Child { cid: Some(c), .. } = e else { continue };
            let clo = if i > 0 { value_key(n.entries.get(i - 1)) } else { lo.clone() };
            let chi = value_key(n.entries.get(i + 1)).or_else(|| hi.clone());
            let child = load_subtree(src, persist_min, *c, n.height - 1, clo.as_deref(), chi.as_deref(), stats)?;
            if child.height >= persist_min && !child.entries.iter().any(|e| matches!(e, Entry::Child { node: Some(_), .. })) {
                visit(&child, clo, chi, persist_min, src, f, stats, depth + 1)?;
            } else {
                // rebuilt from records: fully loaded, walk it in place
                let mut t = Tree::new();
                t.root = child;
                t.walk_blocks(f)?;
            }
        }
        Ok(())
    }
    visit(&r, None, None, persist_min, src, f, &mut stats, 0)?;
    Ok(stats)
}

// ---------- in-memory store (tests, benches) ----------

/// `M/` and `R/` of one repo, in memory.
#[derive(Clone, Default)]
pub struct MemStore {
    pub nodes: HashMap<Cid, Arc<[u8]>>,
    pub records: BTreeMap<Key, Cid>,
}

impl MemStore {
    /// A store holding a fully loaded, written tree: its records, and its
    /// nodes of height >= `persist_min` (the backfill of a repo).
    pub fn from_tree(tree: &Tree, persist_min: i32) -> MemStore {
        let mut s = MemStore::default();
        tree.walk(&mut |k, c| {
            s.records.insert(Arc::from(k), c);
        });
        s.nodes = persisted_nodes(tree, persist_min);
        s
    }

    pub fn apply(&mut self, p: &Persist) {
        for c in &p.deletes {
            self.nodes.remove(c);
        }
        for (c, b) in &p.puts {
            self.nodes.insert(*c, Arc::from(&b[..]));
        }
    }

    pub fn node_bytes(&self) -> usize {
        self.nodes.values().map(|b| b.len()).sum()
    }
}

/// The nodes of a fully loaded, written tree that `persist_min` persists
/// (an empty root has no height and isn't).
pub fn persisted_nodes(tree: &Tree, persist_min: i32) -> HashMap<Cid, Arc<[u8]>> {
    fn rec(n: &Node, persist_min: i32, out: &mut HashMap<Cid, Arc<[u8]>>) {
        if n.height >= persist_min && !n.entries.is_empty() {
            let b = match &n.bytes {
                Some(b) => b.clone(),
                None => {
                    let mut buf = Vec::new();
                    encode_node(n, &mut buf).expect("written tree");
                    Arc::from(buf)
                }
            };
            out.insert(n.cid.expect("written tree"), b);
        }
        for e in &n.entries {
            if let Entry::Child { node: Some(c), .. } = e {
                rec(c, persist_min, out);
            }
        }
    }
    let mut out = HashMap::new();
    rec(&tree.root, persist_min, &mut out);
    out
}

impl Source for MemStore {
    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>> {
        Ok(self.nodes.get(cid).cloned())
    }

    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(Key, Cid)>) -> Result<()> {
        let lo = lo.map_or(Bound::Unbounded, Bound::Excluded);
        let hi = hi.map_or(Bound::Unbounded, Bound::Excluded);
        out.extend(self.records.range::<[u8], _>((lo, hi)).map(|(k, c)| (k.clone(), *c)));
        Ok(())
    }
}
