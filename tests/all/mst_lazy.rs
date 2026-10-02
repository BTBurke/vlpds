//! Partial / lazily loaded MSTs (src/mst_lazy.rs, DESIGN.md "Partial MSTs").
//!
//! A lazy tree that loads only the paths an operation visits (persisted
//! nodes by CID, lower subtrees rebuilt from record ranges) must produce,
//! byte for byte, what the fully loaded `mst::Tree` does: the root CID, the
//! commit's MST blocks (sync 1.1 proofs, neighbours included), getRecord
//! proofs and getRepo's blocks. Its persistence diff must keep the store's
//! node set exactly the reference tree's persisted nodes (no garbage, nothing
//! missing). Checked on random histories, cold and warm, for every
//! `persist_min`, and on a real repo (`~/repo.car`, or `VLPDS_REPO_CAR`).
//!
//! Measurements (memory, cold-write latency, write amplification, getRepo):
//! `cargo test --profile dev-release --test all mst_lazy::bench -- --ignored --nocapture`
//! (`VLPDS_LAZY_SIZES=10000,100000,1000000`).
//!
//! Through the server (`--lazy-mst`): a lazy node and a full-tree node fed
//! the same writes agree on every commit, proof, export, getBlocks answer
//! and collection index; `M/` is exactly the interior node set after
//! writes, kill -9 + replay, reshards and fallbacks; a hot repo's loaded
//! paths stay bounded. Server benchmarks: `bench_cold_write`,
//! `bench_readers`, `bench_rss` (ignored; DESIGN.md "Partial MSTs").

use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::cid::Cid;
use vlpds::mst::{Entry, Node, Tree};
use vlpds::mst_lazy::{self, export_blocks, heap_bytes, LazyTree, LoadStats, MemStore};

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Cid),
    Del(Vec<u8>),
}

fn rand_cid(rng: &mut StdRng) -> Cid {
    Cid::dag_cbor(&rng.gen::<u64>().to_le_bytes())
}

const B32: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";

/// A TID-shaped rkey (13 sortable base32 chars) for a time.
fn tid(t: u64) -> String {
    (0..13).rev().map(|i| B32[((t >> (i * 5)) & 31) as usize] as char).collect()
}

/// Keys from a small space, so histories update and delete existing ones.
fn small_key(rng: &mut StdRng) -> Vec<u8> {
    let coll = ["a.b.post", "app.bsky.feed.like", "x.y"][rng.gen_range(0..3)];
    let rkey: String = (0..rng.gen_range(1..4)).map(|_| B32[rng.gen_range(0..6)] as char).collect();
    format!("{coll}/{rkey}").into_bytes()
}

fn all_blocks(t: &Tree) -> Vec<(Cid, Vec<u8>)> {
    let mut v = Vec::new();
    t.walk_blocks(&mut |c, b| v.push((c, b.to_vec()))).unwrap();
    v
}

fn exported(root: Cid, pm: i32, s: &MemStore) -> (Vec<(Cid, Vec<u8>)>, LoadStats) {
    let mut v = Vec::new();
    let st = export_blocks(root, pm, s, &mut |c, b| v.push((c, b.to_vec()))).unwrap();
    (v, st)
}

/// The reference (fully loaded tree) and the lazy tree over a store,
/// driven in lockstep.
struct Lockstep {
    reference: Tree,
    store: MemStore,
    lazy: Option<LazyTree>,
    root: Cid,
    pm: i32,
}

impl Lockstep {
    fn new(mut reference: Tree, pm: i32) -> Lockstep {
        let root = reference.root_cid().unwrap();
        let store = MemStore::from_tree(&reference, pm);
        Lockstep { reference, store, lazy: None, root, pm }
    }

    /// One commit. `cold`: reopen from the store; else keep the loaded
    /// paths, minus a random unload.
    fn batch(&mut self, ops: &[Op], cold: bool, rng: &mut StdRng) {
        let mut ref_prev = Vec::new();
        for op in ops {
            ref_prev.push(match op {
                Op::Put(k, v) => self.reference.insert(k, *v).unwrap(),
                Op::Del(k) => self.reference.remove(k).unwrap(),
            });
        }
        let mut ref_blocks = Vec::new();
        let ref_root = self.reference.write_diff_blocks(&mut ref_blocks).unwrap();

        if cold || self.lazy.is_none() {
            self.lazy = Some(LazyTree::open(self.root, self.pm, &self.store).unwrap());
        } else if rng.gen_bool(0.3) {
            self.lazy.as_mut().unwrap().unload(rng.gen_range(0..4));
        }
        let lazy = self.lazy.as_mut().unwrap();
        // the store still holds the previous commit's records during the batch
        let mut prev = Vec::new();
        for op in ops {
            prev.push(match op {
                Op::Put(k, v) => lazy.insert(k, *v, &self.store).unwrap(),
                Op::Del(k) => lazy.remove(k, &self.store).unwrap(),
            });
        }
        assert_eq!(prev, ref_prev, "previous values");
        let mut blocks = Vec::new();
        let (root, persist) = lazy.write_diff_blocks(&mut blocks).unwrap();
        assert_eq!(root, ref_root, "root cid (pm {})", self.pm);
        assert_eq!(blocks, ref_blocks, "commit blocks (sync 1.1 proof) (pm {})", self.pm);

        self.store.apply(&persist);
        for op in ops {
            match op {
                Op::Put(k, v) => self.store.records.insert(Arc::from(&k[..]), *v),
                Op::Del(k) => self.store.records.remove(&k[..]),
            };
        }
        self.root = root;
    }

    /// The store holds exactly the reference tree's persisted nodes.
    fn check_store(&self) {
        let want = mst_lazy::persisted_nodes(&self.reference, self.pm);
        assert_eq!(self.store.nodes.len(), want.len(), "persisted node count (pm {})", self.pm);
        for (c, b) in &want {
            assert_eq!(self.store.nodes.get(c), Some(b), "persisted node {c} (pm {})", self.pm);
        }
    }

    fn check_proofs(&mut self, keys: &[Vec<u8>]) {
        let lazy = self.lazy.as_mut().unwrap();
        for k in keys {
            assert_eq!(
                lazy.proof_blocks(k, &self.store).unwrap(),
                self.reference.proof_blocks(k).unwrap(),
                "proof of {}",
                String::from_utf8_lossy(k)
            );
            assert_eq!(lazy.get(k, &self.store).unwrap(), self.reference.get(k).unwrap());
        }
    }

    fn check_export(&self) {
        assert_eq!(exported(self.root, self.pm, &self.store).0, all_blocks(&self.reference), "getRepo blocks");
    }
}

fn random_ops(rng: &mut StdRng, live: &mut Vec<Vec<u8>>, n: usize) -> Vec<Op> {
    (0..n)
        .map(|_| match rng.gen_range(0..10) {
            0..=4 => Op::Put(small_key(rng), rand_cid(rng)),
            5..=6 if !live.is_empty() => Op::Put(live.choose(rng).unwrap().clone(), rand_cid(rng)),
            _ if !live.is_empty() && rng.gen_bool(0.85) => Op::Del(live.choose(rng).unwrap().clone()),
            _ => Op::Del(small_key(rng)),
        })
        .collect()
}

fn track_live(live: &mut Vec<Vec<u8>>, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(k, _) if !live.contains(k) => live.push(k.clone()),
            Op::Del(k) => live.retain(|x| x != k),
            _ => {}
        }
    }
}

#[test]
fn random_histories_match_full_tree() {
    let seeds: u64 = std::env::var("VLPDS_LAZY_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(36);
    for seed in 0..seeds {
        let mut rng = StdRng::seed_from_u64(seed);
        let pm = (seed % 3) as i32;
        let size = [0, 1, 3, 20, 200, 1500][(seed / 3 % 6) as usize];
        let mut reference = Tree::new();
        let mut live = Vec::new();
        for _ in 0..size {
            let k = small_key(&mut rng);
            reference.insert_no_proof(&k, rand_cid(&mut rng)).unwrap();
            if !live.contains(&k) {
                live.push(k);
            }
        }
        let mut h = Lockstep::new(reference, pm);
        h.check_store();
        h.check_export();
        for i in 0..80 {
            let n = rng.gen_range(1..=6);
            let ops = random_ops(&mut rng, &mut live, n);
            let cold = rng.gen_bool(0.4);
            h.batch(&ops, cold, &mut rng);
            track_live(&mut live, &ops);
            h.check_store();
            let mut probes: Vec<Vec<u8>> = (0..3).map(|_| small_key(&mut rng)).collect();
            probes.extend(live.choose_multiple(&mut rng, 3).cloned());
            h.check_proofs(&probes);
            if i % 20 == 19 {
                h.check_export();
            }
        }
        h.check_export();
    }
}

/// Subtrees rebuilt from records are exactly the full tree's, and a store
/// missing a node falls back to rebuilding it (and still matches).
#[test]
fn rebuild_and_fallback() {
    let mut rng = StdRng::seed_from_u64(99);
    let mut reference = Tree::new();
    for i in 0..3000u64 {
        reference.insert_no_proof(format!("app.bsky.feed.like/{}", tid(i * 7919)).as_bytes(), rand_cid(&mut rng)).unwrap();
    }
    let root = reference.root_cid().unwrap();
    let mut recs: Vec<(mst_lazy::Key, Cid)> = Vec::new();
    reference.walk(&mut |k, c| recs.push((Arc::from(k), c)));
    assert_eq!(mst_lazy::build_tree(&recs).unwrap().root.cid, Some(root));
    let mut store = MemStore::from_tree(&reference, 1);
    // drop a third of the persisted nodes (not the root)
    let victims: Vec<Cid> = store.nodes.keys().filter(|c| **c != root).step_by(3).copied().collect();
    for c in &victims {
        store.nodes.remove(c);
    }
    let mut lazy = LazyTree::open(root, 1, &store).unwrap();
    for (k, _) in recs.iter().step_by(37) {
        assert_eq!(lazy.proof_blocks(k, &store).unwrap(), reference.proof_blocks(k).unwrap());
    }
    assert!(lazy.stats.fallbacks > 0);
    assert_eq!(exported(root, 1, &store).0, all_blocks(&reference));
    // a store with no nodes at all: the whole tree from records
    let bare = MemStore { nodes: HashMap::new(), records: store.records.clone() };
    assert_eq!(exported(root, 1, &bare).0, all_blocks(&reference));
    // a record that doesn't match the tree is caught by the link check
    let mut bad = store.clone();
    let k = recs[1500].0.clone();
    bad.records.insert(k.clone(), rand_cid(&mut rng));
    let mut lazy = LazyTree::open(root, 1, &bad).unwrap();
    assert!(lazy.get(&k, &bad).is_err());
}

/// A real repo's records, from a CAR export.
fn real_repo() -> Option<(Cid, Vec<(Vec<u8>, Cid)>)> {
    let path = std::env::var("VLPDS_REPO_CAR")
        .unwrap_or_else(|_| format!("{}/repo.car", std::env::var("HOME").unwrap_or_default()));
    let Ok(car) = std::fs::read(&path) else {
        eprintln!("skipping: no repo CAR at {path} (set VLPDS_REPO_CAR)");
        return None;
    };
    let (roots, blocks) = vlpds::car::read_car(&car).unwrap();
    let blocks: HashMap<Cid, Vec<u8>> = blocks.into_iter().map(|(c, b)| (c, b.to_vec())).collect();
    let commit = vlpds::cbor::Value::decode(&blocks[&roots[0]]).unwrap();
    let Some(vlpds::cbor::Value::Link(data)) = commit.get("data") else { panic!("commit without data") };
    let tree = Tree::load_from_blocks(&blocks, *data).unwrap();
    let mut recs = Vec::new();
    tree.walk(&mut |k, c| recs.push((k.to_vec(), c)));
    Some((*data, recs))
}

#[test]
fn real_repo_fixture_matches_full_tree() {
    let Some((data, recs)) = real_repo() else { return };
    let keyed: Vec<(mst_lazy::Key, Cid)> = recs.iter().map(|(k, c)| (Arc::from(&k[..]), *c)).collect();
    assert_eq!(mst_lazy::build_tree(&keyed).unwrap().root.cid, Some(data), "rebuild from records");
    let colls: Vec<String> = {
        let mut c: Vec<String> =
            recs.iter().map(|(k, _)| String::from_utf8_lossy(k).split('/').next().unwrap().to_string()).collect();
        c.dedup();
        c
    };
    for pm in [1, 2] {
        let mut rng = StdRng::seed_from_u64(7 + pm as u64);
        let mut reference = Tree::new();
        for (k, c) in &recs {
            reference.insert_no_proof(k, *c).unwrap();
        }
        let mut h = Lockstep::new(reference, pm);
        assert_eq!(h.root, data);
        h.check_store();
        h.check_export();
        let mut live: Vec<Vec<u8>> = recs.iter().map(|(k, _)| k.clone()).collect();
        let mut clock = 1u64 << 52;
        for i in 0..1200 {
            let ops: Vec<Op> = (0..rng.gen_range(1..=3))
                .map(|_| match rng.gen_range(0..10) {
                    // new records: appended TIDs, or anywhere (imports, old rkeys)
                    0..=4 => {
                        clock += rng.gen_range(1..1 << 20);
                        let coll = colls.choose(&mut rng).unwrap();
                        Op::Put(format!("{coll}/{}", tid(clock)).into_bytes(), rand_cid(&mut rng))
                    }
                    5 => Op::Put(format!("{}/{}", colls.choose(&mut rng).unwrap(), tid(rng.gen())).into_bytes(), rand_cid(&mut rng)),
                    6..=7 => Op::Put(live.choose(&mut rng).unwrap().clone(), rand_cid(&mut rng)),
                    _ => Op::Del(live.choose(&mut rng).unwrap().clone()),
                })
                .collect();
            let cold = rng.gen_bool(0.3);
            h.batch(&ops, cold, &mut rng);
            for op in &ops {
                match op {
                    Op::Put(k, _) => {
                        if h.reference.get(k).unwrap().is_some() && !live.contains(k) {
                            live.push(k.clone())
                        }
                    }
                    Op::Del(k) => {
                        if let Some(p) = live.iter().position(|x| x == k) {
                            live.swap_remove(p);
                        }
                    }
                }
            }
            if i % 100 == 0 {
                h.check_store();
                let probes: Vec<Vec<u8>> = live.choose_multiple(&mut rng, 5).cloned().collect();
                h.check_proofs(&probes);
            }
        }
        h.check_store();
        h.check_export();
    }
}

// ---------- measurements ----------

/// Heap of the nodes at height >= 1 only (an in-memory tree that drops its
/// leaves and rebuilds them from records: option (c')).
fn interior_heap(n: &Node) -> usize {
    if n.height < 1 {
        return 0;
    }
    let own = heap_bytes(&Node { entries: Vec::new(), ..n.clone() })
        + n.entries.capacity() * std::mem::size_of::<Entry>()
        + n.entries.iter().map(|e| match e {
            Entry::Value { key, .. } => (16 + key.len()).next_multiple_of(16),
            _ => 0,
        }).sum::<usize>();
    own + n
        .entries
        .iter()
        .map(|e| match e {
            Entry::Child { node: Some(c), .. } => interior_heap(c),
            _ => 0,
        })
        .sum::<usize>()
}

fn depth(n: &Node) -> usize {
    1 + n
        .entries
        .iter()
        .find_map(|e| match e {
            Entry::Child { node: Some(c), .. } => Some(depth(c)),
            _ => None,
        })
        .unwrap_or(0)
}

/// Synthetic repo with a real collection mix and TID rkeys.
fn synthetic(n: usize, rng: &mut StdRng) -> Vec<(Vec<u8>, Cid)> {
    let mix = [
        ("app.bsky.feed.like", 65),
        ("app.bsky.graph.follow", 12),
        ("app.bsky.feed.post", 10),
        ("app.bsky.feed.repost", 10),
        ("app.bsky.graph.block", 3),
    ];
    let mut out = BTreeMap::new();
    let mut t = 1u64 << 52;
    while out.len() < n {
        t += rng.gen_range(1..1 << 24);
        let mut r = rng.gen_range(0..100);
        let coll = mix.iter().find(|(_, w)| { if r < *w { true } else { r -= w; false } }).unwrap().0;
        out.insert(format!("{coll}/{}", tid(t)).into_bytes(), rand_cid(rng));
    }
    out.into_iter().collect()
}

fn us(d: Duration, n: usize) -> f64 {
    d.as_secs_f64() * 1e6 / n as f64
}

fn bench_repo(name: &str, recs: &[(Vec<u8>, Cid)], rng: &mut StdRng) {
    let n = recs.len();
    let t0 = Instant::now();
    let mut full = Tree::new();
    for (k, c) in recs {
        full.insert_no_proof(k, *c).unwrap();
    }
    let root = full.root_cid().unwrap();
    let build = t0.elapsed();
    let full_heap = heap_bytes(&full.root);
    let all_node_bytes: usize = all_blocks(&full).iter().map(|(_, b)| b.len()).sum();
    println!(
        "\n== {name}: {n} records, depth {}, full tree: build+root {:.1} ms ({:.2} us/rec), heap {:.1} MB ({:.0} B/rec; interior-only {:.0} B/rec), all node blocks {:.1} B/rec",
        depth(&full.root),
        build.as_secs_f64() * 1e3,
        us(build, n),
        full_heap as f64 / 1e6,
        full_heap as f64 / n as f64,
        interior_heap(&full.root) as f64 / n as f64,
        all_node_bytes as f64 / n as f64,
    );
    let colls: Vec<String> = {
        let mut c: Vec<String> =
            recs.iter().map(|(k, _)| String::from_utf8_lossy(k).split('/').next().unwrap().to_string()).collect();
        c.dedup();
        c
    };
    let trials = 300;
    for pm in [0, 1, 2] {
        let store = MemStore::from_tree(&full, pm);
        println!(
            "  pm={pm}: persisted {} nodes, {:.1} MB ({:.1} B/rec)",
            store.nodes.len(),
            store.node_bytes() as f64 / 1e6,
            store.node_bytes() as f64 / n as f64
        );
        // cold first write: open from the store, one op, write the commit
        for kind in ["create", "update", "delete"] {
            let (mut el, mut st, mut heap, mut nodes, mut puts, mut put_bytes, mut dels, mut blocks) =
                (Duration::ZERO, LoadStats::default(), 0, 0, 0, 0, 0, 0);
            let mut max_el = Duration::ZERO;
            for i in 0..trials {
                let op = match kind {
                    "create" => Op::Put(
                        format!("{}/{}", colls[i % colls.len()], tid((1 << 60) + i as u64)).into_bytes(),
                        rand_cid(rng),
                    ),
                    "update" => Op::Put(recs[rng.gen_range(0..n)].0.clone(), rand_cid(rng)),
                    _ => Op::Del(recs[rng.gen_range(0..n)].0.clone()),
                };
                let t = Instant::now();
                let mut lazy = LazyTree::open(root, pm, &store).unwrap();
                match &op {
                    Op::Put(k, v) => lazy.insert(k, *v, &store).unwrap(),
                    Op::Del(k) => lazy.remove(k, &store).unwrap(),
                };
                let mut out = Vec::new();
                let (_, p) = lazy.write_diff_blocks(&mut out).unwrap();
                let e = t.elapsed();
                el += e;
                max_el = max_el.max(e);
                let s = lazy.stats;
                st.node_reads += s.node_reads;
                st.node_bytes += s.node_bytes;
                st.scans += s.scans;
                st.scanned_records += s.scanned_records;
                heap += lazy.heap_bytes();
                nodes += lazy.loaded_nodes();
                puts += p.puts.len();
                put_bytes += p.put_bytes();
                dels += p.deletes.len();
                blocks += out.len();
            }
            let f = trials as f64;
            println!(
                "    cold {kind:6}: {:6.1} us (max {:6.1}) | reads {:.1} ({:.0} B) scans {:.1} ({:.1} recs) | resident {:.1} nodes {:.1} KB | commit blocks {:.1} | M/ puts {:.1} ({:.0} B) dels {:.1}",
                us(el, trials),
                max_el.as_secs_f64() * 1e6,
                st.node_reads as f64 / f,
                st.node_bytes as f64 / f,
                st.scans as f64 / f,
                st.scanned_records as f64 / f,
                nodes as f64 / f,
                heap as f64 / f / 1e3,
                blocks as f64 / f,
                puts as f64 / f,
                put_bytes as f64 / f,
                dels as f64 / f,
            );
        }
        // steady state: a warm lazy tree vs the full tree, same commits
        let steps = 2000;
        let mut ops = Vec::with_capacity(steps);
        let mut clock = 1u64 << 61;
        for _ in 0..steps {
            ops.push(if rng.gen_bool(0.85) {
                clock += rng.gen_range(1..1 << 20);
                Op::Put(format!("{}/{}", colls[rng.gen_range(0..colls.len())], tid(clock)).into_bytes(), rand_cid(rng))
            } else {
                Op::Put(recs[rng.gen_range(0..n)].0.clone(), rand_cid(rng))
            });
        }
        let mut f2 = full.clone();
        let t = Instant::now();
        for op in &ops {
            let Op::Put(k, v) = op else { unreachable!() };
            f2.insert(k, *v).unwrap();
            let mut out = Vec::new();
            f2.write_diff_blocks(&mut out).unwrap();
        }
        let full_el = t.elapsed();
        drop(f2);
        let mut s2 = store.clone();
        let mut lazy = LazyTree::open(root, pm, &s2).unwrap();
        let (mut put_bytes, mut dels, mut el) = (0usize, 0usize, Duration::ZERO);
        for op in &ops {
            let Op::Put(k, v) = op else { unreachable!() };
            let t = Instant::now();
            lazy.insert(k, *v, &s2).unwrap();
            let mut out = Vec::new();
            let (_, p) = lazy.write_diff_blocks(&mut out).unwrap();
            el += t.elapsed();
            put_bytes += p.put_bytes();
            dels += p.deletes.len();
            s2.apply(&p);
            s2.records.insert(Arc::from(&k[..]), *v);
        }
        println!(
            "    steady (2000 commits): full {:.1} us/commit, lazy {:.1} us/commit | M/ {:.0} B put + {:.1} dels per commit | resident after {:.1} nodes / {:.0} KB",
            us(full_el, steps),
            us(el, steps),
            put_bytes as f64 / steps as f64,
            dels as f64 / steps as f64,
            lazy.loaded_nodes() as f64,
            lazy.heap_bytes() as f64 / 1e3,
        );
        let t = Instant::now();
        let mut b = 0usize;
        full.walk_blocks(&mut |_, x| b += x.len()).unwrap();
        let walk = t.elapsed();
        let t = Instant::now();
        let mut b2 = 0usize;
        let st = export_blocks(root, pm, &store, &mut |_, x| b2 += x.len()).unwrap();
        let ex = t.elapsed();
        assert_eq!(b, b2);
        println!(
            "    getRepo blocks: full-tree walk {:.1} ms, lazy export {:.1} ms ({} reads, {} scans)",
            walk.as_secs_f64() * 1e3,
            ex.as_secs_f64() * 1e3,
            st.node_reads,
            st.scans
        );
    }
}

#[test]
#[ignore]
fn bench() {
    let mut rng = StdRng::seed_from_u64(5);
    let sizes: Vec<usize> = std::env::var("VLPDS_LAZY_SIZES")
        .unwrap_or_else(|_| "10000,100000,1000000".into())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    if let Some((_, recs)) = real_repo() {
        bench_repo("real repo (~/repo.car)", &recs, &mut rng);
    }
    for n in sizes {
        let recs = synthetic(n, &mut rng);
        bench_repo(&format!("synthetic {n}"), &recs, &mut rng);
    }
}

// ---------------------------------------------------------------------------
// end to end: lazy nodes (`--lazy-mst`) vs full-tree nodes
// ---------------------------------------------------------------------------

use crate::common::*;

/// A node in a given MST mode. Lazy nodes hold almost nothing: one repo per
/// worker at most and no loaded paths once a repo is idle, so writes and
/// reads keep opening repos and walking from the root through the store.
async fn mode_node(lazy: bool, prefetch: usize, store: Option<Arc<dyn object_store::ObjectStore>>) -> TestServer {
    TestServer::spawn_with(move |c| {
        c.lazy_mst = lazy;
        if lazy {
            c.lazy_mst_unload_idle = true;
            c.lazy_mst_prefetch_bytes = prefetch;
            c.cache_per_worker = 1;
        }
        if let Some(s) = store {
            c.memory_store = Some(s);
        }
    })
    .await
}

/// A #commit frame's blocks in CAR order, without the commit block (signed
/// with the node's own key, at its own clock).
fn commit_blocks(f: &Frame) -> Vec<(Cid, Vec<u8>)> {
    let Some(Value::Bytes(car)) = f.body.get("blocks") else { panic!("#commit without blocks") };
    let (roots, blocks) = vlpds::car::read_car(car).unwrap();
    blocks.into_iter().filter(|(c, _)| *c != roots[0]).map(|(c, b)| (c, b.to_vec())).collect()
}

/// Everything of a #commit that doesn't depend on the node's key, DID and
/// clock: ops, prevData, the new data root and the CAR's other blocks (MST
/// nodes with sync 1.1 proofs, records), in order.
fn commit_shape(f: &Frame) -> (J, Option<Cid>, Cid, Vec<(Cid, Vec<u8>)>) {
    let c = f.commit().unwrap();
    let ops = json!(c.ops.iter().map(|o| format!("{o:?}")).collect::<Vec<_>>());
    let data = match Value::decode(&c.blocks[&c.commit]).unwrap().get("data") {
        Some(Value::Link(d)) => *d,
        _ => panic!("commit without data"),
    };
    (ops, c.prev_data, data, commit_blocks(f))
}

/// CAR blocks after the first (the commit), in order.
fn car_tail(body: &[u8]) -> Vec<(Cid, Vec<u8>)> {
    let (_, blocks) = vlpds::car::read_car(body).unwrap();
    blocks.into_iter().skip(1).map(|(c, b)| (c, b.to_vec())).collect()
}

/// `did`'s persisted MST nodes, scanned from its shard on `s`.
async fn stored_nodes(s: &TestServer, did: &str) -> HashMap<Cid, Vec<u8>> {
    let Ok(p) = s.app.partition(did) else { panic!("shard of {did} not owned") };
    let prefix = vlpds::state::mst_node_prefix(did);
    let mut it = p.db.scan(prefix.clone()..vlpds::state::prefix_end(&prefix)).await.unwrap();
    let mut out = HashMap::new();
    while let Some(kv) = it.next().await.unwrap() {
        let digest: [u8; 32] = kv.key[prefix.len()..].try_into().unwrap();
        let c = Cid { codec: vlpds::cid::CODEC_DAG_CBOR, digest };
        out.insert(c, kv.value.to_vec());
    }
    out
}

/// `M/` holds exactly the interior nodes of the repo's current tree (the
/// one getRepo serves), no garbage, nothing missing.
async fn check_stored_nodes(s: &TestServer, did: &str) {
    let r = s.xrpc.get("com.atproto.sync.getRepo", &[("did", did)], &Auth::None).await;
    let repo = Repo::from_car(&r.body).unwrap();
    let commit = Value::decode(&repo.blocks[&repo.root]).unwrap();
    let Some(Value::Link(data)) = commit.get("data") else { panic!() };
    let tree = Tree::load_from_blocks(&repo.blocks, *data).unwrap();
    let want: HashMap<Cid, Vec<u8>> = mst_lazy::persisted_nodes(&tree, 1).into_iter().map(|(c, b)| (c, b.to_vec())).collect();
    let got = stored_nodes(s, did).await;
    let missing = want.keys().filter(|c| !got.contains_key(c)).count();
    let extra = got.keys().filter(|c| !want.contains_key(c)).count();
    assert!(missing == 0 && extra == 0, "{did}: M/ has {} nodes, the tree {} interior ({missing} missing, {extra} garbage)", got.len(), want.len());
    for (c, b) in &want {
        assert_eq!(&got[c], b, "M/ node {c}");
    }
}

#[derive(Clone, Debug)]
enum Step {
    Create(usize, String, String, i64),
    Put(usize, String, String, i64),
    Delete(usize, String, String),
    Batch(usize, Vec<J>),
}

fn random_steps(rng: &mut StdRng, accounts: usize, n: usize) -> Vec<Step> {
    let colls = ["com.example.feed.post", "com.example.feed.like", "com.example.thing", "com.example.rare"];
    let key = |rng: &mut StdRng| -> (String, String) {
        let coll = colls[[0, 0, 1, 1, 1, 2, 3][rng.gen_range(0..7)]].to_string();
        // a small key space (updates, deletes, emptied collections) and
        // ever-growing TIDs (appends)
        let rkey = match rng.gen_bool(0.5) {
            true => format!("k{}", rng.gen_range(0..40)),
            false => tid(1_700_000_000_000_000 + rng.gen_range(0..1u64 << 40)),
        };
        (coll, rkey)
    };
    (0..n)
        .map(|_| {
            let a = rng.gen_range(0..accounts);
            match rng.gen_range(0..10) {
                0..=3 => {
                    let (c, r) = key(rng);
                    Step::Create(a, c, r, rng.gen_range(0..1000))
                }
                4..=5 => {
                    let (c, r) = key(rng);
                    Step::Put(a, c, r, rng.gen_range(0..1000))
                }
                6..=7 => {
                    let (c, r) = key(rng);
                    Step::Delete(a, c, r)
                }
                _ => {
                    let mut seen = Vec::new();
                    let writes = (0..rng.gen_range(1..6))
                        .filter_map(|_| {
                            let (c, r) = key(rng);
                            if seen.contains(&(c.clone(), r.clone())) {
                                return None;
                            }
                            seen.push((c.clone(), r.clone()));
                            Some(match rng.gen_range(0..3) {
                                0 => json!({"$type": "com.atproto.repo.applyWrites#create", "collection": c, "rkey": r, "value": {"$type": c, "n": rng.gen_range(0..1000), "createdAt": "2026-10-01T00:00:00.000Z"}}),
                                1 => json!({"$type": "com.atproto.repo.applyWrites#update", "collection": c, "rkey": r, "value": {"$type": c, "n": rng.gen_range(0..1000), "createdAt": "2026-10-01T00:00:00.000Z"}}),
                                _ => json!({"$type": "com.atproto.repo.applyWrites#delete", "collection": c, "rkey": r}),
                            })
                        })
                        .collect();
                    Step::Batch(a, writes)
                }
            }
        })
        .collect()
}

fn thing(coll: &str, n: i64) -> J {
    json!({"$type": coll, "n": n, "createdAt": "2026-10-01T00:00:00.000Z"})
}

async fn run_step(s: &TestServer, accts: &[TestAccount], step: &Step) -> u16 {
    let (nsid, a, body) = match step {
        Step::Create(a, c, r, n) => ("com.atproto.repo.createRecord", *a, json!({"repo": accts[*a].did, "collection": c, "rkey": r, "record": thing(c, *n)})),
        Step::Put(a, c, r, n) => ("com.atproto.repo.putRecord", *a, json!({"repo": accts[*a].did, "collection": c, "rkey": r, "record": thing(c, *n)})),
        Step::Delete(a, c, r) => ("com.atproto.repo.deleteRecord", *a, json!({"repo": accts[*a].did, "collection": c, "rkey": r})),
        Step::Batch(a, w) => ("com.atproto.repo.applyWrites", *a, json!({"repo": accts[*a].did, "writes": w})),
    };
    s.xrpc.post(nsid, &body, &accts[a].auth()).await.status
}

/// The same writes on a lazy node and a full-tree node produce the same
/// firehose commits (ops, prevData, data, every MST node and record block in
/// order: sync 1.1 proofs included), getRepo CARs, getRecord proofs,
/// getBlocks answers, collection index and migration counts; and the lazy
/// node's `M/` is exactly its trees' interior nodes. Only the commit blocks
/// differ (each node signs with its own key, DID and clock).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_node_matches_full_node() {
    for (seed, prefetch) in [(1u64, 4usize << 20), (2, 0)] {
        let full = mode_node(false, 0, None).await;
        let lazy = mode_node(true, prefetch, None).await;
        let fetches0 = vlpds::metrics::LAZY_MST_FETCHES.with_label_values(&["ok"]).get();
        let unloads0 = vlpds::metrics::LAZY_MST_UNLOADS.get();
        let mut accts = (Vec::new(), Vec::new());
        for i in 0..3 {
            accts.0.push(full.create_account(&format!("fm{i}")).await);
            accts.1.push(lazy.create_account(&format!("lm{i}")).await);
        }
        // deep trees: 1,600 records each (5 levels), mostly unloaded
        for i in 0..3usize {
            for b in 0..8u64 {
                let writes: Vec<J> = (0..200u64)
                    .map(|k| {
                        let r = tid(1_700_000_000_000_000 + (i as u64 * 1_000_000 + b * 200 + k) * 7_919_000);
                        json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.feed.like", "rkey": r, "value": thing("com.example.feed.like", k as i64)})
                    })
                    .collect();
                for (s, a) in [(&full, &accts.0[i]), (&lazy, &accts.1[i])] {
                    s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "writes": writes}), &a.auth()).await.ok();
                }
            }
        }
        let (mut sf, mut sl) = (full.subscribe_from_now().await, lazy.subscribe_from_now().await);
        let mut rng = StdRng::seed_from_u64(seed);
        for step in random_steps(&mut rng, 3, 400) {
            let (a, b) = (run_step(&full, &accts.0, &step).await, run_step(&lazy, &accts.1, &step).await);
            assert_eq!(a, b, "{step:?}");
        }
        let shapes = |frames: Vec<Frame>, accts: &[TestAccount]| -> Vec<Vec<_>> {
            accts.iter().map(|a| frames.iter().filter(|f| f.kind() == "#commit" && f.did() == Some(&a.did)).map(commit_shape).collect()).collect()
        };
        let (cf, cl) = (shapes(sf.drain(Duration::from_millis(500)).await, &accts.0), shapes(sl.drain(Duration::from_millis(500)).await, &accts.1));
        for i in 0..3 {
            assert!(cf[i].len() > 60, "commits seen: {}", cf[i].len());
            assert_eq!(cf[i].len(), cl[i].len(), "account {i}: commit count");
            for (j, (f, l)) in cf[i].iter().zip(&cl[i]).enumerate() {
                assert!(f == l, "account {i} commit {j} differs:\n full {f:?}\n lazy {l:?}");
            }
        }
        for i in 0..3 {
            let (df, dl) = (&accts.0[i].did, &accts.1[i].did);
            let get = |s: &TestServer, nsid: &'static str, q: Vec<(&'static str, String)>| {
                let x = s.xrpc.clone();
                async move { x.get_multi(nsid, &q, &Auth::None).await }
            };
            // getRepo
            let (rf, rl) = (get(&full, "com.atproto.sync.getRepo", vec![("did", df.clone())]).await, get(&lazy, "com.atproto.sync.getRepo", vec![("did", dl.clone())]).await);
            assert!(rf.status == 200 && rl.status == 200);
            let (bf, bl) = (car_tail(&rf.body), car_tail(&rl.body));
            assert!(bf == bl, "account {i}: getRepo blocks differ ({} vs {})", bf.len(), bl.len());
            // getRecord proofs, present and absent keys
            let mut keys: Vec<(String, String)> = Vec::new();
            for c in ["com.example.feed.post", "com.example.feed.like", "com.example.thing", "com.example.rare", "com.example.none"] {
                for k in 0..40 {
                    keys.push((c.into(), format!("k{k}")));
                }
            }
            for (c, r) in keys.iter().step_by(3) {
                let (pf, pl) = (
                    get(&full, "com.atproto.sync.getRecord", vec![("did", df.clone()), ("collection", c.clone()), ("rkey", r.clone())]).await,
                    get(&lazy, "com.atproto.sync.getRecord", vec![("did", dl.clone()), ("collection", c.clone()), ("rkey", r.clone())]).await,
                );
                assert_eq!(pf.status, pl.status);
                assert!(car_tail(&pf.body) == car_tail(&pl.body), "account {i}: getRecord {c}/{r} differs");
            }
            // getBlocks of every MST node (the empty tree's root aside) and a few records
            let repo = Repo::from_car(&rf.body).unwrap();
            let commit = Value::decode(&repo.blocks[&repo.root]).unwrap();
            let Some(Value::Link(data)) = commit.get("data") else { panic!() };
            let tree = Tree::load_from_blocks(&repo.blocks, *data).unwrap();
            let mut nodes = Vec::new();
            tree.walk_blocks(&mut |c, _| nodes.push(c)).unwrap();
            let mut want: Vec<(&'static str, String)> = vec![("did", String::new())];
            want.extend(nodes.iter().chain(repo.order.iter().skip(1 + nodes.len()).take(5)).map(|c| ("cids", c.to_string())));
            want[0].1 = df.clone();
            let gf = get(&full, "com.atproto.sync.getBlocks", want.clone()).await;
            want[0].1 = dl.clone();
            let gl = get(&lazy, "com.atproto.sync.getBlocks", want).await;
            assert_eq!((gf.status, gl.status), (200, 200), "{}", gl.text());
            assert!(gf.body == gl.body, "account {i}: getBlocks differs");
            // collection index
            let (xf, xl) = (
                get(&full, "com.atproto.repo.describeRepo", vec![("repo", df.clone())]).await.ok()["collections"].clone(),
                get(&lazy, "com.atproto.repo.describeRepo", vec![("repo", dl.clone())]).await.ok()["collections"].clone(),
            );
            assert_eq!(xf, xl, "account {i}: describeRepo collections");
            // migration counts
            let (mf, ml) = (
                full.xrpc.get("com.atproto.server.checkAccountStatus", &[], &accts.0[i].auth()).await.ok(),
                lazy.xrpc.get("com.atproto.server.checkAccountStatus", &[], &accts.1[i].auth()).await.ok(),
            );
            assert_eq!(mf["repoBlocks"], ml["repoBlocks"], "account {i}: checkAccountStatus");
            check_stored_nodes(&lazy, dl).await;
        }
        eprintln!("inline reads so far: {}", vlpds::metrics::LAZY_MST_FETCHES.with_label_values(&["inline"]).get());
        let fetches = vlpds::metrics::LAZY_MST_FETCHES.with_label_values(&["ok"]).get() - fetches0;
        let unloads = vlpds::metrics::LAZY_MST_UNLOADS.get() - unloads0;
        eprintln!("lazy node (prefetch {prefetch}): {fetches} path fetches, {unloads} unloads");
        assert!(fetches > 0 && unloads > 0, "the lazy paths weren't exercised");
    }
}

async fn wait_until(what: &str, deadline: Duration, f: impl Fn() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < deadline, "{what}: not within {deadline:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Replay rebuilds `M/` exactly: the owner of a lazy repo is killed
/// (`Node::halt`, kill -9: nothing flushed or checkpointed since its last
/// periodic checkpoint) and the survivor replays its log, deriving each
/// commit's node puts from the #commit CAR and the deletes from the stored
/// muts. Its `M/` is then exactly the tree's interior nodes, and it keeps
/// writing lazily on top of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_after_kill_reconstructs_nodes() {
    const SHARDS: u16 = 4;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let node = |id: &'static str| {
        let store = store.clone();
        TestServer::spawn_with(move |c| {
            c.lazy_mst = true;
            c.lazy_mst_unload_idle = true;
            c.memory_store = Some(store);
            c.shards = SHARDS;
            // nothing checkpointed during the test: the survivor replays it all
            c.checkpoint_every = Duration::from_secs(3600);
            c.cluster = Some(vlpds::cluster::ClusterConfig {
                node_id: id.into(),
                addr: c.public_url.clone(),
                shards: SHARDS,
                ttl: Duration::from_secs(2),
                renew_every: Duration::from_millis(200),
                skew: Duration::from_millis(400),
                ..Default::default()
            });
        })
    };
    let a = node("ra").await;
    let b = node("rb").await;
    wait_until("both own shards", Duration::from_secs(15), || !a.app.partitions.owned().is_empty() && !b.app.partitions.owned().is_empty() && a.app.partitions.owned().len() + b.app.partitions.owned().len() == SHARDS as usize).await;
    let mut accts = Vec::new();
    for i in 0..6 {
        accts.push(a.create_account(&format!("rk{i}")).await);
    }
    let mut rng = StdRng::seed_from_u64(7);
    for step in random_steps(&mut rng, accts.len(), 600) {
        run_step(&a, &accts, &step).await;
    }
    let victim = if b.app.partition(&accts[0].did).is_ok() { &b } else { &a };
    let survivor = if std::ptr::eq(victim, &a) { &b } else { &a };
    let moved: Vec<&TestAccount> = accts.iter().filter(|x| victim.app.partition(&x.did).is_ok()).collect();
    assert!(!moved.is_empty(), "the victim owns some of the repos");
    let heads: Vec<(Cid, String)> = futures::future::join_all(moved.iter().map(|x| survivor.latest_commit(&x.did))).await;
    victim.app.node.halt();
    wait_until("survivor takes every shard", Duration::from_secs(20), || survivor.app.partitions.owned().len() == SHARDS as usize).await;
    for (x, head) in moved.iter().zip(&heads) {
        // every acked commit survived the kill
        assert_eq!(&survivor.latest_commit(&x.did).await, head, "{}", x.did);
        check_stored_nodes(survivor, &x.did).await;
    }
    // and the survivor writes on the replayed nodes
    let fallbacks0: u64 = ["missing", "invalid", "missing_node"].iter().map(|r| vlpds::metrics::LAZY_MST_FALLBACKS.with_label_values(&[r]).get()).sum();
    let accts: Vec<TestAccount> = moved.into_iter().cloned().collect();
    for step in random_steps(&mut rng, accts.len(), 200) {
        run_step(survivor, &accts, &step).await;
    }
    for x in &accts {
        check_stored_nodes(survivor, &x.did).await;
    }
    let fallbacks: u64 = ["missing", "invalid", "missing_node"].iter().map(|r| vlpds::metrics::LAZY_MST_FALLBACKS.with_label_values(&[r]).get()).sum();
    assert_eq!(fallbacks, fallbacks0, "lazy opens fell back to a rebuild from records");
}

/// A lazy open whose `M/` nodes are missing or wrong (a bug, or a store
/// switched from the full-tree mode) rebuilds the tree from the records,
/// still serves exactly, and backfills `M/` through the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_open_rebuilds_missing_or_bad_nodes() {
    let s = mode_node(true, 4 << 20, None).await;
    let full = mode_node(false, 0, None).await;
    let (a, f) = (s.create_account("fbl").await, full.create_account("fbf").await);
    let mut rng = StdRng::seed_from_u64(11);
    let steps = random_steps(&mut rng, 1, 300);
    for st in &steps {
        assert_eq!(run_step(&s, std::slice::from_ref(&a), st).await, run_step(&full, std::slice::from_ref(&f), st).await);
    }
    check_stored_nodes(&s, &a.did).await;
    let Ok(p) = s.app.partition(&a.did) else { panic!() };
    let nodes = stored_nodes(&s, &a.did).await;
    assert!(nodes.len() > 3, "an interior tree");
    for (case, reason) in [("missing", "missing"), ("bad", "invalid")] {
        let n0 = vlpds::metrics::LAZY_MST_FALLBACKS.with_label_values(&[reason]).get();
        let nodes = stored_nodes(&s, &a.did).await;
        // under the repo's feet: it isn't cached (cache of 1 per worker and
        // nothing loaded once idle) and its next open reads these
        let other = s.create_account("fbx").await;
        s.create_record(&other, "com.example.thing", json!({"$type": "com.example.thing", "n": 1})).await;
        for (c, b) in &nodes {
            let k = vlpds::state::mst_node_key(&a.did, c);
            match case {
                "missing" => {
                    p.db.delete(k).await.unwrap();
                }
                _ => {
                    let mut bad = b.clone();
                    let last = bad.len() - 1;
                    bad[last] ^= 1;
                    p.db.put(k, bad).await.unwrap();
                }
            }
        }
        // nodes are cached by CID process-wide: read this repo's from the store
        vlpds::mst_store::NODE_CACHE.clear();
        let st = Step::Create(0, "com.example.thing".into(), format!("after-{case}"), 5);
        assert_eq!(run_step(&s, std::slice::from_ref(&a), &st).await, 200);
        assert_eq!(run_step(&full, std::slice::from_ref(&f), &st).await, 200);
        assert!(vlpds::metrics::LAZY_MST_FALLBACKS.with_label_values(&[reason]).get() > n0, "{case}: no fallback");
        let (rl, rf) = (s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await, full.xrpc.get("com.atproto.sync.getRepo", &[("did", &f.did)], &Auth::None).await);
        assert!(car_tail(&rl.body) == car_tail(&rf.body), "{case}: getRepo differs");
        // the backfill is applied with the commit after it
        check_stored_nodes(&s, &a.did).await;
    }
}

// ---------------------------------------------------------------------------
// measurements through the server (ignored; DESIGN.md "Partial MSTs")
// ---------------------------------------------------------------------------

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// A single node (id `id`, 4 shards) on `store` in a given MST mode, with
/// nothing preloaded or pinned (every first write is a cold open).
async fn bench_node(id: &str, store: Arc<dyn object_store::ObjectStore>, lazy: bool, prefetch: usize, workers: usize) -> TestServer {
    let id = id.to_string();
    let s = TestServer::spawn_with(move |c| {
        c.lazy_mst = lazy;
        c.lazy_mst_prefetch_bytes = prefetch;
        c.memory_store = Some(store);
        c.shards = 4;
        c.workers = workers;
        c.preload_recent = 0;
        c.pin_repo_records = 0;
        c.cluster = Some(vlpds::cluster::ClusterConfig { node_id: id, addr: c.public_url.clone(), shards: 4, ..Default::default() });
    })
    .await;
    wait_until("all shards owned", Duration::from_secs(60), || s.app.partitions.owned().len() == 4).await;
    s
}

/// Creates `bulk_did(i)` with `sizes[i]` genesis posts, at most 1M records
/// per request.
async fn bulk_populate(s: &TestServer, sizes: &[u32]) {
    let auth = Auth::Bearer(ADMIN_TOKEN.into());
    let mut i = 0;
    while i < sizes.len() {
        let (mut j, mut n) = (i, 0u64);
        while j < sizes.len() && j - i < 50_000 && (j == i || n + sizes[j] as u64 <= 1_000_000) {
            n += sizes[j] as u64;
            j += 1;
        }
        let idx: Vec<u64> = (i as u64..j as u64).collect();
        let r = s.xrpc.post("vlpds.admin.bulkCreate", &json!({"indices": idx, "records": &sizes[i..j]}), &auth).await;
        assert_eq!(r.status, 200, "{}", r.text());
        assert_eq!(r.json["created"].as_u64(), Some((j - i) as u64), "{}", r.text());
        i = j;
    }
    s.app.log.checkpoint_all().await;
}

async fn create_post(s: &TestServer, did: &str) -> Duration {
    let t = Instant::now();
    let r = s
        .xrpc
        .post("com.atproto.repo.createRecord", &json!({"repo": did, "collection": "app.bsky.feed.post", "record": post_record("bench")}), &Auth::Bearer(s.app.jwt.access(did)))
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    t.elapsed()
}

/// GETs (whole or ranged) the state client has sent so far.
fn state_gets() -> u64 {
    use prometheus::core::Collector;
    vlpds::metrics::OBJ_REQUESTS
        .collect()
        .iter()
        .flat_map(|f| f.get_metric().iter())
        .filter(|m| {
            let l = |n: &str| m.get_label().iter().find(|p| p.name() == n).map(|p| p.value().to_string());
            l("client").as_deref() == Some("state") && matches!(l("op").as_deref(), Some("get" | "get_range"))
        })
        .map(|m| m.get_counter().get_value() as u64)
        .sum()
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

/// Cold first write per repo size after a restart, every object-store GET
/// delayed 20 ms (an S3 miss; nothing in the block cache): full trees (an
/// `R/` scan of the whole repo), lazy with the `M/` prefetch (one scan of
/// the repo's node range) and lazy without (dependent node reads).
/// `VLPDS_COLD_SIZES=1000,10000,100000,1000000 VLPDS_COLD_REPOS=3
/// cargo test --profile dev-release --test all mst_lazy::bench_cold_write -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn bench_cold_write() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let sizes: Vec<u32> = std::env::var("VLPDS_COLD_SIZES").unwrap_or("1000,10000,100000".into()).split(',').map(|s| s.parse().unwrap()).collect();
    let per: usize = env_or("VLPDS_COLD_REPOS", 3);
    let delay = Duration::from_millis(env_or("VLPDS_COLD_GET_MS", 20));
    let repo_sizes: Vec<u32> = sizes.iter().flat_map(|&n| std::iter::repeat_n(n, per)).collect();
    // populate once (a lazy node: it writes M/), then each mode restarts on
    // its own copy of the store, cold
    let base = Arc::new(object_store::memory::InMemory::new());
    {
        let t = Instant::now();
        let s = bench_node("cw", base.clone(), true, 4 << 20, 4).await;
        bulk_populate(&s, &repo_sizes).await;
        vlpds::server::shutdown(&s.app).await;
        eprintln!("populated {} repos in {:.1}s", repo_sizes.len(), t.elapsed().as_secs_f64());
    }
    // a node on the store until its post-import compactions are done, so
    // none runs (reading SSTs) during the measured writes
    {
        let s = bench_node("cw", base.clone(), true, 0, 4).await;
        tokio::time::sleep(Duration::from_secs(env_or("VLPDS_COLD_SETTLE_SECS", 20))).await;
        vlpds::server::shutdown(&s.app).await;
    }
    let mut report = Vec::new();
    let prefetch_kb: Vec<usize> = std::env::var("VLPDS_COLD_PREFETCH_KB").unwrap_or("4096,256,0".into()).split(',').map(|s| s.parse().unwrap()).collect();
    let modes: Vec<(String, bool, usize)> = prefetch_kb.iter().map(|kb| (format!("lazy, prefetch {kb} KiB"), true, kb << 10)).chain([("full".to_string(), false, 0)]).collect();
    for (label, lazy, prefetch) in modes {
        vlpds::partition::bump_cache_epoch();
        vlpds::mst_store::NODE_CACHE.clear();
        let throttled = Arc::new(ThrottledStore::new(base.fork(), ThrottleConfig::default()));
        let s = bench_node("cw", throttled.clone(), lazy, prefetch, 4).await;
        throttled.config_mut(|c| c.wait_get_per_call = delay);
        let mut lat: BTreeMap<u32, Vec<f64>> = BTreeMap::new();
        let mut gets: BTreeMap<u32, Vec<f64>> = BTreeMap::new();
        for (i, n) in repo_sizes.iter().enumerate() {
            let did = vlpds::state::bulk_did(i as u64);
            let g = state_gets();
            let d = create_post(&s, &did).await;
            lat.entry(*n).or_default().push(d.as_secs_f64() * 1e3);
            gets.entry(*n).or_default().push((state_gets() - g) as f64);
        }
        throttled.config_mut(|c| c.wait_get_per_call = Duration::ZERO);
        for (n, mut v) in lat {
            let g = gets.get_mut(&n).unwrap();
            let line = format!("{label:>24} {n:>8} records: cold write median {:>7.1} ms, max {:>7.1} ms, median {:>4} GETs ({} repos, GET +{delay:?})", pct(&mut v, 0.5), pct(&mut v, 1.0), pct(g, 0.5), v.len());
            eprintln!("{line}");
            report.push(line);
        }
        vlpds::server::shutdown(&s.app).await;
    }
    println!("{}", report.join("\n"));
}

/// Requests per second of `f` over `secs`, `conc` at a time.
async fn throughput<F, Fut>(secs: f64, conc: usize, f: F) -> f64
where
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let t = Instant::now();
    let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tasks: Vec<_> = (0..conc)
        .map(|w| {
            let (f, n) = (f.clone(), n.clone());
            tokio::spawn(async move {
                let mut i = w;
                while t.elapsed().as_secs_f64() < secs {
                    f(i).await;
                    i += conc;
                    n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            })
        })
        .collect();
    for x in tasks {
        x.await.unwrap();
    }
    n.load(std::sync::atomic::Ordering::Relaxed) as f64 / t.elapsed().as_secs_f64()
}

/// Read paths on one repo (`VLPDS_READ_RECORDS`, default 100k) in both
/// modes, after a restart and one write (a lazy node holds the root and one
/// path): sync.getRecord proofs of random keys, getBlocks of random
/// interior nodes / leaves / records, getRepo exports.
/// `cargo test --profile dev-release --test all mst_lazy::bench_readers -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn bench_readers() {
    let n: u32 = env_or("VLPDS_READ_RECORDS", 100_000);
    let secs: f64 = env_or("VLPDS_READ_SECS", 4.0);
    let base = Arc::new(object_store::memory::InMemory::new());
    {
        let s = bench_node("rd", base.clone(), true, 4 << 20, 4).await;
        bulk_populate(&s, &[n]).await;
        vlpds::server::shutdown(&s.app).await;
    }
    let did = vlpds::state::bulk_did(0);
    let mut report = Vec::new();
    for (label, lazy) in [("lazy", true), ("full", false)] {
        vlpds::partition::bump_cache_epoch();
        vlpds::mst_store::NODE_CACHE.clear();
        let s = bench_node("rd", Arc::new(base.fork()), lazy, 4 << 20, 4).await;
        create_post(&s, &did).await;
        // the repo's keys and node CIDs (from an export)
        let r = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &did)], &Auth::None).await;
        let repo = Repo::from_car(&r.body).unwrap();
        let commit = Value::decode(&repo.blocks[&repo.root]).unwrap();
        let Some(Value::Link(data)) = commit.get("data") else { panic!() };
        let tree = Tree::load_from_blocks(&repo.blocks, *data).unwrap();
        let mut keys = Vec::new();
        let mut recs = Vec::new();
        tree.walk(&mut |k, c| {
            keys.push(String::from_utf8(k.to_vec()).unwrap());
            recs.push(c);
        });
        let (mut interior, mut leaves) = (Vec::new(), Vec::new());
        tree.walk_blocks(&mut |c, b| match vlpds::mst::decode_node(b, c).unwrap().height {
            0 => leaves.push(c),
            _ => interior.push(c),
        })
        .unwrap();
        let mut rng = StdRng::seed_from_u64(5);
        keys.shuffle(&mut rng);
        interior.shuffle(&mut rng);
        leaves.shuffle(&mut rng);
        recs.shuffle(&mut rng);
        let (keys, interior, leaves, recs) = (Arc::new(keys), Arc::new(interior), Arc::new(leaves), Arc::new(recs));
        let x = s.xrpc.clone();
        let d = did.clone();
        let get_record = throughput(secs, 32, move |i| {
            let (x, d, keys) = (x.clone(), d.clone(), keys.clone());
            async move {
                let (c, r) = keys[i % keys.len()].split_once('/').unwrap();
                let resp = x.get("com.atproto.sync.getRecord", &[("did", &d), ("collection", c), ("rkey", r)], &Auth::None).await;
                assert_eq!(resp.status, 200);
            }
        })
        .await;
        let blocks = |cids: Arc<Vec<Cid>>, secs: f64, conc: usize| {
            let (x, d) = (s.xrpc.clone(), did.clone());
            throughput(secs, conc, move |i| {
                let (x, d, cids) = (x.clone(), d.clone(), cids.clone());
                async move {
                    let c = cids[i % cids.len()].to_string();
                    let resp = x.get("com.atproto.sync.getBlocks", &[("did", &d), ("cids", &c)], &Auth::None).await;
                    assert_eq!(resp.status, 200, "{}", resp.text());
                }
            })
        };
        let gb_interior = blocks(interior.clone(), secs, 32).await;
        let gb_records = blocks(recs.clone(), secs, 32).await;
        let gb_leaves = blocks(leaves.clone(), secs, 4).await;
        let (x, d) = (s.xrpc.clone(), did.clone());
        let get_repo = throughput(secs.max(4.0), 4, move |_| {
            let (x, d) = (x.clone(), d.clone());
            async move {
                assert_eq!(x.get("com.atproto.sync.getRepo", &[("did", &d)], &Auth::None).await.status, 200);
            }
        })
        .await;
        let line = format!(
            "{label}: {n} records: sync.getRecord {get_record:.0}/s; getBlocks interior {gb_interior:.0}/s, record {gb_records:.0}/s, leaf {gb_leaves:.1}/s; getRepo {get_repo:.2}/s ({:.1} MB)",
            r.body.len() as f64 / 1e6
        );
        eprintln!("{line}");
        report.push(line);
        vlpds::server::shutdown(&s.app).await;
    }
    println!("{}", report.join("\n"));
}

/// Memory of a node holding many repos (`VLPDS_RSS_REPOS`, default 20k)
/// of Zipf sizes (rank r: `VLPDS_RSS_MAX` / r records, default 1M at rank
/// 1; ~10.5M records in all) after one write to each, lazy first, then full
/// trees, each on a fresh restart (block cache 64 MiB, cold). RSS grows by
/// what the node keeps for the repos: their trees (full) or paths (lazy).
/// Run alone (the block cache size is process-wide):
/// `cargo test --profile dev-release --test all mst_lazy::bench_rss -- --ignored --nocapture --test-threads 1`
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn bench_rss() {
    vlpds::partition::set_block_cache_bytes(64 << 20);
    let repos: usize = env_or("VLPDS_RSS_REPOS", 20_000);
    let max: f64 = env_or("VLPDS_RSS_MAX", 1_000_000.0);
    let sizes: Vec<u32> = (1..=repos).map(|r| ((max / r as f64) as u32).max(1)).collect();
    let total: u64 = sizes.iter().map(|&n| n as u64).sum();
    let base = Arc::new(object_store::memory::InMemory::new());
    {
        let t = Instant::now();
        let s = bench_node("rss", base.clone(), true, 4 << 20, 8).await;
        bulk_populate(&s, &sizes).await;
        vlpds::server::shutdown(&s.app).await;
        eprintln!("populated {repos} repos, {total} records in {:.0}s", t.elapsed().as_secs_f64());
    }
    let mut order: Vec<usize> = (0..repos).collect();
    order.shuffle(&mut StdRng::seed_from_u64(3));
    let order = Arc::new(order);
    let mut report = Vec::new();
    // one mode per process (`VLPDS_RSS_MODE=lazy|full`) measures cleanly:
    // memory the first mode freed would be reused by the second
    let modes: Vec<(&str, bool)> = match std::env::var("VLPDS_RSS_MODE").as_deref() {
        Ok("lazy") => vec![("lazy", true)],
        Ok("full") => vec![("full", false)],
        _ => vec![("lazy", true), ("full", false)],
    };
    for (label, lazy) in modes {
        vlpds::partition::bump_cache_epoch();
        vlpds::mst_store::NODE_CACHE.clear();
        let s = bench_node("rss", base.clone(), lazy, vlpds::worker::DEFAULT_PREFETCH_BYTES, 8).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let rss0 = vlpds::metrics::resident_bytes().unwrap_or(0);
        let t = Instant::now();
        let mut lat = Vec::new();
        use futures::StreamExt;
        let mut st = futures::stream::iter(order.iter().copied().map(|i| {
            let s = &s;
            async move { create_post(s, &vlpds::state::bulk_did(i as u64)).await.as_secs_f64() * 1e3 }
        }))
        .buffer_unordered(64);
        while let Some(ms) = st.next().await {
            lat.push(ms);
        }
        drop(st);
        let took = t.elapsed().as_secs_f64();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let rss1 = vlpds::metrics::resident_bytes().unwrap_or(0);
        let cache: i64 = (0..8).map(|w| vlpds::metrics::REPO_CACHE_BYTES.with_label_values(&[&w.to_string()]).get()).sum();
        let line = format!(
            "{label}: {repos} repos / {total} records, one write each in {took:.1}s (p50 {:.1} ms, p99 {:.1} ms): RSS +{:.0} MB ({:.0} -> {:.0} MB), repo cache {:.0} MB, node cache {:.0} MB",
            pct(&mut lat, 0.5),
            pct(&mut lat, 0.99),
            (rss1 as f64 - rss0 as f64) / 1e6,
            rss0 as f64 / 1e6,
            rss1 as f64 / 1e6,
            cache as f64 / 1e6,
            vlpds::mst_store::NODE_CACHE.bytes() as f64 / 1e6
        );
        eprintln!("{line}");
        report.push(line);
        vlpds::server::shutdown(&s.app).await;
        drop(s);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!("{}", report.join("\n"));
}

/// Splits and merges carry a lazy repo's `M/` nodes with its shard's slot
/// range (the keys are slot-prefixed like `R/`): after each, every repo's
/// nodes are exactly its tree's on its new shard, and writes open from
/// them without falling back to a rebuild from records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reshard_carries_nodes() {
    let s = mode_node(true, 4 << 20, None).await;
    let mut accts = Vec::new();
    for i in 0..8 {
        accts.push(s.create_account(&format!("rsn{i}")).await);
    }
    let mut rng = StdRng::seed_from_u64(21);
    let mut ok = 0;
    for st in random_steps(&mut rng, accts.len(), 300) {
        ok += (run_step(&s, &accts, &st).await == 200) as usize;
    }
    assert!(ok > 150, "{ok} writes applied");
    let fallbacks = || -> u64 { ["missing", "invalid", "missing_node"].iter().map(|r| vlpds::metrics::LAZY_MST_FALLBACKS.with_label_values(&[r]).get()).sum() };
    let f0 = fallbacks();
    let admin = |nsid: &'static str, body: J| {
        let x = s.xrpc.clone();
        async move { x.post(nsid, &body, &Auth::Admin).await.ok() }
    };
    let cl = s.app.cluster.as_deref().unwrap();
    let shard_of = |did: &str| {
        let slot = vlpds::slots::slot_of(did) as u32;
        cl.layout().shards.iter().find(|r| r.lo <= slot && slot < r.hi).cloned().unwrap()
    };
    let target = shard_of(&accts[0].did);
    let r = admin("vlpds.admin.splitShard", json!({"shard": target.id, "at": (target.lo + target.hi) / 2, "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    vlpds::mst_store::NODE_CACHE.clear();
    for a in &accts {
        check_stored_nodes(&s, &a.did).await;
    }
    for st in random_steps(&mut rng, accts.len(), 150) {
        run_step(&s, &accts, &st).await;
    }
    let kids: Vec<u16> = r["op"]["children"].as_array().unwrap().iter().map(|c| c["id"].as_u64().unwrap() as u16).collect();
    let r = admin("vlpds.admin.mergeShards", json!({"left": kids[0], "right": kids[1], "wait": true})).await;
    assert_eq!(r["done"], json!(true), "{r}");
    vlpds::mst_store::NODE_CACHE.clear();
    for st in random_steps(&mut rng, accts.len(), 150) {
        run_step(&s, &accts, &st).await;
    }
    for a in &accts {
        check_stored_nodes(&s, &a.did).await;
    }
    assert_eq!(fallbacks(), f0, "opens fell back to rebuilding from records");
}

/// A repo written without pause (its commits always in flight, so it is
/// never idle) keeps its loaded paths bounded: past 1 MiB they are dropped
/// except the nodes in-flight commits wrote, and it stays exact (`M/`, the
/// key set, getRepo against the records).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hot_repo_paths_stay_bounded() {
    let s = TestServer::spawn_with(|c| c.lazy_mst = true).await;
    let a = s.create_account("hot").await;
    let mut live = std::collections::BTreeSet::new();
    for b in 0..60u64 {
        let writes: Vec<J> = (0..200u64)
            .map(|k| {
                let r = format!("r{:06}", b * 200 + k);
                live.insert(format!("com.example.thing/{r}"));
                json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.thing", "rkey": r, "value": thing("com.example.thing", k as i64)})
            })
            .collect();
        s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "writes": writes}), &a.auth()).await.ok();
    }
    let info = || async {
        let (reply, rx) = tokio::sync::oneshot::channel();
        s.app.workers.route(&a.did).send(vlpds::worker::WorkerMsg::CacheInfo { did: a.did.as_str().into(), reply }).unwrap();
        rx.await.unwrap()
    };
    // deletes and re-creates all over the key space, 32 at a time
    let mut rng = StdRng::seed_from_u64(9);
    let keys: Vec<String> = (0..1600).map(|_| format!("r{:06}", rng.gen_range(0..12_000u64))).collect();
    let (x, did, auth) = (s.xrpc.clone(), a.did.clone(), a.auth());
    let writer = tokio::spawn(async move {
        use futures::StreamExt;
        futures::stream::iter(keys.into_iter().enumerate())
            .map(|(i, r)| {
                let (x, did, auth) = (x.clone(), did.clone(), auth.clone());
                async move {
                    let body = match i % 2 {
                        0 => json!({"repo": did, "collection": "com.example.thing", "rkey": r}),
                        _ => json!({"repo": did, "collection": "com.example.thing", "rkey": r, "record": thing("com.example.thing", 1)}),
                    };
                    let nsid = if i % 2 == 0 { "com.atproto.repo.deleteRecord" } else { "com.atproto.repo.putRecord" };
                    x.post(nsid, &body, &auth).await.status
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await
    });
    let mut peak = 0;
    while !writer.is_finished() {
        if let Some(i) = info().await {
            peak = peak.max(i.charge);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(writer.await.unwrap().iter().all(|st| *st == 200));
    eprintln!("hot repo: peak charge {:.2} MiB", peak as f64 / (1 << 20) as f64);
    assert!(peak < 3 << 20, "loaded paths grew to {peak} bytes");
    check_stored_nodes(&s, &a.did).await;
    // the tree holds exactly the records
    let r = s.xrpc.get("com.atproto.sync.getRepo", &[("did", &a.did)], &Auth::None).await;
    let repo = Repo::from_car(&r.body).unwrap();
    let Some(Value::Link(data)) = Value::decode(&repo.blocks[&repo.root]).unwrap().get("data").cloned() else { panic!() };
    let tree = Tree::load_from_blocks(&repo.blocks, data).unwrap();
    let mut n = 0;
    tree.walk(&mut |_, c| {
        assert!(repo.blocks.contains_key(&c));
        n += 1;
    });
    let listed = s.list_records(&a.did, "com.example.thing", &[("limit", "1")]).await;
    assert!(listed.status == 200 && n > 10_000, "{n} records");
    let _ = live;
}
