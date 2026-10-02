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
