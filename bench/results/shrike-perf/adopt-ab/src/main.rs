//! Interleaved before/after (+ shrike reference) micro-benchmarks for the
//! "adopt shrike's techniques" lane. Sides: shrike 0.7.0, vlpds_base (a
//! frozen copy of vlpds at f6a9e33 + working tree), vlpds (live tree).
//!
//! usage: ab [--rounds N] [--ms T] [filter...]

mod data;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use shrike::cbor as sc;
use std::collections::HashMap;
use std::hint::black_box;
use std::str::FromStr;
use std::time::{Duration, Instant};

struct Opts {
    rounds: usize,
    target: Duration,
    filter: Vec<String>,
}

impl Opts {
    fn wants(&self, name: &str) -> bool {
        self.filter.is_empty() || self.filter.iter().any(|f| name.contains(f.as_str()))
    }
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 { s[n / 2] } else { (s[n / 2 - 1] + s[n / 2]) / 2.0 }
}

fn fmt_ns(ns: f64) -> String {
    if ns >= 1e6 {
        format!("{:.2} ms", ns / 1e6)
    } else if ns >= 1e3 {
        format!("{:.2} µs", ns / 1e3)
    } else {
        format!("{:.1} ns", ns)
    }
}

fn calibrate(f: &mut dyn FnMut(), target: Duration) -> u64 {
    f();
    let mut n: u64 = 1;
    loop {
        let t = Instant::now();
        for _ in 0..n {
            f();
        }
        let e = t.elapsed();
        if e >= target / 8 || n >= 1 << 32 {
            let per = e.as_secs_f64() / n as f64;
            return ((target.as_secs_f64() / per.max(1e-12)).ceil() as u64).max(1);
        }
        n *= 2;
    }
}

type Side<'a> = (&'a str, &'a mut dyn FnMut());

/// N-way interleaved comparison: each round runs one batch per side, the
/// order rotating between rounds. Prints medians; ratios are per round
/// against the side labelled "base" (if any), median reported.
fn cmp(o: &Opts, name: &str, per: f64, sides: &mut [Side]) {
    if !o.wants(name) {
        return;
    }
    let iters: Vec<u64> = sides.iter_mut().map(|(_, f)| calibrate(*f, o.target)).collect();
    let k = sides.len();
    let mut res = vec![Vec::new(); k];
    for r in 0..o.rounds {
        for j in 0..k {
            let i = (j + r) % k;
            let f = &mut sides[i].1;
            let t = Instant::now();
            for _ in 0..iters[i] {
                f();
            }
            res[i].push(t.elapsed().as_nanos() as f64 / iters[i] as f64 / per);
        }
    }
    report(name, sides.iter().map(|s| s.0).collect(), res);
}

fn report(name: &str, labels: Vec<&str>, res: Vec<Vec<f64>>) {
    let base = labels.iter().position(|l| *l == "base");
    let mut line = format!("{name:<58}");
    for (i, l) in labels.iter().enumerate() {
        line += &format!("  {l}={:>10}", fmt_ns(median(&res[i])));
        if let Some(b) = base {
            if b != i {
                let ratios: Vec<f64> = res[b].iter().zip(&res[i]).map(|(x, y)| x / y).collect();
                line += &format!(" ({:.2}x)", median(&ratios));
            }
        }
    }
    println!("{line}");
}

/// For slow stateful runs: each closure returns (time, units).
fn oneshot(o: &Opts, name: &str, rounds: usize, sides: &mut [(&str, &mut dyn FnMut() -> (Duration, f64))]) {
    if !o.wants(name) {
        return;
    }
    for (_, f) in sides.iter_mut() {
        black_box(f());
    }
    let k = sides.len();
    let mut res = vec![Vec::new(); k];
    for r in 0..rounds {
        for j in 0..k {
            let i = (j + r) % k;
            let (d, n) = (sides[i].1)();
            res[i].push(d.as_nanos() as f64 / n);
        }
    }
    report(name, sides.iter().map(|s| s.0).collect(), res);
}

fn n_cid(c: &sc::Cid) -> vlpds::cid::Cid {
    vlpds::cid::Cid::from_bytes(&c.to_bytes()).unwrap()
}
fn b_cid(c: &sc::Cid) -> vlpds_base::cid::Cid {
    vlpds_base::cid::Cid::from_bytes(&c.to_bytes()).unwrap()
}

const KINDS: &[(&str, &str)] = &[
    ("post", "app.bsky.feed.post"),
    ("like", "app.bsky.feed.like"),
    ("follow", "app.bsky.graph.follow"),
    ("repost", "app.bsky.feed.repost"),
];

fn main() {
    let mut args = std::env::args().skip(1);
    let mut o = Opts { rounds: 11, target: Duration::from_millis(60), filter: Vec::new() };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--rounds" => o.rounds = args.next().unwrap().parse().unwrap(),
            "--ms" => o.target = Duration::from_millis(args.next().unwrap().parse().unwrap()),
            f => o.filter.push(f.to_string()),
        }
    }
    let repo = data::load_repo();
    println!("# repo: {} blocks, {} records; rounds={} batch={:?}", repo.blocks.len(), repo.entries.len(), o.rounds, o.target);

    // ---------- DAG-CBOR decode ----------
    let mut sets: Vec<(String, Vec<&[u8]>)> = Vec::new();
    for (short, coll) in KINDS {
        let recs = data::records(&repo, coll, 5000);
        sets.push((format!("{short} x{}", recs.len()), recs.into_iter().map(|r| r.1).collect()));
    }
    let fixtures = data::interop_fixtures();
    sets.push((format!("interop x{}", fixtures.len()), fixtures.iter().map(|f| f.1.as_slice()).collect()));
    for (name, recs) in &sets {
        // parity on the inputs
        for b in recs {
            let x = vlpds::cbor::Value::decode(b).unwrap();
            assert_eq!(x.to_cbor(), *b);
            assert_eq!(vlpds_base::cbor::Value::decode(b).unwrap().to_cbor(), *b);
        }
        let n = recs.len() as f64;
        cmp(&o, &format!("cbor.decode/{name}"), n, &mut [
            ("shrike", &mut || for b in recs { black_box(sc::decode(black_box(b)).unwrap()); }),
            ("base", &mut || for b in recs { black_box(vlpds_base::cbor::Value::decode(black_box(b)).unwrap()); }),
            ("new", &mut || for b in recs { black_box(vlpds::cbor::Value::decode(black_box(b)).unwrap()); }),
            ("new-ref", &mut || for b in recs { black_box(vlpds::cbor::ValueRef::decode(black_box(b)).unwrap()); }),
        ]);
    }
    // getRecord's transcoder
    for (name, recs) in &sets {
        for b in recs {
            let (mut x, mut y) = (Vec::new(), Vec::new());
            vlpds::cbor::write_json(b, &mut x).unwrap();
            vlpds_base::cbor::write_json(b, &mut y).unwrap();
            assert_eq!(x, y);
        }
        let n = recs.len() as f64;
        let mut out = Vec::with_capacity(1 << 16);
        let mut out2 = Vec::with_capacity(1 << 16);
        cmp(&o, &format!("cbor->json/{name}"), n, &mut [
            ("base", &mut || for b in recs { out.clear(); vlpds_base::cbor::write_json(black_box(b), &mut out).unwrap(); black_box(&out); }),
            ("new", &mut || for b in recs { out2.clear(); vlpds::cbor::write_json(black_box(b), &mut out2).unwrap(); black_box(&out2); }),
        ]);
    }

    // ---------- lexicon ----------
    if o.wants("lexicon") {
        let bundle = "/path/to/vlpds/lexicons/bundle.json";
        let docs: HashMap<String, serde_json::Value> = serde_json::from_slice(&std::fs::read(bundle).unwrap()).unwrap();
        let mut catalog = shrike::lexicon::Catalog::new();
        for d in docs.values() {
            let _ = catalog.add_schema(&serde_json::to_vec(d).unwrap());
        }
        for (short, coll) in KINDS {
            let recs = data::records(&repo, coll, 5000);
            let vals: Vec<(&str, serde_json::Value)> = recs.iter().map(|(r, b)| (*r, shrike::cbor::json::drisl_to_json(b).unwrap())).collect();
            let n = vals.len() as f64;
            cmp(&o, &format!("lexicon/{short} x{}", vals.len()), n, &mut [
                ("shrike", &mut || for (_, j) in &vals { black_box(shrike::lexicon::validate_record(&catalog, coll, black_box(j)).unwrap()); }),
                ("base", &mut || for (r, j) in &vals { black_box(vlpds_base::lexicon::validate_record(coll, r, black_box(j), None, None).unwrap()); }),
                ("new", &mut || for (r, j) in &vals { black_box(vlpds::lexicon::validate_record(coll, r, black_box(j), None, None).unwrap()); }),
            ]);
        }
        let tids: Vec<String> = data::records(&repo, "app.bsky.feed.like", 5000).iter().map(|r| r.0.to_string()).collect();
        cmp(&o, "lexicon/valid_tid x5000 (rkey check)", tids.len() as f64, &mut [
            ("base", &mut || for t in &tids { black_box(vlpds_base::xrpc::syntax::valid_tid(black_box(t))); }),
            ("new", &mut || for t in &tids { black_box(vlpds::xrpc::syntax::valid_tid(black_box(t))); }),
        ]);
    }

    // ---------- CIDs ----------
    let cids: Vec<sc::Cid> = repo.entries.iter().take(5000).map(|e| e.1).collect();
    let strs: Vec<String> = cids.iter().map(|c| c.to_string()).collect();
    let ncids: Vec<_> = cids.iter().map(n_cid).collect();
    let bcids: Vec<_> = cids.iter().map(b_cid).collect();
    for (i, s) in strs.iter().enumerate() {
        assert_eq!(ncids[i].to_string(), *s);
        assert_eq!(vlpds::cid::Cid::parse(s).unwrap(), ncids[i]);
    }
    let n = cids.len() as f64;
    cmp(&o, "cid/to_string", n, &mut [
        ("shrike", &mut || for c in &cids { black_box(black_box(c).to_string()); }),
        ("base", &mut || for c in &bcids { black_box(black_box(c).to_string()); }),
        ("new", &mut || for c in &ncids { black_box(black_box(c).to_string()); }),
    ]);
    let mut wb = Vec::with_capacity(64);
    let mut wn = Vec::with_capacity(64);
    cmp(&o, "cid/write_string (into Vec)", n, &mut [
        ("base", &mut || for c in &bcids { wb.clear(); black_box(c).write_string(&mut wb); black_box(&wb); }),
        ("new", &mut || for c in &ncids { wn.clear(); black_box(c).write_string(&mut wn); black_box(&wn); }),
    ]);
    cmp(&o, "cid/parse", n, &mut [
        ("shrike", &mut || for s in &strs { black_box(sc::Cid::from_str(black_box(s)).unwrap()); }),
        ("base", &mut || for s in &strs { black_box(vlpds_base::cid::Cid::parse(black_box(s)).unwrap()); }),
        ("new", &mut || for s in &strs { black_box(vlpds::cid::Cid::parse(black_box(s)).unwrap()); }),
    ]);

    // ---------- MST node codec ----------
    let mut nodes: Vec<(sc::Cid, &[u8])> = Vec::new();
    let mut stack = vec![repo.data_root];
    while let Some(c) = stack.pop() {
        let b = &repo.blocks[&c];
        let nd = shrike::mst::node::decode_node_data(b).unwrap();
        stack.extend(nd.left);
        stack.extend(nd.entries.iter().filter_map(|e| e.right));
        nodes.push((c, b));
    }
    let n = nodes.len() as f64;
    let nn: Vec<_> = nodes.iter().map(|(c, b)| (n_cid(c), *b)).collect();
    let bn: Vec<_> = nodes.iter().map(|(c, b)| (b_cid(c), *b)).collect();
    cmp(&o, &format!("mst.node/decode x{}", nodes.len()), n, &mut [
        ("shrike", &mut || for (_, b) in &nodes { black_box(shrike::mst::node::decode_node_data(black_box(b)).unwrap()); }),
        ("base", &mut || for (c, b) in &bn { black_box(vlpds_base::mst::decode_node(black_box(b), *c).unwrap()); }),
        ("new", &mut || for (c, b) in &nn { black_box(vlpds::mst::decode_node(black_box(b), *c).unwrap()); }),
    ]);
    let snodes: Vec<_> = nodes.iter().map(|(_, b)| shrike::mst::node::decode_node_data(b).unwrap()).collect();
    let vnodes: Vec<_> = nn.iter().map(|(c, b)| vlpds::mst::decode_node(b, *c).unwrap()).collect();
    let bnodes: Vec<_> = bn.iter().map(|(c, b)| vlpds_base::mst::decode_node(b, *c).unwrap()).collect();
    for (i, nd) in vnodes.iter().enumerate() {
        let mut x = Vec::new();
        vlpds::mst::encode_node(nd, &mut x).unwrap();
        assert_eq!(x, nodes[i].1);
    }
    let (mut b1, mut b2) = (Vec::with_capacity(8192), Vec::with_capacity(8192));
    cmp(&o, &format!("mst.node/encode x{}", nodes.len()), n, &mut [
        ("shrike", &mut || for nd in &snodes { black_box(shrike::mst::node::encode_node_data(black_box(nd)).unwrap()); }),
        ("base", &mut || for nd in &bnodes { b1.clear(); vlpds_base::mst::encode_node(black_box(nd), &mut b1).unwrap(); black_box(&b1); }),
        ("new", &mut || for nd in &vnodes { b2.clear(); vlpds::mst::encode_node(black_box(nd), &mut b2).unwrap(); black_box(&b2); }),
    ]);

    // ---------- MST bulk build (shuffled, as the harness) ----------
    let mut rng = StdRng::seed_from_u64(3);
    let mut order: Vec<(String, sc::Cid)> = repo.entries.clone();
    order.shuffle(&mut rng);
    let n = order.len() as f64;
    let ord_n: Vec<(String, vlpds::cid::Cid)> = order.iter().map(|(k, c)| (k.clone(), n_cid(c))).collect();
    let ord_b: Vec<(String, vlpds_base::cid::Cid)> = order.iter().map(|(k, c)| (k.clone(), b_cid(c))).collect();
    oneshot(&o, "mst.build/43k shuffled: inserts", 7, &mut [
        ("shrike", &mut || {
            let ks: Vec<(String, sc::Cid)> = order.clone();
            let mut t = shrike::mst::DetachedTree::new();
            let t0 = Instant::now();
            for (k, v) in ks { t.insert(&shrike::mst::NoBlocks, k, v).unwrap(); }
            let d = t0.elapsed();
            black_box(t);
            (d, n)
        }),
        ("base", &mut || {
            let mut t = vlpds_base::mst::Tree::new();
            let t0 = Instant::now();
            for (k, v) in &ord_b { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            let d = t0.elapsed();
            black_box(t);
            (d, n)
        }),
        ("new", &mut || {
            let mut t = vlpds::mst::Tree::new();
            let t0 = Instant::now();
            for (k, v) in &ord_n { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            let d = t0.elapsed();
            black_box(t);
            (d, n)
        }),
    ]);
    let want = n_cid(&repo.data_root);
    // cold load: keys arrive sorted from the R/ scan; inserts + root
    let mut sorted_n = ord_n.clone();
    sorted_n.sort();
    let mut sorted_b = ord_b.clone();
    sorted_b.sort_by(|a, b| a.0.cmp(&b.0));
    {
        let mut t = vlpds::mst::Tree::new();
        for (k, v) in &sorted_n { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
        assert_eq!(t.root_cid().unwrap(), want);
    }
    oneshot(&o, "mst.coldload/43k sorted: inserts + root_cid", 9, &mut [
        ("base", &mut || {
            let t0 = Instant::now();
            let mut t = vlpds_base::mst::Tree::new();
            for (k, v) in &sorted_b { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            black_box(t.root_cid().unwrap());
            let d = t0.elapsed();
            black_box(t);
            (d, n)
        }),
        ("new", &mut || {
            let t0 = Instant::now();
            let mut t = vlpds::mst::Tree::new();
            for (k, v) in &sorted_n { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            black_box(t.root_cid().unwrap());
            let d = t0.elapsed();
            black_box(t);
            (d, n)
        }),
    ]);
    // a 1k-record repo (the first 1000 keys)
    let small_n: Vec<_> = sorted_n.iter().take(1000).cloned().collect();
    let small_b: Vec<_> = sorted_b.iter().take(1000).cloned().collect();
    cmp(&o, "mst.coldload/1k sorted: inserts + root_cid (per record)", 1000.0, &mut [
        ("base", &mut || {
            let mut t = vlpds_base::mst::Tree::new();
            for (k, v) in &small_b { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            black_box(t.root_cid().unwrap());
        }),
        ("new", &mut || {
            let mut t = vlpds::mst::Tree::new();
            for (k, v) in &small_n { t.insert_no_proof(k.as_bytes(), *v).unwrap(); }
            black_box(t.root_cid().unwrap());
        }),
    ]);

    // ---------- live-tree commits (insert with proof + diff) ----------
    let mut tn = vlpds::mst::Tree::new();
    for (k, v) in &sorted_n { tn.insert_no_proof(k.as_bytes(), *v).unwrap(); }
    tn.root_cid().unwrap();
    let mut tb = vlpds_base::mst::Tree::new();
    for (k, v) in &sorted_b { tb.insert_no_proof(k.as_bytes(), *v).unwrap(); }
    tb.root_cid().unwrap();
    let fresh: Vec<String> = (0..2000).map(|i| format!("app.bsky.feed.like/3zzz{i:09}")).collect();
    let vn = vlpds::cid::Cid::dag_cbor(b"x");
    let vb = vlpds_base::cid::Cid::dag_cbor(b"x");
    let (mut i1, mut i2) = (0usize, 0usize);
    let (mut o1, mut o2) = (Vec::new(), Vec::new());
    cmp(&o, "mst.commit/43k: insert(proof) + diff + remove(proof) + diff", 1.0, &mut [
        ("base", &mut || {
            let k = fresh[i1 % fresh.len()].as_bytes();
            i1 += 1;
            tb.insert(k, vb).unwrap();
            o1.clear();
            black_box(tb.write_diff_blocks(&mut o1).unwrap());
            tb.remove(k).unwrap();
            o1.clear();
            black_box(tb.write_diff_blocks(&mut o1).unwrap());
        }),
        ("new", &mut || {
            let k = fresh[i2 % fresh.len()].as_bytes();
            i2 += 1;
            tn.insert(k, vn).unwrap();
            o2.clear();
            black_box(tn.write_diff_blocks(&mut o2).unwrap());
            tn.remove(k).unwrap();
            o2.clear();
            black_box(tn.write_diff_blocks(&mut o2).unwrap());
        }),
    ]);
    assert_eq!(tn.root_cid().unwrap(), want);

    // ---------- proofs / getBlocks ----------
    let mut rng = StdRng::seed_from_u64(42);
    let sample: Vec<String> = repo.entries.choose_multiple(&mut rng, 1000).map(|(k, _)| k.clone()).collect();
    for k in &sample {
        let a: Vec<_> = tn.proof_blocks(k.as_bytes()).unwrap().into_iter().map(|(c, b)| (c.to_bytes(), b)).collect();
        let b: Vec<_> = tb.proof_blocks(k.as_bytes()).unwrap().into_iter().map(|(c, b)| (c.to_bytes(), b)).collect();
        assert_eq!(a, b);
    }
    let n = sample.len() as f64;
    cmp(&o, "proof/proof_blocks x1000 (sync.getRecord path)", n, &mut [
        ("base", &mut || for k in &sample { black_box(tb.proof_blocks(k.as_bytes()).unwrap()); }),
        ("new", &mut || for k in &sample { black_box(tn.proof_blocks(k.as_bytes()).unwrap()); }),
    ]);
    let ixn = vlpds::mst::NodeIndex::build(&tn, 1).unwrap();
    let ixb = vlpds_base::mst::NodeIndex::build(&tb, 1).unwrap();
    let node_cids: Vec<sc::Cid> = nodes.iter().map(|x| x.0).collect();
    let pick: Vec<sc::Cid> = node_cids.choose_multiple(&mut rng, 1000).cloned().collect();
    let pn: Vec<_> = pick.iter().map(n_cid).collect();
    let pb: Vec<_> = pick.iter().map(b_cid).collect();
    for (a, b) in pn.iter().zip(&pb) {
        assert_eq!(tn.find_node(a, &ixn).unwrap(), tb.find_node(b, &ixb).unwrap());
    }
    cmp(&o, "getBlocks/find_node x1000 random MST nodes", n, &mut [
        ("base", &mut || for c in &pb { black_box(tb.find_node(c, &ixb).unwrap()); }),
        ("new", &mut || for c in &pn { black_box(tn.find_node(c, &ixn).unwrap()); }),
    ]);
    // a full tree walk of blocks (getRepo / export)
    cmp(&o, "getRepo/walk_blocks 43k", repo.entries.len() as f64, &mut [
        ("base", &mut || { let mut s = 0usize; tb.walk_blocks(&mut |_, b| s += b.len()).unwrap(); black_box(s); }),
        ("new", &mut || { let mut s = 0usize; tn.walk_blocks(&mut |_, b| s += b.len()).unwrap(); black_box(s); }),
    ]);
    // load a whole repo from its blocks (importRepo / proof verify path)
    let blocks_n: HashMap<vlpds::cid::Cid, Vec<u8>> = repo.blocks.iter().map(|(c, b)| (n_cid(c), b.clone())).collect();
    let blocks_b: HashMap<vlpds_base::cid::Cid, Vec<u8>> = repo.blocks.iter().map(|(c, b)| (b_cid(c), b.clone())).collect();
    let dn = n_cid(&repo.data_root);
    let db = b_cid(&repo.data_root);
    let n = repo.entries.len() as f64;
    oneshot(&o, "mst.load_from_blocks 43k (importRepo), per record", 7, &mut [
        ("base", &mut || { let t0 = Instant::now(); black_box(vlpds_base::mst::Tree::load_from_blocks(&blocks_b, db).unwrap()); (t0.elapsed(), n) }),
        ("new", &mut || { let t0 = Instant::now(); black_box(vlpds::mst::Tree::load_from_blocks(&blocks_n, dn).unwrap()); (t0.elapsed(), n) }),
    ]);

    // ---------- CAR read ----------
    cmp(&o, "car/read_car 16.6 MB", repo.car.len() as f64 / 1e6, &mut [
        ("base", &mut || { black_box(vlpds_base::car::read_car(black_box(&repo.car)).unwrap()); }),
        ("new", &mut || { black_box(vlpds::car::read_car(black_box(&repo.car)).unwrap()); }),
    ]);

    // ---------- replay: derive a #commit's muts from its frame ----------
    let frames = data::firehose_frames();
    let frames: Vec<Vec<u8>> = frames.into_iter().filter(|f| vlpds_base::segment::derive_commit_muts(f).is_ok()).collect();
    if !frames.is_empty() {
        for f in &frames {
            let a = vlpds::segment::derive_commit_muts(f).unwrap();
            let b = vlpds_base::segment::derive_commit_muts(f).unwrap();
            assert_eq!(a.len(), b.len());
            for (x, y) in a.iter().zip(&b) {
                assert_eq!((&x.key, &x.val), (&y.key, &y.val));
            }
        }
        let n = frames.len() as f64;
        cmp(&o, &format!("replay/derive_commit_muts x{} frames", frames.len()), n, &mut [
            ("base", &mut || for f in &frames { black_box(vlpds_base::segment::derive_commit_muts(black_box(f)).unwrap()); }),
            ("new", &mut || for f in &frames { black_box(vlpds::segment::derive_commit_muts(black_box(f)).unwrap()); }),
        ]);
    } else {
        println!("# no derivable firehose frames");
    }
    let _ = rng.r#gen::<u8>();
}
