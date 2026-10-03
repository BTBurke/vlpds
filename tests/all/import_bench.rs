//! Memory and time of one big importRepo, by block order. One mode per
//! process (resident memory never shrinks back):
//!
//! ```text
//! IMPORT_BENCH_DIR=<dir> IMPORT_BENCH_N=1000000 IMPORT_BENCH_MODE=gen|stream|cid \
//!   cargo test --profile dev-release --features bench-jemalloc --test all \
//!   import_bench -- --ignored --nocapture
//! ```
//!
//! `gen` writes `<dir>/<n>-{stream,cid}.car`; the others import one.
use crate::common::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::cbor::key_cmp;

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} unset"))
}

fn gen(dir: &str, n: usize) {
    let mut tree = vlpds::mst::Tree::new();
    let mut blocks: Vec<(Cid, Vec<u8>)> = Vec::with_capacity(n + n / 3);
    for i in 0..n {
        let (coll, mut m) = if i % 2 == 0 {
            ("app.bsky.feed.post", vec![
                ("text".to_string(), Value::Text(format!("post number {i}: some ordinary text of a typical length, with a few words more {i}"))),
                ("langs".to_string(), Value::Array(vec![Value::Text("en".into())])),
            ])
        } else {
            ("app.bsky.feed.like", vec![(
                "subject".to_string(),
                Value::Map(vec![
                    ("cid".to_string(), Value::Text("bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".into())),
                    ("uri".to_string(), Value::Text(format!("at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/3l{i:011}"))),
                ]),
            )])
        };
        m.push(("$type".to_string(), Value::Text(coll.into())));
        m.push(("createdAt".to_string(), Value::Text("2026-01-01T00:00:00.000Z".into())));
        m.sort_by(|a, b| key_cmp(&a.0, &b.0));
        let rec = Value::Map(m).to_cbor();
        let c = Cid::dag_cbor(&rec);
        tree.insert_no_proof(format!("{coll}/3l{i:011}").as_bytes(), c).unwrap();
        blocks.push((c, rec));
    }
    let data = tree.write_diff_blocks(&mut blocks).unwrap();
    drop(tree);
    let mut f = vec![
        ("did".to_string(), Value::Text("did:plc:bench".into())),
        ("rev".to_string(), Value::Text("3l3qo2vuowo2b".into())),
        ("data".to_string(), Value::Link(data)),
        ("prev".to_string(), Value::Null),
        ("version".to_string(), Value::Int(3)),
        ("sig".to_string(), Value::Bytes(vec![0; 64])),
    ];
    f.sort_by(|a, b| key_cmp(&a.0, &b.0));
    let commit = Value::Map(f).to_cbor();
    let root = Cid::dag_cbor(&commit);
    blocks.sort_by_key(|(c, _)| c.to_bytes());
    let mut cid = Vec::new();
    vlpds::car::write_header(&mut cid, &root);
    vlpds::car::write_block(&mut cid, &root, &commit);
    for (c, b) in &blocks {
        vlpds::car::write_block(&mut cid, c, b);
    }
    std::fs::write(format!("{dir}/{n}-cid.car"), &cid).unwrap();
    let map: std::collections::HashMap<Cid, Vec<u8>> = blocks.into_iter().collect();
    let stream = vlpds::car_order::write_car((root, &commit), data, &map).unwrap();
    std::fs::write(format!("{dir}/{n}-stream.car"), &stream).unwrap();
    println!("wrote {} and {} bytes", cid.len(), stream.len());
}

fn jemalloc() -> (u64, u64) {
    use tikv_jemalloc_ctl::{epoch, stats};
    epoch::advance().unwrap();
    (stats::allocated::read().unwrap() as u64, stats::resident::read().unwrap() as u64)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn import_bench() {
    let (dir, n, mode) = (env("IMPORT_BENCH_DIR"), env("IMPORT_BENCH_N").parse::<usize>().unwrap(), env("IMPORT_BENCH_MODE"));
    if mode == "gen" {
        return gen(&dir, n);
    }
    let s = TestServer::spawn_with(|c| c.max_import_bytes = 2 << 30).await;
    let a = s.create_account("bench").await;
    let car = std::fs::read(format!("{dir}/{n}-{mode}.car")).unwrap();
    let len = car.len();
    let (base_alloc, base_res) = jemalloc();
    let stop = Arc::new(AtomicBool::new(false));
    let (peak_alloc, peak_res, parse_alloc, parse_res) =
        (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let parsed = || ["stream", "buffered"].iter().map(|p| vlpds::metrics::IMPORT_REPO_PARSES.with_label_values(&[p]).get()).sum::<u64>();
    let parsed0 = parsed();
    let parse_time = Arc::new(AtomicU64::new(0));
    let t = Instant::now();
    let sampler = {
        let (stop, pa, pr, qa, qr, pt) = (stop.clone(), peak_alloc.clone(), peak_res.clone(), parse_alloc.clone(), parse_res.clone(), parse_time.clone());
        std::thread::spawn(move || {
            let mut parsing = true;
            while !stop.load(Ordering::Relaxed) {
                let (al, rs) = jemalloc();
                pa.fetch_max(al, Ordering::Relaxed);
                pr.fetch_max(rs, Ordering::Relaxed);
                if parsing {
                    qa.fetch_max(al, Ordering::Relaxed);
                    qr.fetch_max(rs, Ordering::Relaxed);
                    if parsed() != parsed0 {
                        parsing = false;
                        pt.store(t.elapsed().as_millis() as u64, Ordering::Relaxed);
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };
    let r = s.xrpc.post_bytes("com.atproto.repo.importRepo", car, "application/vnd.ipld.car", &a.auth()).await;
    let took = t.elapsed();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
    let path = if vlpds::metrics::IMPORT_REPO_PARSES.with_label_values(&["stream"]).get() > 0 { "stream" } else { "buffered" };
    let mb = |v: u64| v as f64 / (1 << 20) as f64;
    let ld = |p: &AtomicU64| p.load(Ordering::Relaxed);
    println!(
        "{n} records, {mode} order ({path} parse), CAR {:.0} MB: total {:.2}s (parsed at {:.2}s); \
         over baseline: parse peak heap {:.0} MB / resident {:.0} MB, import peak heap {:.0} MB / resident {:.0} MB",
        mb(len as u64),
        took.as_secs_f64(),
        ld(&parse_time) as f64 / 1000.0,
        mb(ld(&parse_alloc).saturating_sub(base_alloc)),
        mb(ld(&parse_res).saturating_sub(base_res)),
        mb(ld(&peak_alloc).saturating_sub(base_alloc)),
        mb(ld(&peak_res).saturating_sub(base_res)),
    );
}
