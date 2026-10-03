//! Node-log checkpoints (applied marker + memtable flush per shard) are
//! staggered: one shard every `checkpoint_every / shards` instead of every
//! shard back to back each interval (which stalls a CPU-starved runtime
//! right after each checkpoint).
//!
//! `stall_*` measure it (ignored; run one at a time): one node with 128
//! shards on 3 runtime threads (the laptop's --io-threads), writes at a
//! fixed rate, checkpoints every 5 s; a 10 ms ticker records how late the
//! runtime wakes it, per second, next to the writes' latency.

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::state::bulk_did;

async fn node(stagger: bool, every: Duration, shards: u32, store_ms: u64) -> TestServer {
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(Arc::new(throttled_store(store_ms)));
        c.shards = shards;
        c.workers = 2;
        c.checkpoint_every = every;
        c.checkpoint_stagger = stagger;
    })
    .await
}

/// Every shard's applied marker, as of now (log id, ordinal).
async fn markers(s: &TestServer) -> Vec<Option<(String, u64)>> {
    let mut v = Vec::new();
    for p in s.app.partitions.owned() {
        v.push(p.db.get(vlpds::nodelog::META_APPLIED).await.unwrap().map(|b| vlpds::nodelog::decode_marker(&b).unwrap()));
    }
    v
}

/// The staggered loop still checkpoints every shard about once per interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staggered_checkpoints_cover_every_shard() {
    let s = node(true, Duration::from_millis(800), 8, 0).await;
    let a = s.create_account("ckpt").await;
    s.post(&a, "one").await;
    let ord = s.app.log.durable_ordinal.load(Ordering::Acquire);
    let log = s.app.log.log_id.to_string();
    let done = eventually(Duration::from_secs(5), || async { markers(&s).await.iter().all(|m| m.as_ref().is_some_and(|(l, o)| *l == log && *o >= ord)).then_some(()) }).await;
    assert!(done.is_some(), "not every shard checkpointed past {ord}: {:?}", markers(&s).await);
}


async fn stall(label: &str, stagger: bool) {
    let shards: u32 = env_or("STALL_SHARDS", 128);
    let repos: u64 = env_or("STALL_REPOS", 20_000);
    let rate: u64 = env_or("STALL_RATE", 3000);
    let secs: u64 = env_or("STALL_SECS", 30);
    let every = Duration::from_secs(env_or("STALL_EVERY_S", 5));
    let s = node(stagger, every, shards, env_or("STALL_STORE_MS", 10)).await;
    for chunk in (0..repos).collect::<Vec<_>>().chunks(2000) {
        let r = s.xrpc.post("vlpds.admin.bulkCreate", &json!({"indices": chunk, "records": 2}), &Auth::Bearer(ADMIN_TOKEN.into())).await;
        assert_eq!(r.status, 200, "{}", r.text());
    }
    let tokens: Arc<Vec<String>> = Arc::new((0..repos).map(|i| s.app.jwt.access(&bulk_did(i))).collect());
    let n = secs as usize + 5;
    let late_max: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
    let late_sum: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
    let lat: Arc<Vec<parking_lot::Mutex<Vec<u32>>>> = Arc::new((0..n).map(|_| Default::default()).collect());
    let start = Instant::now();
    let probe = {
        let (late_max, late_sum) = (late_max.clone(), late_sum.clone());
        tokio::spawn(async move {
            let mut last = Instant::now();
            while start.elapsed() < Duration::from_secs(secs) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let late = last.elapsed().saturating_sub(Duration::from_millis(10)).as_micros() as u64;
                let k = start.elapsed().as_secs() as usize;
                late_max[k].fetch_max(late, Ordering::Relaxed);
                late_sum[k].fetch_add(late, Ordering::Relaxed);
                last = Instant::now();
            }
        })
    };
    let http = reqwest::Client::builder().timeout(Duration::from_secs(60)).pool_max_idle_per_host(512).build().unwrap();
    let url = format!("{}/xrpc/com.atproto.repo.createRecord", s.url);
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let mut next = Instant::now();
    let mut tasks = Vec::new();
    let mut i = 0u64;
    while start.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep_until(next.into()).await;
        while next <= Instant::now() {
            let sched = next;
            next += interval;
            i += 1;
            let k = (i * 2_654_435_761) % repos;
            let (http, url, tokens, lat) = (http.clone(), url.clone(), tokens.clone(), lat.clone());
            tasks.push(tokio::spawn(async move {
                let body = json!({"repo": bulk_did(k), "collection": "app.bsky.feed.post", "record": post_record("stall")});
                let r = http.post(url).bearer_auth(&tokens[k as usize]).json(&body).send().await;
                assert!(matches!(&r, Ok(r) if r.status().is_success()), "{r:?}");
                lat[(sched - start).as_secs() as usize].lock().push(sched.elapsed().as_millis() as u32);
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }
    probe.await.unwrap();
    eprintln!("[{label}]   t  late_max  late_sum (ms)  write p50  p99  max (ms)");
    let (mut worst, mut total, mut all) = (0u64, 0u64, Vec::new());
    for k in 2..secs as usize {
        let (m, sum) = (late_max[k].load(Ordering::Relaxed), late_sum[k].load(Ordering::Relaxed));
        let mut l = std::mem::take(&mut *lat[k].lock());
        l.sort_unstable();
        let q = |p: f64| l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1))).copied().unwrap_or(0);
        eprintln!("[{label}] {k:3} {:9.1} {:9.1}      {:9} {:4} {:4}", m as f64 / 1e3, sum as f64 / 1e3, q(0.5), q(0.99), l.last().copied().unwrap_or(0));
        worst = worst.max(m);
        total += sum;
        all.extend(l);
    }
    all.sort_unstable();
    eprintln!(
        "[{label}] {shards} shards, {rate}/s, checkpoint every {every:?}: worst tick {:.1} ms, late {:.0} ms/s, write p99 {} ms p99.9 {} ms max {} ms, checkpoint p50 {:.1} ms",
        worst as f64 / 1e3,
        total as f64 / 1e3 / (secs - 2) as f64,
        all[all.len() * 99 / 100],
        all[all.len() * 999 / 1000],
        all.last().unwrap(),
        vlpds::metrics::CHECKPOINT_SHARD.get_sample_sum() / vlpds::metrics::CHECKPOINT_SHARD.get_sample_count().max(1) as f64 * 1e3,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "measurement; run alone with --ignored --nocapture"]
async fn stall_burst() {
    stall("burst", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "measurement; run alone with --ignored --nocapture"]
async fn stall_staggered() {
    stall("staggered", true).await;
}
