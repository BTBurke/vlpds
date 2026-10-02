//! Cold start after a node restart (TODO "Capacity-test findings"): the
//! first writes to a shard's repos on their new owner are cold loads, which
//! used to queue past the forwarder's 3 s time-to-first-byte deadline and
//! fail. Now (DESIGN.md "Forwarding deadlines" and §2):
//! - a forwarded write that hasn't started within 1 s is abandoned unapplied
//!   and answered 503 `RepoLoading`; the forwarding node resends it, so the
//!   client sees latency, not an error;
//! - a shard's recently written repos are persisted with its checkpoints and
//!   preloaded by its next owner.
//!
//! `restart_window_*` measure it (ignored; run one at a time, they print a
//! per-second table): 3 nodes on a store with S3-like latency and bounded
//! concurrency, ~50k repos, a Zipf writer through two nodes while the third
//! restarts gracefully (its shards move to the others and back).

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::state::bulk_did;
use vlpds::worker::{CachedRepo, WorkerMsg};

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>, shards: u32, f: impl FnOnce(&mut vlpds::server::Config)) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = shards;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: peer_url(c),
            shards,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(300),
            ..Default::default()
        });
        f(c);
    })
    .await
}

async fn cache_info(s: &TestServer, did: &str) -> Option<CachedRepo> {
    let (reply, rx) = tokio::sync::oneshot::channel();
    s.app.workers.route(did).send(WorkerMsg::CacheInfo { did: did.into(), reply }).unwrap();
    rx.await.unwrap()
}

/// Bulk accounts `range` with `records` each, every node creating its own.
async fn populate(nodes: &[&TestServer], range: std::ops::Range<u64>, records: u32) {
    let auth = Auth::Bearer(ADMIN_TOKEN.into());
    let mut per: Vec<Vec<u64>> = vec![Vec::new(); nodes.len()];
    for i in range {
        let did = bulk_did(i);
        let k = nodes.iter().position(|n| n.app.partitions.for_key(&did).is_some()).expect("an owner");
        per[k].push(i);
    }
    let reqs = per.iter().enumerate().flat_map(|(k, idx)| idx.chunks(1000).map(move |c| (k, c.to_vec())));
    use futures::StreamExt;
    futures::stream::iter(reqs)
        .map(|(k, c)| {
            let auth = &auth;
            async move {
                let r = nodes[k].xrpc.post("vlpds.admin.bulkCreate", &json!({"indices": c, "records": records}), auth).await;
                assert_eq!(r.status, 200, "{}", r.text());
                assert_eq!(r.json["created"].as_u64(), Some(c.len() as u64), "{}", r.text());
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
}

async fn create_via(s: &TestServer, did: &str, text: &str) -> Resp {
    let token = s.app.jwt.access(did);
    s.xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": did, "collection": "app.bsky.feed.post", "record": post_record(text)}),
            &Auth::Bearer(token),
        )
        .await
}

fn owner<'a>(nodes: &[&'a TestServer], did: &str) -> &'a TestServer {
    nodes.iter().find(|n| n.app.partitions.for_key(did).is_some()).copied().expect("owned")
}

/// Writes keep succeeding, each applied once, while shards move: a second
/// node joins and takes half the shards back while writes flow through
/// both. Writes that reach a node after their shard left it (ShardMoved) or
/// before their repo loaded on the new owner (RepoLoading) were never
/// applied, and the node the client called resends them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_survive_a_handback() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("hb2-a", &store, 8, |_| {}).await;
    populate(&[&a], 0..40, 3).await;
    let t = Instant::now();
    let writer = {
        let (url_a, http) = (a.url.clone(), reqwest::Client::new());
        let tokens: Vec<String> = (0..40).map(|i| a.app.jwt.access(&bulk_did(i))).collect();
        tokio::spawn(async move {
            let mut futs = Vec::new();
            let mut sent = vec![0usize; 40];
            for k in 0..600u64 {
                let i = (k % 40) as usize;
                sent[i] += 1;
                let body = json!({"repo": bulk_did(i as u64), "collection": "app.bsky.feed.post", "record": post_record("hb")});
                let rb = http.post(format!("{url_a}/xrpc/com.atproto.repo.createRecord")).bearer_auth(&tokens[i]).json(&body);
                futs.push(tokio::spawn(async move {
                    let r = rb.send().await.unwrap();
                    (r.status().as_u16(), r.text().await.unwrap_or_default())
                }));
                tokio::time::sleep(Duration::from_millis(4)).await;
            }
            let mut out = Vec::new();
            for f in futs {
                out.push(f.await.unwrap());
            }
            (sent, out)
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b = node("hb2-b", &store, 8, |_| {}).await;
    let (sent, results) = writer.await.unwrap();
    let bad: Vec<_> = results.iter().filter(|r| r.0 != 200).collect();
    assert!(bad.is_empty(), "{} of {} writes failed: {:?}", bad.len(), results.len(), &bad[..bad.len().min(3)]);
    assert!(!b.app.partitions.owned().is_empty(), "b took shards within {:?}", t.elapsed());
    let nodes = [&a, &b];
    for (i, n) in sent.iter().enumerate() {
        let did = bulk_did(i as u64);
        let r = owner(&nodes, &did).list_records(&did, "app.bsky.feed.post", &[("limit", "100")]).await.ok();
        assert_eq!(r["records"].as_array().unwrap().len(), 3 + n, "{did}: every write applied once");
    }
}

/// A lone node serves requests without the routing work, but a write there
/// can still find its shard gone before it starts: frozen for a split, or
/// taken by a node that joined after the check (writes_survive_a_handback
/// once got a ShardMoved that way). It is resent until the shard is back,
/// as on a node with peers, instead of failing with a 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lone_node_resends_a_write_whose_shard_left() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("lone", &store, 4, |_| {}).await;
    populate(&[&a], 0..1, 1).await;
    assert!(a.app.cluster.as_ref().unwrap().alone());
    let did = bulk_did(0);
    let shard = a.app.partitions.shard_of(&did);
    let p = a.app.partitions.get(shard).expect("owned");
    // out of routing and the workers' caches, as a close does when it moves
    a.app.partitions.set(shard, None);
    for w in a.app.workers.senders.iter() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        w.send(WorkerMsg::DropPartition(shard, tx)).unwrap();
        let _ = rx.await;
    }
    let table = a.app.partitions.clone();
    let back = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        table.set(shard, Some(p));
    });
    let r = create_via(&a, &did, "while its shard was away").await;
    assert_eq!(r.status, 200, "{}", r.text());
    back.await.unwrap();
    let r = a.list_records(&did, "app.bsky.feed.post", &[("limit", "100")]).await.ok();
    assert_eq!(r["records"].as_array().unwrap().len(), 2, "applied once");
}

/// A shard's recently written repos survive a restart: the node that opens
/// it next loads them before any request asks (from the set persisted with
/// the checkpoint).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_repos_preloaded_after_restart() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("rp", &store, 4, |_| {}).await;
    populate(&[&a], 0..30, 1).await;
    // write to 10 of them; the other 20 were only created
    for i in 0..10 {
        let r = create_via(&a, &bulk_did(i), "hot").await;
        assert_eq!(r.status, 200, "{}", r.text());
    }
    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;

    let b = node("rp", &store, 4, |_| {}).await;
    for i in 0..10 {
        let did = bulk_did(i);
        let info = eventually(Duration::from_secs(10), || async { cache_info(&b, &did).await })
            .await
            .unwrap_or_else(|| panic!("repo {i} not preloaded"));
        assert!(info.loaded_nodes >= 1, "{info:?}");
    }
    for i in 10..30 {
        assert!(cache_info(&b, &bulk_did(i)).await.is_none(), "only written repos are preloaded");
    }
}

// ---------------------------------------------------------------------------
// measurement
// ---------------------------------------------------------------------------

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Zipf(s) over `n` ranks, mapped through a fixed permutation (so the hot
/// repos spread over the shards).
struct Zipf {
    cdf: Vec<f64>,
    perm: Vec<u64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Zipf {
        use rand::seq::SliceRandom;
        let mut acc = 0.0;
        let mut cdf: Vec<f64> = (1..=n).map(|k| { acc += 1.0 / (k as f64).powf(s); acc }).collect();
        for c in &mut cdf {
            *c /= acc;
        }
        let mut perm: Vec<u64> = (0..n as u64).collect();
        perm.shuffle(&mut rand::rngs::StdRng::seed_from_u64(7));
        Zipf { cdf, perm }
    }
    fn sample(&self, r: &mut impl rand::Rng) -> u64 {
        let u: f64 = r.gen();
        self.perm[self.cdf.partition_point(|&c| c < u).min(self.perm.len() - 1)]
    }
}

use rand::SeedableRng;

#[derive(Default)]
struct Second {
    ok: AtomicU64,
    err: AtomicU64,
    lat: parking_lot::Mutex<Vec<u32>>,
    /// error name (or transport error) -> count
    kinds: parking_lot::Mutex<std::collections::BTreeMap<String, u64>>,
}

/// One run: populate, then write at `rate`/s (Zipf) through a and b for
/// `secs`; c shuts down gracefully at `down_at` and starts again at `up_at`.
async fn restart_window(label: &str, tuned: bool) {
    let repos: u64 = env("COLD_REPOS", 50_000);
    let records: u32 = env("COLD_RECORDS", 20);
    let rate: u64 = env("COLD_RATE", 1500);
    let secs: u64 = env("COLD_SECS", 30);
    let (down_at, up_at) = (env("COLD_DOWN_AT", 4u64), env("COLD_UP_AT", 10u64));
    let lat_ms: u64 = env("COLD_STORE_MS", 10);
    let limit: usize = env("COLD_STORE_CONCURRENCY", 48);
    vlpds::partition::set_block_cache_bytes(64 << 20);
    let store: Arc<dyn object_store::ObjectStore> = {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let d = Duration::from_millis(lat_ms);
        let cfg = ThrottleConfig { wait_get_per_call: d, wait_put_per_call: d, wait_list_per_call: d, wait_delete_per_call: d, ..Default::default() };
        Arc::new(object_store::limit::LimitStore::new(ThrottledStore::new(object_store::memory::InMemory::new(), cfg), limit))
    };
    let shards: u32 = 48;
    let set = move |c: &mut vlpds::server::Config| {
        c.workers = 2;
        // production-like leases: a saturated store must not fail-stop a node
        if let Some(cl) = &mut c.cluster {
            (cl.ttl, cl.renew_every, cl.skew) = (Duration::from_secs(5), Duration::from_secs(1), Duration::from_secs(1));
        }
        if !tuned {
            c.preload_recent = 0;
            c.forwarded_write_start = None;
            c.retry_unapplied_writes = false;
        }
    };
    let a = node("cs-a", &store, shards, set).await;
    let b = node("cs-b", &store, shards, set).await;
    let c = node("cs-c", &store, shards, set).await;
    eventually(Duration::from_secs(20), || async {
        let n = [&a, &b, &c].map(|n| n.app.partitions.owned().len());
        (n.iter().sum::<usize>() == shards as usize && n.iter().all(|&k| k == shards as usize / 3)).then_some(())
    })
    .await
    .expect("converged");
    let t = Instant::now();
    populate(&[&a, &b, &c], 0..repos, records).await;
    eprintln!("[{label}] populated {repos} repos x {records} records in {:.1}s", t.elapsed().as_secs_f64());
    for n in [&a, &b, &c] {
        n.app.log.checkpoint_all().await;
    }

    let misses0 = vlpds::metrics::REPO_CACHE.with_label_values(&["miss"]).get();
    let zipf = Arc::new(Zipf::new(repos as usize, env("COLD_ZIPF_S", 1.0)));
    let tokens: Arc<Vec<String>> = Arc::new((0..repos).map(|i| a.app.jwt.access(&bulk_did(i))).collect());
    let seconds: Arc<Vec<Second>> = Arc::new((0..secs + 40).map(|_| Second::default()).collect());
    let http = reqwest::Client::builder().timeout(Duration::from_secs(60)).pool_max_idle_per_host(512).build().unwrap();
    let urls = [a.url.clone(), b.url.clone()];
    let start = Instant::now();
    let gen = {
        let (zipf, tokens, seconds, http) = (zipf.clone(), tokens.clone(), seconds.clone(), http.clone());
        tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(11);
            let interval = Duration::from_secs_f64(1.0 / rate as f64);
            let mut next = Instant::now();
            let mut n = 0u64;
            let mut tasks = Vec::new();
            while start.elapsed() < Duration::from_secs(secs) {
                tokio::time::sleep_until(next.into()).await;
                while next <= Instant::now() {
                    let sched = next;
                    next += interval;
                    n += 1;
                    let i = zipf.sample(&mut rng);
                    let url = format!("{}/xrpc/com.atproto.repo.createRecord", urls[(n % 2) as usize]);
                    let body = json!({"repo": bulk_did(i), "collection": "app.bsky.feed.post", "record": post_record("zipf")});
                    let (http, tokens, seconds) = (http.clone(), tokens.clone(), seconds.clone());
                    tasks.push(tokio::spawn(async move {
                        let r = http.post(url).bearer_auth(&tokens[i as usize]).json(&body).send().await;
                        let sec = &seconds[(sched - start).as_secs() as usize];
                        match r {
                            Ok(r) if r.status().is_success() => {
                                sec.ok.fetch_add(1, Ordering::Relaxed);
                            }
                            r => {
                                sec.err.fetch_add(1, Ordering::Relaxed);
                                let kind = match r {
                                    Ok(r) => {
                                        let st = r.status().as_u16();
                                        let j: J = r.json().await.unwrap_or(J::Null);
                                        format!("{st} {}", j["error"].as_str().unwrap_or("?"))
                                    }
                                    Err(e) => format!("transport {e}"),
                                };
                                *sec.kinds.lock().entry(kind).or_default() += 1;
                            }
                        }
                        sec.lat.lock().push(sched.elapsed().as_millis() as u32);
                    }));
                }
            }
            for t in tasks {
                let _ = t.await;
            }
        })
    };
    tokio::time::sleep_until((start + Duration::from_secs(down_at)).into()).await;
    let td = Instant::now();
    vlpds::server::shutdown(&c.app).await;
    eprintln!("[{label}] c down at {:.1}s (shutdown {:.1}s)", start.elapsed().as_secs_f64(), td.elapsed().as_secs_f64());
    drop(c);
    tokio::time::sleep_until((start + Duration::from_secs(up_at)).into()).await;
    let c2 = node("cs-c", &store, shards, set).await;
    eprintln!("[{label}] c up at {:.1}s", start.elapsed().as_secs_f64());
    gen.await.unwrap();
    let owned = |n: &TestServer| n.app.partitions.owned().len();
    eprintln!("[{label}] owned after: {} {} {}", owned(&a), owned(&b), owned(&c2));

    let mut all = Vec::new();
    let (mut err_secs, mut errs, mut worst) = (Vec::new(), 0u64, 0u32);
    eprintln!("[{label}]   t   ok/s  err/s   p50   p99   max (ms)");
    for (s, sec) in seconds.iter().enumerate().take(secs as usize) {
        let mut l = std::mem::take(&mut *sec.lat.lock());
        l.sort_unstable();
        let q = |p: f64| l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1))).copied().unwrap_or(0);
        let e = sec.err.load(Ordering::Relaxed);
        eprintln!("[{label}] {s:3} {:6} {e:6} {:5} {:5} {:5} {:?}", sec.ok.load(Ordering::Relaxed), q(0.5), q(0.99), l.last().copied().unwrap_or(0), sec.kinds.lock());
        if s as u64 >= down_at {
            if e > 0 {
                err_secs.push(s);
            }
            errs += e;
            worst = worst.max(q(0.99));
            all.extend(l);
        }
    }
    all.sort_unstable();
    let p99 = all.get(all.len() * 99 / 100).copied().unwrap_or(0);
    eprintln!(
        "[{label}] from the shutdown on: {errs} errors in {} s ({err_secs:?}), p99 {p99} ms, worst 1 s p99 {worst} ms, write retries {}, abandoned {}",
        err_secs.len(),
        vlpds::metrics::WRITE_RETRIES.with_label_values(&["loading"]).get() + vlpds::metrics::WRITE_RETRIES.with_label_values(&["moved"]).get(),
        vlpds::metrics::WRITES_ABANDONED.get()
    );
    eprintln!(
        "[{label}] writes that found their repo cold: {}; recent repos preloaded: {}",
        vlpds::metrics::REPO_CACHE.with_label_values(&["miss"]).get() - misses0,
        vlpds::metrics::REPO_PRELOADS.with_label_values(&["loaded"]).get()
    );
    for l in vlpds::metrics::render().lines().filter(|l| l.starts_with("vlpds_repo_preloads_total")) {
        eprintln!("[{label}] {l}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement; run alone with --ignored --nocapture"]
async fn restart_window_before() {
    restart_window("before", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement; run alone with --ignored --nocapture"]
async fn restart_window_after() {
    restart_window("after", true).await;
}
