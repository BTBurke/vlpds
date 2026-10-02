//! Lease liveness under clock skew (bench/ha O3) and batched drains (O6):
//! in-process nodes sharing one in-memory object store, each with its wall
//! clock offset as the control plane sees it. Peers judge liveness by seeing
//! a lease change on their own monotonic clocks, so seconds of skew (far
//! beyond the TTL) neither fence a live node nor delay a takeover.

use crate::common::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 12;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>, clock_offset_ms: i64) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            clock_offset_ms,
            ..Default::default()
        });
    })
    .await
}

fn cluster(n: &TestServer) -> &vlpds::cluster::Cluster {
    n.app.cluster.as_deref().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn skewed_clocks_keep_every_node_live() {
    let store = Arc::new(object_store::memory::InMemory::new());
    // +-4 s against a 1.5 s TTL and 200 ms skew margin: under the old
    // wall-clock rule "slow" looked dead to its peers right after each
    // renewal, and was fenced (its process exits 3 or 5, ending this test)
    let fast = node("lv-fast", &store, 4_000).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..6).map(|_| fast.create_account("lv"))).await;
    let mid = node("lv-mid", &store, 0).await;
    let slow = node("lv-slow", &store, -4_000).await;

    // writers through every node (forwarded to the owner) for a few TTLs
    let stop = Arc::new(AtomicBool::new(false));
    let (acked, failed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let mut writers = Vec::new();
    for (i, acct) in accounts.iter().cloned().enumerate() {
        let url = [&fast.url, &mid.url, &slow.url][i % 3].clone();
        let (x, stop, acked, failed) = (Xrpc::new(&url), stop.clone(), acked.clone(), failed.clone());
        writers.push(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Acquire) {
                i += 1;
                let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("skew {i}"))});
                if x.post("com.atproto.repo.createRecord", &body, &acct.auth()).await.is_ok() {
                    acked.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, Ordering::Relaxed); // shard moving while nodes join
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let failed_after_join = failed.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Release);
    for w in writers {
        w.await.unwrap();
    }
    assert!(acked.load(Ordering::Relaxed) > 100, "too few writes acked: {acked:?}");
    assert_eq!(failed.load(Ordering::Relaxed), failed_after_join, "writes failed in steady state (a node was presumed dead?)");
    for n in [&fast, &mid, &slow] {
        let c = cluster(n);
        assert!(c.lease_valid(), "{}: lease lapsed", c.cfg.node_id);
        assert!(c.fenced_logs().is_empty(), "{}: fenced {:?}", c.cfg.node_id, c.fenced_logs());
        assert_eq!(c.peers().len(), 2, "{}: peers {:?}", c.cfg.node_id, c.peers());
        assert_eq!(c.owned().len(), SHARDS as usize / 3, "{} owns its fair share", c.cfg.node_id);
    }

    // graceful drain of the fast node: its shards move in one batch and the
    // others (whose clocks are behind) keep serving every account
    let fast_owned = cluster(&fast).owned();
    vlpds::server::shutdown(&fast.app).await;
    assert!(cluster(&fast).owned().is_empty());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while cluster(&mid).owned().len() + cluster(&slow).owned().len() < SHARDS as usize {
        assert!(tokio::time::Instant::now() < deadline, "drained shards {fast_owned:?} never taken");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await; // routing tables catch up
    for (i, acct) in accounts.iter().enumerate() {
        let n = [&mid, &slow][i % 2];
        n.create_record(acct, "app.bsky.feed.post", post_record("after drain")).await;
    }
    assert!(cluster(&mid).fenced_logs().is_empty() && cluster(&slow).fenced_logs().is_empty(), "a drain fences nobody else");
}
