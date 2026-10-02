//! Rebalance handback gap: when a node joins, its peers close their extra
//! shards and hand them over. The joiner used to pick released shards up
//! only on its next control-plane step (~3 s in bench 2026-10-02 §2: a step
//! interval, the step's own reads and CAS, then opening). Releasers now CAS
//! the assignment straight to the joiner and nudge it with the handoff
//! (POST /internal/v1/cluster/nudge), and it opens them at once.
//!
//! Measured per shard, polling every node's partition table and (without
//! the injected latency) the assignment objects:
//! - release -> serve: from the releaser's assignment CAS to the joiner
//!   serving it (target < 500 ms);
//! - unavailable: from the releaser dropping it (start of its close) to the
//!   joiner serving it.
//!
//! Production-like step timing (renew every 1 s, TTL 5 s) so a missed nudge
//! shows up as a ~1 s gap, not the 100 ms the other cluster tests step at.

use crate::common::*;
use object_store::ObjectStoreExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u16 = 12;

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_secs(5),
            renew_every: Duration::from_secs(1),
            skew: Duration::from_secs(1),
            ..Default::default()
        });
    })
    .await
}

fn owned(n: &TestServer) -> Vec<u16> {
    n.app.partitions.owned().iter().map(|p| p.id).collect()
}

/// Per moved shard: (release -> serve, unavailable).
type Gaps = Vec<(u16, Duration, Duration)>;

/// Polls until `joiner` serves `want` shards.
async fn watch_handback(nodes: &[&TestServer], joiner: &TestServer, raw: &object_store::memory::InMemory, want: usize) -> Gaps {
    let j = nodes.iter().position(|n| std::ptr::eq(*n, joiner)).unwrap();
    let jid = joiner.app.cluster.as_ref().unwrap().cfg.node_id.clone();
    let (mut dropped, mut released): (HashMap<u16, Instant>, HashMap<u16, Instant>) = Default::default();
    let mut gaps = HashMap::new();
    let mut prev: Vec<Option<usize>> = vec![None; SHARDS as usize];
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let now = Instant::now();
        let mut cur: Vec<Option<usize>> = vec![None; SHARDS as usize];
        for (i, n) in nodes.iter().enumerate() {
            for s in owned(n) {
                cur[s as usize] = Some(i);
            }
        }
        for s in 0..SHARDS {
            let (p, c) = (prev[s as usize], cur[s as usize]);
            if p.is_some() && p != Some(j) && c.is_none() {
                dropped.insert(s, now);
            }
            if c != Some(j) && !released.contains_key(&s) {
                let path = object_store::path::Path::from(format!("vlpds/assign/{s:03}"));
                if let Ok(r) = raw.get(&path).await {
                    let a: vlpds::cluster::Assignment = serde_json::from_slice(&r.bytes().await.unwrap()).unwrap();
                    if a.owner.as_deref() == Some(jid.as_str()) {
                        released.insert(s, now);
                    }
                }
            }
            if c == Some(j) && p != Some(j) {
                let r = released.get(&s).copied().unwrap_or(now);
                let d = dropped.get(&s).copied().unwrap_or(r);
                gaps.insert(s, (now - r, now - d));
            }
        }
        // (judged on this poll's snapshot: re-reading the joiner here could
        // count shards it opened during the polls above, never recorded)
        let have = cur.iter().filter(|c| **c == Some(j)).count();
        prev = cur;
        if have >= want {
            let mut v: Gaps = gaps.into_iter().map(|(s, (r, d))| (s, r, d)).collect();
            v.sort();
            return v;
        }
        assert!(now < deadline, "joiner never got its share: {:?}", owned(joiner));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// An in-memory store shared by the nodes, with every call (control plane,
/// log, SlateDB) taking `ms`; and the store underneath, for peeking.
fn store_with_latency(ms: u64) -> (Arc<dyn object_store::ObjectStore>, Arc<object_store::memory::InMemory>) {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let mem = Arc::new(object_store::memory::InMemory::new());
    if ms == 0 {
        return (mem.clone(), mem);
    }
    let d = Duration::from_millis(ms);
    let cfg = ThrottleConfig { wait_get_per_call: d, wait_put_per_call: d, wait_list_per_call: d, wait_delete_per_call: d, ..Default::default() };
    (Arc::new(ThrottledStore::new(mem.clone(), cfg)), mem)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn joiner_serves_handed_back_shards_promptly() {
    handback(0, Duration::from_millis(100)).await;
}

/// The same with S3-like latency on every object-store call: release ->
/// serve is the joiner opening the shard's SlateDB (~22 sequential calls);
/// the releaser's close (barrier, checkpoint, flush) adds ~8 more before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn joiner_serves_handed_back_shards_promptly_at_20ms() {
    handback(20, Duration::from_millis(1000)).await;
}

async fn handback(latency_ms: u64, max_unavailable: Duration) {
    let (store, raw) = store_with_latency(latency_ms);
    let a = node("hb-a", &store).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..8).map(|_| a.create_account("hb"))).await;
    assert_eq!(owned(&a).len(), SHARDS as usize);

    let b = node("hb-b", &store).await;
    let gaps_b = watch_handback(&[&a, &b], &b, &raw, SHARDS as usize / 2).await;
    let c = node("hb-c", &store).await;
    let gaps_c = watch_handback(&[&a, &b, &c], &c, &raw, SHARDS as usize / 3).await;
    for (name, gaps) in [("a -> b", &gaps_b), ("a,b -> c", &gaps_c)] {
        let serve = gaps.iter().map(|g| g.1).max().unwrap();
        let gone = gaps.iter().map(|g| g.2).max().unwrap();
        eprintln!("handback {name} ({latency_ms} ms store): {} shards, release -> serve max {serve:?}, unavailable max {gone:?}", gaps.len());
        assert!(serve < Duration::from_millis(500), "handback {name}: release -> serve {serve:?} ({gaps:?})");
        assert!(gone < max_unavailable, "handback {name}: unavailable {gone:?} ({gaps:?})");
    }
    // the moved shards serve (writes through any node land on the new owner)
    tokio::time::sleep(Duration::from_millis(200)).await;
    for (i, acct) in accounts.iter().enumerate() {
        let n = [&a, &b, &c][i % 3];
        n.create_record(acct, "app.bsky.feed.post", post_record("after handback")).await;
    }
    for n in [&a, &b, &c] {
        assert!(n.app.cluster.as_ref().unwrap().fenced_logs().is_empty(), "a rebalance fences nobody");
    }
}
