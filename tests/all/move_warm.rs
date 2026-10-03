//! Warm shard moves: a handoff's recipient reads the shard through a
//! read-only view while its owner keeps writing (`Node::warm_handoff`), and
//! a handback that prewarms over the peer listener still hands every shard
//! over and serves the moved repos.

use crate::common::*;
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: peer_url(c),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(300),
            ..Default::default()
        });
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prewarmed_handback_serves_moved_repos() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("mw-a", &store).await;
    eventually(Duration::from_secs(20), || async { (a.app.partitions.owned().len() == SHARDS as usize).then_some(()) }).await.expect("a owns every shard");
    let mut accts = Vec::new();
    for _ in 0..6 {
        let acct = a.create_account("mw").await;
        for i in 0..5 {
            a.post(&acct, &format!("before {i}")).await;
        }
        accts.push(acct);
    }

    // a peer warms a's shards while a writes to them
    let b = node("mw-b", &store).await;
    let shards: Vec<vlpds::xrpc::internal::PrewarmShard> = a
        .app
        .partitions
        .owned()
        .iter()
        .map(|p| vlpds::xrpc::internal::PrewarmShard { shard: p.id, recent: p.recent.snapshot().iter().map(|d| d.to_string()).collect() })
        .collect();
    assert!(shards.iter().any(|s| !s.recent.is_empty()), "a recorded no recent repos");
    let warm = {
        let node = b.app.node.clone();
        tokio::spawn(async move { node.warm_handoff(shards).await })
    };
    for acct in &accts {
        a.post(acct, "during warm").await;
    }
    tokio::time::timeout(Duration::from_secs(20), warm).await.expect("warm_handoff finished").unwrap();

    // b joins and gets half the shards back, prewarmed over the peer listener
    eventually(Duration::from_secs(30), || async {
        let (na, nb) = (a.app.partitions.owned().len(), b.app.partitions.owned().len());
        (na + nb == SHARDS as usize && nb == SHARDS as usize / 2).then_some(())
    })
    .await
    .expect("handed back half");
    let moved: Vec<&TestAccount> = accts.iter().filter(|x| b.app.partitions.for_key(&x.did).is_some()).collect();
    assert!(!moved.is_empty(), "no test account moved to b");
    for acct in &moved {
        b.post(acct, "after move").await;
        let r = b.xrpc.get("com.atproto.repo.listRecords", &[("repo", acct.did.as_str()), ("collection", "app.bsky.feed.post"), ("limit", "100")], &Auth::None).await.ok();
        assert_eq!(r["records"].as_array().map(Vec::len), Some(7), "{}: {r}", acct.did);
    }
}
