//! A handoff of many busy shards to one peer still prewarms it: the
//! request carries at most `PREWARM_RECENT_BYTES` of recent repos (a full
//! list per shard for 32 shards was over axum's 2 MB default and answered
//! 413, so the peer started cold).

use crate::common::*;
use std::sync::Arc;
use std::time::Duration;
use vlpds::metrics::SHARD_PREWARMS;
use vlpds::xrpc::internal::{prewarm_peers, prewarm_request, PREWARM_RECENT_BYTES};

const SHARDS: u32 = 64;

fn fake_did(shard: u32, i: usize) -> Arc<str> {
    Arc::from(format!("did:plc:{shard:04}{i:020}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_of_64_busy_shards_prewarms() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = cluster_node("pw-a", store.clone(), SHARDS, |_| {}).await;
    eventually(Duration::from_secs(30), || async { (owned(&a) == SHARDS as usize).then_some(()) })
        .await
        .expect("a owns every shard");
    let mut raw = 0;
    for p in a.app.partitions.owned().iter() {
        for i in 0..p.recent.cap() {
            let d = fake_did(p.id.0, i);
            raw += d.len() + 3;
            p.recent.touch(&d);
        }
    }
    assert!(raw > 2 << 20, "recent lists ({raw} B) fit the old limit anyway");

    // every shard to one peer, as the handoff builds it
    let shards: Vec<_> = a
        .app
        .partitions
        .owned()
        .iter()
        .map(|p| (p.id, p.recent.snapshot().iter().map(|d| d.to_string()).collect()))
        .collect();
    let cap = a.app.partitions.owned()[0].recent.cap();
    let ok0 = SHARD_PREWARMS.with_label_values(&["ok"]).get();
    let b = cluster_node("pw-b", store.clone(), SHARDS, |_| {}).await;
    let req = prewarm_request(shards);
    assert_eq!(req.len(), SHARDS as usize);
    let carried: usize = req.iter().flat_map(|s| &s.recent).map(|d| d.len() + 3).sum();
    assert!(carried <= PREWARM_RECENT_BYTES && carried > PREWARM_RECENT_BYTES - 64 * 40, "{carried} B of recent repos");
    // whole ranks, newest first: no shard's list is more than one entry
    // shorter than another's
    let (min, max) =
        (req.iter().map(|s| s.recent.len()).min().unwrap(), req.iter().map(|s| s.recent.len()).max().unwrap());
    assert!(max - min <= 1 && min > 0, "per-shard lists {min}..={max}");
    assert_eq!(req[0].recent[0], fake_did(req[0].shard.0, cap - 1).to_string(), "newest first");

    let out = prewarm_peers(
        &a.app.node.http,
        &a.app.node.internal_token,
        vec![(b.peer_url.clone(), req)],
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(out.len(), 1);
    assert!(out[0].1.is_ok(), "prewarm of {SHARDS} shards: {:?}", out[0].1);
    assert!(SHARD_PREWARMS.with_label_values(&["ok"]).get() >= ok0 + SHARDS as u64);

    // and the real handback of half of them (over 29) to the joiner
    balanced(&[&a, &b]).await;
    assert!(SHARD_PREWARMS.with_label_values(&["ok"]).get() >= ok0 + SHARDS as u64 + SHARDS as u64 / 2);
}
