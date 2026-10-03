//! Account totals load in the background after a shard opens: changes made
//! while a shard's totals are loading (delta rows), on its next owner too,
//! still add up once they load; meanwhile the shard is left out whole.

use crate::common::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 6;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> TestServer {
    let (id, raw) = (id.to_string(), store.clone() as Arc<dyn object_store::ObjectStore>);
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(raw);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: peer_url(c),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await
}

fn owned(s: &TestServer) -> usize {
    s.app.partitions.owned().len()
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn changes_while_totals_load_add_up() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("lazy-tot-a", &store).await;
    let mut accts = Vec::new();
    for _ in 0..16 {
        accts.push(a.create_account("lazy").await);
    }
    let loaded = eventually(Duration::from_secs(10), || async { (vlpds::xrpc::totals_loading(&a.app).1 == 0).then_some(()) }).await;
    assert!(loaded.is_some());
    assert_eq!(vlpds::xrpc::totals(&a.app).accounts, [16, 0, 0, 0, 0]);

    let hold_a = vlpds::totals::hold_loads("lazy-tot-a");
    let hold_b = vlpds::totals::hold_loads("lazy-tot-b");
    let b = node("lazy-tot-b", &store).await;
    wait_for("shards spread", || owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize).await;
    let (t, loading) = vlpds::xrpc::totals_loading(&b.app);
    assert_eq!((t.repos(), loading), (0, owned(&b)), "b's shards are left out while loading");

    // b's shards take delta rows
    for _ in 0..8 {
        accts.push(b.create_account("lazy").await);
    }
    for acct in &accts[..8] {
        let r = b.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &acct.auth()).await;
        assert!(r.is_ok(), "deactivate: {} {}", r.status, r.text());
    }

    // ... and its shards return to a, whose loads wait too: the delta rows
    // b wrote and a's own add up
    vlpds::server::shutdown(&b.app).await;
    wait_for("a holds every shard", || owned(&a) == SHARDS as usize).await;
    assert!(vlpds::xrpc::totals_loading(&a.app).1 > 0, "the returned shards are loading");
    for _ in 0..4 {
        accts.push(a.create_account("lazy").await);
    }
    for acct in &accts[8..12] {
        set_repo_takedown(&a, &acct.did, true).await;
    }
    drop((hold_a, hold_b));

    let r = eventually(Duration::from_secs(20), || async {
        let (kept, loading) = vlpds::xrpc::totals_loading(&a.app);
        let scanned = vlpds::xrpc::scan_totals(&a.app).await.ok()?;
        let today = vlpds::totals::today();
        let heads: i64 = scanned.days.iter().map(|d| d.1).sum();
        (loading == 0 && kept.accounts == scanned.accounts && kept.repos() == heads && kept.written_within(1, today) == scanned.written_within(1, today)).then_some(kept)
    })
    .await;
    let kept = r.expect("kept totals never matched the scan");
    assert_eq!((kept.accounts, kept.repos()), ([16, 8, 4, 0, 0], 28));
}
