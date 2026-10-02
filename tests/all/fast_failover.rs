//! Failover without waiting out lease TTLs (DESIGN.md "Liveness"):
//! - a dead peer is taken over within TTL + skew (+ a step) of its last
//!   renewal (2 nodes, `Node::halt` as kill -9);
//! - a peer that has missed a renewal and whose address refuses TCP
//!   connections (its process is gone) is presumed dead at once, not after
//!   TTL + skew; one that still accepts connections (frozen, or a halted
//!   in-process node whose listener lives on) keeps the TTL rule;
//! - a joiner whose peers all confirm they follow its log (`hello`) joins at
//!   once and gets its share right away.

use crate::common::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>, advertise: Option<String>, ttl: Duration, renew: Duration) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: advertise.unwrap_or_else(|| c.public_url.clone()),
            shards: SHARDS,
            ttl,
            renew_every: renew,
            skew: ttl / 5,
            ..Default::default()
        });
    })
    .await
}

fn owned(s: &TestServer) -> usize {
    s.app.partitions.owned().len()
}

async fn wait_for(what: &str, deadline: Duration, f: impl Fn() -> bool) -> Duration {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < deadline, "{what}: not within {deadline:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    t.elapsed()
}

/// An address nothing listens on (bound once, then closed).
async fn refusing_addr() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    format!("http://{a}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_peer_is_taken_over_before_its_ttl() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (ttl, renew) = (Duration::from_secs(6), Duration::from_millis(200));
    let a = node("a", &store, None, ttl, renew).await;
    // b advertises an address that refuses connections: once it stops
    // renewing, a's probe finds nobody there
    let b = node("b", &store, Some(refusing_addr().await), ttl, renew).await;
    wait_for("b gets its share", Duration::from_secs(10), || owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize).await;
    b.app.node.halt();
    let took = wait_for("a takes b's shards", Duration::from_secs(4), || owned(&a) == SHARDS as usize).await;
    assert!(took < ttl / 2, "takeover after {took:?} (TTL {ttl:?})");
    eprintln!("refused peer taken over after {took:?} (TTL {ttl:?})");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepting_peer_keeps_the_ttl_rule() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (ttl, renew) = (Duration::from_secs(3), Duration::from_millis(200));
    let a = node("a", &store, None, ttl, renew).await;
    let b = node("b", &store, None, ttl, renew).await;
    wait_for("b gets its share", Duration::from_secs(10), || owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize).await;
    // a halted in-process node still accepts connections: frozen, not gone
    b.app.node.halt();
    let t = Instant::now();
    tokio::time::sleep(ttl / 2).await;
    assert!(owned(&a) < SHARDS as usize, "taken over {:?} after the halt, before TTL", t.elapsed());
    // TTL + skew after a's last observation of b's renewal (up to a renew
    // interval after it was sent), plus at most one step to notice and the
    // takeover itself
    let bound = ttl + ttl / 5 + renew * 3 + Duration::from_secs(1);
    wait_for("a takes b's shards within TTL + skew", bound, || owned(&a) == SHARDS as usize).await;
    assert!(t.elapsed() >= ttl, "taken over after {:?}", t.elapsed());
    eprintln!("frozen peer taken over after {:?} (TTL {ttl:?})", t.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn greeted_joiner_skips_its_join_grace() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    // join grace = 2 renew intervals = 3 s
    let (ttl, renew) = (Duration::from_secs(10), Duration::from_millis(1500));
    let a = node("a", &store, None, ttl, renew).await;
    assert_eq!(owned(&a), SHARDS as usize);
    let t = Instant::now();
    let b = node("b", &store, None, ttl, renew).await;
    // a hands b its share at a's next step (<= one renew interval), not
    // after b's join grace plus a's step
    let took = wait_for("b gets its share", Duration::from_secs(6), || owned(&b) == SHARDS as usize / 2).await;
    assert!(took < renew * 2, "b served its share {:?} after joining (join grace {:?})", t.elapsed(), renew * 2);
    eprintln!("joiner served its share {took:?} after joining");
}
