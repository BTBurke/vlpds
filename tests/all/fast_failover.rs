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

async fn node(
    id: &str,
    store: &Arc<dyn object_store::ObjectStore>,
    advertise: Option<String>,
    ttl: Duration,
    renew: Duration,
) -> TestServer {
    cluster_node(id, store.clone(), SHARDS, |c| {
        let l = lease(c);
        (l.ttl, l.renew_every, l.skew) = (ttl, renew, ttl / 5);
        if let Some(a) = advertise {
            l.addr = a;
        }
    })
    .await
}

fn owned(s: &TestServer) -> usize {
    s.app.partitions.owned().len()
}

/// An address nothing listens on (bound once, then closed).
async fn refusing_addr() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    format!("https://{a}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_peer_is_taken_over_before_its_ttl() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (ttl, renew) = (Duration::from_secs(6), Duration::from_millis(200));
    let a = node("a", &store, None, ttl, renew).await;
    // b advertises an address that refuses connections: once it stops
    // renewing, a's probe finds nobody there
    let b = node("b", &store, Some(refusing_addr().await), ttl, renew).await;
    wait_until("b gets its share", Duration::from_secs(10), || {
        owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize
    })
    .await;
    b.app.node.halt();
    let took = wait_until("a takes b's shards", Duration::from_secs(4), || owned(&a) == SHARDS as usize).await;
    assert!(took < ttl / 2, "takeover after {took:?} (TTL {ttl:?})");
    eprintln!("refused peer taken over after {took:?} (TTL {ttl:?})");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepting_peer_keeps_the_ttl_rule() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (ttl, renew) = (Duration::from_secs(3), Duration::from_millis(200));
    let a = node("a", &store, None, ttl, renew).await;
    let b = node("b", &store, None, ttl, renew).await;
    wait_until("b gets its share", Duration::from_secs(10), || {
        owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize
    })
    .await;
    // a halted in-process node still accepts connections: frozen, not gone
    b.app.node.halt();
    let t = Instant::now();
    tokio::time::sleep(ttl / 2).await;
    assert!(owned(&a) < SHARDS as usize, "taken over {:?} after the halt, before TTL", t.elapsed());
    // TTL + skew after a's last observation of b's renewal (up to a renew
    // interval after it was sent), plus at most one step to notice and the
    // takeover itself
    let bound = ttl + ttl / 5 + renew * 3 + Duration::from_secs(1);
    wait_until("a takes b's shards within TTL + skew", bound, || owned(&a) == SHARDS as usize).await;
    assert!(t.elapsed() >= ttl, "taken over after {:?}", t.elapsed());
    eprintln!("frozen peer taken over after {:?} (TTL {ttl:?})", t.elapsed());
}

type Hook = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

/// An in-memory store that can hold GETs of one node's lease: an armed
/// hook catches the next such GET after its data was read, reports it, and
/// answers only once released. So a test can order a peer's delete of the
/// lease against a restart's read and CAS of it exactly.
#[derive(Debug)]
struct LeaseHooks {
    inner: object_store::memory::InMemory,
    lease: &'static str,
    hooks: parking_lot::Mutex<std::collections::VecDeque<Hook>>,
}

impl LeaseHooks {
    /// Catches the next GET of the lease not caught by an earlier hook:
    /// (fires once it has read the lease, releases its answer).
    fn arm(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let ((reached_tx, reached_rx), (go_tx, go_rx)) =
            (tokio::sync::oneshot::channel(), tokio::sync::oneshot::channel());
        self.hooks.lock().push_back((reached_tx, go_rx));
        (reached_rx, go_tx)
    }

    async fn lease_exists(&self) -> bool {
        use futures::StreamExt;
        use object_store::ObjectStore;
        self.inner
            .list(None)
            .any(|m| std::future::ready(m.is_ok_and(|m| m.location.as_ref().ends_with(self.lease))))
            .await
    }
}

impl std::fmt::Display for LeaseHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeaseHooks")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for LeaseHooks {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let r = self.inner.get_opts(location, options).await;
        if location.as_ref().ends_with(self.lease) {
            let hook = self.hooks.lock().pop_front();
            if let Some((reached, go)) = hook {
                let _ = reached.send(());
                let _ = go.await;
            }
        }
        r
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A node restarted under the same id, again and again, while its peer
/// takes each dead incarnation over through the refused-probe fast path:
/// presumed dead, log fenced, shards taken, and a step later its lease
/// deleted (re-read first). The peer's delete races the restart's join
/// (read the old lease, fence its log, CAS over it):
/// - landing between the join's read and its CAS, it failed the start with
///   "precondition failure for path nodes/r: not found"; the join now
///   reads the lease again and creates it;
/// - landing after the CAS (the peer's re-read saw the old lease), it
///   deleted the new incarnation's lease, and its first renewal
///   fail-stopped the process; the renewal now recreates a lease missing
///   over an unfenced log.
///
/// Plain restarts at varying phases of the takeover go in between. Every
/// incarnation must start, renew and get its share.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_id_restarts_race_the_refused_probe_takeover() {
    let hooks = Arc::new(LeaseHooks {
        inner: object_store::memory::InMemory::new(),
        lease: "nodes/r",
        hooks: Default::default(),
    });
    let store: Arc<dyn object_store::ObjectStore> = hooks.clone();
    let (ttl, renew) = (Duration::from_secs(3), Duration::from_millis(400));
    let events = |e: &str| vlpds::metrics::LEASE_EVENTS.with_label_values(&[e]).get();
    let (moved0, recreated0) = (events("join_lease_moved"), events("lease_recreated"));
    let a = node("a", &store, None, ttl, renew).await;
    // each incarnation advertises an address that refuses connects, so a
    // presumes it dead within ~2 renew intervals of its halt
    let restart = |store: Arc<dyn object_store::ObjectStore>| async move {
        node("r", &store, Some(refusing_addr().await), ttl, renew).await
    };
    let mut r = restart(store.clone()).await;
    let mut dead = Vec::new();
    for round in 0..9u32 {
        wait_until("r gets its share", Duration::from_secs(15), || owned(&r) > 0).await;
        r.app.node.halt();
        let next = match round % 3 {
            0 => {
                tokio::time::sleep(Duration::from_millis(400) * round).await;
                restart(store.clone()).await
            }
            race => {
                // a's last read of the live lease has landed; its next one
                // is step 7's re-read before deleting the dead lease
                tokio::time::sleep(renew).await;
                let (a_read, a_go) = hooks.arm();
                tokio::time::timeout(Duration::from_secs(10), a_read).await.expect("a deletes the dead lease").unwrap();
                if race == 1 {
                    // the delete lands between the join's read and its CAS
                    let (r_read, r_go) = hooks.arm();
                    let join = tokio::spawn(restart(store.clone()));
                    tokio::time::timeout(Duration::from_secs(10), r_read)
                        .await
                        .expect("the join reads its lease")
                        .unwrap();
                    let moved = events("join_lease_moved");
                    a_go.send(()).unwrap();
                    retry("a deletes the old lease", || async { (!hooks.lease_exists().await).then_some(()) }).await;
                    r_go.send(()).unwrap();
                    let n = join.await.unwrap();
                    assert!(events("join_lease_moved") > moved, "the join met the vanished lease");
                    n
                } else {
                    // the delete lands after the join's CAS
                    let recreated = events("lease_recreated");
                    let n = restart(store.clone()).await;
                    a_go.send(()).unwrap();
                    wait_until("the renewal recreates the deleted lease", Duration::from_secs(5), || {
                        events("lease_recreated") > recreated
                    })
                    .await;
                    n
                }
            }
        };
        dead.push(std::mem::replace(&mut r, next));
    }
    wait_until("the last incarnation and a split the shards", Duration::from_secs(15), || {
        owned(&r) > 0 && owned(&a) + owned(&r) == SHARDS as usize
    })
    .await;
    // still renewing well past a step (a lost lease would have exited)
    tokio::time::sleep(renew * 4).await;
    assert!(r.app.cluster.as_ref().unwrap().lease_valid());
    assert!(hooks.lease_exists().await);
    eprintln!(
        "lease vanished under a join {}x, recreated at a renewal {}x",
        events("join_lease_moved") - moved0,
        events("lease_recreated") - recreated0
    );
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
    let took = wait_until("b gets its share", Duration::from_secs(6), || owned(&b) == SHARDS as usize / 2).await;
    assert!(took < renew * 2, "b served its share {:?} after joining (join grace {:?})", t.elapsed(), renew * 2);
    eprintln!("joiner served its share {took:?} after joining");
}
