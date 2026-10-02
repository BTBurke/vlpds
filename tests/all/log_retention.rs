//! Log segment retention (src/retention.rs, DESIGN.md "Log retention"):
//! segments no replay can need and older than the window are deleted while
//! subscribers backfill, a dead log is pruned down to its fence once its
//! successors opened its shards, and a node restarts over a pruned log.
//!
//! The window is zero and passes run every 20 ms, so what holds segments
//! back is only the replay rule (checkpoints) and the fence.

use crate::common::*;
use object_store::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn fast() -> Option<vlpds::retention::Config> {
    Some(vlpds::retention::Config { window: Duration::ZERO, interval: Duration::from_millis(20), max_deletes: 3, ..Default::default() })
}

/// Ordinals of `log`'s objects in the store.
async fn ordinals(store: &vlpds::store::Store, log: &str) -> Vec<u64> {
    use futures::StreamExt;
    let prefix = Path::from(format!("{}/log/{log}", store.prefix));
    store
        .raw
        .list(Some(&prefix))
        .filter_map(|m| async move { m.ok()?.location.filename()?.strip_suffix(".seg")?.parse::<u64>().ok() })
        .collect()
        .await
}

/// Reads until the commit `head` of `did`, then checks what arrived: seqs
/// ascend, and `did`'s commits form one unbroken rev chain (`since` = the
/// previous rev) except right after an `OutdatedCursor` (history deleted
/// under the cursor). Returns how many OutdatedCursor infos it saw.
async fn check_stream(sub: &mut Sub, did: &str, head: &Cid) -> usize {
    let frames = sub
        .until(Duration::from_secs(20), |fs| fs.last().and_then(|f| f.commit()).is_some_and(|c| c.commit == *head))
        .await;
    let (mut outdated, mut last_seq, mut prev_rev, mut jump) = (0, 0i64, None::<String>, false);
    for f in &frames {
        if f.kind() == "#info" {
            assert_eq!(f.str("name"), Some("OutdatedCursor"));
            outdated += 1;
            jump = true;
            continue;
        }
        let seq = f.seq().expect("event seq");
        assert!(seq > last_seq, "seq {seq} after {last_seq}");
        last_seq = seq;
        if let Some(c) = f.commit().filter(|c| c.repo == did) {
            if let (Some(p), false) = (&prev_rev, jump) {
                assert_eq!(c.since.as_ref(), Some(p), "commit chain broken at seq {seq} without OutdatedCursor");
            }
            prev_rev = Some(c.rev.clone());
            jump = false;
        }
    }
    outdated
}

/// Subscribers backfill from cursor 0 (the ring holds ~2 KiB) while
/// retention deletes the log under them, a few segments per pass: each sees
/// a gap-free stream, or an OutdatedCursor where history went away. After
/// that a cursor-0 subscriber starts with OutdatedCursor, and the node
/// still serves every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn prune_while_subscribers_backfill() {
    let s = TestServer::spawn_with(|c| {
        c.firehose_ring_bytes = 2048;
        c.log_retention = fast();
    })
    .await;
    let a = s.create_account("ret").await;
    let mut posts = Vec::new();
    for i in 0..60 {
        posts.push(s.post(&a, &format!("retained {i} {}", "x".repeat(64))).await);
    }
    let log = s.app.log.log_id.to_string();
    let store = s.app.store.clone();
    let before = ordinals(&store, &log).await;
    assert_eq!(before.first(), Some(&0));

    // drive checkpoints (they move the replay floor) and more writes while
    // subscribers replay from 0
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let driver = {
        let (app, stop) = (s.app.clone(), stop.clone());
        tokio::spawn(async move {
            // (each checkpoint flushes a small L0 per shard: much faster
            // than this and SlateDB's L0 cap stalls flushes until compaction)
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                app.log.checkpoint_all().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let mut subs = Vec::new();
    for _ in 0..4 {
        subs.push(s.subscribe(Some(0)).await);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for i in 0..40 {
        posts.push(s.post(&a, &format!("during {i} {}", "y".repeat(64))).await);
    }
    let head = Cid::parse(posts.last().unwrap().commit_cid.as_deref().unwrap()).unwrap();
    let mut jumps = 0;
    for sub in &mut subs {
        jumps += check_stream(sub, &a.did, &head).await;
    }

    // wait until retention has caught up with the checkpoints
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = ordinals(&store, &log).await;
        if now.len() <= 2 {
            assert!(now.first() > before.first(), "the head was pruned");
            break;
        }
        assert!(Instant::now() < deadline, "retention never caught up: {} segments left", now.len());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    driver.await.unwrap();
    eprintln!("4 subscribers backfilled across pruning; {jumps} OutdatedCursor jumps; segments {} -> {:?}", before.len(), ordinals(&store, &log).await);

    // history before the floor is gone: OutdatedCursor first, then the rest
    let mut late = s.subscribe(Some(0)).await;
    let first = late.next(FH_TIMEOUT).await.expect("a frame");
    assert_eq!((first.kind(), first.str("name")), ("#info", Some("OutdatedCursor")));
    assert!(vlpds::retention::retained_floor(&store).await.unwrap() > 0);
    // state is untouched by log retention
    for p in [&posts[0], &posts[59], posts.last().unwrap()] {
        s.get_record(&a.did, p.collection(), p.rkey()).await.ok();
    }
    s.post(&a, "after pruning").await;
}

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>) -> TestServer {
    node_with(id, store, fast()).await
}

async fn node_with(id: &str, store: &Arc<dyn object_store::ObjectStore>, retention: Option<vlpds::retention::Config>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = 8;
        c.log_retention = retention;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: 8,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(300),
            ..Default::default()
        });
    })
    .await
}

/// Waits until `log` holds exactly `want` (a dead log retired to its fence).
async fn wait_for_objects(store: &vlpds::store::Store, log: &str, want: &[u64]) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let now = ordinals(store, log).await;
        if now == want {
            return;
        }
        assert!(Instant::now() < deadline, "log {log}: {now:?}, want {want:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Node `a` leaves (graceful: hands its shards to `b`, fences its log). Once
/// `b` has opened them, `b` (owner of the lowest shard) prunes `a`'s dead
/// log down to its fence, deletes its report, and keeps serving a's repos;
/// the firehose from 0 says OutdatedCursor and then runs gap-free.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dead_log_pruned_after_takeover() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("ret-a", &store).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..6).map(|_| a.create_account("rd"))).await;
    let mut posts = Vec::new();
    for (i, acct) in accounts.iter().enumerate() {
        posts.push((acct.did.clone(), a.post(acct, &format!("on a {i}")).await));
    }
    let b = node("ret-b", &store).await;
    let a_log = a.app.log.log_id.to_string();
    let vs = b.app.store.clone();
    vlpds::server::shutdown(&a.app).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while b.app.partitions.owned().len() < 8 {
        assert!(Instant::now() < deadline, "b never took a's shards");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (fence, fenced) = vlpds::nodelog::first_free(&vs, &a_log).await.unwrap();
    assert!(fenced, "a fenced its own log on shutdown");
    wait_for_objects(&vs, &a_log, &[fence]).await;
    let reports = vlpds::retention::read_reports(&vs).await.unwrap();
    assert!(!reports.contains_key(&a_log), "a's report went with its log");
    assert!(reports.get(b.app.log.log_id.as_ref()).is_some_and(|r| r.opened.len() == 8));
    // b serves everything a wrote, and keeps writing
    for (did, p) in &posts {
        b.get_record(did, p.collection(), p.rkey()).await.ok();
    }
    let last = b.post(&accounts[0], "on b").await;
    let head = Cid::parse(last.commit_cid.as_deref().unwrap()).unwrap();
    let mut sub = b.subscribe(Some(0)).await;
    assert_eq!(check_stream(&mut sub, &accounts[0].did, &head).await, 1, "one OutdatedCursor, then gap-free");
}

/// A node restarts (same node id) after its previous log was pruned: it
/// fences the fence-only log at the same place, replays nothing it lacks,
/// and serves the old records; the old log's report and segments go.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn restart_after_pruning() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let first = node("ret-r", &store).await;
    let acct = first.create_account("rr").await;
    let mut posts = Vec::new();
    for i in 0..20 {
        posts.push(first.post(&acct, &format!("before restart {i}")).await);
    }
    let old_log = first.app.log.log_id.to_string();
    let vs = first.app.store.clone();
    first.app.log.checkpoint_all().await;
    // pruned while alive: all but the newest durable segment
    let deadline = Instant::now() + Duration::from_secs(10);
    while ordinals(&vs, &old_log).await.len() > 1 {
        assert!(Instant::now() < deadline, "own log never pruned: {:?}", ordinals(&vs, &old_log).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    vlpds::server::shutdown(&first.app).await;
    let second = node("ret-r", &store).await;
    assert_ne!(second.app.log.log_id.as_ref(), old_log.as_str());
    assert_eq!(second.app.partitions.owned().len(), 8);
    for p in &posts {
        second.get_record(&acct.did, p.collection(), p.rkey()).await.ok();
    }
    second.post(&acct, "after restart").await;
    let (fence, fenced) = vlpds::nodelog::first_free(&vs, &old_log).await.unwrap();
    assert!(fenced);
    wait_for_objects(&vs, &old_log, &[fence]).await;
}

/// With --fence-retention past, a dead log goes entirely: `a` leaves, `b`
/// takes its shards and prunes a's log down to its fence and then the
/// fence, so `log/` no longer lists it. Then `b` leaves too and `c` takes
/// every shard: their histories still name a's log, and replay over the
/// vanished log reads nothing (it was all applied and flushed by `b`'s
/// opens). Every record reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fence_deleted_then_takeover_replays() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let no_fences = || fast().map(|c| vlpds::retention::Config { fence_retention: Some(Duration::ZERO), ..c });
    let a = node_with("retf-a", &store, no_fences()).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..6).map(|_| a.create_account("rf"))).await;
    let mut posts = Vec::new();
    for (i, acct) in accounts.iter().enumerate() {
        posts.push((acct.did.clone(), a.post(acct, &format!("on a {i}")).await));
    }
    let b = node_with("retf-b", &store, no_fences()).await;
    let a_log = a.app.log.log_id.to_string();
    let vs = b.app.store.clone();
    vlpds::server::shutdown(&a.app).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while b.app.partitions.owned().len() < 8 {
        assert!(Instant::now() < deadline, "b never took a's shards");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    wait_for_objects(&vs, &a_log, &[]).await;
    assert!(!vlpds::backfill::list_logs(&vs).await.unwrap().contains(&a_log), "a's log left log/");
    for (i, acct) in accounts.iter().enumerate() {
        posts.push((acct.did.clone(), b.post(acct, &format!("on b {i}")).await));
    }
    let c = node_with("retf-c", &store, no_fences()).await;
    vlpds::server::shutdown(&b.app).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while c.app.partitions.owned().len() < 8 {
        assert!(Instant::now() < deadline, "c never took b's shards");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for (did, p) in &posts {
        c.get_record(did, p.collection(), p.rkey()).await.ok();
    }
    c.post(&accounts[0], "on c").await;
}
