//! Session revocations and takedowns across planned shard moves under
//! authenticated load. Every request reads the caller's security controls
//! (`xrpc::server::ctl`), which fail closed; a move must not turn that into
//! 503s (a write is resent by its entry node once the controls say
//! ShardMoved, a view from before the move stands in), nor let a takedown
//! or revocation made just before the move lapse.

use crate::common::*;
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 8;
const ACCOUNTS: usize = 96;
/// One more account in use every this many ms.
const UNLOCK_MS: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Write,
    Read,
    /// The taken-down record, read with another account's session (whose
    /// controls the record's owner reads from that account's owner).
    TakenDown,
    Revoked,
}

struct Sample {
    at: Instant,
    kind: Kind,
    status: u16,
    error: String,
}

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
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shard_moves_under_authenticated_load() {
    moves_under_load(Duration::ZERO).await;
}

/// Reads (and lists) slow enough during the moves that a joiner opens a
/// shard over seconds, longer than a forwarded request may wait. (Failing
/// closed after a 3 s retry used to fail writes here as Unavailable, which
/// entry nodes don't resend.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn slow_shard_opens_under_authenticated_load() {
    moves_under_load(Duration::from_millis(250)).await;
}

async fn moves_under_load(read_latency: Duration) {
    let throttled = Arc::new(ThrottledStore::new(InMemory::new(), ThrottleConfig::default()));
    let store: Arc<dyn object_store::ObjectStore> = throttled.clone();
    let a = Arc::new(node("cm-a", &store).await);
    let b = Arc::new(node("cm-b", &store).await);
    let c = node("cm-c", &store).await;
    balanced(&[&*a, &*b, &c]).await;

    // most accounts are first used while shards move: no node has their
    // controls cached then
    let accts: Arc<Vec<TestAccount>> =
        Arc::new(futures::future::join_all((0..ACCOUNTS).map(|i| [&*a, &*b, &c][i % 3].create_account("cm"))).await);
    let victim = &accts[0];
    let rec = b.create_record(victim, "app.bsky.feed.post", post_record("taken down before the move")).await;
    // a second session, ended, and the record taken down: just before the moves
    let sess = a.create_session(&victim.handle, PASSWORD).await.ok();
    let revoked = sess["accessJwt"].as_str().unwrap().to_string();
    a.xrpc.post_empty("com.atproto.server.deleteSession", &Auth::Bearer(sess["refreshJwt"].as_str().unwrap().into())).await.ok();
    b.xrpc
        .post(
            "com.atproto.admin.updateSubjectStatus",
            &json!({"subject": {"$type": "com.atproto.repo.strongRef", "uri": rec.uri, "cid": rec.cid}, "takedown": {"applied": true}}),
            &Auth::Admin,
        )
        .await
        .ok();

    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let start = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..16)
        .map(|w| {
            let (a, b, accts, samples, stop, rec, revoked) =
                (a.clone(), b.clone(), accts.clone(), samples.clone(), stop.clone(), rec.clone(), revoked.clone());
            tokio::spawn(async move {
                let mut i = w * 7;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let n = if i % 2 == 0 { &a } else { &b };
                    let used = (4 + start.elapsed().as_millis() as usize / UNLOCK_MS).min(ACCOUNTS);
                    let acct = &accts[rand::random::<usize>() % used];
                    let (kind, r) = match i % 4 {
                        0 => {
                            let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record("under load")});
                            (Kind::Write, n.xrpc.post("com.atproto.repo.createRecord", &body, &acct.auth()).await)
                        }
                        1 => (Kind::Read, n.xrpc.get("com.atproto.server.getSession", &[], &acct.auth()).await),
                        2 => {
                            let q = [("repo", rec.did()), ("collection", rec.collection()), ("rkey", rec.rkey())];
                            (Kind::TakenDown, n.xrpc.get("com.atproto.repo.getRecord", &q, &acct.auth()).await)
                        }
                        _ => (Kind::Revoked, n.xrpc.get("com.atproto.server.getSession", &[], &Auth::Bearer(revoked.clone())).await),
                    };
                    let error = format!("{}: {}", r.error_name().unwrap_or_default(), r.json["message"].as_str().unwrap_or_default());
                    samples.lock().push(Sample { at: Instant::now(), kind, status: r.status, error });
                }
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let moves = Instant::now();
    throttled.config_mut(|c| {
        c.wait_get_per_call = read_latency;
        c.wait_list_per_call = read_latency;
    });
    // a joiner takes shards from all three, then c leaves and hands its
    // shards to the rest: planned moves, with a and b serving throughout
    let d = node("cm-d", &store).await;
    balanced(&[&*a, &*b, &c, &d]).await;
    vlpds::server::shutdown(&c.app).await;
    balanced(&[&*a, &*b, &d]).await;
    let settled = Instant::now();
    throttled.config_mut(|c| *c = ThrottleConfig::default());
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        w.await.unwrap();
    }

    let samples = std::mem::take(&mut *samples.lock());
    let count = |k: Kind| samples.iter().filter(|s| s.kind == k).count();
    let unavailable: Vec<&Sample> = samples.iter().filter(|s| s.status == 503).collect();
    let since = |s: &Sample| s.at.saturating_duration_since(moves);
    eprintln!(
        "{read_latency:?} reads: {} requests ({} writes, {} reads); moves took {:?}; 503s: {}",
        samples.len(),
        count(Kind::Write),
        count(Kind::Read),
        settled - moves,
        unavailable.len()
    );
    for l in vlpds::metrics::render().lines().filter(|l| l.starts_with("vlpds_security_ctl_loads_total") || l.starts_with("vlpds_write_retries_total")) {
        eprintln!("  {l}");
    }
    for s in unavailable.iter().take(20) {
        eprintln!("  503 {:?} at +{:?}: {}", s.kind, since(s), s.error);
    }
    assert!(count(Kind::Write) > 50 && count(Kind::Read) > 50, "load ran through the moves");

    for s in samples.iter() {
        match s.kind {
            Kind::TakenDown => assert_ne!(s.status, 200, "taken-down record served at +{:?}", since(s)),
            Kind::Revoked => assert_ne!(s.status, 200, "revoked session accepted at +{:?}", since(s)),
            _ => {}
        }
    }
    let failed_writes: Vec<&String> = unavailable.iter().filter(|s| s.kind == Kind::Write).map(|s| &s.error).collect();
    assert!(failed_writes.is_empty(), "writes failed during the moves: {failed_writes:?}");
    // reads may still find the account itself mid-move, only while shards move
    for s in &unavailable {
        assert!(s.at >= moves && s.at <= settled + Duration::from_millis(500), "503 outside the moves (+{:?}): {}", since(s), s.error);
    }

    // right after: enforced on every node, the new owners reading their
    // own partitions
    for n in [&*a, &*b, &d] {
        let q = [("repo", rec.did()), ("collection", rec.collection()), ("rkey", rec.rkey())];
        let r = n.xrpc.get("com.atproto.repo.getRecord", &q, &Auth::None).await;
        assert_eq!(r.error_name(), Some("RecordNotFound"), "{r:?}");
        let r = n.xrpc.get("com.atproto.server.getSession", &[], &Auth::Bearer(revoked.clone())).await;
        assert_eq!(r.error_name(), Some("ExpiredToken"), "{r:?}");
        n.xrpc.get("com.atproto.server.getSession", &[], &victim.auth()).await.ok();
    }
}
