//! Security controls across a kill -9 takeover under authenticated load.
//! The dead node's shards are unreadable until a survivor's view of its
//! lease expires and it reopens them, longer than a security-controls
//! check waits for a moving shard; the entry node resends writes and
//! queries answered ShardMoved / RepoLoading / refused (`forward::
//! with_retries`), so clients see latency rather than 503s, and a revoked
//! session or a taken-down record still never gets through.

use crate::common::*;
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 8;
const ACCOUNTS: usize = 48;
/// Longer than a forwarded (2.5 s) or direct (3 s) ctl wait for a move.
const TTL: Duration = Duration::from_secs(4);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Write,
    Read,
    List,
    TakenDown,
    Revoked,
}

struct Sample {
    at: Instant,
    kind: Kind,
    status: u16,
    error: String,
}

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>, advertise: Option<String>) -> TestServer {
    cluster_node(id, store.clone(), SHARDS, |c| {
        let l = lease(c);
        (l.ttl, l.skew) = (TTL, TTL / 5);
        if let Some(a) = advertise {
            l.addr = a;
        }
    })
    .await
}

/// A TCP relay in front of a node's peer listener, so the node can die as
/// a process would: once killed, its connections drop and new ones are
/// accepted (so the lease rule, not the refused-connection probe, decides
/// the takeover) and closed before the TLS handshake, which peers see as a
/// failed connect (nothing sent).
struct Front {
    url: String,
    target: tokio::sync::watch::Sender<Option<std::net::SocketAddr>>,
    dead: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Front {
    async fn bind() -> Front {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", l.local_addr().unwrap());
        let (target, rx) = tokio::sync::watch::channel(None);
        let dead = Arc::new(AtomicBool::new(false));
        let conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Default::default();
        let (d, cs) = (dead.clone(), conns.clone());
        tokio::spawn(async move {
            while let Ok((mut down, _)) = l.accept().await {
                if d.load(Ordering::Acquire) {
                    continue;
                }
                let mut rx = rx.clone();
                cs.lock().push(tokio::spawn(async move {
                    let Ok(to) = rx.wait_for(Option::is_some).await.map(|t| t.unwrap()) else { return };
                    if let Ok(mut up) = tokio::net::TcpStream::connect(to).await {
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    }
                }));
            }
        });
        Front { url, target, dead, conns }
    }

    fn point_at(&self, peer_url: &str) {
        self.target.send_replace(Some(peer_url.trim_start_matches("https://").parse().unwrap()));
    }

    fn kill(&self) {
        self.dead.store(true, Ordering::Release);
        for c in self.conns.lock().drain(..) {
            c.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn kill9_takeover_under_authenticated_load() {
    takeover_under_load(Duration::ZERO).await;
}

/// Store reads slow enough after the kill that the survivors open the dead
/// node's shards over seconds, past the security-controls wait: the
/// checks answer ShardMoved, which entry nodes resend.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn kill9_slow_reopen_under_authenticated_load() {
    takeover_under_load(Duration::from_millis(100)).await;
}

async fn takeover_under_load(read_latency: Duration) {
    let throttled = Arc::new(ThrottledStore::new(InMemory::new(), ThrottleConfig::default()));
    let store: Arc<dyn object_store::ObjectStore> = throttled.clone();
    let a = Arc::new(node("ck-a", &store, None).await);
    let b = Arc::new(node("ck-b", &store, None).await);
    let front = Front::bind().await;
    let c = node("ck-c", &store, Some(front.url.clone())).await;
    front.point_at(&c.peer_url);
    balanced(&[&*a, &*b, &c]).await;

    let accts: Arc<Vec<TestAccount>> =
        Arc::new(futures::future::join_all((0..ACCOUNTS).map(|i| [&*a, &*b, &c][i % 3].create_account("ck"))).await);
    let on_c = |x: &TestAccount| c.app.partitions.for_key(&x.did).is_some();
    let dying = accts.iter().filter(|x| on_c(x)).count();
    assert!(dying >= 4, "only {dying} accounts on the node that dies");
    let victim = accts.iter().find(|x| on_c(x)).unwrap();
    let rec = c.create_record(victim, "app.bsky.feed.post", post_record("taken down before the kill")).await;
    let sess = a.create_session(&victim.handle, PASSWORD).await.ok();
    let revoked = sess["accessJwt"].as_str().unwrap().to_string();
    a.xrpc
        .post_empty("com.atproto.server.deleteSession", &Auth::Bearer(sess["refreshJwt"].as_str().unwrap().into()))
        .await
        .ok();
    b.xrpc
        .post(
            "com.atproto.admin.updateSubjectStatus",
            &json!({"subject": {"$type": "com.atproto.repo.strongRef", "uri": rec.uri, "cid": rec.cid}, "takedown": {"applied": true}}),
            &Auth::Admin,
        )
        .await
        .ok();

    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    // a kind per worker: one stuck resending a write must not hold up reads
    let workers: Vec<_> = (0..20)
        .map(|w| {
            let (a, b, accts, samples, stop, rec, revoked) =
                (a.clone(), b.clone(), accts.clone(), samples.clone(), stop.clone(), rec.clone(), revoked.clone());
            tokio::spawn(async move {
                let mut i = w * 7;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let n = if i % 2 == 0 { &a } else { &b };
                    let acct = &accts[rand::random::<usize>() % accts.len()];
                    let (kind, r) = match w % 5 {
                        0 => {
                            let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record("under load")});
                            (Kind::Write, n.xrpc.post("com.atproto.repo.createRecord", &body, &acct.auth()).await)
                        }
                        1 => (Kind::Read, n.xrpc.get("com.atproto.server.getSession", &[], &acct.auth()).await),
                        2 => {
                            let q = [("repo", acct.did.as_str()), ("collection", "app.bsky.feed.post"), ("limit", "5")];
                            (Kind::List, n.xrpc.get("com.atproto.repo.listRecords", &q, &acct.auth()).await)
                        }
                        3 => {
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
    let killed = Instant::now();
    throttled.config_mut(|c| {
        c.wait_get_per_call = read_latency;
        c.wait_list_per_call = read_latency;
    });
    c.app.node.halt();
    front.kill();
    balanced(&[&*a, &*b]).await;
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
    let since = |s: &Sample| s.at.saturating_duration_since(killed);
    eprintln!(
        "{read_latency:?} reads: {} requests ({} writes, {} reads, {} lists); takeover took {:?}; 503s: {}",
        samples.len(),
        count(Kind::Write),
        count(Kind::Read),
        count(Kind::List),
        settled - killed,
        unavailable.len()
    );
    for l in vlpds::metrics::render().lines().filter(|l| {
        l.starts_with("vlpds_security_ctl_loads_total")
            || l.starts_with("vlpds_write_retries_total")
            || l.starts_with("vlpds_read_retries_total")
    }) {
        eprintln!("  {l}");
    }
    if let Some(s) = unavailable.last() {
        eprintln!("  last 503 at +{:?}", since(s));
    }
    for s in unavailable.iter().take(20) {
        eprintln!("  503 {:?} at +{:?}: {}", s.kind, since(s), s.error);
    }
    assert!(settled - killed >= TTL / 2, "taken over before the lease could expire: {:?}", settled - killed);
    assert!(count(Kind::Write) > 50 && count(Kind::Read) > 50, "load ran through the takeover");

    for s in samples.iter() {
        match s.kind {
            Kind::TakenDown => assert_ne!(s.status, 200, "taken-down record served at +{:?}", since(s)),
            Kind::Revoked => assert_ne!(s.status, 200, "revoked session accepted at +{:?}", since(s)),
            _ => {}
        }
    }
    // a request that had reached the dead node when it died may fail;
    // anything after is resent until a survivor serves it
    let late: Vec<String> = unavailable
        .iter()
        .filter(|s| since(s) > Duration::from_millis(200))
        .map(|s| format!("{:?} +{:?} {}", s.kind, since(s), s.error))
        .collect();
    assert!(late.len() <= 3, "{} 503s during the takeover: {late:?}", late.len());

    for n in [&*a, &*b] {
        let q = [("repo", rec.did()), ("collection", rec.collection()), ("rkey", rec.rkey())];
        let r = n.xrpc.get("com.atproto.repo.getRecord", &q, &Auth::None).await;
        assert_eq!(r.error_name(), Some("RecordNotFound"), "{r:?}");
        let r = n.xrpc.get("com.atproto.server.getSession", &[], &Auth::Bearer(revoked.clone())).await;
        assert_eq!(r.error_name(), Some("ExpiredToken"), "{r:?}");
        n.xrpc.get("com.atproto.server.getSession", &[], &victim.auth()).await.ok();
    }
}
