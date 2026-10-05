//! A write that was applied is never answered as refused (4xx) or as
//! "nothing done" (503 ShardMoved / RepoLoading, which the entry node
//! resends). The spaces fault run on spaces-2 47dc3a5c found one: a space
//! putRecord answered 401 `invalid_dpop_proof` whose value the host held.
//! A node exiting on SIGTERM dropped the answer of a write it had forwarded
//! (and its owner applied), and the run's load balancer, which resends a
//! request whose connection failed, sent it again with its spent DPoP
//! proof.
//!
//! - [`no_write_is_applied_and_refused_through_restarts`]: OAuth writes,
//!   public and space, through such a balancer while nodes shut down
//!   gracefully one after another.
//! - [`drain_answers_requests_in_flight`]: the drain itself.
//! - [`space_write_failing_after_it_applied_is_unknown`]: a failure after
//!   the ack (the authority's served-hash push) is a 500, not ShardMoved.

use super::cluster::{client_on, fronted_node, Plc};
use super::durability::{create, scope};
use super::hooks::*;
use super::phase3::takedown_record;
use crate::common::spaces::SpaceClient;
use crate::common::*;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 6;

fn tag() -> String {
    random_bytes(5).iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug)]
enum Outcome {
    Acked,
    Refused(u16, String),
    Unknown(String),
}

/// What a client sees of one write: a fresh DPoP proof per attempt, sent
/// again only for a new nonce (`use_dpop_nonce`). In front of the nodes, a
/// round-robin balancer that resends the same bytes to the next live node
/// when a connection fails before its answer (as the spaces harness's and
/// tests/E2E.md's balancers do).
struct Balancer {
    nodes: RwLock<Vec<String>>,
    next: AtomicUsize,
    http: reqwest::Client,
    public: String,
}

impl Balancer {
    async fn write(&self, sc: &SpaceClient, nsid: &str, body: &J) -> Outcome {
        let htu = format!("{}/xrpc/{nsid}", self.public);
        let bytes = serde_json::to_vec(body).unwrap();
        for _ in 0..3 {
            let proof = sc.key.proof("POST", &htu, Some(&sc.access));
            let Some(r) = self.send(nsid, &proof, &sc.access, &bytes).await else {
                return Outcome::Unknown("no node answered".into());
            };
            sc.key.update_nonce(r.headers());
            let status = r.status().as_u16();
            let j: J = r.json().await.unwrap_or(J::Null);
            let error = j["error"].as_str().unwrap_or_default().to_string();
            match status {
                200 => return Outcome::Acked,
                401 if error == "use_dpop_nonce" => continue,
                400..=499 => return Outcome::Refused(status, format!("{error}: {}", j["message"])),
                _ => return Outcome::Unknown(format!("{status} {error}")),
            }
        }
        Outcome::Unknown("no DPoP nonce accepted".into())
    }

    async fn send(&self, nsid: &str, proof: &str, access: &str, body: &[u8]) -> Option<reqwest::Response> {
        let nodes = self.nodes.read().clone();
        let first = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..nodes.len() {
            let n = &nodes[(first + i) % nodes.len()];
            let r = self
                .http
                .post(format!("{n}/xrpc/{nsid}"))
                .header("authorization", format!("DPoP {access}"))
                .header("dpop", proof)
                .header("content-type", "application/json")
                .body(body.to_vec())
                .send()
                .await;
            if let Ok(r) = r {
                return Some(r);
            }
        }
        None
    }
}

/// One write's record: `nsid` put `rkey` (a fresh one per write).
struct Write {
    space: Option<String>,
    collection: String,
    rkey: String,
    outcome: Outcome,
}

async fn write_loop(
    lb: &Balancer,
    sc: &SpaceClient,
    space: &str,
    coll: &str,
    post: &str,
    stop: &AtomicBool,
    prefix: String,
) -> Vec<Write> {
    let mut out = Vec::new();
    let mut i = 0;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        let rkey = format!("{prefix}{i}");
        let record = json!({"$type": coll, "text": format!("{prefix} {i}"), "createdAt": now_iso()});
        let in_space = i % 2 == 0;
        let create = i % 3 == 0;
        let (nsid, body, sp, c) = match (in_space, create) {
            (true, true) => (
                "com.atproto.space.createRecord",
                json!({"space": space, "repo": sc.did, "collection": coll, "rkey": rkey, "record": record}),
                Some(space.to_string()),
                coll.to_string(),
            ),
            (true, false) => (
                "com.atproto.space.putRecord",
                json!({"space": space, "repo": sc.did, "collection": coll, "rkey": rkey, "record": record}),
                Some(space.to_string()),
                coll.to_string(),
            ),
            (false, create) => {
                let record = json!({"$type": post, "text": format!("{prefix} {i}"), "createdAt": now_iso()});
                let nsid = if create { "com.atproto.repo.createRecord" } else { "com.atproto.repo.putRecord" };
                let body = json!({"repo": sc.did, "collection": post, "rkey": rkey, "record": record});
                (nsid, body, None, post.to_string())
            }
        };
        let outcome = lb.write(sc, nsid, &body).await;
        out.push(Write { space: sp, collection: c, rkey, outcome });
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    out
}

/// Whether `w` is there, read through the front (pointed at a live node).
async fn present(sc: &SpaceClient, n: &TestServer, w: &Write) -> bool {
    let r = match &w.space {
        Some(space) => {
            let q = [
                ("space", space.as_str()),
                ("repo", sc.did.as_str()),
                ("collection", &w.collection),
                ("rkey", &w.rkey),
            ];
            sc.get("com.atproto.space.getRecord", &q).await
        }
        None => n.get_record(&sc.did, &w.collection, &w.rkey).await,
    };
    match r.status {
        200 => true,
        400 | 404 if r.json["error"] == "RecordNotFound" => false,
        s => panic!("reading {}: {s} {}", w.rkey, r.text()),
    }
}

/// OAuth writes (repo and space, create and put) through a resending
/// balancer while each of three nodes shuts down gracefully (SIGTERM as
/// `main` runs it) and a fresh node joins: no write is both applied and
/// refused, and every acked one is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_write_is_applied_and_refused_through_restarts() {
    let (bucket, front, plc) = (Arc::default(), Front::new().await, Plc::start().await);
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(fronted_node(&format!("ar-{i}"), &bucket, SHARDS, &plc, &front).await);
    }
    balanced(&nodes.iter().collect::<Vec<_>>()).await;
    let t = tag();
    let (st, coll) = (format!("com.example.ar{t}.space"), format!("com.example.ar{t}.note"));
    let post = format!("com.example.ar{t}.post");
    let sc = format!("transition:generic {}", scope(&st, &coll));
    let mut clients = Vec::new();
    for i in 0..9 {
        let c = client_on(&front, &nodes[i % 3], "ar", &sc).await;
        let space = c.create_space(&st, &format!("ar{i}")).await;
        clients.push((c, space));
    }
    let lb = Balancer {
        nodes: RwLock::new(nodes.iter().map(|n| n.url.clone()).collect()),
        next: AtomicUsize::new(0),
        http: reqwest::Client::new(),
        public: front.url.clone(),
    };
    let stop = AtomicBool::new(false);
    let restarts = async {
        let mut cut = 0;
        for i in 0..3 {
            tokio::time::sleep(Duration::from_millis(700)).await;
            let joined = fronted_node(&format!("ar-{}", i + 3), &bucket, SHARDS, &plc, &front).await;
            lb.nodes.write().push(joined.url.clone());
            let victim = &nodes[i];
            if !vlpds::server::shutdown_gracefully(&victim.app, vlpds::server::SHUTDOWN_GRACE).await {
                cut += 1;
            }
            lb.nodes.write().retain(|u| *u != victim.url);
            nodes.push(joined);
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
        stop.store(true, Ordering::Relaxed);
        cut
    };
    let loops = clients
        .iter()
        .enumerate()
        .map(|(i, (c, space))| write_loop(&lb, c, space, &coll, &post, &stop, format!("c{i}x")));
    let started = Instant::now();
    let (cut, writes) = tokio::join!(restarts, futures::future::join_all(loops));
    eprintln!("applied_writes: workload {} ms", started.elapsed().as_millis());
    assert_eq!(cut, 0, "a drain cut requests in flight");

    let live = &nodes[3..];
    balanced(&live.iter().collect::<Vec<_>>()).await;
    front.point(&live[0]);
    let (mut acked, mut refused, mut unknown) = (0, 0, std::collections::BTreeMap::<String, usize>::new());
    let mut bad = Vec::new();
    for ((c, _), ws) in clients.iter_mut().zip(&writes) {
        c.srv.http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        for w in ws {
            let there = present(c, &live[0], w).await;
            match &w.outcome {
                Outcome::Acked => {
                    acked += 1;
                    if !there {
                        bad.push(format!("{}: acked, missing", w.rkey));
                    }
                }
                Outcome::Refused(s, e) => {
                    refused += 1;
                    if refused < 5 {
                        eprintln!("applied_writes: refused {s} {e}");
                    }
                    if there {
                        bad.push(format!("{}: answered {s} {e}, applied", w.rkey));
                    }
                }
                Outcome::Unknown(why) => *unknown.entry(why.clone()).or_default() += 1,
            }
        }
    }
    eprintln!("applied_writes: {acked} acked, {refused} refused, unknown {unknown:?}");
    assert!(bad.is_empty(), "{} bad: {bad:#?}", bad.len());
    assert!(acked > 300, "too few writes acked to say anything: {acked}");
}

/// A request in flight when the drain starts is answered; the listener
/// closes at once, so a new connection is refused (safe for a balancer to
/// resend elsewhere). One still in flight at the end of the grace is cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_answers_requests_in_flight() {
    for grace in [Duration::from_secs(20), Duration::from_millis(300)] {
        let bucket = Arc::default();
        let store = HookedStore::new(&bucket);
        // no hedged PUT carries the segment past the hold
        let s = cluster_node("dn", store.clone(), 4, |c| c.hedge_after = Duration::from_secs(3600)).await;
        let acct = s.create_account("dn").await;
        let needle = format!("in flight at the drain {}", tag());
        let mut held = store.arm(Stage::BeforePut, Act::Pause, &needle);
        let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record(&needle)});
        let rb = s.xrpc.http.post(format!("{}/xrpc/com.atproto.repo.createRecord", s.url));
        let write = s.xrpc.try_send(rb.bearer_auth(&acct.access).json(&body));
        let drain = async {
            held.wait("the write's segment").await;
            let d = s.app.http_drain.clone();
            let draining = tokio::spawn(async move { d.run(grace).await });
            let t = Instant::now();
            while tokio::net::TcpStream::connect(s.addr).await.is_ok() {
                assert!(t.elapsed() < Duration::from_secs(5), "the listener still accepts");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // past the grace when it is short
            tokio::time::sleep((grace * 3).min(Duration::from_secs(1))).await;
            drop(held);
            draining.await.unwrap()
        };
        let (r, clean) = tokio::join!(write, drain);
        eprintln!("drain grace {grace:?}: clean {clean}, write {:?}", r.as_ref().map(|r| r.status));
        if grace > Duration::from_secs(1) {
            assert!(clean, "nothing was left to cut");
            assert_eq!(r.expect("answered").status, 200);
        } else {
            assert!(!clean, "the write outlived the grace");
            assert!(r.is_err(), "cut: {:?}", r.map(|r| r.status));
        }
    }
}

/// The authority's write whose records are partly taken down pushes its
/// served hash after the ack. Its shard leaving in between fails that push:
/// the answer is a 500 (outcome unknown), not ShardMoved, which the entry
/// node would resend (a createRecord then refused RecordAlreadyExists).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn space_write_failing_after_it_applied_is_unknown() {
    let bucket = Arc::default();
    let store = HookedStore::new(&bucket);
    let s = cluster_node("aa", store.clone(), 4, |c| {
        c.spaces = true;
        c.hedge_after = Duration::from_secs(3600);
    })
    .await;
    let t = tag();
    let (st, coll) = (format!("com.example.aa{t}.space"), format!("com.example.aa{t}.note"));
    let auth = SpaceClient::new(&s, "aa", &scope(&st, &coll)).await;
    let space = auth.create_space(&st, "aa").await;
    let r = create(&auth, &space, &coll, "hidden", "taken down").await;
    assert_eq!(r.status, 200, "{r:?}");
    takedown_record(&s, r.json["uri"].as_str().unwrap(), r.json["cid"].as_str().unwrap(), true).await;

    let needle = format!("applied, then its shard left {t}");
    let held = store.arm(Stage::AfterPut, Act::Pause, &needle);
    let shard = s.app.partitions.shard_of(&auth.did);
    let app = &s.app;
    let mover = async move {
        let mut held = held;
        held.wait("the write's segment").await;
        let p = app.partitions.get(shard).expect("owned");
        app.partitions.set(shard, None);
        drop(held);
        tokio::time::sleep(Duration::from_millis(500)).await;
        app.partitions.set(shard, Some(p));
    };
    let (r, ()) = tokio::join!(create(&auth, &space, &coll, "after-ack", &needle), mover);
    assert!(r.status >= 500 && r.json["error"] != "ShardMoved", "{r:?}");
    let q = [("space", space.as_str()), ("repo", auth.did.as_str()), ("collection", &coll), ("rkey", "after-ack")];
    let got = auth.get("com.atproto.space.getRecord", &q).await;
    assert_eq!(got.status, 200, "the write was applied: {got:?}");
}
