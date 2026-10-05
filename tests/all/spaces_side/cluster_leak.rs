//! The sentinel leak test at cluster scale (plan §2.6, phase 2): three
//! nodes, two space authors on each and a public writer per node posting
//! throughout, every author filling its space concurrently. No sentinel of
//! any author reaches:
//!
//! - any node's live subscribeRepos, whole or `?shard=k/n`, read while the
//!   load runs;
//! - any node's cursor-0 replay (from segments: the ring is 2 KiB), whole or
//!   per shard, nor the raw S3 backfill;
//! - the peer log stream each node serves its peers (tapped as a follower),
//!   which also carries no empty frame;
//! - any node's public sync and repo surface for any author.
//!
//! Each author's s* state is in its owner's log, in entries with empty
//! frames, and in no other node's log.

use super::cluster::Plc;
use super::leak::{
    check_public_surface, read_shard, read_to_commit, s3_backfill, scan_log, sub_shard, Planted, Sentinels, SHARDS,
};
use crate::common::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NODES: usize = 3;
const AUTHORS_PER_NODE: usize = 2;
const FILL: usize = 8;

fn is_commit(f: &Frame, cid: &Cid) -> bool {
    matches!(f.body.get("commit"), Some(Value::Link(c)) if c == cid)
}

/// Reads `sub` in the background so the load can't push it off the stream:
/// up to the #commit `marker` once it's set, or, for a shard stream whose
/// range doesn't hold it, until 1.5 s pass without a frame after that.
fn collect(mut sub: Sub, marker: Arc<Mutex<Option<Cid>>>) -> tokio::task::JoinHandle<Vec<Frame>> {
    tokio::spawn(async move {
        let mut frames = Vec::new();
        let mut idle_since = None::<Instant>;
        loop {
            let m = marker.lock().clone();
            match sub.next(Duration::from_millis(250)).await {
                Some(f) => {
                    let done = m.as_ref().is_some_and(|c| is_commit(&f, c));
                    frames.push(f);
                    idle_since = None;
                    if done {
                        return frames;
                    }
                }
                None if sub.closed => return frames,
                None if m.is_some() => {
                    let t = *idle_since.get_or_insert_with(Instant::now);
                    if t.elapsed() > Duration::from_millis(1500) {
                        return frames;
                    }
                }
                None => {}
            }
        }
    })
}

/// A follower's view of `n`'s peer log stream: every binary message until
/// `stop`.
async fn tap(n: &TestServer, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<Vec<Vec<u8>>> {
    use futures::StreamExt;
    let id = cluster(n).cfg.node_id.clone();
    let url = format!("{}/internal/v1/log/stream", n.peer_url.replacen("https", "wss", 1));
    let mut req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url.as_str()).unwrap();
    req.headers_mut().insert("x-vlpds-internal", vlpds::server::DEV_INTERNAL_TOKEN.parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(req, None, false, peer_client().ws_connector(&id))
        .await
        .expect("the peer log stream");
    tokio::spawn(async move {
        let mut out = Vec::new();
        while !stop.load(Ordering::SeqCst) {
            match tokio::time::timeout(Duration::from_millis(250), ws.next()).await {
                Ok(Some(Ok(m))) if m.is_binary() => out.push(m.into_data().to_vec()),
                Ok(Some(Ok(_))) | Err(_) => {}
                Ok(Some(Err(e))) => panic!("{id}: peer log stream: {e}"),
                Ok(None) => panic!("{id}: peer log stream closed under load"),
            }
        }
        out
    })
}

/// The (seq, frame) events of a peer stream batch message (0 ‖ ordinal ‖
/// count ‖ (seq ‖ len ‖ frame)*); None for a watermark.
fn batch_events(m: &[u8]) -> Option<Vec<(i64, &[u8])>> {
    if m.first() != Some(&0) {
        return None;
    }
    let n = u32::from_be_bytes(m[9..13].try_into().unwrap()) as usize;
    let (mut at, mut out) = (13, Vec::with_capacity(n));
    for _ in 0..n {
        let seq = i64::from_be_bytes(m[at..at + 8].try_into().unwrap());
        let len = u32::from_be_bytes(m[at + 8..at + 12].try_into().unwrap()) as usize;
        out.push((seq, &m[at + 12..at + 12 + len]));
        at += 12 + len;
    }
    assert_eq!(at, m.len(), "trailing bytes in a peer stream batch");
    Some(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "spaces core: C1"]
async fn space_writes_stay_off_every_stream_of_a_loaded_cluster() {
    let (bucket, plc) = (Arc::new(object_store::memory::InMemory::new()), Plc::start().await);
    let mut nodes = Vec::new();
    for i in 0..NODES {
        nodes.push(
            cluster_node(&format!("lkx-{i}"), bucket.clone(), 6, |c| {
                plc.apply(c);
                c.spaces = true;
                c.firehose_ring_bytes = 2048;
            })
            .await,
        );
    }
    let refs: Vec<&TestServer> = nodes.iter().collect();
    balanced(&refs).await;
    let mut publics = Vec::new();
    for n in &nodes {
        publics.push(n.create_account("lkxp").await);
    }
    let mut planted: Vec<(usize, Planted)> = Vec::new();
    for (i, n) in nodes.iter().enumerate() {
        for _ in 0..AUTHORS_PER_NODE {
            planted.push((i, Planted::new(n).await));
        }
    }

    let marker = Arc::new(Mutex::new(None::<Cid>));
    let mut lives: Vec<Sub> = Vec::new();
    for n in &nodes {
        lives.push(n.subscribe(None).await);
    }
    nodes[0].sync_subs(&publics[0], &mut lives).await;
    let (mut full, mut shards) = (Vec::new(), Vec::new());
    for (n, live) in nodes.iter().zip(lives) {
        let from = n.settled_now().await;
        full.push(collect(live, marker.clone()));
        for k in 0..SHARDS {
            shards.push((cluster(n).cfg.node_id.clone(), k, collect(sub_shard(n, from, k, SHARDS).await, marker.clone())));
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut taps = Vec::new();
    for n in &nodes {
        taps.push((cluster(n).cfg.node_id.clone(), tap(n, stop.clone()).await));
    }

    let public_posts = AtomicUsize::new(0);
    let fills_done = AtomicBool::new(false);
    let public_load = futures::future::join_all(nodes.iter().zip(&publics).map(|(n, a)| {
        let (posts, done) = (&public_posts, &fills_done);
        async move {
            while !done.load(Ordering::SeqCst) {
                n.post(a, "public load").await;
                posts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }));
    let fills = async {
        futures::future::join_all(planted.iter_mut().map(|(i, p)| p.fill(&nodes[*i], &publics[*i], FILL))).await;
        fills_done.store(true, Ordering::SeqCst);
    };
    tokio::join!(public_load, fills);
    let writes: usize = planted.iter().map(|(_, p)| p.writes).sum();
    eprintln!("{writes} space write calls, {} public posts under them", public_posts.load(Ordering::Relaxed));

    let mut all = Sentinels::default();
    for (_, p) in &planted {
        all.extend(&p.sentinels);
    }
    let last = Cid::parse(nodes[0].post(&planted[0].1.author, "public after").await.commit_cid.as_deref().unwrap()).unwrap();
    *marker.lock() = Some(last);

    let mut fulls = Vec::new();
    for (n, h) in nodes.iter().zip(full) {
        let id = &cluster(n).cfg.node_id;
        let fs = h.await.unwrap();
        assert!(fs.last().is_some_and(|f| is_commit(f, &last)), "{id}: the live stream never reached the marker");
        assert!(fs.len() > writes, "{id}: live stream: {} frames under {writes} space writes", fs.len());
        all.assert_frames_clean(&format!("{id} live subscribeRepos"), &fs);
        fulls.push(fs);
    }
    let mut sharded = 0;
    for (id, k, h) in shards {
        let fs = h.await.unwrap();
        sharded += fs.len();
        all.assert_frames_clean(&format!("{id} live subscribeRepos shard {k}/{SHARDS}"), &fs);
    }
    assert!(sharded > writes, "the shard streams carried {sharded} frames in all");

    for (n, full) in nodes.iter().zip(&fulls) {
        let id = &cluster(n).cfg.node_id;
        let mut replay = n.subscribe(Some(0)).await;
        let fs = read_to_commit(&mut replay, &last).await;
        assert!(!fs.iter().any(|f| f.kind() == "#info"), "{id}: cursor 0 within retention: no OutdatedCursor");
        all.assert_frames_clean(&format!("{id} cursor-0 replay"), &fs);
        for k in 0..SHARDS {
            let mut sub = sub_shard(n, 0, k, SHARDS).await;
            let fs = read_shard(&mut sub, full, k).await;
            all.assert_frames_clean(&format!("{id} cursor-0 replay shard {k}/{SHARDS}"), &fs);
        }
    }
    for (seq, raw) in s3_backfill(&nodes[0]).await {
        all.assert_clean(&format!("S3 backfill seq {seq}"), &raw);
    }

    stop.store(true, Ordering::SeqCst);
    for (id, h) in taps {
        let (mut events, mut empty) = (0, 0);
        for m in h.await.unwrap() {
            all.assert_clean(&format!("{id} peer log stream"), &m);
            for (seq, frame) in batch_events(&m).into_iter().flatten() {
                events += 1;
                if frame.is_empty() {
                    empty += 1;
                    eprintln!("{id}: peer stream event seq {seq} has an empty frame");
                }
            }
        }
        assert!(events > 0, "{id}: the peer log stream carried no events");
        assert_eq!(empty, 0, "{id}: the peer log stream carries frameless (private) entries");
    }

    for (owner, p) in &planted {
        for (i, n) in nodes.iter().enumerate() {
            let (private, seen) = scan_log(n, p).await;
            let id = &cluster(n).cfg.node_id;
            match i == *owner {
                true => assert!(private > p.writes && seen, "{id} (owner): {private} private entries, seen {seen}"),
                false => assert!(!seen, "{id}'s own log carries {}'s space state", p.author.did),
            }
            check_public_surface(n, &p.author.did, p).await;
        }
    }
}
