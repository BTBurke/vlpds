//! Firehose seam at a node's start (bench/ha O1): nodes join a cluster one by
//! one under write load, and every node's subscribeRepos must carry the union
//! of all node logs, each event once, in seq order, with no gaps: replayed
//! from cursor 0 afterwards, consumed from cursor 0 by a subscriber attached
//! the moment the node came up, and (from its first event on) by a cursorless
//! live subscriber attached then.

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u16 = 8;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await
}

/// Collects (seq, raw frame) from a subscription until `target` is set and
/// reached.
fn collect(mut sub: Sub, target: Arc<AtomicI64>) -> tokio::task::JoinHandle<Vec<(i64, Vec<u8>)>> {
    tokio::spawn(async move {
        let mut out: Vec<(i64, Vec<u8>)> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let t = target.load(Ordering::Acquire);
            if t > 0 && out.last().is_some_and(|(s, _)| *s >= t) {
                return out;
            }
            assert!(tokio::time::Instant::now() < deadline, "subscriber never reached {t}; at {:?}", out.last().map(|x| x.0));
            match sub.next(Duration::from_millis(200)).await {
                Some(f) => {
                    assert_ne!(f.kind(), "#info", "unexpected #info frame: {:?}", f.body);
                    if let Some(s) = f.seq() {
                        out.push((s, f.raw));
                    }
                }
                None => assert!(!sub.closed, "subscription closed"),
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn staggered_starts_under_load_lose_no_events() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("fs-a", &store).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..6).map(|_| a.create_account("fs"))).await;

    // writers: each account posts as fast as it can through node a (forwarded
    // to the owner); only acked commits count
    let stop = Arc::new(AtomicBool::new(false));
    let acked: Arc<parking_lot::Mutex<Vec<String>>> = Default::default();
    let mut writers = Vec::new();
    for acct in accounts.clone() {
        let (x, stop, acked) = (Xrpc::new(&a.url), stop.clone(), acked.clone());
        writers.push(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Acquire) {
                i += 1;
                let body = json!({"repo": acct.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("seam {i}"))});
                let r = x.post("com.atproto.repo.createRecord", &body, &acct.auth()).await;
                if r.is_ok() {
                    acked.lock().push(r.ok()["commit"]["cid"].as_str().expect("commit cid").to_string());
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await; // shard moving: retry
                }
            }
        }));
    }

    // nodes join one by one; each gets a cursor-0 and a live subscriber the
    // moment it is up
    let target = Arc::new(AtomicI64::new(0));
    let mut nodes = vec![a];
    let mut from_zero = vec![collect(nodes[0].subscribe(Some(0)).await, target.clone())];
    let mut live = vec![collect(nodes[0].subscribe(None).await, target.clone())];
    for id in ["fs-b", "fs-c", "fs-d"] {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let n = node(id, &store).await;
        from_zero.push(collect(n.subscribe(Some(0)).await, target.clone()));
        live.push(collect(n.subscribe(None).await, target.clone()));
        nodes.push(n);
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    stop.store(true, Ordering::Release);
    for w in writers {
        w.await.unwrap();
    }

    // ground truth: the union of every node log in S3, merged by seq, once
    // every node's merged firehose has passed the last acked write
    let acked = acked.lock().clone();
    assert!(acked.len() > 200, "write load too light: {} acked", acked.len());
    let s3 = nodes[0].app.firehose.store.read().clone().unwrap();
    let union = loop {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1 << 16);
        let s = s3.clone();
        let job = tokio::spawn(async move { vlpds::backfill::backfill(&s, 0, i64::MAX, &tx).await });
        let mut all = Vec::new();
        while let Some((seq, frame)) = rx.recv().await {
            all.push((seq, frame.to_vec()));
        }
        job.await.unwrap().unwrap();
        let commits: HashMap<String, usize> = all
            .iter()
            .filter_map(|(_, raw)| Frame::decode(raw).unwrap().commit().map(|c| c.commit.to_string()))
            .fold(HashMap::new(), |mut m, c| {
                *m.entry(c).or_default() += 1;
                m
            });
        if acked.iter().all(|c| commits.contains_key(c)) {
            for c in &acked {
                assert_eq!(commits[c], 1, "acked commit {c} logged more than once");
            }
            break all;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let seqs: Vec<i64> = union.iter().map(|x| x.0).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "union of logs has duplicate seqs");
    target.store(*seqs.last().unwrap(), Ordering::Release);

    let mismatch = |what: &str, got: &[(i64, Vec<u8>)], want: &[(i64, Vec<u8>)]| {
        let g: Vec<i64> = got.iter().map(|x| x.0).collect();
        let w: Vec<i64> = want.iter().map(|x| x.0).collect();
        let missing: Vec<i64> = w.iter().filter(|s| !g.contains(s)).copied().take(20).collect();
        let extra: Vec<i64> = g.iter().filter(|s| !w.contains(s)).copied().take(20).collect();
        format!("{what}: got {} events, want {}; missing {missing:?} extra {extra:?}", g.len(), w.len())
    };
    for (i, (z, l)) in from_zero.into_iter().zip(live).enumerate() {
        let got = z.await.unwrap();
        assert!(got == union, "{}", mismatch(&format!("node {i} cursor-0 subscriber attached at start"), &got, &union));
        let got = l.await.unwrap();
        let start = seqs.iter().position(|s| *s == got[0].0).expect("live subscriber's first event is in the union");
        assert!(got == union[start..], "{}", mismatch(&format!("node {i} live subscriber attached at start"), &got, &union[start..]));
        let mut replay = collect(nodes[i].subscribe(Some(0)).await, target.clone()).await.unwrap();
        replay.truncate(union.len() + 1);
        assert!(replay == union, "{}", mismatch(&format!("node {i} replay from cursor 0"), &replay, &union));
    }
}
