//! Port of packages/pds/tests/sequencer.test.ts, adapted to vlpds: seqs are
//! strictly increasing but not dense; every acked write appears exactly once;
//! each repo's since/rev and prevData chain is unbroken; concurrent readers
//! see identical streams; #sync carries just the root block.
use crate::common::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const POST: &str = "app.bsky.feed.post";

struct Acked {
    did: String,
    path: String,
    commit: String,
    rev: String,
}

/// Concurrent writes from several accounts; returns every acked write.
async fn concurrent_writes(s: &TestServer, accts: &[TestAccount], per_acct: usize) -> Vec<Acked> {
    let futs = accts.iter().map(|a| async move {
        let mut out = Vec::new();
        let inner = (0..per_acct).map(|i| async move {
            let r = s.post(a, &format!("p{i}")).await;
            Acked { did: a.did.clone(), path: format!("{POST}/{}", r.rkey()), commit: r.commit_cid.unwrap(), rev: r.rev.unwrap() }
        });
        out.extend(futures::future::join_all(inner).await);
        out
    });
    futures::future::join_all(futs).await.into_iter().flatten().collect()
}

fn check_stream(frames: &[Frame], acked: &[Acked]) {
    // strictly increasing, unique seqs
    let seqs: Vec<i64> = frames.iter().map(|f| f.seq().expect("seq")).collect();
    for w in seqs.windows(2) {
        assert!(w[0] < w[1], "seq {} followed by {}", w[0], w[1]);
    }
    // per-repo chains
    let mut last: HashMap<String, (String, Cid)> = HashMap::new();
    let mut ops_by_commit: HashMap<String, Vec<String>> = HashMap::new();
    for f in frames {
        if let Some(sy) = f.sync() {
            last.insert(sy.did.clone(), (sy.rev.clone(), sy.commit_obj().data));
        }
        let Some(c) = f.commit() else { continue };
        let data = c.commit_obj().data;
        if let Some((rev, prev_data)) = last.get(&c.repo) {
            assert_eq!(c.since.as_deref(), Some(rev.as_str()), "{} seq {}: since != previous rev", c.repo, c.seq);
            assert_eq!(c.prev_data, Some(*prev_data), "{} seq {}: prevData != previous data", c.repo, c.seq);
            assert!(c.rev > *rev, "rev must increase");
        } else {
            panic!("{}: #commit seq {} before the repo's #sync", c.repo, c.seq);
        }
        assert_eq!(c.invert().unwrap(), c.prev_data.unwrap(), "seq {} inverts", c.seq);
        last.insert(c.repo.clone(), (c.rev.clone(), data));
        let prior = ops_by_commit.insert(c.commit.to_string(), c.ops.iter().map(|o| format!("{} {}", c.repo, o.path)).collect());
        assert!(prior.is_none(), "commit {} appears twice", c.commit);
    }
    // every acked write appears exactly once, in the commit it was acked with
    let mut seen = HashSet::new();
    for a in acked {
        let ops = ops_by_commit.get(&a.commit).unwrap_or_else(|| panic!("acked commit {} not on the firehose", a.commit));
        let key = format!("{} {}", a.did, a.path);
        assert_eq!(ops.iter().filter(|o| **o == key).count(), 1, "{key} in commit {}", a.commit);
        assert!(seen.insert(key.clone()), "{key} acked twice");
        let _ = &a.rev;
    }
    let total_ops: usize = ops_by_commit.values().map(|v| v.len()).sum();
    assert_eq!(total_ops, acked.len(), "no ops beyond the acked writes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sends_to_outbox_in_order() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob", "carol", "dan"] {
        accts.push(s.create_account(n).await);
    }
    let acked = concurrent_writes(&s, &accts, 25).await;
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub.drain(Duration::from_millis(600)).await;
    check_stream(&frames, &acked);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_cutover_while_writing() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob", "carol"] {
        accts.push(s.create_account(n).await);
    }
    let mut acked = concurrent_writes(&s, &accts, 10).await;
    let mut sub = s.subscribe(Some(0)).await;
    acked.extend(concurrent_writes(&s, &accts, 20).await);
    let n = acked.len();
    let frames = sub
        .until(FH_TIMEOUT, |fs| fs.iter().filter_map(|f| f.commit()).map(|c| c.ops.len()).sum::<usize>() >= n)
        .await;
    let extra = sub.drain(Duration::from_millis(300)).await;
    let mut all = frames;
    all.extend(extra);
    check_stream(&all, &acked);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_gets_events_after_cursor() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for i in 0..20 {
        s.post(&a, &format!("seq {i}")).await; // sequential: one commit each
    }
    let mut sub = s.subscribe(Some(0)).await;
    let all = sub.drain(Duration::from_millis(500)).await;
    for cut in [1usize, all.len() / 3, all.len() - 2] {
        let cursor = all[cut].seq().unwrap();
        let mut sub = s.subscribe(Some(cursor)).await;
        let got = sub.drain(Duration::from_millis(400)).await;
        let want: Vec<&Vec<u8>> = all.iter().filter(|f| f.seq().unwrap() > cursor).map(|f| &f.raw).collect();
        assert_eq!(got.iter().map(|f| &f.raw).collect::<Vec<_>>(), want, "cursor {cursor}");
    }
    // a cursor between two (non-dense) seqs resumes at the next event
    let c0 = all[5].seq().unwrap();
    let c1 = all[6].seq().unwrap();
    if c1 - c0 > 1 {
        let mut sub = s.subscribe(Some(c0 + 1)).await;
        let got = sub.drain(Duration::from_millis(400)).await;
        assert_eq!(got.first().and_then(|f| f.seq()), Some(c1));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffers_events_that_are_not_being_read() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut sub = s.subscribe(Some(0)).await;
    // write while not reading
    let acked = concurrent_writes(&s, std::slice::from_ref(&a), 50).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let n = acked.len();
    let frames = sub
        .until(FH_TIMEOUT, |fs| fs.iter().filter_map(|f| f.commit()).map(|c| c.ops.len()).sum::<usize>() >= n)
        .await;
    check_stream(&frames, &acked);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_open_connections() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob"] {
        accts.push(s.create_account(n).await);
    }
    let acked = concurrent_writes(&s, &accts, 10).await;
    let mut streams = Vec::new();
    let futs = (0..20).map(|_| async {
        let mut sub = s.subscribe(Some(0)).await;
        sub.drain(Duration::from_millis(600)).await
    });
    for fs in futures::future::join_all(futs).await {
        streams.push(fs);
    }
    for fs in &streams {
        check_stream(fs, &acked);
        assert_eq!(fs.iter().map(|f| &f.raw).collect::<Vec<_>>(), streams[0].iter().map(|f| &f.raw).collect::<Vec<_>>());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_block_in_sync_event() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub.wait_for(FH_TIMEOUT, &a.did, "#sync").await;
    let sy = frames.last().unwrap().sync().unwrap();
    assert_eq!(sy.did, a.did);
    let (cid, rev) = s.latest_commit(&a.did).await;
    assert_eq!(sy.commit, cid);
    assert_eq!(sy.rev, rev);
    assert_eq!(sy.blocks.len(), 1);
    assert!(sy.blocks.contains_key(&cid));
}
