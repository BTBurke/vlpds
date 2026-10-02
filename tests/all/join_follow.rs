//! A joiner may ack nothing until every live peer follows its log (DESIGN.md
//! §5 "Firehose" and "Liveness": *Greeting*). A peer that starts following a
//! log late starts at its merger's position then, so it never delivers the
//! log's earlier events: had the joiner acked writes before that, the peer's
//! merged firehose would skip them for good.
//!
//! Adversary: c joins a 2-node cluster under write load while b's step loop
//! is stuck (it can't discover c from the node list) and b answers c's
//! greeting "not following" (`Cluster::test_hold_steps`,
//! `test_ignore_hellos`). b keeps renewing its lease and its merger keeps
//! emitting a's and its own events. The time-based join grace (2 renew
//! intervals) used to expire here, c took a handback from a and acked
//! writes, and once b recovered, b's live subscribers never saw those
//! commits. Now c joins only once b confirms (hello or its lease), and every
//! node's stream is the union of all logs.

use crate::common::*;
use crate::firehose_startup::{collect, mismatch, node_with, s3_union, Writers};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: usize = 8;

fn owned(s: &TestServer) -> usize {
    s.app.partitions.owned().len()
}

async fn wait_for(what: &str, deadline: Duration, f: impl Fn() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < deadline, "{what}: not within {deadline:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn late_follower_loses_no_joiner_events() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node_with("jf-a", &store, None).await;
    let b = node_with("jf-b", &store, None).await;
    wait_for("a and b split the shards", Duration::from_secs(10), || owned(&a) == SHARDS / 2 && owned(&b) == SHARDS / 2).await;
    let accounts: Vec<TestAccount> = futures::future::join_all((0..16).map(|_| a.create_account("jf"))).await;
    let target = Arc::new(AtomicI64::new(0));
    let a_live = collect(a.subscribe(None).await, target.clone());
    let b_live = collect(b.subscribe(None).await, target.clone());
    let b_zero = collect(b.subscribe(Some(0)).await, target.clone());
    let writers = Writers::start(&a, &accounts);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // b can neither discover c (its steps are stuck) nor answer its greeting
    let bc = b.app.cluster.clone().unwrap();
    bc.test_hold_steps(true);
    bc.test_ignore_hellos(true);
    let c = node_with("jf-c", &store, None).await;
    let c_live = collect(c.subscribe(None).await, target.clone());
    // 10 renew intervals: 5 times the old time-based join grace, under load
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let c_owned_while_b_deaf = owned(&c);
    // ... and a while longer for whatever c acked meanwhile to be emitted
    tokio::time::sleep(Duration::from_millis(500)).await;
    bc.test_ignore_hellos(false);
    bc.test_hold_steps(false);
    wait_for("c gets its share once b follows its log", Duration::from_secs(10), || owned(&c) >= 2).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let acked = writers.stop().await;
    assert!(acked.len() > 100, "write load too light: {} acked", acked.len());

    let union = s3_union(&a, &acked).await;
    let seqs: Vec<i64> = union.iter().map(|x| x.0).collect();
    target.store(*seqs.last().unwrap(), Ordering::Release);
    for (name, sub) in [("a live", a_live), ("b live", b_live), ("c live", c_live)] {
        let got = sub.await.unwrap();
        let start = seqs.iter().position(|s| *s == got[0].0).expect("live subscriber's first event is in the union");
        assert!(got == union[start..], "{}", mismatch(name, &got, &union[start..]));
    }
    let got = b_zero.await.unwrap();
    assert!(got == union, "{}", mismatch("b cursor 0", &got, &union));
    for (name, n) in [("a", &a), ("b", &b), ("c", &c)] {
        let mut replay = collect(n.subscribe(Some(0)).await, target.clone()).await.unwrap();
        replay.truncate(union.len() + 1);
        assert!(replay == union, "{}", mismatch(&format!("{name} replay from cursor 0"), &replay, &union));
    }
    assert_eq!(c_owned_while_b_deaf, 0, "c took shards before b followed its log");
}
