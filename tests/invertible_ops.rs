//! Port of packages/pds/tests/sync/invertible-ops.test.ts: every #commit on
//! the firehose can be inverted from its own blocks back to prevData.
mod common;
use common::*;
use std::collections::HashSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_commit_inverts_to_prev_data() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for name in ["alice", "bob", "carol", "dan"] {
        accts.push(s.create_account(name).await);
    }
    let mut posts = Vec::new();
    for i in 0..20 {
        let text = format!("test {i}");
        let futs = accts.iter().map(|a| s.post(a, &text));
        posts.extend(futures::future::join_all(futs).await);
    }
    for p in &posts {
        let a = accts.iter().find(|a| a.did == p.did()).unwrap();
        s.xrpc
            .post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": p.collection(), "rkey": p.rkey()}), &a.auth())
            .await
            .ok();
    }
    let dids: HashSet<String> = accts.iter().map(|a| a.did.clone()).collect();
    let mut sub = s.subscribe(Some(0)).await;
    let mut deletes = 0;
    let mut creates = 0;
    let frames = sub
        .until(FH_TIMEOUT, |fs| {
            let ops: usize = fs.iter().filter_map(|f| f.commit()).map(|c| c.ops.len()).sum();
            ops >= 160
        })
        .await;
    let mut checked = 0;
    for f in &frames {
        let Some(c) = f.commit() else { continue };
        assert!(dids.contains(&c.repo));
        let Some(prev) = c.prev_data else { continue };
        for op in &c.ops {
            match op.action.as_str() {
                "create" => creates += 1,
                "delete" => deletes += 1,
                _ => {}
            }
        }
        let inverted = c.invert().unwrap_or_else(|e| panic!("seq {}: {e}", c.seq));
        assert_eq!(inverted, prev, "seq {}: inverted root != prevData", c.seq);
        checked += 1;
    }
    assert!(checked > 0);
    assert_eq!((creates, deletes), (80, 80));
}
