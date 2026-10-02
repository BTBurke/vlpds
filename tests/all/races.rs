//! Port of atproto/packages/pds/tests/races.test.ts, extended: concurrent
//! writes to one repo all succeed, are all present, and the resulting commit
//! chain (getRepo + firehose since/prevData/inversion) is intact. Also
//! concurrent compare-and-swap writers: exactly one wins.
use crate::common::*;
use std::collections::HashMap;

/// Checks the per-DID chain of #commit events and returns them in order.
fn check_chain(frames: &[Frame], did: &str) -> Vec<CommitEvt> {
    let commits: Vec<CommitEvt> = frames.iter().filter_map(|f| f.commit()).filter(|c| c.repo == did).collect();
    let mut last_seq = i64::MIN;
    let mut prev: Option<(String, Cid)> = None; // (rev, data)
    for c in &commits {
        assert!(c.seq > last_seq, "seqs must increase");
        last_seq = c.seq;
        let obj = c.commit_obj();
        assert_eq!(obj.rev, c.rev);
        assert_eq!(obj.did, did);
        if let Some((rev, data)) = &prev {
            assert_eq!(c.since.as_deref(), Some(rev.as_str()), "since must be the previous rev (seq {})", c.seq);
            assert_eq!(c.prev_data, Some(*data), "prevData must be the previous data (seq {})", c.seq);
            assert!(c.rev > *rev, "revs must increase");
        }
        let inv = c.invert().unwrap_or_else(|e| panic!("seq {} inversion: {e}", c.seq));
        assert_eq!(Some(inv), c.prev_data, "seq {}: inverted root != prevData", c.seq);
        prev = Some((c.rev.clone(), obj.data));
    }
    commits
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_record_writes_all_land() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let key = s.signing_key(&a.did).await;
    let mut sub = s.subscribe(Some(0)).await;

    const N: usize = 100;
    let mut tasks = Vec::new();
    for i in 0..N {
        let xrpc = s.xrpc.clone();
        let a = a.clone();
        tasks.push(tokio::spawn(async move {
            let r = match i % 4 {
                // applyWrites with two creates
                3 => {
                    xrpc.post(
                        "com.atproto.repo.applyWrites",
                        &json!({"repo": a.did, "validate": false, "writes": [
                            {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.like", "rkey": format!("aw{i}a"), "value": {"$type": "app.bsky.feed.like", "subject": {"uri": "at://did:plc:x/app.bsky.feed.post/3jzfcijpj2z2a", "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"}, "createdAt": now_iso()}},
                            {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.like", "rkey": format!("aw{i}b"), "value": {"$type": "app.bsky.feed.like", "subject": {"uri": "at://did:plc:x/app.bsky.feed.post/3jzfcijpj2z2b", "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"}, "createdAt": now_iso()}},
                        ]}),
                        &a.auth(),
                    )
                    .await
                }
                // putRecord with explicit key
                2 => {
                    xrpc.post(
                        "com.atproto.repo.putRecord",
                        &json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": format!("put{i}"), "validate": false, "record": post_record(&format!("put {i}"))}),
                        &a.auth(),
                    )
                    .await
                }
                _ => {
                    xrpc.post(
                        "com.atproto.repo.createRecord",
                        &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("create {i}"))}),
                        &a.auth(),
                    )
                    .await
                }
            };
            (i, r)
        }));
    }
    let mut expected: HashMap<String, String> = HashMap::new(); // path -> cid
    for t in tasks {
        let (i, r) = t.await.unwrap();
        let j = r.ok();
        if i % 4 == 3 {
            for res in j["results"].as_array().unwrap() {
                let uri = res["uri"].as_str().unwrap();
                expected.insert(uri.splitn(4, '/').nth(3).unwrap().to_string(), res["cid"].as_str().unwrap().to_string());
            }
        } else {
            let rr = RecordRef::from_json(&j);
            expected.insert(format!("{}/{}", rr.collection(), rr.rkey()), rr.cid);
        }
    }
    assert_eq!(expected.len(), N + N / 4);

    // all present via listRecords
    let mut listed = 0;
    for coll in ["app.bsky.feed.post", "app.bsky.feed.like"] {
        let mut cursor: Option<String> = None;
        loop {
            let mut q = vec![("limit", "100")];
            if let Some(c) = &cursor {
                q.push(("cursor", c.as_str()));
            }
            let j = s.list_records(&a.did, coll, &q).await.ok();
            let recs = j["records"].as_array().unwrap();
            listed += recs.len();
            cursor = j["cursor"].as_str().map(String::from);
            if cursor.is_none() || recs.is_empty() {
                break;
            }
        }
    }
    assert_eq!(listed, expected.len());

    // the exported repo verifies and contains exactly these records
    let repo = s.get_repo(&a.did).await;
    repo.check_block_hashes().unwrap();
    repo.commit().verify(&key).unwrap();
    let entries: HashMap<String, String> = repo.entries().into_iter().map(|(p, c)| (p, c.to_string())).collect();
    assert_eq!(entries, expected);
    for (path, cid) in &expected {
        assert!(repo.blocks.contains_key(&Cid::parse(cid).unwrap()), "record block for {path} missing from CAR");
    }

    // the firehose chain for this repo is intact and ends at the head
    let (head, rev) = s.latest_commit(&a.did).await;
    let frames = sub.until(FH_TIMEOUT, |fs| fs.iter().any(|f| f.commit().map(|c| c.commit == head).unwrap_or(false))).await;
    let commits = check_chain(&frames, &a.did);
    assert_eq!(commits.last().unwrap().rev, rev);
    for c in &commits {
        c.commit_obj().verify(&key).unwrap();
    }
    // every write appears in exactly one commit's ops
    let mut seen: HashMap<String, usize> = HashMap::new();
    for c in &commits {
        for op in &c.ops {
            *seen.entry(op.path.clone()).or_default() += 1;
        }
    }
    for path in expected.keys() {
        assert_eq!(seen.get(path), Some(&1), "{path} must appear in exactly one #commit");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_swap_commit_exactly_one_wins() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let (head, _) = s.latest_commit(&a.did).await;
    let mut tasks = Vec::new();
    for i in 0..20 {
        let xrpc = s.xrpc.clone();
        let a = a.clone();
        let head = head.to_string();
        tasks.push(tokio::spawn(async move {
            xrpc.post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": "app.bsky.feed.post", "rkey": format!("swap{i}"), "swapCommit": head, "validate": false, "record": post_record("race")}),
                &a.auth(),
            )
            .await
        }));
    }
    let mut ok = 0;
    for t in tasks {
        let r = t.await.unwrap();
        if r.is_ok() {
            ok += 1;
        } else {
            r.err(400, "InvalidSwap");
        }
    }
    assert_eq!(ok, 1, "exactly one swapCommit writer may win");
    assert_eq!(s.get_repo(&a.did).await.entries().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_swap_record_null_exactly_one_wins() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut tasks = Vec::new();
    for i in 0..20 {
        let xrpc = s.xrpc.clone();
        let a = a.clone();
        tasks.push(tokio::spawn(async move {
            xrpc.post(
                "com.atproto.repo.putRecord",
                &json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "swapRecord": null, "record": {"$type": "app.bsky.actor.profile", "displayName": format!("n{i}")}}),
                &a.auth(),
            )
            .await
        }));
    }
    let mut winners = Vec::new();
    for t in tasks {
        let r = t.await.unwrap();
        if r.is_ok() {
            winners.push(r.json["cid"].clone());
        } else {
            r.err(400, "InvalidSwap");
        }
    }
    assert_eq!(winners.len(), 1, "exactly one swapRecord=null writer may win");
    let g = s.get_record(&a.did, "app.bsky.actor.profile", "self").await.ok();
    assert_eq!(g["cid"], winners[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_across_repos() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for _ in 0..8 {
        accts.push(s.create_account("multi").await);
    }
    let mut sub = s.subscribe(Some(0)).await;
    let mut tasks = Vec::new();
    for a in &accts {
        for i in 0..15 {
            let xrpc = s.xrpc.clone();
            let a = a.clone();
            tasks.push(tokio::spawn(async move {
                xrpc.post(
                    "com.atproto.repo.createRecord",
                    &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("{i}"))}),
                    &a.auth(),
                )
                .await
                .ok();
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut heads = HashMap::new();
    for a in &accts {
        heads.insert(a.did.clone(), s.latest_commit(&a.did).await.0);
        assert_eq!(s.get_repo(&a.did).await.entries().len(), 15);
    }
    let frames =
        sub.until(FH_TIMEOUT, |fs| heads.iter().all(|(d, h)| fs.iter().any(|f| f.commit().map(|c| &c.repo == d && c.commit == *h).unwrap_or(false)))).await;
    for a in &accts {
        let commits = check_chain(&frames, &a.did);
        let ops: usize = commits.iter().map(|c| c.ops.len()).sum();
        assert_eq!(ops, 15);
    }
}
