//! Cluster-wide admin listings (searchAccounts, getInviteCodes): in-process
//! nodes sharing one in-memory object store form a real cluster (leases,
//! shard handoff, forwarding); any node's admin listing scatter-gathers over
//! /internal/v1/admin/* and pages across every node's shards, and a dead peer
//! is reported instead of silently dropped.

use crate::common::*;
use object_store::ObjectStoreExt;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 8;

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

/// Waits until the shards are spread at fair share (sizes within one of each
/// other, each shard owned once) and the assignment holds still for 500 ms:
/// "every node owns something" can still be mid-rebalance (e.g. 6/1/1), and
/// later moves would carry a node's accounts off it.
async fn balanced(nodes: &[&TestServer]) {
    let mut stable_since: Option<(Vec<Vec<vlpds::slots::ShardId>>, std::time::Instant)> = None;
    for _ in 0..400 {
        let owned: Vec<Vec<vlpds::slots::ShardId>> = nodes
            .iter()
            .map(|n| {
                let mut v: Vec<vlpds::slots::ShardId> = n.app.partitions.owned().iter().map(|p| p.id).collect();
                v.sort();
                v
            })
            .collect();
        let all: HashSet<vlpds::slots::ShardId> = owned.iter().flatten().copied().collect();
        let sizes: Vec<usize> = owned.iter().map(|o| o.len()).collect();
        let fair = sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1;
        let complete = all.len() == SHARDS as usize && sizes.iter().sum::<usize>() == SHARDS as usize;
        if fair && complete {
            match &stable_since {
                Some((prev, at)) if *prev == owned => {
                    if at.elapsed() >= Duration::from_millis(500) {
                        return;
                    }
                }
                _ => stable_since = Some((owned, std::time::Instant::now())),
            }
        } else {
            stable_since = None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cluster never balanced");
}

/// Every page of `nsid` (key `items`) at `limit`, following cursors.
async fn all_pages(s: &TestServer, nsid: &str, items: &str, extra: &[(&str, &str)], limit: usize) -> (Vec<J>, Vec<J>) {
    let (mut out, mut pages) = (Vec::new(), Vec::new());
    let mut cursor: Option<String> = None;
    for _ in 0..100 {
        let lim = limit.to_string();
        let mut q: Vec<(&str, &str)> = extra.to_vec();
        q.push(("limit", &lim));
        if let Some(c) = &cursor {
            q.push(("cursor", c));
        }
        let r = s.xrpc.get(nsid, &q, &Auth::Admin).await.ok();
        assert!(r.get("unreachableNodes").is_none() && r.get("missingShards").is_none(), "complete cluster: {r}");
        let page = r[items].as_array().unwrap().clone();
        assert!(page.len() <= limit);
        out.extend(page);
        pages.push(r.clone());
        match r["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return (out, pages),
        }
    }
    panic!("too many pages");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_listings_scatter_gather_across_nodes() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("adm-a", &store).await;
    let b = node("adm-b", &store).await;
    let c = node("adm-c", &store).await;
    balanced(&[&a, &b, &c]).await;

    // accounts minted on each node land on that node's shards
    let tag = unique_name("sg");
    let mut want = Vec::new();
    for (i, n) in [&a, &b, &c].iter().enumerate() {
        for j in 0..3 {
            let handle = format!("{tag}{i}x{j}.{HANDLE_DOMAIN}");
            want.push(n.create_account_with(&handle, PASSWORD).await);
        }
    }
    let n = SHARDS;
    let shard = |did: &str| vlpds::state::partition_of(did, n);
    let owners: HashSet<String> = want
        .iter()
        .map(|t| {
            let p = shard(&t.did);
            [&a, &b, &c].iter().find(|s| s.app.partitions.get(p).is_some()).map(|s| s.url.clone()).unwrap()
        })
        .collect();
    assert_eq!(owners.len(), 3, "accounts spread over all three nodes");

    // email-prefix search from any node, paged 2 at a time, sees all 9 in
    // (slot, did) order (layout-independent) with no duplicates
    let prefix = tag.to_ascii_lowercase();
    let slot = |d: &str| vlpds::slots::slot_of(d);
    for s in [&a, &b, &c] {
        let (got, pages) = all_pages(s, "com.atproto.admin.searchAccounts", "accounts", &[("email", &prefix)], 2).await;
        let dids: Vec<String> = got.iter().map(|v| v["did"].as_str().unwrap().to_string()).collect();
        let mut sorted = dids.clone();
        sorted.sort_by_key(|d| (slot(d), d.clone()));
        assert_eq!(dids, sorted, "merged in (slot, did) order");
        let mut expect: Vec<String> = want.iter().map(|t| t.did.clone()).collect();
        expect.sort_by_key(|d| (slot(d), d.clone()));
        assert_eq!(dids, expect, "every account exactly once");
        // the views come from the owning node (handle, email, invites)
        let h = got.iter().find(|v| v["did"] == want[4].did.as_str()).unwrap();
        assert_eq!(h["handle"], want[4].handle.as_str());
        assert_eq!(h["email"], want[4].email.to_ascii_lowercase().as_str());
        assert!(pages.len() >= 5, "9 accounts at 2 per page");
    }

    // invite codes, created through every node (stored on whichever shard
    // `_invite:{code}` hashes to, possibly a peer's)
    let mut codes = Vec::new();
    for s in [&a, &b, &c] {
        let r = s
            .xrpc
            .post("com.atproto.server.createInviteCodes", &json!({"codeCount": 3, "useCount": 1}), &Auth::Admin)
            .await
            .ok();
        for c in r["codes"][0]["codes"].as_array().unwrap() {
            codes.push(c.as_str().unwrap().to_string());
        }
        tokio::time::sleep(Duration::from_millis(5)).await; // distinct createdAt per batch
    }
    for sort in ["recent", "usage"] {
        for s in [&a, &c] {
            let (got, _) = all_pages(s, "com.atproto.admin.getInviteCodes", "codes", &[("sort", sort)], 2).await;
            let listed: Vec<String> = got.iter().map(|v| v["code"].as_str().unwrap().to_string()).collect();
            let uniq: HashSet<&String> = listed.iter().collect();
            assert_eq!(uniq.len(), listed.len(), "no duplicates across pages ({sort})");
            for c in &codes {
                assert!(listed.contains(c), "{c} missing from cluster-wide getInviteCodes ({sort})");
            }
            if sort == "recent" {
                let keys: Vec<(String, String)> = got
                    .iter()
                    .map(|v| (v["createdAt"].as_str().unwrap().to_string(), v["code"].as_str().unwrap().to_string()))
                    .collect();
                let mut desc = keys.clone();
                desc.sort_by(|x, y| y.cmp(x));
                assert_eq!(keys, desc, "createdAt desc, code desc");
            }
        }
    }

    // single page with room to spare: no cursor
    let r = a.xrpc.get("com.atproto.admin.searchAccounts", &[("email", &prefix), ("limit", "100")], &Auth::Admin).await.ok();
    assert_eq!(r["accounts"].as_array().unwrap().len(), 9);
    // the internal endpoint wants the internal token, not admin credentials
    let rb = a.xrpc.http.get(format!("{}/internal/v1/admin/searchAccounts", b.url));
    assert_eq!(a.xrpc.send(rb).await.status, 401);

    // a "live" peer that never answers: reported, not silently dropped. Its
    // lease keeps renewing for the rest of the test: a lease that goes quiet
    // for 1.5 renew intervals at an address refusing connections is presumed
    // dead (and dropped from the peers) within ~150 ms, which under load
    // could happen between the two listings below.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let ghost_store = store.clone();
    let ghost = tokio::spawn(async move {
        for renewals in 1u64.. {
            let lease = vlpds::cluster::NodeLease {
                node_id: "adm-ghost".into(),
                log_id: "adm-ghost.0".into(),
                addr: dead_addr.clone(),
                writer: 254,
                expires_ms: (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + 60_000) as u64,
                renewals,
                next_ordinal: 0,
                draining: false,
                joined: false,
                follows: Default::default(),
                wm_cap: 0,
                rev: "ghost-rev".into(),
                min_level: 1,
                max_level: 1,
                seen_level: 1,
            };
            ghost_store
                .put(&object_store::path::Path::from("vlpds/nodes/adm-ghost"), serde_json::to_vec(&lease).unwrap().into())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    let reports_ghost = |r: &J| r["unreachableNodes"].as_array().is_some_and(|v| v.iter().any(|n| n == "adm-ghost"));
    // each node reports it once its membership step has seen the lease
    let mut seen = None;
    for _ in 0..100 {
        let r = a.xrpc.get("com.atproto.admin.searchAccounts", &[("email", &prefix)], &Auth::Admin).await.ok();
        if reports_ghost(&r) {
            seen = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = seen.expect("unreachable peer reported");
    assert!(r["accounts"].is_array(), "partial results still returned: {r}");
    let mut last = J::Null;
    for _ in 0..100 {
        last = b.xrpc.get("com.atproto.admin.getInviteCodes", &[], &Auth::Admin).await.ok();
        if reports_ghost(&last) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ghost.abort();
    assert!(reports_ghost(&last), "{last}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_listings_single_node_complete() {
    let s = TestServer::spawn().await;
    let tag = unique_name("sn");
    for j in 0..3 {
        s.create_account_with(&format!("{tag}x{j}.{HANDLE_DOMAIN}"), PASSWORD).await;
    }
    let prefix = tag.to_ascii_lowercase();
    let (got, pages) = all_pages(&s, "com.atproto.admin.searchAccounts", "accounts", &[("email", &prefix)], 2).await;
    assert_eq!(got.len(), 3);
    assert_eq!(pages.len(), 2);
    s.xrpc.get("com.atproto.admin.getInviteCodes", &[("cursor", "no-slash")], &Auth::Admin).await.err(400, "InvalidRequest");
    s.xrpc.get("com.atproto.admin.searchAccounts", &[("cursor", "nope")], &Auth::Admin).await.err(400, "InvalidRequest");
}
