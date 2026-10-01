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
        });
    })
    .await
}

/// Waits until every node owns some shards and together they own each once.
async fn balanced(nodes: &[&TestServer]) {
    for _ in 0..200 {
        let owned: Vec<Vec<u16>> =
            nodes.iter().map(|n| n.app.partitions.owned().iter().map(|p| p.id).collect()).collect();
        let all: HashSet<u16> = owned.iter().flatten().copied().collect();
        if owned.iter().all(|o| !o.is_empty()) && all.len() == SHARDS as usize && owned.iter().map(|o| o.len()).sum::<usize>() == SHARDS as usize {
            return;
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
            [&a, &b, &c].iter().find(|s| s.app.partitions.get(p as usize).is_some()).map(|s| s.url.clone()).unwrap()
        })
        .collect();
    assert_eq!(owners.len(), 3, "accounts spread over all three nodes");

    // email-prefix search from any node, paged 2 at a time, sees all 9 in
    // (shard, did) order with no duplicates
    let prefix = tag.to_ascii_lowercase();
    for s in [&a, &b, &c] {
        let (got, pages) = all_pages(s, "com.atproto.admin.searchAccounts", "accounts", &[("email", &prefix)], 2).await;
        let dids: Vec<String> = got.iter().map(|v| v["did"].as_str().unwrap().to_string()).collect();
        let mut sorted = dids.clone();
        sorted.sort_by_key(|d| (shard(d), d.clone()));
        assert_eq!(dids, sorted, "merged in (shard, did) order");
        let mut expect: Vec<String> = want.iter().map(|t| t.did.clone()).collect();
        expect.sort_by_key(|d| (shard(d), d.clone()));
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

    // a "live" peer that never answers: reported, not silently dropped
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let lease = vlpds::cluster::NodeLease {
        node_id: "adm-ghost".into(),
        log_id: "adm-ghost.0".into(),
        addr: dead_addr,
        writer: 254,
        expires_ms: (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + 60_000) as u64,
    };
    store
        .put(&object_store::path::Path::from("vlpds/nodes/adm-ghost"), serde_json::to_vec(&lease).unwrap().into())
        .await
        .unwrap();
    let mut seen = None;
    for _ in 0..100 {
        let r = a.xrpc.get("com.atproto.admin.searchAccounts", &[("email", &prefix)], &Auth::Admin).await.ok();
        if r["unreachableNodes"].as_array().is_some_and(|v| v.iter().any(|n| n == "adm-ghost")) {
            seen = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = seen.expect("unreachable peer reported");
    assert!(r["accounts"].is_array(), "partial results still returned: {r}");
    let r = b.xrpc.get("com.atproto.admin.getInviteCodes", &[], &Auth::Admin).await.ok();
    assert!(r["unreachableNodes"].as_array().is_some_and(|v| v.iter().any(|n| n == "adm-ghost")), "{r}");
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
