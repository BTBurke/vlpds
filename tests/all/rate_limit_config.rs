//! Runtime rate-limit configuration and observability (src/ratelimit.rs,
//! src/ratelimit/{config,runtime}.rs, src/xrpc/ratelimits.rs) on a real
//! two-node cluster sharing one in-memory bucket: a change saved through
//! one node's admin endpoint takes effect on both nodes at once (the writer
//! installs it, the peer is nudged), a DID override exempts that account,
//! the cluster view merges both nodes' heavy hitters and 429 tallies, stale
//! and invalid edits are refused, and an invalid object written behind the
//! API's back leaves every node on its last good config with the error
//! surfaced.

use crate::common::*;
use object_store::ObjectStoreExt;
use std::sync::Arc;
use std::time::Duration;

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.rate_limits_enabled = true;
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: peer_url(c),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(200),
            ..Default::default()
        });
    })
    .await
}

/// Both nodes see each other and own shards.
async fn joined(a: &TestServer, b: &TestServer) {
    eventually(Duration::from_secs(20), || async {
        let ok = [a, b].iter().all(|s| {
            let c = s.app.cluster.as_ref().unwrap();
            c.peers().iter().any(|l| l.node_id != c.cfg.node_id) && !s.app.partitions.owned().is_empty()
        });
        ok.then_some(())
    })
    .await
    .expect("two-node cluster formed");
}

async fn create(s: &TestServer, a: &TestAccount, i: usize) -> Resp {
    let body = json!({"repo": a.did, "collection": "com.example.rl", "record": {"$type": "com.example.rl", "i": i}});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await
}

async fn update(s: &TestServer, config: J, if_version: u64) -> Resp {
    s.xrpc
        .post("vlpds.admin.updateRateLimits", &json!({"config": config, "ifVersion": if_version, "actor": "it-test", "note": "integration"}), &Auth::Admin)
        .await
}

async fn status(s: &TestServer) -> J {
    s.xrpc.get("vlpds.admin.getRateLimits", &[("top", "5")], &Auth::Admin).await.ok()
}

fn limit_header(r: &Resp) -> Option<i64> {
    r.header("ratelimit-limit").and_then(|v| v.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits_change_cluster_wide_and_overrides_exempt() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rlc-a", &store).await;
    let b = node("rlc-b", &store).await;
    joined(&a, &b).await;

    // admin only
    a.xrpc.get("vlpds.admin.getRateLimits", &[], &Auth::None).await.err(401, "AuthenticationRequired");
    let s = status(&a).await;
    assert_eq!(s["configVersion"], 0);
    assert!(s["config"].is_null());
    assert_eq!(s["limiters"][0]["name"], "global-ip");
    assert_eq!(s["limiters"][0]["points"], 3000);

    let trusted = a.create_account("rlctrust").await;
    let normal = a.create_account("rlcnorm").await;

    // v1: a tight repo-write budget, with the trusted account exempt
    let v1 = json!({
        "limiters": {"repo-write-hour": {"points": 6}},
        "overrides": [{"did": trusted.did, "limiters": ["repo-write-hour", "repo-write-day"], "exempt": true, "note": "trusted service"}]
    });
    let r = update(&b, v1.clone(), 0).await.ok();
    assert_eq!(r["version"], 1);
    // installed on the writer and, through the nudge, on its peer before the call returned
    let nodes = r["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2, "{r}");
    assert!(nodes.iter().all(|n| n["ok"] == true && n["configVersion"] == 1), "{r}");
    assert_eq!(a.app.ratelimit.policy().version, 1);
    assert_eq!(b.app.ratelimit.policy().version, 1);
    assert_eq!(r["config"]["history"][0]["by"], "it-test");
    assert_eq!(r["config"]["history"][0]["changes"][0], "repo-write-hour: points 5000→6");

    // the normal account gets 2 creates (3 points each), then 429, through either node
    for (i, s) in [&a, &b].iter().enumerate() {
        let r = create(s, &normal, i).await;
        r.ok();
    }
    let r = create(&b, &normal, 9).await;
    r.err(429, "RateLimitExceeded");
    assert_eq!(limit_header(&r), Some(6));
    // the exempt account writes freely (15+ points), through both nodes
    for i in 0..6 {
        let s = if i % 2 == 0 { &a } else { &b };
        create(s, &trusted, i).await.ok();
    }

    // v2: a new global-ip limit and window: fresh 4-point windows on both nodes
    let v2 = json!({
        "limiters": {"repo-write-hour": {"points": 6}, "global-ip": {"points": 4, "windowSecs": 120}},
        "overrides": v1["overrides"],
    });
    // an edit made against v0 is stale
    update(&a, v2.clone(), 0).await.err(409, "ConfigConflict");
    // and an invalid one is refused with every problem listed
    let r = update(&a, json!({"limiters": {"nope": {}}, "routes": [{"nsid": "bad", "points": 1, "windowSecs": 1}]}), 1).await;
    r.err(400, "InvalidConfig");
    assert!(r.text().contains("limiters.nope") && r.text().contains("routes[0].nsid"), "{}", r.text());
    let r = update(&a, v2, 1).await.ok();
    assert_eq!(r["version"], 2);
    for s in [&a, &b] {
        for i in 0..4 {
            let r = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
            assert_eq!(r.status, 200, "request {i} on {}: {}", s.url, r.text());
            assert_eq!(limit_header(&r), Some(4));
            assert_eq!(r.header("ratelimit-policy").as_deref(), Some("4;w=120"));
        }
        let r = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
        r.err(429, "RateLimitExceeded");
    }

    // the cluster view, from either node: both nodes on v2, merged heavy
    // hitters and 429 tallies
    let s = status(&b).await;
    assert!(s.get("unreachableNodes").is_none(), "{s}");
    assert_eq!(s["configVersion"], 2);
    assert_eq!(s["config"]["history"].as_array().unwrap().len(), 2);
    let nodes = s["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.iter().all(|n| n["reachable"] == true && n["configVersion"] == 2 && n["configError"].is_null()), "{s}");
    let g = s["limiters"].as_array().unwrap().iter().find(|l| l["name"] == "global-ip").unwrap().clone();
    assert_eq!((g["points"].as_u64(), g["windowSecs"].as_u64(), g["default"]["points"].as_u64()), (Some(4), Some(120), Some(3000)));
    let top = &s["top"]["global-ip"][0];
    assert_eq!(top["key"], "127.0.0.1");
    assert_eq!(top["used"], 10, "5 per node: {s}");
    assert_eq!(top["maxNodeUsed"], 5);
    assert_eq!(top["nodes"].as_array().unwrap().len(), 2);
    // the normal account is the top writer, over its limit; the exempt one
    // is never counted
    let writes = s["top"]["repo-write-hour"].as_array().unwrap();
    assert_eq!((writes[0]["key"].as_str(), writes[0]["used"].as_u64(), writes[0]["limit"].as_u64()), (Some(normal.did.as_str()), Some(9), Some(6)));
    assert!(!writes.iter().any(|c| c["key"] == trusted.did.as_str()), "{writes:?}");
    let rej = s["rejections"].as_array().unwrap();
    let find = |l: &str, r: &str| rej.iter().find(|x| x["limiter"] == l && x["route"] == r).cloned();
    assert_eq!(find("global-ip", "com.atproto.server.describeServer").expect("global-ip 429s")["last5m"], 2);
    assert_eq!(find("repo-write-hour", "com.atproto.repo.createRecord").expect("write 429")["total"], 1);
    // ... and per node only with local=true
    let local = a.xrpc.get("vlpds.admin.getRateLimits", &[("local", "true")], &Auth::Admin).await.ok();
    assert_eq!(local["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(local["top"]["global-ip"][0]["used"], 5);

    // an invalid object written behind the API's back: both nodes keep v2
    // and report the error
    let path = vlpds::ratelimit::runtime::config_path(&a.app.store);
    store.put(&path, br#"{"version": 9, "limiters": {"global-ip": {"points": "many"}}}"#.to_vec().into()).await.unwrap();
    for s in [&a, &b] {
        // dev mode: the admin token doubles as the internal token
        let rb = peer_client().post(format!("{}/internal/v1/ratelimits/reload", s.peer_url)).header("x-vlpds-internal", ADMIN_TOKEN);
        let r = s.xrpc.send(rb).await.ok();
        assert_eq!((r["configVersion"].as_u64(), r["configError"]["version"].as_u64()), (Some(2), Some(9)), "{r}");
    }
    let s = status(&a).await;
    assert_eq!(s["configVersion"], 2, "last good config stays: {s}");
    for n in s["nodes"].as_array().unwrap() {
        assert_eq!(n["configVersion"], 2);
        assert_eq!(n["configError"]["version"], 9, "{n}");
    }
    assert_eq!(b.app.ratelimit.policy().builtin(&vlpds::ratelimit::GLOBAL_IP).points, 4);
    // the broken object is replaced through the API by naming its version
    let r = update(&a, json!({}), 9).await.ok();
    assert_eq!(r["version"], 10);
    for s in [&a, &b] {
        let p = s.app.ratelimit.policy();
        assert_eq!((p.version, p.builtin(&vlpds::ratelimit::GLOBAL_IP).points), (10, 3000));
        assert!(s.app.ratelimit.runtime.status().error.is_none());
    }
}

/// A node joining after a change (or that missed the nudge) loads the
/// stored config on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_joiner_loads_the_stored_config() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node("rlj-a", &store).await;
    update(&a, json!({"limiters": {"global-ip": {"points": 77}}}), 0).await.ok();
    let b = node("rlj-b", &store).await;
    eventually(Duration::from_secs(5), || async { (b.app.ratelimit.policy().version == 1).then_some(()) }).await.expect("late joiner picked up v1");
    let r = b.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await;
    assert_eq!(limit_header(&r), Some(77));
}
