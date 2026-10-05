//! `vlpds.admin.listFirehoseSubscribers` (src/xrpc/firehose_subs.rs): admin
//! only, every connected subscriber with its node, the recently gone with
//! their reason, and on a cluster both nodes' subscribers from either node.

use crate::common::*;
use std::sync::Arc;
use std::time::Duration;

async fn list(s: &TestServer) -> J {
    s.xrpc.get("vlpds.admin.listFirehoseSubscribers", &[], &Auth::Admin).await.ok()
}

async fn until_total(s: &TestServer, n: u64) -> J {
    eventually(Duration::from_secs(10), || async {
        let r = list(s).await;
        (r["total"].as_u64() == Some(n)).then_some(r)
    })
    .await
    .unwrap_or_else(|| panic!("{n} subscribers listed"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_subscribers_until_they_leave() {
    let s = TestServer::spawn_with(|c| c.crawlers = vec!["relay.example.com".into()]).await;
    s.xrpc.get("vlpds.admin.listFirehoseSubscribers", &[], &Auth::None).await.err(401, "AuthenticationRequired");
    let a = s.create_account("fsub").await;
    s.xrpc.get("vlpds.admin.listFirehoseSubscribers", &[], &a.auth()).await.err(401, "AuthenticationRequired");

    // the relay hints are cached in the background: wait for them first
    eventually(Duration::from_secs(10), || async {
        s.app.crawlers.relay_hint(&s.app.store, None, "relay.example.com").map(|_| ())
    })
    .await
    .expect("relay hints loaded");
    let relay = Sub::connect_as(&s.ws_url(None), "indigo-relay (relay.example.com)").await;
    let mut other = s.subscribe(Some(0)).await;
    let r = until_total(&s, 2).await;
    assert_eq!(r["live"].as_u64().unwrap() + r["backfilling"].as_u64().unwrap(), 2, "{r}");
    let subs = r["subscribers"].as_array().unwrap();
    for v in subs {
        assert_eq!(v["node"], "single", "{v}");
        assert_eq!(v["ip"], "127.0.0.1", "{v}");
        assert!(v["conn"].as_str().is_some_and(|c| !c.is_empty()), "{v}");
        assert!(["live", "backfilling"].contains(&v["state"].as_str().unwrap()), "{v}");
        assert!(v["connectedAt"].as_u64().unwrap() > 0);
    }
    let by_relay = subs.iter().find(|v| v["relay"] == "relay.example.com").expect("the relay is named");
    assert_eq!(by_relay["userAgent"], "indigo-relay (relay.example.com)");
    assert!(by_relay["cursor"].is_null());
    let backfill = subs.iter().find(|v| v["cursor"] == "0").expect("the cursor subscriber");
    assert!(backfill["relay"].is_null());
    assert_eq!(r["nodes"][0]["node"], "single");
    assert!(r["nodes"][0]["eventsEmitted"].as_u64().is_some());

    // events sent show up in its counters
    s.post(&a, "counted").await;
    other.next(Duration::from_secs(10)).await.expect("an event");
    eventually(Duration::from_secs(10), || async {
        let r = list(&s).await;
        let v = r["subscribers"].as_array().unwrap().iter().find(|v| v["cursor"] == "0").cloned()?;
        (v["events"].as_u64()? > 0 && v["bytes"].as_u64()? > 0 && v["state"] == "live").then_some(())
    })
    .await
    .expect("the cursor subscriber caught up and counted its events");

    let conn = by_relay["conn"].as_str().unwrap().to_string();
    drop(relay);
    let r = until_total(&s, 1).await;
    let gone = r["recentDisconnects"].as_array().unwrap();
    let g = gone.iter().find(|v| v["conn"] == conn.as_str()).expect("recently gone");
    assert!(g["reason"].as_str().is_some_and(|r| r.starts_with("client")), "{g}");
    assert!(g["disconnectedAt"].as_u64().is_some());
    drop(other);
    until_total(&s, 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_every_nodes_subscribers() {
    let store: Arc<object_store::memory::InMemory> = Arc::new(object_store::memory::InMemory::new());
    let a = cluster_node("fsub-a", store.clone(), 4, |_| {}).await;
    let b = cluster_node("fsub-b", store.clone(), 4, |_| {}).await;
    eventually(Duration::from_secs(20), || async {
        [&a, &b]
            .iter()
            .all(|s| {
                let c = s.app.cluster.as_ref().unwrap();
                c.peers().iter().any(|l| l.node_id != c.cfg.node_id)
            })
            .then_some(())
    })
    .await
    .expect("two-node cluster formed");
    let _sa = a.subscribe(None).await;
    let _sb = b.subscribe(None).await;
    for from in [&a, &b] {
        let r = until_total(from, 2).await;
        let mut nodes: Vec<&str> =
            r["subscribers"].as_array().unwrap().iter().map(|v| v["node"].as_str().unwrap()).collect();
        nodes.sort();
        assert_eq!(nodes, ["fsub-a", "fsub-b"], "{r}");
        assert_eq!(r["nodes"].as_array().unwrap().len(), 2, "{r}");
        assert!(r.get("unreachableNodes").is_none(), "{r}");
    }
}
