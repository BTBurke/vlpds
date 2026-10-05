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
        s.app.crawlers.relay_hint(&s.app.store, None, "relay.example.com", None).map(|_| ())
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

/// Answers each PTR and forward query from its tables.
struct StubDns {
    ptr: std::collections::HashMap<std::net::IpAddr, Vec<String>>,
    a: std::collections::HashMap<String, Vec<std::net::IpAddr>>,
}

impl vlpds::ptr::PtrResolver for StubDns {
    fn reverse(&self, ip: std::net::IpAddr) -> futures::future::BoxFuture<'_, Result<Vec<String>, String>> {
        Box::pin(async move { self.ptr.get(&ip).cloned().ok_or_else(|| "NXDOMAIN".into()) })
    }
    fn forward<'a>(&'a self, name: &'a str) -> futures::future::BoxFuture<'a, Result<Vec<std::net::IpAddr>, String>> {
        Box::pin(async move { self.a.get(name).cloned().ok_or_else(|| "NXDOMAIN".into()) })
    }
}

/// A bgp.tools whois stand-in answering every bulk query with `reply`.
async fn stub_whois(reply: &'static str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let mut req = Vec::new();
            let mut buf = [0u8; 4096];
            while !req.ends_with(b"end\n") {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => req.extend_from_slice(&buf[..n]),
                }
            }
            let _ = s.write_all(reply.as_bytes()).await;
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn names_subscribers_by_reverse_dns_and_as() {
    let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
    let dns = StubDns {
        ptr: [
            (ip("5.6.7.8"), vec!["relay3.us-east.relay.example.com.".to_string()]),
            (ip("5.6.7.9"), vec!["spoof.relay.example.com.".to_string()]),
        ]
        .into(),
        a: [
            ("relay3.us-east.relay.example.com".to_string(), vec![ip("5.6.7.8")]),
            ("spoof.relay.example.com".to_string(), vec![ip("9.9.9.9")]),
        ]
        .into(),
    };
    let whois = stub_whois(
        "16276   | 5.6.7.8          | 5.6.0.0/16          | FR | RIPE     | 2016-08-05 | OVH SAS\n\
         0       | 5.6.7.9          | <nil>               |    | Unknown  | 0001-01-01 | ERR_AS_NAME_NOT_FOUND\n",
    )
    .await;
    let s = TestServer::spawn_with(move |c| {
        c.crawlers = vec!["relay.example.com".into()];
        c.trusted_proxies = vec!["127.0.0.1".into()];
        c.ptr_resolver = Some(vlpds::ptr::PtrResolverRef(Arc::new(dns)));
        c.asn_whois = Some(whois);
        c.asn_debounce = Duration::from_millis(50);
    })
    .await;
    eventually(Duration::from_secs(10), || async {
        s.app.crawlers.relay_hint(&s.app.store, None, "", Some("a.relay.example.com")).map(|_| ())
    })
    .await
    .expect("relay hints loaded");

    let ua = "indigo-relay (atproto-relay)";
    let real = Sub::connect_with(&s.ws_url(None), &[("user-agent", ua), ("x-forwarded-for", "5.6.7.8")]).await;
    let spoof = Sub::connect_with(&s.ws_url(None), &[("user-agent", ua), ("x-forwarded-for", "5.6.7.9")]).await;
    // connects only start the lookups; listings answer from the caches
    let r = eventually(Duration::from_secs(10), || async {
        let r = list(&s).await;
        let subs = r["subscribers"].as_array()?;
        (subs.len() == 2 && subs.iter().all(|v| v["ptr"].is_string()) && subs.iter().any(|v| v["asn"].is_u64()))
            .then_some(r)
    })
    .await
    .expect("PTR and AS filled in");
    let by_ip = |a: &str| r["subscribers"].as_array().unwrap().iter().find(|v| v["ip"] == a).cloned().unwrap();
    let v = by_ip("5.6.7.8");
    assert_eq!(v["ptr"], "relay3.us-east.relay.example.com", "{v}");
    assert_eq!(v["ptrVerified"], true, "{v}");
    assert_eq!(v["relay"], "relay.example.com", "a verified PTR in the relay's domain names it: {v}");
    assert_eq!((v["asn"].as_u64(), &v["asName"], &v["asCountry"]), (Some(16276), &json!("OVH SAS"), &json!("FR")));
    let v = by_ip("5.6.7.9");
    assert_eq!(v["ptr"], "spoof.relay.example.com", "{v}");
    assert_eq!(v["ptrVerified"], false, "{v}");
    assert!(v["relay"].is_null(), "an unverified PTR names nothing: {v}");
    assert!(v["asn"].is_null() && v["asName"].is_null(), "{v}");

    drop(real);
    drop(spoof);
    let r = until_total(&s, 0).await;
    let g = r["recentDisconnects"].as_array().unwrap().iter().find(|v| v["ip"] == "5.6.7.8").cloned().unwrap();
    assert_eq!(g["ptr"], "relay3.us-east.relay.example.com", "{g}");
    assert_eq!((g["ptrVerified"].as_bool(), g["asn"].as_u64()), (Some(true), Some(16276)), "{g}");
}
