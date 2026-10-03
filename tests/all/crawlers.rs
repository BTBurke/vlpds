//! Relay crawl requests (src/xrpc/crawlers.rs) against local stand-in
//! relays: one at startup, one more after activity once the interval has
//! passed and none without activity; the console's list and interval
//! round-trip through the bucket and override the flags; a two-node cluster
//! sends once, not once per node.

use crate::common::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::time::Duration;

struct Relay {
    url: String,
    calls: Arc<AtomicUsize>,
    status: Arc<AtomicU16>,
}

impl Relay {
    async fn start() -> Relay {
        use axum::routing::post;
        let calls = Arc::new(AtomicUsize::new(0));
        let status = Arc::new(AtomicU16::new(200));
        let (c, s) = (calls.clone(), status.clone());
        let app = axum::Router::new().route(
            "/xrpc/com.atproto.sync.requestCrawl",
            post(move |axum::Json(b): axum::Json<J>| {
                let (c, s) = (c.clone(), s.clone());
                async move {
                    assert!(b["hostname"].as_str().is_some_and(|h| h.starts_with("127.0.0.1:")), "{b}");
                    c.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::from_u16(s.load(Ordering::SeqCst)).unwrap(), axum::Json(json!({})))
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        Relay { url, calls, status }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

async fn get(s: &TestServer) -> J {
    s.xrpc.get("vlpds.admin.getCrawlers", &[], &Auth::Admin).await.ok()
}

async fn set(s: &TestServer, body: J) -> Resp {
    s.xrpc.post("vlpds.admin.setCrawlers", &body, &Auth::Admin).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_then_after_activity_throttled() {
    let relay = Relay::start().await;
    let u = relay.url.clone();
    let s = TestServer::spawn_with(move |c| {
        c.crawlers = vec![u];
        c.crawl_interval = Duration::from_secs(3);
    })
    .await;
    wait_until("startup requestCrawl", Duration::from_secs(10), || relay.calls() == 1).await;
    let v = get(&s).await;
    assert_eq!(v["relaysSource"], "flags", "{v}");
    assert_eq!(v["intervalSecs"], 3, "{v}");
    let st = &v["relays"][0]["status"];
    assert_eq!((st["ok"].as_bool(), st["httpStatus"].as_u64()), (Some(true), Some(200)), "{v}");

    // activity: told once the interval since the last ask has passed, not before
    let a = s.create_account("crawl").await;
    s.post(&a, "hello relay").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(relay.calls(), 1, "inside the interval");
    wait_until("requestCrawl after activity", Duration::from_secs(10), || relay.calls() == 2).await;

    // no activity since: nothing more
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(relay.calls(), 2, "no activity, no ask");

    // a rejection is recorded with its status, keeping the last success
    relay.status.store(503, Ordering::SeqCst);
    s.post(&a, "again").await;
    wait_until("requestCrawl after more activity", Duration::from_secs(10), || relay.calls() == 3).await;
    let v = retry("rejection recorded", || async {
        let v = get(&s).await;
        (v["relays"][0]["status"]["httpStatus"] == 503).then_some(v)
    })
    .await;
    let st = &v["relays"][0]["status"];
    assert_eq!(st["ok"], false, "{v}");
    assert!(st["lastSuccessMs"].as_u64().is_some(), "{v}");
    let m = reqwest::get(format!("{}/metrics", s.url)).await.unwrap().text().await.unwrap();
    let series = format!(r#"vlpds_request_crawl_total{{relay="{}",result="rejected"}} 1"#, relay.url);
    assert!(m.lines().any(|l| l == series), "{series} in metrics");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_settings_round_trip_and_override_flags() {
    let relay = Relay::start().await;
    let s = TestServer::spawn().await;
    let v = get(&s).await;
    assert_eq!((v["relays"].as_array().map(Vec::len), v["relaysSource"].as_str()), (Some(0), Some("flags")), "{v}");
    assert_eq!(v["sender"], true, "{v}");
    // crawl-now with nothing configured
    s.xrpc.post("vlpds.admin.requestCrawl", &json!({}), &Auth::Admin).await.err(400, "InvalidRequest");

    for bad in [json!({"relays": ["not a host"]}), json!({"relays": ["https://relay.example.com/path"]}), json!({"intervalSecs": 0})] {
        set(&s, bad.clone()).await.err(400, "InvalidRequest");
    }
    let v = set(&s, json!({"relays": [format!("{}/", relay.url), relay.url], "intervalSecs": 3600})).await.ok();
    assert_eq!(v["relays"].as_array().unwrap().len(), 1, "deduplicated: {v}");
    assert_eq!((v["relaysSource"].as_str(), v["intervalSecs"].as_u64()), (Some("stored"), Some(3600)), "{v}");
    // a new relay is asked at once
    wait_until("requestCrawl to the added relay", Duration::from_secs(10), || relay.calls() == 1).await;
    let v = retry("status recorded", || async {
        let v = get(&s).await;
        v["relays"][0]["status"]["ok"].as_bool().is_some_and(|ok| ok).then_some(v)
    })
    .await;
    assert_eq!(v["relays"][0]["relay"], json!(relay.url), "{v}");

    // crawl now ignores the throttle and reports per relay
    let r = s.xrpc.post("vlpds.admin.requestCrawl", &json!({}), &Auth::Admin).await.ok();
    assert_eq!((r["results"][0]["relay"].as_str(), r["results"][0]["ok"].as_bool()), (Some(relay.url.as_str()), Some(true)), "{r}");
    assert_eq!(relay.calls(), 2);
    // a later activity is within the hour: no ask
    let a = s.create_account("crawlset").await;
    s.post(&a, "quiet").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(relay.calls(), 2);

    // interval only: the list stays
    let v = set(&s, json!({"intervalSecs": 60})).await.ok();
    assert_eq!((v["relays"].as_array().map(Vec::len), v["intervalSecs"].as_u64()), (Some(1), Some(60)), "{v}");
    // null: back to the flags (none here)
    let v = set(&s, json!({"relays": null, "intervalSecs": null})).await.ok();
    assert_eq!((v["relays"].as_array().map(Vec::len), v["relaysSource"].as_str(), v["intervalSource"].as_str()), (Some(0), Some("flags"), Some("flags")), "{v}");
    assert_eq!(v["intervalSecs"], 1200, "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_node_of_a_cluster_sends() {
    let relay = Relay::start().await;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = |id: &'static str| {
        let (store, u) = (store.clone(), relay.url.clone());
        async move { cluster_node(id, store, 4, move |c| c.crawlers = vec![u]).await }
    };
    let a = node("crawl-a").await;
    let b = node("crawl-b").await;
    balanced(&[&a, &b]).await;
    let acct = a.create_account("crawlha").await;
    a.post(&acct, "from a cluster").await;
    wait_until("startup requestCrawl", Duration::from_secs(10), || relay.calls() >= 1).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(relay.calls(), 1, "one ask for the cluster");
    let senders = [&a, &b].iter().filter(|n| cluster(n).leads_slot0()).count();
    assert_eq!(senders, 1);
    // both consoles show the same recorded state
    let (va, vb) = (get(&a).await, get(&b).await);
    assert_eq!(va["relays"], vb["relays"]);
    assert_eq!(va["relays"][0]["status"]["ok"], true, "{va}");
}

/// A node on a local address never asks a remote relay (dev and bench runs
/// keep the default bsky.network off the network). `.invalid` never
/// resolves, so a broken guard fails here without leaving the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_nodes_skip_remote_relays() {
    let s = TestServer::spawn_with(|c| c.crawlers = vec!["relay.invalid".into()]).await;
    let v = retry("skip recorded", || async {
        let v = get(&s).await;
        v["relays"][0]["status"]["error"].as_str().is_some_and(|e| e.starts_with("not sent")).then_some(v)
    })
    .await;
    assert_eq!(v["relays"][0]["status"]["ok"], false, "{v}");
    let m = reqwest::get(format!("{}/metrics", s.url)).await.unwrap().text().await.unwrap();
    assert!(m.contains(r#"vlpds_request_crawl_total{relay="relay.invalid",result="failed"} 0"#));
}
