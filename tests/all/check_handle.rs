//! vlpds.identity.checkHandle, the account page's check before
//! updateHandle: availability under the handle domain, and DNS / HTTPS
//! proof of a custom domain reported per method, agreeing with what
//! updateHandle then accepts.
use crate::common::*;
use std::collections::HashMap;
use std::sync::Arc;

async fn check(s: &TestServer, a: &TestAccount, name: &str) -> J {
    check_raw(s, a, name).await.ok()
}

async fn check_raw(s: &TestServer, a: &TestAccount, name: &str) -> Resp {
    s.xrpc.get("vlpds.identity.checkHandle", &[("name", name)], &a.auth()).await
}

fn h(label: &str) -> String {
    format!("{label}.{HANDLE_DOMAIN}")
}

#[derive(Default)]
struct StubTxt(parking_lot::Mutex<HashMap<String, Vec<String>>>);

impl vlpds::handle_resolver::TxtResolver for StubTxt {
    fn txt<'a>(&'a self, name: &'a str) -> futures::future::BoxFuture<'a, Result<Vec<String>, String>> {
        let got = self.0.lock().get(name).cloned();
        Box::pin(async move { got.ok_or_else(|| "NXDOMAIN".to_string()) })
    }
}

impl StubTxt {
    fn set(&self, handle: &str, records: &[&str]) {
        self.0.lock().insert(format!("_atproto.{handle}."), records.iter().map(|r| r.to_string()).collect());
    }
}

/// `.well-known/atproto-did` bodies by handle; anything else a 404.
#[derive(Default)]
struct StubWeb(parking_lot::Mutex<HashMap<String, String>>);

impl vlpds::handle_resolver::WellKnownFetcher for StubWeb {
    fn fetch<'a>(&'a self, handle: &'a str) -> futures::future::BoxFuture<'a, Result<String, String>> {
        let got = self.0.lock().get(handle).cloned();
        Box::pin(async move { got.ok_or_else(|| "status 404 Not Found".to_string()) })
    }
}

/// A server that requires the proof (dev mode off), with stub DNS and web.
async fn proving_server(dns: &Arc<StubTxt>, web: &Arc<StubWeb>) -> TestServer {
    let t = vlpds::handle_resolver::TxtResolverRef(dns.clone());
    let w = vlpds::handle_resolver::WellKnownRef(web.clone());
    TestServer::spawn_with(move |c| {
        c.dev_mode = false;
        c.txt_resolver = Some(t);
        c.well_known_fetcher = Some(w);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_domain_availability() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;

    let free = h(&unique_name("free"));
    let r = check(&s, &a, &free).await;
    assert_eq!((r["status"].as_str(), r["kind"].as_str()), (Some("available"), Some("service")), "{r}");
    assert_eq!(r["handle"], json!(free));
    // typed loosely: @, case, a trailing dot
    let r = check(&s, &a, &format!("@{}.", free.to_uppercase())).await;
    assert_eq!((r["status"].as_str(), r["handle"].as_str()), (Some("available"), Some(free.as_str())), "{r}");

    assert_eq!(check(&s, &a, &b.handle).await["status"], json!("taken"));
    assert_eq!(check(&s, &a, &a.handle).await["status"], json!("current"));
    assert_eq!(check(&s, &a, &h("postmaster")).await["status"], json!("reserved"));

    for (label, says) in [
        ("ab", "too short"),
        ("abcdefghijklmnopqrs", "too long"),
        ("a_b", "letters, numbers and hyphens"),
        ("-abc", "hyphen"),
        ("abc-", "hyphen"),
        ("me.abc", "can't contain dots"),
    ] {
        let r = check(&s, &a, &h(label)).await;
        assert_eq!(r["status"], json!("invalid"), "{label}: {r}");
        assert!(r["message"].as_str().unwrap().contains(says), "{label}: {r}");
    }
    let r = check(&s, &a, "not a domain").await;
    assert_eq!(r["status"], json!("invalid"), "{r}");
    let r = check(&s, &a, "alice.local").await;
    assert!(r["message"].as_str().unwrap().contains(".local"), "{r}");

    // what it says is what updateHandle does
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": free}), &a.auth()).await.ok();
    assert_eq!(check(&s, &b, &free).await["status"], json!("taken"));
    assert_eq!(check(&s, &a, &free).await["status"], json!("current"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn needs_the_account_itself() {
    let s = TestServer::spawn().await;
    s.xrpc.get("vlpds.identity.checkHandle", &[("name", "alice.example.com")], &Auth::None).await.err_status(401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own_domain_over_dns() {
    let (dns, web) = (Arc::new(StubTxt::default()), Arc::new(StubWeb::default()));
    let s = proving_server(&dns, &web).await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let ext = format!("{}.external", unique_name("alice"));

    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["kind"].as_str()), (Some("unverified"), Some("external")), "{r}");
    assert_eq!(r["proofRequired"], json!(true));
    assert_eq!((r["dns"]["result"].as_str(), r["http"]["result"].as_str()), (Some("none"), Some("none")), "{r}");

    dns.set(&ext, &[&format!("did={}", b.did)]);
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["dns"]["result"].as_str()), (Some("unverified"), Some("other")), "{r}");
    assert_eq!(r["dns"]["did"], json!(b.did));

    dns.set(&ext, &[&format!("did={}", a.did), &format!("did={}", b.did)]);
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["dns"]["result"].as_str()), (Some("unverified"), Some("several")), "{r}");

    dns.set(&ext, &["v=spf1 -all", &format!("did={}", a.did)]);
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["method"].as_str()), (Some("verified"), Some("dns")), "{r}");
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": ext}), &a.auth()).await.ok();
    assert_eq!(check(&s, &a, &ext).await["status"], json!("current"));
    // and now it's a's, here
    dns.set(&ext, &[&format!("did={}", b.did)]);
    assert_eq!(check(&s, &b, &ext).await["status"], json!("taken"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own_domain_over_https() {
    let (dns, web) = (Arc::new(StubTxt::default()), Arc::new(StubWeb::default()));
    let s = proving_server(&dns, &web).await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let ext = format!("{}.external", unique_name("alice"));

    web.0.lock().insert(ext.clone(), "<!doctype html>".into());
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["http"]["result"].as_str()), (Some("unverified"), Some("none")), "{r}");
    assert!(r["http"]["detail"].as_str().unwrap().contains("doesn't hold a DID"), "{r}");

    web.0.lock().insert(ext.clone(), b.did.clone());
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["http"]["result"].as_str()), (Some("unverified"), Some("other")), "{r}");

    web.0.lock().insert(ext.clone(), a.did.clone());
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["method"].as_str()), (Some("verified"), Some("http")), "{r}");
    assert_eq!(r["dns"]["result"], json!("none"));

    // a DNS record for someone else wins over the file, as in updateHandle
    dns.set(&ext, &[&format!("did={}", b.did)]);
    let r = check(&s, &a, &ext).await;
    assert_eq!((r["status"].as_str(), r["http"]["result"].as_str()), (Some("unverified"), Some("match")), "{r}");
    let r = s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": ext}), &a.auth()).await;
    r.err(400, "InvalidRequest");

    dns.0.lock().clear();
    assert_eq!(check(&s, &a, &ext).await["status"], json!("verified"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": ext}), &a.auth()).await.ok();
}

/// Dev mode doesn't need the proof, and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_mode_needs_no_proof() {
    let (dns, web) = (Arc::new(StubTxt::default()), Arc::new(StubWeb::default()));
    let (t, w) =
        (vlpds::handle_resolver::TxtResolverRef(dns.clone()), vlpds::handle_resolver::WellKnownRef(web.clone()));
    let s = TestServer::spawn_with(move |c| {
        c.txt_resolver = Some(t);
        c.well_known_fetcher = Some(w);
    })
    .await;
    let a = s.create_account("alice").await;
    let r = check(&s, &a, "alice.example.com").await;
    assert_eq!((r["status"].as_str(), r["proofRequired"].as_bool()), (Some("unverified"), Some(false)), "{r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limited_per_account() {
    let s = TestServer::spawn_with(|c| c.rate_limits_enabled = true).await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let name = h(&unique_name("free"));
    for i in 0..60 {
        let r = check_raw(&s, &a, &name).await;
        assert!(r.is_ok(), "check {i}: {}", r.text());
    }
    let r = check_raw(&s, &a, &name).await;
    r.err(429, "RateLimitExceeded");
    assert_eq!(r.header("ratelimit-limit").as_deref(), Some("60"));
    // another account has its own bucket
    check_raw(&s, &b, &name).await.ok();
}
