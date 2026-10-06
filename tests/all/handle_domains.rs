//! Several service handle domains (`--handle-domains`, DESIGN.md "Handle
//! domains"): every one is offered and served, the longest suffix decides
//! which a handle is under, and an account's own handle changes stay in its
//! home domain (an admin's don't).
use crate::common::*;

const SECOND: &str = "second.test";

async fn two_domains() -> TestServer {
    TestServer::spawn_with(|c| c.handle_domains = vec![HANDLE_DOMAIN.into(), format!(".{SECOND}")]).await
}

fn under(domain: &str, prefix: &str) -> String {
    format!("{}.{domain}", unique_name(prefix))
}

async fn update_handle(s: &TestServer, a: &TestAccount, handle: &str) -> Resp {
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": handle}), &a.auth()).await
}

async fn home(s: &TestServer, a: &TestAccount) -> J {
    s.xrpc.get("vlpds.identity.getHandleDomain", &[], &a.auth()).await.ok()["domain"].clone()
}

async fn stored_home(s: &TestServer, did: &str) -> Option<J> {
    let Ok(a) = s.app.account(did).await else { panic!("account {did} unreadable") };
    a.extra.get(vlpds::handle_domains::HOME_KEY).cloned()
}

async fn well_known(s: &TestServer, host: &str) -> (u16, String) {
    let r = reqwest::Client::new()
        .get(format!("{}/.well-known/atproto-did", s.url))
        .header("host", host)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

async fn tls_check(s: &TestServer, domain: &str) -> u16 {
    reqwest::get(format!("{}/tls-check?domain={domain}", s.url)).await.unwrap().status().as_u16()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_domain_is_offered_and_served() {
    let s = two_domains().await;
    let d = s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok();
    assert_eq!(d["availableUserDomains"], json!([format!(".{HANDLE_DOMAIN}"), format!(".{SECOND}")]));

    for domain in [HANDLE_DOMAIN, SECOND] {
        let a = s.create_account_with(&under(domain, "srv"), PASSWORD).await;
        assert_eq!(s.resolve_handle(&a.handle).await.ok()["did"], json!(a.did));
        assert_eq!(well_known(&s, &a.handle).await, (200, a.did.clone()));
        assert_eq!(tls_check(&s, &a.handle).await, 200);
        assert_eq!(home(&s, &a).await, json!(format!(".{domain}")));
    }
    assert_eq!(tls_check(&s, &under(SECOND, "nobody")).await, 404);
    assert_eq!(tls_check(&s, "alice.elsewhere.test").await, 400);
    assert_eq!(well_known(&s, "alice.elsewhere.test").await.0, 404);
}

/// The reference takes the first listed domain a handle ends with; here the
/// longest wins, so a domain under another one works in either order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_domains_take_the_longest_match() {
    let s = TestServer::spawn_with(|c| c.handle_domains = vec!["nest.test".into(), "at.nest.test".into()]).await;
    let a = s.create_account_with(&under("at.nest.test", "deep"), PASSWORD).await;
    assert_eq!(home(&s, &a).await, json!(".at.nest.test"));
    let r = s
        .xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": under("x.nest.test", "dotted"), "password": PASSWORD, "email": "nest@example.com"}),
            &Auth::None,
        )
        .await;
    r.err(400, "InvalidHandle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own_handle_changes_stay_in_the_home_domain() {
    let s = two_domains().await;
    let a = s.create_account_with(&under(SECOND, "home"), PASSWORD).await;

    // another domain's namespace: refused, and the account page says so
    let other = under(HANDLE_DOMAIN, "away");
    update_handle(&s, &a, &other).await.err(400, "UnsupportedDomain");
    let c = s.xrpc.get("vlpds.identity.checkHandle", &[("name", &other)], &a.auth()).await.ok();
    assert_eq!((c["status"].as_str(), c["kind"].as_str()), (Some("invalid"), Some("service")), "{c}");
    let c = s.xrpc.get("vlpds.identity.checkHandle", &[("name", &under(SECOND, "free"))], &a.auth()).await.ok();
    assert_eq!(c["status"], json!("available"), "{c}");

    // within the home domain
    update_handle(&s, &a, &under(SECOND, "moved")).await.ok();
    assert_eq!(stored_home(&s, &a.did).await, None);

    // a handle of its own (dev mode: no proof), and back home only
    update_handle(&s, &a, "alice-home.example.org").await.ok();
    assert_eq!(stored_home(&s, &a.did).await, Some(json!(SECOND)));
    assert_eq!(home(&s, &a).await, json!(format!(".{SECOND}")));
    update_handle(&s, &a, "alice-home2.example.org").await.ok();
    update_handle(&s, &a, &under(HANDLE_DOMAIN, "back")).await.err(400, "UnsupportedDomain");
    update_handle(&s, &a, &under(SECOND, "back")).await.ok();
    assert_eq!(stored_home(&s, &a.did).await, None);

    // an admin may put it anywhere
    let anywhere = under(HANDLE_DOMAIN, "admin");
    s.xrpc
        .post("com.atproto.admin.updateAccountHandle", &json!({"did": a.did, "handle": anywhere}), &Auth::Admin)
        .await
        .ok();
    assert_eq!(home(&s, &a).await, json!(format!(".{HANDLE_DOMAIN}")));
}

/// One domain: nothing changes, and the home domain is never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_domain_behaves_as_before() {
    let s = TestServer::spawn().await;
    let a = s.create_account("solo").await;
    update_handle(&s, &a, "solo-own.example.org").await.ok();
    assert_eq!(stored_home(&s, &a.did).await, None);
    assert_eq!(home(&s, &a).await, json!(format!(".{HANDLE_DOMAIN}")));
    update_handle(&s, &a, &under(HANDLE_DOMAIN, "solo")).await.ok();
}

/// Suggestions for a taken name stay under the domain asked about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn availability_suggestions_keep_the_domain() {
    let s = two_domains().await;
    let a = s.create_account_with(&under(SECOND, "sugg"), PASSWORD).await;
    let r = s.xrpc.get("com.atproto.temp.checkHandleAvailability", &[("handle", &a.handle)], &Auth::None).await.ok();
    let sugg = r["result"]["suggestions"].as_array().unwrap();
    assert!(!sugg.is_empty(), "{r}");
    for x in sugg {
        assert!(x["handle"].as_str().unwrap().ends_with(&format!(".{SECOND}")), "{r}");
    }
}

// ---- domains managed at runtime (vlpds.admin.*HandleDomain*) ----

async fn admin(s: &TestServer, nsid: &str, domain: &str) -> Resp {
    s.xrpc.post(nsid, &json!({"domain": domain}), &Auth::Admin).await
}

async fn listed(s: &TestServer) -> J {
    s.xrpc.get("vlpds.admin.getHandleDomains", &[], &Auth::Admin).await.ok()["domains"].clone()
}

async fn available(s: &TestServer) -> J {
    s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok()["availableUserDomains"].clone()
}

fn fresh_domain(prefix: &str) -> String {
    format!("{}.test", unique_name(prefix))
}

async fn try_create(s: &TestServer, handle: &str) -> Resp {
    let email = format!("{}@example.com", unique_name("e"));
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": handle, "password": PASSWORD, "email": email}),
            &Auth::None,
        )
        .await
}

/// Add, serve, retire (existing handles keep resolving, no new ones),
/// refuse removal while an account holds one, cancel by adding again,
/// then remove once it's gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_domain_lifecycle() {
    let s = TestServer::spawn_with(|c| c.handle_domain_retire_grace = std::time::Duration::ZERO).await;
    let dom = fresh_domain("mng");
    assert_eq!(listed(&s).await, json!([{"domain": HANDLE_DOMAIN, "source": "config", "state": "active"}]));

    let r = admin(&s, "vlpds.admin.addHandleDomain", &format!(".{}", dom.to_uppercase())).await.ok();
    assert_eq!(
        (r["domain"].as_str(), r["source"].as_str(), r["state"].as_str()),
        (Some(dom.as_str()), Some("managed"), Some("active")),
        "{r}"
    );
    assert_eq!(available(&s).await, json!([format!(".{HANDLE_DOMAIN}"), format!(".{dom}")]));
    let a = s.create_account_with(&under(&dom, "m"), PASSWORD).await;
    assert_eq!(well_known(&s, &a.handle).await, (200, a.did.clone()));
    assert_eq!(home(&s, &a).await, json!(format!(".{dom}")));

    // step 1: retiring
    let r = admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.ok();
    assert_eq!((r["state"].as_str(), r["blockingAccounts"].as_u64()), (Some("retiring"), Some(1)), "{r}");
    assert!(r["removableAfter"].is_string(), "{r}");
    assert_eq!(available(&s).await, json!([format!(".{HANDLE_DOMAIN}")]));
    try_create(&s, &under(&dom, "late")).await.err(400, "UnsupportedDomain");
    update_handle(&s, &a, &under(&dom, "rename")).await.err(400, "UnsupportedDomain");
    assert_eq!(home(&s, &a).await, J::Null);
    assert_eq!(well_known(&s, &a.handle).await, (200, a.did.clone()));
    assert_eq!(tls_check(&s, &a.handle).await, 200);

    // step 2 is refused while the account is there
    let r = admin(&s, "vlpds.admin.removeHandleDomain", &dom).await;
    r.err(400, "HandleDomainInUse");
    assert!(r.text().contains(&a.handle), "{}", r.text());

    // adding it again puts it back in service
    assert_eq!(admin(&s, "vlpds.admin.addHandleDomain", &dom).await.ok()["state"], json!("active"));
    assert_eq!(home(&s, &a).await, json!(format!(".{dom}")));
    admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.ok();

    s.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": a.did}), &Auth::Admin).await.ok();
    let r = admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.ok();
    assert_eq!(r["state"], json!("removed"), "{r}");
    assert_eq!(listed(&s).await.as_array().unwrap().len(), 1);
    assert_eq!(well_known(&s, &under(&dom, "gone")).await.0, 404);
    admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.err(400, "HandleDomainNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_domain_refusals() {
    let s = TestServer::spawn().await;
    admin(&s, "vlpds.admin.removeHandleDomain", HANDLE_DOMAIN).await.err(400, "HandleDomainConfigured");
    admin(&s, "vlpds.admin.addHandleDomain", "not_a_domain").await.err(400, "InvalidRequest");
    let a = s.create_account("nonadmin").await;
    s.xrpc.post("vlpds.admin.addHandleDomain", &json!({"domain": "x.test"}), &a.auth()).await.err_status(401);
    // a configured domain is already there
    assert_eq!(admin(&s, "vlpds.admin.addHandleDomain", HANDLE_DOMAIN).await.ok()["source"], json!("config"));

    // an own-domain handle that would come under the new domain
    let dom = fresh_domain("recl");
    update_handle(&s, &a, &format!("own.{dom}")).await.ok();
    let r = admin(&s, "vlpds.admin.addHandleDomain", &dom).await;
    r.err(400, "HandleDomainInUse");
    assert!(r.text().contains(&a.did), "{}", r.text());

    // within the grace period the removal waits
    let dom = fresh_domain("grace");
    admin(&s, "vlpds.admin.addHandleDomain", &dom).await.ok();
    admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.ok();
    admin(&s, "vlpds.admin.removeHandleDomain", &dom).await.err(400, "HandleDomainRetiring");
}

/// Added on one node, every node serves it at once (the others are asked
/// to re-read); retiring reaches them the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_domains_reach_every_node() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let grace = |c: &mut vlpds::server::Config| c.handle_domain_retire_grace = std::time::Duration::ZERO;
    let a = cluster_node("hd-a", store.clone(), 8, grace).await;
    let b = cluster_node("hd-b", store.clone(), 8, grace).await;
    let c = cluster_node("hd-c", store.clone(), 8, grace).await;
    balanced(&[&a, &b, &c]).await;

    let dom = fresh_domain("clu");
    admin(&a, "vlpds.admin.addHandleDomain", &dom).await.ok();
    for n in [&a, &b, &c] {
        assert_eq!(available(n).await[1], json!(format!(".{dom}")));
    }
    let acct = c.create_account_with(&under(&dom, "c"), PASSWORD).await;
    assert_eq!(well_known(&b, &acct.handle).await, (200, acct.did.clone()));

    admin(&b, "vlpds.admin.removeHandleDomain", &dom).await.ok();
    for n in [&a, &b, &c] {
        assert_eq!(available(n).await.as_array().unwrap().len(), 1);
        try_create(n, &under(&dom, "late")).await.err(400, "UnsupportedDomain");
    }
    // the account is on some node's shards: the cluster-wide scan finds it
    admin(&a, "vlpds.admin.removeHandleDomain", &dom).await.err(400, "HandleDomainInUse");
    a.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": acct.did}), &Auth::Admin).await.ok();
    assert_eq!(admin(&c, "vlpds.admin.removeHandleDomain", &dom).await.ok()["state"], json!("removed"));
    for n in [&a, &b, &c] {
        assert_eq!(well_known(n, &acct.handle).await.0, 404);
    }
}

/// A node started on a bucket that already has managed domains serves them
/// from the first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_reads_the_managed_domains_at_start() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let a = cluster_node("hd-s1", store.clone(), 8, |_| {}).await;
    let dom = fresh_domain("boot");
    admin(&a, "vlpds.admin.addHandleDomain", &dom).await.ok();
    let b = cluster_node("hd-s2", store.clone(), 8, |_| {}).await;
    assert_eq!(available(&b).await[1], json!(format!(".{dom}")));
}

/// A live peer that can't answer might hold an account under the domain or
/// not learn about it: the change waits until every node answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_domain_changes_need_every_node() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let a = cluster_node("hd-g1", store.clone(), 8, |_| {}).await;
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = format!("https://{}", dead.local_addr().unwrap());
    drop(dead);
    let ghost = spawn_ghost(store.clone(), ghost_lease("hd-ghost", dead_addr));
    // once a's membership step has seen its lease
    retry("the ghost is a peer", || async {
        let r = a.xrpc.get("com.atproto.admin.searchAccounts", &[], &Auth::Admin).await.ok();
        r["unreachableNodes"].as_array().is_some_and(|v| v.iter().any(|n| n == "hd-ghost")).then_some(())
    })
    .await;
    let dom = fresh_domain("ghost");
    let r = admin(&a, "vlpds.admin.addHandleDomain", &dom).await;
    r.err(503, "HandleDomainScanIncomplete");
    assert!(r.text().contains("hd-ghost"), "{}", r.text());
    assert_eq!(listed(&a).await.as_array().unwrap().len(), 1, "nothing stored");
    ghost.abort();
}

/// `vlpds admin handle-domains list|add|remove`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_admin_cli_manages_handle_domains() {
    let s = TestServer::spawn_with(|c| c.handle_domain_retire_grace = std::time::Duration::ZERO).await;
    let dom = fresh_domain("cli");
    let run = |args: Vec<String>| {
        let url = s.url.clone();
        async move {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let (r, out) = admin_cli(&url, &args).await;
            r.unwrap_or_else(|e| panic!("{e:#}: {out}"));
            out
        }
    };
    let out = run(vec!["handle-domains".into(), "add".into(), dom.clone()]).await;
    assert!(out.contains(&format!("{dom}: active (managed)")), "{out}");
    let out = run(vec!["handle-domains".into(), "list".into()]).await;
    assert!(out.contains(HANDLE_DOMAIN) && out.contains(&dom), "{out}");
    let out = run(vec!["handle-domains".into(), "remove".into(), dom.clone()]).await;
    assert!(out.contains("retiring") && out.contains("0 account(s)"), "{out}");
    let out = run(vec!["handle-domains".into(), "remove".into(), dom.clone()]).await;
    assert!(out.contains(&format!("{dom}: removed")), "{out}");
}
