//! Handle domains added at runtime (src/handle_domains.rs,
//! src/xrpc/handle_domains.rs): describeServer lists them after the
//! primary, accounts are created and resolve under them (resolveHandle,
//! `/.well-known/atproto-did`, `/tls-check`), removal is refused while
//! accounts hold handles under a domain unless forced, the primary can't be
//! removed, bad domains are refused, invite codes can be limited to one
//! domain, and a change on one node is served by its peers at once.

use crate::common::*;
use std::sync::Arc;

async fn add(s: &TestServer, domain: &str) -> Resp {
    s.xrpc.post("vlpds.admin.addHandleDomain", &json!({"domain": domain}), &Auth::Admin).await
}

async fn remove(s: &TestServer, domain: &str, force: bool) -> Resp {
    s.xrpc.post("vlpds.admin.removeHandleDomain", &json!({"domain": domain, "force": force}), &Auth::Admin).await
}

async fn list(s: &TestServer) -> J {
    s.xrpc.get("vlpds.admin.listHandleDomains", &[], &Auth::Admin).await.ok()
}

async fn domains(s: &TestServer) -> J {
    s.xrpc.get("com.atproto.server.describeServer", &[], &Auth::None).await.ok()["availableUserDomains"].clone()
}

fn accounts(l: &J, domain: &str) -> Option<u64> {
    l["domains"].as_array().unwrap().iter().find(|d| d["domain"] == json!(domain)).and_then(|d| d["accounts"].as_u64())
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
async fn added_domain_serves_handles() {
    let s = TestServer::spawn().await;
    let primary = s.create_account("hdp").await;
    // one domain: describeServer as before
    assert_eq!(domains(&s).await, json!([format!(".{HANDLE_DOMAIN}")]));
    let unknown = format!("{}.group-a.test", unique_name("hd"));
    s.xrpc
        .post(
            "com.atproto.server.createAccount",
            &json!({"handle": unknown, "email": "u@example.com", "password": PASSWORD}),
            &Auth::None,
        )
        .await
        .err(400, "UnsupportedDomain");

    let r = add(&s, "group-a.test").await.ok();
    assert_eq!(r["domain"], "group-a.test");
    assert_eq!(domains(&s).await, json!([format!(".{HANDLE_DOMAIN}"), ".group-a.test"]));

    let a = s.create_account_with(&format!("{}.group-a.test", unique_name("hd")), PASSWORD).await;
    assert_eq!(s.resolve_handle(&a.handle).await.ok()["did"], json!(a.did));
    assert_eq!(s.resolve_handle(&a.handle.to_uppercase()).await.ok()["did"], json!(a.did));
    s.resolve_handle(&format!("nobody{}.group-a.test", unique_name("x"))).await.err(400, "HandleNotFound");
    assert_eq!(well_known(&s, &a.handle).await, (200, a.did.clone()));
    assert_eq!(well_known(&s, &format!("{}:443", a.handle.to_uppercase())).await, (200, a.did.clone()));
    assert_eq!(well_known(&s, &format!("nobody{}.group-a.test", unique_name("x"))).await.0, 404);
    assert_eq!(tls_check(&s, &a.handle).await, 200);
    assert_eq!(tls_check(&s, &primary.handle).await, 200);
    assert_eq!(tls_check(&s, &format!("nobody{}.group-a.test", unique_name("x"))).await, 404);
    assert_eq!(tls_check(&s, "x.group-b.test").await, 400);

    // the same rules as the primary's: one label of 3-18
    for bad in ["ab.group-a.test", "a.b.c.group-a.test", "group-a.test"] {
        let body = json!({"handle": bad, "email": "x@example.com", "password": PASSWORD});
        let r = s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await;
        assert_eq!(r.status, 400, "{bad}: {}", r.text());
    }
    // a primary account can move to the new domain, and back
    let moved = format!("{}.group-a.test", unique_name("mv"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": moved}), &primary.auth()).await.ok();
    assert_eq!(s.resolve_handle(&moved).await.ok()["did"], json!(primary.did));
    let c = s.xrpc.get("vlpds.identity.checkHandle", &[("name", "free-name.group-a.test")], &primary.auth()).await.ok();
    assert_eq!((c["kind"].as_str(), c["status"].as_str()), (Some("service"), Some("available")));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": primary.handle}), &primary.auth()).await.ok();

    // counts and removal
    let l = list(&s).await;
    assert_eq!(l["primary"], json!(HANDLE_DOMAIN));
    assert_eq!(l["domains"][0]["primary"], json!(true));
    assert_eq!(accounts(&l, "group-a.test"), Some(1));
    assert_eq!(accounts(&l, HANDLE_DOMAIN), Some(1));
    let r = remove(&s, "group-a.test", false).await;
    r.err(409, "DomainInUse");
    assert!(r.text().contains("1 active account"), "{}", r.text());
    remove(&s, HANDLE_DOMAIN, true).await.err(400, "CannotRemovePrimary");
    remove(&s, "group-b.test", false).await.err(400, "DomainNotFound");
    let r = remove(&s, "group-a.test", true).await.ok();
    assert_eq!(r["accounts"], 1);
    assert_eq!(domains(&s).await, json!([format!(".{HANDLE_DOMAIN}")]));
    // the account stays and this PDS still knows its handle (resolveHandle
    // answers from local accounts first, as for any handle), but HTTPS
    // verification and certificates stop, so elsewhere it no longer resolves
    s.account_info(&a.did).await.ok();
    assert_eq!(s.resolve_handle(&a.handle).await.ok()["did"], json!(a.did));
    assert_eq!(well_known(&s, &a.handle).await.0, 404);
    assert_eq!(tls_check(&s, &a.handle).await, 400);
    // an unused domain goes without force
    add(&s, "group-c.test").await.ok();
    remove(&s, "group-c.test", false).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_domains_are_refused() {
    let s = TestServer::spawn().await;
    for bad in ["Group.test", "10.0.0.1", "::1", "test", "fly.dev", "co.uk", "x.local", "a_b.test", " "] {
        let r = add(&s, bad).await;
        assert_eq!(r.status, 400, "{bad:?}: {}", r.text());
    }
    add(&s, HANDLE_DOMAIN).await.err(400, "DomainExists");
    add(&s, "group-a.test").await.ok();
    add(&s, "group-a.test").await.err(400, "DomainExists");
    // a domain nested in another is fine: the longest match wins
    add(&s, "at.group-a.test").await.ok();
    let a = s.create_account_with(&format!("{}.at.group-a.test", unique_name("nest")), PASSWORD).await;
    let l = list(&s).await;
    assert_eq!((accounts(&l, "at.group-a.test"), accounts(&l, "group-a.test")), (Some(1), Some(0)));
    assert_eq!(s.resolve_handle(&a.handle).await.ok()["did"], json!(a.did));
    s.xrpc.post("vlpds.admin.addHandleDomain", &json!({"domain": "x.test"}), &Auth::None).await.err_status(401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invite_codes_can_be_limited_to_a_domain() {
    let s = TestServer::spawn_with(|c| c.invite_required = true).await;
    add(&s, "group-a.test").await.ok();
    s.xrpc
        .post(
            "com.atproto.server.createInviteCode",
            &json!({"useCount": 5, "handleDomain": "group-z.test"}),
            &Auth::Admin,
        )
        .await
        .err(400, "InvalidRequest");
    let code = s
        .xrpc
        .post(
            "com.atproto.server.createInviteCode",
            &json!({"useCount": 5, "handleDomain": "group-a.test"}),
            &Auth::Admin,
        )
        .await
        .ok()["code"]
        .as_str()
        .unwrap()
        .to_string();
    let s = &s;
    let create = |handle: String| {
        let body = json!({"handle": handle, "email": format!("{}@example.com", unique_name("e")), "password": PASSWORD, "inviteCode": code});
        async move { s.xrpc.post("com.atproto.server.createAccount", &body, &Auth::None).await }
    };
    let r = create(format!("{}.{HANDLE_DOMAIN}", unique_name("inv"))).await;
    r.err(400, "InvalidInviteCode");
    assert!(r.text().contains("group-a.test"), "{}", r.text());
    create(format!("{}.group-a.test", unique_name("inv"))).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_serve_a_change_at_once() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = cluster_node("hd-a", store.clone(), 4, |_| {}).await;
    let b = cluster_node("hd-b", store.clone(), 4, |_| {}).await;
    balanced(&[&a, &b]).await;
    add(&a, "group-a.test").await.ok();
    // nudged: no wait for b's refresh
    assert_eq!(domains(&b).await, json!([format!(".{HANDLE_DOMAIN}"), ".group-a.test"]));
    let acct = a.create_account_with(&format!("{}.group-a.test", unique_name("hdc")), PASSWORD).await;
    assert!(b.app.remote_owner(&acct.did).is_some(), "owned by a");
    assert_eq!(well_known(&b, &acct.handle).await, (200, acct.did.clone()));
    assert_eq!(tls_check(&b, &acct.handle).await, 200);
    // b counts a's shards
    assert_eq!(accounts(&list(&b).await, "group-a.test"), Some(1));
    remove(&b, "group-a.test", false).await.err(409, "DomainInUse");
    remove(&b, "group-a.test", true).await.ok();
    assert_eq!(domains(&a).await, json!([format!(".{HANDLE_DOMAIN}")]));
    assert_eq!(tls_check(&a, &acct.handle).await, 400);
}
