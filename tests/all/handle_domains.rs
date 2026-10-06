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
