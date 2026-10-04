//! Regressions from the multi-node e2e exercise (tests/E2E.md): calls that
//! worked on one node but failed in a cluster because the request named its
//! account in a way the forwarding layer didn't route by, or the handler
//! read state only the account's owner has. In-process nodes share one
//! in-memory object store (as in admin_cluster.rs); accounts minted on a
//! node land on that node's shards, so every call below goes through a node
//! that doesn't own the account.

use crate::common::*;
use std::collections::HashSet;
use std::sync::Arc;

const SHARDS: u32 = 8;

async fn node(id: &str, store: &Arc<object_store::memory::InMemory>) -> TestServer {
    cluster_node(id, store.clone(), SHARDS, |_| {}).await
}

async fn cluster(prefix: &str) -> (TestServer, TestServer, TestServer) {
    let store = Arc::new(object_store::memory::InMemory::new());
    let a = node(&format!("{prefix}-a"), &store).await;
    let b = node(&format!("{prefix}-b"), &store).await;
    let c = node(&format!("{prefix}-c"), &store).await;
    balanced(&[&a, &b, &c]).await;
    (a, b, c)
}

fn owns(s: &TestServer, did: &str) -> bool {
    s.app.remote_owner(did).is_none()
}

/// Every page of listRepos at `limit` from `s`, following cursors.
async fn list_repos(s: &TestServer, limit: usize) -> Vec<J> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..100 {
        let lim = limit.to_string();
        let mut q = vec![("limit", lim.as_str())];
        if let Some(c) = &cursor {
            q.push(("cursor", c));
        }
        let r = s.xrpc.get("com.atproto.sync.listRepos", &q, &Auth::None).await.ok();
        let page = r["repos"].as_array().unwrap().clone();
        assert!(page.len() <= limit);
        out.extend(page);
        match r["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return out,
        }
    }
    panic!("too many pages");
}

/// listRepos / listReposByCollection on any node scatter-gather like the
/// admin listings and page across every node's shards (not just its own).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_repos_spans_the_cluster() {
    let (a, b, c) = cluster("e2e-lr").await;
    let nodes = [&a, &b, &c];
    let mut want = HashSet::new();
    let mut heads = std::collections::HashMap::new();
    for n in nodes {
        for _ in 0..3 {
            let acct = n.create_account("lr").await;
            n.create_record(&acct, "com.example.e2e", json!({"x": 1})).await;
            let (cid, rev) = n.latest_commit(&acct.did).await;
            heads.insert(acct.did.clone(), (cid.to_string(), rev));
            want.insert(acct.did);
        }
    }
    for n in nodes {
        for limit in [2, 1000] {
            let repos = list_repos(n, limit).await;
            let got: Vec<&str> = repos.iter().map(|r| r["did"].as_str().unwrap()).collect();
            assert_eq!(got.len(), want.len(), "no duplicates or gaps (limit {limit}): {got:?}");
            assert_eq!(got.iter().map(|d| d.to_string()).collect::<HashSet<_>>(), want);
            for r in &repos {
                let (cid, rev) = &heads[r["did"].as_str().unwrap()];
                assert_eq!((r["head"].as_str().unwrap(), r["rev"].as_str().unwrap()), (cid.as_str(), rev.as_str()));
                assert_eq!(r["active"], json!(true));
            }
        }
        let by_coll = |extra: Option<(&'static str, String)>| {
            let x = n.xrpc.clone();
            let q: Vec<(&str, String)> =
                std::iter::once(("collection", "com.example.e2e".to_string())).chain(extra).collect();
            async move { x.get_multi("com.atproto.sync.listReposByCollection", &q, &Auth::None).await.ok() }
        };
        let r = by_coll(None).await;
        let got: HashSet<String> =
            r["repos"].as_array().unwrap().iter().map(|r| r["did"].as_str().unwrap().to_string()).collect();
        assert_eq!(got, want);
        // paged by DID
        let r = by_coll(Some(("limit", "4".into()))).await;
        assert_eq!(r["repos"].as_array().unwrap().len(), 4);
        let r2 = by_coll(Some(("cursor", r["cursor"].as_str().expect("cursor").to_string()))).await;
        assert_eq!(r2["repos"].as_array().unwrap().len(), want.len() - 4);
    }
}

/// resetPassword names only its token, but the account update needs the
/// owner's worker: any node must route it there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_password_through_any_node() {
    let (a, b, c) = cluster("e2e-rp").await;
    let nodes = [&a, &b, &c];
    for i in 0..3 {
        let owner = nodes[i];
        let acct = owner.create_account("rp").await;
        let (req, reset) = (nodes[(i + 1) % 3], nodes[(i + 2) % 3]);
        assert!(!owns(req, &acct.did) && !owns(reset, &acct.did));
        req.xrpc.post("com.atproto.server.requestPasswordReset", &json!({"email": acct.email}), &Auth::None).await.ok();
        // the dev mailbox is per node (the mail is where the request ran):
        // the newest reset token on any of them
        let mut token = None;
        for n in nodes {
            for msg in mails(n, &acct.email).await {
                if msg["purpose"] == "reset_password" {
                    token = msg["token"].as_str().map(String::from);
                }
            }
        }
        let token = token.expect("reset token mailed");
        let reset_to = |pw: &str| {
            reset.xrpc.post_owned(
                "com.atproto.server.resetPassword",
                json!({"token": token, "password": pw}),
                Auth::None,
            )
        };
        reset_to("brand-new-pw").await.ok();
        owner.create_session(&acct.handle, "brand-new-pw").await.ok();
        req.create_session(&acct.handle, PASSWORD).await.err(401, "AuthenticationRequired");
        // single use
        reset_to("again-pw").await.client_err();
    }
}

/// resolveIdentity's `identifier` query parameter wasn't a routing key:
/// a handle owned elsewhere answered 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolve_identity_routes_by_identifier() {
    let (a, b, c) = cluster("e2e-ri").await;
    let acct = a.create_account("ri").await;
    for n in [&b, &c] {
        for ident in [acct.handle.as_str(), acct.did.as_str()] {
            let r =
                n.xrpc.get("com.atproto.identity.resolveIdentity", &[("identifier", ident)], &Auth::None).await.ok();
            assert_eq!(r["did"], json!(acct.did));
            assert_eq!(r["handle"], json!(acct.handle));
            assert_eq!(r["didDoc"]["id"], json!(acct.did));
        }
    }
}

/// HTTPS handle verification (`/.well-known/atproto-did` by Host): every
/// node answers for every account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn well_known_atproto_did_by_host() {
    let (a, b, c) = cluster("e2e-wk").await;
    let acct = a.create_account("wk").await;
    let http = reqwest::Client::new();
    let get = |n: &TestServer, host: String| {
        http.get(format!("{}/.well-known/atproto-did", n.url)).header("host", host).send()
    };
    for n in [&a, &b, &c] {
        let r = get(n, acct.handle.clone()).await.unwrap();
        assert_eq!(r.status(), 200);
        assert!(r.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
        assert_eq!(r.text().await.unwrap(), acct.did);
        // with a port, and case-insensitively
        let r = get(n, format!("{}:443", acct.handle.to_uppercase())).await.unwrap();
        assert_eq!(r.text().await.unwrap(), acct.did);
        for host in [format!("nobody-{}.{HANDLE_DOMAIN}", unique_name("x")), "example.com".to_string()] {
            assert_eq!(get(n, host).await.unwrap().status(), 404);
        }
    }
    // a deactivated account's handle doesn't verify
    a.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &acct.auth()).await.ok();
    for n in [&a, &b, &c] {
        assert_eq!(get(n, acct.handle.clone()).await.unwrap().status(), 404);
    }
}

/// Caddy on-demand TLS asks `/tls-check?domain=` (the reference PDS
/// distribution's semantics): 200 for the PDS hostname and active local
/// handles, 400 for no domain / a domain we don't serve handles on, 404 for
/// unknown or deactivated handles; any node answers for any account.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_check_for_caddy_on_demand_tls() {
    let (a, b, c) = cluster("e2e-tls").await;
    let acct = a.create_account("tls").await;
    let http = reqwest::Client::new();
    let check = |n: &TestServer, domain: Option<&str>| {
        let mut rb = http.get(format!("{}/tls-check", n.url));
        if let Some(d) = domain {
            rb = rb.query(&[("domain", d)]);
        }
        async move {
            let r = rb.send().await.unwrap();
            let status = r.status().as_u16();
            (status, r.json::<J>().await.unwrap_or_default())
        }
    };
    let unknown = format!("nobody-{}.{HANDLE_DOMAIN}", unique_name("x"));
    for n in [&a, &b, &c] {
        // the PDS hostname (--public-url's host)
        assert_eq!(check(n, Some("127.0.0.1")).await, (200, json!({"success": true})));
        let (s, j) = check(n, Some(&acct.handle)).await;
        assert_eq!((s, j), (200, json!({"success": true})), "{}", acct.handle);
        assert_eq!(check(n, Some(&acct.handle.to_uppercase())).await.0, 200);
        let (s, j) = check(n, Some(&unknown)).await;
        assert_eq!((s, j["error"].as_str()), (404, Some("NotFound")));
        let (s, j) = check(n, Some("example.com")).await;
        assert_eq!((s, j["error"].as_str()), (400, Some("InvalidRequest")));
        assert_eq!(check(n, None).await.0, 400);
        assert_eq!(check(n, Some("")).await.0, 400);
    }
    a.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &acct.auth()).await.ok();
    for n in [&a, &b, &c] {
        assert_eq!(check(n, Some(&acct.handle)).await.0, 404);
    }
}

/// Admin calls naming the account as `account` (updateAccountEmail) or
/// `recipientDid` (sendEmail) weren't routed; getAccountInfos read only the
/// receiving node's shards (silently dropping the rest, or 503 on invites).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_account_calls_reach_the_owner() {
    let (a, b, c) = cluster("e2e-ad").await;
    let accts = [a.create_account("ad").await, b.create_account("ad").await, c.create_account("ad").await];
    let inv = a
        .xrpc
        .post("com.atproto.server.createInviteCode", &json!({"useCount": 1, "forAccount": accts[1].did}), &Auth::Admin)
        .await
        .ok();
    for (i, n) in [&a, &b, &c].into_iter().enumerate() {
        let acct = &accts[(i + 1) % 3];
        assert!(!owns(n, &acct.did));
        let email = format!("{}@example.org", unique_name("new"));
        let update = |account: &str, email: &str| {
            n.xrpc.post_owned(
                "com.atproto.admin.updateAccountEmail",
                json!({"account": account, "email": email}),
                Auth::Admin,
            )
        };
        update(&acct.did, &email).await.ok();
        assert_eq!(n.account_info(&acct.did).await.ok()["email"], json!(email));
        // by handle too
        update(&acct.handle, &acct.email).await.ok();
        let body = json!({"recipientDid": acct.did, "content": "hi", "senderDid": "did:plc:admin"});
        let r = n.xrpc.post("com.atproto.admin.sendEmail", &body, &Auth::Admin).await.ok();
        assert_eq!(r["sent"], json!(true));
        let dids: Vec<(&str, String)> = accts.iter().map(|a| ("dids", a.did.clone())).collect();
        let r = n.xrpc.get_multi("com.atproto.admin.getAccountInfos", &dids, &Auth::Admin).await.ok();
        let infos = r["infos"].as_array().unwrap();
        let got: HashSet<&str> = infos.iter().map(|i| i["did"].as_str().unwrap()).collect();
        assert_eq!(got, accts.iter().map(|a| a.did.as_str()).collect::<HashSet<_>>());
        let b_info = infos.iter().find(|i| i["did"] == json!(accts[1].did)).unwrap();
        assert!(
            b_info["invites"].as_array().unwrap().iter().any(|c| c["code"] == inv["code"]),
            "invites of an account owned elsewhere: {b_info}"
        );
    }
}

/// While an account's shard has no serving owner (its node handed its shards
/// back and the routing table names nobody else yet), resolveHandle and
/// createSession answered as if the account didn't exist (400 HandleNotFound,
/// 401 "Invalid identifier or password"); they are a retryable 503 now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unowned_shard_is_unavailable_not_missing() {
    let store = Arc::new(object_store::memory::InMemory::new());
    // A alone, so once it shuts down nothing owns its shards: the window is
    // deterministic instead of racing a peer's (now sub-second) takeover.
    let a = node("e2e-un-a", &store).await;
    balanced(&[&a]).await;
    let acct = a.create_account("un").await;
    vlpds::server::shutdown(&a.app).await;
    for _ in 0..5 {
        let r = a.resolve_handle(&acct.handle).await;
        assert_eq!(r.status, 503, "resolveHandle on an unowned shard: {}", r.text());
        let s = a.create_session(&acct.handle, &acct.password).await;
        assert_eq!(s.status, 503, "createSession on an unowned shard: {}", s.text());
    }
    // a new node takes them over and serves the account
    let b = node("e2e-un-b", &store).await;
    let r = retry("survivor takes the shards over", || async {
        let r = b.resolve_handle(&acct.handle).await;
        assert!(matches!(r.status, 200 | 503), "{}", r.text());
        (r.status == 200).then_some(r)
    })
    .await;
    assert_eq!(r.json["did"], json!(acct.did));
    b.create_session(&acct.handle, &acct.password).await.ok();
}
