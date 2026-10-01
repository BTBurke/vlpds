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
            ..Default::default()
        });
    })
    .await
}

/// Waits until every node owns some shards, together they own each once,
/// and every node's routing table names those owners.
async fn balanced(nodes: &[&TestServer]) {
    for _ in 0..200 {
        let owned: Vec<Vec<u16>> =
            nodes.iter().map(|n| n.app.partitions.owned().iter().map(|p| p.id).collect()).collect();
        let all: HashSet<u16> = owned.iter().flatten().copied().collect();
        let routed = nodes.iter().all(|n| {
            let c = n.app.cluster.as_ref().unwrap();
            owned.iter().zip(nodes).all(|(shards, o)| {
                let id = &o.app.cluster.as_ref().unwrap().cfg.node_id;
                shards.iter().all(|p| c.owner_of(*p).is_some_and(|(owner, _)| &owner == id))
            })
        });
        if owned.iter().all(|o| !o.is_empty())
            && all.len() == SHARDS as usize
            && owned.iter().map(|o| o.len()).sum::<usize>() == SHARDS as usize
            && routed
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cluster never balanced");
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

/// listRepos / listReposByCollection used to 500 ("partition not owned") on
/// any node of a multi-node cluster; they now scatter-gather like the admin
/// listings and page across every node's shards.
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
        let r = n
            .xrpc
            .get("com.atproto.sync.listReposByCollection", &[("collection", "com.example.e2e")], &Auth::None)
            .await
            .ok();
        let got: HashSet<String> =
            r["repos"].as_array().unwrap().iter().map(|r| r["did"].as_str().unwrap().to_string()).collect();
        assert_eq!(got, want);
        // paged by DID
        let r = n
            .xrpc
            .get("com.atproto.sync.listReposByCollection", &[("collection", "com.example.e2e"), ("limit", "4")], &Auth::None)
            .await
            .ok();
        assert_eq!(r["repos"].as_array().unwrap().len(), 4);
        let cur = r["cursor"].as_str().expect("cursor").to_string();
        let r2 = n
            .xrpc
            .get("com.atproto.sync.listReposByCollection", &[("collection", "com.example.e2e"), ("cursor", &cur)], &Auth::None)
            .await
            .ok();
        assert_eq!(r2["repos"].as_array().unwrap().len(), want.len() - 4);
    }
}

/// resetPassword names only its token; it used to run on whichever node got
/// it and fail with 503 (the account update needs the owner's worker).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_password_through_any_node() {
    let (a, b, c) = cluster("e2e-rp").await;
    let nodes = [&a, &b, &c];
    for i in 0..3 {
        let owner = nodes[i];
        let acct = owner.create_account("rp").await;
        let (req, reset) = (nodes[(i + 1) % 3], nodes[(i + 2) % 3]);
        assert!(!owns(req, &acct.did) && !owns(reset, &acct.did));
        req.xrpc
            .post("com.atproto.server.requestPasswordReset", &json!({"email": acct.email}), &Auth::None)
            .await
            .ok();
        // the dev mailbox is per node (the mail is where the request ran):
        // the newest reset token on any of them
        let mut token = None;
        for n in nodes {
            let m = n.dev_mail(&acct.email).await.ok();
            for msg in m["messages"].as_array().unwrap() {
                if msg["purpose"] == "reset_password" {
                    token = msg["token"].as_str().map(String::from);
                }
            }
        }
        let token = token.expect("reset token mailed");
        reset
            .xrpc
            .post("com.atproto.server.resetPassword", &json!({"token": token, "password": "brand-new-pw"}), &Auth::None)
            .await
            .ok();
        owner.create_session(&acct.handle, "brand-new-pw").await.ok();
        req.create_session(&acct.handle, PASSWORD).await.err(401, "AuthenticationRequired");
        // single use
        reset
            .xrpc
            .post("com.atproto.server.resetPassword", &json!({"token": token, "password": "again-pw"}), &Auth::None)
            .await
            .client_err();
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
            let r = n.xrpc.get("com.atproto.identity.resolveIdentity", &[("identifier", ident)], &Auth::None).await.ok();
            assert_eq!(r["did"], json!(acct.did));
            assert_eq!(r["handle"], json!(acct.handle));
            assert_eq!(r["didDoc"]["id"], json!(acct.did));
        }
    }
}

/// HTTPS handle verification (`/.well-known/atproto-did` by Host) was not
/// served at all; every node answers for every account.
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
        n.xrpc
            .post("com.atproto.admin.updateAccountEmail", &json!({"account": acct.did, "email": email}), &Auth::Admin)
            .await
            .ok();
        let info = n.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &acct.did)], &Auth::Admin).await.ok();
        assert_eq!(info["email"], json!(email));
        // by handle too
        n.xrpc
            .post("com.atproto.admin.updateAccountEmail", &json!({"account": acct.handle, "email": acct.email}), &Auth::Admin)
            .await
            .ok();
        let r = n
            .xrpc
            .post("com.atproto.admin.sendEmail", &json!({"recipientDid": acct.did, "content": "hi", "senderDid": "did:plc:admin"}), &Auth::Admin)
            .await
            .ok();
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
        let r = a.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &acct.handle)], &Auth::None).await;
        assert_eq!(r.status, 503, "resolveHandle on an unowned shard: {}", r.text());
        let s = a.create_session(&acct.handle, &acct.password).await;
        assert_eq!(s.status, 503, "createSession on an unowned shard: {}", s.text());
    }
    // a new node takes them over and serves the account
    let b = node("e2e-un-b", &store).await;
    for _ in 0..200 {
        let r = b.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &acct.handle)], &Auth::None).await;
        if r.status == 200 {
            assert_eq!(r.json["did"], json!(acct.did));
            b.create_session(&acct.handle, &acct.password).await.ok();
            return;
        }
        assert_eq!(r.status, 503, "{}", r.text());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("survivor never took the shards over");
}
