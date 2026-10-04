//! Port of packages/pds/tests/sync/list.test.ts: listRepos completeness,
//! pagination, and that each entry's head/rev match getLatestCommit.
use crate::common::*;
use std::collections::BTreeSet;

async fn list_all(s: &TestServer, limit: usize) -> (Vec<J>, usize) {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let lim = limit.to_string();
        let mut q = vec![("limit", lim.as_str())];
        if let Some(c) = &cursor {
            q.push(("cursor", c.as_str()));
        }
        let r = s.xrpc.get("com.atproto.sync.listRepos", &q, &Auth::None).await.ok();
        let page = r["repos"].as_array().expect("repos").clone();
        assert!(page.len() <= limit, "page larger than limit");
        pages += 1;
        assert!(pages < 1000, "pagination does not terminate");
        out.extend(page.clone());
        match r["cursor"].as_str() {
            Some(c) if !page.is_empty() => cursor = Some(c.to_string()),
            _ => break,
        }
    }
    (out, pages)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_all_hosted_repos() {
    let s = TestServer::spawn().await;
    let mut dids = BTreeSet::new();
    for name in ["alice", "bob", "carol", "dan"] {
        let a = s.create_account(name).await;
        s.post(&a, "hi").await;
        dids.insert(a.did);
    }
    let r = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let repos = r["repos"].as_array().unwrap();
    let got: BTreeSet<String> = repos.iter().map(|x| x["did"].as_str().unwrap().to_string()).collect();
    assert_eq!(got, dids);
    assert!(repos.iter().all(|x| x["active"] == json!(true)), "all active: {r}");
    for x in repos {
        let did = x["did"].as_str().unwrap();
        let (cid, rev) = s.latest_commit(did).await;
        assert_eq!(x["head"].as_str(), Some(cid.to_string().as_str()), "head of {did}");
        assert_eq!(x["rev"].as_str(), Some(rev.as_str()), "rev of {did}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paginates_listed_repos() {
    let s = TestServer::spawn().await;
    let mut dids = BTreeSet::new();
    for _ in 0..13 {
        dids.insert(s.create_account("user").await.did);
    }
    let full = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let full = full["repos"].as_array().unwrap().clone();
    assert_eq!(full.len(), 13);
    for limit in [1, 2, 5, 13, 50] {
        let (paged, pages) = list_all(&s, limit).await;
        assert_eq!(paged, full, "limit {limit}: concatenated pages equal the full listing");
        assert!(pages >= 13usize.div_ceil(limit), "limit {limit}: {pages} pages");
    }
    // first page of 2 + rest from its cursor == full
    let p1 = s.xrpc.get("com.atproto.sync.listRepos", &[("limit", "2")], &Auth::None).await.ok();
    let c = p1["cursor"].as_str().expect("cursor on a full page").to_string();
    let p2 = s.xrpc.get("com.atproto.sync.listRepos", &[("cursor", &c)], &Auth::None).await.ok();
    let mut joined = p1["repos"].as_array().unwrap().clone();
    joined.extend(p2["repos"].as_array().unwrap().clone());
    assert_eq!(joined, full);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn includes_inactive_repos_with_status() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    let r = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let find =
        |did: &str| r["repos"].as_array().unwrap().iter().find(|x| x["did"] == json!(did)).cloned().expect("listed");
    let ea = find(&a.did);
    assert_eq!(ea["active"], json!(false));
    assert_eq!(ea["status"], json!("deactivated"));
    assert_eq!(find(&b.did)["active"], json!(true));
}

/// limit outside the lexicon range is a 400 (reference param validation),
/// not silently clamped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_range_is_validated() {
    let s = TestServer::spawn().await;
    let a = s.create_account("lim").await;
    for (nsid, extra, max) in [
        ("com.atproto.sync.listRepos", vec![], "1000"),
        ("com.atproto.sync.listBlobs", vec![("did", a.did.as_str())], "1000"),
        ("com.atproto.sync.listReposByCollection", vec![("collection", "app.bsky.feed.post")], "2000"),
    ] {
        for bad in ["0", "-1", if max == "1000" { "1001" } else { "2001" }] {
            let mut q = extra.clone();
            q.push(("limit", bad));
            s.xrpc.get(nsid, &q, &Auth::None).await.err(400, "InvalidRequest");
        }
        let mut q = extra.clone();
        q.push(("limit", max));
        s.xrpc.get(nsid, &q, &Auth::None).await.ok();
    }
}
