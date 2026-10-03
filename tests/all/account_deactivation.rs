//! Port of atproto/packages/pds/tests/account-deactivation.test.ts:
//! deactivateAccount / activateAccount and their effect on reads, writes,
//! login, handle resolution and the firehose.
use crate::common::*;

struct Fixture {
    s: TestServer,
    a: TestAccount,
    post: RecordRef,
    blob_cid: String,
}

async fn setup() -> Fixture {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let post = s.post(&a, "hello").await;
    let blob = s.upload_blob(&a, PNG_1X1, "image/png").await;
    // profile's key is literal:self
    let profile = json!({"$type": "app.bsky.actor.profile", "displayName": "alice", "avatar": blob});
    s.put_record(&a, "app.bsky.actor.profile", "self", profile).await.ok();
    let blob_cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    Fixture { s, a, post, blob_cid }
}

async fn deactivate(f: &Fixture) {
    f.s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &f.a.auth()).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returns_deactivated_status() {
    let f = setup().await;
    deactivate(&f).await;
    let j = f.s.repo_status(&f.a.did).await.ok();
    assert_eq!(j["did"], json!(f.a.did));
    assert_eq!(j["active"], json!(false));
    assert_eq!(j["status"], json!("deactivated"));
    let info = f.s.account_info(&f.a.did).await.ok();
    assert!(info["deactivatedAt"].is_string(), "{info}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_longer_serves_repo_data() {
    let f = setup().await;
    deactivate(&f).await;
    let s = &f.s;
    let did = f.a.did.as_str();
    for nsid in ["getRepo", "getLatestCommit", "listBlobs"] {
        s.xrpc.get(&format!("com.atproto.sync.{nsid}"), &[("did", did)], &Auth::None).await.err(400, "RepoDeactivated");
    }
    let q = [("did", did), ("collection", f.post.collection()), ("rkey", f.post.rkey())];
    s.xrpc.get("com.atproto.sync.getRecord", &q, &Auth::None).await.err(400, "RepoDeactivated");
    s.get_record(did, f.post.collection(), f.post.rkey()).await.client_err();
    let r = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None).await;
    r.client_err();
    assert!(r.text().contains("deactivated") || r.error_name() == Some("RepoDeactivated"), "{}", r.text());
    s.get_blob(did, &f.blob_cid).await.err(400, "RepoDeactivated");

    let j = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let me = j["repos"].as_array().unwrap().iter().find(|r| r["did"] == json!(did)).cloned().expect("deactivated repo still listed");
    assert_eq!(me["active"], json!(false));
    assert_eq!(me["status"], json!("deactivated"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_longer_resolves_handle() {
    let f = setup().await;
    deactivate(&f).await;
    f.s.resolve_handle(&f.a.handle).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn still_allows_login_and_returns_status() {
    let f = setup().await;
    deactivate(&f).await;
    let j = f.s.create_session(&f.a.did, &f.a.password).await.ok();
    assert_eq!(j["status"], json!("deactivated"));
    assert_eq!(j["active"], json!(false));
    let j = f.s.get_session(&f.a.auth()).await.ok();
    assert_eq!(j["status"], json!("deactivated"));
    assert_eq!(j["active"], json!(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn does_not_allow_writes() {
    // reference findAccount(checkDeactivated): 401 AccountDeactivated
    let f = setup().await;
    deactivate(&f).await;
    let (s, a, p) = (&f.s, &f.a, &f.post);
    let body = json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("blah")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await.err(401, "AccountDeactivated");
    s.put_record(a, p.collection(), p.rkey(), post_record("blah")).await.err(401, "AccountDeactivated");
    s.delete_record(a, p.collection(), p.rkey()).await.err(401, "AccountDeactivated");
    let create = json!([{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post_record("x")}]);
    s.apply_writes(a, create).await.err(401, "AccountDeactivated");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivates() {
    let f = setup().await;
    let mut sub = f.s.subscribe_from_now().await;
    deactivate(&f).await;
    let fr = sub.wait_for(FH_TIMEOUT, &f.a.did, "#account").await;
    let ev = fr.last().unwrap();
    assert_eq!(ev.bool("active"), Some(false));
    assert_eq!(ev.str("status"), Some("deactivated"));

    f.s.xrpc.post_empty("com.atproto.server.activateAccount", &f.a.auth()).await.ok();
    let fr = sub.wait_for(FH_TIMEOUT, &f.a.did, "#account").await;
    let ev = fr.last().unwrap();
    assert_eq!(ev.bool("active"), Some(true));
    assert!(ev.str("status").is_none());

    f.s.get_repo(&f.a.did).await;
    let j = f.s.repo_status(&f.a.did).await.ok();
    assert_eq!(j["active"], json!(true));
    assert!(j.get("status").is_none() || j["status"].is_null(), "{j}");
    let info = f.s.account_info(&f.a.did).await.ok();
    assert!(info.get("deactivatedAt").is_none() || info["deactivatedAt"].is_null(), "{info}");
    // writes work again and resolveHandle is back
    f.s.post(&f.a, "back").await;
    let j = f.s.resolve_handle(&f.a.handle).await.ok();
    assert_eq!(j["did"], json!(f.a.did));
}
