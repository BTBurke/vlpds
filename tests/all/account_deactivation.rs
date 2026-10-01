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
    let up = s.xrpc.post_bytes("com.atproto.repo.uploadBlob", PNG_1X1.to_vec(), "image/png", &a.auth()).await.ok();
    let blob = up["blob"].clone();
    // profile's key is literal:self
    s.xrpc
        .post(
            "com.atproto.repo.putRecord",
            &json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": {"$type": "app.bsky.actor.profile", "displayName": "alice", "avatar": blob}}),
            &a.auth(),
        )
        .await
        .ok();
    let blob_cid = blob["ref"]["$link"].as_str().unwrap().to_string();
    Fixture { s, a, post, blob_cid }
}

async fn deactivate(f: &Fixture) {
    f.s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &f.a.auth()).await.ok();
}

#[track_caller]
fn deactivated_err(r: &Resp, what: &str) {
    assert_eq!(r.status, 400, "{what}: {}", r.text());
    assert_eq!(r.error_name(), Some("RepoDeactivated"), "{what}: {}", r.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returns_deactivated_status() {
    let f = setup().await;
    deactivate(&f).await;
    let j = f.s.xrpc.get("com.atproto.sync.getRepoStatus", &[("did", &f.a.did)], &Auth::None).await.ok();
    assert_eq!(j["did"], json!(f.a.did));
    assert_eq!(j["active"], json!(false));
    assert_eq!(j["status"], json!("deactivated"));
    let info = f.s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &f.a.did)], &Auth::Admin).await.ok();
    assert!(info["deactivatedAt"].is_string(), "{info}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_longer_serves_repo_data() {
    let f = setup().await;
    deactivate(&f).await;
    let s = &f.s;
    let did = f.a.did.as_str();
    deactivated_err(&s.xrpc.get("com.atproto.sync.getRepo", &[("did", did)], &Auth::None).await, "getRepo");
    deactivated_err(&s.xrpc.get("com.atproto.sync.getLatestCommit", &[("did", did)], &Auth::None).await, "getLatestCommit");
    deactivated_err(&s.xrpc.get("com.atproto.sync.listBlobs", &[("did", did)], &Auth::None).await, "listBlobs");
    deactivated_err(
        &s.xrpc
            .get("com.atproto.sync.getRecord", &[("did", did), ("collection", f.post.collection()), ("rkey", f.post.rkey())], &Auth::None)
            .await,
        "sync.getRecord",
    );
    s.get_record(did, f.post.collection(), f.post.rkey()).await.client_err();
    let r = s.xrpc.get("com.atproto.repo.describeRepo", &[("repo", did)], &Auth::None).await;
    r.client_err();
    assert!(r.text().contains("deactivated") || r.error_name() == Some("RepoDeactivated"), "{}", r.text());
    deactivated_err(&s.xrpc.get("com.atproto.sync.getBlob", &[("did", did), ("cid", &f.blob_cid)], &Auth::None).await, "getBlob");

    let j = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let me = j["repos"].as_array().unwrap().iter().find(|r| r["did"] == json!(did)).cloned().expect("deactivated repo still listed");
    assert_eq!(me["active"], json!(false));
    assert_eq!(me["status"], json!("deactivated"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_longer_resolves_handle() {
    let f = setup().await;
    deactivate(&f).await;
    f.s.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &f.a.handle)], &Auth::None).await.client_err();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn still_allows_login_and_returns_status() {
    let f = setup().await;
    deactivate(&f).await;
    let j = f.s.create_session(&f.a.did, &f.a.password).await.ok();
    assert_eq!(j["status"], json!("deactivated"));
    assert_eq!(j["active"], json!(false));
    let j = f.s.xrpc.get("com.atproto.server.getSession", &[], &f.a.auth()).await.ok();
    assert_eq!(j["status"], json!("deactivated"));
    assert_eq!(j["active"], json!(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn does_not_allow_writes() {
    // reference findAccount(checkDeactivated): 401 AccountDeactivated
    let f = setup().await;
    deactivate(&f).await;
    let s = &f.s;
    let auth = f.a.auth();
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.createRecord",
            &json!({"repo": f.a.did, "collection": "app.bsky.feed.post", "record": post_record("blah")}),
            &auth,
        )
        .await;
    r.err(401, "AccountDeactivated");
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.putRecord",
            &json!({"repo": f.a.did, "collection": f.post.collection(), "rkey": f.post.rkey(), "record": post_record("blah")}),
            &auth,
        )
        .await;
    r.err(401, "AccountDeactivated");
    let r = s
        .xrpc
        .post("com.atproto.repo.deleteRecord", &json!({"repo": f.a.did, "collection": f.post.collection(), "rkey": f.post.rkey()}), &auth)
        .await;
    r.err(401, "AccountDeactivated");
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.applyWrites",
            &json!({"repo": f.a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": post_record("x")}]}),
            &auth,
        )
        .await;
    r.err(401, "AccountDeactivated");
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
    let j = f.s.xrpc.get("com.atproto.sync.getRepoStatus", &[("did", &f.a.did)], &Auth::None).await.ok();
    assert_eq!(j["active"], json!(true));
    assert!(j.get("status").is_none() || j["status"].is_null(), "{j}");
    let info = f.s.xrpc.get("com.atproto.admin.getAccountInfo", &[("did", &f.a.did)], &Auth::Admin).await.ok();
    assert!(info.get("deactivatedAt").is_none() || info["deactivatedAt"].is_null(), "{info}");
    // writes work again and resolveHandle is back
    f.s.post(&f.a, "back").await;
    let j = f.s.xrpc.get("com.atproto.identity.resolveHandle", &[("handle", &f.a.handle)], &Auth::None).await.ok();
    assert_eq!(j["did"], json!(f.a.did));
}
