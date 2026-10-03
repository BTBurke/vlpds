//! Port of atproto/packages/pds/tests/account-status.test.ts plus the
//! status effects on reads/writes (getRepoStatus, listRepos, sync reads,
//! repo writes) and the #account firehose events for each transition.
use crate::common::*;

async fn update_status(s: &TestServer, did: &str, extra: J) -> Resp {
    let mut body = json!({"subject": repo_ref(did)});
    for (k, v) in extra.as_object().unwrap() {
        body[k] = v.clone();
    }
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::Admin).await
}

async fn next_account_event(sub: &mut Sub, did: &str) -> Frame {
    sub.wait_for(FH_TIMEOUT, did, "#account").await.pop().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedown_plus_activation_is_an_error() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    let r = update_status(&s, &a.did, json!({"takedown": {"applied": true}, "deactivated": {"applied": false}})).await;
    r.err(400, "InvalidRequest");
    // nothing changed
    assert_eq!(s.repo_status(&a.did).await.ok()["active"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deactivate_takedown_untakedown_activate() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    let mut sub = s.subscribe_from_now().await;

    update_status(&s, &a.did, json!({"deactivated": {"applied": true}})).await.ok();
    let ev = next_account_event(&mut sub, &a.did).await;
    assert_eq!((ev.bool("active"), ev.str("status")), (Some(false), Some("deactivated")));

    update_status(&s, &a.did, json!({"takedown": {"applied": true, "ref": "mod-1"}})).await.ok();
    let ev = next_account_event(&mut sub, &a.did).await;
    assert_eq!((ev.bool("active"), ev.str("status")), (Some(false), Some("takendown")));
    let st = s.repo_status(&a.did).await.ok();
    assert_eq!((st["active"].clone(), st["status"].clone()), (json!(false), json!("takendown")));

    update_status(&s, &a.did, json!({"takedown": {"applied": false}})).await.ok();
    let ev = next_account_event(&mut sub, &a.did).await;
    // still deactivated underneath
    assert_eq!((ev.bool("active"), ev.str("status")), (Some(false), Some("deactivated")));

    update_status(&s, &a.did, json!({"deactivated": {"applied": false}})).await.ok();
    let ev = next_account_event(&mut sub, &a.did).await;
    assert_eq!(ev.bool("active"), Some(true));
    assert!(ev.str("status").is_none());

    s.create_session(&a.handle, &a.password).await.ok();
    assert_eq!(s.repo_status(&a.did).await.ok()["active"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_account_event_without_status_change() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    let mut sub = s.subscribe_from_now().await;
    update_status(&s, &a.did, json!({})).await.ok();
    let ev = next_account_event(&mut sub, &a.did).await;
    assert_eq!(ev.bool("active"), Some(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedown_then_deactivate_reports_both() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    update_status(&s, &a.did, json!({"takedown": {"applied": true}})).await.ok();
    update_status(&s, &a.did, json!({"deactivated": {"applied": true}})).await.ok();
    let j = s.xrpc.get("com.atproto.admin.getSubjectStatus", &[("did", &a.did)], &Auth::Admin).await.ok();
    assert_eq!(j["subject"]["did"], json!(a.did));
    assert_eq!(j["takedown"]["applied"], json!(true));
    assert!(j["takedown"]["ref"].is_string(), "{j}");
    assert_eq!(j["deactivated"]["applied"], json!(true));
    // takedown wins in the public status
    let st = s.repo_status(&a.did).await.ok();
    assert_eq!(st["status"], json!("takendown"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cannot_activate_a_takendown_account() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    update_status(&s, &a.did, json!({"takedown": {"applied": true}})).await.ok();
    let r = update_status(&s, &a.did, json!({"deactivated": {"applied": false}})).await;
    r.client_err();
    assert_eq!(r.error_name(), Some("AccountNotFound"), "{}", r.text());
    // the user cannot reactivate themselves either
    s.xrpc.post_empty("com.atproto.server.activateAccount", &a.auth()).await.client_err();
    assert_eq!(s.repo_status(&a.did).await.ok()["status"], json!("takendown"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_subject_status_requires_admin() {
    let s = TestServer::spawn().await;
    let a = s.create_account("iris").await;
    let body = json!({"subject": repo_ref(&a.did), "takedown": {"applied": true}});
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &Auth::None).await.err_status(401);
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &a.auth()).await.client_err();
    let wrong = Auth::Basic("admin".into(), "wrong".into());
    s.xrpc.post("com.atproto.admin.updateSubjectStatus", &body, &wrong).await.err_status(401);
    assert_eq!(s.repo_status(&a.did).await.ok()["active"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedown_effects_on_reads_and_writes() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tk").await;
    let other = s.create_account("ok").await;
    let p = s.post(&a, "pre-takedown").await;
    update_status(&s, &a.did, json!({"takedown": {"applied": true}})).await.ok();

    let did = a.did.as_str();
    for (nsid, q) in [
        ("com.atproto.sync.getRepo", vec![("did", did)]),
        ("com.atproto.sync.getLatestCommit", vec![("did", did)]),
        ("com.atproto.sync.getRecord", vec![("did", did), ("collection", p.collection()), ("rkey", p.rkey())]),
    ] {
        let r = s.xrpc.get(nsid, &q, &Auth::None).await;
        r.err(400, "RepoTakendown");
    }
    s.get_record(did, p.collection(), p.rkey()).await.client_err();
    s.list_records(did, "app.bsky.feed.post", &[]).await.client_err();

    let st = s.repo_status(did).await.ok();
    assert_eq!((st["active"].clone(), st["status"].clone()), (json!(false), json!("takendown")));
    let j = s.xrpc.get("com.atproto.sync.listRepos", &[], &Auth::None).await.ok();
    let repos = j["repos"].as_array().unwrap();
    let me = repos.iter().find(|r| r["did"] == json!(did)).expect("taken-down repo listed");
    assert_eq!(me["active"], json!(false));
    assert_eq!(me["status"], json!("takendown"));
    let them = repos.iter().find(|r| r["did"] == json!(other.did)).unwrap();
    assert_eq!(them["active"], json!(true));

    // existing access token can no longer write
    let body = json!({"repo": did, "collection": "app.bsky.feed.post", "record": post_record("x")});
    s.xrpc.post("com.atproto.repo.createRecord", &body, &a.auth()).await.client_err();

    // reversal restores everything
    update_status(&s, did, json!({"takedown": {"applied": false}})).await.ok();
    s.get_repo(did).await;
    s.get_record(did, p.collection(), p.rkey()).await.ok();
    // (a takedown may revoke existing sessions; log in again)
    let j = s.create_session(&a.handle, &a.password).await.ok();
    let a2 = TestAccount { access: j["accessJwt"].as_str().unwrap().into(), ..a.clone() };
    s.post(&a2, "post-restore").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_repo_status() {
    let s = TestServer::spawn().await;
    let r = s.repo_status("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").await;
    r.err(400, "RepoNotFound");
    let a = s.create_account("st").await;
    let j = s.repo_status(&a.did).await.ok();
    assert_eq!(j["did"], json!(a.did));
    assert_eq!(j["active"], json!(true));
    let (_, rev) = s.latest_commit(&a.did).await;
    assert_eq!(j["rev"], json!(rev));
}
