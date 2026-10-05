//! Ported from the reference's `tests/space-scope.test.ts` (5b95b2f2), a
//! unit test of `assertSpaceRead`, driven here over XRPC with real OAuth
//! grants and credentials. Each test names its reference case.

use super::ref_net::*;
use crate::common::*;

const FORUM: &str = "com.atmoboards.forum";

/// alice's space of type [`FORUM`] with one record of hers, and dan's
/// record in it too.
async fn setup(net: &Net) -> (crate::common::spaces::SpaceClient, crate::common::spaces::SpaceClient, String) {
    let (alice, dan) = (net.actor("alice", 0).await, net.actor("dan", 0).await);
    let space = net.create_space(&alice, SpaceOpts { space_type: Some(FORUM), ..Default::default() }).await;
    write(&alice, &space, W::new().collection("com.atmoboards.thread").rkey("mine")).await.ok();
    write(&dan, &space, W::new().collection("com.atmoboards.thread").rkey("his")).await.ok();
    (alice, dan, space)
}

/// "reads the caller's own repo with only read_self"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C1"]
async fn reads_own_repo_with_only_read_self() {
    let net = Net::new(0).await;
    let (alice, _, space) = setup(&net).await;
    let app = regrant(&alice, &format!("space:{FORUM}?authority=*&action=read_self")).await;
    app.get("com.atproto.space.listRecords", &[("space", &space), ("repo", &alice.did)]).await.ok();
}

/// "refuses another repo with only read_self"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C1"]
async fn refuses_another_repo_with_only_read_self() {
    let net = Net::new(0).await;
    let (alice, dan, space) = setup(&net).await;
    let app = regrant(&alice, &format!("space:{FORUM}?authority=*&action=read_self")).await;
    app.get("com.atproto.space.listRecords", &[("space", &space), ("repo", &dan.did)]).await.err(400, "RepoNotFound");
}

/// "refuses another repo even with whole-space read"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C1"]
async fn refuses_another_repo_even_with_whole_space_read() {
    let net = Net::new(0).await;
    let (alice, dan, space) = setup(&net).await;
    let app = regrant(&alice, &format!("space:{FORUM}?authority=*&action=read")).await;
    app.get("com.atproto.space.listRecords", &[("space", &space), ("repo", &alice.did)]).await.ok();
    app.get("com.atproto.space.listRecords", &[("space", &space), ("repo", &dan.did)]).await.err(400, "RepoNotFound");
}

/// "refuses another repo on a legacy access token". Divergent: OAuth-only,
/// so a password session doesn't read its own repo either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C1"]
async fn refuses_every_repo_to_a_legacy_access_token() {
    let net = Net::new(0).await;
    let (alice, dan, space) = setup(&net).await;
    let session = Auth::Bearer(alice.session_jwt.clone());
    let x = &net.pds[0].xrpc;
    let own = [("space", space.as_str()), ("repo", alice.did.as_str())];
    x.get("com.atproto.space.listRecords", &own, &session).await.err_status(403);
    let other = [("space", space.as_str()), ("repo", dan.did.as_str())];
    x.get("com.atproto.space.listRecords", &other, &session).await.client_err();
}

/// "read_self is not narrowed by collection"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C1"]
async fn read_self_is_not_narrowed_by_collection() {
    let net = Net::new(0).await;
    let (alice, _, space) = setup(&net).await;
    write(&alice, &space, W::new().collection("com.atmoboards.post").rkey("other")).await.ok();
    let app =
        regrant(&alice, &format!("space:{FORUM}?authority=*&action=read_self&collection=com.atmoboards.thread")).await;
    let q = [
        ("space", space.as_str()),
        ("repo", alice.did.as_str()),
        ("collection", "com.atmoboards.post"),
        ("rkey", "other"),
    ];
    app.get("com.atproto.space.getRecord", &q).await.ok();
}

/// "a space credential reads any repo in its own space"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spaces core: C3"]
async fn a_space_credential_reads_any_repo_in_its_own_space() {
    let net = Net::new(0).await;
    let (alice, dan) = (net.actor("alice", 0).await, net.actor("dan", 0).await);
    let space =
        net.create_space(&alice, SpaceOpts { space_type: Some(FORUM), members: &[&dan], ..Default::default() }).await;
    let other = net
        .create_space(
            &alice,
            SpaceOpts { space_type: Some(FORUM), skey: Some("other"), members: &[&dan], ..Default::default() },
        )
        .await;
    write(&dan, &space, W::new().rkey("his")).await.ok();
    write(&dan, &other, W::new().rkey("his")).await.ok();
    let cred = net.credential_for(&alice, &space).await;
    let base = &net.pds[0].url;
    cred.get(base, "com.atproto.space.listRecords", &[("space", &space), ("repo", &dan.did)]).await.ok();
    let r = cred.get(base, "com.atproto.space.listRecords", &[("space", &other), ("repo", &dan.did)]).await;
    r.err(400, "InvalidCredential");
    refused_mentioning(&r, &["not scoped to this space"]);
}
