//! Reference PDS sync tests (packages/pds/tests/sync/*.test.ts) not covered
//! elsewhere in the suite; see tests/REFERENCE_COVERAGE.md.
use crate::common::*;
use std::time::Duration;

const FH: Duration = Duration::from_secs(10);

fn is_deleted(f: &Frame, did: &str) -> bool {
    f.kind() == "#account" && f.did() == Some(did) && f.str("status") == Some("deleted")
}

/// subscribe-repos.test.ts "account deletions invalidate all seq ops".
/// The reference deletes the account's earlier repo_seq rows, so a replay
/// shows only the `#account {active: false, status: deleted}` event. The
/// vlpds log is the firehose and is immutable, so earlier events stay in a
/// replay (divergent; DESIGN.md "Log = WAL = firehose"). What both guarantee,
/// and this test checks: the deletion is the account's last event, exactly
/// once, nothing for the DID follows it, and the repo is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_deletion_is_the_last_event_on_replay() {
    let s = TestServer::spawn().await;
    let a = s.create_account("baddie").await;
    let other = s.create_account("bystander").await;
    s.post(&a, "about to go").await;
    let new_handle = format!("{}.{HANDLE_DOMAIN}", unique_name("baddieupd"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": new_handle}), &a.auth()).await.ok();
    s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &a.auth()).await.ok();
    let token = s.mail_token(&a.email).await.expect("delete token");
    s.xrpc
        .post(
            "com.atproto.server.deleteAccount",
            &json!({"did": a.did, "password": a.password, "token": token}),
            &Auth::None,
        )
        .await
        .ok();
    // later activity by someone else, so the replay runs past the deletion
    s.post(&other, "still here").await;

    let mut sub = s.subscribe(Some(0)).await;
    let mut frames = sub.until(FH, |fs| fs.iter().any(|f| is_deleted(f, &a.did))).await;
    frames.extend(sub.drain(Duration::from_millis(500)).await);
    let mine: Vec<&Frame> = frames.iter().filter(|f| f.did() == Some(a.did.as_str())).collect();
    let last = mine.last().expect("events for the deleted account");
    assert!(is_deleted(last, &a.did), "last event for the DID is #account deleted: {:?}", last.body);
    assert_eq!(last.bool("active"), Some(false));
    assert!(last.str("time").is_some());
    assert_eq!(mine.iter().filter(|f| is_deleted(f, &a.did)).count(), 1, "one deletion event");
    s.xrpc.get("com.atproto.sync.getLatestCommit", &[("did", &a.did)], &Auth::None).await.err(400, "RepoNotFound");
}
