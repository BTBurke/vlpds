//! Runs the independent Go sync 1.1 verifier (checker/, built on
//! indigo's repo.VerifyCommitMessage) against the firehose of an in-process
//! server after a varied write workload, and requires it to report zero
//! failures. The binary is built with `go build` into CARGO_TARGET_TMPDIR; if
//! no Go toolchain is installed the test fails with a clear message unless
//! VLPDS_SKIP_GO_CHECKER=1 is set.
use crate::common::*;
use std::time::Duration;

/// Builds the checker once per test binary: both tests call this, and two
/// concurrent `go build -o` into one path can leave a half-written binary.
fn build_checker() -> Option<std::path::PathBuf> {
    static BIN: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    BIN.get_or_init(build_checker_once).clone()
}

fn build_checker_once() -> Option<std::path::PathBuf> {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("checker");
    let out = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("vlpds-sync-checker");
    let st = std::process::Command::new("go").args(["build", "-o"]).arg(&out).arg(".").current_dir(&src).status();
    match st {
        Ok(s) if s.success() => Some(out),
        Ok(s) => panic!("go build of checker failed: {s}"),
        Err(e) => {
            if std::env::var("VLPDS_SKIP_GO_CHECKER").as_deref() == Ok("1") {
                eprintln!("skipping: go not available ({e})");
                None
            } else {
                panic!("cannot run `go` to build the checker ({e}); set VLPDS_SKIP_GO_CHECKER=1 to skip")
            }
        }
    }
}

async fn workload(s: &TestServer) -> Vec<TestAccount> {
    let mut accts = Vec::new();
    for i in 0..4 {
        accts.push(s.create_account(&format!("gc{i}")).await);
    }
    // sequential single-record writes
    for (i, a) in accts.iter().enumerate() {
        let mut refs = Vec::new();
        for j in 0..25 {
            refs.push(s.post(a, &format!("post {i}/{j}")).await);
        }
        // updates and deletes
        for r in refs.iter().step_by(3) {
            s.put_record(a, r.collection(), r.rkey(), post_record("edited")).await.ok();
        }
        for r in refs.iter().skip(1).step_by(4) {
            s.delete_record(a, r.collection(), r.rkey()).await.ok();
        }
        // a multi-op applyWrites
        let writes: Vec<J> = (0..10)
            .map(|k| json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.like", "rkey": format!("like{k}"), "value": {"$type": "app.bsky.feed.like", "subject": {"uri": refs[0].uri, "cid": refs[0].cid}, "createdAt": now_iso()}}))
            .chain(refs.iter().skip(2).step_by(5).map(|r| json!({"$type": "com.atproto.repo.applyWrites#delete", "collection": r.collection(), "rkey": r.rkey()})))
            .collect();
        // validate:false: likes need TID rkeys under lexicon validation
        s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "validate": false, "writes": writes}), &a.auth()).await.ok();
    }
    // concurrent burst on one repo (coalesced commits)
    let hot = &accts[0];
    let hs: Vec<_> = (0..150)
        .map(|k| {
            let body = json!({"repo": hot.did, "collection": "app.bsky.feed.post", "record": post_record(&format!("burst {k}"))});
            tokio::spawn(s.xrpc.post_owned("com.atproto.repo.createRecord", body, hot.auth()))
        })
        .collect();
    for h in hs {
        h.await.unwrap().ok();
    }
    // identity + account events in the stream
    let a = &accts[1];
    let new_handle = format!("{}.{HANDLE_DOMAIN}", unique_name("gcren"));
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": new_handle}), &a.auth()).await.ok();
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    s.xrpc.post_empty("com.atproto.server.activateAccount", &a.auth()).await.ok();
    s.post(a, "after reactivation").await;
    accts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn go_checker_accepts_firehose() {
    let Some(bin) = tokio::task::spawn_blocking(build_checker).await.unwrap() else {
        return;
    };
    let s = TestServer::spawn_with(|c| c.workers = 4).await;
    let mut sub = s.subscribe(Some(0)).await;
    let accts = workload(&s).await;

    // wait until every repo's head is on the stream, so we know how many
    // events the checker must see
    let mut want = std::collections::HashMap::new();
    for a in &accts {
        want.insert(a.did.clone(), s.latest_commit(&a.did).await.0);
    }
    let mut frames = sub
        .until(Duration::from_secs(60), |fs| {
            want.iter().all(|(d, c)| fs.iter().any(|f| f.did() == Some(d.as_str()) && matches!(f.body.get("commit"), Some(Value::Link(x)) if x == c)))
        })
        .await;
    // plus anything trailing (e.g. #account/#identity after the last commit)
    frames.extend(sub.drain(Duration::from_millis(500)).await);
    let n = frames.iter().filter(|f| f.seq().is_some()).count();
    let mut kinds = std::collections::BTreeMap::<String, usize>::new();
    for f in &frames {
        *kinds.entry(f.kind().to_string()).or_default() += 1;
    }
    eprintln!("firehose: {n} events {kinds:?}");
    for k in ["#commit", "#sync", "#identity", "#account"] {
        assert!(kinds.contains_key(k), "workload produced no {k} events: {kinds:?}");
    }

    let out = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(&bin).args(["-host", &s.url, "-cursor", "0", "-max-events", &n.to_string(), "-strict", "-quiet"]).output(),
    )
    .await
    .expect("checker timed out")
    .expect("run checker");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!("checker stdout:\n{stdout}\nchecker stderr:\n{stderr}");
    assert!(out.status.success(), "Go sync 1.1 checker reported failures (exit {:?}):\n{stdout}\n{stderr}", out.status.code());
    assert!(stdout.contains(&format!("max-events {n} reached")), "checker did not read all {n} events:\n{stdout}");
}

/// A signing-key rotation, as a relay sees it: from the rotation's
/// `#identity` (the checker refetches the key), the `#sync` of the
/// re-signed head verifies against the new key, and the commits after it
/// chain off the `#sync` and verify too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn go_checker_accepts_key_rotation_resync() {
    let Some(bin) = tokio::task::spawn_blocking(build_checker).await.unwrap() else {
        return;
    };
    let s = TestServer::spawn().await;
    let a = s.create_account("gcrot").await;
    let mut refs = Vec::new();
    for i in 0..5 {
        refs.push(s.post(&a, &format!("before {i}")).await);
    }
    let cursor = s.settled_now().await;
    let mut sub = s.subscribe(Some(cursor)).await;
    s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin).await.ok();
    for i in 0..8 {
        s.post(&a, &format!("after {i}")).await;
    }
    // an update and a delete of records written before the rotation
    s.put_record(&a, refs[0].collection(), refs[0].rkey(), post_record("edited")).await.ok();
    s.delete_record(&a, refs[1].collection(), refs[1].rkey()).await.ok();
    let frames = sub.until(Duration::from_secs(20), |fs| fs.len() >= 12).await;
    let kinds: Vec<&str> = frames.iter().map(|f| f.kind()).collect();
    assert_eq!(&kinds[..3], &["#identity", "#sync", "#commit"], "{kinds:?}");
    let n = frames.len();

    let out = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(&bin)
            .args(["-host", &s.url, "-cursor", &cursor.to_string(), "-max-events", &n.to_string(), "-strict", "-quiet", "-workers", "1"])
            .output(),
    )
    .await
    .expect("checker timed out")
    .expect("run checker");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "Go checker rejected the rotation's events (exit {:?}):\n{stdout}\n{stderr}", out.status.code());
    assert!(stdout.contains(&format!("max-events {n} reached")), "{stdout}");
    let sync_line = stdout.lines().find(|l| l.trim_start().starts_with("#sync:")).unwrap_or_default();
    assert!(sync_line.contains("1 (1 verified clean)"), "{stdout}");
}

/// The checker is not vacuous: pointed at a server whose firehose carries a
/// commit signed by a key that no longer matches the DID document, it fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn go_checker_rejects_wrong_signing_key() {
    let Some(bin) = tokio::task::spawn_blocking(build_checker).await.unwrap() else {
        return;
    };
    let s = TestServer::spawn().await;
    let a = s.create_account("gckey").await;
    for i in 0..5 {
        s.post(&a, &format!("p{i}")).await;
    }
    let mut sub = s.subscribe(Some(0)).await;
    let n = sub.drain(Duration::from_millis(500)).await.iter().filter(|f| f.seq().is_some()).count();
    // rotate the key: historical commits no longer verify against describeRepo's key
    s.xrpc.post("com.atproto.admin.updateAccountSigningKey", &json!({"did": a.did}), &Auth::Admin).await.ok();
    let out = tokio::process::Command::new(&bin)
        .args(["-host", &s.url, "-cursor", "0", "-max-events", &n.to_string(), "-strict", "-quiet", "-workers", "1"])
        .output()
        .await
        .expect("run checker");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "checker accepted commits signed by a key not in the DID document:\n{stdout}");
}
