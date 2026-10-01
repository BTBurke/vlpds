//! Port of packages/pds/tests/sync/subscribe-repos.test.ts: #commit/#sync/
//! #identity/#account events, backfill from a cursor, live tail, error and
//! info frames. Seqs are strictly increasing but not dense.
use crate::common::*;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

const POST: &str = "app.bsky.feed.post";
const IDLE: Duration = Duration::from_millis(600);

fn of_kind<'a>(fs: &'a [Frame], kind: &str) -> Vec<&'a Frame> {
    fs.iter().filter(|f| f.kind() == kind).collect()
}

fn for_did<'a>(fs: &'a [Frame], did: &str) -> Vec<&'a Frame> {
    fs.iter().filter(|f| f.did() == Some(did)).collect()
}

/// Subscribes from the current head of the stream: in-flight events from
/// earlier requests (acked on durability, broadcast a tick later) are skipped.
async fn subscribe_now(s: &TestServer) -> Sub {
    let cur = s.current_seq().await;
    s.subscribe(Some(cur)).await
}

async fn replay_all(s: &TestServer) -> Vec<Frame> {
    let mut sub = s.subscribe(Some(0)).await;
    sub.drain(IDLE).await
}

fn assert_strictly_increasing(fs: &[Frame]) {
    let seqs: Vec<i64> = fs.iter().filter_map(|f| f.seq()).collect();
    assert_eq!(seqs.len(), fs.iter().filter(|f| f.op == 1).count(), "every message frame has a seq");
    for w in seqs.windows(2) {
        assert!(w[0] < w[1], "seqs not strictly increasing: {} then {}", w[0], w[1]);
    }
}

fn verify_account_event(f: &Frame, did: &str, active: bool, status: Option<&str>) {
    assert_eq!(f.kind(), "#account");
    assert!(f.seq().is_some());
    assert_eq!(f.did(), Some(did));
    assert!(f.str("time").is_some(), "#account has time");
    assert_eq!(f.bool("active"), Some(active), "active flag of {:?}", f.body);
    assert_eq!(f.str("status"), status, "status of {:?}", f.body);
}

fn verify_identity_event(f: &Frame, did: &str, handle: &str) {
    assert_eq!(f.kind(), "#identity");
    assert!(f.seq().is_some());
    assert_eq!(f.did(), Some(did));
    assert!(f.str("time").is_some());
    assert_eq!(f.str("handle"), Some(handle));
}

async fn admin_takedown(s: &TestServer, did: &str, applied: bool) {
    s.xrpc
        .post(
            "com.atproto.admin.updateSubjectStatus",
            &json!({"subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did}, "takedown": {"applied": applied}}),
            &Auth::Admin,
        )
        .await
        .ok();
}

/// Rebuilds each repo's contents from the commit stream (checking signature,
/// inversion and the since/prevData chain) and compares them to getRepo.
async fn verify_commit_events(s: &TestServer, frames: &[Frame]) {
    let mut contents: HashMap<String, BTreeMap<String, Cid>> = HashMap::new();
    let mut chain: HashMap<String, (String, Cid)> = HashMap::new(); // did -> (rev, data)
    let mut keys = HashMap::new();
    for f in frames {
        if let Some(sy) = f.sync() {
            let c = sy.commit_obj();
            chain.insert(sy.did.clone(), (sy.rev.clone(), c.data));
            contents.insert(sy.did.clone(), BTreeMap::new());
            continue;
        }
        let Some(c) = f.commit() else { continue };
        if !keys.contains_key(&c.repo) {
            keys.insert(c.repo.clone(), s.signing_key(&c.repo).await);
        }
        let obj = c.commit_obj();
        assert_eq!(obj.did, c.repo);
        assert_eq!(obj.rev, c.rev);
        obj.verify(&keys[&c.repo]).unwrap_or_else(|e| panic!("seq {}: bad signature: {e}", c.seq));
        assert!(!c.too_big, "tooBig is deprecated");
        let prev = c.prev_data.unwrap_or_else(|| panic!("seq {}: #commit without prevData", c.seq));
        assert_eq!(c.invert().unwrap_or_else(|e| panic!("seq {}: {e}", c.seq)), prev, "seq {}: inversion", c.seq);
        if let Some((rev, data)) = chain.get(&c.repo) {
            assert_eq!(c.since.as_deref(), Some(rev.as_str()), "seq {}: since must equal the previous rev", c.seq);
            assert_eq!(prev, *data, "seq {}: prevData must equal the previous data", c.seq);
            assert!(c.rev > *rev, "seq {}: rev must increase", c.seq);
        }
        chain.insert(c.repo.clone(), (c.rev.clone(), obj.data));
        let m = contents.entry(c.repo.clone()).or_default();
        for op in &c.ops {
            match op.action.as_str() {
                "create" | "update" => {
                    m.insert(op.path.clone(), op.cid.expect("create/update op has cid"));
                }
                "delete" => {
                    assert!(op.cid.is_none(), "delete op has null cid");
                    m.remove(&op.path);
                }
                a => panic!("unknown action {a}"),
            }
        }
    }
    for (did, m) in &contents {
        let st = s.xrpc.get("com.atproto.sync.getRepoStatus", &[("did", did)], &Auth::None).await;
        if st.json["active"] != json!(true) {
            continue;
        }
        let repo = s.get_repo(did).await;
        let got: BTreeMap<String, Cid> = repo.entries().into_iter().collect();
        assert_eq!(&got, m, "stream-derived contents of {did} match getRepo");
        assert_eq!(repo.commit().data, chain[did].1, "last data of {did}");
    }
}

/// A mixed workload: posts, likes, profile put/update, deletes.
async fn workload(s: &TestServer, accts: &[TestAccount]) {
    for (i, a) in accts.iter().enumerate() {
        let mut posts = Vec::new();
        for j in 0..5 {
            posts.push(s.post(a, &format!("post {i}.{j}")).await);
        }
        let target = &posts[0];
        s.create_record(
            a,
            "app.bsky.feed.like",
            json!({"$type": "app.bsky.feed.like", "subject": {"uri": target.uri, "cid": target.cid}, "createdAt": now_iso()}),
        )
        .await;
        for d in 0..2 {
            s.xrpc
                .post("com.atproto.repo.putRecord",
                      &json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self",
                              "record": {"$type": "app.bsky.actor.profile", "displayName": format!("name {d}")}}),
                      &a.auth())
                .await
                .ok();
        }
        s.xrpc
            .post("com.atproto.repo.deleteRecord", &json!({"repo": a.did, "collection": POST, "rkey": posts[1].rkey()}), &a.auth())
            .await
            .ok();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_identity_account_events_on_creation() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob", "carol", "dan"] {
        accts.push(s.create_account(n).await);
    }
    for a in &accts {
        s.post(a, "first").await;
    }
    let frames = replay_all(&s).await;
    assert_strictly_increasing(&frames);
    for a in &accts {
        let evts = for_did(&frames, &a.did);
        let ids: Vec<&Frame> = evts.iter().copied().filter(|f| f.kind() == "#identity").collect();
        assert_eq!(ids.len(), 1, "one #identity for {}", a.did);
        verify_identity_event(ids[0], &a.did, &a.handle);
        let accs: Vec<&Frame> = evts.iter().copied().filter(|f| f.kind() == "#account").collect();
        assert_eq!(accs.len(), 1, "one #account for {}", a.did);
        verify_account_event(accs[0], &a.did, true, None);

        let syncs: Vec<&Frame> = evts.iter().copied().filter(|f| f.kind() == "#sync").collect();
        assert_eq!(syncs.len(), 1, "one #sync for {}", a.did);
        let sy = syncs[0].sync().unwrap();
        assert!(syncs[0].str("time").is_some());
        assert_eq!(sy.blocks.len(), 1, "#sync carries only the commit block");
        assert!(sy.blocks.contains_key(&sy.commit));
        let c = sy.commit_obj();
        assert_eq!(c.did, a.did);
        assert_eq!(c.rev, sy.rev);
        c.verify(&s.signing_key(&a.did).await).unwrap();
        assert_eq!(c.data.to_string(), "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm", "new repo has an empty MST");

        // the first #commit chains from the #sync
        let first = evts.iter().find_map(|f| f.commit()).expect("a #commit");
        assert!(first.seq > sy.seq);
        assert_eq!(first.since.as_deref(), Some(sy.rev.as_str()));
        assert_eq!(first.prev_data, Some(c.data));
    }
    verify_commit_events(&s, &frames).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfilled_events_rebuild_repos() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob", "carol", "dan"] {
        accts.push(s.create_account(n).await);
    }
    workload(&s, &accts).await;
    let frames = replay_all(&s).await;
    assert_strictly_increasing(&frames);
    let commits = of_kind(&frames, "#commit").len();
    assert_eq!(commits, 4 * 9, "one #commit per write (sequential writes don't coalesce)");
    verify_commit_events(&s, &frames).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_ops_match_rpc_results() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut sub = s.subscribe(None).await;
    let r = s
        .xrpc
        .post(
            "com.atproto.repo.applyWrites",
            &json!({"repo": a.did, "writes": [
                {"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "3jzfcijpj2z2a", "value": post_record("a")},
                {"$type": "com.atproto.repo.applyWrites#create", "collection": POST, "rkey": "3jzfcijpj2z2b", "value": post_record("b")},
                {"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.actor.profile", "rkey": "self", "value": {"$type": "app.bsky.actor.profile", "displayName": "A"}},
            ]}),
            &a.auth(),
        )
        .await
        .ok();
    let frames = sub.wait_for(FH_TIMEOUT, &a.did, "#commit").await;
    let c = frames.last().unwrap().commit().unwrap();
    assert_eq!(Some(c.commit.to_string()), r["commit"]["cid"].as_str().map(String::from));
    assert_eq!(Some(c.rev.as_str()), r["commit"]["rev"].as_str());
    let results = r["results"].as_array().unwrap();
    assert_eq!(c.ops.len(), results.len());
    let mut from_stream: Vec<(String, String)> = c.ops.iter().map(|o| (o.path.clone(), o.cid.unwrap().to_string())).collect();
    let mut from_rpc: Vec<(String, String)> = results
        .iter()
        .map(|x| {
            let uri = x["uri"].as_str().unwrap();
            (uri.split_once(&format!("{}/", a.did)).unwrap().1.to_string(), x["cid"].as_str().unwrap().to_string())
        })
        .collect();
    from_stream.sort();
    from_rpc.sort();
    assert_eq!(from_stream, from_rpc);
    assert!(c.ops.iter().all(|o| o.action == "create" && o.prev.is_none()));
    // every created record block is in the frame
    for o in &c.ops {
        assert!(c.blocks.contains_key(&o.cid.unwrap()), "record block for {} in #commit blocks", o.path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_tail_without_cursor_has_no_backfill() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for i in 0..5 {
        s.post(&a, &format!("old {i}")).await;
    }
    let old_max = s.current_seq().await;
    let mut subs = vec![s.subscribe(None).await];
    // Wait (by polling with probe writes) until the subscription is live.
    let probe_seq = s.sync_subs(&a, &mut subs).await;
    assert!(probe_seq > old_max, "the live tail starts after the old events");
    let mut sub = subs.pop().unwrap();
    let s2 = &s;
    let a2 = &a;
    let writer = async move {
        for i in 0..20 {
            s2.post(a2, &format!("new {i}")).await;
        }
    };
    let reader = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#commit").len() >= 20);
    let (_, frames) = tokio::join!(writer, reader);
    assert_eq!(of_kind(&frames, "#commit").len(), 20);
    assert!(frames.iter().all(|f| f.seq().unwrap() > old_max), "no backfill without a cursor");
    assert_strictly_increasing(&frames);
    let extra = sub.drain(Duration::from_millis(300)).await;
    assert!(extra.is_empty(), "no extra events: {extra:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cutover_from_backfill_to_live() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    for i in 0..10 {
        s.post(&a, &format!("before {i}")).await;
    }
    let s2 = &s;
    let a2 = &a;
    let writer = async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        for i in 0..30 {
            s2.post(a2, &format!("during {i}")).await;
        }
    };
    let mut sub = s.subscribe(Some(0)).await;
    let reader = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#commit").len() >= 40);
    let (_, frames) = tokio::join!(writer, reader);
    assert_strictly_increasing(&frames);
    assert_eq!(of_kind(&frames, "#commit").len(), 40, "no gaps or duplicates across backfill -> live cutover");
    verify_commit_events(&s, &frames).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfills_only_from_provided_cursor() {
    let s = TestServer::spawn().await;
    let mut accts = Vec::new();
    for n in ["alice", "bob"] {
        accts.push(s.create_account(n).await);
    }
    workload(&s, &accts).await;
    let all = replay_all(&s).await;
    assert!(all.len() > 10);
    let mid = all[all.len() / 2].seq().unwrap();
    let mut sub = s.subscribe(Some(mid)).await;
    let tail = sub.drain(IDLE).await;
    let want: Vec<&Frame> = all.iter().filter(|f| f.seq().unwrap() > mid).collect();
    assert_eq!(tail.len(), want.len(), "events strictly after the cursor");
    for (a, b) in tail.iter().zip(want) {
        assert_eq!(a.raw, b.raw, "replayed frames are byte-identical");
    }
    // cursor == latest seq -> nothing
    let last = all.last().unwrap().seq().unwrap();
    let mut sub = s.subscribe(Some(last)).await;
    assert!(sub.drain(Duration::from_millis(300)).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_events_on_handle_change() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let new_a = format!("{}.{HANDLE_DOMAIN}", unique_name("alice"));
    let new_b = format!("{}.{HANDLE_DOMAIN}", unique_name("bob"));
    let mut sub = subscribe_now(&s).await;
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": new_a}), &a.auth()).await.ok();
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": new_b}), &b.auth()).await.ok();
    // idempotent update re-sends the identity event
    s.xrpc.post("com.atproto.identity.updateHandle", &json!({"handle": new_b}), &b.auth()).await.ok();
    let frames = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#identity").len() >= 3).await;
    let ids = of_kind(&frames, "#identity");
    verify_identity_event(ids[0], &a.did, &new_a);
    verify_identity_event(ids[1], &b.did, &new_b);
    verify_identity_event(ids[2], &b.did, &new_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_events_deactivate_and_takedown() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let b = s.create_account("bob").await;
    let mut sub = subscribe_now(&s).await;
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    s.xrpc.post_empty("com.atproto.server.activateAccount", &a.auth()).await.ok();
    admin_takedown(&s, &b.did, true).await;
    admin_takedown(&s, &b.did, false).await;
    let frames = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#account").len() >= 4).await;
    let acc = of_kind(&frames, "#account");
    verify_account_event(acc[0], &a.did, false, Some("deactivated"));
    verify_account_event(acc[1], &a.did, true, None);
    verify_account_event(acc[2], &b.did, false, Some("takendown"));
    verify_account_event(acc[3], &b.did, true, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_account_events() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut sub = subscribe_now(&s).await;
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    admin_takedown(&s, &a.did, true).await;
    admin_takedown(&s, &a.did, false).await;
    // vlpds revokes sessions on takedown (the TS PDS keeps access tokens
    // valid), so log in again to reactivate.
    let sess = s.create_session(&a.did, &a.password).await.ok();
    let auth = Auth::Bearer(sess["accessJwt"].as_str().unwrap().to_string());
    s.xrpc.post_empty("com.atproto.server.activateAccount", &auth).await.ok();
    let frames = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#account").len() >= 4).await;
    let acc = of_kind(&frames, "#account");
    verify_account_event(acc[0], &a.did, false, Some("deactivated"));
    verify_account_event(acc[1], &a.did, false, Some("takendown"));
    // lifting the takedown reveals the still-deactivated status
    verify_account_event(acc[2], &a.did, false, Some("deactivated"));
    verify_account_event(acc[3], &a.did, true, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_event_on_account_activation() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.post(&a, "hi").await;
    s.xrpc.post("com.atproto.server.deactivateAccount", &json!({}), &a.auth()).await.ok();
    let mut sub = subscribe_now(&s).await;
    s.xrpc.post_empty("com.atproto.server.activateAccount", &a.auth()).await.ok();
    let frames = sub.wait_for(FH_TIMEOUT, &a.did, "#sync").await;
    let sy = frames.last().unwrap().sync().unwrap();
    let (cid, rev) = s.latest_commit(&a.did).await;
    assert_eq!(sy.commit, cid);
    assert_eq!(sy.rev, rev);
    assert_eq!(sy.blocks.len(), 1);
    sy.commit_obj().verify(&s.signing_key(&a.did).await).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_deletion_events() {
    let s = TestServer::spawn().await;
    let b1 = s.create_account("baddie").await;
    let b2 = s.create_account("baddie").await;
    let mut sub = subscribe_now(&s).await;

    // user-initiated deletion with an emailed token
    s.xrpc.post_empty("com.atproto.server.requestAccountDelete", &b1.auth()).await.ok();
    let token = s.mail_token(&b1.email).await.expect("dev-mode delete token for baddie1");
    s.xrpc
        .post("com.atproto.server.deleteAccount", &json!({"did": b1.did, "password": b1.password, "token": token}), &Auth::None)
        .await
        .ok();
    // admin deletion
    s.xrpc.post("com.atproto.admin.deleteAccount", &json!({"did": b2.did}), &Auth::Admin).await.ok();

    let frames = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#account").len() >= 2).await;
    let acc = of_kind(&frames, "#account");
    verify_account_event(acc[0], &b1.did, false, Some("deleted"));
    verify_account_event(acc[1], &b2.did, false, Some("deleted"));
    // the repos are gone
    s.xrpc.get("com.atproto.sync.getLatestCommit", &[("did", &b1.did)], &Auth::None).await.err(400, "RepoNotFound");
    s.xrpc.get("com.atproto.sync.getRepo", &[("did", &b2.did)], &Auth::None).await.err(400, "RepoNotFound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_on_future_cursor() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    s.post(&a, "hi").await;
    let cur = s.current_seq().await;
    // seqs are unix_micros*256 + partition; this is far beyond anything issued.
    let future = cur.max(1) * 2 + 1_000_000_000;
    let mut sub = s.subscribe(Some(future)).await;
    let frames = sub.drain(Duration::from_secs(2)).await;
    assert_eq!(frames.len(), 1, "exactly one (error) frame: {:?}", frames.iter().map(|f| f.body.clone()).collect::<Vec<_>>());
    assert_eq!(frames[0].op, -1);
    assert_eq!(frames[0].str("error"), Some("FutureCursor"));
    assert!(sub.closed, "connection closed after the error frame");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outdated_cursor_info_or_full_replay() {
    // With a tiny in-memory ring, an old cursor must either be served fully
    // (from the log) or be preceded by an #info OutdatedCursor frame.
    let s = TestServer::spawn_with(|c| c.firehose_ring_bytes = 4096).await;
    let a = s.create_account("alice").await;
    let first = s.post(&a, "first").await;
    let _ = first;
    for i in 0..40 {
        s.post(&a, &format!("p {i}")).await;
    }
    let mut sub = s.subscribe(Some(1)).await;
    let frames = sub.drain(IDLE).await;
    assert!(!frames.is_empty());
    let commits = of_kind(&frames, "#commit").len();
    if frames[0].kind() == "#info" {
        assert_eq!(frames[0].op, 1);
        assert_eq!(frames[0].str("name"), Some("OutdatedCursor"));
        assert_strictly_increasing(&frames[1..]);
    } else {
        assert_eq!(commits, 41, "full replay from an old cursor");
        assert_strictly_increasing(&frames);
    }
    // whatever was served must end at the newest event
    let (cid, _) = s.latest_commit(&a.did).await;
    let last = frames.iter().rev().find_map(|f| f.commit()).unwrap();
    assert_eq!(last.commit, cid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_open_connections_see_identical_streams() {
    let s = TestServer::spawn().await;
    let a = s.create_account("alice").await;
    let mut subs = Vec::new();
    for i in 0..10 {
        subs.push(s.subscribe(if i % 2 == 0 { Some(0) } else { None }).await);
    }
    // every stream (backfilled or live) is consumed up to the same probe commit
    s.sync_subs(&a, &mut subs).await;
    for i in 0..15 {
        s.post(&a, &format!("p {i}")).await;
    }
    let mut streams = Vec::new();
    for sub in subs.iter_mut() {
        let fs = sub.until(FH_TIMEOUT, |fs| of_kind(fs, "#commit").len() >= 15).await;
        let commits: Vec<Vec<u8>> = fs.iter().filter(|f| f.kind() == "#commit").map(|f| f.raw.clone()).collect();
        streams.push(commits);
    }
    for s in &streams[1..] {
        assert_eq!(s, &streams[0]);
    }
}
