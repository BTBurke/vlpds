//! atproto interop syntax fixtures (testdata/interop/syntax), exercised
//! through the server: every place the PDS accepts one of these identifiers
//! must reject the invalid ones with a 400 and accept the valid ones.
//!
//! vlpds has no standalone syntax module, so these tests are the only check
//! of identifier validation; failures name the endpoint that let a bad value
//! through (or rejected a good one).
use crate::common::*;

/// Runs every case, then fails once with the full list of mismatches.
struct Mismatches(Vec<String>);
impl Mismatches {
    fn new() -> Self {
        Mismatches(Vec::new())
    }
    fn push(&mut self, s: String) {
        self.0.push(s)
    }
    #[track_caller]
    fn assert_none(&self, what: &str) {
        assert!(
            self.0.is_empty(),
            "{what}: {} mismatches:\n  {}",
            self.0.len(),
            self.0.join("\n  ")
        );
    }
}

fn short(s: &str) -> String {
    if s.len() > 60 {
        format!("{}…({} chars)", &s[..60], s.len())
    } else {
        format!("{s:?}")
    }
}

// ---------------------------------------------------------------------------
// record keys: createRecord / putRecord
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_keys_valid_accepted_by_put_record() {
    let s = TestServer::spawn().await;
    let a = s.create_account("rkv").await;
    let mut bad = Mismatches::new();
    for rkey in fixture_lines("interop/syntax/recordkey_syntax_valid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.putRecord",
                &json!({"repo": a.did, "collection": "com.example.record", "rkey": rkey, "record": {"$type": "com.example.record", "k": rkey}}),
                &a.auth(),
            )
            .await;
        if !r.is_ok() {
            bad.push(format!(
                "putRecord rkey {} rejected: {}",
                short(&rkey),
                r.text()
            ));
            continue;
        }
        let g = s.get_record(&a.did, "com.example.record", &rkey).await;
        if !g.is_ok() || g.json["value"]["k"] != json!(rkey) {
            bad.push(format!(
                "getRecord rkey {} after put: {}",
                short(&rkey),
                g.text()
            ));
        }
    }
    bad.assert_none("valid record keys");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_keys_invalid_rejected() {
    let s = TestServer::spawn().await;
    let a = s.create_account("rki").await;
    let mut bad = Mismatches::new();
    for rkey in fixture_lines("interop/syntax/recordkey_syntax_invalid.txt") {
        for nsid in [
            "com.atproto.repo.createRecord",
            "com.atproto.repo.putRecord",
        ] {
            let r = s
                .xrpc
                .post(nsid, &json!({"repo": a.did, "collection": "com.example.record", "rkey": rkey, "record": {"$type": "com.example.record"}}), &a.auth())
                .await;
            if r.status != 400 {
                bad.push(format!(
                    "{nsid} accepted invalid rkey {} -> {}",
                    short(&rkey),
                    r.text()
                ));
            }
        }
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.applyWrites",
                &json!({"repo": a.did, "writes": [{"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.record", "rkey": rkey, "value": {"$type": "com.example.record"}}]}),
                &a.auth(),
            )
            .await;
        if r.status != 400 {
            bad.push(format!(
                "applyWrites accepted invalid rkey {} -> {}",
                short(&rkey),
                r.text()
            ));
        }
    }
    // Nothing may have been written.
    let l = s
        .list_records(&a.did, "com.example.record", &[("limit", "100")])
        .await;
    if let Some(recs) = l.json["records"].as_array() {
        for r in recs {
            bad.push(format!("record with invalid key was stored: {}", r["uri"]));
        }
    }
    bad.assert_none("invalid record keys");
}

// ---------------------------------------------------------------------------
// NSIDs: collection names
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nsids_valid_accepted_as_collections() {
    let s = TestServer::spawn().await;
    let a = s.create_account("nsv").await;
    let mut bad = Mismatches::new();
    for nsid in fixture_lines("interop/syntax/nsid_syntax_valid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": nsid, "record": {"$type": nsid, "x": 1}}),
                &a.auth(),
            )
            .await;
        if !r.is_ok() {
            bad.push(format!(
                "collection {} rejected: {}",
                short(&nsid),
                r.text()
            ));
        }
    }
    bad.assert_none("valid NSIDs as collections");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nsids_invalid_rejected_as_collections() {
    let s = TestServer::spawn().await;
    let a = s.create_account("nsi").await;
    let mut bad = Mismatches::new();
    for nsid in fixture_lines("interop/syntax/nsid_syntax_invalid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": nsid, "record": {"$type": nsid}}),
                &a.auth(),
            )
            .await;
        if r.status != 400 {
            bad.push(format!(
                "createRecord accepted collection {} -> {}",
                short(&nsid),
                r.text()
            ));
        }
        let r = s
            .xrpc
            .post("com.atproto.repo.putRecord", &json!({"repo": a.did, "collection": nsid, "rkey": "self", "record": {"$type": nsid}}), &a.auth())
            .await;
        if r.status != 400 {
            bad.push(format!(
                "putRecord accepted collection {} -> {}",
                short(&nsid),
                r.text()
            ));
        }
    }
    bad.assert_none("invalid NSIDs as collections");
}

// ---------------------------------------------------------------------------
// handles
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_invalid_rejected_by_create_account() {
    let s = TestServer::spawn().await;
    let mut bad = Mismatches::new();
    for h in fixture_lines("interop/syntax/handle_syntax_invalid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.server.createAccount",
                &json!({"handle": h, "password": PASSWORD, "email": format!("{}@example.com", unique_name("syn"))}),
                &Auth::None,
            )
            .await;
        if r.status != 400 {
            bad.push(format!(
                "createAccount accepted handle {} -> {}",
                short(&h),
                r.text()
            ));
        }
        // and the same label under our own domain
        let under = format!("{h}.{HANDLE_DOMAIN}");
        let r = s
            .xrpc
            .post(
                "com.atproto.server.createAccount",
                &json!({"handle": under, "password": PASSWORD, "email": format!("{}@example.com", unique_name("syn"))}),
                &Auth::None,
            )
            .await;
        // Some invalid handles become valid with a suffix (e.g. a bare TLD); only
        // flag success for strings that are invalid in any position.
        let still_invalid = h.contains(' ')
            || h.contains("..")
            || h.starts_with('.')
            || h.starts_with('-')
            || h.contains("_")
            || h.chars().any(|c| !c.is_ascii());
        if still_invalid && r.status != 400 {
            bad.push(format!(
                "createAccount accepted handle {} -> {}",
                short(&under),
                r.text()
            ));
        }
    }
    bad.assert_none("invalid handles");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_resolve_handle_param_validation() {
    let s = TestServer::spawn().await;
    let mut bad = Mismatches::new();
    for h in fixture_lines("interop/syntax/handle_syntax_invalid.txt") {
        let r = s
            .xrpc
            .get(
                "com.atproto.identity.resolveHandle",
                &[("handle", &h)],
                &Auth::None,
            )
            .await;
        if !(r.status == 400 && r.error_name() == Some("InvalidRequest")) {
            bad.push(format!(
                "resolveHandle({}) -> {} (want 400 InvalidRequest)",
                short(&h),
                r.text()
            ));
        }
    }
    for h in fixture_lines("interop/syntax/handle_syntax_valid.txt") {
        let r = s
            .xrpc
            .get(
                "com.atproto.identity.resolveHandle",
                &[("handle", &h)],
                &Auth::None,
            )
            .await;
        // Unknown but syntactically valid: must not be a param validation error.
        if r.error_name() == Some("InvalidRequest") {
            bad.push(format!(
                "resolveHandle rejected valid handle {} as InvalidRequest",
                short(&h)
            ));
        }
    }
    bad.assert_none("resolveHandle handle syntax");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_valid_labels_under_service_domain_accepted() {
    // Take each valid handle's first label and register it under the service domain.
    let s = TestServer::spawn().await;
    let mut bad = Mismatches::new();
    let mut seen = std::collections::HashSet::new();
    for h in fixture_lines("interop/syntax/handle_syntax_valid.txt") {
        let label = h.split('.').next().unwrap().to_ascii_lowercase();
        // service handles have a minimum label length in the reference PDS (3..=18)
        if label.len() < 3 || label.len() > 18 || !seen.insert(label.clone()) {
            continue;
        }
        let handle = format!("{label}.{HANDLE_DOMAIN}");
        let r = s
            .xrpc
            .post(
                "com.atproto.server.createAccount",
                &json!({"handle": handle, "password": PASSWORD, "email": format!("{}@example.com", unique_name("syn"))}),
                &Auth::None,
            )
            .await;
        if r.error_name() == Some("HandleNotAvailable") && r.text().contains("Reserved") {
            // reserved words (e.g. "friend") are a policy rejection, not syntax
        } else if !r.is_ok() {
            bad.push(format!("createAccount rejected {handle}: {}", r.text()));
        } else if r.json["handle"] != json!(handle) {
            bad.push(format!(
                "createAccount {handle} returned handle {}",
                r.json["handle"]
            ));
        }
    }
    bad.assert_none("valid handle labels");
}

// ---------------------------------------------------------------------------
// DIDs
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dids_param_validation() {
    let s = TestServer::spawn().await;
    let mut bad = Mismatches::new();
    for did in fixture_lines("interop/syntax/did_syntax_invalid.txt") {
        for nsid in [
            "com.atproto.sync.getLatestCommit",
            "com.atproto.sync.getRepoStatus",
            "com.atproto.sync.getRepo",
        ] {
            let r = s.xrpc.get(nsid, &[("did", &did)], &Auth::None).await;
            if !(r.status == 400 && r.error_name() == Some("InvalidRequest")) {
                bad.push(format!(
                    "{nsid}(did={}) -> {} (want 400 InvalidRequest)",
                    short(&did),
                    r.text()
                ));
            }
        }
    }
    for did in fixture_lines("interop/syntax/did_syntax_valid.txt") {
        let r = s
            .xrpc
            .get(
                "com.atproto.sync.getLatestCommit",
                &[("did", &did)],
                &Auth::None,
            )
            .await;
        if !(r.status == 400 || r.status == 404) || r.error_name() == Some("InvalidRequest") {
            bad.push(format!(
                "getLatestCommit(valid unknown did {}) -> {} (want RepoNotFound)",
                short(&did),
                r.text()
            ));
        }
    }
    bad.assert_none("DID params");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_identifiers_as_repo_param() {
    let s = TestServer::spawn().await;
    let mut bad = Mismatches::new();
    for id in fixture_lines("interop/syntax/atidentifier_syntax_invalid.txt") {
        let r = s
            .xrpc
            .get(
                "com.atproto.repo.describeRepo",
                &[("repo", &id)],
                &Auth::None,
            )
            .await;
        if !(r.status == 400 && r.error_name() == Some("InvalidRequest")) {
            bad.push(format!(
                "describeRepo(repo={}) -> {} (want 400 InvalidRequest)",
                short(&id),
                r.text()
            ));
        }
        let r = s
            .xrpc
            .get(
                "com.atproto.repo.listRecords",
                &[("repo", &id), ("collection", "app.bsky.feed.post")],
                &Auth::None,
            )
            .await;
        if !(r.status == 400 && r.error_name() == Some("InvalidRequest")) {
            bad.push(format!(
                "listRecords(repo={}) -> {} (want 400 InvalidRequest)",
                short(&id),
                r.text()
            ));
        }
    }
    bad.assert_none("at-identifier params");
}

// ---------------------------------------------------------------------------
// TIDs (library) and revs
// ---------------------------------------------------------------------------

#[test]
fn tids_parse() {
    let mut bad = Mismatches::new();
    for t in fixture_lines("interop/syntax/tid_syntax_valid.txt") {
        match vlpds::tid::Tid::parse(&t) {
            Some(tid) if tid.to_string() == t => {}
            other => bad.push(format!("valid TID {t} -> {other:?}")),
        }
    }
    for t in fixture_lines("interop/syntax/tid_syntax_invalid.txt") {
        if let Some(tid) = vlpds::tid::Tid::parse(&t) {
            bad.push(format!("invalid TID {} parsed as {tid:?}", short(&t)));
        }
        if is_tid(&t) {
            bad.push(format!("harness is_tid accepted invalid {}", short(&t)));
        }
    }
    bad.assert_none("TIDs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revs_and_generated_rkeys_are_tids() {
    let s = TestServer::spawn().await;
    let a = s.create_account("tid").await;
    let mut last_rev = String::new();
    for i in 0..5 {
        let r = s.post(&a, &format!("tid {i}")).await;
        assert!(is_tid(r.rkey()), "generated rkey {} is not a TID", r.rkey());
        let rev = r.rev.clone().expect("commit.rev");
        assert!(is_tid(&rev), "rev {rev} is not a TID");
        assert!(rev > last_rev, "revs must increase: {last_rev} -> {rev}");
        last_rev = rev;
    }
}

// ---------------------------------------------------------------------------
// CIDs
// ---------------------------------------------------------------------------

#[test]
fn cids_parse_library() {
    // vlpds only handles CIDv1 sha2-256 dag-cbor/raw; every invalid fixture
    // must be rejected, and every valid one that is in that subset must round-trip.
    let mut bad = Mismatches::new();
    for c in fixture_lines("interop/syntax/cid_syntax_invalid.txt") {
        if let Ok(cid) = Cid::parse(&c) {
            bad.push(format!("invalid CID {} parsed as {cid}", short(&c)));
        }
    }
    for c in fixture_lines("interop/syntax/cid_syntax_valid.txt") {
        if let Ok(cid) = Cid::parse(&c) {
            if cid.to_string() != c {
                bad.push(format!("CID {c} round-tripped to {cid}"));
            }
        }
    }
    bad.assert_none("CIDs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cids_invalid_rejected_in_params() {
    let s = TestServer::spawn().await;
    let a = s.create_account("cid").await;
    let p = s.post(&a, "cid").await;
    let mut bad = Mismatches::new();
    for c in fixture_lines("interop/syntax/cid_syntax_invalid.txt") {
        let r = s
            .xrpc
            .get(
                "com.atproto.repo.getRecord",
                &[
                    ("repo", &a.did),
                    ("collection", "app.bsky.feed.post"),
                    ("rkey", p.rkey()),
                    ("cid", &c),
                ],
                &Auth::None,
            )
            .await;
        if !(r.status == 400 && r.error_name() == Some("InvalidRequest")) {
            bad.push(format!(
                "getRecord(cid={}) -> {} (want 400 InvalidRequest)",
                short(&c),
                r.text()
            ));
        }
        let r = s
            .xrpc
            .post("com.atproto.repo.createRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post_record("x"), "swapCommit": c}), &a.auth())
            .await;
        if r.status != 400 {
            bad.push(format!(
                "createRecord(swapCommit={}) -> {}",
                short(&c),
                r.text()
            ));
        }
    }
    bad.assert_none("CID params");
}

// ---------------------------------------------------------------------------
// datetimes and at-uris inside known-lexicon records
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datetimes_in_known_records() {
    // The reference PDS validates records of known lexicons (app.bsky.feed.post
    // createdAt is format=datetime).
    let s = TestServer::spawn().await;
    let a = s.create_account("dt").await;
    let mut bad = Mismatches::new();
    for dt in fixture_lines("interop/syntax/datetime_syntax_valid.txt") {
        let r = s
            .xrpc
            .post("com.atproto.repo.createRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "text": "t", "createdAt": dt}}), &a.auth())
            .await;
        if !r.is_ok() {
            bad.push(format!(
                "valid datetime {} rejected: {}",
                short(&dt),
                r.text()
            ));
        }
    }
    for dt in fixture_lines("interop/syntax/datetime_syntax_invalid.txt") {
        let r = s
            .xrpc
            .post("com.atproto.repo.createRecord", &json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": {"$type": "app.bsky.feed.post", "text": "t", "createdAt": dt}}), &a.auth())
            .await;
        if r.status != 400 {
            bad.push(format!(
                "invalid datetime {} accepted: {}",
                short(&dt),
                r.text()
            ));
        }
    }
    bad.assert_none("datetimes in app.bsky.feed.post");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_uris_in_known_records() {
    // app.bsky.feed.like.subject is a com.atproto.repo.strongRef (uri: format at-uri).
    let s = TestServer::spawn().await;
    let a = s.create_account("uri").await;
    let cid = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";
    let mut bad = Mismatches::new();
    for u in fixture_lines("interop/syntax/aturi_syntax_valid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": "app.bsky.feed.like", "record": {"$type": "app.bsky.feed.like", "subject": {"uri": u, "cid": cid}, "createdAt": now_iso()}}),
                &a.auth(),
            )
            .await;
        if !r.is_ok() {
            bad.push(format!("valid at-uri {} rejected: {}", short(&u), r.text()));
        }
    }
    for u in fixture_lines("interop/syntax/aturi_syntax_invalid.txt") {
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": "app.bsky.feed.like", "record": {"$type": "app.bsky.feed.like", "subject": {"uri": u, "cid": cid}, "createdAt": now_iso()}}),
                &a.auth(),
            )
            .await;
        if r.status != 400 {
            bad.push(format!(
                "invalid at-uri {} accepted: {}",
                short(&u),
                r.text()
            ));
        }
    }
    bad.assert_none("at-uris in app.bsky.feed.like");
}
