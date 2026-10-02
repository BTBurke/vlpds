//! Golden format fixtures (DESIGN.md "Rolling upgrades and format
//! versioning", "Tests and CI"): `testdata/formats/L{n}/` holds what a build
//! at feature level n writes, for every persisted or wire format a reader
//! must keep understanding.
//!
//! - `writers_reproduce_the_max_level_fixtures`: this build's writers, at
//!   the active level, emit `L{MAX_LEVEL}` byte for byte (a writer change
//!   without a new level fails here).
//! - `fixtures_decode_and_reencode`: every level in the build's window
//!   decodes, and re-encodes to the same bytes.
//! - `manifest_freezes_released_levels`: a released level's files match
//!   `testdata/formats/MANIFEST` (sha256), so its fixtures never change.
//!
//! `VLPDS_BLESS=1 cargo test --test all formats:: -- --test-threads=1`
//! rewrites the `MAX_LEVEL` fixtures from the current writers and records
//! the manifest entries of a level that has none yet (or isn't released).
//! The wrapped secret is random (its nonce): blessing writes it only when missing.
//!
//! Not covered yet (TODO.md): private `p/` rows (sessions, OAuth, TOTP),
//! session JWTs, and a SlateDB directory written by the pinned slatedb rev.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use vlpds::cid::Cid;
use vlpds::segment::{self, LogObject, Mutation, SegmentBuilder};
use vlpds::slots::ShardId;
use vlpds::version;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/formats")
}

fn level_dir(level: u32) -> PathBuf {
    root().join(format!("L{level}"))
}

fn bless() -> bool {
    std::env::var("VLPDS_BLESS").is_ok_and(|v| v == "1")
}

const DID: &str = "did:plc:fixture0000000000000000";
const LOG: &str = "node-a.1790000000000000";
const TIME: &str = "2026-10-01T00:00:00.000Z";
const KEK: [u8; 32] = [7; 32];
const SECRET: &[u8] = b"level-1 wrapped secret fixture";

/// A #commit frame (one update) and the record/commit blocks it carries.
fn commit_frame() -> (Vec<u8>, Cid, Vec<u8>, vlpds::tid::Tid) {
    let rec_block = b"\xa1aa\x01".to_vec();
    let rec = Cid::dag_cbor(&rec_block);
    let mut commit_block = Vec::new();
    vlpds::cbor::Value::Map(vec![("did".into(), vlpds::cbor::Value::Text(DID.into())), ("data".into(), vlpds::cbor::Value::Link(rec))]).encode(&mut commit_block);
    let commit = Cid::dag_cbor(&commit_block);
    let mut car = Vec::new();
    vlpds::car::write_header(&mut car, &commit);
    vlpds::car::write_block(&mut car, &commit, &commit_block);
    vlpds::car::write_block(&mut car, &rec, &rec_block);
    let rev = vlpds::tid::Tid::parse("3l3qo2vutsw2b").unwrap();
    let ops = [vlpds::events::RepoOp { action: "update", path: "app.bsky.feed.post/1", cid: Some(rec), prev: Some(commit) }];
    let frame = vlpds::events::commit_frame(&vlpds::events::CommitFrame {
        repo: DID,
        rev: &rev.to_string(),
        since: None,
        commit,
        prev_data: None,
        blocks: &car,
        ops: &ops,
        time: TIME,
    });
    let mut bytes = Vec::new();
    frame.finish(1000 << 8, &mut bytes);
    (bytes, commit, commit_block, rev)
}

fn segment_plain() -> Vec<u8> {
    let (frame, ..) = commit_frame();
    let derived = segment::derive_commit_muts(&frame).unwrap();
    let mut muts = derived.clone();
    muts.push(Mutation { key: Bytes::from(vlpds::state::collection_key("app.bsky.feed.post", DID)), val: Some(Bytes::new()) });
    let m = |k: &str, v: Option<&str>| Mutation { key: Bytes::from(k.to_string()), val: v.map(|v| Bytes::from(v.to_string())) };
    let mut b = SegmentBuilder::for_log(LOG);
    b.push_derived(1000 << 8, ShardId(3), 7, |o| o.extend_from_slice(&frame), &muts, derived.len());
    b.push(1001 << 8, ShardId(70_000), 1, |o| o.extend_from_slice(b"not a frame"), &[m("k-put", Some("v")), m("k-del", None)]);
    b.push(1002 << 8, ShardId(3), 7, |_| {}, &[]);
    b.seal(LOG, 5, 4)
}

/// A like record (it has a backlink: src/backlinks.rs).
fn like_record() -> Vec<u8> {
    let v = serde_json::json!({"$type": "app.bsky.feed.like", "subject": {"uri": "at://did:plc:subject000000000000000000/app.bsky.feed.post/3l3qo2vutsw2a", "cid": Cid::dag_cbor(b"\xa0").to_string()}, "createdAt": TIME});
    vlpds::cbor::Value::from_json(&v).unwrap().to_cbor()
}

/// A segment of one #commit creating a like: its derived muts include the
/// backlink put (`bl/`, `segment::derive_commit_muts`).
fn segment_like() -> Vec<u8> {
    let rec_block = like_record();
    let rec = Cid::dag_cbor(&rec_block);
    let mut commit_block = Vec::new();
    vlpds::cbor::Value::Map(vec![("did".into(), vlpds::cbor::Value::Text(DID.into())), ("data".into(), vlpds::cbor::Value::Link(rec))]).encode(&mut commit_block);
    let commit = Cid::dag_cbor(&commit_block);
    let mut car = Vec::new();
    vlpds::car::write_header(&mut car, &commit);
    vlpds::car::write_block(&mut car, &commit, &commit_block);
    vlpds::car::write_block(&mut car, &rec, &rec_block);
    let ops = [vlpds::events::RepoOp { action: "create", path: "app.bsky.feed.like/3l3qo2vutsw2b", cid: Some(rec), prev: None }];
    let frame = vlpds::events::commit_frame(&vlpds::events::CommitFrame {
        repo: DID,
        rev: "3l3qo2vutsw2c",
        since: None,
        commit,
        prev_data: None,
        blocks: &car,
        ops: &ops,
        time: TIME,
    });
    let mut bytes = Vec::new();
    frame.finish(1010 << 8, &mut bytes);
    let derived = segment::derive_commit_muts(&bytes).unwrap();
    let mut b = SegmentBuilder::for_log(LOG);
    b.push_derived(1010 << 8, ShardId(3), 7, |o| o.extend_from_slice(&bytes), &derived, derived.len());
    b.seal(LOG, 6, 6)
}

/// The backlink index: a like's link, its `bl/` key, and a value of two rkeys.
fn backlinks() -> Vec<u8> {
    let rec = like_record();
    let link = vlpds::backlinks::link("app.bsky.feed.like", &rec).unwrap();
    let rkeys: vlpds::backlinks::Rkeys = vec!["3l3qo2vutsw2b".into(), "3l3qo2vutsw2d".into()];
    let k: BTreeMap<&str, String> = [
        ("record", hex::encode(&rec)),
        ("link", hex::encode(&link)),
        ("key bl/", hex::encode(vlpds::state::backlink_key(DID, &link))),
        ("value", hex::encode(vlpds::backlinks::encode(&rkeys))),
    ]
    .into_iter()
    .collect();
    pretty(&k)
}

fn head() -> vlpds::state::Head {
    let (_, commit, commit_block, rev) = commit_frame();
    vlpds::state::Head { commit, data: Cid::dag_cbor(b"\xa1aa\x01"), rev, commit_block: Bytes::from(commit_block) }
}

fn account() -> vlpds::state::Account {
    let mut a = vlpds::state::Account {
        did: DID.into(),
        handle: "fixture.test".into(),
        wrapped_signing_key: "vw1.kid.AAAA".into(),
        signing_pubkey: "zQ3shfixture".into(),
        password_hash: "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA".into(),
        created_at: TIME.into(),
        status: Some("deactivated".into()),
        email: Some("fixture@example.com".into()),
        email_confirmed: true,
        pending_signing_key: None,
        extra: Default::default(),
    };
    a.extra.insert("preferences".into(), serde_json::json!([{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false}]));
    a
}

fn recent() -> Vec<u8> {
    let r = vlpds::partition::RecentRepos::new(8);
    for d in ["did:plc:c", "did:plc:b", "did:plc:a"] {
        r.touch(&Arc::from(d));
    }
    r.take_dirty().unwrap().to_vec()
}

fn keys() -> Vec<u8> {
    use vlpds::state;
    let cid = Cid::dag_cbor(b"\xa1aa\x01");
    let k: BTreeMap<&str, String> = [
        ("head h/", state::head_key(DID)),
        ("account a/", state::account_key(DID)),
        ("handle", state::handle_key(DID, "fixture.test")),
        ("record R/", state::record_key(DID, "app.bsky.feed.post/1")),
        ("record cid c/", state::record_cid_key(DID, &cid, "app.bsky.feed.post/1")),
        ("collection C/", state::collection_key("app.bsky.feed.post", DID)),
        ("blob ref b/", state::blob_ref_key(DID, &cid, "app.bsky.feed.post/1")),
        ("private p/", state::private_key(DID, "session/abc")),
        ("mst node M/", state::mst_node_key(DID, &cid)),
    ]
    .into_iter()
    .map(|(n, k)| (n, hex::encode(k)))
    .collect();
    pretty(&k)
}

fn pretty<T: serde::Serialize>(v: &T) -> Vec<u8> {
    let mut b = serde_json::to_vec_pretty(v).unwrap();
    b.push(b'\n');
    b
}

fn compact<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).unwrap()
}

fn lease() -> vlpds::cluster::NodeLease {
    vlpds::cluster::NodeLease {
        node_id: "node-a".into(),
        log_id: LOG.into(),
        addr: "http://10.0.0.1:2583".into(),
        writer: 17,
        expires_ms: 1_790_000_010_000,
        renewals: 42,
        next_ordinal: 6,
        draining: false,
        joined: true,
        follows: [("node-b.1790000000000001".to_string(), 255_000i64)].into(),
        wm_cap: 458_240_002_560_000_000,
        rev: "0123abc".into(),
        min_level: 1,
        max_level: 1,
        seen_level: 1,
    }
}

fn assignment() -> vlpds::cluster::Assignment {
    use vlpds::nodelog::Span;
    vlpds::cluster::Assignment {
        owner: Some("node-a".into()),
        log_id: Some(LOG.into()),
        addr: Some("http://10.0.0.1:2583".into()),
        epoch: 3,
        seq_floor: 256_000,
        history: vec![Span { log_id: "node-b.1".into(), epoch: 2, start: 0, end: Some(9) }, Span { log_id: LOG.into(), epoch: 3, start: 6, end: None }],
        frozen: None,
        extra: Default::default(),
    }
}

fn layout() -> vlpds::slots::Layout {
    let l = vlpds::slots::Layout::uniform(4);
    let op = l.plan_split(ShardId(1), None, "node-a").unwrap();
    l.with_op(op)
}

fn report() -> vlpds::retention::Report {
    vlpds::retention::Report { opened: [(ShardId(3), 7), (ShardId(70_000), 1)].into(), pruned_seq: 255_000 }
}

fn ratelimits() -> Vec<u8> {
    let d = vlpds::ratelimit::config::parse(
        br#"{"version": 7, "enabled": true, "limiters": {"global-ip": {"points": 6000}}, "routes": [{"nsid": "app.bsky.feed.getTimeline", "points": 600, "windowSecs": 300}], "overrides": [{"ip": "203.0.113.0/24", "limiters": ["global-ip"], "exempt": true, "note": "relay"}], "updatedAt": "2026-10-01T00:00:00.000Z", "updatedBy": "jaz", "history": [{"version": 7, "at": "2026-10-01T00:00:00.000Z", "by": "jaz", "node": "node-a", "changes": ["routes: + app.bsky.feed.getTimeline"]}]}"#,
    )
    .unwrap();
    compact(&d)
}

fn cluster_version() -> version::ClusterVersion {
    version::ClusterVersion {
        active: 1,
        target: None,
        history: vec![version::Change { level: 1, at: TIME.into(), by: "node-a".into(), extra: Default::default() }],
        extra: Default::default(),
    }
}

/// The writer claim `Cluster::join` writes (a JSON literal there).
fn writer_claim() -> Vec<u8> {
    compact(&serde_json::json!({"node_id": "node-a", "log_id": LOG, "confirmed": true}))
}

fn frames() -> Vec<(&'static str, Vec<u8>)> {
    use vlpds::events;
    let fin = |f: events::Frame, seq: i64| {
        let mut b = Vec::new();
        f.finish(seq, &mut b);
        b
    };
    let mut car = Vec::new();
    let c = Cid::dag_cbor(b"\xa0");
    vlpds::car::write_header(&mut car, &c);
    vlpds::car::write_block(&mut car, &c, b"\xa0");
    vec![
        ("firehose/commit.frame", commit_frame().0),
        ("firehose/identity.frame", fin(events::identity_frame(DID, "fixture.test", TIME), 1003 << 8)),
        ("firehose/account.frame", fin(events::account_frame(DID, false, Some("deactivated"), TIME), 1004 << 8)),
        ("firehose/sync.frame", fin(events::sync_frame(DID, "3l3qo2vutsw2b", &car, TIME), 1005 << 8)),
        ("firehose/error.frame", events::error_frame("FutureCursor", "Cursor in the future.")),
    ]
}

/// Every fixture this build writes at the active level (path -> bytes),
/// except the wrapped secret (random nonce).
fn written() -> Vec<(&'static str, Vec<u8>)> {
    let plain = segment_plain();
    let zstd = segment::compress(&plain, 1).unwrap().expect("compressible");
    let batch = vlpds::nodelog::LogBatch { log_id: LOG.into(), ordinal: 5, events: vec![(1000 << 8, Bytes::from(commit_frame().0)), (1003 << 8, Bytes::from_static(b"frame"))] };
    let h = head();
    let mut v = vec![
        ("segment/plain.seg", plain),
        ("segment/zstd.seg", zstd),
        ("segment/fence.bin", segment::fence_object("node-b").to_vec()),
        ("segment/like.seg", segment_like()),
        ("state/backlinks.json", backlinks()),
        ("state/head.bin", h.encode().to_vec()),
        ("state/record.bin", vlpds::state::record_value(&h.data, h.rev.0, b"\xa1aa\x01").to_vec()),
        ("state/account.json", compact(&account())),
        ("state/applied2.bin", vlpds::nodelog::encode_marker(LOG, 5)),
        ("state/recent.bin", recent()),
        ("state/keys.json", keys()),
        ("control/node_lease.json", compact(&lease())),
        ("control/assignment.json", compact(&assignment())),
        ("control/layout.json", compact(&layout())),
        ("control/writer_claim.json", writer_claim()),
        ("control/retain_report.json", compact(&report())),
        ("control/ratelimits.json", ratelimits()),
        ("control/cluster_version.json", compact(&cluster_version())),
        ("stream/batch.bin", vlpds::remote::encode_batch(&batch).to_vec()),
        ("stream/watermark.bin", vlpds::remote::encode_watermark(1003 << 8).to_vec()),
    ];
    v.extend(frames());
    v
}

const SECRET_FIXTURE: &str = "secrets/vw1.txt";

fn secrets() -> vlpds::secrets::Secrets {
    let k = vlpds::secrets::KekBytes::new(KEK);
    vlpds::secrets::Secrets::new(vec![Arc::new(vlpds::secrets::LocalKek::new(&k))], 1).unwrap()
}

fn files(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(base: &std::path::Path, d: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.push(p.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/"));
            }
        }
    }
    if dir.exists() {
        walk(dir, dir, &mut out);
    }
    out.sort();
    out
}

#[tokio::test]
async fn writers_reproduce_the_max_level_fixtures() {
    assert_eq!(version::active(), version::MAX_LEVEL, "this binary's clusters run the build's max level");
    let dir = level_dir(version::MAX_LEVEL);
    let written = written();
    if bless() {
        for (name, bytes) in &written {
            let p = dir.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            if std::fs::read(&p).ok().as_deref() != Some(bytes.as_slice()) {
                std::fs::write(&p, bytes).unwrap();
                eprintln!("blessed {}", p.display());
            }
        }
        let p = dir.join(SECRET_FIXTURE);
        if !p.exists() {
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            let blob = secrets().wrap(vlpds::secrets::Purpose::SigningKey, DID, SECRET).await.unwrap();
            std::fs::write(&p, format!("{blob}\n")).unwrap();
        }
    }
    for (name, bytes) in &written {
        let got = std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e} (VLPDS_BLESS=1 to record)"));
        assert!(got == *bytes, "{name}: this build writes different bytes than L{} records: a format change needs a new level", version::MAX_LEVEL);
    }
    let mut expected: Vec<String> = written.iter().map(|(n, _)| n.to_string()).chain([SECRET_FIXTURE.to_string()]).collect();
    expected.sort();
    assert_eq!(files(&dir), expected, "fixture files of L{}", version::MAX_LEVEL);
}

fn cbor_reencode(name: &str, b: &[u8]) {
    use vlpds::cbor::Value;
    let (h, n) = Value::decode_prefix(b).unwrap_or_else(|e| panic!("{name}: {e:?}"));
    let body = Value::decode(&b[n..]).unwrap_or_else(|e| panic!("{name}: {e:?}"));
    let mut out = Vec::new();
    h.encode(&mut out);
    body.encode(&mut out);
    assert!(out == b, "{name}: dag-cbor re-encode differs");
}

fn json_reencode<T: serde::Serialize + serde::de::DeserializeOwned>(name: &str, b: &[u8]) -> T {
    let v: T = serde_json::from_slice(b).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert!(compact(&v) == b, "{name}: JSON re-encode differs");
    v
}

/// Decodes one fixture with this build's readers and re-encodes it.
async fn check(level: u32, name: &str, b: &[u8]) {
    let dir = level_dir(level);
    match name {
        "segment/plain.seg" => {
            let (h, _) = segment::parse_header(b).unwrap().unwrap();
            assert_eq!((h.level, h.codec, h.ordinal, h.prefix_end, h.count), (level, segment::CODEC_NONE, 5, 4, 3));
            let LogObject::Segment(h, entries) = segment::parse(Bytes::copy_from_slice(b), true, None).unwrap() else { panic!("{name}") };
            let mut sb = SegmentBuilder::for_log(&h.log_id);
            for e in &entries {
                let frame = e.frame.clone();
                sb.push_derived(e.seq, e.shard, e.epoch, |o| o.extend_from_slice(&frame), &e.muts, e.derived);
            }
            assert_eq!(entries[0].derived, 4, "#commit muts are derived, not stored");
            assert!(sb.seal(&h.log_id, h.ordinal, h.prefix_end) == b, "{name}: re-encode differs");
        }
        "segment/zstd.seg" => {
            let (h, _) = segment::parse_header(b).unwrap().unwrap();
            assert_eq!((h.level, h.codec), (level, segment::CODEC_ZSTD));
            // zstd's output may change with the library; only decoding is a format
            let plain = std::fs::read(dir.join("segment/plain.seg")).unwrap();
            assert!(segment::decode(Bytes::copy_from_slice(b)).unwrap() == plain, "{name}: decodes to plain.seg");
        }
        "segment/like.seg" => {
            let LogObject::Segment(h, entries) = segment::parse(Bytes::copy_from_slice(b), true, None).unwrap() else { panic!("{name}") };
            let e = &entries[0];
            // c/ put, R/ put, the backlink put, h/
            assert_eq!(e.derived, 4, "{name}: #commit muts derived");
            let bl = &e.muts[2];
            assert_eq!(vlpds::state::key_body(&bl.key)[..3], *b"bl/", "{name}: the like's backlink put");
            assert_eq!(bl.val.as_deref(), Some(&b"3l3qo2vutsw2b"[..]));
            let mut sb = SegmentBuilder::for_log(&h.log_id);
            let frame = e.frame.clone();
            sb.push_derived(e.seq, e.shard, e.epoch, |o| o.extend_from_slice(&frame), &e.muts, e.derived);
            assert!(sb.seal(&h.log_id, h.ordinal, h.prefix_end) == b, "{name}: re-encode differs");
        }
        "state/backlinks.json" => {
            let k: BTreeMap<String, String> = serde_json::from_slice(b).unwrap();
            assert!(pretty(&k) == b);
            let hx = |n: &str| hex::decode(&k[n]).unwrap();
            let link = vlpds::backlinks::link("app.bsky.feed.like", &hx("record")).unwrap();
            assert_eq!(link, hx("link"));
            assert_eq!(vlpds::state::backlink_key(DID, &link), hx("key bl/"));
            assert_eq!(vlpds::state::key_slot(&hx("key bl/")), Some(vlpds::slots::slot_of(DID)));
            let v = vlpds::backlinks::decode(&hx("value"));
            assert_eq!(v.len(), 2);
            assert!(vlpds::backlinks::encode(&v) == hx("value"));
        }
        "segment/fence.bin" => {
            let LogObject::Fence { by } = segment::parse(Bytes::copy_from_slice(b), true, None).unwrap() else { panic!("{name}") };
            assert!(segment::fence_object(&by) == b);
        }
        "state/head.bin" => {
            let h = vlpds::state::Head::decode(&Bytes::copy_from_slice(b)).unwrap();
            assert!(h.encode() == b);
        }
        "state/record.bin" => {
            let v = Bytes::copy_from_slice(b);
            let (cid, rec) = vlpds::state::decode_record_value(&v).unwrap();
            assert!(vlpds::state::record_value(&cid, vlpds::state::record_value_rev(b), &rec) == b);
        }
        "state/account.json" => {
            let a: vlpds::state::Account = json_reencode(name, b);
            assert!(a.extra.contains_key("preferences"), "extension fields round-trip");
        }
        "state/applied2.bin" => {
            let (log, ord) = vlpds::nodelog::decode_marker(b).unwrap();
            assert!(vlpds::nodelog::encode_marker(&log, ord) == b);
        }
        "state/recent.bin" => {
            let dids = vlpds::partition::RecentRepos::decode(b);
            let r = vlpds::partition::RecentRepos::new(8);
            for d in dids.iter().rev() {
                r.touch(d);
            }
            assert!(r.take_dirty().unwrap() == b);
        }
        "state/keys.json" => {
            let k: BTreeMap<String, String> = serde_json::from_slice(b).unwrap();
            assert!(pretty(&k) == b);
            for (n, hexkey) in &k {
                let key = hex::decode(hexkey).unwrap();
                assert_eq!(vlpds::state::key_slot(&key), Some(vlpds::slots::slot_of(DID)), "{n}: slot-prefixed by the DID's slot");
            }
        }
        "control/node_lease.json" => {
            json_reencode::<vlpds::cluster::NodeLease>(name, b);
        }
        "control/assignment.json" => {
            json_reencode::<vlpds::cluster::Assignment>(name, b);
        }
        "control/layout.json" => json_reencode::<vlpds::slots::Layout>(name, b).validate().unwrap(),
        "control/writer_claim.json" => {
            json_reencode::<serde_json::Value>(name, b);
        }
        "control/retain_report.json" => {
            json_reencode::<vlpds::retention::Report>(name, b);
        }
        "control/ratelimits.json" => {
            let d = vlpds::ratelimit::config::parse(b).unwrap();
            assert!(compact(&d) == b);
            vlpds::ratelimit::config::compile(Some(&d)).unwrap();
        }
        "control/cluster_version.json" => {
            json_reencode::<version::ClusterVersion>(name, b);
        }
        "stream/batch.bin" => {
            let vlpds::remote::StreamMsg::Batch(batch) = vlpds::remote::decode(&Arc::from(LOG), Bytes::copy_from_slice(b)).unwrap() else { panic!("{name}") };
            assert!(vlpds::remote::encode_batch(&batch) == b);
        }
        "stream/watermark.bin" => {
            let vlpds::remote::StreamMsg::Watermark(w) = vlpds::remote::decode(&Arc::from(LOG), Bytes::copy_from_slice(b)).unwrap() else { panic!("{name}") };
            assert!(vlpds::remote::encode_watermark(w) == b);
        }
        SECRET_FIXTURE => {
            let blob = std::str::from_utf8(b).unwrap().trim_end();
            let plain = secrets().unwrap(vlpds::secrets::Purpose::SigningKey, DID, blob).await.unwrap();
            assert_eq!(&plain.plaintext[..], SECRET);
            let parts: Vec<&str> = blob.splitn(3, '.').collect();
            assert_eq!((parts[0], parts[1]), ("vw1", vlpds::secrets::KekBytes::new(KEK).kid().as_str()));
            assert_eq!(format!("{}.{}.{}\n", parts[0], parts[1], parts[2]).as_bytes(), b);
        }
        n if n.starts_with("firehose/") => cbor_reencode(n, b),
        n => panic!("L{level}/{n}: no reader check for this fixture"),
    }
}

#[tokio::test]
async fn fixtures_decode_and_reencode() {
    for level in version::MIN_LEVEL..=version::MAX_LEVEL {
        let dir = level_dir(level);
        let names = files(&dir);
        assert!(!names.is_empty(), "no fixtures for level {level}");
        for name in names {
            let b = std::fs::read(dir.join(&name)).unwrap();
            check(level, &name, &b).await;
        }
    }
}

/// `MANIFEST`: `sha256  L{n}/path` per fixture file of every recorded level.
fn manifest() -> BTreeMap<String, String> {
    std::fs::read_to_string(root().join("MANIFEST"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|l| {
            let (h, p) = l.split_once("  ").expect("`sha256  path`");
            (p.to_string(), h.to_string())
        })
        .collect()
}

fn hashes(level: u32) -> BTreeMap<String, String> {
    let dir = level_dir(level);
    files(&dir).into_iter().map(|f| (format!("L{level}/{f}"), hex::encode(Sha256::digest(std::fs::read(dir.join(&f)).unwrap())))).collect()
}

#[test]
fn manifest_freezes_released_levels() {
    let mut m = manifest();
    if bless() {
        for level in 1..=version::MAX_LEVEL {
            let recorded = m.keys().any(|k| k.starts_with(&format!("L{level}/")));
            if level > version::RELEASED || !recorded {
                m.retain(|k, _| !k.starts_with(&format!("L{level}/")));
                m.extend(hashes(level));
            }
        }
        let mut out = String::from("# sha256 of every fixture of a recorded feature level (tests/all/formats.rs).\n# A released level's entries never change: a format change is a new level.\n");
        for (p, h) in &m {
            out.push_str(&format!("{h}  {p}\n"));
        }
        std::fs::write(root().join("MANIFEST"), out).unwrap();
    }
    for level in 1..=version::RELEASED {
        let prefix = format!("L{level}/");
        let recorded: BTreeMap<String, String> = m.iter().filter(|(k, _)| k.starts_with(&prefix)).map(|(k, v)| (k.clone(), v.clone())).collect();
        assert!(!recorded.is_empty(), "released level {level} has no manifest entries");
        assert_eq!(hashes(level), recorded, "level {level} is released: its fixtures are frozen (testdata/formats/MANIFEST)");
    }
}
