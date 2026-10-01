//! atproto data-model interop fixtures: JSON <-> DAG-CBOR <-> CID round trips
//! through `vlpds::cbor` / `vlpds::cid`, and the valid/invalid record data
//! fixtures through createRecord / getRecord.
use base64::Engine;
use crate::common::*;

#[derive(serde::Deserialize)]
struct Fixture {
    json: J,
    cbor_base64: String,
    cid: String,
}

fn b64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(s.trim_end_matches('='))
        .unwrap()
}

#[test]
fn fixtures_json_to_cbor_to_cid() {
    let fixtures: Vec<Fixture> =
        serde_json::from_str(&read_fixture("interop/data-model/data-model-fixtures.json")).unwrap();
    assert!(!fixtures.is_empty());
    for (i, f) in fixtures.iter().enumerate() {
        let want = b64(&f.cbor_base64);
        let v = Value::from_json(&f.json).unwrap_or_else(|e| panic!("fixture {i}: from_json: {e}"));
        let got = v.to_cbor();
        assert_eq!(got, want, "fixture {i}: DAG-CBOR encoding differs");
        assert_eq!(
            Cid::dag_cbor(&got).to_string(),
            f.cid,
            "fixture {i}: CID differs"
        );
    }
}

#[test]
fn fixtures_cbor_to_json() {
    let fixtures: Vec<Fixture> =
        serde_json::from_str(&read_fixture("interop/data-model/data-model-fixtures.json")).unwrap();
    for (i, f) in fixtures.iter().enumerate() {
        let cbor = b64(&f.cbor_base64);
        let v = Value::decode(&cbor).unwrap_or_else(|e| panic!("fixture {i}: decode: {e}"));
        assert_eq!(v.to_json(), f.json, "fixture {i}: CBOR -> JSON differs");
        // and re-encoding the decoded value is byte-identical (canonical)
        assert_eq!(v.to_cbor(), cbor, "fixture {i}: decode/encode not stable");
    }
}

#[test]
fn cbor_decoder_rejects_non_canonical_or_unsupported() {
    // floats
    assert!(
        Value::decode(&[0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18]).is_err(),
        "float64 accepted"
    );
    // indefinite-length array
    assert!(
        Value::decode(&[0x9f, 0x01, 0xff]).is_err(),
        "indefinite length accepted"
    );
    // undefined
    assert!(Value::decode(&[0xf7]).is_err(), "undefined accepted");
    // tag other than 42
    assert!(Value::decode(&[0xc1, 0x01]).is_err(), "tag 1 accepted");
    // non-string map key
    assert!(
        Value::decode(&[0xa1, 0x01, 0x02]).is_err(),
        "int map key accepted"
    );
    // trailing bytes
    assert!(
        Value::decode(&[0x01, 0x02]).is_err(),
        "trailing bytes accepted"
    );
}

#[test]
fn cbor_decoder_strictness_gaps() {
    // DAG-CBOR requires minimal integer encoding, sorted map keys and no
    // duplicate keys. These are strictness checks the reference decoders
    // apply; failures here document decoder gaps (not exploitable for
    // records, which vlpds re-encodes from JSON, but relevant for
    // importRepo / blocks received from clients).
    let mut gaps = Vec::new();
    // 1 encoded with a 1-byte length
    if Value::decode(&[0x18, 0x01]).is_ok() {
        gaps.push("non-minimal integer encoding accepted");
    }
    // {"b":1,"a":2} (unsorted)
    if Value::decode(&[0xa2, 0x61, b'b', 0x01, 0x61, b'a', 0x02]).is_ok() {
        gaps.push("unsorted map keys accepted");
    }
    // {"a":1,"a":2} (duplicate)
    if Value::decode(&[0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02]).is_ok() {
        gaps.push("duplicate map keys accepted");
    }
    assert!(gaps.is_empty(), "DAG-CBOR strictness gaps: {gaps:?}");
}

#[test]
fn json_conversion_edge_cases() {
    // $bytes uses unpadded standard base64
    let v = Value::from_json(&json!({"b": {"$bytes": "AQID"}})).unwrap();
    assert_eq!(v.get("b"), Some(&Value::Bytes(vec![1, 2, 3])));
    assert_eq!(v.to_json(), json!({"b": {"$bytes": "AQID"}}));
    // $link
    let c = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";
    let v = Value::from_json(&json!({"l": {"$link": c}})).unwrap();
    assert_eq!(v.get("l"), Some(&Value::Link(Cid::parse(c).unwrap())));
    // canonical key order: length first, then bytewise
    let v = Value::from_json(&json!({"bb": 1, "a": 2, "c": 3, "aaa": 4})).unwrap();
    match v {
        Value::Map(m) => assert_eq!(
            m.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["a", "c", "bb", "aaa"]
        ),
        _ => panic!(),
    }
    // integer limits
    for n in [
        i64::MIN + 1,
        -1,
        0,
        23,
        24,
        255,
        256,
        65535,
        65536,
        i64::MAX,
    ] {
        let v = Value::Int(n);
        assert_eq!(Value::decode(&v.to_cbor()).unwrap(), v);
    }
}

// ---------------------------------------------------------------------------
// valid / invalid record data, through the server
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct Case {
    note: String,
    json: J,
}

/// Wraps a fixture's object fields into a record of collection com.example.blah.
fn as_record(j: &J) -> J {
    let mut rec = serde_json::Map::new();
    rec.insert("$type".into(), json!("com.example.blah"));
    if let J::Object(o) = j {
        for (k, v) in o {
            rec.insert(k.clone(), v.clone());
        }
        J::Object(rec)
    } else {
        // top-level not an object: send it as the record itself
        j.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_record_data_accepted_and_round_trips() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dmv").await;
    let cases: Vec<Case> =
        serde_json::from_str(&read_fixture("interop/data-model/data-model-valid.json")).unwrap();
    let mut bad = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        let rec = as_record(&c.json);
        let rkey = format!("valid{i}");
        let r = s
            .xrpc
            .post("com.atproto.repo.putRecord", &json!({"repo": a.did, "collection": "com.example.blah", "rkey": rkey, "record": rec}), &a.auth())
            .await;
        if !r.is_ok() {
            bad.push(format!("{}: rejected: {}", c.note, r.text()));
            continue;
        }
        let g = s.get_record(&a.did, "com.example.blah", &rkey).await.ok();
        // JSON numbers like 123.0 are integers in the data model
        let want = serde_json::from_str::<J>(&rec.to_string().replace("123.0", "123")).unwrap();
        if g["value"] != want {
            bad.push(format!(
                "{}: getRecord value {} != {}",
                c.note, g["value"], want
            ));
        }
    }
    assert!(
        bad.is_empty(),
        "valid data-model fixtures:\n  {}",
        bad.join("\n  ")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_record_data_rejected() {
    let s = TestServer::spawn().await;
    let a = s.create_account("dmi").await;
    let cases: Vec<Case> =
        serde_json::from_str(&read_fixture("interop/data-model/data-model-invalid.json")).unwrap();
    let mut bad = Vec::new();
    for c in &cases {
        let rec = as_record(&c.json);
        let r = s
            .xrpc
            .post(
                "com.atproto.repo.createRecord",
                &json!({"repo": a.did, "collection": "com.example.blah", "record": rec}),
                &a.auth(),
            )
            .await;
        if r.status != 400 {
            bad.push(format!("{}: createRecord -> {}", c.note, r.text()));
        }
    }
    let l = s.list_records(&a.did, "com.example.blah", &[]).await;
    let stored = l.json["records"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        bad.is_empty() && stored == 0,
        "invalid data-model fixtures ({stored} stored):\n  {}",
        bad.join("\n  ")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixture_records_round_trip_through_server_with_matching_cids() {
    // Every data-model fixture (plus a $type) stored via putRecord comes back
    // byte-identical: getRecord JSON equals the input and the returned CID is
    // the DAG-CBOR CID of the canonical encoding; sync.getRecord's block bytes
    // equal our own encoding.
    let s = TestServer::spawn().await;
    let a = s.create_account("dmf").await;
    let fixtures: Vec<Fixture> =
        serde_json::from_str(&read_fixture("interop/data-model/data-model-fixtures.json")).unwrap();
    for (i, f) in fixtures.iter().enumerate() {
        let mut rec = f.json.clone();
        rec["$type"] = json!("com.example.fixture");
        let want_bytes = Value::from_json(&rec).unwrap().to_cbor();
        let want_cid = Cid::dag_cbor(&want_bytes);
        let rkey = format!("f{i}");
        let r = s
            .xrpc
            .post("com.atproto.repo.putRecord", &json!({"repo": a.did, "collection": "com.example.fixture", "rkey": rkey, "record": rec}), &a.auth())
            .await;
        // A fixture referencing a blob this repo never uploaded is refused, as
        // the reference does (actor-store/blob/transactor.ts
        // processWriteBlobs -> "Could not find blob"); its encoding is still
        // covered by fixtures_json_to_cbor_to_cid.
        if rec.to_string().contains(r#""$type":"blob""#) {
            r.err(400, "BlobNotFound");
            continue;
        }
        let r = r.ok();
        assert_eq!(
            r["cid"],
            json!(want_cid.to_string()),
            "fixture {i}: putRecord cid"
        );
        let g = s
            .get_record(&a.did, "com.example.fixture", &rkey)
            .await
            .ok();
        assert_eq!(g["value"], rec, "fixture {i}: getRecord value");
        assert_eq!(
            g["cid"],
            json!(want_cid.to_string()),
            "fixture {i}: getRecord cid"
        );
        let car = s
            .xrpc
            .get(
                "com.atproto.sync.getRecord",
                &[
                    ("did", &a.did),
                    ("collection", "com.example.fixture"),
                    ("rkey", &rkey),
                ],
                &Auth::None,
            )
            .await;
        assert_eq!(car.status, 200, "sync.getRecord: {}", car.text());
        let repo = Repo::from_car(&car.body).unwrap();
        assert_eq!(
            repo.blocks.get(&want_cid),
            Some(&want_bytes),
            "fixture {i}: record block bytes"
        );
    }
}
