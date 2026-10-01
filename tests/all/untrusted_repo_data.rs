//! Repo data from untrusted CARs: a crafted MST deep enough to overflow the
//! stack used to abort the whole process (load_from_blocks recursion). It is
//! rejected by both paths that load attacker-chosen CARs: importRepo and the
//! record-proof check behind OAuth `include:` scopes.
use crate::common::*;
use vlpds::cbor::{self, key_cmp};
use vlpds::crypto::Keypair;

/// Deep enough to overflow a 2 MiB stack in the unbounded loader.
const DEPTH: usize = 200_000;

/// A signed commit whose MST is a leaf holding `rpath`, under `depth`
/// key-less `{e: [], l: child}` nodes.
fn deep_car(did: &str, kp: &Keypair, rpath: &str, depth: usize) -> Vec<u8> {
    let mut blocks: Vec<(Cid, Vec<u8>)> = Vec::new();
    let rec =
        Value::from_json(&json!({"$type": "com.atproto.lexicon.schema", "id": "com.example.deep"}))
            .unwrap()
            .to_cbor();
    let rec_cid = Cid::dag_cbor(&rec);
    blocks.push((rec_cid, rec));
    let node = |l: Option<Cid>, key: Option<&str>| {
        let mut b = Vec::new();
        cbor::write_map_head(&mut b, 2);
        cbor::write_text(&mut b, "e");
        match key {
            Some(k) => {
                cbor::write_array_head(&mut b, 1);
                cbor::write_map_head(&mut b, 4);
                cbor::write_text(&mut b, "k");
                cbor::write_bytes(&mut b, k.as_bytes());
                cbor::write_text(&mut b, "p");
                cbor::write_uint(&mut b, 0);
                cbor::write_text(&mut b, "t");
                cbor::write_null(&mut b);
                cbor::write_text(&mut b, "v");
                cbor::write_cid(&mut b, &rec_cid);
            }
            None => cbor::write_array_head(&mut b, 0),
        }
        cbor::write_text(&mut b, "l");
        cbor::write_opt_cid(&mut b, l.as_ref());
        b
    };
    let leaf = node(None, Some(rpath));
    let mut c = Cid::dag_cbor(&leaf);
    blocks.push((c, leaf));
    for _ in 0..depth {
        let b = node(Some(c), None);
        c = Cid::dag_cbor(&b);
        blocks.push((c, b));
    }
    let mut fields = vec![
        ("did".to_string(), Value::Text(did.to_string())),
        ("rev".to_string(), Value::Text("3l3qo2vuowo2b".to_string())),
        ("data".to_string(), Value::Link(c)),
        ("prev".to_string(), Value::Null),
        ("version".to_string(), Value::Int(3)),
    ];
    fields.sort_by(|a, b| key_cmp(&a.0, &b.0));
    let sig = kp.sign(&Value::Map(fields.clone()).to_cbor());
    fields.push(("sig".to_string(), Value::Bytes(sig.to_vec())));
    fields.sort_by(|a, b| key_cmp(&a.0, &b.0));
    let commit = Value::Map(fields).to_cbor();
    let root = Cid::dag_cbor(&commit);
    let mut car = Vec::new();
    vlpds::car::write_header(&mut car, &root);
    vlpds::car::write_block(&mut car, &root, &commit);
    for (c, b) in blocks.iter().rev() {
        vlpds::car::write_block(&mut car, c, b);
    }
    car
}

#[test]
fn record_proof_with_deep_mst_is_an_error() {
    let kp = Keypair::generate();
    let did = "did:plc:deepdeepdeepdeepdeepdeep";
    let rpath = "com.atproto.lexicon.schema/com.example.deep";
    let key = kp.public_multibase();
    let shallow = deep_car(did, &kp, rpath, 3);
    let deep = deep_car(did, &kp, rpath, DEPTH);
    // a tokio blocking thread's stack
    let (shallow, deep) = std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            (
                vlpds::oauth::lexicon::verify_record_proof(&shallow, did, &key, rpath),
                vlpds::oauth::lexicon::verify_record_proof(&deep, did, &key, rpath),
            )
        })
        .unwrap()
        .join()
        .unwrap();
    // a short chain of key-less nodes is a valid tree
    assert_eq!(shallow.unwrap()["id"], "com.example.deep");
    let err = deep.unwrap_err();
    assert!(err.contains("too deep"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn import_repo_with_deep_mst_is_rejected() {
    let s = TestServer::spawn().await;
    let a = s.create_account("deep").await;
    let car = deep_car(
        &a.did,
        &Keypair::generate(),
        "com.example.deep/3l3qo2vuowo2b",
        DEPTH,
    );
    let r = s
        .xrpc
        .post_bytes(
            "com.atproto.repo.importRepo",
            car,
            "application/vnd.ipld.car",
            &a.auth(),
        )
        .await;
    r.err(400, "InvalidRequest");
    assert!(r.text().contains("too deep"), "{}", r.text());
    // the server is still up
    s.xrpc
        .get("com.atproto.server.describeServer", &[], &Auth::None)
        .await
        .ok();
}
