//! shrike 0.7.0 hot-path benchmarks, optionally against an independent
//! implementation (`--features compare`). See README.md.
//!
//! usage: shrike-perf [--rounds N] [--ms T] [--out results.jsonl] [--mst-n N] [filter...]
//! filters match "group/case" substrings (e.g. `cbor.decode`, `mst.`).

mod data;
mod timing;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use shrike::cbor as sc;
use shrike::cbor::json::{Integers, drisl_to_json, json_to_drisl};
use shrike::crypto::{SigningKey as _, VerifyingKey as _};
use shrike::mst::{DetachedTree, NoBlocks};
use std::collections::{HashMap, HashSet};
use std::hint::black_box;
use std::str::FromStr;
use std::time::{Duration, Instant};
use timing::{Opts, Row, compare, median};

#[cfg(feature = "compare")]
use vlpds::cid::Cid as VCid;

#[cfg(feature = "compare")]
const OTHER: &str = "independent";
#[cfg(not(feature = "compare"))]
const OTHER: &str = "-";

/// `cmp!(opts, group, case, unit, per, a_label, a_closure, b_closure)`: the
/// b side (the independent implementation) only exists with `compare`.
macro_rules! cmp {
    ($o:expr, $g:expr, $c:expr, $unit:expr, $per:expr, $al:expr, $a:expr, $b:expr) => {{
        let mut a = $a;
        #[cfg(feature = "compare")]
        {
            let mut b = $b;
            compare($o, $g, $c, $unit, $per, $al, &mut a, OTHER, Some(&mut b));
        }
        #[cfg(not(feature = "compare"))]
        {
            compare($o, $g, $c, $unit, $per, $al, &mut a, OTHER, None);
        }
    }};
}

/// Two shrike-reproducible alternatives (no comparison implementation needed).
macro_rules! ab {
    ($o:expr, $g:expr, $c:expr, $unit:expr, $per:expr, $al:expr, $a:expr, $bl:expr, $b:expr) => {{
        let mut a = $a;
        let mut b = $b;
        compare($o, $g, $c, $unit, $per, $al, &mut a, $bl, Some(&mut b));
    }};
}

#[cfg(feature = "compare")]
fn v_cid(c: &sc::Cid) -> VCid {
    VCid::from_bytes(&c.to_bytes()).unwrap()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut o = Opts { rounds: 11, target: Duration::from_millis(60), filter: Vec::new(), out: None };
    let mut mst_n: usize = 1_000_000;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--rounds" => o.rounds = args.next().unwrap().parse().unwrap(),
            "--ms" => o.target = Duration::from_millis(args.next().unwrap().parse().unwrap()),
            "--out" => o.out = Some(std::fs::File::create(args.next().unwrap()).unwrap()),
            "--mst-n" => mst_n = args.next().unwrap().parse().unwrap(),
            f => o.filter.push(f.to_string()),
        }
    }
    println!(
        "# shrike-perf: mode={} sha2-asm-feature={} rounds={} batch={:?}",
        if cfg!(feature = "compare") { "compare" } else { "shrike-only" },
        cfg!(feature = "sha2-asm"),
        o.rounds,
        o.target
    );

    let repo = data::load_repo();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for (k, _) in &repo.entries {
        *counts.entry(k.split_once('/').unwrap().0).or_default() += 1;
    }
    let mut cv: Vec<_> = counts.into_iter().collect();
    cv.sort_by(|a, b| b.1.cmp(&a.1));
    println!(
        "# repo CAR: {} bytes, {} blocks, {} records: {:?}",
        repo.car.len(),
        repo.blocks.len(),
        repo.entries.len(),
        &cv[..cv.len().min(8)]
    );

    cbor(&mut o, &repo);
    json(&mut o, &repo);
    cid(&mut o, &repo);
    lexicon(&mut o, &repo);
    crypto(&mut o);
    car(&mut o, &repo);
    firehose(&mut o);
    mst_codec(&mut o, &repo);
    proofs(&mut o, &repo);
    let real: Vec<String> = repo.entries.iter().map(|(k, _)| k.clone()).collect();
    mst_build_and_commits(&mut o, "real", real);
    if o.filter.is_empty() || o.filter.iter().any(|f| f.contains("mst")) {
        mst_build_and_commits(&mut o, "synthetic", synthetic_keys(mst_n));
    }
}

const KINDS: &[(&str, &str)] = &[
    ("post", "app.bsky.feed.post"),
    ("like", "app.bsky.feed.like"),
    ("follow", "app.bsky.graph.follow"),
    ("repost", "app.bsky.feed.repost"),
    ("profile", "app.bsky.actor.profile"),
];

const CAP: usize = 5000;

// ---------------------------------------------------------------------------
// DAG-CBOR decode / encode
// ---------------------------------------------------------------------------

fn cbor(o: &mut Opts, repo: &data::RepoData) {
    let mut sets: Vec<(String, Vec<&[u8]>)> = Vec::new();
    for (short, coll) in KINDS {
        let recs = data::records(repo, coll, CAP);
        if !recs.is_empty() {
            sets.push((format!("{short} x{}", recs.len()), recs.into_iter().map(|r| r.1).collect()));
        }
    }
    let fixtures = data::interop_fixtures();
    if !fixtures.is_empty() {
        sets.push((format!("interop data-model x{}", fixtures.len()), fixtures.iter().map(|f| f.1.as_slice()).collect()));
    }
    for (name, recs) in &sets {
        let n = recs.len() as f64;
        cmp!(
            o, "cbor.decode", name, "record", n, "shrike cbor::decode",
            || for b in recs { black_box(sc::decode(black_box(b)).unwrap()); },
            || for b in recs { black_box(vlpds::cbor::Value::decode(black_box(b)).unwrap()); }
        );
    }
    // shrike's arena decoder (shrike-only)
    for (name, recs) in sets.iter().take(1) {
        let n = recs.len() as f64;
        let mut bump = bumpalo::Bump::new();
        ab!(
            o, "cbor.decode", &format!("{name} (decode vs decode_bump)"), "record", n, "shrike decode",
            || for b in recs { black_box(sc::decode(black_box(b)).unwrap()); },
            "shrike decode_bump",
            || for b in recs {
                bump.reset();
                black_box(sc::Decoder::new(black_box(b)).decode_bump(&bump).unwrap());
            }
        );
    }
    for (name, recs) in &sets {
        let n = recs.len() as f64;
        let st: Vec<sc::Value> = recs.iter().map(|b| sc::decode(b).unwrap()).collect();
        #[cfg(feature = "compare")]
        let vt: Vec<vlpds::cbor::Value> = recs.iter().map(|b| vlpds::cbor::Value::decode(b).unwrap()).collect();
        let mut sbuf = Vec::with_capacity(4096);
        #[cfg(feature = "compare")]
        let mut vbuf = Vec::with_capacity(4096);
        // both re-encode every record to its original bytes
        for (i, v) in st.iter().enumerate() {
            assert_eq!(sc::encode_value(v).unwrap(), recs[i]);
        }
        cmp!(
            o, "cbor.encode", name, "record", n, "shrike encode_value_into",
            || for v in &st {
                sbuf.clear();
                sc::encode_value_into(black_box(v), &mut sbuf).unwrap();
                black_box(&sbuf);
            },
            || for v in &vt {
                vbuf.clear();
                black_box(v).encode(&mut vbuf);
                black_box(&vbuf);
            }
        );
    }
}

// ---------------------------------------------------------------------------
// JSON <-> DAG-CBOR
// ---------------------------------------------------------------------------

fn json(o: &mut Opts, repo: &data::RepoData) {
    for (short, coll) in KINDS {
        let recs = data::records(repo, coll, CAP);
        if recs.is_empty() {
            continue;
        }
        let name = format!("{short} x{}", recs.len());
        let n = recs.len() as f64;
        let bytes: Vec<&[u8]> = recs.iter().map(|r| r.1).collect();
        let texts: Vec<Vec<u8>> = bytes.iter().map(|b| serde_json::to_vec(&drisl_to_json(b).unwrap()).unwrap()).collect();
        // correctness: both produce the original record bytes / equal JSON
        for (t, b) in texts.iter().zip(&bytes) {
            let j: serde_json::Value = serde_json::from_slice(t).unwrap();
            assert_eq!(&json_to_drisl(&j, Integers::Safe).unwrap(), b);
            #[cfg(feature = "compare")]
            {
                let mut jv = vlpds::cbor::JsonValue::parse(t).unwrap();
                let mut out = Vec::new();
                jv.encode_record(&mut out, &mut vlpds::cbor::RecordRefs::default()).unwrap();
                assert_eq!(&out, b);
                let mut js = Vec::new();
                vlpds::cbor::write_json(b, &mut js).unwrap();
                assert_eq!(serde_json::from_slice::<serde_json::Value>(&js).unwrap(), j);
            }
        }
        cmp!(
            o, "json->cbor", &format!("{name} (parse text + encode)"), "record", n,
            "shrike serde_json::Value + json_to_drisl",
            || for t in &texts {
                let j: serde_json::Value = serde_json::from_slice(black_box(t)).unwrap();
                black_box(json_to_drisl(&j, Integers::Safe).unwrap());
            },
            || for t in &texts {
                let mut jv = vlpds::cbor::JsonValue::parse(black_box(t)).unwrap();
                let mut out = Vec::new();
                let mut refs = vlpds::cbor::RecordRefs::default();
                jv.encode_record(&mut out, &mut refs).unwrap();
                black_box((out, refs));
            }
        );
        // the parse half and the encode half separately
        cmp!(
            o, "json->cbor", &format!("{name} (parse only)"), "record", n, "shrike serde_json::Value",
            || for t in &texts { black_box(serde_json::from_slice::<serde_json::Value>(black_box(t)).unwrap()); },
            || for t in &texts { black_box(vlpds::cbor::JsonValue::parse(black_box(t)).unwrap()); }
        );
        let parsed: Vec<serde_json::Value> = texts.iter().map(|t| serde_json::from_slice(t).unwrap()).collect();
        #[cfg(feature = "compare")]
        let mut vparsed: Vec<vlpds::cbor::JsonValue> = texts.iter().map(|t| vlpds::cbor::JsonValue::parse(t).unwrap()).collect();
        cmp!(
            o, "json->cbor", &format!("{name} (encode pre-parsed)"), "record", n, "shrike json_to_drisl",
            || for j in &parsed { black_box(json_to_drisl(black_box(j), Integers::Safe).unwrap()); },
            || for jv in vparsed.iter_mut() {
                let mut out = Vec::new();
                let mut refs = vlpds::cbor::RecordRefs::default();
                black_box(&mut *jv).encode_record(&mut out, &mut refs).unwrap();
                black_box((out, refs));
            }
        );
        cmp!(
            o, "cbor->json", &format!("{name} (bytes -> JSON text)"), "record", n,
            "shrike drisl_to_json + serde_json::to_vec",
            || for b in &bytes {
                let j = drisl_to_json(black_box(b)).unwrap();
                black_box(serde_json::to_vec(&j).unwrap());
            },
            || for b in &bytes {
                let mut out = Vec::new();
                vlpds::cbor::write_json(black_box(b), &mut out).unwrap();
                black_box(out);
            }
        );
        cmp!(
            o, "cbor->json", &format!("{name} (bytes -> serde_json::Value only)"), "record", n,
            "shrike drisl_to_json",
            || for b in &bytes { black_box(drisl_to_json(black_box(b)).unwrap()); },
            || for b in &bytes {
                let mut out = Vec::new();
                vlpds::cbor::write_json(black_box(b), &mut out).unwrap();
                black_box(out);
            }
        );
    }
}

// ---------------------------------------------------------------------------
// CIDs
// ---------------------------------------------------------------------------

fn cid(o: &mut Opts, repo: &data::RepoData) {
    let recs: Vec<&[u8]> = data::records(repo, "app.bsky.feed.post", CAP).into_iter().map(|r| r.1).collect();
    let avg = recs.iter().map(|r| r.len()).sum::<usize>() / recs.len().max(1);
    let n = recs.len() as f64;
    cmp!(
        o, "cid", &format!("compute (sha-256) post x{} avg {avg} B", recs.len()), "cid", n, "shrike Cid::compute",
        || for b in &recs { black_box(sc::Cid::compute(sc::Codec::Drisl, black_box(b))); },
        || for b in &recs { black_box(VCid::dag_cbor(black_box(b))); }
    );
    let cids: Vec<sc::Cid> = repo.entries.iter().take(CAP).map(|e| e.1).collect();
    let strs: Vec<String> = cids.iter().map(|c| c.to_string()).collect();
    let n = cids.len() as f64;
    #[cfg(feature = "compare")]
    let vcids: Vec<VCid> = cids.iter().map(v_cid).collect();
    cmp!(
        o, "cid", &format!("to_string x{}", cids.len()), "cid", n, "shrike Display",
        || for c in &cids { black_box(black_box(c).to_string()); },
        || for c in &vcids { black_box(black_box(c).to_string()); }
    );
    cmp!(
        o, "cid", &format!("parse x{}", strs.len()), "cid", n, "shrike Cid::from_str",
        || for s in &strs { black_box(sc::Cid::from_str(black_box(s)).unwrap()); },
        || for s in &strs { black_box(VCid::parse(black_box(s)).unwrap()); }
    );
    cmp!(
        o, "cid", &format!("parse $link x{}", strs.len()), "cid", n, "shrike json::parse_cid",
        || for s in &strs { black_box(sc::json::parse_cid(black_box(s)).unwrap()); },
        || for s in &strs { black_box(VCid::parse(black_box(s)).unwrap()); }
    );
}

// ---------------------------------------------------------------------------
// lexicon validation
// ---------------------------------------------------------------------------

fn lexicon(o: &mut Opts, repo: &data::RepoData) {
    if !o.wants("lexicon") {
        return;
    }
    let bundle = data::env_path("LEXICON_BUNDLE", "/path/to/vlpds/lexicons/bundle.json");
    let docs: HashMap<String, serde_json::Value> = serde_json::from_slice(&std::fs::read(&bundle).expect("lexicon bundle ({nsid: doc})")).unwrap();
    let mut catalog = shrike::lexicon::Catalog::new();
    let mut bad = 0;
    for d in docs.values() {
        if catalog.add_schema(&serde_json::to_vec(d).unwrap()).is_err() {
            bad += 1;
        }
    }
    println!("# lexicon catalog: {} docs ({bad} rejected by shrike)", docs.len());
    for (short, coll) in KINDS {
        let recs = data::records(repo, coll, CAP);
        let mut vals: Vec<(&str, serde_json::Value)> = Vec::new();
        let (mut s_rej, mut v_rej) = (0, 0);
        for (rkey, b) in &recs {
            let j = drisl_to_json(b).unwrap();
            let s_ok = shrike::lexicon::validate_record(&catalog, coll, &j).is_ok();
            #[cfg(feature = "compare")]
            let v_ok = vlpds::lexicon::validate_record(coll, rkey, &j, None, None).is_ok();
            #[cfg(not(feature = "compare"))]
            let v_ok = true;
            s_rej += !s_ok as usize;
            v_rej += !v_ok as usize;
            if s_ok && v_ok {
                vals.push((rkey, j));
            }
        }
        if vals.is_empty() {
            continue;
        }
        println!("# lexicon {coll}: {} records, rejected by shrike {s_rej}, by {OTHER} {v_rej}; timing the {} both accept", recs.len(), vals.len());
        let n = vals.len() as f64;
        cmp!(
            o, "lexicon", &format!("{short} x{}", vals.len()), "record", n, "shrike validate_record",
            || for (_, j) in &vals { black_box(shrike::lexicon::validate_record(&catalog, coll, black_box(j)).unwrap()); },
            || for (rkey, j) in &vals { black_box(vlpds::lexicon::validate_record(coll, rkey, black_box(j), None, None).unwrap()); }
        );
    }
}

// ---------------------------------------------------------------------------
// signatures
// ---------------------------------------------------------------------------

fn crypto(o: &mut Opts) {
    let sk_bytes: [u8; 32] = StdRng::seed_from_u64(7).r#gen();
    // a commit's unsigned bytes are ~150 B; signing hashes them first
    let msg: Vec<u8> = (0..150u8).collect();
    let s = shrike::crypto::K256SigningKey::from_bytes(&sk_bytes).unwrap();
    let sig = s.sign(&msg).unwrap();
    let pk33 = s.public_key().to_bytes();
    let svk = shrike::crypto::K256VerifyingKey::from_bytes(&pk33).unwrap();
    #[cfg(feature = "compare")]
    let v = vlpds::crypto::Keypair::from_bytes(&sk_bytes).unwrap();
    #[cfg(feature = "compare")]
    assert_eq!(&v.sign(&msg), sig.as_bytes(), "both are RFC 6979 + low-S");
    cmp!(
        o, "crypto", "k256 sign (150 B msg)", "sig", 1.0, "shrike K256SigningKey::sign",
        || { black_box(s.sign(black_box(&msg)).unwrap()); },
        || { black_box(v.sign(black_box(&msg))); }
    );
    cmp!(
        o, "crypto", "k256 verify (150 B msg)", "verify", 1.0, "shrike K256VerifyingKey::verify",
        || { svk.verify(black_box(&msg), black_box(&sig)).unwrap(); },
        // parses the SEC1 key on every call: its API takes key bytes
        || { assert!(vlpds::crypto::verify_k256(black_box(&pk33), black_box(&msg), sig.as_bytes()).unwrap()); }
    );
    // shrike-reproducible: the same operations on libsecp256k1 (`secp256k1` crate)
    {
        use secp256k1::{Message, PublicKey, SECP256K1, SecretKey, ecdsa::Signature};
        use sha2_digest::digest;
        let sk = SecretKey::from_byte_array(&sk_bytes).unwrap();
        let pk = PublicKey::from_slice(&pk33).unwrap();
        let lsig = Signature::from_compact(sig.as_bytes()).unwrap();
        ab!(
            o, "crypto", "k256 sign: k256 crate vs libsecp256k1", "sig", 1.0, "shrike (RustCrypto k256)",
            || { black_box(s.sign(black_box(&msg)).unwrap()); },
            "secp256k1 crate",
            || {
                let mut sig = SECP256K1.sign_ecdsa(&Message::from_digest(digest(black_box(&msg))), &sk);
                sig.normalize_s();
                black_box(sig.serialize_compact());
            }
        );
        ab!(
            o, "crypto", "k256 verify: k256 crate vs libsecp256k1", "verify", 1.0, "shrike (RustCrypto k256)",
            || { svk.verify(black_box(&msg), black_box(&sig)).unwrap(); },
            "secp256k1 crate (parsed key)",
            || {
                SECP256K1.verify_ecdsa(&Message::from_digest(digest(black_box(&msg))), &lsig, &pk).unwrap();
            }
        );
    }
    // P-256: both sides use RustCrypto p256
    let p = shrike::crypto::P256SigningKey::from_bytes(&sk_bytes).unwrap();
    let psig = p.sign(&msg).unwrap();
    let ppk = shrike::crypto::P256VerifyingKey::from_bytes(&p.public_key().to_bytes()).unwrap();
    {
        use p256::ecdsa::signature::{Signer, Verifier};
        let rk = p256::ecdsa::SigningKey::from_bytes((&sk_bytes).into()).unwrap();
        let rvk = *rk.verifying_key();
        let rsig = p256::ecdsa::Signature::from_slice(psig.as_bytes()).unwrap();
        ab!(
            o, "crypto", "p256 sign (150 B msg)", "sig", 1.0, "shrike P256SigningKey::sign",
            || { black_box(p.sign(black_box(&msg)).unwrap()); },
            "p256 crate directly",
            || {
                let s: p256::ecdsa::Signature = rk.sign(black_box(&msg));
                black_box(s.normalize_s().unwrap_or(s));
            }
        );
        ab!(
            o, "crypto", "p256 verify (150 B msg)", "verify", 1.0, "shrike P256VerifyingKey::verify",
            || { ppk.verify(black_box(&msg), black_box(&psig)).unwrap(); },
            "p256 crate directly",
            || { rvk.verify(black_box(&msg), &rsig).unwrap(); }
        );
    }
}

/// sha256 without depending on a particular sha2 major in this crate.
mod sha2_digest {
    pub fn digest(b: &[u8]) -> [u8; 32] {
        *shrike::cbor::Cid::compute(shrike::cbor::Codec::Raw, b).hash()
    }
}

// ---------------------------------------------------------------------------
// CAR
// ---------------------------------------------------------------------------

fn car(o: &mut Opts, repo: &data::RepoData) {
    let car = &repo.car;
    let mb = car.len() as f64 / 1e6;
    let nblocks = repo.blocks.len();
    cmp!(
        o, "car", &format!("read_all {:.1} MB, {nblocks} blocks", mb), "MB", mb, "shrike car::read_all",
        || { black_box(shrike::car::read_all(black_box(&car[..])).unwrap()); },
        || { black_box(vlpds::car::read_car(black_box(car)).unwrap()); }
    );
    ab!(
        o, "car", &format!("shrike read_all vs Reader::next_block_into {:.1} MB", mb), "MB", mb, "shrike car::read_all",
        || { black_box(shrike::car::read_all(black_box(&car[..])).unwrap()); },
        "shrike next_block_into (reused buffer)",
        || {
            let mut r = shrike::car::Reader::new(black_box(&car[..])).unwrap();
            let mut b = shrike::car::Block::default();
            while r.next_block_into(&mut b).unwrap() {
                black_box(&b);
            }
        }
    );
    cmp!(
        o, "car", &format!("verify (hash every block) {:.1} MB", mb), "MB", mb, "shrike car::verify",
        || { shrike::car::verify(black_box(&car[..])).unwrap(); },
        || {
            let (_, blocks) = vlpds::car::read_car(black_box(car)).unwrap();
            for (c, d) in blocks {
                let h = if c.codec == vlpds::cid::CODEC_RAW { VCid::raw(d) } else { VCid::dag_cbor(d) };
                assert!(h == c);
            }
        }
    );
    let (roots, blocks) = shrike::car::read_all(&car[..]).unwrap();
    #[cfg(feature = "compare")]
    let (vroots, vblocks) = vlpds::car::read_car(car).unwrap();
    cmp!(
        o, "car", &format!("write {:.1} MB, {nblocks} blocks", mb), "MB", mb, "shrike car::write_all",
        || { black_box(shrike::car::write_all(&roots, black_box(&blocks)).unwrap()); },
        || {
            let mut out = Vec::with_capacity(car.len() + 64);
            vlpds::car::write_header(&mut out, &vroots[0]);
            for (c, d) in black_box(&vblocks) {
                vlpds::car::write_block(&mut out, c, d);
            }
            black_box(out);
        }
    );
}

// ---------------------------------------------------------------------------
// firehose frames (shrike only: the other implementation has no decoder)
// ---------------------------------------------------------------------------

fn firehose(o: &mut Opts) {
    let frames = data::firehose_frames();
    if frames.is_empty() {
        return;
    }
    let bytes: usize = frames.iter().map(|f| f.len()).sum();
    for f in &frames {
        shrike::streaming::parse_firehose_frame(f).unwrap();
    }
    let n = frames.len() as f64;
    // the floor: the generic work the frame decoder can't avoid (decode the
    // header and body, read the blocks CAR, hash each block)
    ab!(
        o, "firehose", &format!("#commit frames x{} ({} B avg)", frames.len(), bytes / frames.len()), "frame", n,
        "shrike parse_firehose_frame",
        || for f in &frames { black_box(shrike::streaming::parse_firehose_frame(black_box(f)).unwrap()); },
        "floor: shrike decode + CAR Reader + Cid::compute",
        || for f in &frames {
            let mut d = sc::Decoder::new(black_box(f));
            let _h = d.decode().unwrap();
            let body = d.decode().unwrap();
            let sc::Value::Map(m) = &body else { panic!() };
            let blocks = m.iter().find(|(k, _)| *k == "blocks").map(|(_, v)| v);
            if let Some(sc::Value::Bytes(car)) = blocks {
                let mut r = shrike::car::Reader::new(&car[..]).unwrap();
                let mut b = shrike::car::Block::default();
                while r.next_block_into(&mut b).unwrap() {
                    assert!(sc::Cid::compute(b.cid.codec(), &b.data) == b.cid);
                }
            }
            black_box(body);
        }
    );
}

// ---------------------------------------------------------------------------
// MST node codec
// ---------------------------------------------------------------------------

fn mst_codec(o: &mut Opts, repo: &data::RepoData) {
    // the repo's MST node blocks: every block on the walk from the data root
    let mut nodes: Vec<(sc::Cid, &[u8])> = Vec::new();
    let mut stack = vec![repo.data_root];
    while let Some(c) = stack.pop() {
        let b = &repo.blocks[&c];
        let nd = shrike::mst::node::decode_node_data(b).unwrap();
        stack.extend(nd.left);
        stack.extend(nd.entries.iter().filter_map(|e| e.right));
        nodes.push((c, b));
    }
    let n = nodes.len() as f64;
    cmp!(
        o, "mst.node", &format!("decode x{} real nodes", nodes.len()), "node", n, "shrike decode_node_data",
        || for (_, b) in &nodes { black_box(shrike::mst::node::decode_node_data(black_box(b)).unwrap()); },
        || for (c, b) in &nodes { black_box(vlpds::mst::decode_node(black_box(b), v_cid(c)).unwrap()); }
    );
    let snodes: Vec<_> = nodes.iter().map(|(_, b)| shrike::mst::node::decode_node_data(b).unwrap()).collect();
    #[cfg(feature = "compare")]
    let vnodes: Vec<_> = nodes.iter().map(|(c, b)| vlpds::mst::decode_node(b, v_cid(c)).unwrap()).collect();
    #[cfg(feature = "compare")]
    let mut buf = Vec::with_capacity(8192);
    cmp!(
        o, "mst.node", &format!("encode x{} real nodes", nodes.len()), "node", n, "shrike encode_node_data",
        || for nd in &snodes { black_box(shrike::mst::node::encode_node_data(black_box(nd)).unwrap()); },
        || for nd in &vnodes {
            buf.clear();
            vlpds::mst::encode_node(black_box(nd), &mut buf).unwrap();
            black_box(&buf);
        }
    );
}

// ---------------------------------------------------------------------------
// record proofs (generation and verification) on the real repo
// ---------------------------------------------------------------------------

fn proofs(o: &mut Opts, repo: &data::RepoData) {
    if !o.wants("proof") {
        return;
    }
    let key = shrike::crypto::K256SigningKey::from_bytes(&[9u8; 32]).unwrap();
    let did = shrike::syntax::Did::try_from("did:plc:benchbenchbenchbenchbenc").unwrap();
    let rev = shrike::syntax::Tid::new(1_700_000_000_000_000, 0).unwrap();
    let signed = shrike::repo::Commit::create_signed(did.clone(), rev, repo.data_root, &key).unwrap();
    let mut blocks = repo.blocks.clone();
    blocks.insert(signed.cid, signed.bytes.clone());
    let mut rng = StdRng::seed_from_u64(42);
    let sample: Vec<(String, shrike::syntax::Nsid, shrike::syntax::RecordKey)> = repo
        .entries
        .choose_multiple(&mut rng, 1000)
        .map(|(k, _)| {
            let (c, r) = k.split_once('/').unwrap();
            (k.clone(), shrike::syntax::Nsid::try_from(c).unwrap(), shrike::syntax::RecordKey::try_from(r).unwrap())
        })
        .collect();
    #[cfg(feature = "compare")]
    let (vtree, vrecords, vcommit) = {
        let mut t = vlpds::mst::Tree::new();
        for (k, c) in &repo.entries {
            t.insert_no_proof(k.as_bytes(), v_cid(c)).unwrap();
        }
        assert_eq!(t.root_cid().unwrap(), v_cid(&repo.data_root), "canonical MST");
        let recs: HashMap<VCid, &[u8]> = repo.blocks.iter().map(|(c, b)| (v_cid(c), b.as_slice())).collect();
        (t, recs, v_cid(&signed.cid))
    };
    let n = sample.len() as f64;
    cmp!(
        o, "proof", &format!("generate record proof CAR x{} (real repo, {} keys)", sample.len(), repo.entries.len()), "proof", n,
        "shrike record_proof_car (from blocks)",
        || for (_, c, r) in &sample {
            black_box(shrike::repo::record_proof_car(&blocks, &signed.cid, c, r).unwrap());
        },
        || for (k, _, _) in &sample {
            let mut out = Vec::new();
            vlpds::car::write_header(&mut out, &vcommit);
            vlpds::car::write_block(&mut out, &vcommit, &signed.bytes);
            for (c, b) in vtree.proof_blocks(k.as_bytes()).unwrap() {
                vlpds::car::write_block(&mut out, &c, &b);
            }
            if let Some(rc) = vtree.get(k.as_bytes()).unwrap() {
                vlpds::car::write_block(&mut out, &rc, vrecords[&rc]);
            }
            black_box(out);
        }
    );
    let cars: Vec<Vec<u8>> = sample.iter().take(200).map(|(_, c, r)| shrike::repo::record_proof_car(&blocks, &signed.cid, c, r).unwrap()).collect();
    let vk = key.public_key();
    #[cfg(feature = "compare")]
    let mb = vk.multibase();
    let n = cars.len() as f64;
    cmp!(
        o, "proof", &format!("verify record proof CAR x{} (k256 commit)", cars.len()), "proof", n,
        "shrike verify_record_proof",
        || for (car, (_, c, r)) in cars.iter().zip(&sample) {
            black_box(shrike::repo::verify_record_proof(black_box(car), &did, vk, c, r).unwrap());
        },
        || for (car, (k, _, _)) in cars.iter().zip(&sample) {
            // its verifier (used for lexicon-schema records) ends with a
            // record-type check: every proof step before that has run
            let r = vlpds::oauth::lexicon::verify_record_proof(black_box(car), did.as_str(), &mb, k);
            assert!(matches!(&r, Err(e) if e.starts_with("Invalid record type")) || r.is_ok(), "{r:?}");
            black_box(r);
        }
    );
}

// ---------------------------------------------------------------------------
// MST: bulk build + root, then single-op commits on the built tree
// ---------------------------------------------------------------------------

fn synthetic_keys(n: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(1);
    let colls = [("app.bsky.feed.like", 60), ("app.bsky.feed.post", 25), ("app.bsky.graph.follow", 10), ("app.bsky.feed.repost", 5)];
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(n);
    let mut ts: u64 = 1_680_000_000_000_000;
    while out.len() < n {
        let mut x = rng.gen_range(0..100);
        let mut coll = colls[0].0;
        for (c, w) in colls {
            if x < w {
                coll = c;
                break;
            }
            x -= w;
        }
        ts += rng.gen_range(1..50_000_000);
        let tid = shrike::syntax::Tid::new(ts, rng.gen_range(0..1024)).unwrap();
        let k = format!("{coll}/{tid}");
        if seen.insert(k.clone()) {
            out.push(k);
        }
    }
    out
}

fn rand_cid(rng: &mut StdRng) -> sc::Cid {
    sc::Cid::compute(sc::Codec::Drisl, &rng.r#gen::<[u8; 16]>())
}

fn row(o: &mut Opts, group: &str, case: &str, unit: &str, a: Vec<f64>, b: Vec<f64>, al: &str, bl: &str) -> Row {
    let r = Row { group: group.into(), case: case.into(), unit: unit.into(), a, b, a_label: al.into(), b_label: bl.into() };
    timing::report_pub(o, &r);
    r
}

fn mst_build_and_commits(o: &mut Opts, label: &str, keys: Vec<String>) {
    let n = keys.len();
    let tag = if n >= 1_000_000 { format!("{}M", n / 1_000_000) } else { format!("{}k", n / 1000) };
    let group_build = format!("mst.build/{tag}");
    if !o.wants(&group_build) && !o.wants(&format!("mst.commit/{tag}")) {
        return;
    }
    let mut rng = StdRng::seed_from_u64(3);
    let mut order = keys.clone();
    order.shuffle(&mut rng);
    let vals: Vec<sc::Cid> = (0..n).map(|_| rand_cid(&mut rng)).collect();
    let rounds = if n >= 500_000 { 5 } else { 7 };

    // --- bulk build + root (with every node block returned) ---
    let (mut s_ins, mut s_root, mut v_ins, mut v_root) = (vec![], vec![], vec![], vec![]);
    let mut s_tree_keep: Option<(DetachedTree, sc::Cid, HashMap<sc::Cid, Vec<u8>>)> = None;
    #[cfg(feature = "compare")]
    let mut v_tree_keep: Option<vlpds::mst::Tree> = None;
    let _ = &mut v_ins;
    let _ = &mut v_root;
    for r in 0..rounds + 1 {
        let shrike_first = r % 2 == 0;
        for side in 0..2 {
            if (side == 0) == shrike_first {
                let ks: Vec<String> = order.clone();
                let mut t = DetachedTree::new();
                let t0 = Instant::now();
                for (k, v) in ks.into_iter().zip(&vals) {
                    t.insert(&NoBlocks, k, *v).unwrap();
                }
                let t1 = Instant::now();
                let w = t.flush().unwrap();
                let t2 = Instant::now();
                if r > 0 {
                    s_ins.push((t1 - t0).as_nanos() as f64 / n as f64);
                    s_root.push((t2 - t1).as_nanos() as f64 / n as f64);
                }
                if r == rounds {
                    s_tree_keep = Some((t, w.root, w.new_blocks.into_iter().collect()));
                } else {
                    drop((t, w));
                }
            } else {
                #[cfg(feature = "compare")]
                {
                    let mut t = vlpds::mst::Tree::new();
                    let t0 = Instant::now();
                    for (k, v) in order.iter().zip(&vals) {
                        t.insert_no_proof(k.as_bytes(), v_cid(v)).unwrap();
                    }
                    let t1 = Instant::now();
                    let mut out = Vec::new();
                    let root = t.write_diff_blocks(&mut out).unwrap();
                    let t2 = Instant::now();
                    if r > 0 {
                        v_ins.push((t1 - t0).as_nanos() as f64 / n as f64);
                        v_root.push((t2 - t1).as_nanos() as f64 / n as f64);
                    }
                    if r == rounds {
                        assert_eq!(root.to_bytes(), s_tree_keep.as_ref().map(|x| x.1.to_bytes()).unwrap_or(root.to_bytes()));
                        v_tree_keep = Some(t);
                    }
                    drop(out);
                }
            }
        }
    }
    let lbl = format!("{label} keys, shuffled, x{n}");
    let total = |a: &[f64], b: &[f64]| a.iter().zip(b).map(|(x, y)| x + y).collect::<Vec<f64>>();
    row(o, &group_build, &format!("{lbl}: inserts"), "key", s_ins.clone(), v_ins.clone(), "shrike DetachedTree::insert", OTHER);
    row(o, &group_build, &format!("{lbl}: root + all blocks"), "key", s_root.clone(), v_root.clone(), "shrike flush", OTHER);
    row(o, &group_build, &format!("{lbl}: total"), "key", total(&s_ins, &s_root), total(&v_ins, &v_root), "shrike", OTHER);

    // --- single-key commits on the built tree: insert a new key and compute
    // the new root and changed blocks; then delete it again ---
    let group = format!("mst.commit/{tag}");
    if !o.wants(&group) {
        return;
    }
    let (mut st, s_root0, s_store) = s_tree_keep.unwrap();
    #[cfg(feature = "compare")]
    let mut vt = v_tree_keep.unwrap();
    let present: HashSet<&str> = keys.iter().map(|k| k.as_str()).collect();
    let fresh: Vec<String> = synthetic_keys(n + 40_000).into_iter().filter(|k| !present.contains(k.as_str())).take(20_000).collect();
    let mut pool = fresh.iter().cycle();
    let budget = Duration::from_millis(1500);
    let max_ops = 2000;
    let (mut s_i, mut s_d, mut s_ri, mut s_cp, mut v_i, mut v_d, mut v_pi) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let _ = (&mut v_i, &mut v_d, &mut v_pi);
    let mut reopen_root = s_root0;
    let mut reopen_store = s_store;
    for r in 0..rounds {
        // take a batch of fresh keys (each round its own)
        let batch: Vec<String> = (0..max_ops).map(|_| pool.next().unwrap().clone()).collect();
        let v = rand_cid(&mut rng);
        let shrike_first = r % 2 == 0;
        for side in 0..2 {
            if (side == 0) == shrike_first {
                // shrike, long-lived materialized tree
                let t0 = Instant::now();
                let mut done = 0;
                for k in &batch {
                    st.insert(&NoBlocks, k.clone(), v).unwrap();
                    black_box(st.flush().unwrap());
                    done += 1;
                    if t0.elapsed() > budget {
                        break;
                    }
                }
                s_i.push(t0.elapsed().as_nanos() as f64 / done as f64);
                let t0 = Instant::now();
                for k in &batch[..done] {
                    st.remove(&NoBlocks, k).unwrap();
                    black_box(st.flush().unwrap());
                }
                s_d.push(t0.elapsed().as_nanos() as f64 / done as f64);
                // with the covering proof a #commit needs (insert, flush, covering_proof)
                let t0 = Instant::now();
                let mut done = 0;
                for k in &batch {
                    st.insert(&NoBlocks, k.clone(), v).unwrap();
                    black_box(st.flush().unwrap());
                    black_box(st.covering_proof(&NoBlocks, [k.as_str()]).unwrap());
                    done += 1;
                    if t0.elapsed() > budget {
                        break;
                    }
                }
                s_cp.push(t0.elapsed().as_nanos() as f64 / done as f64);
                for k in &batch[..done] {
                    st.remove(&NoBlocks, k).unwrap();
                }
                st.flush().unwrap();
                // shrike, tree reopened from the block store per commit
                let t0 = Instant::now();
                let mut done = 0;
                for k in &batch {
                    let mut t = DetachedTree::load(reopen_root);
                    t.insert(&reopen_store, k.clone(), v).unwrap();
                    let w = t.flush().unwrap();
                    reopen_root = w.root;
                    reopen_store.extend(w.new_blocks);
                    done += 1;
                    if t0.elapsed() > budget {
                        break;
                    }
                }
                s_ri.push(t0.elapsed().as_nanos() as f64 / done as f64);
                // (reopened tree: the inserted keys stay; the store only grows)
            } else {
                #[cfg(feature = "compare")]
                {
                    let mut out = Vec::new();
                    let t0 = Instant::now();
                    let mut done = 0;
                    for k in &batch {
                        vt.insert_no_proof(k.as_bytes(), v_cid(&v)).unwrap();
                        out.clear();
                        black_box(vt.write_diff_blocks(&mut out).unwrap());
                        done += 1;
                        if t0.elapsed() > budget {
                            break;
                        }
                    }
                    v_i.push(t0.elapsed().as_nanos() as f64 / done as f64);
                    let t0 = Instant::now();
                    for k in &batch[..done] {
                        vt.remove(k.as_bytes()).unwrap();
                        out.clear();
                        black_box(vt.write_diff_blocks(&mut out).unwrap());
                    }
                    v_d.push(t0.elapsed().as_nanos() as f64 / done as f64);
                    // insert with covering-proof marking (blocks for a #commit)
                    let t0 = Instant::now();
                    let mut done = 0;
                    for k in &batch {
                        vt.insert(k.as_bytes(), v_cid(&v)).unwrap();
                        out.clear();
                        black_box(vt.write_diff_blocks(&mut out).unwrap());
                        done += 1;
                        if t0.elapsed() > budget {
                            break;
                        }
                    }
                    v_pi.push(t0.elapsed().as_nanos() as f64 / done as f64);
                    for k in &batch[..done] {
                        vt.remove(k.as_bytes()).unwrap();
                    }
                    vt.root_cid().unwrap();
                }
            }
        }
    }
    row(o, &group, &format!("{label} x{n}: insert 1 key + root/new blocks"), "commit", s_i.clone(), v_i.clone(), "shrike insert+flush (materialized tree)", OTHER);
    row(o, &group, &format!("{label} x{n}: delete 1 key + root/new blocks"), "commit", s_d, v_d, "shrike remove+flush (materialized tree)", OTHER);
    row(o, &group, &format!("{label} x{n}: insert + covering proof"), "commit", s_cp, v_pi, "shrike insert+flush+covering_proof", OTHER);
    row(o, &group, &format!("{label} x{n}: insert, tree reopened per commit"), "commit", s_ri, v_i, "shrike load+insert+flush", OTHER);
    let _ = median(&[]);
}
