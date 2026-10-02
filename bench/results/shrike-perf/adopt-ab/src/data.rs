//! Inputs: a real repository CAR (REPO_CAR, default ~/repo.car), shrike's
//! own test vectors (SHRIKE_TESTDATA: shrike's `testdata/` directory), and
//! the atproto interop data-model fixtures (INTEROP_TESTDATA: the
//! atproto-interop-tests `data-model/` directory).

use shrike::cbor as sc;
use shrike::mst::DetachedTree;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct RepoData {
    pub car: Vec<u8>,
    pub roots: Vec<sc::Cid>,
    pub blocks: HashMap<sc::Cid, Vec<u8>>,
    pub commit_cid: sc::Cid,
    pub data_root: sc::Cid,
    /// (mst key, record cid), key order
    pub entries: Vec<(String, sc::Cid)>,
}

pub fn env_path(var: &str, default: &str) -> PathBuf {
    match std::env::var(var) {
        Ok(v) => PathBuf::from(v),
        Err(_) => PathBuf::from(default.replace('~', &std::env::var("HOME").unwrap_or_default())),
    }
}

pub fn load_repo() -> RepoData {
    let path = env_path("REPO_CAR", "~/repo.car");
    let car = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e} (set REPO_CAR)", path.display()));
    let (roots, blocks) = shrike::car::read_all(&car[..]).expect("repo CAR");
    let blocks: HashMap<sc::Cid, Vec<u8>> = blocks.into_iter().map(|b| (b.cid, b.data)).collect();
    let commit_cid = roots[0];
    let commit = shrike::repo::Commit::from_cbor(&blocks[&commit_cid]).expect("commit");
    let mut t = DetachedTree::load(commit.data);
    let entries = t.entries(&blocks).expect("mst walk");
    RepoData { car, roots, blocks, commit_cid, data_root: commit.data, entries }
}

/// Record bytes of one collection (all of them, or the first `cap`).
pub fn records<'r>(r: &'r RepoData, collection: &str, cap: usize) -> Vec<(&'r str, &'r [u8])> {
    r.entries
        .iter()
        .filter(|(k, _)| k.split_once('/').map(|x| x.0) == Some(collection))
        .filter_map(|(k, c)| Some((k.split_once('/').unwrap().1, r.blocks.get(c)?.as_slice())))
        .take(cap)
        .collect()
}

pub fn read(dir_var: &str, default: &str, rel: &str) -> Option<String> {
    let p = env_path(dir_var, default).join(rel);
    std::fs::read_to_string(&p).ok()
}

pub fn b64(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD_NO_PAD.decode(s.trim_end_matches('=')).unwrap()
}

/// The atproto interop data-model fixtures: (json, cbor).
pub fn interop_fixtures() -> Vec<(serde_json::Value, Vec<u8>)> {
    let Some(s) = read(
        "INTEROP_TESTDATA",
        "/path/to/vlpds/testdata/interop/data-model",
        "data-model-fixtures.json",
    ) else {
        return Vec::new();
    };
    let v: Vec<serde_json::Value> = serde_json::from_str(&s).unwrap();
    v.into_iter()
        .map(|f| (f["json"].clone(), b64(f["cbor_base64"].as_str().unwrap())))
        .collect()
}

/// Real #commit firehose bodies from shrike's testdata, as full frames
/// (header + body).
pub fn firehose_frames() -> Vec<Vec<u8>> {
    let dir = env_path(
        "SHRIKE_FIREHOSE",
        "/path/to/vlpds/testdata/shrike/firehose_commits",
    );
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(&dir) else { return out };
    let mut paths: Vec<_> = rd.filter_map(|e| e.ok()).map(|e| e.path()).collect();
    paths.sort();
    for p in paths {
        let j: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        let mut frame = sc::json::json_to_drisl(&serde_json::json!({"op": 1, "t": "#commit"}), sc::json::Integers::Any).unwrap();
        frame.extend(sc::json::json_to_drisl(&j, sc::json::Integers::Any).unwrap());
        out.push(frame);
    }
    out
}
