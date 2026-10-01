//! checker-rs: an independent sync 1.1 firehose verifier for vlpds, built on
//! shrike (a second Rust atproto implementation) rather than on vlpds's own
//! code. It mirrors the Go checker (../checker, built on indigo): flags,
//! failure kinds and the summary.
//!
//! Every event goes through two verifiers:
//!
//! 1. Our own checks, each made with shrike primitives: seq order; canonical
//!    DAG-CBOR frames and blocks; for a #commit the CAR (version, block
//!    hashes, root == commit CID), the commit object (DID, rev, version 3,
//!    signature against the account's #atproto key from vlpds's
//!    describeRepo / resolveDid), op paths, op CIDs against the new tree,
//!    the sync 1.1 inversion of the ops back to prevData using only the
//!    commit's blocks, and the per-DID chain (since == previous rev, prevData
//!    == previous data, rev increasing); #sync signature and rev; #identity /
//!    #account fields; per-DID event times.
//! 2. shrike's stock `sync::Verifier` (strict inversion, Error policy, a
//!    getRepo source for #sync resyncs), whose verdicts are reported per error
//!    kind. Its one known false positive, inverting an update op (it loads the
//!    key's neighbour subtrees, which an update's inversion doesn't need and
//!    indigo-style producers don't send; SHRIKE_ISSUES.md), is counted as
//!    `shrike_update_overfetch` when our own inversion of that commit succeeds.
//!
//! Usage: checker-rs -host http://127.0.0.1:2620 [-cursor 0] [-max-events N]
//!        [-strict] [-quiet] [-dense] [-idle-exit SECS]
//! Exit codes: 0 ok, 1 failures under -strict, 2 bad flags / no connection.
use anyhow::{anyhow, bail, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value as J;
use shrike::cbor as sc;
use shrike::crypto::VerifyingKey;
use shrike::mst::DetachedTree;
use shrike::repo::Commit;
use shrike::sync::{
    ChainState, IdentityResolver, MemStateStore, RawCommit, RawSyncEvent, SyncRepoSource, Verifier,
    VerifierError, VerifierOptions, VerifierPolicy,
};
use shrike::syntax::{Datetime, Did, Handle};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const MAX_COMMIT_BLOCKS: usize = 2_000_000;
const MAX_SYNC_BLOCKS: usize = 10_000;
const MAX_FAILURE_DETAILS: usize = 20;
const ACCOUNT_STATUSES: &[&str] = &["takendown", "suspended", "deleted", "deactivated", "desynchronized", "throttled"];

// ---------------------------------------------------------------------------
// flags
// ---------------------------------------------------------------------------

struct Config {
    host: String,
    cursor: Option<i64>,
    max_events: u64,
    strict: bool,
    quiet: bool,
    dense: bool,
    idle_exit: Option<Duration>,
}

fn parse_args() -> Result<Config> {
    let mut c = Config {
        host: "http://localhost:2583".into(),
        cursor: None,
        max_events: 0,
        strict: false,
        quiet: false,
        dense: false,
        idle_exit: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let a = a.trim_start_matches('-').to_string();
        let (name, inline) = match a.split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (a, None),
        };
        let mut value = |n: &str| -> Result<String> {
            inline.clone().or_else(|| args.next()).ok_or_else(|| anyhow!("-{n} needs a value"))
        };
        match name.as_str() {
            "host" => c.host = value("host")?,
            "cursor" => c.cursor = Some(value("cursor")?.parse().context("-cursor")?),
            "max-events" => c.max_events = value("max-events")?.parse().context("-max-events")?,
            "idle-exit" => c.idle_exit = Some(Duration::from_secs_f64(value("idle-exit")?.parse().context("-idle-exit")?)),
            "strict" => c.strict = true,
            "quiet" => c.quiet = true,
            "dense" => c.dense = true,
            "h" | "help" => {
                println!("usage: checker-rs -host URL [-cursor N] [-max-events N] [-strict] [-quiet] [-dense] [-idle-exit SECS]");
                std::process::exit(0);
            }
            other => bail!("unknown flag -{other}"),
        }
    }
    Ok(c)
}

fn http_host(host: &str) -> String {
    let h = host.trim_end_matches('/');
    if let Some(r) = h.strip_prefix("ws://") {
        format!("http://{r}")
    } else if let Some(r) = h.strip_prefix("wss://") {
        format!("https://{r}")
    } else {
        h.to_string()
    }
}

fn subscribe_url(host: &str, cursor: Option<i64>) -> Result<String> {
    let h = http_host(host);
    let ws = if let Some(r) = h.strip_prefix("http://") {
        format!("ws://{r}")
    } else if let Some(r) = h.strip_prefix("https://") {
        format!("wss://{r}")
    } else {
        bail!("unsupported scheme in -host {host}")
    };
    let mut u = format!("{ws}/xrpc/com.atproto.sync.subscribeRepos");
    if let Some(c) = cursor {
        u.push_str(&format!("?cursor={c}"));
    }
    Ok(u)
}

// ---------------------------------------------------------------------------
// keys and repos, from the PDS under test (its did:plc ids don't resolve)
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Doc {
    Found(J),
    /// the PDS no longer serves the account (deleted)
    Gone,
}

struct Pds {
    host: String,
    http: reqwest::Client,
    docs: Mutex<HashMap<String, Doc>>,
}

impl Pds {
    fn new(host: &str) -> Self {
        Pds {
            host: http_host(host),
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("http client"),
            docs: Mutex::new(HashMap::new()),
        }
    }

    fn purge(&self, did: &str) {
        self.docs.lock().unwrap().remove(did);
    }

    /// The DID document, from describeRepo, else resolveDid (which still
    /// serves deactivated / taken-down accounts). Retries 503s for 30 s.
    async fn doc(&self, did: &str) -> Result<Doc> {
        if let Some(d) = self.docs.lock().unwrap().get(did) {
            return Ok(d.clone());
        }
        let mut errs = Vec::new();
        for path in [
            format!("/xrpc/com.atproto.repo.describeRepo?repo={did}"),
            format!("/xrpc/com.atproto.identity.resolveDid?did={did}"),
        ] {
            match self.fetch_doc(&path).await {
                Ok(d) => {
                    self.docs.lock().unwrap().insert(did.to_string(), d.clone());
                    return Ok(d);
                }
                Err(e) => errs.push(e.to_string()),
            }
        }
        if errs.iter().all(|e| e.contains("Gone")) {
            return Ok(Doc::Gone);
        }
        bail!("{}", errs.join("; "))
    }

    async fn fetch_doc(&self, path: &str) -> Result<Doc> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let r = self.http.get(format!("{}{path}", self.host)).send().await;
            let retry = match r {
                Ok(resp) => {
                    let status = resp.status();
                    let body: J = resp.json().await.unwrap_or(J::Null);
                    if status.is_success() {
                        return Ok(Doc::Found(body["didDoc"].clone()));
                    }
                    if status == 400 && matches!(body["error"].as_str(), Some("DidNotFound" | "RepoNotFound")) {
                        bail!("Gone");
                    }
                    if status != 503 {
                        bail!("{path}: HTTP {status}: {body}");
                    }
                    format!("HTTP 503: {body}")
                }
                Err(e) => e.to_string(),
            };
            if Instant::now() > deadline {
                bail!("{path}: {retry}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// The account's #atproto key (None when the account is gone).
    async fn key(&self, did: &str) -> Result<Option<Box<dyn VerifyingKey>>> {
        let Doc::Found(doc) = self.doc(did).await? else { return Ok(None) };
        let vms = doc["verificationMethod"].as_array().cloned().unwrap_or_default();
        let vm = vms
            .iter()
            .find(|v| v["id"].as_str().is_some_and(|i| i.ends_with("#atproto")))
            .or(vms.first())
            .ok_or_else(|| anyhow!("didDoc has no verificationMethod"))?;
        let mb = vm["publicKeyMultibase"].as_str().ok_or_else(|| anyhow!("no publicKeyMultibase"))?;
        Ok(Some(shrike::crypto::parse_did_key(&format!("did:key:{mb}"))?))
    }
}

/// shrike's verifier resolves identities through this.
struct PdsResolver(Arc<Pds>);

#[async_trait::async_trait]
impl IdentityResolver for PdsResolver {
    async fn lookup_did(&self, did: &Did) -> Result<Arc<shrike::identity::Identity>, shrike::identity::IdentityError> {
        use shrike::identity::IdentityError as E;
        match self.0.doc(did.as_str()).await {
            Ok(Doc::Found(doc)) => {
                let doc: shrike::identity::DidDocument = serde_json::from_value(doc).map_err(|e| E::InvalidDocument(e.to_string()))?;
                Ok(Arc::new(shrike::identity::Identity::from_document(doc)?))
            }
            Ok(Doc::Gone) => Err(E::NotFound(did.to_string())),
            Err(e) => Err(E::Network(e.to_string())),
        }
    }

    async fn purge(&self, did: &Did) -> Result<(), shrike::identity::IdentityError> {
        self.0.purge(did.as_str());
        Ok(())
    }
}

/// getRepo, for the shrike verifier's #sync resyncs.
struct PdsRepos(Arc<Pds>);

#[async_trait::async_trait]
impl SyncRepoSource for PdsRepos {
    async fn get_repo_car(&self, did: &Did) -> Result<Vec<u8>, shrike::sync::SyncError> {
        let url = format!("{}/xrpc/com.atproto.sync.getRepo?did={}", self.0.host, did.as_str());
        let fail = |e: String| shrike::sync::SyncError::Sync(format!("getRepo: {e}"));
        let resp = self.0.http.get(url).send().await.map_err(|e| fail(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(fail(format!("HTTP {}", resp.status())));
        }
        Ok(resp.bytes().await.map_err(|e| fail(e.to_string()))?.to_vec())
    }
}

// ---------------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Stats {
    events: u64,
    commits: u64,
    commits_ok: u64,
    syncs: u64,
    syncs_ok: u64,
    identity: u64,
    account: u64,
    info: u64,
    unknown: u64,
    sigs_skipped: u64,
    fails: BTreeMap<String, u64>,
    total_fails: u64,
    printed: usize,
    /// shrike stock verifier: verdicts by kind (ok, or its error kind)
    shrike: BTreeMap<String, u64>,
}

impl Stats {
    fn fail(&mut self, kind: &str, seq: i64, did: &str, reason: &str) {
        *self.fails.entry(kind.to_string()).or_default() += 1;
        self.total_fails += 1;
        if self.printed < MAX_FAILURE_DETAILS {
            self.printed += 1;
            println!("FAIL #{} kind={kind} seq={seq} did={did}: {reason}", self.printed);
            if self.printed == MAX_FAILURE_DETAILS {
                println!("(further failure details suppressed; counts continue)");
            }
        }
    }

    fn breakdown(m: &BTreeMap<String, u64>) -> String {
        if m.is_empty() {
            return "none".into();
        }
        m.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(",")
    }
}

// ---------------------------------------------------------------------------
// verification
// ---------------------------------------------------------------------------

type Fails = Vec<(&'static str, String)>;

struct Checker {
    pds: Arc<Pds>,
    shrike: Verifier,
    state: HashMap<String, (String, sc::Cid)>,
    last_time: HashMap<String, Datetime>,
    stats: Stats,
}

/// Decodes one DAG-CBOR value and requires the bytes to be its canonical
/// encoding, inside the atproto data model (no floats).
fn canonical(b: &[u8]) -> Result<(), String> {
    let v = sc::decode(b).map_err(|e| e.to_string())?;
    if has_float(&v) {
        return Err("float (outside the atproto data model)".into());
    }
    let re = sc::encode_value(&v).map_err(|e| e.to_string())?;
    if re != b {
        return Err("not the canonical encoding".into());
    }
    Ok(())
}

fn has_float(v: &sc::Value) -> bool {
    match v {
        sc::Value::Float(_) => true,
        sc::Value::Array(a) => a.iter().any(has_float),
        sc::Value::Map(m) => m.iter().any(|(_, v)| has_float(v)),
        _ => false,
    }
}

/// A commit's CAR: roots and blocks, every block hashing to its CID.
fn read_car(blocks: &[u8], fails: &mut Fails) -> Option<(Vec<sc::Cid>, HashMap<sc::Cid, Vec<u8>>)> {
    let (roots, list) = match shrike::car::read_all(blocks) {
        Ok(x) => x,
        Err(e) => {
            fails.push(("car", format!("reading CAR: {e}")));
            return None;
        }
    };
    let mut map = HashMap::with_capacity(list.len());
    for b in list {
        if sc::Cid::compute(b.cid.codec(), &b.data) != b.cid {
            fails.push(("car", format!("block {} does not hash to its CID", b.cid)));
            return None;
        }
        if b.cid.codec() == sc::Codec::Drisl {
            if let Err(e) = canonical(&b.data) {
                fails.push(("non_canonical", format!("block {}: {e}", b.cid)));
            }
        }
        map.insert(b.cid, b.data);
    }
    if roots.is_empty() {
        fails.push(("car", "CAR has no root".into()));
        return None;
    }
    Some((roots, map))
}

/// Sets `key`'s value in the subtree at `node` (the key exists), re-encoding
/// only the nodes on its path: how an update op is undone.
fn set_existing(store: &mut HashMap<sc::Cid, Vec<u8>>, node: sc::Cid, key: &str, val: sc::Cid) -> Result<sc::Cid, String> {
    use shrike::mst::node::{decode_node_data, encode_node_data};
    let block = store.get(&node).ok_or_else(|| format!("block not found: {node}"))?;
    let mut nd = decode_node_data(block).map_err(|e| e.to_string())?;
    let mut full: Vec<u8> = Vec::new();
    let mut at = Err(nd.entries.len());
    for (i, e) in nd.entries.iter().enumerate() {
        full.truncate(e.prefix_len);
        full.extend_from_slice(&e.key_suffix);
        match key.as_bytes().cmp(&full[..]) {
            std::cmp::Ordering::Equal => at = Ok(i),
            std::cmp::Ordering::Less => at = Err(i),
            std::cmp::Ordering::Greater => continue,
        }
        break;
    }
    match at {
        Ok(i) => nd.entries[i].value = val,
        Err(i) => {
            let slot = if i == 0 { &mut nd.left } else { &mut nd.entries[i - 1].right };
            let c = slot.ok_or_else(|| format!("{key} is not in the tree"))?;
            *slot = Some(set_existing(store, c, key, val)?);
        }
    }
    let bytes = encode_node_data(&nd).map_err(|e| e.to_string())?;
    let c = sc::Cid::compute(sc::Codec::Drisl, &bytes);
    store.insert(c, bytes);
    Ok(c)
}

/// Undoes a commit's ops (newest first) from `data` with only its blocks.
fn invert(raw: &RawCommit, data: sc::Cid, blocks: &HashMap<sc::Cid, Vec<u8>>) -> Result<sc::Cid, String> {
    let mut store = blocks.clone();
    let mut root = data;
    for op in raw.ops.iter().rev() {
        match (op.action.as_str(), op.prev) {
            ("update", Some(prev)) => {
                root = set_existing(&mut store, root, &op.path, prev).map_err(|e| format!("undo update {}: {e}", op.path))?;
                continue;
            }
            ("create", _) | ("delete", Some(_)) => {}
            (a, _) => return Err(format!("{a} op on {} can't be inverted", op.path)),
        }
        let mut t = DetachedTree::load(root);
        let r = match op.prev {
            Some(prev) => t.insert(&store, op.path.clone(), prev).map(drop),
            None => t.remove(&store, &op.path).map(drop),
        };
        r.map_err(|e| format!("undo {} {}: {e}", op.action, op.path))?;
        let w = t.flush().map_err(|e| e.to_string())?;
        store.extend(w.new_blocks);
        root = w.root;
    }
    Ok(root)
}

impl Checker {
    fn check_time(&mut self, did: &str, ts: &str, what: &str, fails: &mut Fails) {
        let dt = match Datetime::try_from(ts) {
            Ok(d) => d,
            Err(e) => {
                fails.push(("bad_field", format!("{what} time {ts:?}: {e}")));
                return;
            }
        };
        if let Some(prev) = self.last_time.get(did) {
            if dt < *prev {
                fails.push(("time_order", format!("{what} time {ts} is before the previous event's {}", prev.as_str())));
                return;
            }
        }
        self.last_time.insert(did.to_string(), dt);
    }

    /// Signature of `commit` against the account's key (refetched once on a
    /// miss, for a rotation). Ok(false) when the account is gone.
    async fn check_sig(&mut self, did: &str, commit: &Commit, fails: &mut Fails) {
        for attempt in 0..2 {
            let key = match self.pds.key(did).await {
                Ok(Some(k)) => k,
                Ok(None) => {
                    self.stats.sigs_skipped += 1;
                    return;
                }
                Err(e) => {
                    fails.push(("key_fetch", e.to_string()));
                    return;
                }
            };
            match commit.verify(key.as_ref()) {
                Ok(()) => return,
                Err(e) if attempt == 1 => fails.push(("signature", format!("key {}: {e}", key.multibase()))),
                Err(_) => self.pds.purge(did),
            }
        }
    }

    async fn verify_commit(&mut self, raw: &RawCommit) -> (Fails, Option<sc::Cid>) {
        let mut fails: Fails = Vec::new();
        let did = raw.repo.as_str().to_string();
        if raw.too_big {
            fails.push(("too_big", "tooBig flag set".into()));
        }
        if raw.rebase {
            fails.push(("rebase", "rebase flag set".into()));
        }
        if raw.blocks.len() > MAX_COMMIT_BLOCKS {
            fails.push(("blocks_size", format!("blocks is {} bytes (max {MAX_COMMIT_BLOCKS})", raw.blocks.len())));
        }
        let mut seen = HashSet::new();
        for op in &raw.ops {
            if !seen.insert(op.path.as_str()) {
                fails.push(("dup_op_path", format!("two ops on {}", op.path)));
            }
            if matches!(op.action.as_str(), "update" | "delete") && op.prev.is_none() {
                fails.push(("op_missing_prev", format!("{} op on {} has no prev", op.action, op.path)));
            }
        }
        if raw.prev_data.is_none() && raw.since.is_some() {
            fails.push(("missing_prevdata", format!("since={} but prevData is null", raw.since.unwrap())));
        }
        self.check_time(&did, &raw.time, "#commit", &mut fails);

        let Some((roots, blocks)) = read_car(&raw.blocks, &mut fails) else {
            self.state.remove(&did);
            return (fails, None);
        };
        if roots[0] != raw.commit {
            fails.push(("commit_cid", format!("msg.commit={} but CAR root={}", raw.commit, roots[0])));
        }
        let commit = match blocks.get(&raw.commit).map(|b| Commit::from_cbor(b)) {
            Some(Ok(c)) => c,
            Some(Err(e)) => {
                fails.push(("car", format!("decoding commit object: {e}")));
                self.state.remove(&did);
                return (fails, None);
            }
            None => {
                fails.push(("car", format!("commit block {} not in CAR", raw.commit)));
                self.state.remove(&did);
                return (fails, None);
            }
        };
        if commit.did.as_str() != did {
            fails.push(("did_mismatch", format!("event repo={did} commit.did={}", commit.did)));
        }
        if commit.rev != raw.rev {
            fails.push(("rev_mismatch", format!("event rev={} commit.rev={}", raw.rev, commit.rev)));
        }
        if commit.version != 3 || commit.sig.is_none() {
            fails.push(("car", format!("commit version {} signed={}", commit.version, commit.sig.is_some())));
        }

        // op CIDs against the new tree, read from the commit's blocks alone
        let mut tree = DetachedTree::load(commit.data);
        for op in &raw.ops {
            match tree.get(&blocks, &op.path) {
                Ok(got) if got == op.cid => {}
                Ok(got) => fails.push(("op_cid", format!("{} {}: op cid {:?}, tree has {:?}", op.action, op.path, op.cid, got))),
                Err(e) => fails.push(("car", format!("tree lookup of {}: {e}", op.path))),
            }
            if let Some(c) = op.cid {
                if !blocks.contains_key(&c) {
                    fails.push(("car", format!("record block {c} for {} not in CAR", op.path)));
                }
            }
        }

        // sync 1.1: the ops, undone with only these blocks, give prevData
        if let Some(prev_data) = raw.prev_data {
            match invert(raw, commit.data, &blocks) {
                Ok(root) if root == prev_data => {}
                Ok(root) => fails.push(("prevdata_mismatch", format!("inverted root {root} != prevData {prev_data}"))),
                Err(e) => fails.push(("prevdata_mismatch", format!("inversion: {e}"))),
            }
        }

        self.check_sig(&did, &commit, &mut fails).await;

        if let Some((prev_rev, prev_data)) = self.state.get(&did) {
            match &raw.since {
                None => fails.push(("chain_since", format!("since is null but previous rev is {prev_rev}"))),
                Some(s) if s.to_string() != *prev_rev => fails.push(("chain_since", format!("since={s}, previous rev={prev_rev}"))),
                _ => {}
            }
            if let Some(pd) = raw.prev_data {
                if pd != *prev_data {
                    fails.push(("chain_prevdata", format!("prevData={pd}, previous data={prev_data}")));
                }
            }
            if raw.rev.to_string() <= *prev_rev {
                fails.push(("chain_rev", format!("rev={} not greater than previous rev={prev_rev}", raw.rev)));
            }
        }
        self.state.insert(did, (commit.rev.to_string(), commit.data));
        (fails, Some(commit.data))
    }

    async fn verify_sync(&mut self, raw: &shrike::sync::RawSync) -> (Fails, Option<(String, sc::Cid)>) {
        let mut fails: Fails = Vec::new();
        let did = raw.did.as_str().to_string();
        self.check_time(&did, &raw.time, "#sync", &mut fails);
        if raw.blocks.len() > MAX_SYNC_BLOCKS {
            fails.push(("blocks_size", format!("#sync blocks is {} bytes (max {MAX_SYNC_BLOCKS})", raw.blocks.len())));
        }
        let Some((roots, blocks)) = read_car(&raw.blocks, &mut fails) else {
            self.state.remove(&did);
            return (fails, None);
        };
        let commit = match blocks.get(&roots[0]).map(|b| Commit::from_cbor(b)) {
            Some(Ok(c)) => c,
            other => {
                fails.push(("car", format!("#sync commit block: {:?}", other.map(|r| r.err().map(|e| e.to_string())))));
                self.state.remove(&did);
                return (fails, None);
            }
        };
        if commit.did.as_str() != did {
            fails.push(("did_mismatch", format!("#sync did={did} commit.did={}", commit.did)));
        }
        if commit.rev.to_string() != raw.rev {
            fails.push(("rev_mismatch", format!("#sync rev={} commit.rev={}", raw.rev, commit.rev)));
        }
        self.check_sig(&did, &commit, &mut fails).await;
        if let Some((prev_rev, _)) = self.state.get(&did) {
            if commit.rev.to_string() < *prev_rev {
                fails.push(("chain_rev", format!("#sync rev={} is older than previous rev={prev_rev}", commit.rev)));
            }
        }
        let st = (commit.rev.to_string(), commit.data);
        self.state.insert(did, st.clone());
        (fails, Some(st))
    }

    fn shrike_verdict(&mut self, kind: String) {
        *self.stats.shrike.entry(kind).or_default() += 1;
    }

    /// Runs shrike's stock verifier on the commit and tallies its verdict.
    /// After an error its chain state is set to this commit (which our own
    /// checks verified), so one rejection doesn't cascade into chain breaks.
    async fn shrike_commit(&mut self, raw: &RawCommit, own: &Fails, data: Option<sc::Cid>) -> Option<(&'static str, String)> {
        let err = match self.shrike.verify_commit(raw).await {
            Ok(_) => {
                self.shrike_verdict("ok".into());
                return None;
            }
            Err(e) => e,
        };
        if let Some(d) = data {
            let _ = self.shrike.state_store().save_chain(&raw.repo, ChainState { rev: raw.rev.to_string(), data: d }).await;
        }
        let has_update = raw.ops.iter().any(|o| o.action == "update");
        let own_inverted = !own.iter().any(|(k, _)| *k == "prevdata_mismatch");
        let msg = err.to_string();
        if has_update && own_inverted && msg.contains("block not found") {
            self.shrike_verdict("shrike_update_overfetch".into());
            return None;
        }
        let kind = verifier_error_kind(&err);
        self.shrike_verdict(kind.clone());
        // an account the PDS no longer serves: its key can't be checked (as above)
        if matches!(err, VerifierError::Identity { .. }) && msg.contains("not found") {
            return None;
        }
        Some(("shrike_verifier", format!("{kind}: {msg}")))
    }
}

fn verifier_error_kind(e: &VerifierError) -> String {
    let dbg = format!("{e:?}");
    dbg.split(|c: char| !c.is_alphanumeric()).next().unwrap_or("Unknown").to_string()
}

// ---------------------------------------------------------------------------
// main loop
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(code) => std::process::ExitCode::from(code),
        Err(e) => {
            eprintln!("{e:#}");
            std::process::ExitCode::from(2)
        }
    }
}

async fn run() -> Result<u8> {
    let cfg = parse_args()?;
    let url = subscribe_url(&cfg.host, cfg.cursor)?;
    let pds = Arc::new(Pds::new(&cfg.host));
    let shrike = Verifier::new(
        VerifierOptions::new(Arc::new(MemStateStore::new()), Arc::new(PdsResolver(pds.clone())))
            .with_verifier_policy(VerifierPolicy::Error)
            .with_lenient_inversion(false)
            .with_repo_source(Arc::new(PdsRepos(pds.clone())))
            .with_resync_rate_limit(shrike::sync::ResyncRateLimit::unlimited()),
    );
    let mut ck = Checker { pds, shrike, state: HashMap::new(), last_time: HashMap::new(), stats: Stats::default() };

    println!("connecting to {url}");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.with_context(|| format!("dial {url}"))?;
    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut last_report_events = 0;
    let (mut first_seq, mut last_seq): (i64, i64) = (-1, -1);
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let reason = loop {
        let idle = cfg.idle_exit.unwrap_or(Duration::from_secs(3600 * 24 * 365));
        let msg = tokio::select! {
            _ = &mut ctrl_c => break "interrupted".to_string(),
            _ = term.recv() => break "terminated".to_string(),
            m = tokio::time::timeout(idle, ws.next()) => match m {
                Err(_) => break format!("idle for {idle:?}"),
                Ok(None) => break "connection closed".to_string(),
                Ok(Some(Err(e))) => break format!("connection error: {e}"),
                Ok(Some(Ok(m))) => m,
            },
        };
        let frame = match msg {
            Message::Binary(b) => b,
            Message::Ping(p) => {
                let _ = ws.send(Message::Pong(p)).await;
                continue;
            }
            Message::Close(_) => break "connection closed by server".to_string(),
            _ => continue,
        };
        ck.stats.events += 1;
        handle_frame(&mut ck, &frame, &cfg, &mut first_seq, &mut last_seq).await;
        if !cfg.quiet && last_report.elapsed() >= Duration::from_secs(5) {
            let rate = (ck.stats.events - last_report_events) as f64 / last_report.elapsed().as_secs_f64();
            println!(
                "{:>8.1} ev/s total={} commits_ok={}/{} syncs_ok={}/{} failures=[{}] shrike=[{}]",
                rate,
                ck.stats.events,
                ck.stats.commits_ok,
                ck.stats.commits,
                ck.stats.syncs_ok,
                ck.stats.syncs,
                Stats::breakdown(&ck.stats.fails),
                Stats::breakdown(&ck.stats.shrike)
            );
            last_report = Instant::now();
            last_report_events = ck.stats.events;
        }
        if cfg.max_events > 0 && ck.stats.events >= cfg.max_events {
            break format!("reached -max-events {}", cfg.max_events);
        }
    };
    let _ = ws.close(None).await;

    let s = &ck.stats;
    let el = start.elapsed();
    println!("\n=== vlpds firehose checker-rs summary ({reason}) ===");
    println!("duration:        {:.3}s ({:.1} ev/s)", el.as_secs_f64(), s.events as f64 / el.as_secs_f64());
    if first_seq >= 0 {
        println!("seq range:       {first_seq} .. {last_seq}");
    }
    println!("events:          {}", s.events);
    println!("  #commit:       {} ({} verified clean)", s.commits, s.commits_ok);
    println!("  #sync:         {} ({} verified clean)", s.syncs, s.syncs_ok);
    println!("  #identity:     {}", s.identity);
    println!("  #account:      {}", s.account);
    println!("  #info:         {}", s.info);
    println!("  unknown:       {}", s.unknown);
    if s.sigs_skipped > 0 {
        println!("sigs skipped:    {} (account deleted: key no longer served)", s.sigs_skipped);
    }
    println!("shrike verifier: [{}]", Stats::breakdown(&s.shrike));
    println!("failures:        {} [{}]", s.total_fails, Stats::breakdown(&s.fails));
    println!("RESULT: {}", if s.total_fails == 0 { "PASS" } else { "FAIL" });
    Ok(if cfg.strict && s.total_fails > 0 { 1 } else { 0 })
}

async fn handle_frame(ck: &mut Checker, frame: &[u8], cfg: &Config, first_seq: &mut i64, last_seq: &mut i64) {
    // header and body: canonical DAG-CBOR, then shrike's frame parser
    let mut dec = sc::Decoder::new(frame);
    let header = dec.decode();
    let split = dec.position();
    let body = dec.decode();
    let (header, body) = match (header, body) {
        (Ok(h), Ok(b)) if dec.is_empty() => (h, b),
        (h, b) => {
            let why = format!("{:?} / {:?} / trailing={}", h.err(), b.err(), !dec.is_empty());
            ck.stats.fail("decode", -1, "", &why);
            return;
        }
    };
    fn get<'a>(v: &sc::Value<'a>, k: &str) -> Option<sc::Value<'a>> {
        match v {
            sc::Value::Map(m) => m.iter().find(|(kk, _)| *kk == k).map(|(_, v)| v.clone()),
            _ => None,
        }
    }
    if let Some(sc::Value::Signed(-1)) = get(&header, "op") {
        let msg = format!("{:?} {:?}", get(&body, "error"), get(&body, "message"));
        ck.stats.fail("upstream_error", -1, "", &msg);
        return;
    }
    let seq = match get(&body, "seq") {
        Some(sc::Value::Unsigned(n)) => n as i64,
        _ => -1,
    };
    let did = match get(&body, "repo").or_else(|| get(&body, "did")) {
        Some(sc::Value::Text(t)) => t.to_string(),
        _ => String::new(),
    };
    for (part, bytes) in [("header", &frame[..split]), ("body", &frame[split..])] {
        if let Err(e) = canonical(bytes) {
            ck.stats.fail("non_canonical", seq, &did, &format!("frame {part}: {e}"));
        }
    }
    if seq >= 0 {
        if *first_seq < 0 {
            *first_seq = seq;
        } else if seq <= *last_seq {
            ck.stats.fail("seq_reorder", seq, &did, &format!("seq {seq} after {last_seq}"));
        } else if cfg.dense && seq != *last_seq + 1 {
            ck.stats.fail("seq_gap", seq, &did, &format!("seq {seq} after {last_seq}"));
        }
        *last_seq = (*last_seq).max(seq);
    }

    let ev = match shrike::sync::raw::parse_raw_sync_frame(frame) {
        Ok(ev) => ev,
        Err(shrike::sync::RawSyncError::UnknownType(t)) => {
            ck.stats.unknown += 1;
            let _ = t;
            return;
        }
        Err(e) => {
            ck.stats.fail("decode", seq, &did, &e.to_string());
            return;
        }
    };
    match ev {
        RawSyncEvent::Commit(raw) => {
            ck.stats.commits += 1;
            let (mut fails, data) = ck.verify_commit(&raw).await;
            if let Some(f) = ck.shrike_commit(&raw, &fails, data).await {
                fails.push(f);
            }
            if fails.is_empty() {
                ck.stats.commits_ok += 1;
            }
            for (k, r) in fails {
                ck.stats.fail(k, raw.seq, raw.repo.as_str(), &r);
            }
        }
        RawSyncEvent::Sync(raw) => {
            ck.stats.syncs += 1;
            let (mut fails, st) = ck.verify_sync(&raw).await;
            match ck.shrike.verify_sync(&raw).await {
                Ok(_) => ck.shrike_verdict("ok".into()),
                Err(e) => {
                    let kind = verifier_error_kind(&e);
                    ck.shrike_verdict(format!("sync:{kind}"));
                    fails.push(("shrike_verifier", format!("#sync {kind}: {e}")));
                    if let Some((rev, data)) = st {
                        let _ = ck.shrike.state_store().save_chain(&raw.did, ChainState { rev, data }).await;
                    }
                }
            }
            if fails.is_empty() {
                ck.stats.syncs_ok += 1;
            }
            for (k, r) in fails {
                ck.stats.fail(k, raw.seq, raw.did.as_str(), &r);
            }
        }
        RawSyncEvent::Identity(raw) => {
            ck.stats.identity += 1;
            let mut fails = Vec::new();
            if let Some(h) = &raw.handle {
                if Handle::try_from(h.as_str()).is_err() {
                    fails.push(("bad_field", format!("#identity handle {h}")));
                }
            }
            ck.check_time(raw.did.as_str(), &raw.time, "#identity", &mut fails);
            ck.pds.purge(raw.did.as_str());
            for (k, r) in fails {
                ck.stats.fail(k, raw.seq, raw.did.as_str(), &r);
            }
        }
        RawSyncEvent::Account(raw) => {
            ck.stats.account += 1;
            let mut fails = Vec::new();
            if let Some(st) = &raw.status {
                if raw.active {
                    fails.push(("bad_field", format!("#account status {st:?} on an active account")));
                }
                if !ACCOUNT_STATUSES.contains(&st.as_str()) {
                    fails.push(("bad_field", format!("#account unknown status {st:?}")));
                }
            }
            ck.check_time(raw.did.as_str(), &raw.time, "#account", &mut fails);
            if let Err(e) = ck.shrike.on_account_event(&raw).await {
                fails.push(("shrike_verifier", format!("#account: {e}")));
            }
            for (k, r) in fails {
                ck.stats.fail(k, raw.seq, raw.did.as_str(), &r);
            }
        }
        RawSyncEvent::Info => ck.stats.info += 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shrike::crypto::K256SigningKey;
    use shrike::repo::{Repo, WriteOp};
    use shrike::sync::RawRepoOp;
    use shrike::syntax::{Nsid, RecordKey, TidClock};

    /// A commit from shrike's own repo, as a firehose #commit.
    fn commits() -> Vec<RawCommit> {
        let key = K256SigningKey::generate();
        let did = Did::try_from("did:plc:checkercheckercheckerch").unwrap();
        let mut repo = Repo::new(did.clone(), TidClock::new(3).unwrap());
        let nsid = Nsid::try_from("app.bsky.feed.like").unwrap();
        let rec = |i: u32| sc::encode_value(&sc::Value::Map(vec![("i", sc::Value::Unsigned(i as u64))])).unwrap();
        let mut out = Vec::new();
        for round in 0..6u32 {
            let writes: Vec<WriteOp> = (0..40u32)
                .map(|i| {
                    let rkey = RecordKey::try_from(format!("k{:03}", (i * 7 + round) % 90).as_str()).unwrap();
                    let exists = repo.get(&nsid, &rkey).unwrap().is_some();
                    match (exists, i % 3) {
                        (false, _) => WriteOp::Create { collection: nsid.clone(), rkey, record: rec(i + round) },
                        (true, 0) => WriteOp::Delete { collection: nsid.clone(), rkey },
                        (true, _) => WriteOp::Update { collection: nsid.clone(), rkey, record: rec(i + round + 100) },
                    }
                })
                .collect(); // distinct rkeys: 7 and 90 are coprime
            let c = repo.apply_writes(&writes, &key).unwrap();
            out.push(RawCommit {
                repo: did.clone(),
                rev: c.commit.rev,
                seq: round as i64,
                time: "2026-01-01T00:00:00.000Z".into(),
                since: c.since,
                commit: c.cid,
                blocks: c.relevant_car().unwrap(),
                ops: c.ops.iter().map(|o| RawRepoOp { action: o.action.as_str().into(), path: o.path(), cid: o.cid, prev: o.prev }).collect(),
                blobs: vec![],
                prev_data: c.prev_data,
                too_big: false,
                rebase: false,
            });
        }
        out
    }

    #[test]
    fn inverts_good_commits_and_catches_bad_ones() {
        for raw in commits() {
            let mut fails = Vec::new();
            let (roots, blocks) = read_car(&raw.blocks, &mut fails).unwrap();
            assert!(fails.is_empty(), "{fails:?}");
            let commit = Commit::from_cbor(&blocks[&roots[0]]).unwrap();
            let Some(prev) = raw.prev_data else { continue };
            assert_eq!(invert(&raw, commit.data, &blocks).unwrap(), prev);
            // a wrong prev value on any op breaks the inversion
            for i in 0..raw.ops.len() {
                let mut bad = raw.clone();
                bad.ops[i].prev = match bad.ops[i].prev {
                    Some(_) => Some(sc::Cid::compute(sc::Codec::Drisl, b"other")),
                    None if bad.ops[i].action == "create" => continue,
                    None => None,
                };
                assert_ne!(invert(&bad, commit.data, &blocks).ok(), Some(prev), "op {i}");
            }
            // a dropped op too
            let mut short = raw.clone();
            short.ops.pop();
            assert_ne!(invert(&short, commit.data, &blocks).ok(), Some(prev));
        }
    }

    /// SHRIKE_ISSUES.md #1, the reason `invert` rewrites updates itself:
    /// shrike's insert of an existing key wants the neighbour subtrees.
    #[test]
    fn shrike_update_overfetch_still_present() {
        use shrike::mst::{height_for_key, NoBlocks};
        let leaf = sc::Cid::compute(sc::Codec::Drisl, b"leaf");
        let new = sc::Cid::compute(sc::Codec::Drisl, b"new");
        let at = |h: u8, n: usize| -> Vec<String> {
            (0..).map(|i| format!("com.example.k/{i:06}")).filter(|k| height_for_key(k) == h).take(n).collect()
        };
        let mut t = DetachedTree::new();
        for k in at(0, 6).into_iter().chain(at(1, 3)) {
            t.insert(&NoBlocks, k, leaf).unwrap();
        }
        let before = t.flush().unwrap().root;
        let key = at(1, 2)[1].clone();
        t.insert(&NoBlocks, key.clone(), new).unwrap();
        let w = t.flush().unwrap();
        let path: HashMap<sc::Cid, Vec<u8>> = w.new_blocks.into_iter().collect();
        let mut inv = DetachedTree::load(w.root);
        assert!(!inv.missing_blocks(&path, [key.as_str()]).unwrap().is_empty(), "fixed upstream: drop set_existing");
        assert!(inv.insert(&path, key.clone(), leaf).is_err());
        // the path alone is enough
        let mut store = path.clone();
        assert_eq!(set_existing(&mut store, w.root, &key, leaf).unwrap(), before);
    }

    #[test]
    fn corrupt_cars_and_non_canonical_blocks_fail() {
        let raw = &commits()[1];
        let mut b = raw.blocks.clone();
        let n = b.len();
        b[n - 3] ^= 1; // inside the last block's data
        let mut fails = Vec::new();
        assert!(read_car(&b, &mut fails).is_none());
        assert_eq!(fails[0].0, "car");
        assert!(canonical(&[0xa2, 0x61, 0x62, 0x01, 0x61, 0x61, 0x02]).is_err(), "unsorted keys");
        assert!(canonical(&[0x18, 0x01]).is_err(), "non-minimal int");
        assert!(canonical(&[0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18]).is_err(), "float");
        assert!(canonical(&[0xa1, 0x61, 0x61, 0x01]).is_ok());
    }
}
