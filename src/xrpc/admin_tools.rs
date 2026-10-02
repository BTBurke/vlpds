//! Operator tools behind `vlpds admin` (src/cli/admin.rs), the reference
//! PDS's `pdsadmin` and packages/pds/src/scripts equivalents that have no
//! com.atproto.admin.* method (DESIGN.md "Admin CLI"):
//!
//! - `vlpds.admin.publishIdentity` (script publish-identity; with `syncPlc`
//!   also script rotate-keys): emits `#identity` for a DID, first making its
//!   PLC document's `atproto` key the signing key this PDS holds.
//! - `vlpds.admin.checkRepo`: a repo's stored state checked against itself
//!   from one snapshot: the head commit (hash, data root, DID, signature),
//!   records (each hashing to its CID), the MST rebuilt from `R/` against
//!   the head's data root, the persisted interior nodes `M/` against that
//!   tree, and the record-CID, blob-ref, backlink and collection indexes.
//! - `vlpds.admin.rebuildRepo` (script rebuild-repo): re-derives the repo
//!   from its records (MST, `M/`, indexes) and signs a new commit, `#sync`
//!   (the worker's ReplaceRepo, guarded by the head commit the records were
//!   read at).
//! - `vlpds.admin.requestCrawl` (pdsadmin request-crawl): asks relays to
//!   crawl this PDS's public hostname, with per-relay results.
//!
//! DID-keyed methods route to the repo's owner like any other `did`
//! parameter (crate::forward).

use super::admin::require_admin;
use super::*;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/vlpds.admin.publishIdentity", post(publish_identity))
        .route("/xrpc/vlpds.admin.checkRepo", get(check_repo))
        .route("/xrpc/vlpds.admin.rebuildRepo", post(rebuild_repo))
        .route("/xrpc/vlpds.admin.requestCrawl", post(request_crawl))
}

fn invalid(message: impl Into<String>) -> XrpcError {
    XrpcError::bad("InvalidRequest", message)
}

fn not_found(did: &str) -> XrpcError {
    XrpcError::bad("RepoNotFound", format!("could not find repo: {did}"))
}

// ---------------------------------------------------------------------------
// publishIdentity
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishIdentityIn {
    did: String,
    /// First make the DID's PLC `atproto` key the signing key held here
    /// (reference rotate-keys); a no-op when it already is.
    #[serde(default)]
    sync_plc: bool,
}

/// Emits `#identity` for an account hosted here (any status but deleted),
/// as the reference's `sequenceIdentity`; caches of its DID document are
/// dropped. With `syncPlc` (the reference's rotate-keys), a did:plc whose
/// directory document names another signing key is updated first (signed
/// with the server rotation key), then the repo is re-signed (an empty
/// commit) and `#identity` + `#sync` emitted, so relays that saw commits
/// fail against the old document resynchronize; a PLC failure emits nothing.
async fn publish_identity(State(app): AppState, Auth(creds): Auth, Json(inp): Json<PublishIdentityIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let did = inp.did;
    let acct = app.account(&did).await.map_err(|_| not_found(&did))?;
    if !inp.sync_plc {
        // a rewrite of the unchanged account row, ordered with the repo's
        // commits, carrying the #identity frame
        let (_, after) = app.mutate_account(&did, true, false, false, |_| Ok(true)).await?;
        app.did_resolver.invalidate(&did);
        return Ok(Json(json!({"did": did, "handle": after.handle, "plcUpdated": J::Null})));
    }
    if acct.pending_signing_key.is_some() {
        return Err(invalid("a signing key rotation is in progress for this account"));
    }
    let mut plc_updated = J::Null;
    if did.starts_with("did:plc:") {
        let plc = app.plc.as_ref().ok_or_else(|| invalid("PLC registration is off on this PDS"))?;
        plc_updated = json!(plc.update_signing_key(&did, &format!("did:key:{}", acct.signing_pubkey)).await?);
    }
    let key = app.secrets.account_signing_key(&acct).await?;
    let head = app.account_op(&did, crate::worker::AccountOp::SigningKey(crate::worker::KeyStep::Finish { key })).await?;
    app.did_resolver.invalidate(&did);
    Ok(Json(json!({"did": did, "handle": acct.handle, "plcUpdated": plc_updated, "rev": head.rev.to_string()})))
}

// ---------------------------------------------------------------------------
// checkRepo / rebuildRepo
// ---------------------------------------------------------------------------

/// A record as ReplaceRepo takes it: (path, cid, bytes, blob refs).
type StoredRecord = (String, Cid, Bytes, Vec<Cid>);

/// Persisted nodes by CID.
type NodeBlocks = HashMap<Cid, Arc<[u8]>>;

/// Problems listed per check (counts are exact).
const LIST_MAX: usize = 20;

/// What `inspect` read and found.
struct Inspection {
    head: Head,
    records: Vec<StoredRecord>,
    /// Records whose bytes don't hash to their CID (or don't decode).
    bad_records: Vec<String>,
    /// The tree rebuilt from the records has the head's data root.
    matches_head: bool,
    /// Keys a rebuild deletes: `M/` nodes not in the tree or not hashing
    /// to their key, stale record-CID and blob-ref index entries.
    stale_keys: Vec<Bytes>,
    report: J,
}

fn sample<T: ToString>(v: impl IntoIterator<Item = T>) -> Vec<String> {
    v.into_iter().take(LIST_MAX).map(|x| x.to_string()).collect()
}

async fn scan_keys<R: slatedb::DbReadOps + Sync + ?Sized>(db: &R, prefix: &[u8]) -> XResult<Vec<(Bytes, Bytes)>> {
    let mut it = db.scan(prefix.to_vec()..state::prefix_end(prefix)).await.map_err(XrpcError::from_err)?;
    let mut out = Vec::new();
    while let Some(kv) = it.next().await.map_err(XrpcError::from_err)? {
        out.push((kv.key, kv.value));
    }
    Ok(out)
}

/// The commit block's checks: (hash, data root, did, signature) agree with
/// the head and the account's public key.
fn check_commit(did: &str, head: &Head, pubkey: &str) -> J {
    let cid_ok = Cid::dag_cbor(&head.commit_block) == head.commit;
    let (mut data_ok, mut did_ok, mut sig_ok) = (false, false, false);
    if let Ok(c) = Value::decode(&head.commit_block) {
        let text = |k: &str| match c.get(k) {
            Some(Value::Text(s)) => Some(s.clone()),
            _ => None,
        };
        data_ok = matches!(c.get("data"), Some(Value::Link(d)) if *d == head.data);
        did_ok = text("did").as_deref() == Some(did);
        if let (Some(cdid), Some(rev), Some(Value::Link(data)), Some(Value::Bytes(sig))) = (text("did"), text("rev"), c.get("data"), c.get("sig")) {
            let unsigned = crate::events::encode_commit(&cdid, &rev, data, None);
            let sec1 = pubkey
                .strip_prefix('z')
                .and_then(|m| bs58::decode(m).into_vec().ok())
                .and_then(|raw| raw.strip_prefix(&[0xe7, 0x01][..]).map(<[u8]>::to_vec));
            sig_ok = sec1.is_some_and(|k| crypto::verify_k256(&k, &unsigned, sig).unwrap_or(false));
        }
    }
    json!({"cidOk": cid_ok, "dataOk": data_ok, "didOk": did_ok, "signatureOk": sig_ok})
}

/// Reads `did`'s state from one snapshot of its shard (taken under the
/// apply lock: a commit's state batch is in it entirely or not at all) and
/// checks it. Runs on the repo's owner; doesn't touch the repo worker, so
/// it works on a repo that fails to load.
async fn inspect(app: &App, did: &str) -> XResult<Inspection> {
    let p = app.partition(did)?;
    let snap = {
        let _g = p.apply_lock.read().await;
        p.db.snapshot().await.map_err(XrpcError::from_err)?
    };
    let get = |k: Vec<u8>| {
        let snap = snap.clone();
        async move { slatedb::DbReadOps::get(snap.as_ref(), k).await.map_err(XrpcError::from_err) }
    };
    let hv = get(state::head_key(did)).await?.ok_or_else(|| not_found(did))?;
    let head = Head::decode(&hv).map_err(XrpcError::from_err)?;
    let acct: Account = match get(state::account_key(did)).await? {
        Some(v) => serde_json::from_slice(&v).map_err(XrpcError::from_err)?,
        None => return Err(XrpcError::internal(format!("{did}: head without account"))),
    };

    // records (R/), each hashing to its CID
    let rprefix = state::record_prefix(did);
    let mut records = Vec::new();
    let mut bad_records = Vec::new();
    for (k, v) in scan_keys(snap.as_ref(), &rprefix).await? {
        let path = String::from_utf8_lossy(&k[rprefix.len()..]).into_owned();
        let Ok((cid, bytes)) = state::decode_record_value(&v) else {
            bad_records.push(path);
            continue;
        };
        let ok = Cid::dag_cbor(&bytes) == cid;
        let mut blobs = Vec::new();
        match Value::decode(&bytes) {
            Ok(v) if ok => super::blob_refs(&v, &mut blobs),
            _ => {
                bad_records.push(path);
                continue;
            }
        }
        records.push((path, cid, bytes, blobs));
    }

    // persisted nodes (M/), the record-CID (c/) and blob-ref (b/) indexes
    let mprefix = state::mst_node_prefix(did);
    let stored: Vec<(Bytes, Bytes)> = scan_keys(snap.as_ref(), &mprefix).await?;
    let cprefix = {
        let k = state::record_cid_prefix(did, &head.data);
        k[..k.len() - 8].to_vec()
    };
    let cid_index: HashSet<Bytes> = scan_keys(snap.as_ref(), &cprefix).await?.into_iter().map(|(k, _)| k).collect();
    let blob_index: HashSet<Bytes> = scan_keys(snap.as_ref(), &state::blob_ref_prefix(did)).await?.into_iter().map(|(k, _)| k).collect();
    let mut colls = BTreeSet::new();
    for (path, ..) in &records {
        colls.insert(crate::worker::collection_of(path).to_string());
    }
    let mut colls_missing = Vec::new();
    for c in &colls {
        if get(state::collection_key(c, did)).await?.is_none() {
            colls_missing.push(c.clone());
        }
    }

    // the tree from the records, its nodes against M/ (CPU: blocking pool)
    let recs: Vec<(crate::mst_lazy::Key, Cid)> = records.iter().map(|(p, c, ..)| (Arc::from(p.as_bytes()), *c)).collect();
    let (rebuilt, want) = tokio::task::spawn_blocking(move || -> XResult<(Cid, NodeBlocks)> {
        let mut tree = crate::mst_lazy::build_tree(&recs).map_err(XrpcError::from_err)?;
        let root = tree.root_cid().map_err(XrpcError::from_err)?;
        Ok((root, crate::mst_lazy::persisted_nodes(&tree, 1)))
    })
    .await
    .map_err(XrpcError::from_err)??;
    let matches_head = rebuilt == head.data;
    let (mut have, mut corrupt, mut stale_keys) = (HashSet::new(), Vec::new(), Vec::new());
    let mut extra = Vec::new();
    for (k, v) in &stored {
        let Ok(digest) = <[u8; 32]>::try_from(&k[mprefix.len()..]) else {
            corrupt.push(hex::encode(&k[mprefix.len()..]));
            stale_keys.push(k.clone());
            continue;
        };
        let c = Cid { codec: crate::cid::CODEC_DAG_CBOR, digest };
        if Cid::dag_cbor(v) != c {
            corrupt.push(c.to_string());
            stale_keys.push(k.clone());
        } else if !want.contains_key(&c) {
            extra.push(c);
            stale_keys.push(k.clone());
        }
        have.insert(c);
    }
    let missing: Vec<&Cid> = want.keys().filter(|c| !have.contains(*c)).collect();

    let want_cids: HashSet<Bytes> = records.iter().map(|(p, c, ..)| Bytes::from(state::record_cid_key(did, c, p))).collect();
    let want_blobs: HashSet<Bytes> = records
        .iter()
        .flat_map(|(p, _, _, bs)| bs.iter().map(move |b| Bytes::from(state::blob_ref_key(did, b, p))))
        .collect();
    let cid_missing = want_cids.difference(&cid_index).count();
    let blob_missing = want_blobs.difference(&blob_index).count();
    let (n0, n1) = (stale_keys.len(), {
        stale_keys.extend(cid_index.difference(&want_cids).cloned());
        stale_keys.len()
    });
    stale_keys.extend(blob_index.difference(&want_blobs).cloned());
    let (cid_extra, blob_extra) = (n1 - n0, stale_keys.len() - n1);

    // the backlink index (bl/): each linked record's rkey under its link
    let bl_index: HashMap<Bytes, Bytes> = scan_keys(snap.as_ref(), &state::backlink_prefix(did)).await?.into_iter().collect();
    let mut want_bl: BTreeMap<Vec<u8>, crate::backlinks::Rkeys> = BTreeMap::new();
    for (path, _, bytes, _) in &records {
        let coll = crate::worker::collection_of(path);
        if let Some(l) = crate::backlinks::link(coll, bytes) {
            want_bl.entry(l).or_default().push(path[coll.len() + 1..].into());
        }
    }
    let want_bl: HashMap<Bytes, Bytes> = want_bl
        .into_iter()
        .map(|(l, mut rkeys)| {
            rkeys.sort();
            (Bytes::from(state::backlink_key(did, &l)), crate::backlinks::encode(&rkeys))
        })
        .collect();
    let bl_missing = want_bl.iter().filter(|(k, v)| bl_index.get(*k) != Some(*v)).count();
    let n2 = stale_keys.len();
    stale_keys.extend(bl_index.keys().filter(|k| !want_bl.contains_key(*k)).cloned());
    let bl_extra = stale_keys.len() - n2;

    let commit = check_commit(did, &head, &acct.signing_pubkey);
    let mut problems: Vec<String> = Vec::new();
    for k in ["cidOk", "dataOk", "didOk", "signatureOk"] {
        if commit[k] != json!(true) {
            problems.push(format!("head commit: {k} is false"));
        }
    }
    if !bad_records.is_empty() {
        problems.push(format!("{} record(s) don't hash to their CID", bad_records.len()));
    }
    if !matches_head {
        problems.push(format!("records rebuild to MST root {rebuilt}, head data is {}", head.data));
    }
    for (n, what) in [
        (missing.len(), "persisted MST node(s) missing"),
        (extra.len(), "persisted MST node(s) not in the tree"),
        (corrupt.len(), "persisted MST node(s) don't hash to their key"),
        (cid_missing, "record-CID index entries missing"),
        (cid_extra, "stale record-CID index entries"),
        (blob_missing, "blob-ref index entries missing"),
        (blob_extra, "stale blob-ref index entries"),
        (bl_missing, "backlink index entries missing or wrong"),
        (bl_extra, "stale backlink index entries"),
        (colls_missing.len(), "collection index entries missing"),
    ] {
        if n > 0 {
            problems.push(format!("{n} {what}"));
        }
    }
    let report = json!({
        "did": did,
        "ok": problems.is_empty(),
        "problems": problems,
        "status": acct.status,
        "head": {"commit": head.commit.to_string(), "data": head.data.to_string(), "rev": head.rev.to_string()},
        "commit": commit,
        "records": {"count": records.len() + bad_records.len(), "badCount": bad_records.len(), "bad": sample(&bad_records)},
        "mst": {"rebuiltRoot": rebuilt.to_string(), "matchesHead": matches_head},
        "nodes": {
            "expected": want.len(), "stored": stored.len(),
            "missing": missing.len(), "extra": extra.len(), "corrupt": corrupt.len(),
            "missingSample": sample(missing), "extraSample": sample(&extra), "corruptSample": sample(&corrupt),
        },
        "indexes": {
            "recordCidMissing": cid_missing, "recordCidExtra": cid_extra,
            "blobRefMissing": blob_missing, "blobRefExtra": blob_extra,
            "backlinkMissing": bl_missing, "backlinkExtra": bl_extra, "backlinks": bl_index.len(),
            "collectionsMissing": colls_missing,
        },
    });
    Ok(Inspection { head, records, bad_records, matches_head, stale_keys, report })
}

#[derive(Deserialize)]
struct DidQ {
    did: String,
}

async fn check_repo(State(app): AppState, Auth(creds): Auth, Query(q): Query<DidQ>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    Ok(Json(inspect(&app, &q.did).await?.report))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RebuildIn {
    did: String,
    /// Check and report what would be written, change nothing.
    #[serde(default)]
    dry_run: bool,
}

/// The reference's rebuild-repo: the repo re-derived from its records (the
/// MST, its persisted nodes, the record-CID, blob-ref and collection
/// indexes; stale `M/` nodes and index entries the check found deleted)
/// under a new signed commit (rev bumped), announced with `#sync`
/// (none while deactivated: activation emits it). The records are read from
/// a snapshot, so the replace is refused if a commit landed since
/// (`InvalidSwap`: run it again). Refused when the records can't be the
/// repo's (one doesn't hash to its CID, or they don't rebuild to the
/// head's data root: records were lost, and the repo can't load to be
/// rewritten), and for taken-down accounts (untakedown first).
async fn rebuild_repo(State(app): AppState, Auth(creds): Auth, Json(inp): Json<RebuildIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let did = inp.did;
    let ins = inspect(&app, &did).await?;
    let mut out = json!({"did": did, "dryRun": inp.dry_run, "records": ins.records.len(), "before": ins.report});
    let refuse = if !ins.bad_records.is_empty() {
        Some("records don't hash to their CIDs")
    } else if !ins.matches_head {
        Some("records don't rebuild to the head's data root (records lost: restore from a backup)")
    } else {
        None
    };
    if let Some(why) = refuse {
        return Err(XrpcError::bad("RepoUnrecoverable", format!("{did}: {why}")));
    }
    if inp.dry_run {
        return Ok(Json(out));
    }
    let (records, stale_keys) = (ins.records, ins.stale_keys);
    let swap = Some(ins.head.commit);
    let head = app.account_op(&did, crate::worker::AccountOp::ReplaceRepo { records, swap_commit: swap, stale_keys }).await?;
    tracing::warn!(%did, commit = %head.commit, rev = %head.rev, "repo rebuilt from its records (admin rebuildRepo)");
    out["commit"] = json!(head.commit.to_string());
    out["rev"] = json!(head.rev.to_string());
    out["after"] = inspect(&app, &did).await?.report;
    Ok(Json(out))
}

// ---------------------------------------------------------------------------
// requestCrawl
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct RequestCrawlIn {
    /// Relay hostnames or URLs (default: the node's `--crawlers`).
    #[serde(default)]
    relays: Vec<String>,
}

/// POSTs `com.atproto.sync.requestCrawl {"hostname": <our public host>}` to
/// each relay (a bare hostname is https), 10 s each, concurrently; reports
/// each result instead of only logging it as the startup crawl does.
async fn request_crawl(State(app): AppState, Auth(creds): Auth, body: Option<Json<RequestCrawlIn>>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let given = body.map(|Json(b)| b).unwrap_or_default().relays;
    let relays: Vec<String> = if given.iter().any(|r| !r.trim().is_empty()) { given } else { app.config.crawlers.clone() }
        .into_iter()
        .map(|r| r.trim().trim_end_matches('/').to_string())
        .filter(|r| !r.is_empty())
        .collect();
    if relays.is_empty() {
        return Err(invalid("no relays given and none configured (--crawlers)"));
    }
    let hostname = super::sync::public_hostname(&app.config.public_url);
    let client = crate::http::public();
    let results = futures::future::join_all(relays.into_iter().map(|relay| {
        let hostname = hostname.clone();
        async move {
            let base = if relay.contains("://") { relay.clone() } else { format!("https://{relay}") };
            let url = format!("{base}/xrpc/com.atproto.sync.requestCrawl");
            let r = client.post(&url).json(&json!({"hostname": hostname})).timeout(std::time::Duration::from_secs(10)).send().await;
            match r {
                Ok(r) if r.status().is_success() => json!({"relay": relay, "url": url, "ok": true, "status": r.status().as_u16()}),
                Ok(r) => {
                    let st = r.status().as_u16();
                    let body = r.text().await.unwrap_or_default();
                    json!({"relay": relay, "url": url, "ok": false, "status": st, "error": body.chars().take(500).collect::<String>()})
                }
                Err(e) => json!({"relay": relay, "url": url, "ok": false, "error": e.to_string()}),
            }
        }
    }))
    .await;
    tracing::info!(%hostname, relays = results.len(), ok = results.iter().filter(|r| r["ok"] == json!(true)).count(), "admin requestCrawl");
    Ok(Json(json!({"hostname": hostname, "results": results})))
}
