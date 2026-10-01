use super::*;
use std::collections::{HashMap, HashSet};

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route(
            "/xrpc/com.atproto.sync.getLatestCommit",
            get(get_latest_commit),
        )
        .route("/xrpc/com.atproto.sync.getRepoStatus", get(get_repo_status))
        .route("/xrpc/com.atproto.sync.getRepo", get(get_repo))
        .route("/xrpc/com.atproto.sync.getCheckout", get(get_checkout))
        .route("/xrpc/com.atproto.sync.getHead", get(get_head))
        .route("/xrpc/com.atproto.sync.getBlocks", get(get_blocks))
        .route("/xrpc/com.atproto.sync.getRecord", get(sync_get_record))
        .route("/xrpc/com.atproto.sync.listRepos", get(list_repos))
        .route(
            "/xrpc/com.atproto.sync.listReposByCollection",
            get(list_repos_by_collection),
        )
        .route(
            "/xrpc/com.atproto.sync.subscribeRepos",
            get(subscribe_repos),
        )
        // Relay-side methods: a PDS doesn't serve these.
        .route("/xrpc/com.atproto.sync.getHostStatus", get(not_implemented))
        .route("/xrpc/com.atproto.sync.listHosts", get(not_implemented))
        .route(
            "/xrpc/com.atproto.sync.notifyOfUpdate",
            post(not_implemented),
        )
        .route("/xrpc/com.atproto.sync.requestCrawl", post(not_implemented))
}

pub(super) async fn not_implemented() -> XrpcError {
    XrpcError {
        status: StatusCode::NOT_IMPLEMENTED,
        error: "MethodNotImplemented".into(),
        message: "Method Not Implemented".into(),
    }
}

/// Reference `assertRepoAvailability`: the account must exist; unless the
/// caller is the repo's own user or an admin, it must also be active
/// (RepoTakendown / RepoDeactivated / RepoSuspended otherwise).
pub(super) async fn assert_available(
    app: &App,
    did: &str,
    creds: Option<&Credentials>,
) -> XResult<Account> {
    let acct = match app.account(did).await {
        Ok(a) => a,
        Err(e) if e.error == "AccountNotFound" => {
            return Err(XrpcError::bad(
                "RepoNotFound",
                format!("Could not find repo for DID: {did}"),
            ))
        }
        Err(e) => return Err(e),
    };
    let self_or_admin = match creds {
        Some(Credentials::Admin) => true,
        Some(c) => c.did() == Some(did),
        None => false,
    };
    if self_or_admin {
        return Ok(acct);
    }
    match acct.status.as_deref() {
        None => Ok(acct),
        Some("takendown") => Err(XrpcError::bad(
            "RepoTakendown",
            format!("Repo has been takendown: {did}"),
        )),
        Some("deactivated") => Err(XrpcError::bad(
            "RepoDeactivated",
            format!("Repo has been deactivated: {did}"),
        )),
        Some(st) => Err(XrpcError::bad(
            &inactive_error(st),
            format!("Repo is {st}: {did}"),
        )),
    }
}

/// Parses a raw query string into decoded pairs, keeping repeated keys
/// (`cids=a&cids=b`), which axum's `Query` can't express.
pub(super) fn query_pairs(raw: &str) -> Vec<(String, String)> {
    fn decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            match b[i] {
                b'+' => out.push(b' '),
                b'%' if i + 2 < b.len()
                    && b[i + 1].is_ascii_hexdigit()
                    && b[i + 2].is_ascii_hexdigit() =>
                {
                    out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'%'));
                    i += 2;
                }
                c => out.push(c),
            }
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
        .collect()
}

#[derive(Deserialize)]
struct DidQ {
    did: String,
}

async fn get_latest_commit(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<DidQ>,
) -> XResult<Json<J>> {
    assert_available(&app, &q.did, creds.as_ref()).await?;
    let head = app.head(&q.did).await?;
    Ok(Json(
        json!({"cid": head.commit.to_string(), "rev": head.rev.to_string()}),
    ))
}

/// Deprecated: the current commit CID.
async fn get_head(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<DidQ>,
) -> XResult<Json<J>> {
    assert_available(&app, &q.did, creds.as_ref()).await?;
    let head = app.head(&q.did).await.map_err(|e| {
        if e.error == "RepoNotFound" {
            XrpcError::bad(
                "HeadNotFound",
                format!("Could not find root for DID: {}", q.did),
            )
        } else {
            e
        }
    })?;
    Ok(Json(json!({"root": head.commit.to_string()})))
}

/// active/status per the lexicon; `rev` only while active.
fn status_fields(acct: &Account) -> (bool, Option<&str>) {
    (acct.status.is_none(), acct.status.as_deref())
}

async fn get_repo_status(State(app): AppState, Query(q): Query<DidQ>) -> XResult<Json<J>> {
    let acct = assert_available(&app, &q.did, Some(&Credentials::Admin)).await?;
    let (active, status) = status_fields(&acct);
    let mut out = json!({"did": q.did, "active": active});
    if let Some(st) = status {
        out["status"] = json!(st);
    }
    if active {
        let head = app.head(&q.did).await?;
        out["rev"] = json!(head.rev.to_string());
    }
    Ok(Json(out))
}

/// Reads a consistent (head, tree) pair from durable state, retrying if a
/// commit lands between reading the head and scanning the records.
pub(super) async fn consistent_tree(
    app: &App,
    did: &str,
) -> XResult<(Head, crate::mst::Tree, Arc<slatedb::DbSnapshot>)> {
    let (view, snap) = app.repo_view(did).await?;
    Ok((view.head.clone(), view.tree.clone(), snap))
}

fn car_response(body: Vec<u8>) -> Response {
    (
        [(header::CONTENT_TYPE, "application/vnd.ipld.car")],
        Body::from(body),
    )
        .into_response()
}

#[derive(Deserialize)]
struct GetRepoQ {
    did: String,
    since: Option<String>,
}

/// Repo CAR. With `since`, records are limited to those written after that
/// rev (each record value carries the rev that wrote it). MST nodes aren't
/// stored per rev, so the current tree is always included: a superset of the
/// reference's block set that still applies cleanly for incremental sync.
async fn get_repo(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<GetRepoQ>,
) -> XResult<Response> {
    let since = match &q.since {
        Some(s) => Some(crate::tid::Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a TID"))?.0),
        None => None,
    };
    assert_available(&app, &q.did, creds.as_ref()).await?;
    export_repo(&app, &q.did, since).await
}

/// Deprecated alias of getRepo.
async fn get_checkout(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<DidQ>,
) -> XResult<Response> {
    assert_available(&app, &q.did, creds.as_ref()).await?;
    export_repo(&app, &q.did, None).await
}

/// Streams the repo CAR: commit, MST nodes (walked on a blocking thread from
/// the durable in-memory tree), then records from a matching SlateDB snapshot.
/// Memory stays bounded (~1 MiB chunks) regardless of repo size.
async fn export_repo(app: &App, did: &str, since: Option<u64>) -> XResult<Response> {
    const CHUNK: usize = 1 << 20;
    let (head, tree, snap) = consistent_tree(app, did).await?;
    let prefix = state::record_prefix(did);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(CHUNK + 4096);
        car::write_header(&mut buf, &head.commit);
        car::write_block(&mut buf, &head.commit, &head.commit_block);
        let tx2 = tx.clone();
        let walked = tokio::task::spawn_blocking(move || {
            let r = tree.walk_blocks(&mut |c, b| {
                car::write_block(&mut buf, &c, b);
                if buf.len() >= CHUNK {
                    let _ = tx2.blocking_send(Ok(Bytes::from(std::mem::replace(&mut buf, Vec::with_capacity(CHUNK + 4096)))));
                }
            });
            (r, buf)
        })
        .await;
        let mut buf = match walked {
            Ok((Ok(()), buf)) => buf,
            _ => {
                let _ = tx.send(Err(std::io::Error::other("mst walk failed"))).await;
                return;
            }
        };
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, ..Default::default() };
        let mut iter = match snap.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await {
            Ok(it) => it,
            Err(e) => {
                let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                return;
            }
        };
        loop {
            match iter.next().await {
                Ok(Some(kv)) => {
                    if since.is_none_or(|s| state::record_value_rev(&kv.value) > s) {
                        if let Ok((cid, bytes)) = state::decode_record_value(&kv.value) {
                            car::write_block(&mut buf, &cid, &bytes);
                        }
                    }
                    if buf.len() >= CHUNK && tx.send(Ok(Bytes::from(std::mem::replace(&mut buf, Vec::with_capacity(CHUNK + 4096))))).await.is_err() {
                        return; // client went away
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                    return;
                }
            }
        }
        if !buf.is_empty() {
            let _ = tx.send(Ok(Bytes::from(buf))).await;
        }
    });
    // fused: body wrappers (response compression) may poll past the end
    let stream = futures::StreamExt::fuse(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    Ok(([(header::CONTENT_TYPE, "application/vnd.ipld.car")], Body::from_stream(stream)).into_response())
}

/// Blocks by CID from the repo's current state: the commit, MST nodes of the
/// (rebuilt) current tree, and current records. Blocks only reachable from
/// older revisions aren't kept and report BlockNotFound.
async fn get_blocks(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> XResult<Response> {
    let pairs = query_pairs(raw.as_deref().unwrap_or(""));
    let did = pairs
        .iter()
        .find(|(k, _)| k == "did")
        .map(|(_, v)| v.clone())
        .ok_or_else(|| {
            XrpcError::bad(
                "InvalidRequest",
                "Error: Params must have the property \"did\"",
            )
        })?;
    let mut want = Vec::new();
    for (k, v) in &pairs {
        if k == "cids" || k == "cids[]" {
            let c = Cid::parse(v)
                .map_err(|_| XrpcError::bad("InvalidRequest", format!("invalid cid: {v}")))?;
            if !want.contains(&c) {
                want.push(c);
            }
        }
    }
    assert_available(&app, &did, creds.as_ref()).await?;
    let (head, tree, snap) = consistent_tree(&app, &did).await?;
    let mut found: HashMap<Cid, Vec<u8>> = HashMap::new();
    if want.contains(&head.commit) {
        found.insert(head.commit, head.commit_block.to_vec());
    }
    let wanted: HashSet<Cid> = want.iter().copied().filter(|c| !found.contains_key(c)).collect();
    // node blocks + one path per record CID, in one pass that stops early
    let mut records: HashMap<Cid, Vec<u8>> = HashMap::new();
    tree.find_cids(&wanted, &mut found, &mut records)
        .map_err(XrpcError::from_err)?;
    for (c, key) in records {
        if found.contains_key(&c) {
            continue;
        }
        let path = String::from_utf8_lossy(&key).into_owned();
        if let Some(v) =
            snap.get(state::record_key(&did, &path))
                .await
                .map_err(XrpcError::from_err)?
        {
            let (cid, bytes) = state::decode_record_value(&v).map_err(XrpcError::from_err)?;
            if cid == c {
                found.insert(c, bytes.to_vec());
            }
        }
    }
    let missing: Vec<String> = want
        .iter()
        .filter(|c| !found.contains_key(c))
        .map(|c| c.to_string())
        .collect();
    if !missing.is_empty() {
        return Err(XrpcError::bad(
            "BlockNotFound",
            format!("Could not find cids: {}", missing.join(",")),
        ));
    }
    // CAR v1 with no roots, as the reference does
    let mut out = Vec::new();
    let mut h = Vec::with_capacity(32);
    crate::cbor::write_map_head(&mut h, 2);
    crate::cbor::write_text(&mut h, "roots");
    crate::cbor::write_array_head(&mut h, 0);
    crate::cbor::write_text(&mut h, "version");
    crate::cbor::write_uint(&mut h, 1);
    car::write_varint(&mut out, h.len() as u64);
    out.extend_from_slice(&h);
    for c in &want {
        car::write_block(&mut out, c, &found[c]);
    }
    Ok(car_response(out))
}

#[derive(Deserialize)]
struct SyncRecordQ {
    did: String,
    collection: String,
    rkey: String,
}

async fn sync_get_record(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<SyncRecordQ>,
) -> XResult<Response> {
    if !super::syntax::valid_nsid(&q.collection) || !super::syntax::valid_rkey(&q.rkey) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "invalid collection or rkey",
        ));
    }
    assert_available(&app, &q.did, creds.as_ref()).await?;
    let (head, tree, snap) = consistent_tree(&app, &q.did).await?;
    let path = format!("{}/{}", q.collection, q.rkey);
    let mut out = Vec::new();
    car::write_header(&mut out, &head.commit);
    car::write_block(&mut out, &head.commit, &head.commit_block);
    for (c, b) in tree
        .proof_blocks(path.as_bytes())
        .map_err(XrpcError::from_err)?
    {
        car::write_block(&mut out, &c, &b);
    }
    if let Some(v) =
        snap.get(state::record_key(&q.did, &path))
            .await
            .map_err(XrpcError::from_err)?
    {
        let (cid, bytes) = state::decode_record_value(&v).map_err(XrpcError::from_err)?;
        car::write_block(&mut out, &cid, &bytes);
    }
    Ok(car_response(out))
}

#[derive(Deserialize)]
pub(super) struct ListReposQ {
    limit: Option<i64>,
    cursor: Option<String>,
}

/// One listRepos entry with its global sort key (shard, did).
#[derive(serde::Serialize, Deserialize)]
pub(super) struct RepoHit {
    shard: usize,
    did: String,
    view: J,
}

/// "{shard}:{did}" -> (shard, did)
fn parse_shard_cursor(c: &str) -> XResult<(usize, String)> {
    let bad = || XrpcError::bad("InvalidRequest", "Malformed cursor");
    let (p, d) = c.split_once(':').ok_or_else(bad)?;
    Ok((p.parse::<usize>().map_err(|_| bad())?, d.to_string()))
}

/// Repos on the shards this node owns, in (shard, did) order after the
/// cursor, at most `limit`; plus the shards it owns. The local half of
/// listRepos (also served to peers by /internal/v1/sync/listRepos).
pub(super) async fn list_repos_local(app: &App, q: &ListReposQ) -> XResult<(Vec<RepoHit>, Vec<u16>)> {
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    let (mut part, mut after) = match &q.cursor {
        Some(c) => {
            let (p, d) = parse_shard_cursor(c)?;
            (p, Some(d))
        }
        None => (0, None),
    };
    let owned: Vec<u16> = app.partitions.owned().iter().map(|p| p.id).collect();
    let mut repos = Vec::new();
    while part < app.partitions.len() && repos.len() < limit {
        let Some(p) = app.partitions.get(part) else {
            part += 1;
            after = None;
            continue;
        };
        let (h_lo, a_lo) = match &after {
            Some(d) => (
                [state::head_key(d), vec![0]].concat(),
                [state::account_key(d), vec![0]].concat(),
            ),
            None => (b"h/".to_vec(), b"a/".to_vec()),
        };
        // heads and accounts are both keyed by DID: merge-join the two scans
        let mut heads =
            p.db.scan(h_lo..state::prefix_end(b"h/"))
                .await
                .map_err(XrpcError::from_err)?;
        let mut accts =
            p.db.scan(a_lo..state::prefix_end(b"a/"))
                .await
                .map_err(XrpcError::from_err)?;
        let mut acct_peek: Option<slatedb::KeyValue> = None;
        let mut acct_done = false;
        while repos.len() < limit {
            let Some(kv) = heads.next().await.map_err(XrpcError::from_err)? else {
                break;
            };
            let did_b = &kv.key[2..];
            let did = String::from_utf8_lossy(did_b).to_string();
            let head = Head::decode(&kv.value).map_err(XrpcError::from_err)?;
            let mut acct: Option<Account> = None;
            while !acct_done {
                if acct_peek.is_none() {
                    acct_peek = accts.next().await.map_err(XrpcError::from_err)?;
                    if acct_peek.is_none() {
                        acct_done = true;
                        break;
                    }
                }
                let a = acct_peek.as_ref().unwrap();
                let a_did = &a.key[2..];
                match a_did.cmp(did_b) {
                    std::cmp::Ordering::Less => acct_peek = None,
                    std::cmp::Ordering::Equal => {
                        acct = serde_json::from_slice(&a.value).ok();
                        acct_peek = None;
                        break;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
            let mut r = json!({"did": did, "head": head.commit.to_string(), "rev": head.rev.to_string(), "active": true});
            if let Some(st) = acct.as_ref().and_then(|a| a.status.as_deref()) {
                r["active"] = json!(false);
                r["status"] = json!(st);
            }
            repos.push(RepoHit { shard: part, did, view: r });
        }
        part += 1;
        after = None;
    }
    Ok((repos, owned))
}

/// Every shard in the cluster (this node's, plus each live peer's via
/// /internal/v1/sync/listRepos), merged in (shard, did) order. Cursor:
/// "{shard}:{did}", the last returned key, so a page resumes on any node.
/// A page stops before the first shard no answering node owns (mid-move, or
/// its owner unreachable) so a relay never skips its repos; if that is the
/// very next shard, 503 (retry).
async fn list_repos(State(app): AppState, Query(q): Query<ListReposQ>) -> XResult<Json<J>> {
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    let from = match &q.cursor {
        Some(c) => parse_shard_cursor(c)?.0,
        None => 0,
    };
    let (mut hits, owned) = list_repos_local(&app, &q).await?;
    let mut query = vec![("limit", limit.to_string())];
    if let Some(c) = &q.cursor {
        query.push(("cursor", c.clone()));
    }
    let g = super::internal::gather(&app, "/internal/v1/sync/listRepos", &query).await;
    let mut covered: HashSet<u16> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        hits.extend(serde_json::from_value::<Vec<RepoHit>>(r.body["repos"].clone()).unwrap_or_default());
    }
    hits.sort_by(|a, b| (a.shard, &a.did).cmp(&(b.shard, &b.did)));
    hits.dedup_by(|a, b| a.did == b.did);
    hits.truncate(limit);
    let missing = (from..app.partitions.len()).find(|p| !covered.contains(&(*p as u16)));
    if let Some(m) = missing {
        hits.retain(|h| h.shard < m);
        if hits.is_empty() {
            return Err(XrpcError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                error: "PartitionUnavailable".into(),
                message: format!("shard {m} has no reachable owner; retry"),
            });
        }
    }
    let more = hits.len() == limit || missing.is_some();
    let cursor = hits.last().map(|h| format!("{}:{}", h.shard, h.did));
    let mut out = json!({"repos": hits.into_iter().map(|h| h.view).collect::<Vec<_>>()});
    if more {
        out["cursor"] = json!(cursor);
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
pub(super) struct ByCollectionQ {
    collection: String,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// DIDs with records in the collection on this node's shards, in DID order
/// after the cursor, at most `limit`; plus the shards it owns. The local half
/// of listReposByCollection (also /internal/v1/sync/listReposByCollection).
pub(super) async fn list_repos_by_collection_local(
    app: &App,
    q: &ByCollectionQ,
) -> XResult<(Vec<String>, Vec<u16>)> {
    if !super::syntax::valid_nsid(&q.collection) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "collection must be a valid nsid",
        ));
    }
    let limit = super::extract::limit_param(q.limit, 500, 1, 2000)?;
    let prefix = state::collection_prefix(&q.collection);
    let lo = match &q.cursor {
        Some(c) => [state::collection_key(&q.collection, c), vec![0]].concat(),
        None => prefix.clone(),
    };
    let hi = state::prefix_end(&prefix);
    let owned = app.partitions.owned();
    let ids: Vec<u16> = owned.iter().map(|p| p.id).collect();
    let mut scans = Vec::new();
    for p in owned {
        let (lo, hi, plen) = (lo.clone(), hi.clone(), prefix.len());
        scans.push(async move {
            let mut iter = p.db.scan(lo..hi).await.map_err(XrpcError::from_err)?;
            let mut dids = Vec::new();
            while dids.len() < limit {
                let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
                    break;
                };
                dids.push(String::from_utf8_lossy(&kv.key[plen..]).into_owned());
            }
            Ok::<_, XrpcError>(dids)
        });
    }
    let mut all = Vec::new();
    for r in futures::future::join_all(scans).await {
        all.extend(r?);
    }
    all.sort();
    all.dedup();
    all.truncate(limit);
    Ok((all, ids))
}

/// Scans the `C/{collection}\0{did}` index of every shard in the cluster
/// (peers via /internal/v1/sync/listReposByCollection) and merges by DID.
/// The cursor is the last DID returned. DID order spans every shard, so a
/// shard with no answering owner fails the page with 503 (retry).
async fn list_repos_by_collection(
    State(app): AppState,
    Query(q): Query<ByCollectionQ>,
) -> XResult<Json<J>> {
    let limit = super::extract::limit_param(q.limit, 500, 1, 2000)?;
    let (mut all, owned) = list_repos_by_collection_local(&app, &q).await?;
    let mut query = vec![("collection", q.collection.clone()), ("limit", limit.to_string())];
    if let Some(c) = &q.cursor {
        query.push(("cursor", c.clone()));
    }
    let g = super::internal::gather(&app, "/internal/v1/sync/listReposByCollection", &query).await;
    let mut covered: HashSet<u16> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        all.extend(serde_json::from_value::<Vec<String>>(r.body["repos"].clone()).unwrap_or_default());
    }
    if let Some(m) = (0..app.partitions.len()).find(|p| !covered.contains(&(*p as u16))) {
        return Err(XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: "PartitionUnavailable".into(),
            message: format!("shard {m} has no reachable owner; retry"),
        });
    }
    all.sort();
    all.dedup();
    all.truncate(limit);
    let mut out = json!({"repos": all.iter().map(|d| json!({"did": d})).collect::<Vec<_>>()});
    if all.len() == limit {
        out["cursor"] = json!(all.last());
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct SubQ {
    cursor: Option<i64>,
}

async fn subscribe_repos(
    State(app): AppState,
    Query(q): Query<SubQ>,
    ws: WebSocketUpgrade,
) -> Response {
    let fh = app.firehose.clone();
    ws.on_upgrade(move |socket| fh.serve(socket, q.cursor))
}

/// Asks each configured relay (`config.crawlers`) to crawl this PDS:
/// POST {crawler}/xrpc/com.atproto.sync.requestCrawl {"hostname": <our host>}.
/// Failures are logged, not returned.
pub async fn request_crawl(app: Arc<App>) {
    let hostname = public_hostname(&app.config.public_url);
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("requestCrawl: http client: {e}");
            return;
        }
    };
    let reqs = app
        .config
        .crawlers
        .iter()
        .filter(|c| !c.trim().is_empty())
        .map(|crawler| {
            let crawler = crawler.trim().trim_end_matches('/');
            let base = if crawler.contains("://") {
                crawler.to_string()
            } else {
                format!("https://{crawler}")
            };
            let url = format!("{base}/xrpc/com.atproto.sync.requestCrawl");
            let (client, hostname) = (client.clone(), hostname.clone());
            async move {
                match client
                    .post(&url)
                    .json(&json!({"hostname": hostname}))
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        tracing::info!(%url, %hostname, "requestCrawl ok")
                    }
                    Ok(r) => {
                        let st = r.status();
                        let body = r.text().await.unwrap_or_default();
                        tracing::warn!(%url, %hostname, %st, %body, "requestCrawl rejected")
                    }
                    Err(e) => tracing::warn!(%url, %hostname, "requestCrawl failed: {e}"),
                }
            }
        });
    futures::future::join_all(reqs).await;
}

/// "https://pds.example.com/" -> "pds.example.com" (port kept if present).
fn public_hostname(url: &str) -> String {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    rest.split('/').next().unwrap_or(rest).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing() {
        let p = query_pairs("did=did%3Aplc%3Aabc&cids=bafy1&cids=bafy2&x=a+b&bad=%zz&t=%4");
        assert_eq!(p[0], ("did".into(), "did:plc:abc".into()));
        assert_eq!(p[1].1, "bafy1");
        assert_eq!(p[2].1, "bafy2");
        assert_eq!(p[3].1, "a b");
        assert_eq!(p[4].1, "%zz");
        assert_eq!(p[5].1, "%4");
    }

    #[test]
    fn hostnames() {
        assert_eq!(
            public_hostname("https://pds.example.com/"),
            "pds.example.com"
        );
        assert_eq!(public_hostname("http://localhost:2583"), "localhost:2583");
    }
}
