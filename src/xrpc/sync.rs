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
    let (view, snap) = app.repo_view(&did).await?;
    let mut found: HashMap<Cid, Vec<u8>> = HashMap::new();
    if want.contains(&view.head.commit) {
        found.insert(view.head.commit, view.head.commit_block.to_vec());
    }
    // MST nodes, if the node index already covers this version
    let rest = |found: &HashMap<Cid, Vec<u8>>| -> Vec<Cid> {
        want.iter().filter(|c| !found.contains_key(c)).copied().collect()
    };
    let todo = rest(&found);
    if !todo.is_empty() {
        found.extend(find_nodes(&view, todo, false).await?);
    }
    // records, by the record CID index (c/ keys) of the matching snapshot
    for c in rest(&found) {
        if c.codec == crate::cid::CODEC_DAG_CBOR {
            if let Some(b) = find_record(&snap, &did, &c).await? {
                found.insert(c, b);
            }
        }
    }
    // the rest can only be nodes: build the index from this view if needed
    let todo = rest(&found);
    if !todo.is_empty() {
        found.extend(find_nodes(&view, todo, true).await?);
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

/// A record block by CID: the c/ index names the paths holding that CID
/// (or one sharing its key prefix); the record at a path must match.
async fn find_record(snap: &slatedb::DbSnapshot, did: &str, cid: &Cid) -> XResult<Option<Vec<u8>>> {
    let prefix = state::record_cid_prefix(did, cid);
    let mut iter = snap
        .scan(prefix.clone()..state::prefix_end(&prefix))
        .await
        .map_err(XrpcError::from_err)?;
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        let Ok(path) = std::str::from_utf8(&kv.key[prefix.len()..]) else {
            continue;
        };
        if let Some(v) = snap.get(state::record_key(did, path)).await.map_err(XrpcError::from_err)? {
            let (c, bytes) = state::decode_record_value(&v).map_err(XrpcError::from_err)?;
            if c == *cid {
                return Ok(Some(bytes.to_vec()));
            }
        }
    }
    Ok(None)
}

/// MST node blocks by CID from the repo's node index (O(depth) each). With
/// `build`, an index that doesn't cover this view's version is rebuilt from
/// the view (one walk, on the blocking pool), then kept up to date by the
/// repo worker; without, nothing is found unless one already covers it.
async fn find_nodes(
    view: &Arc<crate::worker::DurableView>,
    cids: Vec<Cid>,
    build: bool,
) -> XResult<Vec<(Cid, Vec<u8>)>> {
    use crate::mst::{MstError, NodeIndex};
    fn get(view: &crate::worker::DurableView, ix: &NodeIndex, cids: &[Cid]) -> Result<Vec<(Cid, Vec<u8>)>, MstError> {
        let mut out = Vec::new();
        for c in cids {
            if let Some(b) = view.tree.find_node(c, ix)? {
                out.push((*c, b));
            }
        }
        Ok(out)
    }
    let rev = view.head.rev.0;
    {
        let mut cell = view.nodes.lock();
        if let Some(ix) = cell.index.as_ref().filter(|ix| ix.covers(rev)) {
            return get(view, ix, &cids).map_err(XrpcError::from_err);
        }
        if !build {
            return Ok(Vec::new());
        }
        // from now on the worker reports written nodes, so the index built
        // below can catch up with commits made meanwhile
        cell.wanted = true;
    }
    let view = view.clone();
    tokio::task::spawn_blocking(move || {
        let ix = NodeIndex::build(&view.tree, rev)?;
        let out = get(&view, &ix, &cids)?;
        view.nodes.lock().install(ix);
        Ok::<_, MstError>(out)
    })
    .await
    .map_err(XrpcError::from_err)?
    .map_err(XrpcError::from_err)
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

/// A listRepos position: a shard, and the last DID listed in it (None =
/// from the shard's start). Cursor form "{shard}:{did}" / "{shard}:".
pub(super) type RepoPos = (usize, Option<String>);

pub(super) fn parse_list_cursor(c: &str, shards: usize) -> XResult<RepoPos> {
    let bad = || XrpcError::bad("InvalidRequest", "Malformed cursor");
    let (p, d) = c.split_once(':').ok_or_else(bad)?;
    let shard = p.parse::<usize>().map_err(|_| bad())?;
    if shard >= shards {
        return Err(bad());
    }
    Ok((shard, (!d.is_empty()).then(|| d.to_string())))
}

fn list_cursor((shard, did): &RepoPos) -> String {
    format!("{shard}:{}", did.as_deref().unwrap_or(""))
}

/// One listRepos entry.
#[derive(serde::Serialize, Deserialize)]
pub(super) struct RepoView {
    did: String,
    head: String,
    rev: String,
    active: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    status: Option<String>,
}

/// A listRepos page, as served (and as the internal page endpoint returns it).
#[derive(serde::Serialize, Deserialize)]
pub(super) struct ReposPage {
    repos: Vec<RepoView>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    cursor: Option<String>,
}

impl ReposPage {
    pub(super) fn new(repos: Vec<RepoView>, next: Option<RepoPos>) -> ReposPage {
        ReposPage { repos, cursor: next.as_ref().map(list_cursor) }
    }
}

fn json_response(body: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], Body::from(body)).into_response()
}

fn unowned(shard: usize) -> XrpcError {
    XrpcError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        error: "PartitionUnavailable".into(),
        message: format!("shard {shard} has no reachable owner; retry"),
    }
}

/// Up to `limit` repos from `pos` on, through the consecutive shards this
/// node owns; plus where the next page starts (None = past the last shard).
/// Each shard is read from one SlateDB snapshot, heads merge-joined with
/// accounts by DID. 503 if `pos`'s shard isn't ours. The local half of
/// listRepos (also served to peers by /internal/v1/sync/listRepos).
pub(super) async fn list_repos_local(app: &App, pos: RepoPos, limit: usize) -> XResult<(Vec<RepoView>, Option<RepoPos>)> {
    /// Only an account's status (serde skips the rest of the JSON).
    #[derive(Deserialize)]
    struct Status<'a> {
        #[serde(borrow, default)]
        status: Option<std::borrow::Cow<'a, str>>,
    }
    let (mut shard, mut after) = pos;
    let first = shard;
    let mut repos = Vec::with_capacity(limit.min(1000));
    while shard < app.partitions.len() {
        let Some(p) = app.partitions.get(shard) else {
            if shard == first {
                return Err(unowned(shard));
            }
            return Ok((repos, Some((shard, None))));
        };
        let (h_lo, a_lo) = match &after {
            Some(d) => ([state::head_key(d), vec![0]].concat(), [state::account_key(d), vec![0]].concat()),
            None => (b"h/".to_vec(), b"a/".to_vec()),
        };
        let snap = p.db.snapshot().await.map_err(XrpcError::from_err)?;
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, ..Default::default() };
        let mut heads = snap.scan_with_options(h_lo..state::prefix_end(b"h/"), &opts).await.map_err(XrpcError::from_err)?;
        let mut accts = snap.scan_with_options(a_lo..state::prefix_end(b"a/"), &opts).await.map_err(XrpcError::from_err)?;
        let mut acct_peek: Option<slatedb::KeyValue> = None;
        let mut acct_done = false;
        while repos.len() < limit {
            let Some(kv) = heads.next().await.map_err(XrpcError::from_err)? else {
                break;
            };
            let did_b = &kv.key[2..];
            let head = Head::decode(&kv.value).map_err(XrpcError::from_err)?;
            let mut status = None;
            while !acct_done {
                if acct_peek.is_none() {
                    acct_peek = accts.next().await.map_err(XrpcError::from_err)?;
                    if acct_peek.is_none() {
                        acct_done = true;
                        break;
                    }
                }
                let a = acct_peek.as_ref().unwrap();
                match a.key[2..].cmp(did_b) {
                    std::cmp::Ordering::Less => acct_peek = None,
                    std::cmp::Ordering::Equal => {
                        status = serde_json::from_slice::<Status>(&a.value).ok().and_then(|s| s.status.map(|s| s.into_owned()));
                        acct_peek = None;
                        break;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
            repos.push(RepoView {
                did: String::from_utf8_lossy(did_b).into_owned(),
                head: head.commit.to_string(),
                rev: head.rev.to_string(),
                active: status.is_none(),
                status,
            });
        }
        if repos.len() >= limit {
            let last = repos.last().map(|r| r.did.clone());
            return Ok((repos, Some((shard, last))));
        }
        shard += 1;
        after = None;
    }
    Ok((repos, None))
}

/// Most owners one listRepos page visits (a page crossing many small or
/// empty shards owned by different nodes returns early with a cursor).
const LIST_REPOS_MAX_HOPS: usize = 16;

/// Repos in (shard, DID) order. The cursor names a shard and the last DID
/// listed in it; a page is served from that shard's owner's own SlateDB
/// (this node, or the owner via /internal/v1/sync/listRepos), continuing
/// through the following shards that owner holds, and on to the next owner
/// only to fill the page. Each shard's DIDs are a stable key order, so a
/// repo that exists for the whole enumeration is listed exactly once; one
/// created or deleted meanwhile may or may not be. An unreachable owner ends
/// the page early with a cursor at its shard (503 if nothing was listed),
/// so a relay never skips repos.
async fn list_repos(State(app): AppState, Query(q): Query<ListReposQ>) -> XResult<Response> {
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    let mut pos = Some(match &q.cursor {
        Some(c) => parse_list_cursor(c, app.partitions.len())?,
        None => (0, None),
    });
    let mut repos: Vec<RepoView> = Vec::new();
    let mut hops = 0;
    while let Some(p) = pos.clone() {
        if repos.len() >= limit || hops == LIST_REPOS_MAX_HOPS {
            break;
        }
        hops += 1;
        let want = limit - repos.len();
        if app.partitions.get(p.0).is_some() || app.cluster.is_none() {
            let (r, next) = list_repos_local(&app, p, want).await?;
            repos.extend(r);
            pos = next;
            continue;
        }
        match owner_page(&app, &p, want).await {
            Ok((body, page)) => {
                // the owner's page is the whole answer: pass its bytes on
                if repos.is_empty() && (page.repos.len() == want || page.cursor.is_none()) {
                    return Ok(json_response(body.to_vec()));
                }
                repos.extend(page.repos);
                pos = page.cursor.as_deref().map(|c| parse_list_cursor(c, app.partitions.len())).transpose()?;
            }
            Err(e) if repos.is_empty() => return Err(e),
            Err(e) => {
                tracing::warn!(shard = p.0, "listRepos: owner page failed, ending the page early: {}", e.message);
                break;
            }
        }
    }
    let page = ReposPage::new(repos, pos);
    Ok(json_response(serde_json::to_vec(&page).map_err(XrpcError::from_err)?))
}

/// A page from the owner of `pos`'s shard (its body and parsed form).
async fn owner_page(app: &App, pos: &RepoPos, limit: usize) -> XResult<(Bytes, ReposPage)> {
    let c = app.cluster.as_ref().ok_or_else(|| unowned(pos.0))?;
    let Some((owner, addr)) = c.owner_of(pos.0 as u16).filter(|(id, _)| *id != c.cfg.node_id) else {
        return Err(unowned(pos.0));
    };
    let body = super::internal::owner_list_repos(app, &addr, &list_cursor(pos), limit).await.map_err(|e| {
        tracing::warn!(%owner, shard = pos.0, "listRepos owner page: {}", e.message);
        XrpcError { message: format!("shard {}: {}", pos.0, e.message), ..unowned(pos.0) }
    })?;
    let page: ReposPage = serde_json::from_slice(&body).map_err(|e| {
        tracing::warn!(%owner, shard = pos.0, "listRepos owner page: {e}");
        unowned(pos.0)
    })?;
    Ok((body, page))
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
    /// vlpds extension: "k/n", only events whose repo DID's hash slot is in
    /// slice k of n of the 65,536 slots (`slots::SlotRange`).
    shard: Option<String>,
}

async fn subscribe_repos(State(app): AppState, Query(q): Query<SubQ>, req: axum::extract::Request) -> Response {
    let shard = match q.shard.as_deref().map(crate::slots::SlotRange::parse) {
        None => None,
        Some(Some(r)) => Some(r),
        Some(None) => {
            return XrpcError::bad("InvalidRequest", "shard must be k/n with 0 <= k < n <= 65536").into_response();
        }
    };
    app.firehose.upgrade(req, q.cursor, shard)
}

/// Asks each configured relay (`config.crawlers`) to crawl this PDS:
/// POST {crawler}/xrpc/com.atproto.sync.requestCrawl {"hostname": <our host>}.
/// Failures are logged, not returned.
pub async fn request_crawl(app: Arc<App>) {
    let hostname = public_hostname(&app.config.public_url);
    let client = crate::http::public();
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
            let hostname = hostname.clone();
            async move {
                match client
                    .post(&url)
                    .json(&json!({"hostname": hostname}))
                    .timeout(std::time::Duration::from_secs(10))
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
