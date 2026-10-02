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

/// An export reads up to this much of the repo's `M/` range with one scan
/// (~28 B/record: a 1M-record repo's), point reads beyond. Taken from a
/// process-wide budget ([`EXPORT_PREFETCH_MB`]): less, or none, while other
/// exports hold it.
const EXPORT_PREFETCH_BYTES: usize = 64 << 20;

/// `M/` prefetch all exports may hold at once, in MiB.
static EXPORT_PREFETCH_MB: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(512);

/// Default `Config::max_exports`: exports streaming at once. Each holds a
/// blocking-pool thread for its MST walk, so this also bounds what slow
/// readers can take from that pool.
pub const DEFAULT_MAX_EXPORTS: usize = 32;
/// Default `Config::export_stall`.
pub const DEFAULT_EXPORT_STALL: std::time::Duration = std::time::Duration::from_secs(60);
/// How long an export waits for a slot before 503.
const EXPORT_SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
/// Body chunks queued between an export and its response body.
const EXPORT_QUEUE: usize = 4;

/// Record blocks exports may hold while they stream the MST nodes (the CAR
/// puts every node before any record), in MiB, process-wide; and per export.
/// An export takes it 1 MiB at a time as its buffer grows; one that can't
/// get more (or reaches its cap) reads the records it couldn't buffer with a
/// second scan afterwards. A 100k-record repo holds ~15 MB of records.
static EXPORT_BUFFER_MB: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(256);
static EXPORT_BUFFER_MAX_MB: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(64);

/// The per-export record buffer cap in MiB (tests: 0 makes every export
/// read its records twice, 1 splits a ~1 MB repo's between the buffer and
/// the second scan; the bytes are the same).
pub fn set_export_buffer_max_mb(mb: u32) {
    EXPORT_BUFFER_MAX_MB.store(mb, std::sync::atomic::Ordering::Relaxed);
}

/// Records per batch handed from an export's `R/` scan to its MST walk.
const EXPORT_BATCH: usize = 512;

/// The body chunks an export sends (~1 MiB each).
const EXPORT_CHUNK: usize = 1 << 20;

/// Record CIDs an export has written, by the first 128 bits of their
/// SHA-256 digest (which hash well as they are): records with identical
/// contents share a block, and the CAR carries it once.
type SeenCids = HashSet<u128, std::hash::BuildHasherDefault<DigestHasher>>;

#[derive(Default)]
struct DigestHasher(u64);

impl std::hash::Hasher for DigestHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = self.0.rotate_left(8) ^ *b as u64;
        }
    }
    fn write_u128(&mut self, v: u128) {
        self.0 = v as u64;
    }
}

fn cid_key(c: &Cid) -> u128 {
    let mut k = [0u8; 16];
    k.copy_from_slice(&c.digest[..16]);
    u128::from_le_bytes(k)
}

/// What an export's `R/` scan kept for the CAR's tail: the record blocks
/// (CAR-encoded, `since`-filtered, each block once) up to the buffer budget,
/// in body chunks, and the first record it didn't keep, if any (the rest is
/// read again from there).
struct Buffered {
    chunks: Vec<Vec<u8>>,
    bytes: usize,
    resume_at: Option<Vec<u8>>,
    seen: SeenCids,
    _permit: Option<tokio::sync::SemaphorePermit<'static>>,
}

/// One forward `R/` scan for an export: every record's (key, CID), in
/// batches, to the MST walk (`tx`: it rebuilds the leaves from them), and
/// the record blocks kept for after the nodes, up to the buffer budget.
async fn scan_records(
    snap: &slatedb::DbSnapshot,
    did: &str,
    since: Option<u64>,
    tx: tokio::sync::mpsc::Sender<crate::mst_store::RecordBatch>,
) -> anyhow::Result<Buffered> {
    let prefix = state::record_prefix(did);
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, cache_blocks: true, ..Default::default() };
    let mut iter = match snap.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await {
        Ok(it) => state::BatchedScan::new(it),
        Err(e) => {
            let _ = tx.send(Err(e.to_string())).await;
            return Err(e.into());
        }
    };
    let mut out = Buffered { chunks: Vec::new(), bytes: 0, resume_at: None, seen: SeenCids::default(), _permit: None };
    let (mut held_mb, cap) = (0u32, EXPORT_BUFFER_MAX_MB.load(std::sync::atomic::Ordering::Relaxed));
    let new_batch = || crate::mst_store::Records::with_capacity(EXPORT_BATCH);
    let mut batch = new_batch();
    loop {
        let kv = match iter.next().await {
            Ok(Some(kv)) => kv,
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(e.to_string())).await;
                return Err(e.into());
            }
        };
        let (cid, bytes) = match state::record_value_parts(&kv.value) {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.send(Err(e.to_string())).await;
                return Err(e);
            }
        };
        let key = &kv.key[prefix.len()..];
        batch.push(key, cid);
        if batch.len() == EXPORT_BATCH && tx.send(Ok(std::mem::replace(&mut batch, new_batch()))).await.is_err() {
            // the walk is gone (it failed: a finished one has read every
            // record): stop, leaving the rest to a second scan
            out.resume_at.get_or_insert_with(|| key.to_vec());
            return Ok(out);
        }
        if out.resume_at.is_some() || since.is_some_and(|s| state::record_value_rev(&kv.value) <= s) {
            continue;
        }
        let k = cid_key(&cid);
        if out.seen.contains(&k) {
            continue;
        }
        let need = (out.bytes + bytes.len() + 64).div_ceil(1 << 20) as u32;
        while held_mb < need {
            let Some(p) = (held_mb < cap).then(|| EXPORT_BUFFER_MB.try_acquire().ok()).flatten() else { break };
            match &mut out._permit {
                Some(h) => h.merge(p),
                None => out._permit = Some(p),
            }
            held_mb += 1;
        }
        if held_mb < need {
            out.resume_at = Some(key.to_vec());
            continue;
        }
        out.seen.insert(k);
        if out.chunks.last().is_none_or(|c| c.len() >= EXPORT_CHUNK) {
            out.chunks.push(Vec::with_capacity(EXPORT_CHUNK + 4096));
        }
        let Some(chunk) = out.chunks.last_mut() else { unreachable!() };
        let was = chunk.len();
        car::write_block(chunk, &cid, bytes);
        out.bytes += chunk.len() - was;
    }
    if !batch.is_empty() {
        let _ = tx.send(Ok(batch)).await;
    }
    Ok(out)
}

/// An export's slot (`App::exports`).
struct ExportSlot(#[allow(dead_code)] tokio::sync::OwnedSemaphorePermit);

impl Drop for ExportSlot {
    fn drop(&mut self) {
        metrics::SYNC_EXPORTS.with_label_values(&["running"]).dec();
    }
}

/// Decrements a gauge when dropped.
struct GaugeGuard(prometheus::IntGauge);

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// A slot to stream an export in: waits up to [`EXPORT_SLOT_WAIT`], then 503.
async fn export_slot(app: &App) -> XResult<ExportSlot> {
    let p = match app.exports.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            let waiting = metrics::SYNC_EXPORTS.with_label_values(&["waiting"]);
            waiting.inc();
            let _w = GaugeGuard(waiting);
            match tokio::time::timeout(EXPORT_SLOT_WAIT, app.exports.clone().acquire_owned()).await {
                Ok(Ok(p)) => p,
                _ => {
                    metrics::SYNC_EXPORTS_ENDED.with_label_values(&["shed"]).inc();
                    return Err(XrpcError {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        error: "Overloaded".into(),
                        message: "too many repo exports in progress; retry shortly".into(),
                    });
                }
            }
        }
    };
    metrics::SYNC_EXPORTS.with_label_values(&["running"]).inc();
    Ok(ExportSlot(p))
}

/// The `M/` prefetch an export may make now, and its budget permit.
fn prefetch_budget() -> (Option<tokio::sync::SemaphorePermit<'static>>, usize) {
    let want = (EXPORT_PREFETCH_BYTES >> 20) as u32;
    for mb in [want, want / 4, want / 16] {
        if let Ok(p) = EXPORT_PREFETCH_MB.try_acquire_many(mb) {
            return (Some(p), (mb as usize) << 20);
        }
    }
    (None, 0)
}

type ChunkTx = tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>;

/// Queues one body chunk. Err: the client is gone, or took nothing for
/// `stall` (a reader that stopped reading, e.g. an h2 stream at a zero
/// window): the export ends either way.
async fn send_chunk(tx: &ChunkTx, chunk: Vec<u8>, stall: std::time::Duration) -> Result<(), &'static str> {
    match tokio::time::timeout(stall, tx.send(Ok(Bytes::from(chunk)))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err("client_gone"),
        Err(_) => Err("stalled"),
    }
}

/// The body end of an export's chunk queue. An export that gives up
/// (stalled reader, read error) aborts it: the queued chunks are freed at
/// once, and the body ends with an error (the client sees a failed
/// transfer, not a short CAR that looks complete).
struct ExportBody {
    rx: parking_lot::Mutex<tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>>,
    aborted: std::sync::atomic::AtomicBool,
}

impl ExportBody {
    fn abort(&self) {
        self.aborted.store(true, Ordering::Release);
        let mut rx = self.rx.lock();
        rx.close();
        while rx.try_recv().is_ok() {}
    }
}

/// A source that fails once its export has ended (`stop` set by a failed
/// send), so the MST walk stops at its next read instead of walking (and
/// reading) the rest of the tree for nobody.
struct Stoppable<'a, S> {
    inner: S,
    stop: &'a std::cell::Cell<Option<&'static str>>,
}

impl<S> Stoppable<'_, S> {
    fn check(&self) -> Result<(), crate::mst::MstError> {
        match self.stop.get() {
            Some(r) => Err(crate::mst::MstError::Store(format!("export ended: {r}"))),
            None => Ok(()),
        }
    }
}

impl<S: crate::mst_lazy::Source> crate::mst_lazy::Source for Stoppable<'_, S> {
    fn cached(&self, cid: &Cid) -> Option<Arc<crate::mst::Node>> {
        self.inner.cached(cid)
    }
    fn remember(&self, n: &Arc<crate::mst::Node>) {
        self.inner.remember(n)
    }
    fn node(&self, cid: &Cid) -> Result<Option<Arc<[u8]>>, crate::mst::MstError> {
        self.check()?;
        self.inner.node(cid)
    }
    fn records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, out: &mut Vec<(crate::mst_lazy::Key, Cid)>) -> Result<(), crate::mst::MstError> {
        self.check()?;
        self.inner.records(lo, hi, out)
    }
    fn leaf_records(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, enc: &mut crate::mst::LeafEncoder) -> Result<(), crate::mst::MstError> {
        self.check()?;
        self.inner.leaf_records(lo, hi, enc)
    }
}

/// Streams the repo CAR: commit, MST nodes, then records, from one SlateDB
/// snapshot. One forward `R/` scan feeds both: the MST walk (on a blocking
/// thread: interior nodes from `M/`, read ahead with one range scan, leaves
/// rebuilt from the scanned keys and CIDs, one path in memory) runs
/// alongside it, and the record blocks wait in a bounded buffer
/// ([`EXPORT_BUFFER_MB`]) for the nodes to be written; records past the
/// buffer are read again afterwards. Bytes are the same either way.
///
/// Bounded: at most `Config::max_exports` run at once (each holds a
/// blocking thread for its walk), the `M/` prefetch comes from a
/// process-wide budget, and an export whose client goes away or reads
/// nothing for `Config::export_stall` stops at once (walk, scans, queued
/// chunks), so slow readers can't pin blocking threads, snapshots or
/// memory.
async fn export_repo(app: &App, did: &str, since: Option<u64>) -> XResult<Response> {
    let slot = export_slot(app).await?;
    let (view, snap) = app.repo_view(did).await?;
    let head = view.head.clone();
    drop(view);
    let did: Arc<str> = did.into();
    let stall = app.config.export_stall;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(EXPORT_QUEUE);
    let body = Arc::new(ExportBody { rx: parking_lot::Mutex::new(rx), aborted: Default::default() });
    let ours = body.clone();
    tokio::spawn(async move {
        let _slot = slot;
        let reason = match stream_export(snap, did.clone(), head, since, &tx, stall).await {
            Ok(()) => "done",
            Err(r) => r,
        };
        metrics::SYNC_EXPORTS_ENDED.with_label_values(&[reason]).inc();
        if reason != "done" && reason != "client_gone" {
            tracing::debug!(%did, reason, "getRepo export ended early");
            ours.abort();
        }
    });
    // fused: body wrappers (response compression) may poll past the end
    let stream = futures::StreamExt::fuse(futures::stream::poll_fn(move |cx| match body.rx.lock().poll_recv(cx) {
        std::task::Poll::Ready(None) if body.aborted.swap(false, Ordering::AcqRel) => {
            std::task::Poll::Ready(Some(Err(std::io::Error::other("repo export aborted"))))
        }
        r => r,
    }));
    Ok(([(header::CONTENT_TYPE, "application/vnd.ipld.car")], Body::from_stream(stream)).into_response())
}

/// [`export_repo`]'s producer. Err: why it ended early (`client_gone`,
/// `stalled`, `error`).
async fn stream_export(
    snap: Arc<slatedb::DbSnapshot>,
    did: Arc<str>,
    head: Head,
    since: Option<u64>,
    tx: &ChunkTx,
    stall: std::time::Duration,
) -> Result<(), &'static str> {
    const CHUNK: usize = EXPORT_CHUNK;
    let prefix = state::record_prefix(&did);
    let mut buf = Vec::with_capacity(CHUNK + 4096);
    car::write_header(&mut buf, &head.commit);
    car::write_block(&mut buf, &head.commit, &head.commit_block);
    let (ktx, krx) = tokio::sync::mpsc::channel(64);
    let walk = async {
        let (budget, bytes) = prefetch_budget();
        let pre = crate::mst_store::prefetch(&*snap, &did, bytes).await.map(|p| p.0).unwrap_or_default();
        let (snap2, did2, root, tx2) = (snap.clone(), did.clone(), head.data, tx.clone());
        let r = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let stop = std::cell::Cell::new(None);
            let mut emit = |c: Cid, b: &[u8]| {
                if stop.get().is_some() {
                    return;
                }
                car::write_block(&mut buf, &c, b);
                if buf.len() >= CHUNK {
                    let chunk = std::mem::replace(&mut buf, Vec::with_capacity(CHUNK + 4096));
                    if let Err(r) = rt.block_on(send_chunk(&tx2, chunk, stall)) {
                        stop.set(Some(r));
                    }
                }
            };
            let nodes = crate::mst_store::DbSource::new(&*snap2, &did2, &rt).with_prefetched(Some(&pre));
            let src = Stoppable { inner: crate::mst_store::FedSource::new(nodes, krx), stop: &stop };
            let r = crate::mst_lazy::export_blocks(root, 1, &src, &mut emit).map(|_| ());
            (r, stop.get(), buf)
        })
        .await;
        drop(budget);
        r
    };
    let (walked, scanned) = tokio::join!(walk, scan_records(&snap, &did, since, ktx));
    let mut buf = match walked {
        Ok((_, Some(reason), _)) => return Err(reason),
        Ok((Ok(()), None, buf)) => buf,
        Ok((Err(e), None, _)) => {
            tracing::warn!(%did, "getRepo: MST walk failed: {e}");
            return Err("error");
        }
        Err(e) => {
            tracing::warn!(%did, "getRepo: MST walk task: {e}");
            return Err("error");
        }
    };
    let scanned = scanned.map_err(|e| {
        tracing::warn!(%did, "getRepo: record scan failed: {e:#}");
        "error"
    })?;
    // the buffered records (no copies)
    let Buffered { chunks, resume_at, mut seen, _permit: permit, .. } = scanned;
    if !chunks.is_empty() {
        if !buf.is_empty() {
            send_chunk(tx, std::mem::take(&mut buf), stall).await?;
        }
        for c in chunks {
            send_chunk(tx, c, stall).await?;
        }
    }
    drop(permit);
    let Some(start) = resume_at else {
        if !buf.is_empty() {
            send_chunk(tx, buf, stall).await?;
        }
        return Ok(());
    };
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, max_fetch_tasks: 4, ..Default::default() };
    let mut iter = match snap.scan_with_options([&prefix[..], &start[..]].concat()..state::prefix_end(&prefix), &opts).await {
        Ok(it) => state::BatchedScan::new(it),
        Err(e) => {
            tracing::warn!(%did, "getRepo: record rescan failed: {e}");
            return Err("error");
        }
    };
    loop {
        match iter.next().await {
            Ok(Some(kv)) => {
                if since.is_none_or(|s| state::record_value_rev(&kv.value) > s) {
                    if let Ok((cid, bytes)) = state::record_value_parts(&kv.value) {
                        if seen.insert(cid_key(&cid)) {
                            car::write_block(&mut buf, &cid, bytes);
                        }
                    }
                }
                if buf.len() >= CHUNK {
                    send_chunk(tx, std::mem::replace(&mut buf, Vec::with_capacity(CHUNK + 4096)), stall).await?;
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(%did, "getRepo: record rescan failed: {e}");
                return Err("error");
            }
        }
    }
    if !buf.is_empty() {
        send_chunk(tx, buf, stall).await?;
    }
    Ok(())
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
        found.extend(lazy_nodes(&view, &snap, &did, todo, false).await?);
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
        found.extend(lazy_nodes(&view, &snap, &did, todo, true).await?);
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
    let (view, snap) = app.repo_view(&q.did).await?;
    let head = view.head.clone();
    let path = format!("{}/{}", q.collection, q.rkey);
    let mut out = Vec::new();
    car::write_header(&mut out, &head.commit);
    car::write_block(&mut out, &head.commit, &head.commit_block);
    let proof = lazy_proof(&view, &snap, &q.did, &path).await?;
    for (c, b) in proof.map_err(XrpcError::from_err)? {
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

/// A view's proof of `path`: its loaded nodes, the rest read from the
/// view's snapshot (`M/` nodes, a leaf from its record range), checked
/// against their links, without touching the shared tree.
async fn lazy_proof(
    view: &Arc<crate::worker::DurableView>,
    snap: &Arc<slatedb::DbSnapshot>,
    did: &str,
    path: &str,
) -> XResult<Result<Vec<(Cid, Vec<u8>)>, crate::mst::MstError>> {
    Ok(crate::mst_store::proof_blocks(&view.tree.root, &**snap, did, path.as_bytes()).await)
}

/// MST node blocks of a view by CID. First pass (`walk` false): its loaded
/// nodes; leaves the repo's node index places, if it covers the view (no
/// `M/` or record probe first: getBlocks of leaves); then `M/` point reads
/// on its snapshot (which holds exactly the interior nodes of the view's
/// tree). Second pass (`walk`, for what neither found nor the records
/// matched): by the node index, built if needed (one streamed walk of the
/// whole tree from the snapshot, then advanced by the worker per commit). A
/// node found in the index is the end of its first key's path, read by a
/// walk to that key. A miss in an index covering the view is final, so
/// unknown CIDs don't walk the tree again.
async fn lazy_nodes(
    view: &Arc<crate::worker::DurableView>,
    snap: &Arc<slatedb::DbSnapshot>,
    did: &str,
    cids: Vec<Cid>,
    walk: bool,
) -> XResult<Vec<(Cid, Vec<u8>)>> {
    use crate::mst::{NodeIndex, NodeRef};
    let mut want: std::collections::HashSet<Cid> = cids.into_iter().collect();
    let mut out = Vec::new();
    let rev = view.head.rev.0;
    let lookup = |ix: &NodeIndex, want: &HashSet<Cid>| -> Vec<(Cid, NodeRef)> { want.iter().filter_map(|c| ix.get(c).map(|r| (*c, r.clone()))).collect() };
    if !walk {
        crate::mst_lazy::loaded_blocks(&view.tree.root, &want, &mut out).map_err(XrpcError::from_err)?;
        for (c, _) in &out {
            want.remove(c);
        }
        let leaves: Vec<(Cid, NodeRef)> = {
            let cell = view.nodes.lock();
            match cell.index.as_ref().filter(|ix| ix.covers(rev)) {
                Some(ix) => lookup(ix, &want).into_iter().filter(|(_, (_, h))| *h == 0).collect(),
                None => Vec::new(),
            }
        };
        for (c, (key, _)) in leaves {
            if let Some(b) = path_end_block(view, snap, did, &key, &c).await? {
                want.remove(&c);
                out.push((c, b));
            }
        }
        for c in want {
            if c.codec != crate::cid::CODEC_DAG_CBOR {
                continue;
            }
            if let Some(b) = snap.get(state::mst_node_key(did, &c)).await.map_err(XrpcError::from_err)? {
                if Cid::dag_cbor(&b) == c {
                    out.push((c, b.to_vec()));
                }
            }
        }
        return Ok(out);
    }
    let covered = |want: &HashSet<Cid>| {
        let mut cell = view.nodes.lock();
        match cell.index.as_ref().filter(|ix| ix.covers(rev)) {
            Some(ix) => Some(lookup(ix, want)),
            None => {
                // from now on the worker reports written nodes, so the index
                // built below can catch up with commits made meanwhile
                cell.wanted = true;
                None
            }
        }
    };
    let mut refs = covered(&want);
    // One build per repo at a time (the others wait for it and use its
    // index), and a few process-wide: each walks the whole tree on a
    // blocking thread and holds a map of every leaf until it is installed.
    let _turn = match refs {
        Some(_) => None,
        None => {
            let gate = index_build_gate(&view.nodes);
            let turn = gate.lock_owned().await;
            refs = covered(&want);
            Some(turn)
        }
    };
    let refs = match refs {
        Some(r) => r,
        None => {
            let _slot = INDEX_BUILDS.acquire().await.expect("never closed");
            let (snap, did, root) = (snap.clone(), did.to_string(), view.head.data);
            let ix = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let mut map = HashMap::new();
                let scan = crate::mst_store::ScanSource::open(&*snap, &did, crate::mst_store::DbSource::new(&*snap, &did, &rt), &rt)?;
                crate::mst_lazy::export_blocks(root, 1, &scan, &mut |c, b| {
                    // nodes with keys of their own (all leaves): where they sit
                    if let Ok(n) = crate::mst::decode_node(b, c) {
                        if let Some(crate::mst::Entry::Value { key, .. }) = n.entries.iter().find(|e| matches!(e, crate::mst::Entry::Value { .. })) {
                            map.insert(c, (key.clone(), n.height));
                        }
                    }
                })?;
                Ok::<_, crate::mst::MstError>(NodeIndex::from_refs(map, rev))
            })
            .await
            .map_err(XrpcError::from_err)?
            .map_err(XrpcError::from_err)?;
            let r = lookup(&ix, &want);
            view.nodes.lock().install(ix);
            r
        }
    };
    for (c, (key, _)) in refs {
        if let Some(b) = path_end_block(view, snap, did, &key, &c).await? {
            out.push((c, b));
        }
    }
    Ok(out)
}

/// Node index builds running at once, process-wide.
static INDEX_BUILDS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

/// The lock a repo's node index builds take turns on, keyed by its shared
/// index cell (one per cached repo).
fn index_build_gate(cell: &crate::mst::SharedNodeIndex) -> Arc<tokio::sync::Mutex<()>> {
    type Gates = HashMap<usize, std::sync::Weak<tokio::sync::Mutex<()>>>;
    static GATES: std::sync::LazyLock<parking_lot::Mutex<Gates>> = std::sync::LazyLock::new(Default::default);
    let key = Arc::as_ptr(cell) as usize;
    let mut g = GATES.lock();
    if let Some(gate) = g.get(&key).and_then(|w| w.upgrade()) {
        return gate;
    }
    if g.len() >= 1024 {
        g.retain(|_, w| w.strong_count() > 0);
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    g.insert(key, Arc::downgrade(&gate));
    gate
}

/// The block of node `c` if it ends `key`'s path in the view's tree (the
/// node holding its own first key does).
async fn path_end_block(view: &crate::worker::DurableView, snap: &slatedb::DbSnapshot, did: &str, key: &[u8], c: &Cid) -> XResult<Option<Vec<u8>>> {
    let n = crate::mst_store::path_end(&view.tree.root, snap, did, key).await.map_err(XrpcError::from_err)?;
    match n.cid == Some(*c) {
        true => Ok(Some(crate::mst_lazy::node_block(&n).map_err(XrpcError::from_err)?)),
        false => Ok(None),
    }
}

#[derive(Deserialize)]
pub(super) struct ListReposQ {
    limit: Option<i64>,
    cursor: Option<String>,
}

/// A listRepos position: a shard, and the last DID listed in it (None =
/// from the shard's start). Cursor form "{shard}:{did}" / "{shard}:".
/// A position in the global listRepos order, (slot, DID): every repo
/// before it was listed. `after` = the last DID listed (in `slot`), or None
/// at the start of `slot`. Independent of the shard layout, so a cursor
/// stays valid across splits and merges.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RepoPos {
    pub slot: u32,
    pub after: Option<String>,
}

impl RepoPos {
    fn start() -> RepoPos {
        RepoPos { slot: 0, after: None }
    }

    fn after(did: &str) -> RepoPos {
        RepoPos { slot: crate::slots::slot_of(did) as u32, after: Some(did.to_string()) }
    }
}

/// `{slot}:{last DID}` (DID empty at a slot's start).
pub(super) fn parse_list_cursor(c: &str) -> XResult<RepoPos> {
    let bad = || XrpcError::bad("InvalidRequest", "Malformed cursor");
    let (p, d) = c.split_once(':').ok_or_else(bad)?;
    let slot = p.parse::<u32>().map_err(|_| bad())?;
    if slot >= crate::slots::SLOTS || (!d.is_empty() && crate::slots::slot_of(d) as u32 != slot) {
        return Err(bad());
    }
    Ok(RepoPos { slot, after: (!d.is_empty()).then(|| d.to_string()) })
}

fn list_cursor(p: &RepoPos) -> String {
    format!("{}:{}", p.slot, p.after.as_deref().unwrap_or(""))
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

fn unowned(shard: crate::slots::ShardId) -> XrpcError {
    XrpcError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        error: "PartitionUnavailable".into(),
        message: format!("shard {shard} has no reachable owner; retry"),
    }
}

/// Up to `limit` repos from `pos` on, through the shards (in slot order)
/// this node holds consecutively; plus where the next page starts (None =
/// past the last slot). Each shard is read from one SlateDB snapshot, heads
/// merge-joined with accounts in (slot, DID) order. 503 if `pos`'s shard
/// isn't ours. The local half of listRepos (also served to peers by
/// /internal/v1/sync/listRepos).
pub(super) async fn list_repos_local(app: &App, pos: RepoPos, limit: usize) -> XResult<(Vec<RepoView>, Option<RepoPos>)> {
    let mut repos = Vec::with_capacity(limit.min(1000));
    match list_repos_into(app, pos, limit, &mut repos).await {
        Ok(next) => Ok((repos, next)),
        Err(e) => match repos.last() {
            None => Err(e),
            // a shard read failed partway (its handoff closed the DB, a
            // store error): what was listed stands, the next page resumes
            // right after it
            Some(last) => {
                tracing::warn!("listRepos: ending the page early: {}", e.message);
                let next = RepoPos::after(&last.did);
                Ok((repos, Some(next)))
            }
        },
    }
}

/// [`list_repos_local`] into `repos`; returns the next page's start.
async fn list_repos_into(app: &App, pos: RepoPos, limit: usize, repos: &mut Vec<RepoView>) -> XResult<Option<RepoPos>> {
    /// Only an account's status (serde skips the rest of the JSON).
    #[derive(Deserialize)]
    struct Status<'a> {
        #[serde(borrow, default)]
        status: Option<std::borrow::Cow<'a, str>>,
    }
    let layout = app.partitions.layout();
    let mut pos = pos;
    let mut first = true;
    let fam = state::HEAD_FAMILY.len();
    while pos.slot < crate::slots::SLOTS {
        let range = layout.shards[layout.index_of_slot(pos.slot as u16)];
        let Some(p) = app.partitions.get(range.id) else {
            if first {
                return Err(unowned(range.id));
            }
            return Ok(Some(pos));
        };
        first = false;
        let (h_lo, a_lo) = match &pos.after {
            Some(d) => ([state::head_key(d), vec![0]].concat(), [state::account_key(d), vec![0]].concat()),
            None => (state::slot_family(pos.slot as u16, state::HEAD_FAMILY), state::slot_family(pos.slot as u16, state::ACCOUNT_FAMILY)),
        };
        let snap = p.db.snapshot().await.map_err(XrpcError::from_err)?;
        let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, ..Default::default() };
        let mut heads = state::FamilyScan::new(snap.as_ref(), state::HEAD_FAMILY, Some(h_lo), &opts).await.map_err(XrpcError::from_err)?;
        let mut accts = state::FamilyScan::new(snap.as_ref(), state::ACCOUNT_FAMILY, Some(a_lo), &opts).await.map_err(XrpcError::from_err)?;
        let mut acct_peek: Option<slatedb::KeyValue> = None;
        let mut acct_done = false;
        while repos.len() < limit {
            let Some(kv) = heads.next().await.map_err(XrpcError::from_err)? else {
                break;
            };
            // a shard's DB holds only its slots; stop at its end regardless
            if state::key_slot(&kv.key).is_none_or(|s| s as u32 >= range.hi) {
                break;
            }
            let head_pos = state::slot_did(&kv.key, fam);
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
                match state::slot_did(&a.key, fam).cmp(&head_pos) {
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
                did: String::from_utf8_lossy(head_pos.1).into_owned(),
                head: head.commit.to_string(),
                rev: head.rev.to_string(),
                active: status.is_none(),
                status,
            });
        }
        if repos.len() >= limit {
            return Ok(repos.last().map(|r| RepoPos::after(&r.did)));
        }
        pos = RepoPos { slot: range.hi, after: None };
    }
    Ok(None)
}

/// Most owners one listRepos page visits (a page crossing many small or
/// empty shards owned by different nodes returns early with a cursor).
const LIST_REPOS_MAX_HOPS: usize = 16;

/// Repos in (slot, DID) order: the cursor is `{slot}:{last DID}`, a
/// position in an order that doesn't depend on the shard layout. A page is
/// served from the shard owner's own SlateDB (this node, or the owner via
/// /internal/v1/sync/listRepos), continuing through the following shards
/// that owner holds, and on to the next owner only to fill the page. A repo
/// that exists for the whole enumeration is listed exactly once, even across
/// shard splits and merges (DESIGN.md "Online shard split/merge"); one
/// created or deleted meanwhile may or may not be. An unreachable owner ends
/// the page early with a cursor at its shard (503 if nothing was listed),
/// so a relay never skips repos.
async fn list_repos(State(app): AppState, Query(q): Query<ListReposQ>) -> XResult<Response> {
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    let mut pos = Some(match &q.cursor {
        Some(c) => parse_list_cursor(c)?,
        None => RepoPos::start(),
    });
    let mut repos: Vec<RepoView> = Vec::new();
    let mut hops = 0;
    while let Some(p) = pos.clone() {
        if repos.len() >= limit || hops == LIST_REPOS_MAX_HOPS {
            break;
        }
        hops += 1;
        let want = limit - repos.len();
        let shard = app.partitions.layout().shard_of_slot(p.slot as u16);
        if app.partitions.get(shard).is_some() || app.cluster.is_none() {
            match list_repos_local(&app, p, want).await {
                Ok((r, next)) => {
                    repos.extend(r);
                    pos = next;
                    continue;
                }
                Err(e) if repos.is_empty() => return Err(e),
                // what earlier owners listed stands: the cursor resumes here
                Err(e) => {
                    tracing::warn!(shard = shard.0, "listRepos: local shard failed, ending the page early: {}", e.message);
                    break;
                }
            }
        }
        match owner_page(&app, shard, &p, want).await {
            Ok((body, page)) => {
                // the owner's page is the whole answer: pass its bytes on
                if repos.is_empty() && (page.repos.len() == want || page.cursor.is_none()) {
                    return Ok(json_response(body.to_vec()));
                }
                repos.extend(page.repos);
                pos = page.cursor.as_deref().map(parse_list_cursor).transpose()?;
            }
            Err(e) if repos.is_empty() => return Err(e),
            Err(e) => {
                tracing::warn!(shard = shard.0, "listRepos: owner page failed, ending the page early: {}", e.message);
                break;
            }
        }
    }
    let page = ReposPage::new(repos, pos);
    Ok(json_response(serde_json::to_vec(&page).map_err(XrpcError::from_err)?))
}

/// A page from the owner of `shard` (holding `pos`): its body and parsed form.
async fn owner_page(app: &App, shard: crate::slots::ShardId, pos: &RepoPos, limit: usize) -> XResult<(Bytes, ReposPage)> {
    let c = app.cluster.as_ref().ok_or_else(|| unowned(shard))?;
    let Some((owner, addr)) = c.owner_of(shard).filter(|(id, _)| *id != c.cfg.node_id) else {
        return Err(unowned(shard));
    };
    let body = super::internal::owner_list_repos(app, &addr, &list_cursor(pos), limit).await.map_err(|e| {
        tracing::warn!(%owner, shard = shard.0, "listRepos owner page: {}", e.message);
        XrpcError { message: format!("shard {shard}: {}", e.message), ..unowned(shard) }
    })?;
    let page: ReposPage = serde_json::from_slice(&body).map_err(|e| {
        tracing::warn!(%owner, shard = shard.0, "listRepos owner page: {e}");
        unowned(shard)
    })?;
    Ok((body, page))
}

#[derive(Deserialize)]
pub(super) struct ByCollectionQ {
    collection: String,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// DIDs with records in the collection on this node's shards, in (slot,
/// DID) order after the cursor, at most `limit`; plus the shards it owns. The
/// local half of listReposByCollection (also
/// /internal/v1/sync/listReposByCollection).
pub(super) async fn list_repos_by_collection_local(
    app: &App,
    q: &ByCollectionQ,
) -> XResult<(Vec<String>, Vec<crate::slots::ShardId>)> {
    if !super::syntax::valid_nsid(&q.collection) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "collection must be a valid nsid",
        ));
    }
    let limit = super::extract::limit_param(q.limit, 500, 1, 2000)?;
    let fam = state::collection_family(&q.collection);
    let start = q.cursor.as_ref().map(|c| [state::collection_key(&q.collection, c), vec![0]].concat());
    let owned = app.partitions.owned();
    let ids: Vec<crate::slots::ShardId> = owned.iter().map(|p| p.id).collect();
    let mut scans = Vec::new();
    for p in owned {
        let (fam, start) = (fam.clone(), start.clone());
        scans.push(async move {
            let mut iter = state::FamilyScan::new(p.db.as_ref(), &fam, start, &Default::default()).await.map_err(XrpcError::from_err)?;
            let mut dids = Vec::new();
            while dids.len() < limit {
                let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
                    break;
                };
                dids.push(String::from_utf8_lossy(&state::key_body(&kv.key)[fam.len()..]).into_owned());
            }
            Ok::<_, XrpcError>(dids)
        });
    }
    let mut all = Vec::new();
    for r in futures::future::join_all(scans).await {
        all.extend(r?);
    }
    sort_slot_order(&mut all);
    all.truncate(limit);
    Ok((all, ids))
}

/// Sorts DIDs into the global (slot, DID) order and dedups them.
fn sort_slot_order(dids: &mut Vec<String>) {
    dids.sort_by_cached_key(|d| (crate::slots::slot_of(d), d.clone()));
    dids.dedup();
}

/// Scans the collection index of every shard in the cluster (peers via
/// /internal/v1/sync/listReposByCollection) and merges in (slot, DID) order,
/// an order independent of the shard layout. The cursor is the last DID
/// returned. That order spans every shard, so a shard with no answering
/// owner fails the page with 503 (retry).
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
    let mut covered: HashSet<crate::slots::ShardId> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        all.extend(serde_json::from_value::<Vec<String>>(r.body["repos"].clone()).unwrap_or_default());
    }
    if let Some(m) = app.partitions.layout().ids().into_iter().find(|p| !covered.contains(p)) {
        return Err(XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: "PartitionUnavailable".into(),
            message: format!("shard {m} has no reachable owner; retry"),
        });
    }
    sort_slot_order(&mut all);
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
    // the rate limiter's client address (a peer-forwarded request's
    // vouched-for client, not the forwarding node), so the per-IP cap and
    // the rate limits count the same client
    let client = crate::ratelimit::request_client_ip(req.headers(), req.extensions(), &app.ratelimit.trusted);
    app.firehose.upgrade(req, q.cursor, shard, client)
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
pub(super) fn public_hostname(url: &str) -> String {
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
