//! `vlpds.space.importRepo` (`--spaces`; DESIGN.md "Spaces", Q8): an
//! account moving in brings its repo in a space as the 2-root CAR its old
//! host's space.getRepo serves. There's no upstream import yet; this
//! follows the plan's proposal and changes when the upstream contract
//! settles.
//!
//! It's OAuth-only like every space write, so an account moving in imports
//! once it's active here, after its DID points here. The commit is checked
//! before anything is written: its signature and MAC against the DID's
//! current `#atproto` key, or the one its PLC history says it held at the
//! commit's rev (the old host's), and the set hash recomputed from the
//! index. The
//! records then stream in, each block's CID checked and the index naming
//! it, staged in bounded frameless entries; a final entry on the repo's
//! worker switches the head in at the CAR's rev. The oplog stays empty (the
//! spec lets a host drop ops: a syncer falls back to getRepo), and the
//! authority is owed a notify like any write.
//!
//! An import over a repo that's there replaces it, as the public importRepo
//! does: the claim's entry takes the old head away, the old rows are swept
//! in bounded batches, and the new head goes in at the end. The worker
//! refuses the account's writes to the space, and other imports of it,
//! while it imports (`Spaces::begin_import`). An import stopped part way
//! leaves staged rows no head names, which no read serves; the next import
//! of the space deletes them first.

use super::repo::{check_path, imported_record_blobs};
use super::space::{spaces, submit_space, Space};
use super::*;
use crate::oauth::scopes::SpaceAccess;
use crate::space::commit::{self, CommitCtx, SignedCommit};
use crate::space::lthash::LtHash;
use crate::space::repo::{SpaceAck, SpaceError, SpaceOp};
use crate::tid::Tid;
use futures::StreamExt;

pub fn routes() -> Router<Arc<App>> {
    Router::new().route("/xrpc/vlpds.space.importRepo", post(import_repo))
}

/// Rows per staged entry, or fewer at [`BATCH_BYTES`].
const BATCH_ROWS: usize = 1000;
const BATCH_BYTES: usize = 4 << 20;
/// The largest block: the index of a 100k-record repo is ~6 MB.
const MAX_BLOCK: usize = 32 << 20;

fn bad(error: &str, m: impl Into<String>) -> XrpcError {
    XrpcError::bad(error, m)
}

fn invalid(m: impl Into<String>) -> XrpcError {
    bad("InvalidRequest", m)
}

/// A CAR read section by section as the body arrives.
struct CarReader {
    body: axum::body::BodyDataStream,
    buf: Vec<u8>,
    pos: usize,
    total: usize,
    max: usize,
    eof: bool,
}

impl CarReader {
    fn new(body: Body, max: usize) -> CarReader {
        CarReader { body: body.into_data_stream(), buf: Vec::new(), pos: 0, total: 0, max, eof: false }
    }

    /// Whether `n` unread bytes are buffered, reading more as needed.
    async fn fill(&mut self, n: usize) -> XResult<bool> {
        while self.buf.len() - self.pos < n {
            if self.eof {
                return Ok(false);
            }
            if self.pos > 0 && self.pos * 2 >= self.buf.len() {
                self.buf.drain(..self.pos);
                self.pos = 0;
            }
            match self.body.next().await {
                Some(Ok(chunk)) => {
                    self.total += chunk.len();
                    if self.total > self.max {
                        return Err(super::import_stream::too_large(self.max));
                    }
                    self.buf.extend_from_slice(&chunk);
                }
                Some(Err(e)) => return Err(invalid(format!("reading the CAR: {e}"))),
                None => self.eof = true,
            }
        }
        Ok(true)
    }

    /// The next length-prefixed section, None at the end.
    async fn section(&mut self) -> XResult<Option<Vec<u8>>> {
        if !self.fill(1).await? {
            return Ok(None);
        }
        let (len, n) = loop {
            if let Some(v) = crate::car::read_varint(&self.buf[self.pos..]) {
                break v;
            }
            let have = self.buf.len() - self.pos;
            if have >= 10 || !self.fill(have + 1).await? {
                return Err(invalid("invalid CAR: bad section length"));
            }
        };
        if len > MAX_BLOCK as u64 {
            return Err(invalid("invalid CAR: block too large"));
        }
        self.pos += n;
        if !self.fill(len as usize).await? {
            return Err(invalid("invalid CAR: truncated"));
        }
        let out = self.buf[self.pos..self.pos + len as usize].to_vec();
        self.pos += len as usize;
        Ok(Some(out))
    }

    async fn block(&mut self) -> XResult<Option<(Cid, Vec<u8>)>> {
        let Some(mut s) = self.section().await? else { return Ok(None) };
        let (cid, n) = Cid::read_prefix(&s).map_err(|e| invalid(format!("invalid CAR: block CID: {e}")))?;
        s.drain(..n);
        if !crate::car::block_matches(&cid, &s) {
            return Err(invalid(format!("invalid CAR: block {cid} does not match its CID")));
        }
        Ok(Some((cid, s)))
    }
}

fn decode_commit(b: &[u8]) -> XResult<SignedCommit> {
    use crate::cbor::ValueRef;
    let v = ValueRef::decode(b).map_err(|e| invalid(format!("invalid commit block: {e}")))?;
    let bytes = |k: &str| match v.get(k) {
        Some(ValueRef::Bytes(b)) => Ok(b.to_vec()),
        _ => Err(invalid(format!("commit.{k} must be bytes"))),
    };
    let ver = match v.get("ver") {
        Some(ValueRef::Int(n)) => *n,
        _ => return Err(invalid("commit.ver must be an integer")),
    };
    let rev = match v.get("rev") {
        Some(ValueRef::Text(t)) => t.to_string(),
        _ => return Err(invalid("commit.rev must be a string")),
    };
    Ok(SignedCommit { ver, hash: bytes("hash")?, ikm: bytes("ikm")?, sig: bytes("sig")?, mac: bytes("mac")?, rev })
}

/// The index block: path -> CID, each path a valid collection/rkey.
fn decode_index(b: &[u8], max: u64) -> XResult<Vec<(String, Cid)>> {
    use crate::cbor::ValueRef;
    let ValueRef::Map(m) = ValueRef::decode(b).map_err(|e| invalid(format!("invalid index block: {e}")))? else {
        return Err(invalid("the index block must be a map"));
    };
    if m.len() as u64 > max {
        return Err(invalid(format!("Space repo record limit reached: at most {max} records")));
    }
    m.into_iter()
        .map(|(path, v)| {
            let (c, r) = path.split_once('/').ok_or_else(|| invalid(format!("invalid record path {path}")))?;
            check_path(c, Some(r))?;
            match v {
                ValueRef::Link(cid) => Ok((path.to_string(), cid)),
                _ => Err(invalid(format!("index entry {path} is not a CID"))),
            }
        })
        .collect()
}

/// A key held a little before and after its PLC operation's `createdAt`,
/// as the rev and the directory's clock needn't agree.
const KEY_SLACK_MICROS: u64 = 300_000_000;

/// The `#atproto` keys `did` held at `at` (Unix microseconds) by its PLC
/// audit log: each operation's key from its `createdAt` until the next
/// one's (the first from the start). Empty for any other DID method.
async fn past_keys(app: &App, did: &str, at: u64) -> XResult<Vec<String>> {
    if !did.starts_with("did:plc:") {
        return Ok(Vec::new());
    }
    let client = match &app.plc {
        Some(p) => p.client.clone(),
        None => crate::plc::PlcClient::new(&app.config.plc_url),
    };
    let log = client.audit_log(did).await.map_err(|e| {
        XrpcError::unavailable("PlcUnavailable", format!("Could not read the PLC history of {did}: {e}"))
    })?;
    let mut held: Vec<(u64, Option<String>)> = Vec::new();
    for e in log.as_array().into_iter().flatten() {
        if e["nullified"].as_bool() == Some(true) {
            continue;
        }
        let Some(from) = e["createdAt"]
            .as_str()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .and_then(|t| u64::try_from(t.timestamp_micros()).ok())
        else {
            continue;
        };
        let op = &e["operation"];
        let key = op["verificationMethods"]["atproto"].as_str().or(op["signingKey"].as_str()).map(String::from);
        held.push((from, key));
    }
    let mut out = Vec::new();
    for (i, (from, key)) in held.iter().enumerate() {
        let start = if i == 0 { 0 } else { from.saturating_sub(KEY_SLACK_MICROS) };
        let end = held.get(i + 1).map_or(u64::MAX, |(next, _)| next.saturating_add(KEY_SLACK_MICROS));
        if let Some(k) = key.as_ref().filter(|_| (start..=end).contains(&at)) {
            if !out.contains(k) {
                out.push(k.clone());
            }
        }
    }
    Ok(out)
}

/// The DID's current `#atproto` key, as its document says now.
async fn current_key(app: &App, did: &str) -> XResult<String> {
    app.did_resolver.invalidate(did);
    let doc = app.did_resolver.resolve(did).await.map_err(|e| invalid(format!("Could not resolve {did}: {e:?}")))?;
    let mb = crate::did_resolver::signing_key_multibase(&doc)
        .ok_or_else(|| invalid(format!("{did} has no #atproto signing key")))?;
    Ok(format!("did:key:{mb}"))
}

#[derive(Deserialize)]
struct ImportQ {
    space: String,
}

/// Releases the import's claim however it ends.
struct Claim<'a> {
    sp: &'a crate::space::Spaces,
    did: &'a str,
    sid: crate::state::SpaceId,
    nonce: u64,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.sp.end_import(self.did, self.sid, self.nonce);
    }
}

async fn import_repo(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<ImportQ>,
    body: Body,
) -> XResult<Json<J>> {
    let r = import(&app, &creds, &q, body).await;
    crate::metrics::space_import(match &r {
        Ok(_) => "ok",
        Err(e) if e.status.is_server_error() => "error",
        Err(_) => "refused",
    });
    r
}

async fn import(app: &Arc<App>, creds: &Credentials, q: &ImportQ, body: Body) -> XResult<Json<J>> {
    let sp = spaces(app)?;
    let space = Space::parse(&q.space)?;
    let did = creds.user_did()?.to_string();
    // OAuth, as every space write: need_space refuses anything else, and
    // OAuth signs only an active account in
    if !matches!(creds, Credentials::OAuth { .. }) {
        creds.need_space(&space.target(), SpaceAccess::ReadSelf)?;
    }
    if let Some(st) = app.account(&did).await?.status {
        return Err(inactive_account_error(&st));
    }
    // before reading the body (the worker checks again)
    let held = super::space::load_head(sp, &*app.partition(&did)?, &did, &space).await?;
    let mut car = CarReader::new(body, app.config.max_import_bytes);
    let header = car.section().await?.ok_or_else(|| invalid("invalid CAR: empty"))?;
    let roots = crate::car::read_header(&header).map_err(|e| invalid(format!("invalid CAR: {e}")))?;
    let [commit_cid, index_cid] = roots[..] else {
        return Err(invalid("expected two roots: the signed commit and the index"));
    };
    // verifyRepoCarFull's layout: the commit, the index, then one block per
    // index entry in its order, nothing else
    let commit_block = root_block(&mut car, commit_cid, "commit").await?;
    let index_block = root_block(&mut car, index_cid, "index").await?;
    let commit = decode_commit(&commit_block)?;
    let index = decode_index(&index_block, sp.limits.max_records)?;
    let collections: std::collections::BTreeSet<&str> =
        index.iter().filter_map(|(p, _)| p.split_once('/').map(|(c, _)| c)).collect();
    for c in &collections {
        creds.need_space(&space.target(), SpaceAccess::Write("create", c))?;
    }
    if collections.is_empty() {
        creds.need_space(&space.target(), SpaceAccess::ReadSelf)?;
    }
    drop(collections);
    // an authority hears of the import as of any write and refuses a
    // non-writer then, but by that time the repo is in; when it's hosted on
    // this node, refuse up front
    let authority_here = app.partitions.for_key(&space.authority).is_some()
        && super::server::account_if_exists(app, &space.authority).await?.is_some();
    // (no row: the authority moved in without its spaces, which aren't
    // migrated; its own repos come first)
    if authority_here {
        if let Some(row) = super::simplespace::space_row_opt(app, &space).await? {
            if !row.live() {
                return Err(super::simplespace::space_not_found());
            }
            if !super::simplespace::authorize_user(app, &space, &row, &did, "write", None).await? {
                return Err(bad("NotAuthorized", "Not a member allowed to write in this space"));
            }
        }
    }
    let rev = Tid::parse(&commit.rev).ok_or_else(|| invalid("commit.rev must be a TID"))?;
    // every later write's rev follows it, and an authority refuses a
    // notify this far ahead (FutureRev), so the account would go unheard
    if rev.micros() > crate::tid::now_micros() + super::space::FUTURE_REV.as_micros() as u64 {
        return Err(bad("FutureRev", "The commit's rev is in the future"));
    }
    // an equal rev is let through here only to be checked for the same
    // content below, once the commit verifies
    if held.as_ref().is_some_and(|h| rev < h.rev) {
        return Err(invalid("the imported commit's rev must be newer than the repo's in the space"));
    }
    let ctx = CommitCtx { space: &space.uri, author: &did, rev: &commit.rev };
    let current = current_key(app, &did).await?;
    if !commit::verify(&commit, &ctx, &current) {
        let past = past_keys(app, &did, rev.micros()).await?;
        if !past.iter().any(|k| *k != current && commit::verify(&commit, &ctx, k)) {
            return Err(bad(
                "InvalidCommit",
                "The commit's signature or MAC does not verify against a key the account held at its rev",
            ));
        }
    }
    let mut set = LtHash::default();
    for (path, cid) in &index {
        let (c, r) = path.split_once('/').unwrap_or_default();
        set.add(&commit::element(c, r, &cid.to_string()));
    }
    if !commit::matches(&set, &commit) {
        return Err(bad("DigestMismatch", "The index's set hash is not the commit's"));
    }
    if let Some(h) = held.filter(|h| h.rev == rev) {
        // a retried import of the repo as it stands: the set hash covers
        // every (path, CID), so an equal state and count is the same records
        if h.records != index.len() as u64 || h.hash.state() != set.state() {
            return Err(invalid("the imported commit's rev must be newer than the repo's in the space"));
        }
        let records = stage(None, &did, &space, &mut car, index, rev).await?;
        return Ok(Json(json!({"rev": rev.to_string(), "records": records})));
    }

    let nonce = rand::random::<u64>();
    let _claim = Claim { sp, did: &did, sid: space.sid, nonce };
    submit_space(app, &did, &space, SpaceOp::ImportBegin { nonce, rev: Some(rev) }).await?;
    let p = app.partition(&did)?;
    clear_unheaded(&p, &did, &space).await?;
    let staged = stage(Some(&p), &did, &space, &mut car, index, rev).await;
    let records = match staged {
        Ok(n) => n,
        Err(e) => {
            // rows staged with no head over them are never served, but
            // they'd hold blobs and slow the repo's first write (which
            // clears them) until then
            if let Err(c) = clear_unheaded(&p, &did, &space).await {
                tracing::warn!(space = hex::encode(space.sid), "a failed import's rows not cleared: {}", c.message);
            }
            return Err(e);
        }
    };
    let op = SpaceOp::ImportCommit { nonce, rev, hash: Box::new(set), records };
    match submit_space(app, &did, &space, op).await? {
        SpaceAck::Write { .. } => {}
        _ => return Err(XrpcError::internal("unexpected space ack")),
    }
    Ok(Json(json!({"rev": rev.to_string(), "records": records})))
}

async fn root_block(car: &mut CarReader, want: Cid, what: &str) -> XResult<Vec<u8>> {
    match car.block().await? {
        Some((c, b)) if c == want => Ok(b),
        Some((c, _)) => Err(invalid(format!("expected the {what} block {want}, got {c}"))),
        None => Err(invalid("the CAR ends before its roots")),
    }
}

/// Writes the CAR's records (and their blob refs) as rows with no head
/// over them yet; Ok(the record count). With no partition it only checks
/// the blocks.
async fn stage(
    p: Option<&crate::partition::Partition>,
    did: &str,
    space: &Space,
    car: &mut CarReader,
    index: Vec<(String, Cid)>,
    rev: Tid,
) -> XResult<u64> {
    let records = index.len() as u64;
    let mut muts = Vec::new();
    let mut bytes = 0;
    let mut want = index.into_iter();
    while let Some((cid, b)) = car.block().await? {
        let Some((path, expected)) = want.next() else {
            return Err(invalid(format!("the CAR has a block the index doesn't name ({cid})")));
        };
        if cid != expected {
            return Err(invalid(format!("expected block {expected} for {path}, got {cid}")));
        }
        let blobs = imported_record_blobs(&path, &b)?;
        let Some(p) = p else { continue };
        for blob in &blobs {
            let r = Bytes::copy_from_slice(&rev.0.to_be_bytes());
            muts.push(put(state::space_blob_key(did, &space.sid, blob, &path), r));
            muts.push(put(state::space_blob_cid_key(did, blob, &space.sid, &path), Bytes::new()));
        }
        muts.push(put(state::space_record_key(did, &space.sid, &path), state::record_value(&cid, rev.0, &b)));
        bytes += b.len();
        if muts.len() >= BATCH_ROWS || bytes >= BATCH_BYTES {
            write_private_local(p, std::mem::take(&mut muts)).await?;
            bytes = 0;
        }
    }
    if let Some((path, cid)) = want.next() {
        return Err(invalid(format!("the CAR has no block for {path} ({cid})")));
    }
    if let Some(p) = p.filter(|_| !muts.is_empty()) {
        write_private_local(p, muts).await?;
    }
    Ok(records)
}

fn put(key: Vec<u8>, val: Bytes) -> crate::segment::Mutation {
    crate::segment::Mutation { key: key.into(), val: Some(val) }
}

/// Clears the rows of `did`'s headless repo in the space under an import's
/// claim (so no import stages meanwhile). A head that turned up meanwhile
/// leaves them be: they're its own.
pub(super) async fn sweep_unheaded(app: &App, did: &str, space: &Space) -> XResult<()> {
    let sp = spaces(app)?;
    let nonce = rand::random::<u64>();
    let _claim = Claim { sp, did, sid: space.sid, nonce };
    let op = SpaceOp::ImportBegin { nonce, rev: None };
    match super::space::submit_space_once(app, did, space, op).await? {
        Ok(_) => clear_unheaded(&*app.partition(did)?, did, space).await,
        Err(SpaceError::Write(WriteError::Invalid(_))) => Ok(()),
        Err(e) => Err(super::space::space_error(e)),
    }
}

/// Rows of the account's repo in the space with no head over them (an
/// earlier import stopped part way, or a deleted repo's sweep did): gone
/// before this import stages its own.
async fn clear_unheaded(p: &crate::partition::Partition, did: &str, space: &Space) -> XResult<()> {
    for fam in [state::SPACE_BLOB_FAMILY, state::SPACE_RECORD_FAMILY, state::SPACE_OPLOG_FAMILY] {
        let prefix = state::space_prefix(fam, did, &space.sid);
        loop {
            let mut iter = p.db.scan(prefix.clone()..state::prefix_end(&prefix)).await.map_err(XrpcError::from_err)?;
            let rows = iter.next_batch(BATCH_ROWS).await.map_err(XrpcError::from_err)?;
            if rows.is_empty() {
                break;
            }
            let mut muts = Vec::with_capacity(rows.len() * 2);
            for kv in rows {
                if fam == state::SPACE_BLOB_FAMILY {
                    let (cid, path) = crate::space::rows::blob_ref_parts(&kv.key[prefix.len()..])
                        .ok_or_else(|| XrpcError::internal("bad space blob ref key"))?;
                    let cid = Cid::parse(cid).map_err(XrpcError::from_err)?;
                    let key = state::space_blob_cid_key(did, &cid, &space.sid, path);
                    muts.push(crate::segment::Mutation { key: key.into(), val: None });
                }
                muts.push(crate::segment::Mutation { key: kv.key, val: None });
            }
            write_private_local(p, muts).await?;
        }
    }
    Ok(())
}
