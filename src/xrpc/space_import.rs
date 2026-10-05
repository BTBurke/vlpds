//! `vlpds.space.importRepo` (`--spaces`; DESIGN.md "Spaces", Q8): an
//! account moving in brings its repo in a space as the 2-root CAR its old
//! host's space.getRepo serves. There's no upstream import yet; this
//! follows the plan's proposal and changes when the upstream contract
//! settles.
//!
//! The commit is checked before anything is written: its signature and MAC
//! against the DID's current `#atproto` key (the old host's, since the DID
//! hasn't moved yet), and the set hash recomputed from the index. The
//! records then stream in, each block's CID checked and the index naming
//! it, staged in bounded frameless entries; a final entry on the repo's
//! worker switches the head in at the CAR's rev. The oplog stays empty (the
//! spec lets a host drop ops: a syncer falls back to getRepo), and the
//! authority is owed a notify like any write.
//!
//! The worker refuses the account's writes to the space while it imports
//! (`Spaces::begin_import`). An import stopped part way leaves staged rows
//! no head names, which no read serves; the next import of the space
//! deletes them first.

use super::repo::{check_path, imported_record_blobs};
use super::space::{spaces, submit_space, Space};
use super::*;
use crate::oauth::scopes::SpaceAccess;
use crate::space::commit::{self, CommitCtx, SignedCommit};
use crate::space::lthash::LtHash;
use crate::space::repo::{SpaceAck, SpaceError, SpaceOp};
use crate::tid::Tid;
use futures::StreamExt;
use std::collections::HashMap;

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
    let status = app.account(&did).await?.status;
    // OAuth, as every space write. An account moving in is deactivated,
    // and OAuth signs no deactivated account in, so until it's active its
    // own password session imports too, as com.atproto.repo.importRepo
    // takes it. App passwords never do.
    let oauth = matches!(creds, Credentials::OAuth { .. });
    let moving_in = matches!(creds, Credentials::Session { .. }) && status.as_deref() == Some("deactivated");
    if !oauth && !moving_in {
        creds.need_space(&space.target(), SpaceAccess::ReadSelf)?;
    }
    if let Some(st) = status.filter(|s| s != "deactivated") {
        return Err(inactive_account_error(&st));
    }
    // before reading the body (the worker checks again)
    let held = super::space::load_head(sp, &*app.partition(&did)?, &did, &space).await?;
    if held.as_ref().is_some_and(|h| h.records > 0) {
        return Err(invalid("this account already has records in the space; delete them first"));
    }
    let mut car = CarReader::new(body, app.config.max_import_bytes);
    let header = car.section().await?.ok_or_else(|| invalid("invalid CAR: empty"))?;
    let roots = crate::car::read_header(&header).map_err(|e| invalid(format!("invalid CAR: {e}")))?;
    let [commit_cid, index_cid] = roots[..] else {
        return Err(invalid("expected two roots: the signed commit and the index"));
    };
    let (mut commit_block, mut index_block) = (None, None);
    while commit_block.is_none() || index_block.is_none() {
        let (cid, b) = car.block().await?.ok_or_else(|| invalid("the CAR ends before its roots"))?;
        match cid {
            c if c == commit_cid && commit_block.is_none() => commit_block = Some(b),
            c if c == index_cid && index_block.is_none() => index_block = Some(b),
            c => return Err(invalid(format!("block {c} before the roots"))),
        }
    }
    let commit = decode_commit(&commit_block.unwrap_or_default())?;
    let index = decode_index(&index_block.unwrap_or_default(), sp.limits.max_records)?;
    let collections: std::collections::BTreeSet<&str> =
        index.iter().filter_map(|(p, _)| p.split_once('/').map(|(c, _)| c)).collect();
    for c in collections.iter().filter(|_| oauth) {
        creds.need_space(&space.target(), SpaceAccess::Write("create", c))?;
    }
    if collections.is_empty() && oauth {
        creds.need_space(&space.target(), SpaceAccess::ReadSelf)?;
    }
    drop(collections);
    let rev = Tid::parse(&commit.rev).ok_or_else(|| invalid("commit.rev must be a TID"))?;
    // every later write's rev follows it, and an authority refuses a
    // notify this far ahead (FutureRev), so the account would go unheard
    if rev.micros() > crate::tid::now_micros() + super::space::FUTURE_REV.as_micros() as u64 {
        return Err(bad("FutureRev", "The commit's rev is in the future"));
    }
    if held.is_some_and(|h| rev <= h.rev) {
        return Err(invalid("the imported commit's rev must be newer than the repo's in the space"));
    }
    let ctx = CommitCtx { space: &space.uri, author: &did, rev: &commit.rev };
    if !commit::verify(&commit, &ctx, &current_key(app, &did).await?) {
        return Err(bad("InvalidCommit", "The commit's signature or MAC does not verify against the account's key"));
    }
    let mut set = LtHash::default();
    for (path, cid) in &index {
        let (c, r) = path.split_once('/').unwrap_or_default();
        set.add(&commit::element(c, r, &cid.to_string()));
    }
    if !commit::matches(&set, &commit) {
        return Err(bad("DigestMismatch", "The index's set hash is not the commit's"));
    }

    let nonce = rand::random::<u64>();
    let _claim = Claim { sp, did: &did, sid: space.sid, nonce };
    submit_space(app, &did, &space, SpaceOp::ImportBegin { nonce, rev: Some(rev) }).await?;
    let p = app.partition(&did)?;
    clear_unheaded(&p, &did, &space).await?;
    let staged = stage(&p, &did, &space, &mut car, index, rev).await;
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

/// Writes the CAR's records (and their blob refs) as rows with no head
/// over them yet; Ok(the record count).
async fn stage(
    p: &crate::partition::Partition,
    did: &str,
    space: &Space,
    car: &mut CarReader,
    index: Vec<(String, Cid)>,
    rev: Tid,
) -> XResult<u64> {
    // CID -> the index's paths naming it, not yet seen
    let mut want: HashMap<Cid, Vec<String>> = HashMap::with_capacity(index.len());
    for (path, cid) in index.iter() {
        want.entry(*cid).or_default().push(path.clone());
    }
    let records = index.len() as u64;
    drop(index);
    let mut muts = Vec::new();
    let mut bytes = 0;
    while let Some((cid, b)) = car.block().await? {
        // a block the index doesn't name (or names no more) is skipped, as
        // a repo CAR's extra blocks are
        let Some(paths) = want.remove(&cid) else { continue };
        let blobs = imported_record_blobs(&paths[0], &b)?;
        for path in paths {
            muts.push(put(state::space_record_key(did, &space.sid, &path), state::record_value(&cid, rev.0, &b)));
            for blob in &blobs {
                let r = Bytes::copy_from_slice(&rev.0.to_be_bytes());
                muts.push(put(state::space_blob_key(did, &space.sid, blob, &path), r));
                muts.push(put(state::space_blob_cid_key(did, blob, &space.sid, &path), Bytes::new()));
            }
            bytes += b.len();
        }
        if muts.len() >= BATCH_ROWS || bytes >= BATCH_BYTES {
            write_private_local(p, std::mem::take(&mut muts)).await?;
            bytes = 0;
        }
    }
    if let Some((cid, paths)) = want.iter().next() {
        return Err(invalid(format!("the CAR has no block for {} ({cid})", paths[0])));
    }
    if !muts.is_empty() {
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
