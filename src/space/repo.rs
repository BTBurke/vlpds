//! Space writes on the repo worker (`crate::worker`): the per-repo state a
//! worker keeps for the spaces an account writes to (`SpaceHead`) and
//! governs (`HostHead`), what a request needs read first, and the log
//! mutations of each op. Everything here runs on the worker thread except
//! [`fetch`], which the worker runs on the runtime.
//!
//! Prev CIDs: a write needs the CID each path held before it. Paths written
//! by entries not yet applied are in the head's `overlay`; anything else is
//! read from `sR` off the worker thread into `fetched`. The overlay is only
//! pruned (applied entries dropped) while `fetched` is empty, so a value
//! read before an entry applied is never used once that entry's overlay is
//! gone.

use super::heads::DurableSpaceHead;
use super::lthash::LtHash;
use super::rows::{HeadRow, MemberRow, OpAction, OpRow, OutboxRow, Policy, SpaceRow, WriterRow};
use crate::cid::Cid;
use crate::segment::Mutation;
use crate::state::{self, SpaceId};
use crate::tid::{self, Tid};
use crate::worker::WriteError;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const MAX_WRITES: usize = 200;

pub enum SpaceWrite {
    Create {
        collection: String,
        rkey: String,
        cid: Cid,
        bytes: Bytes,
    },
    /// applyWrites#update (`must_exist`), or putRecord, which is a create
    /// or an update by what the path holds: `put` names the scope each would
    /// lack (None: granted).
    Update {
        collection: String,
        rkey: String,
        cid: Cid,
        bytes: Bytes,
        must_exist: bool,
        put: Option<PutScopes>,
    },
    /// deleteRecord of a missing record is a no-op (`must_exist` false).
    Delete {
        collection: String,
        rkey: String,
        must_exist: bool,
    },
}

#[derive(Clone, Debug, Default)]
pub struct PutScopes {
    pub create: Option<String>,
    pub update: Option<String>,
}

impl SpaceWrite {
    fn parts(&self) -> (&str, &str) {
        match self {
            SpaceWrite::Create { collection, rkey, .. }
            | SpaceWrite::Update { collection, rkey, .. }
            | SpaceWrite::Delete { collection, rkey, .. } => (collection, rkey),
        }
    }

    pub fn path(&self) -> String {
        let (c, r) = self.parts();
        format!("{c}/{r}")
    }
}

pub enum SpaceOp {
    /// A write to the worker's account's repo in the space.
    Write { writes: Vec<SpaceWrite> },
    /// The space host records a writer's newer state (notifyWrite), the
    /// worker's account being the authority.
    RecordWriter { writer: String, repo_rev: Tid, hash: [u8; 32] },
    /// simplespace.createSpace by the worker's account.
    CreateSpace { row: SpaceRow },
}

pub struct SpaceReq {
    pub did: Arc<str>,
    pub uri: Arc<str>,
    pub sid: SpaceId,
    pub op: SpaceOp,
    pub spaces: Arc<super::Spaces>,
    pub reply: tokio::sync::oneshot::Sender<Result<SpaceAck, SpaceError>>,
    /// Admission permit, held while queued.
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug, Clone)]
pub enum SpaceOutcome {
    Create {
        path: String,
        cid: Cid,
    },
    Update {
        path: String,
        cid: Cid,
    },
    Delete,
    /// A delete of a record that wasn't there.
    Noop,
}

#[derive(Debug, Clone, Copy)]
pub struct Sequenced {
    pub space_rev: Tid,
    pub prev: Option<Tid>,
}

#[derive(Debug, Clone)]
pub enum SpaceAck {
    /// `rev`: None if nothing was written.
    Write {
        rev: Option<Tid>,
        results: Vec<SpaceOutcome>,
    },
    /// None: not newer than what the host has.
    Writer(Option<Sequenced>),
    Created,
}

#[derive(Debug, Clone)]
pub enum SpaceError {
    Write(WriteError),
    RecordNotFound(String),
    RecordAlreadyExists(String),
    ScopeMissing(String),
    SpaceNotFound,
    SpaceAlreadyExists,
    NotAuthorized(String),
}

impl From<WriteError> for SpaceError {
    fn from(e: WriteError) -> SpaceError {
        SpaceError::Write(e)
    }
}

fn internal(m: impl Into<String>) -> SpaceError {
    SpaceError::Write(WriteError::Internal(m.into()))
}

/// One account's repo in one space, as the worker builds on it: the head
/// after every entry it sent, applied or not.
pub struct SpaceHead {
    pub uri: Arc<str>,
    pub rev: Option<Tid>,
    pub hash: LtHash,
    pub records: u64,
    pub created: u64,
    /// path -> CID (None: deleted) written by an entry, and whether that
    /// entry has applied.
    overlay: HashMap<String, (Option<Cid>, Arc<AtomicBool>)>,
    /// Read from `sR` for the requests about to run.
    fetched: HashMap<String, Option<Cid>>,
}

impl SpaceHead {
    fn new(uri: Arc<str>, row: Option<HeadRow>) -> SpaceHead {
        let (rev, hash, records, created) = match row {
            Some(r) => (Some(r.rev), r.hash, r.records, r.created),
            None => (None, LtHash::default(), 0, 0),
        };
        SpaceHead { uri, rev, hash, records, created, overlay: HashMap::new(), fetched: HashMap::new() }
    }

    fn known(&self, path: &str) -> Option<Option<Cid>> {
        self.overlay.get(path).map(|(c, _)| *c).or_else(|| self.fetched.get(path).copied())
    }

    fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.uri.len() + (self.overlay.len() + self.fetched.len()) * 128
    }
}

/// A space the worker's account governs.
pub struct HostHead {
    pub uri: Arc<str>,
    pub space: Option<SpaceRow>,
    /// The newest `sQ` spaceRev.
    pub max_space_rev: Option<Tid>,
    /// `sW` rows read so far (None: absent).
    pub writers: HashMap<String, Option<WriterRow>>,
    pub members: HashMap<String, Option<MemberRow>>,
}

impl HostHead {
    fn live(&self) -> Option<&SpaceRow> {
        self.space.as_ref().filter(|s| s.live())
    }

    fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.uri.len() + 256 + (self.writers.len() + self.members.len()) * 128
    }
}

#[derive(Default)]
pub struct SpaceStates {
    pub repos: HashMap<SpaceId, SpaceHead>,
    pub hosts: HashMap<SpaceId, HostHead>,
}

impl SpaceStates {
    pub fn heap_bytes(&self) -> usize {
        self.repos.values().map(SpaceHead::heap_bytes).sum::<usize>()
            + self.hosts.values().map(HostHead::heap_bytes).sum::<usize>()
    }

    /// Drops overlay entries whose entries have applied, unless values read
    /// for the next requests are pending use.
    pub fn prune(&mut self) {
        for h in self.repos.values_mut() {
            if h.fetched.is_empty() {
                h.overlay.retain(|_, (_, applied)| !applied.load(Ordering::Acquire));
            }
        }
    }

    /// After the requests that needed them ran.
    pub fn clear_fetched(&mut self) {
        for h in self.repos.values_mut() {
            h.fetched.clear();
        }
    }
}

/// What a worker must read before running a repo's space requests.
#[derive(Default, Debug)]
pub struct SpaceNeed {
    heads: Vec<(SpaceId, Arc<str>)>,
    paths: Vec<(SpaceId, String)>,
    hosts: Vec<(SpaceId, Arc<str>)>,
    writers: Vec<(SpaceId, String)>,
    members: Vec<(SpaceId, String)>,
}

impl SpaceNeed {
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty()
            && self.paths.is_empty()
            && self.hosts.is_empty()
            && self.writers.is_empty()
            && self.members.is_empty()
    }

    fn head(&mut self, st: &SpaceStates, sid: SpaceId, uri: &Arc<str>) {
        if !st.repos.contains_key(&sid) && !self.heads.iter().any(|(s, _)| *s == sid) {
            self.heads.push((sid, uri.clone()));
        }
    }

    fn host(&mut self, st: &SpaceStates, sid: SpaceId, uri: &Arc<str>) {
        if !st.hosts.contains_key(&sid) && !self.hosts.iter().any(|(s, _)| *s == sid) {
            self.hosts.push((sid, uri.clone()));
        }
    }

    fn writer(&mut self, st: &SpaceStates, sid: SpaceId, did: &str) {
        if !st.hosts.get(&sid).is_some_and(|h| h.writers.contains_key(did)) {
            self.writers.push((sid, did.to_string()));
        }
    }

    fn member(&mut self, st: &SpaceStates, sid: SpaceId, did: &str) {
        if !st.hosts.get(&sid).is_some_and(|h| h.members.contains_key(did)) {
            self.members.push((sid, did.to_string()));
        }
    }

    /// Adds what `req` needs that `st` doesn't hold.
    pub fn add(&mut self, st: &SpaceStates, req: &SpaceReq) {
        let (sid, uri, did) = (req.sid, &req.uri, &*req.did);
        match &req.op {
            SpaceOp::Write { writes } => {
                self.head(st, sid, uri);
                for w in writes {
                    let p = w.path();
                    if !st.repos.get(&sid).is_some_and(|h| h.known(&p).is_some())
                        && !self.paths.contains(&(sid, p.clone()))
                    {
                        self.paths.push((sid, p));
                    }
                }
                if authority(uri) == Some(did) {
                    self.host(st, sid, uri);
                    self.writer(st, sid, did);
                }
            }
            SpaceOp::RecordWriter { writer, .. } => {
                self.host(st, sid, uri);
                self.writer(st, sid, writer);
                self.member(st, sid, writer);
            }
            SpaceOp::CreateSpace { .. } => self.host(st, sid, uri),
        }
    }
}

fn authority(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://")?.split('/').next()
}

#[derive(Default)]
pub struct Fetched {
    heads: Vec<(SpaceId, Arc<str>, Option<HeadRow>)>,
    paths: Vec<(SpaceId, String, Option<Cid>)>,
    hosts: Vec<(SpaceId, Arc<str>, Option<SpaceRow>, Option<Tid>)>,
    writers: Vec<(SpaceId, String, Option<WriterRow>)>,
    members: Vec<(SpaceId, String, Option<MemberRow>)>,
}

/// Reads what `need` names. A head or space row naming another URI than
/// requested (a space id collision) is an error.
pub async fn fetch(db: &slatedb::Db, did: &str, need: SpaceNeed) -> anyhow::Result<Fetched> {
    let get = |k: Vec<u8>| db.get(k);
    let mut f = Fetched::default();
    for (sid, uri) in need.heads {
        let row = get(state::space_head_key(did, &sid)).await?.map(|v| HeadRow::decode(&v)).transpose()?;
        if let Some(r) = &row {
            anyhow::ensure!(*r.uri == *uri, "space id collision: {} and {uri}", r.uri);
        }
        f.heads.push((sid, uri, row));
    }
    let paths = futures::future::try_join_all(need.paths.into_iter().map(|(sid, path)| async move {
        let v = db.get(state::space_record_key(did, &sid, &path)).await?;
        let cid = v.map(|v| state::record_value_parts(&v).map(|(c, _)| c)).transpose()?;
        anyhow::Ok((sid, path, cid))
    }))
    .await?;
    f.paths = paths;
    for (sid, uri) in need.hosts {
        let row = get(state::space_key(did, &sid)).await?.map(|v| SpaceRow::decode(&v)).transpose()?;
        if let Some(r) = &row {
            anyhow::ensure!(*r.uri == *uri, "space id collision: {} and {uri}", r.uri);
        }
        let prefix = state::space_prefix(state::SPACE_SEQ_FAMILY, did, &sid);
        let opts = slatedb::config::ScanOptions::default().with_order(slatedb::IterationOrder::Descending);
        let mut it = db.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await?;
        let max = it.next().await?.and_then(|kv| super::rows::seq_rev(&kv.key));
        f.hosts.push((sid, uri, row, max));
    }
    for (sid, w) in need.writers {
        let row = get(state::space_writer_key(did, &sid, &w)).await?.map(|v| WriterRow::decode(&v)).transpose()?;
        f.writers.push((sid, w, row));
    }
    for (sid, m) in need.members {
        let row = get(state::space_member_key(did, &sid, &m)).await?.map(|v| MemberRow::decode(&v)).transpose()?;
        f.members.push((sid, m, row));
    }
    Ok(f)
}

/// Installs fetched state; what is already held is newer and kept.
pub fn install(st: &mut SpaceStates, f: Fetched) {
    for (sid, uri, row) in f.heads {
        st.repos.entry(sid).or_insert_with(|| SpaceHead::new(uri, row));
    }
    for (sid, path, cid) in f.paths {
        if let Some(h) = st.repos.get_mut(&sid) {
            h.fetched.insert(path, cid);
        }
    }
    for (sid, uri, space, max) in f.hosts {
        st.hosts.entry(sid).or_insert_with(|| HostHead {
            uri,
            space,
            max_space_rev: max,
            writers: HashMap::new(),
            members: HashMap::new(),
        });
    }
    for (sid, w, row) in f.writers {
        if let Some(h) = st.hosts.get_mut(&sid) {
            h.writers.entry(w).or_insert(row);
        }
    }
    for (sid, m, row) in f.members {
        if let Some(h) = st.hosts.get_mut(&sid) {
            h.members.entry(m).or_insert(row);
        }
    }
}

fn put(key: Vec<u8>, val: Bytes) -> Mutation {
    Mutation { key: key.into(), val: Some(val) }
}

fn del(key: Vec<u8>) -> Mutation {
    Mutation { key: key.into(), val: None }
}

/// A write's entry: its mutations, the head readers get once it is acked,
/// and the outbox row to send then (None when the author is the authority).
pub struct BuiltWrite {
    pub muts: Vec<Mutation>,
    pub rev: Tid,
    pub head: DurableSpaceHead,
    pub notify: Option<OutboxRow>,
    pub results: Vec<SpaceOutcome>,
}

/// Validates `writes` against the repo as the worker holds it and builds
/// their entry, updating the held state as if it were sent (`applied` is
/// set once it applies). Ok(Err(results)): nothing to write.
#[allow(clippy::too_many_arguments)]
pub fn write(
    st: &mut SpaceStates,
    did: &str,
    sid: SpaceId,
    uri: &Arc<str>,
    writes: Vec<SpaceWrite>,
    clock_id: u64,
    applied: &Arc<AtomicBool>,
    delivered: Vec<(SpaceId, Tid)>,
) -> Result<Result<BuiltWrite, Vec<SpaceOutcome>>, SpaceError> {
    if writes.len() > MAX_WRITES {
        return Err(SpaceError::Write(WriteError::Invalid(format!("Too many writes. Max: {MAX_WRITES}"))));
    }
    let head = st.repos.get_mut(&sid).ok_or_else(|| internal("space head not loaded"))?;
    if *head.uri != **uri {
        return Err(internal(format!("space id collision: {} and {uri}", head.uri)));
    }
    let mut batch: HashMap<String, Option<Cid>> = HashMap::new();
    let mut ops: Vec<(OpRow, Option<Bytes>)> = Vec::with_capacity(writes.len());
    let mut results = Vec::with_capacity(writes.len());
    for w in writes {
        let path = w.path();
        let prev = match batch.get(&path) {
            Some(c) => *c,
            None => head.known(&path).ok_or_else(|| internal(format!("space record {path} not loaded")))?,
        };
        let (action, new, bytes) = match w {
            SpaceWrite::Create { cid, bytes, .. } => {
                if prev.is_some() {
                    return Err(SpaceError::RecordAlreadyExists(format!("Record already exists: {path}")));
                }
                (OpAction::Create, Some(cid), Some(bytes))
            }
            SpaceWrite::Update { cid, bytes, must_exist, put, .. } => {
                if prev.is_none() && must_exist {
                    return Err(SpaceError::RecordNotFound(format!("Record not found: {path}")));
                }
                let missing = put.and_then(|p| if prev.is_some() { p.update } else { p.create });
                if let Some(scope) = missing {
                    return Err(SpaceError::ScopeMissing(scope));
                }
                let action = if prev.is_some() { OpAction::Update } else { OpAction::Create };
                (action, Some(cid), Some(bytes))
            }
            SpaceWrite::Delete { must_exist, .. } => {
                if prev.is_none() {
                    if must_exist {
                        return Err(SpaceError::RecordNotFound(format!("Record not found: {path}")));
                    }
                    results.push(SpaceOutcome::Noop);
                    continue;
                }
                (OpAction::Delete, None, None)
            }
        };
        results.push(match (action, new) {
            (OpAction::Create, Some(cid)) => SpaceOutcome::Create { path: path.clone(), cid },
            (OpAction::Update, Some(cid)) => SpaceOutcome::Update { path: path.clone(), cid },
            _ => SpaceOutcome::Delete,
        });
        batch.insert(path.clone(), new);
        let (collection, rkey) = path.split_once('/').map(|(c, r)| (c.to_string(), r.to_string())).unwrap_or_default();
        ops.push((OpRow { action, collection, rkey, cid: new, prev }, bytes));
    }
    if ops.is_empty() {
        return Ok(Err(results));
    }
    let rev = tid::next_rev(head.rev, clock_id);
    let mut muts = Vec::with_capacity(2 * ops.len() + 6);
    for (idx, (op, bytes)) in ops.iter().enumerate() {
        let path = format!("{}/{}", op.collection, op.rkey);
        if let Some(p) = op.prev {
            head.hash.remove(&super::commit::element(&op.collection, &op.rkey, &p.to_string()));
            head.records = head.records.saturating_sub(1);
        }
        match (op.cid, bytes) {
            (Some(c), Some(b)) => {
                head.hash.add(&super::commit::element(&op.collection, &op.rkey, &c.to_string()));
                head.records += 1;
                muts.push(put(state::space_record_key(did, &sid, &path), state::record_value(&c, rev.0, b)));
            }
            _ => muts.push(del(state::space_record_key(did, &sid, &path))),
        }
        muts.push(put(state::space_oplog_key(did, &sid, rev.0, idx as u16), op.encode()));
    }
    if head.rev.is_none() {
        head.created = tid::now_micros();
    }
    head.rev = Some(rev);
    for (path, cid) in batch {
        head.overlay.insert(path, (cid, applied.clone()));
    }
    let row =
        HeadRow { uri: uri.to_string(), rev, hash: head.hash.clone(), records: head.records, created: head.created };
    muts.push(put(state::space_head_key(did, &sid), row.encode()));
    let digest = head.hash.digest();
    let durable = DurableSpaceHead {
        uri: uri.clone(),
        rev,
        hash: head.hash.clone(),
        records: head.records,
        created: head.created,
        shard: crate::slots::ShardId(0),
        epoch: 0,
    };
    // A delivered rev's row goes only while its space's head is held and
    // still at that rev: this worker orders every `sP` write of the account,
    // and an unheld head may have a newer write whose ack (and outbox
    // enqueue) hasn't run yet. A row left behind costs one resend, which
    // the authority ignores as not newer.
    for (s, d) in delivered {
        if s != sid && st.repos.get(&s).is_some_and(|h| h.rev == Some(d)) {
            muts.push(del(state::space_outbox_key(did, &s)));
        }
    }
    let notify = if authority(uri) == Some(did) {
        record_self(st, did, sid, rev, digest, clock_id, &mut muts)?;
        None
    } else {
        let o = OutboxRow { uri: uri.to_string(), repo_rev: rev, hash: digest };
        muts.push(put(state::space_outbox_key(did, &sid), o.encode()));
        Some(o)
    };
    Ok(Ok(BuiltWrite { muts, rev, head: durable, notify, results }))
}

/// The author is the authority: the space host's writer state moves in the
/// write's own entry, as notifyWrite would move it. A space never created
/// (or deleted) records nothing, as the reference's notify of it fails.
fn record_self(
    st: &mut SpaceStates,
    did: &str,
    sid: SpaceId,
    rev: Tid,
    hash: [u8; 32],
    clock_id: u64,
    muts: &mut Vec<Mutation>,
) -> Result<(), SpaceError> {
    let host = st.hosts.get_mut(&sid).ok_or_else(|| internal("space host state not loaded"))?;
    if host.live().is_none() {
        return Ok(());
    }
    sequence(host, did, sid, did, rev, hash, clock_id, muts)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn sequence(
    host: &mut HostHead,
    authority: &str,
    sid: SpaceId,
    writer: &str,
    repo_rev: Tid,
    hash: [u8; 32],
    clock_id: u64,
    muts: &mut Vec<Mutation>,
) -> Result<Option<Sequenced>, SpaceError> {
    let old = *host.writers.get(writer).ok_or_else(|| internal("space writer state not loaded"))?;
    if old.is_some_and(|o| o.repo_rev >= repo_rev) {
        return Ok(None);
    }
    let prev = host.max_space_rev;
    let space_rev = tid::next_rev(prev, clock_id);
    if let Some(o) = old {
        muts.push(del(state::space_seq_key(authority, &sid, o.space_rev.0)));
    }
    muts.push(put(state::space_seq_key(authority, &sid, space_rev.0), Bytes::copy_from_slice(writer.as_bytes())));
    let row = WriterRow { repo_rev, hash, space_rev };
    muts.push(put(state::space_writer_key(authority, &sid, writer), row.encode()));
    host.writers.insert(writer.to_string(), Some(row));
    host.max_space_rev = Some(space_rev);
    Ok(Some(Sequenced { space_rev, prev }))
}

/// notifyWrite at the authority. Ok(None): not newer (nothing to write).
#[allow(clippy::too_many_arguments)]
pub fn record_writer(
    st: &mut SpaceStates,
    authority: &str,
    sid: SpaceId,
    uri: &str,
    writer: &str,
    repo_rev: Tid,
    hash: [u8; 32],
    clock_id: u64,
) -> Result<Option<(Vec<Mutation>, Sequenced)>, SpaceError> {
    let host = st.hosts.get_mut(&sid).ok_or_else(|| internal("space host state not loaded"))?;
    let space = host.live().filter(|s| s.uri == uri).ok_or(SpaceError::SpaceNotFound)?;
    let allowed = writer == authority
        || match &space.write_policy {
            Policy::Public => true,
            Policy::MemberList => host.members.get(writer).copied().flatten().is_some_and(|m| m.write),
            Policy::ManagingApp { .. } => false,
        };
    if !allowed {
        return Err(SpaceError::NotAuthorized("notifyWrite writer is not authorized".into()));
    }
    let mut muts = Vec::with_capacity(3);
    Ok(sequence(host, authority, sid, writer, repo_rev, hash, clock_id, &mut muts)?.map(|s| (muts, s)))
}

/// simplespace.createSpace: refused while a live space has the URI.
pub fn create_space(
    st: &mut SpaceStates,
    authority: &str,
    sid: SpaceId,
    row: SpaceRow,
) -> Result<Mutation, SpaceError> {
    let host = st.hosts.get_mut(&sid).ok_or_else(|| internal("space host state not loaded"))?;
    if host.live().is_some() {
        return Err(SpaceError::SpaceAlreadyExists);
    }
    let m = put(state::space_key(authority, &sid), row.encode());
    host.space = Some(row);
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    const URI: &str = "at://did:plc:auth/space/com.example.group/x";

    fn states_with(paths: &[(&str, Option<Cid>)]) -> (SpaceStates, SpaceId, Arc<str>) {
        let uri: Arc<str> = URI.into();
        let sid = state::space_id(URI);
        let mut st = SpaceStates::default();
        let mut f = Fetched::default();
        f.heads.push((sid, uri.clone(), None));
        for (p, c) in paths {
            f.paths.push((sid, p.to_string(), *c));
        }
        install(&mut st, f);
        (st, sid, uri)
    }

    fn create(rkey: &str, n: u8) -> SpaceWrite {
        let bytes = Bytes::from(vec![0xa1, 0x61, 0x61, n]);
        SpaceWrite::Create {
            collection: "com.example.post".into(),
            rkey: rkey.into(),
            cid: Cid::dag_cbor(&bytes),
            bytes,
        }
    }

    fn update(rkey: &str, n: u8, must_exist: bool) -> SpaceWrite {
        let bytes = Bytes::from(vec![0xa1, 0x61, 0x61, n]);
        SpaceWrite::Update {
            collection: "com.example.post".into(),
            rkey: rkey.into(),
            cid: Cid::dag_cbor(&bytes),
            bytes,
            must_exist,
            put: None,
        }
    }

    fn go(st: &mut SpaceStates, sid: SpaceId, uri: &Arc<str>, w: Vec<SpaceWrite>) -> Result<BuiltWrite, SpaceError> {
        let applied = Arc::new(AtomicBool::new(false));
        write(st, "did:plc:writer", sid, uri, w, 1, &applied, Vec::new()).map(|r| r.expect("something written"))
    }

    #[test]
    fn batch_validation() {
        let (mut st, sid, uri) = states_with(&[("com.example.post/a", None), ("com.example.post/b", None)]);
        // a create then an update of it in one batch: one rev, two ops
        let b = go(&mut st, sid, &uri, vec![create("a", 1), update("a", 2, true)]).unwrap();
        assert_eq!(b.head.records, 1);
        assert!(b.notify.is_some(), "another authority is notified");
        // a duplicate create in one batch, and an update of a missing record
        let e = go(&mut st, sid, &uri, vec![create("b", 1), create("b", 2)]).err().unwrap();
        assert!(matches!(e, SpaceError::RecordAlreadyExists(_)), "{e:?}");
        let e = go(&mut st, sid, &uri, vec![update("b", 1, true)]).err().unwrap();
        assert!(matches!(e, SpaceError::RecordNotFound(_)), "{e:?}");
        // a refused batch changed nothing
        assert_eq!(st.repos[&sid].records, 1);
        assert_eq!(st.repos[&sid].rev, Some(b.rev));
        // a path never read is an error, not a guess
        let e = go(&mut st, sid, &uri, vec![create("c", 1)]).err().unwrap();
        assert!(matches!(e, SpaceError::Write(WriteError::Internal(_))), "{e:?}");
        // deleteRecord of a missing record writes nothing
        let applied = Arc::new(AtomicBool::new(false));
        let w = vec![SpaceWrite::Delete { collection: "com.example.post".into(), rkey: "b".into(), must_exist: false }];
        assert!(write(&mut st, "did:plc:writer", sid, &uri, w, 1, &applied, Vec::new()).unwrap().is_err());
        // the head's hash is the set's
        let mut want = LtHash::default();
        let c = Cid::dag_cbor(&[0xa1, 0x61, 0x61, 2]);
        want.add(&super::super::commit::element("com.example.post", "a", &c.to_string()));
        assert_eq!(st.repos[&sid].hash, want);
    }

    /// A value read from `sR` before an entry applied is never used once
    /// that entry's overlay is gone: the overlay outlives every fetch.
    #[test]
    fn overlay_survives_a_fetch_racing_an_apply() {
        let (mut st, sid, uri) = states_with(&[("com.example.post/a", None)]);
        let applied = Arc::new(AtomicBool::new(false));
        let b = write(&mut st, "did:plc:writer", sid, &uri, vec![create("a", 1)], 1, &applied, Vec::new())
            .unwrap()
            .ok()
            .unwrap();
        st.clear_fetched();
        // the next request needs another path: a fetch starts while the
        // create is in flight, and reads `a` from before it applied
        let mut stale = Fetched::default();
        stale.paths.push((sid, "com.example.post/a".into(), None));
        stale.paths.push((sid, "com.example.post/z".into(), None));
        // the entry applies before the fetch result is used
        applied.store(true, Ordering::Release);
        install(&mut st, stale);
        st.prune();
        // the overlay still answers for `a`: a create of it is refused
        let e = go(&mut st, sid, &uri, vec![create("a", 2)]).err().unwrap();
        assert!(matches!(e, SpaceError::RecordAlreadyExists(_)), "{e:?}");
        st.clear_fetched();
        // with nothing pending use, the applied entry is pruned
        st.prune();
        assert!(st.repos[&sid].known("com.example.post/a").is_none());
        assert_eq!(st.repos[&sid].rev, Some(b.rev));
    }

    #[test]
    fn host_sequencing() {
        let uri: Arc<str> = URI.into();
        let sid = state::space_id(URI);
        let mut st = SpaceStates::default();
        let mut f = Fetched::default();
        f.hosts.push((sid, uri.clone(), None, None));
        f.writers.push((sid, "did:plc:w".into(), None));
        f.members.push((sid, "did:plc:w".into(), Some(MemberRow { read: true, write: true })));
        f.writers.push((sid, "did:plc:x".into(), None));
        f.members.push((sid, "did:plc:x".into(), None));
        install(&mut st, f);
        let e = record_writer(&mut st, "did:plc:auth", sid, URI, "did:plc:w", Tid(10), [1; 32], 1).err().unwrap();
        assert!(matches!(e, SpaceError::SpaceNotFound));
        create_space(&mut st, "did:plc:auth", sid, SpaceRow::defaults(URI, "t")).unwrap();
        assert!(matches!(
            create_space(&mut st, "did:plc:auth", sid, SpaceRow::defaults(URI, "t")),
            Err(SpaceError::SpaceAlreadyExists)
        ));
        let (muts, s1) =
            record_writer(&mut st, "did:plc:auth", sid, URI, "did:plc:w", Tid(10), [1; 32], 1).unwrap().unwrap();
        assert_eq!(muts.len(), 2);
        assert!(s1.prev.is_none());
        // not newer: nothing
        assert!(record_writer(&mut st, "did:plc:auth", sid, URI, "did:plc:w", Tid(10), [1; 32], 1).unwrap().is_none());
        let (muts, s2) =
            record_writer(&mut st, "did:plc:auth", sid, URI, "did:plc:w", Tid(11), [1; 32], 1).unwrap().unwrap();
        assert_eq!(muts.len(), 3, "the old sQ entry goes");
        assert!(s2.space_rev > s1.space_rev);
        assert_eq!(s2.prev, Some(s1.space_rev));
        // not a member
        let e = record_writer(&mut st, "did:plc:auth", sid, URI, "did:plc:x", Tid(10), [1; 32], 1).err().unwrap();
        assert!(matches!(e, SpaceError::NotAuthorized(_)));
    }
}
