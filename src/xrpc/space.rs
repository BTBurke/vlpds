//! `com.atproto.space.*` (`--spaces`; src/space, DESIGN.md "Spaces"): space
//! record writes through the author's repo worker, reads of space repos by
//! their owner (OAuth) or a space credential, the credential exchange, and
//! the notifyWrite outbox's sends.

use super::authn::{verify_delegation, SpaceAuth};
use super::extract::RecordBody;
use super::repo::{
    check_path, check_rkey_slur, encode_record, json_bytes, opt_bool, opt_str, req_str, take, with_status,
};
use super::*;
use crate::cbor::JsonValue;
use crate::oauth::scopes::{SpaceAccess, SpaceTarget};
use crate::space::heads::DurableSpaceHead;
use crate::space::outbox::{Outcome, Pending};
use crate::space::repo::{PutScopes, SpaceAck, SpaceError, SpaceOp, SpaceOutcome, SpaceReq, SpaceWrite, MAX_WRITES};
use crate::space::rows::{HeadRow, OpRow};
use crate::space::token::{self, TokenType};
use crate::space::Spaces;
use crate::state::SpaceId;
use crate::tid::Tid;
use base64::Engine;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/com.atproto.space.createRecord", post(create_record))
        .route("/xrpc/com.atproto.space.putRecord", post(put_record))
        .route("/xrpc/com.atproto.space.deleteRecord", post(delete_record))
        .route("/xrpc/com.atproto.space.applyWrites", post(apply_writes))
        .route("/xrpc/com.atproto.space.getRecord", get(get_record))
        .route("/xrpc/com.atproto.space.listRecords", get(list_records))
        .route("/xrpc/com.atproto.space.getLatestCommit", get(get_latest_commit))
        .route("/xrpc/com.atproto.space.listRepoOps", get(list_repo_ops))
        .route("/xrpc/com.atproto.space.getDelegationToken", get(get_delegation_token))
        .route("/xrpc/com.atproto.space.getSpaceCredential", post(get_space_credential))
}

pub(super) fn spaces(app: &App) -> XResult<&Arc<Spaces>> {
    app.spaces.as_ref().ok_or_else(|| XrpcError {
        status: StatusCode::NOT_IMPLEMENTED,
        error: "MethodNotImplemented".into(),
        message: "Method Not Implemented".into(),
    })
}

/// A `space-ref` parameter, canonical.
pub(super) struct Space {
    pub uri: String,
    pub authority: String,
    pub space_type: String,
    pub skey: String,
    pub sid: SpaceId,
}

impl Space {
    pub fn parse(s: &str) -> XResult<Space> {
        let u = super::syntax::parse_space_uri(s)
            .filter(|u| u.record.is_none())
            .ok_or_else(|| XrpcError::bad("InvalidRequest", format!("Not a space uri: {s}")))?;
        let uri = format!("at://{}/space/{}/{}", u.authority, u.space_type, u.skey);
        Ok(Space {
            sid: state::space_id(&uri),
            authority: u.authority.into(),
            space_type: u.space_type.into(),
            skey: u.skey.into(),
            uri,
        })
    }

    pub fn target(&self) -> SpaceTarget<'_> {
        SpaceTarget { space_type: &self.space_type, authority: &self.authority, skey: &self.skey }
    }

    fn record_uri(&self, author: &str, path: &str) -> String {
        format!("{}/{author}/{path}", self.uri)
    }
}

fn space_error(e: SpaceError) -> XrpcError {
    match e {
        SpaceError::Write(w) => w.into(),
        SpaceError::RecordNotFound(m) => XrpcError::bad("RecordNotFound", m),
        SpaceError::RecordAlreadyExists(m) => XrpcError::bad("RecordAlreadyExists", m),
        SpaceError::ScopeMissing(scope) => super::authn::scope_refused("oauth", &scope),
        SpaceError::SpaceNotFound => XrpcError::bad("SpaceNotFound", "Space not found"),
        SpaceError::SpaceAlreadyExists => XrpcError::bad("SpaceAlreadyExists", "Space already exists"),
        SpaceError::NotAuthorized(m) => forbidden(m),
    }
}

fn forbidden(m: impl Into<String>) -> XrpcError {
    XrpcError { status: StatusCode::FORBIDDEN, error: "Forbidden".into(), message: m.into() }
}

/// Queues `op` on `did`'s repo worker, which orders it with the account's
/// commits and status changes, and waits for its ack (durable, applied).
pub(super) async fn submit_space(app: &App, did: &str, space: &Space, op: SpaceOp) -> XResult<SpaceAck> {
    let sp = spaces(app)?.clone();
    let Ok(permit) = app.write_permits.clone().try_acquire_owned() else {
        metrics::WRITES_SHED.inc();
        return Err(XrpcError::unavailable("Overloaded", "too many writes in flight; retry with backoff"));
    };
    let (tx, rx) = oneshot::channel();
    let req = SpaceReq {
        did: did.into(),
        uri: space.uri.as_str().into(),
        sid: space.sid,
        op,
        spaces: sp,
        reply: tx,
        permit: Some(permit),
    };
    app.workers.route(did).send(WorkerMsg::Space(req)).map_err(XrpcError::from_err)?;
    rx.await.map_err(|_| XrpcError::internal("worker dropped request"))?.map_err(space_error)
}

/// Reference: `repo` must be the caller (ForbiddenError).
fn writer(creds: &Credentials, repo: &str) -> XResult<String> {
    let did = creds.user_did()?;
    if did != repo {
        return Err(forbidden("repo must match authenticated user"));
    }
    Ok(did.to_string())
}

/// Until space blobs are referenced (`sb`), a space record naming a blob
/// is refused, so no space-only upload becomes servable.
fn no_blobs(blobs: &[Cid]) -> XResult<()> {
    match blobs.is_empty() {
        true => Ok(()),
        false => Err(XrpcError::bad("InvalidRequest", "blobs in space records are not supported yet")),
    }
}

struct Prepared {
    collection: String,
    rkey: String,
    cid: Cid,
    bytes: Bytes,
    status: crate::lexicon::ValidationStatus,
}

/// Reference prepareCreate/prepareUpdate of a space record.
async fn prepare(
    app: &Arc<App>,
    collection: String,
    rkey: String,
    mut record: JsonValue<'_>,
    validate: Option<bool>,
) -> XResult<Prepared> {
    check_path(&collection, Some(&rkey))?;
    check_rkey_slur(Some(&rkey))?;
    let schema = crate::lexicon::resolve_record_schema(app, &collection, validate).await;
    let (cid, bytes, blobs, status, _) = encode_record(&mut record, &collection, &rkey, validate, schema.as_deref())?;
    no_blobs(&blobs)?;
    Ok(Prepared { collection, rkey, cid, bytes, status })
}

fn write_result(op: &str, r: &XResult<impl Sized>) {
    let result = match r {
        Ok(_) => "ok",
        Err(e) if e.status.is_server_error() => "error",
        Err(_) => "refused",
    };
    metrics::space_write(op, result);
}

async fn create_record(State(app): AppState, Auth(creds): Auth, body: RecordBody) -> XResult<Json<J>> {
    let r = create_record_inner(&app, &creds, body).await;
    write_result("createRecord", &r);
    r
}

async fn create_record_inner(app: &Arc<App>, creds: &Credentials, body: RecordBody) -> XResult<Json<J>> {
    spaces(app)?;
    let mut v = body.parse()?;
    let space = Space::parse(&req_str(&v, "space")?)?;
    let (repo, collection) = (req_str(&v, "repo")?, req_str(&v, "collection")?);
    let (rkey, validate) = (opt_str(&v, "rkey")?, opt_bool(&v, "validate")?);
    let record = take(&mut v, "record")?;
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::CREATE_POINTS)?;
    let did = writer(creds, &repo)?;
    creds.need_space(&space.target(), SpaceAccess::Write("create", &collection))?;
    check_path(&collection, rkey.as_deref())?;
    let rkey = rkey.unwrap_or_else(|| app.tids.next().to_string());
    let p = prepare(app, collection, rkey, record, validate).await?;
    let path = format!("{}/{}", p.collection, p.rkey);
    let w = SpaceWrite::Create { collection: p.collection, rkey: p.rkey, cid: p.cid, bytes: p.bytes };
    submit_space(app, &did, &space, SpaceOp::Write { writes: vec![w] }).await?;
    Ok(Json(with_status(json!({"uri": space.record_uri(&did, &path), "cid": p.cid.to_string()}), p.status)))
}

async fn put_record(State(app): AppState, Auth(creds): Auth, body: RecordBody) -> XResult<Json<J>> {
    let r = put_record_inner(&app, &creds, body).await;
    write_result("putRecord", &r);
    r
}

async fn put_record_inner(app: &Arc<App>, creds: &Credentials, body: RecordBody) -> XResult<Json<J>> {
    spaces(app)?;
    let mut v = body.parse()?;
    let space = Space::parse(&req_str(&v, "space")?)?;
    let (repo, collection, rkey) = (req_str(&v, "repo")?, req_str(&v, "collection")?, req_str(&v, "rkey")?);
    let validate = opt_bool(&v, "validate")?;
    let record = take(&mut v, "record")?;
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::UPDATE_POINTS)?;
    let did = writer(creds, &repo)?;
    // a create or an update by what the path holds: the worker asks for the
    // scope of the one it turns out to be (reference putRecord)
    let t = space.target();
    let missing = |action| {
        let a = SpaceAccess::Write(action, &collection);
        (!creds.allows_space(&t, a)).then(|| crate::oauth::scopes::SpacePermission::needed_for(&t, a))
    };
    let put = PutScopes { create: missing("create"), update: missing("update") };
    if put.create.is_some() && put.update.is_some() {
        // neither would do: the refusal names the create scope, as for a new record
        creds.need_space(&t, SpaceAccess::Write("create", &collection))?;
    }
    let p = prepare(app, collection, rkey, record, validate).await?;
    let path = format!("{}/{}", p.collection, p.rkey);
    let w = SpaceWrite::Update {
        collection: p.collection,
        rkey: p.rkey,
        cid: p.cid,
        bytes: p.bytes,
        must_exist: false,
        put: Some(put).filter(|p| p.create.is_some() || p.update.is_some()),
    };
    submit_space(app, &did, &space, SpaceOp::Write { writes: vec![w] }).await?;
    Ok(Json(with_status(json!({"uri": space.record_uri(&did, &path), "cid": p.cid.to_string()}), p.status)))
}

#[derive(Deserialize)]
struct DeleteRecordIn {
    space: String,
    repo: String,
    collection: String,
    rkey: String,
}

async fn delete_record(State(app): AppState, Auth(creds): Auth, Json(inp): Json<DeleteRecordIn>) -> XResult<Json<J>> {
    let r = delete_record_inner(&app, &creds, inp).await;
    write_result("deleteRecord", &r);
    r
}

async fn delete_record_inner(app: &App, creds: &Credentials, inp: DeleteRecordIn) -> XResult<Json<J>> {
    spaces(app)?;
    let space = Space::parse(&inp.space)?;
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::DELETE_POINTS)?;
    let did = writer(creds, &inp.repo)?;
    creds.need_space(&space.target(), SpaceAccess::Write("delete", &inp.collection))?;
    check_path(&inp.collection, Some(&inp.rkey))?;
    // idempotent, as com.atproto.repo.deleteRecord is
    let w = SpaceWrite::Delete { collection: inp.collection, rkey: inp.rkey, must_exist: false };
    submit_space(app, &did, &space, SpaceOp::Write { writes: vec![w] }).await?;
    Ok(Json(json!({})))
}

const CREATE: &str = "com.atproto.space.applyWrites#create";
const UPDATE: &str = "com.atproto.space.applyWrites#update";
const DELETE: &str = "com.atproto.space.applyWrites#delete";

async fn apply_writes(State(app): AppState, Auth(creds): Auth, body: RecordBody) -> XResult<Json<J>> {
    let r = apply_writes_inner(&app, &creds, body).await;
    write_result("applyWrites", &r);
    r
}

async fn apply_writes_inner(app: &Arc<App>, creds: &Credentials, body: RecordBody) -> XResult<Json<J>> {
    spaces(app)?;
    let mut v = body.parse()?;
    let space = Space::parse(&req_str(&v, "space")?)?;
    let repo = req_str(&v, "repo")?;
    let validate = opt_bool(&v, "validate")?;
    let mut writes = match take(&mut v, "writes")? {
        JsonValue::Array(a) => a,
        _ => return Err(super::repo::field_err("invalid type for `writes`, expected a sequence".into())),
    };
    {
        use crate::ratelimit::*;
        let points = writes
            .iter()
            .map(|w| match w.get("$type").and_then(|t| t.as_str()) {
                Some(CREATE) => CREATE_POINTS,
                Some(UPDATE) => UPDATE_POINTS,
                _ => DELETE_POINTS,
            })
            .sum();
        check_repo_write(creds.did(), points)?;
    }
    let did = writer(creds, &repo)?;
    if writes.len() > MAX_WRITES {
        return Err(XrpcError::bad("InvalidRequest", format!("Too many writes. Max: {MAX_WRITES}")));
    }
    let mut ops = Vec::with_capacity(writes.len());
    let mut statuses = Vec::with_capacity(writes.len());
    for w in writes.iter_mut() {
        let t = w.get("$type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let collection = w.get("collection").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let rkey = w.get("rkey").and_then(|v| v.as_str()).map(String::from);
        let action = match t.as_str() {
            CREATE => "create",
            UPDATE => "update",
            DELETE => "delete",
            _ => return Err(XrpcError::bad("InvalidRequest", format!("Action not supported: {t}"))),
        };
        creds.need_space(&space.target(), SpaceAccess::Write(action, &collection))?;
        check_path(&collection, rkey.as_deref())?;
        if action == "delete" {
            let rkey = rkey.ok_or_else(|| XrpcError::bad("InvalidRequest", "delete requires rkey"))?;
            statuses.push(None);
            ops.push(SpaceWrite::Delete { collection, rkey, must_exist: true });
            continue;
        }
        let rkey = match (action, rkey) {
            ("create", r) => r.unwrap_or_else(|| app.tids.next().to_string()),
            (_, Some(r)) => r,
            (_, None) => return Err(XrpcError::bad("InvalidRequest", "update requires rkey")),
        };
        let value = w.get_mut("value").map(|x| std::mem::replace(x, JsonValue::Null)).unwrap_or(JsonValue::Null);
        let p = prepare(app, collection, rkey, value, validate).await?;
        statuses.push(p.status);
        ops.push(match action {
            "create" => SpaceWrite::Create { collection: p.collection, rkey: p.rkey, cid: p.cid, bytes: p.bytes },
            _ => SpaceWrite::Update {
                collection: p.collection,
                rkey: p.rkey,
                cid: p.cid,
                bytes: p.bytes,
                must_exist: true,
                put: None,
            },
        });
    }
    let results = match submit_space(app, &did, &space, SpaceOp::Write { writes: ops }).await? {
        SpaceAck::Write { results, .. } => results,
        _ => return Err(XrpcError::internal("unexpected space ack")),
    };
    let results: Vec<J> = results
        .iter()
        .zip(statuses)
        .map(|(r, status)| match r {
            SpaceOutcome::Create { path, cid } => with_status(
                json!({"$type": "com.atproto.space.applyWrites#createResult", "uri": space.record_uri(&did, path), "cid": cid.to_string()}),
                status,
            ),
            SpaceOutcome::Update { path, cid } => with_status(
                json!({"$type": "com.atproto.space.applyWrites#updateResult", "uri": space.record_uri(&did, path), "cid": cid.to_string()}),
                status,
            ),
            SpaceOutcome::Delete | SpaceOutcome::Noop => json!({"$type": "com.atproto.space.applyWrites#deleteResult"}),
        })
        .collect();
    Ok(Json(json!({"results": results})))
}

/// Reference `assertSpaceRead`: a credential reads any member's repo it is
/// addressed to (the audience) in its own space; an account reads its own
/// repo with `read_self`. Whether a repo exists in a space the caller can't
/// read is none of its business: RepoNotFound either way. Ok(true): a
/// self-read.
pub(super) fn assert_space_read(creds: &Credentials, space: &Space, repo: &str) -> XResult<bool> {
    match creds {
        Credentials::SpaceCredential { audience, space: s, .. } => {
            assert_credential_space(audience, s, space, repo)?;
            Ok(false)
        }
        c => {
            if c.did() != Some(repo) {
                return Err(XrpcError::bad("RepoNotFound", format!("Could not find repo for DID: {repo}")));
            }
            c.need_space(&space.target(), SpaceAccess::ReadSelf)?;
            Ok(true)
        }
    }
}

/// Reference `assertCredentialSpace`: the request's audience is the repo
/// read (the authority for host methods), and the credential is this
/// space's. The audience header is signed but names no method or URL, so
/// these checks are what bind a signature to this request.
pub(super) fn assert_credential_space(audience: &str, cred_space: &str, space: &Space, target: &str) -> XResult<()> {
    if audience != target {
        return Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "BadSpaceAudience".into(),
            message: "space audience does not match the request".into(),
        });
    }
    if cred_space != space.uri {
        return Err(XrpcError::bad("InvalidCredential", "Credential is not scoped to this space"));
    }
    Ok(())
}

fn auth_label(creds: &Credentials) -> &'static str {
    match creds {
        Credentials::SpaceCredential { .. } => "credential",
        _ => "oauth",
    }
}

/// Reference `assertRepoAvailability`, from the account cache (a hot
/// repo's reads read no state): only its owner reads an inactive repo.
/// Returns the account's signing key, which signs commits per read.
async fn available(app: &App, repo: &str, self_read: bool) -> XResult<Arc<Keypair>> {
    let not_found = || XrpcError::bad("RepoNotFound", format!("Could not find repo for DID: {repo}"));
    let (key, status) = match super::proxy::account_key_status(app, repo).await {
        Ok(a) => a,
        Err(e) if e.error == "AccountNotFound" => return Err(not_found()),
        Err(e) => return Err(e),
    };
    match status.as_deref() {
        Some("deleted") => Err(not_found()),
        None => Ok(key),
        Some(_) if self_read => Ok(key),
        Some("takendown") => Err(XrpcError::bad("RepoTakendown", format!("Repo has been takendown: {repo}"))),
        Some("deactivated") => Err(XrpcError::bad("RepoDeactivated", format!("Repo has been deactivated: {repo}"))),
        Some(st) => Err(XrpcError::bad(&inactive_error(st), format!("Repo is {st}: {repo}"))),
    }
}

fn head_of(row: HeadRow, space: &Space, p: &crate::partition::Partition) -> XResult<DurableSpaceHead> {
    if row.uri != space.uri {
        return Err(XrpcError::internal(format!("space id collision: {} and {}", row.uri, space.uri)));
    }
    Ok(DurableSpaceHead {
        uri: row.uri.into(),
        rev: row.rev,
        hash: row.hash,
        records: row.records,
        created: row.created,
        shard: p.id,
        epoch: p.epoch,
    })
}

/// `repo`'s durable head in `space`: from the heads map, else read once.
/// None: the account never wrote there.
async fn load_head(
    sp: &Spaces,
    p: &crate::partition::Partition,
    repo: &str,
    space: &Space,
) -> XResult<Option<Arc<DurableSpaceHead>>> {
    if let Some(h) = sp.heads.get(repo, &space.sid, p.id, p.epoch) {
        if *h.uri != *space.uri {
            return Err(XrpcError::internal(format!("space id collision: {} and {}", h.uri, space.uri)));
        }
        return Ok(Some(h));
    }
    sp.heads.note_load();
    let Some(v) = p.db.get(state::space_head_key(repo, &space.sid)).await.map_err(XrpcError::from_err)? else {
        return Ok(None);
    };
    let h = Arc::new(head_of(HeadRow::decode(&v).map_err(XrpcError::from_err)?, space, p)?);
    sp.heads.publish(repo, &space.sid, h.clone());
    Ok(Some(h))
}

fn b64(b: &[u8]) -> J {
    json!({"$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(b)})
}

/// `buildSignedCommit`: a fresh ikm per response, signed by the author.
fn signed_commit(key: &Keypair, space: &Space, author: &str, head: &DurableSpaceHead) -> XResult<J> {
    let rev = head.rev.to_string();
    let ctx = crate::space::commit::CommitCtx { space: &space.uri, author, rev: &rev };
    let c = crate::space::commit::sign(&head.hash, &ctx, rand::random(), |b| {
        Ok::<_, std::convert::Infallible>(key.sign(b))
    })
    .map_err(|_| XrpcError::internal("space commit context too long"))?;
    Ok(
        json!({"ver": c.ver, "hash": b64(&c.hash), "ikm": b64(&c.ikm), "sig": b64(&c.sig), "mac": b64(&c.mac), "rev": c.rev}),
    )
}

#[derive(Deserialize)]
struct RecordQ {
    space: String,
    repo: String,
    collection: String,
    rkey: String,
}

async fn get_record(State(app): AppState, SpaceAuth(creds): SpaceAuth, Query(q): Query<RecordQ>) -> XResult<Response> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    check_path(&q.collection, Some(&q.rkey))?;
    available(&app, &q.repo, self_read).await?;
    metrics::space_read("getRecord", auth_label(&creds));
    let p = app.partition(&q.repo)?;
    let path = format!("{}/{}", q.collection, q.rkey);
    let uri = space.record_uri(&q.repo, &path);
    let not_found = || XrpcError::bad("RecordNotFound", format!("Could not locate record: {uri}"));
    // the head names the space: a space id shared with another fails here
    if load_head(sp, &p, &q.repo, &space).await?.is_none() {
        return Err(not_found());
    }
    let key = state::space_record_key(&q.repo, &space.sid, &path);
    let v = p.db.get(key).await.map_err(XrpcError::from_err)?.ok_or_else(not_found)?;
    let (cid, bytes) = state::record_value_parts(&v).map_err(XrpcError::from_err)?;
    let mut out = Vec::with_capacity(bytes.len() * 2 + 256);
    out.extend_from_slice(b"{\"uri\":");
    serde_json::to_writer(&mut out, &uri).map_err(XrpcError::from_err)?;
    out.extend_from_slice(b",\"cid\":\"");
    cid.write_string(&mut out);
    out.extend_from_slice(b"\",\"value\":");
    crate::cbor::write_json(bytes, &mut out).map_err(XrpcError::from_err)?;
    out.push(b'}');
    Ok(json_bytes(out))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListRecordsQ {
    space: String,
    repo: String,
    collection: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
    reverse: Option<bool>,
    exclude_values: Option<bool>,
}

/// Newest path first unless `reverse`, as the reference orders by URI; the
/// cursor is the last record's URI.
async fn list_records(
    State(app): AppState,
    SpaceAuth(creds): SpaceAuth,
    Query(q): Query<ListRecordsQ>,
) -> XResult<Response> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    if let Some(c) = &q.collection {
        check_path(c, None)?;
    }
    let limit = super::extract::limit_param(q.limit, 50, 1, 1000)?;
    available(&app, &q.repo, self_read).await?;
    metrics::space_read("listRecords", auth_label(&creds));
    let p = app.partition(&q.repo)?;
    let mut out = Vec::with_capacity(limit * 256);
    out.extend_from_slice(b"{\"records\":[");
    if load_head(sp, &p, &q.repo, &space).await?.is_none() {
        out.extend_from_slice(b"]}");
        return Ok(json_bytes(out));
    }
    let base = state::space_prefix(state::SPACE_RECORD_FAMILY, &q.repo, &space.sid);
    let prefix = match &q.collection {
        Some(c) => [&base[..], c.as_bytes(), b"/"].concat(),
        None => base.clone(),
    };
    let end = state::prefix_end(&prefix);
    let ascending = q.reverse.unwrap_or(false);
    let after = q.cursor.as_deref().and_then(|c| c.strip_prefix(&space.record_uri(&q.repo, "")));
    let (lo, hi) = match (after, ascending) {
        (Some(c), true) => ([&base[..], c.as_bytes(), &[0]].concat().max(prefix.clone()), end),
        (Some(c), false) => (prefix.clone(), [&base[..], c.as_bytes()].concat().min(end)),
        (None, _) => (prefix.clone(), end),
    };
    if lo >= hi {
        out.extend_from_slice(b"]}");
        return Ok(json_bytes(out));
    }
    let order = if ascending { slatedb::IterationOrder::Ascending } else { slatedb::IterationOrder::Descending };
    let opts = slatedb::config::ScanOptions::default().with_order(order);
    let mut iter = p.db.scan_with_options(lo..hi, &opts).await.map_err(XrpcError::from_err)?;
    let (mut n, mut last) = (0, None);
    while n < limit {
        let rows = iter.next_batch(limit - n).await.map_err(XrpcError::from_err)?;
        if rows.is_empty() {
            break;
        }
        for kv in rows {
            let path = std::str::from_utf8(&kv.key[base.len()..]).map_err(XrpcError::from_err)?;
            let (collection, rkey) = path.split_once('/').unwrap_or((path, ""));
            let (cid, bytes) = state::record_value_parts(&kv.value).map_err(XrpcError::from_err)?;
            if n > 0 {
                out.push(b',');
            }
            out.extend_from_slice(b"{\"collection\":");
            serde_json::to_writer(&mut out, collection).map_err(XrpcError::from_err)?;
            out.extend_from_slice(b",\"rkey\":");
            serde_json::to_writer(&mut out, rkey).map_err(XrpcError::from_err)?;
            out.extend_from_slice(b",\"cid\":\"");
            cid.write_string(&mut out);
            out.push(b'"');
            if !q.exclude_values.unwrap_or(false) {
                out.extend_from_slice(b",\"value\":");
                crate::cbor::write_json(bytes, &mut out).map_err(XrpcError::from_err)?;
            }
            out.push(b'}');
            n += 1;
            last = Some(path.to_string());
        }
    }
    out.push(b']');
    if let (true, Some(path)) = (n == limit, last) {
        out.extend_from_slice(b",\"cursor\":");
        serde_json::to_writer(&mut out, &space.record_uri(&q.repo, &path)).map_err(XrpcError::from_err)?;
    }
    out.push(b'}');
    Ok(json_bytes(out))
}

#[derive(Deserialize)]
struct RepoQ {
    space: String,
    repo: String,
}

async fn get_latest_commit(
    State(app): AppState,
    SpaceAuth(creds): SpaceAuth,
    Query(q): Query<RepoQ>,
) -> XResult<Json<J>> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    let key = available(&app, &q.repo, self_read).await?;
    metrics::space_read("getLatestCommit", auth_label(&creds));
    let p = app.partition(&q.repo)?;
    let head = load_head(sp, &p, &q.repo, &space)
        .await?
        .ok_or_else(|| XrpcError::bad("RepoNotFound", format!("Could not find repo for space: {}", space.uri)))?;
    Ok(Json(json!({"commit": signed_commit(&key, &space, &q.repo, &head)?})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListRepoOpsQ {
    space: String,
    repo: String,
    since: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
    exclude_values: Option<bool>,
}

/// `{rev}/{idx}`. The reference compares the rev as a string; a TID's
/// string order is its numeric order, and anything else can't be a rev.
fn parse_cursor(c: &str) -> XResult<(Tid, u16)> {
    let malformed = || XrpcError::bad("MalformedCursor", "Malformed cursor");
    let (rev, idx) = c.split_once('/').ok_or_else(malformed)?;
    Ok((Tid::parse(rev).ok_or_else(malformed)?, idx.parse().map_err(|_| malformed())?))
}

/// Ops after `since` (or the cursor), each with the record's current value
/// when it is still the op's (a superseded one is left off). A page that
/// reaches the head carries the signed commit; a full one, a cursor
/// instead. `since` at (or past) the head is answered from memory.
async fn list_repo_ops(
    State(app): AppState,
    SpaceAuth(creds): SpaceAuth,
    Query(q): Query<ListRepoOpsQ>,
) -> XResult<Json<J>> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    let since = match q.since.as_deref() {
        Some(s) => Some(Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a valid tid"))?),
        None => None,
    };
    let limit = super::extract::limit_param(q.limit, 100, 1, 1000)?;
    let cursor = q.cursor.as_deref().map(parse_cursor).transpose()?;
    let key = available(&app, &q.repo, self_read).await?;
    metrics::space_read("listRepoOps", auth_label(&creds));
    let start = Instant::now();
    let p = app.partition(&q.repo)?;
    let head = load_head(sp, &p, &q.repo, &space).await?;
    let caught_up = match (&head, cursor, since) {
        (None, ..) => true,
        (Some(h), None, Some(s)) => s >= h.rev,
        _ => false,
    };
    if caught_up {
        let mut out = json!({"ops": []});
        if let Some(h) = &head {
            out["commit"] = signed_commit(&key, &space, &q.repo, h)?;
        }
        metrics::space_list_repo_ops("noop", start.elapsed());
        return Ok(Json(out));
    }
    // one snapshot: the commit describes exactly the ops' end state
    let snap = p.db.snapshot().await.map_err(XrpcError::from_err)?;
    let head = match snap.get(state::space_head_key(&q.repo, &space.sid)).await.map_err(XrpcError::from_err)? {
        Some(v) => head_of(HeadRow::decode(&v).map_err(XrpcError::from_err)?, &space, &p)?,
        None => return Ok(Json(json!({"ops": []}))),
    };
    let prefix = state::space_prefix(state::SPACE_OPLOG_FAMILY, &q.repo, &space.sid);
    let after = |rev: u64, idx: u32| -> Vec<u8> {
        let (rev, idx) = match u16::try_from(idx) {
            Ok(i) => (rev, i),
            Err(_) => (rev.saturating_add(1), 0),
        };
        [&prefix[..], &rev.to_be_bytes(), &idx.to_be_bytes()].concat()
    };
    let mut lo = prefix.clone();
    if let Some(s) = since {
        lo = lo.max(after(s.0.saturating_add(1), 0));
    }
    if let Some((rev, idx)) = cursor {
        lo = lo.max(after(rev.0, idx as u32 + 1));
    }
    let opts = slatedb::config::ScanOptions::default();
    let mut iter = snap.scan_with_options(lo..state::prefix_end(&prefix), &opts).await.map_err(XrpcError::from_err)?;
    let values = !q.exclude_values.unwrap_or(false);
    let mut ops = Vec::with_capacity(limit.min(256));
    let mut last = None;
    while ops.len() < limit {
        let rows = iter.next_batch(limit - ops.len()).await.map_err(XrpcError::from_err)?;
        if rows.is_empty() {
            break;
        }
        for kv in rows {
            let (rev, idx) = crate::space::rows::oplog_position(&kv.key)
                .ok_or_else(|| XrpcError::internal("malformed space oplog key"))?;
            let op = OpRow::decode(&kv.value).map_err(XrpcError::from_err)?;
            let mut o = json!({
                "rev": rev.to_string(),
                "collection": op.collection,
                "rkey": op.rkey,
                "cid": op.cid.map(|c| c.to_string()),
                "prev": op.prev.map(|c| c.to_string()),
            });
            if let (true, Some(cid)) = (values, op.cid) {
                let path = format!("{}/{}", op.collection, op.rkey);
                let cur =
                    snap.get(state::space_record_key(&q.repo, &space.sid, &path)).await.map_err(XrpcError::from_err)?;
                if let Some(v) = cur {
                    let (c, bytes) = state::record_value_parts(&v).map_err(XrpcError::from_err)?;
                    if c == cid {
                        let mut j = Vec::with_capacity(bytes.len() * 2);
                        crate::cbor::write_json(bytes, &mut j).map_err(XrpcError::from_err)?;
                        o["value"] = serde_json::from_slice(&j).map_err(XrpcError::from_err)?;
                    }
                }
            }
            ops.push(o);
            last = Some((rev, idx));
        }
    }
    let mut out = json!({});
    if ops.len() < limit {
        out["commit"] = signed_commit(&key, &space, &q.repo, &head)?;
    } else if let Some((rev, idx)) = last {
        out["cursor"] = json!(format!("{rev}/{idx}"));
    }
    out["ops"] = J::Array(ops);
    metrics::space_list_repo_ops("scan", start.elapsed());
    Ok(Json(out))
}

#[derive(Deserialize)]
struct SpaceQ {
    space: String,
}

/// Reference getDelegationToken: only a whole-space `read` grant mints one
/// (and so only OAuth: space data is OAuth-only here).
async fn get_delegation_token(State(app): AppState, Auth(creds): Auth, Query(q): Query<SpaceQ>) -> XResult<Json<J>> {
    spaces(&app)?;
    let space = Space::parse(&q.space)?;
    creds.need_space(&space.target(), SpaceAccess::Read)?;
    let did = creds.user_did()?.to_string();
    let (key, status) = super::proxy::account_key_status(&app, &did).await?;
    if let Some(st) = status {
        return Err(inactive_account_error(&st));
    }
    let aud = token::space_host_aud(&space.authority);
    let mint = token::Mint { iss: &did, sub: &space.uri, aud: Some(&aud), ..Default::default() };
    let now = crate::tid::now_micros() as i64 / 1_000_000;
    let tok = token::encode(TokenType::Delegation, &mint, "ES256K", now, &token::new_jti(), |b| {
        Ok::<_, std::convert::Infallible>(key.sign(b))
    })
    .map_err(|e| XrpcError::internal(format!("delegation token: {e:?}")))?;
    metrics::space_delegation();
    Ok(Json(json!({"token": tok})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialIn {
    space: String,
    client_attestation: Option<String>,
}

/// At the space's authority (forwarded there by `space`): the delegation
/// token checked and claimed, then the simplespace policy. The credential
/// is bound to the key that signed this request.
async fn get_space_credential(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<CredentialIn>,
) -> XResult<Json<J>> {
    spaces(&app)?;
    let r = issue_credential(&app, &headers, inp).await;
    metrics::space_credential_issued(match &r {
        Ok(_) => "ok",
        Err(e) if e.status.is_server_error() => "error",
        Err(e) if e.status == StatusCode::UNAUTHORIZED => "bad_token",
        Err(_) => "refused",
    });
    r
}

async fn issue_credential(app: &App, headers: &HeaderMap, inp: CredentialIn) -> XResult<Json<J>> {
    let d = verify_delegation(app, headers).await?;
    if d.space != inp.space {
        return Err(XrpcError::bad(
            "InvalidDelegationToken",
            "Delegation token subject does not match requested space",
        ));
    }
    let space = Space::parse(&inp.space)?;
    let row = super::simplespace::space_row(app, &space).await?;
    if row.deleted_at.is_some() {
        return Err(XrpcError::bad("SpaceDeleted", "Space has been deleted"));
    }
    // the app perimeter first: a refused app is never disclosed further
    if let crate::space::rows::AppAccess::AllowList { .. } = row.app_access {
        // client attestations come with the space host (until then, none is
        // accepted)
        let _ = inp.client_attestation;
        return Err(XrpcError::bad("AppNotAuthorized", "Application not authorized for this space"));
    }
    if !super::simplespace::may_read(app, &space, &row, &d.user).await? {
        return Err(XrpcError::bad("UserNotAuthorized", "User not authorized for this space"));
    }
    let (key, _) = super::proxy::account_key_status(app, &space.authority).await?;
    let mint = token::Mint { iss: &space.authority, sub: &space.uri, key_id: Some(&d.key_id), ..Default::default() };
    let now = crate::tid::now_micros() as i64 / 1_000_000;
    let cred = token::encode(TokenType::Credential, &mint, "ES256K", now, &token::new_jti(), |b| {
        Ok::<_, std::convert::Infallible>(key.sign(b))
    })
    .map_err(|e| XrpcError::internal(format!("space credential: {e:?}")))?;
    Ok(Json(json!({"credential": cred})))
}

/// Whether `outcome` is worth another try: the reference retries network
/// failures and these statuses (`@atproto/lex` RETRYABLE_HTTP_STATUS_CODES).
fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504 | 522 | 524)
}

const NOTIFY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// One outbox send of `p`: a writer's newest durable rev to its space's
/// authority. A local authority records it on its own worker; any other
/// gets notifyWrite at its space host with the writer's service auth.
pub async fn deliver(app: &App, p: &Pending) -> Outcome {
    // the shard may have moved since the row was taken
    if app.partition(&p.did).is_err() || app.remote_owner(&p.did).is_some() {
        return Outcome::Gone;
    }
    let (key, status) = match super::proxy::account_key_status(app, &p.did).await {
        Ok(a) => a,
        Err(e) if e.error == "AccountNotFound" => return Outcome::Gone,
        Err(e) => return Outcome::Retry(format!("{}: {}", e.error, e.message)),
    };
    match status.as_deref() {
        Some("deleted") => return Outcome::Gone,
        Some(_) => return Outcome::Wait,
        None => {}
    }
    let Ok(space) = Space::parse(&p.uri) else { return Outcome::Refused("not a space uri".into()) };
    let local = app.partition(&space.authority).is_ok()
        && app.remote_owner(&space.authority).is_none()
        && super::server::account_if_exists(app, &space.authority).await.is_ok_and(|a| a.is_some());
    if local {
        let op = SpaceOp::RecordWriter { writer: p.did.to_string(), repo_rev: p.repo_rev, hash: p.hash };
        return match submit_space(app, &space.authority, &space, op).await {
            Ok(_) => Outcome::Delivered,
            Err(e) if e.status.is_server_error() => Outcome::Retry(format!("{}: {}", e.error, e.message)),
            Err(e) => Outcome::Refused(format!("{}: {}", e.error, e.message)),
        };
    }
    let endpoint = match app.did_resolver.resolve(&space.authority).await {
        Ok(doc) => crate::did_resolver::service_endpoint(&doc, "atproto_space_host")
            .or_else(|| crate::did_resolver::service_endpoint(&doc, "atproto_pds")),
        Err(e) => return Outcome::Retry(format!("could not resolve {}: {e:?}", space.authority)),
    };
    let Some(endpoint) = endpoint else { return Outcome::Retry(format!("{} names no space host", space.authority)) };
    let aud = token::space_host_aud(&space.authority);
    let lxm = "com.atproto.space.notifyWrite";
    let jwt = match crate::auth::service_auth_jwt(&key, &p.did, &aud, Some(lxm), 60) {
        Ok(j) => j,
        Err(e) => return Outcome::Retry(format!("service auth: {e}")),
    };
    let body = json!({
        "space": space.uri,
        "repo": &*p.did,
        "repoRev": p.repo_rev.to_string(),
        "hash": b64(&p.hash),
    });
    let url = format!("{}/xrpc/{lxm}", endpoint.trim_end_matches('/'));
    let req = match crate::http::guarded(app.config.dev_mode).request(reqwest::Method::POST, &url) {
        Ok(r) => r,
        Err(e) => return Outcome::Refused(format!("space host {url}: {e}")),
    };
    let sent = req.bearer_auth(jwt).timeout(NOTIFY_TIMEOUT).json(&body).send().await;
    match sent {
        Ok(r) if r.status().is_success() => Outcome::Delivered,
        Ok(r) if retryable_status(r.status().as_u16()) => Outcome::Retry(format!("{} from {url}", r.status())),
        Ok(r) => Outcome::Refused(format!("{} from {url}", r.status())),
        Err(e) => Outcome::Retry(format!("{url}: {e}")),
    }
}
