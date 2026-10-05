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
        .route("/xrpc/com.atproto.space.getBlob", get(get_blob))
        .route("/xrpc/com.atproto.space.listBlobs", get(list_blobs))
        .route("/xrpc/com.atproto.space.getLatestCommit", get(get_latest_commit))
        .route("/xrpc/com.atproto.space.listRepoOps", get(list_repo_ops))
        .route("/xrpc/com.atproto.space.getDelegationToken", get(get_delegation_token))
        .route("/xrpc/com.atproto.space.getSpaceCredential", post(get_space_credential))
        .route("/xrpc/com.atproto.space.listSpaces", get(list_spaces))
        .route("/xrpc/com.atproto.space.notifyCredentialRevoked", post(notify_credential_revoked))
        .route("/xrpc/com.atproto.space.getRepo", get(get_repo))
        .route("/xrpc/com.atproto.space.notifyWrite", post(notify_write))
        .route("/xrpc/com.atproto.space.listRepos", get(list_repos))
        .route("/xrpc/com.atproto.space.registerNotify", post(register_notify))
        .route("/xrpc/com.atproto.space.unregisterNotify", post(unregister_notify))
}

pub fn internal_routes() -> Router<Arc<App>> {
    Router::new()
        .route("/internal/v1/space/revocations/reload", post(internal_reload_revocations))
        .route("/internal/v1/space/notify", post(internal_notify))
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
        SpaceError::SpaceDeleted => XrpcError::bad("SpaceDeleted", "Space has been deleted"),
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

struct Prepared {
    collection: String,
    rkey: String,
    cid: Cid,
    bytes: Bytes,
    blobs: Vec<Cid>,
    decls: Vec<super::repo::BlobDecl>,
    status: crate::lexicon::ValidationStatus,
}

/// Reference prepareCreate/prepareUpdate of a space record. Its blobs are
/// checked as a repo write's are (`check_blobs`), by the caller.
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
    let (cid, bytes, blobs, status, decls) =
        encode_record(&mut record, &collection, &rkey, validate, schema.as_deref())?;
    Ok(Prepared { collection, rkey, cid, bytes, blobs, decls, status })
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
    let _held = super::repo::check_blobs(app, &did, &p.decls).await?;
    let path = format!("{}/{}", p.collection, p.rkey);
    let w = SpaceWrite::Create { collection: p.collection, rkey: p.rkey, cid: p.cid, bytes: p.bytes, blobs: p.blobs };
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
    let _held = super::repo::check_blobs(app, &did, &p.decls).await?;
    let path = format!("{}/{}", p.collection, p.rkey);
    let w = SpaceWrite::Update {
        collection: p.collection,
        rkey: p.rkey,
        cid: p.cid,
        bytes: p.bytes,
        blobs: p.blobs,
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
    let mut decls = Vec::new();
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
        decls.extend(p.decls);
        ops.push(match action {
            "create" => SpaceWrite::Create {
                collection: p.collection,
                rkey: p.rkey,
                cid: p.cid,
                bytes: p.bytes,
                blobs: p.blobs,
            },
            _ => SpaceWrite::Update {
                collection: p.collection,
                rkey: p.rkey,
                cid: p.cid,
                bytes: p.bytes,
                blobs: p.blobs,
                must_exist: true,
                put: None,
            },
        });
    }
    let _held = super::repo::check_blobs(app, &did, &decls).await?;
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
        metrics::space_credential_check("audience");
        return Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "BadSpaceAudience".into(),
            message: "space audience does not match the request".into(),
        });
    }
    if cred_space != space.uri {
        metrics::space_credential_check("space");
        return Err(XrpcError::bad("InvalidCredential", "Credential is not scoped to this space"));
    }
    metrics::space_credential_check("ok");
    Ok(())
}

/// A space record's takedown, beside the account's `rec/` ones
/// (`sec/td/space/{sid}/{collection}/{rkey}`). The space's id rather than
/// its URI keeps the name short; the record's author is the account.
pub(super) fn takedown_name(sid: &SpaceId, path: &str) -> String {
    format!("space/{}/{path}", hex::encode(sid))
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
    if super::server::ctl(&app, &q.repo).await?.has_takedown(&takedown_name(&space.sid, &path)) {
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
    let takedowns = super::server::ctl(&app, &q.repo).await?;
    let mut iter = p.db.scan_with_options(lo..hi, &opts).await.map_err(XrpcError::from_err)?;
    let (mut n, mut last) = (0, None);
    while n < limit {
        let rows = iter.next_batch(limit - n).await.map_err(XrpcError::from_err)?;
        if rows.is_empty() {
            break;
        }
        for kv in rows {
            let path = std::str::from_utf8(&kv.key[base.len()..]).map_err(XrpcError::from_err)?;
            if takedowns.has_takedown(&takedown_name(&space.sid, path)) {
                last = Some(path.to_string());
                continue;
            }
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
struct BlobQ {
    space: String,
    repo: String,
    cid: String,
}

/// Reference space.getBlob: only a blob a record of this repo in this space
/// names (`sb`), so a credential for one space reads none of another's, nor
/// an upload no record names; whether such a blob exists isn't revealed.
async fn get_blob(State(app): AppState, SpaceAuth(creds): SpaceAuth, Query(q): Query<BlobQ>) -> XResult<Response> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    let cid = Cid::parse(&q.cid).map_err(|_| XrpcError::bad("InvalidRequest", "Invalid cid"))?;
    available(&app, &q.repo, self_read).await?;
    metrics::space_read("getBlob", auth_label(&creds));
    let not_found = || XrpcError::bad("BlobNotFound", "Blob not found");
    let p = app.partition(&q.repo)?;
    // the head names the space: a space id shared with another fails here
    if load_head(sp, &p, &q.repo, &space).await?.is_none() {
        return Err(not_found());
    }
    let prefix = state::space_blob_prefix(&q.repo, &space.sid, &cid);
    let mut iter = p.db.scan(prefix.clone()..state::prefix_end(&prefix)).await.map_err(XrpcError::from_err)?;
    if iter.next().await.map_err(XrpcError::from_err)?.is_none() {
        return Err(not_found());
    }
    if super::admin::is_blob_takendown(&app, &q.repo, &q.cid).await? {
        return Err(not_found());
    }
    match app.store.raw.get(&super::blobs::blob_path(&app, &q.repo, cid)).await {
        Ok(r) => Ok(super::blobs::blob_response(r, &cid)),
        Err(object_store::Error::NotFound { .. }) => Err(not_found()),
        Err(e) => Err(XrpcError::from_err(e)),
    }
}

#[derive(Deserialize)]
struct ListBlobsQ {
    space: String,
    repo: String,
    since: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// Reference space.listBlobs: the distinct blobs this repo's records in
/// this space name, in CID order; with `since`, those named by a record
/// written after that rev. Only a full page has a cursor.
async fn list_blobs(
    State(app): AppState,
    SpaceAuth(creds): SpaceAuth,
    Query(q): Query<ListBlobsQ>,
) -> XResult<Json<J>> {
    let sp = spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    let since = match q.since.as_deref() {
        Some(s) => Some(Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a TID"))?),
        None => None,
    };
    let limit = super::extract::limit_param(q.limit, 500, 1, 1000)?;
    available(&app, &q.repo, self_read).await?;
    metrics::space_read("listBlobs", auth_label(&creds));
    let p = app.partition(&q.repo)?;
    if load_head(sp, &p, &q.repo, &space).await?.is_none() {
        return Ok(Json(json!({"cids": []})));
    }
    let prefix = state::space_prefix(state::SPACE_BLOB_FAMILY, &q.repo, &space.sid);
    let lo = match &q.cursor {
        // past every key of the cursor's CID
        Some(c) => state::prefix_end(&[&prefix[..], c.as_bytes(), b"\0"].concat()),
        None => prefix.clone(),
    };
    let opts = slatedb::config::ScanOptions::default();
    let mut iter = p.db.scan_with_options(lo..state::prefix_end(&prefix), &opts).await.map_err(XrpcError::from_err)?;
    let mut cids: Vec<String> = Vec::new();
    'scan: loop {
        let rows = iter.next_batch(limit.max(64)).await.map_err(XrpcError::from_err)?;
        if rows.is_empty() {
            break;
        }
        for kv in rows {
            let (cid, _) = crate::space::rows::blob_ref_parts(&kv.key[prefix.len()..])
                .ok_or_else(|| XrpcError::internal("bad space blob ref key"))?;
            if cids.last().is_some_and(|c| c == cid) {
                continue;
            }
            if since.is_some_and(|s| crate::space::rows::blob_ref_rev(&kv.value) <= s) {
                continue;
            }
            if cids.len() == limit {
                break 'scan;
            }
            cids.push(cid.to_string());
        }
    }
    let mut out = json!({"cids": cids});
    if cids.len() == limit {
        out["cursor"] = json!(cids.last());
    }
    Ok(Json(out))
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
    let takedowns = match values {
        true => Some(super::server::ctl(&app, &q.repo).await?),
        false => None,
    };
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
            if let (Some(td), Some(cid)) = (&takedowns, op.cid) {
                let path = format!("{}/{}", op.collection, op.rkey);
                let hidden = td.has_takedown(&takedown_name(&space.sid, &path));
                let cur = match hidden {
                    true => None,
                    false => snap
                        .get(state::space_record_key(&q.repo, &space.sid, &path))
                        .await
                        .map_err(XrpcError::from_err)?,
                };
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
    super::simplespace::assert_space_host(app, &space).await?;
    let client_id = match &inp.client_attestation {
        Some(a) => {
            let aud = token::space_host_aud(&space.authority);
            Some(crate::space::attestation::verify(app, a, &aud, &space.authority).await?)
        }
        None => None,
    };
    let row = super::simplespace::space_row(app, &space).await?;
    if !row.live() {
        // the durable signal that a space is gone, for a syncer that missed
        // notifySpaceDeleted
        return Err(XrpcError::bad("SpaceDeleted", "Space has been deleted"));
    }
    // the app perimeter first: decided from the config alone, so a refused
    // app is never disclosed to a managing app
    if let crate::space::rows::AppAccess::AllowList { allowed } = &row.app_access {
        if !client_id.as_ref().is_some_and(|c| allowed.contains(c)) {
            return Err(XrpcError::bad("AppNotAuthorized", "Application not authorized for this space"));
        }
    }
    if !super::simplespace::authorize_user(app, &space, &row, &d.user, "read", client_id.as_deref()).await? {
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListSpacesQ {
    space_type: Option<String>,
    did: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// Reference listSpaces: the spaces the account holds a repo in, plus
/// those it governs (the reference's `ensureSpace` at createSpace), by
/// URI. The filters are the scope target, so an unfiltered listing needs
/// a wildcard grant.
async fn list_spaces(State(app): AppState, Auth(creds): Auth, Query(q): Query<ListSpacesQ>) -> XResult<Json<J>> {
    spaces(&app)?;
    if q.space_type.as_deref().is_some_and(|t| !super::syntax::valid_nsid(t)) {
        return Err(XrpcError::bad("InvalidRequest", "spaceType must be an NSID"));
    }
    if q.did.as_deref().is_some_and(|d| !super::syntax::valid_did(d)) {
        return Err(XrpcError::bad("InvalidRequest", "did must be a DID"));
    }
    let limit = super::extract::limit_param(q.limit, 50, 1, 100)?;
    let target = SpaceTarget {
        space_type: q.space_type.as_deref().unwrap_or("*"),
        authority: q.did.as_deref().unwrap_or("*"),
        skey: "*",
    };
    creds.need_space(&target, SpaceAccess::ReadSelf)?;
    let did = creds.user_did()?.to_string();
    let p = app.partition(&did)?;
    let mut uris = std::collections::BTreeSet::new();
    for fam in [state::SPACE_HEAD_FAMILY, state::SPACE_FAMILY] {
        let prefix = state::space_did_prefix(fam, &did);
        let opts = slatedb::config::ScanOptions::default();
        let mut iter =
            p.db.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts)
                .await
                .map_err(XrpcError::from_err)?;
        while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
            let uri = match fam == state::SPACE_HEAD_FAMILY {
                true => HeadRow::decode(&kv.value).map_err(XrpcError::from_err)?.uri,
                false => match crate::space::rows::SpaceRow::decode(&kv.value).map_err(XrpcError::from_err)? {
                    row if row.deleted_at.is_none() => row.uri,
                    _ => continue,
                },
            };
            let Some(u) = super::syntax::parse_space_uri(&uri) else { continue };
            let keep = q.space_type.as_deref().is_none_or(|t| t == u.space_type)
                && q.did.as_deref().is_none_or(|d| d == u.authority)
                && q.cursor.as_deref().is_none_or(|c| uri.as_str() > c);
            if keep {
                uris.insert(uri);
            }
        }
    }
    let page: Vec<String> = uris.into_iter().take(limit).collect();
    let mut out = json!({"spaces": page.iter().map(|u| json!({"uri": u})).collect::<Vec<_>>()});
    if page.len() == limit {
        out["cursor"] = json!(page.last());
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct RevokedIn {
    space: String,
    credentials: Vec<String>,
}

const REVOKE_NUDGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Reference notifyCredentialRevoked: the space's authority, by service
/// auth addressed to an account hosted here, revokes credentials of its
/// space. Enforced cluster-wide: the 200 comes once the revocation is in
/// the bucket's control object and the live peers were asked to reload it
/// (each given a second to answer; one that misses it re-reads it within
/// minutes).
async fn notify_credential_revoked(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<RevokedIn>,
) -> XResult<StatusCode> {
    let sp = spaces(&app)?.clone();
    let lxm = "com.atproto.space.notifyCredentialRevoked";
    let auth = super::authn::verify_space_service_jwt(&app, &headers, lxm).await?;
    let space = Space::parse(&inp.space)?;
    if inp.credentials.is_empty() || inp.credentials.len() > 100 || inp.credentials.iter().any(String::is_empty) {
        return Err(XrpcError::bad("InvalidRequest", "credentials must hold 1 to 100 non-empty jtis"));
    }
    if auth.iss != space.authority {
        return Err(forbidden("Revocation issuer is not the space authority"));
    }
    let hosted = super::syntax::valid_did(&auth.aud)
        && match super::internal::account_anywhere(&app, &auth.aud).await {
            Ok(_) => true,
            Err(e) if e.error == "AccountNotFound" => false,
            Err(e) => return Err(e),
        };
    if !hosted {
        return Err(forbidden("Revocation audience does not match a repo hosted here"));
    }
    sp.revoke(&app.store, &space.uri, &inp.credentials)
        .await
        .map_err(|e| XrpcError::unavailable("Unavailable", format!("revocation not stored: {e:#}")))?;
    nudge_revocation_peers(&app).await;
    Ok(StatusCode::OK)
}

async fn nudge_revocation_peers(app: &Arc<App>) {
    let Some(c) = &app.cluster else { return };
    let me = c.cfg.node_id.clone();
    let sends = c.peers().into_iter().filter(|l| l.node_id != me).map(|l| {
        let app = app.clone();
        async move {
            let r = app
                .http
                .post(format!("{}/internal/v1/space/revocations/reload", l.addr.trim_end_matches('/')))
                .header(super::internal::HDR, &app.config.internal_token)
                .timeout(REVOKE_NUDGE_TIMEOUT)
                .send()
                .await
                .and_then(|r| r.error_for_status());
            if let Err(e) = r {
                tracing::warn!(peer = %l.node_id, "space revocation nudge failed (it re-reads on its own): {e}");
            }
        }
    });
    futures::future::join_all(sends).await;
}

async fn internal_reload_revocations(State(app): AppState, headers: HeaderMap) -> XResult<Json<J>> {
    super::internal::check(&app, &headers)?;
    let sp = spaces(&app)?;
    sp.refresh_revocations(&app.store)
        .await
        .map_err(|e| XrpcError::unavailable("Unavailable", format!("revocations unreadable: {e:#}")))?;
    Ok(Json(json!({"revocations": sp.revocations.len()})))
}

const SWEEP_BATCH: usize = 500;

/// Deletes every Spaces row of `did` (its repos in spaces, the spaces it
/// governs and their host state), in bounded entries, whether or not
/// `--spaces` is on now. Rerun by a deletion that stopped part way.
pub(super) async fn delete_account_rows(app: &App, did: &str) -> XResult<()> {
    let p = app.partition(did)?;
    for fam in state::SPACE_FAMILIES {
        let prefix = state::space_did_prefix(fam, did);
        let end = state::prefix_end(&prefix);
        loop {
            let opts = slatedb::config::ScanOptions::default();
            let mut iter =
                p.db.scan_with_options(prefix.clone()..end.clone(), &opts).await.map_err(XrpcError::from_err)?;
            let rows = iter.next_batch(SWEEP_BATCH).await.map_err(XrpcError::from_err)?;
            if rows.is_empty() {
                break;
            }
            let muts = rows.into_iter().map(|kv| crate::segment::Mutation { key: kv.key, val: None }).collect();
            super::write_private_local(&p, muts).await?;
        }
    }
    if let Some(sp) = &app.spaces {
        sp.forget_account(did);
    }
    Ok(())
}

/// Whether `outcome` is worth another try: the reference retries network
/// failures and these statuses (`@atproto/lex` RETRYABLE_HTTP_STATUS_CODES).
pub(crate) fn retryable_status(status: u16) -> bool {
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
    let outcome = |r: XResult<()>| match r {
        Ok(()) => Outcome::Delivered,
        Err(e) if e.status.is_server_error() => Outcome::Retry(format!("{}: {}", e.error, e.message)),
        Err(e) => Outcome::Refused(format!("{}: {}", e.error, e.message)),
    };
    // an authority hosted by this cluster is told without HTTP or service
    // auth: here, or at its shard's owner
    if let Some(owner) = app.remote_owner(&space.authority) {
        match notify_owner(app, &owner, &space, p).await {
            Ok(Some(r)) => return outcome(r),
            Ok(None) => {}
            Err(e) => return Outcome::Retry(format!("{}: {}", e.error, e.message)),
        }
    } else if app.partition(&space.authority).is_ok()
        && super::server::account_if_exists(app, &space.authority).await.is_ok_and(|a| a.is_some())
    {
        let r = process_notify_write(app, &space, &p.did, p.repo_rev, p.hash).await;
        metrics::space_notify("in", notify_in_result(&r));
        return outcome(r.map(|_| ()));
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

#[derive(serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InternalNotify {
    space: String,
    repo: String,
    repo_rev: String,
    hash: String,
}

/// An outbox send to the owner of the authority's shard, another node of
/// this cluster. Ok(None): the authority isn't an account of the cluster;
/// Ok(Some(the authority's answer)); Err: the owner couldn't be asked.
async fn notify_owner(app: &App, owner: &str, space: &Space, p: &Pending) -> XResult<Option<XResult<()>>> {
    let body = InternalNotify {
        space: space.uri.clone(),
        repo: p.did.to_string(),
        repo_rev: p.repo_rev.to_string(),
        hash: base64::engine::general_purpose::STANDARD_NO_PAD.encode(p.hash),
    };
    let r = app
        .http
        .post(format!("{}/internal/v1/space/notify", owner.trim_end_matches('/')))
        .header(super::internal::HDR, &app.config.internal_token)
        .timeout(NOTIFY_TIMEOUT)
        .json(&body)
        .send()
        .await
        .map_err(|e| XrpcError::unavailable("PartitionUnavailable", format!("partition owner: {e}")))?;
    let status = r.status();
    let v: J = r.json().await.unwrap_or_default();
    if status.is_success() {
        return Ok(v["hosted"].as_bool().unwrap_or(false).then_some(Ok(())));
    }
    let e = XrpcError {
        status,
        error: v["error"].as_str().unwrap_or("InternalServerError").into(),
        message: v["message"].as_str().unwrap_or_default().into(),
    };
    match status.is_server_error() {
        true => Err(e),
        false => Ok(Some(Err(e))),
    }
}

/// [`notify_owner`] at the owner: notifyWrite from a writer on another node
/// of the cluster, trusted as the cluster's own outbox (no service auth).
async fn internal_notify(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<InternalNotify>,
) -> XResult<Json<J>> {
    super::internal::check(&app, &headers)?;
    let sp = spaces(&app)?;
    sp.peer_notifies.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let space = Space::parse(&inp.space)?;
    let bad = |m: &str| XrpcError::bad("InvalidRequest", m.to_string());
    let repo_rev = Tid::parse(&inp.repo_rev).ok_or_else(|| bad("repoRev must be a valid TID"))?;
    let hash: [u8; 32] = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(&inp.hash)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| bad("hash must be 32 bytes"))?;
    if app.remote_owner(&space.authority).is_some() {
        return Err(XrpcError::unavailable(crate::forward::SHARD_MOVED, "the authority's shard moved"));
    }
    app.partition(&space.authority)?;
    if super::server::account_if_exists(&app, &space.authority).await?.is_none() {
        return Ok(Json(json!({"hosted": false})));
    }
    let r = process_notify_write(&app, &space, &inp.repo, repo_rev, hash).await;
    metrics::space_notify("in", notify_in_result(&r));
    r?;
    Ok(Json(json!({"hosted": true})))
}

/// Deletes `service`'s registration for the space `uri` at its authority
/// (whose shard is this node's), if it's still expired: renewed since, it
/// stays.
pub async fn prune_registration(app: &App, uri: &str, service: &str) -> anyhow::Result<()> {
    let space = Space::parse(uri).map_err(|e| anyhow::anyhow!("{}", e.message))?;
    let p = app.partition(&space.authority).map_err(|e| anyhow::anyhow!("{}", e.message))?;
    let Some(v) = p.db.get(state::space_notify_key(&space.authority, &space.sid, service)).await? else {
        return Ok(());
    };
    if crate::space::rows::NotifyRow::decode(&v)?.expires > crate::tid::now_micros() {
        return Ok(());
    }
    let op = SpaceOp::UnregisterNotify { service: service.to_string() };
    submit_space(app, &space.authority, &space, op).await.map_err(|e| anyhow::anyhow!("{}", e.message))?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetRepoQ {
    space: String,
    repo: String,
    exclude_values: Option<bool>,
}

/// A signed commit as a CAR block (dag-cbor; the reference's `SignedCommit`
/// with byte fields as bytes).
fn commit_block(c: &crate::space::commit::SignedCommit) -> Vec<u8> {
    let mut b = Vec::with_capacity(256);
    crate::cbor::write_map_head(&mut b, 6);
    // canonical key order: by length, then bytes
    for (k, v) in [("ikm", &c.ikm), ("mac", &c.mac)] {
        crate::cbor::write_text(&mut b, k);
        crate::cbor::write_bytes(&mut b, v);
    }
    crate::cbor::write_text(&mut b, "rev");
    crate::cbor::write_text(&mut b, &c.rev);
    crate::cbor::write_text(&mut b, "sig");
    crate::cbor::write_bytes(&mut b, &c.sig);
    crate::cbor::write_text(&mut b, "ver");
    crate::cbor::write_int(&mut b, c.ver);
    crate::cbor::write_text(&mut b, "hash");
    crate::cbor::write_bytes(&mut b, &c.hash);
    b
}

/// Reference getRepo (`serializeRepo`), streamed in two passes over one
/// snapshot (src/space/car.rs) under an export slot (`--max-exports`), and
/// ended for a client that reads nothing for `--export-stall-secs`. Pass 1
/// holds the paths and CIDs only: at most `--space-repo-max-records` of
/// them. A record taken down keeps its index entry (the commit's hash
/// covers it) but not its block; with `excludeValues` only the roots go.
async fn get_repo(State(app): AppState, SpaceAuth(creds): SpaceAuth, Query(q): Query<GetRepoQ>) -> XResult<Response> {
    use crate::space::car::{Car, Entries, RepoEncoder};
    spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let self_read = assert_space_read(&creds, &space, &q.repo)?;
    let key = available(&app, &q.repo, self_read).await?;
    metrics::space_read("getRepo", auth_label(&creds));
    let slot = super::sync::export_slot(&app).await?;
    let p = app.partition(&q.repo)?;
    let snap = Arc::new(p.db.snapshot().await.map_err(XrpcError::from_err)?);
    let not_found = || XrpcError::bad("RepoNotFound", format!("Could not find repo for space: {}", space.uri));
    let v = snap.get(state::space_head_key(&q.repo, &space.sid)).await.map_err(XrpcError::from_err)?;
    let head = head_of(HeadRow::decode(&v.ok_or_else(not_found)?).map_err(XrpcError::from_err)?, &space, &p)?;
    let values = !q.exclude_values.unwrap_or(false);
    let takedowns = match values {
        true => Some(super::server::ctl(&app, &q.repo).await?),
        false => None,
    };
    let prefix = state::space_prefix(state::SPACE_RECORD_FAMILY, &q.repo, &space.sid);
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 4 << 20, cache_blocks: true, ..Default::default() };
    let mut iter = state::BatchedScan::new(
        snap.scan_with_options(prefix.clone()..state::prefix_end(&prefix), &opts).await.map_err(XrpcError::from_err)?,
    );
    let mut entries = Entries::default();
    while let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? {
        let path = std::str::from_utf8(&kv.key[prefix.len()..]).map_err(XrpcError::from_err)?;
        let (cid, _) = state::record_value_parts(&kv.value).map_err(XrpcError::from_err)?;
        entries.push(path, cid);
    }
    drop(iter);
    let rev = head.rev.to_string();
    let ctx = crate::space::commit::CommitCtx { space: &space.uri, author: &q.repo, rev: &rev };
    let commit = crate::space::commit::sign(&head.hash, &ctx, rand::random(), |b| {
        Ok::<_, std::convert::Infallible>(key.sign(b))
    })
    .map_err(|_| XrpcError::internal("space commit context too long"))?;
    let (stall, repo, sid) = (app.config.export_stall, q.repo.clone(), space.sid);
    Ok(super::sync::export_body(slot, "application/vnd.ipld.car", move |tx| async move {
        const CHUNK: usize = super::sync::EXPORT_CHUNK;
        let mut car = Car::new(commit_block(&commit));
        let order = car.order(&entries);
        car.begin(&entries, &order);
        let held = entries.heap_bytes() + order.capacity() * std::mem::size_of::<std::ops::Range<usize>>();
        // what an export holds: pass 1's paths and CIDs and the chunk being
        // filled (the body queue is the sync exports' budget)
        metrics::space_export_bytes(held + CHUNK);
        let mut buf = Vec::with_capacity(CHUNK + 4096);
        while car.prelude(&mut buf, CHUNK) {
            flush_chunk(&tx, &mut buf, stall).await?;
        }
        if values {
            let td = takedowns.as_ref().expect("read with values");
            for run in &order {
                let (first, last) = (entries.path(run.start), entries.path(run.end - 1));
                let lo = [&prefix[..], first.as_bytes()].concat();
                let hi = [&prefix[..], last.as_bytes(), &[0]].concat();
                let mut it = match snap.scan_with_options(lo..hi, &opts).await {
                    Ok(it) => state::BatchedScan::new(it),
                    Err(e) => {
                        tracing::warn!(%repo, "space getRepo: record scan failed: {e}");
                        return Err("error");
                    }
                };
                for i in run.clone() {
                    let kv = match it.next().await {
                        Ok(Some(kv)) => kv,
                        Ok(None) => return Err("error"),
                        Err(e) => {
                            tracing::warn!(%repo, "space getRepo: record scan failed: {e}");
                            return Err("error");
                        }
                    };
                    let path = entries.path(i);
                    let (cid, bytes) = match state::record_value_parts(&kv.value) {
                        Ok(v) if &kv.key[prefix.len()..] == path.as_bytes() && v.0 == *entries.cid(i) => v,
                        _ => {
                            tracing::warn!(%repo, path, "space getRepo: the snapshot changed between passes");
                            return Err("error");
                        }
                    };
                    if td.has_takedown(&takedown_name(&sid, path)) {
                        continue;
                    }
                    car.record(&cid, bytes, &mut buf);
                    if buf.len() >= CHUNK {
                        flush_chunk(&tx, &mut buf, stall).await?;
                    }
                }
            }
        }
        if !buf.is_empty() {
            flush_chunk(&tx, &mut buf, stall).await?;
        }
        Ok(())
    }))
}

async fn flush_chunk(
    tx: &super::sync::ChunkTx,
    buf: &mut Vec<u8>,
    stall: std::time::Duration,
) -> Result<(), &'static str> {
    let chunk = std::mem::replace(buf, Vec::with_capacity(super::sync::EXPORT_CHUNK + 4096));
    super::sync::send_chunk(tx, chunk, stall).await
}

/// How far ahead of this host's clock a notified repoRev may be.
const FUTURE_REV: std::time::Duration = std::time::Duration::from_secs(300);

/// Reference `processNotifyWrite` at the space's authority: the space must
/// be live here, the writer admitted by the write policy (the authority
/// always), and its repoRev newer than the one recorded (Ok(None) if not).
/// Recorded through the authority's worker, which assigns the spaceRev and
/// queues the forward to registered services once it's durable.
pub(super) async fn process_notify_write(
    app: &App,
    space: &Space,
    writer: &str,
    repo_rev: Tid,
    hash: [u8; 32],
) -> XResult<Option<crate::space::repo::Sequenced>> {
    super::simplespace::assert_space_host(app, space).await?;
    if repo_rev.micros() > crate::tid::now_micros() + FUTURE_REV.as_micros() as u64 {
        return Err(XrpcError::bad("FutureRev", "Repo revision is in the future"));
    }
    let row = super::simplespace::live_space(app, space).await?;
    let managing_app = match &row.write_policy {
        crate::space::rows::Policy::ManagingApp { .. } if writer != space.authority => {
            Some(super::simplespace::authorize_user(app, space, &row, writer, "write", None).await?)
        }
        _ => None,
    };
    let op = SpaceOp::RecordWriter { writer: writer.to_string(), repo_rev, hash, managing_app };
    match submit_space(app, &space.authority, space, op).await? {
        SpaceAck::Writer(seq) => Ok(seq),
        _ => Err(XrpcError::internal("unexpected space ack")),
    }
}

fn notify_in_result(r: &XResult<Option<crate::space::repo::Sequenced>>) -> &'static str {
    match r {
        Ok(Some(_)) => "ok",
        Ok(None) => "noop",
        Err(e) if e.status.is_server_error() => "error",
        Err(_) => "refused",
    }
}

/// `$bytes` of exactly `n` bytes.
fn bytes_field<const N: usize>(v: &J) -> Option<[u8; N]> {
    let s = v.get("$bytes")?.as_str()?;
    let b = base64::engine::general_purpose::STANDARD_NO_PAD.decode(s.trim_end_matches('=')).ok()?;
    b.try_into().ok()
}

/// Reference notifyWrite at a space host: the writer's repo host, by
/// service auth from the writer (`iss` is the claimed `repo`, so no host
/// notifies for another's account) addressed to the space's authority. The
/// repoRev is checked before auth, as the reference's input validation is.
async fn notify_write(State(app): AppState, headers: HeaderMap, Json(inp): Json<J>) -> XResult<StatusCode> {
    spaces(&app)?;
    let r = notify_write_inner(&app, &headers, &inp).await;
    metrics::space_notify("in", notify_in_result(&r));
    r.map(|_| StatusCode::OK)
}

async fn notify_write_inner(app: &App, headers: &HeaderMap, inp: &J) -> XResult<Option<crate::space::repo::Sequenced>> {
    let field = |k: &str| inp.get(k).and_then(|v| v.as_str());
    let repo_rev = field("repoRev")
        .and_then(Tid::parse)
        .ok_or_else(|| XrpcError::bad("InvalidRequest", "Input/repoRev must be a valid TID"))?;
    let space = Space::parse(field("space").unwrap_or(""))?;
    let repo = field("repo")
        .filter(|d| super::syntax::valid_did(d))
        .ok_or_else(|| XrpcError::bad("InvalidRequest", "Input/repo must be a valid did"))?;
    let hash = inp
        .get("hash")
        .and_then(bytes_field::<32>)
        .ok_or_else(|| XrpcError::bad("InvalidRequest", "Input/hash must be 32 bytes"))?;
    let auth = super::authn::verify_space_service_jwt(app, headers, "com.atproto.space.notifyWrite").await?;
    if auth.iss != repo {
        return Err(forbidden("notifyWrite iss does not match claimed writer"));
    }
    if auth.aud != space.authority && auth.aud != token::space_host_aud(&space.authority) {
        return Err(forbidden("notifyWrite aud does not match the space authority"));
    }
    process_notify_write(app, &space, repo, repo_rev, hash).await
}

/// A request the space host answers for a credential holder only: the
/// credential is this space's and addressed to the authority.
async fn host_credential(app: &App, headers: &HeaderMap, space: &Space) -> XResult<()> {
    match super::authn::verify_space_credential(app, headers).await? {
        Credentials::SpaceCredential { audience, space: s, .. } => {
            assert_credential_space(&audience, &s, space, &space.authority)
        }
        _ => Err(XrpcError::internal("not a space credential")),
    }
}

#[derive(Deserialize)]
struct ListReposQ {
    space: String,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// The first spaceRev a listRepos cursor admits. The reference compares it
/// with spaceRevs as a plain string; a TID's string order is its numeric
/// order, so the first TID whose string is greater is found by bisection.
fn space_rev_after(cursor: &str) -> Option<u64> {
    if let Some(t) = Tid::parse(cursor) {
        return t.0.checked_add(1);
    }
    let (mut lo, mut hi) = (0u64, 1u64 << 63);
    if Tid(hi - 1).to_string().as_str() <= cursor {
        return None;
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if Tid(mid).to_string().as_str() > cursor {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Some(lo)
}

/// Reference listRepos: each writer's latest state, in spaceRev order after
/// the cursor (`sQ` joined to `sW` in one snapshot). A writer updated while
/// a client pages may show up again, as the lexicon says.
async fn list_repos(State(app): AppState, headers: HeaderMap, Query(q): Query<ListReposQ>) -> XResult<Json<J>> {
    spaces(&app)?;
    let space = Space::parse(&q.space)?;
    let limit = super::extract::limit_param(q.limit, 100, 1, 1000)?;
    host_credential(&app, &headers, &space).await?;
    super::simplespace::live_space(&app, &space).await?;
    metrics::space_read("listRepos", "credential");
    let p = app.partition(&space.authority)?;
    let snap = p.db.snapshot().await.map_err(XrpcError::from_err)?;
    let prefix = state::space_prefix(state::SPACE_SEQ_FAMILY, &space.authority, &space.sid);
    let lo = match q.cursor.as_deref() {
        None => prefix.clone(),
        Some(c) => match space_rev_after(c) {
            Some(rev) => [&prefix[..], &rev.to_be_bytes()].concat(),
            None => return Ok(Json(json!({"repos": []}))),
        },
    };
    let opts = slatedb::config::ScanOptions::default();
    let mut iter = snap.scan_with_options(lo..state::prefix_end(&prefix), &opts).await.map_err(XrpcError::from_err)?;
    let mut repos = Vec::with_capacity(limit.min(256));
    let mut last = None;
    while repos.len() < limit {
        let rows = iter.next_batch(limit - repos.len()).await.map_err(XrpcError::from_err)?;
        if rows.is_empty() {
            break;
        }
        for kv in rows {
            let space_rev =
                crate::space::rows::seq_rev(&kv.key).ok_or_else(|| XrpcError::internal("malformed space seq key"))?;
            let writer = std::str::from_utf8(&kv.value).map_err(XrpcError::from_err)?;
            let k = state::space_writer_key(&space.authority, &space.sid, writer);
            let Some(v) = snap.get(k).await.map_err(XrpcError::from_err)? else { continue };
            let w = crate::space::rows::WriterRow::decode(&v).map_err(XrpcError::from_err)?;
            repos.push(json!({
                "did": writer,
                "repoRev": w.repo_rev.to_string(),
                "hash": b64(&w.hash),
                "spaceRev": space_rev.to_string(),
            }));
            last = Some(space_rev);
        }
    }
    let mut out = json!({"repos": repos});
    if let Some(rev) = last {
        out["cursor"] = json!(rev.to_string());
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct RegisterIn {
    space: String,
    service: String,
}

/// Reference registerNotify: a credential holder subscribes a service (a DID
/// with an optional fragment) to the space's write notifications for a
/// day. Registering again replaces the endpoint and extends the expiry.
async fn register_notify(State(app): AppState, headers: HeaderMap, Json(inp): Json<RegisterIn>) -> XResult<Json<J>> {
    spaces(&app)?;
    let space = Space::parse(&inp.space)?;
    host_credential(&app, &headers, &space).await?;
    super::simplespace::assert_space_host(&app, &space).await?;
    let Some(endpoint) = crate::space::host::resolve_service_endpoint(&app, &inp.service).await else {
        return Err(XrpcError::bad(
            "ServiceNotResolvable",
            format!("Could not resolve a service endpoint for {}", inp.service),
        ));
    };
    let expires = crate::tid::now_micros() + crate::space::host::REGISTRATION_TTL.as_micros() as u64;
    let row = crate::space::rows::NotifyRow { endpoint, expires };
    submit_space(&app, &space.authority, &space, SpaceOp::RegisterNotify { service: inp.service, row }).await?;
    let at =
        chrono::DateTime::from_timestamp_micros(expires as i64).ok_or_else(|| XrpcError::internal("bad expiry"))?;
    Ok(Json(json!({"expiresAt": at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)})))
}

/// Reference unregisterNotify: not resolved again (a subscriber whose DID
/// document changed can still withdraw); fine when nothing was registered.
async fn unregister_notify(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<RegisterIn>,
) -> XResult<StatusCode> {
    spaces(&app)?;
    let space = Space::parse(&inp.space)?;
    host_credential(&app, &headers, &space).await?;
    super::simplespace::assert_space_host(&app, &space).await?;
    submit_space(&app, &space.authority, &space, SpaceOp::UnregisterNotify { service: inp.service }).await?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_repos_cursors_compare_as_strings() {
        let t = Tid::from_parts(1_790_000_000_000_000, 3);
        assert_eq!(space_rev_after(&t.to_string()), Some(t.0 + 1));
        for c in ["", "0", "2", "3jzfcijpj2z2", "3jzfcijpj2z2a0", "abc", "b", "bzzzzzzzzzzz"] {
            let first = space_rev_after(c).unwrap();
            assert!(Tid(first).to_string().as_str() > c, "{c}");
            if first > 0 {
                assert!(Tid(first - 1).to_string().as_str() <= c, "{c}");
            }
        }
        // past every TID
        assert_eq!(space_rev_after("c"), None);
        assert_eq!(space_rev_after("zzz"), None);
    }

    #[test]
    fn commit_blocks_are_canonical() {
        let c = crate::space::commit::SignedCommit {
            ver: 1,
            hash: vec![1; 32],
            ikm: vec![2; 32],
            sig: vec![3; 64],
            mac: vec![4; 32],
            rev: "3jzfcijpj2z2a".into(),
        };
        let b = commit_block(&c);
        let v = crate::cbor::Value::decode(&b).unwrap();
        assert_eq!(v.to_cbor(), b, "decodes and re-encodes to the same bytes");
        assert_eq!(v.get("rev").and_then(|r| r.as_str()), Some("3jzfcijpj2z2a"));
    }
}
