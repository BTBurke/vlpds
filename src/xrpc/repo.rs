use super::extract::RecordBody;
use super::*;
use crate::cbor::{JsonValue, RecordRefs};

pub fn routes() -> Router<Arc<App>> {
    let r = Router::new()
        .route("/xrpc/com.atproto.repo.createRecord", post(create_record))
        .route("/xrpc/com.atproto.repo.putRecord", post(put_record))
        .route("/xrpc/com.atproto.repo.deleteRecord", post(delete_record))
        .route("/xrpc/com.atproto.repo.applyWrites", post(apply_writes))
        .route("/xrpc/com.atproto.repo.getRecord", get(get_record))
        .route("/xrpc/com.atproto.repo.listRecords", get(list_records))
        .route("/xrpc/com.atproto.repo.describeRepo", get(describe_repo))
        .route(
            "/xrpc/com.atproto.repo.importRepo",
            post(import_repo).layer(axum::extract::DefaultBodyLimit::max(MAX_IMPORT_BYTES)),
        );
    r
}

/// Largest CAR importRepo accepts (it is parsed in memory).
const MAX_IMPORT_BYTES: usize = 1 << 30;

/// Collection must be an NSID and rkey a valid record key.
fn check_path(collection: &str, rkey: Option<&str>) -> XResult<()> {
    if !super::syntax::valid_nsid(collection) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!("Invalid collection: {collection} is not a valid NSID"),
        ));
    }
    if let Some(r) = rkey {
        if !super::syntax::valid_rkey(r) {
            return Err(XrpcError::bad(
                "InvalidRequest",
                format!("Invalid record key: {r}"),
            ));
        }
    }
    Ok(())
}

fn parse_cid_opt(v: &Option<String>) -> XResult<Option<Cid>> {
    v.as_deref()
        .map(|s| {
            Cid::parse(s).map_err(|_| XrpcError::bad("InvalidRequest", format!("bad cid {s}")))
        })
        .transpose()
}

/// An encoded record: (cid, DAG-CBOR bytes, blob refs, validation status,
/// declared blob refs).
type Encoded = (Cid, Bytes, Vec<Cid>, crate::lexicon::ValidationStatus, Vec<BlobDecl>);

/// A blob ref as the record declares it: (cid, mimeType, size).
type BlobDecl = (Cid, Option<String>, Option<i64>);

/// JSON record -> DAG-CBOR, as the reference's prepareWrite: a missing
/// `$type` defaults to the collection and any other value must equal it,
/// then known lexicons are validated (record key included); `resolved` is
/// the dynamically resolved lexicon of `collection`, if any. One pass over
/// the parsed tree writes the canonical bytes and collects blob refs (and
/// legacy blob refs); validation then reads the same tree.
fn encode_record(
    v: &mut JsonValue,
    collection: &str,
    rkey: &str,
    validate: Option<bool>,
    resolved: Option<&crate::lexicon::Lexicons>,
) -> XResult<Encoded> {
    if !matches!(v, JsonValue::Object(_)) {
        return Err(XrpcError::bad("InvalidRequest", "record must be an object"));
    }
    match v.get("$type") {
        None => v.insert("$type", JsonValue::Str(collection.to_string().into())),
        Some(JsonValue::Str(t)) if t == collection => {}
        Some(t) => {
            return Err(XrpcError::bad(
                "InvalidRequest",
                format!("Invalid $type: expected {collection}, got {}", t.to_json()),
            ))
        }
    }
    let mut bytes = Vec::with_capacity(512);
    let mut refs = RecordRefs::default();
    v.encode_record(&mut bytes, &mut refs)
        .map_err(|e| XrpcError::bad("InvalidRequest", e.to_string()))?;
    let status = crate::lexicon::validate_record(collection, rkey, &*v, validate, resolved)
        .map_err(|e| XrpcError::bad("InvalidRequest", e))?;
    if let Some(c) = refs.legacy {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!("Legacy blobs are not allowed ({c})"),
        ));
    }
    if bytes.len() > 1_000_000 {
        return Err(XrpcError::bad("InvalidRequest", "record too large"));
    }
    let blobs = refs.cids();
    Ok((Cid::dag_cbor(&bytes), Bytes::from(bytes), blobs, status, refs.blobs))
}

/// Adds `validationStatus` unless validation was skipped.
fn with_status(mut out: J, status: crate::lexicon::ValidationStatus) -> J {
    if let Some(st) = status {
        out["validationStatus"] = json!(st);
    }
    out
}

/// Input fields of a record write, read from the validated body tree (the
/// input lexicon has already checked their types; the errors below are
/// what the old serde structs reported).
fn field_err(m: String) -> XrpcError {
    XrpcError::bad("InvalidRequest", format!("Invalid JSON body: {m}"))
}

fn opt_str(v: &JsonValue, k: &str) -> XResult<Option<String>> {
    match v.get(k) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Str(s)) => Ok(Some(s.to_string())),
        Some(_) => Err(field_err(format!("invalid type for `{k}`, expected a string"))),
    }
}

fn req_str(v: &JsonValue, k: &str) -> XResult<String> {
    opt_str(v, k)?.ok_or_else(|| field_err(format!("missing field `{k}`")))
}

fn opt_bool(v: &JsonValue, k: &str) -> XResult<Option<bool>> {
    match v.get(k) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(field_err(format!("invalid type for `{k}`, expected a boolean"))),
    }
}

/// Moves `k` out of the body tree.
fn take<'a>(v: &mut JsonValue<'a>, k: &str) -> XResult<JsonValue<'a>> {
    v.get_mut(k)
        .map(|x| std::mem::replace(x, JsonValue::Null))
        .ok_or_else(|| field_err(format!("missing field `{k}`")))
}

/// Every blob a write references must have been uploaded by the repo and not
/// be taken down (reference: processWriteBlobs -> "Could not find blob"),
/// and its declared mimeType and size must match the stored blob (reference
/// verifyBlob), so lexicon `accept`/`maxSize` checks hold for the real bytes.
async fn check_blobs(app: &App, did: &str, decls: &[BlobDecl]) -> XResult<()> {
    for (cid, mime, size) in decls {
        let missing = || XrpcError::bad("BlobNotFound", format!("Could not find blob: {cid}"));
        if super::admin::is_blob_takendown(app, did, &cid.to_string()).await {
            return Err(missing());
        }
        let opts = object_store::GetOptions { head: true, ..Default::default() };
        let found = match app.store.raw.get_opts(&super::blobs::blob_path(app, did, cid), opts).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Err(missing()),
            Err(e) => return Err(XrpcError::from_err(e)),
        };
        let stored_mime = super::blobs::stored_mime(&found.attributes);
        if mime.as_deref() != Some(stored_mime.as_str()) {
            return Err(XrpcError::bad(
                "InvalidMimeType",
                format!(
                    "Referenced Mimetype does not match stored blob. Expected: {stored_mime}, Got: {}",
                    mime.as_deref().unwrap_or("undefined")
                ),
            ));
        }
        let stored_size = found.meta.size;
        if *size != i64::try_from(stored_size).ok() {
            return Err(XrpcError::bad(
                "InvalidSize",
                format!(
                    "Referenced Size does not match stored blob. Expected: {stored_size}, Got: {}",
                    size.map_or("undefined".to_string(), |n| n.to_string())
                ),
            ));
        }
    }
    Ok(())
}

async fn submit(
    app: &App,
    did: Arc<str>,
    writes: Vec<Write>,
    swap_commit: Option<Cid>,
) -> XResult<CommitAck> {
    let Ok(_permit) = app.write_permits.try_acquire() else {
        STATS.write_errors.fetch_add(1, Ordering::Relaxed);
        metrics::WRITES_SHED.inc();
        return Err(XrpcError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: "Overloaded".into(),
            message: "too many writes in flight; retry with backoff".into(),
        });
    };
    let start = Instant::now();
    STATS.write_requests.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    app.workers
        .route(&did)
        .send(WorkerMsg::Write(WriteReq {
            did,
            writes,
            swap_commit,
            reply: tx,
        }))
        .map_err(XrpcError::from_err)?;
    let r = rx
        .await
        .map_err(|_| XrpcError::internal("worker dropped request"))?;
    STATS.record_request(start.elapsed());
    r.map_err(|e| {
        STATS.write_errors.fetch_add(1, Ordering::Relaxed);
        let kind = match &e {
            WriteError::RepoNotFound => "repo_not_found",
            WriteError::RepoInactive(_) => "repo_inactive",
            WriteError::InvalidSwap(_) => "invalid_swap",
            WriteError::Invalid(_) => "invalid",
            WriteError::Internal(_) => "internal",
            WriteError::Unavailable(_) => "unavailable",
        };
        metrics::WRITE_ERRORS.with_label_values(&[kind]).inc();
        e.into()
    })
}

fn commit_json(ack: &CommitAck) -> J {
    json!({"cid": ack.commit.to_string(), "rev": ack.rev.to_string()})
}

fn uri(did: &str, path: &str) -> String {
    format!("at://{did}/{path}")
}

struct CreateRecordIn<'a> {
    repo: String,
    collection: String,
    rkey: Option<String>,
    record: JsonValue<'a>,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

impl<'a> CreateRecordIn<'a> {
    fn from_tree(mut v: JsonValue<'a>) -> XResult<Self> {
        Ok(CreateRecordIn {
            repo: req_str(&v, "repo")?,
            collection: req_str(&v, "collection")?,
            rkey: opt_str(&v, "rkey")?,
            swap_commit: opt_str(&v, "swapCommit")?,
            validate: opt_bool(&v, "validate")?,
            record: take(&mut v, "record")?,
        })
    }
}

async fn create_record(
    State(app): AppState,
    Auth(creds): Auth,
    body: RecordBody,
) -> XResult<Json<J>> {
    let mut inp = CreateRecordIn::from_tree(body.parse()?)?;
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::CREATE_POINTS)?;
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    creds.require(creds.allows_repo(&inp.collection, "create"))?;
    check_path(&inp.collection, inp.rkey.as_deref())?;
    // as the reference: no rkey = a fresh TID (validated against the schema's key)
    let rkey = inp.rkey.unwrap_or_else(|| app.tids.next().to_string());
    let schema = crate::lexicon::resolve_record_schema(&app, &inp.collection, inp.validate).await;
    let (cid, bytes, blobs, status, decls) =
        encode_record(&mut inp.record, &inp.collection, &rkey, inp.validate, schema.as_deref())?;
    check_blobs(&app, &did, &decls).await?;
    let swap = parse_cid_opt(&inp.swap_commit)?;
    let path = format!("{}/{}", inp.collection, rkey);
    let ack = submit(
        &app,
        did.clone(),
        vec![Write::Create {
            collection: inp.collection,
            rkey,
            cid,
            bytes,
            blobs,
        }],
        swap,
    )
    .await?;
    Ok(Json(with_status(
        json!({
            "uri": uri(&did, &path),
            "cid": cid.to_string(),
            "commit": commit_json(&ack),
        }),
        status,
    )))
}

struct PutRecordIn<'a> {
    repo: String,
    collection: String,
    rkey: String,
    record: JsonValue<'a>,
    /// None = absent, Some(None) = an explicit null.
    swap_record: Option<Option<String>>,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

impl<'a> PutRecordIn<'a> {
    fn from_tree(mut v: JsonValue<'a>) -> XResult<Self> {
        Ok(PutRecordIn {
            repo: req_str(&v, "repo")?,
            collection: req_str(&v, "collection")?,
            rkey: req_str(&v, "rkey")?,
            swap_record: match v.get("swapRecord") {
                None => None,
                Some(_) => Some(opt_str(&v, "swapRecord")?),
            },
            swap_commit: opt_str(&v, "swapCommit")?,
            validate: opt_bool(&v, "validate")?,
            record: take(&mut v, "record")?,
        })
    }
}

fn parse_swap_record(v: &Option<Option<String>>) -> XResult<Option<Option<Cid>>> {
    match v {
        None => Ok(None),
        Some(None) => Ok(Some(None)),
        Some(Some(s)) => {
            Ok(Some(Some(Cid::parse(s).map_err(|_| {
                XrpcError::bad("InvalidRequest", "bad swapRecord")
            })?)))
        }
    }
}

async fn put_record(
    State(app): AppState,
    Auth(creds): Auth,
    body: RecordBody,
) -> XResult<Json<J>> {
    let mut inp = PutRecordIn::from_tree(body.parse()?)?;
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::UPDATE_POINTS)?;
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    creds.require(
        creds.allows_repo(&inp.collection, "create")
            && creds.allows_repo(&inp.collection, "update"),
    )?;
    check_path(&inp.collection, Some(&inp.rkey))?;
    let schema = crate::lexicon::resolve_record_schema(&app, &inp.collection, inp.validate).await;
    let (cid, bytes, blobs, status, decls) =
        encode_record(&mut inp.record, &inp.collection, &inp.rkey, inp.validate, schema.as_deref())?;
    let swap = parse_cid_opt(&inp.swap_commit)?;
    let swap_record = parse_swap_record(&inp.swap_record)?;
    let path = format!("{}/{}", inp.collection, inp.rkey);
    // Writing the record it already holds is a no-op: no commit, the current
    // cid back and no `commit` field (reference putRecord; it skips the swap
    // checks too).
    let p = app.partition(&did)?;
    if let Some(cur) = p
        .db
        .get(state::record_key(&did, &path))
        .await
        .map_err(XrpcError::from_err)?
    {
        let (cur, _) = state::decode_record_value(&cur).map_err(XrpcError::from_err)?;
        if cur == cid {
            app.ensure_active(&did).await?;
            return Ok(Json(with_status(
                json!({"uri": uri(&did, &path), "cid": cid.to_string()}),
                status,
            )));
        }
    }
    check_blobs(&app, &did, &decls).await?;
    let w = Write::Update {
        collection: inp.collection,
        rkey: inp.rkey,
        cid,
        bytes,
        blobs,
        swap: swap_record,
        must_exist: false,
    };
    let ack = submit(&app, did.clone(), vec![w], swap).await?;
    Ok(Json(with_status(
        json!({
            "uri": uri(&did, &path),
            "cid": cid.to_string(),
            "commit": commit_json(&ack),
        }),
        status,
    )))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteRecordIn {
    repo: String,
    collection: String,
    rkey: String,
    swap_record: Option<String>,
    swap_commit: Option<String>,
}

async fn delete_record(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<DeleteRecordIn>,
) -> XResult<Json<J>> {
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::DELETE_POINTS)?;
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    creds.require(creds.allows_repo(&inp.collection, "delete"))?;
    check_path(&inp.collection, Some(&inp.rkey))?;
    let swap = parse_cid_opt(&inp.swap_commit)?;
    let swap_record = parse_cid_opt(&inp.swap_record)?.map(Some);
    // Deleting a record that doesn't exist is a no-op with no commit (as in
    // the reference). The worker would otherwise emit an empty commit whose
    // CAR lacks the unchanged MST root, which relays reject.
    let path = format!("{}/{}", inp.collection, inp.rkey);
    let p = app.partition(&did)?;
    if p.db
        .get(state::record_key(&did, &path))
        .await
        .map_err(XrpcError::from_err)?
        .is_none()
    {
        app.ensure_active(&did).await?;
        return Ok(Json(json!({})));
    }
    let w = Write::Delete {
        collection: inp.collection,
        rkey: inp.rkey,
        swap: swap_record,
    };
    let ack = submit(&app, did, vec![w], swap).await?;
    Ok(Json(json!({"commit": commit_json(&ack)})))
}

struct ApplyWritesIn<'a> {
    repo: String,
    writes: Vec<JsonValue<'a>>,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

impl<'a> ApplyWritesIn<'a> {
    fn from_tree(mut v: JsonValue<'a>) -> XResult<Self> {
        Ok(ApplyWritesIn {
            repo: req_str(&v, "repo")?,
            swap_commit: opt_str(&v, "swapCommit")?,
            validate: opt_bool(&v, "validate")?,
            writes: match take(&mut v, "writes")? {
                JsonValue::Array(a) => a,
                _ => return Err(field_err("invalid type for `writes`, expected a sequence".into())),
            },
        })
    }
}

async fn apply_writes(
    State(app): AppState,
    Auth(creds): Auth,
    body: RecordBody,
) -> XResult<Json<J>> {
    let mut inp = ApplyWritesIn::from_tree(body.parse()?)?;
    {
        use crate::ratelimit::*;
        let points = inp
            .writes
            .iter()
            .map(|w| match w.get("$type").and_then(|t| t.as_str()) {
                Some("com.atproto.repo.applyWrites#create") => CREATE_POINTS,
                Some("com.atproto.repo.applyWrites#update") => UPDATE_POINTS,
                _ => DELETE_POINTS,
            })
            .sum();
        check_repo_write(creds.did(), points)?;
    }
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    let swap = parse_cid_opt(&inp.swap_commit)?;
    if inp.writes.len() > crate::worker::MAX_COMMIT_OPS {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!("Too many writes. Max: {}", crate::worker::MAX_COMMIT_OPS),
        ));
    }
    let mut writes = Vec::with_capacity(inp.writes.len());
    let mut statuses = Vec::with_capacity(inp.writes.len());
    let mut decls = Vec::new();
    // dynamically resolved lexicons, once per collection
    let mut schemas: std::collections::HashMap<String, Option<Arc<crate::lexicon::Lexicons>>> = Default::default();
    for w in inp.writes.iter_mut() {
        let t = w.get("$type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let collection = w
            .get("collection")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let rkey = w.get("rkey").and_then(|v| v.as_str()).map(String::from);
        let action = match t.as_str() {
            "com.atproto.repo.applyWrites#create" => "create",
            "com.atproto.repo.applyWrites#update" => "update",
            _ => "delete",
        };
        creds.require(creds.allows_repo(&collection, action))?;
        check_path(&collection, rkey.as_deref())?;
        if action != "delete" && !schemas.contains_key(&collection) {
            let s = crate::lexicon::resolve_record_schema(&app, &collection, inp.validate).await;
            schemas.insert(collection.clone(), s);
        }
        let schema = schemas.get(&collection).cloned().flatten();
        let mut value = w.get_mut("value").map(|x| std::mem::replace(x, JsonValue::Null)).unwrap_or(JsonValue::Null);
        match t.as_str() {
            "com.atproto.repo.applyWrites#create" => {
                let rkey = rkey.unwrap_or_else(|| app.tids.next().to_string());
                let (cid, bytes, blobs, status, d) =
                    encode_record(&mut value, &collection, &rkey, inp.validate, schema.as_deref())?;
                statuses.push(status);
                decls.extend(d);
                writes.push(Write::Create {
                    collection,
                    rkey,
                    cid,
                    bytes,
                    blobs,
                });
            }
            "com.atproto.repo.applyWrites#update" => {
                let rkey =
                    rkey.ok_or_else(|| XrpcError::bad("InvalidRequest", "update requires rkey"))?;
                let (cid, bytes, blobs, status, d) =
                    encode_record(&mut value, &collection, &rkey, inp.validate, schema.as_deref())?;
                statuses.push(status);
                decls.extend(d);
                writes.push(Write::Update {
                    collection,
                    rkey,
                    cid,
                    bytes,
                    blobs,
                    swap: None,
                    must_exist: true,
                });
            }
            "com.atproto.repo.applyWrites#delete" => {
                let rkey =
                    rkey.ok_or_else(|| XrpcError::bad("InvalidRequest", "delete requires rkey"))?;
                statuses.push(None);
                writes.push(Write::Delete {
                    collection,
                    rkey,
                    swap: None,
                });
            }
            _ => {
                return Err(XrpcError::bad(
                    "InvalidRequest",
                    format!("unknown write type {t}"),
                ))
            }
        }
    }
    check_blobs(&app, &did, &decls).await?;
    let ack = submit(&app, did.clone(), writes, swap).await?;
    let results: Vec<J> = ack
        .results
        .iter()
        .zip(statuses)
        .map(|(r, status)| match r {
            WriteOutcome::Create { path, cid } => with_status(json!({"$type": "com.atproto.repo.applyWrites#createResult", "uri": uri(&did, path), "cid": cid.to_string()}), status),
            WriteOutcome::Update { path, cid } => with_status(json!({"$type": "com.atproto.repo.applyWrites#updateResult", "uri": uri(&did, path), "cid": cid.to_string()}), status),
            WriteOutcome::Delete => json!({"$type": "com.atproto.repo.applyWrites#deleteResult"}),
        })
        .collect();
    Ok(Json(
        json!({"commit": commit_json(&ack), "results": results}),
    ))
}

#[derive(Deserialize)]
struct GetRecordQ {
    repo: String,
    collection: String,
    rkey: String,
    cid: Option<String>,
}

/// Records of repos not hosted here (unknown handle, or no local account
/// for the DID) are piped through to the AppView, as in the reference. In
/// cluster mode a DID owned by another node was already forwarded there.
async fn get_record(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    headers: HeaderMap,
    req_uri: axum::http::Uri,
    Query(q): Query<GetRecordQ>,
) -> XResult<Response> {
    check_path(&q.collection, Some(&q.rkey))?;
    let not_hosted = |e: &XrpcError| e.error == "RepoNotFound" || e.error == "AccountNotFound";
    let local = match app.resolve_repo(&q.repo).await {
        Ok(did) => match app.account(&did).await {
            Ok(_) => Some(did),
            Err(e) if not_hosted(&e) => None,
            Err(e) => return Err(e),
        },
        Err(e) if not_hosted(&e) => None,
        Err(e) => return Err(e),
    };
    let Some(did) = local else {
        if app.config.appview.is_none() {
            return Err(XrpcError::bad("InvalidRequest", "Could not locate record"));
        }
        return super::proxy::pipethrough_unauthed(&app, &headers, &req_uri, "com.atproto.repo.getRecord").await;
    };
    super::sync::assert_available(&app, &did, creds.as_ref()).await?;
    let p = app.partition(&did)?;
    let path = format!("{}/{}", q.collection, q.rkey);
    let v =
        p.db.get(state::record_key(&did, &path))
            .await
            .map_err(XrpcError::from_err)?;
    let v = v.ok_or_else(|| {
        XrpcError::bad(
            "RecordNotFound",
            format!("Could not locate record: at://{did}/{path}"),
        )
    })?;
    let (cid, bytes) = state::decode_record_value(&v).map_err(XrpcError::from_err)?;
    if super::admin::is_record_takendown(&app, &did, &path).await {
        return Err(XrpcError::bad(
            "RecordNotFound",
            format!("Could not locate record: at://{did}/{path}"),
        ));
    }
    if let Some(want) = &q.cid {
        if *want != cid.to_string() {
            return Err(XrpcError::bad(
                "RecordNotFound",
                format!("Could not locate record: at://{did}/{path}"),
            ));
        }
    }
    let mut out = Vec::with_capacity(bytes.len() * 2 + 128);
    write_record_json(&mut out, &uri(&did, &path), &cid, &bytes)?;
    Ok(json_bytes(out))
}

/// Appends `{"uri","cid","value"}` for one stored record, transcoding the
/// DAG-CBOR value straight to JSON (no intermediate value tree).
fn write_record_json(out: &mut Vec<u8>, uri: &str, cid: &Cid, bytes: &[u8]) -> XResult<()> {
    out.extend_from_slice(b"{\"uri\":");
    serde_json::to_writer(&mut *out, uri).map_err(XrpcError::from_err)?;
    out.extend_from_slice(b",\"cid\":\"");
    cid.write_string(out);
    out.extend_from_slice(b"\",\"value\":");
    crate::cbor::write_json(bytes, out).map_err(XrpcError::from_err)?;
    out.push(b'}');
    Ok(())
}

fn json_bytes(body: Vec<u8>) -> Response {
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

#[derive(Deserialize)]
struct ListRecordsQ {
    repo: String,
    collection: String,
    limit: Option<i64>,
    cursor: Option<String>,
    reverse: Option<bool>,
}

async fn list_records(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<ListRecordsQ>,
) -> XResult<Response> {
    check_path(&q.collection, None)?;
    let did = app.resolve_repo(&q.repo).await?;
    super::sync::assert_available(&app, &did, creds.as_ref()).await?;
    let p = app.partition(&did)?;
    // lexicon: integer, minimum 1, maximum 100
    let limit = match q.limit {
        None => 50,
        Some(n @ 1..=100) => n as usize,
        Some(n) => {
            return Err(XrpcError::bad(
                "InvalidRequest",
                format!("limit must be between 1 and 100, got {n}"),
            ))
        }
    };
    let prefix = state::record_key(&did, &format!("{}/", q.collection));
    let end = state::prefix_end(&prefix);
    // default order is newest first (descending rkey); reverse=true is ascending
    let ascending = q.reverse.unwrap_or(false);
    let (lo, hi) = match (&q.cursor, ascending) {
        (Some(c), true) => {
            let mut k = prefix.clone();
            k.extend_from_slice(c.as_bytes());
            k.push(0);
            (k, end)
        }
        (Some(c), false) => {
            let mut k = prefix.clone();
            k.extend_from_slice(c.as_bytes());
            (prefix.clone(), k)
        }
        (None, _) => (prefix.clone(), end),
    };
    let order = if ascending {
        slatedb::IterationOrder::Ascending
    } else {
        slatedb::IterationOrder::Descending
    };
    let opts = slatedb::config::ScanOptions::default().with_order(order);
    // the repo's takedowns, read once for the page
    let takedowns = super::server::ctl(&app, &did).await;
    let mut iter =
        p.db.scan_with_options(lo..hi, &opts)
            .await
            .map_err(XrpcError::from_err)?;
    let mut out = Vec::with_capacity(limit * 512);
    out.extend_from_slice(b"{\"records\":[");
    let mut n = 0;
    let mut last_rkey = None;
    while n < limit {
        let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
            break;
        };
        let rkey = String::from_utf8_lossy(&kv.key[prefix.len()..]).to_string();
        let rec_uri = uri(&did, &format!("{}/{}", q.collection, rkey));
        if takedowns.has_takedown(&format!("rec/{}/{rkey}", q.collection)) {
            last_rkey = Some(rkey);
            continue;
        }
        let (cid, bytes) = state::decode_record_value(&kv.value).map_err(XrpcError::from_err)?;
        if n > 0 {
            out.push(b',');
        }
        write_record_json(&mut out, &rec_uri, &cid, &bytes)?;
        n += 1;
        last_rkey = Some(rkey);
    }
    out.push(b']');
    if let (true, Some(c)) = (n == limit, &last_rkey) {
        out.extend_from_slice(b",\"cursor\":");
        serde_json::to_writer(&mut out, c).map_err(XrpcError::from_err)?;
    }
    out.push(b'}');
    Ok(json_bytes(out))
}

#[derive(Deserialize)]
struct RepoQ {
    repo: String,
}

/// Collections in the repo: one seek per collection over its contiguous
/// `R/{did}\0{collection}/...` key range.
async fn list_collections(app: &App, did: &str) -> XResult<Vec<String>> {
    let p = app.partition(did)?;
    let prefix = state::record_prefix(did);
    let end = state::prefix_end(&prefix);
    let mut lo = prefix.clone();
    let mut out = Vec::new();
    loop {
        let mut iter =
            p.db.scan(lo..end.clone())
                .await
                .map_err(XrpcError::from_err)?;
        let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
            break;
        };
        let path = String::from_utf8_lossy(&kv.key[prefix.len()..]).into_owned();
        let coll = crate::worker::collection_of(&path).to_string();
        lo = state::prefix_end(&state::record_key(did, &format!("{coll}/")));
        out.push(coll);
    }
    Ok(out)
}

async fn describe_repo(State(app): AppState, Query(q): Query<RepoQ>) -> XResult<Json<J>> {
    let did = app.resolve_repo(&q.repo).await?;
    let acct = super::sync::assert_available(&app, &did, None).await?;
    let handle_is_correct =
        app.resolve_handle(&acct.handle).await?.as_deref() == Some(acct.did.as_str());
    Ok(Json(json!({
        "handle": if handle_is_correct { acct.handle.as_str() } else { "handle.invalid" },
        "did": acct.did,
        "didDoc": super::identity::did_doc(&app, &acct)?,
        "collections": list_collections(&app, &did).await?,
        "handleIsCorrect": handle_is_correct,
    })))
}

/// Replaces the caller's repo with the contents of a CAR (one root: a
/// commit). The MST is loaded from the CAR and checked complete; every
/// record block must hash to its CID. The worker writes a new commit (new
/// rev, signed with our key) and emits `#sync` unless the account is
/// deactivated (migration in: activation announces it). Like the reference,
/// neither the imported commit's signature nor its `did` is checked: only
/// its contents are used, re-signed for the caller's DID.
async fn import_repo(
    State(app): AppState,
    Auth(creds): Auth,
    body: AxBytes,
) -> XResult<StatusCode> {
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?
        .to_string();
    creds.require(creds.allows_account("repo", "manage"))?;
    let acct = app.account(&did).await?;
    if matches!(
        acct.status.as_deref(),
        Some("takendown") | Some("suspended")
    ) {
        return Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AccountTakedown".into(),
            message: "Account has been taken down".into(),
        });
    }
    let records = tokio::task::spawn_blocking(move || parse_import(&body).map(|r| (did, r)))
        .await
        .map_err(XrpcError::from_err)?;
    let (did, records) = records?;
    app.account_op(&did, crate::worker::AccountOp::ReplaceRepo { records })
        .await?;
    Ok(StatusCode::OK)
}

type ImportedRecord = (String, Cid, Bytes, Vec<Cid>);

fn parse_import(body: &[u8]) -> XResult<Vec<ImportedRecord>> {
    let bad = |m: String| XrpcError::bad("InvalidRequest", m);
    let (roots, blocks) = car::read_car(body).map_err(|e| bad(format!("invalid CAR: {e}")))?;
    if roots.len() != 1 {
        return Err(bad("expected one root".into()));
    }
    let mut map: std::collections::HashMap<Cid, Vec<u8>> =
        std::collections::HashMap::with_capacity(blocks.len());
    for (c, b) in blocks {
        let actual = if c.codec == crate::cid::CODEC_RAW {
            Cid::raw(b)
        } else {
            Cid::dag_cbor(b)
        };
        if actual != c {
            return Err(bad(format!("block does not match its cid: {c}")));
        }
        map.insert(c, b.to_vec());
    }
    let commit_bytes = map
        .get(&roots[0])
        .ok_or_else(|| bad("missing commit block".into()))?;
    let commit = Value::decode(commit_bytes).map_err(|e| bad(format!("invalid commit: {e}")))?;
    match commit.get("version") {
        Some(Value::Int(2 | 3)) => {}
        _ => return Err(bad("unsupported commit version".into())),
    }
    let Some(Value::Link(data)) = commit.get("data") else {
        return Err(bad("commit has no data root".into()));
    };
    let tree = crate::mst::Tree::load_from_blocks(&map, *data)
        .map_err(|e| bad(format!("could not load MST: {e}")))?;
    let mut entries: Vec<(String, Cid)> = Vec::new();
    let mut key_err = None;
    tree.walk(&mut |k, c| match std::str::from_utf8(k) {
        Ok(p) => entries.push((p.to_string(), c)),
        Err(_) => key_err = Some(String::from_utf8_lossy(k).into_owned()),
    });
    if let Some(k) = key_err {
        return Err(bad(format!("invalid record path {k}")));
    }
    // walk() skips subtrees missing from the CAR: rebuilding from the walked
    // entries reproduces `data` only if the tree was complete.
    let mut check = crate::mst::Tree::new();
    for (path, cid) in &entries {
        check
            .insert_no_proof(path.as_bytes(), *cid)
            .map_err(|e| bad(format!("invalid record path {path}: {e}")))?;
    }
    if check.root_cid().map_err(XrpcError::from_err)? != *data {
        return Err(bad("CAR does not contain the complete MST".into()));
    }
    let mut out = Vec::with_capacity(entries.len());
    for (path, cid) in entries {
        if !super::syntax::valid_record_path(&path) {
            return Err(bad(format!("invalid record path {path}")));
        }
        let bytes = map
            .get(&cid)
            .ok_or_else(|| bad(format!("missing record block {cid} at {path}")))?;
        let v =
            Value::decode(bytes).map_err(|_| bad(format!("Could not parse record at '{path}'")))?;
        let mut blobs = Vec::new();
        blob_refs(&v, &mut blobs);
        out.push((path, cid, Bytes::from(bytes.clone()), blobs));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record path before `JsonValue` (body -> `serde_json::Value` ->
    /// input lexicon -> serde struct -> `Value::from_json` -> lexicon ->
    /// three walks -> `to_cbor`), kept as the oracle for the new one.
    mod legacy {
        use super::super::*;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct CreateRecordIn {
            pub collection: String,
            pub rkey: Option<String>,
            pub record: J,
            pub validate: Option<bool>,
        }

        fn blob_decls(v: &Value, out: &mut Vec<BlobDecl>) {
            match v {
                Value::Map(m) => {
                    if v.get("$type").and_then(|t| t.as_str()) == Some("blob") {
                        if let Some(Value::Link(c)) = v.get("ref") {
                            let mime = v.get("mimeType").and_then(|m| m.as_str()).map(String::from);
                            let size = match v.get("size") {
                                Some(Value::Int(n)) => Some(*n),
                                _ => None,
                            };
                            out.push((*c, mime, size));
                        }
                    }
                    for (_, child) in m {
                        blob_decls(child, out);
                    }
                }
                Value::Array(a) => a.iter().for_each(|c| blob_decls(c, out)),
                _ => {}
            }
        }

        fn legacy_blob(v: &Value) -> Option<String> {
            match v {
                Value::Map(m) => {
                    if v.get("$type").is_none() {
                        if let (Some(Value::Text(c)), Some(Value::Text(_))) = (v.get("cid"), v.get("mimeType")) {
                            if Cid::parse(c).is_ok() {
                                return Some(c.clone());
                            }
                        }
                    }
                    m.iter().find_map(|(_, c)| legacy_blob(c))
                }
                Value::Array(a) => a.iter().find_map(legacy_blob),
                _ => None,
            }
        }

        pub fn encode_record(v: &J, collection: &str, rkey: &str, validate: Option<bool>) -> XResult<Encoded> {
            let J::Object(o) = v else {
                return Err(XrpcError::bad("InvalidRequest", "record must be an object"));
            };
            let defaulted;
            let v = match o.get("$type") {
                None => {
                    let mut o = o.clone();
                    o.insert("$type".into(), J::String(collection.into()));
                    defaulted = J::Object(o);
                    &defaulted
                }
                Some(J::String(t)) if t == collection => v,
                Some(t) => {
                    return Err(XrpcError::bad(
                        "InvalidRequest",
                        format!("Invalid $type: expected {collection}, got {t}"),
                    ))
                }
            };
            let val = Value::from_json(v).map_err(|e| XrpcError::bad("InvalidRequest", e.to_string()))?;
            let status = crate::lexicon::validate_record(collection, rkey, &val, validate, None)
                .map_err(|e| XrpcError::bad("InvalidRequest", e))?;
            if let Some(c) = legacy_blob(&val) {
                return Err(XrpcError::bad("InvalidRequest", format!("Legacy blobs are not allowed ({c})")));
            }
            let bytes = val.to_cbor();
            if bytes.len() > 1_000_000 {
                return Err(XrpcError::bad("InvalidRequest", "record too large"));
            }
            let mut blobs = Vec::new();
            blob_refs(&val, &mut blobs);
            let mut decls = Vec::new();
            blob_decls(&val, &mut decls);
            Ok((Cid::dag_cbor(&bytes), Bytes::from(bytes), blobs, status, decls))
        }

        /// createRecord body -> encoded record, the old way.
        pub fn create(body: &[u8]) -> XResult<Encoded> {
            let nsid = "com.atproto.repo.createRecord";
            let v: J = serde_json::from_slice(body)
                .map_err(|e| XrpcError::bad("InvalidRequest", format!("Invalid JSON body: {e}")))?;
            crate::lexicon::validate_input(nsid, &v).map_err(|e| XrpcError::bad("InvalidRequest", e))?;
            let inp: CreateRecordIn = serde_json::from_value(v)
                .map_err(|e| XrpcError::bad("InvalidRequest", format!("Invalid JSON body: {e}")))?;
            let rkey = inp.rkey.unwrap_or_else(|| "3jui7kd54zh2y".into());
            encode_record(&inp.record, &inp.collection, &rkey, inp.validate)
        }
    }

    /// createRecord body -> encoded record, as the handler does it.
    fn create(body: &RecordBody) -> XResult<Encoded> {
        let mut inp = CreateRecordIn::from_tree(body.parse()?)?;
        let rkey = inp.rkey.take().unwrap_or_else(|| "3jui7kd54zh2y".into());
        encode_record(&mut inp.record, &inp.collection, &rkey, inp.validate, None)
    }

    fn same(a: &XResult<Encoded>, b: &XResult<Encoded>) -> bool {
        match (a, b) {
            (Ok(a), Ok(b)) => a == b,
            (Err(a), Err(b)) => (a.status, &a.error, &a.message) == (b.status, &b.error, &b.message),
            _ => false,
        }
    }

    fn show(r: &XResult<Encoded>) -> String {
        match r {
            Ok(e) => format!("ok {} {:?} {:?}", e.0, e.3, e.4),
            Err(e) => format!("{} {}: {}", e.status, e.error, e.message),
        }
    }

    const POST: &str = r#"{"$type":"app.bsky.feed.post","text":"Check out this thing @alice.bsky.social wrote about merkle search trees https://example.com/mst — really neat","createdAt":"2026-10-01T12:34:56.789Z","langs":["en"],"facets":[{"index":{"byteStart":15,"byteEnd":34},"features":[{"$type":"app.bsky.richtext.facet#mention","did":"did:plc:ewvi7nxzyoun6zhxrhs64oiz"}]},{"index":{"byteStart":75,"byteEnd":99},"features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://example.com/mst"}]}],"reply":{"root":{"uri":"at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/3l3qo2vuowo2b","cid":"bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},"parent":{"uri":"at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/3l3qo2vuowo2b","cid":"bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"}},"embed":{"$type":"app.bsky.embed.images","images":[{"alt":"a diagram of a tree","image":{"$type":"blob","ref":{"$link":"bafkreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},"mimeType":"image/jpeg","size":123456},"aspectRatio":{"width":1200,"height":800}}]}}"#;
    const LIKE: &str = r#"{"$type":"app.bsky.feed.like","subject":{"uri":"at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.post/3l3qo2vuowo2b","cid":"bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},"createdAt":"2026-10-01T12:34:56.789Z"}"#;

    fn body(collection: &str, record: &str, extra: &str) -> Vec<u8> {
        format!(r#"{{"repo":"did:plc:ewvi7nxzyoun6zhxrhs64oiz","collection":"{collection}"{extra},"record":{record}}}"#).into_bytes()
    }

    /// The handler path against the oracle: same CID, bytes, blob refs,
    /// declarations and validation status, or the same error.
    #[test]
    fn record_path_matches_legacy() {
        let post_no_type = POST.replacen(r#""$type":"app.bsky.feed.post","#, "", 1);
        let cases: Vec<(&str, String, &str)> = vec![
            ("app.bsky.feed.post", POST.into(), ""),
            ("app.bsky.feed.like", LIKE.into(), ""),
            ("app.bsky.feed.post", post_no_type.clone(), ""),
            ("app.bsky.feed.like", POST.into(), ""),
            ("app.bsky.feed.post", post_no_type.replace("2026-10-01T12:34:56.789Z", "yesterday"), ""),
            ("app.bsky.feed.post", POST.replace("image/jpeg", "text/html"), ""),
            ("app.bsky.feed.post", POST.replace("123456", "123456.0"), ""),
            ("app.bsky.feed.post", POST.replace("123456", "1.5"), ""),
            ("app.bsky.feed.post", POST.replace(r#""langs":["en"]"#, r#""langs":["en"],"langs":[5]"#), ""),
            ("app.bsky.feed.post", POST.replace(r#""langs":["en"]"#, r#""langs":[5],"langs":["en"]"#), ""),
            ("app.bsky.feed.post", POST.replace(r#""alt":"a diagram of a tree""#, r#""alt":"x","legacy":{"cid":"bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm","mimeType":"image/png"}"#), ""),
            ("app.bsky.feed.post", POST.replace("bafkreie5737", "bafkreiX5737"), ""),
            ("app.bsky.feed.post", POST.into(), r#","validate":false"#),
            ("app.bsky.feed.post", POST.into(), r#","validate":true"#),
            ("com.example.thing", r#"{"a":1.0,"b":{"$bytes":"AQID"},"c":{"$link":"bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"}}"#.into(), ""),
            ("com.example.thing", r#"{"a":1.0}"#.into(), r#","validate":true"#),
            ("com.example.thing", r#"{"$type":5}"#.into(), ""),
            ("com.example.thing", r#"{"$type":"com.example.other"}"#.into(), ""),
            ("com.example.thing", r#""text""#.into(), ""),
            ("com.example.thing", r#"[]"#.into(), ""),
            ("com.example.thing", r#"{"x":{"$link":"bad","y":1}}"#.into(), ""),
            ("com.example.thing", r#"{"x":9223372036854775808}"#.into(), ""),
            ("com.example.thing", r#"{"x":{"$type":"blob","ref":{"$link":"bafkreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},"mimeType":"a/b","size":18446744073709551615}}"#.into(), ""),
            ("com.example.thing", r#"{"z":1.5,"a":{"$link":"bad"}}"#.into(), ""),
            ("com.example.thing", format!(r#"{{"big":"{}"}}"#, "x".repeat(1_000_001)), ""),
        ];
        for (coll, rec, extra) in &cases {
            let b = body(coll, rec, extra);
            let old = legacy::create(&b);
            let new = create(&RecordBody::new("com.atproto.repo.createRecord", b.clone()));
            assert!(same(&old, &new), "{coll} {extra} {}:\n old {}\n new {}", &rec[..rec.len().min(200)], show(&old), show(&new));
        }
        // input-level failures read the same too
        for b in [
            &br#"{"repo":1,"collection":"a.b.c","record":{}}"#[..],
            br#"{"repo":"did:plc:abc","record":{}}"#,
            br#"{"repo":"did:plc:abc","collection":"a.b.c","record":{},"validate":"yes"}"#,
            br#"{"repo":"did:plc:abc","collection":"a.b.c","record":{"#,
            br#"[]"#,
        ] {
            let old = legacy::create(b);
            let new = create(&RecordBody::new("com.atproto.repo.createRecord", b.to_vec()));
            assert!(same(&old, &new), "{}:\n old {}\n new {}", String::from_utf8_lossy(b), show(&old), show(&new));
        }
    }

    /// `cargo test --profile dev-release --lib xrpc::repo::tests::bench_record_path -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_record_path() {
        let n = 300_000u32;
        let run = |name: &str, f: &dyn Fn() -> usize| {
            let mut sink = 0;
            for _ in 0..n / 10 {
                sink += f();
            }
            let t = Instant::now();
            for _ in 0..n {
                sink += f();
            }
            let us = t.elapsed().as_secs_f64() * 1e6 / n as f64;
            println!("{name:44} {us:6.2} us/op ({})", sink % 7);
        };
        for (name, coll, rec) in [("post", "app.bsky.feed.post", POST), ("like", "app.bsky.feed.like", LIKE)] {
            let b = body(coll, rec, "");
            let rb = RecordBody::new("com.atproto.repo.createRecord", b.clone());
            assert!(same(&legacy::create(&b), &create(&rb)));
            run(&format!("{name} createRecord body -> record: old"), &|| legacy::create(&b).ok().unwrap().1.len());
            run(&format!("{name} createRecord body -> record: new"), &|| create(&rb).ok().unwrap().1.len());
            run(&format!("{name}   parse body tree"), &|| rb.parse().ok().unwrap().get("record").is_some() as usize);
            run(&format!("{name}   parse + input lexicon"), &|| CreateRecordIn::from_tree(rb.parse().ok().unwrap()).ok().unwrap().repo.len());
            let mut rec = CreateRecordIn::from_tree(rb.parse().ok().unwrap()).ok().unwrap().record;
            let mut out = Vec::new();
            rec.encode_record(&mut out, &mut Default::default()).unwrap();
            run(&format!("{name}   encode only"), &|| {
                let mut r = rec.clone();
                let mut out = Vec::with_capacity(512);
                r.encode_record(&mut out, &mut Default::default()).unwrap();
                out.len()
            });
            run(&format!("{name}   clone only"), &|| matches!(rec.clone(), JsonValue::Object(_)) as usize);
            run(&format!("{name}   record lexicon only"), &|| {
                crate::lexicon::validate_record(coll, "3jui7kd54zh2y", &rec, None, None).unwrap().unwrap().len()
            });
            run(&format!("{name}   sha256 cid only"), &|| Cid::dag_cbor(&out).digest[0] as usize);
        }
    }
}
