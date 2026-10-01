use super::*;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
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
        )
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

/// An encoded record: (cid, DAG-CBOR bytes, blob refs, validation status).
type Encoded = (Cid, Bytes, Vec<Cid>, crate::lexicon::ValidationStatus);

/// JSON record -> DAG-CBOR, as the reference's prepareWrite: a missing
/// `$type` defaults to the collection and any other value must equal it,
/// then known lexicons are validated (record key included).
fn encode_record(v: &J, collection: &str, rkey: &str, validate: Option<bool>) -> XResult<Encoded> {
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
    let status = crate::lexicon::validate_record(collection, rkey, &val, validate)
        .map_err(|e| XrpcError::bad("InvalidRequest", e))?;
    if let Some(c) = legacy_blob(&val) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            format!("Legacy blobs are not allowed ({c})"),
        ));
    }
    let bytes = val.to_cbor();
    if bytes.len() > 1_000_000 {
        return Err(XrpcError::bad("InvalidRequest", "record too large"));
    }
    let mut blobs = Vec::new();
    blob_refs(&val, &mut blobs);
    Ok((Cid::dag_cbor(&bytes), Bytes::from(bytes), blobs, status))
}

/// Adds `validationStatus` unless validation was skipped.
fn with_status(mut out: J, status: crate::lexicon::ValidationStatus) -> J {
    if let Some(st) = status {
        out["validationStatus"] = json!(st);
    }
    out
}

/// A legacy blob ref (`{"cid": "<cid>", "mimeType": "..."}`) anywhere in the
/// record; the reference refuses to create new ones (prepare.ts).
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

/// Every blob a write references must have been uploaded by the repo and not
/// be taken down (reference: processWriteBlobs -> "Could not find blob").
async fn check_blobs<'a>(app: &App, did: &str, blobs: impl IntoIterator<Item = &'a Cid>) -> XResult<()> {
    for cid in blobs {
        let missing = || XrpcError::bad("BlobNotFound", format!("Could not find blob: {cid}"));
        if super::admin::is_blob_takendown(app, did, &cid.to_string()).await {
            return Err(missing());
        }
        match app.store.raw.head(&super::blobs::blob_path(app, did, cid)).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => return Err(missing()),
            Err(e) => return Err(XrpcError::from_err(e)),
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateRecordIn {
    repo: String,
    collection: String,
    rkey: Option<String>,
    record: J,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

async fn create_record(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<CreateRecordIn>,
) -> XResult<Json<J>> {
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::CREATE_POINTS)?;
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    creds.require(creds.allows_repo(&inp.collection, "create"))?;
    check_path(&inp.collection, inp.rkey.as_deref())?;
    // as the reference: no rkey = a fresh TID (validated against the schema's key)
    let rkey = inp.rkey.unwrap_or_else(|| app.tids.next().to_string());
    let (cid, bytes, blobs, status) =
        encode_record(&inp.record, &inp.collection, &rkey, inp.validate)?;
    check_blobs(&app, &did, &blobs).await?;
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PutRecordIn {
    repo: String,
    collection: String,
    rkey: String,
    record: J,
    #[serde(default, deserialize_with = "nullable")]
    swap_record: Option<Option<String>>,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

/// Distinguishes an absent field (None) from an explicit null (Some(None)).
fn nullable<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Ok(Some(Option::<String>::deserialize(d)?))
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
    Json(inp): Json<PutRecordIn>,
) -> XResult<Json<J>> {
    crate::ratelimit::check_repo_write(creds.did(), crate::ratelimit::UPDATE_POINTS)?;
    let did = authed_repo(&app, &creds, &inp.repo).await?;
    creds.require(
        creds.allows_repo(&inp.collection, "create")
            && creds.allows_repo(&inp.collection, "update"),
    )?;
    check_path(&inp.collection, Some(&inp.rkey))?;
    let (cid, bytes, blobs, status) =
        encode_record(&inp.record, &inp.collection, &inp.rkey, inp.validate)?;
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
    check_blobs(&app, &did, &blobs).await?;
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApplyWritesIn {
    repo: String,
    writes: Vec<J>,
    swap_commit: Option<String>,
    validate: Option<bool>,
}

async fn apply_writes(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<ApplyWritesIn>,
) -> XResult<Json<J>> {
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
    for w in &inp.writes {
        let t = w.get("$type").and_then(|v| v.as_str()).unwrap_or("");
        let collection = w
            .get("collection")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let rkey = w.get("rkey").and_then(|v| v.as_str()).map(String::from);
        let action = match t {
            "com.atproto.repo.applyWrites#create" => "create",
            "com.atproto.repo.applyWrites#update" => "update",
            _ => "delete",
        };
        creds.require(creds.allows_repo(&collection, action))?;
        check_path(&collection, rkey.as_deref())?;
        match t {
            "com.atproto.repo.applyWrites#create" => {
                let rkey = rkey.unwrap_or_else(|| app.tids.next().to_string());
                let (cid, bytes, blobs, status) =
                    encode_record(w.get("value").unwrap_or(&J::Null), &collection, &rkey, inp.validate)?;
                statuses.push(status);
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
                let (cid, bytes, blobs, status) =
                    encode_record(w.get("value").unwrap_or(&J::Null), &collection, &rkey, inp.validate)?;
                statuses.push(status);
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
    for w in &writes {
        if let Write::Create { blobs, .. } | Write::Update { blobs, .. } = w {
            check_blobs(&app, &did, blobs).await?;
        }
    }
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

async fn get_record(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Query(q): Query<GetRecordQ>,
) -> XResult<Json<J>> {
    check_path(&q.collection, Some(&q.rkey))?;
    let did = app.resolve_repo(&q.repo).await?;
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
    if super::admin::is_takendown(&app, &uri(&did, &path)).await {
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
    let value = Value::decode(&bytes).map_err(XrpcError::from_err)?;
    Ok(Json(
        json!({"uri": uri(&did, &path), "cid": cid.to_string(), "value": value.to_json()}),
    ))
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
) -> XResult<Json<J>> {
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
    let mut iter =
        p.db.scan_with_options(lo..hi, &opts)
            .await
            .map_err(XrpcError::from_err)?;
    let mut records = Vec::new();
    let mut last_rkey = None;
    while records.len() < limit {
        let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
            break;
        };
        let rkey = String::from_utf8_lossy(&kv.key[prefix.len()..]).to_string();
        let rec_uri = uri(&did, &format!("{}/{}", q.collection, rkey));
        if super::admin::is_takendown(&app, &rec_uri).await {
            last_rkey = Some(rkey);
            continue;
        }
        let (cid, bytes) = state::decode_record_value(&kv.value).map_err(XrpcError::from_err)?;
        let value = Value::decode(&bytes).map_err(XrpcError::from_err)?;
        records.push(json!({"uri": rec_uri, "cid": cid.to_string(), "value": value.to_json()}));
        last_rkey = Some(rkey);
    }
    let mut out = json!({"records": records});
    if records_full(&out, limit) {
        out["cursor"] = json!(last_rkey);
    }
    Ok(Json(out))
}

fn records_full(out: &J, limit: usize) -> bool {
    out["records"]
        .as_array()
        .map(|a| a.len() == limit)
        .unwrap_or(false)
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

/// Replaces the caller's repo with the contents of a CAR (one root: a signed
/// commit whose `did` is the caller). The MST is loaded from the CAR and
/// checked complete; every record block must hash to its CID. Emits `#sync`
/// via the worker (new rev, re-signed with our key). Like the reference, the
/// imported commit's signature isn't checked: only its contents are used.
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
    let records = tokio::task::spawn_blocking(move || parse_import(&body, &did).map(|r| (did, r)))
        .await
        .map_err(XrpcError::from_err)?;
    let (did, records) = records?;
    app.account_op(&did, crate::worker::AccountOp::ReplaceRepo { records })
        .await?;
    Ok(StatusCode::OK)
}

type ImportedRecord = (String, Cid, Bytes, Vec<Cid>);

fn parse_import(body: &[u8], did: &str) -> XResult<Vec<ImportedRecord>> {
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
    let commit_did = commit.get("did").and_then(|v| v.as_str()).unwrap_or("");
    if commit_did != did {
        return Err(bad(format!("commit is for {commit_did}, not {did}")));
    }
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
