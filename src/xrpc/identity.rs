use super::*;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route(
            "/xrpc/com.atproto.identity.resolveHandle",
            get(resolve_handle),
        )
        .route("/xrpc/com.atproto.identity.resolveDid", get(resolve_did))
        .route(
            "/xrpc/com.atproto.identity.resolveIdentity",
            get(resolve_identity),
        )
        .route(
            "/xrpc/com.atproto.identity.refreshIdentity",
            post(refresh_identity),
        )
        .route(
            "/xrpc/com.atproto.identity.updateHandle",
            post(update_handle),
        )
        .route(
            "/xrpc/com.atproto.identity.getRecommendedDidCredentials",
            get(get_recommended_did_credentials),
        )
        .route(
            "/xrpc/com.atproto.identity.requestPlcOperationSignature",
            post(plc_unsupported),
        )
        .route(
            "/xrpc/com.atproto.identity.signPlcOperation",
            post(plc_unsupported),
        )
        .route(
            "/xrpc/com.atproto.identity.submitPlcOperation",
            post(plc_unsupported),
        )
}

/// The DID document of a local account (also used by describeRepo).
pub(super) fn did_doc(app: &App, acct: &Account) -> XResult<J> {
    let key = Keypair::from_bytes(&hex::decode(&acct.signing_key).map_err(XrpcError::from_err)?)
        .map_err(XrpcError::from_err)?;
    Ok(json!({
        "@context": ["https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1", "https://w3id.org/security/suites/secp256k1-2019/v1"],
        "id": acct.did,
        "alsoKnownAs": [format!("at://{}", acct.handle)],
        "verificationMethod": [{
            "id": format!("{}#atproto", acct.did),
            "type": "Multikey",
            "controller": acct.did,
            "publicKeyMultibase": key.public_multibase(),
        }],
        "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": app.public_url}],
    }))
}

#[derive(Deserialize)]
struct HandleQ {
    handle: String,
}

async fn resolve_handle(State(app): AppState, Query(q): Query<HandleQ>) -> XResult<Json<J>> {
    let handle = q.handle.to_ascii_lowercase();
    if !super::syntax::valid_handle(&handle) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Error: handle must be a valid handle",
        ));
    }
    // Like the reference's getAccount(handle): deactivated and taken-down
    // accounts don't resolve.
    let did = app.resolve_handle(&handle).await?;
    let active = match &did {
        Some(d) => app.account(d).await.is_ok_and(|a| a.status.is_none()),
        None => false,
    };
    match did {
        Some(did) if active => Ok(Json(json!({"did": did}))),
        _ => Err(XrpcError::bad("HandleNotFound", "Unable to resolve handle")),
    }
}

/// Local accounts only: these DIDs are minted here and not registered with
/// PLC, so this server is the only place they resolve. Anything else is
/// DidNotFound.
async fn local_account(app: &App, did: &str) -> XResult<Account> {
    match app.account(did).await {
        Ok(a) => Ok(a),
        Err(e) if e.error == "AccountNotFound" => Err(XrpcError::bad(
            "DidNotFound",
            format!("DID not found: {did}"),
        )),
        Err(e) => Err(e),
    }
}

#[derive(Deserialize)]
struct DidQ {
    did: String,
}

async fn resolve_did(State(app): AppState, Query(q): Query<DidQ>) -> XResult<Json<J>> {
    if !super::syntax::valid_did(&q.did) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "Error: did must be a valid did",
        ));
    }
    let acct = local_account(&app, &q.did).await?;
    Ok(Json(json!({"didDoc": did_doc(&app, &acct)?})))
}

/// identifier (handle or DID) -> (did, handle, didDoc). The handle is
/// "handle.invalid" unless it resolves back to the DID.
async fn identity_info(app: &App, identifier: &str) -> XResult<(Account, J)> {
    let acct = if identifier.starts_with("did:") {
        if !super::syntax::valid_did(identifier) {
            return Err(XrpcError::bad(
                "InvalidRequest",
                "Error: identifier must be a valid at-identifier",
            ));
        }
        local_account(app, identifier).await?
    } else {
        let handle = identifier.to_ascii_lowercase();
        if !super::syntax::valid_handle(&handle) {
            return Err(XrpcError::bad(
                "InvalidRequest",
                "Error: identifier must be a valid at-identifier",
            ));
        }
        let did = app.resolve_handle(&handle).await?.ok_or_else(|| {
            XrpcError::bad(
                "HandleNotFound",
                format!("Unable to resolve handle: {handle}"),
            )
        })?;
        local_account(app, &did).await?
    };
    let handle = match app.resolve_handle(&acct.handle).await? {
        Some(d) if d == acct.did => acct.handle.clone(),
        _ => "handle.invalid".to_string(),
    };
    let info = json!({"did": acct.did, "handle": handle, "didDoc": did_doc(app, &acct)?});
    Ok((acct, info))
}

#[derive(Deserialize)]
struct IdentifierQ {
    identifier: String,
}

async fn resolve_identity(State(app): AppState, Query(q): Query<IdentifierQ>) -> XResult<Json<J>> {
    Ok(Json(identity_info(&app, &q.identifier).await?.1))
}

/// Returns the identity info. When called by the account itself (or an
/// admin) it also emits a `#identity` event so downstream caches refresh;
/// anonymous callers can't make us emit events.
async fn refresh_identity(
    State(app): AppState,
    MaybeAuth(creds): MaybeAuth,
    Json(inp): Json<IdentifierQ>,
) -> XResult<Json<J>> {
    let (acct, info) = identity_info(&app, &inp.identifier).await?;
    let may_emit = match &creds {
        Some(Credentials::Admin) => true,
        Some(c) => c.did() == Some(acct.did.as_str()) && c.allows_identity("*"),
        None => false,
    };
    if may_emit && acct.status.as_deref() != Some("takendown") {
        let did = acct.did.clone();
        let op = crate::worker::AccountOp::Update {
            account: acct,
            old_handle: None,
            identity_event: true,
            account_event: false,
        };
        app.account_op(&did, op).await?;
    }
    Ok(Json(info))
}

#[derive(Deserialize)]
struct UpdateHandleIn {
    handle: String,
}

fn handle_object(app: &App, handle: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/handle/{}", app.store.prefix, handle))
}

/// Checks a requested handle: a single label under our handle domain
/// (3-18 chars, like the reference), or an external domain that proves
/// control with `https://{handle}/.well-known/atproto-did`. In dev mode the
/// external proof is skipped. (DNS TXT `_atproto` verification isn't
/// implemented: no DNS resolver dependency.)
async fn check_new_handle(app: &App, handle: &str, did: &str) -> XResult<()> {
    if !super::syntax::valid_handle(handle) {
        return Err(XrpcError::bad(
            "InvalidHandle",
            "Input/handle must be a valid handle",
        ));
    }
    let suffix = format!(".{}", app.handle_domain);
    if handle.ends_with(&suffix) {
        // same rules as createAccount, reserved names included
        return super::server::ensure_service_handle(app, handle, false);
    }
    if app.config.dev_mode {
        return Ok(());
    }
    let url = format!("https://{handle}/.well-known/atproto-did");
    let resolved = async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .ok()?;
        let r = client.get(&url).send().await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        let body = r.text().await.ok()?;
        Some(body.lines().next().unwrap_or("").trim().to_string())
    }
    .await;
    if resolved.as_deref() != Some(did) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "External handle did not resolve to DID",
        ));
    }
    Ok(())
}

async fn update_handle(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateHandleIn>,
) -> XResult<StatusCode> {
    creds.require(creds.allows_identity("handle"))?;
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?
        .to_string();
    {
        use crate::ratelimit::*;
        check(&[&UPDATE_HANDLE_5MIN, &UPDATE_HANDLE_DAY], &did, 1)?;
    }
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
    let handle = inp.handle.trim().to_ascii_lowercase();
    let old = acct.handle.clone();
    if handle != old {
        check_new_handle(&app, &handle, &did).await?;
        // Global uniqueness: conditional create of handle/{handle}. An object
        // already holding our DID is a retry of an interrupted update.
        let path = handle_object(&app, &handle);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match app
            .store
            .raw
            .put_opts(&path, PutPayload::from(did.clone().into_bytes()), opts)
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                if app.resolve_handle(&handle).await?.as_deref() != Some(did.as_str()) {
                    return Err(XrpcError::bad("HandleNotAvailable", "Handle already taken"));
                }
            }
            Err(e) => return Err(XrpcError::from_err(e)),
        }
        let mut next = acct.clone();
        next.handle = handle.clone();
        let op = crate::worker::AccountOp::Update {
            account: next,
            old_handle: Some(old.clone()),
            identity_event: true,
            account_event: false,
        };
        if let Err(e) = app.account_op(&did, op).await {
            let _ = app.store.raw.delete(&path).await;
            return Err(e);
        }
        // Release the old claim (only if it's still ours).
        if app.resolve_handle(&old).await.ok().flatten().as_deref() == Some(did.as_str()) {
            if let Err(e) = app.store.raw.delete(&handle_object(&app, &old)).await {
                tracing::warn!(%did, %old, "updateHandle: failed to release old handle: {e}");
            }
        }
    } else {
        // Same handle: the reference still re-announces it.
        let op = crate::worker::AccountOp::Update {
            account: acct,
            old_handle: None,
            identity_event: true,
            account_event: false,
        };
        app.account_op(&did, op).await?;
    }
    Ok(StatusCode::OK)
}

async fn get_recommended_did_credentials(
    State(app): AppState,
    Auth(creds): Auth,
) -> XResult<Json<J>> {
    let did = creds
        .did()
        .ok_or_else(|| XrpcError::auth("user credentials required"))?
        .to_string();
    let acct = app.account(&did).await?;
    let key = Keypair::from_bytes(&hex::decode(&acct.signing_key).map_err(XrpcError::from_err)?)
        .map_err(XrpcError::from_err)?;
    Ok(Json(json!({
        "alsoKnownAs": [format!("at://{}", acct.handle)],
        "verificationMethods": {"atproto": key.did_key()},
        // This PDS holds no PLC rotation key (DIDs are minted locally and
        // never registered), so it recommends none.
        "rotationKeys": [],
        "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": app.public_url}},
    })))
}

/// requestPlcOperationSignature / signPlcOperation / submitPlcOperation.
/// Accounts here use locally minted did:plc identifiers that were never
/// registered with a PLC directory and this server has no rotation key, so
/// there is no PLC log to extend.
async fn plc_unsupported() -> XrpcError {
    XrpcError {
        status: StatusCode::NOT_IMPLEMENTED,
        error: "MethodNotImplemented".into(),
        message: "PLC operations are not supported: this PDS mints did:plc identifiers locally without registering them with a PLC directory".into(),
    }
}
