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
            post(request_plc_operation_signature),
        )
        .route(
            "/xrpc/com.atproto.identity.signPlcOperation",
            post(sign_plc_operation),
        )
        .route(
            "/xrpc/com.atproto.identity.submitPlcOperation",
            post(submit_plc_operation),
        )
        .route("/.well-known/atproto-did", get(well_known_atproto_did))
}

/// HTTPS handle verification for handles under our domain (the reference's
/// well-known.ts): the request's Host is the handle; its DID as text/plain,
/// or 404 unless it is an active account here.
async fn well_known_atproto_did(State(app): AppState, headers: HeaderMap) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let handle = match host.rsplit_once(':') {
        Some((h, port)) if port.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => host,
    }
    .to_ascii_lowercase();
    let not_found = || (StatusCode::NOT_FOUND, "User not found").into_response();
    if !handle.ends_with(&format!(".{}", app.handle_domain)) {
        return not_found();
    }
    let Ok(Some(did)) = app.resolve_handle(&handle).await else {
        return not_found();
    };
    match super::internal::account_anywhere(&app, &did).await {
        Ok(a) if a.status.is_none() && a.handle == handle => {
            ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], did).into_response()
        }
        // owner unreachable / shard moving: retry, not "no such user"
        Err(e) if e.status.is_server_error() => e.into_response(),
        _ => not_found(),
    }
}

/// The DID document of a local account (also used by describeRepo).
pub(super) fn did_doc(app: &App, acct: &Account) -> XResult<J> {
    Ok(json!({
        "@context": ["https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1", "https://w3id.org/security/suites/secp256k1-2019/v1"],
        "id": acct.did,
        "alsoKnownAs": [format!("at://{}", acct.handle)],
        "verificationMethod": [{
            "id": format!("{}#atproto", acct.did),
            "type": "Multikey",
            "controller": acct.did,
            "publicKeyMultibase": acct.signing_pubkey,
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
    // a shard mid-move is a 503 (retry), not "no such handle"
    let active = match &did {
        Some(d) => super::server::account_if_exists(&app, d)
            .await?
            .is_some_and(|a| a.status.is_none()),
        None => false,
    };
    match did {
        Some(did) if active => Ok(Json(json!({"did": did}))),
        _ => Err(XrpcError::bad("HandleNotFound", "Unable to resolve handle")),
    }
}

/// Local accounts only (their documents are generated here; with PLC
/// registration on, the same document the directory serves for an account
/// hosted here). Anything else is DidNotFound.
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
    if may_emit {
        // writes nothing: the worker re-announces its current account (a
        // snapshot sent from here could undo a concurrent takedown)
        app.mutate_account(&acct.did, true, false, false, |a| {
            Ok(a.status.as_deref() != Some("takendown"))
        })
        .await?;
    }
    Ok(Json(info))
}

#[derive(Deserialize)]
struct UpdateHandleIn {
    handle: String,
}

/// Checks a requested handle: a single label under our handle domain
/// (3-18 chars, like the reference), or an external domain that proves
/// control with `https://{handle}/.well-known/atproto-did`. In dev mode the
/// external proof is skipped. (DNS TXT `_atproto` verification isn't
/// implemented: no DNS resolver dependency.)
pub(super) async fn check_new_handle(app: &App, handle: &str, did: &str) -> XResult<()> {
    // syntax + disallowed TLDs, then the slur filter (reference order)
    super::server::normalize_handle(handle)?;
    super::server::ensure_no_slur(handle)?;
    let suffix = format!(".{}", app.handle_domain);
    if handle.ends_with(&suffix) {
        // same rules as createAccount, reserved names included
        return super::server::ensure_service_handle(app, handle, false);
    }
    if app.config.dev_mode {
        return Ok(());
    }
    let resolved = well_known_did(handle, false).await.ok();
    if resolved.as_deref() != Some(did) {
        return Err(XrpcError::bad(
            "InvalidRequest",
            "External handle did not resolve to DID",
        ));
    }
    Ok(())
}

/// The DID served at `https://{handle}/.well-known/atproto-did`, fetched with
/// the SSRF-guarded client (outside dev mode a handle resolving to a private
/// or loopback address is refused before connecting), a 5 s deadline and a
/// small body cap.
async fn well_known_did(handle: &str, dev_mode: bool) -> Result<String, String> {
    use futures::StreamExt;
    const MAX_BYTES: usize = 2048;
    let url = format!("https://{handle}/.well-known/atproto-did");
    let fetch = async {
        let r = crate::http::guarded(dev_mode).get(&url).send().await.map_err(|e| format!("{e:?}"))?;
        if !r.status().is_success() {
            return Err(format!("status {}", r.status()));
        }
        let mut buf = Vec::new();
        let mut s = r.bytes_stream();
        while let Some(c) = s.next().await {
            buf.extend_from_slice(&c.map_err(|e| e.to_string())?);
            if buf.len() > MAX_BYTES {
                return Err("response too large".into());
            }
        }
        let body = String::from_utf8_lossy(&buf);
        Ok(body.lines().next().unwrap_or("").trim().to_string())
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), fetch)
        .await
        .map_err(|_| "timed out".to_string())?
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
    // early refusal; set_handle re-checks against the worker's state
    let acct = app.account(&did).await?;
    if super::server::is_takendown_account(&acct) {
        return Err(super::server::takedown_error());
    }
    let handle = inp.handle.trim().to_ascii_lowercase();
    if handle != acct.handle {
        // the slow part (external .well-known proof) runs before the op
        check_new_handle(&app, &handle, &did).await?;
    }
    // same handle: the reference still re-announces it
    set_handle(&app, &did, &handle, true).await?;
    Ok(StatusCode::OK)
}

/// Moves the account to `handle` (already validated) and emits #identity.
/// Global uniqueness is a conditional create of handle/{handle} (an object
/// already holding our DID is a retry of an interrupted update). The worker
/// then swaps the handle on its current state, and the handle that state held
/// is released. `user`: the account's own request, refused while taken down
/// or suspended (admins may rename those).
pub(super) async fn set_handle(app: &App, did: &str, handle: &str, user: bool) -> XResult<()> {
    // only decides whether to claim; the op re-checks against current state
    let read = app.account(did).await?.handle;
    let claimed = handle != read;
    if claimed && !super::server::claim_handle(app, handle, did).await? {
        return Err(XrpcError::bad("HandleNotAvailable", format!("Handle already taken: {handle}")));
    }
    // The DID document first (reference AccountManager.updateHandle): a PLC
    // update op, acknowledged by the directory, before the local change; a
    // failure changes nothing here. (The reverse failure, PLC updated and
    // the local swap failing, is fixed by retrying: the PLC step is then a
    // no-op.)
    if let Err(e) = update_did_doc_handle(app, did, handle).await {
        if claimed {
            super::server::release_handle(app, handle, did).await;
        }
        return Err(e);
    }
    let h = handle.to_string();
    let res = app
        .mutate_account(did, true, false, false, move |a| {
            if user && super::server::is_takendown_account(a) {
                return Err(super::server::takedown_error());
            }
            if a.handle != h && !claimed {
                // renamed since the read above: we hold no claim on `h`
                return Err(XrpcError::bad("InvalidRequest", "Handle changed concurrently, retry"));
            }
            a.handle = h;
            Ok(true)
        })
        .await;
    match res {
        Ok((before, _)) => {
            if before.handle != handle {
                super::server::release_handle(app, &before.handle, did).await;
            }
            Ok(())
        }
        Err(e) => {
            // keep the claim if a concurrent update moved us onto it anyway
            if claimed && app.account(did).await.map_or(true, |a| a.handle != handle) {
                super::server::release_handle(app, handle, did).await;
            }
            Err(e)
        }
    }
}

/// With PLC registration on: a did:plc gets a PLC update op for `handle`
/// (signed with the server rotation key); a did:web must already name the
/// handle in its document. Off: nothing (the document is generated here).
async fn update_did_doc_handle(app: &App, did: &str, handle: &str) -> XResult<()> {
    let Some(plc) = &app.plc else { return Ok(()) };
    if did.starts_with("did:plc:") {
        plc.update_handle(did, handle).await?;
    } else {
        app.did_resolver.invalidate(did);
        let doc = app.did_resolver.resolve(did).await.map_err(|e| XrpcError::bad("InvalidRequest", e.to_string()))?;
        let at = doc["alsoKnownAs"].as_array().and_then(|a| a.iter().filter_map(J::as_str).find(|h| h.starts_with("at://")));
        if at != Some(format!("at://{handle}").as_str()) {
            return Err(XrpcError::bad("InvalidRequest", "DID is not properly configured for handle"));
        }
    }
    app.did_resolver.invalidate(did);
    Ok(())
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
    Ok(Json(json!({
        "alsoKnownAs": [format!("at://{}", acct.handle)],
        "verificationMethods": {"atproto": format!("did:key:{}", acct.signing_pubkey)},
        // [server recovery key?, server rotation key]; none when PLC
        // registration is off (no rotation key here)
        "rotationKeys": app.plc.as_ref().map(|p| p.recommended_rotation_keys()).unwrap_or_default(),
        "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": app.public_url}},
    })))
}

/// The PLC operation endpoints when PLC registration is off
/// (`--plc-mode unregistered`, dev only): there is no rotation key and no
/// PLC log to extend.
fn plc_unsupported() -> XrpcError {
    XrpcError {
        status: StatusCode::NOT_IMPLEMENTED,
        error: "MethodNotImplemented".into(),
        message: "PLC operations are not supported: PLC registration is off on this PDS (--plc-mode unregistered, dev only)".into(),
    }
}

fn plc_service(app: &App) -> XResult<&Arc<crate::plc::Plc>> {
    app.plc.as_ref().ok_or_else(plc_unsupported)
}

/// requestPlcOperationSignature / signPlcOperation auth (reference
/// ACCESS_FULL plus taken-down sessions, `identity:*`): a full session, a
/// taken-down account's restricted session, or OAuth with `identity:*`; no
/// app passwords.
fn plc_signer(creds: &Credentials) -> XResult<String> {
    match creds {
        Credentials::Session { did } | Credentials::Takendown { did } => Ok(did.clone()),
        Credentials::OAuth { did, .. } => {
            creds.require(creds.allows_identity("*"))?;
            Ok(did.clone())
        }
        Credentials::AppPassword { .. } => Err(XrpcError {
            status: StatusCode::BAD_REQUEST,
            error: "InvalidToken".into(),
            message: "Bad token scope".into(),
        }),
        Credentials::Admin => Err(XrpcError::auth("user credentials required")),
    }
}

/// Mails a `plc_operation` token (reference requestPlcOperationSignature):
/// deactivated and taken-down accounts too.
async fn request_plc_operation_signature(State(app): AppState, Auth(creds): Auth) -> XResult<StatusCode> {
    plc_service(&app)?;
    let did = plc_signer(&creds)?;
    let acct = app.account(&did).await.map_err(|_| XrpcError::bad("InvalidRequest", "account not found"))?;
    let email = acct.email.clone().ok_or_else(|| XrpcError::bad("InvalidRequest", "account does not have an email address"))?;
    let token = super::server::create_email_token(&app, &did, "plc_operation").await?;
    super::server::deliver(
        &app,
        &email,
        "PLC Update Operation Requested",
        &format!("We received a request to update your PLC. Your confirmation code is {token}"),
        "plc_operation",
        Some(&token),
    );
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignPlcIn {
    token: Option<String>,
    rotation_keys: Option<J>,
    also_known_as: Option<J>,
    verification_methods: Option<J>,
    services: Option<J>,
}

/// Signs an update of the account's DID with the server rotation key
/// (reference signPlcOperation): the emailed token, then the DID's last op
/// with the requested fields replaced (`createUpdateOp`). Not submitted:
/// the client sends it to the PDS that will host the account (migration
/// out) or to submitPlcOperation here.
async fn sign_plc_operation(State(app): AppState, Auth(creds): Auth, Json(inp): Json<SignPlcIn>) -> XResult<Json<J>> {
    let plc = plc_service(&app)?.clone();
    let did = plc_signer(&creds)?;
    let token = inp
        .token
        .as_deref()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| XrpcError::bad("InvalidRequest", "email confirmation token required to sign PLC operations"))?;
    super::server::assert_email_token(&app, &did, "plc_operation", token).await?;
    super::server::delete_email_tokens(&app, &did, &["plc_operation"]).await?;
    if !did.starts_with("did:plc:") {
        return Err(XrpcError::bad("InvalidRequest", format!("not a did:plc: {did}")));
    }
    let last = plc.last_op(&did).await?;
    // (the reference casts the requested fields without checking them; an
    // op of the wrong shape is refused here rather than signed)
    let operation = plc.update_op(&last, |m| {
        for (k, v) in [
            ("rotationKeys", inp.rotation_keys),
            ("alsoKnownAs", inp.also_known_as),
            ("verificationMethods", inp.verification_methods),
            ("services", inp.services),
        ] {
            if let Some(v) = v {
                m.insert(k.into(), v);
            }
        }
        Ok(())
    })?;
    Ok(Json(json!({"operation": operation})))
}

#[derive(Deserialize)]
struct SubmitPlcIn {
    operation: J,
}

/// Forwards a signed operation for the caller's DID to the PLC directory
/// after the reference's checks (submitPlcOperation): the server's rotation
/// key stays a rotation key, the atproto_pds service is this PDS, the
/// atproto key is the account's signing key, and the first alias is the
/// account's handle. Then #identity.
async fn submit_plc_operation(State(app): AppState, Auth(creds): Auth, Json(inp): Json<SubmitPlcIn>) -> XResult<StatusCode> {
    creds.require(creds.allows_identity("*"))?;
    let did = creds.did().ok_or_else(|| XrpcError::auth("user credentials required"))?.to_string();
    let plc = plc_service(&app)?.clone();
    let op = inp.operation;
    let bad = |m: &str| XrpcError::bad("InvalidRequest", m);
    if crate::plc::op_type(&op, true).ok() != Some(crate::plc::OpType::Operation) {
        return Err(bad("Invalid operation"));
    }
    if !op["rotationKeys"].as_array().is_some_and(|a| a.iter().any(|k| k == plc.rotation_did_key())) {
        return Err(bad("Rotation keys do not include server's rotation key"));
    }
    let pds = &op["services"]["atproto_pds"];
    if pds["type"] != "AtprotoPersonalDataServer" {
        return Err(bad("Incorrect type on atproto_pds service"));
    }
    if pds["endpoint"] != app.public_url.as_str() {
        return Err(bad("Incorrect endpoint on atproto_pds service"));
    }
    let acct = app.account(&did).await?;
    if op["verificationMethods"]["atproto"] != format!("did:key:{}", acct.signing_pubkey).as_str() {
        return Err(bad("Incorrect signing key"));
    }
    if !acct.handle.is_empty() && op["alsoKnownAs"].get(0) != Some(&J::String(format!("at://{}", acct.handle))) {
        return Err(bad("Incorrect handle in alsoKnownAs"));
    }
    if !did.starts_with("did:plc:") {
        return Err(bad(&format!("not a did:plc: {did}")));
    }
    plc.client.send(&did, &op, "submit").await?;
    app.did_resolver.invalidate(&did);
    // #identity (writes nothing; not for a taken-down account, as refreshIdentity)
    app.mutate_account(&did, true, false, false, |a| Ok(a.status.as_deref() != Some("takendown"))).await?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn well_known_check_refuses_private_hosts() {
        // "localhost" resolves to loopback only: refused at resolution,
        // before any connection
        let e = well_known_did("localhost", false).await.unwrap_err();
        assert!(e.contains("public unicast"), "{e}");
    }
}
