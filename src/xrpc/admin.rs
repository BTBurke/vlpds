//! com.atproto.admin.* (Basic `admin:<token>` auth), invite-code storage,
//! subject takedowns (`is_takendown` for other modules), the dev-mode
//! mailbox (vlpds.admin.getDevMail) and the vlpds.admin.bulkCreate simulator.

use super::authn::Credentials;
use super::server::{
    delete_account_fully, ensure_loaded, ext, get_json, invalid_request, normalize_handle, pmut,
    recompute_status, scan_private, scan_private_routing, set_deactivated,
    set_email, set_extra, to_json_bytes, update_account, NEW_PASSWORD_MAX_LENGTH, TAKEDOWNS,
};
use super::*;
use crate::worker::AccountOp;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/vlpds.admin.bulkCreate", post(bulk_create))
        .route("/xrpc/vlpds.admin.getDevMail", get(get_dev_mail))
        .route(
            "/xrpc/com.atproto.admin.getAccountInfo",
            get(get_account_info),
        )
        .route(
            "/xrpc/com.atproto.admin.getAccountInfos",
            get(get_account_infos),
        )
        .route(
            "/xrpc/com.atproto.admin.searchAccounts",
            get(search_accounts),
        )
        .route(
            "/xrpc/com.atproto.admin.updateAccountHandle",
            post(update_account_handle),
        )
        .route(
            "/xrpc/com.atproto.admin.updateAccountEmail",
            post(update_account_email),
        )
        .route(
            "/xrpc/com.atproto.admin.updateAccountPassword",
            post(update_account_password),
        )
        .route(
            "/xrpc/com.atproto.admin.updateAccountSigningKey",
            post(update_account_signing_key),
        )
        .route(
            "/xrpc/com.atproto.admin.updateSubjectStatus",
            post(update_subject_status),
        )
        .route(
            "/xrpc/com.atproto.admin.getSubjectStatus",
            get(get_subject_status),
        )
        .route(
            "/xrpc/com.atproto.admin.deleteAccount",
            post(delete_account),
        )
        .route(
            "/xrpc/com.atproto.admin.disableAccountInvites",
            post(disable_account_invites),
        )
        .route(
            "/xrpc/com.atproto.admin.enableAccountInvites",
            post(enable_account_invites),
        )
        .route(
            "/xrpc/com.atproto.admin.disableInviteCodes",
            post(disable_invite_codes),
        )
        .route(
            "/xrpc/com.atproto.admin.getInviteCodes",
            get(get_invite_codes),
        )
        .route("/xrpc/com.atproto.admin.sendEmail", post(send_email))
}

fn require_admin(creds: &Credentials) -> XResult<()> {
    match creds {
        Credentials::Admin => Ok(()),
        _ => Err(XrpcError {
            status: StatusCode::UNAUTHORIZED,
            error: "AuthenticationRequired".into(),
            message: "admin credentials required".into(),
        }),
    }
}

/// application/x-www-form-urlencoded pairs (repeated keys kept).
fn query_pairs(raw: &str) -> Vec<(String, String)> {
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
        String::from_utf8_lossy(&out).to_string()
    }
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), String::new()),
        })
        .collect()
}

fn not_found(message: &str) -> XrpcError {
    XrpcError::bad("NotFound", message)
}

// ---------------------------------------------------------------------------
// invite codes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InviteUse {
    pub used_by: String,
    pub used_at: String,
}

/// Stored at p/_invite:{code}\0c in the reference's CodeDetail shape.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InviteCode {
    pub code: String,
    pub available: i64,
    pub disabled: bool,
    pub for_account: String,
    pub created_by: String,
    pub created_at: String,
    #[serde(default)]
    pub uses: Vec<InviteUse>,
}

fn invite_routing(code: &str) -> String {
    format!("_invite:{code}")
}

/// `{hostname with . -> -}-xxxxx-xxxxx`
pub(super) fn gen_invite_code(app: &App) -> String {
    let host = app
        .public_url
        .split("://")
        .nth(1)
        .unwrap_or(&app.public_url)
        .split(['/', ':'])
        .next()
        .unwrap_or("vlpds")
        .replace('.', "-");
    format!("{host}-{}", super::server::random_token())
}

async fn get_invite(app: &App, code: &str) -> XResult<Option<InviteCode>> {
    get_json(app, &invite_routing(code), "c").await
}

async fn put_invite(app: &App, inv: &InviteCode) -> XResult<()> {
    let r = invite_routing(&inv.code);
    app.put_private(&r, vec![pmut(&r, "c", Some(to_json_bytes(inv)))])
        .await
}

pub(super) async fn create_invites(
    app: &App,
    account: &str,
    codes: &[String],
    use_count: i64,
    disabled: bool,
) -> XResult<()> {
    let now = crate::events::now_rfc3339();
    for code in codes {
        let inv = InviteCode {
            code: code.clone(),
            available: use_count,
            disabled,
            for_account: account.to_string(),
            created_by: "admin".into(),
            created_at: now.clone(),
            uses: Vec::new(),
        };
        put_invite(app, &inv).await?;
    }
    let muts = codes
        .iter()
        .map(|c| pmut(account, &format!("invite/{c}"), Some(Vec::new())))
        .collect();
    app.put_private(account, muts).await
}

/// Invite codes created for `account` (a DID or "admin").
pub(super) async fn account_invites(app: &App, account: &str) -> XResult<Vec<InviteCode>> {
    let mut out = Vec::new();
    for (name, _) in scan_private(app, account, "invite/").await? {
        if let Some(inv) = get_invite(app, &name["invite/".len()..]).await? {
            out.push(inv);
        }
    }
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Ok(out)
}

fn invite_slot_path(app: &App, code: &str, slot: i64) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{}/invite-use/{}/{slot}",
        app.store.prefix,
        hex::encode(code.as_bytes())
    ))
}

/// A claimed use of an invite code (see [`claim_invite_use`]).
pub(super) struct InviteClaim {
    code: String,
    slot: i64,
}

/// Atomically claims one use of `code` for `did`: the code's uses are slots
/// `0..available`, each claimed by a conditional create of
/// `invite-use/{code}/{slot}` (`If-None-Match: *`), so concurrent signups
/// (on any node) can't over-use it. The claim is released with
/// [`release_invite_use`] if account creation then fails, or recorded on the
/// code with [`record_invite_use`] once it succeeds.
pub(super) async fn claim_invite_use(app: &App, code: &str, did: &str) -> XResult<InviteClaim> {
    let unavailable = || XrpcError::bad("InvalidInviteCode", "Provided invite code not available");
    let inv = get_invite(app, code).await?.ok_or_else(unavailable)?;
    if inv.disabled || inv.available <= inv.uses.len() as i64 {
        return Err(unavailable());
    }
    if inv.for_account.starts_with("did:") {
        if let Ok(a) = app.account(&inv.for_account).await {
            if a.status.as_deref() == Some("takendown") {
                return Err(unavailable());
            }
        }
    }
    // recorded uses fill the low slots; a released claim can leave a gap
    let used = (inv.uses.len() as i64).min(inv.available);
    for slot in (used..inv.available).chain(0..used) {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match app
            .store
            .raw
            .put_opts(
                &invite_slot_path(app, code, slot),
                PutPayload::from(did.as_bytes().to_vec()),
                opts,
            )
            .await
        {
            Ok(_) => {
                return Ok(InviteClaim {
                    code: code.to_string(),
                    slot,
                })
            }
            Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(e) => return Err(XrpcError::from_err(e)),
        }
    }
    Err(unavailable())
}

/// Gives back a claimed use (the signup failed after claiming).
pub(super) async fn release_invite_use(app: &App, claim: InviteClaim) {
    if let Err(e) = app
        .store
        .raw
        .delete(&invite_slot_path(app, &claim.code, claim.slot))
        .await
    {
        tracing::warn!(code = %claim.code, "failed to release invite claim: {e}");
    }
}

pub(super) async fn record_invite_use(app: &App, claim: &InviteClaim, did: &str) -> XResult<()> {
    let code = claim.code.as_str();
    let e = ext(app);
    let _g = e.lock(&invite_routing(code)).await;
    let mut inv = get_invite(app, code)
        .await?
        .ok_or_else(|| XrpcError::bad("InvalidInviteCode", "Provided invite code not available"))?;
    inv.uses.push(InviteUse {
        used_by: did.to_string(),
        used_at: crate::events::now_rfc3339(),
    });
    put_invite(app, &inv).await
}

async fn set_invites_disabled(app: &App, codes: &[String], disabled: bool) -> XResult<()> {
    let e = ext(app);
    for code in codes {
        let _g = e.lock(&invite_routing(code)).await;
        if let Some(mut inv) = get_invite(app, code).await? {
            if inv.disabled != disabled {
                inv.disabled = disabled;
                put_invite(app, &inv).await?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// takedowns
// ---------------------------------------------------------------------------

/// Is `subject` (an at:// record URI or a blob CID) taken down? In-memory
/// check (sets loaded from p/_takedowns and refreshed periodically).
#[allow(dead_code)]
pub async fn is_takendown(app: &App, subject: &str) -> bool {
    let e = ext(app);
    ensure_loaded(app, &e).await;
    e.has_takedown(subject)
}

/// Is blob `cid` of repo `did` taken down?
#[allow(dead_code)]
pub async fn is_blob_takendown(app: &App, did: &str, cid: &str) -> bool {
    let e = ext(app);
    ensure_loaded(app, &e).await;
    e.has_takedown(&format!("blob/{did}/{cid}"))
}

async fn set_subject_takedown(app: &App, name: &str, val: Option<J>) -> XResult<()> {
    let applied = val.is_some();
    app.put_private(
        TAKEDOWNS,
        vec![pmut(TAKEDOWNS, name, val.map(|v| to_json_bytes(&v)))],
    )
    .await?;
    ext(app).set_takedown(&super::server::takedown_keys(name), applied);
    Ok(())
}

// ---------------------------------------------------------------------------
// account views
// ---------------------------------------------------------------------------

async fn account_view(app: &App, a: &Account) -> XResult<J> {
    let invites: Vec<J> = account_invites(app, &a.did)
        .await?
        .into_iter()
        .map(|c| serde_json::to_value(c).unwrap())
        .collect();
    let mut v = json!({
        "did": a.did,
        "handle": a.handle,
        "indexedAt": a.created_at,
        "invites": invites,
        "invitesDisabled": a.extra.get("invitesDisabled").and_then(|v| v.as_bool()).unwrap_or(false),
    });
    if let Some(e) = &a.email {
        v["email"] = json!(e);
    }
    for k in ["emailConfirmedAt", "deactivatedAt"] {
        if let Some(s) = a.extra.get(k).and_then(|v| v.as_str()) {
            v[k] = json!(s);
        }
    }
    if a.email_confirmed && v.get("emailConfirmedAt").is_none() {
        v["emailConfirmedAt"] = json!(a.created_at);
    }
    if let Some(code) = a.extra.get("invitedBy").and_then(|v| v.as_str()) {
        if let Some(inv) = get_invite(app, code).await? {
            v["invitedBy"] = serde_json::to_value(inv).unwrap();
        }
    }
    Ok(v)
}

#[derive(Deserialize)]
struct DidQ {
    did: String,
}

async fn get_account_info(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<DidQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let a = app
        .account(&q.did)
        .await
        .map_err(|_| not_found("Account not found"))?;
    Ok(Json(account_view(&app, &a).await?))
}

async fn get_account_infos(
    State(app): AppState,
    Auth(creds): Auth,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let mut infos = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (k, did) in query_pairs(raw.as_deref().unwrap_or("")) {
        if (k == "dids" || k == "dids[]") && seen.insert(did.clone()) {
            if let Ok(a) = app.account(&did).await {
                infos.push(account_view(&app, &a).await?);
            }
        }
    }
    Ok(Json(json!({"infos": infos})))
}

#[derive(Deserialize)]
struct SearchQ {
    email: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

/// Scans `a/` across owned partitions; `email` filters by case-insensitive
/// prefix. Cursor: `{partition}:{did}`.
async fn search_accounts(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<SearchQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let limit = q.limit.unwrap_or(50).clamp(1, 100);
    let email = q
        .email
        .as_deref()
        .map(|e| e.trim().to_ascii_lowercase())
        .filter(|e| !e.is_empty());
    let (mut part, mut after) = match q.cursor.as_deref().and_then(|c| c.split_once(':')) {
        Some((p, d)) => (
            p.parse::<usize>()
                .map_err(|_| invalid_request("Malformed cursor"))?,
            Some(d.to_string()),
        ),
        None => (0, None),
    };
    let mut out = Vec::new();
    let mut cursor = None;
    while part < app.partitions.len() && out.len() < limit {
        if let Some(p) = app.partitions.get(part) {
            let lo = match &after {
                Some(d) => [state::account_key(d), vec![0]].concat(),
                None => b"a/".to_vec(),
            };
            let mut iter =
                p.db.scan(lo..state::prefix_end(b"a/"))
                    .await
                    .map_err(XrpcError::from_err)?;
            while out.len() < limit {
                let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
                    break;
                };
                let did = String::from_utf8_lossy(&kv.key[2..]).to_string();
                cursor = Some(format!("{part}:{did}"));
                let Ok(a) = serde_json::from_slice::<Account>(&kv.value) else {
                    continue;
                };
                if let Some(e) = &email {
                    if !a
                        .email
                        .as_deref()
                        .is_some_and(|ae| ae.starts_with(e.as_str()))
                    {
                        continue;
                    }
                }
                out.push(account_view(&app, &a).await?);
            }
        }
        if out.len() < limit {
            part += 1;
            after = None;
            cursor = None;
        }
    }
    let mut res = json!({"accounts": out});
    if let Some(c) = cursor {
        res["cursor"] = json!(c);
    }
    Ok(Json(res))
}

// ---------------------------------------------------------------------------
// account updates
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct UpdateHandleIn {
    did: String,
    handle: String,
}

async fn update_account_handle(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateHandleIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    let handle = normalize_handle(&inp.handle)?;
    let did = inp.did.clone();
    let e = ext(&app);
    let _g = e.lock(&did).await;
    let acct = app
        .account(&did)
        .await
        .map_err(|_| invalid_request(format!("Account not found: {did}")))?;
    let old = acct.handle.clone();
    if handle != old && !super::server::claim_handle(&app, &handle, &did).await? {
        return Err(XrpcError::bad("HandleNotAvailable", format!("Handle already taken: {handle}")));
    }
    let mut next = acct.clone();
    next.handle = handle.clone();
    let op = AccountOp::Update {
        account: next,
        old_handle: Some(old.clone()),
        identity_event: true,
        account_event: false,
    };
    if let Err(err) = app.account_op(&did, op).await {
        if handle != old {
            super::server::release_handle(&app, &handle, &did).await;
        }
        return Err(err);
    }
    if handle != old {
        super::server::release_handle(&app, &old, &did).await;
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct UpdateEmailIn {
    account: String,
    email: String,
}

async fn update_account_email(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateEmailIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    let did = app
        .resolve_repo(&inp.account)
        .await
        .map_err(|_| invalid_request(format!("Account does not exist: {}", inp.account)))?;
    app.account(&did)
        .await
        .map_err(|_| invalid_request(format!("Account does not exist: {}", inp.account)))?;
    set_email(&app, &did, &inp.email).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct UpdatePasswordIn {
    did: String,
    password: String,
}

async fn update_account_password(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdatePasswordIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    if inp.password.len() > NEW_PASSWORD_MAX_LENGTH {
        return Err(invalid_request("Invalid password length."));
    }
    app.account(&inp.did)
        .await
        .map_err(|_| invalid_request(format!("Account not found: {}", inp.did)))?;
    super::server::change_password(&app, &inp.did, &inp.password).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateSigningKeyIn {
    did: String,
    signing_key: Option<String>,
}

/// Rotates the account's repo signing key. The PDS signs commits, so it must
/// hold the private key: `signingKey` must be a did:key reserved with
/// server.reserveSigningKey, or omitted/"generate" for a fresh key. The DID
/// document changes, so an #identity event is emitted.
async fn update_account_signing_key(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateSigningKeyIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    app.account(&inp.did)
        .await
        .map_err(|_| invalid_request(format!("Account not found: {}", inp.did)))?;
    let key = match inp
        .signing_key
        .as_deref()
        .filter(|k| !k.is_empty() && *k != "generate")
    {
        Some(dk) => {
            if !dk.starts_with("did:key:") {
                return Err(invalid_request("signingKey must be a did:key"));
            }
            super::server::take_reserved_key(&app, dk)
                .await?
                .ok_or_else(|| invalid_request("signingKey is not a key reserved on this PDS (use com.atproto.server.reserveSigningKey)"))?
        }
        None => Keypair::generate(),
    };
    let did_key = key.did_key();
    let secret = hex::encode(key.to_bytes());
    update_account(&app, &inp.did, true, false, |a| {
        a.signing_key = secret;
        Ok(())
    })
    .await?;
    app.did_resolver.invalidate(&inp.did);
    Ok(Json(json!({"signingKey": did_key})))
}

// ---------------------------------------------------------------------------
// subject status
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct StatusAttr {
    applied: bool,
    #[serde(rename = "ref")]
    r#ref: Option<String>,
}

#[derive(Deserialize)]
struct UpdateSubjectStatusIn {
    subject: J,
    takedown: Option<StatusAttr>,
    deactivated: Option<StatusAttr>,
}

enum Subject {
    Repo(String),
    Record {
        uri: String,
        did: String,
        cid: Option<String>,
    },
    Blob {
        did: String,
        cid: String,
    },
}

fn parse_subject(s: &J) -> XResult<Subject> {
    let t = s["$type"].as_str().unwrap_or("");
    match t {
        "com.atproto.admin.defs#repoRef" => Ok(Subject::Repo(
            s["did"]
                .as_str()
                .ok_or_else(|| invalid_request("subject.did required"))?
                .to_string(),
        )),
        "com.atproto.repo.strongRef" => {
            let uri = s["uri"]
                .as_str()
                .ok_or_else(|| invalid_request("subject.uri required"))?
                .to_string();
            let did = uri
                .strip_prefix("at://")
                .and_then(|r| r.split('/').next())
                .filter(|d| d.starts_with("did:"))
                .ok_or_else(|| invalid_request("invalid at-uri"))?
                .to_string();
            Ok(Subject::Record {
                uri,
                did,
                cid: s["cid"].as_str().map(str::to_string),
            })
        }
        "com.atproto.admin.defs#repoBlobRef" => Ok(Subject::Blob {
            did: s["did"]
                .as_str()
                .ok_or_else(|| invalid_request("subject.did required"))?
                .to_string(),
            cid: s["cid"]
                .as_str()
                .ok_or_else(|| invalid_request("subject.cid required"))?
                .to_string(),
        }),
        _ => Err(invalid_request(format!("Invalid subject ({t})"))),
    }
}

/// Deletes every OAuth session of `did`, so its DPoP access tokens stop
/// verifying (verify_dpop requires the live session) and can't be refreshed.
async fn revoke_oauth_sessions(app: &App, did: &str) {
    match crate::oauth::store::list_sessions(app, did).await {
        Ok(sessions) => {
            for s in sessions {
                if let Err(e) = crate::oauth::store::delete_session(app, did, &s.id).await {
                    tracing::warn!(%did, "takedown: deleting OAuth session failed: {}", e.description);
                }
            }
        }
        Err(e) => tracing::warn!(%did, "takedown: listing OAuth sessions failed: {}", e.description),
    }
}

async fn update_subject_status(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<UpdateSubjectStatusIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    if inp.takedown.as_ref().is_some_and(|t| t.applied)
        && inp.deactivated.as_ref().is_some_and(|d| !d.applied)
    {
        return Err(invalid_request(
            "Cannot activate and takedown an account at the same time",
        ));
    }
    let subject = parse_subject(&inp.subject)?;
    if let Some(td) = &inp.takedown {
        match &subject {
            Subject::Repo(did) => {
                let r = td
                    .r#ref
                    .clone()
                    .unwrap_or_else(crate::events::now_rfc3339);
                let applied = td.applied;
                update_account(&app, did, false, true, |a| {
                    set_extra(a, "takedownRef", if applied { json!(r) } else { J::Null });
                    recompute_status(a);
                    Ok(())
                })
                .await
                .map_err(|_| invalid_request(format!("Account not found: {did}")))?;
                if applied {
                    // refresh tokens and OAuth tokens, as the reference's
                    // takedownAccount (revokeRefreshTokensByDid, token.removeByDid);
                    // legacy access tokens stay valid until they expire
                    super::server::revoke_refresh_tokens(&app, did).await?;
                    revoke_oauth_sessions(&app, did).await;
                }
            }
            Subject::Record { uri, did, cid } => {
                let v = td
                    .applied
                    .then(|| json!({"uri": uri, "did": did, "cid": cid, "ref": td.r#ref}));
                set_subject_takedown(&app, &format!("rec/{uri}"), v).await?;
            }
            Subject::Blob { did, cid } => {
                let v = td
                    .applied
                    .then(|| json!({"did": did, "cid": cid, "ref": td.r#ref}));
                set_subject_takedown(&app, &format!("blob/{did}/{cid}"), v).await?;
            }
        }
    }
    if let (Some(d), Subject::Repo(did)) = (&inp.deactivated, &subject) {
        set_deactivated(&app, did, d.applied, None).await?;
    }
    if inp.takedown.is_none() && inp.deactivated.is_none() {
        if let Subject::Repo(did) = &subject {
            // re-announce the current status
            update_account(&app, did, false, true, |_| Ok(())).await?;
        }
    }
    let mut out = json!({"subject": inp.subject});
    if let Some(td) = &inp.takedown {
        out["takedown"] = json!({"applied": td.applied});
        if let Some(r) = &td.r#ref {
            out["takedown"]["ref"] = json!(r);
        }
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct SubjectStatusQ {
    did: Option<String>,
    uri: Option<String>,
    blob: Option<String>,
}

async fn get_subject_status(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<SubjectStatusQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let body = if let Some(blob) = &q.blob {
        let did = q
            .did
            .as_deref()
            .ok_or_else(|| invalid_request("Must provide a did to request blob state"))?;
        get_json::<J>(&app, TAKEDOWNS, &format!("blob/{did}/{blob}")).await?.map(|t| {
            json!({
                "subject": {"$type": "com.atproto.admin.defs#repoBlobRef", "did": did, "cid": blob},
                "takedown": status_attr(true, t["ref"].as_str()),
            })
        })
    } else if let Some(uri) = &q.uri {
        let td = get_json::<J>(&app, TAKEDOWNS, &format!("rec/{uri}")).await?;
        let cid = current_record_cid(&app, uri).await;
        match (td, cid) {
            (Some(t), Some(cid)) => Some(json!({
                "subject": {"$type": "com.atproto.repo.strongRef", "uri": uri, "cid": cid},
                "takedown": status_attr(true, t["ref"].as_str()),
            })),
            _ => None,
        }
    } else if let Some(did) = &q.did {
        app.account(did).await.ok().map(|a| {
            let tref = a.extra.get("takedownRef").and_then(|v| v.as_str());
            json!({
                "subject": {"$type": "com.atproto.admin.defs#repoRef", "did": did},
                "takedown": status_attr(tref.is_some(), tref),
                "deactivated": {"applied": a.extra.get("deactivatedAt").is_some_and(|v| !v.is_null())},
            })
        })
    } else {
        return Err(invalid_request("No provided subject"));
    };
    body.map(Json).ok_or_else(|| not_found("Subject not found"))
}

fn status_attr(applied: bool, r: Option<&str>) -> J {
    let mut v = json!({"applied": applied});
    if let Some(r) = r {
        v["ref"] = json!(r);
    }
    v
}

async fn current_record_cid(app: &App, uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("at://")?;
    let (did, path) = rest.split_once('/')?;
    let p = app.partition(did).ok()?;
    let v = p.db.get(state::record_key(did, path)).await.ok()??;
    let (cid, _) = state::decode_record_value(&v).ok()?;
    Some(cid.to_string())
}

#[derive(Deserialize)]
struct DeleteAccountIn {
    did: String,
}

async fn delete_account(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<DeleteAccountIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    app.account(&inp.did)
        .await
        .map_err(|_| invalid_request(format!("Account not found: {}", inp.did)))?;
    delete_account_fully(&app, &inp.did).await?;
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// invites (admin)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AccountIn {
    account: String,
    #[allow(dead_code)]
    note: Option<String>,
}

async fn set_account_invites_disabled(app: &App, account: &str, disabled: bool) -> XResult<()> {
    let did = app.resolve_repo(account).await?;
    update_account(app, &did, false, false, |a| {
        set_extra(a, "invitesDisabled", json!(disabled));
        Ok(())
    })
    .await?;
    let codes: Vec<String> = account_invites(app, &did)
        .await?
        .into_iter()
        .map(|c| c.code)
        .collect();
    set_invites_disabled(app, &codes, disabled).await
}

async fn disable_account_invites(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<AccountIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    set_account_invites_disabled(&app, &inp.account, true).await?;
    Ok(StatusCode::OK)
}

async fn enable_account_invites(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<AccountIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    set_account_invites_disabled(&app, &inp.account, false).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize, Default)]
struct DisableCodesIn {
    #[serde(default)]
    codes: Vec<String>,
    #[serde(default)]
    accounts: Vec<String>,
}

async fn disable_invite_codes(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<DisableCodesIn>,
) -> XResult<StatusCode> {
    require_admin(&creds)?;
    if inp.accounts.iter().any(|a| a == "admin") {
        return Err(invalid_request("cannot disable admin invite codes"));
    }
    let mut codes = inp.codes.clone();
    for a in &inp.accounts {
        codes.extend(account_invites(&app, a).await?.into_iter().map(|c| c.code));
    }
    set_invites_disabled(&app, &codes, true).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct InviteCodesQ {
    sort: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

/// All invite codes, sorted "recent" (createdAt desc) or "usage" (uses desc).
/// Cursor: the last returned code.
async fn get_invite_codes(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<InviteCodesQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let sort = q.sort.as_deref().unwrap_or("recent");
    if sort != "recent" && sort != "usage" {
        return Err(invalid_request(format!("unknown sort method: {sort}")));
    }
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let mut all: Vec<InviteCode> = scan_private_routing(&app, "_invite:")
        .await?
        .into_iter()
        .filter(|(_, name, _)| name == "c")
        .filter_map(|(_, _, v)| serde_json::from_slice(&v).ok())
        .collect();
    if sort == "recent" {
        all.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.code.cmp(&a.code)));
    } else {
        all.sort_by(|a, b| b.uses.len().cmp(&a.uses.len()).then(b.code.cmp(&a.code)));
    }
    let start = match &q.cursor {
        Some(c) => all
            .iter()
            .position(|i| &i.code == c)
            .map(|p| p + 1)
            .unwrap_or(all.len()),
        None => 0,
    };
    let page: Vec<InviteCode> = all.iter().skip(start).take(limit).cloned().collect();
    let mut out =
        json!({"codes": page.iter().map(|c| serde_json::to_value(c).unwrap()).collect::<Vec<_>>()});
    if page.len() == limit && start + limit < all.len() {
        out["cursor"] = json!(page.last().map(|c| c.code.clone()));
    }
    Ok(Json(out))
}

// ---------------------------------------------------------------------------
// email
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendEmailIn {
    recipient_did: String,
    content: String,
    subject: Option<String>,
    #[allow(dead_code)]
    sender_did: Option<String>,
    #[allow(dead_code)]
    comment: Option<String>,
}

async fn send_email(
    State(app): AppState,
    Auth(creds): Auth,
    Json(inp): Json<SendEmailIn>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let a = app
        .account(&inp.recipient_did)
        .await
        .map_err(|_| invalid_request("Recipient not found"))?;
    let to = a
        .email
        .ok_or_else(|| invalid_request("account does not have an email address"))?;
    let subject = inp.subject.unwrap_or_else(|| "Message via your PDS".into());
    super::server::deliver(&app, &to, &subject, &inp.content, "admin", None);
    Ok(Json(json!({"sent": true})))
}

#[derive(Deserialize)]
struct DevMailQ {
    email: String,
}

/// Dev mode only: mail "sent" to an address (newest last) and the latest token.
async fn get_dev_mail(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<DevMailQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    if !app.config.dev_mode {
        return Err(XrpcError {
            status: StatusCode::NOT_FOUND,
            error: "MethodNotImplemented".into(),
            message: "dev mail is only available in dev mode".into(),
        });
    }
    let email = q.email.trim().to_ascii_lowercase();
    let e = ext(&app);
    let msgs = e.dev_mail.lock().get(&email).cloned().unwrap_or_default();
    let token = msgs.iter().rev().find_map(|m| m.token.clone());
    let latest = msgs.last().cloned();
    Ok(Json(
        json!({"email": email, "token": token, "latest": latest, "messages": msgs}),
    ))
}

// ---------------------------------------------------------------------------
// simulation
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct BulkCreateIn {
    start: u64,
    count: u64,
    records: u32,
}

/// Simulation-only: creates accounts `start..start+count` with deterministic
/// DIDs (`state::bulk_did`) and `records` genesis posts each. Emits the normal
/// #identity/#account/#sync events but skips the global handle claim object.
async fn bulk_create(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<BulkCreateIn>,
) -> XResult<Json<J>> {
    let tok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if tok != Some(app.admin_token.as_str()) {
        return Err(XrpcError::auth("admin token required"));
    }
    let mut waits = Vec::with_capacity(inp.count as usize);
    let mut skipped = 0u64;
    for i in inp.start..inp.start + inp.count {
        let did = state::bulk_did(i);
        // cluster mode: each node creates the accounts in partitions it owns
        // (send the same bulk request to every node)
        if app.cluster.is_some() && app.remote_owner(&did).is_some() {
            skipped += 1;
            continue;
        }
        let handle = state::bulk_handle(i);
        let key = Arc::new(Keypair::generate());
        let acct = Account {
            did: did.clone(),
            handle: handle.clone(),
            signing_key: hex::encode(key.to_bytes()),
            // simulation accounts share one precomputed hash (Argon2id is ~20 ms each)
            password_hash: BULK_PASSWORD_HASH.clone(),
            created_at: crate::events::now_rfc3339(),
            ..Default::default()
        };
        let mut records = Vec::with_capacity(inp.records as usize);
        for r in 0..inp.records {
            let v = Value::from_json(&json!({
                "$type": "app.bsky.feed.post",
                "text": format!("genesis post {r} of account {i}"),
                "createdAt": "2026-09-30T00:00:00.000Z",
            }))
            .map_err(XrpcError::from_err)?;
            let bytes = v.to_cbor();
            let path = format!("app.bsky.feed.post/{}", app.tids.next());
            records.push((path, Cid::dag_cbor(&bytes), Bytes::from(bytes)));
        }
        let (tx, rx) = oneshot::channel();
        app.workers
            .route(&did)
            .send(WorkerMsg::CreateRepo(CreateRepoReq {
                did: did.into(),
                handle,
                key,
                account_json: Bytes::from(serde_json::to_vec(&acct).unwrap()),
                records,
                reply: tx,
            }))
            .map_err(XrpcError::from_err)?;
        waits.push(rx);
    }
    let mut ok = 0u64;
    let mut failed = 0u64;
    for w in waits {
        match w.await {
            Ok(Ok(_)) => ok += 1,
            _ => failed += 1,
        }
    }
    Ok(Json(json!({"created": ok, "failed": failed, "skipped": skipped})))
}

static BULK_PASSWORD_HASH: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| state::hash_password_blocking("hunter2"));
