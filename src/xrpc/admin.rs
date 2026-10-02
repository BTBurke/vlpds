//! com.atproto.admin.* (Basic `admin:<token>` auth), invite-code storage,
//! subject takedowns (`is_record_takendown` / `is_blob_takendown`, kept in
//! the account's partition under `sec/td/`), the dev-mode
//! mailbox (vlpds.admin.getDevMail) and the vlpds.admin.bulkCreate simulator.

use super::authn::Credentials;
use super::server::{
    ctl, delete_account_fully, ext, get_json, invalid_request, normalize_handle, pmut, put_sec,
    recompute_status, scan_private_routing, set_deactivated,
    set_email, set_extra, to_json_bytes, update_account, NEW_PASSWORD_MAX_LENGTH, TAKEDOWN,
};
use super::*;

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/vlpds.admin.bulkCreate", post(bulk_create))
        .route("/xrpc/vlpds.admin.rewrapSecrets", post(rewrap_secrets))
        .route("/xrpc/vlpds.admin.rotatePlcKeys", post(rotate_plc_keys))
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
        .route("/xrpc/vlpds.admin.getShardLayout", get(get_shard_layout))
        .route("/xrpc/vlpds.admin.splitShard", post(split_shard))
        .route("/xrpc/vlpds.admin.mergeShards", post(merge_shards))
        .route("/xrpc/vlpds.admin.abortReshard", post(abort_reshard))
}

// ---------------------------------------------------------------------------
// shard layout: online split/merge (src/reshard.rs)
// ---------------------------------------------------------------------------

fn cluster_of(app: &App) -> XResult<&Arc<crate::cluster::Cluster>> {
    app.cluster.as_ref().ok_or_else(|| invalid_request("no cluster"))
}

fn layout_json(app: &App) -> XResult<J> {
    let c = cluster_of(app)?;
    let l = c.layout();
    let shards: Vec<J> = l
        .shards
        .iter()
        .map(|r| json!({"id": r.id, "lo": r.lo, "hi": r.hi, "owner": c.owner_of(r.id).map(|o| o.0)}))
        .collect();
    Ok(json!({"version": l.version, "shards": shards, "nextId": l.next_id, "op": l.op}))
}

/// The shard layout this node routes by, with owners and any op in flight.
async fn get_shard_layout(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    require_admin(&creds)?;
    Ok(Json(layout_json(&app)?))
}

#[derive(Deserialize)]
struct SplitIn {
    shard: u16,
    at: Option<u32>,
    #[serde(default)]
    wait: bool,
}

#[derive(Deserialize)]
struct MergeIn {
    left: u16,
    right: u16,
    #[serde(default)]
    wait: bool,
}

/// Plans `plan`; with `wait`, returns once it flipped (or was aborted).
async fn reshard(app: &Arc<App>, plan: crate::reshard::Plan, wait: bool) -> XResult<Json<J>> {
    let c = cluster_of(app)?;
    let host: Arc<dyn crate::cluster::ShardHost> = app.node.clone();
    let before = c.layout().version;
    let op = c.plan_reshard(&host, plan).await.map_err(|e| invalid_request(format!("{e:#}")))?;
    let mut out = json!({"op": op});
    if wait {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let l = c.layout();
            if l.version > before && l.op.as_ref().is_none_or(|o| o.id != op.id) {
                out["done"] = json!(l.shards.iter().any(|r| op.children.iter().any(|ch| ch.id == r.id)));
                break;
            }
            if l.op.is_none() && l.version == before {
                out["done"] = json!(false); // aborted
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(XrpcError { status: StatusCode::GATEWAY_TIMEOUT, error: "Timeout".into(), message: format!("op {} still in progress", op.id) });
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    out["layout"] = layout_json(app)?;
    Ok(Json(out))
}

/// Splits a shard online (DESIGN.md "Online shard split/merge").
async fn split_shard(State(app): AppState, Auth(creds): Auth, Json(inp): Json<SplitIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    reshard(&app, crate::reshard::Plan::Split { shard: inp.shard, at: inp.at }, inp.wait).await
}

/// Merges two adjacent shards online.
async fn merge_shards(State(app): AppState, Auth(creds): Auth, Json(inp): Json<MergeIn>) -> XResult<Json<J>> {
    require_admin(&creds)?;
    reshard(&app, crate::reshard::Plan::Merge { left: inp.left, right: inp.right }, inp.wait).await
}

/// Aborts the split/merge in progress (only before it flips).
async fn abort_reshard(State(app): AppState, Auth(creds): Auth) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let c = cluster_of(&app)?;
    let host: Arc<dyn crate::cluster::ShardHost> = app.node.clone();
    let op = c.abort_reshard(&host).await.map_err(XrpcError::from_err)?;
    Ok(Json(json!({"aborted": op, "layout": layout_json(&app)?})))
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
    // `account` may be owned elsewhere (getAccountInfos, disableInviteCodes)
    for (name, _) in super::internal::scan_private_anywhere(app, account, "invite/").await? {
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

/// Is record `{collection}/{rkey}` of `did` taken down? Takedowns live in
/// the account's own partition (`sec/td/`), so the owner checks them from
/// its cached per-DID view (`server::ctl`).
pub async fn is_record_takendown(app: &App, did: &str, path: &str) -> bool {
    ctl(app, did).await.has_takedown(&format!("rec/{path}"))
}

/// Is blob `cid` of repo `did` taken down?
pub async fn is_blob_takendown(app: &App, did: &str, cid: &str) -> bool {
    ctl(app, did).await.has_takedown(&format!("blob/{cid}"))
}

/// Applies (`val` = Some) or lifts a record/blob takedown of `did`; `name` is
/// relative to `sec/td/` (`rec/{collection}/{rkey}` or `blob/{cid}`).
async fn set_subject_takedown(app: &App, did: &str, name: &str, val: Option<J>) -> XResult<()> {
    put_sec(
        app,
        did,
        vec![pmut(did, &format!("{TAKEDOWN}{name}"), val.map(|v| to_json_bytes(&v)))],
    )
    .await
}

/// `{collection}/{rkey}` of an at:// URI naming a record of `did`.
fn record_path<'a>(uri: &'a str, did: &str) -> XResult<&'a str> {
    uri.strip_prefix("at://")
        .and_then(|r| r.strip_prefix(did))
        .and_then(|r| r.strip_prefix('/'))
        .filter(|p| p.split('/').count() == 2 && !p.split('/').any(str::is_empty))
        .ok_or_else(|| invalid_request("invalid at-uri"))
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
            // the DIDs live on any node's shards (the request routes by none)
            if let Ok(a) = super::internal::account_anywhere(&app, &did).await {
                infos.push(account_view(&app, &a).await?);
            }
        }
    }
    Ok(Json(json!({"infos": infos})))
}

#[derive(Deserialize)]
pub(super) struct SearchQ {
    email: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

/// One searchAccounts hit, keyed by the global sort key (slot, did).
#[derive(serde::Serialize, Deserialize)]
pub(super) struct AccountHit {
    slot: u32,
    did: String,
    view: J,
}

/// (limit, lowercased email prefix, resume after this DID)
type SearchParams = (usize, Option<String>, Option<String>);

impl SearchQ {
    fn parsed(&self) -> XResult<SearchParams> {
        let limit = self.limit.unwrap_or(50).clamp(1, 100);
        let email = self
            .email
            .as_deref()
            .map(|e| e.trim().to_ascii_lowercase())
            .filter(|e| !e.is_empty());
        let after = match self.cursor.as_deref().filter(|c| !c.is_empty()) {
            Some(c) => {
                let (p, d) = c.split_once(':').ok_or_else(|| invalid_request("Malformed cursor"))?;
                let slot = p.parse::<u32>().map_err(|_| invalid_request("Malformed cursor"))?;
                if d.is_empty() || crate::slots::slot_of(d) as u32 != slot {
                    return Err(invalid_request("Malformed cursor"));
                }
                Some(d.to_string())
            }
            None => None,
        };
        Ok((limit, email, after))
    }
}

/// Accounts on the shards this node owns, in (slot, did) order after the
/// cursor, at most `limit`; plus the shards scanned. The local half of
/// searchAccounts (also served to peers by /internal/v1/admin/searchAccounts).
pub(super) async fn search_accounts_local(app: &App, q: &SearchQ) -> XResult<(Vec<AccountHit>, Vec<u16>)> {
    let (limit, email, after) = q.parsed()?;
    let layout = app.partitions.layout();
    let mut owned = app.partitions.owned();
    owned.sort_by_key(|p| layout.range_of(p.id).map_or(u32::MAX, |r| r.lo));
    let ids: Vec<u16> = owned.iter().map(|p| p.id).collect();
    let start = after.as_deref().map(|d| [state::account_key(d), vec![0]].concat());
    let mut out = Vec::new();
    for p in owned {
        if out.len() >= limit {
            break;
        }
        let mut iter = state::FamilyScan::new(p.db.as_ref(), state::ACCOUNT_FAMILY, start.clone(), &Default::default())
            .await
            .map_err(XrpcError::from_err)?;
        while out.len() < limit {
            let Some(kv) = iter.next().await.map_err(XrpcError::from_err)? else {
                break;
            };
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
            let (slot, did) = state::slot_did(&kv.key, state::ACCOUNT_FAMILY.len());
            let slot = u16::from_be_bytes([slot[0], slot[1]]) as u32;
            let did = String::from_utf8_lossy(did).to_string();
            out.push(AccountHit { slot, did, view: account_view(app, &a).await? });
        }
    }
    Ok((out, ids))
}

/// Scans the accounts of every shard in the cluster (this node's, plus each
/// live peer's via /internal/v1/admin/searchAccounts), merged in (slot, did)
/// order, an order independent of the shard layout; `email` filters by
/// case-insensitive prefix. Cursor: `{slot}:{did}`, the last returned key,
/// so a page resumes on every node. Unreachable peers / unowned shards are
/// reported (`unreachableNodes`, `missingShards`) instead of silently dropped.
async fn search_accounts(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<SearchQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let (limit, _, after) = q.parsed()?;
    let (mut hits, owned) = search_accounts_local(&app, &q).await?;
    let mut query = vec![("limit", limit.to_string())];
    if let Some(e) = &q.email {
        query.push(("email", e.clone()));
    }
    if let Some(c) = &q.cursor {
        query.push(("cursor", c.clone()));
    }
    let g = super::internal::gather(&app, "/internal/v1/admin/searchAccounts", &query).await;
    let mut covered: std::collections::HashSet<u16> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        hits.extend(serde_json::from_value::<Vec<AccountHit>>(r.body["accounts"].clone()).unwrap_or_default());
    }
    hits.sort_by(|a, b| (a.slot, &a.did).cmp(&(b.slot, &b.did)));
    hits.dedup_by(|a, b| a.did == b.did);
    hits.truncate(limit);
    let cursor = (hits.len() == limit).then(|| hits.last().map(|h| format!("{}:{}", h.slot, h.did))).flatten();
    // slots before the cursor's are done; only shards past it can be missing
    let from = after.map(|d| crate::slots::slot_of(&d) as u32).unwrap_or(0);
    let mut res = json!({"accounts": hits.into_iter().map(|h| h.view).collect::<Vec<_>>()});
    if let Some(c) = cursor {
        res["cursor"] = json!(c);
    }
    partial_fields(&app, &mut res, g.unreachable, &covered, from);
    Ok(Json(res))
}

/// Marks a scatter-gather result incomplete: `unreachableNodes` (peers that
/// timed out or failed) and `missingShards` (shards holding slots >= `from`
/// that no answering node owned, e.g. mid-move). Absent when complete.
fn partial_fields(
    app: &App,
    res: &mut J,
    unreachable: Vec<String>,
    covered: &std::collections::HashSet<u16>,
    from: u32,
) {
    let missing: Vec<u16> =
        app.partitions.layout().shards.iter().filter(|r| r.hi > from && !covered.contains(&r.id)).map(|r| r.id).collect();
    if !unreachable.is_empty() {
        res["unreachableNodes"] = json!(unreachable);
    }
    if !missing.is_empty() {
        res["missingShards"] = json!(missing);
    }
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
    // reference allowAnyValid: no slur or reserved-name checks, but a
    // service-domain handle still has to be one 3-18 char label
    if handle.ends_with(&format!(".{}", app.handle_domain)) {
        super::server::ensure_service_handle(&app, &handle, true)?;
    }
    let did = inp.did.clone();
    app.account(&did)
        .await
        .map_err(|_| invalid_request(format!("Account not found: {did}")))?;
    super::identity::set_handle(&app, &did, &handle, false).await?;
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
/// document changes, so an #identity event is emitted; with PLC
/// registration on, a did:plc's `atproto` key is first updated in the PLC
/// directory (as the reference's rotate-keys script), and a failure there
/// changes nothing here.
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
    if let (Some(plc), true) = (&app.plc, inp.did.starts_with("did:plc:")) {
        plc.update_signing_key(&inp.did, &did_key).await?;
    }
    // wrapped for the row; cached unwrapped, so the repo's reload after the
    // rotation needs no unwrap
    let (wrapped, pubkey) = app.secrets.wrap_signing_key(&inp.did, &Arc::new(key)).await?;
    update_account(&app, &inp.did, true, false, |a| {
        a.wrapped_signing_key = wrapped;
        a.signing_pubkey = pubkey;
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
    if let Err(e) = crate::oauth::store::revoke_all_sessions(app, did).await {
        tracing::warn!(%did, "takedown: revoking OAuth sessions failed: {}", e.description);
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
                update_account(&app, did, false, true, move |a| {
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
                set_subject_takedown(&app, did, &format!("rec/{}", record_path(uri, did)?), v).await?;
            }
            Subject::Blob { did, cid } => {
                let v = td
                    .applied
                    .then(|| json!({"did": did, "cid": cid, "ref": td.r#ref}));
                set_subject_takedown(&app, did, &format!("blob/{cid}"), v).await?;
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
        get_json::<J>(&app, did, &format!("{TAKEDOWN}blob/{blob}")).await?.map(|t| {
            json!({
                "subject": {"$type": "com.atproto.admin.defs#repoBlobRef", "did": did, "cid": blob},
                "takedown": status_attr(true, t["ref"].as_str()),
            })
        })
    } else if let Some(uri) = &q.uri {
        let did = uri
            .strip_prefix("at://")
            .and_then(|r| r.split('/').next())
            .ok_or_else(|| invalid_request("invalid at-uri"))?;
        let td = get_json::<J>(&app, did, &format!("{TAKEDOWN}rec/{}", record_path(uri, did)?)).await?;
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
    update_account(app, &did, false, false, move |a| {
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
pub(super) struct InviteCodesQ {
    sort: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// Global sort key of an invite code (listed descending).
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum InviteKey {
    Recent(String, String),
    Usage(usize, String),
}

impl InviteKey {
    fn of(usage: bool, c: &InviteCode) -> InviteKey {
        if usage {
            InviteKey::Usage(c.uses.len(), c.code.clone())
        } else {
            InviteKey::Recent(c.created_at.clone(), c.code.clone())
        }
    }

    /// `{createdAt|uses}/{code}` (codes and timestamps never contain '/').
    fn cursor(&self) -> String {
        match self {
            InviteKey::Recent(t, c) => format!("{t}/{c}"),
            InviteKey::Usage(n, c) => format!("{n}/{c}"),
        }
    }

    fn parse(usage: bool, s: &str) -> XResult<InviteKey> {
        let bad = || invalid_request("Malformed cursor");
        let (k, c) = s.split_once('/').ok_or_else(bad)?;
        Ok(if usage {
            InviteKey::Usage(k.parse().map_err(|_| bad())?, c.to_string())
        } else {
            InviteKey::Recent(k.to_string(), c.to_string())
        })
    }
}

impl InviteCodesQ {
    /// (usage sort, limit, resume-after key)
    fn parsed(&self) -> XResult<(bool, usize, Option<InviteKey>)> {
        let sort = self.sort.as_deref().unwrap_or("recent");
        if sort != "recent" && sort != "usage" {
            return Err(invalid_request(format!("unknown sort method: {sort}")));
        }
        let usage = sort == "usage";
        let limit = super::extract::limit_param(self.limit, 100, 1, 500)?;
        let after = self.cursor.as_deref().filter(|c| !c.is_empty()).map(|c| InviteKey::parse(usage, c)).transpose()?;
        Ok((usage, limit, after))
    }
}

/// Invite codes on the shards this node owns that sort after the cursor, in
/// order, at most `limit + 1` (so the merger knows whether more exist); plus
/// the shards scanned. Also served to peers by /internal/v1/admin/inviteCodes.
pub(super) async fn invite_codes_local(app: &App, q: &InviteCodesQ) -> XResult<(Vec<InviteCode>, Vec<u16>)> {
    let (usage, limit, after) = q.parsed()?;
    let owned: Vec<u16> = app.partitions.owned().iter().map(|p| p.id).collect();
    let mut all: Vec<(InviteKey, InviteCode)> = scan_private_routing(app, "_invite:")
        .await?
        .into_iter()
        .filter(|(_, name, _)| name == "c")
        .filter_map(|(_, _, v)| serde_json::from_slice::<InviteCode>(&v).ok())
        .map(|c| (InviteKey::of(usage, &c), c))
        .filter(|(k, _)| after.as_ref().is_none_or(|a| k < a))
        .collect();
    all.sort_by(|a, b| b.0.cmp(&a.0));
    all.truncate(limit + 1);
    Ok((all.into_iter().map(|(_, c)| c).collect(), owned))
}

/// All invite codes in the cluster (this node's shards plus each live
/// peer's), sorted "recent" (createdAt desc) or "usage" (uses desc), code
/// desc as the tiebreak. Cursor: the last returned code's sort key, so the
/// next page resumes on every node. Partial results are flagged as in
/// searchAccounts.
async fn get_invite_codes(
    State(app): AppState,
    Auth(creds): Auth,
    Query(q): Query<InviteCodesQ>,
) -> XResult<Json<J>> {
    require_admin(&creds)?;
    let (usage, limit, _) = q.parsed()?;
    let (codes, owned) = invite_codes_local(&app, &q).await?;
    let mut all: Vec<(InviteKey, InviteCode)> = codes.into_iter().map(|c| (InviteKey::of(usage, &c), c)).collect();
    let mut query = vec![("limit", limit.to_string()), ("sort", (if usage { "usage" } else { "recent" }).to_string())];
    if let Some(c) = &q.cursor {
        query.push(("cursor", c.clone()));
    }
    let g = super::internal::gather(&app, "/internal/v1/admin/inviteCodes", &query).await;
    let mut covered: std::collections::HashSet<u16> = owned.into_iter().collect();
    for r in g.replies {
        covered.extend(r.owned);
        let codes = serde_json::from_value::<Vec<InviteCode>>(r.body["codes"].clone()).unwrap_or_default();
        all.extend(codes.into_iter().map(|c| (InviteKey::of(usage, &c), c)));
    }
    all.sort_by(|a, b| b.0.cmp(&a.0));
    all.dedup_by(|a, b| a.1.code == b.1.code);
    let more = all.len() > limit;
    all.truncate(limit);
    let mut out = json!({"codes": all.iter().map(|(_, c)| serde_json::to_value(c).unwrap()).collect::<Vec<_>>()});
    if more {
        out["cursor"] = json!(all.last().map(|(k, _)| k.cursor()));
    }
    partial_fields(&app, &mut out, g.unreachable, &covered, 0);
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
    super::server::deliver_moderation(&app, &to, &subject, &inp.content);
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
    #[serde(default)]
    start: u64,
    #[serde(default)]
    count: u64,
    /// Explicit account indexes, instead of `start..start+count` (a load
    /// generator sends each node only the DIDs it owns).
    #[serde(default)]
    indices: Option<Vec<u64>>,
    records: BulkRecords,
}

/// Genesis records: one count for every account, or one per account
/// (aligned with the account list).
#[derive(Deserialize)]
#[serde(untagged)]
enum BulkRecords {
    All(u32),
    Each(Vec<u32>),
}

/// Accounts and genesis records one bulkCreate request may carry.
const BULK_MAX_ACCOUNTS: usize = 100_000;
const BULK_MAX_RECORDS: u64 = 1_000_000;
/// Head lookups in flight per request (the existence check).
const BULK_EXISTS_CONCURRENCY: usize = 64;

/// Simulation-only: creates accounts `start..start+count` (or `indices`)
/// with deterministic DIDs (`state::bulk_did`) and genesis posts (`records`:
/// one count, or one per account). Emits the normal #identity/#account/#sync
/// events but skips the global handle claim object, and never touches the
/// PLC directory, even with PLC registration on: these synthetic DIDs are
/// not registered anywhere (benchmarks must not hammer a real PLC).
///
/// Idempotent, so a resumed range is safe: an account whose head is stored
/// (or that its worker holds) is left alone and counted as `existing`.
/// Accounts in shards this node doesn't serve are counted as `notOwned`
/// (send each node its own DIDs, or the same range to every node).
async fn bulk_create(
    State(app): AppState,
    headers: HeaderMap,
    Json(inp): Json<BulkCreateIn>,
) -> XResult<Json<J>> {
    use futures::StreamExt;
    let tok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if !tok.is_some_and(|t| crate::auth::token_eq(&app.admin_token, t)) {
        return Err(XrpcError::auth("admin token required"));
    }
    let idx: Vec<u64> = match inp.indices {
        Some(v) => v,
        None => {
            if inp.count as usize > BULK_MAX_ACCOUNTS {
                return Err(invalid_request(format!("at most {BULK_MAX_ACCOUNTS} accounts per request")));
            }
            (inp.start..inp.start.saturating_add(inp.count)).collect()
        }
    };
    if idx.len() > BULK_MAX_ACCOUNTS {
        return Err(invalid_request(format!("at most {BULK_MAX_ACCOUNTS} accounts per request")));
    }
    let records: Vec<u32> = match inp.records {
        BulkRecords::All(n) => vec![n; idx.len()],
        BulkRecords::Each(v) if v.len() == idx.len() => v,
        BulkRecords::Each(v) => {
            return Err(invalid_request(format!("records has {} entries for {} accounts", v.len(), idx.len())));
        }
    };
    if records.iter().map(|&n| n as u64).sum::<u64>() > BULK_MAX_RECORDS {
        return Err(invalid_request(format!("at most {BULK_MAX_RECORDS} genesis records per request")));
    }
    // owned accounts whose head isn't stored yet
    let mut not_owned = 0u64;
    let mut owned = Vec::with_capacity(idx.len());
    for (&i, &n) in idx.iter().zip(&records) {
        let did = state::bulk_did(i);
        match app.partitions.for_key(&did) {
            Some(p) => owned.push((i, n, did, p)),
            None => not_owned += 1,
        }
    }
    let checked: Vec<_> = futures::stream::iter(owned)
        .map(|(i, n, did, p)| async move {
            let exists = p.db.get(state::head_key(&did)).await.map(|h| h.is_some());
            (i, n, did, exists)
        })
        .buffered(BULK_EXISTS_CONCURRENCY)
        .collect()
        .await;
    let mut waits = Vec::with_capacity(checked.len());
    let mut existing = 0u64;
    for (i, n, did, exists) in checked {
        if exists.map_err(XrpcError::from_err)? {
            existing += 1;
            continue;
        }
        let handle = state::bulk_handle(i);
        let key = Arc::new(Keypair::generate());
        let (wrapped_signing_key, signing_pubkey) = app.secrets.wrap_signing_key(&did, &key).await?;
        let acct = Account {
            did: did.clone(),
            handle: handle.clone(),
            wrapped_signing_key,
            signing_pubkey,
            // simulation accounts share one precomputed hash (Argon2id is ~20 ms each)
            password_hash: BULK_PASSWORD_HASH.clone(),
            created_at: crate::events::now_rfc3339(),
            ..Default::default()
        };
        let mut recs = Vec::with_capacity(n as usize);
        for r in 0..n {
            let v = Value::from_json(&json!({
                "$type": "app.bsky.feed.post",
                "text": format!("genesis post {r} of account {i}"),
                "createdAt": "2026-09-30T00:00:00.000Z",
            }))
            .map_err(XrpcError::from_err)?;
            let bytes = v.to_cbor();
            let path = format!("app.bsky.feed.post/{}", app.tids.next());
            recs.push((path, Cid::dag_cbor(&bytes), Bytes::from(bytes)));
        }
        let (tx, rx) = oneshot::channel();
        app.workers
            .route(&did)
            .send(WorkerMsg::CreateRepo(CreateRepoReq {
                did: did.into(),
                handle,
                key,
                account_json: Bytes::from(serde_json::to_vec(&acct).unwrap()),
                records: recs,
                reply: tx,
            }))
            .map_err(XrpcError::from_err)?;
        waits.push((n, rx));
    }
    let (mut created, mut created_records, mut failed) = (0u64, 0u64, 0u64);
    for (n, w) in waits {
        match w.await {
            Ok(Ok(_)) => {
                created += 1;
                created_records += n as u64;
            }
            // created by a concurrent request since the head check (the
            // worker still holds it)
            Ok(Err(WriteError::Invalid(m))) if m == crate::worker::REPO_EXISTS => existing += 1,
            _ => failed += 1,
        }
    }
    Ok(Json(json!({
        "created": created, "records": created_records, "existing": existing,
        "notOwned": not_owned, "failed": failed,
    })))
}

static BULK_PASSWORD_HASH: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| state::hash_password_blocking("hunter2"));

// ---------------------------------------------------------------------------
// PLC rotation key rotation (DESIGN.md "PLC identity")
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RotatePlcIn {
    /// Count DIDs still on a retired key, change nothing.
    #[serde(default)]
    dry_run: bool,
}

/// PLC updates in flight per rotatePlcKeys run (the directory rate-limits).
const ROTATE_PLC_CONCURRENCY: usize = 4;

/// For every did:plc account of the shards this node owns whose DID lists
/// a retired server rotation key (`--plc-rotation-key-old-file`) and not
/// the current one: a PLC update signed by the retired key that lists the
/// current key instead. Run it on every node after rolling out a new
/// rotation key, then with `dryRun` until each reports `rotated: 0` before
/// retiring the old key. Idempotent. `foreign`: DIDs that list none of our
/// keys (migrated away).
async fn rotate_plc_keys(State(app): AppState, Auth(creds): Auth, body: Option<Json<RotatePlcIn>>) -> XResult<Json<J>> {
    use futures::StreamExt;
    require_admin(&creds)?;
    let plc = app.plc.clone().ok_or_else(|| invalid_request("PLC registration is off on this PDS"))?;
    let dry = body.map(|Json(b)| b).unwrap_or_default().dry_run;
    let mut dids = Vec::new();
    for p in app.partitions.owned() {
        let mut it = state::FamilyScan::new(p.db.as_ref(), state::ACCOUNT_FAMILY, None, &Default::default())
            .await
            .map_err(XrpcError::from_err)?;
        while let Some(kv) = it.next().await.map_err(XrpcError::from_err)? {
            let a: Account = serde_json::from_slice(&kv.value).map_err(XrpcError::from_err)?;
            if crate::plc::valid_plc_did(&a.did) {
                dids.push(a.did);
            }
        }
    }
    let accounts = dids.len();
    let results: Vec<(String, Result<crate::plc::KeyRotation, crate::plc::PlcError>)> = futures::stream::iter(dids)
        .map(|did| {
            let plc = plc.clone();
            async move {
                let r = plc.rotate_server_key(&did, dry).await;
                (did, r)
            }
        })
        .buffer_unordered(ROTATE_PLC_CONCURRENCY)
        .collect()
        .await;
    let (mut current, mut rotated, mut foreign) = (0u64, 0u64, 0u64);
    let mut errors = Vec::new();
    for (did, r) in results {
        match r {
            Ok(crate::plc::KeyRotation::Current) => current += 1,
            Ok(crate::plc::KeyRotation::Rotated) => rotated += 1,
            Ok(crate::plc::KeyRotation::Foreign) => foreign += 1,
            // synthetic (bulkCreate) DIDs were never registered
            Err(crate::plc::PlcError::NotFound(_)) => foreign += 1,
            Err(e) => errors.push(format!("{did}: {e}")),
        }
    }
    tracing::info!(accounts, current, rotated, foreign, errors = errors.len(), dry_run = dry, rotation_key = plc.rotation_did_key(), "rotate PLC keys");
    let failed = errors.len();
    errors.truncate(20);
    Ok(Json(json!({
        "rotationKey": plc.rotation_did_key(),
        "dryRun": dry,
        "accounts": accounts,
        "current": current,
        "rotated": rotated,
        "foreign": foreign,
        "failed": failed,
        "errors": errors,
    })))
}

// ---------------------------------------------------------------------------
// KEK rotation (DESIGN.md "Secrets at rest")
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RewrapIn {
    /// Count what is stale, change nothing.
    #[serde(default)]
    dry_run: bool,
    /// Also unwrap blobs already under the current KEK's id, to find (and
    /// rewrap) ones under an older version of a Cloud KMS key. One KMS
    /// decrypt per secret.
    #[serde(default)]
    check_versions: bool,
}

/// Rewraps every secret at rest in the shards this node owns under the
/// current KEK: account signing keys, reserved signing keys and TOTP
/// secrets. Run it on every node after adding a new KEK (the old one
/// still configured for unwrap), and again with `dryRun` until each
/// reports `stale: 0` before retiring the old KEK. Shards that move during
/// a run are covered by running it again. Idempotent.
async fn rewrap_secrets(State(app): AppState, Auth(creds): Auth, body: Option<Json<RewrapIn>>) -> XResult<Json<J>> {
    use crate::secrets::Purpose;
    use futures::StreamExt;
    require_admin(&creds)?;
    let inp = body.map(|Json(b)| b).unwrap_or_default();
    let started = std::time::Instant::now();
    // the accounts of the owned shards
    let mut dids = Vec::new();
    for p in app.partitions.owned() {
        let mut it = state::FamilyScan::new(p.db.as_ref(), state::ACCOUNT_FAMILY, None, &Default::default())
            .await
            .map_err(XrpcError::from_err)?;
        while let Some(kv) = it.next().await.map_err(XrpcError::from_err)? {
            let a: Account = serde_json::from_slice(&kv.value).map_err(XrpcError::from_err)?;
            dids.push((a.did, a.wrapped_signing_key));
        }
    }
    let (accounts, check, dry) = (dids.len(), inp.check_versions, inp.dry_run);
    let app2 = app.clone();
    // (stale signing key, stale TOTP, errors)
    let results: Vec<(bool, bool, Option<String>)> = futures::stream::iter(dids)
        .map(|(did, blob)| {
            let app = app2.clone();
            async move {
                let r: XResult<(bool, bool)> = async {
                    let key_stale = if !check && app.secrets.is_current(&blob) {
                        false
                    } else if dry {
                        app.secrets.unwrap(Purpose::SigningKey, &did, &blob).await?.stale
                    } else {
                        match app.secrets.rewrap(Purpose::SigningKey, &did, &blob).await? {
                            None => false,
                            Some(new) => {
                                update_account(&app, &did, false, false, move |a| {
                                    // unless rotated meanwhile
                                    if a.wrapped_signing_key == blob {
                                        a.wrapped_signing_key = new;
                                    }
                                    Ok(())
                                })
                                .await?;
                                true
                            }
                        }
                    };
                    let totp_stale = crate::totp::rewrap(&app, &did, check, dry).await?;
                    Ok((key_stale, totp_stale))
                }
                .await;
                match r {
                    Ok((k, t)) => (k, t, None),
                    Err(e) => (false, false, Some(format!("{did}: {}", e.message))),
                }
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;
    let mut errors: Vec<String> = Vec::new();
    let (mut keys, mut totp) = (0u64, 0u64);
    for (k, t, e) in results {
        keys += k as u64;
        totp += t as u64;
        errors.extend(e);
    }
    // reserved signing keys (did:key-indexed rows carry the wrapped key)
    let mut reserved = 0u64;
    for (routing, name, val) in super::server::scan_private_routing(&app, "_reserved:").await? {
        let Some(did_key) = routing.strip_prefix("_reserved:").filter(|r| r.starts_with("did:key:") && name == "k") else {
            continue;
        };
        let Ok(mut rec) = serde_json::from_slice::<J>(&val) else { continue };
        let Some(blob) = rec["key"].as_str().map(str::to_string) else { continue };
        if !check && app.secrets.is_current(&blob) {
            continue;
        }
        let r = if dry {
            app.secrets.unwrap(Purpose::ReservedKey, did_key, &blob).await.map(|u| u.stale.then_some(String::new()))
        } else {
            app.secrets.rewrap(Purpose::ReservedKey, did_key, &blob).await
        };
        match r {
            Ok(None) => {}
            Ok(Some(_)) if dry => reserved += 1,
            Ok(Some(new)) => {
                rec["key"] = json!(new);
                app.put_private(&routing, vec![pmut(&routing, "k", Some(to_json_bytes(&rec)))]).await?;
                reserved += 1;
            }
            Err(e) => errors.push(format!("{did_key}: {e}")),
        }
    }
    let stale = keys + totp + reserved;
    tracing::info!(accounts, signing_keys = keys, totp, reserved, errors = errors.len(), dry_run = dry, kek = app.secrets.current_kid(), elapsed_ms = started.elapsed().as_millis() as u64, "rewrap secrets");
    let failed = errors.len();
    errors.truncate(20);
    Ok(Json(json!({
        "kek": app.secrets.current_kid(),
        "dryRun": dry,
        "accounts": accounts,
        // stale secrets found (dry run) or rewrapped
        "stale": stale,
        "signingKeys": keys,
        "totpSecrets": totp,
        "reservedKeys": reserved,
        "failed": failed,
        "errors": errors,
    })))
}
