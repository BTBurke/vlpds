//! `com.atproto.simplespace.*` (`--spaces`): the space host role for spaces
//! anchored on an account hosted here. Space state (`sS`, `sM`) lives in
//! the authority's shard and changes through its repo worker.

use super::authn::SpaceAuth;
use super::space::{assert_credential_space, spaces, submit_space, Space};
use super::*;
use crate::oauth::scopes::SpaceAccess;
use crate::space::repo::SpaceOp;
use crate::space::rows::{AppAccess, MemberRow, Policy, SpaceRow};

pub fn routes() -> Router<Arc<App>> {
    Router::new()
        .route("/xrpc/com.atproto.simplespace.createSpace", post(create_space))
        .route("/xrpc/com.atproto.simplespace.getSpace", get(get_space))
}

const DEFS: &str = "com.atproto.simplespace.defs";

fn lex_type(v: &J) -> &str {
    v.get("$type").and_then(|t| t.as_str()).unwrap_or("")
}

/// Reference `lexPolicyToDb`.
fn policy_from_lex(v: &J) -> XResult<Policy> {
    let t = lex_type(v);
    match t.strip_prefix(DEFS).unwrap_or("") {
        "#publicPolicy" => Ok(Policy::Public),
        "#memberListPolicy" => Ok(Policy::MemberList),
        "#managingAppPolicy" => {
            let app = v.get("managingApp").and_then(|a| a.as_str()).unwrap_or("");
            if !app.starts_with("did:") {
                return Err(XrpcError::bad(
                    "UnsupportedPolicy",
                    format!("managingApp must be a DID with an optional service fragment, got: {app}"),
                ));
            }
            Ok(Policy::ManagingApp { managing_app: app.into() })
        }
        _ => Err(XrpcError::bad("UnsupportedPolicy", format!("Unsupported policy: {t}"))),
    }
}

fn policy_to_lex(p: &Policy) -> J {
    match p {
        Policy::Public => json!({"$type": format!("{DEFS}#publicPolicy")}),
        Policy::MemberList => json!({"$type": format!("{DEFS}#memberListPolicy")}),
        Policy::ManagingApp { managing_app } => {
            json!({"$type": format!("{DEFS}#managingAppPolicy"), "managingApp": managing_app})
        }
    }
}

/// Reference `lexAppAccessToDb`.
fn app_access_from_lex(v: &J) -> XResult<AppAccess> {
    let t = lex_type(v);
    match t.strip_prefix(DEFS).unwrap_or("") {
        "#open" => Ok(AppAccess::Open),
        "#allowList" => {
            let allowed = v
                .get("allowed")
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|c| c.as_str().map(String::from)).collect::<Vec<_>>());
            let allowed = allowed.ok_or_else(|| XrpcError::bad("InvalidRequest", "allowList requires allowed"))?;
            Ok(AppAccess::AllowList { allowed })
        }
        _ => Err(XrpcError::bad("UnsupportedAppAccess", format!("Unsupported appAccess: {t}"))),
    }
}

fn app_access_to_lex(a: &AppAccess) -> J {
    match a {
        AppAccess::Open => json!({"$type": format!("{DEFS}#open")}),
        AppAccess::AllowList { allowed } => json!({"$type": format!("{DEFS}#allowList"), "allowed": allowed}),
    }
}

/// The space's row at its authority, which must be an account hosted here
/// (reference `assertSpaceHost`). A deleted space is its tombstone.
pub(super) async fn space_row(app: &App, space: &Space) -> XResult<SpaceRow> {
    let not_found = || XrpcError::bad("SpaceNotFound", "Space not found");
    if super::server::account_if_exists(app, &space.authority).await?.is_none() {
        return Err(not_found());
    }
    let p = app.partition(&space.authority)?;
    let v = p.db.get(state::space_key(&space.authority, &space.sid)).await.map_err(XrpcError::from_err)?;
    let row = SpaceRow::decode(&v.ok_or_else(not_found)?).map_err(XrpcError::from_err)?;
    if row.uri != space.uri {
        return Err(XrpcError::internal(format!("space id collision: {} and {}", row.uri, space.uri)));
    }
    Ok(row)
}

/// Reference `authorizeUser` for reading: the authority can't lock itself
/// out; a member-list space admits members with `read`. A managing app's
/// verdict comes with the space host's policies; until then it admits no
/// one but the authority.
pub(super) async fn may_read(app: &App, space: &Space, row: &SpaceRow, user: &str) -> XResult<bool> {
    if user == space.authority {
        return Ok(true);
    }
    match &row.read_policy {
        Policy::Public => Ok(true),
        Policy::MemberList => {
            let p = app.partition(&space.authority)?;
            let k = state::space_member_key(&space.authority, &space.sid, user);
            let m = p.db.get(k).await.map_err(XrpcError::from_err)?;
            Ok(m.map(|v| MemberRow::decode(&v)).transpose().map_err(XrpcError::from_err)?.is_some_and(|m| m.read))
        }
        Policy::ManagingApp { .. } => Ok(false),
    }
}

/// Reference createSpace: anchored on the caller, `skey` a TID unless
/// given.
async fn create_space(State(app): AppState, Auth(creds): Auth, Json(inp): Json<J>) -> XResult<Json<J>> {
    spaces(&app)?;
    let did = creds.user_did()?.to_string();
    let space_type = inp.get("spaceType").and_then(|t| t.as_str()).unwrap_or("");
    let skey = match inp.get("skey").and_then(|s| s.as_str()) {
        Some(s) => s.to_string(),
        None => app.tids.next().to_string(),
    };
    let space = Space::parse(&format!("at://{did}/space/{space_type}/{skey}"))?;
    creds.need_space(&space.target(), SpaceAccess::Manage("create"))?;
    let read_policy = policy_from_lex(&inp["readPolicy"])?;
    let write_policy = policy_from_lex(&inp["writePolicy"])?;
    let app_access = app_access_from_lex(&inp["appAccess"])?;
    let row = SpaceRow {
        uri: space.uri.clone(),
        read_policy,
        write_policy,
        app_access,
        created_at: crate::events::now_rfc3339(),
        deleted_at: None,
    };
    submit_space(&app, &did, &space, SpaceOp::CreateSpace { row }).await?;
    Ok(Json(json!({"uri": space.uri})))
}

#[derive(Deserialize)]
struct SpaceQ {
    space: String,
}

/// Reference getSpace: the authority itself, or a credential addressed to
/// the authority (a member hosted elsewhere can't present OAuth here).
async fn get_space(State(app): AppState, SpaceAuth(creds): SpaceAuth, Query(q): Query<SpaceQ>) -> XResult<Json<J>> {
    spaces(&app)?;
    let space = Space::parse(&q.space)?;
    match &creds {
        Credentials::SpaceCredential { audience, space: s, .. } => {
            assert_credential_space(audience, s, &space, &space.authority)?
        }
        c => {
            c.need_space(&space.target(), SpaceAccess::ReadSelf)?;
            if c.did() != Some(space.authority.as_str()) {
                return Err(XrpcError::bad("NotSpaceOwner", "Not the space owner"));
            }
        }
    }
    let row = space_row(&app, &space).await?;
    if row.deleted_at.is_some() {
        return Err(XrpcError::bad("SpaceNotFound", "Space not found"));
    }
    Ok(Json(json!({
        "uri": row.uri,
        "readPolicy": policy_to_lex(&row.read_policy),
        "writePolicy": policy_to_lex(&row.write_policy),
        "appAccess": app_access_to_lex(&row.app_access),
    })))
}
