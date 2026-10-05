//! Passkeys (docs/oauth-2fa.md "Passkeys"): the account's WebAuthn
//! credentials, their registration and removal on the Security page, and
//! the checks the sign-in paths share (`crate::webauthn` does the
//! cryptography).
//!
//! One private row per account, `p/{did}\0passkeys` ([`Passkeys`]), written
//! with a compare-and-set at the account's owner. Public keys aren't
//! secrets, so it isn't KEK-wrapped: a passkey sign-in never needs the key
//! service.
//!
//! Challenges are stateless (`webauthn::mint_challenge`, keyed from
//! `jwt_secret`): any node mints and checks them, rendering a sign-in page
//! writes nothing, and each is claimed once, cluster-wide, at the account's
//! owner after its signature verified.

use super::cas::{Cond, Op};
use super::server::{now_secs, to_json_bytes};
use super::*;
use crate::oauth::util::{b64u, b64u_decode};
use crate::webauthn::{self, Fail};
use sha2::{Digest, Sha256};

pub(super) const ROW: &str = "passkeys";
pub const MAX_PASSKEYS: usize = 20;
pub const MAX_NAME: usize = 64;
const CAS_ROUNDS: usize = 8;

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Passkeys {
    #[serde(default)]
    pub creds: Vec<Cred>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Cred {
    /// The credential id, base64url.
    pub id: String,
    /// The COSE_Key as registered, base64url.
    pub public_key: String,
    pub alg: i64,
    pub sign_count: u32,
    #[serde(default, skip_serializing_if = "is_false")]
    pub backup_eligible: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub backed_up: bool,
    /// `credProps.rk`, when the browser said.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discoverable: Option<bool>,
    /// The registration was user-verified (a PIN or biometric).
    #[serde(default, skip_serializing_if = "is_false")]
    pub uv: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transports: Vec<String>,
    /// Hex; all zeros for most passkeys (attestation "none").
    pub aaguid: String,
    pub name: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<u64>,
    /// A counter went backwards on a key that can't be synced: refused from
    /// then on until the owner removes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspect_at: Option<u64>,
}

impl Cred {
    /// What a session records as the passkey that signed it in.
    pub fn auth_ref(&self) -> String {
        auth_ref(&self.id)
    }
}

/// Short and fixed-size, so session rows don't carry a 1 KiB credential id.
pub(super) fn auth_ref(id: &str) -> String {
    format!("pk:{}", hex::encode(&Sha256::digest(id.as_bytes())[..12]))
}

pub(super) async fn load_raw(app: &App, did: &str) -> XResult<(Passkeys, Option<Bytes>)> {
    match app.get_private(did, ROW).await? {
        Some(v) => Ok((serde_json::from_slice(&v).map_err(XrpcError::from_err)?, Some(v))),
        None => Ok((Passkeys::default(), None)),
    }
}

pub(super) async fn load(app: &App, did: &str) -> XResult<Passkeys> {
    Ok(load_raw(app, did).await?.0)
}

/// Ok(false): the row changed since `read`; nothing was written.
pub(super) async fn save_if(app: &App, did: &str, p: &Passkeys, read: Option<Bytes>) -> XResult<bool> {
    let val = (!p.creds.is_empty()).then(|| Bytes::from(to_json_bytes(p)));
    Ok(app.private_cas(did, vec![Cond::eq(ROW, read)], vec![Op::put(ROW, val)]).await?.applied)
}

pub(super) async fn has_any(app: &App, did: &str) -> XResult<bool> {
    Ok(!load(app, did).await?.creds.is_empty())
}

/// The challenge MAC key, from `jwt_secret` like the CSRF and DPoP keys.
pub(super) fn challenge_key(app: &App) -> [u8; 32] {
    crate::oauth::util::derive_secret(&app.config.jwt_secret, "webauthn")
}

pub(super) fn rp(app: &App) -> XResult<webauthn::Rp> {
    webauthn::Rp::from_public_url(&app.public_url).ok_or_else(|| XrpcError::internal("public URL has no host"))
}

/// The WebAuthn user handle: the DID's bytes, which a discoverable sign-in
/// hands back so the finish goes to that account's owner with no index.
/// None for a DID over the spec's 64 bytes (a long did:web), whose passkeys
/// are second factors only.
pub fn user_handle(did: &str) -> Option<&[u8]> {
    (did.len() <= webauthn::MAX_USER_HANDLE).then_some(did.as_bytes())
}

/// The DID a user handle names, if it could be one.
pub fn did_from_user_handle(b64: &str) -> Option<String> {
    if b64.len() > 88 {
        return None;
    }
    let b = b64u_decode(b64)?;
    let s = String::from_utf8(b).ok()?;
    (s.starts_with("did:") && s.len() <= webauthn::MAX_USER_HANDLE && s.bytes().all(|c| c.is_ascii_graphic()))
        .then_some(s)
}

pub(super) fn count_failure(f: Fail) {
    crate::metrics::PASSKEY_FAILURES.with_label_values(&[f.reason()]).inc();
}

/// An assertion as the browser posted it, each part base64url.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AssertionIn {
    pub id: String,
    pub client_data_json: String,
    pub authenticator_data: String,
    pub signature: String,
    #[serde(default)]
    pub user_handle: Option<String>,
}

/// Base64 text over these is refused before decoding.
const MAX_ID_B64: usize = (webauthn::MAX_CREDENTIAL_ID * 4).div_ceil(3);
const MAX_CDJ_B64: usize = (webauthn::MAX_CLIENT_DATA * 4).div_ceil(3);
const MAX_AD_B64: usize = (webauthn::MAX_AUTHENTICATOR_DATA * 4).div_ceil(3);
const MAX_SIG_B64: usize = (webauthn::MAX_SIGNATURE * 4).div_ceil(3);
pub(super) const MAX_ATT_B64: usize = (webauthn::MAX_ATTESTATION_OBJECT * 4).div_ceil(3);

pub(super) fn decode_capped(s: &str, max: usize) -> Result<Vec<u8>, Fail> {
    if s.len() > max {
        return Err(Fail::TooLarge);
    }
    b64u_decode(s).ok_or(Fail::Malformed)
}

struct Decoded {
    id: Vec<u8>,
    cdj: Vec<u8>,
    ad: Vec<u8>,
    sig: Vec<u8>,
}

fn decode(a: &AssertionIn) -> Result<Decoded, Fail> {
    Ok(Decoded {
        id: decode_capped(&a.id, MAX_ID_B64)?,
        cdj: decode_capped(&a.client_data_json, MAX_CDJ_B64)?,
        ad: decode_capped(&a.authenticator_data, MAX_AD_B64)?,
        sig: decode_capped(&a.signature, MAX_SIG_B64)?,
    })
}

/// What a challenge was minted for: `purpose` and `binding` are checked by
/// its MAC.
pub(super) struct Expect<'a> {
    pub purpose: &'a str,
    pub binding: String,
    /// Passwordless needs a PIN or biometric; a second factor only presence.
    pub require_uv: bool,
}

/// A passkey that just signed in.
pub(super) struct Used {
    pub cred: Cred,
}

/// Why a passkey sign-in failed: a [`Fail`] for the metrics, or a server
/// error to pass on.
pub(super) enum UseErr {
    Refused(Fail),
    Server(XrpcError),
}

impl From<XrpcError> for UseErr {
    fn from(e: XrpcError) -> UseErr {
        UseErr::Server(e)
    }
}

impl From<Fail> for UseErr {
    fn from(f: Fail) -> UseErr {
        UseErr::Refused(f)
    }
}

/// Checks an assertion for `did` end to end: the challenge (purpose,
/// binding, expiry), every ceremony check, that the credential is one of
/// the account's, then claims the challenge once cluster-wide (after the
/// signature, so junk never costs a write) and applies the counter rule
/// with a compare-and-set, which also fails if the passkey was removed
/// meanwhile. Counts the failure reason.
pub(super) async fn use_passkey(app: &App, did: &str, a: &AssertionIn, ex: &Expect<'_>) -> Result<Used, UseErr> {
    let r = use_inner(app, did, a, ex).await;
    if let Err(UseErr::Refused(f)) = &r {
        count_failure(*f);
    }
    r
}

async fn use_inner(app: &App, did: &str, a: &AssertionIn, ex: &Expect<'_>) -> Result<Used, UseErr> {
    let d = decode(a)?;
    let now = now_secs();
    let challenge = webauthn::client_data_challenge(&d.cdj)?;
    let ch = webauthn::open_challenge(&challenge_key(app), ex.purpose, &ex.binding, &challenge, now)?;
    let id = b64u(&d.id);
    let (row, _) = load_raw(app, did).await?;
    let cred = row.creds.iter().find(|c| c.id == id).cloned().ok_or(Fail::UnknownCredential)?;
    if cred.suspect_at.is_some() {
        return Err(Fail::Counter.into());
    }
    let key = webauthn::PublicKey::from_cose(&b64u_decode(&cred.public_key).ok_or(Fail::Key)?)?;
    let rp = rp(app)?;
    let got = webauthn::verify_assertion(&rp, &challenge, &key, &d.cdj, &d.ad, &d.sig, ex.require_uv)?;
    // the BE flag is fixed for a credential's life
    if got.backup_eligible != cred.backup_eligible {
        return Err(Fail::Malformed.into());
    }
    match super::internal::claim_replay_anywhere(app, did, &ch.replay_key(), ch.exp as i64).await? {
        true => {}
        false => return Err(Fail::Replay.into()),
    }
    let mut regressed = None;
    for _ in 0..CAS_ROUNDS {
        let (mut row, raw) = load_raw(app, did).await?;
        let Some(c) = row.creds.iter_mut().find(|c| c.id == id) else {
            return Err(Fail::UnknownCredential.into());
        };
        if c.suspect_at.is_some() {
            return Err(Fail::Counter.into());
        }
        let back = webauthn::counter_regressed(c.sign_count, got.sign_count);
        if back && !c.backup_eligible {
            // a hardware key reporting an older count: a clone, or replayed
            // signatures. Refused, and flagged until the owner removes it.
            c.suspect_at = Some(now);
        } else {
            c.sign_count = c.sign_count.max(got.sign_count);
            c.last_used_at = Some(now);
            c.backed_up = got.backed_up;
        }
        let out = c.clone();
        if save_if(app, did, &row, raw).await? {
            if back {
                regressed = Some(out.backup_eligible);
            }
            if out.suspect_at.is_some() {
                crate::metrics::PASSKEY_COUNTER_REGRESSIONS.with_label_values(&["refused"]).inc();
                tracing::warn!(did, "passkey counter went backwards on a hardware key: flagged");
                suspect_mail(app, did, &out).await;
                return Err(Fail::Counter.into());
            }
            if regressed == Some(true) {
                crate::metrics::PASSKEY_COUNTER_REGRESSIONS.with_label_values(&["accepted"]).inc();
                tracing::info!(did, "passkey counter went backwards on a synced passkey: accepted");
            }
            return Ok(Used { cred: out });
        }
    }
    Err(super::server::cas_conflict().into())
}

async fn suspect_mail(app: &App, did: &str, c: &Cred) {
    let what = format!(
        "Your passkey \u{201c}{}\u{201d} was refused because it looks like it was copied. Remove it on the Security page, and add it again if it's yours.",
        c.name
    );
    security_mail(app, did, &what).await;
}

/// Mails the owner about a change to how the account signs in. Never fails
/// the change: a refused budget or a missing address is only logged.
pub(super) async fn security_mail(app: &App, did: &str, what: &str) {
    let Ok(acct) = super::internal::account_anywhere(app, did).await else { return };
    let Some(email) = acct.email.clone() else { return };
    let permit = match super::server::mail_permit(app, Some(did), &email, crate::mail::SECURITY_PURPOSE, false).await {
        Ok(p) => p,
        Err(e) => {
            tracing::info!(did, error = %e.message, "security mail not sent");
            return;
        }
    };
    let at = chrono::DateTime::from_timestamp(now_secs() as i64, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default();
    super::server::deliver(
        app,
        permit,
        &email,
        crate::mail::Email::SecurityChange { handle: &acct.handle, what, at: &at },
    );
}

/// Golden fixtures (`super::private_rows`).
pub(super) fn fixture_rows(did: &str) -> Vec<super::private_rows::PrivateRow> {
    let p = Passkeys {
        creds: vec![
            Cred {
                id: "9y1xA8Tmg1FEmT-c7_fvWZ_uoTuoih3OvR45_oAK-cwHWhAbXrl2q62iLVTjiyEZ7O7n-CROOY494k7Q3xrs_w".into(),
                public_key: "pQECAyYgASFYIEhW1CRfuNlIN6XTPKw0RbvzeaIlRMrDwwep-uq_-3WQIlgg1FZwd_RZRsqS_qgKCDvcVh7ScoKNo3w5h5fv3ihUSww".into(),
                alg: webauthn::ALG_ES256,
                sign_count: 23,
                backup_eligible: true,
                backed_up: true,
                discoverable: Some(true),
                uv: true,
                transports: vec!["internal".into(), "hybrid".into()],
                aaguid: "00".repeat(16),
                name: "iPhone".into(),
                created_at: 1_790_000_000,
                last_used_at: Some(1_790_000_100),
                suspect_at: None,
            },
            Cred {
                id: "AAEC".into(),
                public_key: "pAEBAycgBiFYIMz6_SUFLiDid2Yhlq0YboyJ-CDrIrNpkPUGmJp4D3Dp".into(),
                alg: webauthn::ALG_EDDSA,
                sign_count: 7,
                aaguid: "ee".repeat(16),
                name: "YubiKey".into(),
                created_at: 1_790_000_200,
                suspect_at: Some(1_790_000_300),
                ..Default::default()
            },
        ],
    };
    vec![(did.into(), ROW.into(), to_json_bytes(&p))]
}

pub(super) fn check_row(routing: &str, name: &str, val: &[u8]) -> Option<anyhow::Result<&'static str>> {
    (routing.starts_with("did:") && name == ROW).then(|| super::private_rows::typed_row::<Passkeys>("passkeys", val))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_handles() {
        let did = "did:plc:abcdefghijklmnopqrstuvwx";
        assert_eq!(user_handle(did), Some(did.as_bytes()));
        assert_eq!(did_from_user_handle(&b64u(did)).as_deref(), Some(did));
        let long = format!("did:web:{}.example", "a".repeat(60));
        assert_eq!(user_handle(&long), None);
        assert_eq!(did_from_user_handle(&b64u(&long)), None);
        assert_eq!(did_from_user_handle(&b64u("not a did")), None);
        assert_eq!(did_from_user_handle("!!"), None);
        assert_eq!(auth_ref("AAEC").len(), 3 + 24);
    }
}
