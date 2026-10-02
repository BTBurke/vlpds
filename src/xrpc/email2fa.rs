//! Email second factor: the reference PDS's `emailAuthFactor` (the factor
//! the Bluesky app offers), next to vlpds's own TOTP (src/totp.rs).
//!
//! - State: `Account.extra.emailAuthFactorAt` (RFC 3339; absent = off).
//!   Toggled through `com.atproto.server.updateEmail` with the account's
//!   current address: enabling needs a confirmed email and no token;
//!   disabling is two-phase (the first call mails an `update_email` code and
//!   fails `TokenRequired`, the second carries it), so a hijacked session
//!   can't silently drop the factor. Changing the address (user or admin)
//!   clears the factor, as in the reference: codes must not go to an
//!   unconfirmed inbox.
//! - Sign-in: a password login (createSession or the OAuth sign-in page;
//!   app passwords bypass it, as in the reference) with the factor on and no
//!   code mails a fresh `auth_factor` code (15 min, single use, keyed digest
//!   at rest like every email token) and fails 401
//!   `AuthFactorTokenRequired`; `authFactorToken` carries the code back.
//! - Guessing: wrong codes count against a per-account lockout with TOTP's
//!   schedule (every [`crate::totp::MAX_FAILURES`] wrong codes lock the
//!   factor for 5 min, doubling up to a day; 429 `RateLimitExceeded` while
//!   locked, and no code is mailed then), on top of createSession's
//!   per-identifier rate limits. The reference has only the latter.
//!
//! Precedence: when TOTP is also enabled, TOTP alone is asked for (a TOTP
//! or recovery code); no email code is mailed or accepted. Both can stay
//! enabled, so disabling TOTP falls back to the email factor, but the
//! weaker factor never stands in for the stronger one.

use super::server::{
    assert_email_token, create_email_token, delete_email_tokens, deliver, get_json, invalid_request, pmut,
    to_json_bytes,
};
use super::*;

/// Account field holding when the factor was enabled.
pub(super) const FLAG: &str = "emailAuthFactorAt";
/// Email-token purpose of sign-in codes.
pub(super) const PURPOSE: &str = "auth_factor";
/// Private-state name of the wrong-code lockout counters.
const LOCKOUT_NAME: &str = "eotp_lock";

pub(super) fn enabled(a: &Account) -> bool {
    a.extra.get(FLAG).is_some_and(|v| v.is_string())
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Lockout {
    failures: u32,
    locked_until: u64,
}

/// The factor a failed second-factor check asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Factor {
    Totp,
    /// A code was (or would have been) mailed; `hint` is the obfuscated
    /// address, as the reference shows it (`a***e@e***m`).
    Email { hint: String },
}

pub(super) struct FactorErr {
    pub err: XrpcError,
    pub factor: Factor,
}

impl From<FactorErr> for XrpcError {
    fn from(e: FactorErr) -> XrpcError {
        e.err
    }
}

/// Reference `obfuscateEmail`: first and last character of each side.
pub(super) fn obfuscate_email(email: &str) -> String {
    fn word(w: &str) -> String {
        let first = w.chars().next().map(String::from).unwrap_or_default();
        let last = w.chars().last().map(String::from).unwrap_or_default();
        format!("{first}***{last}")
    }
    let (local, domain) = email.split_once('@').unwrap_or((email, ""));
    format!("{}@{}", word(local), word(domain))
}

fn factor_required() -> XrpcError {
    XrpcError {
        status: StatusCode::UNAUTHORIZED,
        error: "AuthFactorTokenRequired".into(),
        message: "A sign in code has been sent to your email address".into(),
    }
}

/// Second factor of a password login (createSession, OAuth sign-in): TOTP
/// when enabled, else the email factor when enabled, else nothing.
pub(super) async fn check_second_factor(app: &App, acct: &Account, code: Option<&str>) -> Result<(), FactorErr> {
    let code = code.map(str::trim).filter(|c| !c.is_empty());
    let totp = crate::totp::enabled_for(app, acct)
        .await
        .map_err(|err| FactorErr { err, factor: Factor::Totp })?;
    if totp {
        return crate::totp::check_second_factor(app, acct, code)
            .await
            .map_err(|err| FactorErr { err, factor: Factor::Totp });
    }
    match (&acct.email, enabled(acct)) {
        (Some(email), true) => {
            let factor = Factor::Email { hint: obfuscate_email(email) };
            check_email_code(app, acct, email, code).await.map_err(|err| FactorErr { err, factor })
        }
        _ => Ok(()),
    }
}

async fn check_email_code(app: &App, acct: &Account, email: &str, code: Option<&str>) -> XResult<()> {
    let did = acct.did.as_str();
    // the TOTP lock also serializes this factor's counter updates
    let _g = crate::totp::lock(did).await;
    let mut lk: Lockout = get_json(app, did, LOCKOUT_NAME).await?.unwrap_or_default();
    let now = crate::totp::now_secs();
    if now < lk.locked_until {
        return Err(crate::totp::locked_out());
    }
    let Some(code) = code else {
        let token = create_email_token(app, did, PURPOSE).await?;
        deliver(app, email, crate::mail::Email::SignInAuthFactor { handle: Some(&acct.handle), token: &token });
        return Err(factor_required());
    };
    match assert_email_token(app, did, PURPOSE, code).await {
        Ok(()) => {
            delete_email_tokens(app, did, &[PURPOSE]).await?;
            if lk.failures > 0 {
                save_lockout(app, did, None).await?;
            }
            Ok(())
        }
        // an expired code was right once: not a guess
        Err(e) if e.error != "InvalidToken" => Err(e),
        Err(e) => {
            crate::totp::record_failure_in(&mut lk.failures, &mut lk.locked_until, now);
            save_lockout(app, did, Some(&lk)).await?;
            Err(if now < lk.locked_until { crate::totp::locked_out() } else { e })
        }
    }
}

async fn save_lockout(app: &App, did: &str, lk: Option<&Lockout>) -> XResult<()> {
    app.put_private(did, vec![pmut(did, LOCKOUT_NAME, lk.map(to_json_bytes))]).await
}

/// updateEmail `emailAuthFactor: true` on the confirmed current address.
/// Idempotent; no token needed (enabling only adds protection).
pub(super) async fn enable(app: &App, did: &str) -> XResult<()> {
    app.mutate_account(did, false, false, false, |a| {
        if enabled(a) {
            return Ok(false);
        }
        if a.email.is_none() || !a.email_confirmed {
            return Err(invalid_request(
                "A confirmed email address is required to enable email-based two-factor authentication",
            ));
        }
        super::server::set_extra(a, FLAG, json!(crate::events::now_rfc3339()));
        Ok(true)
    })
    .await?;
    Ok(())
}

/// updateEmail `emailAuthFactor: false` on the current address. Without a
/// token: mails an `update_email` code (the token requestEmailUpdate mints
/// too, which is what the Bluesky app sends) and fails `TokenRequired`.
/// With one: checks and spends it, then clears the factor. A no-op when the
/// factor is already off.
pub(super) async fn disable(app: &App, acct: &Account, token: Option<&str>) -> XResult<()> {
    if !enabled(acct) {
        return Ok(());
    }
    let did = acct.did.as_str();
    let email = acct.email.clone().ok_or_else(|| XrpcError::internal("account has no email address"))?;
    let Some(token) = token.map(str::trim).filter(|t| !t.is_empty()) else {
        let otp = create_email_token(app, did, "update_email").await?;
        deliver(app, &email, crate::mail::Email::UpdateEmail { token: &otp });
        return Err(XrpcError::bad("TokenRequired", "confirmation token required"));
    };
    assert_email_token(app, did, "update_email", token).await?;
    delete_email_tokens(app, did, &["update_email"]).await?;
    app.mutate_account(did, false, false, false, move |a| {
        // only the address the code went to (an email change clears it anyway)
        if !enabled(a) || a.email.as_deref() != Some(email.as_str()) {
            return Ok(false);
        }
        a.extra.remove(FLAG);
        Ok(true)
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obfuscates_like_the_reference() {
        assert_eq!(obfuscate_email("alice@example.com"), "a***e@e***m");
        assert_eq!(obfuscate_email("a@b.co"), "a***a@b***o");
    }
}
