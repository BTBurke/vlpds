//! Periodic cleanup of expired private rows (mostly OAuth) in the partitions
//! this node owns:
//! - `oauth:req:{id}` authorization (PAR) requests and codes past their
//!   expiry; consumed requests (code-reuse tombstones) are kept
//!   [`CONSUMED_REQUEST_RETENTION`] longer so a replayed code still revokes
//!   the session it created;
//! - `oauth:cc:{hash}` PKCE `code_challenge` reuse markers older than
//!   [`CODE_CHALLENGE_REPLAY_TIMEFRAME`];
//! - `oauth:dev:{id}` browser devices unseen for [`AUTHENTICATION_MAX_AGE`]
//!   (every login on them has expired);
//! - `{did}` / `oauth/ses/{id}` sessions past their client's session or
//!   refresh-token lifetime;
//! - `oauth/replay/{hash}` persisted single-use claims (DPoP proof, client
//!   assertion and request-object `jti`s) past their `until`, and the
//!   in-memory replay caches;
//! - and, not OAuth but the same walk over every private row, `sec/rvk/`
//!   session revocations of an account (deleted or not) once every token
//!   they revoke has expired (`xrpc::revocation_expired`).
//!
//! Each tick examines at most a fixed number of keys per partition and
//! deletes at most a fixed number of rows, resuming from a per-partition
//! cursor, so a large keyspace is swept over several ticks. Every candidate
//! is re-read under the row's lock before it is deleted, so a row that was
//! renewed in between (a re-claimed code challenge) survives. All the expiry
//! conditions only become true with time, except a device's `last_seen_at`
//! which a concurrent visit can bump; losing that race just starts a new
//! device session.

use super::client::{
    ClientAuth, REFRESH_LIFETIME, REFRESH_LIFETIME_EXTENDED, SESSION_LIFETIME,
    SESSION_LIFETIME_EXTENDED,
};
use super::store::{self, Device, RequestData, Session};
use super::{OAuthError, AUTHENTICATION_MAX_AGE, CODE_CHALLENGE_REPLAY_TIMEFRAME};
use crate::xrpc::App;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// How long a consumed request is kept after its code expired.
pub const CONSUMED_REQUEST_RETENTION: i64 = 7 * 86_400;
/// Sweep period.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Keys examined per partition per tick.
pub const SCAN_BUDGET: usize = 5_000;
/// Rows deleted per tick (each is one log write).
pub const DELETE_BUDGET: usize = 500;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Request,
    CodeChallenge,
    Device,
    Session,
    Replay,
    Revocation,
}

fn classify(routing: &str, name: &str) -> Option<Kind> {
    if routing.starts_with("oauth:req:") && name == "oauth/req" {
        Some(Kind::Request)
    } else if routing.starts_with("oauth:cc:") && name == "oauth/cc" {
        Some(Kind::CodeChallenge)
    } else if routing.starts_with("oauth:dev:") && name == "oauth/dev" {
        Some(Kind::Device)
    } else if routing.starts_with("did:") && name.starts_with("oauth/ses/") {
        Some(Kind::Session)
    } else if name.starts_with(super::util::REPLAY_ROW) {
        Some(Kind::Replay)
    } else if crate::xrpc::revocation_expired(routing, name, b"", 0).is_some() {
        Some(Kind::Revocation)
    } else {
        None
    }
}

fn request_expired(r: &RequestData, now: i64) -> bool {
    match r.consumed {
        Some(_) => now > r.expires_at + CONSUMED_REQUEST_RETENTION,
        None => now > r.expires_at,
    }
}

fn session_expired(s: &Session, now: i64) -> bool {
    let (session_lt, refresh_lt) = match s.client_auth {
        ClientAuth::None => (SESSION_LIFETIME, REFRESH_LIFETIME),
        ClientAuth::PrivateKeyJwt { .. } => (SESSION_LIFETIME_EXTENDED, REFRESH_LIFETIME_EXTENDED),
    };
    now - s.created_at > session_lt || now - s.updated_at > refresh_lt
}

/// Whether the stored value is expired. Unparseable rows count as expired.
fn expired(kind: Kind, routing: &str, name: &str, val: &[u8], now: i64) -> bool {
    match kind {
        // past its window, or claimed for longer than any claim may be (a
        // row from before the cap: util::MAX_CLAIM_TTL)
        Kind::Replay => serde_json::from_slice::<i64>(val)
            .map(|until| until <= now || until > now + super::util::MAX_CLAIM_TTL)
            .unwrap_or(true),
        Kind::Revocation => {
            crate::xrpc::revocation_expired(routing, name, val, now.max(0) as u64).unwrap_or(false)
        }
        Kind::Request => serde_json::from_slice::<RequestData>(val)
            .map(|r| request_expired(&r, now))
            .unwrap_or(true),
        Kind::CodeChallenge => serde_json::from_slice::<i64>(val)
            .map(|at| now - at >= CODE_CHALLENGE_REPLAY_TIMEFRAME)
            .unwrap_or(true),
        Kind::Device => serde_json::from_slice::<Device>(val)
            .map(|d| now - d.last_seen_at > AUTHENTICATION_MAX_AGE)
            .unwrap_or(true),
        Kind::Session => serde_json::from_slice::<Session>(val)
            .map(|s| session_expired(&s, now))
            .unwrap_or(true),
    }
}

fn server_err(e: impl std::fmt::Display) -> OAuthError {
    OAuthError::server_error(&e.to_string())
}

/// The lock the row's writers take (see `store::lock` users).
fn lock_key(kind: Kind, routing: &str, name: &str) -> String {
    match kind {
        Kind::Request => format!("req:{}", routing.trim_start_matches("oauth:req:")),
        Kind::Session => format!("ses:{}", name.trim_start_matches("oauth/ses/")),
        Kind::CodeChallenge | Kind::Device | Kind::Replay | Kind::Revocation => routing.to_string(),
    }
}

/// Re-reads the row under its lock and deletes it if still expired.
async fn delete_if_expired(
    app: &App,
    kind: Kind,
    routing: &str,
    name: &str,
    now: i64,
) -> Result<bool, OAuthError> {
    let _g = store::lock(app, &lock_key(kind, routing, name)).await;
    let Some(val) = app.get_private(routing, name).await? else {
        return Ok(false);
    };
    if !expired(kind, routing, name, &val, now) {
        return Ok(false);
    }
    if kind == Kind::Revocation {
        // through server.rs, which drops its cached view of the account
        crate::xrpc::drop_revocation(app, routing, name).await?;
        return Ok(true);
    }
    // on condition that it still holds what was judged expired: a row
    // rewritten since (a session refreshed on another node) survives
    let cond = crate::xrpc::cas::Cond::eq(name, Some(val));
    store::put_if::<()>(app, routing, name, None, vec![cond]).await
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Keys examined.
    pub scanned: usize,
    /// Rows deleted.
    pub removed: usize,
    /// Expired replay-cache entries dropped.
    pub replay_entries: usize,
    /// Expired persisted single-use claims and session revocations deleted
    /// (counted apart from `removed`; both count against the delete budget).
    pub claims_removed: usize,
}

/// Sweep state: where each partition's scan resumes.
#[derive(Default)]
pub struct Sweeper {
    cursors: HashMap<crate::slots::ShardId, Vec<u8>>,
}

impl Sweeper {
    pub fn new() -> Sweeper {
        Sweeper::default()
    }

    /// One bounded pass: up to `scan_budget` keys per owned partition and
    /// `delete_budget` deletions overall, judged at time `now`.
    pub async fn tick(
        &mut self,
        app: &App,
        now: i64,
        scan_budget: usize,
        delete_budget: usize,
    ) -> Result<SweepStats, OAuthError> {
        let mut st = SweepStats {
            replay_entries: super::util::sweep_replays(app),
            ..Default::default()
        };
        let fam = crate::state::PRIVATE_FAMILY;
        for p in app.partitions.owned() {
            let start = self.cursors.remove(&p.id);
            let mut it = crate::state::FamilyScan::new(p.db.as_ref(), fam, start, &Default::default()).await.map_err(server_err)?;
            let mut examined = 0;
            let mut resume = None;
            while let Some(kv) = it.next().await.map_err(server_err)? {
                if examined >= scan_budget || st.removed + st.claims_removed >= delete_budget {
                    resume = Some(kv.key.to_vec());
                    break;
                }
                examined += 1;
                let rest = String::from_utf8_lossy(&crate::state::key_body(&kv.key)[fam.len()..]);
                let Some((routing, name)) = rest.split_once('\0') else {
                    continue;
                };
                let Some(kind) = classify(routing, name) else {
                    continue;
                };
                if expired(kind, routing, name, &kv.value, now)
                    && delete_if_expired(app, kind, routing, name, now).await?
                {
                    match kind {
                        Kind::Replay | Kind::Revocation => st.claims_removed += 1,
                        _ => st.removed += 1,
                    }
                }
            }
            st.scanned += examined;
            if let Some(k) = resume {
                self.cursors.insert(p.id, k);
            }
        }
        Ok(st)
    }
}

/// Background sweep of expired OAuth rows (every [`SWEEP_INTERVAL`]).
pub fn spawn_gc(app: Arc<App>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sweeper = Sweeper::new();
        let mut tick = tokio::time::interval(SWEEP_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let now = super::util::now_secs();
            match sweeper.tick(&app, now, SCAN_BUDGET, DELETE_BUDGET).await {
                Ok(s) if s.removed + s.claims_removed > 0 => {
                    tracing::info!(removed = s.removed, claims = s.claims_removed, scanned = s.scanned, "oauth gc")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("oauth gc: {}", e.description),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_oauth_rows_only() {
        assert_eq!(
            classify("oauth:req:req-x", "oauth/req"),
            Some(Kind::Request)
        );
        assert_eq!(
            classify("oauth:cc:abc", "oauth/cc"),
            Some(Kind::CodeChallenge)
        );
        assert_eq!(classify("oauth:dev:dev-x", "oauth/dev"), Some(Kind::Device));
        assert_eq!(
            classify("did:plc:x", "oauth/ses/ses-1"),
            Some(Kind::Session)
        );
        assert_eq!(classify("did:plc:x", "oauth/authz/h"), None);
        assert_eq!(classify("oauth:lex:com.example.x", "oauth/lex"), None);
        assert_eq!(classify("_reserved:x", "key"), None);
        assert_eq!(classify("did:plc:x", "oauth/replay/abc"), Some(Kind::Replay));
        assert_eq!(classify("oauth:jkt:j", "oauth/replay/abc"), Some(Kind::Replay));
        assert_eq!(classify("did:plc:x", "sec/rvk/d/0000000000000001"), Some(Kind::Revocation));
        assert_eq!(classify("did:plc:x", "sec/rvk/f/fam"), Some(Kind::Revocation));
        assert_eq!(classify("did:plc:x", "sec/td/rec/a/b"), None);
        assert_eq!(
            lock_key(Kind::Request, "oauth:req:req-1", "oauth/req"),
            "req:req-1"
        );
        assert_eq!(
            lock_key(Kind::Session, "did:plc:x", "oauth/ses/ses-1"),
            "ses:ses-1"
        );
    }

    #[test]
    fn expiry_rules() {
        let now = 1_000_000_000;
        let x = |kind, val: &[u8]| expired(kind, "oauth:x", "n", val, now);
        assert!(!x(Kind::CodeChallenge, b"999999999"));
        assert!(x(
            Kind::CodeChallenge,
            (now - CODE_CHALLENGE_REPLAY_TIMEFRAME)
                .to_string()
                .as_bytes(),
        ));
        assert!(x(Kind::Request, b"not json"));
        assert!(!x(Kind::Replay, (now + 1).to_string().as_bytes()));
        assert!(x(Kind::Replay, now.to_string().as_bytes()));
        let rvk = |val: &[u8]| expired(Kind::Revocation, "did:plc:x", "sec/rvk/f/fam", val, now);
        assert!(!rvk(format!("{{\"exp\":{now}}}").as_bytes()));
        assert!(rvk(format!("{{\"exp\":{}}}", now - 1).as_bytes()));
        assert!(rvk(b"garbage"));
        let s = Session {
            id: "ses-1".into(),
            did: "did:plc:x".into(),
            client_id: "c".into(),
            client_auth: ClientAuth::None,
            dpop_jkt: "j".into(),
            scope: "atproto".into(),
            token_scope: "atproto".into(),
            created_at: now - 10,
            updated_at: now - 10,
            expires_at: 0,
            token_id: "t".into(),
            refresh_gen: 0,
            refresh_salt: "s".into(),
            device_id: None,
            request_id: None,
        };
        assert!(!session_expired(&s, now));
        assert!(session_expired(&s, now + REFRESH_LIFETIME + 1));
        let conf = Session {
            client_auth: ClientAuth::PrivateKeyJwt {
                alg: "ES256".into(),
                kid: "k".into(),
                jkt: "j".into(),
            },
            ..s
        };
        assert!(!session_expired(&conf, now + REFRESH_LIFETIME + 1));
        assert!(session_expired(&conf, now + REFRESH_LIFETIME_EXTENDED + 1));
    }
}
