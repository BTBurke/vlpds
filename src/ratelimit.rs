//! Rate limits with the reference PDS's buckets and values
//! (packages/pds/src/rate-limits.ts and the `rateLimit` arrays of the
//! api/com/atproto/** handlers), in-memory fixed windows like the
//! reference's `MemoryRateLimiter` (rate-limiter-flexible).
//!
//! | bucket | window | points | key |
//! |---|---|---|---|
//! | global-ip (every XRPC call except sync.getRepo) | 5 min | 3000 | client IP |
//! | sync.getRepo | 5 min | 6000 | IP |
//! | server.createSession | 1 day / 5 min | 300 / 30 | identifier + IP |
//! | ↳ sign-in-account (vlpds; any IP, shared with the OAuth sign-in) | 1 h | 100 | DID |
//! | server.createAccount | 5 min | 100 | IP |
//! | server.deleteAccount | 5 min | 50 | IP |
//! | server.requestPasswordReset | 1 day / 1 h | 50 / 15 | IP |
//! | server.resetPassword | 5 min | 50 | IP |
//! | repo.uploadBlob | 1 day | 1000 | IP |
//! | identity.updateHandle | 5 min / 1 day | 10 / 50 | DID |
//! | server.requestAccountDelete, requestEmailConfirmation, requestEmailUpdate (each) | 1 day / 1 h | 15 / 5 | DID |
//! | repo-write-hour / repo-write-day (shared by every repo write) | 1 h / 1 day | 5000 / 35000 | DID; create=3, update=2, delete=1 points |
//! | OAuth sign-in posts (`/oauth/authorize/sign-in`, `/oauth/account/sign-in`) | | | |
//! | ↳ global-ip + oauth-sign-in-ip | 5 min | 3000 / 100 | client IP |
//! | ↳ the createSession buckets (shared with it) | 1 day / 5 min | 300 / 30 | identifier (or pending DID) + IP |
//! | ↳ sign-in-account (vlpds; any IP) | 1 h | 100 | DID |
//! | oauth-ip (vlpds): `/oauth/par`, `/oauth/token`, `/oauth/revoke` | 5 min | 3000 | client IP |
//! | server.reserveSigningKey (vlpds) | 1 h | 100 | IP |
//! | ↳ reserve-signing-key-node (vlpds; calls that reserve a new key) | 1 day | 5000 | the node |
//!
//! The reference's oauth-provider has no sign-in limits of its own; the PDS
//! applies createSession's to its account-manager login. vlpds adds a per-IP
//! cap and a per-account cap across IPs to both createSession and the OAuth
//! sign-in (password guessing is Argon2 CPU); TOTP guessing is bounded
//! separately by the persisted per-account lockout in `crate::totp`. The
//! per-account cap lets anyone hold an account's password sign-ins off for
//! up to an hour (100 guesses); app passwords and live sessions keep
//! working. reserveSigningKey (unauthenticated in the reference too) costs a
//! key-service wrap and a stored row per new key, hence its per-IP and
//! per-node caps (the node cap bounds outstanding reservations: 24 h TTL).
//! The OAuth endpoints' bucket bounds DPoP and client-auth work per address.
//!
//! IPv6 clients are keyed by their /64 ([`ip_key`]).
//!
//! Responses carry `RateLimit-Limit` / `-Remaining` / `-Reset` / `-Policy`
//! for the tightest bucket the request consumed (plus `Retry-After` on a
//! 429 `RateLimitExceeded`), as xrpc-server's `HttpRateLimiter` does.
//!
//! Counters live in 64 mutex-sharded maps keyed by a hash of (bucket,
//! window, key), so there is no global lock; each shard drops expired
//! windows as it is touched (amortized), so memory is bounded by the keys
//! active within the longest window.
//!
//! IP-keyed buckets are checked by [`layer`] before the handler runs;
//! DID- and body-keyed buckets are checked by the handlers through [`check`]
//! once the request is authenticated and parsed (the reference consumes
//! route limits after auth and input validation). Both report into a
//! request-scoped task-local so the response gets the headers.
//!
//! Bypass: admin (`Basic admin:<token>`) and node-to-node
//! (`x-vlpds-internal: <token>`) requests, plus the reference's
//! `x-ratelimit-bypass: <key>` when a bypass key is configured.
//!
//! **Cluster mode.** Counters are per node. Per-DID buckets (repo writes,
//! updateHandle, email flows, sign-in-account) are effectively exact
//! because requests for a DID are forwarded to and served by the node
//! owning its partition (createSession routes by its body's identifier,
//! never by query parameters or an unverified token). Per-IP buckets count
//! what one node serves: requests a node forwards are counted on the owner,
//! keyed by the client address the entry node resolved ([`ClientIp`], sent
//! as [`CLIENT_IP_HEADER`] and trusted only next to the forwarding marker's
//! valid internal token), never by the forwarding node's address. A client
//! spreading requests across N nodes can get up to N× the per-IP budget.
//!
//! **Runtime configuration and observability** (DESIGN.md "Rate limits:
//! observability and runtime config"). The values above are defaults: a
//! versioned config object in the bucket ([`config`], written with CAS by
//! `vlpds.admin.updateRateLimits`, picked up by every node through
//! [`runtime`]) can change any bucket's points and window, disable it, add
//! IP-keyed buckets for further XRPC methods, and exempt or re-limit
//! IPs/CIDRs and DIDs. The effective [`Policy`] is swapped atomically;
//! counters are keyed by (bucket, window, key), so a new limit keeps every
//! live window and a new window length starts fresh ones. Each counter shard
//! also keeps a few heavy hitters per bucket ([`Counters::top`]) and 429s are
//! tallied by bucket and route ([`Rejections`]), both bounded, for the
//! operator console.

pub mod config;
pub mod runtime;

use crate::xrpc::XrpcError;
use axum::extract::{ConnectInfo, MatchedPath, Request};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use parking_lot::{Mutex, RwLock};
use prometheus::{IntCounter, IntCounterVec, IntGauge};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

/// What a bucket is keyed by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KeyKind {
    /// The client IP.
    Ip,
    /// `{identifier}-{client ip}` (createSession and the OAuth sign-in).
    IdentifierIp,
    /// The authenticated account's DID.
    Did,
    /// One counter for the whole node ([`NODE_KEY`]).
    Node,
}

/// The key of [`KeyKind::Node`] buckets.
pub const NODE_KEY: &str = "node";

/// One built-in bucket: its default `points` per fixed `window_ms` window.
/// `idx` is its position in [`BUILTIN`].
#[derive(Debug)]
pub struct Limit {
    pub idx: usize,
    pub name: &'static str,
    pub key: KeyKind,
    /// Which requests consume it (for the console).
    pub scope: &'static str,
    pub window_ms: u64,
    pub points: u32,
}

macro_rules! limit {
    ($id:ident, $idx:expr, $name:expr, $key:ident, $scope:expr, $window:expr, $points:expr) => {
        pub const $id: Limit = Limit {
            idx: $idx,
            name: $name,
            key: KeyKind::$key,
            scope: $scope,
            window_ms: $window,
            points: $points,
        };
    };
}

limit!(GLOBAL_IP, 0, "global-ip", Ip, "every XRPC call except sync.getRepo; OAuth sign-in", 5 * MINUTE, 3000);
limit!(GET_REPO, 1, "com.atproto.sync.getRepo-0", Ip, "sync.getRepo", 5 * MINUTE, 6000);
limit!(CREATE_SESSION_DAY, 2, "com.atproto.server.createSession-0", IdentifierIp, "server.createSession; OAuth sign-in", DAY, 300);
limit!(CREATE_SESSION_5MIN, 3, "com.atproto.server.createSession-1", IdentifierIp, "server.createSession; OAuth sign-in", 5 * MINUTE, 30);
limit!(CREATE_ACCOUNT, 4, "com.atproto.server.createAccount-0", Ip, "server.createAccount; OAuth sign-up", 5 * MINUTE, 100);
limit!(DELETE_ACCOUNT, 5, "com.atproto.server.deleteAccount-0", Ip, "server.deleteAccount", 5 * MINUTE, 50);
limit!(REQUEST_PASSWORD_RESET_DAY, 6, "com.atproto.server.requestPasswordReset-0", Ip, "server.requestPasswordReset", DAY, 50);
limit!(REQUEST_PASSWORD_RESET_HOUR, 7, "com.atproto.server.requestPasswordReset-1", Ip, "server.requestPasswordReset", HOUR, 15);
limit!(RESET_PASSWORD, 8, "com.atproto.server.resetPassword-0", Ip, "server.resetPassword", 5 * MINUTE, 50);
limit!(UPLOAD_BLOB, 9, "com.atproto.repo.uploadBlob-0", Ip, "repo.uploadBlob", DAY, 1000);
limit!(UPDATE_HANDLE_5MIN, 10, "com.atproto.identity.updateHandle-0", Did, "identity.updateHandle", 5 * MINUTE, 10);
limit!(UPDATE_HANDLE_DAY, 11, "com.atproto.identity.updateHandle-1", Did, "identity.updateHandle", DAY, 50);
limit!(REQUEST_ACCOUNT_DELETE_DAY, 12, "com.atproto.server.requestAccountDelete-0", Did, "server.requestAccountDelete", DAY, 15);
limit!(REQUEST_ACCOUNT_DELETE_HOUR, 13, "com.atproto.server.requestAccountDelete-1", Did, "server.requestAccountDelete", HOUR, 5);
limit!(REQUEST_EMAIL_CONFIRMATION_DAY, 14, "com.atproto.server.requestEmailConfirmation-0", Did, "server.requestEmailConfirmation", DAY, 15);
limit!(REQUEST_EMAIL_CONFIRMATION_HOUR, 15, "com.atproto.server.requestEmailConfirmation-1", Did, "server.requestEmailConfirmation", HOUR, 5);
limit!(REQUEST_EMAIL_UPDATE_DAY, 16, "com.atproto.server.requestEmailUpdate-0", Did, "server.requestEmailUpdate", DAY, 15);
limit!(REQUEST_EMAIL_UPDATE_HOUR, 17, "com.atproto.server.requestEmailUpdate-1", Did, "server.requestEmailUpdate", HOUR, 5);
limit!(REPO_WRITE_HOUR, 18, "repo-write-hour", Did, "every repo write (create 3, update 2, delete 1 points)", HOUR, 5000);
limit!(REPO_WRITE_DAY, 19, "repo-write-day", Did, "every repo write (create 3, update 2, delete 1 points)", DAY, 35000);
limit!(OAUTH_SIGN_IN_IP, 20, "oauth-sign-in-ip", Ip, "OAuth sign-in form posts", 5 * MINUTE, 100);
limit!(SIGN_IN_ACCOUNT, 21, "sign-in-account", Did, "server.createSession; OAuth sign-in (both steps), from any IP", HOUR, 100);
limit!(OAUTH_IP, 22, "oauth-ip", Ip, "OAuth /oauth/par, /oauth/token, /oauth/revoke", 5 * MINUTE, 3000);
limit!(RESERVE_SIGNING_KEY_IP, 23, "com.atproto.server.reserveSigningKey-0", Ip, "server.reserveSigningKey", HOUR, 100);
limit!(RESERVE_SIGNING_KEY_NODE, 24, "reserve-signing-key-node", Node, "server.reserveSigningKey calls that reserve a new key (one KMS wrap each)", DAY, 5000);

/// Every built-in bucket, indexed by [`Limit::idx`].
pub const BUILTIN: [&Limit; 25] = [
    &GLOBAL_IP,
    &GET_REPO,
    &CREATE_SESSION_DAY,
    &CREATE_SESSION_5MIN,
    &CREATE_ACCOUNT,
    &DELETE_ACCOUNT,
    &REQUEST_PASSWORD_RESET_DAY,
    &REQUEST_PASSWORD_RESET_HOUR,
    &RESET_PASSWORD,
    &UPLOAD_BLOB,
    &UPDATE_HANDLE_5MIN,
    &UPDATE_HANDLE_DAY,
    &REQUEST_ACCOUNT_DELETE_DAY,
    &REQUEST_ACCOUNT_DELETE_HOUR,
    &REQUEST_EMAIL_CONFIRMATION_DAY,
    &REQUEST_EMAIL_CONFIRMATION_HOUR,
    &REQUEST_EMAIL_UPDATE_DAY,
    &REQUEST_EMAIL_UPDATE_HOUR,
    &REPO_WRITE_HOUR,
    &REPO_WRITE_DAY,
    &OAUTH_SIGN_IN_IP,
    &SIGN_IN_ACCOUNT,
    &OAUTH_IP,
    &RESERVE_SIGNING_KEY_IP,
    &RESERVE_SIGNING_KEY_NODE,
];

/// OAuth endpoints the layer limits per IP ([`OAUTH_IP`]; not global-ip,
/// which is XRPC's). A confidential client's backend calls these for all of
/// its users from one address: raise or exempt it with an IP override.
const OAUTH_IP_PATHS: [&str; 3] = ["/oauth/par", "/oauth/token", "/oauth/revoke"];

/// Browser form posts that run the rate-limit context (checked by the
/// handler, which renders its own page on a 429).
const OAUTH_SIGN_IN_PATHS: [&str; 2] = ["/oauth/authorize/sign-in", "/oauth/account/sign-in"];

/// Repo write points (reference: create=3, update=2, delete=1).
pub const CREATE_POINTS: u32 = 3;
pub const UPDATE_POINTS: u32 = 2;
pub const DELETE_POINTS: u32 = 1;

/// IP-keyed route buckets, checked before the handler.
fn ip_route_limits(path: &str) -> &'static [&'static Limit] {
    match path {
        "/xrpc/com.atproto.sync.getRepo" => &[&GET_REPO],
        "/xrpc/com.atproto.server.createAccount" => &[&CREATE_ACCOUNT],
        "/xrpc/com.atproto.server.deleteAccount" => &[&DELETE_ACCOUNT],
        "/xrpc/com.atproto.server.requestPasswordReset" => {
            &[&REQUEST_PASSWORD_RESET_DAY, &REQUEST_PASSWORD_RESET_HOUR]
        }
        "/xrpc/com.atproto.server.resetPassword" => &[&RESET_PASSWORD],
        "/xrpc/com.atproto.repo.uploadBlob" => &[&UPLOAD_BLOB],
        "/xrpc/com.atproto.server.reserveSigningKey" => &[&RESERVE_SIGNING_KEY_IP],
        _ => &[],
    }
}

/// XRPC paths the layer never limits (route buckets on them would be inert).
pub fn unlimited_path(path: &str) -> bool {
    path == "/xrpc/_health" || path == "/xrpc/com.atproto.sync.subscribeRepos"
}

// ---------------------------------------------------------------------------
// effective policy (defaults + the runtime config object)
// ---------------------------------------------------------------------------

/// A bucket as currently in force.
#[derive(Clone, Debug)]
pub struct Spec {
    pub name: Arc<str>,
    pub key: KeyKind,
    pub scope: Arc<str>,
    pub window_ms: u64,
    pub points: u32,
    pub enabled: bool,
    /// Groups heavy hitters by bucket name (stable within the process).
    tag: u64,
}

impl Spec {
    pub fn new(name: &str, key: KeyKind, scope: &str, window_ms: u64, points: u32, enabled: bool) -> Spec {
        Spec {
            name: name.into(),
            key,
            scope: scope.into(),
            window_ms,
            points,
            enabled,
            tag: name_tag(name),
        }
    }

    fn of(l: &Limit) -> Spec {
        Spec::new(l.name, l.key, l.scope, l.window_ms, l.points, true)
    }
}

fn name_tag(name: &str) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    name.hash(&mut h);
    h.finish()
}

/// What an override does to the buckets it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Not counted at all.
    Exempt,
    /// Held to this many points instead (same window, same counter).
    Points(u32),
}

#[derive(Clone, Debug)]
pub struct Ov {
    /// Bucket names it covers (empty: every bucket).
    pub limiters: Vec<String>,
    pub action: Action,
}

impl Ov {
    fn covers(&self, name: &str) -> bool {
        self.limiters.is_empty() || self.limiters.iter().any(|l| l == name)
    }
}

/// The limits in force: built-in buckets (values possibly changed), extra
/// IP-keyed route buckets, and IP/CIDR and DID overrides. Immutable; a
/// config change installs a new one ([`Limiter::install`]).
#[derive(Debug)]
pub struct Policy {
    /// Version of the config object it came from (0: no object, defaults).
    pub version: u64,
    /// false: nothing is limited (the config's global switch).
    pub enabled: bool,
    pub(crate) builtin: Vec<Spec>,
    /// `/xrpc/{nsid}` -> extra IP-keyed bucket (`route:{nsid}`).
    pub(crate) routes: BTreeMap<String, Spec>,
    pub(crate) ip_ov: Vec<(Cidr, Ov)>,
    pub(crate) did_ov: HashMap<String, Vec<Ov>>,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            version: 0,
            enabled: true,
            builtin: BUILTIN.iter().map(|l| Spec::of(l)).collect(),
            routes: BTreeMap::new(),
            ip_ov: Vec::new(),
            did_ov: HashMap::new(),
        }
    }
}

impl Policy {
    pub fn builtin(&self, l: &Limit) -> &Spec {
        &self.builtin[l.idx]
    }

    /// Every bucket: built-ins in table order, then route buckets by path.
    pub fn specs(&self) -> impl Iterator<Item = &Spec> {
        self.builtin.iter().chain(self.routes.values())
    }

    pub fn spec(&self, name: &str) -> Option<&Spec> {
        self.specs().find(|s| &*s.name == name)
    }

    /// Indices of the IP overrides matching `ip` (computed once per request).
    fn ip_matches(&self, ip: Option<IpAddr>) -> Vec<usize> {
        match ip {
            Some(ip) if !self.ip_ov.is_empty() => self
                .ip_ov
                .iter()
                .enumerate()
                .filter(|(_, (c, _))| c.contains(&ip))
                .map(|(i, _)| i)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// The override for consuming bucket `name` with `key` in a request
    /// whose client IP matched the IP overrides `ip_ov`: DID overrides match
    /// a DID key, IP overrides the request's client IP. Exempt wins;
    /// otherwise the largest custom limit.
    pub fn override_for(&self, name: &str, key: &str, ip_ov: &[usize]) -> Option<Action> {
        if self.did_ov.is_empty() && ip_ov.is_empty() {
            return None;
        }
        let dids = self.did_ov.get(key).into_iter().flatten();
        let ips = ip_ov.iter().filter_map(|i| self.ip_ov.get(*i).map(|(_, o)| o));
        let mut out = None;
        for o in dids.chain(ips).filter(|o| o.covers(name)) {
            match (o.action, out) {
                (Action::Exempt, _) => return Some(Action::Exempt),
                (Action::Points(p), Some(Action::Points(q))) if q >= p => {}
                (a, _) => out = Some(a),
            }
        }
        out
    }

    /// The limit `key` is held to in `spec` (None: exempt), for display:
    /// the client IP is recovered from the key (an IP, or `{id}-{ip}`).
    pub fn limit_for_key(&self, spec: &Spec, key: &str) -> Option<u32> {
        // an IPv6 key is its /64 ([`ip_key`]): its network address stands in
        let parse = |k: &str| k.trim_end_matches("/64").parse::<IpAddr>().ok();
        let ip = match spec.key {
            KeyKind::Ip => parse(key),
            KeyKind::IdentifierIp => key.rsplit_once('-').and_then(|(_, ip)| parse(ip)),
            KeyKind::Did | KeyKind::Node => None,
        };
        match self.override_for(&spec.name, key, &self.ip_matches(ip)) {
            Some(Action::Exempt) => None,
            Some(Action::Points(p)) => Some(p),
            None => Some(spec.points),
        }
    }
}

// ---------------------------------------------------------------------------
// counters
// ---------------------------------------------------------------------------

/// Outcome of consuming from one bucket.
#[derive(Clone, Copy, Debug)]
pub struct Status {
    pub limit: u32,
    pub window_ms: u64,
    pub remaining: u32,
    /// Unix ms at which the window resets.
    pub reset_ms: u64,
    pub exceeded: bool,
}

#[derive(Clone, Copy)]
struct Window {
    reset_ms: u64,
    used: u32,
}

/// A heavy-hitter candidate: a key with high use in its current window.
#[derive(Clone, Debug)]
struct Cand {
    id: u64,
    window_ms: u64,
    key: Box<str>,
    used: u32,
    reset_ms: u64,
}

struct Shard {
    map: HashMap<u64, Window>,
    /// Bucket tag -> at most [`TOP_PER_SHARD`] candidates.
    top: HashMap<u64, Vec<Cand>>,
    next_sweep_ms: u64,
}

const SHARDS: usize = 64;
/// A shard sweeps expired windows at most this often (and only when touched).
const SWEEP_EVERY_MS: u64 = 10_000;
/// Heavy hitters kept per bucket per shard: 64 x 8 = 512 candidates per
/// bucket, so a top-N list is exact unless more than 8 of its keys hash to
/// one shard.
const TOP_PER_SHARD: usize = 8;
/// Longest key kept for a candidate (identifiers are client-chosen).
const TOP_KEY_MAX: usize = 96;

/// Sharded fixed-window counters.
pub struct Counters {
    shards: Box<[Mutex<Shard>]>,
    hasher: std::collections::hash_map::RandomState,
}

impl Default for Counters {
    fn default() -> Self {
        Counters {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        map: HashMap::new(),
                        top: HashMap::new(),
                        next_sweep_ms: 0,
                    })
                })
                .collect(),
            hasher: Default::default(),
        }
    }
}

/// One key's use of a bucket, for the console.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Consumer {
    pub key: String,
    pub used: u32,
    /// The limit this key is held to (None: exempt by an override).
    pub limit: Option<u32>,
    pub reset_ms: u64,
}

impl Counters {
    /// Consumes `points` from `limit`'s default window for `key` at `now_ms`.
    pub fn consume(&self, limit: &Limit, key: &str, points: u32, now_ms: u64) -> Status {
        self.consume_spec(&Spec::of(limit), limit.points, key, points, now_ms)
    }

    /// Consumes `points` from `spec`'s window for `key`, held to `limit`.
    /// Windows are keyed by (bucket, window length, key): a new limit keeps
    /// a key's live window, a new window length starts a fresh one.
    pub fn consume_spec(&self, spec: &Spec, limit: u32, key: &str, points: u32, now_ms: u64) -> Status {
        let mut h = self.hasher.build_hasher();
        spec.name.hash(&mut h);
        spec.window_ms.hash(&mut h);
        key.hash(&mut h);
        let id = h.finish();
        let mut guard = self.shards[(id >> 58) as usize % SHARDS].lock();
        let shard = &mut *guard;
        if now_ms >= shard.next_sweep_ms {
            shard.map.retain(|_, w| w.reset_ms > now_ms);
            shard.top.retain(|_, l| {
                l.retain(|c| c.reset_ms > now_ms);
                !l.is_empty()
            });
            shard.next_sweep_ms = now_ms + SWEEP_EVERY_MS;
        }
        let w = shard.map.entry(id).or_insert(Window {
            reset_ms: now_ms + spec.window_ms,
            used: 0,
        });
        if w.reset_ms <= now_ms {
            *w = Window {
                reset_ms: now_ms + spec.window_ms,
                used: 0,
            };
        }
        // As rate-limiter-flexible: points are counted even when rejected.
        w.used = w.used.saturating_add(points);
        let (used, reset_ms) = (w.used, w.reset_ms);
        track(shard.top.entry(spec.tag).or_default(), id, spec.window_ms, key, used, reset_ms, now_ms);
        Status {
            limit,
            window_ms: spec.window_ms,
            remaining: limit.saturating_sub(used),
            reset_ms,
            exceeded: used > limit,
        }
    }

    /// Live windows (tests / metrics).
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().map.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `n` keys with the most points used in their current window, for
    /// each bucket of `policy` (approximate: see [`TOP_PER_SHARD`]).
    pub fn top(&self, policy: &Policy, n: usize, now_ms: u64) -> BTreeMap<String, Vec<Consumer>> {
        let mut by_tag: HashMap<u64, Vec<Cand>> = HashMap::new();
        for s in self.shards.iter() {
            let s = s.lock();
            for (tag, l) in &s.top {
                by_tag.entry(*tag).or_default().extend(l.iter().filter(|c| c.reset_ms > now_ms).cloned());
            }
        }
        let mut out = BTreeMap::new();
        for spec in policy.specs() {
            let Some(mut cands) = by_tag.remove(&spec.tag) else { continue };
            cands.retain(|c| c.window_ms == spec.window_ms);
            if cands.is_empty() {
                continue;
            }
            cands.sort_by(|a, b| b.used.cmp(&a.used).then_with(|| a.key.cmp(&b.key)));
            cands.truncate(n);
            let list = cands
                .into_iter()
                .map(|c| Consumer {
                    limit: policy.limit_for_key(spec, &c.key),
                    key: c.key.into(),
                    used: c.used,
                    reset_ms: c.reset_ms,
                })
                .collect();
            out.insert(spec.name.to_string(), list);
        }
        out
    }
}

/// Keeps `list` holding the heaviest current-window keys of one bucket in
/// one shard: updates the key's entry, or replaces the lightest (expired
/// entries weigh nothing) once the list is full. Allocates only when a key
/// enters the list.
fn track(list: &mut Vec<Cand>, id: u64, window_ms: u64, key: &str, used: u32, reset_ms: u64, now_ms: u64) {
    if let Some(c) = list.iter_mut().find(|c| c.id == id) {
        c.used = used;
        c.reset_ms = reset_ms;
        return;
    }
    let cand = || Cand {
        id,
        window_ms,
        key: truncate(key, TOP_KEY_MAX).into(),
        used,
        reset_ms,
    };
    if list.len() < TOP_PER_SHARD {
        list.push(cand());
        return;
    }
    let weight = |c: &Cand| if c.reset_ms <= now_ms { 0 } else { c.used };
    if let Some((i, min)) = list.iter().enumerate().map(|(i, c)| (i, weight(c))).min_by_key(|x| x.1) {
        if used > min {
            list[i] = cand();
        }
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// 429 tallies (bounded: bucket x route, both bounded sets)
// ---------------------------------------------------------------------------

static REJECTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "vlpds_rate_limit_rejections_total",
        "Requests over a rate-limit bucket, by bucket and route (matched XRPC method or path, else _proxy_or_unmatched)",
        &["limiter", "route"]
    )
    .unwrap()
});
pub(crate) static CONFIG_VERSION: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "vlpds_rate_limit_config_version",
        "Version of the rate-limit config object in force on this node (0: flag defaults)"
    )
    .unwrap()
});
pub(crate) static CONFIG_ERRORS: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "vlpds_rate_limit_config_errors_total",
        "Rate-limit config objects rejected by validation (the last good config stays in force)"
    )
    .unwrap()
});
pub(crate) static CONFIG_LOADS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "vlpds_rate_limit_config_loads_total",
        "Rate-limit config refreshes by result (applied, unchanged, invalid, error)",
        &["result"]
    )
    .unwrap()
});

/// Minutes of 429 history kept per series.
const REJECT_MINUTES: usize = 15;
/// Series cap (bucket x route); past it, new series share one row.
const MAX_REJECT_SERIES: usize = 1024;

#[derive(Clone, Copy, Default)]
struct Ring {
    /// (unix minute, count), slot = minute % REJECT_MINUTES.
    mins: [(u64, u64); REJECT_MINUTES],
    total: u64,
}

/// 429 counts by bucket and route over the last minutes.
#[derive(Default)]
pub struct Rejections {
    map: Mutex<HashMap<(Arc<str>, String), Ring>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RejectionCount {
    pub limiter: String,
    pub route: String,
    /// The current minute plus the previous 0 / 4 / 14.
    pub last1m: u64,
    pub last5m: u64,
    pub last15m: u64,
    /// Since this node started.
    pub total: u64,
}

impl Rejections {
    pub fn record(&self, limiter: &Arc<str>, route: &str, now_ms: u64) {
        REJECTIONS.with_label_values(&[&**limiter, route]).inc();
        let minute = now_ms / MINUTE;
        let mut m = self.map.lock();
        let k = (limiter.clone(), route.to_string());
        let k = if m.len() >= MAX_REJECT_SERIES && !m.contains_key(&k) {
            (Arc::from("_other"), "_other".to_string())
        } else {
            k
        };
        let r = m.entry(k).or_default();
        let slot = &mut r.mins[(minute % REJECT_MINUTES as u64) as usize];
        if slot.0 != minute {
            *slot = (minute, 0);
        }
        slot.1 += 1;
        r.total += 1;
    }

    pub fn snapshot(&self, now_ms: u64) -> Vec<RejectionCount> {
        let minute = now_ms / MINUTE;
        let within = |r: &Ring, n: u64| -> u64 { r.mins.iter().filter(|(m, _)| *m + n > minute && *m <= minute).map(|(_, c)| c).sum() };
        let mut v: Vec<RejectionCount> = self
            .map
            .lock()
            .iter()
            .map(|((l, route), r)| RejectionCount {
                limiter: l.to_string(),
                route: route.clone(),
                last1m: within(r, 1),
                last5m: within(r, 5),
                last15m: within(r, 15),
                total: r.total,
            })
            .collect();
        v.sort_by(|a, b| {
            b.last5m
                .cmp(&a.last5m)
                .then(b.total.cmp(&a.total))
                .then_with(|| (&a.limiter, &a.route).cmp(&(&b.limiter, &b.route)))
        });
        v
    }
}

// ---------------------------------------------------------------------------
// client IP
// ---------------------------------------------------------------------------

/// An IP or CIDR block (`10.0.0.0/8`, `::1`, `fd00::/8`).
#[derive(Clone, Debug)]
pub struct Cidr {
    net: IpAddr,
    bits: u8,
}

impl Cidr {
    pub fn parse(s: &str) -> Option<Cidr> {
        let (ip, bits) = match s.trim().split_once('/') {
            Some((ip, b)) => (ip.parse::<IpAddr>().ok()?, Some(b.parse::<u8>().ok()?)),
            None => (s.trim().parse::<IpAddr>().ok()?, None),
        };
        let max = if ip.is_ipv4() { 32 } else { 128 };
        let bits = bits.unwrap_or(max);
        (bits <= max).then_some(Cidr { net: ip, bits })
    }

    pub fn contains(&self, ip: &IpAddr) -> bool {
        fn prefix_eq(a: &[u8], b: &[u8], bits: u8) -> bool {
            let (full, rem) = ((bits / 8) as usize, bits % 8);
            a[..full] == b[..full]
                && (rem == 0 || (a[full] ^ b[full]) & (0xffu8 << (8 - rem)) == 0)
        }
        match (self.net, ip.to_canonical()) {
            (IpAddr::V4(n), IpAddr::V4(i)) => prefix_eq(&n.octets(), &i.octets(), self.bits),
            (IpAddr::V6(n), IpAddr::V6(i)) => prefix_eq(&n.octets(), &i.octets(), self.bits),
            _ => false,
        }
    }
}

/// The client address: the TCP peer, or, when the peer is a trusted proxy,
/// the right-most `X-Forwarded-For` entry that isn't itself trusted
/// (Express `trust proxy` semantics). Entries may carry a port
/// (`1.2.3.4:5678`, `[2001:db8::1]:443`); walking stops at an entry that
/// doesn't parse, keeping the last trusted hop, so a garbled entry never
/// lets the walk reach further-left (client-written) ones.
pub fn client_ip(headers: &HeaderMap, peer: Option<IpAddr>, trusted: &[Cidr]) -> Option<IpAddr> {
    let peer = peer?.to_canonical();
    let is_trusted = |ip: &IpAddr| trusted.iter().any(|c| c.contains(ip));
    if trusted.is_empty() || !is_trusted(&peer) {
        return Some(peer);
    }
    let mut ip = peer;
    let hops: Vec<&str> = headers
        .get_all("x-forwarded-for")
        .iter()
        .map(|v| v.to_str().unwrap_or(""))
        .flat_map(|v| v.split(','))
        .collect();
    for hop in hops.into_iter().rev() {
        let Some(h) = parse_hop(hop) else { break };
        ip = h.to_canonical();
        if !is_trusted(&ip) {
            break;
        }
    }
    Some(ip)
}

/// One `X-Forwarded-For` entry: an IP, `v4:port`, `[v6]` or `[v6]:port`.
fn parse_hop(s: &str) -> Option<IpAddr> {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        let (v6, tail) = rest.split_once(']')?;
        if !(tail.is_empty() || tail.strip_prefix(':').is_some_and(|p| p.parse::<u16>().is_ok())) {
            return None;
        }
        return v6.parse::<std::net::Ipv6Addr>().ok().map(IpAddr::V6);
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    let (v4, port) = s.split_once(':')?;
    port.parse::<u16>().ok()?;
    v4.parse::<std::net::Ipv4Addr>().ok().map(IpAddr::V4)
}

/// The rate-limit key of a client address: an IPv4 address as is, an IPv6
/// one as its /64 (`2001:db8:1:2::/64`; one subscriber's allocation, which
/// it can fill with fresh addresses at will).
pub fn ip_key(ip: IpAddr) -> String {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            let net = std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0);
            format!("{net}/64")
        }
    }
}

/// Request extension: the client address as the node the client called
/// resolved it. Set by `crate::forward::route` on a request it forwards
/// (sent along as [`CLIENT_IP_HEADER`]) and on a request a peer forwarded
/// (from that header, trusted only with the peer's valid internal token);
/// it then wins over the TCP peer, which is the forwarding node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// Node-to-node header carrying [`ClientIp`] on a forwarded request.
/// Only honored next to a valid `x-vlpds-forwarded` token; a client's copy
/// is dropped.
pub const CLIENT_IP_HEADER: &str = "x-vlpds-client-ip";

/// The client address of a request: [`ClientIp`] if set, else [`client_ip`]
/// of its TCP peer and headers.
pub fn request_client_ip(headers: &HeaderMap, ext: &axum::http::Extensions, trusted: &[Cidr]) -> Option<IpAddr> {
    if let Some(ClientIp(ip)) = ext.get::<ClientIp>() {
        return Some(*ip);
    }
    let peer = ext.get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip());
    client_ip(headers, peer, trusted)
}

// ---------------------------------------------------------------------------
// the limiter (one per node) and the request-scoped context
// ---------------------------------------------------------------------------

pub struct Limiter {
    pub counters: Counters,
    pub trusted: Vec<Cidr>,
    pub bypass_key: Option<String>,
    /// `--no-rate-limits` not given, so the layer is installed. When false
    /// the config can still be edited, but this node counts nothing.
    pub enabled_by_flag: bool,
    /// Admin / internal tokens for the bypass check.
    cfg: crate::server::Config,
    policy: RwLock<Arc<Policy>>,
    pub rejections: Rejections,
    pub runtime: runtime::Runtime,
}

/// What the console shows for one node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSnapshot {
    pub node: String,
    pub enabled_by_flag: bool,
    pub config_version: u64,
    pub config_error: Option<runtime::ConfigError>,
    pub loaded_at_ms: Option<u64>,
    pub checked_at_ms: Option<u64>,
    /// Bucket name -> heaviest keys in their current window.
    pub top: BTreeMap<String, Vec<Consumer>>,
    pub rejections: Vec<RejectionCount>,
    pub live_windows: usize,
}

impl Limiter {
    pub fn new(cfg: &crate::server::Config) -> Limiter {
        // registered up front so /metrics lists them before the first event
        LazyLock::force(&REJECTIONS);
        LazyLock::force(&CONFIG_VERSION);
        LazyLock::force(&CONFIG_ERRORS);
        LazyLock::force(&CONFIG_LOADS);
        Limiter {
            counters: Counters::default(),
            trusted: cfg
                .trusted_proxies
                .iter()
                .filter_map(|s| {
                    let c = Cidr::parse(s);
                    if c.is_none() {
                        tracing::warn!(proxy = %s, "ignoring unparseable trusted proxy");
                    }
                    c
                })
                .collect(),
            bypass_key: cfg.rate_limit_bypass_key.clone().filter(|k| !k.is_empty()),
            enabled_by_flag: cfg.rate_limits_enabled,
            cfg: cfg.clone(),
            policy: RwLock::new(Arc::new(Policy::default())),
            rejections: Rejections::default(),
            runtime: runtime::Runtime::default(),
        }
    }

    /// The policy in force.
    pub fn policy(&self) -> Arc<Policy> {
        self.policy.read().clone()
    }

    /// Swaps in `p` atomically: requests already running finish on the old
    /// one; counters are untouched.
    pub fn install(&self, p: Policy) {
        CONFIG_VERSION.set(p.version as i64);
        *self.policy.write() = Arc::new(p);
    }

    pub fn snapshot(&self, node: &str, top: usize) -> NodeSnapshot {
        let now = now_ms();
        let policy = self.policy();
        let st = self.runtime.status();
        NodeSnapshot {
            node: node.to_string(),
            enabled_by_flag: self.enabled_by_flag,
            config_version: policy.version,
            config_error: st.error,
            loaded_at_ms: st.loaded_at_ms,
            checked_at_ms: st.checked_at_ms,
            top: self.counters.top(&policy, top, now),
            rejections: self.rejections.snapshot(now),
            live_windows: self.counters.len(),
        }
    }

    fn bypassed(&self, headers: &HeaderMap) -> bool {
        let hdr = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
        if let Some(t) = hdr("x-vlpds-internal") {
            if crate::xrpc::internal::internal_token_ok(&self.cfg, t) {
                return true;
            }
        }
        if let (Some(k), Some(v)) = (&self.bypass_key, hdr("x-ratelimit-bypass")) {
            if crate::auth::token_eq(k, v) {
                return true;
            }
        }
        if let Some(b) = hdr("authorization").and_then(|v| v.strip_prefix("Basic ")) {
            if crate::auth::basic_admin_ok(b, &self.cfg.admin_token) {
                return true;
            }
        }
        false
    }
}

struct Ctx {
    limiter: Arc<Limiter>,
    /// The policy this request runs under (one snapshot per request).
    policy: Arc<Policy>,
    ip: String,
    /// IP overrides matching the client IP.
    ip_ov: Vec<usize>,
    /// For 429 tallies.
    route: Option<MatchedPath>,
    bypass: bool,
    /// Tightest status consumed so far (least remaining; exceeded wins).
    tightest: Option<Status>,
}

impl Ctx {
    fn record(&mut self, s: Status) {
        let replace = match &self.tightest {
            None => true,
            Some(t) => (s.exceeded && !t.exceeded) || (s.exceeded == t.exceeded && s.remaining < t.remaining),
        };
        if replace {
            self.tightest = Some(s);
        }
    }

    /// Route label: the matched XRPC method (or matched path), so bounded
    /// by the router's routes.
    fn route_label(&self) -> &str {
        match &self.route {
            Some(m) => m.as_str().strip_prefix("/xrpc/").unwrap_or(m.as_str()),
            None => "_proxy_or_unmatched",
        }
    }

    fn consume(&mut self, limits: &[&'static Limit], key: &str, points: u32) -> Result<(), XrpcError> {
        let policy = self.policy.clone();
        self.consume_specs(limits.iter().map(|l| policy.builtin(l)), key, points)
    }

    fn consume_specs<'a>(&mut self, specs: impl Iterator<Item = &'a Spec>, key: &str, points: u32) -> Result<(), XrpcError> {
        if self.bypass || points == 0 || !self.policy.enabled {
            return Ok(());
        }
        let now = now_ms();
        let mut exceeded = false;
        for spec in specs {
            if !spec.enabled {
                continue;
            }
            let limit = match self.policy.override_for(&spec.name, key, &self.ip_ov) {
                Some(Action::Exempt) => continue,
                Some(Action::Points(p)) => p,
                None => spec.points,
            };
            let s = self.limiter.counters.consume_spec(spec, limit, key, points, now);
            if s.exceeded {
                exceeded = true;
                self.limiter.rejections.record(&spec.name, self.route_label(), now);
            }
            self.record(s);
        }
        if exceeded {
            crate::metrics::RATE_LIMITED.inc();
            return Err(exceeded_error());
        }
        Ok(())
    }
}

tokio::task_local! {
    static CTX: RefCell<Ctx>;
}

pub fn exceeded_error() -> XrpcError {
    XrpcError {
        status: StatusCode::TOO_MANY_REQUESTS,
        error: "RateLimitExceeded".into(),
        message: "Rate Limit Exceeded".into(),
    }
}

/// Consumes `points` from each of `limits` for `key` (a DID, or a
/// handler-computed key). No-op when rate limiting is off or bypassed.
pub fn check(limits: &[&'static Limit], key: &str, points: u32) -> Result<(), XrpcError> {
    CTX.try_with(|c| c.borrow_mut().consume(limits, key, points))
        .unwrap_or(Ok(()))
}

/// Like [`check`], keyed by `{prefix}-{client ip}` (createSession's
/// `${identifier}-${ip}`).
pub fn check_with_ip(limits: &[&'static Limit], prefix: &str, points: u32) -> Result<(), XrpcError> {
    CTX.try_with(|c| {
        let mut c = c.borrow_mut();
        let key = format!("{prefix}-{}", c.ip);
        c.consume(limits, &key, points)
    })
    .unwrap_or(Ok(()))
}

/// Like [`check`], keyed by the client IP alone.
pub fn check_ip(limits: &[&'static Limit], points: u32) -> Result<(), XrpcError> {
    CTX.try_with(|c| {
        let mut c = c.borrow_mut();
        let key = c.ip.clone();
        c.consume(limits, &key, points)
    })
    .unwrap_or(Ok(()))
}

/// The repo-write buckets for `did` (hour and day).
pub fn check_repo_write(did: Option<&str>, points: u32) -> Result<(), XrpcError> {
    match did {
        Some(d) => check(&[&REPO_WRITE_HOUR, &REPO_WRITE_DAY], d, points),
        None => Ok(()),
    }
}

fn set_headers(h: &mut HeaderMap, s: &Status) {
    let num = |n: u64| HeaderValue::from(n);
    h.insert(HeaderName::from_static("ratelimit-limit"), num(s.limit as u64));
    h.insert(HeaderName::from_static("ratelimit-remaining"), num(s.remaining as u64));
    h.insert(HeaderName::from_static("ratelimit-reset"), num(s.reset_ms / 1000));
    if let Ok(v) = HeaderValue::from_str(&format!("{};w={}", s.limit, s.window_ms / 1000)) {
        h.insert(HeaderName::from_static("ratelimit-policy"), v);
    }
    if s.exceeded {
        let secs = s.reset_ms.saturating_sub(now_ms()).div_ceil(1000);
        h.insert(header::RETRY_AFTER, num(secs));
    }
}

/// Middleware: per-IP buckets (built-in and configured route buckets), the
/// request context for handler-level buckets, and the RateLimit-* headers.
pub async fn layer(
    axum::extract::State(limiter): axum::extract::State<Arc<Limiter>>,
    req: Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    // OAuth sign-in forms: context only; the handler consumes its buckets
    let post = req.method() == axum::http::Method::POST;
    let sign_in_form = post && OAUTH_SIGN_IN_PATHS.contains(&path);
    let oauth_endpoint = post && OAUTH_IP_PATHS.contains(&path);
    if !sign_in_form && !oauth_endpoint && (!path.starts_with("/xrpc/") || unlimited_path(path)) {
        return next.run(req).await;
    }
    let bypass = limiter.bypassed(req.headers());
    let ip_addr = request_client_ip(req.headers(), req.extensions(), &limiter.trusted);
    let ip = ip_addr.map(ip_key).unwrap_or_else(|| "unknown".into());
    let route: &[&Limit] = if oauth_endpoint { &[&OAUTH_IP] } else { ip_route_limits(path) };
    let global = !sign_in_form && !oauth_endpoint && path != "/xrpc/com.atproto.sync.getRepo";
    let policy = limiter.policy();
    let custom = policy.routes.get(path).cloned();
    let ctx = Ctx {
        ip_ov: policy.ip_matches(ip_addr),
        route: req.extensions().get::<MatchedPath>().cloned(),
        policy,
        limiter,
        ip,
        bypass,
        tightest: None,
    };
    CTX.scope(RefCell::new(ctx), async move {
        let pre = CTX.with(|c| {
            let mut c = c.borrow_mut();
            let ip = c.ip.clone();
            let g = if global {
                c.consume(&[&GLOBAL_IP], &ip, 1)
            } else {
                Ok(())
            };
            let r = c.consume(route, &ip, 1);
            let x = match &custom {
                Some(spec) => c.consume_specs(std::iter::once(spec), &ip, 1),
                None => Ok(()),
            };
            g.and(r).and(x)
        });
        let mut resp = match pre {
            Ok(()) => next.run(req).await,
            // OAuth endpoints answer in OAuth's error shape
            Err(_) if oauth_endpoint => (
                StatusCode::TOO_MANY_REQUESTS,
                axum::Json(serde_json::json!({"error": "rate_limit_exceeded", "error_description": "Rate Limit Exceeded"})),
            )
                .into_response(),
            Err(e) => e.into_response(),
        };
        if let Some(s) = CTX.with(|c| c.borrow().tightest) {
            set_headers(resp.headers_mut(), &s);
        }
        resp
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_window() {
        let c = Counters::default();
        const L: Limit = Limit {
            idx: 0,
            name: "t",
            key: KeyKind::Ip,
            scope: "",
            window_ms: 1000,
            points: 5,
        };
        for i in 0..5 {
            let s = c.consume(&L, "k", 1, 10);
            assert!(!s.exceeded);
            assert_eq!(s.remaining, 4 - i);
        }
        assert!(c.consume(&L, "k", 1, 500).exceeded);
        // other keys are independent
        assert!(!c.consume(&L, "k2", 5, 500).exceeded);
        // window resets
        let s = c.consume(&L, "k", 1, 1010);
        assert!(!s.exceeded);
        assert_eq!(s.reset_ms, 2010);
        // expired windows are swept from each shard as it is touched
        assert_eq!(c.len(), 2);
        for i in 0..1000 {
            c.consume(&L, &format!("x{i}"), 1, 100_000);
        }
        assert_eq!(c.len(), 1000, "old windows of every touched shard dropped");
    }

    #[test]
    fn builtin_table_is_indexed() {
        for (i, l) in BUILTIN.iter().enumerate() {
            assert_eq!(l.idx, i, "{}", l.name);
        }
        let mut names: Vec<_> = BUILTIN.iter().map(|l| l.name).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), BUILTIN.len());
    }

    #[test]
    fn new_limit_keeps_the_window_new_window_starts_fresh() {
        let c = Counters::default();
        let mut spec = Policy::default().builtin(&GLOBAL_IP).clone();
        for _ in 0..10 {
            c.consume_spec(&spec, 10, "1.2.3.4", 1, 0);
        }
        assert!(c.consume_spec(&spec, 10, "1.2.3.4", 1, 1).exceeded);
        // raised limit, same window: the 11 points used so far still count
        let s = c.consume_spec(&spec, 20, "1.2.3.4", 1, 2);
        assert_eq!((s.exceeded, s.remaining), (false, 8));
        // a different window length: a fresh window
        spec.window_ms = 60_000;
        let s = c.consume_spec(&spec, 20, "1.2.3.4", 1, 3);
        assert_eq!(s.remaining, 19);
    }

    #[test]
    fn heavy_hitters_are_bounded_and_ordered() {
        let c = Counters::default();
        let p = Policy::default();
        let spec = p.builtin(&GLOBAL_IP).clone();
        // 5000 distinct keys using 1..=50 points each, plus three heavy keys
        for i in 0..5000u32 {
            c.consume_spec(&spec, spec.points, &format!("10.0.{}.{}", i / 256, i % 256), i % 50 + 1, 1000);
        }
        for (k, n) in [("1.1.1.1", 900), ("2.2.2.2", 800), ("3.3.3.3", 700)] {
            c.consume_spec(&spec, spec.points, k, n, 1000);
        }
        let top = c.top(&p, 5, 1001);
        let l = &top["global-ip"];
        assert_eq!(l.len(), 5);
        assert_eq!((l[0].key.as_str(), l[0].used), ("1.1.1.1", 900));
        assert_eq!((l[1].key.as_str(), l[1].used), ("2.2.2.2", 800));
        assert_eq!((l[2].key.as_str(), l[2].used), ("3.3.3.3", 700));
        assert_eq!(l[3].used, 50);
        assert_eq!(l[0].limit, Some(3000));
        let cands: usize = c.shards.iter().map(|s| s.lock().top.values().map(|v| v.len()).sum::<usize>()).sum();
        assert!(cands <= SHARDS * TOP_PER_SHARD, "{cands} candidates");
        // expired windows drop out of the report
        assert!(c.top(&p, 5, 1000 + spec.window_ms + 1).is_empty());
    }

    #[test]
    fn long_keys_are_truncated() {
        let c = Counters::default();
        let p = Policy::default();
        let spec = p.builtin(&CREATE_SESSION_5MIN).clone();
        let key = format!("{}-1.2.3.4", "é".repeat(200));
        c.consume_spec(&spec, 30, &key, 1, 0);
        let top = c.top(&p, 1, 1);
        assert!(top[spec.name.as_ref()][0].key.len() <= TOP_KEY_MAX);
    }

    #[test]
    fn rejections_window() {
        let r = Rejections::default();
        let l: Arc<str> = "global-ip".into();
        let t0 = 100 * MINUTE;
        r.record(&l, "com.atproto.repo.createRecord", t0);
        r.record(&l, "com.atproto.repo.createRecord", t0 + 2 * MINUTE);
        r.record(&l, "com.atproto.repo.createRecord", t0 + 10 * MINUTE);
        let s = r.snapshot(t0 + 10 * MINUTE);
        assert_eq!(s.len(), 1);
        assert_eq!((s[0].last1m, s[0].last5m, s[0].last15m, s[0].total), (1, 1, 3, 3));
        // 20 minutes on, only the total remains
        let s = r.snapshot(t0 + 30 * MINUTE);
        assert_eq!((s[0].last15m, s[0].total), (0, 3));
    }

    #[test]
    fn cidr_and_forwarded_for() {
        let c = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(c.contains(&"10.1.2.3".parse().unwrap()));
        assert!(!c.contains(&"11.1.2.3".parse().unwrap()));
        assert!(Cidr::parse("::1").unwrap().contains(&"::1".parse().unwrap()));
        // IPv4-mapped IPv6 clients match IPv4 blocks
        assert!(c.contains(&"::ffff:10.0.0.1".parse().unwrap()));
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "1.2.3.4, 10.0.0.2".parse().unwrap());
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        // untrusted peer: XFF ignored
        assert_eq!(client_ip(&h, Some(peer), &[]), Some(peer));
        // trusted peer: right-most untrusted hop
        assert_eq!(
            client_ip(&h, Some(peer), &[c]),
            Some("1.2.3.4".parse().unwrap())
        );
    }

    #[test]
    fn forwarded_for_ports_and_garbage() {
        let trusted = [Cidr::parse("10.0.0.0/8").unwrap()];
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let ip = |xff: &str| {
            let mut h = HeaderMap::new();
            h.insert("x-forwarded-for", xff.parse().unwrap());
            client_ip(&h, Some(peer), &trusted).unwrap().to_string()
        };
        // ports are stripped (v4 and bracketed v6)
        assert_eq!(ip("9.9.9.9, 1.2.3.4:5678"), "1.2.3.4");
        assert_eq!(ip("9.9.9.9, [2001:db8::7]:443"), "2001:db8::7");
        assert_eq!(ip("[2001:db8::8]"), "2001:db8::8");
        assert_eq!(ip("1.2.3.4:5678, 10.0.0.9:80"), "1.2.3.4");
        // an unparseable entry stops the walk at the last trusted hop: the
        // client-written entries left of it are never reached
        assert_eq!(ip("6.6.6.6, unknown, 10.0.0.2"), "10.0.0.2");
        assert_eq!(ip("6.6.6.6, 1.2.3.4:http"), "10.0.0.1");
        assert_eq!(ip("6.6.6.6, [::1]x"), "10.0.0.1");
        // several headers read as one list
        let mut h = HeaderMap::new();
        h.append("x-forwarded-for", "6.6.6.6".parse().unwrap());
        h.append("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert_eq!(client_ip(&h, Some(peer), &trusted), Some("1.2.3.4".parse().unwrap()));
    }

    #[test]
    fn ipv6_keys_are_the_64() {
        let k = |s: &str| ip_key(s.parse().unwrap());
        assert_eq!(k("1.2.3.4"), "1.2.3.4");
        assert_eq!(k("::ffff:1.2.3.4"), "1.2.3.4");
        assert_eq!(k("2001:db8:1:2::1"), "2001:db8:1:2::/64");
        assert_eq!(k("2001:db8:1:2:ffff:ffff:ffff:ffff"), "2001:db8:1:2::/64");
        assert_ne!(k("2001:db8:1:3::1"), k("2001:db8:1:2::1"));
        // the console's limit lookup finds IP overrides for a /64 key
        let mut p = Policy::default();
        p.ip_ov.push((Cidr::parse("2001:db8:1::/48").unwrap(), Ov { limiters: vec![], action: Action::Exempt }));
        let spec = p.builtin(&GLOBAL_IP).clone();
        assert_eq!(p.limit_for_key(&spec, "2001:db8:1:2::/64"), None);
        assert_eq!(p.limit_for_key(&spec, "2001:db8:2:2::/64"), Some(3000));
    }

    /// A forwarding peer's [`ClientIp`] wins over the TCP peer (the
    /// forwarding node) and over X-Forwarded-For.
    #[test]
    fn client_ip_extension_wins() {
        let mut ext = axum::http::Extensions::new();
        ext.insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 1], 9))));
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "7.7.7.7".parse().unwrap());
        let trusted = [Cidr::parse("10.0.0.0/8").unwrap()];
        assert_eq!(request_client_ip(&h, &ext, &trusted), Some("7.7.7.7".parse().unwrap()));
        ext.insert(ClientIp("203.0.113.5".parse().unwrap()));
        assert_eq!(request_client_ip(&h, &ext, &trusted), Some("203.0.113.5".parse().unwrap()));
    }
}
