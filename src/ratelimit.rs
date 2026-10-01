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
//!
//! The reference's oauth-provider has no sign-in limits of its own; the PDS
//! applies createSession's to its account-manager login. vlpds adds a per-IP
//! cap and a per-account cap across IPs (password guessing is Argon2 CPU);
//! TOTP guessing is bounded separately by the persisted per-account lockout
//! in `crate::totp`.
//!
//! Responses carry `RateLimit-Limit` / `-Remaining` / `-Reset` / `-Policy`
//! for the tightest bucket the request consumed (plus `Retry-After` on a
//! 429 `RateLimitExceeded`), as xrpc-server's `HttpRateLimiter` does.
//!
//! Counters live in 64 mutex-sharded maps keyed by a hash of (bucket, key),
//! so there is no global lock; each shard drops expired windows as it is
//! touched (amortized), so memory is bounded by the keys active within the
//! longest window.
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
//! updateHandle, email flows) are effectively exact because requests for a
//! DID are forwarded to and served by the node owning its partition.
//! Per-IP buckets count what one node serves: requests a node forwards are
//! counted on the owner, keyed by the client IP only if the forwarding nodes
//! are listed in `trusted_proxies` (forwarding appends `X-Forwarded-For`);
//! otherwise they count against the forwarding node's address. A client
//! spreading requests across N nodes can get up to N× the per-IP budget.

use crate::xrpc::XrpcError;
use axum::extract::{ConnectInfo, Request};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

/// One bucket definition: `points` per fixed `window_ms` window.
#[derive(Debug)]
pub struct Limit {
    pub name: &'static str,
    pub window_ms: u64,
    pub points: u32,
}

macro_rules! limit {
    ($id:ident, $name:expr, $window:expr, $points:expr) => {
        pub const $id: Limit = Limit {
            name: $name,
            window_ms: $window,
            points: $points,
        };
    };
}

limit!(GLOBAL_IP, "global-ip", 5 * MINUTE, 3000);
limit!(GET_REPO, "com.atproto.sync.getRepo-0", 5 * MINUTE, 6000);
limit!(CREATE_SESSION_DAY, "com.atproto.server.createSession-0", DAY, 300);
limit!(CREATE_SESSION_5MIN, "com.atproto.server.createSession-1", 5 * MINUTE, 30);
limit!(CREATE_ACCOUNT, "com.atproto.server.createAccount-0", 5 * MINUTE, 100);
limit!(DELETE_ACCOUNT, "com.atproto.server.deleteAccount-0", 5 * MINUTE, 50);
limit!(REQUEST_PASSWORD_RESET_DAY, "com.atproto.server.requestPasswordReset-0", DAY, 50);
limit!(REQUEST_PASSWORD_RESET_HOUR, "com.atproto.server.requestPasswordReset-1", HOUR, 15);
limit!(RESET_PASSWORD, "com.atproto.server.resetPassword-0", 5 * MINUTE, 50);
limit!(UPLOAD_BLOB, "com.atproto.repo.uploadBlob-0", DAY, 1000);
limit!(UPDATE_HANDLE_5MIN, "com.atproto.identity.updateHandle-0", 5 * MINUTE, 10);
limit!(UPDATE_HANDLE_DAY, "com.atproto.identity.updateHandle-1", DAY, 50);
limit!(REQUEST_ACCOUNT_DELETE_DAY, "com.atproto.server.requestAccountDelete-0", DAY, 15);
limit!(REQUEST_ACCOUNT_DELETE_HOUR, "com.atproto.server.requestAccountDelete-1", HOUR, 5);
limit!(REQUEST_EMAIL_CONFIRMATION_DAY, "com.atproto.server.requestEmailConfirmation-0", DAY, 15);
limit!(REQUEST_EMAIL_CONFIRMATION_HOUR, "com.atproto.server.requestEmailConfirmation-1", HOUR, 5);
limit!(REQUEST_EMAIL_UPDATE_DAY, "com.atproto.server.requestEmailUpdate-0", DAY, 15);
limit!(REQUEST_EMAIL_UPDATE_HOUR, "com.atproto.server.requestEmailUpdate-1", HOUR, 5);
limit!(REPO_WRITE_HOUR, "repo-write-hour", HOUR, 5000);
limit!(REPO_WRITE_DAY, "repo-write-day", DAY, 35000);
limit!(OAUTH_SIGN_IN_IP, "oauth-sign-in-ip", 5 * MINUTE, 100);
limit!(SIGN_IN_ACCOUNT, "sign-in-account", HOUR, 100);

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
        _ => &[],
    }
}

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

struct Shard {
    map: HashMap<u64, Window>,
    next_sweep_ms: u64,
}

const SHARDS: usize = 64;
/// A shard sweeps expired windows at most this often (and only when touched).
const SWEEP_EVERY_MS: u64 = 10_000;

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
                        next_sweep_ms: 0,
                    })
                })
                .collect(),
            hasher: Default::default(),
        }
    }
}

impl Counters {
    /// Consumes `points` from `limit`'s window for `key` at time `now_ms`.
    pub fn consume(&self, limit: &Limit, key: &str, points: u32, now_ms: u64) -> Status {
        let mut h = self.hasher.build_hasher();
        limit.name.hash(&mut h);
        key.hash(&mut h);
        let id = h.finish();
        let mut shard = self.shards[(id >> 58) as usize % SHARDS].lock();
        if now_ms >= shard.next_sweep_ms {
            shard.map.retain(|_, w| w.reset_ms > now_ms);
            shard.next_sweep_ms = now_ms + SWEEP_EVERY_MS;
        }
        let w = shard.map.entry(id).or_insert(Window {
            reset_ms: now_ms + limit.window_ms,
            used: 0,
        });
        if w.reset_ms <= now_ms {
            *w = Window {
                reset_ms: now_ms + limit.window_ms,
                used: 0,
            };
        }
        // As rate-limiter-flexible: points are counted even when rejected.
        w.used = w.used.saturating_add(points);
        Status {
            limit: limit.points,
            window_ms: limit.window_ms,
            remaining: limit.points.saturating_sub(w.used),
            reset_ms: w.reset_ms,
            exceeded: w.used > limit.points,
        }
    }

    /// Live windows (tests / metrics).
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().map.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
/// (Express `trust proxy` semantics).
pub fn client_ip(headers: &HeaderMap, peer: Option<IpAddr>, trusted: &[Cidr]) -> Option<IpAddr> {
    let peer = peer?.to_canonical();
    let is_trusted = |ip: &IpAddr| trusted.iter().any(|c| c.contains(ip));
    if trusted.is_empty() || !is_trusted(&peer) {
        return Some(peer);
    }
    let mut ip = peer;
    let hops: Vec<IpAddr> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|s| s.trim().parse::<IpAddr>().ok())
        .collect();
    for hop in hops.into_iter().rev() {
        ip = hop.to_canonical();
        if !is_trusted(&ip) {
            break;
        }
    }
    Some(ip)
}

// ---------------------------------------------------------------------------
// request-scoped context
// ---------------------------------------------------------------------------

pub struct Limiter {
    pub counters: Counters,
    pub trusted: Vec<Cidr>,
    pub bypass_key: Option<String>,
    /// Admin / internal tokens for the bypass check.
    cfg: crate::server::Config,
}

impl Limiter {
    pub fn new(cfg: &crate::server::Config) -> Limiter {
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
            cfg: cfg.clone(),
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
    ip: String,
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

    fn consume(&mut self, limits: &[&'static Limit], key: &str, points: u32) -> Result<(), XrpcError> {
        if self.bypass || points == 0 {
            return Ok(());
        }
        let now = now_ms();
        let mut exceeded = false;
        for l in limits {
            let s = self.limiter.counters.consume(l, key, points, now);
            exceeded |= s.exceeded;
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

/// Middleware: per-IP buckets, the request context for handler-level
/// buckets, and the RateLimit-* response headers.
pub async fn layer(
    axum::extract::State(limiter): axum::extract::State<Arc<Limiter>>,
    req: Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    // OAuth sign-in forms: context only; the handler consumes its buckets
    let sign_in_form = req.method() == axum::http::Method::POST && OAUTH_SIGN_IN_PATHS.contains(&path);
    if !sign_in_form
        && (!path.starts_with("/xrpc/")
            || path == "/xrpc/_health"
            || path == "/xrpc/com.atproto.sync.subscribeRepos")
    {
        return next.run(req).await;
    }
    let bypass = limiter.bypassed(req.headers());
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    let ip = client_ip(req.headers(), peer, &limiter.trusted)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".into());
    let route = ip_route_limits(path);
    let global = !sign_in_form && path != "/xrpc/com.atproto.sync.getRepo";
    let ctx = Ctx {
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
            g.and(r)
        });
        let mut resp = match pre {
            Ok(()) => next.run(req).await,
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
            name: "t",
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
    fn cidr_and_forwarded_for() {
        let c = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(c.contains(&"10.1.2.3".parse().unwrap()));
        assert!(!c.contains(&"11.1.2.3".parse().unwrap()));
        assert!(Cidr::parse("::1").unwrap().contains(&"::1".parse().unwrap()));
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
}
