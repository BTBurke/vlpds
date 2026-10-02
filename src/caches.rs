//! Entry caps of the in-memory caches, sized from one memory budget (by
//! default a fraction of the process's memory) split by weight and divided
//! by each cache's approximate entry size. The caps are process-wide (most
//! caches are statics) and read on every insert, so a lowered cap applies
//! on the next insert into a full shard.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Weak};

pub const DEFAULT_BUDGET_FRACTION: f64 = 0.10;
const FALLBACK_MEMORY: u64 = 4 << 30;
const MIN_ENTRIES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cache {
    SessionTokens,
    OAuthTokens,
    ProxyAccounts,
    ProxyJwts,
    DidDocs,
    Lexicons,
    OAuthClients,
    PermissionSets,
    SecurityControls,
    /// A miss is a KEK unwrap (a Cloud KMS round trip in production).
    SigningKeys,
    RecentWrites,
}

const N: usize = 11;

impl Cache {
    pub const ALL: [Cache; N] = [
        Cache::SessionTokens,
        Cache::OAuthTokens,
        Cache::ProxyAccounts,
        Cache::ProxyJwts,
        Cache::DidDocs,
        Cache::Lexicons,
        Cache::OAuthClients,
        Cache::PermissionSets,
        Cache::SecurityControls,
        Cache::SigningKeys,
        Cache::RecentWrites,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Cache::SessionTokens => "session_tokens",
            Cache::OAuthTokens => "oauth_tokens",
            Cache::ProxyAccounts => "proxy_accounts",
            Cache::ProxyJwts => "proxy_jwts",
            Cache::DidDocs => "did_docs",
            Cache::Lexicons => "lexicons",
            Cache::OAuthClients => "oauth_clients",
            Cache::PermissionSets => "permission_sets",
            Cache::SecurityControls => "security_controls",
            Cache::SigningKeys => "signing_keys",
            Cache::RecentWrites => "recent_writes",
        }
    }

    /// Approximate bytes per entry (key, value, map and allocator overhead).
    pub fn entry_bytes(self) -> usize {
        match self {
            // token (~250 B) + Arc<Claims> + signature key
            Cache::SessionTokens => 400,
            // token (~700 B) + decoded header and payload
            Cache::OAuthTokens => 1536,
            // DID + Arc<Keypair> (lazily derived public key) + status
            Cache::ProxyAccounts => 256,
            // (iss, aud, lxm) + the JWT (~400 B)
            Cache::ProxyJwts => 640,
            // parsed JSON document
            Cache::DidDocs => 3 << 10,
            // compiled schema of one record type
            Cache::Lexicons => 32 << 10,
            // metadata + JWKS
            Cache::OAuthClients => 8 << 10,
            Cache::PermissionSets => 4 << 10,
            // DID + Arc<Ctl> (empty sets for almost every account)
            Cache::SecurityControls => 256,
            // DID + public multibase + Arc<Keypair> + LRU links
            Cache::SigningKeys => 256,
            // DID + head/base revs; most entries hold no records (a repo
            // read through the proxy), a writer's up to 32 (posts ~1 KiB)
            Cache::RecentWrites => 512,
        }
    }

    /// Percent of the budget; the weights sum to 100.
    fn weight(self) -> u64 {
        match self {
            Cache::SessionTokens => 25,
            Cache::OAuthTokens => 21,
            Cache::ProxyAccounts => 15,
            Cache::ProxyJwts => 10,
            Cache::DidDocs => 10,
            Cache::Lexicons => 2,
            Cache::OAuthClients => 2,
            Cache::PermissionSets => 1,
            Cache::SecurityControls => 6,
            Cache::SigningKeys => 4,
            Cache::RecentWrites => 4,
        }
    }

    fn idx(self) -> usize {
        Cache::ALL.iter().position(|c| *c == self).unwrap()
    }
}

impl std::str::FromStr for Cache {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Cache> {
        Cache::ALL
            .into_iter()
            .find(|c| c.name() == s)
            .ok_or_else(|| anyhow::anyhow!("unknown cache {s:?} (one of {})", Cache::ALL.map(|c| c.name()).join(", ")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caps([usize; N]);

impl Caps {
    pub fn from_budget(bytes: u64) -> Caps {
        Caps(Cache::ALL.map(|c| ((bytes / 100 * c.weight()) as usize / c.entry_bytes()).max(MIN_ENTRIES)))
    }

    pub fn get(&self, c: Cache) -> usize {
        self.0[c.idx()]
    }

    pub fn set(&mut self, c: Cache, entries: usize) {
        self.0[c.idx()] = entries.max(1);
    }

    pub fn total_bytes(&self) -> u64 {
        Cache::ALL.iter().map(|c| (self.get(*c) * c.entry_bytes()) as u64).sum()
    }
}

impl std::fmt::Display for Caps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, c) in Cache::ALL.iter().enumerate() {
            let n = self.get(*c);
            let sep = if i == 0 { "" } else { ", " };
            write!(f, "{sep}{}={n} (~{} MiB)", c.name(), (n * c.entry_bytes()) >> 20)?;
        }
        Ok(())
    }
}

/// `name=entries` overrides (`--cache-entries`).
pub fn parse_overrides(v: &[String]) -> anyhow::Result<Vec<(Cache, usize)>> {
    v.iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (k, n) = s.split_once('=').ok_or_else(|| anyhow::anyhow!("expected <cache>=<entries>: {s}"))?;
            Ok((k.trim().parse()?, n.trim().parse()?))
        })
        .collect()
}

/// Physical RAM, or the cgroup limit when lower.
pub fn memory_bytes() -> Option<u64> {
    match (sys::physical_memory(), sys::cgroup_limit()) {
        (Some(p), Some(c)) => Some(p.min(c)),
        (p, c) => p.or(c),
    }
}

/// Also returns the budget (None: [`DEFAULT_BUDGET_FRACTION`] of [`memory_bytes`]).
pub fn resolve(budget: Option<u64>, overrides: &[(Cache, usize)]) -> (Caps, u64) {
    let budget = budget.unwrap_or_else(|| (memory_bytes().unwrap_or(FALLBACK_MEMORY) as f64 * DEFAULT_BUDGET_FRACTION) as u64);
    let mut caps = Caps::from_budget(budget);
    for (c, n) in overrides {
        caps.set(*c, *n);
    }
    (caps, budget)
}

static CAPS: LazyLock<[AtomicUsize; N]> = LazyLock::new(|| resolve(None, &[]).0 .0.map(AtomicUsize::new));

pub fn apply(caps: &Caps) {
    for (a, n) in CAPS.iter().zip(caps.0) {
        a.store(n, Ordering::Relaxed);
    }
}

pub fn cap(c: Cache) -> usize {
    CAPS[c.idx()].load(Ordering::Relaxed)
}

pub fn current() -> Caps {
    Caps(Cache::ALL.map(cap))
}

pub trait Len: Send + Sync {
    fn len(&self) -> usize;
}

impl<K: Send, V: Send> Len for parking_lot::Mutex<std::collections::HashMap<K, V>> {
    fn len(&self) -> usize {
        self.lock().len()
    }
}

impl<K: Send + Sync, V: Send + Sync> Len for parking_lot::RwLock<std::collections::HashMap<K, V>> {
    fn len(&self) -> usize {
        self.read().len()
    }
}

static TRACKED: LazyLock<parking_lot::Mutex<Vec<(Cache, Weak<dyn Len>)>>> = LazyLock::new(Default::default);

/// Registers `cache` for the entry-count metrics until it is dropped.
pub fn track<T: Len + 'static>(kind: Cache, cache: Arc<T>) -> Arc<T> {
    let w: Weak<dyn Len> = Arc::downgrade(&cache) as Weak<dyn Len>;
    let mut t = TRACKED.lock();
    t.retain(|(_, w)| w.strong_count() > 0);
    t.push((kind, w));
    cache
}

/// Summed over each cache's live instances.
pub fn entries() -> Caps {
    let live: Vec<(Cache, Arc<dyn Len>)> = TRACKED.lock().iter().filter_map(|(c, w)| Some((*c, w.upgrade()?))).collect();
    let mut n = [0usize; N];
    for (c, l) in live {
        n[c.idx()] += l.len();
    }
    Caps(n)
}

pub fn refresh_metrics() {
    let n = entries();
    for c in Cache::ALL {
        let l = [c.name()];
        crate::metrics::CACHE_ENTRIES.with_label_values(&l).set(n.get(c) as i64);
        crate::metrics::CACHE_BYTES.with_label_values(&l).set((n.get(c) * c.entry_bytes()) as i64);
        crate::metrics::CACHE_CAPACITY.with_label_values(&l).set(cap(c) as i64);
    }
}

#[cfg(target_os = "linux")]
mod sys {
    pub fn physical_memory() -> Option<u64> {
        let s = std::fs::read_to_string("/proc/meminfo").ok()?;
        let kb: u64 = s.lines().find_map(|l| l.strip_prefix("MemTotal:"))?.split_whitespace().next()?.parse().ok()?;
        Some(kb * 1024)
    }

    /// cgroup v2 `memory.max` of our cgroup (or the root of the mount, as
    /// in a container), else cgroup v1 `memory.limit_in_bytes`.
    pub fn cgroup_limit() -> Option<u64> {
        let own = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|s| s.lines().find_map(|l| l.strip_prefix("0::").map(|p| p.trim().to_string())));
        let mut paths = Vec::new();
        if let Some(p) = own.filter(|p| p != "/") {
            paths.push(format!("/sys/fs/cgroup{p}/memory.max"));
        }
        paths.push("/sys/fs/cgroup/memory.max".into());
        paths.push("/sys/fs/cgroup/memory/memory.limit_in_bytes".into());
        // "max" (v2) or a near-u64::MAX value (v1) mean no limit
        paths.iter().find_map(|p| std::fs::read_to_string(p).ok()?.trim().parse::<u64>().ok().filter(|v| *v < 1 << 60))
    }
}

#[cfg(target_os = "macos")]
mod sys {
    pub fn physical_memory() -> Option<u64> {
        extern "C" {
            fn sysctlbyname(name: *const std::ffi::c_char, old: *mut std::ffi::c_void, oldlen: *mut usize, new: *mut std::ffi::c_void, newlen: usize) -> i32;
        }
        let (mut v, mut len) = (0u64, std::mem::size_of::<u64>());
        let ok = unsafe { sysctlbyname(c"hw.memsize".as_ptr(), &mut v as *mut u64 as *mut _, &mut len, std::ptr::null_mut(), 0) } == 0;
        (ok && v > 0).then_some(v)
    }

    pub fn cgroup_limit() -> Option<u64> {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod sys {
    pub fn physical_memory() -> Option<u64> {
        None
    }
    pub fn cgroup_limit() -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_split() {
        assert_eq!(Cache::ALL.iter().map(|c| c.weight()).sum::<u64>(), 100);
        let caps = Caps::from_budget(1 << 30);
        // 25% of 1 GiB at 400 B each
        assert_eq!(caps.get(Cache::SessionTokens), (((1u64 << 30) / 100 * 25) / 400) as usize);
        assert!(caps.total_bytes() <= 1 << 30, "{caps}");
        assert!(caps.total_bytes() > (1 << 30) * 9 / 10, "{caps}");
        // tiny budgets keep a floor
        assert!(Cache::ALL.iter().all(|c| Caps::from_budget(0).get(*c) == MIN_ENTRIES));
        let (c, b) = resolve(Some(64 << 20), &parse_overrides(&["proxy_accounts=5".into(), " did_docs = 7 ".into()]).unwrap());
        assert_eq!(b, 64 << 20);
        assert_eq!((c.get(Cache::ProxyAccounts), c.get(Cache::DidDocs)), (5, 7));
        assert_eq!(c.get(Cache::SessionTokens), Caps::from_budget(64 << 20).get(Cache::SessionTokens));
        assert!(parse_overrides(&["nope=1".into()]).is_err());
        assert!(parse_overrides(&["did_docs".into()]).is_err());
        assert!(format!("{c}").contains("proxy_accounts=5 (~0 MiB)"));
    }

    #[test]
    fn memory_is_detected() {
        let m = memory_bytes().expect("RAM size on linux/macos");
        assert!(m >= 256 << 20, "{m}");
    }

    #[test]
    fn tracked_entries() {
        let m = Arc::new(parking_lot::Mutex::new(std::collections::HashMap::<u32, u32>::new()));
        let m = track(Cache::PermissionSets, m);
        m.lock().extend((0..3).map(|i| (i, i)));
        assert!(entries().get(Cache::PermissionSets) >= 3);
        let w = Arc::downgrade(&m);
        drop(m);
        assert!(w.upgrade().is_none(), "tracking holds no strong reference");
    }
}
