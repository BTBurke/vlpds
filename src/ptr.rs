//! Reverse DNS for firehose subscribers on the operator console: a
//! forward-confirmed (FCrDNS) PTR name per address, looked up in the
//! background and cached. Anyone who controls an address's reverse zone can
//! claim any name, so a name only counts as verified when it resolves back
//! to the same address.

use futures::future::BoxFuture;
use std::collections::HashSet;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// For the whole lookup: the PTR query and the forward checks.
pub const TIMEOUT: Duration = Duration::from_secs(2);
pub const POSITIVE_TTL: Duration = Duration::from_secs(3600);
pub const NEGATIVE_TTL: Duration = Duration::from_secs(600);
pub const MAX_ENTRIES: usize = 4096;
const CONCURRENCY: usize = 4;
/// PTR names checked per address; a reverse zone can list any number.
const MAX_NAMES: usize = 4;
const MAX_NAME_LEN: usize = 253;

pub trait PtrResolver: Send + Sync {
    /// The PTR names, without the trailing dot.
    fn reverse(&self, ip: IpAddr) -> BoxFuture<'_, Result<Vec<String>, String>>;
    fn forward<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, String>>;
}

#[derive(Clone)]
pub struct PtrResolverRef(pub Arc<dyn PtrResolver>);

impl std::fmt::Debug for PtrResolverRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PtrResolver")
    }
}

struct SystemPtr;

static SYSTEM: LazyLock<Option<hickory_resolver::TokioResolver>> =
    LazyLock::new(|| hickory_resolver::TokioResolver::builder_tokio().ok().map(|b| b.build()));

impl PtrResolver for SystemPtr {
    fn reverse(&self, ip: IpAddr) -> BoxFuture<'_, Result<Vec<String>, String>> {
        Box::pin(async move {
            let r = SYSTEM.as_ref().ok_or("DNS resolver unavailable")?;
            let l = r.reverse_lookup(ip).await.map_err(|e| e.to_string())?;
            Ok(l.iter().map(|p| p.to_string()).collect())
        })
    }

    fn forward<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, String>> {
        Box::pin(async move {
            let r = SYSTEM.as_ref().ok_or("DNS resolver unavailable")?;
            // fully qualified, so no search domain is tried
            let l = r.lookup_ip(format!("{name}.")).await.map_err(|e| e.to_string())?;
            Ok(l.iter().collect())
        })
    }
}

/// What an address's lookup found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ptr {
    pub name: Option<String>,
    /// `name` resolves back to the address.
    pub verified: bool,
}

fn clean(name: &str) -> Option<String> {
    let n = name.trim_end_matches('.').to_ascii_lowercase();
    (!n.is_empty() && n.len() <= MAX_NAME_LEN && n.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b)))
        .then_some(n)
}

/// The first PTR name that resolves back to `ip`; failing that, the first
/// name, unverified. A timeout or error is no name.
pub async fn lookup(r: &dyn PtrResolver, ip: IpAddr) -> Ptr {
    let ip = ip.to_canonical();
    let check = async {
        let names: Vec<String> = r.reverse(ip).await.ok()?.iter().filter_map(|n| clean(n)).take(MAX_NAMES).collect();
        for n in &names {
            if r.forward(n).await.is_ok_and(|ips| ips.iter().any(|a| a.to_canonical() == ip)) {
                return Some(Ptr { name: Some(n.clone()), verified: true });
            }
        }
        Some(Ptr { name: names.into_iter().next(), verified: false })
    };
    tokio::time::timeout(TIMEOUT, check).await.ok().flatten().unwrap_or_default()
}

struct Cached {
    ptr: Ptr,
    expires: Instant,
}

/// Lookups never block a caller: `get` answers from the cache and starts a
/// lookup for a missing or expired address.
pub struct PtrCache {
    resolver: Arc<dyn PtrResolver>,
    cache: parking_lot::Mutex<lru::LruCache<IpAddr, Cached>>,
    pending: parking_lot::Mutex<HashSet<IpAddr>>,
    slots: Arc<tokio::sync::Semaphore>,
}

impl PtrCache {
    pub fn new(configured: Option<&PtrResolverRef>) -> Arc<PtrCache> {
        Self::with(configured.map(|r| r.0.clone()).unwrap_or_else(|| Arc::new(SystemPtr)), MAX_ENTRIES)
    }

    pub fn with(resolver: Arc<dyn PtrResolver>, max_entries: usize) -> Arc<PtrCache> {
        Arc::new(PtrCache {
            resolver,
            cache: parking_lot::Mutex::new(lru::LruCache::new(NonZeroUsize::new(max_entries.max(1)).unwrap())),
            pending: Default::default(),
            slots: Arc::new(tokio::sync::Semaphore::new(CONCURRENCY)),
        })
    }

    /// The cached answer, if any (an expired one too, while it's looked up
    /// again).
    pub fn get(self: &Arc<Self>, ip: IpAddr) -> Option<Ptr> {
        let ip = ip.to_canonical();
        let hit = self.cache.lock().get(&ip).map(|c| (c.ptr.clone(), c.expires > Instant::now()));
        if !hit.as_ref().is_some_and(|(_, fresh)| *fresh) {
            self.warm(ip);
        }
        hit.map(|(p, _)| p)
    }

    /// Starts a lookup unless one is under way. Lookups waiting for a slot
    /// are bounded by the cache size, so a flood of new addresses can't pile
    /// up tasks.
    pub fn warm(self: &Arc<Self>, ip: IpAddr) {
        let ip = ip.to_canonical();
        {
            let mut p = self.pending.lock();
            if p.len() >= self.cache.lock().cap().get() || !p.insert(ip) {
                return;
            }
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.pending.lock().remove(&ip);
            return;
        };
        let c = self.clone();
        rt.spawn(async move {
            let _slot = c.slots.clone().acquire_owned().await;
            let ptr = lookup(c.resolver.as_ref(), ip).await;
            c.insert(ip, ptr);
            c.pending.lock().remove(&ip);
        });
    }

    fn insert(&self, ip: IpAddr, ptr: Ptr) {
        let ttl = if ptr.verified { POSITIVE_TTL } else { NEGATIVE_TTL };
        self.cache.lock().put(ip, Cached { ptr, expires: Instant::now() + ttl });
    }

    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Stub {
        ptr: HashMap<IpAddr, Vec<String>>,
        a: HashMap<String, Vec<IpAddr>>,
        hang: bool,
        reverses: AtomicUsize,
    }

    impl PtrResolver for Stub {
        fn reverse(&self, ip: IpAddr) -> BoxFuture<'_, Result<Vec<String>, String>> {
            self.reverses.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.hang {
                    std::future::pending::<()>().await;
                }
                self.ptr.get(&ip).cloned().ok_or_else(|| "NXDOMAIN".into())
            })
        }
        fn forward<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<IpAddr>, String>> {
            Box::pin(async move { self.a.get(name).cloned().ok_or_else(|| "NXDOMAIN".into()) })
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn forward_confirmed() {
        let mut s = Stub::default();
        s.ptr.insert(ip("192.0.2.1"), vec!["Relay1.Example.COM.".into()]);
        s.a.insert("relay1.example.com".into(), vec![ip("192.0.2.9"), ip("192.0.2.1")]);
        assert_eq!(lookup(&s, ip("192.0.2.1")).await, Ptr { name: Some("relay1.example.com".into()), verified: true });
        // an IPv4-mapped address is the IPv4 one
        assert!(lookup(&s, ip("::ffff:192.0.2.1")).await.verified);
    }

    #[tokio::test]
    async fn mismatched_is_unverified() {
        let mut s = Stub::default();
        s.ptr.insert(ip("192.0.2.2"), vec!["relay.bsky.network".into(), "other.example".into()]);
        s.a.insert("relay.bsky.network".into(), vec![ip("198.51.100.7")]);
        assert_eq!(lookup(&s, ip("192.0.2.2")).await, Ptr { name: Some("relay.bsky.network".into()), verified: false });
        // a later name that does resolve back wins
        s.a.insert("other.example".into(), vec![ip("192.0.2.2")]);
        assert_eq!(lookup(&s, ip("192.0.2.2")).await, Ptr { name: Some("other.example".into()), verified: true });
        // no PTR, or junk in it
        assert_eq!(lookup(&s, ip("192.0.2.3")).await, Ptr::default());
        s.ptr.insert(ip("192.0.2.4"), vec!["<b>evil</b>".into()]);
        assert_eq!(lookup(&s, ip("192.0.2.4")).await, Ptr::default());
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_is_no_name() {
        let s = Stub { hang: true, ..Default::default() };
        let t = tokio::time::Instant::now();
        assert_eq!(lookup(&s, ip("192.0.2.1")).await, Ptr::default());
        assert_eq!(t.elapsed(), TIMEOUT);
    }

    async fn settle(c: &Arc<PtrCache>) {
        for _ in 0..100 {
            if c.pending.lock().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("lookups never finished");
    }

    #[tokio::test]
    async fn cache_answers_after_a_background_lookup() {
        let mut s = Stub::default();
        s.ptr.insert(ip("192.0.2.1"), vec!["a.example".into()]);
        s.a.insert("a.example".into(), vec![ip("192.0.2.1")]);
        let s = Arc::new(s);
        let c = PtrCache::with(s.clone(), 8);
        assert_eq!(c.get(ip("192.0.2.1")), None);
        // a second ask while it's under way starts nothing more
        assert_eq!(c.get(ip("192.0.2.1")), None);
        settle(&c).await;
        assert_eq!(c.get(ip("192.0.2.1")), Some(Ptr { name: Some("a.example".into()), verified: true }));
        assert_eq!(c.get(ip("192.0.2.2")), None);
        settle(&c).await;
        assert_eq!(c.get(ip("192.0.2.2")), Some(Ptr::default()));
        assert_eq!(s.reverses.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cache_is_bounded() {
        let c = PtrCache::with(Arc::new(Stub::default()), 3);
        for i in 0..10u8 {
            c.warm(IpAddr::from([192, 0, 2, i]));
            settle(&c).await;
        }
        assert_eq!(c.len(), 3);
        // least recently used go first
        assert!(c.cache.lock().peek(&IpAddr::from([192, 0, 2, 9])).is_some());
        assert!(c.cache.lock().peek(&IpAddr::from([192, 0, 2, 0])).is_none());
    }

    #[tokio::test]
    async fn negative_answers_expire_sooner() {
        let c = PtrCache::with(Arc::new(Stub::default()), 8);
        c.insert(ip("192.0.2.1"), Ptr { name: Some("a.example".into()), verified: true });
        c.insert(ip("192.0.2.2"), Ptr::default());
        c.insert(ip("192.0.2.3"), Ptr { name: Some("spoof.example".into()), verified: false });
        let left = |a: &str| c.cache.lock().peek(&ip(a)).unwrap().expires.saturating_duration_since(Instant::now());
        assert!(left("192.0.2.1") > NEGATIVE_TTL);
        assert!(left("192.0.2.2") <= NEGATIVE_TTL);
        assert!(left("192.0.2.3") <= NEGATIVE_TTL);
    }
}
