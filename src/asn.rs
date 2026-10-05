//! The origin AS of each firehose subscriber's address, for the operator
//! console: bgp.tools' whois (TCP 43) in bulk mode, batched in the
//! background and cached. Off (`--asn-lookup off`) means no connection at
//! all.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const BGP_TOOLS: &str = "bgp.tools:43";
pub const POSITIVE_TTL: Duration = Duration::from_secs(24 * 3600);
pub const NEGATIVE_TTL: Duration = Duration::from_secs(3600);
pub const BACKOFF: Duration = Duration::from_secs(600);
/// Connect, query and read, all told.
pub const TIMEOUT: Duration = Duration::from_secs(5);
/// Addresses wanted within this long go out in one query.
pub const DEBOUNCE: Duration = Duration::from_secs(30);
pub const MAX_ENTRIES: usize = 4096;
const MAX_BATCH: usize = 1000;
const MAX_REPLY_BYTES: u64 = 1 << 20;
const MAX_NAME_CHARS: usize = 120;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsnInfo {
    pub asn: u32,
    pub name: Option<String>,
    pub country: Option<String>,
}

/// The cache key: the address, or its /64 for IPv6 (one host's addresses).
fn key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => {
            let s = v6.segments();
            IpAddr::from([s[0], s[1], s[2], s[3], 0, 0, 0, 0])
        }
        v4 => v4,
    }
}

/// Only publicly routed addresses are worth asking about.
fn routable(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            let s0 = v6.segments()[0];
            !(v6.is_loopback() || v6.is_unspecified() || (s0 & 0xfe00) == 0xfc00 || (s0 & 0xffc0) == 0xfe80)
        }
    }
}

/// bgp.tools' verbose bulk answer, one line per query (more for an address
/// with several origins: the first wins):
/// `AS | IP | prefix | CC | registry | allocated | AS name`. AS 0 or a
/// missing line is no match (None).
pub fn parse(reply: &str) -> HashMap<IpAddr, Option<AsnInfo>> {
    let mut out = HashMap::new();
    for line in reply.lines() {
        let f: Vec<&str> = line.split('|').map(str::trim).collect();
        if f.len() < 7 {
            continue;
        }
        let (Ok(asn), Ok(ip)) = (f[0].parse::<u32>(), f[1].parse::<IpAddr>()) else { continue };
        let nonempty = |s: &str| (!s.is_empty()).then(|| s.chars().take(MAX_NAME_CHARS).collect::<String>());
        let name = f[6..].join("|");
        let info = (asn != 0).then(|| AsnInfo {
            asn,
            name: nonempty(&name).filter(|n| !n.starts_with("ERR_")),
            country: nonempty(f[3]),
        });
        out.entry(ip.to_canonical()).or_insert(info);
    }
    out
}

async fn query(server: &str, ips: &[IpAddr]) -> std::io::Result<String> {
    let mut req = String::from("begin\nverbose\n");
    for ip in ips {
        req.push_str(&ip.to_string());
        req.push('\n');
    }
    req.push_str("end\n");
    let mut s = tokio::net::TcpStream::connect(server).await?;
    s.write_all(req.as_bytes()).await?;
    let mut buf = Vec::new();
    s.take(MAX_REPLY_BYTES).read_to_end(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

struct Cached {
    info: Option<AsnInfo>,
    expires: Instant,
}

pub struct AsnCache {
    /// None: off.
    server: Option<String>,
    debounce: Duration,
    cache: parking_lot::Mutex<lru::LruCache<IpAddr, Cached>>,
    /// Keys wanted, with the address to ask about.
    wanted: parking_lot::Mutex<HashMap<IpAddr, IpAddr>>,
    backoff_until: parking_lot::Mutex<Option<Instant>>,
    wake: tokio::sync::Notify,
    started: AtomicBool,
}

impl AsnCache {
    pub fn with(server: Option<String>, debounce: Duration, max_entries: usize) -> Arc<AsnCache> {
        Arc::new(AsnCache {
            server,
            debounce,
            cache: parking_lot::Mutex::new(lru::LruCache::new(NonZeroUsize::new(max_entries.max(1)).unwrap())),
            wanted: Default::default(),
            backoff_until: Default::default(),
            wake: Default::default(),
            started: AtomicBool::new(false),
        })
    }

    pub fn enabled(&self) -> bool {
        self.server.is_some()
    }

    /// The cached answer (an expired one too, until it's replaced); a miss
    /// is queued for the next batch.
    pub fn get(self: &Arc<Self>, ip: IpAddr) -> Option<AsnInfo> {
        if !self.enabled() || !routable(ip) {
            return None;
        }
        let k = key(ip);
        let hit = self.cache.lock().get(&k).map(|c| (c.info.clone(), c.expires > Instant::now()));
        if !hit.as_ref().is_some_and(|(_, fresh)| *fresh) {
            self.want(k, ip.to_canonical());
        }
        hit.and_then(|(i, _)| i)
    }

    fn want(self: &Arc<Self>, k: IpAddr, ip: IpAddr) {
        {
            let mut w = self.wanted.lock();
            if w.len() >= self.cache.lock().cap().get() {
                return;
            }
            w.entry(k).or_insert(ip);
        }
        if !self.started.swap(true, Ordering::AcqRel) {
            match tokio::runtime::Handle::try_current() {
                Ok(rt) => {
                    rt.spawn(run(Arc::downgrade(self)));
                }
                Err(_) => self.started.store(false, Ordering::Release),
            }
        }
        self.wake.notify_one();
    }

    /// One batch: everything wanted, in one connection.
    async fn round(&self) {
        let Some(server) = &self.server else { return };
        if self.backoff_until.lock().is_some_and(|t| t > Instant::now()) {
            return;
        }
        let batch: Vec<(IpAddr, IpAddr)> = {
            let mut w = self.wanted.lock();
            let keys: Vec<IpAddr> = w.keys().take(MAX_BATCH).copied().collect();
            keys.into_iter().filter_map(|k| w.remove(&k).map(|ip| (k, ip))).collect()
        };
        if batch.is_empty() {
            return;
        }
        let ips: Vec<IpAddr> = batch.iter().map(|(_, ip)| *ip).collect();
        let answers = match tokio::time::timeout(TIMEOUT, query(server, &ips)).await {
            Ok(Ok(reply)) => {
                let a = parse(&reply);
                if a.is_empty() {
                    tracing::warn!(addresses = ips.len(), "ASN lookup: whois answered nothing usable; backing off");
                    *self.backoff_until.lock() = Some(Instant::now() + BACKOFF);
                }
                a
            }
            Ok(Err(e)) => {
                tracing::warn!("ASN lookup failed (backing off): {e}");
                *self.backoff_until.lock() = Some(Instant::now() + BACKOFF);
                HashMap::new()
            }
            Err(_) => {
                tracing::warn!("ASN lookup timed out (backing off)");
                *self.backoff_until.lock() = Some(Instant::now() + BACKOFF);
                HashMap::new()
            }
        };
        let now = Instant::now();
        let mut c = self.cache.lock();
        for (k, ip) in batch {
            let info = answers.get(&ip).cloned().flatten();
            let ttl = if info.is_some() { POSITIVE_TTL } else { NEGATIVE_TTL };
            c.put(k, Cached { info, expires: now + ttl });
        }
    }

    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

async fn run(weak: Weak<AsnCache>) {
    loop {
        let (wake, debounce) = {
            let Some(c) = weak.upgrade() else { return };
            let c2 = c.clone();
            (async move { c2.wake.notified().await }, c.debounce)
        };
        wake.await;
        tokio::time::sleep(debounce).await;
        let Some(c) = weak.upgrade() else { return };
        c.round().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Captured from bgp.tools (verbose bulk), plus a second origin for
    /// 1.1.1.1 as a multi-origin prefix answers.
    const SAMPLE: &str = "\
13335   | 1.1.1.1          | 1.1.1.0/24          | US | ARIN     | 2010-07-14 | Cloudflare, Inc.
4826    | 1.1.1.1          | 1.1.1.0/24          | AU | APNIC    | 2010-07-14 | Vocus Connect International Backbone
15169   | 2001:4860:4860::8888                     | 2001:4860::/32      | US | ARIN     | 2000-03-30 | Google LLC
0       | 10.0.0.1         | <nil>               |    | Unknown  | 0001-01-01 | ERR_AS_NAME_NOT_FOUND
";

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn parses_verbose_bulk_output() {
        let p = parse(SAMPLE);
        assert_eq!(
            p[&ip("1.1.1.1")],
            Some(AsnInfo { asn: 13335, name: Some("Cloudflare, Inc.".into()), country: Some("US".into()) })
        );
        assert_eq!(p[&ip("2001:4860:4860::8888")].as_ref().unwrap().asn, 15169);
        assert_eq!(p[&ip("10.0.0.1")], None);
        // the header of a single (non-bulk) query, and junk, are skipped
        let p = parse("AS | IP | BGP Prefix | CC | Registry | Allocated | AS Name\nnonsense\n16276 | 51.68.1.2 | 51.68.0.0/16 | FR | RIPE | 2016-08-05 | OVH SAS\n");
        assert_eq!(p.len(), 1);
        assert_eq!(p[&ip("51.68.1.2")].as_ref().unwrap().name.as_deref(), Some("OVH SAS"));
        // an AS with no name known
        let p = parse("64500 | 192.0.2.1 | 192.0.2.0/24 |  | Unknown | 0001-01-01 | ERR_AS_NAME_NOT_FOUND\n");
        assert_eq!(p[&ip("192.0.2.1")], Some(AsnInfo { asn: 64500, name: None, country: None }));
    }

    #[test]
    fn keys_and_routability() {
        assert_eq!(key(ip("2001:db8:1:2:3:4:5:6")), ip("2001:db8:1:2::"));
        assert_eq!(key(ip("::ffff:1.2.3.4")), ip("1.2.3.4"));
        for a in ["127.0.0.1", "10.1.2.3", "192.168.0.1", "100.64.0.1", "::1", "fd00::1", "fe80::1"] {
            assert!(!routable(ip(a)), "{a}");
        }
        for a in ["1.1.1.1", "51.68.1.2", "2001:4860::1"] {
            assert!(routable(ip(a)), "{a}");
        }
    }

    /// A whois stand-in: answers each bulk query from `answers` and records
    /// what it was asked.
    async fn stub(answers: &'static str) -> (String, Arc<parking_lot::Mutex<Vec<String>>>, Arc<AtomicUsize>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let asked = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let conns = Arc::new(AtomicUsize::new(0));
        let (a, n) = (asked.clone(), conns.clone());
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                n.fetch_add(1, Ordering::SeqCst);
                let mut req = Vec::new();
                let mut buf = [0u8; 4096];
                while !req.ends_with(b"end\n") {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(k) => req.extend_from_slice(&buf[..k]),
                    }
                }
                a.lock().push(String::from_utf8_lossy(&req).into_owned());
                let _ = s.write_all(answers.as_bytes()).await;
            }
        });
        (addr, asked, conns)
    }

    async fn until(f: impl Fn() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("never happened");
    }

    #[tokio::test]
    async fn batches_misses_into_one_query_and_caches() {
        let (addr, asked, conns) = stub(SAMPLE).await;
        let c = AsnCache::with(Some(addr), Duration::from_millis(50), 16);
        assert_eq!(c.get(ip("1.1.1.1")), None);
        assert_eq!(c.get(ip("2001:4860:4860::8888")), None);
        assert_eq!(c.get(ip("8.8.8.8")), None);
        // not routable: never asked
        assert_eq!(c.get(ip("10.0.0.1")), None);
        until(|| c.len() == 3).await;
        assert_eq!(conns.load(Ordering::SeqCst), 1);
        let q = asked.lock()[0].clone();
        assert!(q.starts_with("begin\nverbose\n") && q.ends_with("end\n"), "{q}");
        assert!(q.contains("1.1.1.1\n") && q.contains("8.8.8.8\n") && !q.contains("10.0.0.1"), "{q}");
        assert_eq!(c.get(ip("1.1.1.1")).unwrap().asn, 13335);
        // another address in the same /64
        assert_eq!(c.get(ip("2001:4860:4860::1")).unwrap().asn, 15169);
        // no answer for it: cached as no match, for less long
        assert_eq!(c.get(ip("8.8.8.8")), None);
        let left = c.cache.lock().peek(&ip("8.8.8.8")).unwrap().expires.saturating_duration_since(Instant::now());
        assert!(left <= NEGATIVE_TTL);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(conns.load(Ordering::SeqCst), 1, "cached answers ask nothing");
    }

    #[tokio::test]
    async fn failure_backs_off() {
        // nothing listens here
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        drop(l);
        let c = AsnCache::with(Some(addr), Duration::from_millis(10), 16);
        c.get(ip("1.1.1.1"));
        until(|| c.backoff_until.lock().is_some()).await;
        until(|| c.len() == 1).await;
        assert_eq!(c.get(ip("1.1.1.1")), None);
        c.get(ip("9.9.9.9"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        // held until the backoff ends
        assert!(c.wanted.lock().contains_key(&ip("9.9.9.9")));
    }

    #[tokio::test]
    async fn the_cache_is_bounded() {
        let (addr, _, _) = stub("").await;
        let c = AsnCache::with(Some(addr), Duration::from_millis(1), 4);
        for i in 1..=4u8 {
            c.get(IpAddr::from([1, 1, 1, i]));
        }
        until(|| c.len() == 4).await;
        // an empty answer backs off; clear it to go on
        *c.backoff_until.lock() = None;
        for i in 5..=8u8 {
            c.get(IpAddr::from([1, 1, 1, i]));
        }
        until(|| c.cache.lock().peek(&IpAddr::from([1, 1, 1, 8])).is_some()).await;
        assert_eq!(c.len(), 4);
        assert!(c.cache.lock().peek(&IpAddr::from([1, 1, 1, 1])).is_none());
    }

    #[tokio::test]
    async fn off_never_connects() {
        let (_addr, _, conns) = stub(SAMPLE).await;
        let c = AsnCache::with(None, Duration::from_millis(1), 16);
        assert!(!c.enabled());
        assert_eq!(c.get(ip("1.1.1.1")), None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(c.wanted.lock().is_empty());
        assert!(!c.started.load(Ordering::SeqCst));
        assert_eq!(conns.load(Ordering::SeqCst), 0);
    }
}
