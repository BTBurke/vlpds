//! External handle resolution, as the reference's `HandleResolver`
//! (@atproto/identity): DNS TXT `_atproto.<handle>` (`did=<DID>`) and
//! `https://<handle>/.well-known/atproto-did`, started together; the DNS
//! answer wins when there is one, else the HTTPS one. Each has a 3 s deadline
//! (the reference's `timeout` default). The reference's optional backup
//! nameservers are not implemented.
//!
//! Abuse bounds: TXT lookups go only to the system resolver (or the injected
//! [`TxtResolver`]), for a fully qualified name (trailing dot: no search
//! domains), at most [`MAX_TXT_RECORDS`] records of at most
//! [`MAX_TXT_BYTES`] each are considered. The HTTPS fetch is the caller's
//! (SSRF-guarded, size-capped: `xrpc::identity`).

use futures::future::BoxFuture;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Deadline of each method (reference HandleResolver `timeout`: 3000 ms).
pub const TIMEOUT: Duration = Duration::from_secs(3);
/// TXT records considered per lookup; the rest are ignored.
pub const MAX_TXT_RECORDS: usize = 32;
/// A longer TXT record (its strings joined) is ignored.
pub const MAX_TXT_BYTES: usize = 4096;

const SUBDOMAIN: &str = "_atproto";
const PREFIX: &str = "did=";

/// TXT lookups: each record's character-strings joined.
pub trait TxtResolver: Send + Sync {
    fn txt<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>>;
}

/// A shared resolver in `server::Config` (Debug for the config's derive).
#[derive(Clone)]
pub struct TxtResolverRef(pub Arc<dyn TxtResolver>);

impl std::fmt::Debug for TxtResolverRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TxtResolver")
    }
}

/// The system resolver (hickory, /etc/resolv.conf).
struct SystemTxt;

static SYSTEM: LazyLock<Option<hickory_resolver::TokioResolver>> = LazyLock::new(|| {
    hickory_resolver::TokioResolver::builder_tokio()
        .ok()
        .map(|b| b.build())
});

impl TxtResolver for SystemTxt {
    fn txt<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>> {
        Box::pin(async move {
            let r = SYSTEM.as_ref().ok_or("DNS resolver unavailable")?;
            let lookup = r.txt_lookup(name).await.map_err(|e| e.to_string())?;
            Ok(lookup
                .iter()
                .take(MAX_TXT_RECORDS)
                .map(|txt| {
                    txt.txt_data()
                        .iter()
                        .map(|c| String::from_utf8_lossy(c).into_owned())
                        .collect::<String>()
                })
                .collect())
        })
    }
}

/// The configured resolver, else the system one.
pub fn resolver(configured: Option<&TxtResolverRef>) -> Arc<dyn TxtResolver> {
    static DEFAULT: LazyLock<Arc<dyn TxtResolver>> = LazyLock::new(|| Arc::new(SystemTxt));
    configured.map(|r| r.0.clone()).unwrap_or_else(|| DEFAULT.clone())
}

/// The DID of exactly one `did=` record (reference `parseDnsResult`: none
/// or several is no answer).
pub fn parse_dns_result(records: &[String]) -> Option<String> {
    let found: Vec<&str> = records
        .iter()
        .take(MAX_TXT_RECORDS)
        .filter(|r| r.len() <= MAX_TXT_BYTES)
        .filter_map(|r| r.strip_prefix(PREFIX))
        .collect();
    match found.as_slice() {
        [did] => Some(did.to_string()),
        _ => None,
    }
}

/// `_atproto.<handle>` TXT -> DID; any failure (NXDOMAIN, timeout) is None.
pub async fn resolve_dns(r: &dyn TxtResolver, handle: &str) -> Option<String> {
    let name = format!("{SUBDOMAIN}.{handle}.");
    let records = tokio::time::timeout(TIMEOUT, r.txt(&name)).await.ok()?.ok()?;
    parse_dns_result(&records)
}

/// The reference's resolution order: DNS and `http` concurrently; DNS's
/// answer if it has one (the HTTPS fetch is dropped), else `http`'s (a value
/// starting with `did:`).
pub async fn resolve<H>(r: &dyn TxtResolver, handle: &str, http: H) -> Option<String>
where
    H: std::future::Future<Output = Option<String>>,
{
    let dns = resolve_dns(r, handle);
    tokio::pin!(dns);
    tokio::pin!(http);
    let mut http_res: Option<Option<String>> = None;
    let dns_res = loop {
        tokio::select! {
            d = &mut dns => break d,
            h = &mut http, if http_res.is_none() => http_res = Some(h),
        }
    };
    if dns_res.is_some() {
        return dns_res;
    }
    let h = match http_res {
        Some(h) => h,
        None => http.await,
    };
    h.filter(|d| d.starts_with("did:"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Stub(HashMap<String, Vec<String>>);

    impl TxtResolver for Stub {
        fn txt<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>> {
            Box::pin(async move { self.0.get(name).cloned().ok_or_else(|| "NXDOMAIN".to_string()) })
        }
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn parses_exactly_one_did_record() {
        assert_eq!(parse_dns_result(&s(&["did=did:plc:abc"])), Some("did:plc:abc".into()));
        assert_eq!(parse_dns_result(&s(&["v=spf1 -all", "did=did:plc:abc"])), Some("did:plc:abc".into()));
        assert_eq!(parse_dns_result(&s(&["did=did:plc:a", "did=did:plc:b"])), None);
        assert_eq!(parse_dns_result(&s(&["foo"])), None);
        assert_eq!(parse_dns_result(&[]), None);
        let long = format!("did={}", "x".repeat(MAX_TXT_BYTES));
        assert_eq!(parse_dns_result(&[long, "did=did:plc:abc".into()]), Some("did:plc:abc".into()));
    }

    #[tokio::test]
    async fn dns_wins_then_http() {
        let mut m = HashMap::new();
        m.insert("_atproto.dns.test.".to_string(), s(&["did=did:plc:dns"]));
        let r = Stub(m);
        let http = |v: Option<&str>| {
            let v = v.map(String::from);
            async move { v }
        };
        assert_eq!(resolve(&r, "dns.test", http(Some("did:plc:http"))).await.as_deref(), Some("did:plc:dns"));
        assert_eq!(resolve(&r, "web.test", http(Some("did:plc:http"))).await.as_deref(), Some("did:plc:http"));
        assert_eq!(resolve(&r, "web.test", http(Some("not-a-did"))).await, None);
        assert_eq!(resolve(&r, "none.test", http(None)).await, None);
    }

    #[tokio::test]
    async fn dns_timeout_is_no_answer() {
        struct Hang;
        impl TxtResolver for Hang {
            fn txt<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>> {
                Box::pin(futures::future::pending())
            }
        }
        // waits out the 3 s DNS deadline
        let got = resolve(&Hang, "slow.test", async { Some("did:plc:http".to_string()) }).await;
        assert_eq!(got.as_deref(), Some("did:plc:http"));
    }
}
