//! External handle resolution, as the reference's `HandleResolver`
//! (@atproto/identity): DNS TXT and `/.well-known/atproto-did` started
//! together, DNS's answer winning. The reference's backup nameservers are
//! not implemented. TXT names are fully qualified (no search domains); the
//! HTTPS fetch is the caller's (SSRF-guarded: `xrpc::identity`).

use futures::future::BoxFuture;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Per method (reference HandleResolver `timeout`).
pub const TIMEOUT: Duration = Duration::from_secs(3);
const MAX_TXT_RECORDS: usize = 32;
/// A longer TXT record (its strings joined) is ignored.
const MAX_TXT_BYTES: usize = 4096;

const SUBDOMAIN: &str = "_atproto";
const PREFIX: &str = "did=";

/// Each record's character-strings joined.
pub trait TxtResolver: Send + Sync {
    fn txt<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>>;
}

/// Debug for `server::Config`'s derive.
#[derive(Clone)]
pub struct TxtResolverRef(pub Arc<dyn TxtResolver>);

impl std::fmt::Debug for TxtResolverRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TxtResolver")
    }
}

struct SystemTxt;

static SYSTEM: LazyLock<Option<hickory_resolver::TokioResolver>> =
    LazyLock::new(|| hickory_resolver::TokioResolver::builder_tokio().ok().map(|b| b.build()));

impl TxtResolver for SystemTxt {
    fn txt<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<String>, String>> {
        Box::pin(async move {
            let r = SYSTEM.as_ref().ok_or("DNS resolver unavailable")?;
            let lookup = r.txt_lookup(name).await.map_err(|e| e.to_string())?;
            Ok(lookup
                .iter()
                .take(MAX_TXT_RECORDS)
                .map(|txt| txt.txt_data().iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect::<String>())
                .collect())
        })
    }
}

pub fn resolver(configured: Option<&TxtResolverRef>) -> Arc<dyn TxtResolver> {
    static DEFAULT: LazyLock<Arc<dyn TxtResolver>> = LazyLock::new(|| Arc::new(SystemTxt));
    configured.map(|r| r.0.clone()).unwrap_or_else(|| DEFAULT.clone())
}

/// `https://<handle>/.well-known/atproto-did`'s first line, trimmed. The
/// default is `xrpc::identity`'s SSRF-guarded fetch; tests inject a stub.
pub trait WellKnownFetcher: Send + Sync {
    fn fetch<'a>(&'a self, handle: &'a str) -> BoxFuture<'a, Result<String, String>>;
}

#[derive(Clone)]
pub struct WellKnownRef(pub Arc<dyn WellKnownFetcher>);

impl std::fmt::Debug for WellKnownRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WellKnownFetcher")
    }
}

/// What `_atproto.<handle>` holds (the account page's handle check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsAnswer {
    One(String),
    /// Several `did=` records, which prove nothing.
    Several,
    /// No `did=` record, no such name, or no answer in time.
    Nothing,
}

fn dns_answer(records: &[String]) -> DnsAnswer {
    let found: Vec<&str> = records
        .iter()
        .take(MAX_TXT_RECORDS)
        .filter(|r| r.len() <= MAX_TXT_BYTES)
        .filter_map(|r| r.strip_prefix(PREFIX))
        .collect();
    match found.as_slice() {
        [did] => DnsAnswer::One(did.to_string()),
        [] => DnsAnswer::Nothing,
        _ => DnsAnswer::Several,
    }
}

pub async fn lookup_dns(r: &dyn TxtResolver, handle: &str) -> DnsAnswer {
    let name = format!("{SUBDOMAIN}.{handle}.");
    match tokio::time::timeout(TIMEOUT, r.txt(&name)).await {
        Ok(Ok(records)) => dns_answer(&records),
        _ => DnsAnswer::Nothing,
    }
}

/// Reference `parseDnsResult`: zero or several `did=` records is no answer.
async fn resolve_dns(r: &dyn TxtResolver, handle: &str) -> Option<String> {
    match lookup_dns(r, handle).await {
        DnsAnswer::One(did) => Some(did),
        _ => None,
    }
}

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
    let counted = |result: &str| crate::metrics::HANDLE_RESOLUTIONS.with_label_values(&[result]).inc();
    if dns_res.is_some() {
        counted("dns");
        return dns_res;
    }
    let h = match http_res {
        Some(h) => h,
        None => http.await,
    };
    let h = h.filter(|d| d.starts_with("did:"));
    counted(if h.is_some() { "http" } else { "not_found" });
    h
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
        let one = |d: &str| DnsAnswer::One(d.into());
        assert_eq!(dns_answer(&s(&["did=did:plc:abc"])), one("did:plc:abc"));
        assert_eq!(dns_answer(&s(&["v=spf1 -all", "did=did:plc:abc"])), one("did:plc:abc"));
        assert_eq!(dns_answer(&s(&["did=did:plc:a", "did=did:plc:b"])), DnsAnswer::Several);
        assert_eq!(dns_answer(&s(&["foo"])), DnsAnswer::Nothing);
        assert_eq!(dns_answer(&[]), DnsAnswer::Nothing);
        let long = format!("did={}", "x".repeat(MAX_TXT_BYTES));
        assert_eq!(dns_answer(&[long, "did=did:plc:abc".into()]), one("did:plc:abc"));
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
