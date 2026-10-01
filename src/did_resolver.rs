//! DID document resolution (did:plc via the PLC directory, did:web via
//! /.well-known/did.json) with an in-memory TTL cache, plus the SSRF-guarded
//! HTTP client used for all outbound requests to user-controlled endpoints
//! (did:web hosts, proxied service endpoints).
//!
//! DIDs hosted on this PDS are resolved by the caller without the network
//! (see `xrpc::proxy::resolve_did`).

use parking_lot::Mutex;
use serde_json::Value as J;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(600);
const CACHE_MAX: usize = 100_000;
const MAX_DOC_BYTES: usize = 256 << 10;
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("unsupported or malformed DID: {0}")]
    BadDid(String),
    #[error("DID not found: {0}")]
    NotFound(String),
    #[error("could not resolve DID {0}: {1}")]
    Failed(String, String),
}

pub struct DidResolver {
    plc_url: String,
    /// Allow http:// and private addresses (dev/test only).
    pub allow_insecure: bool,
    /// SSRF-guarded client (non-public addresses refused unless `allow_insecure`).
    http: reqwest::Client,
    /// Client for the operator-configured PLC directory (may be local).
    plc_http: reqwest::Client,
    cache: Mutex<HashMap<String, (Instant, Arc<J>)>>,
}

impl DidResolver {
    pub fn new(plc_url: &str, allow_insecure: bool) -> DidResolver {
        DidResolver {
            plc_url: plc_url.trim_end_matches('/').to_string(),
            allow_insecure,
            http: guarded_client(allow_insecure),
            plc_http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(RESOLVE_TIMEOUT)
                .build()
                .expect("reqwest client"),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The SSRF-guarded client (no redirects, connect/read timeouts; private
    /// addresses rejected at DNS resolution unless `allow_insecure`).
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    pub fn cached(&self, did: &str) -> Option<Arc<J>> {
        let c = self.cache.lock();
        c.get(did)
            .filter(|(at, _)| at.elapsed() < CACHE_TTL)
            .map(|(_, d)| d.clone())
    }

    pub fn invalidate(&self, did: &str) {
        self.cache.lock().remove(did);
    }

    pub async fn resolve(&self, did: &str) -> Result<Arc<J>, ResolveError> {
        if let Some(d) = self.cached(did) {
            return Ok(d);
        }
        let (url, client) = if let Some(id) = did.strip_prefix("did:plc:") {
            if id.is_empty() || !id.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return Err(ResolveError::BadDid(did.into()));
            }
            (format!("{}/{}", self.plc_url, did), &self.plc_http)
        } else if let Some(rest) = did.strip_prefix("did:web:") {
            let url = self.did_web_url(did, rest)?;
            // IP-literal hosts bypass the client's DNS filter; check them here.
            let parsed = reqwest::Url::parse(&url).map_err(|_| ResolveError::BadDid(did.into()))?;
            let local_http = parsed.scheme() == "http" && parsed.host_str() == Some("localhost");
            if !local_http {
                check_outbound_url(&parsed, self.allow_insecure)
                    .map_err(|e| ResolveError::Failed(did.into(), e))?;
            }
            (url, &self.http)
        } else {
            return Err(ResolveError::BadDid(did.into()));
        };
        let doc = tokio::time::timeout(RESOLVE_TIMEOUT, fetch_json(client, &url))
            .await
            .map_err(|_| ResolveError::Failed(did.into(), "timed out".into()))?
            .map_err(|e| match e {
                FetchError::NotFound => ResolveError::NotFound(did.into()),
                FetchError::Other(m) => ResolveError::Failed(did.into(), m),
            })?;
        if doc.get("id").and_then(|v| v.as_str()) != Some(did) {
            return Err(ResolveError::Failed(
                did.into(),
                "document id does not match DID".into(),
            ));
        }
        let doc = Arc::new(doc);
        let mut c = self.cache.lock();
        if c.len() >= CACHE_MAX {
            c.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
            if c.len() >= CACHE_MAX {
                c.clear();
            }
        }
        c.insert(did.to_string(), (Instant::now(), doc.clone()));
        Ok(doc)
    }

    fn did_web_url(&self, did: &str, rest: &str) -> Result<String, ResolveError> {
        // atproto only supports hostname-level did:web (no path segments).
        if rest.is_empty() || rest.contains(':') || rest.contains('/') {
            return Err(ResolveError::BadDid(did.into()));
        }
        let host = rest.replace("%3A", ":").replace("%3a", ":");
        if !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b':')
        {
            return Err(ResolveError::BadDid(did.into()));
        }
        let hostname = host.split(':').next().unwrap_or("");
        // https, except localhost (as the TS resolver does) and, in dev mode,
        // IP literals (so tests can serve did.json from 127.0.0.1:port).
        let plain = hostname == "localhost" || (self.allow_insecure && is_ip_literal(hostname));
        let scheme = if plain { "http" } else { "https" };
        Ok(format!("{scheme}://{host}/.well-known/did.json"))
    }
}

fn is_ip_literal(h: &str) -> bool {
    h.parse::<IpAddr>().is_ok()
}

enum FetchError {
    NotFound,
    Other(String),
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> Result<J, FetchError> {
    use futures::StreamExt;
    let resp = client
        .get(url)
        .header("accept", "application/did+ld+json, application/json")
        .send()
        .await
        .map_err(|e| FetchError::Other(e.to_string()))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND || resp.status() == reqwest::StatusCode::GONE
    {
        return Err(FetchError::NotFound);
    }
    if !resp.status().is_success() {
        return Err(FetchError::Other(format!("status {}", resp.status())));
    }
    if resp
        .content_length()
        .is_some_and(|l| l as usize > MAX_DOC_BYTES)
    {
        return Err(FetchError::Other("document too large".into()));
    }
    let mut buf = Vec::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        let chunk = chunk.map_err(|e| FetchError::Other(e.to_string()))?;
        if buf.len() + chunk.len() > MAX_DOC_BYTES {
            return Err(FetchError::Other("document too large".into()));
        }
        buf.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&buf).map_err(|e| FetchError::Other(format!("invalid JSON: {e}")))
}

/// Finds a service endpoint in a DID document by fragment id ("atproto_pds",
/// "bsky_appview", ...). Accepts both "#id" and "{did}#id" forms.
pub fn service_endpoint(doc: &J, service_id: &str) -> Option<String> {
    let did = doc.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let short = format!("#{service_id}");
    let full = format!("{did}#{service_id}");
    doc.get("service")?.as_array()?.iter().find_map(|s| {
        let id = s.get("id")?.as_str()?;
        if id != short && id != full {
            return None;
        }
        let ep = s.get("serviceEndpoint")?.as_str()?;
        reqwest::Url::parse(ep)
            .ok()
            .filter(|u| matches!(u.scheme(), "http" | "https") && u.host().is_some())?;
        Some(ep.to_string())
    })
}

/// atproto signing key (#atproto verification method) as a multibase string.
pub fn signing_key_multibase(doc: &J) -> Option<String> {
    let did = doc.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let full = format!("{did}#atproto");
    doc.get("verificationMethod")?
        .as_array()?
        .iter()
        .find_map(|m| {
            let id = m.get("id")?.as_str()?;
            (id == "#atproto" || id == full)
                .then(|| m.get("publicKeyMultibase")?.as_str().map(String::from))?
        })
}

/// True for globally routable unicast addresses. Loopback, private (RFC 1918 /
/// ULA), link-local, CGNAT, multicast, unspecified, documentation and other
/// special-purpose ranges are rejected (SSRF protection).
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_unspecified()
                || v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64/10 CGNAT
                || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0/24
                || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18/15 benchmarking
                || o[0] >= 240) // reserved
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00 // ULA fc00::/7
                || (s[0] & 0xffc0) == 0xfe80 // link-local
                || (s[0] == 0x2001 && s[1] == 0x0db8) // documentation
                || (s[0] == 0x0064 && s[1] == 0xff9b) // NAT64
                || (s[0] == 0 && s[1] == 0 && s[2] == 0 && s[3] == 0 && s[4] == 0 && s[5] == 0))
            // IPv4-compatible
        }
    }
}

/// DNS resolver that drops non-public addresses, so a hostname can't be used
/// to reach internal services.
struct PublicOnlyResolver;

impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|a| is_public_ip(a.ip()))
                .collect();
            if addrs.is_empty() {
                return Err(format!("{host} did not resolve to a public unicast address").into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn guarded_client(allow_insecure: bool) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(60));
    if !allow_insecure {
        b = b.dns_resolver(Arc::new(PublicOnlyResolver));
    }
    b.build().expect("reqwest client")
}

/// Checks a URL against the SSRF policy before connecting: https only and no
/// non-public IP literals (DNS names are filtered by the client's resolver).
pub fn check_outbound_url(url: &reqwest::Url, allow_insecure: bool) -> Result<(), String> {
    if allow_insecure {
        return match url.scheme() {
            "http" | "https" => Ok(()),
            s => Err(format!("Forbidden protocol \"{s}:\"")),
        };
    }
    if url.scheme() != "https" {
        return Err(format!("Forbidden protocol \"{}:\"", url.scheme()));
    }
    let host = url.host_str().ok_or("missing host")?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<IpAddr>() {
        if !is_public_ip(ip) {
            return Err("Hostname resolved to non-unicast address".into());
        }
    } else if bare.eq_ignore_ascii_case("localhost")
        || bare.to_ascii_lowercase().ends_with(".localhost")
    {
        return Err("Hostname resolved to non-unicast address".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ips() {
        for s in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.0.1",
            "172.16.5.5",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:10.0.0.1",
            "224.0.0.1",
        ] {
            assert!(!is_public_ip(s.parse().unwrap()), "{s}");
        }
        for s in ["8.8.8.8", "1.1.1.1", "2606:4700::1111", "::ffff:8.8.8.8"] {
            assert!(is_public_ip(s.parse().unwrap()), "{s}");
        }
    }

    #[test]
    fn did_web_urls() {
        let r = DidResolver::new("https://plc.directory", false);
        assert_eq!(
            r.did_web_url("did:web:example.com", "example.com").unwrap(),
            "https://example.com/.well-known/did.json"
        );
        assert_eq!(
            r.did_web_url("x", "localhost%3A1234").unwrap(),
            "http://localhost:1234/.well-known/did.json"
        );
        assert!(r.did_web_url("x", "example.com:path").is_err());
        let dev = DidResolver::new("https://plc.directory", true);
        assert_eq!(
            dev.did_web_url("x", "127.0.0.1%3A99").unwrap(),
            "http://127.0.0.1:99/.well-known/did.json"
        );
        assert_eq!(
            dev.did_web_url("x", "example.com").unwrap(),
            "https://example.com/.well-known/did.json"
        );
    }

    #[test]
    fn service_lookup() {
        let doc = serde_json::json!({"id": "did:web:x", "service": [
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example"},
            {"id": "did:web:x#bsky_appview", "type": "BskyAppView", "serviceEndpoint": "https://api.example"},
        ]});
        assert_eq!(
            service_endpoint(&doc, "atproto_pds").as_deref(),
            Some("https://pds.example")
        );
        assert_eq!(
            service_endpoint(&doc, "bsky_appview").as_deref(),
            Some("https://api.example")
        );
        assert_eq!(service_endpoint(&doc, "nope"), None);
    }
}
