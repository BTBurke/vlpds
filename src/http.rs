//! Outbound HTTP clients: one builder per role, each client built once and
//! shared, so connections are pooled and reused (DESIGN.md "HTTP").
//!
//! - [`PeerClient`]: node-to-node (forwarding, internal calls). h2c prior
//!   knowledge, large windows, keepalive PINGs, a few connections per peer.
//! - [`public`]: operator-configured upstreams (AppView, report service, PLC
//!   directory, relays). h2 via ALPN on https, a pooled HTTP/1.1 on http.
//! - [`guarded`]: targets derived from user input (did:web hosts, handle
//!   `.well-known`, OAuth client metadata, lexicon authorities, service
//!   endpoints from DID documents). [`public`] plus a DNS resolver that drops
//!   non-public addresses (unless dev mode).
//!
//! No client follows redirects: forwarded and proxied responses go back to
//! the caller as they are, and a redirect from a user-controlled host could
//! point anywhere. Every new outbound connection counts in
//! `vlpds_http_client_connects_total{role}`, so reuse regressions show up.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::time::Duration;

const USER_AGENT: &str = concat!("vlpds/", env!("CARGO_PKG_VERSION"));

/// Connections per peer node (`--peer-connections`). reqwest multiplexes
/// every request to a host over ONE h2 connection; several spread the h2
/// connection driver's work over threads and keep one stalled connection
/// from black-holing every forward.
pub const DEFAULT_PEER_CONNECTIONS: usize = 4;

/// Every role: TCP_NODELAY, TCP keepalive (dead peers behind NATs and
/// half-open connections are noticed even without traffic), no redirects,
/// connection counting.
fn base(role: &'static str) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .connector_layer(CountConnects(role))
}

/// Outbound to the internet (h2 negotiated by ALPN, else HTTP/1.1).
/// `max_idle` must cover the steady-state concurrency per host: a busy
/// HTTP/1.1 upstream beyond it opens and closes a connection per request.
fn outbound(role: &'static str, max_idle: usize) -> reqwest::ClientBuilder {
    base(role)
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(max_idle)
        // below the 90-120 s idle close of common load balancers/CDNs, so we
        // close first instead of racing a reused socket the server dropped
        .pool_idle_timeout(Duration::from_secs(60))
        // h2 only: a PING after 20 s without frames; no answer in 10 s = dead
        .http2_keep_alive_interval(Duration::from_secs(20))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
}

/// Operator-configured upstreams (AppView proxy, report service, PLC
/// directory, requestCrawl). Callers set per-request deadlines.
pub fn public() -> &'static reqwest::Client {
    static C: LazyLock<reqwest::Client> = LazyLock::new(|| {
        // the AppView proxy runs ~100-500 requests in flight to one host
        outbound("public", 1024).build().expect("reqwest client")
    });
    &C
}

/// Client for user-controlled URLs: [`public`]'s settings, plus (outside
/// dev mode) a resolver that refuses non-public addresses. Pair it with
/// [`crate::did_resolver::check_outbound_url`] for the scheme and IP literals
/// (the resolver only sees DNS names). Callers set per-request deadlines.
pub fn guarded(dev_mode: bool) -> &'static reqwest::Client {
    static STRICT: LazyLock<reqwest::Client> = LazyLock::new(|| {
        outbound("guarded", 32)
            .dns_resolver(Arc::new(PublicOnlyResolver))
            .build()
            .expect("reqwest client")
    });
    static DEV: LazyLock<reqwest::Client> =
        LazyLock::new(|| outbound("guarded", 32).build().expect("reqwest client"));
    if dev_mode {
        &DEV
    } else {
        &STRICT
    }
}

/// Node-to-node client: `n` independent h2c clients (one connection per
/// peer each), picked round-robin. Derefs to the next client, so
/// `app.http.get(..)` spreads calls over the connections.
#[derive(Clone)]
pub struct PeerClient(Arc<PeerInner>);

struct PeerInner {
    clients: Vec<reqwest::Client>,
    next: AtomicUsize,
}

impl PeerClient {
    pub fn new(n: usize) -> reqwest::Result<PeerClient> {
        let clients = (0..n.max(1)).map(|_| peer_builder().build()).collect::<Result<_, _>>()?;
        Ok(PeerClient(Arc::new(PeerInner { clients, next: AtomicUsize::new(0) })))
    }

    /// Wraps one existing client (tests).
    pub fn single(c: reqwest::Client) -> PeerClient {
        PeerClient(Arc::new(PeerInner { clients: vec![c], next: AtomicUsize::new(0) }))
    }

    pub fn pick(&self) -> &reqwest::Client {
        let c = &self.0.clients;
        if c.len() == 1 {
            return &c[0];
        }
        &c[self.0.next.fetch_add(1, Ordering::Relaxed) % c.len()]
    }
}

impl std::ops::Deref for PeerClient {
    type Target = reqwest::Client;
    fn deref(&self) -> &reqwest::Client {
        self.pick()
    }
}

/// Peers speak h2c (the listener is HTTP/1 + HTTP/2 auto). With HTTP/1.1,
/// forwarding ~10k writes/s at ~100 ms each needed ~1k concurrent
/// connections per peer: beyond the 256 pooled ones every request opened and
/// closed a TCP connection, and at 50k/s across 3 nodes the forwards blew
/// the TTFB deadline and the cluster collapsed to ~2k/s (bench 2026-10-02).
fn peer_builder() -> reqwest::ClientBuilder {
    base("peer")
        .http2_prior_knowledge()
        .http2_initial_stream_window_size(4 << 20)
        .http2_initial_connection_window_size(64 << 20)
        // a half-open connection would otherwise black-hole every forward on
        // it until each one's TTFB deadline: PING every 10 s, idle or not,
        // and drop the connection after 5 s without an answer
        .http2_keep_alive_interval(Duration::from_secs(10))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        // fail fast when a peer is unreachable (forwards return 503)
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(15))
}

/// DNS resolver that drops non-public addresses, so a hostname can't be used
/// to reach internal services (SSRF).
pub struct PublicOnlyResolver;

impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = public_addrs(&host).await?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// `host`'s public unicast addresses; an error when it has none.
pub async fn public_addrs(host: &str) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error + Send + Sync>> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, 0))
        .await?
        .filter(|a| crate::did_resolver::is_public_ip(a.ip()))
        .collect();
    if addrs.is_empty() {
        return Err(format!("{host} did not resolve to a public unicast address").into());
    }
    Ok(addrs)
}

/// Connector layer counting new connections per client role.
#[derive(Clone)]
struct CountConnects(&'static str);

impl<S> tower::Layer<S> for CountConnects {
    type Service = Counted<S>;
    fn layer(&self, inner: S) -> Counted<S> {
        Counted { inner, role: self.0 }
    }
}

#[derive(Clone)]
struct Counted<S> {
    inner: S,
    role: &'static str,
}

impl<S: tower::Service<R>, R> tower::Service<R> for Counted<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: R) -> S::Future {
        crate::metrics::HTTP_CLIENT_CONNECTS.with_label_values(&[self.role]).inc();
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn guarded_refuses_private_hosts() {
        // localhost resolves to loopback only: the strict client never connects
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let accepted = tokio::spawn(async move { l.accept().await.is_ok() });
        let url = format!("http://localhost:{port}/.well-known/atproto-did");
        let e = guarded(false).get(&url).send().await.unwrap_err();
        assert!(e.is_connect(), "{e:?}");
        assert!(format!("{e:?}").contains("public unicast"), "{e:?}");
        assert!(public_addrs("localhost").await.is_err());
        // dev mode reaches it
        let _ = tokio::time::timeout(Duration::from_secs(2), guarded(true).get(&url).send()).await;
        assert!(tokio::time::timeout(Duration::from_secs(2), accepted).await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn peer_connections_are_reused() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", l.local_addr().unwrap());
        let router = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
        tokio::spawn(crate::server::serve(l, router));
        let peers = PeerClient::new(3).unwrap();
        let count = || crate::metrics::HTTP_CLIENT_CONNECTS.with_label_values(&["peer"]).get();
        let before = count();
        for _ in 0..60 {
            let r = peers.get(&url).send().await.unwrap();
            assert_eq!(r.version(), reqwest::Version::HTTP_2);
            assert_eq!(r.text().await.unwrap(), "ok");
        }
        // one connection per client (other tests may connect concurrently)
        let opened = count() - before;
        assert!((3..10).contains(&opened), "{opened} connections for 60 requests");
    }
}
