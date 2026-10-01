//! Outbound HTTP clients: one builder per role, each client built once and
//! shared, so connections are pooled and reused (DESIGN.md "HTTP").
//!
//! - [`PeerClient`]: node-to-node (forwarding, internal calls). h2c prior
//!   knowledge, large windows, keepalive PINGs, a few connections per peer.
//! - [`public`]: operator-configured upstreams (AppView, report service, PLC
//!   directory, relays). h2 via ALPN on https, a pooled HTTP/1.1 on http.
//!   The AppView proxy has its own: [`proxy`] (https, one client per IO
//!   thread) and [`h1`] (plain http, per-thread connection pools).
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
    outbound_no_read_timeout(role, max_idle).read_timeout(Duration::from_secs(30))
}

/// [`outbound`] without the per-read timeout. reqwest arms that timer for
/// the response head and re-arms it for every body frame, and every tokio
/// timer operation takes the runtime's one timer-wheel lock: callers that
/// bound their requests themselves skip it.
fn outbound_no_read_timeout(role: &'static str, max_idle: usize) -> reqwest::ClientBuilder {
    base(role)
        .connect_timeout(Duration::from_secs(5))
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

/// The AppView / report-service proxy client for `https://` upstreams
/// (plain `http://` ones use [`h1`]): [`public`]'s settings without the read
/// timeout (the proxy bounds the response head and body idle time itself),
/// as one client (connection pool; one h2 connection per host) per IO
/// thread. A single pool is one mutex that every proxied request takes
/// twice (checkout, return): at ~50k req/s over 6 threads that lock was
/// ~10% of the proxy's CPU. Each thread sticks to its own client.
pub fn proxy() -> &'static reqwest::Client {
    static C: LazyLock<Vec<reqwest::Client>> = LazyLock::new(|| {
        let n = std::thread::available_parallelism().map_or(8, |n| n.get()).clamp(2, 64);
        (0..n)
            .map(|_| outbound_no_read_timeout("public", 1024).build().expect("reqwest client"))
            .collect()
    });
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SHARD: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    &C[SHARD.with(|s| *s) % C.len()]
}

/// Plain-HTTP/1.1 client for the proxy fast path (an operator-configured
/// `http://` AppView): hyper's connection API under a per-thread pool of
/// idle connections. Compared with reqwest + hyper-util's pool, a request
/// normally takes no lock (the pool is the calling thread's, and a
/// connection goes back to whichever thread finishes its response), parses
/// no URL and runs no retry/redirect layers. Threads keep up to
/// [`LOCAL_IDLE`] idle connections each; beyond that they go to a shared
/// pool, which a thread with none left takes from before connecting, so the
/// connection count follows the total concurrency, not threads x peak.
///
/// A connection is reused once its response body has been read to the end;
/// one dropped mid-body is closed. A request that fails before it was
/// written on a reused connection (the server closed it while idle) is
/// retried once on a new one, like hyper-util's pool. Idle connections are
/// closed after [`H1_IDLE`] (checked when the thread next uses its pool).
pub mod h1 {
    use super::*;
    use axum::body::Body;
    use bytes::Bytes;
    use hyper::client::conn::http1::SendRequest;
    use std::cell::RefCell;
    use std::time::Instant;

    use axum::http;

    pub type Response = http::Response<PooledBody>;

    /// Idle connections kept per thread and host.
    pub const LOCAL_IDLE: usize = 32;
    /// Idle connections kept in the shared pool, per host.
    const SHARED_IDLE: usize = 1024;
    /// Idle connections older than this are closed (below the 90-120 s idle
    /// close of common load balancers, like [`super::public`]'s pool).
    pub const H1_IDLE: Duration = Duration::from_secs(60);
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    struct Idle {
        conn: SendRequest<Body>,
        since: Instant,
    }

    /// authority ("host:port") -> idle connections, most recent last
    type Pool = Vec<(Arc<str>, Vec<Idle>)>;

    thread_local! {
        static LOCAL: RefCell<Pool> = const { RefCell::new(Vec::new()) };
    }
    static SHARED: parking_lot::Mutex<Pool> = parking_lot::Mutex::new(Vec::new());

    /// The most recently used live connection of `pool` to `authority`;
    /// drops expired ones.
    fn take(pool: &mut Pool, authority: &str) -> Option<SendRequest<Body>> {
        let idle = &mut pool.iter_mut().find(|(a, _)| **a == *authority)?.1;
        // the oldest sit at the front
        if idle.first().is_some_and(|c| c.since.elapsed() > H1_IDLE) {
            idle.retain(|c| c.since.elapsed() <= H1_IDLE);
        }
        // (one just handed back may not be ready yet: its connection task
        // finishes the previous exchange first; `send` waits for it)
        while let Some(c) = idle.pop() {
            if !c.conn.is_closed() {
                return Some(c.conn);
            }
        }
        None
    }

    /// Adds `conn` unless `pool` already holds `max` for `authority`
    /// (then hands it back).
    fn put(pool: &mut Pool, authority: &Arc<str>, conn: SendRequest<Body>, max: usize) -> Option<SendRequest<Body>> {
        let i = match pool.iter().position(|(a, _)| **a == **authority) {
            Some(i) => i,
            None => {
                pool.push((authority.clone(), Vec::new()));
                pool.len() - 1
            }
        };
        let idle = &mut pool[i].1;
        if idle.len() >= max {
            return Some(conn);
        }
        idle.push(Idle { conn, since: Instant::now() });
        None
    }

    fn checkout(authority: &str) -> Option<SendRequest<Body>> {
        LOCAL.with_borrow_mut(|p| take(p, authority)).or_else(|| take(&mut SHARED.lock(), authority))
    }

    fn checkin(authority: &Arc<str>, conn: SendRequest<Body>) {
        if conn.is_closed() {
            return;
        }
        if let Some(conn) = LOCAL.with_borrow_mut(|p| put(p, authority, conn, LOCAL_IDLE)) {
            put(&mut SHARED.lock(), authority, conn, SHARED_IDLE);
        }
    }

    async fn connect(role: &'static str, authority: &str) -> Result<SendRequest<Body>, BoxError> {
        crate::metrics::HTTP_CLIENT_CONNECTS.with_label_values(&[role]).inc();
        let connect = async {
            let mut last = None;
            for addr in tokio::net::lookup_host(authority).await? {
                let sock = if addr.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
                sock.set_keepalive(true)?;
                sock.set_nodelay(true)?;
                match sock.connect(addr).await {
                    Ok(s) => return Ok(s),
                    Err(e) => last = Some(e),
                }
            }
            Err(last.unwrap_or_else(|| std::io::Error::other("no address")))
        };
        let tcp = tokio::time::timeout(CONNECT_TIMEOUT, connect).await.map_err(|_| "connect timeout")??;
        let (send, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp)).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("upstream connection: {e}");
            }
        });
        Ok(send)
    }

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    /// Sends `req` (origin-form URI; Host is set here) to `authority`
    /// ("host:port") and returns the response, whose body hands the
    /// connection back to the pool once read to the end.
    pub async fn send(
        role: &'static str,
        authority: &Arc<str>,
        mut req: http::Request<Body>,
    ) -> Result<Response, BoxError> {
        let host = http::HeaderValue::from_str(authority)?;
        let h = req.headers_mut();
        h.insert(http::header::HOST, host);
        h.entry(http::header::USER_AGENT).or_insert(http::HeaderValue::from_static(USER_AGENT));
        h.entry(http::header::ACCEPT).or_insert(http::HeaderValue::from_static("*/*"));
        let (mut conn, mut reused) = match checkout(authority) {
            Some(c) => (c, true),
            None => (connect(role, authority).await?, false),
        };
        if conn.ready().await.is_err() {
            (conn, reused) = (connect(role, authority).await?, false);
        }
        let resp = match conn.try_send_request(req).await {
            Ok(r) => r,
            Err(mut e) => match e.take_message() {
                // never written: the idle connection was closed under us
                Some(req) if reused => {
                    conn = connect(role, authority).await?;
                    conn.send_request(req).await?
                }
                _ => return Err(e.into_error().into()),
            },
        };
        let (parts, body) = resp.into_parts();
        let body = PooledBody { body, conn: Some(conn), authority: authority.clone() };
        Ok(http::Response::from_parts(parts, body))
    }

    /// A response body that returns its connection to the pool at its end.
    pub struct PooledBody {
        body: hyper::body::Incoming,
        conn: Option<SendRequest<Body>>,
        authority: Arc<str>,
    }

    impl PooledBody {
        /// Back to the pool, if the exchange is complete.
        fn release(&mut self) {
            if hyper::body::Body::is_end_stream(&self.body) {
                if let Some(c) = self.conn.take() {
                    checkin(&self.authority, c);
                }
            }
        }
    }

    impl Drop for PooledBody {
        fn drop(&mut self) {
            // e.g. a body with nothing in it, never polled; one dropped
            // mid-way closes its connection instead
            self.release();
        }
    }

    impl hyper::body::Body for PooledBody {
        type Data = Bytes;
        type Error = hyper::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
            let r = std::pin::Pin::new(&mut self.body).poll_frame(cx);
            // readers stop at `is_end_stream` (a known length), before None
            if matches!(r, Poll::Ready(None)) || self.body.is_end_stream() {
                self.release();
            }
            r
        }

        fn is_end_stream(&self) -> bool {
            self.body.is_end_stream()
        }

        fn size_hint(&self) -> hyper::body::SizeHint {
            self.body.size_hint()
        }
    }
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
