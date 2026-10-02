//! Outbound HTTP clients: one builder per role, each client built once and
//! shared, so connections are pooled and reused (DESIGN.md "HTTP").
//!
//! - [`PeerClient`]: node-to-node (forwarding, internal calls). h2c prior
//!   knowledge, large windows, keepalive PINGs, a few connections per peer.
//! - [`public`]: operator-configured upstreams (AppView, report service, PLC
//!   directory, relays). h2 via ALPN on https, a pooled HTTP/1.1 on http.
//!   The AppView proxy has its own: [`proxy`] (https, one client per IO
//!   thread) and [`h1`] (plain http, one capped pool per host with
//!   per-thread slots).
//! - [`guarded`]: targets derived from user input (did:web hosts, handle
//!   `.well-known`, OAuth client metadata, lexicon authorities, service
//!   endpoints from DID documents). [`public`] plus a DNS resolver that drops
//!   non-public addresses (unless dev mode).
//!
//! No client follows redirects: forwarded and proxied responses go back to
//! the caller as they are, and a redirect from a user-controlled host could
//! point anywhere. Every new outbound connection counts in
//! `vlpds_http_client_connects_total{role}`, so reuse regressions show up.
//!
//! Response bodies streamed from an upstream to a client (forwards, proxied
//! calls) go through [`stall::Watched`]: one whose client stops taking it
//! is dropped after [`stall::WRITE_STALL`], so a client that never reads
//! can't pin upstream flow-control windows or pooled connections.

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
/// `http://` AppView): hyper's connection API under a lock-light pool.
/// Compared with reqwest + hyper-util's pool, a request normally takes only
/// its own thread's slot lock (uncontended), parses no URL and runs no
/// retry/redirect layers.
///
/// Pool, per upstream host ("host:port"):
/// - idle connections sit in per-thread slots: a finished response puts its
///   connection in the slot of the thread that read its body to the end, and
///   a request takes from its own thread's slot first (most recent first);
/// - a thread whose slot is empty takes one from another slot before it
///   connects, so connections never pile up per thread when tasks hop
///   threads: the count follows the concurrency, not threads x peak;
/// - at most [`MAX_CONNS`] connections are open per host (idle + busy; a
///   permit is held by each connection's task until the socket closes).
///   A request at the cap waits for a connection to come back or close
///   (`vlpds_http_client_pool_waits_total`), within the caller's deadline.
///
/// A connection is reused once its response body has been read to the end;
/// one dropped mid-body (e.g. the client went away) is closed, which also
/// ends the upstream exchange. A request that fails before it was written
/// on a reused connection (the server closed it while idle) is retried once
/// on a new one, like hyper-util's pool. Idle connections are closed after
/// [`H1_IDLE`] (checked when their slot is next used).
///
/// Hosts are never dropped: only operator-configured upstreams use this.
pub mod h1 {
    use super::*;
    use axum::body::Body;
    use bytes::Bytes;
    use hyper::client::conn::http1::SendRequest;
    use std::cell::Cell;
    use std::time::Instant;
    use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

    use axum::http;

    pub type Response = http::Response<PooledBody>;

    /// Connections open per upstream host (idle and in use). The AppView
    /// proxy runs ~100-500 requests in flight to one host per node.
    pub const MAX_CONNS: usize = 1024;
    /// Idle connections older than this are closed (below the 90-120 s idle
    /// close of common load balancers, like [`super::public`]'s pool).
    pub const H1_IDLE: Duration = Duration::from_secs(60);
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// A waiter at the cap re-checks the pool this often (a backstop: a
    /// returned connection or a freed permit wakes it first).
    const WAIT_RECHECK: Duration = Duration::from_millis(50);

    struct Idle {
        conn: SendRequest<Body>,
        since: Instant,
    }

    /// One thread's idle connections (most recent last). Padded to its own
    /// cache lines: threads update their own slots on every request.
    #[repr(align(128))]
    struct Slot {
        idle: parking_lot::Mutex<Vec<Idle>>,
        /// `idle.len()`, readable without the lock (stealers skip empty slots)
        len: AtomicUsize,
    }

    /// The pool of one upstream host.
    pub struct Host {
        authority: Box<str>,
        slots: Box<[Slot]>,
        /// one permit per open connection
        open: Arc<Semaphore>,
        max: usize,
        /// requests waiting at the cap
        waiting: AtomicUsize,
        returned: Notify,
    }

    static HOSTS: parking_lot::RwLock<Vec<&'static Host>> = parking_lot::RwLock::new(Vec::new());
    static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        /// this thread's slot number (modulo each host's slot count)
        static SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
        /// the last host this thread used (nearly always the one AppView)
        static LAST: Cell<Option<&'static Host>> = const { Cell::new(None) };
    }

    /// The pool for `authority`, created with `max` connections if new.
    fn host_with(authority: &str, max: usize) -> &'static Host {
        if let Some(h) = LAST.get().filter(|h| *h.authority == *authority) {
            return h;
        }
        let found = HOSTS.read().iter().copied().find(|h| *h.authority == *authority);
        let h = found.unwrap_or_else(|| {
            let mut hosts = HOSTS.write();
            if let Some(h) = hosts.iter().copied().find(|h| *h.authority == *authority) {
                return h;
            }
            let n = std::thread::available_parallelism().map_or(8, |n| n.get()).clamp(1, 64);
            let h: &'static Host = Box::leak(Box::new(Host {
                authority: authority.into(),
                slots: (0..n).map(|_| Slot { idle: Default::default(), len: AtomicUsize::new(0) }).collect(),
                open: Arc::new(Semaphore::new(max.max(1))),
                max: max.max(1),
                waiting: AtomicUsize::new(0),
                returned: Notify::new(),
            }));
            hosts.push(h);
            h
        });
        LAST.set(Some(h));
        h
    }

    /// The pool for `authority`.
    pub fn host(authority: &str) -> &'static Host {
        host_with(authority, MAX_CONNS)
    }

    /// Creates `authority`'s pool with a cap of `max` connections (tests).
    /// No effect if the pool exists.
    pub fn host_with_max(authority: &str, max: usize) -> &'static Host {
        host_with(authority, max)
    }

    impl Host {
        /// Connections open (idle and in use).
        pub fn open_connections(&self) -> usize {
            self.max - self.open.available_permits()
        }

        /// Idle connections across all slots.
        pub fn idle_connections(&self) -> usize {
            self.slots.iter().map(|s| s.len.load(Ordering::Relaxed)).sum()
        }

        fn my_slot(&self) -> usize {
            SLOT.with(|s| *s) % self.slots.len()
        }

        /// The most recent live idle connection of slot `i`; drops expired
        /// and closed ones on the way.
        fn take_from(&self, i: usize) -> Option<SendRequest<Body>> {
            let slot = &self.slots[i];
            let mut idle = slot.idle.lock();
            // the oldest sit at the front
            if idle.first().is_some_and(|c| c.since.elapsed() > H1_IDLE) {
                idle.retain(|c| c.since.elapsed() <= H1_IDLE);
            }
            let mut got = None;
            // (one just handed back may not be ready yet: its connection task
            // finishes the previous exchange first; `send` waits for it)
            while let Some(c) = idle.pop() {
                if !c.conn.is_closed() {
                    got = Some(c.conn);
                    break;
                }
            }
            slot.len.store(idle.len(), Ordering::SeqCst);
            got
        }

        /// An idle connection: this thread's slot first, then the others'.
        fn take(&self) -> Option<SendRequest<Body>> {
            let mine = self.my_slot();
            if self.slots[mine].len.load(Ordering::SeqCst) > 0 {
                if let Some(c) = self.take_from(mine) {
                    return Some(c);
                }
            }
            let n = self.slots.len();
            (1..n)
                .map(|k| (mine + k) % n)
                .filter(|&i| self.slots[i].len.load(Ordering::SeqCst) > 0)
                .find_map(|i| self.take_from(i))
        }

        /// Back to this thread's slot (a closed one is dropped).
        fn put(&self, conn: SendRequest<Body>) {
            if conn.is_closed() {
                return;
            }
            let slot = &self.slots[self.my_slot()];
            {
                let mut idle = slot.idle.lock();
                idle.push(Idle { conn, since: Instant::now() });
                slot.len.store(idle.len(), Ordering::SeqCst);
            }
            // (SeqCst on both sides: a waiter either sees this connection
            // in its re-check or is counted here)
            if self.waiting.load(Ordering::SeqCst) > 0 {
                self.returned.notify_one();
            }
        }

        /// A connection to send on and whether it was reused: an idle one,
        /// else a new one if under the cap, else the first to come back or
        /// the first permit a closed one frees.
        async fn checkout(&'static self, role: &'static str) -> Result<(SendRequest<Body>, bool), BoxError> {
            if let Some(c) = self.take() {
                return Ok((c, true));
            }
            if let Ok(p) = self.open.clone().try_acquire_owned() {
                return Ok((connect(role, self, p).await?, false));
            }
            crate::metrics::HTTP_CLIENT_POOL_WAITS.with_label_values(&[role]).inc();
            struct Waiting<'a>(&'a AtomicUsize);
            impl Drop for Waiting<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            self.waiting.fetch_add(1, Ordering::SeqCst);
            let _waiting = Waiting(&self.waiting);
            loop {
                let returned = self.returned.notified();
                tokio::pin!(returned);
                returned.as_mut().enable();
                if let Some(c) = self.take() {
                    return Ok((c, true));
                }
                tokio::select! {
                    p = self.open.clone().acquire_owned() => {
                        let p = p.map_err(|_| "pool closed")?;
                        return Ok((connect(role, self, p).await?, false));
                    }
                    _ = &mut returned => {}
                    _ = tokio::time::sleep(WAIT_RECHECK) => {}
                }
            }
        }

        /// A new connection, within the cap (waits for a permit).
        async fn fresh(&'static self, role: &'static str) -> Result<SendRequest<Body>, BoxError> {
            let p = match self.open.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => self.open.clone().acquire_owned().await.map_err(|_| "pool closed")?,
            };
            connect(role, self, p).await
        }
    }

    /// Opens a connection to `host`; its task holds `permit` until the
    /// connection closes.
    async fn connect(
        role: &'static str,
        host: &Host,
        permit: OwnedSemaphorePermit,
    ) -> Result<SendRequest<Body>, BoxError> {
        crate::metrics::HTTP_CLIENT_CONNECTS.with_label_values(&[role]).inc();
        let authority = &*host.authority;
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
            drop(permit);
        });
        Ok(send)
    }

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    /// Sends `req` (origin-form URI; Host is set here) to `authority`
    /// ("host:port") and returns the response, whose body hands the
    /// connection back to the pool once read to the end.
    pub async fn send(
        role: &'static str,
        authority: &str,
        mut req: http::Request<Body>,
    ) -> Result<Response, BoxError> {
        let host = self::host(authority);
        let h = req.headers_mut();
        h.insert(http::header::HOST, http::HeaderValue::from_str(authority)?);
        h.entry(http::header::USER_AGENT).or_insert(http::HeaderValue::from_static(USER_AGENT));
        h.entry(http::header::ACCEPT).or_insert(http::HeaderValue::from_static("*/*"));
        // a request body still uploading when the response ends keeps its
        // connection busy: such a connection is not pooled (PooledBody)
        let req_done = match hyper::body::Body::size_hint(req.body()).exact() {
            Some(0) => None,
            _ => {
                let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let body = std::mem::take(req.body_mut());
                *req.body_mut() = Body::new(TrackEnd { body, done: done.clone() });
                Some(done)
            }
        };
        let (mut conn, mut reused) = host.checkout(role).await?;
        if conn.ready().await.is_err() {
            drop(conn);
            (conn, reused) = (host.fresh(role).await?, false);
        }
        let resp = match conn.try_send_request(req).await {
            Ok(r) => r,
            Err(mut e) => match e.take_message() {
                // never written: the idle connection was closed under us
                Some(req) if reused => {
                    drop(conn);
                    conn = host.fresh(role).await?;
                    conn.send_request(req).await?
                }
                _ => return Err(e.into_error().into()),
            },
        };
        let (parts, body) = resp.into_parts();
        let body = PooledBody { body, conn: Some(conn), host, req_done };
        Ok(http::Response::from_parts(parts, body))
    }

    /// A request body that flags when it has been sent to the end.
    struct TrackEnd {
        body: Body,
        done: Arc<std::sync::atomic::AtomicBool>,
    }

    impl hyper::body::Body for TrackEnd {
        type Data = Bytes;
        type Error = axum::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, axum::Error>>> {
            let r = std::pin::Pin::new(&mut self.body).poll_frame(cx);
            if matches!(r, Poll::Ready(None)) || (matches!(r, Poll::Ready(Some(Ok(_)))) && self.body.is_end_stream()) {
                self.done.store(true, Ordering::Release);
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

    /// A response body that returns its connection to the pool at its end.
    pub struct PooledBody {
        body: hyper::body::Incoming,
        conn: Option<SendRequest<Body>>,
        host: &'static Host,
        /// set once the request body was sent to the end (None: no body)
        req_done: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    impl PooledBody {
        /// Back to the pool, if the exchange is complete: the response read
        /// to the end and the request body sent. One whose upload is still
        /// going (the upstream answered early) is not pooled: the next
        /// request on it would wait for that upload. Dropping the handle
        /// lets the connection finish the exchange and close.
        fn release(&mut self) {
            if hyper::body::Body::is_end_stream(&self.body) {
                if let Some(c) = self.conn.take() {
                    if self.req_done.as_ref().is_none_or(|d| d.load(Ordering::Acquire)) {
                        self.host.put(c);
                    }
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

/// Write-progress deadlines for response bodies streamed from an upstream
/// to a client (forwarded peer responses, proxied AppView responses).
///
/// The server polls a response body only when it can send more: an h2
/// stream whose client keeps its window at zero, or an HTTP/1.1 socket
/// whose client doesn't read, is never polled again. The upstream side then
/// stays busy for as long as the client likes: a peer h2 stream holds its
/// unread bytes out of the connection's flow-control window (enough of them
/// stall every forward on that connection), and a pooled AppView
/// connection is never returned. Each upstream call has its own deadlines
/// (time to first byte, upstream idle), but none of them sees a client that
/// stopped reading.
///
/// [`Watched`] notes when the client got the response head, and then each
/// chunk that wasn't the last; a sweeper thread drops the upstream body of
/// every watched body whose next poll hasn't come [`WRITE_STALL`] later (h2
/// resets the upstream stream and frees its window; an h1 connection
/// closes). A later poll gets an error, which resets the client's stream /
/// closes its connection. An upstream that keeps the body waiting is not a
/// stall (the callers' own idle deadlines cover that). Cost per body: one
/// allocation and an uncontended shard lock at creation, an uncontended
/// lock per chunk; bodies already buffered need none
/// ([`Watched::unwatched`]).
pub mod stall {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::AtomicU64;
    use std::sync::Weak;
    use std::time::Instant;

    /// How long a client may leave a watched body unpolled after taking a
    /// chunk. Generous: a client reading at all is polled far more often (a
    /// socket buffer or window's worth at its pace).
    pub const WRITE_STALL: Duration = Duration::from_secs(30);
    const SWEEP: Duration = Duration::from_secs(1);
    const SHARDS: usize = 16;

    static LIMIT_MS: AtomicU64 = AtomicU64::new(WRITE_STALL.as_millis() as u64);
    static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
    static STALLED: AtomicU64 = AtomicU64::new(0);

    /// Changes [`WRITE_STALL`] for this process (tests).
    pub fn set_limit(d: Duration) {
        LIMIT_MS.store(d.as_millis().max(1) as u64, Ordering::Relaxed);
    }

    /// Bodies dropped for a stalled client so far.
    pub fn stalled_total() -> u64 {
        STALLED.load(Ordering::Relaxed)
    }

    /// ms since [`EPOCH`], never 0.
    fn now_ms() -> u64 {
        EPOCH.elapsed().as_millis() as u64 + 1
    }

    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    trait Reap: Send + Sync {
        fn stalled(&self, now: u64, limit: u64) -> bool;
        /// Drops the upstream body if it is (still) stalled at `now`.
        fn reap(&self, now: u64, limit: u64) -> bool;
    }

    struct Registry {
        shards: Vec<parking_lot::Mutex<Vec<Weak<dyn Reap>>>>,
        next: AtomicUsize,
    }

    static REG: LazyLock<Registry> = LazyLock::new(|| {
        std::thread::Builder::new()
            .name("vlpds-stall-sweep".into())
            .spawn(|| loop {
                std::thread::sleep(SWEEP);
                sweep();
            })
            .expect("stall sweeper thread");
        Registry { shards: (0..SHARDS).map(|_| Default::default()).collect(), next: AtomicUsize::new(0) }
    });

    thread_local! {
        static SHARD: usize = REG.next.fetch_add(1, Ordering::Relaxed) % SHARDS;
    }

    fn register(w: Weak<dyn Reap>) {
        let i = SHARD.with(|s| *s);
        REG.shards[i].lock().push(w);
    }

    /// One pass: forgets finished bodies, drops stalled ones' upstreams.
    /// Returns how many were dropped.
    fn sweep() -> usize {
        let (now, limit) = (now_ms(), LIMIT_MS.load(Ordering::Relaxed));
        let mut stalled = Vec::new();
        for s in &REG.shards {
            s.lock().retain(|w| match w.upgrade() {
                None => false,
                Some(e) if e.stalled(now, limit) => {
                    stalled.push(e);
                    false
                }
                Some(_) => true,
            });
        }
        // (dropped outside the shard locks)
        let mut n = 0;
        for e in stalled {
            if e.reap(now, limit) {
                n += 1;
            } else {
                // polled as we looked: back on the list
                register(Arc::downgrade(&e));
            }
        }
        if n > 0 {
            STALLED.fetch_add(n as u64, Ordering::Relaxed);
            crate::metrics::HTTP_STALLED_BODIES.inc_by(n as u64);
            tracing::info!("dropped {n} upstream response bodies whose clients stopped reading");
        }
        n
    }

    struct Shared<B> {
        /// None once dropped for a stall
        body: parking_lot::Mutex<Option<B>>,
        /// when a chunk (not the last) was handed out and no poll has come
        /// since ([`now_ms`]); 0 = not waiting on the client. Written under
        /// `body`'s lock.
        waiting: AtomicU64,
    }

    impl<B: Send> Reap for Shared<B> {
        fn stalled(&self, now: u64, limit: u64) -> bool {
            let w = self.waiting.load(Ordering::Relaxed);
            w != 0 && now.saturating_sub(w) >= limit
        }

        fn reap(&self, now: u64, limit: u64) -> bool {
            // a body being polled right now is not stalled
            let Some(mut g) = self.body.try_lock() else { return false };
            if !self.stalled(now, limit) {
                return false;
            }
            let b = g.take();
            drop(g);
            drop(b);
            true
        }
    }

    enum State<B> {
        /// not watched
        Own(B),
        Shared(Arc<Shared<B>>),
    }

    /// A response body with a write-progress deadline (module docs), and
    /// `H` held until the body is dropped (e.g. an admission slot).
    pub struct Watched<B, H = ()> {
        state: State<B>,
        _hold: H,
    }

    impl<B: hyper::body::Body + Send + 'static> Watched<B, ()> {
        pub fn new(body: B) -> Self {
            Self::with_hold(body, ())
        }
    }

    impl<B: hyper::body::Body + Send + 'static, H> Watched<B, H> {
        /// Watched from now on: the client has the response head and must
        /// start taking the body (a client whose h2 window is zero from the
        /// start never polls it at all).
        pub fn with_hold(body: B, hold: H) -> Self {
            let waiting = if body.is_end_stream() { 0 } else { now_ms() };
            let s = Arc::new(Shared { body: parking_lot::Mutex::new(Some(body)), waiting: AtomicU64::new(waiting) });
            register(Arc::downgrade(&s) as Weak<dyn Reap>);
            Watched { state: State::Shared(s), _hold: hold }
        }

        /// Not watched (a body that holds no upstream), only holding `hold`.
        pub fn unwatched(body: B, hold: H) -> Self {
            Watched { state: State::Own(body), _hold: hold }
        }
    }

    fn stalled_error() -> BoxError {
        "client stopped reading the response".into()
    }

    impl<B, H> hyper::body::Body for Watched<B, H>
    where
        B: hyper::body::Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
        H: Unpin,
    {
        type Data = Bytes;
        type Error = BoxError;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
            match &mut self.state {
                State::Own(b) => std::pin::Pin::new(b).poll_frame(cx).map(|o| o.map(|r| r.map_err(Into::into))),
                State::Shared(s) => {
                    let mut g = s.body.lock();
                    s.waiting.store(0, Ordering::Relaxed);
                    let Some(b) = g.as_mut() else { return Poll::Ready(Some(Err(stalled_error()))) };
                    let r = std::pin::Pin::new(&mut *b).poll_frame(cx);
                    // handed a chunk with more to come: the client's turn
                    // (pending: the upstream's, which has its own deadlines)
                    if matches!(r, Poll::Ready(Some(Ok(_)))) && !b.is_end_stream() {
                        s.waiting.store(now_ms(), Ordering::Relaxed);
                    }
                    r.map(|o| o.map(|r| r.map_err(Into::into)))
                }
            }
        }

        fn is_end_stream(&self) -> bool {
            match &self.state {
                State::Own(b) => b.is_end_stream(),
                State::Shared(s) => s.body.lock().as_ref().is_some_and(|b| b.is_end_stream()),
            }
        }

        fn size_hint(&self) -> hyper::body::SizeHint {
            match &self.state {
                State::Own(b) => b.size_hint(),
                State::Shared(s) => s.body.lock().as_ref().map(|b| b.size_hint()).unwrap_or_default(),
            }
        }
    }

    /// Runs one sweep now (tests).
    pub fn sweep_now() -> usize {
        sweep()
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
///
/// Bulk downloads ([`is_bulk`]: repo exports, blobs, block fetches; large,
/// unauthenticated, streamed to clients at their pace) go over `n` other
/// connections ([`PeerClient::for_path`]): clients that read them slowly or
/// not at all fill only those connections' flow-control windows, never the
/// ones every other forward shares (and [`stall::Watched`] drops a body
/// whose client stopped reading).
#[derive(Clone)]
pub struct PeerClient(Arc<PeerInner>);

struct PeerInner {
    clients: Vec<reqwest::Client>,
    /// for [`is_bulk`] paths (empty: share `clients`)
    bulk: Vec<reqwest::Client>,
    next: AtomicUsize,
}

/// Peer calls whose responses are bulk downloads (see [`PeerClient`]).
pub fn is_bulk(path: &str) -> bool {
    matches!(
        path.strip_prefix("/xrpc/"),
        Some("com.atproto.sync.getRepo" | "com.atproto.sync.getBlob" | "com.atproto.sync.getBlocks")
    )
}

impl PeerClient {
    pub fn new(n: usize) -> reqwest::Result<PeerClient> {
        let clients = (0..n.max(1)).map(|_| peer_builder().build()).collect::<Result<_, _>>()?;
        let bulk = (0..n.max(1)).map(|_| peer_builder().build()).collect::<Result<_, _>>()?;
        Ok(PeerClient(Arc::new(PeerInner { clients, bulk, next: AtomicUsize::new(0) })))
    }

    /// Wraps one existing client (tests).
    pub fn single(c: reqwest::Client) -> PeerClient {
        PeerClient(Arc::new(PeerInner { clients: vec![c], bulk: Vec::new(), next: AtomicUsize::new(0) }))
    }

    pub fn pick(&self) -> &reqwest::Client {
        Self::pick_of(&self.0.clients, &self.0.next)
    }

    /// The client for a call to `path` (bulk downloads on their own
    /// connections).
    pub fn for_path(&self, path: &str) -> &reqwest::Client {
        if !self.0.bulk.is_empty() && is_bulk(path) {
            return Self::pick_of(&self.0.bulk, &self.0.next);
        }
        self.pick()
    }

    fn pick_of<'a>(c: &'a [reqwest::Client], next: &AtomicUsize) -> &'a reqwest::Client {
        if c.len() == 1 {
            return &c[0];
        }
        &c[next.fetch_add(1, Ordering::Relaxed) % c.len()]
    }
}

impl std::ops::Deref for PeerClient {
    type Target = reqwest::Client;
    fn deref(&self) -> &reqwest::Client {
        self.pick()
    }
}

/// Peer h2 receive windows. A response the entry node's client doesn't
/// read keeps its unread bytes, at most one stream window, out of the
/// connection window until [`stall::Watched`] drops it. 1 MiB per stream /
/// 64 MiB per connection takes 64 such responses per connection (x
/// connections per peer) to stall it, while one stream still moves ~5 GB/s
/// at LAN RTTs. (Was 4 MiB / 64 MiB: 16 unread exports stalled a
/// connection.)
pub const PEER_STREAM_WINDOW: u32 = 1 << 20;
pub const PEER_CONNECTION_WINDOW: u32 = 64 << 20;

/// Peers speak h2c (the listener is HTTP/1 + HTTP/2 auto). With HTTP/1.1,
/// forwarding ~10k writes/s at ~100 ms each needed ~1k concurrent
/// connections per peer: beyond the 256 pooled ones every request opened and
/// closed a TCP connection, and at 50k/s across 3 nodes the forwards blew
/// the TTFB deadline and the cluster collapsed to ~2k/s (bench 2026-10-02).
fn peer_builder() -> reqwest::ClientBuilder {
    base("peer")
        .http2_prior_knowledge()
        .http2_initial_stream_window_size(PEER_STREAM_WINDOW)
        .http2_initial_connection_window_size(PEER_CONNECTION_WINDOW)
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

    /// An upstream answering every request with 3,000 bytes after `delay`;
    /// returns its `host:port`.
    async fn h1_upstream(delay: Duration) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authority = l.local_addr().unwrap().to_string();
        let router = axum::Router::new().fallback(move || async move {
            tokio::time::sleep(delay).await;
            "x".repeat(3000)
        });
        tokio::spawn(crate::server::serve(l, router));
        authority
    }

    fn h1_get(path: &str) -> axum::http::Request<axum::body::Body> {
        let mut r = axum::http::Request::new(axum::body::Body::empty());
        *r.uri_mut() = path.parse().unwrap();
        r
    }

    fn h1_connects(role: &str) -> u64 {
        crate::metrics::HTTP_CLIENT_CONNECTS.with_label_values(&[role]).get()
    }

    /// Sends a GET on a new OS thread and reads its body to the end on
    /// another: the request runs on a thread whose own slot is empty, and
    /// the connection goes back to a slot the next request doesn't use first.
    fn h1_hop(rt: &tokio::runtime::Handle, role: &'static str, authority: &str) {
        let (h, a) = (rt.clone(), authority.to_string());
        let resp = std::thread::spawn(move || h.block_on(h1::send(role, &a, h1_get("/hop"))).unwrap()).join().unwrap();
        let h = rt.clone();
        let len = std::thread::spawn(move || {
            h.block_on(axum::body::to_bytes(axum::body::Body::new(resp.into_body()), usize::MAX)).unwrap().len()
        })
        .join()
        .unwrap();
        assert_eq!(len, 3000);
    }

    /// Requests and bodies on ever-new threads: per-thread pools connected
    /// once per hop; the shared slots keep it to one connection per request
    /// in flight (vlpds_http_client_connects_total).
    #[test]
    fn h1_pool_survives_thread_hops() {
        const ROLE: &str = "test-h1-hops";
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let authority = rt.block_on(h1_upstream(Duration::ZERO));
        for _ in 0..64 {
            h1_hop(rt.handle(), ROLE, &authority);
        }
        assert_eq!(h1_connects(ROLE), 1, "sequential requests hopping threads");
        let host = h1::host(&authority);
        assert_eq!((host.open_connections(), host.idle_connections()), (1, 1));

        // 8 concurrent chains of hopping requests: at most 8 connections
        let chains: Vec<_> = (0..8)
            .map(|_| {
                let (h, a) = (rt.handle().clone(), authority.clone());
                std::thread::spawn(move || {
                    for _ in 0..24 {
                        h1_hop(&h, ROLE, &a);
                    }
                })
            })
            .collect();
        for c in chains {
            c.join().unwrap();
        }
        let n = h1_connects(ROLE);
        assert!(n <= 8, "{n} connections for 8 chains of thread-hopping requests");
        assert_eq!(host.open_connections() as u64, n);
    }

    /// At most `max` connections per host: requests beyond it wait for one
    /// to come back instead of connecting.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn h1_pool_is_capped() {
        const ROLE: &str = "test-h1-cap";
        let authority = h1_upstream(Duration::from_millis(20)).await;
        let host = h1::host_with_max(&authority, 4);
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let a = authority.clone();
                tokio::spawn(async move {
                    for _ in 0..4 {
                        let r = h1::send(ROLE, &a, h1_get("/cap")).await.unwrap();
                        let b = axum::body::to_bytes(axum::body::Body::new(r.into_body()), usize::MAX).await.unwrap();
                        assert_eq!(b.len(), 3000);
                    }
                })
            })
            .collect();
        let mut max_open = 0;
        while !tasks.iter().all(|t| t.is_finished()) {
            max_open = max_open.max(host.open_connections());
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(max_open <= 4, "{max_open} open");
        assert!(h1_connects(ROLE) <= 4, "{} connects", h1_connects(ROLE));
        let waits = crate::metrics::HTTP_CLIENT_POOL_WAITS.with_label_values(&[ROLE]).get();
        assert!(waits > 0, "128 requests over 4 connections waited");
    }

    /// An upstream that answers before the request body is uploaded: the
    /// connection is not pooled (the next request on it would wait behind
    /// that upload), and it closes once the upload ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn h1_early_answer_during_upload_is_not_pooled() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use futures::StreamExt;
        const ROLE: &str = "test-h1-early";
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authority = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 64 << 10];
                    let _ = s.read(&mut buf).await; // the head (and whatever came with it)
                    let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok").await;
                    while s.read(&mut buf).await.is_ok_and(|n| n > 0) {}
                });
            }
        });
        let (go, wait) = tokio::sync::oneshot::channel::<()>();
        let upload = futures::stream::once(async { Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"part one")) }).chain(
            futures::stream::once(async move {
                let _ = wait.await;
                Ok(bytes::Bytes::from_static(b"part two"))
            }),
        );
        let mut req = axum::http::Request::new(axum::body::Body::from_stream(upload));
        *req.method_mut() = axum::http::Method::POST;
        *req.uri_mut() = "/upload".parse().unwrap();
        let r = h1::send(ROLE, &authority, req).await.unwrap();
        let b = axum::body::to_bytes(axum::body::Body::new(r.into_body()), usize::MAX).await.unwrap();
        assert_eq!(&b[..], b"ok");
        let host = h1::host(&authority);
        assert_eq!(host.idle_connections(), 0, "pooled while its upload is still going");
        // a new request gets a connection of its own at once
        let r = tokio::time::timeout(Duration::from_secs(2), h1::send(ROLE, &authority, h1_get("/next"))).await;
        assert!(r.expect("waited behind the upload").is_ok());
        drop(go);
    }

    /// A body dropped part-way (the client went away) closes its connection,
    /// which ends the upstream exchange and frees the permit, instead of
    /// pooling it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn h1_dropped_body_closes_its_connection() {
        const ROLE: &str = "test-h1-drop";
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(t) = self.0.take() {
                    let _ = t.send(());
                }
            }
        }
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authority = l.local_addr().unwrap().to_string();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel::<()>();
        let closed_tx = Arc::new(parking_lot::Mutex::new(Some(closed_tx)));
        // a body that never ends; its stream is dropped when the connection closes
        let router = axum::Router::new().fallback(move || {
            let guard = OnDrop(closed_tx.lock().take());
            async move {
                let s = futures::stream::unfold(guard, |g| async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Some((Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"chunk")), g))
                });
                axum::body::Body::from_stream(s)
            }
        });
        tokio::spawn(crate::server::serve(l, router));
        let r = h1::send(ROLE, &authority, h1_get("/stream")).await.unwrap();
        let mut body = axum::body::Body::new(r.into_body()).into_data_stream();
        let first = futures::StreamExt::next(&mut body).await.unwrap().unwrap();
        assert_eq!(first.as_ref(), b"chunk");
        drop(body);
        tokio::time::timeout(Duration::from_secs(5), closed_rx).await.expect("upstream saw the close").unwrap();
        let host = h1::host(&authority);
        let t = std::time::Instant::now();
        while host.open_connections() > 0 {
            assert!(t.elapsed() < Duration::from_secs(5), "permit not freed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(host.idle_connections(), 0);
    }
}
