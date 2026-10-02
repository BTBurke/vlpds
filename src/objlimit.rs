//! Bounds the object-store requests one client (connection pool) has in
//! flight, so a burst of requests queues for a permit instead of opening a
//! connection each.
//!
//! Why: the HTTP client opens a new connection whenever every pooled one is
//! busy, and keeps at most `pool_max_idle_per_host` of them afterwards; the
//! rest close into TIME_WAIT. Unbounded, a takeover at load (shard opens,
//! replay, and a cold repo load for every write to the moved shards) had
//! 21,500 + 8,255 sockets open on two nodes within 10 s, the host's whole
//! ephemeral port range: every new connection then failed (`transport error
//! of kind Connect`), lease renewals with them, and both survivors
//! fail-stopped (bench/results/benchbox-2026-10-02-head, "Failover"). With
//! at most `limit` requests in flight and the pool keeping `limit` idle
//! connections, a pool never holds more than `limit` connections and never
//! churns them.
//!
//! Each client has a main lane and optionally a reserved one that requests
//! matching its [`Reserve`] use instead (the log client: segment PUTs, so
//! replay and backfill reads never delay a commit; the control-plane client:
//! node-lease PUTs, so a step's fan-out never delays a renewal). The
//! control plane also has its own client (connection pool) altogether.
//!
//! A permit is held for the whole request: retries and backoff inside the
//! client, and a GET's body until it is read to its end or dropped (the
//! connection is busy until then) — except blob GETs, whose bodies stream to
//! HTTP clients at their pace (one slow reader must not hold a permit), and
//! LIST / bulk-DELETE streams, which hold one until their first response
//! only (a caller may issue requests while it walks a listing; holding it
//! across them could deadlock a saturated pool). Uncontended, acquiring is
//! one atomic op: steady state never waits.
//!
//! Metrics (`client` = log | state | ctl, `lane` = main | reserved):
//! `vlpds_object_store_inflight` (requests holding a permit),
//! `vlpds_object_store_inflight_limit`, `vlpds_object_store_permit_waits_total`
//! (requests that found every permit taken) and
//! `vlpds_object_store_permit_wait_seconds` (how long those waited).

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use prometheus::{Histogram, IntCounter, IntGauge};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default in-flight bound of the state client (SlateDB, blobs, account
/// indexes): `--store-inflight`. Steady state at 20k writes/s was under 100
/// sockets per node (xh3g); this is headroom, not a throttle.
pub const DEFAULT_STATE_INFLIGHT: usize = 1024;
/// Default in-flight bound of the log client's reads (replay, firehose
/// backfill, peer followers, retention): `--log-store-inflight`.
pub const DEFAULT_LOG_INFLIGHT: usize = 256;
/// Log client writes (segment PUTs and their hedges, fences): their own
/// lane, at least this many (see [`log_write_permits`]).
pub const LOG_WRITE_PERMITS: usize = 64;
/// Control-plane client: every control-plane call but lease writes. A step
/// fans out to at most 32 calls at once.
pub const CTL_PERMITS: usize = 64;
/// Control-plane client, node-lease PUTs (renewals) only.
pub const LEASE_PERMITS: usize = 8;

/// The log client's write lane for `log_inflight` segment PUTs in flight
/// (each may be hedged once more).
pub fn log_write_permits(log_inflight: usize) -> usize {
    LOG_WRITE_PERMITS.max(4 * log_inflight)
}

/// Which requests take the reserved lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reserve {
    /// No reserved lane.
    None,
    /// Every write (PUT, multipart, copy): the log client's segment PUTs.
    Writes,
    /// PUTs to node leases (`nodes/*`): the control plane's renewals.
    LeaseWrites,
}

impl Reserve {
    fn reserved(self, write: bool, comp: &str) -> bool {
        match self {
            Reserve::None => false,
            Reserve::Writes => write,
            Reserve::LeaseWrites => write && comp == "ctl_lease",
        }
    }
}

/// One client's in-flight bounds.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Main lane permits.
    pub main: usize,
    /// Reserved lane permits (used when `reserve` isn't None).
    pub reserved: usize,
    pub reserve: Reserve,
}

impl Limits {
    pub fn new(main: usize) -> Limits {
        Limits { main: main.max(1), reserved: 0, reserve: Reserve::None }
    }

    pub fn with_reserved(self, reserve: Reserve, n: usize) -> Limits {
        Limits { reserved: n.max(1), reserve, ..self }
    }

    /// Connections the client may have open at once: what its pool should
    /// keep idle so none is closed and reopened.
    pub fn connections(&self) -> usize {
        self.main + if self.reserve == Reserve::None { 0 } else { self.reserved }
    }
}

/// One lane: a semaphore and its metrics.
#[derive(Clone, Debug)]
struct Lane {
    sem: Arc<Semaphore>,
    inflight: IntGauge,
    waits: IntCounter,
    wait_s: Histogram,
}

impl Lane {
    fn new(client: &str, lane: &str, n: usize) -> Lane {
        let l = [client, lane];
        crate::metrics::OBJ_INFLIGHT_LIMIT.with_label_values(&l).set(n as i64);
        Lane {
            sem: Arc::new(Semaphore::new(n)),
            inflight: crate::metrics::OBJ_INFLIGHT.with_label_values(&l),
            waits: crate::metrics::OBJ_PERMIT_WAITS.with_label_values(&l),
            wait_s: crate::metrics::OBJ_PERMIT_WAIT_SECONDS.with_label_values(&l),
        }
    }

    async fn acquire(&self) -> Permit {
        let p = match self.sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                self.waits.inc();
                let t = Instant::now();
                let p = self.sem.clone().acquire_owned().await.expect("object-store semaphore is never closed");
                self.wait_s.observe(t.elapsed().as_secs_f64());
                p
            }
        };
        self.inflight.inc();
        Permit { _p: p, inflight: self.inflight.clone() }
    }
}

/// A request in flight (dropped: the permit goes back).
struct Permit {
    _p: OwnedSemaphorePermit,
    inflight: IntGauge,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.inflight.dec();
    }
}

#[derive(Debug)]
pub struct Limited {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    client: &'static str,
    main: Lane,
    reserved: Option<Lane>,
    reserve: Reserve,
}

/// Wraps `inner` so at most `limits` of its requests are in flight.
pub fn limited(inner: Arc<dyn ObjectStore>, prefix: &str, client: &'static str, limits: Limits) -> Arc<dyn ObjectStore> {
    Arc::new(Limited::new(inner, prefix, client, limits))
}

impl Limited {
    pub fn new(inner: Arc<dyn ObjectStore>, prefix: &str, client: &'static str, limits: Limits) -> Limited {
        Limited {
            inner,
            prefix: prefix.trim_end_matches('/').to_string(),
            client,
            main: Lane::new(client, "main", limits.main),
            reserved: (limits.reserve != Reserve::None).then(|| Lane::new(client, "reserved", limits.reserved)),
            reserve: limits.reserve,
        }
    }

    fn lane(&self, write: bool, p: &Path) -> &Lane {
        match &self.reserved {
            Some(r) if self.reserve.reserved(write, crate::objstats::component(&self.prefix, p.as_ref())) => r,
            _ => &self.main,
        }
    }
}

impl std::fmt::Display for Limited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limited({}, {})", self.client, self.inner)
    }
}

/// `s`, holding `permit` until it ends (or fails) or is dropped.
fn hold_until_end<T: Send + 'static>(s: BoxStream<'static, Result<T>>, permit: Permit) -> BoxStream<'static, Result<T>> {
    let mut permit = Some(permit);
    let mut s = s;
    futures::stream::poll_fn(move |cx| {
        let item = futures::ready!(s.poll_next_unpin(cx));
        if !matches!(item, Some(Ok(_))) {
            drop(permit.take());
        }
        std::task::Poll::Ready(item)
    })
    .boxed()
}

/// The stream `make` opens once a permit of `lane` is held, holding it until
/// the first response (item, error or end).
fn hold_until_first<T: Send + 'static>(lane: Lane, make: impl FnOnce() -> BoxStream<'static, T> + Send + 'static) -> BoxStream<'static, T> {
    futures::stream::once(async move {
        let mut permit = Some(lane.acquire().await);
        make().inspect(move |_| {
            permit.take();
        })
    })
    .flatten()
    .boxed()
}

#[async_trait]
impl ObjectStore for Limited {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let _p = self.lane(true, location).acquire().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let lane = self.lane(true, location).clone();
        let inner = {
            let _p = lane.acquire().await;
            self.inner.put_multipart_opts(location, opts).await?
        };
        Ok(Box::new(LimitedUpload { inner, lane }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let p = self.lane(false, location).acquire().await;
        let head = options.head;
        let mut r = self.inner.get_opts(location, options).await?;
        // see the module doc: blob bodies stream at an HTTP client's pace
        if !head && crate::objstats::component(&self.prefix, location.as_ref()) != "blob" {
            if let GetResultPayload::Stream(s) = r.payload {
                r.payload = GetResultPayload::Stream(hold_until_end(s, p));
            }
        }
        Ok(r)
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        hold_until_first(self.main.clone(), move || inner.delete_stream(locations))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let (inner, prefix) = (self.inner.clone(), prefix.cloned());
        hold_until_first(self.main.clone(), move || inner.list(prefix.as_ref()))
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        let (inner, prefix, offset) = (self.inner.clone(), prefix.cloned(), offset.clone());
        hold_until_first(self.main.clone(), move || inner.list_with_offset(prefix.as_ref(), &offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _p = self.main.acquire().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _p = self.lane(true, to).acquire().await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct LimitedUpload {
    inner: Box<dyn MultipartUpload>,
    lane: Lane,
}

#[async_trait]
impl MultipartUpload for LimitedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // the part's request starts when its future is first polled
        let part = self.inner.put_part(data);
        let lane = self.lane.clone();
        Box::pin(async move {
            let _p = lane.acquire().await;
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let _p = self.lane.acquire().await;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _p = self.lane.acquire().await;
        self.inner.abort().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, PutMode};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Counts concurrent requests (to the response head; a GET's body until
    /// read) and their peak; each takes `delay`.
    #[derive(Debug)]
    pub(crate) struct Gauge {
        inner: Arc<dyn ObjectStore>,
        read_delay: Duration,
        write_delay: Duration,
        pub now: Arc<AtomicUsize>,
        pub peak: Arc<AtomicUsize>,
    }

    impl Gauge {
        pub(crate) fn new(inner: Arc<dyn ObjectStore>, delay: Duration) -> Arc<Gauge> {
            Self::with(inner, delay, delay)
        }

        pub(crate) fn with(inner: Arc<dyn ObjectStore>, read_delay: Duration, write_delay: Duration) -> Arc<Gauge> {
            Arc::new(Gauge { inner, read_delay, write_delay, now: Default::default(), peak: Default::default() })
        }

        async fn enter(&self, write: bool) -> impl Drop {
            struct G(Arc<AtomicUsize>);
            impl Drop for G {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            let g = G(self.now.clone());
            tokio::time::sleep(if write { self.write_delay } else { self.read_delay }).await;
            g
        }
    }

    impl std::fmt::Display for Gauge {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Gauge")
        }
    }

    #[async_trait]
    impl ObjectStore for Gauge {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> Result<PutResult> {
            let _g = self.enter(true).await;
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> Result<GetResult> {
            let _g = self.enter(false).await;
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(&self, l: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }
        fn list_with_offset(&self, p: Option<&Path>, o: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(p, o)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            let _g = self.enter(false).await;
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> Result<()> {
            let _g = self.enter(true).await;
            self.inner.copy_opts(f, t, o).await
        }
    }

    #[tokio::test]
    async fn bounds_concurrent_requests() {
        let gauge = Gauge::new(Arc::new(object_store::memory::InMemory::new()), Duration::from_millis(5));
        let s = limited(gauge.clone(), "lim", "test_bound", Limits::new(8));
        let p = Path::from("lim/state/001/x");
        s.put(&p, PutPayload::from_static(b"hello")).await.unwrap();
        futures::future::join_all((0..200).map(|_| async { s.get(&p).await.unwrap().bytes().await.unwrap() })).await;
        assert!(gauge.peak.load(Ordering::SeqCst) <= 8, "peak {}", gauge.peak.load(Ordering::SeqCst));
        let l = ["test_bound", "main"];
        assert_eq!(crate::metrics::OBJ_INFLIGHT.with_label_values(&l).get(), 0, "every permit returned");
        assert!(crate::metrics::OBJ_PERMIT_WAITS.with_label_values(&l).get() > 0);
        assert_eq!(crate::metrics::OBJ_INFLIGHT_LIMIT.with_label_values(&l).get(), 8);
    }

    #[tokio::test]
    async fn get_bodies_hold_their_permit_until_read() {
        let s = limited(Arc::new(object_store::memory::InMemory::new()), "lim", "test_body", Limits::new(1));
        let (a, b) = (Path::from("lim/state/001/a"), Path::from("lim/blob/did/cid"));
        s.put(&a, PutPayload::from_static(b"a")).await.unwrap();
        s.put(&b, PutPayload::from_static(b"b")).await.unwrap();
        let held = s.get(&a).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), s.get(&a)).await.is_err(), "the unread body holds the only permit");
        drop(held);
        // a blob body streams at its reader's pace: released at the head
        let blob = s.get(&b).await.unwrap();
        assert_eq!(s.get(&a).await.unwrap().bytes().await.unwrap().as_ref(), b"a");
        assert_eq!(blob.bytes().await.unwrap().as_ref(), b"b");
    }

    #[tokio::test]
    async fn listings_release_after_their_first_response() {
        let s = limited(Arc::new(object_store::memory::InMemory::new()), "lim", "test_list", Limits::new(1));
        for i in 0..3 {
            s.put(&Path::from(format!("lim/log/x/{i}")), PutPayload::from_static(b"x")).await.unwrap();
        }
        // a caller may GET each listed key while it walks the listing
        let mut list = s.list(Some(&Path::from("lim/log/x")));
        let mut n = 0;
        while let Some(m) = list.next().await {
            let m = m.unwrap();
            tokio::time::timeout(Duration::from_secs(5), s.get(&m.location)).await.expect("no deadlock").unwrap();
            n += 1;
        }
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn reserved_lane_is_never_starved() {
        // reads stall for an hour; writes answer at once
        let gauge = Gauge::with(Arc::new(object_store::memory::InMemory::new()), Duration::from_secs(3600), Duration::ZERO);
        let s = limited(gauge, "lim", "test_ctl", Limits::new(2).with_reserved(Reserve::LeaseWrites, 1));
        // the main lane is full, more requests queue behind it
        let stalled: Vec<_> = (0..4)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move { s.get(&Path::from(format!("lim/assign/{i}"))).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(crate::metrics::OBJ_INFLIGHT.with_label_values(&["test_ctl", "main"]).get(), 2);
        // a lease PUT still goes through; a PUT elsewhere waits in the main lane
        let (lease, assign) = (Path::from("lim/nodes/n1"), Path::from("lim/assign/9"));
        let renew = s.put_opts(&lease, PutPayload::from_static(b"{}"), PutMode::Overwrite.into());
        tokio::time::timeout(Duration::from_secs(5), renew).await.expect("renewal not starved").unwrap();
        let other = s.put_opts(&assign, PutPayload::from_static(b"{}"), PutMode::Overwrite.into());
        assert!(tokio::time::timeout(Duration::from_millis(50), other).await.is_err());
        for t in stalled {
            t.abort();
        }
    }
}
