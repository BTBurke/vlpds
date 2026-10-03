//! Object-store pressure during a takeover: the survivor opens the dead
//! node's shards and then gets a cold repo load for every write to them.
//! Unbounded, each request opens its own connection, the host runs out of
//! ephemeral ports, and lease renewals fail with everything else. Here the store emulates S3 latency and a
//! host's port budget (requests past it fail like a refused connect); the
//! survivor must keep its requests in flight bounded (`objlimit`), never
//! hit the budget, keep its lease, and answer every write.

use crate::common::*;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 8;
/// `--store-inflight` of each node.
const STATE_INFLIGHT: usize = 8;
/// Requests in flight (both nodes) past which the store refuses more.
const PORT_BUDGET: usize = 160;
const LATENCY: Duration = Duration::from_millis(15);

/// Counts requests in flight at the bottom of the stack (all of state's
/// and the total), adds latency and enforces the port budget once armed.
#[derive(Debug, Default)]
struct Pressure {
    inner: object_store::memory::InMemory,
    armed: AtomicBool,
    total: AtomicUsize,
    total_peak: AtomicUsize,
    state: AtomicUsize,
    state_peak: AtomicUsize,
    refused: AtomicUsize,
}

struct InFlight<'a>(&'a Pressure, bool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.total.fetch_sub(1, Ordering::SeqCst);
        if self.1 {
            self.0.state.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Pressure {
    async fn enter(&self, p: &Path) -> object_store::Result<Option<InFlight<'_>>> {
        if !self.armed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let state = p.as_ref().split('/').nth(1) == Some("state");
        let n = self.total.fetch_add(1, Ordering::SeqCst) + 1;
        if state {
            let s = self.state.fetch_add(1, Ordering::SeqCst) + 1;
            self.state_peak.fetch_max(s, Ordering::SeqCst);
        }
        let g = InFlight(self, state);
        self.total_peak.fetch_max(n, Ordering::SeqCst);
        if n > PORT_BUDGET {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic { store: "pressure", source: "transport error of kind Connect: no ephemeral port".into() });
        }
        tokio::time::sleep(LATENCY).await;
        Ok(Some(g))
    }
}

impl std::fmt::Display for Pressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pressure")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Pressure {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        let _g = self.enter(location).await?;
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let _g = self.enter(location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let _g = self.enter(location).await?;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, l: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let _g = match prefix {
            Some(p) => self.enter(p).await?,
            None => None,
        };
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        let _g = self.enter(to).await?;
        self.inner.copy_opts(from, to, options).await
    }
}

async fn node(id: &str, store: &Arc<Pressure>) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.store_inflight = STATE_INFLIGHT;
        c.log_store_inflight = 16;
        c.checkpoint_every = Duration::from_millis(200);
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: peer_url(c),
            shards: SHARDS,
            ttl: Duration::from_secs(3),
            renew_every: Duration::from_millis(200),
            skew: Duration::from_millis(600),
            ..Default::default()
        });
    })
    .await
}

async fn wait_for(what: &str, deadline: Duration, f: impl Fn() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < deadline, "{what}: not within {deadline:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `s.create_account`, retried while password hashing sheds load (503
/// Overloaded: the Argon2 permits are process-wide, shared with every test
/// running at the time). // candidate for common
async fn create_account_retrying(s: &TestServer, prefix: &str) -> TestAccount {
    let t = Instant::now();
    loop {
        let handle = format!("{}.{HANDLE_DOMAIN}", unique_name(prefix));
        let email = format!("{}@example.com", handle.replace('.', "-"));
        let r = s.xrpc.post("com.atproto.server.createAccount", &json!({"handle": handle, "password": PASSWORD, "email": email}), &Auth::None).await;
        if r.status == 503 && t.elapsed() < Duration::from_secs(60) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let j = r.ok();
        return TestAccount {
            did: j["did"].as_str().unwrap().into(),
            handle,
            password: PASSWORD.into(),
            email,
            access: j["accessJwt"].as_str().unwrap().into(),
            refresh: j["refreshJwt"].as_str().unwrap().into(),
        };
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn takeover_cold_loads_stay_bounded_and_the_lease_renews() {
    let store = Arc::new(Pressure::default());
    let a = node("a", &store).await;
    let b = node("b", &store).await;
    let owned = |s: &TestServer| s.app.partitions.owned().len();
    wait_for("b gets its share", Duration::from_secs(10), || owned(&b) > 0 && owned(&a) + owned(&b) == SHARDS as usize).await;

    // accounts (a new account's DID lands on a shard of the node creating it)
    use futures::StreamExt;
    let accounts: Vec<TestAccount> = futures::stream::iter(0..400).map(|_| create_account_retrying(&b, "press")).buffer_unordered(32).collect().await;
    let cold: Vec<&TestAccount> = accounts.iter().filter(|x| b.app.partitions.for_key(&x.did).is_some()).collect();
    assert!(cold.len() > 100, "b holds {} of the {} accounts (owns {} shards, a {})", cold.len(), accounts.len(), owned(&b), owned(&a));
    // checkpoints flush b's writes to SSTs: a's loads read them from the store
    tokio::time::sleep(Duration::from_secs(1)).await;
    // and nothing b's loads cached in the process-wide block cache helps a
    vlpds::partition::bump_cache_epoch();

    let ca = a.app.cluster.clone().unwrap();
    store.armed.store(true, Ordering::Release);
    b.app.node.halt();
    wait_for("a takes b's shards over", Duration::from_secs(15), || owned(&a) == SHARDS as usize).await;
    // every cold repo written at once, right after the takeover
    let t = Instant::now();
    let results = futures::future::join_all(cold.iter().map(|x| a.create_record(x, "app.bsky.feed.post", post_record("after the takeover")))).await;
    let took = t.elapsed();
    assert_eq!(results.len(), cold.len());
    let (state_peak, total_peak, refused) = (store.state_peak.load(Ordering::SeqCst), store.total_peak.load(Ordering::SeqCst), store.refused.load(Ordering::SeqCst));
    eprintln!("{} cold writes in {took:?}: state requests in flight peak {state_peak}, total {total_peak}, refused {refused}", cold.len());
    assert!(state_peak <= 2 * STATE_INFLIGHT, "state requests in flight peaked at {state_peak} (bound {STATE_INFLIGHT} per node)");
    assert_eq!(refused, 0, "requests past the port budget ({PORT_BUDGET}); total in flight peaked at {total_peak}");
    assert!(ca.lease_valid() && !ca.halted(), "a kept its lease");
    // the renew loop kept going: validity ends TTL - skew (2.4 s) after a
    // renewal's send, so over 1 s left = renewed within the last 1.4 s
    wait_for("a renews its lease", Duration::from_secs(3), || ca.lease_valid() && ca.lease_validity_secs() > 1.0).await;
    assert_eq!(owned(&a), SHARDS as usize);
}
