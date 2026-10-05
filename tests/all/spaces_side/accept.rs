//! The brief's sync acceptance criteria that hold without timing:
//!
//! - `noop_polls_read_nothing`: a listRepoOps poll at the head answers from
//!   memory. A run of polls loads no space head, fills neither the account
//!   nor the takedown-set cache from SlateDB, and sends no bucket request.
//! - `space_writes_add_no_bucket_puts`: a space write rides its node-log
//!   segment, outbox and all. Sequential writes into a self-governed space
//!   and a remote authority's space PUT at most one segment each and nothing
//!   outside the log and the shard DBs, and the shard DBs get far fewer PUTs
//!   than writes (a flush or checkpoint, never one per write).

use super::hooks::StubDid;
use crate::common::spaces::{space_uri, SpaceClient};
use crate::common::*;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Bucket requests by kind: the node log, the shard DBs, everything else.
#[derive(Debug, Default)]
struct Counts {
    log_puts: AtomicU64,
    state_puts: AtomicU64,
    other_puts: AtomicU64,
    gets: AtomicU64,
}

#[derive(Debug)]
struct Counting {
    inner: Arc<dyn object_store::ObjectStore>,
    n: Arc<Counts>,
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting")
    }
}

impl Counting {
    fn put(&self, p: &Path) {
        let parts: Vec<&str> = p.as_ref().split('/').collect();
        let c = if parts.contains(&"log") {
            &self.n.log_puts
        } else if parts.contains(&"state") {
            &self.n.state_puts
        } else {
            &self.n.other_puts
        };
        c.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put(location);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put(location);
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.n.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.n.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.n.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.put(to);
        self.inner.copy_opts(from, to, options).await
    }
}

async fn counted_node() -> (TestServer, Arc<Counts>) {
    let n = Arc::new(Counts::default());
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(Counting { inner: Arc::new(object_store::memory::InMemory::new()), n: n.clone() });
    let s = TestServer::spawn_with(move |c| {
        c.spaces = true;
        c.memory_store = Some(store);
    })
    .await;
    (s, n)
}

fn names() -> (String, String) {
    let t: String = random_bytes(5).iter().map(|b| format!("{b:02x}")).collect();
    (format!("com.example.acc{t}.space"), format!("com.example.acc{t}.note"))
}

fn scope(space_type: &str, collection: &str) -> String {
    format!(
        "space:{space_type}?authority=*&collection={collection}&action=read&action=create&action=update&action=delete&manage=create"
    )
}

fn note(collection: &str, i: usize) -> J {
    json!({"$type": collection, "text": format!("note {i}"), "createdAt": now_iso()})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noop_polls_read_nothing() {
    let (s, n) = counted_node().await;
    let (st, coll) = names();
    let a = SpaceClient::new(&s, &unique_name("acn"), &scope(&st, &coll)).await;
    let space = a.create_space(&st, "noop").await;
    for i in 0..3 {
        a.create_record(&space, &coll, None, note(&coll, i)).await.ok();
    }
    let cred = a.credential(&space).await;
    let rq = [("space", space.as_str()), ("repo", a.did.as_str())];
    let head = a.signed_get(&s.url, "com.atproto.space.listRepoOps", &rq, &cred, &a.did).await.ok()["commit"]["rev"]
        .as_str()
        .unwrap()
        .to_string();
    let since = [("space", space.as_str()), ("repo", a.did.as_str()), ("since", head.as_str())];
    // the first poll may fill the caches
    a.signed_get(&s.url, "com.atproto.space.listRepoOps", &since, &cred, &a.did).await.ok();

    let sp = s.app.spaces.as_ref().unwrap();
    let loads = || sp.heads.loads.load(Ordering::Relaxed);
    let fills = || sp.cache_fills.load(Ordering::Relaxed);
    let gets = || n.gets.load(Ordering::Relaxed);
    let (l0, f0, g0) = (loads(), fills(), gets());
    for _ in 0..100 {
        let r = a.signed_get(&s.url, "com.atproto.space.listRepoOps", &since, &cred, &a.did).await.ok();
        assert_eq!(r["ops"], json!([]));
        assert_eq!(r["commit"]["rev"], json!(head));
    }
    assert_eq!(loads(), l0, "a poll at the head loaded the space head from SlateDB");
    assert_eq!(fills(), f0, "a poll at the head read the account or its takedowns from SlateDB");
    assert_eq!(gets(), g0, "a poll at the head read the bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn space_writes_add_no_bucket_puts() {
    let (s, n) = counted_node().await;
    let (st, coll) = names();
    let a = SpaceClient::new(&s, &unique_name("acp"), &scope(&st, &coll)).await;
    let own = a.create_space(&st, "own").await;
    let authority = StubDid::spawn().await;
    let remote = space_uri(&authority.did, &st, "remote");
    // first writes of each space, and the first notify, off the books
    a.create_record(&own, &coll, None, note(&coll, 0)).await.ok();
    a.create_record(&remote, &coll, None, note(&coll, 0)).await.ok();
    wait_until("the first notify", Duration::from_secs(10), || !authority.accepted().is_empty()).await;

    const N: u64 = 40;
    let puts = || {
        (n.log_puts.load(Ordering::Relaxed), n.state_puts.load(Ordering::Relaxed), n.other_puts.load(Ordering::Relaxed))
    };
    let (log0, state0, other0) = puts();
    for i in 0..N as usize {
        let space = if i % 2 == 0 { &own } else { &remote };
        a.create_record(space, &coll, None, note(&coll, i + 1)).await.ok();
    }
    let head = a.get("com.atproto.space.getLatestCommit", &[("space", &remote), ("repo", &a.did)]).await;
    let last = head.ok()["commit"]["rev"].as_str().unwrap().to_string();
    // the outbox has caught up: its sends cost no PUT either
    wait_until("the newest notify", Duration::from_secs(10), || {
        authority.accepted().last().is_some_and(|x| x.body["repoRev"].as_str() == Some(&last))
    })
    .await;
    let (log1, state1, other1) = puts();
    let (log, state, other) = (log1 - log0, state1 - state0, other1 - other0);
    eprintln!("{N} space writes: {log} log PUTs, {state} shard DB PUTs, {other} other PUTs");
    assert!(log <= N, "{log} log segment PUTs for {N} sequential space writes");
    assert_eq!(other, 0, "space writes PUT outside the log and the shard DBs");
    assert!(state < N / 4, "{state} shard DB PUTs for {N} space writes: one per write?");
}
