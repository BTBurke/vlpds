//! A store slow or failing at boot (an R2 cluster start: one control-plane
//! GET took over 3 s against a 3 s lease TTL and the node exited): startup
//! retries, and the node comes up and serves.

use crate::common::*;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: u32 = 4;
/// Over the call deadline of the TTL `cluster_node` sets (1.5 s).
const STALL: Duration = Duration::from_secs(4);

/// The next `n` requests of `op` to a key containing a substring fail
/// ("getfail") or stall for STALL first ("list").
#[derive(Debug, Default)]
struct Boot {
    inner: object_store::memory::InMemory,
    armed: Mutex<Vec<(&'static str, &'static str)>>,
    hit: Mutex<Vec<(&'static str, Instant)>>,
}

impl Boot {
    fn take(&self, op: &'static str, path: &str) -> bool {
        let mut a = self.armed.lock();
        let Some(i) = a.iter().position(|(o, p)| *o == op && path.contains(p)) else { return false };
        a.remove(i);
        self.hit.lock().push((op, Instant::now()));
        true
    }
}

impl std::fmt::Display for Boot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Boot")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Boot {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.take("getfail", location.as_ref()) {
            return Err(object_store::Error::Generic { store: "Boot", source: "503 Service Unavailable".into() });
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        l: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(l)
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::StreamExt;
        let inner = self.inner.list(prefix);
        if self.take("list", prefix.map_or("", |p| p.as_ref())) {
            return futures::stream::once(async move {
                tokio::time::sleep(STALL).await;
                inner
            })
            .flatten()
            .boxed();
        }
        inner
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_or_failing_store_at_boot_is_retried() {
    let store = Arc::new(Boot::default());
    store.armed.lock().extend([
        ("getfail", "cluster/version"),
        ("getfail", "nodes/a"),
        ("list", "vlpds/nodes"),
        ("list", "vlpds/nodes"),
    ]);
    let t = Instant::now();
    let n = cluster_node("a", store.clone(), SHARDS, |_| {}).await;
    let took = t.elapsed();
    assert!(store.armed.lock().is_empty(), "left armed: {:?}", store.armed.lock());
    assert_eq!(store.hit.lock().len(), 4);
    assert!(took >= Duration::from_millis(1500) * 2, "both LISTs waited out the call deadline: {took:?}");
    assert_eq!(owned(&n), SHARDS as usize);
    assert!(cluster(&n).lease_valid());
    let acct = n.create_account("boot").await;
    n.create_record(&acct, "app.bsky.feed.post", post_record("after a slow boot")).await;
}
