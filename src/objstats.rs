//! Counts object-store requests by billable operation and by what the key
//! is (log segment, SlateDB manifest/SST, control plane, ...), at the
//! bottom of the stack: every request that reaches the wire is counted
//! once (SlateDB's retries and hedged segment PUTs included; SlateDB's
//! disk-cache hits never get here). This is what an S3/GCS/R2 bill counts.
//!
//! Exported as `vlpds_object_store_requests_total{op,component,client}`
//! and `vlpds_object_store_bytes_total{dir,component,client}` (dir = up |
//! down). `client` is the connection pool (`log`: segment PUTs, fences,
//! retention, replay and firehose reads; `state`: SlateDB, control plane,
//! blobs). Who inside
//! SlateDB issued a request (db / gc / compactor) is in SlateDB's own
//! `slatedb_object_store_request_count_total{component}`.
//!
//! Ops: `put` (overwrite), `put_create` (If-None-Match), `put_cas`
//! (If-Match), `get`, `get_range`, `head`, `list` (one per 1,000-key
//! page), `delete` (objects), `delete_batch` (bulk-delete requests, one
//! per 1,000 objects of a stream), `copy`, `mpu_create`, `mpu_part`,
//! `mpu_complete`, `mpu_abort`.
//!
//! `VLPDS_INJECT_STATE_MS` / `VLPDS_INJECT_LOG_MS` (bench only) add S3-like
//! latency per request of that client; see `Latency`.

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use std::sync::Arc;

/// What an object key is, from its path under the store prefix.
pub fn component(prefix: &str, path: &str) -> &'static str {
    let rel = path.strip_prefix(prefix).and_then(|r| r.strip_prefix('/')).unwrap_or(path);
    let mut it = rel.split('/');
    match it.next().unwrap_or("") {
        "log" => "log_segment",
        "retain" => "retention_report",
        "nodes" => "ctl_lease",
        "assign" => "ctl_assign",
        "writers" => "ctl_writer",
        "handle" | "email" => "account_index",
        "blob" | "blob-gc" | "blob-tmp" => "blob",
        "state" => {
            let _shard = it.next();
            match it.next().unwrap_or("") {
                "manifest" => "state_manifest",
                "compacted" => "state_sst",
                "wal" => "state_wal",
                "compactions" => "state_compactions",
                // SlateDB's GC boundary files (`gc/manifest.boundary`, ...):
                // read on every latest-manifest/compactions read
                "gc" => "state_gc_boundary",
                _ => "state_other",
            }
        }
        _ => "other",
    }
}

#[derive(Debug)]
pub struct Counting {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    client: &'static str,
    latency: Option<Latency>,
}

/// Bench-only injected latency for every request of a client (lognormal
/// around a median; reads = get/head/list_with_delimiter, writes =
/// put/copy/multipart create; streamed LISTs and DELETEs are not delayed).
/// Segment PUTs have their own (`--inject-put-ms`); this emulates S3 for
/// SlateDB and the control plane over a local MinIO, where call latency
/// paces the timer-driven loops (checkpoints, compaction) and so the op
/// counts. Set by `VLPDS_INJECT_STATE_MS=<read ms>,<write ms>[,<sigma>]`.
#[derive(Debug, Clone, Copy)]
struct Latency {
    read_ms: f64,
    write_ms: f64,
    sigma: f64,
}

impl Latency {
    fn from_env(client: &str) -> Option<Latency> {
        let v = std::env::var(format!("VLPDS_INJECT_{}_MS", client.to_ascii_uppercase())).ok()?;
        let f: Vec<f64> = v.split(',').filter_map(|x| x.trim().parse().ok()).collect();
        (f.len() >= 2).then(|| Latency { read_ms: f[0], write_ms: f[1], sigma: f.get(2).copied().unwrap_or(0.5) })
    }
}

async fn sleep_lognormal(median_ms: f64, sigma: f64) {
    if median_ms <= 0.0 {
        return;
    }
    let u1: f64 = rand::random::<f64>().max(1e-12);
    let u2: f64 = rand::random();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    tokio::time::sleep(std::time::Duration::from_secs_f64(median_ms * (sigma * z).exp() / 1000.0)).await;
}

/// Wraps `inner` so every request is counted under `client`.
pub fn counted(inner: Arc<dyn ObjectStore>, prefix: &str, client: &'static str) -> Arc<dyn ObjectStore> {
    Arc::new(Counting { inner, prefix: prefix.trim_end_matches('/').to_string(), client, latency: Latency::from_env(client) })
}

fn req(op: &str, comp: &str, client: &str) {
    crate::metrics::OBJ_REQUESTS.with_label_values(&[op, comp, client]).inc();
}

fn bytes(dir: &str, comp: &str, client: &str, n: u64) {
    if n > 0 {
        crate::metrics::OBJ_BYTES.with_label_values(&[dir, comp, client]).inc_by(n);
    }
}

impl Counting {
    async fn read_delay(&self) {
        if let Some(l) = self.latency {
            sleep_lognormal(l.read_ms, l.sigma).await;
        }
    }

    async fn write_delay(&self) {
        if let Some(l) = self.latency {
            sleep_lognormal(l.write_ms, l.sigma).await;
        }
    }

    fn comp(&self, p: &Path) -> &'static str {
        component(&self.prefix, p.as_ref())
    }

    /// Counts a listing: one request up front, one more per 1,000 keys
    /// (S3/GCS/R2 page size).
    fn count_list(&self, comp: &'static str, s: BoxStream<'static, Result<ObjectMeta>>) -> BoxStream<'static, Result<ObjectMeta>> {
        let client = self.client;
        req("list", comp, client);
        let mut n = 0u64;
        s.inspect(move |_| {
            n += 1;
            if n.is_multiple_of(1000) {
                req("list", comp, client);
            }
        })
        .boxed()
    }
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Counting {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let comp = self.comp(location);
        let op = match opts.mode {
            PutMode::Overwrite => "put",
            PutMode::Create => "put_create",
            PutMode::Update(_) => "put_cas",
        };
        req(op, comp, self.client);
        bytes("up", comp, self.client, payload.content_length() as u64);
        self.write_delay().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let comp = self.comp(location);
        req("mpu_create", comp, self.client);
        self.write_delay().await;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload { inner, comp, client: self.client }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let comp = self.comp(location);
        let op = if options.head {
            "head"
        } else if options.range.is_some() {
            "get_range"
        } else {
            "get"
        };
        req(op, comp, self.client);
        let head = options.head;
        self.read_delay().await;
        let r = self.inner.get_opts(location, options).await?;
        if !head {
            bytes("down", comp, self.client, r.range.end - r.range.start);
        }
        Ok(r)
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let (prefix, client) = (self.prefix.clone(), self.client);
        let mut n = 0u64;
        let locations = locations
            .inspect(move |p| {
                if let Ok(p) = p {
                    let comp = component(&prefix, p.as_ref());
                    if n.is_multiple_of(1000) {
                        req("delete_batch", comp, client);
                    }
                    n += 1;
                    req("delete", comp, client);
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let comp = prefix.map_or("other", |p| self.comp(p));
        self.count_list(comp, self.inner.list(prefix))
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        let comp = prefix.map_or("other", |p| self.comp(p));
        self.count_list(comp, self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let comp = prefix.map_or("other", |p| self.comp(p));
        req("list", comp, self.client);
        self.read_delay().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        req("copy", self.comp(to), self.client);
        self.write_delay().await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    comp: &'static str,
    client: &'static str,
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        req("mpu_part", self.comp, self.client);
        bytes("up", self.comp, self.client, data.content_length() as u64);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        req("mpu_complete", self.comp, self.client);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        req("mpu_abort", self.comp, self.client);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[test]
    fn components() {
        assert_eq!(component("vlpds", "vlpds/log/ab/000000000001.seg"), "log_segment");
        assert_eq!(component("vlpds", "vlpds/state/003/manifest/00000000000000000001.manifest"), "state_manifest");
        assert_eq!(component("vlpds", "vlpds/state/003/compacted/01J.sst"), "state_sst");
        assert_eq!(component("a/b", "a/b/assign/007"), "ctl_assign");
        assert_eq!(component("vlpds", "vlpds/nodes/n1"), "ctl_lease");
        assert_eq!(component("vlpds", "vlpds/blob/did/cid"), "blob");
        assert_eq!(component("vlpds", "vlpds/state/005/gc/manifest.boundary"), "state_gc_boundary");
    }

    #[tokio::test]
    async fn counts_ops() {
        let s = counted(Arc::new(object_store::memory::InMemory::new()), "objstats-test", "state");
        let p = Path::from("objstats-test/retain/x");
        let n = |op: &str| crate::metrics::OBJ_REQUESTS.with_label_values(&[op, "retention_report", "state"]).get();
        let (put0, get0, list0, del0) = (n("put_create"), n("get_range"), n("list"), n("delete"));
        s.put_opts(&p, PutPayload::from_static(b"hello"), PutMode::Create.into()).await.unwrap();
        s.get_range(&p, 0..2).await.unwrap();
        let _: Vec<_> = s.list(Some(&Path::from("objstats-test/retain"))).collect().await;
        s.delete(&p).await.unwrap();
        assert_eq!(n("put_create") - put0, 1);
        assert_eq!(n("get_range") - get0, 1);
        assert_eq!(n("list") - list0, 1);
        assert_eq!(n("delete") - del0, 1);
    }
}
