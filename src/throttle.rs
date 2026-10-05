//! Store throttling (HTTP 429, and S3's 503 SlowDown). object_store retries
//! both inside its client, so the request counters above it (objstats.rs)
//! never see them: each answer is counted here, at the HTTP layer, by the
//! kind of key it was for.
//!
//! R2 takes about one write a second to one key and answers more with 429.
//! Leases, assignments and writer claims are CAS'd on fixed keys, by several
//! nodes at once during a takeover, so the control plane's writes back off
//! from a 1 s floor ([`ctl_write_retry`]). Segment keys are written once
//! and keep object_store's default schedule.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, RetryConfig,
};
use std::sync::Arc;
use std::time::Duration;

pub const KINDS: [&str; 3] = ["lease", "segment", "other"];

/// The first retry of a throttled control-plane write waits at least this
/// long (one write a second to a key).
pub const CTL_WRITE_FLOOR: Duration = Duration::from_secs(1);
/// No retry of a control-plane write waits longer.
pub const CTL_WRITE_CAP: Duration = Duration::from_secs(4);

/// `lease` for control-plane objects (node leases, assignments, writer
/// claims, the cluster version), `segment` for commit-log segments.
pub fn kind(component: &str) -> &'static str {
    match component {
        c if c.starts_with("ctl_") => "lease",
        "log_segment" => "segment",
        _ => "other",
    }
}

/// The retry schedule of the control plane's writes. object_store waits
/// exactly `init_backoff` before the first retry and then a uniform draw
/// from [init, 2 x the last wait], capped. `jitter` (in [0, 1)) moves this
/// client's floor up to half again, so nodes throttled on one key together
/// don't all resend a second later.
pub fn ctl_write_retry(jitter: f64) -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: CTL_WRITE_FLOOR.mul_f64(1.0 + jitter.clamp(0.0, 1.0) / 2.0),
            max_backoff: CTL_WRITE_CAP,
            base: 2.0,
        },
        ..RetryConfig::default()
    }
}

/// Wraps the reqwest connector to count throttled answers.
#[derive(Debug)]
pub struct Counting {
    /// The leading `/{bucket}/` of a path-style request.
    bucket: String,
    prefix: String,
}

impl Counting {
    pub fn new(bucket: &str, prefix: &str) -> Counting {
        Counting { bucket: format!("/{bucket}/"), prefix: prefix.trim_end_matches('/').to_string() }
    }
}

impl HttpConnector for Counting {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(CountingService { inner, bucket: self.bucket.clone(), prefix: self.prefix.clone() }))
    }
}

#[derive(Debug)]
struct CountingService {
    inner: HttpClient,
    bucket: String,
    prefix: String,
}

impl CountingService {
    fn kind_of(&self, uri_path: &str) -> &'static str {
        let key = uri_path.strip_prefix(self.bucket.as_str()).unwrap_or(uri_path);
        kind(crate::objstats::component(&self.prefix, key))
    }
}

pub fn throttled(status: u16) -> bool {
    status == 429 || status == 503
}

#[async_trait]
impl HttpService for CountingService {
    async fn call(&self, req: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let kind = self.kind_of(req.uri().path());
        let r = self.inner.execute(req).await?;
        if throttled(r.status().as_u16()) {
            crate::metrics::STORE_THROTTLED.with_label_values(&[kind]).inc();
        }
        Ok(r)
    }
}

/// Writes on one client, everything else on another: the control plane's
/// writes take [`ctl_write_retry`] and its reads keep the default schedule.
#[derive(Debug)]
pub struct SplitWrites {
    pub reads: Arc<dyn ObjectStore>,
    pub writes: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for SplitWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplitWrites({})", self.reads)
    }
}

#[async_trait]
impl ObjectStore for SplitWrites {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.writes.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.writes.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.reads.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.writes.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reads.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reads.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.reads.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.writes.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds() {
        for (path, want) in [
            ("/b/vlpds/nodes/a", "lease"),
            ("/b/vlpds/assign/0000000001", "lease"),
            ("/b/vlpds/writers/007", "lease"),
            ("/b/vlpds/cluster/version", "lease"),
            ("/b/vlpds/log/a.1/00000000000000000001.seg", "segment"),
            ("/b/vlpds/state/0000000001/manifest/1.manifest", "other"),
            ("/elsewhere/x", "other"),
        ] {
            let s = CountingService {
                inner: ReqwestConnector::default().connect(&ClientOptions::new()).unwrap(),
                bucket: "/b/".into(),
                prefix: "vlpds".into(),
            };
            assert_eq!(s.kind_of(path), want, "{path}");
        }
    }

    #[test]
    fn ctl_write_schedule_starts_at_the_floor_and_is_capped() {
        let r = ctl_write_retry(0.0);
        assert_eq!(r.backoff.init_backoff, CTL_WRITE_FLOOR);
        assert_eq!(r.backoff.max_backoff, CTL_WRITE_CAP);
        assert_eq!(r.backoff.base, 2.0);
        assert_eq!(ctl_write_retry(0.999_999).backoff.init_backoff.as_millis(), 1499);
        assert_eq!(ctl_write_retry(7.0).backoff.init_backoff, CTL_WRITE_FLOOR.mul_f64(1.5));
        // the rest is object_store's default (10 retries within 3 min)
        let d = RetryConfig::default();
        assert_eq!((r.max_retries, r.retry_timeout), (d.max_retries, d.retry_timeout));
    }

    /// Against a fake S3 that answers each key's first two PUTs 429: the
    /// control plane's retries wait at least the floor, a segment's keep
    /// object_store's 100 ms start, and every 429 is counted by kind.
    #[tokio::test]
    async fn throttled_writes_back_off_by_client_and_are_counted() {
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use object_store::{ObjectStoreExt, PutMode, UpdateVersion};
        use std::time::Instant;

        let hits = Arc::new(parking_lot::Mutex::new(Vec::<(String, Instant)>::new()));
        let h = hits.clone();
        let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
            let h = h.clone();
            async move {
                let key = format!("{} {}", req.method(), req.uri().path());
                let n = {
                    let mut v = h.lock();
                    v.push((key.clone(), Instant::now()));
                    v.iter().filter(|(k, _)| *k == key).count()
                };
                if n <= 2 {
                    (StatusCode::TOO_MANY_REQUESTS, "<Error><Code>TooManyRequests</Code></Error>").into_response()
                } else {
                    (StatusCode::OK, [("etag", "\"e1\"")], "").into_response()
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cfg = crate::store::S3Config {
            endpoint: format!("http://{addr}"),
            bucket: "b".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            region: "auto".into(),
        };
        let ctl = crate::store::Store::s3_ctl(&cfg, "vlpds", 4).unwrap();
        let log = crate::store::Store::s3(&cfg, "vlpds", None, 4).unwrap();
        let count = |k: &str| crate::metrics::STORE_THROTTLED.with_label_values(&[k]).get();
        let (lease0, seg0) = (count("lease"), count("segment"));

        let cas = PutOptions {
            mode: PutMode::Update(UpdateVersion { e_tag: Some("\"e0\"".into()), version: None }),
            ..Default::default()
        };
        ctl.raw.put_opts(&Path::from("vlpds/nodes/a"), PutPayload::from_static(b"{}"), cas).await.unwrap();
        log.raw
            .put(&Path::from("vlpds/log/a.1/00000000000000000001.seg"), PutPayload::from_static(b"x"))
            .await
            .unwrap();

        let sent =
            |key: &str| -> Vec<Instant> { hits.lock().iter().filter(|(k, _)| k == key).map(|&(_, t)| t).collect() };
        let lease = sent("PUT /b/vlpds/nodes/a");
        let seg = sent("PUT /b/vlpds/log/a.1/00000000000000000001.seg");
        assert_eq!((lease.len(), seg.len()), (3, 3));
        for w in lease.windows(2) {
            assert!(w[1] - w[0] >= CTL_WRITE_FLOOR, "lease retry {:?} after the last", w[1] - w[0]);
            assert!(w[1] - w[0] < CTL_WRITE_CAP + Duration::from_millis(500), "lease retry {:?}", w[1] - w[0]);
        }
        assert!(seg[2] - seg[0] < CTL_WRITE_FLOOR, "segment retries keep the default: {:?}", seg[2] - seg[0]);
        assert_eq!((count("lease") - lease0, count("segment") - seg0), (2, 2));
    }
}
