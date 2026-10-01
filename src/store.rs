//! Object store handle plus optional injected latency on our own segment PUTs,
//! to emulate S3 Standard/Express against a local MinIO.

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct Store {
    pub raw: Arc<dyn ObjectStore>,
    pub prefix: String,
    /// (median ms, lognormal sigma)
    pub latency: Option<(f64, f64)>,
}

#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl Store {
    pub fn s3(cfg: &S3Config, prefix: &str, latency: Option<(f64, f64)>) -> anyhow::Result<Store> {
        let s3 = AmazonS3Builder::new()
            .with_endpoint(&cfg.endpoint)
            .with_bucket_name(&cfg.bucket)
            .with_access_key_id(&cfg.access_key)
            .with_secret_access_key(&cfg.secret_key)
            .with_region(&cfg.region)
            .with_virtual_hosted_style_request(false)
            .with_client_options(
                // HTTP/1.1 pool (object_store's default, stated: h2 to S3 is
                // slower and S3 caps streams per connection). S3 closes idle
                // connections after ~20 s; dropping ours at 15 s avoids
                // reusing one the server is closing (a reset on the next
                // request). TCP keepalive isn't exposed by ClientOptions; a
                // busy pool doesn't need it and an idle one is closed at 15 s.
                object_store::ClientOptions::new()
                    .with_http1_only()
                    .with_pool_max_idle_per_host(256)
                    .with_pool_idle_timeout(Duration::from_secs(15))
                    .with_connect_timeout(Duration::from_secs(2))
                    .with_timeout(Duration::from_secs(30))
                    // must be set after with_client_options would overwrite it
                    .with_allow_http(cfg.endpoint.starts_with("http://")),
            )
            .build()?;
        Ok(Store {
            raw: Arc::new(s3),
            prefix: prefix.trim_end_matches('/').to_string(),
            latency,
        })
    }

    pub fn memory(latency: Option<(f64, f64)>) -> Store {
        Store {
            raw: Arc::new(object_store::memory::InMemory::new()),
            prefix: "vlpds".into(),
            latency,
        }
    }

    /// Counts this handle's requests (`objstats`) under `client`. Wrap each
    /// underlying client once.
    pub fn counted(self, client: &'static str) -> Store {
        Store { raw: crate::objstats::counted(self.raw, &self.prefix, client), ..self }
    }

    pub async fn inject_latency(&self) {
        if let Some((median, sigma)) = self.latency {
            // Box-Muller standard normal -> lognormal around the median
            let u1: f64 = rand::random::<f64>().max(1e-12);
            let u2: f64 = rand::random();
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let ms = median * (sigma * z).exp();
            tokio::time::sleep(Duration::from_secs_f64(ms / 1000.0)).await;
        }
    }
}
