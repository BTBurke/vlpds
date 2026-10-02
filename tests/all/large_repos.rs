//! Log and repo-cache storage (TODO "Storage measured on real data"):
//! segments are stored zstd-compressed and every reader decodes them, and
//! large repos are pinned in the repo cache, indexed under `L/` and
//! preloaded when their shard opens on another node incarnation.

use crate::common::*;
use object_store::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::worker::{CachedRepo, WorkerMsg};

const SHARDS: u16 = 4;

async fn node(id: &str, store: &Arc<dyn object_store::ObjectStore>, pin: u64) -> TestServer {
    let (id, store) = (id.to_string(), store.clone());
    TestServer::spawn_with(move |c| {
        c.memory_store = Some(store);
        c.shards = SHARDS;
        c.pin_repo_records = pin;
        // only the large-repo preload (recent repos: tests/all/cold_start.rs)
        c.preload_recent = 0;
        c.cluster = Some(vlpds::cluster::ClusterConfig {
            node_id: id,
            addr: c.public_url.clone(),
            shards: SHARDS,
            ttl: Duration::from_millis(1500),
            renew_every: Duration::from_millis(100),
            skew: Duration::from_millis(300),
            ..Default::default()
        });
    })
    .await
}

async fn cache_info(s: &TestServer, did: &str) -> Option<CachedRepo> {
    let (reply, rx) = tokio::sync::oneshot::channel();
    s.app.workers.route(did).send(WorkerMsg::CacheInfo { did: did.into(), reply }).unwrap();
    rx.await.unwrap()
}

/// The repo's `L/` index value (its record count when it was marked), from its shard.
async fn large_key(s: &TestServer, did: &str) -> Option<u64> {
    let p = s.app.partitions.get(vlpds::state::partition_of(did, SHARDS) as usize).expect("shard owned");
    let v = p.db.get(vlpds::state::large_repo_key(did)).await.unwrap()?;
    Some(u64::from_be_bytes(v[..8].try_into().unwrap()))
}

async fn until<T>(what: &str, mut f: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_n(s: &TestServer, a: &TestAccount, from: usize, n: usize) {
    let writes: Vec<J> = (from..from + n)
        .map(|i| json!({"$type": "com.atproto.repo.applyWrites#create", "collection": "com.example.thing", "rkey": format!("p{i:05}"), "value": {"$type": "com.example.thing", "n": i}}))
        .collect();
    s.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": a.did, "writes": writes}), &a.auth()).await.ok();
}

/// A repo past the pin threshold is pinned and indexed; the next owner of
/// its shard (a restarted node) loads it before any request asks; once it
/// shrinks below half the threshold it is unpinned and unindexed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_repos_pinned_and_preloaded() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = node("lr", &store, 30).await;
    let big = a.create_account("lrbig").await;
    let small = a.create_account("lrsmall").await;
    create_n(&a, &big, 0, 40).await;
    create_n(&a, &small, 0, 5).await;
    let info = cache_info(&a, &big.did).await.expect("cached");
    assert!(info.large && info.records == 40, "{info:?}");
    assert!(info.charge >= 40 * vlpds::worker::MST_BYTES_PER_RECORD);
    assert!(!cache_info(&a, &small.did).await.expect("cached").large);
    assert_eq!(until("L/ key", async || large_key(&a, &big.did).await).await, 40);
    assert_eq!(large_key(&a, &small.did).await, None);
    a.app.log.checkpoint_all().await;
    vlpds::server::shutdown(&a.app).await;

    // the same node id restarts: it reclaims every shard and preloads the
    // big repo with no request for it
    let b = node("lr", &store, 30).await;
    let info = until("preload", async || cache_info(&b, &big.did).await).await;
    assert!(info.large && info.records == 40, "{info:?}");
    assert!(cache_info(&b, &small.did).await.is_none(), "small repos aren't preloaded");

    // below half the threshold: unpinned, index key deleted
    let deletes: Vec<J> = (0..26).map(|i| json!({"$type": "com.atproto.repo.applyWrites#delete", "collection": "com.example.thing", "rkey": format!("p{i:05}")})).collect();
    b.xrpc.post("com.atproto.repo.applyWrites", &json!({"repo": big.did, "writes": deletes}), &big.auth()).await.ok();
    let info = cache_info(&b, &big.did).await.expect("cached");
    assert!(!info.large && info.records == 14, "{info:?}");
    until("L/ key deleted", async || large_key(&b, &big.did).await.is_none().then_some(())).await;
}

/// Segments are PUT with a zstd body behind an uncompressed header, and
/// decode to the bytes the writer sealed: a cursor-0 backfill (S3 reads)
/// returns every commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segments_stored_compressed() {
    use futures::StreamExt;
    use object_store::ObjectStoreExt;
    let s = TestServer::spawn().await;
    let a = s.create_account("zseg").await;
    let mut last = None;
    for i in 0..20 {
        last = Some(s.post(&a, &format!("a post of a typical length, number {i}: {}", "lorem ipsum ".repeat(8))).await);
    }
    let store = &s.app.store;
    let prefix = Path::from(format!("{}/log/{}", store.prefix, s.app.log.log_id));
    let metas: Vec<_> = store.raw.list(Some(&prefix)).map(|m| m.unwrap()).collect().await;
    let (mut zstd, mut stored, mut raw) = (0, 0u64, 0u64);
    for m in &metas {
        let data = store.raw.get(&m.location).await.unwrap().bytes().await.unwrap();
        let Some((h, hl)) = vlpds::segment::parse_header(&data).unwrap() else { continue };
        if h.codec == vlpds::segment::CODEC_ZSTD {
            zstd += 1;
        }
        stored += data.len() as u64;
        raw += (hl + h.body_len as usize) as u64;
        let decoded = vlpds::segment::decode(data).unwrap();
        assert_eq!(decoded.len(), hl + h.body_len as usize);
        assert!(matches!(vlpds::segment::parse(decoded, true, None).unwrap(), vlpds::segment::LogObject::Segment(..)));
    }
    assert!(zstd > 0 && stored < raw, "{zstd} compressed segments, {stored} B stored for {raw} B");
    let head = Cid::parse(last.unwrap().commit_cid.as_deref().unwrap()).unwrap();
    let mut sub = s.subscribe(Some(0)).await;
    let frames = sub.until(Duration::from_secs(10), |fs| fs.last().and_then(|f| f.commit()).is_some_and(|c| c.commit == head)).await;
    assert_eq!(frames.iter().filter(|f| f.did() == Some(a.did.as_str()) && f.kind() == "#commit").count(), 20);
}
