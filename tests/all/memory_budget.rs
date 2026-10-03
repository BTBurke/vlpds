//! The memory budget (src/memory.rs): a node refuses explicit cache sizes
//! over its budget, and a running node exports its plan and the pool
//! caches' sizes. The plan and the SST caches are process-wide (the first
//! node built here sets them), so only shapes are checked.

use crate::common::*;

fn gauge(metrics: &str, series: &str) -> Option<f64> {
    metrics.lines().find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.trim().parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversize_caches_refuse_to_start() {
    let mut cfg = vlpds::server::Config::default();
    let limit = vlpds::memory::limit_bytes().expect("memory limit on linux/macos");
    cfg.memory.block = Some(limit);
    let e = vlpds::server::build(cfg).await.err().expect("a block cache the size of RAM is refused");
    assert!(format!("{e:#}").contains("needed but the budget is"), "{e:#}");

    let mut cfg = vlpds::server::Config::default();
    cfg.memory.budget = Some(vlpds::memory::BudgetSpec::Bytes(limit * 2));
    let e = vlpds::server::build(cfg).await.err().expect("a budget over the limit is refused");
    assert!(format!("{e:#}").contains("over this node's memory limit"), "{e:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_and_pool_caches_are_exported() {
    let s = TestServer::spawn().await;
    s.create_account("membudget").await;
    let m = reqwest::get(format!("{}/metrics", s.url)).await.unwrap().text().await.unwrap();
    let part = |p: &str| gauge(&m, &format!("vlpds_memory_budget_bytes{{part=\"{p}\"}}")).unwrap_or_else(|| panic!("no {p}"));
    let fixed: f64 = ["runtime", "in_memory_caches", "mst_node_cache", "firehose", "backfill", "exports", "import", "headroom"].iter().map(|p| part(p)).sum();
    assert_eq!(fixed + part("pool"), part("budget"));
    // registration re-plans at once: the pool caches' gauges are there
    for c in ["meta", "block", "repo"] {
        for k in ["target", "capacity"] {
            let v = gauge(&m, &format!("vlpds_memory_cache_bytes{{cache=\"{c}\",kind=\"{k}\"}}")).unwrap_or_else(|| panic!("{c} {k}"));
            // the metadata target is 0 until a shard has SSTs
            assert!(v > 0.0 || (c, k) == ("meta", "target"), "{c} {k}: {v}");
        }
    }
    assert!(gauge(&m, "vlpds_sst_meta_decode_ratio").is_some_and(|r| (1.0..=2.0).contains(&r)));
    assert!(gauge(&m, "vlpds_meta_cache_shortfall_bytes").is_some());
    assert!(gauge(&m, "vlpds_sst_meta_need_bytes").is_some());
}
