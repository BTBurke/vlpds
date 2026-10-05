//! Per-node maintenance (`vlpds admin rewrap-secrets` / `rotate-plc-keys`,
//! src/cli/admin.rs `per_node`) covers every shard: each node lists the
//! shards it scanned, and a shard that moved between two nodes' calls (in
//! neither list) is rerun on its new owner.

use crate::common::*;
use std::collections::HashSet;
use std::sync::Arc;

async fn admin_json(url: &str, args: &[&str]) -> (anyhow::Result<()>, J) {
    let mut a = vec!["--json"];
    a.extend_from_slice(args);
    let (r, out) = admin_cli(url, &a).await;
    (r, serde_json::from_str(&out).unwrap_or_else(|e| panic!("{e}: {out}")))
}

fn scanned(row: &J) -> Vec<u64> {
    row["result"]["scanned"].as_array().into_iter().flatten().filter_map(J::as_u64).collect()
}

/// b leaves (handing its shards to a) after a answered and before b is
/// called: neither scans b's shards in the first pass. The CLI finds them
/// missing from the union and reruns them on a.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_moving_between_node_calls_is_rerun_on_its_owner() {
    const SHARDS: u32 = 8;
    let store = Arc::new(object_store::memory::InMemory::new());
    let tag = unique_name("mc");
    let (na, nb) = (format!("{tag}-a"), format!("{tag}-b"));
    let a = cluster_node(&na, store.clone(), SHARDS, |_| {}).await;
    let b = Arc::new(cluster_node(&nb, store.clone(), SHARDS, |_| {}).await);
    balanced(&[&a, &*b]).await;
    let mut dids = Vec::new();
    for _ in 0..3 {
        dids.push(a.create_account("mc").await.did);
        dids.push(b.create_account("mc").await.did);
    }
    let b_shards: HashSet<u64> = b.app.partitions.owned().iter().map(|p| p.id.0 as u64).collect();
    assert!(dids.iter().any(|d| b.app.partition(d).is_ok()), "an account on b");

    // the direct call: only the named shards it owns
    let first = *b_shards.iter().next().unwrap();
    let j = b
        .xrpc
        .post("vlpds.admin.rewrapSecrets", &json!({"dryRun": true, "shards": [first, 9999]}), &Auth::Admin)
        .await
        .ok();
    assert_eq!(j["scanned"], json!([first]), "{j}");
    assert!(j["layoutVersion"].as_u64().is_some(), "{j}");

    {
        let (na, b) = (na.clone(), b.clone());
        vlpds::cli::admin::set_after_node_hook(Some(Arc::new(move |node: String| {
            let (na, b) = (na.clone(), b.clone());
            Box::pin(async move {
                if node == na {
                    vlpds::server::shutdown(&b.app).await;
                }
            })
        })));
    }
    let (r, j) = admin_json(&a.url, &["rewrap-secrets", "--dry-run"]).await;
    vlpds::cli::admin::set_after_node_hook(None);
    r.unwrap_or_else(|e| panic!("{e:#}: {j}"));
    let rows = j.as_array().unwrap();
    let row =
        |name: &str| rows.iter().find(|r| r["node"] == json!(name)).unwrap_or_else(|| panic!("no {name} row: {j}"));
    assert!(scanned(row(&na)).iter().all(|s| !b_shards.contains(s)), "a scanned only its own shards first: {j}");
    assert!(scanned(row(&nb)).is_empty(), "b had handed its shards away: {j}");
    // a may adopt b's shards over more than one rerun round
    let rerun: HashSet<u64> =
        rows.iter().filter(|r| r["node"] == json!(format!("{na} (rerun)"))).flat_map(scanned).collect();
    assert_eq!(rerun, b_shards, "b's former shards rerun on a: {j}");
    let all: HashSet<u64> = rows.iter().flat_map(scanned).collect();
    assert_eq!(all.len(), SHARDS as usize, "{j}");
    let accounts: u64 = rows.iter().filter_map(|r| r["result"]["accounts"].as_u64()).sum();
    assert_eq!(accounts, dids.len() as u64, "every account once: {j}");
}
