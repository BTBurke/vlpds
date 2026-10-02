//! Periodic totals for the operator dashboard: accounts by status, repos
//! written recently, and the SST disk cache's size.
//!
//! Every `--account-stats-interval-secs` (default 15 min; 0 = off) each node
//! scans the account (`a/`) and head (`h/`) rows of the shards it owns, from
//! a snapshot, and exports its share (`vlpds_accounts{status}`,
//! `vlpds_repos_written_within{window}`); the dashboard sums the nodes. One
//! pass reads two small rows per account, in key order, once per interval:
//! never per scrape. A shard moving between nodes mid-interval is counted
//! by both or neither until the next pass.

use super::*;
use std::time::Duration;

/// Repo-activity windows of `vlpds_repos_written_within` (label, seconds).
const WINDOWS: [(&str, u64); 3] = [("1d", 86_400), ("7d", 7 * 86_400), ("30d", 30 * 86_400)];
/// `vlpds_accounts` statuses (anything else counts as `other`).
const STATUSES: [&str; 5] = ["active", "deactivated", "takendown", "suspended", "other"];

#[derive(Default, Debug, PartialEq)]
pub struct Counts {
    /// By [`STATUSES`] index.
    pub accounts: [i64; 5],
    /// Repos (heads), and those written within each of [`WINDOWS`].
    pub repos: i64,
    pub written_within: [i64; 3],
}

fn status_index(status: Option<&str>) -> usize {
    match status {
        None => 0,
        Some(s) => STATUSES[1..4].iter().position(|k| *k == s).map_or(4, |i| i + 1),
    }
}

/// Counts the accounts and repos of every shard open on this node, each
/// within its layout range (a split child still reading its parent's SSTs
/// sees the parent's other keys too).
pub async fn count(app: &App, now_micros: u64) -> anyhow::Result<Counts> {
    /// Only an account's status (serde skips the rest of the JSON).
    #[derive(serde::Deserialize)]
    struct Status<'a> {
        #[serde(borrow, default)]
        status: Option<std::borrow::Cow<'a, str>>,
    }
    let layout = app.partitions.layout();
    let mut c = Counts::default();
    let opts = slatedb::config::ScanOptions { read_ahead_bytes: 1 << 20, max_fetch_tasks: 2, ..Default::default() };
    for range in &layout.shards {
        let Some(p) = app.partitions.get(range.id) else { continue };
        let snap = p.db.snapshot().await?;
        let lo = range.lo as u16;
        let in_range = |key: &[u8]| state::key_slot(key).is_some_and(|s| (s as u32) < range.hi);
        let mut accts = state::FamilyScan::new(snap.as_ref(), state::ACCOUNT_FAMILY, Some(state::slot_family(lo, state::ACCOUNT_FAMILY)), &opts).await?;
        while let Some(kv) = accts.next().await? {
            if !in_range(&kv.key) {
                break;
            }
            let status = serde_json::from_slice::<Status>(&kv.value).ok().and_then(|s| s.status);
            c.accounts[status_index(status.as_deref())] += 1;
        }
        let mut heads = state::FamilyScan::new(snap.as_ref(), state::HEAD_FAMILY, Some(state::slot_family(lo, state::HEAD_FAMILY)), &opts).await?;
        while let Some(kv) = heads.next().await? {
            if !in_range(&kv.key) {
                break;
            }
            c.repos += 1;
            let Ok(head) = state::Head::decode(&kv.value) else { continue };
            let age = now_micros.saturating_sub(head.rev.micros());
            for (i, (_, secs)) in WINDOWS.iter().enumerate() {
                if age <= secs * 1_000_000 {
                    c.written_within[i] += 1;
                }
            }
        }
    }
    Ok(c)
}

fn export(c: &Counts) {
    for (i, s) in STATUSES.iter().enumerate() {
        metrics::ACCOUNTS.with_label_values(&[s]).set(c.accounts[i]);
    }
    for (i, (w, _)) in WINDOWS.iter().enumerate() {
        metrics::REPOS_WRITTEN_WITHIN.with_label_values(&[w]).set(c.written_within[i]);
    }
    metrics::REPOS_WRITTEN_WITHIN.with_label_values(&["all"]).set(c.repos);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64());
    metrics::ACCOUNT_STATS_TIME.set(now);
}

/// Bytes of the files under `dir` (recursively; unreadable entries skipped).
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
    rd.flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            Ok(t) if t.is_file() => e.metadata().map_or(0, |m| m.len()),
            _ => 0,
        })
        .sum()
}

async fn export_disk_cache(app: &App) {
    let Some(cfg) = app.node.disk_cache.clone() else { return };
    let owned = app.partitions.owned().len().max(1) as u64;
    let capacity = cfg.node_bytes.unwrap_or_else(|| app.node.shard_disk_cache().map_or(0, |c| c.shard_bytes) * owned);
    let dir = cfg.dir.clone();
    let used = tokio::task::spawn_blocking(move || dir_bytes(&dir)).await.unwrap_or(0);
    metrics::DISK_CACHE_BYTES.with_label_values(&["used"]).set(used as i64);
    metrics::DISK_CACHE_BYTES.with_label_values(&["capacity"]).set(capacity as i64);
}

/// The periodic count (see the module docs); a no-op task when the interval
/// is zero.
pub fn spawn_account_stats(app: Arc<App>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let every = app.config.account_stats_interval;
        if every.is_zero() {
            return;
        }
        // first pass soon after startup, once shards have opened
        tokio::time::sleep(Duration::from_secs(30).min(every)).await;
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64);
            match count(&app, now).await {
                Ok(c) => export(&c),
                Err(e) => tracing::warn!("account stats: {e:#}"),
            }
            export_disk_cache(&app).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses() {
        assert_eq!(status_index(None), 0);
        assert_eq!(status_index(Some("deactivated")), 1);
        assert_eq!(status_index(Some("takendown")), 2);
        assert_eq!(status_index(Some("suspended")), 3);
        assert_eq!(status_index(Some("deleted")), 4);
    }
}
