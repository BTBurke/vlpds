//! Process lifecycle metrics that survive the process: start time, and how
//! the previous process ended.
//!
//! A fail-stop exits right after it decides to, so a counter bumped on the
//! way out is almost never scraped. Instead every fail-stop path calls
//! [`fail_stop`], which records its reason and exit code in a small local
//! file (`--exit-state-file`, by default `vlpds-exit-{node_id}.json` in
//! `--cache-dir`) before exiting. The next process on that file reads it at
//! startup ([`init`]) and exports it as `vlpds_last_exit_reason_info{reason,
//! code}` for as long as it runs, next to `vlpds_process_start_time_seconds`
//! (and the standard `process_start_time_seconds`). `init` then marks the
//! file `running`: a process that dies without recording an exit (SIGKILL,
//! OOM kill, abort, host loss) leaves that marker, which the next start
//! reports as `crash`. A graceful stop records `clean`.
//!
//! Cluster-side, the node that fences an incarnation's log because it ended
//! without fencing it itself counts `vlpds_peer_takeovers_total{reason}`
//! (cluster.rs): that survives the dead process too, on a live peer.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// When this process started (forced early in `main`).
static STARTED: LazyLock<f64> = LazyLock::new(unix_secs);

/// The exit-state file, once [`init`] ran with one.
static FILE: OnceLock<PathBuf> = OnceLock::new();

fn unix_secs() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// What the exit-state file holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExitRecord {
    /// `running` while a process uses the file, else how it ended.
    pub reason: String,
    /// Exit code (None while running).
    pub code: Option<i32>,
    /// Unix seconds of the record.
    pub at: f64,
    pub pid: u32,
}

/// How the previous process ended, as exported: (reason, code label, time).
pub fn previous(record: Option<ExitRecord>) -> (String, String, f64) {
    match record {
        None => ("none".into(), String::new(), 0.0),
        Some(r) if r.reason == "running" => ("crash".into(), String::new(), 0.0),
        Some(r) => (r.reason, r.code.map(|c| c.to_string()).unwrap_or_default(), r.at),
    }
}

fn read(path: &Path) -> Option<ExitRecord> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// Writes `rec` atomically (temp file + rename).
fn write(path: &Path, rec: &ExitRecord) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(rec).unwrap_or_default())?;
    std::fs::rename(&tmp, path)
}

/// Startup: exports the start time and the previous exit recorded in `path`
/// (if any), then marks the file `running`.
pub fn init(path: Option<PathBuf>) {
    LazyLock::force(&STARTED);
    let prev = path.as_deref().and_then(read);
    let (reason, code, at) = previous(prev);
    crate::metrics::LAST_EXIT.reset();
    crate::metrics::LAST_EXIT.with_label_values(&[reason.as_str(), code.as_str()]).set(1);
    crate::metrics::LAST_EXIT_TIME.set(at);
    if reason != "none" {
        tracing::info!(reason, code, "previous process exit");
    }
    let Some(path) = path else { return };
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(dir);
    }
    let running = ExitRecord { reason: "running".into(), code: None, at: *STARTED, pid: std::process::id() };
    match write(&path, &running) {
        Ok(()) => {
            let _ = FILE.set(path);
        }
        Err(e) => tracing::warn!(path = %path.display(), "exit-state file not writable (fail-stop reasons will not be kept): {e}"),
    }
}

/// Records how this process ends (no-op without an exit-state file).
pub fn record_exit(code: i32, reason: &str) {
    if let Some(path) = FILE.get() {
        let rec = ExitRecord { reason: reason.into(), code: Some(code), at: unix_secs(), pid: std::process::id() };
        if let Err(e) = write(path, &rec) {
            tracing::warn!(path = %path.display(), "recording exit failed: {e}");
        }
    }
}

/// Fail-stop: records `reason` and exits with `code` (2: segment upload,
/// 3: log fenced / ordinal taken, 4: state apply, 5: lease lost or lapsed).
pub fn fail_stop(code: i32, reason: &str) -> ! {
    record_exit(code, reason);
    std::process::exit(code)
}

/// Scrape-time refresh: the start time gauges.
pub fn refresh_metrics() {
    crate::metrics::PROCESS_START.set(*STARTED);
    crate::metrics::PROCESS_START_STD.set(*STARTED);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn previous_exit_from_the_file() {
        let dir = std::env::temp_dir().join(format!("vlpds-lifecycle-{}-{}", std::process::id(), crate::tid::now_micros()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("exit.json");
        // no file: first run
        assert_eq!(previous(read(&path)).0, "none");
        // a process that started and vanished without recording an exit
        write(&path, &ExitRecord { reason: "running".into(), code: None, at: 1.0, pid: 1 }).unwrap();
        assert_eq!(previous(read(&path)), ("crash".into(), String::new(), 0.0));
        // a fail-stop recorded its reason and code
        write(&path, &ExitRecord { reason: "fenced".into(), code: Some(3), at: 42.0, pid: 1 }).unwrap();
        assert_eq!(previous(read(&path)), ("fenced".into(), "3".into(), 42.0));
        // garbage reads as no record
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(previous(read(&path)).0, "none");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The only test that calls `init` (it sets the process-wide file).
    #[test]
    fn init_exports_the_previous_exit_and_marks_running() {
        let dir = std::env::temp_dir().join(format!("vlpds-lifecycle-init-{}-{}", std::process::id(), crate::tid::now_micros()));
        let path = dir.join("sub").join("exit.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        write(&path, &ExitRecord { reason: "lease_lost".into(), code: Some(5), at: 7.0, pid: 1 }).unwrap();
        init(Some(path.clone()));
        assert_eq!(crate::metrics::LAST_EXIT.with_label_values(&["lease_lost", "5"]).get(), 1);
        assert_eq!(crate::metrics::LAST_EXIT_TIME.get(), 7.0);
        let now = read(&path).unwrap();
        assert_eq!((now.reason.as_str(), now.code, now.pid), ("running", None, std::process::id()));
        // what a fail-stop leaves for the next start
        record_exit(3, "fenced");
        assert_eq!(previous(read(&path)).0, "fenced");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn start_time_is_exported() {
        refresh_metrics();
        let now = unix_secs();
        let v = crate::metrics::PROCESS_START.get();
        assert!(v > 1.7e9 && v <= now, "{v}");
        assert_eq!(crate::metrics::PROCESS_START_STD.get(), v);
    }
}
