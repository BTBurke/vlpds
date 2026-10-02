//! Feature levels: which persisted and wire formats a build reads and
//! writes, and which ones the cluster has committed to (DESIGN.md "Rolling
//! upgrades and format versioning").
//!
//! - A **level** is one integer naming a set of formats. Every format change
//!   that puts new bytes in the bucket or on the wire gets the next level
//!   ([`LEVELS`]).
//! - A **build** reads everything in [`MIN_LEVEL`]`..=`[`MAX_LEVEL`] and can
//!   run a cluster whose active level is anywhere in that window.
//! - The **cluster** commits to one active level in `cluster/version`
//!   ([`ClusterVersion`], CAS'd). Writers emit the active level's formats
//!   ([`active`]), so a build whose `MAX_LEVEL` is ahead of the cluster writes
//!   byte for byte what the previous build writes: that is the rollback window.
//! - Raising the active level is manual and one way (`vlpds admin cluster
//!   finalize`, `Cluster::finalize_level`): first a `target`, then a check
//!   that every live node's lease can run it, then `active`. A node checks
//!   the object before it touches any data and again after its lease is
//!   written, so a node that can't run the level exits 7
//!   (`incompatible_level`, [`refuse`]) instead of reading or writing
//!   anything.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};

/// One feature level.
#[derive(Debug)]
pub struct Level {
    pub level: u32,
    pub name: &'static str,
    pub description: &'static str,
    /// Puts new bytes in the bucket (a persistent level can never be
    /// lowered again); false = only gates wire behavior.
    pub persistent: bool,
    /// The log segment magic written at this level, if the level changed it.
    pub segment_magic: Option<&'static [u8; 8]>,
}

/// Every level this build knows, oldest first. Level 1 is the baseline:
/// the formats in production on day one, recorded in testdata/formats/L1.
pub const LEVELS: &[Level] = &[Level {
    level: 1,
    name: "baseline",
    description: "VLSEG06 log segments (32-bit shard tags, zstd bodies, derived #commit muts), \
                  VLFENCE fences, h/ R/ c/ C/ b/ n/ M/ a/ p/ state rows, meta/applied2 markers, \
                  JSON control objects, vw1 wrapped secrets, log stream messages 0 (batch) and 1 (watermark)",
    persistent: true,
    segment_magic: Some(b"VLSEG06\n"),
}];

/// Lowest level this build can run (and read data of).
pub const MIN_LEVEL: u32 = 1;
/// Highest level this build can run.
pub const MAX_LEVEL: u32 = 1;
/// Highest level that has shipped: its fixtures (testdata/formats/L{n})
/// are frozen by testdata/formats/MANIFEST.
pub const RELEASED: u32 = 1;

/// Level assumed for a lease or object written before levels existed.
pub fn legacy_level() -> u32 {
    1
}

/// The control-plane object holding the cluster's levels.
pub const OBJECT: &str = "cluster/version";

/// A range of levels a build can run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    pub min: u32,
    pub max: u32,
}

impl Default for Window {
    fn default() -> Self {
        Window::BUILD
    }
}

impl Window {
    /// This build's window.
    pub const BUILD: Window = Window { min: MIN_LEVEL, max: MAX_LEVEL };

    pub fn contains(&self, level: u32) -> bool {
        (self.min..=self.max).contains(&level)
    }

    /// Whether a node with this window may run a cluster at `v`: it reads
    /// the active level, and a raise in progress (`target`) isn't past it.
    /// Err = why not (for the exit-7 log line).
    pub fn check(&self, v: &ClusterVersion) -> Result<(), String> {
        if !self.contains(v.active) {
            return Err(format!("cluster level {} is outside this build's levels {}..={}", v.active, self.min, self.max));
        }
        if let Some(t) = v.target.filter(|t| *t > self.max) {
            return Err(format!("cluster is raising its level to {t}, past this build's max level {}", self.max));
        }
        Ok(())
    }
}

/// `cluster/version`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterVersion {
    /// The level every writer emits.
    pub active: u32,
    /// Set while a raise is in progress (`Cluster::finalize_level` step 1);
    /// a node whose max level is below it refuses to start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<u32>,
    /// Every change of `active`, oldest first.
    #[serde(default)]
    pub history: Vec<Change>,
    /// Fields of a newer build, kept by read-modify-CAS.
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One change of the active level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub level: u32,
    /// RFC 3339.
    pub at: String,
    /// Node id that wrote it (and the operator, for a finalize).
    pub by: String,
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl ClusterVersion {
    pub fn new(active: u32, by: &str) -> ClusterVersion {
        ClusterVersion { active, target: None, history: vec![Change::new(active, by)], extra: Default::default() }
    }
}

impl Change {
    pub fn new(level: u32, by: &str) -> Change {
        Change { level, at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), by: by.into(), extra: Default::default() }
    }
}

/// The level writers emit, as this process last read it (MIN_LEVEL until
/// the cluster's object is read). Process-wide: in-process test clusters
/// share it.
static ACTIVE: AtomicU32 = AtomicU32::new(0);

pub fn active() -> u32 {
    match ACTIVE.load(Ordering::Acquire) {
        0 => MIN_LEVEL,
        l => l,
    }
}

/// Records the active level read from `cluster/version` (and exports it).
pub fn set_active(level: u32) {
    let prev = ACTIVE.swap(level, Ordering::AcqRel);
    if prev != 0 && prev != level {
        tracing::warn!(from = prev, to = level, "cluster feature level changed");
    }
    crate::metrics::FEATURE_LEVEL.with_label_values(&["active"]).set(level as i64);
}

/// The segment magic writers emit at `level`: that of the newest level at
/// or below it that set one.
pub fn segment_magic(level: u32) -> &'static [u8; 8] {
    LEVELS.iter().rev().filter(|l| l.level <= level).find_map(|l| l.segment_magic).unwrap_or(crate::segment::MAGIC)
}

/// The segment magics this build reads, with the level each belongs to.
pub fn segment_magics() -> impl Iterator<Item = (&'static [u8; 8], u32)> {
    LEVELS.iter().filter(|l| l.level >= MIN_LEVEL && l.level <= MAX_LEVEL).filter_map(|l| l.segment_magic.map(|m| (m, l.level)))
}

/// The level of a segment magic, if this build reads it.
pub fn segment_level(magic: &[u8]) -> Option<u32> {
    segment_magics().find(|(m, _)| m.as_slice() == magic).map(|(_, l)| l)
}

/// This build's git revision (as in `vlpds_build_info`), computed once.
pub fn build_rev() -> &'static str {
    static REV: LazyLock<String> = LazyLock::new(crate::profiling::git_rev);
    &REV
}

/// Formats counted by `vlpds_format_errors_total` (exported at 0 by [`init_metrics`]).
pub const FORMATS: &[&str] = &["segment", "log_stream", "applied_marker", "cluster_version", "control_object"];

/// A decode failed on an unknown magic, codec, message type or value tag:
/// a node of a newer level wrote it, or it is corrupt.
pub fn format_error(format: &'static str) {
    crate::metrics::FORMAT_ERRORS.with_label_values(&[format]).inc();
}

/// Exports this build's window and every format error counter at 0.
pub fn init_metrics() {
    crate::metrics::FEATURE_LEVEL.with_label_values(&["binary_min"]).set(MIN_LEVEL as i64);
    crate::metrics::FEATURE_LEVEL.with_label_values(&["binary_max"]).set(MAX_LEVEL as i64);
    for f in FORMATS {
        crate::metrics::FORMAT_ERRORS.with_label_values(&[f]);
    }
}

pub const EXIT_CODE: i32 = 7;
pub const EXIT_REASON: &str = "incompatible_level";

type RefuseHook = Arc<dyn Fn(&str, &str) + Send + Sync>;
static REFUSE_HOOK: parking_lot::RwLock<Option<RefuseHook>> = parking_lot::RwLock::new(None);

/// Tests: replaces the exit-7 fail-stop with `f(node_id, why)`; the caller
/// then gets an error instead of the process exiting.
#[doc(hidden)]
pub fn set_refuse_hook(f: Option<RefuseHook>) {
    *REFUSE_HOOK.write() = f;
}

/// A node can't run the cluster's level: fail-stop 7 `incompatible_level`
/// (or the test hook). Returns the error the caller propagates when a hook
/// is set.
pub fn refuse(node_id: &str, why: &str) -> anyhow::Error {
    tracing::error!(node = node_id, levels = ?Window::BUILD, "incompatible feature level: {why}; fail-stop (exit {EXIT_CODE})");
    let hook = REFUSE_HOOK.read().clone();
    match hook {
        Some(h) => h(node_id, why),
        // unit tests never exit the test binary
        None if cfg!(test) => {}
        None => crate::lifecycle::fail_stop(EXIT_CODE, EXIT_REASON),
    }
    anyhow::anyhow!("{EXIT_REASON}: {why}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_table_is_consistent() {
        for (i, l) in LEVELS.iter().enumerate() {
            assert_eq!(l.level, i as u32 + 1, "levels are dense from 1");
        }
        const { assert!(MIN_LEVEL >= 1 && MIN_LEVEL <= MAX_LEVEL && RELEASED <= MAX_LEVEL) };
        assert_eq!(MAX_LEVEL as usize, LEVELS.len());
        assert_eq!(segment_magic(MAX_LEVEL), crate::segment::MAGIC);
        // a level above every table entry writes the newest magic
        assert_eq!(segment_magic(MAX_LEVEL + 5), segment_magic(MAX_LEVEL));
        assert_eq!(segment_level(b"VLSEG06\n"), Some(1));
        assert_eq!(segment_level(b"VLSEG05\n"), None);
    }

    #[test]
    fn window_checks() {
        let w = Window { min: 1, max: 2 };
        let v = |active, target| ClusterVersion { active, target, history: vec![], extra: Default::default() };
        assert!(w.check(&v(1, None)).is_ok());
        assert!(w.check(&v(2, None)).is_ok());
        assert!(w.check(&v(1, Some(2))).is_ok());
        assert!(w.check(&v(3, None)).unwrap_err().contains("outside"));
        assert!(w.check(&v(1, Some(3))).unwrap_err().contains("raising"));
        assert!(Window { min: 2, max: 3 }.check(&v(1, None)).is_err(), "can no longer read level 1");
    }

    /// A newer build's fields survive an older node's read-modify-write.
    #[test]
    fn unknown_fields_round_trip() {
        let j = serde_json::json!({"active": 1, "history": [{"level": 1, "at": "t", "by": "a", "op": "x"}], "pinned": true});
        let v: ClusterVersion = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(&v).unwrap(), j);
        // fields written before `target`/`history` existed default
        let v: ClusterVersion = serde_json::from_str(r#"{"active": 1}"#).unwrap();
        assert_eq!((v.target, v.history.len()), (None, 0));
    }
}
