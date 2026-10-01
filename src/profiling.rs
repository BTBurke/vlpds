//! CPU profiling (cargo feature `profiling`; bench/obs/README.md).
//!
//! - Continuous: `--pyroscope-url` starts a Pyroscope agent (pprof-rs,
//!   100 Hz SIGPROF sampling, framehop unwinding) pushing every 10 s, tagged
//!   with the node id and git revision.
//! - On demand: `GET /debug/pprof/profile?seconds=N` (admin token, Bearer or
//!   Basic `admin:<token>`) samples for N seconds and returns a pprof protobuf
//!   (`go tool pprof -top`), or a flamegraph SVG with `format=svg`.
//!
//! Both use pyroscope's pprof-rs backend, a single SIGPROF sampler: with the
//! agent running the endpoint answers 409 (query Pyroscope instead:
//! bench/obs/profile.sh -p).
//! Heap profiling is not wired up: jemalloc_pprof needs Linux (/proc maps);
//! the `vlpds_jemalloc_bytes` gauges cover allocator totals.

use crate::xrpc::App;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Set while the Pyroscope agent owns SIGPROF.
static CONTINUOUS: AtomicBool = AtomicBool::new(false);
/// Set while an on-demand profile runs (one at a time).
static ON_DEMAND: AtomicBool = AtomicBool::new(false);

/// The source tree's `git describe --always --dirty` (tags the profiles and
/// `vlpds_build_info`), else VLPDS_GIT_REV at build time, else "unknown".
pub fn git_rev() -> String {
    if let Some(r) = option_env!("VLPDS_GIT_REV") {
        return r.to_string();
    }
    std::process::Command::new("git")
        .args([
            "-C",
            env!("CARGO_MANIFEST_DIR"),
            "describe",
            "--always",
            "--dirty",
            "--abbrev=10",
        ])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

pub const ENABLED: bool = cfg!(feature = "profiling");

/// Starts the Pyroscope agent for the life of the process. Call before the
/// tokio runtime exists (the agent's blocking HTTP client owns a runtime).
#[cfg(feature = "profiling")]
pub fn start_pyroscope(url: &str, node_id: &str, rev: &str) -> anyhow::Result<()> {
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;
    let backend = pprof_backend(PprofConfig { sample_rate: 100 }, BackendConfig::default());
    let agent = PyroscopeAgentBuilder::new(
        url,
        "vlpds",
        100,
        "pyroscope-rs",
        env!("CARGO_PKG_VERSION"),
        backend,
    )
    .tags(vec![("node_id", node_id), ("rev", rev)])
    .func(tidy_report)
    .build()
    .map_err(|e| anyhow::anyhow!("pyroscope agent: {e}"))?;
    let running = agent
        .start()
        .map_err(|e| anyhow::anyhow!("pyroscope agent: {e}"))?;
    CONTINUOUS.store(true, Ordering::Release);
    // runs until exit (the last <10 s of samples are not flushed)
    std::mem::forget(running);
    Ok(())
}

#[cfg(not(feature = "profiling"))]
pub fn start_pyroscope(_url: &str, _node_id: &str, _rev: &str) -> anyhow::Result<()> {
    anyhow::bail!("--pyroscope-url needs a build with `--features profiling`")
}

pub fn routes() -> Router<Arc<App>> {
    Router::new().route("/debug/pprof/profile", get(profile))
}

#[derive(Deserialize)]
struct ProfileQ {
    seconds: Option<u64>,
    /// Sampling rate (Hz), default 99.
    frequency: Option<i32>,
    /// `svg` for a flamegraph; default pprof protobuf.
    format: Option<String>,
}

fn admin_ok(app: &App, headers: &HeaderMap) -> bool {
    let Some(h) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    if let Some(t) = h.strip_prefix("Bearer ") {
        return crate::auth::token_eq(&app.admin_token, t);
    }
    h.strip_prefix("Basic ")
        .is_some_and(|b| crate::auth::basic_admin_ok(b, &app.admin_token))
}

fn text(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, msg.into() + "\n").into_response()
}

async fn profile(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<ProfileQ>,
) -> Response {
    if !admin_ok(&app, &headers) {
        return text(
            StatusCode::UNAUTHORIZED,
            "admin token required (Authorization: Bearer <admin token>)",
        );
    }
    if !ENABLED {
        return text(
            StatusCode::NOT_IMPLEMENTED,
            "built without the profiling feature (cargo build --features profiling)",
        );
    }
    if CONTINUOUS.load(Ordering::Acquire) {
        return text(
            StatusCode::CONFLICT,
            "the Pyroscope agent is sampling this process (--pyroscope-url): query Pyroscope instead (bench/obs/profile.sh -p)",
        );
    }
    if ON_DEMAND.swap(true, Ordering::AcqRel) {
        return text(StatusCode::CONFLICT, "a profile is already being taken");
    }
    let secs = q.seconds.unwrap_or(10).clamp(1, 300);
    let freq = q.frequency.unwrap_or(99).clamp(1, 1000);
    let svg = q.format.as_deref() == Some("svg");
    // sampling sleeps and symbolizing is CPU-heavy: a blocking thread
    let r = tokio::task::spawn_blocking(move || sample(secs, freq, svg)).await;
    ON_DEMAND.store(false, Ordering::Release);
    match r {
        Ok(Ok((ct, body))) => ([(header::CONTENT_TYPE, ct)], body).into_response(),
        Ok(Err(e)) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("profiling failed: {e}"),
        ),
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("profiling task failed: {e}"),
        ),
    }
}

#[cfg(feature = "profiling")]
fn sample(secs: u64, freq: i32, svg: bool) -> anyhow::Result<(&'static str, Vec<u8>)> {
    use prost::Message;
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig, ReportData};
    let err = |e: pyroscope::PyroscopeError| anyhow::anyhow!("{e}");
    let start = std::time::SystemTime::now();
    let mut backend = pprof_backend(
        PprofConfig {
            sample_rate: freq as u32,
        },
        BackendConfig::default(),
    )
    .initialize()
    .map_err(err)?;
    std::thread::sleep(std::time::Duration::from_secs(secs));
    let batch = backend.report().map_err(err)?;
    // drops the profiler guard: SIGPROF timer off
    backend.shutdown().map_err(err)?;
    let ReportData::Reports(reports) = batch.data else {
        anyhow::bail!("unexpected raw pprof report");
    };
    let reports: Vec<_> = reports.into_iter().map(tidy_report).collect();
    if svg {
        // collapsed stacks (root first) for inferno
        let mut lines = Vec::new();
        for r in &reports {
            for (st, n) in &r.data {
                let names: Vec<&str> = st
                    .frames
                    .iter()
                    .rev()
                    .map(|f| f.name.as_deref().unwrap_or("?"))
                    .collect();
                lines.push(format!("{} {n}", names.join(";")));
            }
        }
        let mut opts = inferno::flamegraph::Options::default();
        opts.title = format!("vlpds CPU, {secs} s at {freq} Hz");
        let mut out = Vec::new();
        inferno::flamegraph::from_lines(&mut opts, lines.iter().map(String::as_str), &mut out)?;
        return Ok(("image/svg+xml", out));
    }
    let t0 = start
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let profile = pyroscope::encode::pprof::encode(&reports, freq as u32, t0, secs * 1_000_000_000);
    Ok(("application/octet-stream", profile.encode_to_vec()))
}

/// Readable frames for `go tool pprof -top` and Pyroscope:
/// - inlined frames come back as bare names (`park`, `{closure#0}`): suffix
///   them with their source file so same-named ones stay apart;
/// - macOS: system library frames (libsystem_kernel syscalls, pthread, ...)
///   symbolize to the nearest *exported* symbol of the dyld shared cache,
///   i.e. to wrong names (`_macx_swapoff` for a kevent). They are the frames
///   without source info whose C names start with `_`; fold each run of them
///   into one `[libsystem]` frame (callers keep the time; pprof `-hide`
///   charges it to them). Other platforms resolve libc correctly.
#[cfg(feature = "profiling")]
fn tidy_report(mut r: pyroscope::backend::Report) -> pyroscope::backend::Report {
    let mut data = std::collections::HashMap::with_capacity(r.data.len());
    for (mut st, n) in r.data.drain() {
        let mut frames: Vec<pyroscope::backend::StackFrame> = Vec::with_capacity(st.frames.len());
        for mut f in st.frames.drain(..) {
            let file = f.filename.as_deref().unwrap_or("");
            let system = cfg!(target_os = "macos")
                && file.is_empty()
                && f.name
                    .as_deref()
                    .is_some_and(|n| n.starts_with('_') && !n.starts_with("_rjem"));
            if !system
                && !file.is_empty()
                && f.name.as_deref().is_some_and(|n| {
                    !n.starts_with('<') && !n.split('<').next().unwrap_or("").contains("::")
                })
            {
                // (pyroscope puts the full path in relative_path)
                let path = f
                    .relative_path
                    .as_deref()
                    .or(f.absolute_path.as_deref())
                    .unwrap_or("");
                let origin = source_origin(path, file);
                f.name = Some(format!("{} @{origin}", f.name.as_deref().unwrap_or("")));
            }
            if system {
                if frames
                    .last()
                    .is_some_and(|p| p.name.as_deref() == Some("[libsystem]"))
                {
                    continue;
                }
                f.name = Some("[libsystem]".into());
            }
            frames.push(f);
        }
        st.frames = frames;
        *data.entry(st).or_insert(0) += n;
    }
    r.data = data;
    r
}

/// `crate:file.rs` for a source path: registry crates by name (version
/// dropped), the standard library as std/core/alloc, this crate as vlpds.
#[cfg(feature = "profiling")]
fn source_origin(path: &str, file: &str) -> String {
    let krate = if let Some(rest) = path.split("/registry/src/").nth(1) {
        // index.crates.io-<hash>/<name>-<version>/src/...
        rest.split('/')
            .nth(1)
            .map(|d| d.rsplit_once('-').map_or(d, |(n, _)| n).to_string())
    } else if let Some(rest) = path
        .split("/library/")
        .nth(1)
        .filter(|_| path.contains("rust"))
    {
        rest.split('/').next().map(str::to_string)
    } else if path.starts_with(env!("CARGO_MANIFEST_DIR")) {
        Some("vlpds".into())
    } else {
        None
    };
    match krate {
        Some(k) => format!("{k}:{file}"),
        None => file.to_string(),
    }
}

#[cfg(not(feature = "profiling"))]
fn sample(_secs: u64, _freq: i32, _svg: bool) -> anyhow::Result<(&'static str, Vec<u8>)> {
    anyhow::bail!("built without the profiling feature")
}
