//! Prometheus metrics, exposed at /metrics.

use prometheus::{
    exponential_buckets, register_gauge, register_gauge_vec, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, Encoder, Gauge, GaugeVec, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, TextEncoder,
};
use std::sync::LazyLock;

/// 0.1ms .. ~52s
fn latency_buckets() -> Vec<f64> {
    exponential_buckets(0.0001, 2.0, 20).unwrap()
}

macro_rules! lazy {
    ($name:ident: $t:ty = $e:expr) => {
        pub static $name: LazyLock<$t> = LazyLock::new(|| $e.unwrap());
    };
}

// ---- HTTP ----
lazy!(HTTP_REQUESTS: IntCounterVec = register_int_counter_vec!("vlpds_http_requests_total", "XRPC requests by method and status", &["method", "status"]));
lazy!(HTTP_DURATION: HistogramVec = register_histogram_vec!("vlpds_http_request_duration_seconds", "XRPC request latency", &["method"], latency_buckets()));
lazy!(HTTP_INFLIGHT: IntGauge = register_int_gauge!("vlpds_http_requests_inflight", "XRPC requests in flight"));
lazy!(HTTP_CLIENT_CONNECTS: IntCounterVec = register_int_counter_vec!("vlpds_http_client_connects_total", "New outbound connections by client role (peer, public, guarded); should stay flat under steady load", &["role"]));
lazy!(HTTP_SERVER_CONNECTIONS: IntCounter = register_int_counter!("vlpds_http_server_connections_total", "Accepted inbound TCP connections"));
lazy!(HTTP_SERVER_OPEN: IntGauge = register_int_gauge!("vlpds_http_server_connections_open", "Inbound connections open"));
lazy!(HTTP_SERVER_ACTIVE: IntGaugeVec = register_int_gauge_vec!("vlpds_http_server_active_requests", "Inbound requests (h2: streams) awaiting their response head, by HTTP version", &["version"]));
lazy!(RATE_LIMITED: IntCounter = register_int_counter!("vlpds_rate_limited_total", "Requests rejected with 429 RateLimitExceeded"));
lazy!(WRITES_SHED: IntCounter = register_int_counter!("vlpds_writes_shed_total", "Write requests rejected by admission control (503)"));

// ---- repo workers ----
lazy!(COMMITS: IntCounter = register_int_counter!("vlpds_commits_total", "Commits built"));
lazy!(OPS: IntCounterVec = register_int_counter_vec!("vlpds_ops_total", "Record ops committed by action", &["action"]));
lazy!(COMMIT_OPS: Histogram = register_histogram!("vlpds_commit_ops", "Ops per commit (write coalescing)", vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 200.0]));
lazy!(COMMIT_REQUESTS: Histogram = register_histogram!("vlpds_commit_requests", "Write requests coalesced per commit", vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]));
lazy!(COMMIT_BUILD: Histogram = register_histogram!("vlpds_commit_build_seconds", "CPU time to build+sign a commit", exponential_buckets(0.000005, 2.0, 16).unwrap()));
lazy!(COMMIT_BLOCKS_BYTES: Histogram = register_histogram!("vlpds_commit_car_bytes", "Size of a commit's block CAR", exponential_buckets(256.0, 2.0, 14).unwrap()));
lazy!(WRITE_ERRORS: IntCounterVec = register_int_counter_vec!("vlpds_write_errors_total", "Rejected writes by kind", &["kind"]));
lazy!(WORKER_BATCH: Histogram = register_histogram!("vlpds_worker_batch_messages", "Messages drained per worker loop iteration", exponential_buckets(1.0, 2.0, 14).unwrap()));
lazy!(WORKER_QUEUE: IntGaugeVec = register_int_gauge_vec!("vlpds_worker_queue_depth", "Messages queued per worker", &["worker"]));
lazy!(CACHED_REPOS: IntGaugeVec = register_int_gauge_vec!("vlpds_cached_repos", "Repos held in memory per worker", &["worker"]));
lazy!(LOADING_REPOS: IntGauge = register_int_gauge!("vlpds_repos_loading", "Cold repo loads in progress"));
lazy!(REPO_LOADS: IntCounterVec = register_int_counter_vec!("vlpds_repo_loads_total", "Cold repo loads by result", &["result"]));
lazy!(REPO_LOAD_DURATION: Histogram = register_histogram!("vlpds_repo_load_seconds", "Cold repo load latency (scan + MST rebuild + verify)", latency_buckets()));
lazy!(REPO_LOAD_RECORDS: Histogram = register_histogram!("vlpds_repo_load_records", "Records per cold-loaded repo", exponential_buckets(1.0, 4.0, 12).unwrap()));
lazy!(REPO_EVICTIONS: IntCounter = register_int_counter!("vlpds_repo_evictions_total", "Repos evicted from worker caches"));

// ---- partitions / log ----
lazy!(SEQ_QUEUE: IntGaugeVec = register_int_gauge_vec!("vlpds_sequencer_queue_depth", "Log entries waiting for the sequencer", &["partition"]));
lazy!(SEGMENTS: IntCounterVec = register_int_counter_vec!("vlpds_segments_total", "Segments made durable", &["partition"]));
lazy!(SEGMENT_BYTES: Histogram = register_histogram!("vlpds_segment_bytes", "Segment object size", exponential_buckets(1024.0, 2.0, 14).unwrap()));
lazy!(SEGMENT_EVENTS: Histogram = register_histogram!("vlpds_segment_events", "Firehose events per segment", exponential_buckets(1.0, 2.0, 16).unwrap()));
lazy!(SEGMENT_BYTES_TOTAL: IntCounter = register_int_counter!("vlpds_segment_bytes_total", "Bytes written to the log"));
lazy!(PUT_DURATION: HistogramVec = register_histogram_vec!("vlpds_segment_put_seconds", "Segment PUT latency until durable (incl. hedges/retries)", &["partition"], latency_buckets()));
lazy!(PUT_ATTEMPTS: IntCounterVec = register_int_counter_vec!("vlpds_segment_put_attempts_total", "Segment PUT attempts by result", &["result"]));
lazy!(PUT_HEDGES: IntCounter = register_int_counter!("vlpds_segment_put_hedges_total", "Hedged (duplicate) segment PUTs started"));
lazy!(APPLY_DURATION: Histogram = register_histogram!("vlpds_state_apply_seconds", "SlateDB batch apply latency per segment", latency_buckets()));
lazy!(COMMIT_LATENCY: Histogram = register_histogram!("vlpds_commit_durable_seconds", "Commit enqueue -> durable+applied+acked", latency_buckets()));
lazy!(WATERMARK_LAG: IntGaugeVec = register_int_gauge_vec!("vlpds_watermark_lag_microseconds", "now - partition watermark", &["partition"]));
lazy!(LAST_SEQ: IntGaugeVec = register_int_gauge_vec!("vlpds_partition_durable_seq", "Last durable seq per partition", &["partition"]));
lazy!(REPLAYED_SEGMENTS: IntCounter = register_int_counter!("vlpds_recovery_replayed_segments_total", "Log segments replayed at startup"));

// ---- firehose ----
lazy!(FIREHOSE_EVENTS: IntCounter = register_int_counter!("vlpds_firehose_events_total", "Events emitted by the merger"));
lazy!(FIREHOSE_BATCH: Histogram = register_histogram!("vlpds_firehose_merge_batch_events", "Events per merged batch", exponential_buckets(1.0, 2.0, 16).unwrap()));
lazy!(FIREHOSE_SUBSCRIBERS: IntGauge = register_int_gauge!("vlpds_firehose_subscribers", "Connected subscribeRepos clients"));
lazy!(FIREHOSE_RING_BYTES: IntGauge = register_int_gauge!("vlpds_firehose_ring_bytes", "Bytes held in the in-memory firehose ring"));
lazy!(FIREHOSE_DISCONNECTS: IntCounterVec = register_int_counter_vec!("vlpds_firehose_disconnects_total", "Subscriber disconnects by reason", &["reason"]));
lazy!(FIREHOSE_SENT: IntCounter = register_int_counter!("vlpds_firehose_frames_sent_total", "Frames sent to subscribers"));
lazy!(FIREHOSE_MERGE_QUEUE_BYTES: IntGauge = register_int_gauge!("vlpds_firehose_merge_queue_bytes", "Frame bytes queued in the merger waiting for the min watermark"));
lazy!(FIREHOSE_SPILLS: IntCounter = register_int_counter!("vlpds_firehose_merge_spills_total", "Logs the merger stopped queueing (over budget) and reads back from S3"));
lazy!(FIREHOSE_SPILL_SEGMENTS: IntCounter = register_int_counter!("vlpds_firehose_merge_spill_segments_total", "Segments the merger read back from S3 for spilled logs"));
lazy!(FIREHOSE_SENT_BYTES: IntCounter = register_int_counter!("vlpds_firehose_bytes_sent_total", "Websocket bytes written to subscribers (frames + headers)"));
lazy!(FIREHOSE_BACKFILL_GETS: IntCounter = register_int_counter!("vlpds_firehose_backfill_gets_total", "Segment GETs made by cursor backfill readers"));
lazy!(FIREHOSE_BACKFILL_CACHE: IntCounterVec = register_int_counter_vec!("vlpds_firehose_backfill_cache_total", "Backfill segment cache lookups (hit includes joining a GET in flight)", &["result"]));
lazy!(FIREHOSE_BACKFILL_EVENTS: IntCounter = register_int_counter!("vlpds_firehose_backfill_events_total", "Events sent to subscribers from S3 backfill"));
lazy!(LOG_LIVE_BYTES: IntGauge = register_int_gauge!("vlpds_log_live_ring_bytes", "Segment bytes pinned by the node log's live ring (peer streams)"));
lazy!(LOG_STREAM_LAGGED: IntCounter = register_int_counter!("vlpds_log_stream_lagged_total", "Peer log streams dropped for falling behind the live ring (they catch up from S3)"));

// ---- log retention (retention.rs) ----
lazy!(RETENTION_DELETED_OBJECTS: IntCounterVec = register_int_counter_vec!("vlpds_retention_deleted_objects_total", "Log objects deleted by retention, by log (own, dead)", &["log"]));
lazy!(RETENTION_DELETED_BYTES: IntCounterVec = register_int_counter_vec!("vlpds_retention_deleted_bytes_total", "Log bytes deleted by retention, by log (own, dead)", &["log"]));
lazy!(RETENTION_PRUNED_SEQ: IntGauge = register_int_gauge!("vlpds_retention_pruned_seq", "Highest seq this node has deleted from any log (older cursors get OutdatedCursor)"));
lazy!(RETENTION_REPLAY_HOLD: IntGauge = register_int_gauge!("vlpds_retention_replay_hold_segments", "Segments of our log kept only because a crash replay could still need them (durable ordinal - replay floor)"));
lazy!(RETENTION_TICKS: IntCounterVec = register_int_counter_vec!("vlpds_retention_ticks_total", "Retention passes by result", &["result"]));

// ---- proxy ----
lazy!(PROXY_CACHE: IntCounterVec = register_int_counter_vec!("vlpds_proxy_cache_total", "Proxy fast-path cache lookups", &["result"]));

// ---- cluster ----
lazy!(FORWARDED: IntCounter = register_int_counter!("vlpds_requests_forwarded_total", "Requests proxied to the partition owner"));
lazy!(OWNED_PARTITIONS: IntGauge = register_int_gauge!("vlpds_owned_partitions", "Partitions this node owns"));
lazy!(LEASE_EVENTS: IntCounterVec = register_int_counter_vec!("vlpds_lease_events_total", "Partition lease transitions", &["event"]));
lazy!(CLUSTER_STORE_REQUESTS: IntCounterVec = register_int_counter_vec!("vlpds_cluster_store_requests_total", "Control-plane object-store requests (leases, assignments, writer claims, fences) by op", &["op"]));
lazy!(CLUSTER_NUDGES: IntCounterVec = register_int_counter_vec!("vlpds_cluster_nudges_total", "Early control-plane steps asked of peers after a release (sent, failed) or by peers (received)", &["dir"]));

// ---- memory ----
lazy!(JEMALLOC: IntGaugeVec = register_int_gauge_vec!("vlpds_jemalloc_bytes", "jemalloc stats", &["stat"]));

// ---- pipeline breakdown (bench/obs dashboard) ----
lazy!(COMMIT_STAGE: HistogramVec = register_histogram_vec!("vlpds_commit_stage_seconds", "Per-segment commit pipeline stages: seal_wait (oldest entry's enqueue -> PUT start), put (-> durable), apply_lock (finalizer waiting for shard apply locks), apply (SlateDB batches), ack (acks + repo views published)", &["stage"], latency_buckets()));
lazy!(PUTS_INFLIGHT: IntGauge = register_int_gauge!("vlpds_segment_puts_inflight", "Segment PUT attempts in flight (hedges included)"));
lazy!(REPO_CACHE: IntCounterVec = register_int_counter_vec!("vlpds_repo_cache_lookups_total", "Worker repo lookups for queued requests: hit (cached), miss (starts a cold load), loading (joins one in flight)", &["result"]));
lazy!(FORWARDS: IntCounterVec = register_int_counter_vec!("vlpds_forwards_total", "Forwarded requests by the owner's status class (5xx includes owner unreachable / past its TTFB deadline)", &["result"]));
lazy!(FORWARD_DURATION: Histogram = register_histogram!("vlpds_forward_seconds", "Forwarded request time to the owner's response head", latency_buckets()));
lazy!(FIREHOSE_EMIT_DELAY: Histogram = register_histogram!("vlpds_firehose_emit_delay_seconds", "Seq assignment of a merged batch's oldest event -> firehose emit", latency_buckets()));
lazy!(BUILD_INFO: IntGaugeVec = register_int_gauge_vec!("vlpds_build_info", "1, labeled with node id, git revision and whether the profiling feature is built in", &["node_id", "rev", "profiling"]));

// ---- process / runtime (the prometheus crate's process collector is Linux-only and off) ----
lazy!(PROCESS_RSS: IntGauge = register_int_gauge!("vlpds_process_resident_bytes", "Resident set size"));
lazy!(PROCESS_CPU: GaugeVec = register_gauge_vec!("vlpds_process_cpu_seconds_total", "CPU time consumed by mode (getrusage)", &["mode"]));
lazy!(PROCESS_THREADS: IntGauge = register_int_gauge!("vlpds_process_threads", "OS threads"));
lazy!(TOKIO_WORKERS: IntGauge = register_int_gauge!("vlpds_tokio_workers", "Tokio worker threads"));
lazy!(TOKIO_TASKS: IntGauge = register_int_gauge!("vlpds_tokio_alive_tasks", "Tokio tasks alive"));
lazy!(TOKIO_GLOBAL_QUEUE: IntGauge = register_int_gauge!("vlpds_tokio_global_queue_depth", "Tasks in the tokio injection queue"));
lazy!(TOKIO_BUSY: Gauge = register_gauge!("vlpds_tokio_busy_seconds_total", "Busy time summed over tokio workers (rate / workers = utilization)"));

/// Increments a gauge until dropped (in-flight counts that survive cancellation).
pub struct InflightGuard(&'static IntGauge);

impl InflightGuard {
    pub fn new(g: &'static LazyLock<IntGauge>) -> InflightGuard {
        g.inc();
        InflightGuard(g)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Records a forwarded request's outcome (forward.rs).
pub fn observe_forward(status: u16, start: std::time::Instant) {
    FORWARD_DURATION.observe(start.elapsed().as_secs_f64());
    let class = match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    };
    FORWARDS.with_label_values(&[class]).inc();
}

pub fn render() -> String {
    refresh_jemalloc();
    refresh_process();
    refresh_tokio();
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&prometheus::gather(), &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

#[cfg(not(feature = "jemalloc"))]
fn refresh_jemalloc() {}

#[cfg(feature = "jemalloc")]
fn refresh_jemalloc() {
    use tikv_jemalloc_ctl::{epoch, stats};
    if epoch::advance().is_err() {
        return;
    }
    for (name, v) in [
        ("allocated", stats::allocated::read()),
        ("active", stats::active::read()),
        ("resident", stats::resident::read()),
        ("mapped", stats::mapped::read()),
        ("retained", stats::retained::read()),
        ("metadata", stats::metadata::read()),
    ] {
        if let Ok(v) = v {
            JEMALLOC.with_label_values(&[name]).set(v as i64);
        }
    }
}

fn refresh_tokio() {
    let Ok(h) = tokio::runtime::Handle::try_current() else { return };
    let m = h.metrics();
    let n = m.num_workers();
    TOKIO_WORKERS.set(n as i64);
    TOKIO_TASKS.set(m.num_alive_tasks() as i64);
    TOKIO_GLOBAL_QUEUE.set(m.global_queue_depth() as i64);
    TOKIO_BUSY.set((0..n).map(|w| m.worker_total_busy_duration(w).as_secs_f64()).sum());
}

fn refresh_process() {
    if let Some((user, system)) = sys::cpu_seconds() {
        PROCESS_CPU.with_label_values(&["user"]).set(user);
        PROCESS_CPU.with_label_values(&["system"]).set(system);
    }
    if let Some((rss, threads)) = sys::rss_threads() {
        PROCESS_RSS.set(rss as i64);
        PROCESS_THREADS.set(threads as i64);
    }
}

/// Process stats without a libc dependency: getrusage (macOS and 64-bit
/// Linux share the layout but for `tv_usec`'s width), proc_pidinfo on macOS,
/// /proc/self/status on Linux.
#[cfg(all(any(target_os = "macos", target_os = "linux"), target_pointer_width = "64"))]
mod sys {
    #[repr(C)]
    struct Timeval {
        sec: i64,
        #[cfg(target_os = "macos")]
        usec: i32,
        #[cfg(target_os = "linux")]
        usec: i64,
    }
    #[repr(C)]
    struct Rusage {
        utime: Timeval,
        stime: Timeval,
        rest: [i64; 14],
    }
    extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }

    pub fn cpu_seconds() -> Option<(f64, f64)> {
        let mut r = std::mem::MaybeUninit::<Rusage>::zeroed();
        // RUSAGE_SELF = 0
        if unsafe { getrusage(0, r.as_mut_ptr()) } != 0 {
            return None;
        }
        let r = unsafe { r.assume_init() };
        let s = |t: &Timeval| t.sec as f64 + t.usec as f64 / 1e6;
        Some((s(&r.utime), s(&r.stime)))
    }

    #[cfg(target_os = "macos")]
    pub fn rss_threads() -> Option<(u64, u64)> {
        /// <sys/proc_info.h> struct proc_taskinfo
        #[repr(C)]
        struct ProcTaskinfo {
            virtual_size: u64,
            resident_size: u64,
            total_user: u64,
            total_system: u64,
            threads_user: u64,
            threads_system: u64,
            policy: i32,
            faults: i32,
            pageins: i32,
            cow_faults: i32,
            messages_sent: i32,
            messages_received: i32,
            syscalls_mach: i32,
            syscalls_unix: i32,
            csw: i32,
            threadnum: i32,
            numrunning: i32,
            priority: i32,
        }
        extern "C" {
            fn proc_pidinfo(pid: i32, flavor: i32, arg: u64, buffer: *mut ProcTaskinfo, size: i32) -> i32;
        }
        const PROC_PIDTASKINFO: i32 = 4;
        let size = std::mem::size_of::<ProcTaskinfo>() as i32;
        let mut ti = std::mem::MaybeUninit::<ProcTaskinfo>::zeroed();
        let n = unsafe { proc_pidinfo(std::process::id() as i32, PROC_PIDTASKINFO, 0, ti.as_mut_ptr(), size) };
        if n != size {
            return None;
        }
        let ti = unsafe { ti.assume_init() };
        Some((ti.resident_size, ti.threadnum.max(0) as u64))
    }

    #[cfg(target_os = "linux")]
    pub fn rss_threads() -> Option<(u64, u64)> {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        let field = |name: &str| {
            s.lines()
                .find_map(|l| l.strip_prefix(name))
                .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        };
        Some((field("VmRSS:")? * 1024, field("Threads:")?))
    }
}

#[cfg(not(all(any(target_os = "macos", target_os = "linux"), target_pointer_width = "64")))]
mod sys {
    pub fn cpu_seconds() -> Option<(f64, f64)> {
        None
    }
    pub fn rss_threads() -> Option<(u64, u64)> {
        None
    }
}

/// Attaches the SlateDB stats bridge (feature `slatedb-metrics`) to a DB.
pub fn with_slatedb_metrics<P: Into<slatedb::object_store::path::Path>>(
    b: slatedb::DbBuilder<P>,
) -> slatedb::DbBuilder<P> {
    #[cfg(feature = "slatedb-metrics")]
    let b = b.with_metrics_recorder(slatedb_bridge::RECORDER.clone());
    b
}

/// The SlateDB metrics bridge, for components built outside a `DbBuilder`
/// (the deferred compactor in partition.rs).
#[cfg(feature = "slatedb-metrics")]
pub fn slatedb_recorder() -> std::sync::Arc<dyn slatedb_common::metrics::MetricsRecorder> {
    slatedb_bridge::RECORDER.clone()
}

/// SlateDB's metrics recorder, bridged into the default registry as
/// `slatedb_*` (dots -> underscores; counters get `_total`). Every shard DB
/// registers the same names: counters and histograms are shared (they sum
/// naturally), gauges add each DB's delta so the exported value is the sum
/// over this node's open DBs (a closed DB's handle subtracts its share).
#[cfg(feature = "slatedb-metrics")]
mod slatedb_bridge {
    use parking_lot::Mutex;
    use prometheus::{HistogramOpts, Opts};
    use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, LazyLock};

    pub static RECORDER: LazyLock<Arc<dyn MetricsRecorder>> = LazyLock::new(|| Arc::new(Bridge::default()));

    #[derive(Default)]
    struct Bridge {
        counters: Mutex<HashMap<String, Option<super::IntCounterVec>>>,
        gauges: Mutex<HashMap<String, Option<super::IntGaugeVec>>>,
        hists: Mutex<HashMap<String, Option<super::HistogramVec>>>,
    }

    fn prom_name(name: &str) -> String {
        name.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect()
    }

    fn help(description: &str, name: &str) -> String {
        if description.is_empty() { name.to_string() } else { description.to_string() }
    }

    /// One vec per name (first registration's label keys win; a mismatching
    /// later registration gets a no-op handle).
    fn vec<V: Clone>(
        map: &Mutex<HashMap<String, Option<V>>>,
        name: &str,
        labels: &[(&str, &str)],
        make: impl FnOnce(&[&str]) -> prometheus::Result<V>,
    ) -> Option<V> {
        let mut m = map.lock();
        m.entry(name.to_string())
            .or_insert_with(|| {
                let keys: Vec<&str> = labels.iter().map(|(k, _)| *k).collect();
                make(&keys).ok()
            })
            .clone()
    }

    struct Noop;
    impl CounterFn for Noop {
        fn increment(&self, _: u64) {}
    }
    impl GaugeFn for Noop {
        fn set(&self, _: i64) {}
    }
    impl UpDownCounterFn for Noop {
        fn increment(&self, _: i64) {}
    }
    impl HistogramFn for Noop {
        fn record(&self, _: f64) {}
    }

    struct Counter(prometheus::IntCounter);
    impl CounterFn for Counter {
        fn increment(&self, v: u64) {
            self.0.inc_by(v);
        }
    }

    /// This DB's share of a summed gauge.
    struct Share {
        g: prometheus::IntGauge,
        last: AtomicI64,
    }
    impl GaugeFn for Share {
        fn set(&self, v: i64) {
            self.g.add(v - self.last.swap(v, Ordering::Relaxed));
        }
    }
    impl UpDownCounterFn for Share {
        fn increment(&self, v: i64) {
            self.last.fetch_add(v, Ordering::Relaxed);
            self.g.add(v);
        }
    }
    impl Drop for Share {
        fn drop(&mut self) {
            self.g.sub(self.last.load(Ordering::Relaxed));
        }
    }

    struct Hist(prometheus::Histogram);
    impl HistogramFn for Hist {
        fn record(&self, v: f64) {
            self.0.observe(v);
        }
    }

    /// Drops per-instance ids (compactor `worker_id` ULIDs: new series every
    /// start); the instances' values aggregate instead.
    fn keep<'a>(labels: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        labels.iter().filter(|(k, _)| !k.ends_with("_id")).copied().collect()
    }

    fn values<'a>(labels: &'a [(&'a str, &'a str)]) -> Vec<&'a str> {
        labels.iter().map(|(_, v)| *v).collect()
    }

    impl Bridge {
        fn share(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Option<Share> {
            let labels = &keep(labels);
            let v = vec(&self.gauges, name, labels, |keys| {
                prometheus::register_int_gauge_vec!(Opts::new(prom_name(name), help(description, name)), keys)
            })?;
            let g = v.get_metric_with_label_values(&values(labels)).ok()?;
            Some(Share { g, last: AtomicI64::new(0) })
        }
    }

    impl MetricsRecorder for Bridge {
        fn register_counter(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn CounterFn> {
            let labels = &keep(labels);
            let v = vec(&self.counters, name, labels, |keys| {
                prometheus::register_int_counter_vec!(Opts::new(prom_name(name) + "_total", help(description, name)), keys)
            });
            match v.and_then(|v| v.get_metric_with_label_values(&values(labels)).ok()) {
                Some(c) => Arc::new(Counter(c)),
                None => Arc::new(Noop),
            }
        }

        fn register_gauge(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn GaugeFn> {
            match self.share(name, description, labels) {
                Some(s) => Arc::new(s),
                None => Arc::new(Noop),
            }
        }

        fn register_up_down_counter(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn UpDownCounterFn> {
            match self.share(name, description, labels) {
                Some(s) => Arc::new(s),
                None => Arc::new(Noop),
            }
        }

        fn register_histogram(&self, name: &str, description: &str, labels: &[(&str, &str)], boundaries: &[f64]) -> Arc<dyn HistogramFn> {
            let labels = &keep(labels);
            let v = vec(&self.hists, name, labels, |keys| {
                prometheus::register_histogram_vec!(
                    HistogramOpts::new(prom_name(name), help(description, name)).buckets(boundaries.to_vec()),
                    keys
                )
            });
            match v.and_then(|v| v.get_metric_with_label_values(&values(labels)).ok()) {
                Some(h) => Arc::new(Hist(h)),
                None => Arc::new(Noop),
            }
        }
    }
}

/// XRPC method name from a request path, for metric labels.
pub fn method_label(path: &str) -> &str {
    match path.strip_prefix("/xrpc/") {
        Some(m) if !m.is_empty() => m,
        _ => "other",
    }
}
