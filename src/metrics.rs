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

// ---- runtime ----
lazy!(RUNTIME_LATE: Histogram = register_histogram!("vlpds_runtime_tick_late_seconds", "How late a 10 ms ticker on the tokio runtime wakes (runtime threads blocked or starved)", exponential_buckets(0.001, 2.0, 12).unwrap()));
lazy!(RUNTIME_LATE_TOTAL: prometheus::Counter = prometheus::register_counter!("vlpds_runtime_late_seconds_total", "Sum of the 10 ms ticker's lateness: time the runtime could not run a ready task promptly"));

// ---- HTTP ----
lazy!(HTTP_REQUESTS: IntCounterVec = register_int_counter_vec!("vlpds_http_requests_total", "XRPC requests by method and status", &["method", "status"]));
lazy!(HTTP_DURATION: HistogramVec = register_histogram_vec!("vlpds_http_request_duration_seconds", "XRPC request latency", &["method"], latency_buckets()));
lazy!(HTTP_INFLIGHT: IntGauge = register_int_gauge!("vlpds_http_requests_inflight", "XRPC requests in flight"));
lazy!(HTTP_CLIENT_CONNECTS: IntCounterVec = register_int_counter_vec!("vlpds_http_client_connects_total", "New outbound connections by client role (peer, public, guarded); should stay flat under steady load", &["role"]));
lazy!(HTTP_CLIENT_POOL_WAITS: IntCounterVec = register_int_counter_vec!("vlpds_http_client_pool_waits_total", "Proxy requests that waited for an upstream connection at the per-host cap (src/http.rs h1::MAX_CONNS)", &["role"]));
lazy!(HTTP_SERVER_CONNECTIONS: IntCounter = register_int_counter!("vlpds_http_server_connections_total", "Accepted inbound TCP connections"));
lazy!(HTTP_SERVER_OPEN: IntGauge = register_int_gauge!("vlpds_http_server_connections_open", "Inbound connections open"));
lazy!(HTTP_SERVER_ACCEPT_ERRORS: IntCounter = register_int_counter!("vlpds_http_server_accept_errors_total", "Failed accepts on a listener (retried after 50 ms; e.g. out of file descriptors)"));
lazy!(HTTP_SERVER_ACTIVE: IntGaugeVec = register_int_gauge_vec!("vlpds_http_server_active_requests", "Inbound requests (h2: streams) awaiting their response head, by HTTP version", &["version"]));
lazy!(RATE_LIMITED: IntCounter = register_int_counter!("vlpds_rate_limited_total", "Requests rejected with 429 RateLimitExceeded"));
lazy!(WRITES_SHED: IntCounter = register_int_counter!("vlpds_writes_shed_total", "Write requests rejected by admission control (503)"));
lazy!(ARGON2_SHED: IntCounter = register_int_counter!("vlpds_argon2_shed_total", "Password checks/hashes (createSession, createAccount, OAuth sign-in, password changes) answered 503 Overloaded: every Argon2 permit stayed busy for 2 s"));
lazy!(PROXY_REJECTED: IntCounterVec = register_int_counter_vec!("vlpds_proxy_rejected_total", "Proxied (AppView/service) requests refused before forwarding, by reason (account_cap: 429 at 64 in flight for one account on its owner)", &["reason"]));
lazy!(HTTP_STALLED_BODIES: IntCounter = register_int_counter!("vlpds_http_stalled_bodies_total", "Proxied/forwarded response bodies dropped because the client stopped reading for 30 s (write-progress deadline, src/http.rs stall)"));

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
lazy!(REPO_LOAD_DURATION: Histogram = register_histogram!("vlpds_repo_load_seconds", "Cold repo load latency (head, account, M/ prefetch, MST root + the first request's paths, blob refs)", latency_buckets()));
lazy!(REPO_EVICTIONS: IntCounter = register_int_counter!("vlpds_repo_evictions_total", "Repos evicted from worker caches"));
lazy!(REPO_CACHE_BYTES: IntGaugeVec = register_int_gauge_vec!("vlpds_repo_cache_bytes", "Approximate heap of the repos a worker holds (their loaded MST paths)", &["worker"]));
lazy!(REPO_PRELOADS: IntCounterVec = register_int_counter_vec!("vlpds_repo_preloads_total", "Preloads of recently written repos after a shard open, by result", &["result"]));
lazy!(LAZY_MST_READS: IntCounterVec = register_int_counter_vec!("vlpds_lazy_mst_reads_total", "Lazy MST store reads by kind (node: M/ point read; leaf: R/ range scan), outside a prefetch", &["kind"]));
lazy!(LAZY_MST_PREFETCH_BYTES: Histogram = register_histogram!("vlpds_lazy_mst_prefetch_bytes", "Bytes of a repo's M/ range read ahead by one scan on a lazy cold open", exponential_buckets(1024.0, 4.0, 10).unwrap()));
lazy!(LAZY_MST_FETCHES: IntCounterVec = register_int_counter_vec!("vlpds_lazy_mst_fetches_total", "Path loads a repo worker handed to the blocking pool before applying writes (lazy MSTs), by result", &["result"]));
lazy!(LAZY_MST_FALLBACKS: IntCounterVec = register_int_counter_vec!("vlpds_lazy_mst_fallbacks_total", "Lazy MST opens rebuilt from all of the repo's records, by reason (missing: no persisted root; invalid: a node or rebuilt subtree didn't match its link)", &["reason"]));
lazy!(LAZY_MST_UNLOADS: IntCounter = register_int_counter!("vlpds_lazy_mst_unloads_total", "Repos whose loaded MST paths were dropped (back to the root) to keep the worker's path cache in its byte budget"));
lazy!(WRITES_ABANDONED: IntCounter = register_int_counter!("vlpds_writes_abandoned_total", "Forwarded writes answered 503 RepoLoading before their worker started them (never applied; the forwarding node retries)"));

// ---- partitions / log ----
lazy!(CHECKPOINT_SHARD: Histogram = register_histogram!("vlpds_checkpoint_shard_seconds", "One shard's checkpoint (applied marker + memtable flush)", latency_buckets()));
lazy!(SEQ_QUEUE: IntGaugeVec = register_int_gauge_vec!("vlpds_sequencer_queue_depth", "Log entries waiting for the sequencer", &["partition"]));
lazy!(SEGMENTS: IntCounterVec = register_int_counter_vec!("vlpds_segments_total", "Segments made durable", &["partition"]));
lazy!(SEGMENT_BYTES: Histogram = register_histogram!("vlpds_segment_bytes", "Segment object size", exponential_buckets(1024.0, 2.0, 14).unwrap()));
lazy!(SEGMENT_EVENTS: Histogram = register_histogram!("vlpds_segment_events", "Firehose events per segment", exponential_buckets(1.0, 2.0, 16).unwrap()));
lazy!(SEGMENT_BYTES_TOTAL: IntCounter = register_int_counter!("vlpds_segment_bytes_total", "Bytes written to the log (uncompressed segments)"));
lazy!(SEGMENT_STALL_SEALS: IntCounter = register_int_counter!("vlpds_segment_stall_seals_total", "Segments sealed early because the oldest PUT in flight stalled (over 2x the recent PUT latency)"));
lazy!(SEGMENT_STORED_BYTES_TOTAL: IntCounter = register_int_counter!("vlpds_segment_stored_bytes_total", "Bytes of segment objects PUT (after compression)"));
lazy!(SEGMENT_COMPRESS: Histogram = register_histogram!("vlpds_segment_compress_seconds", "CPU time to zstd one segment body before its PUT", latency_buckets()));
lazy!(SEGMENT_DECODES: IntCounter = register_int_counter!("vlpds_segment_decodes_total", "Compressed segments decompressed by readers (replay, follower catch-up, backfill, merger read-back)"));
lazy!(PUT_DURATION: HistogramVec = register_histogram_vec!("vlpds_segment_put_seconds", "Segment PUT latency until durable (incl. hedges/retries)", &["partition"], latency_buckets()));
lazy!(PUT_ATTEMPTS: IntCounterVec = register_int_counter_vec!("vlpds_segment_put_attempts_total", "Segment PUT attempts by result", &["result"]));
lazy!(PUT_HEDGES: IntCounter = register_int_counter!("vlpds_segment_put_hedges_total", "Hedged (duplicate) segment PUTs started"));
lazy!(APPLY_DURATION: Histogram = register_histogram!("vlpds_state_apply_seconds", "SlateDB batch apply latency per segment", latency_buckets()));
lazy!(COMMIT_LATENCY: Histogram = register_histogram!("vlpds_commit_durable_seconds", "Commit enqueue -> durable+applied+acked", latency_buckets()));
lazy!(WATERMARK_LAG: IntGaugeVec = register_int_gauge_vec!("vlpds_watermark_lag_microseconds", "now - partition watermark", &["partition"]));
lazy!(LAST_SEQ: IntGaugeVec = register_int_gauge_vec!("vlpds_partition_durable_seq", "Last durable seq per partition", &["partition"]));
lazy!(REPLAYED_SEGMENTS: IntCounter = register_int_counter!("vlpds_recovery_replayed_segments_total", "Log segments replayed when opening shards (previous owners' log tails after a crash or takeover)"));

// ---- firehose ----
lazy!(FIREHOSE_EVENTS: IntCounter = register_int_counter!("vlpds_firehose_events_total", "Events emitted by the merger"));
lazy!(FIREHOSE_BATCH: Histogram = register_histogram!("vlpds_firehose_merge_batch_events", "Events per merged batch", exponential_buckets(1.0, 2.0, 16).unwrap()));
lazy!(FIREHOSE_SUBSCRIBERS: IntGauge = register_int_gauge!("vlpds_firehose_subscribers", "Connected subscribeRepos clients"));
lazy!(FIREHOSE_RING_BYTES: IntGauge = register_int_gauge!("vlpds_firehose_ring_bytes", "Bytes held in the in-memory firehose ring"));
lazy!(FIREHOSE_DISCONNECTS: IntCounterVec = register_int_counter_vec!("vlpds_firehose_disconnects_total", "Subscriber disconnects by reason", &["reason"]));
lazy!(FIREHOSE_SENT: IntCounter = register_int_counter!("vlpds_firehose_frames_sent_total", "Frames sent to subscribers"));
lazy!(FIREHOSE_MERGE_QUEUE_BYTES: IntGauge = register_int_gauge!("vlpds_firehose_merge_queue_bytes", "Frame bytes queued in the merger waiting for the min watermark"));
lazy!(FIREHOSE_MERGE_QUEUE_BUDGET: IntGauge = register_int_gauge!("vlpds_firehose_merge_queue_budget_bytes", "Configured byte budget of the merger's queues (--firehose-merge-queue-mb); past it a log spills to S3 read-back"));
lazy!(FIREHOSE_MAX_LAG: IntGauge = register_int_gauge!("vlpds_firehose_max_lag_bytes", "Configured subscriber lag past which a live subscriber is cut off with ConsumerTooSlow (--firehose-max-lag-mb)"));
lazy!(FIREHOSE_SPILLS: IntCounter = register_int_counter!("vlpds_firehose_merge_spills_total", "Logs the merger stopped queueing (over budget) and reads back from S3"));
lazy!(FIREHOSE_SPILL_SEGMENTS: IntCounter = register_int_counter!("vlpds_firehose_merge_spill_segments_total", "Segments the merger read back from S3 for spilled logs"));
lazy!(FIREHOSE_SENT_BYTES: IntCounter = register_int_counter!("vlpds_firehose_bytes_sent_total", "Websocket bytes written to subscribers (frames + headers)"));
lazy!(FIREHOSE_BACKFILL_GETS: IntCounter = register_int_counter!("vlpds_firehose_backfill_gets_total", "Segment GETs made by cursor backfill readers"));
lazy!(FIREHOSE_BACKFILL_CACHE: IntCounterVec = register_int_counter_vec!("vlpds_firehose_backfill_cache_total", "Backfill segment cache lookups (hit includes joining a GET in flight)", &["result"]));
lazy!(FIREHOSE_BACKFILL_EVENTS: IntCounter = register_int_counter!("vlpds_firehose_backfill_events_total", "Events sent to subscribers from S3 backfill"));
lazy!(FIREHOSE_BACKFILLS: IntGaugeVec = register_int_gauge_vec!("vlpds_firehose_backfills", "Cursor backfills running, and waiting for a slot (--firehose-max-backfills)", &["state"]));
lazy!(FIREHOSE_REJECTED: IntCounterVec = register_int_counter_vec!("vlpds_firehose_rejected_total", "subscribeRepos connections refused before the upgrade, by reason (per_ip: --firehose-max-per-ip)", &["reason"]));
lazy!(SYNC_EXPORTS: IntGaugeVec = register_int_gauge_vec!("vlpds_sync_exports", "getRepo exports streaming, and waiting for a slot (--max-exports)", &["state"]));
lazy!(SYNC_EXPORTS_ENDED: IntCounterVec = register_int_counter_vec!("vlpds_sync_exports_ended_total", "getRepo exports by how they ended: done, client_gone, stalled (the client read nothing for --export-stall-secs), error, shed (no slot within 10 s: 503)", &["reason"]));
lazy!(FIREHOSE_BACKFILL_RETRIES: IntCounterVec = register_int_counter_vec!("vlpds_firehose_backfill_retries_total", "Cursor backfill retries: seek (a log seek re-run after retention pruned the log's head below the cursor under it), pruned (a whole backfill re-run for the same reason) or error (S3)", &["reason"]));
lazy!(LOG_LIVE_BYTES: IntGauge = register_int_gauge!("vlpds_log_live_ring_bytes", "Segment bytes pinned by the node log's live ring (peer streams)"));
lazy!(LOG_STREAM_LAGGED: IntCounter = register_int_counter!("vlpds_log_stream_lagged_total", "Peer log streams dropped for falling behind the live ring (they catch up from S3)"));

// ---- log retention (retention.rs) ----
lazy!(RETENTION_DELETED_OBJECTS: IntCounterVec = register_int_counter_vec!("vlpds_retention_deleted_objects_total", "Log objects deleted by retention, by log (own, dead) or fence (a dead log's fence past --fence-retention)", &["log"]));
lazy!(RETENTION_DELETED_BYTES: IntCounterVec = register_int_counter_vec!("vlpds_retention_deleted_bytes_total", "Log bytes deleted by retention, by log (own, dead)", &["log"]));
lazy!(RETENTION_PRUNED_SEQ: IntGauge = register_int_gauge!("vlpds_retention_pruned_seq", "Highest seq this node has deleted from any log (older cursors get OutdatedCursor)"));
lazy!(RETENTION_REPLAY_HOLD: IntGauge = register_int_gauge!("vlpds_retention_replay_hold_segments", "Segments of our log kept only because a crash replay could still need them (durable ordinal - replay floor)"));
lazy!(RETENTION_LISTS_SKIPPED: IntCounterVec = register_int_counter_vec!("vlpds_retention_lists_skipped_total", "Retention LISTs a pass skipped because nothing could be due yet (own: our log's first segment is inside the window or held by the replay floor; dead: no dead logs but retired ones whose fences aren't due, live set unchanged); each still runs at least hourly", &["list"]));
lazy!(RETENTION_TICKS: IntCounterVec = register_int_counter_vec!("vlpds_retention_ticks_total", "Retention passes by result", &["result"]));

// ---- proxy ----
lazy!(PROXY_CACHE: IntCounterVec = register_int_counter_vec!("vlpds_proxy_cache_total", "Proxy fast-path cache lookups", &["result"]));
lazy!(READ_AFTER_WRITE: IntCounterVec = register_int_counter_vec!("vlpds_proxy_read_after_write_total", "Proxied reads with an AppView rev: how the requester's records since it were found (log_nothing, log_records, store_read) and what was returned (munged, unchanged, failed)", &["result"]));

// ---- in-memory caches (caches.rs) ----
lazy!(CACHE_ENTRIES: IntGaugeVec = register_int_gauge_vec!("vlpds_cache_entries", "Entries held per in-memory cache", &["cache"]));
lazy!(CACHE_BYTES: IntGaugeVec = register_int_gauge_vec!("vlpds_cache_bytes", "Approximate bytes held per in-memory cache (entries x estimated entry size)", &["cache"]));
lazy!(CACHE_CAPACITY: IntGaugeVec = register_int_gauge_vec!("vlpds_cache_capacity_entries", "Entry cap per in-memory cache (--cache-budget-mb, --cache-entries)", &["cache"]));

// ---- cluster ----
lazy!(FORWARDED: IntCounter = register_int_counter!("vlpds_requests_forwarded_total", "Requests proxied to the partition owner"));
lazy!(WRITE_RETRIES: IntCounterVec = register_int_counter_vec!("vlpds_write_retries_total", "Repo writes the entry node resent after a not-applied 503, by reason (loading: RepoLoading; moved: ShardMoved)", &["reason"]));
lazy!(OWNED_PARTITIONS: IntGauge = register_int_gauge!("vlpds_owned_partitions", "Partitions this node owns"));
lazy!(LEASE_EVENTS: IntCounterVec = register_int_counter_vec!("vlpds_lease_events_total", "Partition lease transitions", &["event"]));
lazy!(OBJ_REQUESTS: IntCounterVec = register_int_counter_vec!("vlpds_object_store_requests_total", "Object-store requests sent to the store, by billable op (put, put_create, put_cas, get, get_range, head, list pages, delete, delete_batch, copy, mpu_*), key component, client pool and result (ok, not_found, precondition, timeout, error, cancelled: the caller dropped it unanswered) (objstats.rs)", &["op", "component", "client", "result"]));
lazy!(OBJ_BYTES: IntCounterVec = register_int_counter_vec!("vlpds_object_store_bytes_total", "Object-store payload bytes by direction (up, down), key component and client pool", &["dir", "component", "client"]));
lazy!(OBJ_INFLIGHT: IntGaugeVec = register_int_gauge_vec!("vlpds_object_store_inflight", "Object-store requests holding an in-flight permit, by client pool (log, state, ctl) and lane (main; reserved: log writes, ctl lease writes) (objlimit.rs)", &["client", "lane"]));
lazy!(OBJ_INFLIGHT_LIMIT: IntGaugeVec = register_int_gauge_vec!("vlpds_object_store_inflight_limit", "In-flight permits of each object-store client pool and lane (--store-inflight, --log-store-inflight)", &["client", "lane"]));
lazy!(OBJ_PERMIT_WAITS: IntCounterVec = register_int_counter_vec!("vlpds_object_store_permit_waits_total", "Object-store requests that found every in-flight permit of their pool and lane taken and queued", &["client", "lane"]));
lazy!(OBJ_PERMIT_WAIT_SECONDS: HistogramVec = register_histogram_vec!("vlpds_object_store_permit_wait_seconds", "How long object-store requests that queued for an in-flight permit waited", &["client", "lane"], latency_buckets()));
lazy!(CLUSTER_STORE_REQUESTS: IntCounterVec = register_int_counter_vec!("vlpds_cluster_store_requests_total", "Control-plane object-store requests (leases, assignments, writer claims, fences) by op", &["op"]));
lazy!(CLUSTER_NUDGES: IntCounterVec = register_int_counter_vec!("vlpds_cluster_nudges_total", "Early control-plane steps asked of peers after a release (sent, failed) or by peers (received)", &["dir"]));
lazy!(CLUSTER_LONE_SKIPS: IntCounterVec = register_int_counter_vec!("vlpds_cluster_lone_skips_total", "Control-plane LISTs a lone node skipped (nodes: membership reused, listed at least once per TTL; assign: assignments unchanged but by our own writes, listed every 25 steps)", &["list"]));
lazy!(CLUSTER_STORE_TIMEOUTS: IntCounterVec = register_int_counter_vec!("vlpds_cluster_store_timeouts_total", "Control-plane object-store calls a step gave up on at their deadline (min(TTL, 5 s)); the step retries next tick", &["op"]));
lazy!(COMPACTION_POLL_MODE: IntCounterVec = register_int_counter_vec!("vlpds_compaction_poll_switches_total", "Shard compactors switched to fast polls (deep L0) or back to slow (--compaction-polling adaptive)", &["mode"]));
lazy!(LAYOUT_VERSION: IntGauge = register_int_gauge!("vlpds_shard_layout_version", "Version of the shard layout this node routes by (grows with each split/merge)"));
lazy!(RESHARD_EVENTS: IntCounterVec = register_int_counter_vec!("vlpds_reshard_events_total", "Shard split/merge steps this node performed: planned, frozen (parents closed), split / merged (layout flipped), aborted", &["event"]));
lazy!(RESHARD_SECONDS: Histogram = register_histogram!("vlpds_reshard_drive_seconds", "Driver time from every parent frozen to the children open (clone, assignments, flip, open)", exponential_buckets(0.01, 2.0, 14).unwrap()));
lazy!(RESHARD_GC_PASSES: IntCounterVec = register_int_counter_vec!("vlpds_reshard_gc_passes_total", "Retired-state GC passes (src/reshard_gc.rs) by result (ok, error), skipped ones not counted (vlpds_reshard_gc_skipped_passes_total); the dir/assign half runs on the owner of slot 0's shard only", &["result"]));
lazy!(RESHARD_GC_DELETED: IntCounterVec = register_int_counter_vec!("vlpds_reshard_gc_deleted_total", "Retired-state GC deletes: state_dirs (retired split/merge parents, aborted ops' clones), state_objects (their objects), assign_records", &["kind"]));
lazy!(RESHARD_GC_RETIRED: IntGaugeVec = register_int_gauge_vec!("vlpds_reshard_gc_retired_dirs", "State dirs of shards no longer in the layout, as of the last GC pass (leader only): total, and the ones checked this pass by state: deletable, checkpoint (a clone, reader or backup still holds a checkpoint in it), grace (changed within --reshard-gc-grace), referenced (a manifest lists its SSTs although it holds no checkpoint: never deleted, investigate), other (no manifest, or an owner)", &["state"]));
lazy!(RESHARD_GC_SKIPPED: IntCounter = register_int_counter!("vlpds_reshard_gc_skipped_passes_total", "Retired-state GC dir passes skipped after the layout GET (no LISTs) because the layout is unchanged since a full pass that found no dir or assign/ record out of the layout; a full pass still runs at least hourly, and at once after any layout change. Not counted in vlpds_reshard_gc_passes_total"));
lazy!(RESHARD_GC_ORPHAN_ASSIGNS: IntGauge = register_int_gauge!("vlpds_reshard_gc_orphan_assign_records", "assign/ records of shards out of the layout whose state dir is gone, as of the last GC pass (deleted by it, bounded per pass)"));
lazy!(FORCED_COMPACTIONS: IntCounterVec = register_int_counter_vec!("vlpds_forced_compactions_total", "Compactions this node submitted for its shards, by kind (detach: rewrite SSTs inherited from a split/merge parent; full: --full-compaction-every) and result (submitted, completed, failed: retried next pass)", &["kind", "result"]));
lazy!(SHARDS_INHERITED: IntGauge = register_int_gauge!("vlpds_shards_with_inherited_ssts", "Shards open here still reading SSTs of a split/merge parent (external SSTs): each pins its parent's state dir until a forced compaction rewrites them"));

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
lazy!(PROCESS_CPU: prometheus::CounterVec = prometheus::register_counter_vec!("vlpds_process_cpu_seconds_total", "CPU time consumed by mode (getrusage)", &["mode"]));
lazy!(PROCESS_THREADS: IntGauge = register_int_gauge!("vlpds_process_threads", "OS threads"));
lazy!(TOKIO_WORKERS: IntGauge = register_int_gauge!("vlpds_tokio_workers", "Tokio worker threads"));
lazy!(TOKIO_TASKS: IntGauge = register_int_gauge!("vlpds_tokio_alive_tasks", "Tokio tasks alive"));
lazy!(TOKIO_GLOBAL_QUEUE: IntGauge = register_int_gauge!("vlpds_tokio_global_queue_depth", "Tasks in the tokio injection queue"));
lazy!(TOKIO_BUSY: prometheus::Counter = prometheus::register_counter!("vlpds_tokio_busy_seconds_total", "Busy time summed over tokio workers (rate / workers = utilization)"));

// ---- leases, fail-stops, takeovers (ops/RUNBOOK.md) ----
/// `vlpds_lease_renew_seconds`, plus the same round trip as a fraction of
/// the configured TTL (`vlpds_lease_renew_ttl_ratio`, recorded once
/// [`export_lease_config`] has set the TTL): alert thresholds like "over
/// 0.2 x TTL" then work at any --lease-ttl-ms (ops/alerts.yml).
pub struct LeaseRenewHistogram {
    secs: Histogram,
    ttl_ratio: Histogram,
}

impl LeaseRenewHistogram {
    pub fn observe(&self, secs: f64) {
        self.secs.observe(secs);
        let ttl = LEASE_TTL.get();
        if ttl > 0.0 {
            self.ttl_ratio.observe(secs / ttl);
        }
    }

    pub fn get_sample_count(&self) -> u64 {
        self.secs.get_sample_count()
    }
}

pub static LEASE_RENEW_SECONDS: LazyLock<LeaseRenewHistogram> = LazyLock::new(|| LeaseRenewHistogram {
    secs: register_histogram!("vlpds_lease_renew_seconds", "Node lease renewal round trip (the CAS PUT of nodes/{node_id}), answered or failed. Validity ends TTL - skew after a renewal's send time, so round trips over 0.4 x TTL (4 s at the default TTL) open a gap and the node fail-stops", exponential_buckets(0.001, 2.0, 14).unwrap()).unwrap(),
    ttl_ratio: register_histogram!("vlpds_lease_renew_ttl_ratio", "Node lease renewal round trip as a fraction of the lease TTL (vlpds_lease_renew_seconds / vlpds_lease_ttl_seconds). Over 0.4 the node's validity gaps and it fail-stops", vec![0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0]).unwrap(),
});
lazy!(LEASE_TTL: Gauge = register_gauge!("vlpds_lease_ttl_seconds", "Configured node lease TTL (--lease-ttl-ms). Renewal ceiling = 0.4 x TTL; a crashed node's shards are taken over after about TTL + skew"));
lazy!(LEASE_RENEW_INTERVAL: Gauge = register_gauge!("vlpds_lease_renew_interval_seconds", "Configured node lease renewal interval (TTL / 5)"));
lazy!(LEASE_SKEW: Gauge = register_gauge!("vlpds_lease_skew_seconds", "Configured clock-skew margin of the node lease (TTL / 5): validity ends TTL - skew after a renewal's send time"));
lazy!(LEASE_RENEW_ERRORS: IntCounterVec = register_int_counter_vec!("vlpds_lease_renew_errors_total", "Failed node lease renewals by kind: timeout / error (retried next interval), conflict (someone rewrote our lease: fail-stop), lapsed (validity ended before the renewal: fail-stop)", &["kind"]));
lazy!(LEASE_VALIDITY: GaugeVec = register_gauge_vec!("vlpds_lease_validity_seconds", "Seconds until this node's own lease validity ends (TTL - skew after the send time of its last successful renewal), computed at scrape. Normally between TTL - skew - one renew interval and TTL - skew; negative = lapsed", &["node_id"]));
lazy!(PEER_TAKEOVERS: IntCounterVec = register_int_counter_vec!("vlpds_peer_takeovers_total", "Log incarnations this node fenced because they ended without fencing themselves (crash, kill, fail-stop, partition): peer = a dead peer's log, before taking its shards; restart = our own previous incarnation's, at startup. A graceful stop fences its own log and is not counted", &["reason"]));
lazy!(PROCESS_START: Gauge = register_gauge!("vlpds_process_start_time_seconds", "Start time of this process since the Unix epoch, in seconds"));
lazy!(PROCESS_START_STD: Gauge = register_gauge!("process_start_time_seconds", "Start time of the process since unix epoch in seconds."));
lazy!(LAST_EXIT: IntGaugeVec = register_int_gauge_vec!("vlpds_last_exit_reason_info", "1, labeled with how the previous process using this exit-state file ended (lifecycle.rs): a fail-stop reason with its exit code, clean, error, crash (no exit recorded: SIGKILL, OOM kill, abort, host loss) or none (first run, or no exit-state file)", &["reason", "code"]));
lazy!(LAST_EXIT_TIME: Gauge = register_gauge!("vlpds_last_exit_time_seconds", "When the previous process recorded its exit (Unix seconds; 0 if unknown)"));
lazy!(FEATURE_LEVEL: IntGaugeVec = register_int_gauge_vec!("vlpds_feature_level", "Feature levels (version.rs): active = the cluster's active level as this node last read cluster/version (what writers emit), binary_min / binary_max = the levels this build can run. binary_max > active on every node = a finalize is available", &["kind"]));
lazy!(FORMAT_ERRORS: IntCounterVec = register_int_counter_vec!("vlpds_format_errors_total", "Decodes that failed on an unknown or malformed format marker (segment: magic/codec; log_stream: message type, skipped; applied_marker: meta/applied2; cluster_version / control_object: unreadable control JSON). Any is a node of a newer level writing early, or corruption", &["format"]));
lazy!(SIGNATURE_VERIFY_FAILURES: IntCounterVec = register_int_counter_vec!("vlpds_signature_verify_failures_total", "Signatures that failed verification right after signing (never emitted; crypto.rs), by purpose (commit, service_auth, oauth_token, plc_operation; key_load: a loaded key's scalar no longer derives its public key). Any is a suspected memory/CPU fault; 3 within a minute fail-stop the node (signature_fault)", &["purpose"]));

// ---- shard opens / replay ----
lazy!(SHARDS_OPENED: IntCounterVec = register_int_counter_vec!("vlpds_shards_opened_total", "Shard opens (acquire, adopt, takeover, reshard children) by result", &["result"]));
lazy!(SHARD_OPEN_SECONDS: HistogramVec = register_histogram_vec!("vlpds_shard_open_seconds", "One batch of shard opens until served (SlateDB open + log replay + flush), by kind: replay (it replayed segments: a takeover after a crash) or clean (nothing to replay: a handback)", &["kind"], exponential_buckets(0.01, 2.0, 14).unwrap()));
lazy!(REPLAY_SECONDS: Histogram = register_histogram!("vlpds_recovery_replay_seconds", "Replay step of a shard-open batch that replayed at least one segment (previous owners' log tails)", exponential_buckets(0.01, 2.0, 14).unwrap()));
lazy!(LAYOUT_SHARDS: IntGauge = register_int_gauge!("vlpds_shard_layout_shards", "Shards in the layout this node routes by (changes with each split/merge)"));

// ---- capacity ----
lazy!(MEMORY_LIMIT: IntGauge = register_int_gauge!("vlpds_memory_limit_bytes", "Memory this process may use: physical RAM, or the cgroup limit when lower (caches.rs; absent if neither is readable)"));
lazy!(REPO_CACHE_CAPACITY: IntGauge = register_int_gauge!("vlpds_repo_cache_capacity_bytes", "Byte budget of the repo workers' caches, all workers together (--repo-cache-mb); compare with sum(vlpds_repo_cache_bytes)"));

// ---- object-store latency (objstats.rs) ----
lazy!(OBJ_DURATION: HistogramVec = register_histogram_vec!("vlpds_object_store_request_seconds", "Object-store request latency by op and key component (objstats.rs), answered requests only: to the response head for GETs, to the first page for LISTs; deletes are not timed", &["op", "component"], latency_buckets()));

// ---- retention ----
lazy!(RETENTION_PASS_SECONDS: Histogram = register_histogram!("vlpds_retention_pass_seconds", "One log retention pass, ok or failed", exponential_buckets(0.01, 2.0, 14).unwrap()));
lazy!(RETENTION_DEAD_SEGMENTS: IntGauge = register_int_gauge!("vlpds_retention_dead_log_segments", "Log objects left below the end of dead (writer gone) logs, as of the last pass that checked them all; only the dead-log pruner (owner of slot 0's shard) reports non-zero"));
lazy!(RETENTION_DEAD_LOGS: IntGaugeVec = register_int_gauge_vec!("vlpds_retention_dead_logs", "Dead logs by state as of the last pass that checked them all: unfenced (no successor fenced it yet), needed (a shard's replay may still read it), pruning (segments inside the window, or deleting), fenced (pruned to its fence, which goes after --fence-retention)", &["state"]));

/// Exports counters at 0 before their first event. A counter series that
/// first appears already at 1 has no earlier sample, so `rate()` and
/// `increase()` never see that event: a kill -9 survivor's
/// `vlpds_peer_takeovers_total{reason="peer"}` showed 1 and
/// `VlpdsUncleanNodeExit` never fired. Covers every unlabelled counter here,
/// the histograms alerts take `_count` from, and the bounded label values of
/// the labelled counters ops/alerts.yml and the dashboard read. Elsewhere:
/// `vlpds_format_errors_total` (version::init_metrics), the signature
/// failures (crypto::touch_metrics), PLC write ops (plc::touch_metrics), KMS
/// requests (Secrets::new, per configured backend), permit waits
/// (objlimit's lanes), retention passes and retired-state GC / forced
/// compactions (when their loops are spawned, so `VlpdsRetentionNotRunning`
/// stays quiet on nodes that don't run retention). Not pre-created:
/// per-route families (`vlpds_http_requests_total`,
/// `vlpds_rate_limit_rejections_total`: limiter x route, of which
/// `vlpds_rate_limited_total` is the bounded total) and
/// `vlpds_object_store_requests_total` (op x component x client x result;
/// its alerts are rates over 1/s).
///
/// Idempotent and cheap after the first call; server startup calls it
/// before joining the cluster (a restart's own takeover), and [`render`]
/// does too.
pub fn init_counters() {
    static DONE: std::sync::Once = std::sync::Once::new();
    DONE.call_once(|| {
        for c in UNLABELLED_COUNTERS {
            LazyLock::force(c);
        }
        LazyLock::force(&RUNTIME_LATE_TOTAL);
        for h in ALERT_HISTOGRAMS {
            LazyLock::force(h);
        }
        for (vec, values) in LABELLED_COUNTERS {
            for v in *values {
                vec.with_label_values(&[v]);
            }
        }
        for kind in ["replay", "clean"] {
            SHARD_OPEN_SECONDS.with_label_values(&[kind]);
        }
    });
}

/// Unlabelled integer counters, exported at 0 by [`init_counters`].
static UNLABELLED_COUNTERS: &[&LazyLock<IntCounter>] = &[
    &HTTP_SERVER_CONNECTIONS,
    &HTTP_SERVER_ACCEPT_ERRORS,
    &RATE_LIMITED,
    &WRITES_SHED,
    &ARGON2_SHED,
    &HTTP_STALLED_BODIES,
    &COMMITS,
    &REPO_EVICTIONS,
    &LAZY_MST_UNLOADS,
    &WRITES_ABANDONED,
    &SEGMENT_BYTES_TOTAL,
    &SEGMENT_STALL_SEALS,
    &SEGMENT_STORED_BYTES_TOTAL,
    &SEGMENT_DECODES,
    &PUT_HEDGES,
    &REPLAYED_SEGMENTS,
    &FIREHOSE_EVENTS,
    &FIREHOSE_SENT,
    &FIREHOSE_SPILLS,
    &FIREHOSE_SPILL_SEGMENTS,
    &FIREHOSE_SENT_BYTES,
    &FIREHOSE_BACKFILL_GETS,
    &FIREHOSE_BACKFILL_EVENTS,
    &LOG_STREAM_LAGGED,
    &FORWARDED,
    &RESHARD_GC_SKIPPED,
];

/// Unlabelled histograms whose `_count` an alert rates (a node that never
/// observed one must still show 0, e.g. `VlpdsCheckpointsStalled`).
static ALERT_HISTOGRAMS: &[&LazyLock<Histogram>] = &[&COMMIT_LATENCY, &CHECKPOINT_SHARD, &FORWARD_DURATION, &FIREHOSE_EMIT_DELAY, &REPLAY_SECONDS];

/// One-label counters and every value their code paths emit.
#[allow(clippy::type_complexity)]
static LABELLED_COUNTERS: &[(&LazyLock<IntCounterVec>, &[&str])] = &[
    (&PEER_TAKEOVERS, &["peer", "restart"]),
    (&LEASE_EVENTS, &["opened", "closed", "lost", "peer_refused", "lease_recreated", "history_full", "join_lease_moved", "shutdown_fence_failed"]),
    (&LEASE_RENEW_ERRORS, &["timeout", "error", "conflict", "lapsed"]),
    (&CLUSTER_STORE_TIMEOUTS, &["get", "put", "list", "delete", "fence", "fence-scan"]),
    (&SHARDS_OPENED, &["ok", "error"]),
    (&PUT_ATTEMPTS, &["ok", "already_exists", "error"]),
    (&WRITE_ERRORS, &["repo_not_found", "repo_inactive", "invalid_swap", "invalid", "internal", "unavailable", "key_unavailable", "signature_fault", "not_started"]),
    (&WRITE_RETRIES, &["unreachable", "loading", "moved"]),
    (&FORWARDS, &["2xx", "3xx", "4xx", "5xx"]),
    (&PROXY_REJECTED, &["account_cap"]),
    (&REPO_CACHE, &["hit", "miss", "loading"]),
    (&REPO_LOADS, &["ok", "error", "not_found", "stale"]),
    (&LAZY_MST_FALLBACKS, &["missing", "missing_node", "invalid"]),
    (&FIREHOSE_DISCONNECTS, &["too_slow"]),
    (&FIREHOSE_REJECTED, &["per_ip"]),
    (&SYNC_EXPORTS_ENDED, &["done", "client_gone", "stalled", "error", "shed"]),
];

/// Retention passes at 0 (Retention::spawn: only nodes that run it).
pub fn init_retention_counters() {
    for r in ["ok", "error"] {
        RETENTION_TICKS.with_label_values(&[r]);
    }
}

/// Retired-state GC passes and forced compactions at 0 (ReshardGc::spawn).
pub fn init_reshard_gc_counters() {
    for r in ["ok", "error"] {
        RESHARD_GC_PASSES.with_label_values(&[r]);
    }
    for kind in ["detach", "full"] {
        for r in ["submitted", "completed", "failed"] {
            FORCED_COMPACTIONS.with_label_values(&[kind, r]);
        }
    }
}

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

/// Exports the lease configuration (vlpds_lease_{ttl,renew_interval,skew}_seconds)
/// so alert thresholds can scale with it; also enables
/// `vlpds_lease_renew_ttl_ratio`.
pub fn export_lease_config(ttl: std::time::Duration, renew_every: std::time::Duration, skew: std::time::Duration) {
    LEASE_TTL.set(ttl.as_secs_f64());
    LEASE_RENEW_INTERVAL.set(renew_every.as_secs_f64());
    LEASE_SKEW.set(skew.as_secs_f64());
}

/// Exports the firehose byte budgets alerts compare against.
pub fn export_firehose_config(merge_queue_bytes: usize, max_lag_bytes: usize) {
    FIREHOSE_MERGE_QUEUE_BUDGET.set(merge_queue_bytes as i64);
    FIREHOSE_MAX_LAG.set(max_lag_bytes as i64);
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

type Refresher = Box<dyn Fn() -> bool + Send + Sync>;

/// Gauges computed at scrape time (lease validity left); see [`on_render`].
static REFRESHERS: LazyLock<parking_lot::Mutex<Vec<Refresher>>> = LazyLock::new(Default::default);

/// Runs `f` before every render while it returns true (false: its source
/// is gone, drop it).
pub fn on_render(f: impl Fn() -> bool + Send + Sync + 'static) {
    REFRESHERS.lock().push(Box::new(f));
}

pub fn render() -> String {
    init_counters();
    REFRESHERS.lock().retain(|f| f());
    crate::lifecycle::refresh_metrics();
    crate::crypto::touch_metrics();
    refresh_jemalloc();
    refresh_process();
    refresh_tokio();
    crate::caches::refresh_metrics();
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

/// Moves a counter that mirrors a cumulative total read at scrape time
/// (getrusage, tokio's busy time) up to `total`. Never down: a total read
/// from another runtime, or one that went back, leaves it where it is.
/// Serialized, so concurrent scrapes don't add the same delta twice.
fn advance(c: &prometheus::Counter, total: f64) {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    let _g = LOCK.lock();
    let cur = c.get();
    if total > cur {
        c.inc_by(total - cur);
    }
}

fn refresh_tokio() {
    let Ok(h) = tokio::runtime::Handle::try_current() else { return };
    let m = h.metrics();
    let n = m.num_workers();
    TOKIO_WORKERS.set(n as i64);
    TOKIO_TASKS.set(m.num_alive_tasks() as i64);
    TOKIO_GLOBAL_QUEUE.set(m.global_queue_depth() as i64);
    advance(&TOKIO_BUSY, (0..n).map(|w| m.worker_total_busy_duration(w).as_secs_f64()).sum());
}

/// Resident set size of this process (benchmarks), if the platform reports it.
pub fn resident_bytes() -> Option<u64> {
    #[cfg(all(any(target_os = "macos", target_os = "linux"), target_pointer_width = "64"))]
    return sys::rss_threads().map(|(rss, _)| rss);
    #[allow(unreachable_code)]
    None
}

fn refresh_process() {
    if let Some((user, system)) = sys::cpu_seconds() {
        advance(&PROCESS_CPU.with_label_values(&["user"]), user);
        advance(&PROCESS_CPU.with_label_values(&["system"]), system);
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
