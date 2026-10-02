#!/usr/bin/env python3
"""Generates dashboards/vlpds.json (provisioned; Grafana reloads it within 5 s).

    python3 bench/obs/grafana/gen_dashboard.py

Queries use [$__rate_interval] (4 s at the 1 s scrape) and aggregate before
histogram_quantile, so a 1 s refresh stays cheap.
"""
import json
import os

# Overrides render the same dashboard for another Grafana, e.g. the lab
# one that benchbox's Alloy remote-writes to (deploy/ansible, see bench/obs/README.md).
PROM = {"type": "prometheus", "uid": os.environ.get("VLPDS_PROM_UID", "prom")}
PYRO = {"type": "grafana-pyroscope-datasource", "uid": os.environ.get("VLPDS_PYRO_UID", "pyroscope")}
I = 'instance=~"$instance"'
RI = "[$__rate_interval]"
# MinIO is scraped every 5 s: rate windows need >= 2 samples
MRI = "[20s]"
WRITE_METHODS = r"com\\.atproto\\.repo\\.(createRecord|putRecord|deleteRecord|applyWrites)"
PROXY_METHODS = r"(app\\.bsky|chat\\.bsky|tools\\.ozone)\\..*"

panels = []
_id = [0]
_y = [0]
_x = [0]
_row_h = [0]


def nid():
    _id[0] += 1
    return _id[0]


def place(w, h):
    if _x[0] + w > 24:
        _x[0] = 0
        _y[0] += _row_h[0]
        _row_h[0] = 0
    pos = {"x": _x[0], "y": _y[0], "w": w, "h": h}
    _x[0] += w
    _row_h[0] = max(_row_h[0], h)
    return pos


def newline():
    if _x[0]:
        _x[0] = 0
        _y[0] += _row_h[0]
        _row_h[0] = 0


current_row = [None]


def row(title, collapsed=False):
    newline()
    r = {"type": "row", "id": nid(), "title": title, "collapsed": collapsed, "gridPos": {"x": 0, "y": _y[0], "w": 24, "h": 1}, "panels": []}
    _y[0] += 1
    panels.append(r)
    current_row[0] = r if collapsed else None


def add(p):
    (current_row[0]["panels"] if current_row[0] else panels).append(p)


def t(expr, legend="", ref=None, **kw):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "range": True, **kw}


def ts(title, targets, unit="short", w=8, h=7, stack=False, desc="", min0=True, overrides=None, log=False, bars=False):
    for i, tg in enumerate(targets):
        tg["refId"] = chr(65 + i)
    custom = {"lineWidth": 1, "fillOpacity": 10 if not stack else 60, "showPoints": "never", "spanNulls": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    if bars:
        custom.update(drawStyle="bars", fillOpacity=80, lineWidth=0)
    if log:
        custom["scaleDistribution"] = {"type": "log", "log": 10}
    defaults = {"unit": unit, "custom": custom}
    if min0 and not log:
        defaults["min"] = 0
    add({
        "type": "timeseries", "id": nid(), "title": title, "description": desc, "datasource": PROM,
        "gridPos": place(w, h), "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    })


def stat(title, expr, unit="short", w=4, h=4, desc="", decimals=None):
    d = {"unit": unit, "color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "blue", "value": None}]}}
    if decimals is not None:
        d["decimals"] = decimals
    add({
        "type": "stat", "id": nid(), "title": title, "description": desc, "datasource": PROM, "gridPos": place(w, h),
        "targets": [dict(t(expr), refId="A", instant=False)],
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "graphMode": "area",
                    "colorMode": "value", "textMode": "value", "justifyMode": "auto"},
    })


def quantiles(metric, sel=I, by="", qs=(0.5, 0.9, 0.99, 0.999), legend_prefix=""):
    """histogram_quantile per q over sum by (le[,by])."""
    grp = "le" + (f", {by}" if by else "")
    out = []
    for q in qs:
        name = {0.5: "p50", 0.9: "p90", 0.99: "p99", 0.999: "p99.9"}[q]
        lg = f"{legend_prefix}{name}" + (f" {{{{{by}}}}}" if by else "")
        out.append(t(f"histogram_quantile({q}, sum by ({grp}) (rate({metric}_bucket{{{sel}}}{RI})))", lg))
    return out


def rate(metric, sel=I, by=None, legend=None):
    if by:
        return t(f"sum by ({by}) (rate({metric}{{{sel}}}{RI}))", legend or f"{{{{{by}}}}}")
    return t(f"sum(rate({metric}{{{sel}}}{RI}))", legend or metric.replace("vlpds_", ""))


def mean(metric, sel=I, by=None, legend=None):
    b = f" by ({by})" if by else ""
    return t(f"sum{b} (rate({metric}_sum{{{sel}}}{RI})) / sum{b} (rate({metric}_count{{{sel}}}{RI}))",
             legend or (f"{{{{{by}}}}}" if by else "mean"))


# ---------------------------------------------------------------- overview
row("Overview")
stat("XRPC req/s", f"sum(rate(vlpds_http_requests_total{{{I}}}{RI}))", "reqps")
stat("Write req/s", f'sum(rate(vlpds_http_requests_total{{{I}, method=~"{WRITE_METHODS}"}}{RI}))', "reqps")
stat("Commits/s", f"sum(rate(vlpds_commits_total{{{I}}}{RI}))", "short")
stat("Commit p99 (durable)", f"histogram_quantile(0.99, sum by (le) (rate(vlpds_commit_durable_seconds_bucket{{{I}}}{RI})))", "s")
stat("CPU (cores)", f"sum(rate(vlpds_process_cpu_seconds_total{{{I}}}{RI}))", "short", decimals=2)
stat("RSS", f"sum(vlpds_process_resident_bytes{{{I}}})", "bytes")
add({
    "type": "table", "id": nid(), "title": "Nodes", "datasource": PROM, "gridPos": place(24, 4),
    "description": "vlpds_build_info: node id, git revision (`-dirty` = uncommitted changes), profiling feature",
    "targets": [dict(t(f"vlpds_build_info{{{I}}}"), refId="A", instant=True, range=False, format="table")],
    "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True, "job": True}}}],
    "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True, "cellHeight": "sm"},
})

# ---------------------------------------------------------------- writes
row("Writes")
ts("Write requests/s by method", [rate("vlpds_http_requests_total", f'{I}, method=~"com\\\\.atproto\\\\.repo\\\\..*"', "method")], "reqps")
ts("Write request latency (createRecord/putRecord/deleteRecord/applyWrites)",
   quantiles("vlpds_http_request_duration_seconds", f'{I}, method=~"{WRITE_METHODS}"'), "s",
   desc="Server-side XRPC latency, from the vlpds_http_request_duration_seconds histogram (0.1 ms .. 52 s, x2 buckets)")
ts("Commit latency: enqueue -> durable + applied + acked", quantiles("vlpds_commit_durable_seconds"), "s")
ts("Commits/s and record ops/s", [rate("vlpds_commits_total", legend="commits"), rate("vlpds_ops_total", by="action", legend="ops {{action}}")], "short")
ts("Coalescing", [mean("vlpds_commit_requests", legend="requests / commit"), mean("vlpds_commit_ops", legend="ops / commit"),
                  mean("vlpds_segment_events", legend="events / segment"), mean("vlpds_worker_batch_messages", legend="msgs / worker batch")],
   "short", desc="Means over the rate window: write requests per commit, ops per commit, firehose events per segment, worker messages per loop")
ts("Commit build (MST + sign) CPU time", quantiles("vlpds_commit_build_seconds", qs=(0.5, 0.99, 0.999)), "s")
ts("Rejected writes", [rate("vlpds_write_errors_total", by="kind", legend="error {{kind}}"), rate("vlpds_writes_shed_total", legend="shed (503)"),
                       rate("vlpds_rate_limited_total", legend="rate limited (429)")], "short")
ts("Commit size (block CAR bytes)", quantiles("vlpds_commit_car_bytes", qs=(0.5, 0.99)), "bytes")

# ---------------------------------------------------------------- pipeline breakdown
row("Commit pipeline breakdown")
ts("Mean time per stage (per segment)", [mean("vlpds_commit_stage_seconds", by="stage")], "s", stack=True, w=8,
   desc="seal_wait: oldest entry's enqueue -> PUT start; put: PUT until durable (hedges/retries incl.); "
        "apply_lock: finalizer waiting for the shards' apply locks; apply: SlateDB batches; ack: acks + repo views. "
        "Stacked means approximate the commit latency of a segment's oldest entry.")
ts("Stage p50", quantiles("vlpds_commit_stage_seconds", by="stage", qs=(0.5,)), "s", w=8)
ts("Stage p99", quantiles("vlpds_commit_stage_seconds", by="stage", qs=(0.99,)), "s", w=8)
ts("Sequencer queue / PUTs in flight", [t(f"sum(vlpds_sequencer_queue_depth{{{I}}})", "sequencer queue"),
                                        t(f"sum(vlpds_segment_puts_inflight{{{I}}})", "PUTs in flight")], "short", w=8)
ts("Worker queue depth", [t(f"sum(vlpds_worker_queue_depth{{{I}}})", "sum"), t(f"max(vlpds_worker_queue_depth{{{I}}})", "max worker")], "short", w=8)
ts("Watermark lag", [t(f"max(vlpds_watermark_lag_microseconds{{{I}}}) / 1e6", "{{instance}}")], "s", w=8)

# ---------------------------------------------------------------- log
row("Log / segments")
ts("Segments/s", [rate("vlpds_segments_total", legend="segments/s")], "short", w=6)
ts("Log bytes/s", [rate("vlpds_segment_bytes_total", legend="bytes/s")], "Bps", w=6)
ts("Segment size", quantiles("vlpds_segment_bytes", qs=(0.5, 0.99)) + [mean("vlpds_segment_bytes", legend="mean")], "bytes", w=6)
ts("Events per segment", quantiles("vlpds_segment_events", qs=(0.5, 0.99)), "short", w=6)
ts("Segment PUT latency (until durable)", quantiles("vlpds_segment_put_seconds"), "s")
ts("PUT attempts by result, hedges", [rate("vlpds_segment_put_attempts_total", by="result", legend="attempt {{result}}"),
                                      rate("vlpds_segment_put_hedges_total", legend="hedges")], "short",
   desc="already_exists = a conditional-PUT conflict (our own hedge winning, or a fence)")
ts("SlateDB apply latency per segment", quantiles("vlpds_state_apply_seconds", qs=(0.5, 0.99, 0.999)), "s")

# ---------------------------------------------------------------- firehose
row("Firehose")
ts("Events emitted / frames sent per s", [rate("vlpds_firehose_events_total", legend="events emitted"),
                                          rate("vlpds_firehose_frames_sent_total", legend="frames sent")], "short")
ts("Emit delay (seq assigned -> emitted)", quantiles("vlpds_firehose_emit_delay_seconds", qs=(0.5, 0.99, 0.999)), "s")
ts("Subscribers", [t(f"sum(vlpds_firehose_subscribers{{{I}}})", "subscribers")], "short")
ts("Merger queue, rings", [t(f"sum(vlpds_firehose_merge_queue_bytes{{{I}}})", "merger queue"),
                          t(f"sum(vlpds_firehose_ring_bytes{{{I}}})", "firehose ring"),
                          t(f"sum(vlpds_log_live_ring_bytes{{{I}}})", "log live ring")], "bytes")
ts("Spills, lagged followers, disconnects", [rate("vlpds_firehose_merge_spills_total", legend="merger spills"),
                                            rate("vlpds_firehose_merge_spill_segments_total", legend="spill segments read"),
                                            rate("vlpds_log_stream_lagged_total", legend="lagged peer streams"),
                                            rate("vlpds_firehose_disconnects_total", by="reason", legend="disconnect {{reason}}")], "short")
ts("Merged batch size", quantiles("vlpds_firehose_merge_batch_events", qs=(0.5, 0.99)), "short")

# ---------------------------------------------------------------- repo workers
row("Repo workers")
ts("Repo lookups by result", [rate("vlpds_repo_cache_lookups_total", by="result")], "short", stack=True,
   desc="hit: cached; miss: starts a cold load; loading: joins a load in flight")
ts("Repo cache hit ratio", [t(f'sum(rate(vlpds_repo_cache_lookups_total{{{I}, result="hit"}}{RI})) / sum(rate(vlpds_repo_cache_lookups_total{{{I}}}{RI}))', "hit ratio")],
   "percentunit")
ts("Cold loads by result, evictions", [rate("vlpds_repo_loads_total", by="result", legend="load {{result}}"),
                                       rate("vlpds_repo_evictions_total", legend="evictions"),
                                       t(f"sum(vlpds_repos_loading{{{I}}})", "loading now")], "short",
   desc="stale = a load finished after its shard moved (dropped, reloaded)")
ts("Cold load latency", quantiles("vlpds_repo_load_seconds", qs=(0.5, 0.99, 0.999)), "s")
ts("Cached repos", [t(f"sum(vlpds_cached_repos{{{I}}})", "cached")], "short")
ts("M/ bytes prefetched per cold open", quantiles("vlpds_lazy_mst_prefetch_bytes", qs=(0.5, 0.99)), "bytes")

# ---------------------------------------------------------------- HTTP
row("HTTP server")
ts("Requests/s by method (top 12)", [t(f"topk(12, sum by (method) (rate(vlpds_http_requests_total{{{I}}}{RI})))", "{{method}}")], "reqps", w=12, h=8)
ts("Requests/s by status", [rate("vlpds_http_requests_total", by="status")], "reqps", w=12, h=8)
ts("p99 latency by method (top 12)",
   [t(f"topk(12, histogram_quantile(0.99, sum by (le, method) (rate(vlpds_http_request_duration_seconds_bucket{{{I}}}{RI}))))", "{{method}}")],
   "s", w=12, h=8)
ts("In flight", [t(f"sum(vlpds_http_requests_inflight{{{I}}})", "XRPC in flight"),
                 t(f"sum by (version) (vlpds_http_server_active_requests{{{I}}})", "awaiting head {{version}}")], "short", w=12, h=8)
ts("Connections", [t(f"sum(vlpds_http_server_connections_open{{{I}}})", "inbound open"),
                   rate("vlpds_http_server_connections_total", legend="inbound accepted/s"),
                   rate("vlpds_http_client_connects_total", by="role", legend="outbound connects/s {{role}}")], "short", w=12,
   desc="Outbound connects should stay flat under steady load (pooled h2c to peers)")
ts("Other vlpds_http_* metrics",
   [t(f'sum by (__name__) ({{__name__=~"vlpds_http_.*", __name__!~"vlpds_http_(requests_total|request_duration_seconds_.*|requests_inflight|client_connects_total|server_connections_total|server_connections_open|server_active_requests)", {I}}})',
      "{{__name__}}")], "short", w=12,
   desc="Catch-all for HTTP metrics added later (raw values: counters climb)")

# ---------------------------------------------------------------- cluster
row("Cluster")
ts("Owned shards", [t(f"vlpds_owned_partitions{{{I}}}", "{{instance}}")], "short", w=6)
ts("Lease events/s", [rate("vlpds_lease_events_total", by="event")], "short", w=6)
ts("Forwards/s by owner status", [rate("vlpds_forwards_total", by="result", legend="forward {{result}}")], "short", w=6,
   desc="5xx includes owner unreachable / past its TTFB deadline (503 PartitionUnavailable)")
ts("Forward latency (to owner's response head)", quantiles("vlpds_forward_seconds", qs=(0.5, 0.99, 0.999)), "s", w=6)
ts("Control-plane object-store requests/s", [rate("vlpds_cluster_store_requests_total", by="op")], "short", w=12)
ts("Forwards/s per node", [t(f"sum by (instance) (rate(vlpds_forwards_total{{{I}}}{RI}))", "{{instance}}")], "short", w=12)

# ---------------------------------------------------------------- proxy
row("AppView proxy", collapsed=True)
ts("Proxied requests/s by status", [rate("vlpds_http_requests_total", f'{I}, method=~"{PROXY_METHODS}"', "status")], "reqps")
ts("Proxied request latency", quantiles("vlpds_http_request_duration_seconds", f'{I}, method=~"{PROXY_METHODS}"'), "s")
ts("Proxy fast-path cache", [rate("vlpds_proxy_cache_total", by="result")], "short", stack=True)

# ---------------------------------------------------------------- slatedb
row("SlateDB (sum over this node's shard DBs)", collapsed=True)
ts("Block cache hit rate by entry kind",
   [t(f'sum by (entry_kind) (rate(slatedb_db_cache_access_count_total{{{I}, result="hit"}}{RI})) / sum by (entry_kind) (rate(slatedb_db_cache_access_count_total{{{I}}}{RI}))', "{{entry_kind}}")],
   "percentunit")
ts("DB requests/s", [rate("slatedb_db_request_count_total", by="op"), rate("slatedb_db_write_batch_count_total", legend="write batches"),
                     rate("slatedb_db_write_ops_total", legend="write ops")], "short")
ts("Memtables / L0", [t(f"sum(slatedb_db_total_mem_size_bytes{{{I}}})", "memtable bytes")], "bytes",
   overrides=[{"matcher": {"id": "byName", "options": "L0 SSTs"}, "properties": [{"id": "unit", "value": "short"}, {"id": "custom.axisPlacement", "value": "right"}]}])
panels_last = (current_row[0]["panels"] if current_row[0] else panels)[-1]
panels_last["targets"].append(dict(t(f"sum(slatedb_db_l0_sst_count{{{I}}})", "L0 SSTs"), refId="B"))
ts("Flushes, backpressure, stalls", [rate("slatedb_db_immutable_memtable_flushes_total", legend="memtable flushes"),
                                     rate("slatedb_db_backpressure_count_total", legend="backpressure"),
                                     rate("slatedb_db_l0_stall_count_total", by="type", legend="L0 stall {{type}}")], "short")
ts("Flush / compaction bytes/s", [rate("slatedb_db_l0_flush_bytes_total", legend="L0 flush"), rate("slatedb_db_memtable_write_bytes_total", legend="memtable writes"),
                                  rate("slatedb_compactor_bytes_compacted_total", legend="compacted")], "Bps")
ts("Object store requests/s (SlateDB)", [t(f"sum by (component, api) (rate(slatedb_object_store_request_count_total{{{I}}}{RI})) > 0", "{{component}} {{api}}")], "short")
ts("Object store p99 (SlateDB)", [t(f"histogram_quantile(0.99, sum by (le, api) (rate(slatedb_object_store_request_duration_seconds_bucket{{{I}}}{RI})))", "{{api}}")], "s")

# ---------------------------------------------------------------- minio
row("MinIO (5 s scrape)", collapsed=True)
ts("S3 requests/s by API", [t(f"sum by (api) (rate(minio_s3_requests_total{MRI})) > 0", "{{api}}")], "reqps")
ts("S3 TTFB p99 by API", [t(f"histogram_quantile(0.99, sum by (le, api) (rate(minio_s3_requests_ttfb_seconds_distribution{MRI})))", "{{api}}")], "s")
ts("S3 traffic", [t(f"sum(rate(minio_s3_traffic_received_bytes{MRI}))", "received"), t(f"sum(rate(minio_s3_traffic_sent_bytes{MRI}))", "sent")], "Bps")
ts("S3 errors / in flight", [t(f"sum(rate(minio_s3_requests_errors_total{MRI}))", "errors/s"), t("sum(minio_s3_requests_inflight_total)", "in flight"),
                             t("sum(minio_s3_requests_waiting_total)", "waiting")], "short")
ts("Bucket usage", [t("sum by (instance) (minio_cluster_usage_total_bytes)", "{{instance}}")], "bytes")

# ---------------------------------------------------------------- process
row("Process / runtime")
ts("CPU (cores) by mode", [t(f"sum by (mode) (rate(vlpds_process_cpu_seconds_total{{{I}}}{RI}))", "{{mode}}")], "short", stack=True, w=6)
ts("Memory", [t(f"sum(vlpds_process_resident_bytes{{{I}}})", "RSS")] +
   [t(f'sum(vlpds_jemalloc_bytes{{{I}, stat="{s}"}})', f"jemalloc {s}") for s in ("allocated", "resident")], "bytes", w=6)
ts("Tokio worker utilization", [t(f"sum(rate(vlpds_tokio_busy_seconds_total{{{I}}}{RI})) / sum(vlpds_tokio_workers{{{I}}})", "busy")], "percentunit", w=6,
   desc="Busy time / workers: near 100% = the IO runtime is saturated")
ts("Tokio tasks / injection queue, threads", [t(f"sum(vlpds_tokio_alive_tasks{{{I}}})", "alive tasks"),
                                             t(f"sum(vlpds_tokio_global_queue_depth{{{I}}})", "injection queue"),
                                             t(f"sum(vlpds_process_threads{{{I}}})", "OS threads")], "short", w=6)

# ---------------------------------------------------------------- profiles
row("CPU profile (Pyroscope; nodes run with --pyroscope-url)", collapsed=True)
add({
    "type": "flamegraph", "id": nid(), "title": "CPU flamegraph (dashboard time range)", "datasource": PYRO, "gridPos": place(24, 16),
    "targets": [{"datasource": PYRO, "refId": "A", "queryType": "profile", "groupBy": [],
                 "profileTypeId": "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                 "labelSelector": '{service_name="vlpds", node_id=~"$node"}'}],
    "options": {},
})

dashboard = {
    "uid": os.environ.get("VLPDS_DASH_UID", "vlpds"),
    "title": os.environ.get("VLPDS_DASH_TITLE", "vlpds"),
    "tags": ["vlpds"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["1s", "2s", "5s", "10s", "30s", "1m"]},
    "schemaVersion": 39,
    "version": 1,
    "annotations": {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True,
         "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
        {"datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "iconColor": "#FF9830",
         "name": "bench steps", "target": {"type": "tags", "tags": ["vlpds-bench"], "matchAny": True, "limit": 500}},
    ]},
    "templating": {"list": [
        {"name": "instance", "label": "instance", "type": "query", "datasource": PROM,
         "query": {"query": "label_values(vlpds_build_info, instance)", "refId": "instance"},
         "definition": "label_values(vlpds_build_info, instance)", "refresh": 2, "multi": True, "includeAll": True,
         "allValue": ".*", "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1},
        {"name": "node", "label": "node (profiles)", "type": "query", "datasource": PROM,
         "query": {"query": 'label_values(vlpds_build_info{instance=~"$instance"}, node_id)', "refId": "node"},
         "definition": 'label_values(vlpds_build_info{instance=~"$instance"}, node_id)', "refresh": 2, "multi": True,
         "includeAll": True, "allValue": ".*", "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1},
    ]},
    "panels": panels,
}

out = os.environ.get("VLPDS_DASH_OUT") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "vlpds.json")
with open(out, "w") as f:
    json.dump(dashboard, f, indent=1)
    f.write("\n")
print(f"wrote {out}: {sum(1 + len(p.get('panels', [])) for p in panels)} panels")
