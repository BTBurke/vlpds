# bench/obs: metrics and profiles for load tests

A local stack for watching vlpds under load at 1 s resolution, plus CPU
profiling that works with or without it.

| | URL (127.0.0.1 only) | |
|---|---|---|
| Grafana | http://127.0.0.1:3300/d/vlpds | anonymous admin; the `vlpds` dashboard is the home page |
| Prometheus | http://127.0.0.1:9090 | 1 s scrape + evaluation, 2 days / 2 GB retention |
| Pyroscope | http://127.0.0.1:4040 | continuous CPU profiles, 48 h retention |

```sh
just obs-up      # writes prometheus/minio.token, starts the stack (docker compose, project vlpds-obs)
just obs-down    # stops it, keeps the data (docker compose -f bench/obs/docker-compose.yml down -v wipes it)
```

Caps: Prometheus 2 GB RAM / 1 CPU, Pyroscope 1 GB / 1 CPU, Grafana 512 MB / 1 CPU.
Under a light load it uses ~450 MB and a few % of one core.

## What gets scraped

- **vlpds** nodes on the host via `host.docker.internal`, at the ports the bench
  scripts use (`prometheus/targets/vlpds.json`, re-read every 5 s, so you can add
  ports without restarting anything): 2583 (bench.py, step.sh), 2620 (`just dev`),
  2700-2715 (ad hoc; 2700 is bench.py's stub AppView), 2800-2802 (tests/E2E.md),
  7100-7105 (bench.py `cluster_up`, hactl), and 7700-7705 (hactl container nodes).
  Ports with nothing listening show up as `up == 0`, which is fine. Each node's
  `instance` label is `127.0.0.1:<port>`.
  The bench scripts serve `/metrics` on the app port (the default). A node run
  with `--metrics-listen <addr>` serves `/metrics` and `/debug/pprof` only there
  (404 on the app port): add that port to `targets/vlpds.json` instead, and point
  `profile.sh` at it.
- **MinIO**: the native one on :9200 (bench.py) and the compose one on :9000
  (`just bench`), from `/minio/v2/metrics/cluster` every 5 s. The bearer token is the
  JWT that `mc admin prometheus generate` would print, built from
  minioadmin/minioadmin by `minio-token.py`, so MinIO doesn't need restarting.
- **Process stats** come from vlpds itself, not node_exporter, which is of little
  use on macOS: `vlpds_process_*` (getrusage, plus proc_pidinfo on macOS or /proc on
  Linux) and `vlpds_tokio_*`.

## Profiling

The CPU profiler is the cargo feature `profiling`:
`cargo build --release --features profiling`. It uses pyroscope's pprof-rs backend:
SIGPROF sampling with framehop unwinding, which works on macOS arm64 and on Linux.
There are two ways to use it.

**On demand** (no stack needed). This is the main tool for an optimization loop:

```sh
bench/obs/profile.sh 127.0.0.1:2583 10          # top 30 by self and by cumulative CPU time
bench/obs/profile.sh -n 50 -f 'mst|cbor' 127.0.0.1:2583 20   # focus on stacks through matching frames
bench/obs/profile.sh -o /tmp/a.pb -svg 127.0.0.1:2583 10     # keep the pprof (go tool pprof -http=: /tmp/a.pb) + a flamegraph SVG
just profile 127.0.0.1:2583 10
```

`profile.sh` calls `GET /debug/pprof/profile?seconds=N[&frequency=Hz][&format=svg]`
with the admin token (`VLPDS_ADMIN_TOKEN`, default `dev-admin-token`) and prints
`go tool pprof -top` twice:

- **by self**: time spent in the function itself. `[libsystem]` time is charged to
  its caller, so a syscall shows up under the Rust code that made it.
- **by cumulative**: frames shared by every thread (thread start, tokio task polling,
  unwind guards) are dropped from the display. The numbers are unchanged.

The header gives the CPU total in cores and the `[libsystem]` share.

Other options: `-i REGEX` ignores frames, `-l` gives per-line output.

**Continuous**: run nodes with `--pyroscope-url http://127.0.0.1:4040`
(`VLPDS_PYROSCOPE_URL`). They push 100 Hz CPU profiles every 10 s, tagged with
`service_name=vlpds`, `node_id` and `rev` (`git describe --dirty` of the source tree).
In bench.py, set `VLPDS_EXTRA="--pyroscope-url http://127.0.0.1:4040"`.

- **Browse**: in Grafana, open Explore → Pyroscope, or the dashboard's collapsed
  "CPU profile" row, which shows a flamegraph for the time range and `$node`.
- **Text**: `bench/obs/profile.sh -p [node_id] [seconds]` gives the same top tables
  for the last N seconds. The newest 10 s is not ingested yet.
- **Only one sampler at a time**: while the agent runs, the on-demand endpoint
  answers 409.

Notes:
- **Frame names on macOS**: system-library frames (kernel syscalls, pthread)
  resolve to wrong names from the dyld shared cache, so they are folded into a
  single `[libsystem]` frame.
- **Inlined frames**: these come back as bare names, so they get a crate and file
  suffix, as in `unpark @parking_lot_core:unix.rs` or `{closure#0} @mio:stream.rs`.
- **What a light load looks like**: at low rates nearly all CPU is thread wakeups and
  socket I/O (`[libsystem]` is 95% or more). Profile at a rate that saturates
  something before reading much into it.
- **Heap profiling is not wired up**: `jemalloc_pprof` needs Linux (/proc maps).
  `vlpds_jemalloc_bytes{stat}` tracks allocator totals.

## The dashboard

Most rows are open by default. The proxy, SlateDB, MinIO and profile rows are
collapsed, which keeps a 1 s refresh cheap. Choose 1 s in the refresh picker; the
default is 5 s. `$instance` filters every query, and `$node` filters the
flamegraph. Each step bench.py runs shows up as an orange region annotation
(tag `vlpds-bench`).

| Row | What to look at |
|---|---|
| Overview | XRPC and write req/s, commits/s, commit p99, CPU cores, RSS; table of nodes with their git rev |
| Writes | write req/s by method; write latency p50/p90/p99/p99.9; commit durable latency; commits/ops per s; coalescing (requests/commit, ops/commit, events/segment, msgs/worker batch); commit build CPU; rejected writes; commit CAR size |
| Commit pipeline breakdown | `vlpds_commit_stage_seconds{stage}` stacked means and p50/p99 for `seal_wait` (oldest entry's enqueue to PUT start), `put`, `apply_lock`, `apply`, `ack`; sequencer queue, PUTs in flight, worker queues, watermark lag |
| Log / segments | segments/s, bytes/s, segment size, events/segment, PUT latency quantiles, PUT attempts by result (`already_exists` = conditional-PUT conflict) and hedges, apply latency |
| Firehose | events emitted and frames sent per s; emit delay (seq assigned to emitted); subscribers; merger queue, firehose ring and live ring bytes; spills, lagged peer streams, disconnects; merge batch size |
| Repo workers | lookups by hit/miss/loading and hit ratio; cold loads by result (incl. `stale`), evictions, loads in flight; load latency; cached repos |
| HTTP server | req/s by method (top 12) and status; p99 by method; in flight / awaiting head by HTTP version; connections (inbound open/accepted, outbound by role); catch-all for `vlpds_http_*` metrics added later |
| Cluster | owned shards per node, lease events, forwards by the owner's status class, forward latency, control-plane object-store ops |
| AppView proxy | proxied req/s by status and latency, fast-path cache results |
| SlateDB | block cache hit rate by entry kind, DB requests, memtable bytes and L0 SSTs, flushes/backpressure/stalls, flush and compaction bytes, SlateDB's object-store requests and p99. These are summed over the node's shard DBs. |
| MinIO | S3 req/s and TTFB p99 by API, traffic, errors/in flight, bucket usage |
| Process / runtime | CPU cores by user/system, RSS vs jemalloc, tokio worker utilization (busy / workers), tasks, injection queue, OS threads |

Quantiles come from Prometheus histograms, so they are bucket-resolution
estimates. The latency buckets go from 0.1 ms to 52 s in steps of ×2. Read p99.9
over short windows with care.

The dashboard JSON is generated: edit `grafana/gen_dashboard.py`, then run
`python3 bench/obs/grafana/gen_dashboard.py`. Grafana reloads the file within 5 s.
Edits made in the UI are allowed but get overwritten by the next file change.
