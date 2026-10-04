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

## The dashboards

Two dashboards serve the bench stack and a deployment's Grafana, both
generated by `just dashboards` (`grafana/gen_dashboard.py`,
operator panels in `grafana/gen_operator.py`):

- **vlpds** (uid `vlpds`, the home page): the PDS operator's view, in their
  words. Is it up (status, errors, latency of posting / reading / signing in
  / uploading, the Bluesky app via the AppView), users (accounts by status,
  active accounts, sign-ups, sign-ins), content (posts, likes, follows,
  media per hour), federation (relays, requestCrawl, PLC, identity events),
  moderation (reports, takedowns, abusive traffic blocked), email,
  resources and an estimated storage request bill (Class A / B prices are
  dashboard variables), and the firing alerts in plain words. Account totals
  are kept exact per slot with every change (DESIGN.md "Account totals");
  active-account windows are counted in whole UTC days.
- **vlpds internals** (uid `vlpds-internals`): the engineer's view, below.

The internals dashboard's **Health** row at the top is always open and answers
"is it healthy?" at a glance; every other row is collapsed (open the one you
need, which also keeps a 1 s bench refresh cheap; pick 1 s in the refresh
picker, the default is 10 s).

- **Variables**: `cluster` (Alloy's remote_write adds it; the bench Prometheus
  has none, which All still matches) and `node` (shows node ids; its values
  are `instance` labels, so it works for the bench's `127.0.0.1:<port>` and
  prod's node-id instances). Every query is scoped by both, except the alert
  timeline (cluster only) and the Shards stat (whole cluster).
- **Legends** name nodes by node id, joined from `vlpds_build_info`.
- **Colours** are fixed for status classes (2xx green, 3xx blue, 4xx yellow,
  429 orange, 5xx red) and bare quantiles (p50 green, p90 yellow, p99 orange,
  p99.9 red). Dashed lines are the `ops/alerts.yml` thresholds.
- **Descriptions** (the (i) on each panel) say what bad looks like and link
  the alert's section in `ops/RUNBOOK.md`; the Runbook and Alert rules links
  sit top right.
- **Annotations**: restarts (purple, on), Vlpds paging alerts (red regions, off
  by default) and each bench.py step (orange, tag `vlpds-bench`).
- Rare-event panels hide all-zero series and say so ("none (good)") rather
  than "No data". A labelled counter that is first created by its first event
  (e.g. `vlpds_peer_takeovers_total{reason}`) is born at 1, so `rate()` never
  sees that first event.

| Row | What to look at |
|---|---|
| Health (open) | Stats coloured at the alert thresholds: req/s, 5xx share (proxied excluded, as VlpdsHttp5xxHigh), 429 share, read / write / commit p99, firehose lag p99 and subscribers; nodes up/down, shards owned/unowned, worst lease renew/TTL, store errors/s and permit waits/s, restarts (all / worst node), fail-stops in 30 min, Vlpds alerts firing by severity. Then: alert timeline, req/s by status class, 5xx and 429 ratio, p99 read/write/commit, owned shards by node vs layout, lease renew/TTL and firehose lag by node, store failures, CPU and memory/limit by node, and a nodes table (id, instance, rev, up, uptime, owned shards, last exit) |
| Writes and commit pipeline | write req/s by method; write and commit latency quantiles; commits and ops/s; coalescing; rejected/shed/abandoned/resent writes; `vlpds_commit_stage_seconds` stacked means and p99 by stage; sequencer queue, PUTs in flight, busiest worker queue; watermark lag by node; commit build CPU; commit CAR size |
| Log, segments, retention | segments/s by node; raw vs stored bytes/s; segment size and events; PUT latency; PUT attempts by result (`already_exists` = conditional-PUT conflict), hedges, stall seals; apply and checkpoint latency; retention deletes, passes, dead logs and replay hold |
| Firehose and sync exports | events/frames/backfill events per s; emit delay; subscribers by node and bytes sent; disconnects and refusals by reason; cursor backfills running/waiting and retries; backfill cache hit ratio; merger queue vs budget and rings; spills and lagged peer streams; getRepo exports streaming/waiting and how they ended |
| Repo workers and caches | lookups by result; hit ratio by node; cold loads, evictions; load latency; repo cache share of its byte budget; lazy MST reads/fetches/fallbacks; in-memory cache fill and bytes; proxy fast-path cache |
| HTTP and proxy | req/s and p99 by method (top 12); 5xx by method; in flight; connections; proxied req/s by status class and latency; upstream pool waits and read-after-write |
| Auth and abuse | rate-limit rejections by limiter and by route; rate-limit config version; Argon2 shedding; proxy refusals (account cap); stalled bodies, accept errors, firehose per-IP refusals, shed/stalled exports |
| Cluster: leases, ownership, failover | lease renewal round trip by node vs the 0.4 x TTL ceiling; lease validity left; renew errors and takeovers; lease events (incl. `history_full`); shard open time by kind; shards opened, segments replayed; exit state / feature levels table; format errors and signature faults |
| Cluster: forwarding, control plane, resharding | forwards by owner status, latency, per node; control-plane requests, latency, timeouts/nudges/lone skips; resharding; retired-state GC; forced compactions |
| Object store | permits in use / limit, permit waits and wait p99 per pool (log, state, ctl) and lane; requests by client and op; non-ok results; p99 by key component; bytes by client and direction |
| SlateDB | block cache hit rate, DB requests, memtables and L0, flushes/backpressure/stalls, flush and compaction bytes, SlateDB's own store requests and error count (which includes not-found answers). Summed over each node's shard DBs |
| Process and runtime | CPU by mode, RSS by node, jemalloc, tokio utilization and runtime lateness by node, tasks/injection queue/threads |
| Key service, PLC directory, mail | KMS calls by result and latency, signing-key cache, PLC directory calls, outbound mail |
| MinIO (bench only) | S3 req/s and TTFB p99 by API, traffic, errors/in flight, bucket usage. Empty outside the bench |
| CPU profile | Pyroscope flamegraph for the selected nodes |

Quantiles come from Prometheus histograms, so they are bucket-resolution
estimates. The latency buckets go from 0.1 ms to 52 s in steps of ×2. Read p99.9
over short windows with care.

The dashboard JSON is generated: `grafana/dashboards/vlpds.json` (this
stack). Edit `grafana/gen_dashboard.py`, then write it:

```sh
just dashboards          # python3 bench/obs/grafana/gen_dashboard.py
just dashboards --check  # exit 1 if a copy is stale
```

That copy is import-ready (any Grafana's Import dialog asks for the
Prometheus) and `vlpds dashboards` prints it. This stack provisions it
as is: Grafana picks the only Prometheus. `VLPDS_PROM_UID` /
`VLPDS_PYRO_UID` with `VLPDS_DASH_OUT` render an extra copy pre-set to your
own Grafana's datasource uids, for file provisioning.

This stack reloads the file within 5 s. Edits made in the UI are allowed but
get overwritten by the next file change.
