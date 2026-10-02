# vlpds on benchbox, 2026-10-01/02: 2e64422 ("before") and fa0975c ("after")

Same box, driver and methodology as `../2026-10-02-benchbox/RESULTS.md` (Linux
baseline at f81857c) and `../2026-10-02/RESULTS.md` (laptop). Open-loop
latency from the scheduled send time, 10 s warmup and a 20 s window, a 200/s
hot repo and a firehose consumer on every grid step. Saturated = achieved
< 93% of offered, > 1% errors, or p99 > 2 s. inj25 = `--inject-put-ms 25`
(lognormal, sigma 0.5) on segment PUTs.

- **before** = 2e64422 (waves A+B: pipelined log PUTs `--log-inflight` (default 4),
  HTTP defaults, firehose runtime, getBlocks index). Default `--io-threads 6`.
- **after** = fa0975c (waves C–E: log retention, zstd SSTs and VLSEG05 zstd
  segment bodies, `--io-threads` = cores (32 here), proxy fast path,
  sharded firehose, scalable listRepos, pinned big repos). The
  `loadgen` used for the after set is fa0975c plus
  `loadgen-shard-listrepos.patch` (`fanout --shard-of N` and a `list-repos`
  enumeration command). The vlpds binary is exactly fa0975c.

All runs went through `bench/benchbox/run.sh` under `guard.sh`. Every run
started in the 19:54–00:00 UTC window (batch pipeline inactive). Nothing
aborted for the pipeline.

## Setup

- Box, kernel and disk as in the baseline. NVMe fsync is ~6.5 ms. 16C/32T.
- **vlpds and loadgen are native processes.** Only MinIO runs in Docker.
- **MinIO, tuned (every run except the A/B "defaults" row):**
  - `--network host --ipc host --log-driver none --security-opt seccomp=unconfined --ulimit nofile=1048576:1048576`;
  - no cgroup CPU/mem limits and no cpuset;
  - `MINIO_BROWSER=off`, `MINIO_API_REQUESTS_MAX=65536`, `GOMAXPROCS=32`;
  - single-drive mode;
  - data on NVMe (`~/vlpds-bench/minio`) unless marked tmpfs (`--tmpfs /data:size=32g`).

  `minio.sh info` prints this per run (in the `run-*.log` files). No
  docker-proxy ran for 9200/9201, even in the defaults container (benchbox's
  dockerd doesn't start a userland proxy for it).
- **Host:** no sysctl changes. `TcpExtListenOverflows` went from 16 to 1683
  over the session. The step that caused it wasn't isolated; it is likely
  the 1000-subscriber fan-out connect burst or the proxy runs at 1024
  in flight. somaxconn is 4096.
- **Profiles:**
  - before: on-demand `/debug/pprof` from the `--features profiling` build (`prof/*.pb`);
  - after: `--pyroscope-url http://127.0.0.1:4100` into Alloy → central
    Pyroscope (`service_name=vlpds`), headline steps only (`after-*-prof.jsonl`).
  - The headline numbers come from the default build (no sampler).
- **Runner changes:**
  - `CAP_GB` 250, plus `MIN_FREE_GB` 255 (abort if / drops below it);
  - `BIN_DIR`, `VARIANTS` (several server configs per suite);
  - `PROFILE_RATES` / `PROFILE_PROXY`;
  - `OUT` for every experiment;
  - `minio.sh` tuned/defaults/info.

### Docker overhead A/B (`docker-ab.jsonl`, 10k/5k inj0, before binary)

| MinIO container | 50k/s p50 / p99 | 75k/s achieved, p50 / p99 |
|---|---|---|
| Docker defaults (bridge + `-p`, json-file logs, default seccomp/ulimit) | 24.9 / 45.5 | 75.2k, 51.3 / 68.4 |
| Tuned (above) | 31.0 / 50.0 | 75.2k (1.1k dropped), 50.7 / 81.0 |

The two are within run-to-run noise: Docker was not the bottleneck. The
disk (fsync per PUT) and vlpds CPU are.

## Headline vs baselines (single node, MinIO on NVMe, 10k/5k unless noted)

| Benchmark | Laptop (b0282ce) | benchbox f81857c | before 2e64422 (K=4) | after fa0975c (K=4) |
|---|---|---|---|---|
| inj0, 10k/s p50 / p99 | 1.8 / 16.4 | 14.3 / 27.3 | 14.8 / 26.8 | 17.7 / 61.4 (K=1: 8.2 / 16.3) |
| inj0, 50k/s p50 / p99 | 5.3 / 28.3 | 54.0 / 259 | 55.2 / 74.1 | 39.6 / 122 |
| inj0, 75k/s | p99 65.9 | saturated (54.7k) | 75.2k, p99 240 | 75.2k, p99 197 |
| inj0 ceiling (achieved) | 88k | ~54k | ~80k | **~96k** |
| inj25, 50k/s p50 / p99 | sat. 44k | sat. 31k | 117 / 295 | **75 / 136** |
| inj25 ceiling | 44k | 31k | ~72–76k | **~80k** (1M/50k: 88k; 10M/50k: 86k) |
| 1M/50k inj25, 75k/s | – | – (38k ceiling) | 65k, p99 795 | **75.2k, p99 225** |
| 10M/50k inj25, 50k/s | p99 252 (laptop) | not run | – | 49.1k, p99 670 (75k/s: p99 421) |
| tmpfs MinIO, inj0 ceiling | – | 92.7k | – | 100k/s clean at p99 24 ms; ~94k at 125k |
| Proxy, 50k active | 81.6k | 204k (io16) | 196k (io16) | **315k** p99 7.0 |
| Proxy, 1M active | 72.8k | 155k (io16) | 167k (io32) | **268k** p99 3.9 |
| 3 nodes inj25, 50k/s p50 / p99 | 59 / 150 | 107 / 372 | 201 / 1140 | **123 / 276** |
| 10M/50k resource run, 50k/s inj25 | 9.8 GB RSS | not run | 39.9k achieved, p99 951 | **49.9k, p99 212**, RSS 14.8 GB |

## 1. Write grid: K scaling (`grid-10k*.jsonl`, `grid-1m.jsonl`, `after-grid-*.jsonl`)

One fresh prefix per (variant, injection), deleted afterwards. The first
attempt ran every variant on one prefix. It hit the 250 GB cap after 23 min
(~180 MB/s of MinIO growth), and latency and RSS drifted from one variant to
the next (`grid-10k-sharedprefix-aborted.jsonl`, not used). The inj0 phase of
a suite that ran inj25 first is also slowed by the compaction backlog (PUT
p50 10 → 20 ms). For before, the inj0 numbers come from fresh inj0-only runs
(`grid-10k-inj0first.jsonl`). After runs are all fresh per (K, inj).

Ceiling = best achieved rate. Cells are p50 / p99 ms; "sat" means the step
saturated.

| Shape | K | 25k/s | 50k/s | 75k/s | Ceiling |
|---|---|---|---|---|---|
| before 10k/5k inj25 | 1 | 76 / 151 | sat (31.2k, p99 901) | – | ~31k |
| before 10k/5k inj25 | 2 | 76 / 141 | 146 / 459 | sat (52.1k) | ~52k |
| before 10k/5k inj25 | 4 | 62 / 128 | 117 / 295 | 72.3k, 214 / 644 | ~76k |
| before 10k/5k inj25 | 4, 32 MB seg | 106 / 199 | 128 / 680 | 73.3k, 142 / 418 | ~80k |
| before 10k/5k inj25 | 4, io16 / io32 | 72 / 148 · 71 / 154 | 144 / 493 · 165 / 758 | sat 64–70k | 69k / 62k |
| before 10k/5k inj0 (fresh) | 1 / 2 / 4 | 17 / 71 · 47 / 88 · 27 / 72 | sat 41k · 133 / 440 · 55 / 74 | – · sat 54k · 75.2k, 187 / 240 | **41k / 54k / 80k** |
| before 1M/50k inj25 | 1 / 4 | 119 / 248 · 80 / 196 | sat 30.5k · 156 / 352 | – · sat 65k | 30k / 65k |
| after 10k/5k inj25 | 1 | 75 / 154 | sat (46.0k, p99 575) | – | ~46k |
| after 10k/5k inj25 | 4 | 65 / 126 | **75 / 136** | 74.1k, 184 / 355 | ~80k |
| after 10k/5k inj0 | 1 / 4 | 16 / 50 · 41 / 68 | 53 / 154 · 40 / 122 | sat 52k · 75.2k, 60 / 197 | 52k / **96k** |
| after 1M/50k inj25 | 1 / 4 | 77 / 545 · 86 / 220 | 48.2k sat · 86 / 181 | 44.6k · **75.2k, 118 / 225** | 48k / 88k |
| after 1M/50k inj0 | 1 / 4 | 42 / 72 · 40 / 74 | 59 / 187 · 43 / 78 | 69k sat · 72.8k, 63 / 607 | 69k / 98k |
| after 10M/50k inj25 | 1 / 4 | 133 / 887 · 93 / 433 | 43.2k sat · 86 / 670 | – · 74.1k, 134 / 421 | 43k / 86k |
| after 10M/50k inj0 | 1 / 4 | 53 / 260 · 56 / 359 | 57 / 655 · 49 / 128 | 64.9k sat · 75.1k, 58 / 252 | 65k / 99k |
| after tmpfs 10k/5k inj0 | 1 / 4 | 2.1 / 4.0 · 2.1 / 3.9 | 2.7 / 4.8 · 2.7 / 4.6 | 5.6 / 9.0 · 5.6 / 8.7 | 100k clean; ~89k / 94k |
| after tmpfs 10k/5k inj25 | 1 / 4 | 58 / 144 · 53 / 111 | 80 / 166 · 51 / 105 | sat 52k · 63 / 151 | 52k / **97k** |

Findings:

1. **K moves the inj25 ceiling ~1.7–2.4×, not 4×, and the tail does not grow.**
   - before: K=1 31k, K=2 52k, K=4 76k (2.4×).
   - after: 46k → 80k on 10k/5k, 48k → 88k on 1M/50k, 43k → 86k on 10M/50k
     (1.7–2×).
   - At equal offered load, K=4's p99 is the same or lower: 25k/s 126 vs
     154 ms; 50k/s 136 ms where K=1 is already saturated.
   - Past ~80–100k/s the server is CPU-bound, not log-bound: 1300–1600% at
     saturation for after, with p50 pinned ~200 ms. tmpfs MinIO tops out at
     the same ~94–97k.
   - K=1 itself rose from 31k to 46k: VLSEG05 zstd stores segments ~2×
     smaller, so each PUT carries ~2× more commits.
2. **Segment compression:** 1.9–2.6× (`segment_bytes_total / segment_stored_bytes_total`):
   - 2.6× on 10M/50k at low rates, 1.9× at 125k/s (bigger mixed segments).
   - Cost: 2–5 µs of compress CPU per commit, growing with rate.
   - After's p99 at 50k/s inj25 is 136 vs 295 ms before.
3. **inj0 on NVMe:** the before single-log ceiling (41k at K=1, the
   baseline's 54k) is gone: 96–99k at K=4.
   - Regression to look at: at 10k/s, after K=4 has p50 17.7 / p99 61 ms
     vs K=1's 8.2 / 16.3 ms (before: both 14.8 ms). It's a single
     sample, and the cause wasn't investigated (K=4 wrote 3.1k segments in
     the step vs K=1's 6.9k, so its segments were larger and sealed less
     often).
4. **32 MB segments (before, K=4):** about the same ceiling (~80k), with a
   worse tail at 25–50k/s. Keep 8 MB.
5. **More IO threads didn't help the write path** (before: io16 69k, io32
   62k vs 76k with 6). After's default (= cores) is 1.7× faster on the
   proxy and doesn't hurt writes.
6. Steps right after a 1M/10M bulk carry compaction debt. The first 10k/s
   step on 10M/50k has p99 450–740 ms, then it improves at 25–50k/s. Read
   the 10M first rows with that in mind.

### Profiles: write path, 10k/5k inj25 K=4, 50k/s

Top 10 by self time:

| before (pprof, 10 s, 486% CPU) | % | after (Pyroscope, 36 s) | % |
|---|---|---|---|
| secp256k1 u128_accum_mul | 8.4 | secp256k1 u128_accum_mul | 4.6 |
| slice compare (MST/key order) | 5.9 | memcpy | 3.5 |
| atomic_sub (Arc drops) | 3.8 | slice compare | 3.4 |
| memcpy | 3.7 | atomic_sub | 3.1 |
| atomic_load | 3.7 | atomic_load | 3.1 |
| secp256k1 sha256_transform | 3.5 | secp256k1 sha256_transform | 2.7 |
| secp256k1 modinv64_divsteps | 2.8 | atomic_add | 2.6 |
| atomic_add | 2.7 | syscall | 2.2 |
| secp256k1 fe_mul_inner | 2.2 | secp256k1 fe_storage_cmov | 1.6 |
| secp256k1 fe_storage_cmov | 2.1 | slice equal | 1.6 |

- Commit signing (libsecp256k1) is still the largest single cost, ~15–20%
  of CPU together.
- MST key compares and memcpy come next, then Arc refcount traffic (~9%).
- The CPU wall at ~95k/s is signing plus MST, spread over 8 workers
  (`--workers 8`). More workers, or batching signatures per commit group,
  is the next lever.

### Hot repo (before only, `hot.jsonl`)

| Rate | inj0 p50 / p99 | req/commit | inj25 p50 / p99 | req/commit |
|---|---|---|---|---|
| 1k/s | 8.7 / 66 | 1.02 | 68 / 145 | 1.03 |
| 5k/s | 27 / 62 | 2.55 | 74 / 145 | 2.51 |
| 20k/s | 38 / 139 | 5.62 | 85 / 169 | 5.17 |

The baseline had 20k/s at p99 78 (inj0) / 160 (inj25). inj0 p99 is worse
on 2e64422 (139). Not rerun on fa0975c.

## 2. Proxy fast path (`proxy.jsonl` before, `after-proxy.jsonl`)

1M bulk accounts, stub AppView (2 KB body), loadgen `proxy`.
- before: `--io-threads 16` (or 32), loadgen 6 (10) threads, stub 4.
- after: io-threads = 32, stub 8, loadgen 8 threads, `PROXY_CONNECTIONS` =
  the loadgen's h2 connections.

| | 50k active, 128 / 512 / 1024 in flight (req/s, p99 ms) | 1M active, 128 / 512 / 1024 |
|---|---|---|
| before, io16 | 188k 1.2 · 196k 5.4 · 194k 11.9 | 145k 1.6 · 157k 7.0 · 148k 16.8 |
| before, io32 | 148k 1.6 · 147k 7.0 · 158k 13.9 | 144k 1.6 · 163k 6.3 · 167k 13.2 |
| after, 16 conns | 272k 0.9 · 310k 3.4 · **313k** 6.6 | 216k 1.2 · **268k** 3.9 · 254k 8.4 |
| after, 64 conns | 260k 1.0 · 298k 3.7 · **315k** 7.0 | 207k 1.2 · 261k 4.2 · 243k 9.4 |
| after, 256 conns | 248k 1.0 · 286k 4.2 · 295k 8.2 | 198k 1.3 · 243k 4.8 · 216k 11.2 |

- **+60% at 50k active, +65–70% at 1M active.**
  - Server CPU per request: before ~75 µs (50k) / ~95–104 µs (1M);
    after ~53 µs / ~63–80 µs.
  - The token and account caches work: at 512 in flight, 0 account misses
    at 50k active and ~0.5% at 1M (before: 2M misses per step at 1M).
- 16 ≈ 64 connections > 256. More h2 connections cost ~5–15%: the
  per-connection lock isn't the limit on Linux; per-connection overhead is.
- **The box is the limit now:**
  - server ~1650–1790%, stub ~300%, loadgen ~650% = ~27 of 32 threads;
  - 2M req/s needs the client and stub off this box and/or a lower µs/request.
- **Where the time goes (after, Pyroscope, 50k active, 512 in flight, 292k req/s):**
  - top self: atomic_sub 6.9%, atomic_add 6.3%, memcpy 4.4%, slice equal
    3.6%, atomic_load 2.7%, hpack huffman decode 2.0%, writev 1.9%, slice
    `eq` (cum 5.1%), tower oneshot `project` 1.4%, CAS 1.2%, clock_gettime 1.1%;
  - **drop of a hickory-resolver `Lookup` future: 5.9% cumulative**, which
    looks like a DNS lookup or its future being built per upstream request;
  - rustc_demangle 3% is the Pyroscope agent itself.
  - Before, at 196k req/s, refcounts were 16.5%. The Arc clones per
    request were `Arc<App>` (14% of those), `Bytes` shallow clones (14%),
    `Arc<str>`, and reqwest's client internals (pool mutex, rustls config,
    resolver, connector config).
  - Next: drop the per-request resolver lookup (pin the AppView address)
    and the remaining per-request Arc clones.

## 3. Three-node cluster (`cluster.jsonl` before, `after-cluster.jsonl`)

3 processes × (`--workers 3 --io-threads 3`), 1M bulk accounts / 50k
active, inj25. One loadgen per node at rate/3, unrouted: ~2/3 of requests
are forwarded.

| Offered | before 2e64422: achieved, p50≤ / p99≤ | after fa0975c | benchbox f81857c |
|---|---|---|---|
| 25k | 25.2k, 116 / 625 | 25.2k, **65 / 151** | – |
| 50k | 50.2k, 201 / 1140 | 50.2k, **123 / 276** | 50k, 107 / 372 |
| 75k | sat 64.7k | sat 70.0k (p99 2.4 s) | 74.9k |
| 100k | – | sat 60.3k | ~74k |

Each node sits at ~400% at saturation (3 IO threads + 3 workers each), so
the cluster ceiling is CPU per node. The f81857c baseline reached ~74k
with the same per-node sizing; before and after saturate at 65–70k.

**Failover at 30k/s** (5 s windows; loadgens on the surviving nodes,
errors are 503 PartitionUnavailable):

| Event | before | after |
|---|---|---|
| kill -9 n3 at t=20, restarted at t=35.3 (serving at 35.6) | survivors 503 a third of requests for ~15 s; the dead node's shards are not taken over before the restart; ~1.3k/s partial 503s for ~12 s after the restart; done by t≈50 | same: 15 s at ~3.3k/s, then ~6.5k errors per 5 s window until t≈50. Firehose lag p99 14.7 s |
| SIGTERM n2 at t=20 (stop took 2.5 s), restarted, serving at 32.8 | ~5.7k 503s at stop (~1.7 s of its shards' traffic), then ~9.6k during handback (~3 s worth) | ~5.4k at stop (~1.6 s), ~9.7k during handback (~3 s); firehose lag p99 0.5 s |

**Handback is not sub-second here.** On SIGTERM the release costs ~1.6 s
of the leaving node's third of the traffic. The rejoin costs ~3 s more
(503 "partition not owned by this node" while shards move back). After a
kill -9, 503s run from the kill until ~15 s after the restart. The node
reclaims its shards (stable node id) instead of a survivor taking them
over at TTL. The in-repo test measured release → serve in ~1 ms, and
446 ms at 20 ms per store call. That doesn't show under load with
injected PUT latency and 85 shards per node. Each shard's SlateDB open
(~22 sequential store calls × 25 ms) is the likely cost.

## 4. Firehose (`firehose.jsonl` before, `after-firehose.jsonl`)

**End-to-end lag** (grid consumer, commit time → receipt) tracks the ack:
fh p50/p99 within 0–5 ms of the write p50/p99 at every rate up to the
ceiling (`fh-lag` in the grid JSONL). Examples (after, 10k/5k, K=4):
- inj0: 18/62 at 10k/s, 38/120 at 50k, 58/193 at 75k;
- inj25: 64/125 at 25k, 73/134 at 50k.

**Fan-out**, 100k repos, background writes, `loadgen fanout` 20 s:

| Writes/s | Subs | Delivered ev/s (MB/s) | Lag p50/p99 before | Lag p50/p99 after | Write p99 before / after |
|---|---|---|---|---|---|
| 2k | 10 | 20k (28) | 15 / 214 | 11 / 30 | 166 / 28 |
| 2k | 1000 | 2.0M (2.9 GB/s) | 17 / 63 | 17 / 30 | 48 / 29 |
| 10k | 100 | 1.0M (1.7 GB/s) | 45 / 100 | 43 / 90 | 92 / 90 |
| 10k | 1000 | 9.96M (17.7 GB/s) | 55 / 159 | 24 / 71 | 127 / **64** |
| 50k | 1 | 50k | 53 / 457 | 42 / 76 | 440 / **79** |
| 50k | 10 | 500k (1.0 GB/s) | 59 / 743 | 43 / 162 | 727 / **153** |
| 10k, `?shard=k/4` | 4 / 40 / 400 | 10k / 100k / 1.0M (2.1 GB/s) | – | 49/151 · 37/69 · 30/68 | – / 162 · 125 · 64 |
| 50k, `?shard=k/4` | 4 / 40 | 50k / 500k | – | 44/80 · 39/69 | – / 83 · 100 |

- **Write p99 stays low under fan-out:** at most 153 ms (after) at
  500k ev/s out. Laptop b0282ce: 0.85–2 s.
- 1000 subscribers at 10k ev/s each (~10M ev/s, 17.7 GB/s over loopback):
  every subscriber kept up (min 9962 ev/s). The shared pre-framed batches
  make egress a memcpy problem.
- Sharded subscriptions: each of the 4 slices gets ~1/4 of the events
  (2.45–2.52k of 10k/s, 12.4–12.6k of 50k/s). The union equals the full
  stream, 0 out-of-order.

**Cursor backfill** (`--firehose-ring-mb 64`, cursor=1 from S3):
- before: the whole 4.70M-event log in < 10 s per subscriber,
  ≥ **470k ev/s** each (laptop b0282ce: 35k ev/s);
- after: 8.64M events in < 10 s, ≥ **864k ev/s** (1.67 GB/s) per subscriber,
  4 subscribers ≥ 3.46M ev/s total.
- Both are lower bounds: the 10 s window outlasted the log.

## 5. Repo-size sweep (`sweep.jsonl` before, `after-sweep.jsonl`)

One repo per size, no injection. Cells are ops/s / p99 ms at concurrency 16.

| Method | 100 | 10k | 1M | 10M |
|---|---|---|---|---|
| getRecord before | 132k / 0.30 | 111k / 0.35 | 51k / 0.68 | 63k / 0.56 |
| getRecord after | 115k / 0.33 | 80k / 0.44 | 53k / 0.60 | **35k / 0.79** |
| getBlocks10 before | 28.5k / 1.0 | 13.9k / 1.7 | 4.7k / 4.5 | 1.9k / 10.3 |
| getBlocks10 after | 25.7k / 0.96 | 12.9k / 1.6 | 4.7k / 4.5 | **3.6k / 5.9** |
| listRecords before / after | 35k / 36k | 34k / 36k | 27k / 28k | 15k / 17k |
| describeRepo before / after | 28k / 25k | 28k / 25k | 20k / 17k | 10k / 13k |
| listBlobs before / after | 113k / 94k | 38k / 35k | 9.6k / 12.0k | 7.7k / 11.6k |
| getRepo export (before = after) | – | 3 MB 10 ms | 276 MB 0.85 s | 2.77 GB 8.3 s (~330 MB/s) |
| fill rec/s before / after | 10k / 12k | 88k / 135k | 123k / 220k | 47k / 70k |

- getBlocks is fixed relative to b0282ce (laptop: 22/s at 10M; benchbox now
  1.9–3.6k/s with 10 random CIDs per call). It still drops ~7× from 100 to
  10M records.
- **Regression:**
  - getRecord after is 13–28% lower on small repos, and **−44% at 10M**
    (63k → 35k/s). Likely zstd SST block decompression on point reads
    (block cache misses at 10M), or the pinned-repo path.
  - describeRepo and listBlobs at 100 records also lost ~10–17%.
  - Worth a profile.
- Fill: 1.5–1.8× faster at every size (zstd SSTs, single-shard ingest fixes).

**listRepos enumeration** (after, 10M repos, `limit=1000`): 10M repos in
58–66 s = **152–173k repos/s** from one client. Page p50 6 ms, p99 10–18 ms,
1.48 GB of JSON.

## 6. Methods (before only, `methods.jsonl`, inj0, 64 in flight, 2000 small repos)

| Method | ops/s | p50 / p99 ms | benchbox f81857c |
|---|---|---|---|
| describeServer | 186k | 0.30 / 0.81 | 200k |
| getRecord | 110k | 0.54 / 1.24 | 116k |
| getLatestCommit / getRepoStatus | 133k / 134k | 0.44 / 1.1 | – |
| sync.getRecord / sync.getRepo | 107k / 42k | 0.57 / 1.2 · 1.5 / 2.5 | – |
| getSession | 76k | 0.81 / 1.7 | – |
| createRecord / putRecord / applyWrites10 | 4.7k / 4.7k / 4.4k | 12.7 / 24 | 4.9k |
| deleteRecord | 97k | 0.62 / 1.4 | – |
| uploadBlob 64k / 1M | 2.3k / 273 | 26 / 60 · 190 / 692 | – |
| createSession / createAccount | 841 / 396 | 72 / 127 · 124 / 1043 | 829 / 792 |

Commit-producing writes at 64 in flight are still fsync-bound on NVMe
(PUT ~10–20 ms). createAccount is half the baseline's rate (396 vs 792/s).
The p99 is 1 s, so it is worth checking.

## 7. Resources at 10M repos (`resource.jsonl` before, `after-resource.jsonl`)

Bulk of 10M × 5 records, then a fresh server per active window, idle 15 s,
then 70 s at 50k/s inj25. Defaults otherwise.

| | before 10M/50k | before 10M/500k | after 10M/50k | after 10M/500k |
|---|---|---|---|---|
| bulk 10M | 280 s (36k/s) | – | **119 s** (84k/s) | – |
| 50k/s inj25 | **39.9k achieved**, p50 274, p99 951 | 24.5k achieved, p99 2.0 s | **49.9k**, p50 78, p99 212 | 49.4k, p50 77, p99 538 |
| server CPU avg | 568% | 816% | 830% | 1668% |
| RSS after load | 14.3 GB | 10.8 GB | 14.8 GB | 14.5 GB |
| jemalloc allocated | 10.7 GB | 8.7 GB | 12.0 GB | 12.6 GB |
| cached repos | 400k (8 × 50k) | 400k | 800k (16 workers) | 731k |
| repo cache bytes | – | – | ~3.4 GB (16 × 0.21) | ~3.5 GB |
| token cache | – | – | 0.88 GB | 1.23 GB |
| SlateDB disk cache | 41 GB | 45 GB | **15 GB** | 16 GB |

- after sustains the 50k/s that before couldn't, at 10M/50k and 10M/500k.
  The laptop at b0282ce needed `--block-cache-mb 16384` for 500k.
- RSS is ~15 GB: ≤ 5 GiB block + meta cache, ~3.5 GB repo cache, ~1 GB
  token cache, plus memtables and the firehose ring. RSS is flat between
  50k and 500k active, because the caches are bounded by entries and bytes.
- zstd SSTs cut the on-disk SlateDB cache ~2.7× (41 → 15 GB). The segment
  compression ratio is in §1 (1.9–2.6× for 2–5 µs/commit).

## What limits each path now

- **Single-node writes:** CPU. Signing + MST + Arc traffic, ~1600% at
  ~95k/s. K=4 removed the log cap; tmpfs and NVMe now top out within ~5%
  of each other.
- **Proxy:** the box. Server + stub + loadgen share 32 threads. Per-request
  CPU is ~53 µs, with refcount traffic and a resolver lookup per upstream
  request in it.
- **Cluster:** per-node CPU at 3+3 threads per node. The rejoin and
  failover handback cost 3–15 s of 503s for the moving third of traffic.
- **Firehose:** not a limit at these rates. Writes stay < 160 ms p99 at
  500k ev/s egress.
- **Reads at 10M:** getRecord regressed (35k/s). getBlocks is 3.6k/s.

## Files

- **before:**
  - `grid-10k.jsonl` (6 variants, inj25 then inj0; inj0 rows contaminated, see §1);
  - `grid-10k-inj0first.jsonl`, `grid-1m.jsonl`, `grid-prof.jsonl` +
    `prof/*.pb`, `proxy.jsonl`, `proxy-prof.jsonl`;
  - `hot.jsonl`, `cluster.jsonl`, `firehose.jsonl`, `methods.jsonl`,
    `sweep.jsonl`, `resource.jsonl`, `resource-grid.jsonl`;
  - `docker-ab.jsonl`;
  - `grid-10k-sharedprefix-aborted.jsonl` (discarded).
- **after:** `after-grid-{10k,1m,10m,10k-tmpfs,prof}.jsonl`,
  `after-proxy{,-prof}.jsonl`, `after-cluster.jsonl`,
  `after-firehose.jsonl`, `after-sweep.jsonl`, `after-resource{,-grid}.jsonl`.
- `loadgen-shard-listrepos.patch`, and `run-*.log` per invocation (local
  time tags, UTC−7).

## Disk state on benchbox at the end

- MinIO container removed, MinIO data and `.trash` wiped, scratch caches deleted.
- `~/vlpds-bench` = 5.0 GB:
  - `target/` (fa0975c release `vlpds`, patched `loadgen`);
  - `target-prof/` (fa0975c `--features profiling`);
  - `src/` (fa0975c + loadgen patch), scripts, `results/`.
- / has 563 GB free (564 GB at the start). The lowest seen was ~313 GB, at
  the aborted shared-prefix run (the 250 GB cap tripped).
- No sysctls changed; no other containers started (only procmon-agent runs).
