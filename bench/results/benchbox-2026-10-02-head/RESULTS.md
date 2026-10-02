# vlpds on benchbox, 2026-10-02: head (fcf29b3) vs the fa0975c baseline

Commit under test: **fcf29b3** (vlpds head when the campaign started; c1ea14b
and 723889c landed during it and are not included). Baseline:
`../2026-10-03-benchbox/RESULTS.md` ("after" = fa0975c), same box, driver and
methodology unless noted. Release build (`[profile.release]`, debuginfo),
native vlpds/loadgen, MinIO in Docker with the tuned flags (host network, no
log driver, no seccomp, nofile 1M; `minio.sh info` in every `run-*.log`).
Every run went through `bench/benchbox/drive.sh` → `runner.sh` under `guard.sh`
(batch pipeline idle, next trigger ≥ 75 min away); nothing aborted.

Driver: `bench.py` here is a copy of `../2026-10-02/bench.py` with:
64-shard-aware cluster convergence (`vlpds_shard_layout_shards`), `SHARDS`
env for clusters, per-step `cpu_us_per_commit` (process CPU / commits) and
`commit_build_us` (`vlpds_commit_build_seconds` mean), and a `coldload`
command (block 3). `shardsweep.py` is block 2's driver (a copy of the cost
model's `measure.py`).

## Summary

| Area | Result vs fa0975c / design claims |
|---|---|
| Single-node write ceiling | **Regression: 88k → 64k (1M/50k inj25), 98k → 68k (inj0), 80k → 57k (10k/5k inj25); p99 at 50k/s inj25 181 → 542 ms.** CPU per commit +35–50%. Bisected (1f): partial MSTs' per-commit `repo_bytes` re-walk (`mst_lazy::heap_bytes`, 12% of CPU) and hedged signing + verify-after-sign (+20 µs/commit on the commit thread) |
| getRecord at 10M records | **69.6k/s, confirmed ≥ 63k** (fa0975c 35k) |
| getRepo / getBlocks at 10M | **Regression**: getRepo export 8.3 → 22 s (330 → 120 MB/s), getBlocks10 3.6k → 2.4k/s |
| createAccount | **832/s confirmed** (vs 396 at 2e64422, 792 at f81857c), p99 127 ms |
| Proxy, PROXY_CONNECTIONS 16/64/256 | 16 ≈ 64 > 256 (−7–11%) on Linux; 334k req/s at 50k active, 302k at 1M (+6% / +13% vs fa0975c), 49–53 µs/req |
| Shard sweep 16/32/64/256 (3 nodes) | Cost model's 64-shard extrapolation **within 4%** on requests ($1,589 vs $1,655/mo S3 at 345 commits/s); model 8–11% high elsewhere; latency and CPU/commit independent of shard count; 0.40 polling GET/s per shard exactly |
| Partial MSTs + NVMe disk cache | 1M-record repo cold write 162/251 ms (p50/p99, S3-latency misses) → **16/29 ms disk-warm** (mem-warm 9/14); 10k–100k repos cold p50 7–14 ms; RSS ~3 GB with 2,340 big repos cached |
| Cross-host 3-node cluster | Ceiling 20–30k/s (devhost-bound). **Critical: a kill -9 makes the survivors fail-stop** (S3 connection explosion → ephemeral-port exhaustion → lease lapse); devhost-node firehose subscribers lag 4–5 s at 20k/s |
| 100M-account capacity run | see Block 5 (bulk on one MinIO trips 10 s leases; a restart after those fail-stops left 26/64 shards unowned) |

## Block 1: confirmation runs

### 1a. Single-node write grid (`grid-*.jsonl`): **regression**

Fresh prefix per row (bulk, then stair-step), K=4 (default), 64 shards
(default; the baseline had 256), open-loop, 10 s warmup + 20 s window, 200/s
hot repo + firehose consumer. Cells p50 / p99 ms.

| Shape | 25k/s | 50k/s | 75k/s | Ceiling | fa0975c (after) |
|---|---|---|---|---|---|
| 1M/50k inj25 | 67 / 131 | 49.6k achieved, 11k dropped, 100 / **542** | sat **64.3k** | **~64k** | 86/220 · 86/181 · 75.2k 118/225 · **88k** |
| 1M/50k inj0 | 42 / 106 | 48.9k, 42k dropped, 66 / **600** | sat **67.8k** | **~68k** | 40/74 · 43/78 · 72.8k 63/607 · **98k** |
| 10k/5k inj25 | 64 / 124 (10k/s: 66 / 125) | 50.2k, 154 / **405** | sat **57.3k** | **~57k** | 65/126 · 75/136 · 74.1k 184/355 · **80k** |

CPU per commit (whole process CPU ÷ commits in the step):

| Shape | 25k/s | 50k/s | at saturation | fa0975c 25k / 50k / 75k |
|---|---|---|---|---|
| 1M/50k inj25 | 234 µs | 218 µs | 242–252 µs (1621%) | 152 / 164 / 139 µs |
| 1M/50k inj0 | 233 µs | 217 µs | 241–248 µs (1678%) | 160 / 147 / 162 µs |
| 10k/5k inj25 | 204 µs | 244 µs | 298 µs (1808%) | 155 / 141 / 144 µs |

- **The write path costs ~35–50% more CPU per commit than fa0975c**, and
  the single-node ceiling fell accordingly: 88k → 64k (1M inj25), 98k → 68k
  (1M inj0), 80k → 57k (10k/5k inj25). At 50k/s the server is at
  1060–1220% (fa0975c: 710–820%), so the tail and drops at 50k/s are the CPU
  wall arriving earlier, not the log.
- `vlpds_commit_build_seconds` (build + sign) averages **57–70 µs** per
  commit here; it is wall time inside the worker, so it includes run-queue
  waits under load (it rises with rate).
- 25k/s on 1M/50k is not comparable one-to-one: these grids start at 25k/s,
  so that step does the window's ~63k cold loads that the baseline's 10k/s
  step absorbed. 50k/s+ has the same load counts (~1.3k vs ~0.8k).
- Bisect: see 1f.

### 1b. getRecord / read sweep at 10M records (`sweep.jsonl`)

One repo per size, concurrency 16, ops/s / p99 ms.

| Method | 100 | 10k | 1M | 10M | fa0975c 10M | 2e64422 10M |
|---|---|---|---|---|---|---|
| getRecord | 131.4k / 0.27 | 126.6k / 0.28 | 79.2k / 0.44 | **69.6k / 0.46** | 35k / 0.79 | 63k / 0.56 |
| sync.getRecord | 96.7k / 0.35 | 90.4k / 0.37 | 82.7k / 0.40 | 69.1k / 0.47 | – | – |
| getLatestCommit | 118.2k / 0.31 | 112.3k / 0.33 | 107.8k / 0.34 | 93.4k / 0.38 | – | – |
| getRepoStatus | 119.4k / 0.32 | 116.7k / 0.32 | 113.7k / 0.32 | 97.4k / 0.36 | – | – |
| listRecords | 48.6k / 0.60 | 37.9k / 0.72 | 27.5k / 0.93 | 22.0k / 1.13 | 17k | 15k |
| describeRepo | 26.5k / 0.95 | 26.5k / 0.94 | 17.7k / 1.29 | 16.1k / 1.41 | 13k | 10k |
| getBlocks10 | 24.6k / 1.03 | 26.6k / 0.98 | 6.8k / 3.10 | **2.4k / 8.79** | 3.6k / 5.9 | 1.9k / 10.3 |
| listBlobs | 111.0k / 0.31 | 50.3k / 0.57 | 14.3k / 1.71 | 11.9k / 1.98 | 11.6k | 7.7k |
| getRepo export | 28 KB | 2.7 MB 9 ms | 276 MB **1.30 s** | 2.77 GB **21.8–23.5 s** (~120 MB/s) | 0.85 s / 8.3 s (~330 MB/s) | same |
| fill rec/s | 10k | 103k | 208k | 97k | 70k (10M) | 47k |

- **getRecord at 10M: 69.6k/s, confirmed ≥ 63k** (TODO; fa0975c 35k). The
  MetaCache fix holds; small repos are also back above fa0975c (131k vs 115k).
- **Regression: getRepo export of big repos is 1.5–2.8× slower**: 1M records
  0.85 → 1.30 s, 10M 8.3 → 22 s (330 → ~120 MB/s). Partial MSTs read the
  tree from storage instead of a loaded full tree (the full-tree mode is gone).
- **Regression: getBlocks10 at 10M records 3.6k → 2.4k/s** (p99 5.9 → 8.8
  ms); at 1M it improved (4.7k → 6.8k).
- Fill is faster at every size (10M: 70k → 97k rec/s).

### 1c. createAccount / methods (`methods.jsonl`, inj0, 64 in flight, 2000 small repos)

| Method | ops/s | p50 / p99 ms | 2e64422 | benchbox f81857c |
|---|---|---|---|---|
| **createAccount** | **832** | 73 / 127 | 396 (p99 1043) | 792 |
| createSession | 1,070 | 58 / 98 | 841 | 829 |
| refreshSession | 2,161 | 26 / 80 | – | – |
| createRecord / putRecord / applyWrites10 | 7.4k / 7.4k / 7.4k | 8.2 / 14.4 | 4.7k / 4.7k / 4.4k | 4.9k |
| applyWrites200 | 2.9k | 22 / 33 | – | – |
| deleteRecord | 73.9k | 0.85 / 1.4 | 97k | – |
| describeServer | 141.9k | 0.44 / 0.87 | 186k | 200k |
| getRecord | 84.6k | 0.75 / 1.26 | 110k | 116k |
| getLatestCommit / getRepoStatus | 94.1k / 91.8k | 0.67 / 1.1 | 133k / 134k | – |
| sync.getRecord / sync.getRepo | 72.5k / 43.1k | 0.87 / 1.4 · 1.5 / 2.4 | 107k / 42k | – |
| getSession | 77.9k | 0.81 / 1.3 | 76k | – |
| listRecords / describeRepo | 67.3k / 39.3k | – | – | – |
| uploadBlob 64k / 1M | 2.3k / 243 | 26 / 181 · 264 / 396 | 2.3k / 273 | – |
| getBlob 64k | 26.5k | 2.4 / 4.3 | – | – |
| listRepos | 1.8k | 35 / 54 | – | – |

- **createAccount: 832/s (confirmed; TODO asked vs 396/792)**: 2.1× 2e64422
  and above the f81857c baseline; p99 1043 → 127 ms. createSession +27%.
- Commit-producing writes at 64 in flight: 4.7k → 7.4k/s (fsync-bound;
  p50 12.7 → 8.2 ms).
- The cheap reads at 64 in flight are 20–30% below the 2e64422 numbers
  (describeServer 186k → 142k, getRecord 110k → 85k), **but not a
  regression**: fa0975c's binary on the same box today does the same
  (134k / 79k / 91k, `ab-methods.jsonl`), so it is box/run-to-run, and head
  is at or above fa0975c on every read (describeServer 142–165k, getRecord
  85k). createAccount in the A/B runs: fa0975c 645/s (p99 531) → head
  832–978/s.

### 1d. Proxy at PROXY_CONNECTIONS 16 / 64 / 256 (`proxy-c{16,64,256}.jsonl`)

1M bulk accounts, stub AppView (2 KB), io-threads 32, stub 8 threads,
loadgen 8 threads; req/s and p99 ms; server CPU µs per proxied request.

| h2 conns | 50k active: 128 / 512 / 1024 in flight | µs/req | 1M active: 128 / 512 / 1024 | µs/req |
|---|---|---|---|---|
| 16 | 278k 0.92 · 315k 3.3 · **321k** 6.4 | 52.5–52.9 | 196k 1.4 · 294k 3.7 · 295k 7.0 | 59–97 |
| 64 | 270k 0.97 · 323k 3.5 · **334k** 6.8 | **48.7–51.6** | 187k 1.4 · 294k 3.9 · **302k** 7.4 | 56–100 |
| 256 | 256k 1.0 · 294k 4.1 · 299k 8.1 | 51.8–53.5 | 170k 1.5 · 262k 4.6 · 268k 8.9 | 62–108 |
| fa0975c 16 / 64 / 256 | 313k / 315k / 295k (1024) | ~53 | 268k / 261k / 243k (512) | 63–80 |

- **Linux confirms 16 ≈ 64 > 256** (TODO "proxy bench with PROXY_CONNECTIONS
  16/64/256"): 256 connections cost 7–11% throughput and ~1–3 µs/req; no
  per-connection lock cost shows at 16.
- vs fa0975c: +6% at 50k active (334k vs 315k), **+13% at 1M active**
  (302k vs 268k); µs/req 53 → 49 at 50k, 63–80 → 56–66 at 1M (512+ in
  flight). The 128-in-flight 1M row (~100 µs/req) is the account/JWT cache
  warm-up: 1M misses happen inside its 15 s.
- Still box-bound: server 1500–1790%, stub ~300%, loadgen ~650–730%.

### 1e. Commit CPU with hedged + verified signing and partial MSTs

`worker::bench_commit_cpu` (`bench/benchbox/commit-cpu.sh`, `cargo test
--release --lib`, thread CPU of the commit path: validate, MST insert,
diff + node refs, sign, CAR, frame, mutations, durable-view swap; best of 7
× 3,000 commits), 3 interleaved rounds per commit (`commit-cpu.jsonl`):

| Commit | 20-record repo | 5,000-record repo | state B/commit | segment B/commit |
|---|---|---|---|---|
| c36f6d6 (full trees, deterministic signing) | 18.7–18.9 µs | 18.7 µs | – (not printed) | – |
| 522e9aa (partial MSTs only, before hedging) | 22.9 µs | 23.4–23.5 µs | 3,277 / 3,503 | 3,743 / 3,946 |
| **fcf29b3 (head: hedged nonce + verify-after-sign)** | **43.4–43.9 µs** | **43.9 µs** | 3,277 / 3,503 | 3,743 / 3,946 |

- **Hedged nonces + verify-after-sign cost +20.5 µs per commit on the
  commit thread (23 → 44 µs, 1.9×)**: a secp256k1 verification is ~2× a
  signature, plus 32 bytes of ChaCha per signature. On a 16-core node at
  ~60k commits/s that is ~1.2 cores. Spread over all threads the node-level
  delta was +18–30 µs/commit (1f). Options if this matters: verify on a
  sample, or verify asynchronously before the segment PUT rather than on the
  worker's critical path (it still must finish before the commit is
  sequenced).
- **Partial MSTs on the commit path itself: +4.6 µs** (18.8 → 23.4 µs) on a
  loaded tree. That is small; the node-level partial-MST cost (+45–70 µs) is
  outside this benchmark: the per-commit `repo_bytes` re-walk (1f) and the
  node loads/unloads.
- State bytes per commit 3.3–3.5 KB (keys + values written to SlateDB,
  incl. `M/` interior nodes), segment bytes 3.7–3.9 KB (frame + stored
  muts, before zstd).

### 1f. Bisect of the write-path CPU regression (`ab-grid-10k-inj25.jsonl`, `ab-methods.jsonl`, `prof-grid-10k-inj25.jsonl`)

Each commit's own `vlpds` + `loadgen` built on benchbox (`bench/benchbox/build-at.sh`),
same driver, 10k/5k inj25, fresh prefix each, run back to back in one window.
Cells p50 / p99 ms; CPU = process CPU per commit; build = mean
`vlpds_commit_build_seconds`; RSS at 50k/s.

| Binary | 25k/s | 50k/s | 75k/s | Ceiling | CPU µs/commit 25k / 50k | build µs | RSS GB |
|---|---|---|---|---|---|---|---|
| fa0975c (baseline, rerun today) | 66 / 133 | 61 / 130 | 144 / 328 | 77.5k | 154 / 141 | 34 | 4.6 |
| c36f6d6 (write-path CPU pass) | 63 / 128 | 86 / 173 | 193 / 358 | 75.1k | 138 / 134 | 29 | 4.7 |
| c36f6d6 `--shards 64` | 81 / 154 | 100 / 202 | 201 / 424 | 73.9k | 131 / 130 | 29 | 4.5 |
| **a710e0c `--lazy-mst false`** | 82 / 140 | 97 / 201 | 217 / 352 | **74.3k** | **137 / 133** | 28 | **4.8** |
| **a710e0c (partial MSTs on)** | 62 / 117 | 113 / 327 | sat 58.8k | **58.8k** | **182 / 204** | 32 | **11.0** |
| b034d2b | 64 / 118 | 127 / 512 | sat 57.6k | 57.6k | 177 / 205 | 33 | 9.0 |
| 522e9aa (before hedged signing) | 63 / 125 | 104 / 246 | sat 58.3k | 58.3k | 176 / 206 | 32 | 9.1 |
| **1e48efd (hedged nonce + verify-after-sign)** | 64 / 124 | 110 / 562 | sat 63.0k | 63.0k | 194 / 235 | **62** | 9.1 |
| fcf29b3 (head) | 65 / 124 | 102 / 527 | sat 57.6k | 57.6k | 203 / 248 | 65 | 9.4 |
| fcf29b3 `--shards 256` | 63 / 120 | 110 / 269 | sat 54.5k | 54.5k | 208 / 247 | 64 | 11.2 |

Two causes, both confirmed by A/B on one binary or adjacent commits:

1. **Partial MSTs (a710e0c, `--lazy-mst`): +45–70 µs CPU per commit, ceiling
   74k → 59k, RSS at 50k/s 4.8 → 11 GB.** Same binary, flag on vs off. The
   profile of head at 50k/s (Pyroscope, `service_name=vlpds`,
   2026-10-02T11:39:35–11:40:15Z, BIN_DIR=target-prof) has
   **`vlpds::mst_lazy::heap_bytes` at 11.6% flat / 12.9% cumulative of all
   CPU**: `worker::repo_bytes` → `LazyTree::heap_bytes` walks every loaded
   node of the repo on every commit (worker.rs:961 `let charge =
   repo_bytes(st)`, plus 1416/1448) to charge the repo cache budget. Its
   cost grows with how much of a repo is loaded, so repos that keep taking
   writes (the 200/s hot repo, the active window) pay more every commit.
   Fix: account the charge incrementally (delta on node load/insert/unload)
   instead of re-walking. The RSS doubling is worth a look too
   (loaded paths + the lazy node cache vs the full trees it replaced).
2. **Hedged nonces + verify-after-sign (1e48efd): +25–30 µs per commit**
   (commit build 32 → 62 µs; process CPU +18–30 µs). In the profile,
   libsecp256k1 (field mul/sqr, u128 accum, modinv) is ~20% of CPU, up from
   ~15% for signing alone at fa0975c. This is a deliberate hardening; the
   cost is a verification per commit (~2× a signature). The ceiling moved
   58 → 63k between 522e9aa and 1e48efd only by noise; the CPU did go up.

Not causes: the default shard count (64 vs 256: same CPU on head and on
c36f6d6), everything between a710e0c and 522e9aa (b034d2b metrics, rate
limits, proxy pool: within noise of a710e0c), and the 32-bit shard ids /
later commits (1e48efd → head within noise).

Other profile lines at 50k/s (head): slice compare 3.7% (SlateDB
memtable), memcpy 3.3%, Arc refcount atomics ~7.6%, zstd segment compress
~2%, `mst_lazy::locate`/`loaded_path` ~3.2% cumulative, SlateDB merge
iterator 3.4% cumulative, and the hickory `Lookup` drop glue 1.7% (the
identical-drop-glue fold noted in TODO, not real DNS).

## Block 2: shard-count sweep at today's load shape (`shardsweep.jsonl`, `shardsweep-summary.json`)

`shardsweep.py sweep --nodes 3 --shard-list 16,32,64,256 --high 20000`: the
cost model's method (`../cost-model-2026-10-02/measure.py`) on benchbox, per
shard count a fresh prefix: 1 node populates 1M repos (real/32
distribution, 16M records), then 3 nodes (2 workers + 4 IO threads each,
30 s lease TTL, `--log-retention 20m`, disk cache) join; phases **join 180 s
→ idle 120 s → 345 commits/s 480 s → 900/s 480 s → 20,000/s 300 s → drain
120 s**; a 50k-repo active window with 12 new repos/s (≈100–180 cold
loads/s), 98% creates / 2% deletes, all writes through n1 (2/3 forwarded).
Injected S3 latency: segment PUTs 30 ms median, state pool 20 ms reads / 30
ms writes (σ 0.5). Every node scraped every 30 s. Requests/s summed over the
3 nodes; "model" = `cost_model.requests()` fitted on the cost model's own
256/1,024-shard runs, with the current defaults (10 s manifest poll, 30 s
compactor polls) and this run's TTL; $ = S3 request cost per month at the
measured (model) rate. Table from `sweep_report.py`:

| shards | phase | commits/s | loads/s | CPU µs/commit | cores | p50 / p99 ms | L0 SSTs/shard avg (all-shard max) | L0 stalls | seg PUT | SST PUT | CAS | poll GET | SST GET | LIST | ctl | Class A / B | model A / B | S3 req $/mo (model) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 16 | idle | 0.0 | 0.0 | - | 0.09 | - / - | 2.31 (37) | 0 | 0.0 | 0.0 | 0.0 | 6.3 | 0.0 | 1.1 | 1.8 | 1.6 / 8.0 | 1.8 / 7.7 | $30 ($32) |
| 16 | avg | 344.3 | 105.6 | 1,135 | 0.39 | 62.4 / 138.4 | 3.34 (65) | 0 | 74.4 | 2.9 | 4.8 | 65.1 | 18.0 | 2.1 | 1.8 | 84.7 / 84.7 | 90.6 / 157.3 | $1,203 ($1,357) |
| 16 | burst | 897.8 | 113.0 | 757 | 0.68 | 66.4 / 145.2 | 3.34 (64) | 0 | 69.6 | 3.1 | 5.3 | 74.0 | 22.0 | 2.3 | 1.8 | 80.8 / 97.7 | 90.9 / 167.3 | $1,166 ($1,372) |
| 16 | high | 19,434.1 | 177.7 | 291 | 5.65 | 111.0 / 376.3 | 2.68 (59) | 0 | 46.5 | 5.4 | 9.4 | 170.1 | 43.1 | 2.7 | 1.5 | 65.6 / 214.6 | 91.1 / 267.9 | $1,088 ($1,479) |
| 32 | idle | 0.0 | 0.0 | - | 0.13 | - / - | 1.53 (49) | 0 | 0.0 | 0.0 | 0.0 | 12.9 | 0.0 | 1.1 | 2.0 | 1.6 / 14.8 | 2.0 / 14.3 | $37 ($41) |
| 32 | avg | 344.3 | 105.6 | 1,365 | 0.47 | 63.1 / 140.0 | 3.26 (128) | 0 | 73.8 | 5.5 | 9.0 | 109.0 | 34.2 | 2.9 | 2.0 | 91.7 / 145.1 | 97.1 / 176.9 | $1,358 ($1,463) |
| 32 | burst | 897.8 | 113.0 | 867 | 0.78 | 66.5 / 144.4 | 3.46 (130) | 0 | 69.7 | 5.8 | 9.5 | 119.3 | 40.0 | 2.9 | 2.2 | 88.4 / 161.3 | 97.4 / 186.9 | $1,333 ($1,477) |
| 32 | high | 19,469.6 | 177.7 | 306 | 5.97 | 114.6 / 372.7 | 3.45 (129) | 0 | 45.3 | 6.5 | 11.4 | 134.3 | 55.1 | 3.0 | 1.5 | 68.0 / 190.8 | 97.5 / 287.5 | $1,095 ($1,585) |
| 64 | idle | 0.0 | 0.0 | - | 0.11 | - / - | 0.34 (22) | 0 | 0.0 | 0.0 | 0.0 | 25.7 | 0.0 | 1.1 | 2.6 | 1.6 / 28.1 | 2.3 / 27.5 | $51 ($59) |
| 64 | avg | 344.2 | 105.6 | 1,334 | 0.46 | 63.2 / 142.7 | 3.28 (273) | 0 | 73.9 | 9.5 | 15.4 | 162.8 | 58.0 | 3.7 | 2.6 | 103.0 / 223.3 | 108.8 / 213.5 | $1,589 ($1,655) |
| 64 | burst | 897.6 | 113.0 | 853 | 0.77 | 66.9 / 146.6 | 3.45 (247) | 0 | 69.1 | 10.4 | 16.4 | 183.4 | 69.6 | 4.0 | 2.8 | 100.4 / 255.7 | 109.1 / 223.5 | $1,589 ($1,669) |
| 64 | high | 19,524.8 | 177.7 | 300 | 5.86 | 111.3 / 361.2 | 3.19 (238) | 0 | 45.7 | 9.8 | 14.9 | 134.6 | 72.9 | 2.5 | 1.5 | 73.4 / 208.9 | 109.2 / 324.2 | $1,185 ($1,777) |
| 256 | idle | 0.0 | 0.0 | - | 0.10 | - / - | 0.27 (68) | 0 | 0.0 | 0.0 | 0.0 | 102.2 | 0.0 | 1.1 | 5.8 | 1.6 / 107.8 | 4.2 / 107.1 | $135 ($168) |
| 256 | avg | 344.2 | 105.6 | 1,323 | 0.46 | 64.8 / 144.3 | 2.75 (914) | 0 | 72.3 | 20.2 | 36.7 | 344.7 | 92.6 | 7.2 | 5.8 | 137.0 / 442.9 | 156.1 / 386.4 | $2,267 ($2,459) |
| 256 | burst | 897.4 | 113.1 | 883 | 0.79 | 68.4 / 149.5 | 3.00 (895) | 0 | 67.9 | 25.0 | 40.6 | 396.2 | 162.8 | 7.5 | 6.8 | 141.5 / 565.7 | 156.4 / 396.4 | $2,456 ($2,474) |
| 256 | high | 19,629.1 | 177.6 | 301 | 5.91 | 115.8 / 318.5 | 2.68 (857) | 0 | 44.2 | 21.1 | 32.7 | 321.4 | 139.1 | 3.7 | 1.5 | 102.3 / 461.8 | 156.6 / 497.0 | $1,831 ($2,581) |

(join/drain rows: `sweep_report.py`; they are the one-off handoff and
compaction-drain costs.)

Findings:

- **The cost model's per-shard extrapolation holds below 256 shards.** At
  345 commits/s the measured request bill is $1,203 / $1,358 / $1,589 / $2,267
  per month at 16 / 32 / 64 / 256 shards; the model says $1,357 / $1,463 /
  $1,655 / $2,459. **64 shards: within 4%** (Class A 103 vs 109/s, Class B
  223 vs 214/s). The model runs 8–11% high at 16/32/256 and never low. At
  900/s it is within 1–15% (high again). Going 256 → 64 shards cut the
  request bill 30% and 64 → 16 another 24%, as predicted.
- **Polling is exactly per-shard:** idle Class B 8.0 / 14.8 / 28.1 / 107.8
  per s = **0.40 GET/s per shard** at every count (analytic 2(1/10 + 2/30 +
  1/30) = 0.40).
- **No adaptive fast-poll episodes and no L0 stalls at any shard count**
  (`vlpds_compaction_poll_switches_total` never incremented;
  `slatedb_db_l0_stall_count_total` 0): L0 averages 2.7–3.5 SSTs per shard
  under load at every count, so the "~130 extra GETs/s" the cost model saw
  from fast-mode episodes (256 shards, 1 node) did not recur here.
- **Shard count changes neither latency nor CPU per commit**: p50/p99 63/138–144
  ms at 345/s and 66–68/145–150 ms at 900/s (the 30 ms injected PUT + 20–30 ms
  state latency dominate), 111–116 / 318–376 ms at 20k/s. CPU at 20k/s is
  291–306 µs per commit for the cluster (5.7–6.0 cores; 2/3 of writes are
  forwarded). At 345/s the cluster burns 0.39–0.47 cores, mostly the fixed
  per-node and per-shard background (idle: 0.09–0.13 cores).
- **At 20k/s the model is high on requests** ($1,088–1,831 measured vs
  $1,479–2,581): segment PUTs fall from ~74 to ~45/s (bigger, longer PUTs
  carry more per round trip) and polling/SST GETs grow less than the model's
  per-flush/per-load terms. The model is conservative at high load.
- The 20k/s step is near this sizing's limit: 79k–143k requests dropped at
  the loadgen's 5,000-in-flight cap during 1-s stalls (achieved 19.4–19.6k).
- Handoff (join) costs scale with shards: 256 shards ran 37 Class A + 296
  Class B per s for 180 s while 171 shards moved (≈ 39 Class A per moved
  shard, in line with the cost model's ≲ 60).

## Block 3: partial MSTs + NVMe disk cache, cold writes on big repos (`coldload*.jsonl`)

`bench.py coldload 10000:2000,100000:300,1000000:40 20,30,0.5 <inj_put>`:
one node (defaults: 64 shards, `--cache-dir` on NVMe, partial MSTs), bulk
2,000 repos × 10k records, 300 × 100k and 40 × 1M (90M records, 21.9 GB in
MinIO), restart, then **one createRecord per repo** (8 in flight, closed
loop) in three passes: **cold** (fresh process, SST disk cache wiped),
**disk-warm** (restart, disk cache kept from the cold pass), **mem-warm**
(no restart: repo-cache hits). `VLPDS_INJECT_STATE_MS=20,30,0.5` gives every
SlateDB/control-plane request S3-like latency (20 ms reads / 30 ms writes,
lognormal), so a disk-cache miss costs what it would on S3.

Clean run: `coldload-nopreload-inj0.jsonl` (`--preload-recent 0`, no
segment PUT injection, so the write latency is the load + commit). Write
latency ms (client) and repo load time (`vlpds_repo_load_seconds`):

| Repo size | cold p50 / p90 / p99 / max | load mean (hist p50 / p99 ≤) | SST GETs per write | disk-warm p50 / p99 | load mean | mem-warm p50 / p99 |
|---|---|---|---|---|---|---|
| 10k records (×2000) | 7.1 / 45 / 98 / 157 | 3.0 ms (1.6 / 51) | 0.42 | 8.1 / 15.8 | 2.1 ms | 7.5 / 14.1 |
| 100k (×300) | 13.9 / 47 / 99 / 120 | 17.1 ms (12.8 / 102) | 0.62 | 12.7 / 17.4 | 6.0 ms | 7.8 / 12.0 |
| 1M (×40) | **162 / 203 / 251** / 251 | **156 ms** (205 / 410) | **5.75** | 16.1 / 29.4 | 9.1 ms | 8.7 / 13.5 |

- **The NVMe disk cache takes the big-repo cold write from ~160–250 ms to
  16–29 ms** (1M records); with it, a cold write costs ~1–8 ms more than a
  cached repo at every size. Without it, a 1M-record repo's first write
  pays ~6 dependent S3 range GETs (at 20 ms each); 10k–100k-record repos
  pay < 1 SST GET per write on average (one prefetch scan reads the M/
  range: ~270 KB per 10k repo, ~1 MB per 100k/1M repo, the 1 MiB cap).
- The cold pass grows the disk cache 0.7 → 5.2 GB for 2,340 repos.
- **RSS stays ~3 GB with all 2,340 repos (incl. 40 × 1M) cached** after
  their writes: only the written paths are loaded. (After the bulk fill
  itself RSS was 9–19 GB: the import builds whole trees; they drop back on
  restart.)
- **Preload confounds a "warm" restart:** in the first run
  (`coldload.jsonl`, preload on, 25 ms segment PUTs) the restarted node had
  preloaded all 2,340 recently written repos (`--preload-recent`) before the
  pass started, so its "disk-warm" pass had 0 loads. That is the designed
  behaviour for restarts/handoffs; the table above disables it to measure a
  disk-cache-warm load. With 25 ms PUTs the cold-vs-warm gap is the same
  (1M: p50 218 vs 63–65 ms).
- State bytes per commit: see 1e (`bench_commit_cpu`).

## Block 4: cross-machine cluster, benchbox ↔ devhost (`xh3-inj25/`, `xh3g/`, `xh3f/`)

`bench/xhost` (driver on devhost), commit fcf29b3 built once on devhost
(Zen 2, `sync.sh -b devhost`; the same binary runs on benchbox). 3 nodes:
**n1, n2 on devhost** (EPYC 7302P, 16 vCPU; `--workers 4 --io-threads 8`
each), **n3 on benchbox** (`--workers 5 --io-threads 10`), MinIO + loadgens on
benchbox (10.0.0.51:9200 over 2.5 GbE, RTT 0.2–0.5 ms), 64 shards
(converged 22/21/21 in 1.5 s), 1M accounts (real/128), 200k active,
`--inject 25` (segment PUTs), lease TTL 10 s (driver default), one loadgen per node at rate/3 (2/3 forwarded), +200/s hot repo,
1 firehose subscriber per node.

Stairs (60 s measured after 15 s warmup):

| Offered | achieved | p50≤ / p99≤ ms | cores n1 / n2 / n3 | devhost busy | firehose completeness n1 / n2 / n3 (lag p99) |
|---|---|---|---|---|---|
| 10k | 10.2k, 0 err | 63 / 285 (rerun 63 / 291) | 4.2 / 3.8 / 2.1 | 9.4 of 16 | 1.0 / 1.0 / 1.0 (0.45–0.8 s) |
| 20k | 20.2k, 0 err | 107 / 383 (rerun 106 / 429) | 5.4 / 5.4 / 3.0 | 12.5 | **0.08 / 0.76 / 1.0** (rerun 0.10 / 0.27 / 1.0; lag 3.9–5.0 s) |
| 30k | 30.2k, 4 err | 284 / **2,939** (sat.) | 6.6 / 6.4 / 4.3 | 14.8 | 0.008 / 0.015 / 1.0 |

- **Cross-host ceiling ~20–30k commits/s, bounded by devhost** (two nodes on
  16 Zen 2 vCPU, 14.8 busy at 30k; n3 on benchbox used 4.3 cores). Not
  NIC-bound: ≤ 75 MB/s each way per host.
- **Firehose on the devhost nodes falls behind from 20k/s**: subscribers on
  n1/n2 received 8–76% of the commits within the step (lag p99 3.9–5 s),
  while n3 (benchbox) stayed at 1.0. In-order (0 out-of-order); it is the
  devhost nodes' merge/fan-out falling behind under CPU pressure, not loss —
  but a subscriber on a CPU-starved node lags seconds. Worth a look with
  `--profile-secs` (merge vs peer log stream).

### Failover: **survivors fail-stop after a kill -9 (port exhaustion) — critical**

| Run | Victim | Result |
|---|---|---|
| `xh3-inj25` kill9 n3 (benchbox) at 12k/s | n3 | survivors n1, n2 **both fail-stopped** ~17–37 s after the kill; the restarted n3 ended up owning all 64 shards (`[0, 0, 64]`); 223k errors, achieved 3.4k of 12k |
| `xh3-inj25` sigterm n3 | n3 | cluster already down to n3 only: 1.28M errors |
| `xh3-inj25` failover `--victim n1` (devhost) | n1 | same: n2 + n3 fail-stopped, `[64, 0, 0]` |
| `xh3f` kill9 n3 **with no population / no successful writes** | n3 | **takeover 4.68 s**, rejoin converged at once (22/22/20): the takeover path itself works |
| `xh3g` (repro, 1M population, stairs 10k/20k, kill9 n3 at 12k/s) | n3 | reproduced: `[0, 0, 64]`, 546k errors |

Cause (`xh3g/hosts.jsonl` socket counts per node, `xh3g/node-logs/`):
~6 s after the kill the survivors detect n3 ("peer missed a renewal and
refuses connections: presumed dead") and take its shards over while 12k
writes/s keep arriving. Their **open S3 sockets go from ~30 / ~55 to
21,500 (n1) and 8,255 (n2) within 10 s** — together the whole ephemeral
port range of devhost (32768–60999 = 28,231 ports). From then on every new
object-store connection fails (`transport error of kind Connect`), the
lease renewals fail with them, and both nodes fail-stop: `vlpds::cluster:
node lease lapsed before renewal: fail-stop` (n2 at +17 s, n1 at +37 s).
The restarted victim then takes every shard.

- The object-store client has **no bound on concurrent connections**: the
  takeover's shard opens + replay + a cold repo load for every write to the
  moved shards (each load its own S3 GETs) open a new connection per
  in-flight request. On one box over loopback this never showed (benchbox-only
  3-node runs hand back before it builds up, and loopback has the same
  port range but fewer simultaneous requests).
- Fix candidates (src/, not done here): cap object-store connections per
  pool (a semaphore on in-flight requests and a pool max-idle that keeps them
  reused), keep lease renewal on its own small reserved client so S3
  pressure cannot starve it, and back off cold loads/resends while a shard is
  opening.
- Host-side mitigation for deployment notes: `net.ipv4.ip_local_port_range`
  wider (1024–65535) only delays it.

Run-to-run: the 10k/20k stairs reproduced within 5% between `xh3-inj25` and
`xh3g`. The `report` step of `xhost.py` fails on these runs (`'str' object
has no attribute 'get'`); the tables above are from `steps.jsonl` and the
driver log. Harness fix made: `bench/xhost/sync.sh` now `chmod +x` binaries
copied host→laptop→host (`scp -3` dropped the exec bit).

## Block 5: 100M-account capacity run (`capacity-100m/`): **not completed, blocked by two bugs**

Plan (from `../capacity-2026-10-01-laptop/NOTES.md`): `bench/benchbox/capacity.sh
all --total 100000000 --nodes 4 --mode native --chunk 5000000 --settle-s 900
--active 500000 --rates 10000,25000,50000,75000,100000 --log-retention 3m`
(real/128 distribution: 530.6M records, mean 5.31, p99 70, max 4,640;
`loadgen dist` estimates 114 GB of live state; measured 2.2 KB/account
transient during the bulk ⇒ ~220 GB peak, inside the 250 GB cap / 255 GB
min-free). Five attempts, all inside one guard window; the population was
deleted afterwards.

| Attempt | Settings | What happened |
|---|---|---|
| 1 | defaults: lease TTL 10 s, bulk concurrency 16 | 5M accounts in 47 s (107k/s, 566k records/s); in chunk 2 **all three other nodes fail-stopped** ("node lease lapsed before segment PUT"). n4's lease renewals (a CAS on MinIO) took 5.7 s and then > 9 s while process CPU went flat (`metrics.jsonl`): the single-drive MinIO stalled under ~270 MB/s of fsynced bulk writes |
| 2 | resume, TTL 30 s, concurrency 8 | **after restarting on the prefix, 26 of 64 shards stayed unowned for > 10 min** (`[3, 16, 16, 3]`); the bulk silently skipped their accounts (~41% of 3 chunks: "created + existing != count"). Aborted, prefix deleted |
| 3 | fresh, TTL 30 s, concurrency 8 | 0 → 32M at 94k → 40k accounts/s; chunk 7 failed on a client timeout of bulkCreate to one node (nodes healthy) |
| 4 | resume, concurrency 4 | clean restart (converged 16×4 in 5.6 s); 32M → 60.8M at ~24–27k accounts/s; chunk 13 failed on bulkCreate timeouts, and at shutdown **n3, n4 fail-stopped**: "close failed: barrier not durable within 30 s" (control-plane LISTs were timing out at 5 s) |
| 5 | resume, concurrency 2 | **unowned shards again: `[0, 0, 16, 16]`, shards 32–63 unowned for 5 min.** Aborted, prefix deleted |

**Bug A — shards assigned to a dead incarnation of a live node id are
never taken over** (`capacity-100m/attempt{2,5}-unowned/`): after the
fail-stops, each restarted node fences its previous incarnation's log
("fenced dead node's log n3.1790960609058412") and takes a fair share of
*free* shards, but shards whose `assign/` record still names the old
incarnation (e.g. shard 33: `owner n3, log_id n3.1790960609058412`, the
log just fenced) are not reclaimed by the new n3 and not taken over by
anyone else (presumably because node id n3 is alive). Nodes n1/n2 own 0
shards in attempt 5. A clean shutdown + restart (attempt 4) converged fine;
it takes a fail-stop before the restart. Writes to those shards fail;
`bulkCreate` reports them as neither created nor existing.

**Bug B / environment — the object store stalls long enough to lapse
leases during bulk import on benchbox's single MinIO**: a 10 s TTL lapsed in
attempt 1 (renewal CAS stuck > 9 s); with 30 s TTL the nodes survived the
bulk but bulkCreate requests timed out client-side and shard closes timed
out at shutdown (barrier not durable in 30 s ⇒ fail-stop). This is mostly
benchbox's MinIO (one NVMe, fsync per PUT) being far slower than S3 under
~250 MB/s of writes, but two vlpds-side issues show: the lease renewal
shares the stalled state-pool client with the bulk's SST traffic, and a
slow store at shutdown turns a graceful stop into a fail-stop (which then
triggers bug A on restart).

Measured on the way (TODO "bulk import peak SST bytes with the new GC
settings"): with `--slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m`
the prefix grew ~2.1–2.3 KB per account during the bulk (`s3 105 GB` at 50M,
139 GB at 60M including compaction garbage and the 3-min log), vs the
~0.7 KB/account settled at 5M on the laptop: ~3× live while importing.
Bulk rate on benchbox: 94–107k accounts/s at the start, falling to 24–27k/s
by 40–60M accounts as compaction debt and MinIO latency grow.

Next attempt needs: bug A fixed (and ideally the shutdown fail-stop), then a
paced bulk (`--bulk-concurrency 2–4`, ~25k accounts/s ⇒ ~70 min for 100M)
across two guard windows, or MinIO with more than one drive.

## Regressions and bugs found (for TODO)

1. **Write-path CPU +35–50% per commit, single-node ceiling −27–31%**
   (1a, 1f): partial MSTs re-walk every loaded node of a repo per commit to
   charge the cache (`worker::repo_bytes` → `mst_lazy::heap_bytes`, 12% of
   CPU at 50k/s), plus hedged signing + verify-after-sign (+20 µs/commit).
   Also RSS at 50k/s 4.8 → 9–11 GB with partial MSTs on.
2. **getRepo export of big repos 1.5–2.8× slower; getBlocks10 at 10M
   records −33%** (1b).
3. **Survivors fail-stop after a peer's kill -9 under write load** (block 4):
   unbounded object-store connections during takeover exhaust ephemeral
   ports (21.5k + 8.3k S3 sockets on one host); lease renewal starves.
4. **Shards stuck unowned after fail-stop + restart** (block 5, bug A).
5. **Graceful shutdown fail-stops when the store is slow** ("barrier not
   durable within 30 s") and lease renewal shares the bulk's stalled client
   (block 5, bug B).
6. Firehose subscribers on CPU-starved nodes lag 4–5 s and see 8–76% of
   commits within a 60 s step at 20k/s (block 4).
7. Harness: `xhost.py report` crashes (`'str' object has no attribute
   'get'`); `bench/xhost/sync.sh` dropped exec bits on `scp -3` copies (fixed).

Confirmed (TODO items that can be closed): getRecord ≥ 63k at 10M (69.6k);
createAccount vs 396/792 (832/s); proxy PROXY_CONNECTIONS 16/64/256 on
Linux (16 ≈ 64 > 256); K=4 low-load latency not reproducible (10k/s:
K=4 66/125 ms, same as fa0975c 64/142); runtime stalls after the 10 s
checkpoint: not seen in any grid/sweep step here (the 20k/s shard-sweep
drops were loadgen-cap stalls, not investigated further).

## Files

- Drivers/scripts: `bench.py` (copy of `../2026-10-02/bench.py` + changes
  above), `tables.py` (copy), `shardsweep.py` (copy of the cost model's
  `measure.py` + `sweep`), `sweep_report.py`; `bench/benchbox/drive.sh`,
  `attach.sh`, `build-at.sh`, `commit-cpu.sh` (new);
  `bench/xhost/sync.sh` (exec-bit fix).
- Block 1: `grid-1m-inj25.jsonl`, `grid-1m-inj0.jsonl`, `grid-10k-inj25.jsonl`,
  `sweep.jsonl`, `methods.jsonl`, `proxy-c{16,64,256}.jsonl`,
  `ab-grid-10k-inj25.jsonl`, `ab-methods.jsonl`, `prof-grid-10k-inj25.jsonl`,
  `commit-cpu.jsonl`.
- Block 2: `shardsweep.jsonl`, `shardsweep-smoke.jsonl`, `shardsweep-summary.json`.
- Block 3: `coldload.jsonl` (preload on, inj25), `coldload-nopreload-inj0.jsonl`.
- Block 4: `xh3-inj25/`, `xh3g/` (+ `node-logs/` excerpts), `xh3f/`.
- Block 5: `capacity-100m/` (`populate.jsonl`, `metrics.jsonl.gz`, driver
  `RESULTS.md` of the last attempt, `attempt{2,5}-unowned/` cluster tables,
  assign records and log excerpts).
- `run-*.log`: one per invocation (local time tags, UTC−7).

## State of benchbox / devhost at the end

- benchbox: MinIO container removed, MinIO data, `.trash` and abandoned
  multipart uploads deleted, scratch caches empty; no vlpds/loadgen/MinIO
  processes; `~/vlpds-bench` 7.5 GB (`target/` and `target-prof/` = fcf29b3,
  `xhost/` binaries, `src/`, results). / has 560 GB free (562 at the start).
  The A/B build dirs (`target-<sha>`, `target-ab`, `target-test-*`,
  `src-*`) were deleted. A stale `tail -F` of an old 2026-10-02-benchbox log
  (from an earlier session, started 2026-10-01 16:52) was killed.
- devhost: xhost populations, state and the xhost MinIO deleted; no nodes
  running; `~/vlpds-bench/xhost/target` = fcf29b3.
