# vlpds on benchbox, 2026-10-02 round 2: head 8be1e84 vs fcf29b3 / fa0975c

Commit under test: **8be1e84** (vlpds head: e453b75 write-path CPU pass with
incremental repo recharge, SlateDB fork fc2aae0 + 93691e3 batched scans,
e7bcf55 cheaper export, 1e0c440/8be1e84 bounded object-store requests + own
control-plane client, b56b5f5/48a4e18 stranded-shard reclaim, plus the
security/proxy/runtime fixes). Previous round: `../benchbox-2026-10-02-head/RESULTS.md`
(fcf29b3). Same box, MinIO flags and drivers. While the campaign ran, the
local `vlpds` branch moved on to 5a62601 (Argon2 shedding, HeapMemo test,
docs, SlateDB fork rev 68106cc, dashboards, peer mTLS, compat removals); none
of it is a lazy-MST / heap_bytes / byte-accounting src change, so no second
head was tested. Drivers shipped from the working tree were pinned where it
mattered: the capacity driver is `git show 8be1e84:…/bench/capacity/run.py`
(the newer one passes `--peer-tls-dir`, which 8be1e84 rejects).

Drivers: `bench.py` = the previous round's copy plus `restart` (block 2) and
`vlpds_object_store_permit_waits_total` in step deltas. Cross-host runs use
`bench/xhost` unchanged, plus a 1 s sampler on devhost (`objsample.py` here,
output `xh*/xh*-*.jsonl`) that keeps the labels of
`vlpds_object_store_{inflight,inflight_limit,permit_waits_total,requests_total,bytes_total}`,
`vlpds_owned_partitions` and lease metrics (the xhost scraper sums families
and drops labels). Every benchbox run went through `drive.sh`/`runner.sh` under
`guard.sh` (`GUARD_MIN=25–40` for the short runs, abort at 10 min). The
first window's benchbox runs (restart, the first sweeps, the grids) ran
against the xhost MinIO container, which also serves 127.0.0.1:9200 (same
tuned flags and disk; see finding 9); the second window's used the bench
MinIO.

## Summary

| Area | Result |
|---|---|
| Cross-host kill -9 / SIGTERM (block 1) | **Fixed: survivors no longer fail-stop.** Peak S3 sockets 1.1–1.5k per node (was 21.5k + 8.3k), takeover 12.4 s (kill -9 n3) / 5.4 s (kill -9 n1, devhost) / 1.5–2.3 s (SIGTERM), 0 permit waits on the log and ctl lanes. fcf29b3 fail-stops even on a SIGTERM (`xh5-fcf`). **New:** each shard move still costs ~30–45 s of 503s at 12k/s (76–140k errors): cold repo loads on the new owner need 10–65 SST GETs each and saturate the 1,024 state permits; `--store-inflight 8192` doesn't help (same errors, 12k sockets) |
| Fast same-id restart (block 2) | **Fixed: all 64 shards owned in every scenario** (100k / 1M accounts): +9.4–9.7 s (one node), +9.5 s (two), +13.8–13.9 s (all four), +5.2–7.3 s after a 15 s outage. The ~9 s is the join gate waiting for the dead incarnation's watermark cap (≈ TTL), not stranding |
| getRepo export (block 3) | 1M **fixed** (0.79–0.84 s vs 0.86 s fa0975c). 10M **still 2.1–2.6× slower** (17–22 s vs 8.3 s); confirmed partial-MST cause by A/B on a710e0c (8.2 s `--lazy-mst false` vs 31 s on); profile: one synchronous point get per interior MST node (31% of CPU) |
| getBlocks10 at 10M (block 3) | **No regression**: head 2.0–3.5k/s (mean 2.8k, 4 runs) vs fa0975c 2.4–2.9k (mean 2.55k, 3 runs); ±25% run-to-run |
| Single-node writes (block 4) | 10k/5k inj25 ceiling ~58k (fcf29b3 57.6k, fa0975c 74.5k same day); CPU/commit at 50k/s 226 µs (fcf29b3 248, fa0975c 141); p99 at 50k/s 527 → 197 ms; `heap_bytes` gone from the profile, sign + verify 24% of CPU; RSS 9.4 GB vs 4.5 GB |
| HA `reshard-kill9`, `retention-kill9` (block 5) | **Both PASS**, 0 acked writes lost, outages 3.3 s / 4.5 s, dead log retired to its fence |
| 100M capacity (block 6) | **Population completed for the first time** (100M accounts, 520M records, 103 GB; restarts converged 16×4 in 5.6 s), but bulk fell 78k → 3k accounts/s past ~75M and **the first 10k/s stair fail-stopped 3 of 4 nodes** (lease lapsed 35–48 s past a 30 s TTL). **New:** filter-cache misses re-download and zstd-decode whole SST bloom filters (`read_filters` 26% of CPU, 1–2.4 GB/s of SST GETs per node) |

## Block 1: cross-host failover recheck (`xh4/`)

Same topology and flags as `xh3g`: n1, n2 on devhost (4 workers / 8 io each),
n3 on benchbox (5 / 10), MinIO + loadgens on benchbox, 64 shards, 1M accounts
(real/128), 200k active, `--inject 25`, lease TTL 10 s, binary 8be1e84 built
on devhost (Zen 2) and copied to benchbox. Stairs 10k/20k, then kill -9 /
SIGTERM of the victim at 12k/s (signal at +35 s of a 105 s step, restart
20 s later; load enters through the survivors only).

Stairs reproduce xh3g within noise: 10k/s 10.2k achieved, p50/p99 63/292 ms
(xh3g 63/291); 20k/s 20.2k, 100/353 ms (106/429); peak S3 sockets per node
25–44 at 10k, 41–273 at 20k. Firehose completeness on the devhost nodes at
20k/s is still 0.07–0.10 (n3 1.0): the CPU-starved-subscriber lag of the
previous round is unchanged.

| Run | Survivors | Takeover | Errors (after signal) | Clean again at | Peak S3 sockets n1 / n2 / n3 | State pool saturated (≥1000 of 1024 in flight) | `permit_waits` state lane n1 / n2 / n3 |
|---|---|---|---|---|---|---|---|
| xh3g kill9 n3 (fcf29b3) | **both fail-stopped** | never | 546k | never | 21,500 / 8,255 / – | (no bound) | – |
| **kill9 n3** | **both up** | **12.4 s** | 129k over 43 s | +49 s | **1,315 / 1,173 / 1,439** | 16 / 14 / 11 s | 251k / 121k / 262k |
| **sigterm n3** | up | 1.5 s (exit 1.5 s) | 134k over 37 s | +44 s | 1,475 / 1,391 / 1,366 | 4 / 2 / 8 s | 55k / 62k / 23k |
| **kill9 n1 (devhost victim)** | **both up** | **5.4 s** | 109k after the kill (+168k before it, see below) | +42 s | 1,347 / 1,236 / 1,257 | 1 / 11 / 6 s | 93k / 180k / 104k |
| **sigterm n1** | up | 2.3 s | 140k over 43 s | +57 s | 1,359 / 1,149 / 1,130 | 4 / 7 / 2 s | 210k / 141k / 68k |

(`errors` are loadgen errors from the signal on; achieved 8.9–10.6k of 12k.
Takeover = victim's exit → survivors own all its shards. Peak sockets from
`hosts.jsonl` (2 s probe), inflight/waits from the 1 s sampler; the log and
ctl clients never exceeded 12 in flight, the ctl `reserved` lane (lease PUTs)
1, with **0 permit waits on log and ctl** in every run.)

- **Fixed: the survivors no longer fail-stop.** Peak S3 sockets per node
  ~1.1–1.5k (the 1,024 state permits + log/ctl), vs 21.5k + 8.3k at
  fcf29b3; no `Connect` errors, no lease lapses, rejoin converges in
  3.7–11.7 s. The ctl client's reserved lease lane never waited.
- **But a shard move under load still costs ~40 s of errors at 12k/s**, for
  SIGTERM as much as kill -9, and a freshly started cluster pays the same at
  its first writes (`kill9-n1`: 168k errors in the 35 s before the kill, the
  cluster had just been restarted by the driver). Errors are 503s from
  forwards (`forward: owner did not answer in time` ~6–12k per 20 s per
  node, `repo load failed: partition not owned by this node`, and `loading
  session revocations/takedowns failed: ... ShardMoved`, see below).
  Mechanism (`xh4-all.jsonl`, `metrics.jsonl`):
  - On the new owner, repo loads of the moved shards cost **10–65 SST range
    GETs per load** (8–9k GETs/s per node for 15 s) vs **~1 GET/load** in
    steady state (block cache warm). The state pool sits at its 1,024
    permits for 11–16 s (kill9) / 2–8 s (sigterm), 50–250k requests queue for
    a permit, commits on the survivors fall from ~4k/s to 0.3–1k/s each, and
    forwarded writes hit the 3 s forward deadline.
  - The 20 s restart then moves shards back (rejoin) and the whole storm
    repeats on the rejoining node (`kill9-n3`: n3's preload of 40,960 repos +
    request loads at +36..+50 s), so each failover is two storms. Errors at
    12k/s: kill9-n3 1–4k/s from +7 s to +48 s; sigterm-n3 ~1k/s right after
    the stop, then 2–16k/s after the rejoin.
  - Preloads are not the GET source: 32 at a time, they finish late
    (`vlpds_repo_preloads_total` reaches 20–40k only 30–50 s after the move).
  - `loading session revocations/takedowns failed` is a new per-request
    lookup on the account's partition (ShardMoved while the shard moves):
    ~6–13k warnings per 20 s per node during the move. It **fails the
    request closed** (`xrpc/server.rs` `stale_or_unavailable`: 503 unless a
    cached view younger than `STALE_MAX_SECS` exists), so during a move the
    entry node 503s writes of any account whose security controls it hasn't
    cached, on top of the write's own forward/load errors.

### SIGTERM A/B: head vs `--store-inflight 8192` vs fcf29b3 (`xh5-*/`)

Same topology, each a fresh 1M population (deleted afterwards), stair 10k/s
(warm-up), then SIGTERM of n3 at 12k/s with restart 20 s later. fcf29b3 is
the round-1 binary built on devhost (`target-fcf29b3`).

| Run | Survivors | Takeover | Errors after the signal | Peak S3 sockets n1 / n2 / n3 | Peak state in flight | `permit_waits` (state) n1 / n2 / n3 |
|---|---|---|---|---|---|---|
| **xh5-head** (8be1e84) | up | 1.7 s | **81k** over 32 s; achieved 10.9k of 12k | 1,368 / 1,354 / 1,473 | 1,024 (cap) | 117k / 95k / 184k |
| **xh5-inf8k** (`NODE_EXTRA=--store-inflight 8192`) | up | 3.1 s | **76k** over 32 s; 10.5k | **12,529 / 7,731 / 12,157** | 5,316–5,908 | 16k / 0 / 124k |
| **xh5-fcf** (fcf29b3) | **both fail-stopped** (n1 unreachable +4.6 s, n2 +19.6 s) | never | **476k** over 70 s; 3.3k; `[0, 0, 64]` after | 14,184 / 5,586 / 102 | (no bound) | – |

- **fcf29b3 loses the cluster even on a graceful SIGTERM** under this load
  (the port-exhaustion fail-stop is not kill -9 specific); head survives it.
- **The permit bound is not what stretches the handoff**: with 8,192 state
  permits the nodes put 5–6k requests in flight and open 7.7–12.5k S3
  sockets (most of the way back to the port-exhaustion zone) for the same
  ~32 s / 76–81k errors. The storm is bound by the cold block cache + MinIO
  throughput, so 1,024 is the better default; raising it only buys sockets.
- Same run, head, fresh (xh5-head) vs after a kill -9 cycle (xh4
  sigterm-n3): 81k vs 134k errors.

## Block 2: fast same-id restart (`restart.jsonl`, `restart-c4-n*.server.log`)

`bench.py restart 100000 3000 <scenarios>`: 4 native nodes on benchbox (ids
n1..n4, 64 shards, lease TTL 10 s, 3 workers / 3 io each), 100k bulk
accounts, 3,000 writes/s through n1 (n2 when n1 is a victim; ¾ forwarded).
Each scenario kill -9s its victims 20 s into an 80 s step, respawns them
with the same `--node-id` after `down` s (all victims in parallel) and polls
`vlpds_owned_partitions` + every routing table every 0.25 s.

| Scenario | Victims | Down | Serving | **All 64 owned by live processes** | Balanced | Errors (3k/s) |
|---|---|---|---|---|---|---|
| one-fast | n4 | 0.5 s | 0.26 s | **+9.7 s** | +9.7 s (16×4) | 5.5k |
| two-fast | n3, n4 | 0.5 s | 0.6 s | **+9.5 s** | +9.5 s | 10.8k |
| all-fast | all 4 | 0.5 s | 0.7 s | **+13.9 s** | +13.9 s | 30.0k |
| all-slow (round 1 block 5's shape: every lease lapsed) | all 4 | 15 s | 0.7 s | **+5.2 s** | +5.2 s | 49.7k (incl. the 15 s down) |
| two-slow (control) | n3, n4 | 15 s | 0.3 s | +0.3 s (survivors held them) | +1.6 s | 2.6k |

Same driver at **1M accounts and 10,000 writes/s** (`restart-1m.jsonl`):

| Scenario | All 64 owned | Errors (5 s windows after the kill) | Clean again |
|---|---|---|---|
| one-fast (n4) | +9.4 s | 4.5k · 8.0k · 0.2k (12.7k) | +15 s |
| all-fast | +13.8 s | 22.7k · 24.5k · 17.9k · 0.8k (65.9k) | +20 s |
| all-slow | +7.3 s (after 15 s down) | 50k × 3 (down) · 10.9k · 4.8k | +10 s after restart |

On one box (loopback MinIO, warm page cache) the restarted shards serve at
full rate as soon as they are owned: no cold-load storm like the
cross-host runs (block 1).

- **Bug A (round 1 block 5, "shards stuck unowned after fail-stop + restart") is
  fixed**: in every scenario all 64 shards were owned by a live process
  within 14 s and the layout came back to 16×4; nothing stayed with a dead
  incarnation. The block-5 shape (all nodes down past their leases, then
  restarted) converged in 5.2 s.
- **A fast same-id restart takes ~TTL, not "seconds"**: the respawned node
  fences its old log at once ("fenced dead node's log"), then waits ~8.5–8.9 s
  in `not joining yet: our clock is behind a peer's merged firehose floor`
  (cluster.rs `try_join` → `wait_seq_past`): the floor includes the dead
  incarnation's `wm_cap`, i.e. roughly its lease expiry, so the new
  incarnation may not sequence until the old lease would have run out. Its
  shards (still assigned to the old incarnation) are unavailable meanwhile;
  with all four restarting together it is 13.9 s. This looks deliberate
  (no seq below what the dead incarnation could have issued); a same-id
  restart that has fenced its predecessor's log could cap at that log's
  last seq instead of the lease-time `wm_cap`, if that is safe.

## Block 3: read regressions at 1M / 10M records (`sweep-*.jsonl`, `rep-sweep-*.jsonl`)

`bench.py sweep` (one repo per size, concurrency 16, 10 s per method), head,
fa0975c (`build-at.sh fa0975c`) and a710e0c (the commit that turned partial
MSTs on, with and without `--lazy-mst false`) alternated in the same windows:
`sweep-*` = 1M + 10M once each (first window), `prof-sweep-8be1e84` = a head
run on the profiling build pushing to Pyroscope, `rep-sweep-*` = 10M repeats
(second window: head, fa0975c, a710e0c lazy, a710e0c full, head, fa0975c).

**getRepo export** (2.77 GB at 10M, 276 MB at 1M; two exports per run):

| Binary | 1M | 10M (each run: export 1 / export 2) |
|---|---|---|
| **8be1e84 (head)** | **0.79–0.84 s** | 21.6 / 20.1 · 18.4 / 17.0 (prof) · 19.2 / 18.1 · 20.8 / 19.4 s → **17–22 s, ~140 MB/s** |
| fa0975c | 0.86 s | 8.28 / 8.28 · 8.30 / 8.33 · 8.27 / 8.31 s → **8.3 s, 334 MB/s** |
| **a710e0c `--lazy-mst false`** (full trees) | – | **8.20 / 8.20 s** |
| **a710e0c (partial MSTs on)** | – | **31.6 / 31.0 s** (88 MB/s) |
| fcf29b3 (round 1) | 1.30 s | 21.8–23.5 s |

**getBlocks10 at 10M** (ops/s; 10 random record CIDs of 200 sampled per call):

| Binary | runs | mean |
|---|---|---|
| 8be1e84 | 2,250 · 3,454 · 3,426 · 2,027 | **2,789** |
| fa0975c | 2,881 · 2,372 · 2,402 | **2,552** |
| a710e0c lazy / full | 2,047 / 1,785 | – |
| (round 1: fcf29b3 2.4k, fa0975c 3.6k) | | |

Other 10M reads, head vs fa0975c (first window; repeats agree): getRecord
65k vs 35k, listRecords 22k vs 16k, describeRepo 15k vs 13k, listBlobs 12k vs
11k, sync.getRecord 67k vs 61k; fill 92–110k vs 65–74k records/s. At 1M:
getBlocks10 11.5k vs 4.7k, getRecord 79k vs 51k.

- **getRepo export at 1M: fixed** (1.30 s at fcf29b3 → 0.79–0.84 s, at or
  better than fa0975c's 0.86 s).
- **getRepo export at 10M: still regressed, 2.1–2.6× fa0975c** (17–22 s vs
  8.3 s), and **the cause is confirmed by A/B on one binary: a710e0c exports
  in 8.2 s with `--lazy-mst false` and 31 s with partial MSTs on**. e7bcf55 +
  93691e3 (+ fork rev fc2aae0) and the rest of a710e0c..head took it from 31 s to ~19 s; the remaining 2.3×
  is the partial-MST walk. No further bisect needed.
  Profile (Pyroscope `service_name=vlpds`, 2026-10-02T18:27:53–18:28:27Z,
  BIN_DIR=target-prof): the export runs on ~0.8 core; **`mst_lazy::export_blocks`
  → `visit` → `export_subtree` → `src.node(cid)` does one synchronous
  SlateDB point `get` (`block_on`) per persisted interior node: 31% of CPU**
  (`Reader::get_key_value…` 8.3 of 26.3 s), most of it per-get iterator setup
  over every L0 SST and sorted run (`GetIterator::with_lookahead` /
  `init_source` 4.9 s, bloom/index lookups); the per-leaf `R/` range scans
  (`BatchedScan`, `scan_records`) are another ~30%. fa0975c streamed the
  already-loaded full tree and paid only the R/ scan. The 10M export costs
  2.5× per byte what the 1M one does: more SSTs per get, and far more
  interior nodes than the 1 MiB M/ read-ahead covers. Fix candidates: for an
  export (which visits every node) scan the repo's whole `M/` range
  sequentially instead of point gets, and/or walk subtrees concurrently.
- **getBlocks10 at 10M: no regression.** Run-to-run spread is ±25% for both
  binaries (head 2.0–3.5k, mean 2.8k; fa0975c 2.4–2.9k, mean 2.55k); round
  1's 3.6k vs 2.4k was one draw from each. The calls are 10 record lookups
  through the `c/` index (two SlateDB gets each) behind `assert_available`
  (one account get): block-cache / disk-cache bound, with a 10M repo larger
  than the block cache. Partial MSTs don't matter here (a710e0c lazy 2.0k vs
  full 1.8k). At 1M head is 2.4× fa0975c.
- Everything else at 10M is at or above fa0975c (getRecord 1.9×).

## Block 4: single-node write grid (confirmation only) (`grid-*.jsonl`, `ab-grid-10k-inj25.jsonl`, `prof-grid-10k-inj25.jsonl`)

Block 1a shapes, fresh prefix per row, K=4, 64 shards, 10 s warmup + 20 s,
200/s hot repo + firehose consumer; fa0975c rerun on 10k/5k inj25 in the same
window. Cells p50 / p99 ms; CPU = process CPU ÷ commits; build = mean
`vlpds_commit_build_seconds`; RSS max in the step.

| Shape | 25k/s | 50k/s | 75k/s | Ceiling | CPU µs/commit 25k / 50k / sat | build µs | RSS GB at 50k |
|---|---|---|---|---|---|---|---|
| **10k/5k inj25 head** | 62 / 125 | 104 / 197 | sat 58.0k | **~58k** | 198 / 226 / 253 | 59–70 | 9.4 |
| 10k/5k inj25 fcf29b3 (round 1) | 65 / 124 | 102 / 527 | sat 57.6k | 57.6k | 203 / 248 / – | 65 | 9.4 |
| 10k/5k inj25 **fa0975c today** | 66 / 125 | 66 / 137 | 74.3k (25k dropped), 100k: sat 74.7k | **~74.5k** | 156 / 141 / 147–163 | 34–39 | 4.5 |
| **1M/50k inj25 head** | 92 / 355 | 49.9k, 107 / 419 | sat 67.7k | **~68k** | 233 / 220 / 233 | 56–68 | 14.5 |
| 1M/50k inj25 fcf29b3 / fa0975c (round 1) | 67/131 · 86/220 | 49.6k 100/542 · 86/181 | 64.3k · 75.2k | 64k · 88k | 234/218/242–252 · 152/164/139 | – | – |
| **1M/50k inj0 head** | 37 / 94 | 50.2k, 55 / 359 | sat 72.0k | **~72k** | 235 / 218 / 239 | 56–69 | 14.3 |
| 1M/50k inj0 fcf29b3 / fa0975c (round 1) | 42/106 · 40/74 | 48.9k 66/600 · 43/78 | 67.8k · 72.8k | 68k · 98k | 233/217/241–248 · 160/147/162 | – | – |

- **Ceiling up 0–6% vs fcf29b3, still 20–27% below fa0975c** (58k vs 74.5k
  same day on 10k/5k; 68k / 72k vs 88k / 98k on 1M). CPU per commit at 50k/s
  226 vs 248 µs (10k/5k), unchanged on 1M (218–220). The p99 at 50k/s
  improved (527 → 197 ms on 10k/5k; 542 → 419 / 600 → 359 on 1M).
- **The incremental recharge worked**: `mst_lazy::heap_bytes` is gone from the
  profile (round 1: 11.6% flat), lazy-MST code is now ~4% (`loaded_path`
  2.2%, `LazyTree::walk` 1.7%, `locate` 1.4%). Profile at 50k/s
  (2026-10-02T18:40:48–18:41:06Z): **`worker::sign_commit` 24% of CPU, of
  which `ecdsa_verify` (verify-after-sign) 15% and signing 8%**; SlateDB
  memtable/flush ~16%; forwarding/HTTP middleware ~15%; zstd 3.5%. The
  remaining ~70–85 µs/commit gap to fa0975c is mostly the hedged sign +
  verify (~50 µs by the profile's share) plus the partial-MST loads.
- **RSS at 50k/s is still 2× fa0975c** (9.4 vs 4.5 GB on 10k/5k; 14–16.5 GB
  on 1M right after the bulk). No commit mentioning lazy MST / heap_bytes /
  byte accounting with a src change landed during the run (98557e5 is
  docs: "block cache fill explains the partial-MST RSS growth; defaults
  kept").


## Block 5: HA scenarios `reshard-kill9` and `retention-kill9` (`ha/`)

`bench/ha/hactl.py run reshard-kill9 retention-kill9` on benchbox through
`drive.sh` (guarded), head release binaries (`VLPDS_BIN_DIR=~/vlpds-bench/target/release`),
the Go tools (`checker`, `faultproxy`, `fhaudit`) cross-compiled on the laptop
for linux/amd64, harness defaults (3 nodes, lease TTL 3 s, 150 writes/s per
node, 32 probes, `--inject-put-ms 25`, `--log-retention 45s` for retention),
`VLPDS_HA_CLEANUP=0` (no `vlpds-minio:local` image on benchbox; prefixes deleted
by hand). `ha/summary.md` is the harness table; the raw firehose audit dumps
and acked-write lists were dropped (size), logs and `result.json` kept.

| Scenario | Verdict | Acked / lost | Checker | Firehose | Probe outage | Final layout | Notes |
|---|---|---|---|---|---|---|---|
| reshard-kill9 | **PASS** | 43,870 / 0 | PASS (43,874 commits, 0 fails) | live n1/n3 0 missing; replay n1/n2(rejoined)/n3 0 missing; histories agree | 12.0–15.3 s (3.3 s, 340 probe failures) | 22/21/22, layout v4 | split of shard 23 with n2 killed as it froze the parent; survivors finished it; merge 11+12 and split 0 done |
| retention-kill9 | **PASS** | 175,314 / 0 | PASS (175,317 commits) | live n1/n3 0 missing, live histories agree | 75.0–79.5 s (4.5 s) | 22/20/22 | dead log retired to its fence (object 2225 only); deleted own 3,820 / 408 / 3,741, dead 1,847; 0 retention errors; old-cursor subscribers OK |

Both match the fa0975c baselines in `bench/ha/RESULTS.md` (ret1/ret2:
75.0–79.5/79.8 s outage, ~4,000 own objects deleted per node, fence-only
dead log).

## Block 6: 100M capacity retry

`bench/benchbox/capacity.sh all --name cap100m-r2 --total 100000000 --nodes 4
--mode native --chunk 5000000 --settle-s 900 --active 500000 --rates
10000,25000,50000,75000,100000 --log-retention 3m --lease-ttl-ms 30000
--bulk-concurrency 4`: round 1's plan, paced (bulk concurrency 4 instead of
16, lease TTL 30 s). Results in `capacity-100m/` (driver `RESULTS.md`,
`populate.jsonl`, `metrics.jsonl.gz`, `run-*.log`).

**The 100M population completed for the first time** (round 1 never got past
60.8M): 100,000,000 accounts / 530.6M records in 2 h 21 min of bulk over
three node sessions. The run was stopped twice on purpose (the A/B and the
profile below); both graceful stops were clean, and every restart converged
16×4 in 5.6 s: no unowned shards, no fail-stops, no lease lapses (TTL 30 s).
MinIO held 75–120 GB during the bulk (GC kept it bounded) and 103 GB at
100M (~1 KB per account).

| Accounts | Bulk rate (accounts/s) | Notes |
|---|---|---|
| 0–5M | 78k | |
| 5–20M | 50k → 36k | |
| 20–60M | 31k → 24k | |
| 60–75M | 21k → 17k | |
| 75–80M | **9.2k** | |
| 80–85M | **6.4k** | MinIO's NVMe at 99% util reading ~1 GB/s; `cluster step failed: control-plane list timed out` ~190× per node, retention passes failing on LIST timeouts |
| 85–86M | 5.7k | restarted with `--lazy-mst-prefetch-kb 0` (A/B) |
| 86–100M | 5.5k → 3.0k | restarted on the profiling build |

**Why the bulk collapses past ~75M (new finding): SST bloom filters stop
fitting in the cache, and every point get that misses re-downloads and
re-decompresses a whole filter.** At 80–85M, n1 alone pulled **2.36 GB/s of
`state_sst` range GETs (2,927/s, ~800 KB each) for 10 MB/s of SST writes**;
MinIO sent 4.36 GB/s to the four nodes (6,750 GETs/s) while receiving 62 MB/s.
Profile (Pyroscope, profiling build, 2026-10-02T22:11:30–22:13:30Z, bulk at
86M): **`slatedb::Db::get` is 46% of all CPU, and `TableStore::read_filters`
→ `SsTableFormat::read_filters` is 26% (an object-store `read_range` plus a
zstd decode of the filter, 16%)**. The gets come from `admin::bulk_create`
(24%: existing-account checks: almost all answered negative by a filter) and the
worker's `handle_control` (28%). Compaction is 20%. Cache counters agree:
filter misses ~760/s per node at a 97.6% hit rate, each a multi-hundred-KB
fetch. Compacted SSTs are 256 MB (6 per shard, ~1.5 GB per shard), so each
filter is large, and 16 shards × ~1.5 GB per node outgrow the 3.6 GB block
cache. Disabling the lazy-MST prefetch (its 1 MiB scan read-ahead was the
first suspect) cut SST reads only to 1.04 GB/s per node (0.75 MB per created
account) and did not speed the bulk up (5.7k/s). On S3 this is also a GET
and egress bill. Candidates: pin filters and indexes (a never-evicted
metadata cache sized from the SST count), smaller compacted SSTs or
partitioned filters, a larger block cache per node; and avoid the negative
existence get per bulk-created account.

**Stairs at 100M: the cluster fell over at the first stair (10k/s).**
`stair-10000` (500k active window, 4 loadgens) achieved 0 commits/s: the
state pools went straight to 1,024 in flight with 100–225k permit waits per
node within 25 s, the nodes stopped answering scrapes, and **n1, n2, n3
fail-stopped with `node lease lapsed past takeover: fail-stop` (lapsed 35–48 s
past a 30 s TTL)** ~85 s in; the kill-9 phase that followed found only n4
(`[0, 0, 0, 64]`, 0 achieved). Lease renewals stopped completing (renew count
669 → 673 then nothing) while MinIO was saturated by cold loads that each
re-fetch filters (above). The ctl client's reserved lease lane bounds
permits on the client, not the queue inside a saturated MinIO, so round 1's
"lease renewal starves behind a stalled store" (bug B) still happens when the
store itself stalls. The node logs were deleted with the population (the
capacity driver's `cleanup` removes its scratch dir); the counts above are
from them before cleanup and from `capacity-100m/metrics-10s.jsonl.gz`
(downsampled to 10 s from the 1 s scrape), `steps.jsonl` and the driver's
`RESULTS.md`. The population was deleted afterwards.

## New bugs / findings (for TODO)

1. **Fixed (confirm and close):** survivors fail-stopping after a peer's
   kill -9 (round 1 bug 3): S3 sockets now peak at ~1.1–1.5k per node, no
   lease lapses, on kill -9 and SIGTERM, benchbox and devhost victims. fcf29b3
   lost the cluster even on a SIGTERM (`xh5-fcf`).
2. **Fixed (confirm and close):** shards stuck unowned after fail-stop +
   same-id restart (round 1 bug 4 / block 5 bug A): 64/64 owned within
   5–14 s in every scenario, 100k and 1M accounts.
3. **New: a shard move under cross-host load costs ~30–45 s of 503s** (block
   1): new owners need 10–65 SST range GETs per repo load (vs ~1 warm),
   saturate the state pool for 5–16 s, forwards hit the 3 s deadline;
   81–140k errors at 12k/s per SIGTERM or kill -9, and a rejoin repeats it.
   Not the permit bound (`--store-inflight 8192`: same errors, 12k sockets).
   Candidates: warm the new owner's block cache from the old owner's hot SST
   ranges / preload more aggressively before taking writes; let the old
   owner keep serving until the new one is warm (handback); shed or queue
   cold loads ahead of the forward deadline instead of timing out.
4. **New: per-request security-control lookup fails closed during a move**
   (`loading session revocations/takedowns failed … ShardMoved`, 6–13k per
   20 s per node): `xrpc/server.rs` returns 503 when no cached view is
   younger than `STALE_MAX_SECS`, adding errors to every write of an
   uncached account whose shard is moving.
5. **getRepo export of a 10M-record repo 2.1–2.6× slower than fa0975c**
   (17–22 s vs 8.3 s), caused by partial MSTs (a710e0c: 8.2 s with
   `--lazy-mst false`, 31 s with it on): one synchronous point get per
   interior node in `mst_lazy::export_blocks`. 1M is fixed.
6. **A fast same-id restart waits ~TTL before serving its shards**
   (`not joining yet: our clock is behind a peer's merged firehose`, 8.5–8.9 s
   at TTL 10 s): by design as far as I can tell (dead incarnation's `wm_cap`);
   could cap at the fenced log's last seq if that is safe.
7. Not regressions: getBlocks10 at 10M (±25% run-to-run, head mean 2.8k vs
   fa0975c 2.55k); write ceiling 58–72k (+0–6% vs fcf29b3; still −20–27% vs
   fa0975c: hedged sign + verify ~24% of CPU, RSS 2×) — accepted for now.
8. **New: 100M accounts on 4 nodes (one box) — bulk slows 78k → 3k
   accounts/s past ~75M and the first 10k/s stair kills 3 of 4 nodes**
   (block 6): every filter-cache miss re-downloads and zstd-decodes a whole
   SST bloom filter (`read_filters` 26% of CPU, ~0.6–0.8 MB range GETs,
   1–2.4 GB/s of SST reads per node for 10 MB/s of writes; 256 MB compacted
   SSTs, 3.6 GB block cache per node), then lease renewals stall behind the
   saturated store and the nodes fail-stop (lapsed 35–48 s past a 30 s TTL).
9. Harness: **`bench/benchbox/remote/minio.sh up` doesn't notice that
   `vlpds-xhost-minio` already serves 127.0.0.1:9200** (the xhost MinIO binds
   it as well as the LAN address), so benchbox benches run during an xhost
   session silently use the xhost container and `cleanup_prefix` deletes from
   the wrong data dir (~60 GB of `bench-*` prefixes leaked into
   `~/vlpds-bench/xhost/minio` today; deleted). Either bind the xhost MinIO
   to the LAN address only, or have `minio.sh up` refuse when 9200 is taken
   by another container. Also: `xhost.py --cleanup` deletes the state dirs,
   node logs included (fetch them first); `xhost.py report` still crashes.

## Files

- `bench.py` (round-1 copy + `restart`, permit waits in step deltas),
  `tables.py` (copy), `objsample.py` (the 1 s labelled object-store sampler
  run on devhost for the xhost runs).
- Block 1: `xh4/` (stairs + kill9/sigterm of n3, then kill9/sigterm of n1):
  `steps.jsonl`, `hosts.jsonl`, `metrics.jsonl.gz`, `xh4-all.jsonl.gz` /
  `xh4-failover.jsonl.gz` (sampler), `lg/`, `node-logs/n*.log.gz` (both runs,
  appended), driver logs. `xh5-head/`, `xh5-inf8k/`, `xh5-fcf/` (SIGTERM A/B;
  node logs were deleted by `xhost.py --cleanup`).
- Block 2: `restart.jsonl` (100k / 3k/s), `restart-1m.jsonl` (1M / 10k/s):
  per-scenario event with the ownership timeline and per-5 s loadgen windows;
  `restart-c4-n*.server.log.gz` (both runs, `==== restart <scenario>` markers).
- Block 3: `sweep-8be1e84.jsonl`, `sweep-fa0975c.jsonl` (1M + 10M),
  `prof-sweep-8be1e84.jsonl`, `rep-sweep-{8be1e84,fa0975c,a710e0c-lazy,a710e0c-full}.jsonl` (10M repeats).
- Block 4: `grid-10k-inj25.jsonl`, `grid-1m-inj25.jsonl`, `grid-1m-inj0.jsonl`,
  `ab-grid-10k-inj25.jsonl` (fa0975c), `prof-grid-10k-inj25.jsonl`.
- Block 5: `ha/` (`summary.md`, per scenario `result.json`, node / proxy /
  checker / loadgen logs, `probe.csv.gz`).
- Block 6: `capacity-100m/` (`RESULTS.md` from the driver, `populate.jsonl`,
  `steps.jsonl`, `metrics-10s.jsonl.gz`, `run-*.log` of the four sessions).
- `run-*.log`: one per benchbox invocation (local time tags, UTC−7).

## State of benchbox / devhost at the end

- benchbox: no vlpds / loadgen / MinIO / driver processes; the bench MinIO
  container is down and `~/vlpds-bench/minio` (incl. the 100M population)
  deleted; the xhost MinIO container and its data purged; build dirs
  `target-fa0975c`, `target-a710e0c`, `target-ab`, `src-ab`,
  `xhost/target-fcf29b3` and the HA tools/outputs deleted; scratch caches and
  state dirs empty. `~/vlpds-bench` is 8.8 GB (`target/`, `target-prof/` =
  8be1e84, `xhost/target` = 8be1e84, `src/`, results); / has 557 GB free
  (560 GB at the start).
- devhost: xhost state, sampler output, `target-fcf29b3` and `src-fcf`
  deleted; no nodes or MinIO running; `~/vlpds-bench/xhost/target` = 8be1e84;
  / 371 GB free (same as at the start).
