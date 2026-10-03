# vlpds on benchbox, 2026-10-03 round 3: head b252b08 (+ 999f7f0) vs round 2 (8be1e84)

Commit under test: **b252b08** (`vlpds` head when the round started: shard-move
warm/prewarm + single-flight meta cache e562977, ctl lookups wait out moves
6aa5f5f, one-scan getRepo f7acd23, cache-wide filter budget + compaction
worker writes into the DB cache + 64 MiB SSTs + `--meta-cache-mb` b252b08,
mTLS-only peers, exact account totals f0ff7bd, streaming importRepo e46a30d).
Mid-campaign `origin/vlpds` moved to **999f7f0** (6777d74/d4ded7b streamable
CAR getRepo, 999f7f0 O(1) checkAccountStatus); block 4/5 measure it.
Previous round: `../benchbox-2026-10-02-round2/RESULTS.md` (8be1e84).

Times are UTC in the logs (benchbox), PDT (UTC−7) in local file names.

## Summary

| Area | Result |
|---|---|
| `just benchbox-quick` (block 1) | First real campaign-driver run: **19.7 min wall** (build 5.1 min + 14.6 min of steps) vs README "18 min + ~7 min build". One driver bug fixed (pgrep self-match). Grid ceilings 1M/50k 58.8k, 10k/5k 53.7k (round 2: 67.7k / 58.0k); **ABBA A/B (block 4) puts b252b08 at −5% vs 8be1e84 on the 1M shape** (61.0/61.2k vs 62.4/66.5k), CPU/commit +3%: under the 10% bar |
| Cross-host failover (block 2) | **SIGTERM of the benchbox node: 2.1k errors (round 2: 134k)**; kill -9 of the devhost node 11.8k (round 2: 109k + 168k start-up storm); SIGTERM devhost 36.7k (140k); **kill -9 of the benchbox node still 72–97k** (129k). SST GETs per cold repo load 2–4.8 (round 2: 10–65). New: a graceful stop now takes 9.6–10.4 s (was 1.5–2.3 s); **bug: handoff prewarm 413s above ~29 shards** |
| 100M capacity (block 3) | **Filter refetch gone**: 0 meta-cache misses through the whole bulk (2 transient bursts at restarts), SST GETs 4.6–7.8 KB per created account (round 2 ~750 KB at 80–85M). With `--cache-dir` on the MinIO NVMe the bulk stalled at 75–77M (disk cache writing ~190 MB/s next to MinIO, `bulkCreate` timeouts, driver stop, 2 fail-stops on "barrier not durable"); **resumed without the disk cache: 77→100M at 18–30k/s (round 2: 3–6.4k/s)**, 100M in 4,732 s of bulk. **Stair 10k/s at 100M fell over again** (12 commits/s): cold loads 15–50 s each, S3 latency 0.5–1.8 s; MinIO grew to 24.4 GB RSS and the box OOM-froze for ~17 min (other services unresponsive), then all 4 nodes fail-stopped on lease lapse (1,033–1,040 s) |
| getRepo export (block 4) | b252b08 10M **10.6–10.8 s** (round 2 17–22 s, fa0975c 8.3 s), 1M 0.81–0.86 s. **999f7f0 (streamable CAR): 10M 4.6–4.9 s, 1M 0.44–0.47 s**: 1.7–1.8× faster than fa0975c |
| checkAccountStatus at 1M records (block 5) | b252b08 **1.37 s** p50 (1.33–1.73 s), 999f7f0 **138 ms** p50 (133–151 ms), same counts: 10× faster, not yet O(1)-cheap |
| Account-totals load at shard open (block 6) | Not isolated by a metric, but the post-replay open phase (flush + `ShardTotals::load` + warm wait) is **~10.5 s for 16 shards at 77M accounts** (n4, 9 segments replayed), 18–41 s on nodes with replay; cluster restart converged in **57 s** (round 2, 100M: 5.6 s). At 1M / 64 shards over the LAN: 7.6–9.9 s (fresh population: 0.2 s) |

## Block 1: `just benchbox-quick` on b252b08 (`quick/`)

`BENCHBOX_OUT=benchbox-2026-10-03-round3/quick BASELINE=benchbox-2026-10-02-round2 just benchbox-quick b252b08`.

| Step | README / plan estimate | Took |
|---|---|---|
| build (one sha, deps from `target-base` seeded from `target`) | ~7 min (`BUILD_EST_S` 420) | 304 s |
| grid-1m-inj25 | 140 s | 119 s |
| grid-10k-inj25 | 120 s | 103 s |
| sweep (1M + 10M) | 400 s | 364 s |
| methods sample | 70 s | 57 s |
| proxy | 75 s | **32 s** |
| failover (3 nodes, kill -9 + SIGTERM) | 270 s | 205 s |
| **total** | 18 min of runs + ~7 min build | **19.7 min** (14.6 + 5.1) |

All estimates were long; `durations.json` now holds the learned ones.

Driver findings:

1. **Fixed (trivial):** `campaign.sh` refused to start ("another campaign is
   running") because `ssh benchbox 'pgrep -f "vlpds-bench/campaign.py run"'`
   matches itself: Tailscale SSH's `tailscaled be-child ... --cmd=<the
   command>` process carries the pattern in its argv. Now
   `pgrep -f "vlpds-bench/[c]ampaign.py run"` (bench/benchbox/campaign.sh:54,
   uncommitted). The other pgreps already use bracket/regex forms that don't
   self-match.
2. `benchbox-quick <sha> <sha>...` (README: "HEAD, or `just benchbox-quick
   <sha>...`") measures only the first sha: `profile_steps` gives quick/full
   steps only `head`, but `campaign.sh` ships (and leaves in `srcs/`) sources
   for every sha. Either loop the profile over shas or document "one sha".
3. SUMMARY.md baselines match by file name, so the read sweep and getRepo
   have no deltas against round 2 (`sweep-8be1e84.jsonl` vs `sweep.jsonl`);
   cluster/failover rows never get deltas.
4. `capacity.sh`'s runner line prints `commit 8be1e84` (from `~/vlpds-bench/COMMIT`)
   when `BIN_DIR=bin/<sha>` runs another commit (cosmetic; the driver's
   RESULTS.md names the right binaries).
5. xhost `run.sh cleanup` doesn't copy `node-logs/` back (its log has no
   `out /…` line); fetched by hand. `xhost.py report` works again.

Results (`quick/SUMMARY.md`, deltas vs round 2):

| Shape | 25k/s | 50k/s | 75k/s offered | CPU µs/commit 25k / 50k / sat | RSS GB at 50k |
|---|---|---|---|---|---|
| 1M/50k inj25 | 66 / 122 ms | 49.6k, 106 / 477 ms | **58.8k** (round 2 67.7k) | 237 / 222 / 238 (round 2 233 / 220 / 233) | 14.5 |
| 10k/5k inj25 | 84 / 167 ms | 50.0k, 156 / 397 ms | **53.7k** (round 2 58.0k) | 185 / 218 / 251 (198 / 226 / 253) | 8.6 |

The ceilings looked −7% / −13% with unchanged CPU per commit, so block 4
reran the 1M shape ABBA against 8be1e84: **−5%** (below). Methods sample at
or above fcf29b3's (describeServer 156k, getRecord 85k, createRecord 7.3k,
createAccount 989/s). Proxy 1M × 512: 225k req/s, p99 5.1 ms, 94 µs/req.
Read sweep in block 4. One-box 3-node failover at 10k/s: kill -9 51k errors,
SIGTERM 36.7k (stop took 5.9 s); no earlier baseline for this shape.

## Block 2: cross-host failover (`xh6/`)

Round 2's `xh4` shape: n1, n2 on devhost (4 workers / 8 io), n3 on benchbox
(5 / 10), MinIO + loadgens on benchbox, 64 shards, 1M accounts (real/128),
200k active, `--inject 25`, lease TTL 10 s; b252b08 built once on devhost
(`sync.sh -b devhost`); peers mTLS (CA made on the first host and copied).
Stairs 10k/20k, then kill -9 / SIGTERM at 12k/s (signal at +35 s,
restart 20 s later). `xhost.py all` (n3 kill9 + sigterm), `failover --victim
n1`, then a kill -9 n3 repeat. 1 s labelled sampler on devhost
(`xh6/sampler/objsample.py`); per-event numbers from `xh6/sampler/events.py`
(output `events.txt`).

Stairs match round 2: 10k/s p50/p99 61/269 ms (63/292), 20k/s 105/755 ms
(100/353); firehose completeness on the devhost nodes at 20k/s still
0.10–0.23 (n3 1.0).

| Event | Errors after the signal (round 2) | Seconds with errors (round 2 window) | Worst 1 s p99 | Takeover | Victim exit | Peak S3 sockets n1 / n2 / n3 | State pool at cap (1,024) |
|---|---|---|---|---|---|---|---|
| kill9 n3 (benchbox) | **97,233** (129k) | 26 (43) | 22.9 s | 8.5 s (12.4) | 0.5 s | 1,536 / 1,467 / 658 | n1, n2 |
| kill9 n3, repeat | **72,036** | 31 | 19.2 s | 9.5 s | 0.5 s | 1,186 / 1,640 / 1,069 | no (762–770) |
| sigterm n3 | **2,131** (134k) | 4 (37) | 13.4 s | 12.2 s (1.5) | **9.6 s** (1.5) | 748 / 1,042 / 925 | no (≤ 242) |
| kill9 n1 (devhost) | **11,784** (109k + 168k before the kill) | 11 | 14.6 s | 6.0 s (5.4) | 0.8 s | 1,060 / 1,427 / 1,323 | n2, n3 |
| sigterm n1 | **36,739** (140k) | 9 (43) | 14.1 s | 10.7 s (2.3) | **10.4 s** (2.3) | 1,151 / 889 / 1,035 | no |

New metrics per event (sums over the three nodes, signal −2 s to step end,
which includes the rejoin):

| Event | `vlpds_shard_warm_seconds` batches / sum | `shard_warm_ssts` ok | `vlpds_security_ctl_loads_total` loaded / coalesced / stale_moving / **moved (503)** / unavailable | `vlpds_meta_cache_loads_total` filter fetched / shared, index fetched / shared | SST GETs per repo load, first 30 s, survivors |
|---|---|---|---|---|---|
| kill9 n3 | 6 / 1.2 s | 954 | 310k / 9.6k / 21.0k / **19.0k** / 474 | 676 / 7, 684 / 11 | 4.1, 4.5 |
| kill9 n3 rep | 6 / 1.3 s | 1,111 | 307k / 8.4k / 81.0k / **15.4k** / 1 | 563 / 8, 635 / 11 | 2.6, 4.8 |
| sigterm n3 | 8 / 2.8 s | 1,120 | 324k / 4.2k / 110k / **3.7k** / 0 | 561 / 7, 567 / 8 | 2.3, 3.0 |
| kill9 n1 | 6 / 1.8 s | 981 | 312k / 7.4k / 10.6k / **17.4k** / 1 | 647 / 21, 696 / 60 | 2.2, 2.4 |
| sigterm n1 | 8 / 3.3 s | 1,394 | 325k / 6.4k / 22.1k / **16.3k** / 0 | 456 / 6, 450 / 5 | 2.1, 2.4 |

- **Planned handoffs are fixed when they prewarm**: SIGTERM of the benchbox
  node 2.1k errors (one burst at the exit) vs 134k; the state pool never
  saturated. Recipients log `handoff prewarmed shards=10 recent=20480
  warmed=20480 elapsed_ms=4.9–8.0 s` before the stop, which is why a
  graceful stop now takes ~10 s and takeover is counted from the signal
  (10.7–12.2 s). The SIGTERM of a devhost node still cost 36.7k (the
  devhost nodes are the CPU-starved ones; most errors came at the rejoin).
- **Cold loads cost 2–4.8 SST GETs** (round 2: 10–65) and warm-on-open
  takes 0.1–2.4 s per batch. kill -9 of the devhost node is down to 11.8k
  errors with no start-up storm.
- **kill -9 of the benchbox node still costs 72–97k errors over 26–31 s**:
  the two devhost survivors take 22 shards cold (nothing to prewarm from)
  and their state pools sit at 762–1,024 in flight with 20–76k permit
  waits; errors peak after the survivors own the shards (+8.5 s) and again
  at the rejoin. Better than round 2 (129k / 43 s), not "far fewer".
- **ctl lookups still 503 during moves**: `moved` = 15–19k per kill -9 and
  3.7–16k per SIGTERM. 6aa5f5f waits at most `CTL_RETRY_FOR` (direct) /
  `TTFB_FAST − 0.5 s` (forwarded) for a move (`xrpc/server.rs` ~468); a
  kill -9 move takes 6–10 s, so the wait runs out. `unavailable` is gone
  except during kill -9 n3 (474, "partition owner: error sending request"
  to the dead node).

### New bug: handoff prewarm fails with 413 above ~29 shards (`xh6/node-logs/n2.log.gz`)

```
02:49:03.840 INFO vlpds::server: graceful shutdown: releasing shards shards=32
02:49:03.893 WARN vlpds::xrpc::internal: handoff prewarm failed: HTTP status client error (413 Payload Too Large) for url (https://10.0.0.51:8100/internal/v1/cluster/prewarm)
02:49:03.893 INFO vlpds::cluster: recipients prewarmed shards=32 elapsed_ms=38
```

Four times, every time a node handed 32 shards to one recipient (the
teardowns at the end of each invocation). `PrewarmIn` carries each shard's
recent DIDs (`recent: Vec<String>`, up to 2,048 per shard, ~35 B each in
JSON), and `cluster_prewarm` takes it through axum's default 2 MB `Json`
limit: 21 shards / 43,008 DIDs went through, 32 shards / ~65k didn't. Any
handoff of ≳ 29 busy shards to one peer (a 2-node cluster's rolling restart,
a scale-down) silently starts cold. Fix: raise the route's body limit, or
send DIDs per shard / cap the list.

## Block 3: 100M capacity retry (`capacity-100m/`)

Round 2's paced plan: `capacity.sh all --name cap100m-r3 --total 100000000
--nodes 4 --mode native --chunk 5000000 --settle-s 900 --active 500000
--rates 10000,25000,50000,75000,100000 --log-retention 3m --lease-ttl-ms
30000 --bulk-concurrency 4`, `BIN_DIR=bin/b252b08`, `NODE_EXTRA="--meta-cache-mb
1536"`, plus `--cache-dir` with `--disk-cache-shard-mb 1024` (16 GB per node)
in session 1. Per node 3 workers / 6 io, block and repo cache 3,612 MB each
(as round 2). 5 s labelled sampler of every node (`objsample5.jsonl.gz`,
`objsample5.py`), per-process disk I/O from session 2 (`procio.jsonl`).
Node logs (both sessions, saved by the driver's cleanup) in `node-logs/`.

| Accounts | Round 2 accounts/s | **Round 3 accounts/s** | Notes |
|---|---|---|---|
| 0–5M | 78.3k | 61.3k | session 1: SST disk cache on (MinIO's NVMe) |
| 5–20M | 49.6k → 36.3k | 37.5k → 31.0k | |
| 20–40M | 27.4k → 26.5k | 21.7k → 23.6k | |
| 40–60M | 23.3k → 24.0k | 23.4k → 16.5k | meta cache full (1,611 of 1,611 MB) from ~55M; still 0 misses |
| 60–75M | 20.5k → 16.9k | 14.1k → 10.6k | NVMe 100% util, iowait 70%, kswapd 90% CPU, 4 GB in swap |
| 75–77.2M | 9.2k | **stalled** | memtable flushes stuck ("no memtable/WAL flushed yet" for minutes), `bulkCreate` timed out on n1/n3/n4; driver stopped the nodes, n1/n3 fail-stopped on "close failed: barrier not durable within 30 s" |
| 77.2–80M | 9.2k | **29.5k** | session 2: restarted **without the disk cache** |
| 80–85M | 6.4k | **20.8k** | |
| 85–90M | 5.7k → 5.5k | **18.0k** | |
| 90–100M | 3.0–3.2k | **18.8k, 19.1k** | |
| **0–100M** | 2 h 21 min over 3 sessions | **4,732 s** (79 min) bulk, 514M records, 112 GB settled (1,120 B/account) | |

SST reads and meta cache per chunk (`chunk-sst-gets.md`, from the sampler,
which started at 30M; the 77–85M rows straddle the restart and count its
warm-on-open):

| Accounts | KB of SST GETs per created account | meta-cache misses (filter / index) |
|---|---|---|
| 30–55M | 3.3–7.8 | 0 / 0 |
| 55–75M (disk cache on, I/O-bound) | 9.0–15.5 | 5 / 30 |
| 85–100M (no disk cache) | **4.6–5.6** | **0 / 0** |
| round 2, 80–85M | ~750 (1.04–2.36 GB/s per node) | ~760 filter misses/s per node |

- **The filter-refetch collapse is fixed.** The meta cache filled to its
  1,536 MiB cap by ~55M (it holds ~3.5–5× `vlpds_sst_meta_bytes`, the
  encoded size: 284 MB at 50M, 448 MB at 77M) yet missed nothing; each
  node read 18–45 MB/s of SSTs, against 1–2.4 GB/s per node in round 2.
- **Session 1's stall was the SST disk cache, not vlpds:** at 75M MinIO
  received 25–48 MB/s while the NVMe wrote 240 MB/s (read 168 MB/s, 100%
  util), so ~190 MB/s were disk-cache writes on MinIO's drive (plus swap).
  MinIO's ingest fell from 138 to 25 MB/s over the session as the cache
  filled (`slatedb::cached_object_store` "evictor queue skipped cache
  write" warnings). Restarted without it, the same population went 3× faster
  past 77M. On this box a disk cache must live on another drive.
- **No lease lapse during the bulk** (TTL 30 s). Session 1's two fail-stops
  were driver-initiated stops of nodes whose memtables couldn't flush.
- MinIO itself reads ~650 MB/s from disk to serve ~130 MB/s of SST GETs
  (session 2, `/proc/<pid>/io`): ~5× read amplification on this
  single-drive setup.

**Stairs at 100M: still fails at the first stair (10k/s), and this time
took the box down.** `stair-10000` (500k active window): 12 commits/s,
257k errors, 471k dropped (round 2: 0 commits/s). Per node (sampler): state
in flight at the 1,024 cap from the first second, S3 latency 0.5–1.8 s (was
1–2 ms during the settle), 2.5–4.6k repos loading per node, repo loads
taking 15–50 s and completing at ~0.2/s. MinIO's RSS grew to 24.4 GB
(4,096 concurrent cold GETs; the 4 nodes at 8.5–9.1 GB each, loadgens
0.33 GB each), swap filled at 04:37 UTC, and **benchbox froze for ~17 min**
(sshd, Tailscale, Alloy and other services unresponsive) until the OOM killer
took MinIO at 04:54:47; all four nodes then fail-stopped with `node lease
lapsed past takeover: fail-stop lapsed_ms=1,033,166–1,040,555` (the host,
not the store, stalled them). I stopped the run there (no kill phase) and
deleted the population. Memory budget note: the driver gives caches 60% of
RAM and `--meta-cache-mb 1536` comes on top (+6 GB over round 2), which
left MinIO no headroom. Next time cap MinIO's memory (`docker --memory`) or
lower `--mem-gb`. The cold-load storm itself is round 2's (cold repos at
500k active need many uncached SST reads each and the single NVMe can't
serve them).

## Block 4: reads, getRepo export, and the write A/B (`ab/`, `quick/sweep.jsonl`)

`ab/` is one custom `campaign.py run` plan (`ab/plan-in.json`): grid
1M/50k inj25 ABBA 8be1e84/b252b08, sweep 1M+10M on 999f7f0, sweep 1M ABBA
8be1e84/b252b08, checkAccountStatus ABBA b252b08/999f7f0. 8be1e84's binaries
are round 2's `~/vlpds-bench/target` (copied into `bin/8be1e84`).

**getRepo export** (2.77 GB at 10M, 276 MB at 1M; two exports per run):

| Binary | 1M | 10M |
|---|---|---|
| fa0975c (round 2) | 0.86 s | 8.28–8.33 s |
| 8be1e84 (round 2) | 0.79–0.84 s | 17–22 s |
| **b252b08** | 0.81–0.86 s (and 0.77–0.86 in the A/B) | **10.6–10.8 s** (261 MB/s) |
| **999f7f0** (streamable CAR, 6777d74 + d4ded7b) | **0.44–0.47 s** | **4.6–4.9 s** (~580 MB/s) |

f7acd23's one M/ scan took 10M from 17–22 s to 10.7 s; the streamable
single pass halves it again, 1.7–1.8× faster than fa0975c.

**Write A/B, grid 1M/50k inj25, ABBA** (`ab/ab-grid-1m-inj25.jsonl`):

| Binary | 75k/s offered → achieved | p99 at 50k/s | CPU µs/commit 25k / 50k / 75k |
|---|---|---|---|
| 8be1e84 r1 / r2 | 62.4k / 66.5k | 576 / 577 ms | 232–233 / 217 / 233 |
| b252b08 r1 / r2 | 61.0k / 61.2k | 623 / 683 ms | 237 / 222 / 240–241 |

−5% on the ceiling, +2–3% CPU per commit: not a regression worth chasing
under the "write perf is good enough" rule (>10% bar).

**1M read sweep A/B** (`ab/ab-sweep-1m.jsonl`, ABBA):

| Binary | getBlocks10 | listRecords | describeRepo | getRecord | 1M fill |
|---|---|---|---|---|---|
| 8be1e84 | 11.5k / 11.4k | 36.9k / 36.6k | 21.1k / 21.0k | 77.7k / 80.0k | 4.6 / 4.7 s |
| b252b08 | **7.1k / 10.1k** (quick: 6.9k) | **28.8k / 34.1k** (quick: 28.6k) | 18.2k / 19.9k | 79.2k / 73.3k | 5.7 / 5.2 s (quick: 11.1 s) |
| 999f7f0 | 7.3k | 29.2k | | 78.4k | 8.8 s |

Possible read regression at 1M since 8be1e84: getBlocks10 −12 to −40%
(three of four b252b08/999f7f0 runs at ~7k vs 11.4k), listRecords −7 to
−22%, fill slower. Run-to-run spread is wide, so it needs a bisect over
8be1e84..b252b08 (the cache-wide filter budget / 64 MiB SSTs / meta-cache
split of b252b08 are the first suspects). 10M reads are at round 2's levels
(getBlocks10 2.0k, inside round 2's 2.0–3.5k; getRecord 61–67k vs 65k).

## Block 5: checkAccountStatus at 1M records (`ab/acctstatus.jsonl`, `acctstatus.py`)

One node, one repo of 1,000,000 records + 10k blobs (`loadgen sweep --reuse
--fill-only`), then 20 sequential calls and 8 concurrent callers for 10 s.

| Binary | p50 | min–max | 8 callers | Response |
|---|---|---|---|---|
| b252b08 r1 / r2 | **1,365 / 1,366 ms** | 1,332–1,727 ms | 4.0 / 3.8 /s | repoBlocks 1,266,979, indexedRecords 1,000,000, blobs 10,000 / 10,000 |
| 999f7f0 r1 / r2 | **138 / 137 ms** | 133–151 ms | 12.8 / 12.7 /s | identical counts |

10× faster with identical answers. Still 133 ms at 1M and only ~13/s
with 8 callers, so something in the call is still proportional to repo or
blob count. It's worth a profile if "O(1)" is the goal.

## Block 6: shard-open cost of the account totals (`T/`)

There is no metric for `ShardTotals::load` alone. The `shards opened` log
line's `elapsed_ms − replayed_ms` covers the post-replay memtable flush (only
when segments were replayed), the totals load of every shard (32 at a time)
and any wait for the warm-up.

| Where | Shards | Replayed segments | Warm done after | elapsed − replayed |
|---|---|---|---|---|
| xh6 n1 (devhost, LAN S3), 1M accounts, fresh population | 64 | 0 | 0 ms | **0 ms** |
| xh6 n1, same population after the stairs (3 restarts) | 64 | 0 | 3.8 / 3.9 / 1.2 s | **7.6 / 9.4 / 8.3 s** |
| xh6 n3 (benchbox, local S3), takeovers | 21–32 | 0 | 0.4–0.7 s | 0.9–2.2 s |
| capacity n4, 77M accounts, after session 1's crash | 16 | 9 | 0.7 s | **10.5 s** |
| capacity n1 / n2 / n3, same restart | 16 | 1,494–2,831 | 1.8–2.4 s | 18.3 / 38.3 / 41.3 s (incl. flush) |

The 77M restart converged in **57 s** (round 2's 100M restarts: 5.6 s), and
xh6 node start-ups went from 1.7 s (fresh) to 9.9–11.7 s. n4's 10.5 s with
nine segments replayed and the warm-up done after 0.7 s is mostly the totals
load. That's plausible since every entry rewrites its slot's `T/` row, so after
heavy writes the row's versions sit across many L0 SSTs and memtables
until compaction. A timing log or histogram around `ShardTotals::load`
would confirm it. Worth fixing before relying on fast restarts.

## New bugs / findings (for TODO)

1. **Handoff prewarm 413s (Payload Too Large) at ≳ 29 shards per recipient**
   (block 2): `PrewarmIn.recent` DIDs exceed axum's 2 MB JSON limit; the
   recipient silently starts cold. Evidence: `xh6/node-logs/n2.log.gz`, 4×.
2. **Shard open got slow after account totals (f0ff7bd)** (block 6):
   7.6–10.5 s of post-replay time at 16–64 shards after writes (fresh: 0 s);
   57 s cluster restart at 77M (round 2: 5.6 s at 100M).
3. **ctl lookups still 503 during kill -9 moves** (block 2): `moved`
   15–19k per kill -9, 3.7–16k per SIGTERM; the wait budget is shorter than
   a kill -9 move (6–10 s).
4. **kill -9 of the benchbox node still 72–97k errors** (block 2): survivors
   cold-load 22 shards with nothing to prewarm from; state pools saturate.
   SIGTERM is fixed (2.1k) at the price of ~10 s graceful stops.
5. **100M stair 10k/s still collapses** (block 3): cold loads 15–50 s at 0.5–1.8 s
   S3 latency; MinIO ballooned to 24 GB and OOM-froze benchbox for 17 min.
   Harness: cap MinIO memory; don't stack `--meta-cache-mb` on the 60% cache
   budget; never put `--cache-dir` on MinIO's NVMe here.
6. **Possible 1M read regression since 8be1e84** (block 4): getBlocks10
   7–10k vs 11.4k, listRecords −7 to −22%; needs a bisect.
7. checkAccountStatus: 10× faster at 999f7f0, but still 133 ms at 1M
   records (block 5).
8. Confirmed and closed: filter refetch at 75M+ (0 misses, 5 KB/account of
   SST reads); getRepo 10M (4.6–4.9 s at 999f7f0); planned-handoff error
   storms (SIGTERM 2.1k); write ceiling within 5% of round 2.
9. Driver: see block 1 (fixed pgrep self-match; quick/full ignore extra
   shas; baseline file-name matching; capacity runner prints the wrong
   commit; xhost cleanup doesn't copy node logs).

## Files

- `quick/`: the campaign (`SUMMARY.md`, `campaign.log`, `campaign.jsonl`,
  per-step jsonl and `run-*.log`).
- `xh6/`: cross-host run (`RESULTS.md` from `xhost.py report`, `steps.jsonl`,
  `hosts.jsonl`, `metrics.jsonl.gz`, `lg/`, `node-logs/n{1,2,3}.log.gz`,
  `run-*.log`; `sampler/objsample.py`, `sampler/xh6-all.jsonl.gz` (1 s,
  labelled), `sampler/events.py` + `events.txt`).
- `capacity-100m/`: driver `RESULTS.md`, `populate.jsonl`, `steps.jsonl`,
  `metrics.jsonl.gz`, `objsample5.jsonl.gz` (+ `objsample5.py`,
  `caprate.py`, `chunkget.py`, `chunk-sst-gets.md`), `procio.jsonl`,
  `node-logs/` (full node logs), `node-logs-session{1,2}/` (bulk and stair
  loadgen stderr), `run-195938-all.log` (session 1), `run-211357-all.log`
  (session 2).
- `ab/`: block 4/5 campaign (`plan-in.json`, `SUMMARY.md`, jsonl, logs).
- `acctstatus.py`: the checkAccountStatus driver (imports `bench.py`).

## State of benchbox / devhost at the end

- benchbox: no vlpds / loadgen / MinIO / driver / sampler processes; the
  bench MinIO container is down and `~/vlpds-bench/minio` deleted (the 100M
  population was deleted by `capacity.sh cleanup`); `scratch/bench`,
  `srcs/*`, `target-b-*`, the samplers and the copied `bin/8be1e84` removed.
  Kept: `bin/{b252b08,999f7f0}` (the campaign driver's build cache),
  `target-base`, `target`, `target-prof`, results. `~/vlpds-bench` 15 GB, /
  552 GB free (558 GB at the start). other services active and answering;
  batch pipeline idle (next 07:15 UTC).
- devhost: xh6 state, sampler and its output deleted; no nodes running;
  `~/vlpds-bench/xhost/target` = b252b08; / 371 GB free (same as at the start).
