# vlpds capacity test: dry1m

Driver: `bench/capacity/run.py` (`report --name dry1m --out bench/results/capacity-2026-10-01-laptop --total 1000000 --nodes 3 --active 20000 --rates 5000,10000,20000,40000,60000 --duration 30 --warmup 10 --chunk 100000 --settle-s 420`). 3 native nodes on one box (ports 2700-2702), MinIO at http://127.0.0.1:9200, prefix `dry1m`, binaries `/tmp/scratch/capacity/bin`.
Per node: `--workers 2 --io-threads 3 --block-cache-mb 3694 --repo-cache-mb 3694 --log-retention 5m --slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m`  (no SST disk cache).

## Population

1,000,000 accounts, records per repo `real/scale=128/knee=2/group=32`: 5,305,920 records (mean 5.32, p50 2, p90 5, p99 69, p99.9 487, max 2040; 103,552 empty repos). Real network: mean 455 (508 over repos with records), p50 10, p99 9,803, max 593,772.

| Accounts | Records | Time | Accounts/s | Records/s | S3 bytes settled | Bytes/account | Settle |
|---|---|---|---|---|---|---|---|
| 1,000,000 | 5,305,920 | 10 s | 102,297 | 542,781 | 2.23 GB (assign 0.00, log 0.63, nodes 0.00, retain 0.00, state 1.59, writers 0.00) | 2226 | 132 s |

Per chunk: 117,580/s, 128,258/s, 100,951/s, 108,366/s, 114,948/s, 105,953/s, 87,431/s, 86,055/s, 92,586/s, 93,455/s

## Active window stairs (20,000 active of 1,000,000, churn 200 repos/s)

Open-loop, latency from the scheduled send; one loadgen per node at rate/N (not DID-routed: (N-1)/N forwarded), +200/s hot repo and a firehose consumer on n1. Window 10 s warmup + 30 s measured.

| Offered/s | Achieved/s | Err (warmup incl.) | Dropped | p50 | p99 | p99.9 | FH lag p50/p99 | Loads/s | Commits/s | CPU % per node | RSS GB per node | S3 req/s (PUT/GET) | S3 in/out MB/s | Seg PUT/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 5,200 | 5,201 | 0 (11768) | 0 | 2.9 | 54.2 | 82.7 | 7.2/64.3 | 242 | 5133 | 58 / 54 / 57 | 1.06 / 1.0 / 1.03 | 7553 (2306/4673) | 7.0/13.6 | 2179 |
| 10,200 | 10,199 | 0 (0) | 0 | 10.8 | 82.7 | 173.2 | 11.6/65.1 | 201 | 10143 | 83 / 80 / 81 | 1.65 / 1.57 / 1.6 | 2385 (813/1561) | 8.3/5.4 | 1081 |
| 20,200 | 19,670 | 15891 (15891) | 0 | 3.9 | 4313.1 | 4956.2 | 7.7/3538.9 | 175 | 17650 | 111 / 110 / 112 | 2.71 / 2.62 / 2.66 | 3540 (1944/1593) | 20.1/4.4 | 1735 |

Saturated at 20,000/s offered.

SlateDB compaction counters (per s, per stair): `compactor_bytes_compacted_total` 365207.2 / 934204.3 / 1510936.6; `compactor_jobs_claimed_total` 6.4 / 6.7 / 6.2; `compactor_ssts_written_total` 13.6 / 25.0 / 19.3

## kill -9 of n3 at 6,000/s

Killed at 30.0 s (from loadgen start), restarted at 60.7 s, serving at 61.0 s; survivors owned every shard 20.7 s after the kill; load entered through the survivors only; rejoin converged in 19.5 s. Measured window: achieved 5,218/s of 6,200, 120,880 errors, p99<= 7327.7 ms. Seconds below 50% of offered: 26 (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 51, 52, 53, 54, 55, 56, 57, 58).

| t (s) | ok/s | errors/s | p99 ms |
|---|---|---|---|
| 25 | 6,208 | 0 | 28.7 |
| 26 | 6,234 | 0 | 26.7 |
| 27 | 6,196 | 0 | 24.4 |
| 28 | 5,974 | 0 | 55.6 |
| 29 | 6,131 | 0 | 338.9 |
| 30 | 5,984 | 462 | 214.3 |
| 31 | 4,160 | 2,018 | 95.2 |
| 32 | 4,245 | 2,013 | 151.0 |
| 33 | 4,186 | 2,022 | 61.2 |
| 34 | 4,178 | 2,020 | 139.4 |
| 35 | 4,194 | 2,001 | 31.2 |
| 36 | 4,157 | 1,995 | 31.6 |
| 37 | 4,186 | 2,080 | 27.9 |
| 38 | 4,157 | 2,012 | 38.9 |
| 39 | 4,189 | 2,031 | 24.6 |
| 40 | 4,126 | 2,068 | 25.0 |
| 41 | 3,668 | 1,949 | 255.2 |
| 42 | 4,711 | 2,078 | 239.1 |
| 43 | 4,084 | 2,005 | 129.1 |
| 44 | 4,112 | 1,988 | 227.2 |
| 45 | 4,273 | 2,066 | 189.3 |
| 46 | 4,224 | 1,964 | 197.5 |
| 47 | 4,258 | 2,011 | 293.1 |
| 48 | 4,079 | 2,012 | 143.0 |
| 49 | 4,300 | 1,966 | 82.4 |
| 50 | 4,092 | 2,015 | 134.8 |
| 51 | 1,159 | 111 | 703.0 |
| 52 | 781 | 0 | 1849.3 |
| 53 | 578 | 0 | 2920.4 |
| 54 | 566 | 263 | 3792.9 |
| 55 | 793 | 1,249 | 4489.2 |
| 56 | 1,138 | 2,011 | 5701.6 |
| 57 | 307 | 1,526 | 6324.2 |
| 58 | 1,647 | 1,222 | 7458.8 |
| 59 | 3,724 | 1,941 | 8167.4 |
| 60 | 5,173 | 2,713 | 8511.5 |
| 61 | 9,097 | 2,445 | 8749.1 |
| 62 | 18,987 | 206 | 7610.4 |
| 63 | 16,769 | 0 | 6033.4 |
| 64 | 11,210 | 0 | 2971.6 |
| 65 | 3,979 | 0 | 931.3 |
| 66 | 5,455 | 0 | 1412.1 |
| 67 | 5,124 | 934 | 1470.5 |
| 68 | 4,361 | 2,255 | 1423.4 |
| 69 | 6,739 | 2,049 | 1645.6 |
| 70 | 4,908 | 2,299 | 537.1 |
| 71 | 4,421 | 761 | 231.2 |
| 72 | 4,057 | 576 | 1109.0 |
| 73 | 4,483 | 874 | 2388.0 |
| 74 | 4,278 | 1,960 | 96.1 |
| 75 | 4,221 | 1,851 | 2818.0 |
| 76 | 4,576 | 1,935 | 2963.5 |

## Dry-run notes (laptop, 2026-10-01)

Tooling check at small scale, **not a benchmark**: the M4 Pro laptop was shared with
an HA test cluster, two rustc builds and native MinIO serving other agents (load
average 20–37 on 14 cores the whole time). Binaries: `dev-release` profile from the
working tree at fa0975c + uncommitted edits (not the fat-LTO release build).

- **Population.** 1M accounts × real/128 (knee 2), 5.3 M records, 3 nodes: 102k
  accounts/s, 543k records/s (10 s). A separate 5M run (`bulk5m`, deleted) ran at
  29–69k accounts/s per 500k chunk (44k/s mean, 236k records/s, 113 s) under
  heavier load. The rate falls with the box's load, not with size.
- **Bytes.** Settled live state (compacted SSTs) at 5M: **~700 B/account**
  (13.7 MB × 256 shards = 3.5 GB). That's ~323 B per repo plus ~71 B per
  synthetic genesis record (real records are ~154 B). Log segments from the bulk
  are **~590 B/account** until `--log-retention` expires them. Mid-bulk, du
  showed ~2 KB/account: L0s, compaction garbage held by the 2 min checkpoint, and
  the log. GC only runs while nodes run: a `populate` that stops the nodes right
  after the settle freezes the garbage until the next start.
- **Stairs (20k active of 1M, churn 200/s, 3 nodes, 2 workers + 3 IO threads each).**
  5k/s and 10k/s were clean, with p99 54–83 ms. 20k/s saturated: periodic
  whole-node stalls ("tokio runtime stall late_ms" up to 710 ms right after each
  10 s node-log checkpoint), 3 s forward timeouts, and p99 4.3 s. On this box that
  is CPU starvation. Watch for it on benchbox with 32 threads.
- **Cold start.** The first ~6 s after the bulk and ~20 s after a node restart
  served nothing: every first write to a window repo is a cold load, and
  forwards hit the 3 s owner deadline. That time sits in the warmup, so measured
  errors exclude it (`errors_measured`). The `kill9` step restarts every node,
  so its seconds 1–19 are this stall, not the kill.
- **kill -9 of n3 at 6k/s (load through n1+n2 only).** The survivors owned every
  shard **20.7 s** after the kill (lease TTL 10 s). The restarted n3 served
  within 0.3 s and rejoin converged in 19.5 s. After the takeover there was a
  second ~10 s dip, the handback. In the first attempt (`steps-run1.jsonl`), MinIO
  timeouts (30 s object_store retries, shared MinIO) stretched the takeover past
  60 s.
- **Resume.** The populate was SIGTERMed in chunk 4. Resuming from the lowest
  per-node watermark worked, but **bulkCreate re-creates DIDs that already exist**
  (`created 95904` of 95904 in a range n2/n3 had already finished), which orphans
  their earlier rows. See the server asks below.

## Server changes this test wants (not made; src/ is out of this lane)

1. `vlpds.admin.bulkCreate` should skip DIDs that already exist (check `h/`, not
   just the repo cache) and report them as `existing`. Then a resumed chunk is
   idempotent. Today a resume re-creates up to concurrency × batch accounts per
   node, with new keys, a new head and orphaned `R/`/`c/` rows.
2. Let bulkCreate take per-account record counts (`records: [u32]` with
   `count` entries). The distribution then costs 1 request per 1,000 accounts
   instead of ~20 (`loadgen dist`: 1.95 M requests per node at 100M; every
   node receives every range).
3. Optional: also take an explicit index list, so loadgen can send each node
   only the DIDs it owns (today each node hashes and skips 2/3 of every range).

## Benchbox run plan

Population: **100M accounts, `--dist real --dist-scale 128 --dist-knee 2`**.
Draws of 0–2 records stay exact, and the excess above 2 is divided by 128, so the tail keeps its
shape. The result: 529 M records, mean 5.3, p50 2, p90 5, p99 70, p99.9 453, max ~2.9k
(`loadgen dist --count 100000000 --dist real --dist-scale 128 --dist-knee 2`).
The real network has mean 455, p99 9.8k and max 594k: the scaling divides the
records stored ~85×, while the repo count stays real. Consequence: cold loads are
cheaper than on the real network (the median active repo has 2 records, not 10).

Disk (measured per-account costs × 100M):
- live state ~70 GB
- bulk peak ~100–120 GB: garbage + L0 backlog + ~10 GB of bulk log at 5 min retention
- stairs: + log at W × rate × ~2.5 KB/commit, i.e. 75 GB at 100k/s with
  `--log-retention 5m`; use `--log-retention 3m` above 75k/s

That is ≤ ~200 GB in `~/vlpds-bench`, under `CAP_GB=250`, while `MIN_FREE_GB=255`
keeps ≥ 255 GB free of the 566 GB. Fallback if the cap trips: `--dist-knee 0`
(mean 3.5, ~55 GB live).

```bash
# 0. commit the tooling (sync.sh ships `git archive HEAD`), then:
bench/benchbox/sync.sh
bench/benchbox/capacity.sh plan --total 100000000            # no servers: prints the population
# 1. population (resumable; re-run the same line after a guard abort)
bench/benchbox/capacity.sh populate --total 100000000 --nodes 4 --mode docker --chunk 5000000 --settle-s 900
# 2. stairs + kill on the kept population (500k active, churn 5k/s)
bench/benchbox/capacity.sh all --total 100000000 --nodes 4 --mode docker --active 500000 \
    --rates 10000,25000,50000,75000,100000,125000 --duration 60 --warmup 30 --kill-down 30 --log-retention 3m
# 3. read RESULTS.md, then delete the population
bench/benchbox/capacity.sh cleanup --total 100000000 --nodes 4
ssh operator@benchbox '~/vlpds-bench/minio.sh wipe; ~/vlpds-bench/minio.sh down'
```

Defaults on benchbox (32 threads, 62 GB, 4 nodes): per node `--io-threads 6 --workers 3`,
block cache and repo cache ~3.3 GB each (35% each of a 60%-of-RAM budget split over the nodes; `--mem-gb` to change). Ports
2700–2703 are scraped by Alloy into the vlpds dashboard. The containers use
`--network host --ipc host --log-driver none --security-opt seccomp=unconfined`,
nofile 1M and no cgroup limits, with the release binary bind-mounted into
`ubuntu:24.04` (pulled once).

Expected duration:
- bulk at an assumed 40–80k accounts/s (laptop 44–102k/s; benchbox pays ~6.5 ms
  fsync PUTs, though bulk segments are large): 21–42 min, + ≤ 15 min settle
- node startup on 100M: ~1 min
- stairs: 6 × 90 s ≈ 10 min
- kill: ~3 min

About **1–1.5 h**: one guard window (batch runs every 6 h, `guard.sh` wants ≥ 75 min
before the next run), or two if the populate is split across windows.

## Files

`populate.jsonl` (per chunk + summary), `steps.jsonl` (per step: loadgen results, 1 s windows, metric summary), `metrics.jsonl` (gzipped here; 1 s scrape of every node + MinIO; prefix du every 30 s). Regenerate: `bench/capacity/run.py report <same flags>`.
