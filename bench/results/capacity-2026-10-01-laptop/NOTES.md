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

Docker mode was validated on the laptop too: 3 containers from a Linux image built with
`bench/ha/Dockerfile` (`DOCKER_IMAGE=<img> DOCKER_BIN=image`; image since removed)
on host networking, 50k accounts, 2 stairs, then `docker kill -s KILL` and a restart.
Takeover took 22.4 s, then cleanup ran. On benchbox, the default bind-mounts the
release binary instead.
