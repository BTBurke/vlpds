# vlpds benchmarks, 2026-10-02 (commit b0282ce + loadgen additions)

Machine: Apple M4 Pro laptop (14 cores: 10P+4E, 48 GB), quiet apart from
macOS `BTLEServer` pegging ~1 core the whole time. Storage: native MinIO on
127.0.0.1:9200 (laptop SSD), a fresh prefix per run, deleted afterwards.
Binaries: `CARGO_TARGET_DIR=target/bench cargo build --release --bins` (fat
LTO, jemalloc). Servers run `--dev-mode --no-rate-limits --cache-dir <tmp>`,
defaults otherwise (8 repo workers, 6 IO threads, 256 shards, 8 MB max segment).
Load generator, MinIO and the server all share the laptop.

Driver: `bench/results/2026-10-02/bench.py` (stdlib Python). Raw numbers per
step are JSON lines in this directory: `grid.jsonl`, `grid-segsize.jsonl`,
`cluster.jsonl`, `methods.jsonl`, `sweep.jsonl`, `firehose.jsonl`,
`proxy.jsonl`, `resource.json`.

Latencies are open-loop, measured from the scheduled send time (no
coordinated omission), over a 20 s window after a 10 s warmup. Every grid step
also runs a 200/s hot repo and a firehose consumer. "Saturated" means
achieved < 93% of offered, >1% errors, or p99 > 2 s; the stair stops there.
"inj25" means `--inject-put-ms 25` (lognormal, sigma 0.5) on segment PUTs.

## Summary

| Experiment | Headline | vs old |
|---|---|---|
| 1. Write grid, 1 node | inj25: 25k/s at p50 62 / p99 135–150 ms on every shape (10M/500k needs `--block-cache-mb 16384`). 50k/s at p99 163–275 ms where 5–50k active (10k/5k saturated at 44k). Ceiling ~44–54k/s. inj0: 75k/s at p50 8 ms / p99 32 ms (10k/5k, after the apply fix); ceiling ~95k/s | Old: 75k/s at p99 129 ms (inj25). **Regression: one segment PUT in flight × 8 MB cap ≈ 155 MB/s per node log.** 32 MB segments give 75k/s at p99 245 ms |
| 1. Hot repo | 20k writes/s to one repo at p99 22 ms (inj0) / 132 ms (inj25) | Old: hot p99 ≈ fleet p99; still true |
| 1. Memory | 10M/50k: 7–9 GB RSS (was 12–20 GB); 10M/500k: 7.6 GB (was 27 GB) | Fixed per-shard SlateDB caches |
| 2. 3 nodes (1 laptop) | 50k/s at p99 150 ms (inj25); saturates ~70k/s (box-bound). kill -9: ~15 s outage for the dead node's shards (TTL 10 s) + ~3 s handback gap; SIGTERM ~2 s | First run collapsed at 50k/s: **fixed** (h2 peer client) |
| 3. Methods | Small-repo reads 48–86k/s, p99 < 3.5 ms; writes 32–58k/s closed-loop (inj0); logins Argon2-bound at ~600/s | – |
| 4. Repo size 100→10M | Point reads flat (getRecord 68k→40k/s, p99 < 0.8 ms); getRepo 400 MB/s at any size (10M = 2.77 GB in 6.8 s); getBlocks O(repo): 22/s at 10M after a 2.2× fix | – |
| 5. Firehose | Delivery adds 0–4 ms over the ack (1 node), ~20 ms (3-node merge). Fan-out clean to ~300 MB/s; box tops out at ~1.8 GB/s, and fan-out load pushes write p99 to 1–2 s. Backfill 35k ev/s per subscriber (sequential GETs) | Old: firehose lag p50 99–110 ms at inj25; now ≈ ack latency |
| 6. Proxy | 73–91k req/s, p99 2.6–3.2 ms, ~65 µs of server CPU per request | Target 200k–2M: not reached on a shared laptop; syscall/HTTP-stack-bound |
| 7. Resources @10M/50k, 50k/s | RSS 9.8 GB: ≤5 GiB shared SST cache, ~0.6 GB repo cache (~1.7 KB/repo), 0.5 GB firehose ring, up to ~2 GB memtables (est.) | Explains the old 11–27 GB |

## 1. Single-node write scale grid

Command: `bench.py suite <total> <actives> 0,25 <rates>`. Per total: one
prefix, `loadgen bulk` (5 records/repo), then for each injection mode a server
(re)start and a stair per active window, using `loadgen run --sim-total T
--sim-active A --sim-churn A/100 --hot-rate 200 --firehose` (deterministic
bulk DIDs, self-minted tokens, mix 80/10/10 create/update/delete).
Bulk creation: 10M accounts in 129–130 s (77k accounts/s; old 77–101k/s).

### Headline vs the old (per-shard-log) numbers, 25 ms injected PUT latency

| Shape | Old | New, default 8 MB segments | New, 32 MB segments |
|---|---|---|---|
| 20k / 10k repos, all active | 50k/s p50 49 p99 122 | 25k/s p50 64 p99 135; 50k/s saturates (44k/s achieved) | 50k/s p50 85 p99 169; 75k/s p50 140 p99 245 |
| 1M repos / 500k active | 25k/s p50 47 p99 120 | 25k/s p50 62 p99 148; 50k/s p50 115 p99 275 | – |
| 10M repos / 50k active | 75k/s p50 54 p99 129 | 50k/s p50 103 p99 252 (shared cache: p50 83 p99 163); 75k/s saturates (~54k/s) | – |
| 10M repos / 500k active | – | 10k/s p50 53 p99 116; 25k/s fails (p99 2.6 s) | 16 GiB shared cache: 25k/s p50 61 p99 144 |

Without injection (local MinIO, PUT p50 0.5–5 ms): 75k/s at p50 41–57 ms /
p99 66–114 ms for every shape with a 5k–50k active window, and saturation at
~78–88k/s (achieved) at 100k offered. Up to 50k/s, p50 is 2–5 ms and p99
16–28 ms.

### Findings

1. **The node log is the throughput ceiling, not CPU.** One segment PUT is in
   flight per node log, and a segment is capped at `--max-segment-mb 8`. So a
   node commits at most ~8 MB per (PUT latency + upload time). At 25 ms
   injected that's 18–19 segs/s × 8.3 MB ≈ 155 MB/s ≈ 44–54k commits/s
   (~3.5 KB of segment per single-record commit). Without injection it's
   ~34 segs/s × 8.2 MB ≈ 280 MB/s ≈ 78–80k commits/s; the server sat at 650–720%
   CPU of 14 cores. The old design had one log per shard (many PUTs in flight)
   and reached 75k/s under injection. `--max-segment-mb 32` restores 75k/s with
   injection, but the tail grows (p99 245 ms), because each 32 MB PUT is slow on
   its own. **Fix (not done, design change): allow 2–4 segment PUTs in flight
   per log and finalize in ordinal order.** Fencing still works by ordinal; the
   replay "hole" rule needs care. Shrinking segment bytes per commit (TODO
   "segment bytes", ~15–25%) moves the ceiling proportionally.
2. **Commit latency is ~1.5–2 PUTs, by design.** A commit waits for the
   in-flight PUT to finish, then its own: p50 ≈ 52 ms with 25 ms (σ 0.5)
   injected, versus 49 ms on the old design at low load.
3. **Memory bug fixed: per-shard SlateDB caches.** Each of the 256 shard DBs
   got SlateDB's default private 512 MiB block cache + 128 MiB meta cache
   (~160 GiB worst case). RSS crept to 19.5–27 GB at 10M repos, and the laptop
   swapped (7.3 of 8 GB swap in use). Now one shared cache (`--block-cache-mb`,
   default 4096, meta +25%) serves every shard DB (`src/partition.rs`).
   10M/50k peak RSS 13.6 → 7.2 GB; 10M/500k 27 → 7.6 GB (4 GiB cache) or
   ~20 GB (16 GiB).
4. **Large active windows are bounded by cold loads.** At 500k active out of
   10M, the window is bigger than the repo cache (8 workers × 50k = 400k).
   About 7.5–8k loads/s at 700–870% CPU, and the fleet tail grows (p99
   250–470 ms at 10k/s with a 4 GiB block cache). Over the same step the hot
   repo and firehose lag stay at p99 ~40 ms (inj0), so the tail comes from
   loads, not the commit path. A 16 GiB block cache fixes 10k–25k/s. Old 1M/500k
   at 25k/s: p99 120 ms; new: 148 ms.
5. With 5k–50k active, the hot repo's p99 equals the fleet's in every step, as
   before.

Tables: `python3 tables.py grid [file]`. Files: `grid.jsonl` (original
binary, per-shard caches), `grid-sharedcache.jsonl` (10M rerun, 4 GiB shared),
`grid-cache16g.jsonl` (10M/500k, 16 GiB), `grid-segsize.jsonl` (10k/5k inj25,
32 MB segments).

| Shape (total/active/inj) | Offered/s | Achieved/s | Err | Dropped | p50 ms | p90 | p99 | p99.9 | Hot p99 | FH lag p50/p99 | Srv CPU % | RSS GB | Loads/s | Commit/seg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10000/5000/inj0 | 10200 | 10200 | 0 | 0 | 1.8 | 2.5 | 16.4 | 25.8 | 17.8 | 2.3/16.6 | 104 | 1.35 | 0 | 7 |
| 10000/5000/inj0 | 25200 | 25200 | 0 | 0 | 2.6 | 4.2 | 20.7 | 29.5 | 20.6 | 2.5/20.8 | 234 | 2.15 | 0 | 22 |
| 10000/5000/inj0 | 50200 | 50200 | 0 | 0 | 5.3 | 11.0 | 28.3 | 42.5 | 28.4 | 4.8/27.9 | 441 | 3.63 | 0 | 77 |
| 10000/5000/inj0 | 75200 | 75202 | 0 | 0 | 41.5 | 59.1 | 65.9 | 71.1 | 65.6 | 40.8/65.9 | 638 | 5.62 | 0 | 407 |
| 10000/5000/inj0 | 100200 | 88188 | 3 | 281059 | 222.2 | 232.4 | 243.2 | 251.6 | 242.9 | 226.2/251.5 | 722 | 7.68 | 0 | 2304 |
| 10000/5000/inj25 | 10200 | 10200 | 0 | 0 | 53.5 | 88.4 | 126.6 | 143.0 | 127.7 | 54.0/127.0 | 165 | 4.78 | 215 | 338 |
| 10000/5000/inj25 | 25200 | 25200 | 0 | 0 | 63.6 | 96.4 | 135.2 | 167.2 | 134.0 | 65.2/138.2 | 227 | 5.44 | 1 | 941 |
| 10000/5000/inj25 | 50200 | 44314 | 0 | 166323 | 420.9 | 491.3 | 562.2 | 582.1 | 561.7 | 425.2/566.3 | 378 | 6.75 | 0 | 2353 |
| 1000000/50000/inj0 | 10200 | 10200 | 0 | 0 | 1.8 | 2.5 | 17.3 | 32.6 | 17.5 | 2.2/17.5 | 144 | 8.8 | 1997 | 7 |
| 1000000/50000/inj0 | 25200 | 25200 | 0 | 0 | 2.6 | 4.0 | 19.1 | 24.9 | 19.3 | 2.6/19.4 | 252 | 6.22 | 123 | 22 |
| 1000000/50000/inj0 | 50200 | 50200 | 0 | 0 | 4.3 | 7.2 | 20.3 | 35.0 | 20.8 | 3.9/20.2 | 452 | 7.52 | 28 | 62 |
| 1000000/50000/inj0 | 75200 | 75202 | 0 | 0 | 47.2 | 55.7 | 65.4 | 73.7 | 64.8 | 46.9/66.4 | 636 | 9.44 | 9 | 401 |
| 1000000/50000/inj0 | 100200 | 85943 | 1 | 390060 | 226.0 | 248.1 | 265.5 | 279.3 | 264.4 | 239.6/579.6 | 704 | 9.41 | 4 | 2746 |
| 1000000/500000/inj0 | 10200 | 10200 | 0 | 0 | 1.9 | 3.7 | 9.0 | 23.8 | 9.0 | 2.2/8.8 | 280 | 12.2 | 7489 | 7 |
| 1000000/500000/inj0 | 25200 | 25200 | 0 | 0 | 2.6 | 4.9 | 16.7 | 50.9 | 16.7 | 2.5/16.0 | 398 | 13.24 | 8552 | 20 |
| 1000000/500000/inj0 | 50200 | 50200 | 0 | 0 | 50.5 | 159.9 | 186.5 | 200.6 | 185.2 | 49.6/184.4 | 700 | 14.21 | 14331 | 253 |
| 1000000/500000/inj0 | 75200 | 49456 | 2 | 874364 | 385.3 | 437.2 | 528.9 | 696.8 | 506.1 | 390.1/507.1 | 714 | 15.05 | 13588 | 2494 |
| 1000000/50000/inj25 | 10200 | 10200 | 0 | 0 | 52.0 | 84.8 | 128.6 | 169.2 | 128.5 | 52.3/130.3 | 231 | 7.38 | 1996 | 328 |
| 1000000/50000/inj25 | 25200 | 25200 | 0 | 0 | 62.8 | 96.4 | 140.5 | 163.1 | 140.4 | 64.4/144.3 | 233 | 8.31 | 121 | 911 |
| 1000000/50000/inj25 | 50200 | 50200 | 0 | 0 | 86.3 | 134.5 | 203.6 | 228.9 | 203.3 | 90.0/209.4 | 432 | 9.61 | 29 | 2268 |
| 1000000/50000/inj25 | 75200 | 54374 | 3 | 617795 | 351.2 | 406.5 | 491.3 | 526.8 | 493.6 | 354.8/496.1 | 458 | 11.2 | 8 | 2858 |
| 1000000/500000/inj25 | 10200 | 10200 | 0 | 0 | 53.9 | 95.2 | 146.7 | 183.3 | 146.7 | 54.0/147.3 | 264 | 13.55 | 7488 | 329 |
| 1000000/500000/inj25 | 25200 | 25200 | 0 | 0 | 62.1 | 100.9 | 148.4 | 186.6 | 147.5 | 63.3/150.8 | 418 | 15.81 | 8577 | 893 |
| 1000000/500000/inj25 | 50200 | 50200 | 0 | 23472 | 114.5 | 214.3 | 274.9 | 301.1 | 270.1 | 121.5/274.2 | 725 | 17.78 | 14076 | 2431 |
| 1000000/500000/inj25 | 75200 | 47794 | 3 | 879522 | 408.8 | 443.9 | 478.5 | 507.4 | 458.5 | 413.4/473.1 | 701 | 17.24 | 13610 | 2914 |
| 10000000/50000/inj0 | 10200 | 10200 | 0 | 0 | 1.9 | 3.1 | 13.8 | 28.0 | 13.1 | 2.3/13.5 | 300 | 19.53 | 2000 | 8 |
| 10000000/50000/inj0 | 25200 | 25200 | 0 | 0 | 2.6 | 4.0 | 16.8 | 28.8 | 16.3 | 2.6/17.0 | 262 | 8.0 | 119 | 22 |
| 10000000/50000/inj0 | 50200 | 50200 | 0 | 0 | 4.2 | 6.6 | 18.9 | 32.1 | 18.7 | 3.9/19.0 | 456 | 8.3 | 28 | 62 |
| 10000000/50000/inj0 | 75200 | 75202 | 0 | 0 | 56.8 | 79.8 | 113.7 | 135.4 | 111.7 | 57.3/110.9 | 640 | 8.16 | 8 | 448 |
| 10000000/50000/inj0 | 100200 | 83776 | 3 | 449657 | 233.0 | 247.0 | 258.6 | 267.5 | 258.4 | 238.5/266.2 | 711 | 8.31 | 4 | 2777 |
| 10000000/500000/inj0 | 10200 | 10200 | 0 | 0 | 3.0 | 9.0 | 65.9 | 101.4 | 53.1 | 2.7/48.1 | 574 | 11.55 | 7479 | 11 |
| 10000000/500000/inj0 | 25200 | 25200 | 0 | 5281 | 20.7 | 693.8 | 1251.3 | 1386.5 | 348.4 | 23.4/312.8 | 659 | 14.39 | 8538 | 82 |
| 10000000/500000/inj0 | 50200 | 28190 | 1 | 664939 | 584.7 | 1051.6 | 1408.0 | 1649.7 | 873.5 | 413.2/819.7 | 664 | 12.28 | 8300 | 2440 |
| 10000000/50000/inj25 | 10200 | 10200 | 0 | 0 | 54.3 | 86.1 | 118.3 | 136.2 | 118.7 | 54.5/118.8 | 467 | 9.74 | 1994 | 358 |
| 10000000/50000/inj25 | 25200 | 25200 | 0 | 0 | 65.7 | 99.3 | 131.6 | 172.5 | 132.2 | 67.5/133.1 | 415 | 12.11 | 123 | 953 |
| 10000000/50000/inj25 | 50200 | 50200 | 0 | 0 | 103.2 | 199.8 | 251.6 | 284.2 | 251.4 | 108.0/255.0 | 434 | 12.16 | 30 | 2367 |
| 10000000/50000/inj25 | 75200 | 54162 | 1 | 623379 | 352.8 | 411.4 | 501.2 | 571.9 | 501.5 | 356.1/506.1 | 452 | 13.64 | 8 | 2858 |
| 10000000/500000/inj25 | 10200 | 10200 | 0 | 0 | 53.0 | 85.4 | 116.1 | 130.4 | 115.3 | 52.8/116.3 | 513 | 22.35 | 7492 | 328 |
| 10000000/500000/inj25 | 25200 | 24643 | 0 | 11137 | 63.4 | 108.2 | 2555.9 | 2764.8 | 2519.0 | 64.8/2316.3 | 522 | 27.07 | 8455 | 890 |

Shared 4 GiB cache (10M):

| Shape (total/active/inj) | Offered/s | Achieved/s | Err | Dropped | p50 ms | p90 | p99 | p99.9 | Hot p99 | FH lag p50/p99 | Srv CPU % | RSS GB | Loads/s | Commit/seg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10000000/50000/inj0 | 10200 | 10200 | 0 | 0 | 2.0 | 3.9 | 17.2 | 47.1 | 17.3 | 2.4/16.9 | 365 | 10.24 | 2000 | 8 |
| 10000000/50000/inj0 | 25200 | 25200 | 0 | 0 | 2.6 | 4.1 | 22.3 | 37.7 | 22.5 | 2.4/22.5 | 246 | 7.26 | 120 | 22 |
| 10000000/50000/inj0 | 50200 | 50200 | 0 | 0 | 4.2 | 7.2 | 23.5 | 31.2 | 23.2 | 3.9/23.5 | 464 | 7.69 | 28 | 61 |
| 10000000/50000/inj0 | 75200 | 75202 | 0 | 0 | 47.2 | 57.9 | 68.5 | 76.9 | 68.7 | 47.1/68.6 | 649 | 8.9 | 9 | 388 |
| 10000000/500000/inj0 | 10200 | 10200 | 0 | 0 | 2.8 | 6.9 | 252.9 | 313.1 | 38.9 | 2.6/37.3 | 739 | 9.38 | 7499 | 13 |
| 10000000/500000/inj0 | 25200 | 24422 | 4 | 39858 | 60.8 | 2379.8 | 2490.4 | 2521.1 | 79.7 | 46.8/66.0 | 863 | 8.61 | 8023 | 283 |
| 10000000/50000/inj25 | 10200 | 10200 | 0 | 0 | 53.2 | 85.8 | 126.7 | 174.3 | 127.0 | 53.5/129.2 | 308 | 4.85 | 1998 | 340 |
| 10000000/50000/inj25 | 25200 | 25200 | 0 | 0 | 62.6 | 104.3 | 154.2 | 181.5 | 153.5 | 64.3/158.2 | 272 | 5.3 | 122 | 909 |
| 10000000/50000/inj25 | 50200 | 50200 | 0 | 0 | 83.3 | 123.8 | 162.9 | 179.7 | 163.7 | 86.8/166.7 | 427 | 6.16 | 28 | 2198 |
| 10000000/50000/inj25 | 75200 | 54934 | 1 | 583641 | 345.9 | 410.9 | 467.7 | 485.4 | 466.7 | 350.0/472.6 | 454 | 7.17 | 7 | 2943 |
| 10000000/500000/inj25 | 10200 | 10200 | 0 | 0 | 80.0 | 377.3 | 473.1 | 512.3 | 175.7 | 69.9/168.3 | 768 | 6.97 | 7476 | 372 |
| 10000000/500000/inj25 | 25200 | 24620 | 0 | 29950 | 125.8 | 1932.3 | 2175.0 | 2256.9 | 188.2 | 100.2/180.0 | 870 | 7.58 | 8192 | 990 |

Shared 16 GiB cache (10M/500k):

| Shape (total/active/inj) | Offered/s | Achieved/s | Err | Dropped | p50 ms | p90 | p99 | p99.9 | Hot p99 | FH lag p50/p99 | Srv CPU % | RSS GB | Loads/s | Commit/seg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10000000/500000/inj0 | 10200 | 10200 | 0 | 0 | 2.5 | 8.7 | 109.6 | 152.8 | 85.7 | 2.4/32.3 | 516 | 25.96 | 7741 | 8 |
| 10000000/500000/inj0 | 25200 | 25200 | 0 | 0 | 2.9 | 4.7 | 24.7 | 45.2 | 23.5 | 2.8/22.7 | 601 | 19.97 | 9087 | 31 |
| 10000000/500000/inj0 | 50200 | 38878 | 1 | 338139 | 427.8 | 739.3 | 970.2 | 1186.8 | 634.4 | 303.9/512.8 | 734 | 20.59 | 11329 | 2560 |
| 10000000/500000/inj25 | 10200 | 10200 | 0 | 0 | 51.9 | 84.1 | 116.6 | 142.7 | 116.6 | 51.7/116.1 | 562 | 13.49 | 7745 | 338 |
| 10000000/500000/inj25 | 25200 | 25200 | 0 | 0 | 61.3 | 99.4 | 144.0 | 173.3 | 142.7 | 62.8/146.4 | 510 | 18.02 | 9081 | 883 |
| 10000000/500000/inj25 | 50200 | 44011 | 0 | 199596 | 374.3 | 611.3 | 763.9 | 876.0 | 511.5 | 267.0/394.2 | 775 | 20.66 | 12509 | 3221 |

32 MB segments (10k/5k, inj25):

| Shape (total/active/inj) | Offered/s | Achieved/s | Err | Dropped | p50 ms | p90 | p99 | p99.9 | Hot p99 | FH lag p50/p99 | Srv CPU % | RSS GB | Loads/s | Commit/seg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| seg32-inj25 | 25200 | 25200 | 0 | 0 | 61.3 | 95.0 | 132.0 | 148.9 | 131.6 | 63.2/136.1 | 204 | 1.78 | 0 | 906 |
| seg32-inj25 | 50200 | 50200 | 0 | 0 | 85.2 | 124.5 | 169.3 | 204.5 | 169.9 | 89.0/174.7 | 413 | 3.04 | 0 | 2253 |
| seg32-inj25 | 75200 | 75081 | 0 | 2423 | 140.4 | 188.8 | 245.4 | 277.8 | 246.1 | 151.2/258.7 | 572 | 4.52 | 0 | 5354 |
| seg32-inj25 | 100200 | 86182 | 0 | 418301 | 208.8 | 253.1 | 295.2 | 324.4 | 293.9 | 228.6/303.1 | 645 | 6.17 | 0 | 8896 |

### Hot repo (`bench.py hot 200,1000,5000,20000 0,25`, `hot.jsonl`)

Only writes to one repo, no fleet traffic. Commits are pipelined: a repo's
next commit doesn't wait for the previous one to be durable, so coalescing
only starts once the worker falls behind.

| Hot writes/s | inj | Achieved | p50 ms | p99 | p99.9 | Requests/commit | Commits/s |
|---|---|---|---|---|---|---|---|
| 200 | 0 | 200 | 3.8 | 10.6 | 21.6 | 1.01 | 199 |
| 1,000 | 0 | 1,000 | 1.2 | 11.9 | 25.0 | 1.08 | 926 |
| 5,000 | 0 | 5,000 | 1.6 | 20.3 | 39.9 | 2.37 | 2,114 |
| 20,000 | 0 | 20,000 | 1.9 | 22.5 | 30.8 | 3.62 | 5,522 |
| 200 | 25 | 200 | 51.4 | 116.5 | 145.3 | 1.01 | 199 |
| 1,000 | 25 | 1,000 | 50.9 | 121.1 | 156.2 | 1.23 | 810 |
| 5,000 | 25 | 5,000 | 53.7 | 124.9 | 147.7 | 3.32 | 1,508 |
| 20,000 | 25 | 20,000 | 53.6 | 131.7 | 159.6 | 4.15 | 4,824 |

A single repo sustains 20k writes/s at the same latency as 200/s.

### Fix: apply segment batches to shards concurrently (`src/nodelog.rs`)

The log finalizer awaited each touched shard's SlateDB write one after
another (a segment touches up to every owned shard), so one shard on memtable
backpressure stalled the rest. It now issues them concurrently
(`join_all`). Same 10k/5k inj0 stair, `grid-apply.jsonl`:

| Offered/s | Before: achieved, p50 / p99 ms, apply p50 | After: achieved, p50 / p99 ms, apply p50 |
|---|---|---|
| 50,200 | 50,200, 5.3 / 28.3, 0.8 ms | 50,200, 3.4 / 15.3, 0.2 ms |
| 75,200 | 75,202, 41.5 / 65.9, 5.1 ms | 75,202, 7.9 / 32.0, 0.9 ms |
| 100,200 | 88,188, 222.2 / 243.2, 12.7 ms | 94,633, 207.2 / 226.4, 6.0 ms |

The 75k/s point drops from p50 41.5 → 7.9 ms, and single-node saturation
(no injection) rises from ~88k to ~95k/s. The grid tables above were taken
before this fix; the cluster runs in section 2 include it.

## 2. Multi-node (3 native processes, shared MinIO prefix)

`bench.py cluster 1000000 50000 <inj> <rates>` with `FAILOVER_RATE=30000`.
Nodes: `--node-id n1..n3 --workers 3 --io-threads 3 --lease-ttl-ms 10000`, 256
shards (86/86/84 each). One `loadgen run` per node at rate/3 (not
DID-routed, so ~2/3 of writes are forwarded); the hot repo and the firehose
consumer go through n1. The bulk create goes to every node (each creates
the DIDs it owns). The harness waits until every node's routing table names
an owner for all 256 shards.

### Bug fixed: HTTP/1.1 node-to-node forwarding collapsed at 50k/s

The first run (`cluster-http1.jsonl`) did 25k/s fine (p99 130 ms). At 50k/s
the cluster collapsed to 1.8k/s achieved, with 140k errors (`owner did not
answer in time`, then `PartitionUnavailable: owner unreachable`) and CPU
*falling* to ~50% per node. The peer client was HTTP/1.1 with 256 pooled
connections per host. Forwarding ~11k writes/s per node at 50–100 ms each
needs ~1k concurrent requests per peer, so most requests opened and closed a
TCP connection; forwards blew the 3 s time-to-first-byte deadline and clients
piled up. Fix (`src/server.rs`): the peer client uses HTTP/2 prior knowledge
(the listener already serves h2c) with 4 MiB / 64 MiB windows. After the fix
(`cluster-h2-serialapply.jsonl`, `cluster.jsonl`) 50k/s runs at p99 143–150 ms
with 0 errors. Caveat: if peers ever sit behind a TLS terminator, it must
negotiate h2.

### Results (final binary: shared cache + concurrent apply + h2 peers)

| Shape | Offered/s | Achieved/s | Err | Dropped | worst p50 | worst p99 | worst p99.9 | FH lag p50/p99 (n1) | CPU % per node | Forwarded/s |
|---|---|---|---|---|---|---|---|---|---|---|
| 3n/1000000/50000/inj25 | 25200 | 25199 | 0 | 0 | 51.8 | 128.1 | 158.8 | 73.3/147.3 | 136 / 134 / 131 | 16854 |
| 3n/1000000/50000/inj25 | 50200 | 50201 | 0 | 0 | 59.2 | 150.1 | 204.7 | 85.8/183.4 | 216 / 198 / 196 | 33537 |
| 3n/1000000/50000/inj25 | 75200 | 69421 | 13815 | 136598 | 881.2 | 1534.0 | 1798.1 | 10371.1/12927.0 | 254 / 254 / 248 | 47154 |
| 3n/1000000/50000/inj25/failover | 30200 | 25152 | 302894 | 0 | 54.7 | 308.0 | 559.6 | 82.4/14409.7 | 152 / 143 / 0 | 2555 |
| 3n/1000000/50000/inj25/sigterm | 30200 | 27319 | 172890 | 0 | 56.9 | 1511.4 | 2648.1 | 79.4/827.9 | 167 / 0 / 162 | -4186 |
| 3n/1000000/50000/inj0 | 50200 | 50201 | 0 | 0 | 9.9 | 48.6 | 68.5 | 11.4/56.7 | 219 / 215 / 211 | 33544 |
| 3n/1000000/50000/inj0 | 75200 | 68416 | 40541 | 116100 | 889.9 | 1626.1 | 1925.1 | 7901.2/12255.2 | 257 / 253 / 250 | 47625 |

Saturation is ~68–70k/s aggregate, with or without injection, at only
~250% CPU per node: **3 nodes on one laptop do no better than 1 node**
(single node: ~50k/s inj25, ~95k/s inj0). Per node at saturation: ~23k
commits/s, 9–10 segs/s of ~6 MB, PUT p50 20 ms (inj0), apply p50 85–100 ms,
commit p50 550–720 ms. In a cluster each write costs more:
- ~2/3 of writes cross two nodes' HTTP stacks (no DID-aware load balancer);
- every node merges and frames the *whole* cluster firehose (67–70k ev/s
  per node, 3× its own commits);
- each node had only 3 IO threads, sharing HTTP, the merger, peer log
  following and SlateDB.

Not profiled. A real scale-out number needs separate machines and a
DID-routed balancer.

### Failover / rebalance at 30k/s (TTL 10 s)

kill -9 of n3 at t=20 s, restarted at t=35 s (same node id):
- Its shards (1/3 of writes from the surviving nodes' clients) returned 503
  for **~15 s**: n1/n2 fenced n3's log and took its 84 shards 14.9 s after
  the kill (TTL 10 s + 2 s skew margin + step interval), replaying 260
  segments in 0.7–1.1 s.
- The restarted n3 then got its fair share back: the survivors released at
  +17.5 s and n3 acquired at +20.1 s, so there was a second ~3 s gap
  between release and the joiner's next control-plane step.
- After that, steady state again: p50 55 ms, p99 120–140 ms.
- Clients pinned to n3 itself got connection refused while it was down
  (172k of the 303k errors).
- The p99 over the measured window was 308 ms; the outage shows up as errors
  (503 retryable), not latency.

SIGTERM of n2 at t=20 s, restarted at t≈32 s:
- Graceful release took 1.6 s.
- Peers acquired 1.7–2.4 s after the release.
- Handback after the restart again had a ~3–4 s release→acquire gap.
- About 25k of the 173k errors came from clients of n1/n3; the rest came from
  n2's own clients while it was down.

Improvement (not done): when the owner releases a shard for a known
joiner, nudge the joiner's control-plane step (or let the releaser CAS the
assignment straight to the joiner). That would close the ~3 s handback gap,
which hits every rebalance.


## 3. Per-XRPC-method throughput / latency

`bench.py methods 0` then `bench.py methods 25 <write methods>`. Each runs on a
fresh single node: `loadgen setup --accounts 2000 --records 100`, then
`loadgen methods --concurrency 64 --seconds 10`. This is a **closed loop**
(64 in flight), so throughput = 64 / latency: for writes under 25 ms
injection it measures latency, not capacity (capacity is in section 1).

Loadgen bugs fixed along the way (`src/bin/loadgen.rs`):
- `methods` sampled rkeys with `buffer_unordered`, so `rkeys[i]` belonged to
  another account. getRecord got 80% RecordNotFound; sync.getRecord
  "succeeded" by returning non-existence proofs.
- createAccount handles exceeded the 18-character first-label limit.

Those rows were re-run.

| Method | inj ms | ops/s | p50 ms | p90 | p99 | p99.9 | max | errors |
|---|---|---|---|---|---|---|---|---|
| describeServer | 0 | 86364 | 0.74 | 0.93 | 1.13 | 1.31 | 4.3 | 0 |
| listRecords | 0 | 48254 | 1.29 | 1.77 | 2.21 | 2.64 | 4.4 | 0 |
| describeRepo | 0 | 28457 | 2.10 | 3.06 | 5.09 | 15.13 | 32.7 | 0 |
| getLatestCommit | 0 | 75676 | 0.83 | 1.10 | 1.40 | 1.81 | 7.4 | 0 |
| getRepoStatus | 0 | 75361 | 0.83 | 1.11 | 1.40 | 1.64 | 3.9 | 0 |
| sync.getRepo | 0 | 34570 | 1.79 | 2.37 | 3.19 | 10.85 | 27.1 | 0 |
| listRepos | 0 | 2388 | 25.04 | 46.21 | 64.48 | 75.07 | 78.8 | 0 |
| createRecord | 0 | 32235 | 1.94 | 2.45 | 3.06 | 12.74 | 31.5 | 0 |
| putRecord | 0 | 33153 | 1.90 | 2.38 | 2.95 | 6.79 | 18.2 | 0 |
| deleteRecord | 0 | 57813 | 1.09 | 1.43 | 1.80 | 2.10 | 3.6 | 0 |
| applyWrites10 | 0 | 21252 | 2.79 | 3.19 | 8.97 | 22.82 | 27.8 | 0 |
| applyWrites200 | 0 | 2980 | 21.20 | 22.54 | 29.92 | 56.73 | 59.0 | 0 |
| uploadBlob64k | 0 | 2164 | 26.18 | 53.70 | 87.23 | 114.56 | 148.7 | 0 |
| uploadBlob1m | 0 | 1841 | 29.21 | 58.34 | 109.12 | 165.76 | 331.0 | 0 |
| getBlob64k | 0 | 22260 | 2.81 | 3.83 | 5.70 | 7.63 | 14.7 | 0 |
| createSession | 0 | 611 | 98.30 | 150.66 | 233.73 | 286.98 | 357.6 | 0 |
| getSession | 0 | 66489 | 0.94 | 1.27 | 1.63 | 2.17 | 8.2 | 0 |
| refreshSession | 0 | 31655 | 1.86 | 3.04 | 6.71 | 15.81 | 39.0 | 0 |
| getRecord | 0 | 70941 | 0.89 | 1.18 | 1.49 | 1.82 | 6.2 | 0 |
| sync.getRecord | 0 | 64879 | 0.98 | 1.27 | 1.57 | 1.99 | 5.8 | 0 |
| createAccount | 0 | 566 | 106.62 | 158.85 | 232.96 | 289.28 | 342.8 | 0 |
| createRecord | 25 | 992 | 60.19 | 90.56 | 139.90 | 171.01 | 171.1 | 0 |
| putRecord | 25 | 972 | 63.13 | 93.57 | 128.25 | 161.53 | 162.3 | 0 |
| deleteRecord | 25 | 53556 | 1.17 | 1.54 | 1.99 | 2.65 | 9.8 | 0 |
| applyWrites10 | 25 | 960 | 63.10 | 94.66 | 149.89 | 152.57 | 166.4 | 0 |
| applyWrites200 | 25 | 749 | 82.24 | 128.06 | 171.01 | 216.32 | 230.7 | 0 |
| uploadBlob64k | 25 | 2141 | 26.22 | 56.38 | 89.73 | 157.57 | 251.5 | 0 |
| createAccount | 25 | 378 | 164.86 | 220.29 | 264.19 | 294.91 | 303.9 | 0 |

Notes:
- Reads of small repos run at 48–86k/s on one node (getRecord 71k,
  getLatestCommit 76k, listRecords 48k, sync.getRepo of a 100-record repo
  35k/s) with p99 < 3.5 ms. The loadgen shares the machine.
- describeRepo (28k/s, p99 5 ms) is the slowest per-repo read. It builds
  the DID doc, checks handle → DID and scans the collection index.
- listRepos (limit 500) is 2.4k/s at p50 25 ms: a cluster-wide scatter-gather
  page over 256 shards.
- deleteRecord here deletes a nonexistent like (no commit), so it measures
  the request path only.
- createSession / createAccount are Argon2id-bound: ~600/s at 64 in flight
  (~20 ms of CPU each). That's the limit for logins per node; size login
  bursts accordingly.
- uploadBlob: 64 KiB ×2.1k/s (137 MB/s), 1 MiB ×1.8k/s (~1.9 GB/s into MinIO).
  getBlob 64 KiB: 22k/s.
- With 25 ms injection every commit-producing write is ~60 ms p50 / 130–150 ms
  p99 (two PUTs; see section 1). applyWrites with 200 creates: 82 ms p50.


## 4. Repo-size sweep (100 → 10M records, single repo)

`bench.py sweep 100,1000,10000,100000,1000000,10000000` runs `loadgen sweep`
on one server with no injection. For each size it:
- creates a fresh account;
- uploads one distinct blob per 100 records (up to 100k blobs);
- fills the repo with applyWrites (200 creates per call, 16 in flight) using
  deterministic time-ordered TID rkeys, with 1% of records embedding a blob;
- runs each read method closed-loop for 10 s at concurrency 16, with
  rkeys/CIDs sampled uniformly over the whole repo;
- times getRepo exports.

No separate bulk path was needed. The applyWrites pipeline filled 10M records
in 291 s; at 10M the fill was bounded by SlateDB backpressure on the single
shard (see notes). The loadgen `sweep` changes in this run:
- TID rkeys, so reads sample the whole repo instead of the newest 1000;
- blobs, so listBlobs has an index to page through;
- getBlocks10 (10 random record CIDs per call);
- listBlobsDeep (cursor at the median blob CID).

Cells are ops/s / p99 ms.

| Method | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
|---|---|---|---|---|---|---|
| getRecord | 68.1k / 0.41 | 59.1k / 0.47 | 65.7k / 0.43 | 51.9k / 0.52 | 37.0k / 0.77 | 39.9k / 0.74 |
| listRecords | 20.9k / 1.23 | 20.3k / 1.26 | 22.9k / 1.13 | 21.4k / 1.21 | 17.9k / 1.63 | 17.2k / 1.60 |
| listRecordsDeep | 31.6k / 0.87 | 30.2k / 0.90 | 24.3k / 1.01 | 29.6k / 0.86 | 23.2k / 1.07 | 24.8k / 1.04 |
| describeRepo | 27.4k / 1.00 | 22.7k / 1.22 | 23.6k / 1.13 | 26.6k / 1.10 | 17.5k / 1.48 | 10.7k / 2.42 |
| getLatestCommit | 63.8k / 0.42 | 62.7k / 0.41 | 62.2k / 0.41 | 63.4k / 0.42 | 61.2k / 0.45 | 60.3k / 0.45 |
| getRepoStatus | 63.4k / 0.41 | 62.4k / 0.41 | 62.9k / 0.42 | 62.7k / 0.41 | 60.4k / 0.43 | 59.9k / 0.49 |
| sync.getRecord | 54.8k / 0.47 | 53.5k / 0.49 | 53.1k / 0.49 | 52.9k / 0.49 | 50.6k / 0.52 | 49.3k / 0.60 |
| getBlocks10 | 25.8k / 1.04 | 23.6k / 1.16 | 15.2k / 1.81 | 2.6k / 12.88 | 236 / 153 | 22 / 1700 |
| listBlobs | 62.7k / 0.42 | 56.9k / 0.47 | 24.8k / 1.01 | 10.7k / 2.57 | 9.5k / 2.91 | 8.3k / 3.58 |
| listBlobsDeep | 62.2k / 0.41 | 56.7k / 0.46 | 38.8k / 0.69 | 11.1k / 2.62 | 9.5k / 3.10 | 8.4k / 3.61 |
| getBlocks10 before fix | 25.4k / 1.07 | 22.8k / 1.20 | 9.5k / 3.13 | 1.2k / 27.82 | 106 / 338 | – |
| fill (records/s) | 35k | 172k | 379k | 381k | 51k | 34k |
| getRepo export | 0.0 MB in 0 ms | 0.3 MB in 1 ms | 2.7 MB in 6 ms | 27.5 MB in 67 ms | 275.8 MB in 689 ms | 2768 MB in 6.7 s |
| server RSS max (GB) | 0.11 | 0.15 | 0.17 | 0.33 | 1.64 | 6.13 |

Findings:
- **Point reads are flat in repo size.** getRecord drops from 68k/s to 40k/s
  (p99 0.41 → 0.74 ms) between 100 and 10M records. sync.getRecord (with MST
  proof) goes 54k → 49k/s; getLatestCommit and getRepoStatus stay at ~60k/s.
- listRecords (100/page) holds 17–23k/s at every size. describeRepo falls to
  10.7k/s at 10M: it scans the collection index.
- listBlobs: 62k/s with 1 blob, 8.3k/s with 100k blobs (p99 3.6 ms). A
  500-entry page costs the same anywhere in the set.
- **getRepo streams at ~400 MB/s at any size**: 10M records = 2.77 GB in
  6.8 s, first byte immediately, and memory stays flat (server RSS 6.1 GB
  holding the 10M-record in-memory tree; 1.6 GB at 1M).
- **getBlocks was O(repo) and heavy. Partly fixed.** It ran `walk_blocks`
  (re-encoding every MST node) plus a full `walk` per request: 106/s at 1M
  records. New `mst::Tree::find_cids` does one pass, encodes only the wanted
  nodes and stops once everything is found: 2.2× faster (236/s at 1M; 22/s,
  p99 1.7 s, at 10M). It's still a full tree walk for random record CIDs.
  **Fix (not done): a CID → path index for records** (a SlateDB key or an
  in-memory map built at load), making getBlocks O(k log n).
- **Single-shard ingest stalls at 10M.** Filling one repo runs at
  46k records/s in bursts, then stops for 4–11 s on SlateDB memtable
  backpressure (`max_unflushed_bytes` 64 MiB; L0 flush/compaction of one shard
  can't keep up). Average: 34k records/s at 10M (51k at 1M, ~380k at 10k–100k).
  A whole repo lives in one shard, so this bounds a single huge repo's
  import/burst rate, not the fleet's. Tune per-shard flush/L0 settings or
  give hot shards more compaction.


## 5. Firehose

### End-to-end latency (commit → subscriber receipt)

Measured by the `--firehose` consumer in the grid and cluster runs: event
`time` (set when the commit is built, before durability) → receipt. An event
is emitted only after its segment is durable and applied, so this lag is
"time to ack" plus delivery.

| Setup | Writes/s | Write ack p50 / p99 ms | Firehose lag p50 / p99 ms |
|---|---|---|---|
| 1 node, 1M/50k, inj0 | 10k / 25k / 50k / 75k | 1.8/17 · 2.6/19 · 4.3/20 · 47/65 | 2.2/17.5 · 2.6/19 · 3.9/20 · 47/66 |
| 1 node, 1M/50k, inj25 | 10k / 25k / 50k | 52/129 · 63/141 · 86/204 | 52/130 · 64/144 · 90/209 |
| 3 nodes (n1's merged stream), inj25 | 25k / 50k | 52/128 · 59/150 | 73/147 · 86/183 |
| 3 nodes, inj0 | 50k | 9.9/49 | 11.4/57 |
| 3 nodes, kill -9 of n3 at 30k/s | | | p50 82, **p99 14.4 s** |
| 3 nodes, SIGTERM of n2 at 30k/s | | | p50 79, p99 828 |

- Single node: firehose delivery adds 0–4 ms over the ack.
- With 3 nodes the merged stream adds ~20 ms at p50: it emits only at or below
  the minimum watermark of all three logs.
- When a node is killed, the merged stream on every node **stalls until the
  dead log is fenced (~15 s at TTL 10 s)**, then drains. That's by design (it
  keeps order across failover). With a graceful stop the stall is under 1 s.

### Fan-out (`bench.py firehose 0`, `firehose.jsonl`)

One node with 100k bulk repos, a background write load at the given rate,
then `loadgen fanout --subscribers N --seconds 20` (8 client threads; each
subscriber decodes every 64th frame for lag). The subscribers, loadgen and
server share the laptop.

| Writes/s | Subscribers | Delivered ev/s (total) | MB/s | Per-sub min / max ev/s | Lag p50 / p99 ms | Write p99 ms | Server CPU % |
|---|---|---|---|---|---|---|---|
| 2k | 1 | 2.0k | 2.7 | 2000 / 2000 | 2.7 / 15 | 11.6 | 37 |
| 2k | 10 | 20k | 28 | 2000 / 2000 | 2.5 / 21 | 18.7 | 50 |
| 2k | 100 | 200k | 286 | 2000 / 2000 | 3.1 / 12 | 11.4 | 223 |
| 2k | 1000 | 588k | 861 | 0 / 1984 | 541 / 11,674 | **852** | 165 |
| 10k | 1 | 10k | 15 | 10000 / 10000 | 2.4 / 18 | 18.1 | 117 |
| 10k | 10 | 100k | 164 | 9999 / 9999 | 2.8 / 17 | 15.8 | 180 |
| 10k | 100 | 901k | 1,544 | 2908 / 9938 | 817 / 4,000 | **1,193** | 273 |
| 10k | 1000 | 1.01M | 1,787 | 8 / 9639 | 3,973 / 12,280 | **1,960** | 267 |
| 50k | 1 | 50k | 95 | 50007 | 3.3 / 19 | 20.1 | 473 |
| 50k | 10 | 495k | 1,009 | 49411 / 49555 | 139 / 1,155 | 428 | 645 |

- Fan-out is clean up to ~200k ev/s / ~300 MB/s total (100 subscribers at
  2k ev/s, today's Bluesky rate: lag p99 12 ms).
- Total egress tops out at ~1–1.8 GB/s, ~1M ev/s on this box (server,
  subscribers and writers share 14 cores and loopback).
- Past that, subscribers fall behind unevenly. One of 1000 got 0–8 ev/s,
  i.e. starved or dropped early (the harness can't tell which; every
  subscriber connected). The "min" column is the worst subscriber.
- **Fan-out load leaks into the write path:** write p99 rose from ~15 ms to
  0.85–2 s once egress passed ~0.5–1 GB/s. Subscribers' websocket sends run on
  the same Tokio runtime as HTTP and the log finalizer. This is the case for
  the separate firehose fan-out tier in DESIGN.md (or at least a dedicated
  runtime for subscriber I/O).

### Cursor backfill from S3

The server ran with `--firehose-ring-mb 64`, so `cursor=1` (the oldest)
starts in S3 segments. The log held 4.73M events (mostly the 100k-account
bulk plus ~2M commits).

| Subscribers | ev/s per subscriber | Total ev/s | MB/s total |
|---|---|---|---|
| 1 | 35.0k | 35.0k | 13.3 |
| 4 | 33.9k–35.2k | 138k | 50.8 |

Backfill scales with subscribers but is ~35k ev/s per subscriber: each
log's segments are fetched **one GET at a time** (`backfill.rs` `LogCursor::fill`,
no read-ahead). A subscriber that falls back to S3 while the live rate is above
~35k ev/s can never catch up. **Fix (not done, small): prefetch 4–8
segments ahead per log.** The early events were small bulk #identity/#account
frames (~380 B), so MB/s understates what commit-heavy segments would give.

## 6. Proxy fast path (`atproto-proxy` → stub AppView)

`bench.py proxy 50000,200000,1000000 128,512`:
- one node with `--appview http://127.0.0.1:2700,did:web:stub.test`;
- 1M bulk accounts;
- `loadgen stub-appview` (a new axum server returning a fixed 2 KiB JSON body);
- `loadgen proxy`: closed-loop `GET /xrpc/app.bsky.feed.getTimeline?limit=50`
  with self-minted access tokens for a window of `active` accounts, 64 h2
  connections, 15 s after a 3 s warmup.

All three processes share the laptop.

| Active accounts | In flight | Upstream | req/s | p50 ms | p99 | p99.9 | Server CPU % | Stub % | Client % | account miss / hit | JWT miss / hit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 50,000 | 128 | HTTP/1.1 | 81,618 | 1.55 | 2.59 | 3.14 | 542 | 167 | 288 | 355k / 2536k | 50k / 1395k |
| 50,000 | 512 | HTTP/1.1 | 90,684 | 5.38 | 12.46 | 15.34 | 538 | 170 | 296 | 354k / 2908k | 50k / 1581k |
| 200,000 | 128 | HTTP/1.1 | 81,119 | 1.54 | 2.79 | 3.31 | 554 | 157 | 278 | 816k / 2087k | 150k / 1301k |
| 200,000 | 512 | HTTP/1.1 | 85,720 | 5.61 | 13.78 | 16.96 | 556 | 154 | 283 | 835k / 2253k | 157k / 1387k |
| 1,000,000 | 128 | HTTP/1.1 | 72,585 | 1.71 | 3.19 | 5.42 | 571 | 131 | 244 | 1146k / 1465k | 614k / 691k |
| 1,000,000 | 512 | HTTP/1.1 | 72,778 | 6.61 | 16.10 | 22.03 | 562 | 122 | 238 | 1156k / 1483k | 336k / 983k |
| 50,000 | 128 | h2c | 71,095 | 1.77 | 2.97 | 3.43 | 485 | 108 | 234 | 343k / 2173k | 50k / 1208k |
| 50,000 | 512 | h2c | 74,976 | 6.78 | 9.83 | 11.60 | 491 | 108 | 248 | 339k / 2367k | 50k / 1303k |
| 1,000,000 | 128 | h2c | 62,933 | 1.98 | 3.41 | 4.15 | 517 | 98 | 204 | 1006k / 1246k | 642k / 484k |
| 1,000,000 | 512 | h2c | 68,415 | 7.33 | 12.37 | 20.19 | 522 | 99 | 225 | 1088k / 1377k | 313k / 919k |

**73–91k req/s on one node, vs the 200k–2M target.** p99 is 2.6–3.2 ms at
128 in flight. The server uses ~5.5 cores, i.e. **~65 µs of CPU per proxied
request**; the stub and client take another ~4.5 cores, so the 14-core box is
nearly saturated. A per-node number above ~100k/s needs the load generator on
other machines.

Profile (`sample`, 8 s at 76k req/s, 50k active):
- 46% of busy samples are socket syscalls (`writev`, `recvfrom`): a
  client-side h2 frame write plus an upstream HTTP/1.1 write and read per
  request;
- 9% jemalloc / memmove;
- 5% mutex waits;
- the rest is spread thin over hyper/h2/axum/reqwest (HPACK decode, URL
  parsing, header maps, tower layers).

Nothing in vlpds' own proxy code stands out. The fast-path caches work:
- JWT misses ≈ one per (account, method) per 30 s;
- account misses come from the 2 s ACCT_TTL (status freshness). At 1M
  active, ~45% of account lookups miss (a SlateDB read + JSON + key parse,
  ~40 µs each), costing ~11% of throughput vs 50k active.

Tried: h2c to the upstream (opt-in switch, reverted). One multiplexed
connection to the stub was **slower** (63–75k vs 73–91k req/s). A real https
AppView already negotiates h2 via ALPN. To reach 200k+/node:
- move the HTTP stack off the per-request syscall path (larger h2 write
  batching / `writev` coalescing; keep many upstream h1 connections or
  several h2 connections);
- cut per-request allocations (header maps, URL rebuild);
- run the proxy on the read/proxy tier with its own cores.

Not attempted further (45-minute budget).

## 7. Resource profile at 10M repos / 50k active (`bench.py resource`, `resource.json`)

Final binary (shared 4 GiB block + 1 GiB meta cache). Bulk-create 10M
accounts (173 s, 58k/s: slower than 77k earlier because the shared cache now
takes flush output), then 50k writes/s with 25 ms injection for 70 s.
Repeated with `--cache-per-worker 6250` (50k cached repos instead of 400k).

| Point | RSS GB | jemalloc allocated / active / resident GB | Cached repos | Firehose ring GB |
|---|---|---|---|---|
| cpw=50000: started | 0.07 | 0.112 / 0.13 / 0.141 | 0 | 0.0 |
| cpw=50000: after bulk | 11.9 | 9.554 / 10.539 / 11.943 | 400,000 | 0.536 |
| cpw=50000: idle | 10.45 | 8.399 / 9.706 / 10.493 | 400,000 | 0.536 |
| cpw=50000: after 70 s load | 9.75 | 7.65 / 9.165 / 9.778 | 400,000 | 0.532 |
| cpw=6250: started | 0.14 | 0.164 / 0.222 / 0.244 | 0 | 0.0 |
| cpw=6250: idle | 0.15 | 0.16 / 0.232 / 0.249 | 0 | 0.0 |
| cpw=6250: after 70 s load | 7.78 | 7.072 / 7.423 / 7.851 | 50,000 | 0.535 |

Load, cpw=50000: 50200/s achieved, p50 78.8 p99 186.0 p99.9 240.5 ms, server CPU avg 578.3% (max 901.6%), RSS max 11.74 GB.

Load, cpw=6250: 50200/s achieved, p50 84.1 p99 171.6 p99.9 192.4 ms, server CPU avg 518.6% (max 952.0%), RSS max 7.88 GB.

**Where the memory goes** (allocated 7.65 GB under load, final binary):

| Component | Size | Notes |
|---|---|---|
| Shared SST block + meta cache | ≤ 5 GiB | 4 GiB + 1 GiB; filled by reads *and* by flush output |
| Repo cache | ~0.6 GB | 400k cached 5-record repos at ~1.7 KB each (7.65 vs 7.07 GB going from 400k to 50k cached) |
| Firehose ring | 0.53 GB | `--firehose-ring-mb 512` default |
| SlateDB memtables | up to ~2 GB | estimated, not measured: 256 shards × 8 MB `l0_sst_size` active memtable, flushed by the 10 s checkpoint; `max_unflushed_bytes` 64 MiB × 256 allows 16 GiB under backpressure |

The earlier 11–27 GB at 10M repos was mostly the per-shard SlateDB caches
(256 × 640 MiB possible, fixed here), plus bulk-time memtables. Peak RSS
after the bulk was 11.9 GB, with 9.6 GB allocated; it drops to 9.8 GB under
load and 7.8 GB with the smaller repo cache.

Remaining levers:
- a node-wide memtable budget instead of per-shard `max_unflushed_bytes`;
- `--block-cache-mb` sized to the active window (16 GiB fixed the
  10M/500k tail in section 1);
- configurable ring sizes (TODO "Ops").

A jemalloc heap profile wasn't needed for this breakdown; it wasn't taken.


## Fixes made in this run (all tests green: `CARGO_TARGET_DIR=target/agent-tests cargo test`, 400 passed)

1. `src/partition.rs`, `src/main.rs`: one SST block/meta cache shared by every
   shard DB (`--block-cache-mb`, default 4096; meta gets +25%). Before, there
   was a private 512 + 128 MiB cache per shard (×256).
2. `src/nodelog.rs`: the finalizer applies a segment's per-shard batches
   concurrently.
3. `src/server.rs`: node-to-node client uses HTTP/2 prior knowledge with
   4/64 MiB windows. HTTP/1.1 connection churn collapsed the cluster at
   50k/s.
4. `src/mst.rs` (`Tree::find_cids`), `src/xrpc/sync.rs`: getBlocks makes one
   early-exit pass and encodes only the wanted nodes.
5. `src/bin/loadgen.rs`:
   - `methods`: rkeys sampled in account order; createAccount handles ≤ 18
     chars.
   - `sweep`: handle length; TID rkeys; blobs; getBlocks10 / listBlobsDeep.
   - New subcommands `stub-appview` and `proxy`.

   **Note: `src/bin/loadgen.rs` is gitignored** (the `.gitignore` rule
   `bin/`), so it is not tracked at all.

## Not fixed (bigger changes)

- Node log: allow 2–4 segment PUTs in flight (ordered finalize) and shrink
  segment bytes per commit. Throughput ceiling, section 1.
- getBlocks: CID → path index (O(k log n)).
- Backfill: per-log segment read-ahead (35k ev/s per subscriber now).
- Firehose fan-out on its own runtime or tier (it pushes write p99 to 1–2 s).
- Shard handback: the joiner acquires ~3 s after the release; nudge it or CAS
  straight to the joiner.
- Single-shard ingest: SlateDB memtable backpressure stalls (4–11 s) on a
  10M-record repo fill; a node-wide memtable budget.
- Proxy per-request CPU (~65 µs), dominated by the HTTP stack and syscalls.

## Not measured / caveats

- **All numbers come from one laptop.** The load generator, MinIO and every
  node share 14 cores, plus a macOS `BTLEServer` stuck at ~100% of one core.
  Multi-node and proxy ceilings are box-bound, not design-bound.
- **Grid tables predate the concurrent-apply fix.** The section 1 grid tables
  (`grid*.jsonl`) were taken before it; only the 10k/5k inj0 stair was rerun
  after it.
- **10M/500k ran into memory pressure on the original binary.** The laptop
  was swapping (7.3 GB of 8 GB swap in use).
- **S3 request counts come from vlpds' own metrics, not MinIO.** The JSON has
  segment PUTs and attempts, hedges and control-plane requests; SlateDB's own
  GET/PUT counts aren't exported (TODO "SlateDB/object-store metrics").
- **No jemalloc heap profile.** The memory breakdown is by elimination
  (cache-size and repo-cache A/B), and the memtable share is estimated.
- **Not measured:** containerized multi-node runs; e2e latency under backfill
  plus live load together; fan-out on multi-node merged streams.

## Disk state at the end

- Every `bench-*` prefix in MinIO is deleted; only the pre-existing
  `ha-final-s3-slow-all/` remains (not from this run).
- **MinIO did not purge its own `.minio.sys/tmp/.trash`:** 73 GB piled up
  after three deletes. `bench.py cleanup` now empties it.
- SlateDB cache dirs and scratch logs are deleted.
- `target/bench` is 1.6 GB. Free disk at the end: 360 GiB (363 GiB at the start).
