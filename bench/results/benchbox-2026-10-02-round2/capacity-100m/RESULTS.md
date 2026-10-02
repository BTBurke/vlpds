# vlpds capacity test: cap100m-r2

Driver: `bench/capacity/run.py` (`all --name cap100m-r2 --total 100000000 --nodes 4 --mode native --chunk 5000000 --settle-s 900 --active 500000 --rates 10000,25000,50000,75000,100000 --log-retention 3m --lease-ttl-ms 30000 --bulk-concurrency 4`). 4 native nodes on one box (ports 2700-2703), MinIO at http://127.0.0.1:9200, prefix `cap100m-r2`, binaries `/home/operator/vlpds-bench/target-prof/release`.
Per node: `--workers 3 --io-threads 6 --block-cache-mb 3612 --repo-cache-mb 3612 --log-retention 3m --slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m` --pyroscope-url http://127.0.0.1:4100 (no SST disk cache).

## Population

100,000,000 accounts, records per repo `real/scale=128/knee=2/group=1`: 520,313,332 records (mean 5.31, p50 2, p90 5, p99 70, p99.9 460, max 4640; 10,495,814 empty repos). Real network: mean 455 (508 over repos with records), p50 10, p99 9,803, max 593,772.

| Accounts | Records | Time | Accounts/s | Records/s | S3 bytes settled | Bytes/account | Settle |
|---|---|---|---|---|---|---|---|
| 100,000,000 | 520,313,332 | 8007 s | 12,489 | 64,983 | 103.84 GB (assign 0.00, cluster 0.00, log 0.30, nodes 0.00, retain 0.00, state 103.54, writers 0.00) | 1038 | 61 s |

Per chunk: 78,294/s, 49,630/s, 42,774/s, 36,254/s, 27,403/s, 31,235/s, 28,086/s, 26,520/s, 23,305/s, 24,871/s, 24,964/s, 24,048/s, 20,534/s, 18,844/s, 16,943/s, 9,150/s, 6,390/s, 5,473/s, 3,038/s, 3,164/s

## Active window stairs (500,000 active of 100,000,000, churn 5000 repos/s)

Open-loop, latency from the scheduled send; one loadgen per node at rate/N (not DID-routed: (N-1)/N forwarded), +200/s hot repo and a firehose consumer on n1. Window 15 s warmup + 60 s measured.

| Offered/s | Achieved/s | Err (warmup incl.) | Dropped | p50 | p99 | p99.9 | FH lag p50/p99 | Loads/s | Commits/s | CPU % per node | RSS GB per node | S3 req/s (PUT/GET) | S3 in/out MB/s | Seg PUT/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10,200 | 0 | 268552 (356579) | 408413 | 0 | 0 | 0 | 20561.9/22544.4 | 0 | 0 | 143 / 112 / 129 / 123 | 10.05 / 8.87 / 9.61 / 9.12 | 0 (0/0) | 0.0/0.0 | 0 |

Saturated at 10,000/s offered.

## kill -9 of n4 at 6,000/s

Killed at 35.0 s (from loadgen start), restarted at 55.0 s, serving at 55.4 s; survivors owned every shard > 20 s after the kill; load entered through the survivors only; rejoin converged in None s. Measured window: achieved 0/s of 6,200, 651,000 errors, p99<= 0 ms. Seconds below 50% of offered: 105 (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30).

| t (s) | ok/s | errors/s | p99 ms |
|---|---|---|---|
| 30 | 0 | 6,206 | 0.0 |
| 31 | 0 | 6,199 | 0.0 |
| 32 | 0 | 6,198 | 0.0 |
| 33 | 0 | 6,202 | 0.0 |
| 34 | 0 | 6,201 | 0.0 |
| 35 | 0 | 6,197 | 0.0 |
| 36 | 0 | 6,201 | 0.0 |
| 37 | 0 | 6,201 | 0.0 |
| 38 | 0 | 6,198 | 0.0 |
| 39 | 0 | 6,200 | 0.0 |
| 40 | 0 | 6,200 | 0.0 |
| 41 | 0 | 6,201 | 0.0 |
| 42 | 0 | 6,199 | 0.0 |
| 43 | 0 | 6,201 | 0.0 |
| 44 | 0 | 6,200 | 0.0 |
| 45 | 0 | 6,196 | 0.0 |
| 46 | 0 | 6,203 | 0.0 |
| 47 | 0 | 6,200 | 0.0 |
| 48 | 0 | 6,199 | 0.0 |
| 49 | 0 | 6,202 | 0.0 |
| 50 | 0 | 6,198 | 0.0 |
| 51 | 0 | 6,202 | 0.0 |
| 52 | 0 | 6,199 | 0.0 |
| 53 | 0 | 6,201 | 0.0 |
| 54 | 0 | 6,204 | 0.0 |
| 55 | 0 | 6,191 | 0.0 |
| 56 | 0 | 6,205 | 0.0 |
| 57 | 0 | 6,199 | 0.0 |
| 58 | 0 | 6,199 | 0.0 |
| 59 | 0 | 6,200 | 0.0 |
| 60 | 0 | 6,198 | 0.0 |
| 61 | 0 | 6,200 | 0.0 |
| 62 | 0 | 6,203 | 0.0 |
| 63 | 0 | 6,203 | 0.0 |
| 64 | 0 | 6,197 | 0.0 |
| 65 | 0 | 6,203 | 0.0 |
| 66 | 0 | 6,196 | 0.0 |
| 67 | 0 | 6,202 | 0.0 |
| 68 | 0 | 6,201 | 0.0 |
| 69 | 0 | 6,201 | 0.0 |
| 70 | 0 | 6,198 | 0.0 |

## Files

`populate.jsonl` (per chunk + summary), `steps.jsonl` (per step: loadgen results, 1 s windows, metric summary), `metrics.jsonl` (1 s scrape of every node + MinIO; prefix du every 30 s). Regenerate: `bench/capacity/run.py report <same flags>`.
