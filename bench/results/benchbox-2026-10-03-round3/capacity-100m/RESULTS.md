# vlpds capacity test: cap100m-r3

Driver: `bench/capacity/run.py` (`all --name cap100m-r3 --total 100000000 --nodes 4 --mode native --chunk 5000000 --settle-s 900 --active 500000 --rates 10000,25000,50000,75000,100000 --log-retention 3m --lease-ttl-ms 30000 --bulk-concurrency 4`). 4 native nodes on one box (ports 2700-2703), MinIO at http://127.0.0.1:9200, prefix `cap100m-r3`, binaries `/home/operator/vlpds-bench/bin/b252b08/release`.
Per node: `--workers 3 --io-threads 6 --block-cache-mb 3612 --repo-cache-mb 3612 --log-retention 3m --slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m` --meta-cache-mb 1536 (no SST disk cache).

## Population

100,000,000 accounts, records per repo `real/scale=128/knee=2/group=1`: 513,992,689 records (mean 5.31, p50 2, p90 5, p99 70, p99.9 460, max 4640; 10,495,814 empty repos). Real network: mean 455 (508 over repos with records), p50 10, p99 9,803, max 593,772.

| Accounts | Records | Time | Accounts/s | Records/s | S3 bytes settled | Bytes/account | Settle |
|---|---|---|---|---|---|---|---|
| 100,000,000 | 513,992,689 | 4732 s | 21,131 | 108,610 | 111.98 GB (assign 0.00, cluster 0.00, log 1.08, nodes 0.00, retain 0.00, state 110.89, writers 0.00) | 1120 | 127 s |

Per chunk: 61,289/s, 37,498/s, 36,005/s, 30,974/s, 21,656/s, 30,302/s, 23,628/s, 23,415/s, 23,100/s, 20,470/s, 22,708/s, 16,467/s, 14,059/s, 11,336/s, 10,552/s, 29,529/s, 20,818/s, 17,966/s, 18,789/s, 19,110/s

## Active window stairs (500,000 active of 100,000,000, churn 5000 repos/s)

Open-loop, latency from the scheduled send; one loadgen per node at rate/N (not DID-routed: (N-1)/N forwarded), +200/s hot repo and a firehose consumer on n1. Window 15 s warmup + 60 s measured.

| Offered/s | Achieved/s | Err (warmup incl.) | Dropped | p50 | p99 | p99.9 | FH lag p50/p99 | Loads/s | Commits/s | CPU % per node | RSS GB per node | S3 req/s (PUT/GET) | S3 in/out MB/s | Seg PUT/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10,200 | 12 | 257415 (291257) | 470880 | 38961.2 | 41844.7 | 41943.0 | 3416.1/6426.6 | 1 | 6 | 56 / 63 / 54 / 55 | 8.88 / 9.1 / 8.89 / 8.6 | 5599 (2/5596) | 0.0/46.0 | 1 |

Saturated at 10,000/s offered.

## Files

`populate.jsonl` (per chunk + summary), `steps.jsonl` (per step: loadgen results, 1 s windows, metric summary), `metrics.jsonl` (1 s scrape of every node + MinIO; prefix du every 30 s). Regenerate: `bench/capacity/run.py report <same flags>`.
