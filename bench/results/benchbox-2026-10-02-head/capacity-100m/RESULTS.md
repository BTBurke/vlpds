# vlpds capacity test: cap-100000000-4n

Driver: `bench/capacity/run.py` (`all --total 100000000 --nodes 4 --mode native --chunk 5000000 --settle-s 900 --active 500000 --rates 10000,25000,50000,75000,100000 --duration 60 --warmup 30 --kill-down 30 --log-retention 3m --lease-ttl-ms 30000 --bulk-concurrency 2`). 4 native nodes on one box (ports 2700-2703), MinIO at http://127.0.0.1:9200, prefix `cap-100000000-4n`, binaries `/home/operator/vlpds-bench/target/release`.
Per node: `--workers 3 --io-threads 6 --block-cache-mb 3612 --repo-cache-mb 3612 --log-retention 3m --slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m`  (no SST disk cache).

## Population

Per chunk: 106,768/s, 266,729/s, 103,542/s, 90,312/s, 64,525/s, 93,682/s, 60,111/s, 55,048/s, 41,359/s, 46,346/s, 40,479/s, 130,740/s, 39,216/s, 23,576/s, 26,816/s, 24,258/s, 24,241/s

## Files

`populate.jsonl` (per chunk + summary), `steps.jsonl` (per step: loadgen results, 1 s windows, metric summary), `metrics.jsonl` (1 s scrape of every node + MinIO; prefix du every 30 s). Regenerate: `bench/capacity/run.py report <same flags>`.
