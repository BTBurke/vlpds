# vlpds cross-host bench: xh-smoke

Driver `bench/xhost/xhost.py smoke --name xh-smoke --node devhost:10.0.0.46:7100 --node devhost:10.0.0.46:7101 --minio devhost:10.0.0.46:9300 --loadgen devhost:10.0.0.46 --total 3000 --active 2000 --rates 500 --duration 30 --warmup 5 --min-free devhost=60 --cap-gb devhost=5 --mem-frac 0.1`, commit 43ed85f. MinIO http://10.0.0.46:9300 on devhost; loadgens on devhost; inject 0 ms per PUT.

| Node | Host | Listen / advertise | Flags |
|---|---|---|---|
| n1 | devhost (16 cpus) | http://10.0.0.46:7100 | --workers 2 --io-threads 4 --block-cache-mb 1208 --repo-cache-mb 1208 |
| n2 | devhost (16 cpus) | http://10.0.0.46:7101 | --workers 2 --io-threads 4 --block-cache-mb 1208 --repo-cache-mb 1208 |

Preflight: devhost: 16 cpus, 124 GB free, clock 0.0 ms (rtt 0.0 ms)

## Stairs

| Offered/s | Achieved/s | Err | p50 | p99 | p99.9 | Fwd frac | FH lag p99 | Commits/s per node | Node cores (worker/io/fh) | Host cores busy (other) | NIC rx/tx MB/s | Peer fwd tx / stream rx MB/s | S3 req/s | FH completeness min |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 700 | 700 | 0 | 7.4 | 20.2 | 32.2 | 0.355 | 23.9 | n1 440 / n2 250 | n1 0.59 (0.06/0.0/0.04); n2 0.47 (0.03/0.0/0.02) | devhost 5.11 (2.52) | devhost 0.2/0.49 | n1 0.07/0.36; n2 0.06/1.05 | 1659 | n1 1.0 n2 1.0 |
| 700 | 700 | 0 | 7.1 | 156.2 | 185.5 | 0.352 | 171.3 | n1 443 / n2 246 | n1 0.59 (0.06/0.49/0.04); n2 0.46 (0.03/0.41/0.02) | devhost 5.71 (3.17) | devhost 0.45/0.51 | n1 0.06/0.44; n2 0.07/1.18 | 1619 | n1 1.0 n2 1.0 |

## kill9-n2 at 300/s (n2 on devhost)

Signal at 15.0 s (loadgen clock), exited 15.21 s, survivors owned all shards > kill-down s after the signal, restarted 30.4 s, serving 31.72 s, rejoin converged in 9.2 s. Error window: 2,200 errors over 16 s (16..31 s); first error: ` createRecord 503 Service Unavailable: {"error":"PartitionUnavailable","message":"owner unreachable: error sending request for url (http://10.0.0.46:7101/xrpc/c`. Achieved 451/s, p99<= 1885.2 ms.

| t (s) | ok/s | errors/s | p99 ms |
|---|---|---|---|
| 12 | 499 | 0 | 13.9 |
| 13 | 500 | 0 | 8.5 |
| 14 | 501 | 0 | 14.0 |
| 15 | 499 | 0 | 19.9 |
| 16 | 358 | 143 | 10.9 |
| 17 | 361 | 139 | 9.1 |
| 18 | 357 | 143 | 7.9 |
| 19 | 354 | 146 | 16.0 |
| 20 | 350 | 150 | 9.0 |
| 21 | 351 | 149 | 16.5 |
| 22 | 337 | 164 | 9.8 |
| 23 | 359 | 139 | 9.1 |
| 24 | 345 | 155 | 15.3 |
| 25 | 351 | 149 | 8.3 |
| 26 | 361 | 140 | 9.7 |
| 27 | 372 | 127 | 11.6 |
| 28 | 354 | 146 | 10.3 |
| 29 | 353 | 148 | 14.8 |
| 30 | 361 | 139 | 12.7 |
| 31 | 340 | 23 | 43.2 |
| 32 | 370 | 0 | 121.7 |
| 33 | 347 | 0 | 15.8 |
| 34 | 919 | 0 | 3242.0 |
| 35 | 499 | 0 | 16.1 |
| 36 | 501 | 0 | 9.2 |
| 37 | 500 | 0 | 19.3 |
| 38 | 500 | 0 | 13.7 |
| 39 | 499 | 0 | 17.2 |
| 40 | 191 | 0 | 68.6 |
| 41 | 810 | 0 | 1823.7 |

## sigterm-n2 at 300/s (n2 on devhost)

Signal at 15.0 s (loadgen clock), exited 16.66 s, survivors owned all shards 1.67 s after the signal, restarted 31.67 s, serving 32.2 s, rejoin converged in 7.2 s. Error window: 472 errors over 1 s (17..17 s); first error: ` createRecord 503 Service Unavailable: {"error":"PartitionUnavailable","message":"owner unreachable: error sending request for url (http://10.0.0.46:7101/xrpc/c`. Achieved 490/s, p99<= 1509.4 ms.

| t (s) | ok/s | errors/s | p99 ms |
|---|---|---|---|
| 12 | 501 | 0 | 10.0 |
| 13 | 499 | 0 | 14.3 |
| 14 | 500 | 0 | 12.6 |
| 15 | 499 | 0 | 9.9 |
| 16 | 150 | 0 | 50.8 |
| 17 | 378 | 472 | 1560.6 |
| 18 | 500 | 0 | 15.2 |
| 19 | 500 | 0 | 15.0 |
| 20 | 501 | 0 | 13.0 |
| 21 | 500 | 0 | 9.6 |
| 22 | 500 | 0 | 14.3 |
| 23 | 500 | 0 | 16.5 |
| 24 | 500 | 0 | 15.7 |
| 25 | 500 | 0 | 11.3 |
| 26 | 500 | 0 | 26.5 |
| 27 | 500 | 0 | 9.9 |
| 28 | 500 | 0 | 16.1 |
| 29 | 500 | 0 | 15.9 |
| 30 | 500 | 0 | 8.9 |
| 31 | 500 | 0 | 10.7 |
| 32 | 500 | 0 | 19.9 |
| 33 | 499 | 0 | 19.9 |
| 34 | 500 | 0 | 15.5 |
| 35 | 500 | 0 | 12.0 |
| 36 | 501 | 0 | 9.5 |
| 37 | 425 | 0 | 30.7 |
| 38 | 328 | 0 | 211.2 |
| 39 | 408 | 0 | 333.1 |
| 40 | 839 | 0 | 2953.2 |
| 41 | 500 | 0 | 9.2 |
| 42 | 501 | 0 | 10.2 |

## Firehose at 250/s, 2 subscribers per node

15,748 commits cluster-wide in the window.

| Node | Min events/sub | Completeness (min sub) | Emitted/commits | Lag p50 / p99 / max ms | Out-of-order (sampled) |
|---|---|---|---|---|---|
| n1 | 15748 | 1.0 | 1.0 | 8.327 / 20.191 / 29.279 | 0 |
| n2 | 15748 | 1.0 | 1.0 | 9.223 / 22.543 / 26.111 | 0 |

## Files

`steps.jsonl` (per step: loadgen results, 1 s windows, per-node metric rates `nodes_m`, per-host CPU/NIC/socket rates `hosts`, firehose completeness), `metrics.jsonl` (node + MinIO scrape), `hosts.jsonl` (probe samples), `lg/` (loadgen stderr), `profiles/`.
