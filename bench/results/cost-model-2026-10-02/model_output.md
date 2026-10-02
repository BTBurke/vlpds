## coefficients
  poll_get_per_shard               3.2614
  poll_analytic_per_shard          3.2
  gc_ops_per_shard_meas            0.013329
  ctl_put_per_node                 0.49985
  ctl_list_per_node                1.0164
  ctl_get_per_shard                0.020007
  seg_put_latency_mean_s           0.033994
  seg_overhead_s                   0.0028071
  hedge_frac                       0.0094617
  t_flush_s                        0.072285
  flush_classA                     4.4151
  flush_classA_noingest            3.0558
  flush_classB                     9.0696
  load_classB                      1.2815
  retention_classA_per_node        0.049784
  retention_classB_per_node        0.0090471

## validation (requests/s: measured vs model)
| run | nodes | shards | commits/s | Class A meas | model | Class B meas | model |
|---|---|---|---|---|---|---|---|
| costmodel one/idle | 1 | 256 | 0 | 5.0 | 4.1 (-18%) | 840 | 840 (-0%) |
| costmodel one/avg | 1 | 256 | 345 | 67.2 | 71.2 (+6%) | 975 | 1011 (+4%) |
| costmodel one2/idle | 1 | 256 | 0 | 30.6 | 30.6 (-0%) | 917 | 918 (+0%) |
| costmodel one2/avg | 1 | 256 | 343 | 70.0 | 70.2 (+0%) | 1008 | 999 (-1%) |
| costmodel one2/peak | 1 | 256 | 437 | 73.3 | 70.2 (-4%) | 1039 | 1034 (-1%) |
| costmodel one2/burst | 1 | 256 | 894 | 72.9 | 70.2 (-4%) | 1067 | 1038 (-3%) |
| costmodel one2/avg2 | 1 | 256 | 342 | 67.4 | 70.2 (+4%) | 1017 | 1031 (+1%) |
| cost1024 s1024/idle | 1 | 1024 | 0 | 2.1 | 11.1 (+432%) | 3295 | 3347 (+2%) |
| cost1024 s1024b/idle | 1 | 1024 | 0 | 0.7 | 11.1 (+1493%) | 3274 | 3347 (+2%) |
| cost1024 s1024b/avg | 1 | 1024 | 343 | 85.2 | 92.4 (+8%) | 3472 | 3499 (+1%) |
| costmodel three/avg | 3 | 256 | 340 | 158.6 | 156.1 (-2%) | 1164 | 1046 (-10%) |

## breakdown: bluesky-today, 3 nodes, 256 shards (requests/s; $/mo S3 | GCS | R2)
| component | Class A /s | Class B /s | S3 $/mo | GCS $/mo | R2 $/mo |
|---|---|---|---|---|---|
| log segment PUTs (If-None-Match; incl. ~1% hedges) | 82.0 | 0 | $1,078 | $1,078 | $965 |
| log retention (LIST/report; DELETEs) | 0.2 | 0 | $2 | $2 | $0 |
| SlateDB polling, per shard (manifest/compactions probe GET + GC boundary GET) | 0.0 | 104 | $110 | $110 | $95 |
| SlateDB GC passes, per shard (LIST x5, boundary CAS) | 3.4 | 0 | $34 | $34 | $36 |
| checkpoint flush + compaction (SST/manifest/compactions PUTs, SST GETs) | 69.9 | 144 | $1,070 | $1,070 | $955 |
| cold repo loads (SST range GETs past the disk cache) | 0.0 | 45 | $47 | $47 | $39 |
| compaction bytes (2 MiB input GETs, <=256 MiB output PUTs) | 0.0 | 0 | $0 | $0 | $0 |
| control plane (lease CAS, LIST nodes/ + assign/, assignment + peer lease GETs) | 4.5 | 8 | $68 | $68 | $53 |
| storage: state (zstd SSTs, live) | 3,703 GB | | $85 | $69 | $56 |
| storage: SlateDB transient (replaced SSTs until checkpoint expiry + GC) | 926 GB | | $21 | $17 | $14 |
| storage: log, 72 h retention (zstd segments) | 234 GB | | $5 | $4 | $4 |
| **total** | | | **$2,521** | **$2,500** | **$2,244** |

## sensitivity: bluesky-today 3n/256s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults: manifest poll 10 s, compactor/worker 30 s) | 418 | 792 | $2,521 | $2,500 | $2,244 | +0 |
| previous defaults (manifest poll 1 s, compactor/worker 5 s) | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +768 |
| DB manifest poll 5 s | 418 | 930 | $2,576 | $2,555 | $2,294 | +55 |
| DB manifest poll 30 s | 418 | 701 | $2,485 | $2,463 | $2,211 | -37 |
| compactor + worker polls 60 s | 418 | 724 | $2,494 | $2,472 | $2,219 | -27 |
| checkpoint every 30 s | 317 | 584 | $1,929 | $1,908 | $1,711 | -592 |
| checkpoint every 60 s | 280 | 507 | $1,712 | $1,691 | $1,516 | -809 |
| segment linger 50 ms | 295 | 792 | $1,902 | $1,881 | $1,687 | -619 |
| segment linger 100 ms | 261 | 792 | $1,735 | $1,713 | $1,535 | -787 |
| segment linger 250 ms | 231 | 792 | $1,582 | $1,561 | $1,398 | -939 |
| K = 1 | 418 | 792 | $2,521 | $2,500 | $2,244 | +0 |
| K = 8 | 418 | 792 | $2,521 | $2,500 | $2,244 | +0 |
| segment cap 2 MiB | 418 | 792 | $2,521 | $2,500 | $2,244 | +0 |
| segment cap 32 MiB | 418 | 792 | $2,521 | $2,500 | $2,244 | +0 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 937 | 956 | $5,178 | $5,156 | $4,637 | +2,656 |
| GC interval 30 min | 414 | 792 | $2,499 | $2,477 | $2,217 | -22 |
| all tuned (latency trades): defaults + checkpoint 30 s, linger 100 ms | 159 | 584 | $1,143 | $1,121 | $1,003 | -1,378 |

## sensitivity: bluesky-today 8n/1024s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults: manifest poll 10 s, compactor/worker 30 s) | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | +0 |
| previous defaults (manifest poll 1 s, compactor/worker 5 s) | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +3,074 |
| DB manifest poll 5 s | 1,205 | 3,162 | $7,402 | $7,380 | $6,668 | +220 |
| DB manifest poll 30 s | 1,205 | 2,247 | $7,036 | $7,015 | $6,339 | -146 |
| compactor + worker polls 60 s | 1,205 | 2,338 | $7,072 | $7,051 | $6,372 | -110 |
| checkpoint every 30 s | 890 | 1,967 | $5,350 | $5,329 | $4,822 | -1,832 |
| checkpoint every 60 s | 759 | 1,697 | $4,586 | $4,565 | $4,134 | -2,596 |
| segment linger 50 ms | 942 | 2,613 | $5,866 | $5,844 | $5,285 | -1,317 |
| segment linger 100 ms | 854 | 2,613 | $5,427 | $5,406 | $4,889 | -1,755 |
| segment linger 250 ms | 773 | 2,613 | $5,021 | $5,000 | $4,524 | -2,161 |
| K = 1 | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | +0 |
| K = 8 | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | +0 |
| segment cap 2 MiB | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | +0 |
| segment cap 32 MiB | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | +0 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 1,915 | 3,393 | $11,045 | $11,024 | $9,949 | +3,863 |
| GC interval 30 min | 1,187 | 2,613 | $7,092 | $7,071 | $6,363 | -90 |
| all tuned (latency trades): defaults + checkpoint 30 s, linger 100 ms | 539 | 1,967 | $3,595 | $3,574 | $3,241 | -3,587 |

## sensitivity: sizing-100x 8n/1024s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults: manifest poll 10 s, compactor/worker 30 s) | 1,790 | 6,940 | $16,525 | $15,612 | $13,720 | +0 |
| previous defaults (manifest poll 1 s, compactor/worker 5 s) | 1,790 | 14,625 | $19,599 | $18,686 | $16,486 | +3,074 |
| DB manifest poll 5 s | 1,790 | 7,489 | $16,745 | $15,832 | $13,917 | +220 |
| DB manifest poll 30 s | 1,790 | 6,574 | $16,379 | $15,466 | $13,588 | -146 |
| compactor + worker polls 60 s | 1,790 | 6,665 | $16,415 | $15,502 | $13,621 | -110 |
| checkpoint every 30 s | 1,475 | 6,293 | $14,693 | $13,780 | $12,071 | -1,832 |
| checkpoint every 60 s | 1,344 | 6,024 | $13,929 | $13,016 | $11,384 | -2,596 |
| segment linger 50 ms | 1,790 | 6,940 | $16,525 | $15,612 | $13,720 | +0 |
| segment linger 100 ms | 1,790 | 6,940 | $16,525 | $15,612 | $13,720 | +0 |
| segment linger 250 ms | 1,790 | 6,940 | $16,525 | $15,612 | $13,720 | +0 |
| K = 1 | 1,279 | 6,940 | $13,972 | $13,059 | $11,420 | -2,553 |
| K = 8 | 2,877 | 6,940 | $21,963 | $21,050 | $18,619 | +5,438 |
| segment cap 2 MiB | 5,052 | 6,940 | $32,839 | $31,926 | $28,417 | +16,314 |
| segment cap 32 MiB | 1,279 | 6,940 | $13,972 | $13,059 | $11,420 | -2,553 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 3,493 | 7,720 | $25,356 | $24,443 | $21,673 | +8,831 |
| GC interval 30 min | 1,772 | 6,940 | $16,435 | $15,522 | $13,612 | -90 |
| all tuned (latency trades): defaults + checkpoint 30 s, linger 100 ms | 1,475 | 6,293 | $14,693 | $13,780 | $12,071 | -1,832 |

## bluesky-today: 28.9 M record ops/day as commits (334/s avg, ~420/s peak hour), 56 M bsky-hosted repos, 23.9 B records
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 792 | 4,863 | $2,521 (ops $2,409) | $2,500 (ops $2,409) | $2,244 (ops $2,171) |
| 3 | 1024 | 606 | 1,983 | 4,863 | $3,934 (ops $3,822) | $3,912 (ops $3,822) | $3,546 (ops $3,473) |
| 8 | 256 | 787 | 976 | 4,863 | $4,439 (ops $4,327) | $4,418 (ops $4,327) | $3,971 (ops $3,899) |
| 8 | 1024 | 1,205 | 2,613 | 4,863 | $7,182 (ops $7,070) | $7,161 (ops $7,070) | $6,471 (ops $6,398) |
| 16 | 256 | 1,059 | 1,270 | 4,863 | $5,913 (ops $5,802) | $5,892 (ops $5,802) | $5,299 (ops $5,227) |
| 16 | 1024 | 1,668 | 3,256 | 4,863 | $9,755 (ops $9,643) | $9,734 (ops $9,643) | $8,787 (ops $8,714) |

## bluesky-today-90M: same, every PLC DID (89.9 M) as a repo
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 792 | 4,877 | $2,521 (ops $2,409) | $2,500 (ops $2,409) | $2,244 (ops $2,171) |
| 3 | 1024 | 606 | 1,983 | 4,877 | $3,934 (ops $3,822) | $3,913 (ops $3,822) | $3,546 (ops $3,473) |
| 8 | 256 | 787 | 976 | 4,877 | $4,439 (ops $4,327) | $4,418 (ops $4,327) | $3,972 (ops $3,899) |
| 8 | 1024 | 1,205 | 2,613 | 4,877 | $7,182 (ops $7,070) | $7,161 (ops $7,070) | $6,471 (ops $6,398) |
| 16 | 256 | 1,059 | 1,270 | 4,877 | $5,914 (ops $5,802) | $5,892 (ops $5,802) | $5,300 (ops $5,227) |
| 16 | 1024 | 1,668 | 3,256 | 4,877 | $9,755 (ops $9,643) | $9,734 (ops $9,643) | $8,787 (ops $8,714) |

## sizing-today: DESIGN sizing baseline: 50 M repos, 25 B records, 2,000/s daily peak (~1,600/s avg)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 419 | 880 | 5,959 | $2,586 (ops $2,449) | $2,560 (ops $2,449) | $2,296 (ops $2,207) |
| 3 | 1024 | 607 | 2,070 | 5,959 | $3,998 (ops $3,861) | $3,972 (ops $3,861) | $3,598 (ops $3,508) |
| 8 | 256 | 858 | 1,064 | 5,959 | $4,853 (ops $4,716) | $4,827 (ops $4,716) | $4,338 (ops $4,249) |
| 8 | 1024 | 1,276 | 2,700 | 5,959 | $7,597 (ops $7,460) | $7,571 (ops $7,460) | $6,838 (ops $6,748) |
| 16 | 256 | 1,485 | 1,357 | 5,959 | $8,107 (ops $7,970) | $8,081 (ops $7,970) | $7,269 (ops $7,180) |
| 16 | 1024 | 2,095 | 3,344 | 5,959 | $11,949 (ops $11,812) | $11,923 (ops $11,812) | $10,757 (ops $10,668) |

## sizing-100x: 100x writes (200k/s peak, ~160k/s avg), 20x accounts (1 B) and records (500 B)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 1,294 | 5,119 | 208,753 | $13,317 (ops $8,516) | $12,404 (ops $8,516) | $10,802 (ops $7,671) |
| 3 | 1024 | 1,481 | 6,310 | 208,753 | $14,730 (ops $9,929) | $13,817 (ops $9,929) | $12,104 (ops $8,973) |
| 8 | 256 | 1,372 | 5,303 | 208,753 | $13,782 (ops $8,980) | $12,869 (ops $8,980) | $11,220 (ops $8,089) |
| 8 | 1024 | 1,790 | 6,940 | 208,753 | $16,525 (ops $11,724) | $15,612 (ops $11,724) | $13,720 (ops $10,589) |
| 16 | 256 | 1,496 | 5,597 | 208,753 | $14,522 (ops $9,720) | $13,609 (ops $9,720) | $11,887 (ops $8,756) |
| 16 | 1024 | 2,106 | 7,583 | 208,753 | $18,363 (ops $13,562) | $17,450 (ops $13,562) | $15,375 (ops $12,243) |
