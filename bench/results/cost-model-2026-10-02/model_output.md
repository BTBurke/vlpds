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
| SlateDB polling, per shard (manifest/compactions probe GET + GC boundary GET) | 0.0 | 835 | $878 | $878 | $787 |
| SlateDB GC passes, per shard (LIST x5, boundary CAS) | 3.4 | 0 | $34 | $34 | $36 |
| checkpoint flush + compaction (SST/manifest/compactions PUTs, SST GETs) | 69.9 | 144 | $1,070 | $1,070 | $955 |
| cold repo loads (SST range GETs past the disk cache) | 0.0 | 45 | $47 | $47 | $39 |
| compaction bytes (2 MiB input GETs, <=256 MiB output PUTs) | 0.0 | 0 | $0 | $0 | $0 |
| control plane (lease CAS, LIST nodes/ + assign/, assignment + peer lease GETs) | 4.5 | 8 | $68 | $68 | $53 |
| storage: state (zstd SSTs, live) | 3,703 GB | | $85 | $69 | $56 |
| storage: SlateDB transient (replaced SSTs until checkpoint expiry + GC) | 926 GB | | $21 | $17 | $14 |
| storage: log, 72 h retention (zstd segments) | 234 GB | | $5 | $4 | $4 |
| **total** | | | **$3,290** | **$3,268** | **$2,936** |

## sensitivity: bluesky-today 3n/256s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults) | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +0 |
| DB manifest poll 5 s | 418 | 1,616 | $2,850 | $2,829 | $2,541 | -439 |
| DB manifest poll 10 s | 418 | 1,478 | $2,796 | $2,774 | $2,491 | -494 |
| DB manifest poll 30 s | 418 | 1,387 | $2,759 | $2,738 | $2,458 | -531 |
| compactor + worker polls 30 s | 418 | 2,027 | $3,015 | $2,994 | $2,689 | -274 |
| manifest 10 s + compactor/worker 30 s | 418 | 792 | $2,521 | $2,500 | $2,244 | -768 |
| checkpoint every 30 s | 317 | 2,505 | $2,698 | $2,677 | $2,403 | -592 |
| checkpoint every 60 s | 280 | 2,428 | $2,481 | $2,460 | $2,208 | -809 |
| segment linger 50 ms | 295 | 2,714 | $2,671 | $2,649 | $2,378 | -619 |
| segment linger 100 ms | 261 | 2,714 | $2,503 | $2,482 | $2,227 | -787 |
| segment linger 250 ms | 231 | 2,714 | $2,351 | $2,330 | $2,090 | -939 |
| K = 1 | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +0 |
| K = 8 | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +0 |
| segment cap 2 MiB | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +0 |
| segment cap 32 MiB | 418 | 2,714 | $3,290 | $3,268 | $2,936 | +0 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 937 | 2,877 | $5,946 | $5,925 | $5,329 | +2,656 |
| GC interval 30 min | 414 | 2,714 | $3,267 | $3,246 | $2,909 | -22 |
| all tuned: manifest 10 s, polls 30 s, checkpoint 30 s, linger 100 ms | 159 | 584 | $1,143 | $1,121 | $1,003 | -2,147 |

## sensitivity: bluesky-today 8n/1024s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults) | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +0 |
| DB manifest poll 5 s | 1,205 | 5,906 | $8,500 | $8,478 | $7,656 | -1,757 |
| DB manifest poll 10 s | 1,205 | 5,358 | $8,280 | $8,259 | $7,459 | -1,976 |
| DB manifest poll 30 s | 1,205 | 4,992 | $8,134 | $8,112 | $7,327 | -2,122 |
| compactor + worker polls 30 s | 1,205 | 7,553 | $9,158 | $9,137 | $8,249 | -1,098 |
| manifest 10 s + compactor/worker 30 s | 1,205 | 2,613 | $7,182 | $7,161 | $6,471 | -3,074 |
| checkpoint every 30 s | 890 | 9,651 | $8,424 | $8,403 | $7,589 | -1,832 |
| checkpoint every 60 s | 759 | 9,382 | $7,660 | $7,639 | $6,901 | -2,596 |
| segment linger 50 ms | 942 | 10,298 | $8,940 | $8,918 | $8,051 | -1,317 |
| segment linger 100 ms | 854 | 10,298 | $8,501 | $8,480 | $7,656 | -1,755 |
| segment linger 250 ms | 773 | 10,298 | $8,095 | $8,074 | $7,291 | -2,161 |
| K = 1 | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +0 |
| K = 8 | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +0 |
| segment cap 2 MiB | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +0 |
| segment cap 32 MiB | 1,205 | 10,298 | $10,256 | $10,235 | $9,237 | +0 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 1,915 | 11,078 | $14,119 | $14,098 | $12,715 | +3,863 |
| GC interval 30 min | 1,187 | 10,298 | $10,166 | $10,145 | $9,129 | -90 |
| all tuned: manifest 10 s, polls 30 s, checkpoint 30 s, linger 100 ms | 539 | 1,967 | $3,595 | $3,574 | $3,241 | -6,661 |

## sensitivity: sizing-100x 8n/1024s ($/mo)
| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |
|---|---|---|---|---|---|---|
| baseline (defaults) | 1,790 | 14,625 | $19,599 | $18,686 | $16,486 | +0 |
| DB manifest poll 5 s | 1,790 | 10,233 | $17,842 | $16,929 | $14,905 | -1,757 |
| DB manifest poll 10 s | 1,790 | 9,684 | $17,623 | $16,710 | $14,708 | -1,976 |
| DB manifest poll 30 s | 1,790 | 9,318 | $17,476 | $16,563 | $14,576 | -2,122 |
| compactor + worker polls 30 s | 1,790 | 11,880 | $18,501 | $17,588 | $15,498 | -1,098 |
| manifest 10 s + compactor/worker 30 s | 1,790 | 6,940 | $16,525 | $15,612 | $13,720 | -3,074 |
| checkpoint every 30 s | 1,475 | 13,978 | $17,767 | $16,854 | $14,838 | -1,832 |
| checkpoint every 60 s | 1,344 | 13,709 | $17,003 | $16,090 | $14,150 | -2,596 |
| segment linger 50 ms | 1,790 | 14,625 | $19,599 | $18,686 | $16,486 | +0 |
| segment linger 100 ms | 1,790 | 14,625 | $19,599 | $18,686 | $16,486 | +0 |
| segment linger 250 ms | 1,790 | 14,625 | $19,599 | $18,686 | $16,486 | +0 |
| K = 1 | 1,279 | 14,625 | $17,046 | $16,133 | $14,187 | -2,553 |
| K = 8 | 2,877 | 14,625 | $25,037 | $24,124 | $21,385 | +5,438 |
| segment cap 2 MiB | 5,052 | 14,625 | $35,913 | $35,000 | $31,184 | +16,314 |
| segment cap 32 MiB | 1,279 | 14,625 | $17,046 | $16,133 | $14,187 | -2,553 |
| S3 Express-like latency (6 ms PUTs, t_flush 15 ms) | 3,493 | 15,405 | $28,430 | $27,517 | $24,440 | +8,831 |
| GC interval 30 min | 1,772 | 14,625 | $19,509 | $18,596 | $16,379 | -90 |
| all tuned: manifest 10 s, polls 30 s, checkpoint 30 s, linger 100 ms | 1,475 | 6,293 | $14,693 | $13,780 | $12,071 | -4,906 |

## bluesky-today: 28.9 M record ops/day as commits (334/s avg, ~420/s peak hour), 56 M bsky-hosted repos, 23.9 B records
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 2,714 | 4,863 | $3,290 (ops $3,178) | $3,268 (ops $3,178) | $2,936 (ops $2,863) |
| 3 | 1024 | 606 | 9,668 | 4,863 | $7,008 (ops $6,896) | $6,986 (ops $6,896) | $6,312 (ops $6,239) |
| 8 | 256 | 787 | 2,898 | 4,863 | $5,207 (ops $5,095) | $5,186 (ops $5,095) | $4,663 (ops $4,590) |
| 8 | 1024 | 1,205 | 10,298 | 4,863 | $10,256 (ops $10,144) | $10,235 (ops $10,144) | $9,237 (ops $9,164) |
| 16 | 256 | 1,059 | 3,191 | 4,863 | $6,682 (ops $6,570) | $6,661 (ops $6,570) | $5,991 (ops $5,918) |
| 16 | 1024 | 1,668 | 10,941 | 4,863 | $12,829 (ops $12,717) | $12,808 (ops $12,717) | $11,554 (ops $11,481) |

## bluesky-today-90M: same, every PLC DID (89.9 M) as a repo
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 2,714 | 4,877 | $3,290 (ops $3,178) | $3,269 (ops $3,178) | $2,936 (ops $2,863) |
| 3 | 1024 | 606 | 9,668 | 4,877 | $7,008 (ops $6,896) | $6,987 (ops $6,896) | $6,313 (ops $6,239) |
| 8 | 256 | 787 | 2,898 | 4,877 | $5,208 (ops $5,095) | $5,186 (ops $5,095) | $4,663 (ops $4,590) |
| 8 | 1024 | 1,205 | 10,298 | 4,877 | $10,256 (ops $10,144) | $10,235 (ops $10,144) | $9,237 (ops $9,164) |
| 16 | 256 | 1,059 | 3,191 | 4,877 | $6,682 (ops $6,570) | $6,661 (ops $6,570) | $5,991 (ops $5,918) |
| 16 | 1024 | 1,668 | 10,941 | 4,877 | $12,829 (ops $12,717) | $12,808 (ops $12,717) | $11,554 (ops $11,481) |

## sizing-today: DESIGN sizing baseline: 50 M repos, 25 B records, 2,000/s daily peak (~1,600/s avg)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 419 | 2,801 | 5,959 | $3,354 (ops $3,217) | $3,328 (ops $3,217) | $2,988 (ops $2,898) |
| 3 | 1024 | 607 | 9,755 | 5,959 | $7,072 (ops $6,935) | $7,046 (ops $6,935) | $6,364 (ops $6,275) |
| 8 | 256 | 858 | 2,985 | 5,959 | $5,622 (ops $5,485) | $5,596 (ops $5,485) | $5,030 (ops $4,941) |
| 8 | 1024 | 1,276 | 10,385 | 5,959 | $10,671 (ops $10,534) | $10,644 (ops $10,534) | $9,604 (ops $9,515) |
| 16 | 256 | 1,485 | 3,279 | 5,959 | $8,875 (ops $8,738) | $8,849 (ops $8,738) | $7,961 (ops $7,872) |
| 16 | 1024 | 2,095 | 11,029 | 5,959 | $15,023 (ops $14,886) | $14,997 (ops $14,886) | $13,524 (ops $13,434) |

## sizing-100x: 100x writes (200k/s peak, ~160k/s avg), 20x accounts (1 B) and records (500 B)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 1,294 | 7,040 | 208,753 | $14,086 (ops $9,285) | $13,173 (ops $9,285) | $11,494 (ops $8,363) |
| 3 | 1024 | 1,481 | 13,994 | 208,753 | $17,804 (ops $13,003) | $16,891 (ops $13,003) | $14,871 (ops $11,740) |
| 8 | 256 | 1,372 | 7,224 | 208,753 | $14,550 (ops $9,749) | $13,637 (ops $9,749) | $11,912 (ops $8,781) |
| 8 | 1024 | 1,790 | 14,625 | 208,753 | $19,599 (ops $14,798) | $18,686 (ops $14,798) | $16,486 (ops $13,355) |
| 16 | 256 | 1,496 | 7,518 | 208,753 | $15,290 (ops $10,489) | $14,377 (ops $10,489) | $12,578 (ops $9,447) |
| 16 | 1024 | 2,106 | 15,268 | 208,753 | $21,437 (ops $16,636) | $20,524 (ops $16,636) | $18,141 (ops $15,010) |
