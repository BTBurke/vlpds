### idle1 (1800 s window; ['--listen', '127.0.0.1:2701', '--public-url', 'http://127.0.0.1:2701', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-idle1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '60000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/idle1/cache', '--inject-put-ms', '30'])

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| ctl_lease | 0.167 | 0.0000 | 0.0000 |
| ctl_assign | 0.0833 | 0.0011 | 0.0000 |
| log_segment | 0.0333 | 0.0000 | 0.0000 |
| state_gc_boundary | 0.0011 | 0.220 | 0.0000 |
| state_manifest | 0.0033 | 0.152 | 0.0017 |
| state_compactions | 0.0028 | 0.0694 | 0.0011 |
| other | 0.0000 | 0.1000 | 0.0000 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| state_sst | 0.0017 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0139 | 0.0000 |
| **total** | **0.296** | **0.556** | 0.0028 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | ctl_assign | ok | 150 | 0.0833 | 300 |
| A | list | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_cas | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | list | state_compactions | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_compactions | ok | 2 | 0.0011 | 4 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| B | get | state_gc_boundary | precondition | 329 | 0.183 | 658 |
| B | get | state_manifest | not_found | 270 | 0.150 | 540 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_compactions | not_found | 124 | 0.0689 | 248 |
| B | get | state_gc_boundary | not_found | 63 | 0.0350 | 126 |
| B | get | ctl_version | ok | 25 | 0.0139 | 50 |
| B | get | state_gc_boundary | ok | 4 | 0.0022 | 8 |
| B | get | state_manifest | ok | 3 | 0.0017 | 6 |
| B | get | ctl_assign | ok | 2 | 0.0011 | 4 |
| B | get | state_compactions | ok | 1 | 0.0006 | 2 |
| free | delete | state_manifest | ok | 3 | 0.0017 | 6 |
| free | delete | state_compactions | ok | 2 | 0.0011 | 4 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| db | get | main | 0.240 |
| compactor | get | main | 0.133 |
| gc | list | main | 0.0050 |
| db | list | main | 0.0033 |
| db | delete | main | 0.0028 |
| db | put | main | 0.0011 |

### pers1 (1800 s window; ['--listen', '127.0.0.1:2702', '--public-url', 'http://127.0.0.1:2702', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-pers1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '60000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/pers1/cache', '--inject-put-ms', '30'])

workload in window: blob_upload 8, follow 7, getBlob 15, getRepo 6, like 30, post 23, repost 8 

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| ctl_lease | 0.167 | 0.0000 | 0.0000 |
| state_manifest | 0.105 | 0.341 | 0.0378 |
| state_compactions | 0.102 | 0.325 | 0.0433 |
| ctl_assign | 0.0833 | 0.0011 | 0.0000 |
| log_segment | 0.0711 | 0.0000 | 0.0000 |
| state_sst | 0.0461 | 0.144 | 0.0000 |
| state_gc_boundary | 0.0033 | 0.560 | 0.0000 |
| other | 0.0000 | 0.1000 | 0.0000 |
| blob | 0.0044 | 0.0128 | 0.0000 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0139 | 0.0000 |
| **total** | **0.586** | **1.498** | 0.0811 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | ctl_assign | ok | 150 | 0.0833 | 300 |
| A | list | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_cas | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_create | state_compactions | ok | 100 | 0.0556 | 200 |
| A | put_create | state_manifest | ok | 100 | 0.0556 | 200 |
| A | put | state_sst | ok | 80 | 0.0444 | 160 |
| A | delete_batch | state_compactions | ok | 78 | 0.0433 | 156 |
| A | put_create | log_segment | ok | 68 | 0.0378 | 136 |
| A | delete_batch | state_manifest | ok | 68 | 0.0378 | 136 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | state_manifest | ok | 19 | 0.0106 | 38 |
| A | put | blob | ok | 8 | 0.0044 | 16 |
| A | list | state_compactions | ok | 6 | 0.0033 | 12 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | put_cas | state_gc_boundary | ok | 4 | 0.0022 | 8 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| A | put_create | state_manifest | precondition | 2 | 0.0011 | 4 |
| B | get | state_gc_boundary | precondition | 836 | 0.464 | 1672 |
| B | get | state_compactions | not_found | 471 | 0.262 | 942 |
| B | get | state_manifest | not_found | 316 | 0.176 | 632 |
| B | get | state_manifest | ok | 296 | 0.164 | 592 |
| B | get_range | state_sst | ok | 260 | 0.144 | 520 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_gc_boundary | not_found | 160 | 0.0889 | 320 |
| B | get | state_compactions | ok | 114 | 0.0633 | 228 |
| B | get | ctl_version | ok | 25 | 0.0139 | 50 |
| B | get | blob | ok | 15 | 0.0083 | 30 |
| B | get | state_gc_boundary | ok | 12 | 0.0067 | 24 |
| B | head | blob | ok | 8 | 0.0044 | 16 |
| B | get | ctl_assign | ok | 2 | 0.0011 | 4 |
| B | head | state_manifest | ok | 2 | 0.0011 | 4 |
| free | delete | state_compactions | ok | 78 | 0.0433 | 156 |
| free | delete | state_manifest | ok | 68 | 0.0378 | 136 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| compactor | get | main | 0.632 |
| db | get | main | 0.361 |
| db | delete | main | 0.0811 |
| db | put | main | 0.0711 |
| compactor | put | main | 0.0556 |
| db | get_range | main | 0.0444 |
| db | list | main | 0.0050 |
| gc | list | main | 0.0050 |
| db | head | main | 0.0011 |

### idle64 (1800 s window; ['--listen', '127.0.0.1:2703', '--public-url', 'http://127.0.0.1:2703', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-idle64', '--dev-mode', '--node-id', 'n1', '--shards', '64', '--lease-ttl-ms', '10000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/idle64/cache', '--inject-put-ms', '30'])

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| state_gc_boundary | 0.0711 | 14.0 | 0.0000 |
| ctl_lease | 1.000 | 0.0000 | 0.0000 |
| state_manifest | 0.213 | 9.704 | 0.107 |
| state_compactions | 0.182 | 4.377 | 0.0711 |
| ctl_assign | 0.500 | 0.217 | 0.0000 |
| state_wal | 0.213 | 0.0000 | 0.0000 |
| state_sst | 0.107 | 0.0000 | 0.0000 |
| log_segment | 0.0333 | 0.0000 | 0.0000 |
| other | 0.0000 | 0.1000 | 0.0000 |
| ctl_version | 0.0000 | 0.0833 | 0.0000 |
| **total** | **2.320** | **28.5** | 0.178 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | ctl_assign | ok | 900 | 0.500 | 1800 |
| A | list | ctl_lease | ok | 900 | 0.500 | 1800 |
| A | put_cas | ctl_lease | ok | 900 | 0.500 | 1800 |
| A | list | state_wal | ok | 384 | 0.213 | 768 |
| A | list | state_compactions | ok | 200 | 0.111 | 400 |
| A | delete_batch | state_manifest | ok | 192 | 0.107 | 384 |
| A | list | state_manifest | ok | 192 | 0.107 | 384 |
| A | list | state_sst | ok | 192 | 0.107 | 384 |
| A | delete_batch | state_compactions | ok | 128 | 0.0711 | 256 |
| A | put_create | state_gc_boundary | ok | 128 | 0.0711 | 256 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| B | get | state_gc_boundary | precondition | 21072 | 11.7 | 42144 |
| B | get | state_manifest | not_found | 17276 | 9.598 | 34552 |
| B | get | state_compactions | not_found | 7857 | 4.365 | 15714 |
| B | get | state_gc_boundary | not_found | 3993 | 2.218 | 7986 |
| B | get | ctl_assign | ok | 390 | 0.217 | 780 |
| B | get | state_gc_boundary | ok | 206 | 0.114 | 412 |
| B | get | state_manifest | ok | 192 | 0.107 | 384 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | ctl_version | ok | 150 | 0.0833 | 300 |
| B | get | state_compactions | ok | 21 | 0.0117 | 42 |
| free | delete | state_manifest | ok | 192 | 0.107 | 384 |
| free | delete | state_compactions | ok | 128 | 0.0711 | 256 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| db | get | main | 15.3 |
| compactor | get | main | 8.530 |
| gc | list | main | 0.320 |
| db | list | main | 0.218 |
| db | delete | main | 0.178 |
| db | put | main | 0.0711 |

### tidle1 (1800 s window; ['--listen', '127.0.0.1:2704', '--public-url', 'http://127.0.0.1:2704', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-tidle1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '300000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/tidle1/cache', '--inject-put-ms', '30', '--slatedb-manifest-poll', '60s', '--compaction-poll', '120s'])

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| log_segment | 0.0333 | 0.0000 | 0.0000 |
| ctl_lease | 0.0333 | 0.0000 | 0.0000 |
| ctl_assign | 0.0167 | 0.0000 | 0.0000 |
| other | 0.0000 | 0.1000 | 0.0000 |
| state_manifest | 0.0033 | 0.0433 | 0.0017 |
| state_gc_boundary | 0.0011 | 0.0611 | 0.0000 |
| state_compactions | 0.0028 | 0.0189 | 0.0011 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| state_sst | 0.0017 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0028 | 0.0000 |
| **total** | **0.0956** | **0.226** | 0.0028 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | ctl_assign | ok | 30 | 0.0167 | 60 |
| A | list | ctl_lease | ok | 30 | 0.0167 | 60 |
| A | put_cas | ctl_lease | ok | 30 | 0.0167 | 60 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | list | state_compactions | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_compactions | ok | 2 | 0.0011 | 4 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_gc_boundary | precondition | 90 | 0.0500 | 180 |
| B | get | state_manifest | not_found | 75 | 0.0417 | 150 |
| B | get | state_compactions | not_found | 33 | 0.0183 | 66 |
| B | get | state_gc_boundary | not_found | 17 | 0.0094 | 34 |
| B | get | ctl_version | ok | 5 | 0.0028 | 10 |
| B | get | state_gc_boundary | ok | 3 | 0.0017 | 6 |
| B | get | state_manifest | ok | 3 | 0.0017 | 6 |
| B | get | state_compactions | ok | 1 | 0.0006 | 2 |
| free | delete | state_manifest | ok | 3 | 0.0017 | 6 |
| free | delete | state_compactions | ok | 2 | 0.0011 | 4 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| db | get | main | 0.0733 |
| compactor | get | main | 0.0333 |
| gc | list | main | 0.0050 |
| db | list | main | 0.0033 |
| db | delete | main | 0.0028 |
| db | put | main | 0.0011 |

### tpers1 (1800 s window; ['--listen', '127.0.0.1:2705', '--public-url', 'http://127.0.0.1:2705', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-tpers1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '300000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/tpers1/cache', '--inject-put-ms', '30', '--slatedb-manifest-poll', '60s', '--compaction-poll', '120s'])

workload in window: blob_upload 8, follow 6, getBlob 15, getRepo 6, like 33, post 20, repost 9 

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| state_compactions | 0.113 | 0.885 | 0.0428 |
| state_manifest | 0.111 | 0.417 | 0.0394 |
| state_gc_boundary | 0.0033 | 1.252 | 0.0000 |
| log_segment | 0.0711 | 0.0000 | 0.0000 |
| state_sst | 0.0444 | 0.129 | 0.0000 |
| ctl_lease | 0.0333 | 0.0000 | 0.0000 |
| ctl_assign | 0.0167 | 0.0000 | 0.0000 |
| other | 0.0000 | 0.1000 | 0.0000 |
| blob | 0.0044 | 0.0128 | 0.0000 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0028 | 0.0000 |
| **total** | **0.401** | **2.798** | 0.0822 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | put_create | state_manifest | ok | 98 | 0.0544 | 196 |
| A | put_create | state_compactions | ok | 97 | 0.0539 | 194 |
| A | delete_batch | state_compactions | ok | 77 | 0.0428 | 154 |
| A | put | state_sst | ok | 77 | 0.0428 | 154 |
| A | delete_batch | state_manifest | ok | 71 | 0.0394 | 142 |
| A | put_create | log_segment | ok | 68 | 0.0378 | 136 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | ctl_assign | ok | 30 | 0.0167 | 60 |
| A | list | ctl_lease | ok | 30 | 0.0167 | 60 |
| A | put_cas | ctl_lease | ok | 30 | 0.0167 | 60 |
| A | list | state_manifest | ok | 28 | 0.0156 | 56 |
| A | list | state_compactions | ok | 26 | 0.0144 | 52 |
| A | put | blob | ok | 8 | 0.0044 | 16 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | put_create | state_compactions | precondition | 4 | 0.0022 | 8 |
| A | put_cas | state_gc_boundary | ok | 4 | 0.0022 | 8 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| A | put_create | state_manifest | precondition | 2 | 0.0011 | 4 |
| B | get | state_gc_boundary | precondition | 2043 | 1.135 | 4086 |
| B | get | state_compactions | not_found | 1469 | 0.816 | 2938 |
| B | get | state_manifest | not_found | 539 | 0.299 | 1078 |
| B | get_range | state_sst | ok | 232 | 0.129 | 464 |
| B | get | state_manifest | ok | 210 | 0.117 | 420 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_gc_boundary | not_found | 174 | 0.0967 | 348 |
| B | get | state_compactions | ok | 121 | 0.0672 | 242 |
| B | get | state_gc_boundary | ok | 36 | 0.0200 | 72 |
| B | get | blob | ok | 15 | 0.0083 | 30 |
| B | head | blob | ok | 8 | 0.0044 | 16 |
| B | get | ctl_version | ok | 5 | 0.0028 | 10 |
| B | head | state_compactions | ok | 3 | 0.0017 | 6 |
| B | head | state_manifest | ok | 2 | 0.0011 | 4 |
| free | delete | state_compactions | ok | 77 | 0.0428 | 154 |
| free | delete | state_manifest | ok | 71 | 0.0394 | 142 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| compactor | get | main | 1.736 |
| db | get | main | 0.227 |
| db | delete | main | 0.0822 |
| db | put | main | 0.0711 |
| compactor | put | main | 0.0572 |
| db | get_range | main | 0.0450 |
| compactor | list | main | 0.0122 |
| db | list | main | 0.0061 |
| gc | list | main | 0.0050 |
| compactor | head | main | 0.0017 |
| db | head | main | 0.0011 |

### bidle1 (1800 s window; ['--listen', '127.0.0.1:2710', '--public-url', 'http://127.0.0.1:2710', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-bidle1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '60000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/bidle1/cache', '--inject-put-ms', '30', '--slatedb-manifest-poll', '60s'])

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| ctl_lease | 0.167 | 0.0000 | 0.0000 |
| ctl_assign | 0.0833 | 0.0011 | 0.0000 |
| log_segment | 0.0333 | 0.0000 | 0.0000 |
| state_gc_boundary | 0.0011 | 0.134 | 0.0000 |
| state_manifest | 0.0033 | 0.0683 | 0.0017 |
| state_compactions | 0.0028 | 0.0672 | 0.0011 |
| other | 0.0000 | 0.1000 | 0.0000 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| state_sst | 0.0017 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0139 | 0.0000 |
| **total** | **0.296** | **0.384** | 0.0028 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | ctl_assign | ok | 150 | 0.0833 | 300 |
| A | list | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_cas | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | list | state_compactions | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_manifest | ok | 3 | 0.0017 | 6 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | delete_batch | state_compactions | ok | 2 | 0.0011 | 4 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| B | get | state_gc_boundary | precondition | 199 | 0.111 | 398 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_manifest | not_found | 120 | 0.0667 | 240 |
| B | get | state_compactions | not_found | 119 | 0.0661 | 238 |
| B | get | state_gc_boundary | not_found | 39 | 0.0217 | 78 |
| B | get | ctl_version | ok | 25 | 0.0139 | 50 |
| B | get | state_gc_boundary | ok | 3 | 0.0017 | 6 |
| B | get | state_manifest | ok | 3 | 0.0017 | 6 |
| B | get | ctl_assign | ok | 2 | 0.0011 | 4 |
| B | get | state_compactions | ok | 2 | 0.0011 | 4 |
| free | delete | state_manifest | ok | 3 | 0.0017 | 6 |
| free | delete | state_compactions | ok | 2 | 0.0011 | 4 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| compactor | get | main | 0.133 |
| db | get | main | 0.0739 |
| gc | list | main | 0.0050 |
| db | list | main | 0.0033 |
| db | delete | main | 0.0028 |
| db | put | main | 0.0011 |

### bpers1 (1800 s window; ['--listen', '127.0.0.1:2711', '--public-url', 'http://127.0.0.1:2711', '--s3-endpoint', 'http://127.0.0.1:9310', '--prefix', 'tiny-bpers1', '--dev-mode', '--node-id', 'n1', '--shards', '1', '--lease-ttl-ms', '60000', '--repo-cache-mb', '512', '--block-cache-mb', '256', '--cache-budget-mb', '256', '--cache-dir', '/tmp/scratch/nodes/bpers1/cache', '--inject-put-ms', '30', '--slatedb-manifest-poll', '60s'])

workload in window: blob_upload 8, follow 3, getBlob 15, getRepo 6, like 44, post 17, repost 4 

| component | Class A /s | Class B /s | free (DELETE) /s |
|---|---|---|---|
| ctl_lease | 0.167 | 0.0000 | 0.0000 |
| state_compactions | 0.102 | 0.371 | 0.0433 |
| state_manifest | 0.103 | 0.272 | 0.0378 |
| ctl_assign | 0.0833 | 0.0011 | 0.0000 |
| log_segment | 0.0711 | 0.0000 | 0.0000 |
| state_sst | 0.0461 | 0.144 | 0.0000 |
| state_gc_boundary | 0.0033 | 0.538 | 0.0000 |
| other | 0.0000 | 0.1000 | 0.0000 |
| blob | 0.0044 | 0.0128 | 0.0000 |
| state_wal | 0.0033 | 0.0000 | 0.0000 |
| ctl_version | 0.0000 | 0.0139 | 0.0000 |
| **total** | **0.584** | **1.453** | 0.0811 |

| class | op | component | result | count | /s | per hour |
|---|---|---|---|---|---|---|
| A | list | ctl_assign | ok | 150 | 0.0833 | 300 |
| A | list | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_cas | ctl_lease | ok | 150 | 0.0833 | 300 |
| A | put_create | state_compactions | ok | 100 | 0.0556 | 200 |
| A | put_create | state_manifest | ok | 100 | 0.0556 | 200 |
| A | put | state_sst | ok | 80 | 0.0444 | 160 |
| A | delete_batch | state_compactions | ok | 78 | 0.0433 | 156 |
| A | put_create | log_segment | ok | 68 | 0.0378 | 136 |
| A | delete_batch | state_manifest | ok | 68 | 0.0378 | 136 |
| A | list | log_segment | ok | 60 | 0.0333 | 120 |
| A | list | state_manifest | ok | 18 | 0.0100 | 36 |
| A | put | blob | ok | 8 | 0.0044 | 16 |
| A | list | state_compactions | ok | 6 | 0.0033 | 12 |
| A | list | state_wal | ok | 6 | 0.0033 | 12 |
| A | put_cas | state_gc_boundary | ok | 4 | 0.0022 | 8 |
| A | list | state_sst | ok | 3 | 0.0017 | 6 |
| A | put_create | state_gc_boundary | ok | 2 | 0.0011 | 4 |
| B | get | state_gc_boundary | precondition | 819 | 0.455 | 1638 |
| B | get | state_compactions | not_found | 554 | 0.308 | 1108 |
| B | get | state_manifest | ok | 295 | 0.164 | 590 |
| B | get_range | state_sst | ok | 259 | 0.144 | 518 |
| B | get | state_manifest | not_found | 195 | 0.108 | 390 |
| B | get | other | not_found | 180 | 0.1000 | 360 |
| B | get | state_gc_boundary | not_found | 138 | 0.0767 | 276 |
| B | get | state_compactions | ok | 114 | 0.0633 | 228 |
| B | get | ctl_version | ok | 25 | 0.0139 | 50 |
| B | get | blob | ok | 15 | 0.0083 | 30 |
| B | get | state_gc_boundary | ok | 12 | 0.0067 | 24 |
| B | head | blob | ok | 8 | 0.0044 | 16 |
| B | get | ctl_assign | ok | 2 | 0.0011 | 4 |
| free | delete | state_compactions | ok | 78 | 0.0433 | 156 |
| free | delete | state_manifest | ok | 68 | 0.0378 | 136 |

SlateDB's own counts (`slatedb_object_store_request_count_total`):

| slatedb component | api | store | /s |
|---|---|---|---|
| compactor | get | main | 0.728 |
| db | get | main | 0.225 |
| db | delete | main | 0.0811 |
| db | put | main | 0.0700 |
| db | get_range | main | 0.0644 |
| compactor | put | main | 0.0556 |
| db | list | main | 0.0050 |
| gc | list | main | 0.0050 |

