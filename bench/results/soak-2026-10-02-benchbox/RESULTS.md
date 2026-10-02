# vlpds soak: soak1

Driver `bench/soak/soak.py` (`report --name soak1 --hours 7 --nodes 4 --shards 64 --population 200000 --pop-records 10 --mid-repos 32 --mid-records 5000 --write-rate 1500 --read-rate 200 --procs 6 --cycle restart:5400,calm:1800,reshard:5400,calm:1800,mixed:3600 --restart-every 90 --reshard-every 90 --log-retention 90s --checkpoint-lifetime 2m --gc-min-age 2m`), binaries `/home/target/release`, MinIO `http://127.0.0.1:9200` prefix `soak1`. 4 native nodes (ports 2700-2703; n1..n1 never restarted), initial 64 shards (kept within +-2), `--log-retention 90s --slatedb-checkpoint-lifetime 2m --slatedb-gc-min-age 2m --lease-ttl-ms 10000` .

Load: 1500.0 writes/s (Zipf s=1.0 cap 0.002 over 200,000 bulk repos x 10 records; creates/deletes/updates target 95.9/3.8/0.3 %, every 25th repo delete-heavy at 45% deletes), 200.0 reads/s over 32 mid repos x 5000 records + fresh records, one firehose subscriber, a cursor backfill every 120 s (45 s back). Cycle `restart:5400,calm:1800,reshard:5400,calm:1800,mixed:3600` on the soak clock; restarts every 90 s in `restart` phases (30% kill -9), split/merge every 90 s in `reshard` phases.

## History reached

Soak clock 7.00 h; 168 node incarnations (160 restarts: 122 SIGTERM, 38 kill -9; 2 cluster starts), 100 reshard ops (50 splits, 50 merges, 0 failed), 37,745,964 client-acked writes (36,215,709 creates, 1,417,649 deletes, 112,606 updates). Layout now 64 shards, next id 214.

Restarts: median exit 1.61 s (SIGTERM), serving 0.27 s, converged 5.1 s; first/last 5 converge: [5.07, 4.56, 4.57, 5.08, 6.12] / [6.64, 5.11, 4.58, 5.1, 4.6].
Reshards: 100/100 done; secs first/last 5: [0.39, 0.27, 1.0, 0.37, 0.38] / [0.93, 0.44, 0.45, 0.72, 0.33]. Errors: []
Backfills: 210 probes; first-event s first/last 5: [0.001, 0.0, 0.0, 0.211, 0.001] / [0.0, 0.0, 0.001, 0.001, 0.0]; outdated 1; errors 1.
Client errors (after retries): {'w_create': 103, 'w_delete': 5, 'r_syncGetRecord': 1}; retried on refused connections 75,182, on 502/503/504 305,270. Firehose reconnects 0, out-of-order 0, OutdatedCursor 0.

## Verdict per suspected growth path

A metric *grows with history* when a multiple regression on cumulative incarnations, reshard ops and commits attributes >= 10% growth over the run to incarnations or reshards with |t| >= 3; *grows with data* when only commits do; *flat* when the last-10% mean is within 10% of the first-10% mean. Latency/request metrics use calm samples only (no restart/reshard in progress or within 30 s after converging); the first 300 s are skipped. Samples are autocorrelated, so t values are optimistic: read the effect sizes.

| Path | Metric | n | first -> last | change | verdict |
|---|---|---|---|---|---|
| a: Parents pinned by children -> read amplification | retired bytes still referenced by live manifests | 2488 | 0 -> 1,987,528,143 | 153% | grows with history (reshard ops: +184%) |
| a: Parents pinned by children -> read amplification | live shards with external SSTs | 2488 | 0 -> 35.3 | 147% | grows with history (reshard ops: +93%) |
| a: Parents pinned by children -> read amplification | external dbs per live shard | 2488 | 0 -> 0.64 | 129% | grows with history (reshard ops: +116%) |
| a: Parents pinned by children -> read amplification | state SST GET+HEAD / client read | 1614 | 4.06 -> 4.93 | 21.3% | grows with data (+620%) |
| a: Parents pinned by children -> read amplification | getRecord p99 (ms) | 1614 | 26.4 -> 168 | 537% | grows with data (+2774%) |
| a: Parents pinned by children -> read amplification | listRecords p99 (ms) | 1614 | 26.8 -> 167 | 524% | grows with data (+2736%) |
| b: Retired parents' state dirs never deleted | retired state dirs | 2488 | 0 -> 150 | 195% | grows with history (reshard ops: +196%) |
| b: Retired parents' state dirs never deleted | retired state bytes | 2488 | 0 -> 6,436,203,935 | 202% | grows with history (reshard ops: +227%) |
| b: Retired parents' state dirs never deleted | state bytes / live-shard bytes | 2488 | 1 -> 2.49 | 149% | grows with history (reshard ops: +334%) |
| c: Retired assign/ records grow the per-step LIST | assign/ objects | 2488 | 65 -> 215 | 231% | grows with history (reshard ops: +230%) |
| c: Retired assign/ records grow the per-step LIST | assign/ LIST response bytes | 2488 | 13,850 -> 45,321 | 227% | grows with history (reshard ops: +226%) |
| c: Retired assign/ records grow the per-step LIST | assign/ requests/s (all ops) | 1614 | 3.25 -> 4.8 | 47.7% | grows with history (reshard ops: +247%) |
| c: Retired assign/ records grow the per-step LIST | assign/ GETs/s | 1614 | 1.24 -> 2.8 | 127% | grows with history (reshard ops: +646%) |
| d: Dead-log fences grow the log/ LIST with every restart | log ids under log/ (incarnations kept) | 2488 | 21.5 -> 167 | 676% | grows with history (incarnations: +743%) |
| d: Dead-log fences grow the log/ LIST with every restart | fence-only dead logs | 2488 | 16.1 -> 163 | 910% | grows with history (incarnations: +968%) |
| d: Dead-log fences grow the log/ LIST with every restart | log/ delimiter LIST response bytes | 2488 | 1,993 -> 13,631 | 584% | grows with history (incarnations: +642%) |
| d: Dead-log fences grow the log/ LIST with every restart | log/ LIST pages/s (wrapper) | 1614 | 1.05 -> 0.288 | -72.5% | falls (-72%) |
| d: Dead-log fences grow the log/ LIST with every restart | backfill time to first event (s) | 210 | 0.0389 -> 0.0362 | -6.9% | flat |
| e: Per-log metric labels (cardinality per restart) | n1 /metrics series | 2488 | 1,524 -> 1,601 | 5.1% | flat |
| f: Tombstones linger under size-tiered compaction | live-shard bytes / live record | 2488 | 207 -> 123 | -40.2% | falls (-40%) |
| f: Tombstones linger under size-tiered compaction | listRecords delete-heavy p99 (ms) | 1614 | 27.8 -> 171 | 513% | grows with data (+2619%) |
| f: Tombstones linger under size-tiered compaction | SSTs per live shard | 2488 | 69.7 -> 103 | 47.2% | grows with data (+427%) |
| -: Generic: latency, requests/commit, memory | write p99 (ms) | 1614 | 114 -> 276 | 143% | grows with data (+703%) |
| -: Generic: latency, requests/commit, memory | object-store requests / commit | 1614 | 1.11 -> 1.26 | 13.2% | grows with data (+343%) |
| -: Generic: latency, requests/commit, memory | n1 RSS (MB) | 2488 | 3,740 -> 6,350 | 69.8% | grows with data (+192%) |
| -: Generic: latency, requests/commit, memory | n1 tokio alive tasks | 2488 | 630 -> 629 | -0.3% | flat |
| -: Generic: latency, requests/commit, memory | firehose lag p99 (ms) | 1614 | 115 -> 159 | 38.7% | changes +39%, attribution unclear; best single fit: commits r2 0.007 |

## Trends (all metrics)

Slopes from simple linear fits (r2 in parentheses); multiple-regression effects are the growth over the run attributed to each counter, % of the first-10% mean (t).

| Metric | mode | per hour | per 100 incarnations | per 100 reshards | per 1M commits | MR effect: incarn. / reshards / commits | MR r2 | verdict |
|---|---|---|---|---|---|---|---|---|
| write p50 (ms) | calm | 4.45 (0.003) | 19 (0.003) | 15.8 (0.002) | 0.824 (0.003) | -221% (-1.4) / -339% (-2.4) / +761% (2.5) | 0.007 | changes +155%, attribution unclear; best single fit: incarnations r2 0.003 |
| write p99 (ms) | calm | 14 (0.007) | 55.1 (0.005) | 52.6 (0.004) | 2.6 (0.007) | -244% (-2.6) / -291% (-3.5) / +703% (3.9) | 0.014 | grows with data (+703%) |
| getRecord p99 (ms) | calm | 11.3 (0.005) | 45.3 (0.004) | 39.8 (0.003) | 2.09 (0.005) | -936% (-2.5) / -1196% (-3.5) / +2774% (3.8) | 0.013 | grows with data (+2774%) |
| listRecords p99 (ms) | calm | 11.1 (0.005) | 44.6 (0.004) | 39 (0.003) | 2.06 (0.005) | -925% (-2.5) / -1182% (-3.6) / +2736% (3.8) | 0.013 | grows with data (+2736%) |
| sync.getRecord p99 (ms) | calm | 11.5 (0.005) | 46.6 (0.004) | 40.7 (0.003) | 2.14 (0.005) | -939% (-2.4) / -1221% (-3.6) / +2823% (3.8) | 0.013 | grows with data (+2823%) |
| getRepo p50 (ms) | calm | 3.63 (0.003) | 15.2 (0.002) | 12.5 (0.001) | 0.673 (0.003) | -675% (-1.5) / -964% (-2.4) / +2171% (2.5) | 0.006 | changes +405%, attribution unclear; best single fit: commits r2 0.003 |
| listRecords delete-heavy p99 (ms) | calm | 11.3 (0.005) | 46.1 (0.004) | 39.5 (0.003) | 2.1 (0.005) | -863% (-2.4) / -1141% (-3.6) / +2619% (3.8) | 0.013 | grows with data (+2619%) |
| object-store requests / commit | calm | 0.0678 (0.035) | 0.11 (0.005) | 0.375 (0.045) | 0.0126 (0.035) | -241% (-12.4) / -62% (-3.6) / +343% (9.2) | 0.133 | grows with data (+343%) |
| state SST GET+HEAD / client read | calm | 0.524 (0.054) | 0.895 (0.008) | 2.96 (0.073) | 0.0972 (0.054) | -463% (-14.5) / -84% (-3.0) / +620% (10.1) | 0.189 | grows with data (+620%) |
| manifest requests/s | calm | 0.0434 (0.0) | -0.348 (0.0) | -4.47 (0.001) | 0.00836 (0.0) | -74% (-5.1) / -98% (-7.5) / +201% (7.1) | 0.034 | grows with data (+201%) |
| SST requests/s | calm | 105 (0.056) | 181 (0.008) | 588 (0.074) | 19.5 (0.056) | -466% (-15.0) / -96% (-3.5) / +639% (10.6) | 0.195 | grows with data (+639%) |
| assign/ requests/s (all ops) | calm | 0.593 (0.032) | 2.17 (0.021) | 3.29 (0.042) | 0.11 (0.032) | +49% (0.8) / +247% (4.3) / -256% (-2.1) | 0.047 | grows with history (reshard ops: +247%) |
| assign/ GETs/s | calm | 0.593 (0.032) | 2.17 (0.021) | 3.29 (0.042) | 0.11 (0.032) | +124% (0.7) / +646% (4.3) / -664% (-2.1) | 0.047 | grows with history (reshard ops: +646%) |
| log/ LIST pages/s (wrapper) | calm | 0.282 (0.009) | 1.26 (0.009) | 1.72 (0.014) | 0.0523 (0.009) | +748% (4.1) / +943% (5.9) / -1776% (-5.1) | 0.03 | falls (-72%) |
| retain/ requests/s | calm | 0.183 (0.027) | 0.935 (0.033) | 1 (0.036) | 0.034 (0.027) | +828% (5.3) / +831% (5.8) / -1673% (-5.5) | 0.075 | grows with history (reshard ops: +831%) |
| assign/ objects | all | 30.1 (0.933) | 122 (0.732) | 150 (1.0) | 5.58 (0.933) | -2% (-19.4) / +230% (3385.1) / +2% (16.2) | 1.0 | grows with history (reshard ops: +230%) |
| assign/ LIST response bytes | all | 6,308 (0.933) | 25,578 (0.733) | 31,420 (1.0) | 1,169 (0.933) | -1% (-6.4) / +226% (3254.8) / +2% (14.5) | 1.0 | grows with history (reshard ops: +226%) |
| log ids under log/ (incarnations kept) | all | 20.5 (0.882) | 100 (1.0) | 90 (0.733) | 3.8 (0.882) | +743% (8213.2) / -0% (-3.5) / +1% (3.3) | 1.0 | grows with history (incarnations: +743%) |
| fence-only dead logs | all | 20.5 (0.882) | 99.9 (1.0) | 89.7 (0.728) | 3.8 (0.881) | +968% (893.7) / -39% (-40.8) / +76% (37.2) | 1.0 | grows with history (incarnations: +968%) |
| log/ delimiter LIST response bytes | all | 1,642 (0.882) | 8,001 (1.0) | 7,203 (0.733) | 304 (0.882) | +642% (8210.8) / -0% (-2.2) / +0% (2.1) | 1.0 | grows with history (incarnations: +642%) |
| objects in live logs | all | -360 (0.021) | -1,615 (0.02) | -2,737 (0.052) | -66.6 (0.021) | -67% (-14.6) / -97% (-24.0) / +184% (21.2) | 0.206 | grows with data (+184%) |
| retain/ reports | all | 0.00882 (0.001) | 0.039 (0.001) | 0.191 (0.018) | 0.00163 (0.001) | +46% (16.7) / +69% (28.8) / -135% (-26.1) | 0.252 | falls (-12%) |
| retired state dirs | all | 30.1 (0.933) | 122 (0.734) | 150 (1.0) | 5.58 (0.933) | +3% (18.6) / +196% (1250.8) / -5% (-13.9) | 1.0 | grows with history (reshard ops: +196%) |
| retired state bytes | all | 1,293,630,497 (0.933) | 5,356,631,098 (0.765) | 6,429,208,288 (0.995) | 239,790,274 (0.933) | +64% (89.2) / +227% (363.9) / -91% (-67.2) | 0.999 | grows with history (reshard ops: +227%) |
| retired bytes still referenced by live manifests | all | 429,893,915 (0.809) | 1,614,543,568 (0.545) | 2,211,551,694 (0.924) | 79,697,613 (0.809) | -121% (-29.1) / +184% (50.5) / +81% (10.3) | 0.953 | grows with history (reshard ops: +184%) |
| live shards with external SSTs | all | 7.2 (0.764) | 26.9 (0.508) | 36.4 (0.839) | 1.34 (0.764) | -169% (-26.9) / +93% (17.0) / +229% (19.3) | 0.877 | grows with history (reshard ops: +93%) |
| external dbs per live shard | all | 0.14 (0.658) | 0.487 (0.382) | 0.727 (0.769) | 0.0259 (0.658) | -233% (-33.8) / +116% (19.3) / +240% (18.5) | 0.855 | grows with history (reshard ops: +116%) |
| state bytes / live-shard bytes | all | 0.383 (0.723) | 1.41 (0.469) | 2.04 (0.883) | 0.071 (0.723) | -98% (-16.9) / +334% (66.0) / -112% (-10.2) | 0.939 | grows with history (reshard ops: +334%) |
| live-shard bytes / live record | all | -16.9 (0.693) | -61.5 (0.438) | -86.2 (0.777) | -3.14 (0.693) | +55% (26.0) / -27% (-14.8) / -68% (-16.9) | 0.829 | falls (-40%) |
| SSTs per live shard | all | 0.206 (0.0) | 2.11 (0.002) | -6.69 (0.018) | 0.0386 (0.0) | -125% (-21.4) / -226% (-44.3) / +427% (38.7) | 0.444 | grows with data (+427%) |
| manifest objects (live shards) | all | -7.33 (0.0) | 65.5 (0.001) | -284 (0.019) | -1.35 (0.0) | -34% (-8.9) / -92% (-27.5) / +156% (21.7) | 0.249 | grows with data (+156%) |
| n1 /metrics series | all | 11.2 (0.372) | 33.8 (0.16) | 51.8 (0.341) | 2.09 (0.372) | -20% (-64.6) / -10% (-38.9) / +37% (64.3) | 0.766 | flat |
| n1 RSS (MB) | all | 370 (0.797) | 1,516 (0.639) | 1,670 (0.7) | 68.7 (0.797) | -60% (-27.0) / -51% (-26.0) / +192% (45.7) | 0.851 | grows with data (+192%) |
| n1 tokio alive tasks | all | 2.99 (0.011) | 10.5 (0.007) | 18.8 (0.019) | 0.555 (0.011) | +4% (1.7) / +14% (6.7) / -19% (-4.4) | 0.032 | flat |
| n1 in-memory caches (MB) | all | -0.282 (0.001) | -6.2 (0.023) | -3.41 (0.006) | -0.0516 (0.001) | -293% (-34.8) / -216% (-29.5) / +556% (35.0) | 0.352 | grows with data (+556%) |
| n1 firehose merge queues (MB) | all | 0.0105 (0.003) | 0.0467 (0.003) | 0.0604 (0.004) | 0.00195 (0.003) | +191% (1.9) / +263% (2.9) / -463% (-2.4) | 0.006 | falls (-17%) |
| firehose lag p99 (ms) | calm | 4.96 (0.007) | 18.4 (0.005) | 22 (0.006) | 0.919 (0.007) | -60% (-1.9) / -48% (-1.7) / +149% (2.4) | 0.01 | changes +39%, attribution unclear; best single fit: commits r2 0.007 |
| backfill time to first event (s) | probe | -0.00384 (0.002) | -0.0119 (0.001) | -0.00778 (0.0) | -0.000712 (0.002) | +643% (1.5) / +694% (1.9) / -1598% (-2.0) | 0.02 | flat |

## Growth by phase segment

State-like metrics: summed change inside each phase type's segments (last 3 minus first 3 samples of a segment). Growth concentrated in `restart` segments is per-incarnation, in `reshard` segments per split/merge, in `calm` segments time/data-driven (writes never stop). Latency/request metrics: median of each calm segment, in order (a drifting steady state shows as a rising sequence).

| Metric | sum delta in restart (segments) | sum delta in calm (segments) | sum delta in reshard (segments) | sum delta in mixed (segments) |
|---|---|---|---|---|
| assign/ objects | 0 (2) | 0 (3) | 88.7 (1) | 58.7 (1) |
| assign/ LIST response bytes | 86.7 (2) | 0 (3) | 18,532 (1) | 12,305 (1) |
| log ids under log/ (incarnations kept) | 116 (2) | 0 (3) | 0 (1) | 43.3 (1) |
| fence-only dead logs | 118 (2) | 2 (3) | 0 (1) | 42.7 (1) |
| log/ delimiter LIST response bytes | 9,254 (2) | 0 (3) | 0 (1) | 3,468 (1) |
| objects in live logs | 8,005 (2) | -4,350 (3) | 5,094 (1) | 4,551 (1) |
| retain/ reports | -2.67 (2) | -1.67 (3) | 0 (1) | 1 (1) |
| retired state dirs | 0 (2) | 0 (3) | 90 (1) | 60 (1) |
| retired state bytes | 225,158 (2) | 271,556 (3) | 3,374,656,339 (1) | 3,061,089,607 (1) |
| retired bytes still referenced by live manifests | -618,160,324 (2) | -823,937,029 (3) | 1,858,520,386 (1) | 1,454,603,057 (1) |
| live shards with external SSTs | -6 (2) | -13 (3) | 32 (1) | 19 (1) |
| external dbs per live shard | -0.317 (2) | -0.43 (3) | 0.8 (1) | 0.53 (1) |
| state bytes / live-shard bytes | -1.4 (2) | -0.438 (3) | 1.48 (1) | 1.89 (1) |
| live-shard bytes / live record | 10.1 (2) | 41 (3) | -58.9 (1) | -62.8 (1) |
| SSTs per live shard | 29.4 (2) | 39.3 (3) | 24.6 (1) | -39.3 (1) |
| manifest objects (live shards) | 1,628 (2) | 195 (3) | 1,316 (1) | -1,280 (1) |
| n1 /metrics series | 38 (2) | 0 (3) | 61.7 (1) | -26 (1) |
| n1 RSS (MB) | 2,904 (2) | 592 (3) | 544 (1) | 287 (1) |
| n1 tokio alive tasks | -177 (2) | 2 (3) | -12.3 (1) | -27.7 (1) |
| n1 in-memory caches (MB) | 39.1 (2) | 26 (3) | 9.4 (1) | -32.2 (1) |
| n1 firehose merge queues (MB) | -0.183 (2) | 0.0133 (3) | -0.0233 (1) | -0.0233 (1) |

| Metric (calm segments) | medians in order |
|---|---|
| write p50 (ms) | 23 -> 23.4 -> 24.5 |
| write p99 (ms) | 50.5 -> 70.8 -> 79.9 |
| getRecord p99 (ms) | 2.08 -> 2.46 -> 2.08 |
| listRecords p99 (ms) | 2.77 -> 3.29 -> 2.58 |
| sync.getRecord p99 (ms) | 1.54 -> 1.7 -> 1.5 |
| getRepo p50 (ms) | 7.12 -> 7.44 -> 7.13 |
| listRecords delete-heavy p99 (ms) | 2.98 -> 3.81 -> 3.18 |
| object-store requests / commit | 1.23 -> 1.86 -> 0.96 |
| state SST GET+HEAD / client read | 5.35 -> 9.43 -> 4.2 |
| manifest requests/s | 113 -> 131 -> 111 |
| SST requests/s | 1,096 -> 1,942 -> 854 |
| assign/ requests/s (all ops) | 2 -> 2 -> 2 |
| assign/ GETs/s | 0 -> 0 -> 0 |
| log/ LIST pages/s (wrapper) | 0.3 -> 0.3 -> 0 |
| retain/ requests/s | 0.1 -> 0.1 -> 0.2 |
| firehose lag p99 (ms) | 66.7 -> 83.7 -> 96 |

Regressor correlations (calm/all samples of the first metric): {'h.incarnations~h.reshards': 0.84, 'h.incarnations~h.commits': 0.934, 'h.reshards~h.commits': 0.963}. Above ~0.95 the attribution between those counters is unreliable (run longer cycles, or a schedule with uneven storms).

## Last sample

`lat.w.p50` 33.5 | `lat.w.p99` 1,131 | `lat.r.p99` 915 | `req.per_commit` 0.927 | `req.total_s` 1,382 | `assign.keys` 215 | `assign.list_bytes` 45321 | `log.ids` 168 | `log.fence_only` 164 | `log.list_bytes` 13712 | `retain.keys` 4 | `state.dirs` 214 | `state.retired_dirs` 150 | `state.live_bytes` 4301240540 | `state.retired_bytes` 6436254412 | `state.pinned_bytes` 1836103649 | `state.total_over_live` 2.5 | `state.live_with_ext` 32 | `state.ext_refs_mean` 0.56 | `state.sst_per_shard_mean` 69 | `node.n1.rss_mb` 6,408 | `node.n1.series` 1601 | `node.n1.tasks` 610 | `fh.lag_p99` 107

## Files

`samples.jsonl` (one row per sample: load latency per op, requests by op/component, LIST sizes, state/ breakdown, per-node gauges + SlateDB gauges, history counters), `samples.csv` (plots-ready subset), `events.jsonl` (restarts, reshards, backfill probes, starts/stops), `analysis.json` (fits). Regenerate: `bench/soak/soak.py report <same flags>`.

## Benchbox soak notes (hand-written) — BASELINE, unfixed 3cdbfac

Binaries: `bench/benchbox/sync.sh 3cdbfac`, run from `target-prof` (`--features profiling`, `NODE_EXTRA="--pyroscope-url
http://127.0.0.1:4100"`); the generated header's "binaries /home/target/release" is wrong (the `report` call has no
BENCH_BIN). MinIO in Docker on benchbox's NVMe. Command as recommended (no changes): 4 nodes, 64 shards, 200k x 10 +
32 x 5000, 1,500 writes/s + 200 reads/s, `--procs 6`, cycle `restart:5400,calm:1800,reshard:5400,calm:1800,mixed:3600`,
retention 90 s, checkpoint lifetime / GC min age 2 m. The generator held 1,500 w/s from the first 10 s sample (6 load
procs at ~11% CPU each, 0 dropped); `--procs` was not raised.

Two windows: 02:00-06:50 UTC (0 -> 4.83 h, paused 25 min before the 07:15 batch pipeline) and 07:49-09:59 UTC
(4.83 -> 7.00 h). The resume restarts every node, so n1 ("never restarted") was restarted once at 4.83 h. History: 168
incarnations (160 restarts, 38 kill -9), 100 split/merge ops (0 failed), 37.7 M acked writes. Peak MinIO data 12 GB,
free space >= 550 GB throughout (cap 150 GB never approached).

Harness bug fixed during the run (bench/soak/soak.py, uncommitted): on pause, the load processes (which ignore SIGTERM,
so `terminate()` was a no-op) hung at exit, their final stats flush blocked on a stats queue nobody drained; the
sequential `join(60)` x 6 cost 6 min, then soak.py never exited and runner.sh's 10-min guard abort killed it
(rc 10 -> the loop still resumed correctly; state had been saved). Fix: drain `stats_q` (counting the last windows'
ops into history) while the children wind down, then `kill()` stragglers. Window 2 stopped in 10 s with it.

### Verdicts per path (unfixed baseline)

- **(a) parent pinning -> read amplification: grows with reshards; read cost up, latency hidden by cache.** Pinned
  retired bytes +2.2 GB per 100 reshards (r2 0.92; 1.84 GB at the end), live shards with external SSTs 0 -> 32-35 of
  64, external dbs per live shard 0 -> 0.56-0.64. Compaction detaches some (pinned bytes fall 0.6-0.8 GB across the
  restart/calm segments) but net growth is linear in reshards. State SST GET+HEAD per client read (calm medians):
  3.8 -> 5.2 before any reshard, 9.6 / 9.4 in the reshard hours, 8.4-9.5 after, 6.4 in the last hour; SST requests/s
  calm medians 1,096 -> 1,942 -> 854. Read p99s flat (getRecord 1.8-2.5 ms, listRecords 2.2-3.3 ms calm medians).
  The automatic "getRecord p99 26 -> 168 ms, grows with data" verdict is driven by stall windows (see below), not by
  a drifting steady state.
- **(b) retired state dirs: unbounded, linear in reshards.** Exactly 1.5 dirs per op (150 after 100) and +64 MB per op
  (6.44 GB retired vs 4.30 GB live at the end, total/live 1.0 -> 2.5). Zero change in calm/restart segments.
- **(c) assign/ records: linear in reshards, cost not yet visible.** 65 -> 215 objects (+1.5 per op), LIST response
  13.9 -> 45.3 KB (+314 B per op), still 1 page. assign/ requests/s calm medians flat at 2/s (GETs 0); the MR
  "grows with history" on the means comes from spikes right after ops.
- **(d) dead-log fences: +1.00 log id per incarnation** (168 ids for 168 incarnations, 164 fence-only, r2 1.0),
  log/ delimiter LIST +80 B per incarnation (2.0 -> 13.6 KB). LIST pages/s and backfill time-to-first-event flat. At
  this rate one LIST page (1,000 keys) is crossed after ~1,000 restarts.
- **(e) metric cardinality: flat.** n1 series 1,524 -> 1,601 (+5%, all from label combinations first touched in the
  first reshard hour; restarts add none).
- **(f) tombstones: not visible.** live bytes/record 207 -> 123 B (compaction catches up), SSTs per live shard 69-103
  (grows in calm segments, compacted down at restarts; 69 at the end), delete-heavy listRecords calm p99 medians
  2.7 -> 3.8 -> 3.2 ms.

### Generic trends

- Write latency (calm medians): p50 22.5 -> 24.5 ms (+9% over 7 h); p99 52-59 -> 75-79 ms (+35-50%, rising in each
  calm segment: 50.5 -> 70.8 -> 79.9). Object-store requests per commit 1.0 -> 1.2 -> 1.9-2.0 after the reshard phase
  -> 1.4 in the last hour. Firehose lag p99 73 -> 96 ms. The ~22 ms write p50 floor is benchbox's MinIO fsync path.
- **Restart cost grows** (not in the auto verdicts; table below): SIGTERM exit median 0.9 -> 1.25 s over the first
  60 restarts, 1.62 s after the reshard phase, 1.75 s at the end; post-restart object-store requests/commit 2.0 ->
  2.7 -> 3.6 -> 4.1; worst write p99 within 40 s of a SIGTERM restart 0.27-0.40 s -> 0.68-0.88 s. The step happened
  across the reshard-only hours (between restart 60 and 61), so it tracks reshard history (more shards with external
  dbs / pinned parents to flush and reopen per takeover). Serving 0.25 s and converged ~5 s stay flat.
- Reshard op time flat (median 0.39-0.59 s; max 3.5 s in the mixed phase).
- n1 RSS 1.2 -> 6.4 GB: cache fill, not a leak. jemalloc allocated minus the repo cache is ~3.2-3.5 GB from 0.75 h
  on (block cache ~1.9 GB + base); the repo cache filled 0.17 -> 1.7 GB of its ~1.9 GB budget. Tokio tasks flat
  (~610-650).
- Stall windows (10 s samples with write p50 > 200 ms or p99 > 1 s): 30/14/9/14/37/29/22 per soak hour. 136 of 155
  are within 40 s of a restart or reshard (kill -9 failovers show the expected 7-8 s p99 = lease TTL; SIGTERM restarts
  0.3-0.9 s). 19 are op-free; the worst (09:58:40-55) is cluster-wide object-store latency (segment PUT p50 16 ->
  200-280 ms on all four nodes at once, commits/s halved), amplified by the generator: 288 worker threads cap
  in-flight requests, so commit latency above ~190 ms backs the open-loop queue up and client p50 reaches seconds.
  Op-free stalls rose 4 -> 6 -> 8 across the three calm segments, i.e. with data/compaction I/O on the shared disk.

### vlpds bugs found

1. **Client-visible 500 during split/merge right after a restart**: `InternalServerError "repo load failed: Closed
   error: db is closed"` (103 createRecord, 5 deleteRecord, 1 sync.getRecord). Only in the `mixed` phase, where a
   split/merge runs ~1 s after a restarted node converged; the moving shard's DB is closed under in-flight repo loads
   and the error is surfaced as a 500 instead of a retryable 503/PartitionUnavailable (the load balancer and the
   generator only retry 502/503/504). Also logged as `large-repo index scan failed: Closed error` and
   `oauth gc: Closed error: db is closed`. Evidence: `evidence/n*-closed-errors.log`; full node logs on benchbox in
   `~/vlpds-bench/results/soak-2026-10-02-benchbox-nodelogs/`.
2. **OutdatedCursor inside retention** (1 of 210 probes, 0.78 h, restart phase): n3 answered a 44.7 s-old cursor
   (retention 90 s) with `outdated` and a seq gap of 8.15e9, i.e. events in range were already pruned. Suspect
   retention of a just-dead incarnation's log. Worth a targeted test.
3. (harness-side, minor) one backfill probe at 6.5 h got `ConsumerTooSlow` reading at ~87k ev/s: the Python consumer,
   not the server.

No crashes, panics, data loss or stuck shards: 100/100 reshards done, every restart converged (4.1-9.3 s), firehose
1.00 events/commit, 0 out-of-order, 0 reconnects.

### What to fix first

1. The `db is closed` 500 (map Closed to retryable PartitionUnavailable / retry the load on the new owner): the only
   client-visible failure.
2. (b) retired-parent GC, together with (a) forced detach: 64 MB + 1.5 dirs per reshard is the only unbounded storage
   growth, and pinned parents are the likely cause of the restart-cost step (exit time and post-restart
   requests/commit up ~2x). (a) first if detach makes (b)'s GC trivially safe.
3. (d) fence GC, then (c): both linear but ~1,000 ops away from costing an extra LIST page per step.
4. (e)/(f): nothing to do at this horizon.

Side note: batch pipeline's 07:15 UTC run failed with "OpenJev never came up" (the OpenJev container never answered on
:8093). The soak's nodes were stopped and only the idle MinIO container (ports 9200/9201) was up; the same 07:15 run
also failed this way on Oct 01. Probably unrelated, but flagging it.

## Summary tables (`summary_tables.py .`, robust to stall windows)

### Per-hour medians of calm samples (no op in progress or within 40 s of one; 5 min warmup after each start)

| soak h | phase(s) | n | write p50 ms | write p99 ms | getRecord p99 ms | listRecords p99 ms | listRecords delete-heavy p99 ms | obj-store req/commit | state SST GET+HEAD / read | assign/ GETs/s | firehose lag p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0-1 | restart | 183 | 22.49 | 58.97 | 1.81 | 2.18 | 2.65 | 1.0 | 3.8 | 0.0 | 73.64 |
| 1-2 | calm,restart | 280 | 22.99 | 52.04 | 2.01 | 2.71 | 2.92 | 1.22 | 5.22 | 0.0 | 65.22 |
| 2-3 | reshard | 197 | 23.25 | 69.99 | 2.36 | 3.09 | 3.37 | 1.98 | 9.61 | 0.0 | 79.37 |
| 3-4 | calm,reshard | 278 | 23.41 | 75.36 | 2.46 | 3.25 | 3.79 | 1.88 | 9.36 | 0.0 | 85.47 |
| 4-5 | mixed | 152 | 23.44 | 60.72 | 2.04 | 2.33 | 3.09 | 1.75 | 8.38 | 0.0 | 75.35 |
| 5-6 | restart | 196 | 23.88 | 68.48 | 1.99 | 2.16 | 3.06 | 1.85 | 9.52 | 0.0 | 83.53 |
| 6-7 | calm,restart | 278 | 24.45 | 79.26 | 2.07 | 2.52 | 3.18 | 1.37 | 6.35 | 0.0 | 96.32 |

### Stall windows (10 s samples with write p50 > 200 ms or p99 > 1 s), per soak hour and phase

| soak h | samples | stalls in restart | in reshard | in calm | in mixed | of which within 30 s of an op |
|---|---|---|---|---|---|---|
| 0-1 | 359 | 30 | 0 | 0 | 0 | 30 |
| 1-2 | 360 | 10 | 0 | 4 | 0 | 10 |
| 2-3 | 360 | 0 | 9 | 0 | 0 | 8 |
| 3-4 | 360 | 0 | 8 | 6 | 0 | 8 |
| 4-5 | 360 | 0 | 0 | 0 | 37 | 37 |
| 5-6 | 360 | 29 | 0 | 0 | 0 | 29 |
| 6-7 | 359 | 14 | 0 | 8 | 0 | 14 |

### Restart cost by restart ordinal (bins of ~20)

| restarts # | soak h | SIGTERM exit s (med) | serving s (med) | converged s (med) | kill -9 share | max write p99 in 40 s after (med, SIGTERM) | req/commit in 40 s after (med) |
|---|---|---|---|---|---|---|---|
| 1-20 | 0.00-0.48 | 0.9 | 0.24 | 5.08 | 5/20 | 309.91 | 2.02 |
| 21-40 | 0.50-0.98 | 1.0 | 0.3 | 5.08 | 5/20 | 265.07 | 2.34 |
| 41-60 | 1.00-1.48 | 1.25 | 0.27 | 5.34 | 2/20 | 403.41 | 2.73 |
| 61-80 | 4.00-4.48 | 1.62 | 0.29 | 4.87 | 6/20 | 677.25 | 3.63 |
| 81-100 | 4.50-4.98 | 1.61 | 0.25 | 5.1 | 5/20 | 694.53 | 3.75 |
| 101-120 | 5.00-5.48 | 1.7 | 0.27 | 5.1 | 6/20 | 725.25 | 3.65 |
| 121-140 | 5.51-5.98 | 1.7 | 0.26 | 5.62 | 3/20 | 884.96 | 3.87 |
| 141-160 | 6.01-6.48 | 1.75 | 0.25 | 4.86 | 6/20 | 774.81 | 4.11 |

### Reshard ops by ordinal (bins of 10)

| reshards # | soak h | op secs (med / max) | converge s (med) | retired dirs after | retired GB after | pinned GB after | assign/ objects after | assign/ LIST B after |
|---|---|---|---|---|---|---|---|---|
| 1-10 | 2.00-2.23 | 0.39 / 1.0 | 0.27 | 15 | 0.54 | 0.54 | 80 | 17038 |
| 11-20 | 2.25-2.48 | 0.4 / 0.68 | 0.01 | 30 | 1.15 | 1.01 | 95 | 20173 |
| 21-30 | 2.50-2.73 | 0.41 / 0.82 | 0.51 | 45 | 1.59 | 1.21 | 110 | 23309 |
| 31-40 | 2.75-2.98 | 0.43 / 0.89 | 0.01 | 60 | 2.25 | 1.49 | 125 | 26444 |
| 41-50 | 3.00-3.23 | 0.48 / 0.83 | 0.26 | 75 | 2.82 | 1.62 | 140 | 29579 |
| 51-60 | 3.25-3.48 | 0.43 / 0.76 | 0.51 | 90 | 3.37 | 1.86 | 155 | 32714 |
| 61-70 | 4.00-4.23 | 0.59 / 3.46 | 0.52 | 105 | 4.34 | 1.88 | 170 | 35849 |
| 71-80 | 4.25-4.48 | 0.42 / 1.33 | 0.52 | 120 | 4.91 | 2.12 | 185 | 38986 |
| 81-90 | 4.50-4.73 | 0.43 / 2.18 | 0.53 | 135 | 5.62 | 2.41 | 200 | 42149 |
| 91-100 | 4.76-4.98 | 0.47 / 0.93 | 0.53 | 150 | 6.44 | 2.73 | 215 | 45298 |

### n1 memory (n1 is restarted only by the window-2 resume at 4.83 h)

| soak h | RSS MB | jemalloc allocated MB | repo cache MB | in-memory caches MB | tokio tasks | /metrics series |
|---|---|---|---|---|---|---|
| 0.00 | 1200.1 | 749.6 | 165.7 | 1.67 | 841.0 | 1486 |
| 0.25 | 3069.3 | 2324.1 | 126.7 | 28.2 | 655.0 | 1523 |
| 0.75 | 4423.4 | 3485.8 | 322.5 | 59.27 | 615.0 | 1525 |
| 1.25 | 4569.6 | 3550.3 | 409.4 | 26.61 | 611.0 | 1548 |
| 1.75 | 4894.5 | 3915.8 | 705.2 | 52.39 | 607.0 | 1548 |
| 2.25 | 5044.0 | 4044.1 | 814.6 | 67.04 | 611.0 | 1620 |
| 2.75 | 5413.5 | 4428.7 | 1137.6 | 79.32 | 613.0 | 1623 |
| 3.25 | 5480.8 | 4299.0 | 970.0 | 75.79 | 629.0 | 1623 |
| 3.75 | 5588.3 | 4595.1 | 1265.8 | 68.01 | 647.0 | 1623 |
| 4.25 | 5956.4 | 4481.0 | 1027.3 | 76.87 | 639.0 | 1623 |
| 4.75 | 6293.7 | 4493.9 | 970.7 | 15.89 | 612.0 | 1630 |
| 5.25 | 5472.9 | 4269.5 | 1070.4 | 51.39 | 605.0 | 1598 |
| 5.75 | 5915.9 | 4601.6 | 1295.5 | 60.87 | 607.0 | 1601 |
| 6.25 | 6179.0 | 5017.9 | 1716.9 | 32.8 | 622.0 | 1601 |
| 6.75 | 6324.1 | 5092.2 | 1710.0 | 58.71 | 605.0 | 1601 |

### Client errors after retries, by phase

| phase | op | errors |
|---|---|---|
| mixed | r_syncGetRecord | 1 |
| mixed | w_create | 103 |
| mixed | w_delete | 5 |

First error texts: `w_create`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'; `w_delete`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'; `r_syncGetRecord`: 500 b'{"error":"InternalServerError","message":"repo load failed: Closed error: db is closed"}'
