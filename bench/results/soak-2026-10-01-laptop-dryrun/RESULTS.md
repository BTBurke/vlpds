# vlpds soak: soak-dry

Driver `bench/soak/soak.py` (the dry-run flags in NOTES/commands), binaries: isolated build of 3cdbfac, MinIO `http://127.0.0.1:9200` prefix `soak-dry`. 3 native nodes (ports 2700-2702; n1..n1 never restarted), initial 16 shards (kept within +-2), `--log-retention 60s --slatedb-checkpoint-lifetime 1m --slatedb-gc-min-age 1m --lease-ttl-ms 10000` .

Load: 150.0 writes/s (Zipf s=1.0 cap 0.002 over 3,000 bulk repos x 5 records; creates/deletes/updates target 95.9/3.8/0.3 %, every 25th repo delete-heavy at 45% deletes), 30.0 reads/s over 6 mid repos x 1000 records + fresh records, one firehose subscriber, a cursor backfill every 60 s (30 s back). Cycle `restart:240,calm:120,reshard:240,calm:120` on the soak clock; restarts every 30 s in `restart` phases (30% kill -9), split/merge every 25 s in `reshard` phases.

## History reached

Soak clock 0.42 h; 22 node incarnations (19 restarts: 15 SIGTERM, 4 kill -9; 1 cluster starts), 19 reshard ops (10 splits, 9 merges, 0 failed), 226,208 client-acked writes (217,122 creates, 8,414 deletes, 672 updates). Layout now 17 shards, next id 45.

Restarts: median exit 0.31 s (SIGTERM), serving 0.2 s, converged 4.59 s; first/last 5 converge: [5.09, 5.08, 4.07, 4.07, 6.07] / [4.55, 6.07, 4.07, 4.58, 6.08].
Reshards: 19/19 done; secs first/last 5: [0.06, 0.09, 0.07, 0.08, 0.04] / [0.07, 0.59, 0.09, 0.34, 0.13]. Errors: []
Backfills: 25 probes; first-event s first/last 5: [0.189, 0.343, 0.542, 0.0, 0.001] / [0.001, 0.001, 0.001, 0.0, 0.258]; outdated 0; errors 0.
Client errors (after retries): none; retried on refused connections 1,306, on 502/503/504 2,043. Firehose reconnects 0, out-of-order 0, OutdatedCursor 0.

## Verdict per suspected growth path

A metric *grows with history* when a multiple regression on cumulative incarnations, reshard ops and commits attributes >= 10% growth over the run to incarnations or reshards with |t| >= 3; *grows with data* when only commits do; *flat* when the last-10% mean is within 10% of the first-10% mean. Latency/request metrics use calm samples only (no restart/reshard in progress or within 20 s after converging); the first 120 s are skipped. Samples are autocorrelated, so t values are optimistic: read the effect sizes.

| Path | Metric | n | first -> last | change | verdict |
|---|---|---|---|---|---|
| a: Parents pinned by children -> read amplification | retired bytes still referenced by live manifests | 139 | 0 -> 37,257,362 | 200% | grows with history (reshard ops: +323%) |
| a: Parents pinned by children -> read amplification | live shards with external SSTs | 139 | 0 -> 9.69 | 161% | grows with history (reshard ops: +300%) |
| a: Parents pinned by children -> read amplification | external dbs per live shard | 139 | 0 -> 0.965 | 172% | grows with history (reshard ops: +345%) |
| a: Parents pinned by children -> read amplification | state SST GET+HEAD / client read | 52 | 1.71 -> 2.67 | 55.8% | changes +56%, attribution unclear; best single fit: reshard ops r2 0.288 |
| a: Parents pinned by children -> read amplification | getRecord p99 (ms) | 52 | 10.2 -> 5.39 | -47.4% | falls (-47%) |
| a: Parents pinned by children -> read amplification | listRecords p99 (ms) | 52 | 15 -> 11.4 | -23.8% | falls (-24%) |
| b: Retired parents' state dirs never deleted | retired state dirs | 139 | 0 -> 28 | 199% | grows with history (reshard ops: +183%) |
| b: Retired parents' state dirs never deleted | retired state bytes | 139 | 0 -> 67,423,801 | 212% | grows with history (reshard ops: +179%) |
| b: Retired parents' state dirs never deleted | state bytes / live-shard bytes | 139 | 1 -> 2.04 | 104% | grows with history (reshard ops: +245%) |
| c: Retired assign/ records grow the per-step LIST | assign/ objects | 139 | 17 -> 46 | 171% | grows with history (reshard ops: +165%) |
| c: Retired assign/ records grow the per-step LIST | assign/ LIST response bytes | 139 | 3,860 -> 10,008 | 159% | grows with history (reshard ops: +154%) |
| c: Retired assign/ records grow the per-step LIST | assign/ requests/s (all ops) | 52 | 1.84 -> 1.5 | -18.5% | falls (-18%) |
| c: Retired assign/ records grow the per-step LIST | assign/ GETs/s | 52 | 0.34 -> 0 | -100% | falls (-100%) |
| d: Dead-log fences grow the log/ LIST with every restart | log ids under log/ (incarnations kept) | 139 | 9.62 -> 19.9 | 107% | grows with history (incarnations: +143%) |
| d: Dead-log fences grow the log/ LIST with every restart | fence-only dead logs | 139 | 0.538 -> 16 | 2,871% | grows with data (+7334%) |
| d: Dead-log fences grow the log/ LIST with every restart | log/ delimiter LIST response bytes | 139 | 1,072 -> 1,928 | 79.9% | grows with history (incarnations: +107%) |
| d: Dead-log fences grow the log/ LIST with every restart | log/ LIST pages/s (wrapper) | 52 | 0.76 -> 0.1 | -86.8% | falls (-87%) |
| d: Dead-log fences grow the log/ LIST with every restart | backfill time to first event (s) | 25 | 0.266 -> 0.129 | -51.5% | falls (-52%) |
| e: Per-log metric labels (cardinality per restart) | n1 /metrics series | 139 | 1,496 -> 1,587 | 6.1% | flat |
| f: Tombstones linger under size-tiered compaction | live-shard bytes / live record | 139 | 370 -> 297 | -19.7% | falls (-20%) |
| f: Tombstones linger under size-tiered compaction | listRecords delete-heavy p99 (ms) | 52 | 6.91 -> 6.87 | -0.6% | flat |
| f: Tombstones linger under size-tiered compaction | SSTs per live shard | 139 | 26.7 -> 72 | 169% | grows with data (+850%) |
| -: Generic: latency, requests/commit, memory | write p99 (ms) | 52 | 17.5 -> 12.2 | -30.2% | falls (-30%) |
| -: Generic: latency, requests/commit, memory | object-store requests / commit | 52 | 1.99 -> 2.7 | 35.6% | changes +36%, attribution unclear; best single fit: incarnations r2 0.005 |
| -: Generic: latency, requests/commit, memory | n1 RSS (MB) | 139 | 148 -> 226 | 53.3% | grows with history (incarnations: +577%) |
| -: Generic: latency, requests/commit, memory | n1 tokio alive tasks | 139 | 201 -> 206 | 2.5% | flat |
| -: Generic: latency, requests/commit, memory | firehose lag p99 (ms) | 52 | 15.2 -> 12.3 | -18.7% | falls (-19%) |

## Trends (all metrics)

Slopes from simple linear fits (r2 in parentheses); multiple-regression effects are the growth over the run attributed to each counter, % of the first-10% mean (t).

| Metric | mode | per hour | per 100 incarnations | per 100 reshards | per 1M commits | MR effect: incarn. / reshards / commits | MR r2 | verdict |
|---|---|---|---|---|---|---|---|---|
| write p50 (ms) | calm | 2.24 (0.169) | 8.97 (0.345) | 2.34 (0.067) | 4.14 (0.169) | +54% (2.3) / +21% (0.5) / -70% (-1.0) | 0.429 | changes +12%, attribution unclear; best single fit: incarnations r2 0.345 |
| write p99 (ms) | calm | -0.472 (0.0) | 3.46 (0.0) | 2.6 (0.001) | -0.865 (0.0) | +177% (3.2) / +356% (3.3) / -585% (-3.3) | 0.186 | falls (-30%) |
| getRecord p99 (ms) | calm | -9.25 (0.092) | -18.5 (0.047) | -15.6 (0.094) | -17.1 (0.092) | +81% (1.6) / +128% (1.3) / -263% (-1.6) | 0.141 | falls (-47%) |
| listRecords p99 (ms) | calm | -5.27 (0.021) | -13.5 (0.017) | -7.69 (0.016) | -9.76 (0.021) | +23% (0.5) / +51% (0.6) / -92% (-0.6) | 0.027 | falls (-24%) |
| sync.getRecord p99 (ms) | calm | 0.603 (0.0) | 7.93 (0.01) | -0.984 (0.0) | 1.12 (0.0) | +65% (1.1) / +72% (0.6) / -155% (-0.9) | 0.045 | falls (-11%) |
| getRepo p50 (ms) | calm | 1.23 (0.051) | 2.57 (0.028) | 2.25 (0.062) | 2.28 (0.051) | +6% (0.2) / +29% (0.5) / -25% (-0.3) | 0.064 | flat |
| listRecords delete-heavy p99 (ms) | calm | -1.55 (0.005) | -0.239 (0.0) | -3.77 (0.01) | -2.87 (0.005) | +34% (0.6) / +23% (0.2) / -76% (-0.4) | 0.025 | flat |
| object-store requests / commit | calm | 0.962 (0.004) | 2.96 (0.005) | 1.66 (0.004) | 1.78 (0.004) | +105% (0.7) / +202% (0.7) / -322% (-0.7) | 0.015 | changes +36%, attribution unclear; best single fit: incarnations r2 0.005 |
| state SST GET+HEAD / client read | calm | 2.64 (0.169) | 2.77 (0.024) | 5.74 (0.288) | 4.88 (0.169) | -92% (-1.8) / +21% (0.2) / +152% (0.9) | 0.435 | changes +56%, attribution unclear; best single fit: reshard ops r2 0.288 |
| manifest requests/s | calm | 2.56 (0.012) | 3.69 (0.003) | 5.57 (0.02) | 4.74 (0.012) | +4% (0.2) / +17% (0.6) / -18% (-0.4) | 0.028 | flat |
| SST requests/s | calm | 78.4 (0.159) | 80.7 (0.021) | 171 (0.274) | 145 (0.159) | -85% (-1.7) / +22% (0.2) / +137% (0.8) | 0.417 | changes +51%, attribution unclear; best single fit: reshard ops r2 0.274 |
| assign/ requests/s (all ops) | calm | -0.0954 (0.0) | 1.83 (0.003) | -0.87 (0.002) | -0.176 (0.0) | +76% (0.7) / +56% (0.2) / -157% (-0.4) | 0.023 | falls (-18%) |
| assign/ GETs/s | calm | -0.0972 (0.0) | 1.77 (0.003) | -0.855 (0.002) | -0.179 (0.0) | +397% (0.6) / +287% (0.2) / -817% (-0.4) | 0.023 | falls (-100%) |
| log/ LIST pages/s (wrapper) | calm | -0.527 (0.002) | 1.3 (0.002) | -1.48 (0.006) | -0.974 (0.002) | +429% (1.6) / +594% (1.1) / -1176% (-1.4) | 0.063 | falls (-87%) |
| retain/ requests/s | calm | -0.815 (0.014) | 1.48 (0.006) | -2.42 (0.045) | -1.51 (0.014) | +619% (1.5) / +621% (0.7) / -1489% (-1.1) | 0.223 | falls (-78%) |
| assign/ objects | all | 81.7 (0.923) | 191 (0.666) | 152 (0.999) | 151 (0.923) | -5% (-1.7) / +165% (54.1) / +10% (1.8) | 0.999 | grows with history (reshard ops: +165%) |
| assign/ LIST response bytes | all | 17,327 (0.923) | 40,524 (0.666) | 32,280 (0.999) | 32,095 (0.923) | -4% (-1.7) / +154% (54.1) / +9% (1.8) | 0.999 | grows with history (reshard ops: +154%) |
| log ids under log/ (incarnations kept) | all | 34 (0.876) | 99.9 (1.0) | 53.1 (0.667) | 62.9 (0.876) | +143% (105.5) / -2% (-1.2) / +4% (1.4) | 1.0 | grows with history (incarnations: +143%) |
| fence-only dead logs | all | 39.1 (0.851) | 97.9 (0.704) | 67 (0.78) | 72.4 (0.851) | -2266% (-4.6) / -2254% (-4.1) / +7334% (7.2) | 0.871 | grows with data (+7334%) |
| log/ delimiter LIST response bytes | all | 2,820 (0.876) | 8,294 (1.0) | 4,407 (0.668) | 5,223 (0.876) | +107% (105.5) / -1% (-1.2) / +3% (1.5) | 1.0 | grows with history (incarnations: +107%) |
| objects in live logs | all | 4,280 (0.016) | 1,319 (0.0) | 11,908 (0.039) | 7,957 (0.016) | -85% (-1.6) / +27% (0.5) / +66% (0.6) | 0.107 | changes +30%, attribution unclear; best single fit: reshard ops r2 0.039 |
| retain/ reports | all | -0.872 (0.004) | 7.82 (0.041) | -4.94 (0.039) | -1.61 (0.004) | +372% (11.1) / +165% (4.4) / -532% (-7.7) | 0.609 | falls (-34%) |
| retired state dirs | all | 79.6 (0.928) | 187 (0.677) | 148 (0.996) | 148 (0.928) | -5% (-0.8) / +183% (26.2) / +23% (1.8) | 0.996 | grows with history (reshard ops: +183%) |
| retired state bytes | all | 197,055,874 (0.923) | 466,566,714 (0.682) | 363,409,043 (0.979) | 365,013,359 (0.923) | -11% (-0.7) / +179% (10.1) / +54% (1.7) | 0.98 | grows with history (reshard ops: +179%) |
| retired bytes still referenced by live manifests | all | 97,986,751 (0.626) | 189,652,743 (0.309) | 202,648,154 (0.835) | 181,505,607 (0.626) | -137% (-5.0) / +323% (10.4) / -21% (-0.4) | 0.943 | grows with history (reshard ops: +323%) |
| live shards with external SSTs | all | 22.8 (0.546) | 42.2 (0.248) | 48.3 (0.767) | 42.2 (0.546) | -82% (-3.1) / +300% (10.1) / -103% (-1.9) | 0.912 | grows with history (reshard ops: +300%) |
| external dbs per live shard | all | 2.23 (0.402) | 3.35 (0.12) | 5.08 (0.654) | 4.12 (0.402) | -219% (-9.0) / +345% (12.7) / -18% (-0.4) | 0.95 | grows with history (reshard ops: +345%) |
| state bytes / live-shard bytes | all | 3.25 (0.761) | 7.45 (0.527) | 6.28 (0.886) | 6.02 (0.761) | +79% (4.1) / +245% (11.2) / -214% (-5.3) | 0.91 | grows with history (reshard ops: +245%) |
| live-shard bytes / live record | all | -398 (0.518) | -1,091 (0.514) | -712 (0.518) | -737 (0.518) | -116% (-7.9) / -129% (-7.8) / +207% (6.8) | 0.677 | falls (-20%) |
| SSTs per live shard | all | 77 (0.398) | 142 (0.18) | 147 (0.453) | 142 (0.398) | -454% (-10.4) / -293% (-6.0) / +850% (9.4) | 0.699 | grows with data (+850%) |
| manifest objects (live shards) | all | 1,359 (0.446) | 3,924 (0.49) | 1,938 (0.283) | 2,517 (0.446) | -113% (-4.3) / -229% (-7.8) / +418% (7.8) | 0.651 | grows with data (+418%) |
| n1 /metrics series | all | 272 (0.749) | 665 (0.587) | 502 (0.791) | 505 (0.749) | +4% (2.6) / +9% (5.2) / -7% (-2.2) | 0.803 | flat |
| n1 RSS (MB) | all | 662 (0.155) | 2,295 (0.246) | 917 (0.093) | 1,227 (0.156) | +577% (3.5) / +174% (0.9) / -554% (-1.6) | 0.29 | grows with history (incarnations: +577%) |
| n1 tokio alive tasks | all | 22.6 (0.063) | 49 (0.039) | 46.3 (0.083) | 41.9 (0.064) | +5% (0.7) / +14% (1.7) / -16% (-1.1) | 0.094 | flat |
| n1 in-memory caches (MB) | all | 1.24 (0.736) | 3.39 (0.725) | 2.04 (0.625) | 2.3 (0.736) | +15% (1.5) / -3% (-0.3) / +26% (1.3) | 0.755 | changes +39%, attribution unclear; best single fit: commits r2 0.736 |
| n1 firehose merge queues (MB) | all | 0.000708 (0.009) | 0.0018 (0.007) | 0.00137 (0.01) | 0.00131 (0.009) | +1211% (0.7) / +1666% (0.8) / -2554% (-0.7) | 0.014 | flat |
| firehose lag p99 (ms) | calm | 3.64 (0.004) | 13.9 (0.008) | 8.15 (0.007) | 6.75 (0.004) | +168% (2.6) / +333% (2.6) / -542% (-2.6) | 0.131 | falls (-19%) |
| backfill time to first event (s) | probe | -0.376 (0.12) | -1.09 (0.154) | -0.633 (0.098) | -0.697 (0.12) | -241% (-1.2) / -156% (-0.8) / +329% (0.9) | 0.184 | falls (-52%) |

## Growth by phase segment

State-like metrics: summed change inside each phase type's segments (last 3 minus first 3 samples of a segment). Growth concentrated in `restart` segments is per-incarnation, in `reshard` segments per split/merge, in `calm` segments time/data-driven (writes never stop). Latency/request metrics: median of each calm segment, in order (a drifting steady state shows as a rising sequence).

| Metric | sum delta in restart (segments) | sum delta in calm (segments) | sum delta in reshard (segments) |
|---|---|---|---|
| assign/ objects | 0 (3) | 0 (4) | 24.7 (2) |
| assign/ LIST response bytes | 0 (3) | 0 (4) | 5,229 (2) |
| log ids under log/ (incarnations kept) | 11.3 (3) | 0 (4) | 0 (2) |
| fence-only dead logs | 0.667 (3) | 8.67 (4) | 2.33 (2) |
| log/ delimiter LIST response bytes | 942 (3) | 0 (4) | 0 (2) |
| objects in live logs | -5,496 (3) | 15,765 (4) | 14,236 (2) |
| retain/ reports | 7.67 (3) | -4.67 (4) | -1 (2) |
| retired state dirs | 0 (3) | 1 (4) | 22 (2) |
| retired state bytes | 96,659 (3) | 4,667,679 (4) | 57,915,654 (2) |
| retired bytes still referenced by live manifests | -20,608,879 (3) | 3,379,898 (4) | 52,471,969 (2) |
| live shards with external SSTs | -4.67 (3) | -0.333 (4) | 11.3 (2) |
| external dbs per live shard | -0.903 (3) | 0.16 (4) | 1.56 (2) |
| state bytes / live-shard bytes | 0.136 (3) | -0.366 (4) | 1.44 (2) |
| live-shard bytes / live record | -107 (3) | 222 (4) | -210 (2) |
| SSTs per live shard | -32.8 (3) | 67 (4) | 4.47 (2) |
| manifest objects (live shards) | 279 (3) | 811 (4) | -581 (2) |
| n1 /metrics series | 11 (3) | 0 (4) | 63.3 (2) |
| n1 RSS (MB) | 214 (3) | 191 (4) | -332 (2) |
| n1 tokio alive tasks | 15 (3) | -1.33 (4) | 2.67 (2) |
| n1 in-memory caches (MB) | 0.527 (3) | 0.09 (4) | 0.0333 (2) |
| n1 firehose merge queues (MB) | 0 (3) | 0 (4) | 0 (2) |

| Metric (calm segments) | medians in order |
|---|---|
| write p50 (ms) | 2.97 -> 3.21 -> 4.44 -> 3.67 |
| write p99 (ms) | 14.1 -> 14.1 -> 13.3 -> 12.5 |
| getRecord p99 (ms) | 5.55 -> 7.4 -> 7.72 -> 5.22 |
| listRecords p99 (ms) | 10.4 -> 7.8 -> 7.84 -> 7.94 |
| sync.getRecord p99 (ms) | 6.43 -> 8.19 -> 8.36 -> 5.96 |
| getRepo p50 (ms) | 2.81 -> 3.56 -> 3.28 -> 3.35 |
| listRecords delete-heavy p99 (ms) | 5.97 -> 7.22 -> 8.08 -> 4.7 |
| object-store requests / commit | 1.91 -> 3.85 -> 2.12 -> 3.15 |
| state SST GET+HEAD / client read | 1.6 -> 2.41 -> 1.48 -> 3.14 |
| manifest requests/s | 28.5 -> 30.1 -> 28.1 -> 30 |
| SST requests/s | 51 -> 76.3 -> 47.8 -> 95.7 |
| assign/ requests/s (all ops) | 1.5 -> 1.5 -> 1.5 -> 1.5 |
| assign/ GETs/s | 0 -> 0 -> 0 -> 0 |
| log/ LIST pages/s (wrapper) | 0.1 -> 0.3 -> 0.1 -> 0.2 |
| retain/ requests/s | 0.1 -> 0.1 -> 0.1 -> 0.1 |
| firehose lag p99 (ms) | 14.1 -> 14.9 -> 13.6 -> 12.6 |

Regressor correlations (calm/all samples of the first metric): {'h.incarnations~h.reshards': 0.745, 'h.incarnations~h.commits': 0.906, 'h.reshards~h.commits': 0.953}. Above ~0.95 the attribution between those counters is unreliable (run longer cycles, or a schedule with uneven storms).

## Last sample

`lat.w.p50` 2.91 | `lat.w.p99` 20.4 | `lat.r.p99` 7.03 | `req.per_commit` 7.49 | `req.total_s` 1,095 | `assign.keys` 46 | `assign.list_bytes` 10008 | `log.ids` 22 | `log.fence_only` 16 | `log.list_bytes` 2100 | `retain.keys` 3 | `state.dirs` 45 | `state.retired_dirs` 28 | `state.live_bytes` 51467619 | `state.retired_bytes` 67454237 | `state.pinned_bytes` 29103496 | `state.total_over_live` 2.31 | `state.live_with_ext` 8 | `state.ext_refs_mean` 0.71 | `state.sst_per_shard_mean` 49.6 | `node.n1.rss_mb` 277 | `node.n1.series` 1587 | `node.n1.tasks` 245 | `fh.lag_p99` 1,963

## Dry-run notes (hand-written)

Laptop (M-series), HEAD 3cdbfac built in isolation (git archive + own CARGO_TARGET_DIR), native MinIO shared with
the cost-model lane's benchmark (so `minio.*` counters include its traffic; use the `req.*` wrapper counters).
25 min of soak clock is far too short for attribution between the three history counters (reshards~commits r =
0.95); the phase-segment table is the reliable view here. Early signals per suspected path:

- **(a) parent pinning -> read amplification: present, read cost inconclusive.** After 19 reshards 9-10 of 16-17
  live shards still reference a parent (external dbs per live shard ~1, max 2), and 29-37 MB of retired dirs are
  still referenced by live manifests: size-tiered compaction did not detach children within 25 min. SST
  GET+HEAD per client read rose 1.6 -> 2.4 -> 1.5 -> 3.1 across calm segments (noisy; the block cache hides most
  of it at this size); read p99s flat/falling. Needs the long run with a bigger dataset than the block cache.
- **(b) retired state dirs: grows with reshard ops, linearly.** +1.5 dirs per reshard (28 dirs after 19 ops), all
  of the growth inside reshard segments; retired bytes 67 MB vs 51 MB live (total/live 1.0 -> 2.3). Unbounded at
  HEAD: no retired-parent GC.
- **(c) retired assign/ records: grows with reshard ops.** assign/ 17 -> 46 objects (+1.5 per op), LIST response
  +215 B per op; still 1 LIST page, so per-step request counts are flat until ~1,000 shard ids. Grows only in
  reshard segments.
- **(d) dead-log fences: grows with incarnations, exactly +1 log id per restart** (22 ids for 22 incarnations;
  16 are fence-only, the rest wait for retention). log/ delimiter LIST +~45 B per incarnation; backfill time to
  first event flat (< 0.3 s). Pages flat until ~1,000 incarnations, then every list_logs (backfill, retention)
  pays an extra LIST page per 1,000 restarts.
- **(e) metric cardinality: flat at HEAD** (retention metrics are labelled own/dead, not per log). n1's series
  1,496 -> 1,587, growth from label combinations first touched early; 1,583-1,587 over the last 10 min (new series are now logged per sample).
- **(f) tombstones: not visible yet.** listRecords on delete-heavy repos flat (~5-8 ms p99); live bytes/record
  fell (compaction caught up). SSTs per live shard rose 27 -> 72 (grows in calm segments, drops at restarts),
  and manifest objects grow in calm segments: compaction/GC lag under the 1 min checkpoint/GC windows, not
  tombstones per se. Watch on benchbox.
- **Other:** write p50 ~3 ms / p99 ~12-14 ms in every calm segment (flat); firehose 1.00 events per commit, 0
  out-of-order, lag p99 ~13-15 ms; restart -> converged 4-6 s throughout (no growth); reshard op time 0.04 s
  early vs 0.1-0.6 s late (small sample, watch). n1 (never restarted) RSS climbed 67 -> 745 MB over 20 min then
  fell to 117 MB with no restart (cache/ring fill and release, jemalloc stats are now sampled to tell which).
  Object-store requests per commit ~2-3 in calm samples, spikes to 7+ around restarts (replay + opens).

## Files

`samples.jsonl` (one row per sample: load latency per op, requests by op/component, LIST sizes, state/ breakdown, per-node gauges + SlateDB gauges, history counters), `samples.csv` (plots-ready subset), `events.jsonl` (restarts, reshards, backfill probes, starts/stops), `analysis.json` (fits). Regenerate: `bench/soak/soak.py report <same flags>`.
