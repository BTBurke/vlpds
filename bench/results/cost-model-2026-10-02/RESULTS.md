# vlpds object-store cost model: requests + storage on S3 / GCS / R2 (2026-10-01/02)

**Question:** what would the object store cost per month if all of Bluesky's current write load ran on
vlpds? Ops are counted on the wire by a new wrapper, measured at the real load profile, fitted to a
model, validated against runs it was not fitted on, and priced at current list prices. Blobs are excluded
except for one rough line at the end.

## Headline

Prices are monthly list prices: S3 Standard us-east-1, GCS Standard regional, R2 Standard. Everything
runs in-region, so egress is $0.

| config | S3 | GCS | R2 | what dominates |
|---|---|---|---|---|
| **Bluesky today, 3 nodes / 256 shards, current defaults** | **$3,290** | **$3,268** | **$2,936** | segment PUTs 33%, checkpoint+compaction 33%, SlateDB polling GETs 27%, storage 3% |
| Bluesky today, 8 nodes / 1,024 shards, defaults | $10,256 | $10,235 | $9,237 | polling GETs (x4 shards) |
| **Bluesky today, 3 / 256, tuned knobs** (manifest poll 10 s, compactor polls 30 s, checkpoint 30 s, 100 ms segment linger) | **$1,143** | **$1,121** | **$1,003** | storage ~10%, rest split roughly evenly |
| Sizing scenario today (50 M repos, 25 B records, 2k/s peak), 3 / 256, defaults / tuned | $3,354 / $1,203 | $3,328 / $1,177 | $2,988 / $1,051 | same as Bluesky today |
| Sizing 100x writes (200k/s peak), 8 / 1,024, defaults / tuned (+ K=1) | $19,599 / $10,615 | $18,686 / $9,702 | $16,486 / $8,397 | storage of ~209 TB, segment PUTs, polling |

> **Defaults changed after these runs** (section "Defaults changed" at the end): manifest poll 10 s,
> compactor/worker slow polls 30 s, idle checkpoints skipped. With them the model prices Bluesky today
> at 3 nodes / 256 shards at **$2,521 (S3) / $2,500 (GCS) / $2,244 (R2)**, and 8 / 1,024 at $7,182 S3.
> The tables below are the measured runs and the projections at the *previous* defaults.

How the costs scale, in order of impact:
1. **Requests cost far more than storage at Bluesky's write rate.** State plus log is ~4.9 TB
   (~$110/mo on S3).
2. **SlateDB polls are a per-shard fixed cost.** They don't depend on load: 3.26 GETs/s per shard
   = 835/s at 256 shards and 3,300/s at 1,024 shards.
3. **Checkpoint flushes are a per-shard cost too, once a node is writing.** Each one is an L0 SST PUT
   plus a manifest CAS and the compactions it later triggers: ~4.4 Class A requests per flush.
4. **Segment PUTs are paid per node, not per commit.** A node's log PUTs once per PUT round trip
   whenever anything is queued: ~27/s per node at 345 commits/s and still 27/s at 894 commits/s.

Shard count and node count drive the bill. Write rate barely does until a node passes about 20k commits/s.

## Inputs (real load)
Records and ops come from ClickHouse on 2026-10-01: `default.repo_records` (firehose tail, Dec 2025 to
now, with an `operation` column) and `default.crawl_records` (backfill crawl, Feb 19 2026).

| quantity | value | source |
|---|---|---|
| record ops/day (2026-09-24..30) | creates 27.7 M (25.8–28.9 M), updates 0.09 M, deletes 1.08 M (3.8%): **28.83 M/day = 334/s avg** | `SELECT toDate(indexed_at) d, countIf(operation='create'), countIf(operation='update'), countIf(operation='delete'), count() FROM default.repo_records WHERE indexed_at >= '2026-09-24' AND indexed_at < '2026-10-01' GROUP BY d` (daily totals 26.9–30.1 M) |
| hourly shape | min 0.48x, peak hour 1.26x avg (~420/s); minute bursts assumed ~2x peak hour | likes by hour (coordinator query on repo_records) |
| commits | **each op is modeled as one commit** (applyWrites batching is rare) | assumption |
| records today | crawl 17.75 B + tail creates since the crawl 6.44 B − tail deletes 0.254 B ≈ **23.9 B** | crawl_records + repo_records |
| repos | PLC: 89.9 M DIDs, 56.0 M on *.bsky.network PDSes, 33.7 M elsewhere; the crawl found 39.0 M repos with data. The model uses **56 M** (everyone hosted by Bluesky), with a 90 M sensitivity line | plc table / crawl_repos |
| state bytes | 154.2 B/record + 323 B/repo (zstd SSTs) | bench/results/storage-2026-10-02 |
| log bytes | 5,370 B/commit uncompressed, ~2,700 B stored (zstd 1, ~2x) | storage-2026-10-02, DESIGN "Log compression" |

## Method

### Instrumentation (`src/objstats.rs`, `src/store.rs`, `src/metrics.rs`)
- **Where it counts.** `Store::counted(client)` wraps the raw `ObjectStore` of both pools (`log` and
  `state`) in `server::build`. It sits at the bottom of the stack: SlateDB's retries, hedged segment
  PUTs and the compactor are all counted, and disk-cache hits never reach it. That matches what a bill
  counts.
- **Op labels.** Requests are labeled by billable op:
  - `put` (overwrite), `put_create` (If-None-Match), `put_cas` (If-Match)
  - `get`, `get_range`, `head`
  - `list`: one per 1,000-key page
  - `delete` (per object), `delete_batch` (per bulk DeleteObjects request of ≤1,000 keys)
  - `copy`, `mpu_create/part/complete/abort`
- **Component labels.** Each request is also labeled by key component, taken from the path:
  - `log_segment`, `retention_report`
  - `state_manifest`, `state_sst`, `state_compactions`, `state_wal`, and `state_gc_boundary` (SlateDB's
    `gc/*.boundary` files; see below)
  - `ctl_lease`, `ctl_assign`, `ctl_writer`, `account_index`, `blob`
- **Metrics.** `vlpds_object_store_requests_total{op,component,client}` and
  `vlpds_object_store_bytes_total{dir,component,client}`. Labels are resolved per request; there is no
  extra I/O. SlateDB's own `slatedb_object_store_request_count_total{component=db|gc|compactor}` says
  which SlateDB part issued a request.
- **Injected latency (bench only).** `VLPDS_INJECT_STATE_MS=<read>,<write>[,<sigma>]` adds lognormal
  latency to every state-pool request. The checkpoint and compaction loops are paced by call latency,
  so op counts on a local MinIO depend on it. Segment PUTs keep `--inject-put-ms`.

### Runs (`measure.py`, laptop, local MinIO, shared machine; `nice`)
- **Latency.** Segment PUTs 30 ms median, lognormal σ 0.5 (mean 34 ms, ~1% hedged at 100 ms). State
  pool 20 ms reads / 30 ms writes, σ 0.5.
- **Node settings.** Release build with defaults: 256 shards, K=4, 8 MiB segments, 10 s checkpoints,
  zstd SSTs and segments, adaptive compaction polling at 5 s while L0 is shallow, GC min age 10 min,
  checkpoint lifetime 1 h. The node also had a disk cache dir. Log retention was 20 min instead of 72 h,
  so retention passes delete during the run; their LIST pattern doesn't depend on the window.
- **Population.** 1 M repos created in bulk with the real records-per-repo distribution scaled 1/32:
  16.0 M records, 2.7 GB of SSTs. A 1,024-shard run used 100 k repos.
- **Load.** Open-loop loadgen in sim mode: a 50 k-repo active window, with 12 new repos/s entering it
  (≈ 1 M distinct writers/day). 98% creates, 2% deletes.
- **Phases.** Each ran 5–20 min:
  - 1 node, 256 shards: fresh idle, avg 345/s, peak 440/s, burst 900/s, avg again, plus idle after writes
  - 1 node, 1,024 shards: idle and avg
  - 3 nodes, 256 shards: join (handoffs), idle, avg
- **Rates.** Every node's /metrics was scraped every 30 s into `raw*.jsonl`. Rates are last minus first
  snapshot per phase.
- **Bench issues.**
  - A ~5 s MinIO stall on the shared laptop at 15:48 fail-stopped the first 1-node run (10 s lease).
    Its idle phase and the first 13.5 min of avg are kept, and the rest was rerun as `one2` with a 30 s
    lease TTL. `analyze.py` drops snapshots where any node didn't answer.
  - A stale loadgen polluted `one2/settle`, which is unused. As a side effect, `one2/idle` is an
    "idle after writes" phase, in which checkpoints keep flushing.
- **GC.** GC deletes started inside the 3-node run (≥1 h after the SSTs were replaced). Deletes are
  free on all three providers. GC LIST passes run every 10 min per shard and are modeled analytically.

### Measured request rates (req/s, all nodes summed)
| run / phase | nodes | shards | secs | commits/s | segment PUT | SST PUT | manifest CAS | compactions CAS | polling GETs | SST range GET | LIST | ctl | DELETE (objs) | cold loads/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| costmodel one/idle (fresh node, no writes) | 1 | 256 | 900 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 835 | 0.0 | 3.9 | 6.6 | 13.8 | 0 |
| costmodel one/avg | 1 | 256 | 810 | 345 | 27.3 | 12.0 | 11.4 | 10.6 | 919 | 54.9 | 4.7 | 2.5 | 7.0 | 69 |
| costmodel one2/idle (after writes) | 1 | 256 | 301 | 0 | 0.0 | 9.0 | 10.7 | 8.6 | 893 | 23.4 | 2.1 | 1.4 | 7.8 | 0 |
| costmodel one2/avg | 1 | 256 | 905 | 343 | 27.3 | 12.4 | 12.1 | 11.4 | 949 | 58.9 | 5.5 | 0.8 | 29.1 | 63 |
| costmodel one2/peak | 1 | 256 | 604 | 437 | 27.4 | 14.6 | 12.7 | 12.8 | 952 | 86.7 | 4.9 | 0.9 | 44.0 | 90 |
| costmodel one2/burst | 1 | 256 | 604 | 894 | 26.8 | 15.7 | 12.1 | 12.5 | 959 | 107.7 | 4.7 | 0.5 | 50.5 | 93 |
| costmodel one2/avg2 | 1 | 256 | 605 | 342 | 27.2 | 12.4 | 11.4 | 10.7 | 953 | 63.9 | 4.6 | 0.9 | 48.4 | 88 |
| cost1024 s1024b/idle (fresh node) | 1 | 1024 | 300 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 3274 | 0.0 | 0.5 | 0.7 | 0.0 | 0 |
| cost1024 s1024b/avg | 1 | 1024 | 904 | 343 | 27.3 | 12.2 | 13.7 | 11.4 | 3434 | 36.0 | 15.9 | 1.8 | 16.3 | 33 |
| costmodel three/join (170 shard handoffs) | 3 | 256 | 245 | 0 | 0.0 | 7.2 | 11.0 | 10.6 | 980 | 37.0 | 10.7 | 8.7 | 31.1 | 0 |
| costmodel three/idle (post-handoff) | 3 | 256 | 724 | 0 | 0.0 | 6.1 | 7.9 | 7.3 | 921 | 17.5 | 4.1 | 7.8 | 49.6 | 0 |
| costmodel three/avg | 3 | 256 | 1218 | 340 | 80.9 | 21.5 | 23.9 | 23.9 | 1064 | 97.0 | 7.0 | 4.0 | 44.7 | 50 |

What the measurements show:
- **Segment PUTs are flat with load.** 27.3/s per node at 345, 437 and 894 commits/s. Three nodes do
  80.9/s, which is 3 x 27. One PUT round trip carries whatever queued meanwhile. K>1 and the size cap
  only matter once a node writes 2 MiB (8 MiB / K) per round trip, ≈ 23k commits/s/node.
- **Polling is per-shard, fixed and linear.** 835 GETs/s at 256 shards and 3,274 at 1,024
  (3.26 vs 3.20 per shard). Every SlateDB "read latest" of a sequenced file (manifest, compactions) is a
  probe GET of id+1, which is usually a 404, plus a GET of `gc/<file>.boundary`. Analytically:
  - DB manifest poll every 1 s: 2 GET/s
  - compactor coordinator every 5 s (manifest + compactions): 0.8 GET/s
  - compaction worker every 5 s (compactions): 0.4 GET/s
  - total **3.2 GET/s per shard** (measured: 3.26)
- **Checkpoint flushes follow a cycle, not the write rate.** `checkpoint_all` flushes a node's shards
  one at a time, ~72 ms each at the injected latency, then sleeps 10 s. With s shards per node that is
  s / (10 + 0.072·s) L0 flushes/s: 9.0/s at 256 shards and 12.2/s at 1,024 (both measured). Each flush
  writes the applied marker even with no new data, so an idle node whose log has segments keeps flushing
  every shard (`one2/idle`). A fresh node doesn't (`one/idle`).
- **Compaction adds to the flush cost.** It adds ~1.4 Class A per flush under ingest. The flush bundle
  is 3.06 Class A/flush with no ingest and 4.42 with ingest. Compaction input GETs and post-CAS manifest
  re-reads add ~9 Class B per flush.
- **Cold repo loads are cheap.** ~1.3 SST range GETs each beyond the local disk cache.
- **Handoff is a one-off cost.** The 3-node join (170 shard moves) ran ~42 Class A/s for 245 s, ≲60
  Class A per moved shard.

## Model (`cost_model.py`)
Requests/s = per-shard + per-flush + per-node + per-load terms:

| term | formula (defaults) | coefficient (fitted on the 1-node 256-shard phases) |
|---|---|---|
| SlateDB polling | shards x 2(1/manifest_poll + 2/compactor_poll + 1/worker_poll) GET | 3.26 GET/s/shard (analytic 3.2) |
| SlateDB GC | shards x 6 Class A per 10 min pass (LIST x5, boundary CAS) + 2 bulk deletes | analytic |
| checkpoint flush + compaction | nodes x s/(ckpt + s·t_flush) flushes/s, s = shards/node | t_flush 0.072 s; 4.42 A + 9.07 B per flush (3.06 A idle) |
| segment PUTs | nodes x max(min(c, 1/(L+t_o+linger+e^(-cT)/c)), c·5370 B/(cap/K)) x (1+hedges), c = commits/s/node | L = 34 ms mean, t_o = 2.8 ms, hedges 0.95% |
| retention | per node: 0.05 A/s (paged LIST, report); DELETEs = segments (free) | measured |
| cold loads | loads/s x 1.28 range GET | measured |
| compaction bytes | c x 2 KB rewritten per commit / 2 MiB GETs, / 256 MiB PUTs | analytic; matters only at 100x |
| control plane | per node: lease CAS + LIST nodes/ + LIST assign/ per step (TTL/5 = 2 s); 0.02 GET/s/shard (assignment re-reads); 0.5 GET/s per peer | measured |

The production cold-load rate is a guess: 35/s today (≈ 3 M loads/day, so every daily writer is loaded
~3x), 60/s for sizing-today and 1,200/s at 100x. It is ≤2% of cost in every case.

### Validation: the 1,024-shard and 3-node phases were not used in the fit
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

- Total requests agree within ±6% on the fitted phases. The out-of-sample runs land within
  −2%/+8% Class A and −10%/+1% Class B.
- The 3-node run under-predicts Class B by 10%. It flushed more SSTs than the cycle model (21.5 vs
  15.8/s), and each flush's manifest re-reads follow.
- The idle Class A misses on the 1,024-shard idle phases are 2–11 requests/s in absolute terms. Their
  windows (≤300 s) held no 10-min GC pass, which the model spreads evenly.

## Op → price class per provider
| our op | S3 | GCS (XML/JSON) | R2 |
|---|---|---|---|
| put / put_create (If-None-Match) / put_cas (If-Match) | PUT tier ($0.005/1k); conditional writes bill as PUT | Class A (objects.insert) | Class A (PutObject) |
| list (per 1,000-key page) | LIST = PUT tier | Class A (objects.list / GET Bucket) | Class A (ListObjects) |
| get / get_range / head | GET tier ($0.0004/1k) | Class B (objects.get, GET/HEAD Object) | Class B (GetObject, HeadObject) |
| delete (per object) | free | free (objects.delete) | free (DeleteObject) |
| delete_batch (bulk DeleteObjects) | free (DELETE) | n/a: deletes one by one, free | **not in R2's free list: modeled as Class A** |
| mpu create/part/complete | each is a PUT | Class A (XML POST/PUT) | Class A (Create/UploadPart/Complete) |
| copy | PUT tier | Class A | Class A |

vlpds itself uses no multipart for log or state. SlateDB writes SSTs up to 256 MiB with single PUTs.
Blobs use multipart above a size threshold, and those parts would each be a Class A.

**Prices** (fetched 2026-10-01):
- **AWS S3 Standard, us-east-1** (https://aws.amazon.com/s3/pricing/)
  - Storage: $0.023/GB-mo for the first 50 TB, $0.022 for the next 450 TB, $0.021 above.
  - Requests: PUT/COPY/POST/LIST $0.005 per 1k; GET/SELECT/other $0.0004 per 1k; DELETE/CANCEL free.
    Conditional writes and multipart parts bill as PUT.
  - Transfer: S3 → EC2 in the same region is free.
- **Google Cloud Storage Standard, regional (us-central1, flat namespace)**
  (https://cloud.google.com/storage/pricing)
  - Storage: $0.000027397/GiB-hour = $0.020/GiB-mo.
  - Operations: Class A $0.005 per 1k, Class B $0.0004 per 1k, deletes free.
  - Transfer: free within the same location.
  - **Soft delete is on by default (7 days).** It would keep every deleted segment and replaced SST
    billable for a week (~0.6 TB of log plus compaction churn). Disable it on the bucket.
- **Cloudflare R2 Standard** (https://developers.cloudflare.com/r2/pricing/)
  - Storage: $0.015/GB-mo.
  - Operations: Class A $4.50 per M, Class B $0.36 per M. DeleteObject and AbortMultipartUpload are free.
  - Egress: free.
  - Free tier: 10 GB-mo, 1 M Class A and 10 M Class B per month.

R2 bills less per request than S3/GCS for both classes ($4.50 vs $5.00 per M Class A, $0.36 vs $0.40
per M Class B), and its storage is 35% cheaper. So R2 is the cheapest everywhere, by ~10% on today's
request-dominated bill. What R2 really saves is egress: blob serving and relay backfill from outside
the cloud.

## Breakdown: Bluesky today, 3 nodes, 256 shards, defaults
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

Storage:
- **State.** 23.9 B records x 154.2 B + 56 M repos x 323 B = 3.7 TB live, plus a 25% budget for
  replaced SSTs held by checkpoints and GC min-age. The bench prefix ran 1.03–1.06x live in steady
  writes, and DESIGN budgets up to ~2x during imports.
- **Log.** 334/s x 3 days x 2.7 KB = 234 GB.

## Projections (defaults)
### Bluesky today: 334/s avg, 56 M repos, 23.9 B records
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 2,714 | 4,863 | $3,290 (ops $3,178) | $3,268 (ops $3,178) | $2,936 (ops $2,863) |
| 3 | 1024 | 606 | 9,668 | 4,863 | $7,008 (ops $6,896) | $6,986 (ops $6,896) | $6,312 (ops $6,239) |
| 8 | 256 | 787 | 2,898 | 4,863 | $5,207 (ops $5,095) | $5,186 (ops $5,095) | $4,663 (ops $4,590) |
| 8 | 1024 | 1,205 | 10,298 | 4,863 | $10,256 (ops $10,144) | $10,235 (ops $10,144) | $9,237 (ops $9,164) |
| 16 | 256 | 1,059 | 3,191 | 4,863 | $6,682 (ops $6,570) | $6,661 (ops $6,570) | $5,991 (ops $5,918) |
| 16 | 1024 | 1,668 | 10,941 | 4,863 | $12,829 (ops $12,717) | $12,808 (ops $12,717) | $11,554 (ops $11,481) |

### Bluesky today with every PLC DID (89.9 M) as a repo
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 418 | 2,714 | 4,877 | $3,290 (ops $3,178) | $3,269 (ops $3,178) | $2,936 (ops $2,863) |
| 3 | 1024 | 606 | 9,668 | 4,877 | $7,008 (ops $6,896) | $6,987 (ops $6,896) | $6,313 (ops $6,239) |
| 8 | 256 | 787 | 2,898 | 4,877 | $5,208 (ops $5,095) | $5,186 (ops $5,095) | $4,663 (ops $4,590) |
| 8 | 1024 | 1,205 | 10,298 | 4,877 | $10,256 (ops $10,144) | $10,235 (ops $10,144) | $9,237 (ops $9,164) |
| 16 | 256 | 1,059 | 3,191 | 4,877 | $6,682 (ops $6,570) | $6,661 (ops $6,570) | $5,991 (ops $5,918) |
| 16 | 1024 | 1,668 | 10,941 | 4,877 | $12,829 (ops $12,717) | $12,808 (ops $12,717) | $11,554 (ops $11,481) |

The extra repos add only 11 GB of state.

### Sizing scenario (DESIGN "Initial deployment sizing"): 50 M repos, 25 B records, 2,000/s peak (~1,600/s avg)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 419 | 2,801 | 5,959 | $3,354 (ops $3,217) | $3,328 (ops $3,217) | $2,988 (ops $2,898) |
| 3 | 1024 | 607 | 9,755 | 5,959 | $7,072 (ops $6,935) | $7,046 (ops $6,935) | $6,364 (ops $6,275) |
| 8 | 256 | 858 | 2,985 | 5,959 | $5,622 (ops $5,485) | $5,596 (ops $5,485) | $5,030 (ops $4,941) |
| 8 | 1024 | 1,276 | 10,385 | 5,959 | $10,671 (ops $10,534) | $10,644 (ops $10,534) | $9,604 (ops $9,515) |
| 16 | 256 | 1,485 | 3,279 | 5,959 | $8,875 (ops $8,738) | $8,849 (ops $8,738) | $7,961 (ops $7,872) |
| 16 | 1024 | 2,095 | 11,029 | 5,959 | $15,023 (ops $14,886) | $14,997 (ops $14,886) | $13,524 (ops $13,434) |

### Sizing x headroom: 100x writes (~160k/s avg, 200k/s peak), 20x accounts (1 B) and records (500 billion)
| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| 3 | 256 | 1,294 | 7,040 | 208,753 | $14,086 (ops $9,285) | $13,173 (ops $9,285) | $11,494 (ops $8,363) |
| 3 | 1024 | 1,481 | 13,994 | 208,753 | $17,804 (ops $13,003) | $16,891 (ops $13,003) | $14,871 (ops $11,740) |
| 8 | 256 | 1,372 | 7,224 | 208,753 | $14,550 (ops $9,749) | $13,637 (ops $9,749) | $11,912 (ops $8,781) |
| 8 | 1024 | 1,790 | 14,625 | 208,753 | $19,599 (ops $14,798) | $18,686 (ops $14,798) | $16,486 (ops $13,355) |
| 16 | 256 | 1,496 | 7,518 | 208,753 | $15,290 (ops $10,489) | $14,377 (ops $10,489) | $12,578 (ops $9,447) |
| 16 | 1024 | 2,106 | 15,268 | 208,753 | $21,437 (ops $16,636) | $20,524 (ops $16,636) | $18,141 (ops $15,010) |

At 100x, 3 nodes is CPU-infeasible (DESIGN plans 5–6 x 16 vCPU). The row is there for the op math.
Segment PUTs become size-bound: 160k x 5,370 B / (8 MiB/4) ≈ 410 PUT/s with K=4. Storage of 209 TB is
~$4.8k/mo on S3.

### Tuned knobs: manifest poll 10 s, compactor/worker polls 30 s, checkpoint 30 s, segment linger 100 ms (K=1 at 100x)
| scenario | nodes | shards | Class A M/mo | Class B M/mo | S3 | GCS | R2 |
|---|---|---|---|---|---|---|---|
| bluesky-today | 3 | 256 | 159 | 584 | $1,143 | $1,121 | $1,003 |
| bluesky-today | 8 | 256 | 287 | 670 | $1,814 | $1,793 | $1,607 |
| bluesky-today | 8 | 1024 | 539 | 1,967 | $3,595 | $3,574 | $3,241 |
| bluesky-today | 16 | 1024 | 783 | 2,292 | $4,945 | $4,924 | $4,456 |
| sizing-today | 3 | 256 | 160 | 671 | $1,203 | $1,177 | $1,051 |
| sizing-today | 8 | 256 | 287 | 757 | $1,875 | $1,848 | $1,656 |
| sizing-today | 8 | 1024 | 539 | 2,054 | $3,656 | $3,630 | $3,289 |
| sizing-today | 16 | 1024 | 789 | 2,379 | $5,036 | $5,010 | $4,532 |
| sizing-100x | 3 | 256 | 376 | 4,910 | $8,647 | $7,734 | $6,595 |
| sizing-100x | 8 | 256 | 407 | 4,996 | $8,833 | $7,920 | $6,763 |
| sizing-100x | 8 | 1024 | 659 | 6,293 | $10,615 | $9,702 | $8,397 |
| sizing-100x | 16 | 1024 | 793 | 6,619 | $11,412 | $10,499 | $9,114 |

## Sensitivity
### Bluesky today, 3 nodes / 256 shards
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

### Bluesky today, 8 nodes / 1,024 shards
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

### 100x writes, 8 nodes / 1,024 shards
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

What the knobs do:
- **Shard count is the largest multiplier at today's write rate.** Polling, GC and checkpoint flushes are
  all per shard. 1,024 shards cost ~2–3x what 256 do. Keep 256 until a shard needs to split; online
  split exists, so there is no need to pre-shard.
- **`manifest_poll_interval` (SlateDB DB, 1 s default) is the biggest single GET line.** Its 2 GETs/s
  per shard are 63% of polling. Each node is the only writer of its shards' DBs, and the compactor
  runs in-process, so a 10–30 s poll costs only a delay in picking up compaction results. That means L0s
  linger; the 32 x 16 MiB L0 budget covers it. 10 s saves ~$500/mo at 256 shards and ~$2k/mo at 1,024.
  Now `--slatedb-manifest-poll`, default 10 s ("Defaults changed").
- **Compactor and worker polls** (5 s while adaptive polling is slow) are another 1.2 GET/s per shard.
  30 s saves ~$270/mo (256) or ~$1.1k/mo (1,024). Adaptive polling already keeps them slow, so raising
  the slow interval to 30 s is the change.
- **Checkpoint interval (10 s).** Each pass flushes every shard of a node, including shards with nothing
  new: the marker write dirties the memtable. 30 s saves ~$590/mo and 60 s ~$810/mo at 3/256. The cost
  is a longer crash replay (more log to re-read) and a slower retention floor. Cheaper still, and code
  only: skip the flush for shards whose applied ordinal hasn't moved since their last checkpoint (or
  checkpoint idle shards every few minutes). That removes the idle-shard flushes entirely. Done for
  shards already checkpointed at the log's ordinal ("Defaults changed"); per-shard skipping while the
  log moves would lengthen a successor's replay, so it isn't done.
- **Segment linger** (not implemented; a minimum time between seals). One node's log PUTs ~27 times a
  second regardless of load, which is $350/node/mo on S3. A 100 ms linger cuts that to ~9/s for +~50 ms
  mean ack latency. At 100x the size cap governs instead, so linger does nothing there.
- **K and segment cap** do nothing below ~23k commits/s/node. At 100x a node seals at cap/K, so K=4
  makes 2 MiB segments:
  - K=1 or a 32 MiB cap saves ~$2.5k/mo
  - a 2 MiB cap costs +$16k/mo
  For the high-throughput case, keep K>1 for latency but raise the cap: 32 MiB with K=4 → 8 MiB seals.
- **Lower PUT latency costs more.** S3 Express-like 6 ms PUTs mean more segment PUTs and faster
  checkpoint passes, which means more flushes: +80% at 3/256.
- **GC interval** (10 min) is small: ~$20–90/mo.

### Recommended settings per provider
Request classes cost about the same on S3 and GCS ($5/M Class A, $0.40/M Class B). R2 is $4.50/M and
$0.36/M, so the same knobs win everywhere. No knob shifts cost between classes in a way that favors one
provider.

| setting | S3 / GCS | R2 |
|---|---|---|
| shards | 256 (split online when one is hot) | 256 |
| DB manifest poll | 10 s | 10–30 s |
| compactor + worker polls (adaptive slow) | 30 s | 30 s |
| checkpoint | 30 s, and skip shards with nothing new (code) | same |
| segment linger | 50–100 ms if the ack-latency budget allows (~$250–$350/node/mo) | same |
| K / segment cap | K=4, cap 8 MiB today; cap 32 MiB before ~20k commits/s/node | same |
| bucket | lifecycle rule aborting incomplete MPUs (DESIGN §6); GCS: **disable soft delete** | lifecycle rule for incomplete MPUs |

Effect of the tuned settings: Bluesky today drops from $3.3k to $1.1k/mo (S3) and from $2.9k to $1.0k
(R2) at 3 nodes / 256 shards. At 8 nodes / 1,024 shards it drops from $10.3k to $3.6k (S3).

## Blobs (optional, rough)
**Blob bytes.** Taken from blob refs (`ref/size/mimeType`) in the crawl sample
(`crawl_records`, `repo` in `[did:plc:aa, did:plc:ab)`, ~1/1,023 of repos), posts plus profiles:

| kind | refs | unique CIDs | bytes | mean |
|---|---|---|---|---|
| image | 591,901 | 493,727 | 292 GB | 494 KB |
| video | 15,329 | 14,764 | 104 GB | 6.8 MB |

Unique bytes are ≈ 244 GB of images + 100 GB of video ≈ 344 GB, x 1,023 ≈ **~350 TB**. That counts only
blobs still referenced in Feb 2026, with no derived thumbnails or transcodes.

**Cost.**
- Storage: S3 ~$7.8k/mo, GCS ~$6.6k/mo, R2 ~$5.3k/mo.
- Uploads: ~1 M blobs/day (≈25% of 3.4 M posts/day carry media) ≈ 30 M PUTs/mo ≈ $150/mo.
- Reads (getBlob through a CDN): not modeled.
- Egress, if blobs are served to the internet straight from the bucket, would dwarf all of this on
  S3/GCS and is free on R2.

## Caveats
- **Latency injection.** Absolute op counts for checkpoints depend on per-call latency (t_flush). The
  injected 20/30 ms is S3-Standard-like. Real S3 tail latency would lengthen passes slightly and lower
  the cost.
- **Synthetic records.** Records are small (~540 B of segment per commit vs 5.4 KB real). That doesn't
  change op counts here, which are latency- or count-bound, but it does mean compaction bytes are modeled
  analytically.
- **Commit batching.** One op = one commit. Batched applyWrites would only lower segment bytes; PUT
  counts are latency-bound anyway.
- **Not modeled:** reads served to clients (SlateDB block and disk caches), firehose backfill
  (relays reading old segments: 1 GET per ~2.7 KB x commits replayed, e.g. a full 72 h replay ≈ 0.3 M
  GETs ≈ $0.12), blob GC, PLC/handle objects.
- **Prices are list prices** with no committed-use or enterprise discounts.

## Files
- `measure.py`: the run driver (population, phases, scrapes).
- `analyze.py`: per-phase rates.
- `cost_model.py`: fit, validate, project, price and sensitivity. `python3 cost_model.py` re-derives
  every table here (its baseline is now the new defaults; "previous defaults" is a sensitivity row).
- `compare.py`, `raw_defaults_before.jsonl`, `raw_defaults_after.jsonl`, `compare_output.md`: the
  "Defaults changed" before/after runs.
- `model_output.md` / `model_output.json`: its output. `tuned_table.md`: the tuned table.
- `raw.jsonl` (1 node, 256 shards), `raw1024.jsonl` (1,024 shards), `raw3.jsonl` (3 nodes): every
  scrape, with labeled `vlpds_object_store_requests_total` / `_bytes_total`,
  `slatedb_object_store_request_count_total`, control-plane and segment counters, plus family sums.
- The MinIO prefixes (`costmodel`, `cost1024`, `costsmoke`), MinIO's `.trash`, and the node caches and
  logs were deleted after the runs.

## Defaults changed (2026-10-01 evening)
Latency-neutral cost changes only (user policy: no latency-for-cost trades, so no segment linger and
checkpoints stay at 10 s):

| knob | before | now | flag |
|---|---|---|---|
| SlateDB DB manifest poll | 1 s | **10 s** | `--slatedb-manifest-poll` |
| compactor + worker polls while L0 is shallow (adaptive slow mode) | 5 s | **30 s** | `--compaction-poll` |
| fast polls while L0 >= 8 (adaptive) | 500 ms | 500 ms, plus a writer manifest refresh every 500 ms | |
| checkpoint of a shard already checkpointed at the log's durable ordinal | marker write + L0 flush | **skipped** | |

Why these don't cost latency (DESIGN.md §4 "Polling defaults", HA "Checkpoints"):
- The node is its shards' only writer. Reads see its writes immediately (memtable, then its own flushes'
  manifest); a poll only picks up compaction results, which each flush's manifest CAS reloads anyway.
  The one place a writer waits on a manifest read is a full L0, so while L0 is deep it refreshes every
  500 ms. Tested: `tests/all/cost_defaults.rs` (`own_writes_visible_with_slow_manifest_poll`; unpaced
  2M-record single-shard ingest, 3 runs each, old vs new polls: worst write 0.14–1.12 s vs 0.18–1.17 s,
  throughput 207k–378k vs 227k–416k records/s, i.e. within noise).
- A skipped checkpoint has nothing to write: no segment arrived since that shard's last one. While the
  log moves every shard is still checkpointed each pass, so successors replay no more than before and
  replay floors (retention) still advance with the log (`idle_checkpoint_writes_nothing`).

### Measured (`measure.py run --plan defaults`, `compare.py`)
Same method as above: 1 node, 256 shards, local MinIO, injected 30 ms segment PUTs and 20/30 ms
state-pool latency, 100 k repos (real/32 distribution), the binary before the change and then after it,
on the same prefix. Phases: fresh idle (5 min), 345 commits/s (10 min), idle after writes (5 min).
Requests/s:

| run | phase | commits/s | segment PUT | SST PUT | manifest CAS | compactions CAS | polling GETs | SST GET | LIST | Class A | Class B | S3 req $/mo per node |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| before | idle (fresh) | 0 | 0 | 0 | 0 | 0 | 818 | 0 | 0.4 | 0.6 | 822 | $872 |
| after | idle (fresh) | 0 | 0 | 0 | 0 | 0 | **103** | 0 | 0.4 | 0.6 | **107** | **$121** |
| before | avg | 344 | 27.7 | 11.0 | 12.7 | 9.5 | 904 | 37.2 | 4.7 | 63.0 | 946 | $1,895 |
| after | avg | 344 | 28.2 | 11.6 | 13.2 | 11.4 | **266** | 47.1 | 4.6 | 66.6 | **318** | **$1,418** |
| before | idle after writes (steady 90 s window) | 0 | 0 | 6.9 | 7.2 | 0.6 | 836 | 1.4 | 0.5 | 15.3 | 837 | $1,081 |
| after | idle after writes (steady 90 s window) | 0 | 0 | **0** | **0** | **0** | **102** | 0 | 0.4 | **0.6** | **102** | **$115** |

(The whole idle-after-writes phases average $2,053 before and $987 after: both include ~2 min of
compaction draining the avg phase's L0s and one 10-min GC pass. The steady windows are 90 s after
the drain with no GC pass in them; the GC passes' LIST/CAS/delete_batch spikes are the same in both.)

- **Idle:** polling 818 -> 103 GETs/s (3.2 -> 0.40 per shard, the analytic 2(1/10 + 2/30 + 1/30) =
  0.40). An idle node that has written no longer flushes: SST PUTs 6.9/s and manifest CAS 7.2/s -> 0 once
  compaction drains ($1,081 -> $115/mo per node).
- **345 commits/s:** Class B -66% (946 -> 318/s), Class A unchanged within noise (63 -> 67/s): every
  shard still gets entries every pass at this rate (~1.35 commits/s/shard), so the checkpoint skip does
  nothing under load, as expected. Request cost per node -25% ($1,895 -> $1,418/mo at S3 prices).
- **Model check.** The updated model (`DEFAULT_KNOBS` now 10 s / 30 s / 30 s) predicts the after-avg
  phase at 70.2 A / 189 B; measured 66.6 A / 318 B. The extra ~130 GETs/s are adaptive fast-mode
  episodes: with 30 s slow cycles plus min 4 sources, a shard's L0 reaches the deep mark (8) during
  steady load now and then, and while fast the compactor and the writer's refresh poll every 500 ms
  (per-scrape polling swings 118–816/s). That costs ~$135/mo per node at S3 prices and is not in the
  model; the projections below are ~10–15% low on Class B under load because of it. (SST GETs differ
  because the before run loaded repos cold: 42.8 vs 1.1 loads/s.)

### Model with the new defaults (`python3 cost_model.py`; `model_output.md`)
| scenario | nodes | shards | previous defaults S3 / GCS / R2 | new defaults S3 / GCS / R2 |
|---|---|---|---|---|
| Bluesky today | 3 | 256 | $3,290 / $3,268 / $2,936 | **$2,521 / $2,500 / $2,244** |
| Bluesky today | 8 | 1,024 | $10,256 / $10,235 / $9,237 | **$7,182 / $7,161 / $6,471** |

The remaining big lines at 3 / 256 are segment PUTs ($1,078 S3) and checkpoint flushes under load
($1,070): both latency trades to cut further (linger, longer checkpoints), so they stay.

### Bucket settings (deploy; DESIGN.md §4 "Bucket settings")
- **GCS: disable bucket soft delete.** The 7-day default keeps every deleted log segment and replaced
  SST billable for a week.
- **S3 / R2: lifecycle rule aborting incomplete multipart uploads** (DESIGN.md §6). vlpds only uses
  multipart for large blobs; parts of uploads whose process died are billed until aborted.

The MinIO prefix `costdef` and the node caches were deleted after the runs.
