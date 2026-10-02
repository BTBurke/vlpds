# Tiny PDS on object storage: request rate and monthly price, idle and personal use (2026-10-02)

**Question:** what does a single-node vlpds hosting one account cost in object-store requests on R2 and
S3? We measured it idle and under personal use, then compared it with a 64-shard / 10 s-lease node. We
also looked at which idle request sources can be cut.

## Headline

| configuration (one node, one account) | Class A /mo | Class B /mo | **R2 $/mo** | **S3 $/mo** |
|---|---|---|---|---|
| idle, `--shards 1 --lease-ttl-ms 60000`, defaults otherwise (`idle1`) | 0.78 M | 1.46 M | **$0.00** | **$4.43** |
| personal use extrapolated: idle1 + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, 2 GB stored | 0.82 M | 1.61 M | **$0.00** | **$4.71** |
| idle, `--shards 64 --lease-ttl-ms 10000` (`idle64`) | 6.10 M | 74.9 M | **$46.32** | **$58.13** |
| idle, flags-only tiny profile: idle1 + `--slatedb-manifest-poll 60s` (`bidle1`) | 0.78 M | 1.01 M | $0.00 | $4.25 |
| idle, idle1 + `--lease-ttl-ms 300000 --slatedb-manifest-poll 60s --compaction-poll 120s` (`tidle1`) | 0.25 M | 0.59 M | $0.00 | $1.46 |

Month = 730 h. R2: Class A $4.50/M after 1 M free, Class B $0.36/M after 10 M free, storage $0.015/GB
after 10 GB free, no egress fees. The free tier is per Cloudflare account, so other buckets on the same
account share it. S3 us-east-1: $0.005 per 1k PUT/LIST, $0.0004 per 1k GET, $0.023/GB, and DELETEs (bulk
included) are free. The S3 figures leave out AWS's 12-month free tier.

- **On R2 a one-account PDS costs $0 at the shards-1 / 60 s-lease configuration.** Idle Class A is
  0.78 M/mo against a 1 M free tier. Commits cost ~7.6 Class A each, so the account can make ~950
  commits/day (≈30 k/mo) before R2 starts billing. Past that it is about $0.034 per 1,000 commits.
  Class B (1.5 M of 10 M) and storage are nowhere near their limits.
- **On S3 the idle floor is ~$4.4/mo.** Almost all of it is Class A from the cluster control plane: a
  lease CAS, a `LIST nodes/` and a `LIST assign/` every TTL/5. Personal use adds ~$0.3.
- **Shard count and lease TTL drive the bill, not load.** 64 shards with a 10 s lease idle at $46 (R2) /
  $58 (S3). Each shard costs ~0.44 Class B/s (SlateDB polling) plus ~0.012 Class A/s (SlateDB GC). A
  lease TTL T costs 15/T Class A/s (3 requests per T/5).
- **The lease TTL is not free for a single node.** A graceful restart (SIGTERM) can write again after
  ~1 s at any TTL. After a **crash** (SIGKILL/OOM/power loss), the restarted node can't write for about a
  TTL: 10.7 s at 10 s, 52.9 s at 60 s, **281 s at 300 s** (see "Restart probe"). So 60 s is a reasonable
  tiny default, and anything longer trades crash downtime for pennies.

## Measured request rates (req/s, Class A / Class B, measurement window)

All runs: one node, one account, release build of `37f79d9`, 30-min window after 5-min warmup.

| component | idle1 | pers1 | idle64 | bidle1 | bpers1 | tidle1 | tpers1 |
|---|---|---|---|---|---|---|---|
| ctl_lease (lease CAS + `LIST nodes/`) | 0.167 / 0 | 0.167 / 0 | 1.000 / 0 | 0.167 / 0 | 0.167 / 0 | 0.0333 / 0 | 0.0333 / 0 |
| ctl_assign (`LIST assign/`; full re-read every 150 steps) | 0.0833 / 0.0011 | 0.0833 / 0.0011 | 0.500 / 0.217 | 0.0833 / 0.0011 | 0.0833 / 0.0011 | 0.0167 / 0 | 0.0167 / 0 |
| ctl_version (`cluster/version`, once per TTL) | 0 / 0.0139 | 0 / 0.0139 | 0 / 0.0833 | 0 / 0.0139 | 0 / 0.0139 | 0 / 0.0028 | 0 / 0.0028 |
| log_segment (retention LISTs; segment PUTs) | 0.0333 / 0 | 0.0711 / 0 | 0.0333 / 0 | 0.0333 / 0 | 0.0711 / 0 | 0.0333 / 0 | 0.0711 / 0 |
| state_manifest | 0.0033 / 0.152 | 0.105 / 0.341 | 0.213 / 9.704 | 0.0033 / 0.0683 | 0.103 / 0.272 | 0.0033 / 0.0433 | 0.111 / 0.417 |
| state_compactions | 0.0028 / 0.0694 | 0.102 / 0.325 | 0.182 / 4.377 | 0.0028 / 0.0672 | 0.102 / 0.371 | 0.0028 / 0.0189 | 0.113 / 0.885 |
| state_gc_boundary (SlateDB `gc/*.boundary`) | 0.0011 / 0.220 | 0.0033 / 0.560 | 0.0711 / 14.0 | 0.0011 / 0.134 | 0.0033 / 0.538 | 0.0011 / 0.0611 | 0.0033 / 1.252 |
| state_sst | 0.0017 / 0 | 0.0461 / 0.144 | 0.107 / 0 | 0.0017 / 0 | 0.0461 / 0.144 | 0.0017 / 0 | 0.0444 / 0.129 |
| state_wal (GC LISTs only; WAL is off) | 0.0033 / 0 | 0.0033 / 0 | 0.213 / 0 | 0.0033 / 0 | 0.0033 / 0 | 0.0033 / 0 | 0.0033 / 0 |
| other (`config/ratelimits.json` poll) | 0 / 0.1000 | 0 / 0.1000 | 0 / 0.1000 | 0 / 0.1000 | 0 / 0.1000 | 0 / 0.1000 | 0 / 0.1000 |
| blob | 0 / 0 | 0.0044 / 0.0128 | 0 / 0 | 0 / 0 | 0.0044 / 0.0128 | 0 / 0 | 0.0044 / 0.0128 |
| **total** | **0.296 / 0.556** | **0.586 / 1.498** | **2.320 / 28.5** | **0.296 / 0.384** | **0.584 / 1.453** | **0.0956 / 0.226** | **0.401 / 2.798** |

Runs:
- `idle1`: `--shards 1 --lease-ttl-ms 60000`, idle (the requested configuration).
- `pers1`: idle1 plus the personal-use trickle.
- `idle64`: `--shards 64 --lease-ttl-ms 10000`, idle (the current defaults).
- `bidle1` / `bpers1`: idle1 / pers1 plus `--slatedb-manifest-poll 60s`.
- `tidle1` / `tpers1`: idle1 / pers1 plus `--lease-ttl-ms 300000 --slatedb-manifest-poll 60s --compaction-poll 120s`.

Full per-op tables (op × component × result, SlateDB's own per-component counts) are in `tables.md`.

### Where an idle tiny node's requests go (idle1)

| source | cadence | Class A /mo | Class B /mo | share of S3 $ |
|---|---|---|---|---|
| node lease renewal (`put_cas nodes/n1`) | TTL/5 = 12 s | 219 k | | 25% |
| membership `LIST nodes/` (cluster step) | TTL/5 | 219 k | | 25% |
| assignments `LIST assign/` (cluster step) | TTL/5 | 219 k | 3 k | 25% |
| log retention: `LIST log/<own log>` + `LIST log/` (dead-log scan) | 60 s | 88 k | | 10% |
| SlateDB GC (LISTs of 5 dirs, boundary CAS, ~1 bulk delete per dir per pass) | 10 min per dir | 32 k | | 4% |
| SlateDB polling: DB manifest probe (10 s), compactor manifest + compactions (30 s), worker compactions (30 s), each probe a 404 GET of the next id + a GET of its `gc/*.boundary` | | | 1,159 k | 10% |
| rate-limit config poll (`config/ratelimits.json`, a 404 when no object) | 10 s | | 263 k | 2% |
| `cluster/version` re-read | once per TTL | | 37 k | <1% |
| **total** | | **778 k** | **1,461 k** | $4.43 |

At TTL 60 s the control plane makes up 84% of idle Class A. Measured cadences match the code:
`renew_every = TTL/5` (`main.rs`), one lease CAS on its own loop, and one `LIST nodes/` plus one
`LIST assign/` per step (`cluster.rs` `step_body`). Each retention pass is two LISTs (`retention.rs`
`prune` and `prune_dead` → `backfill::list_logs`). SlateDB GC runs every 600 s per directory
(`garbage_collector::DEFAULT_INTERVAL`, not configurable from vlpds). Blob GC runs every 1.5 h
(`blob_gc_grace / 4`) and none fell in the window; it does a few LISTs per pass, which is negligible.

## Personal use

**Workload (`tinypds.py --workload personal`).** One account doing a "realistic trickle", compressed in
count but not in shape:
- one commit every 30 s: likes 60%, posts 20%, reposts 10%, follows 10%;
- a 300 KB image `uploadBlob` plus an image post every 4 min;
- `sync.getBlob` of a random uploaded blob every 2 min (AppView/CDN fetch);
- `sync.getRepo` every 5 min (relay/backfill).

That is 120 commits/h, ~14x the per-hour rate of 200 commits/day. It was chosen over replaying 200/day
literally, which would give ~4 commits per 30-min window (too few to measure). Commits are still >10 s
apart, so each one pays exactly what an isolated commit pays at a real 200/day: its own segment PUT, its
own checkpoint flush (L0 SST + manifest CAS) and its share of compaction. The per-commit marginal
(pers1 − idle1) is therefore linear and is scaled to 200/day. Real usage is burstier (a like spree inside
one 10 s checkpoint shares one flush), so this is an upper bound per commit.

**Marginal cost per commit** (68 commits in the window, pers1 − idle1):

| | per commit |
|---|---|
| segment PUT (`put_create log/…`) | 1.00 A |
| L0 SST PUT + compaction output | 1.18 A |
| manifest CAS (flush + compaction) | 1.47 A |
| `.compactions` CAS | 1.47 A |
| SlateDB GC bulk deletes of replaced manifests/compactions files (`DeleteObjects`, 1 key each) | 2.08 A |
| manifest LIST (GC) | 0.24 A |
| **Class A total** | **7.56** (5.49 without the bulk deletes) |
| manifest/compactions re-reads and probes, boundary GETs, compaction input range GETs | **24.6 B** |

| read / upload | requests |
|---|---|
| `uploadBlob` (300 KB, single PUT; multipart only above 8 MiB) | 1 A (PUT) + 1 B (HEAD) |
| `sync.getBlob` | 1 B (GET; no blob cache) |
| `sync.getRepo` | 0 (served from the repo/block cache; `probe300.log`) |
| firehose subscriber (relay live tail) | not measured; served from the in-memory ring, and only a reconnect with an old cursor reads segments |

**Extrapolated month** (`price.py`), at 200 commits/day, 5 blobs/day, 20 getBlob/day and 10 getRepo/day:

| base | Class A /mo | Class B /mo | R2 $/mo (0.01 / 2 / 20 GB stored) | S3 $/mo (0.01 / 2 / 20 GB) |
|---|---|---|---|---|
| idle1 (requested config) | 0.823 M | 1.61 M | $0 / $0 / $0.15 | $4.66 / $4.71 / $5.12 |
| bidle1 (+ manifest poll 60 s) | 0.823 M | 1.18 M | $0 / $0 / $0.15 | $4.49 / $4.53 / $4.95 |
| tidle1 (+ TTL 300 s, compaction poll 120 s) | 0.300 M | 1.01 M | $0 / $0 / $0.15 | $1.80 / $1.85 / $2.26 |

Storage of the account itself is negligible: the 30-min personal run left 3.9 MB in its prefix, 2.8 MB
of it blobs. The 72 h log at 200 commits/day is ~2 MB. Blobs decide whether R2's 10 GB free storage
covers it.

Running the measured trickle itself for a month (pers1: 120 commits/h) would cost R2 $2.42 ($1.47 if
bulk deletes are free) and S3 $8.20.

## 64 shards / 10 s lease for comparison (idle64)

- **Control plane.** 1.0 A/s lease + `LIST nodes/`, 0.5 A/s `LIST assign/`, and 0.22 B/s of assignment
  re-reads (64 GETs every 150 steps). That is 3.94 M Class A/mo from the 10 s TTL alone.
- **Per shard.** 0.439 B/s from polling and boundary GETs (0.40 analytic plus ~0.03 of extra manifest
  reads), and 0.0123 A/s from GC: LIST ×~6 per 10 min, boundary CAS, and bulk deletes. Over a month that
  is 1.15 M B + 32 k A per shard, ≈ $0.56/shard/mo on R2 past the free tier and $0.62 on S3.
- **GC spikes.** GC passes are bunched: 30-s windows peak at 26 A/s and 79 B/s.
- **Price.** R2 $46.32 ($44.22 if bulk deletes are free), S3 $58.13.

The 64-shard default is built for a multi-node deployment. A tiny PDS should start its prefix with
`--shards 1`, because the layout stored in the prefix wins over the flag on later starts.

## Restart probe: what the lease TTL costs a single node

`probe.py`: one-shard node, one account, then a SIGTERM restart and a SIGKILL restart with the same
`--node-id`. Each restart is timed until a `createRecord` succeeds, with the session made before the kill.

| `--lease-ttl-ms` | graceful (SIGTERM) restart: first write after | crash (SIGKILL) restart: `/xrpc/_health` after | crash restart: first write after |
|---|---|---|---|
| 10,000 | 0.76 s | 2.5 s | **10.7 s** |
| 60,000 | 0.78 s | 12.6 s | **52.9 s** |
| 300,000 | 0.85 s | 60.7 s | **281 s** |

- **Graceful restart.** Shutdown fences the log and deletes the lease, so a restart has nothing to wait
  for.
- **Crash restart.** The new incarnation fences its predecessor's log at once. It then refuses to join
  (`not joining yet: our clock is behind a peer's merged firehose`) until its clock passes the
  predecessor's published `wm_cap`, which is the last renewal's send time + TTL (`cluster.rs`
  `write_lease`, `try_join`, `wait_seq_past`). Meanwhile writes get 503 `ShardMoved`. The inline first
  step also blocks `/xrpc/_health` for up to `renew_every` (60 s at TTL 300 s).

## Recommendations for a "tiny" profile

**Flags only, no code change.** Use for a one-node, few-account PDS:

```
--shards 1                    # at prefix creation; the stored layout wins later
--lease-ttl-ms 60000          # 0.25 A/s control plane (vs 1.5 at 10 s); crash restart ~1 min
--slatedb-manifest-poll 60s   # -0.17 B/s idle (-31% Class B), no Class A change
--repo-cache-mb 512 --block-cache-mb 256 --cache-budget-mb 256   # memory only; no request effect at this size
# keep --compaction-poll 30s, --checkpoint-every 10s, --log-retention 72h
```

R2 ≈ $0/mo at personal use, S3 ≈ $4.5/mo.

| knob | effect measured | trade-off | verdict |
|---|---|---|---|
| `--shards 1` (vs 64) | removes ~28 B/s + ~0.8 A/s of per-shard polling/GC | one shard serializes all writes. That is fine at personal scale (it absorbs bulk imports, `tests/all/shard_ingest.rs`), and it can split online later | **yes** |
| `--lease-ttl-ms 60000` (vs 10 s) | control plane 1.5 → 0.25 A/s (−3.3 M A/mo) | crash-restart write outage 11 s → 53 s. Graceful restarts unaffected. In a multi-node cluster, takeover of a dead peer is ~TTL + skew | **yes** (tiny) |
| `--lease-ttl-ms 300000` | control plane 0.25 → 0.05 A/s (−0.53 M A/mo, −$2.6/mo on S3; $0 on R2) | crash-restart outage **~4.7 min**, and health blocked 60 s | only if crash downtime doesn't matter; better fixed in code (below) |
| `--slatedb-manifest-poll 60s` (vs 10 s) | idle B 0.556 → 0.384/s. Per-commit B 24.6 → 28 (within noise) | the writer sees compactor results ≤60 s later. Reads touch a few more bloom-filtered L0s in the meantime; SSTs stay alive via the 1 h checkpoint lifetime. While L0 runs deep, the 500 ms fast refresh still applies | **yes** |
| `--compaction-poll 120s` (vs 30 s) | idle B −0.15/s, but under personal load B **rose** (per commit 24.6 → 67.8). L0 reaches the deep mark (8) between slow cycles, so adaptive fast mode (500 ms polls) kicks in for minutes | longer L0s, more fast-mode episodes | **no**: keep 30 s |
| `--log-retention` shorter | no request change: a pass LISTs every 60 s whatever the window | firehose backfill window | no effect on cost |

**Code changes worth making.** None was made here. These would roughly halve the S3 idle floor and widen
the R2 free-tier headroom. Savings are against idle1 (0.296 A/s):

1. **Retention pass interval.** Make it a flag (`--log-retention-interval`, default 60 s; tiny 10 min),
   or skip the LISTs when no segment can be due: the oldest segment is younger than the window and the
   replay floor hasn't moved.
   - Saves 0.030 A/s = **79 k A/mo** (~10% of idle A).
   - Trade-off: segments past the window linger ≤ interval longer, and dead logs retire later.
2. **Lone-node step.** When the `LIST nodes/` listing is just us with an unchanged ETag set, skip the
   `LIST assign/` (keep the every-150-steps full resync).
   - Peers only change assignments after their lease is in `nodes/` (a takeover needs a live lease and a
     fence), so the trigger for re-reading is a change in the nodes listing.
   - Saves 0.083 A/s = **219 k A/mo** (28%).
   - Trade-off: an out-of-band edit of `assign/` (an admin tool writing the bucket directly) is seen at
     the full-resync cadence, not the next step.
3. **Membership LIST cadence for a lone node.** A joiner greets existing nodes directly
   (`host.greet` → `learn_peer`), so a lone node could `LIST nodes/` every few renewals instead of every
   step. The renewal CAS stays at TTL/5.
   - Saves up to 0.067 A/s (**175 k A/mo**).
   - Trade-off: a joiner whose greeting is lost waits up to the longer cadence.
4. **Decouple the crash-restart wait from TTL.** Publish `wm_cap` as send + ~2 × renew interval rather
   than send + TTL, so a merger with late renewals stops advancing its watermark instead of running to
   lease expiry.
   - A crash restart would then wait ~2 × TTL/5, which makes long TTLs (and their −80% control-plane
     cost) usable on a single node.
   - Trade-off: the firehose watermark stalls during slow renewals. This needs review against the HA
     invariants (DESIGN "Why safety needs no clocks").
5. **Rate-limit config poll.** `ratelimit::runtime::REFRESH_EVERY` is a 10 s constant. Make it a flag
   or skip it while alone (an admin update on this node installs at once, and peers are nudged).
   - Saves 0.1 B/s = 263 k B/mo: $0 on R2, $0.11 on S3.
   - Low value; only for completeness.
6. **Single deletes for 1-key batches.** SlateDB GC issues a bulk `DeleteObjects` per replaced
   manifest/compactions file, ~2.1 per commit. `DeleteObject` is free on R2, while `DeleteObjects` is
   not on R2's free list (treated as Class A here, as in `cost-model-2026-10-02`). Issuing a plain DELETE
   when a batch holds one key removes ~27% of per-commit Class A on R2 and is neutral on S3.
7. **SlateDB GC interval** (upstream knob, 10 min fixed). Per shard it is ~32 k A/mo: irrelevant at 1
   shard, ~$8–10/mo at 64 shards on S3. Exposing it (e.g. 1 h for tiny) only delays garbage deletion.
8. **SlateDB boundary GET per probe.** Every "read latest" probe also GETs `gc/<file>.boundary`, which
   doubles polling Class B. Caching the boundary between probes is an upstream SlateDB change. It halves
   per-shard Class B and matters for multi-shard nodes, not tiny ones.

Projection with flags-only tiny profile + code changes 1–3: idle A ≈ 0.296 − 0.030 − 0.083 − 0.067 ≈
**0.12 A/s (0.30 M/mo)** and B ≈ 0.38 B/s (1.0 M/mo). S3 ≈ **$1.9/mo**, R2 $0. On R2 that leaves room
for ~3,000 commits/day inside the free tier, at TTL 60 s and with no change to crash-restart behavior.

None of these cuts touch the write path's latency: segment PUTs per commit, the 10 s checkpoint and the
compaction cadence stay as they are.

## Method

- **Setup.**
  - Laptop (M-series, 14 cores), shared with other agents' work.
  - MinIO in Docker (`vlpds-minio:local` image from `build/Dockerfile.minio`), container
    `vlpds-minio-tinypds` on 127.0.0.1:9310, tmpfs data, one bucket `vlpds`, one prefix per run.
  - Release build of `37f79d9`.
- **Node flags.**
  - `--dev-mode` is needed for the local PLC mode and dev secrets. It changes no background loop.
  - The node is a one-node cluster: every node runs the cluster protocol.
  - Rate limits are on (not `--no-rate-limits`), so the rate-limit config poll is included.
  - `--repo-cache-mb 512 --block-cache-mb 256 --cache-budget-mb 256 --cache-dir <dir>`.
  - Injected S3-like latency: `--inject-put-ms 30` on segment PUTs and `VLPDS_INJECT_STATE_MS=20,30` on
    the state pool, as in `cost-model-2026-10-02`. Idle cadences are timer-driven, so latency barely
    matters at this load.
  - Everything else is at production defaults: manifest poll 10 s, compaction poll 30 s (adaptive),
    checkpoint 10 s skipping idle shards, log retention 72 h with a pass every 60 s, GC min age 10 min,
    checkpoint lifetime 1 h, version check per TTL.
- **Counting.** `vlpds_object_store_requests_total{op,component,client,result}`, recorded below every
  cache and retry, as on a bill. It is cross-checked against SlateDB's
  `slatedb_object_store_request_count_total{component}`. `/metrics` was scraped every 30 s from startup.
  Rates are the last minus the first scrape of the 30-min window that starts 5 min after the account
  was created, which excludes prefix creation (e.g. 175 A/s for the first 30 s at 64 shards).
- **Billing classes.**
  - Class A: `put`, `put_create`, `put_cas`, `list` (per page), `copy`, `mpu_*`, `delete_batch`.
  - Class B: `get`, `get_range`, `head`.
  - Free: `delete`.
  - `delete_batch` (bulk `DeleteObjects`) is Class A on R2 and free on S3. Tables show R2 both ways.
- **Concurrency.** The three requested runs ran concurrently (separate prefixes and ports, one MinIO),
  followed by the two variant pairs and the probes. Counts are per node, so runs can't pollute each
  other.
- **Per-30-s spread.** idle1 0.2–0.6 A/s and 0.41–1.37 B/s. idle64 1.5–26 A/s (GC passes).

## Caveats

- Local MinIO, not R2/S3. Request counts are the client's, and R2/S3 bill the same calls. Retries on
  real-store errors would add a little.
- 30-min windows contain 3 SlateDB GC passes and 30 retention passes, but no blob-GC pass (every 1.5 h)
  and at most one full assignment resync. Both are analytically negligible at one shard.
- The personal-use extrapolation assumes per-commit cost is linear at commits ≥10 s apart (true here:
  one segment and one flush each). Bursts are cheaper per commit.
- The R2 free tier is per account. If other R2 usage on the account already consumes the 1 M Class A,
  idle1 costs 0.78 M × $4.50/M + 1.46 M × $0.36/M = **$4.0/mo** and the personal workload ~$4.3.
- `getRepo` was served entirely from cache on a 100-record repo. A cold load of a large repo after a
  restart costs ~1.3 SST range GETs per load beyond the disk cache (`cost-model-2026-10-02`).

## Files

- `tinypds.py`: runner (node + account + workload + scrapes → `<run>.jsonl`, gzipped after the runs).
- `analyze.py`: window rates. `analyze.py <run>` prints per-op tables, `--matrix` the component matrix,
  `--timeline` per-scrape rates.
- `price.py`: monthly prices and the personal-use extrapolation (output in `price_output.md`).
- `probe.py`: read-cost probe (`probe300.log` → `probe_reads.json`) and restart probe (`probe10.json`,
  `probe60b.json`, `probe300b.json`). `probe60.log` is a first attempt whose SIGKILL leg tripped the
  createSession rate limit.
- `tables.md`: full per-op tables for every run.
- `*.jsonl.gz`: raw scrapes. `*.log`: runner logs.

The MinIO container, its data, the node scratch dirs and the build's target dir were deleted after the
runs.

## Follow-up: lone-node control plane and retention skips (code changes 1–3)

Code changes 1–3 above are now implemented:
- Retention passes skip LISTs that can't find anything, and the interval is a
  flag (`--log-retention-interval`).
- A lone node skips `LIST assign/`.
- A lone node runs `LIST nodes/` once per TTL instead of every step.

Change 4 (`wm_cap` / TTL) is not implemented. DESIGN.md "Lone-node control
plane" has the safety argument, and "Log retention" the skip rules.

**Method.** Same runner, flags and windows as above: `--shards 1
--lease-ttl-ms 60000`, a 5-min warmup, then a 30-min window, counted with
objstats. MinIO ran in its own tmpfs container on 127.0.0.1:9417, removed
afterwards. All five runs ran at the same time:
- `lbidle1` / `lbpers1`: release build of `b597b61` (before), idle / personal.
- `laidle1` / `lapers1`: the same build plus this change, idle / personal.
- `la10idle1`: `laidle1` plus `--log-retention-interval 10m`.

Request rates (Class A / Class B per second):

| component | lbidle1 | laidle1 | la10idle1 | lbpers1 | lapers1 |
|---|---|---|---|---|---|
| ctl_lease (lease CAS + `LIST nodes/`) | 0.167 / 0 | **0.100** / 0 | 0.100 / 0 | 0.167 / 0 | **0.100** / 0 |
| ctl_assign (`LIST assign/`) | 0.100 / 0.0178 | **0.020** / 0.0178 | 0.020 / 0.0178 | 0.100 / 0.0178 | **0.020** / 0.0178 |
| log_segment (retention LISTs; segment PUTs) | 0.0333 / 0 | **0** / 0 | 0.0011 / 0 | 0.0711 / 0 | **0.0383** / 0 |
| state_other (`LIST state/`, reshard GC) | 0.0333 / 0 | 0.0333 / 0 | 0.0333 / 0 | 0.0333 / 0 | 0.0333 / 0 |
| everything else (SlateDB, rate-limit poll, version, blobs) | 0.0122 / 0.557 | 0.0122 / 0.552 | 0.0122 / 0.558 | 0.265 / 1.598 | 0.267 / 1.622 |
| **total** | **0.346 / 0.575** | **0.166 / 0.570** | **0.167 / 0.576** | **0.637 / 1.616** | **0.459 / 1.640** |

Monthly cost:

| run | Class A /mo | Class B /mo | R2 $/mo | S3 $/mo |
|---|---|---|---|---|
| lbidle1 (before) | 0.908 M | 1.51 M | $0.00 | $5.14 |
| **laidle1 (after)** | **0.435 M** | 1.50 M | $0.00 | **$2.77** |
| la10idle1 (after, 10-min passes) | 0.439 M | 1.51 M | $0.00 | $2.80 |
| lbpers1 (before, 120 commits/h) | 1.67 M | 4.25 M | $3.03 | $10.06 |
| lapers1 (after, 120 commits/h) | 1.21 M | 4.31 M | $0.93 | $7.75 |

- **Idle Class A fell 52%**, from 0.346 to 0.166 A/s (−0.18 A/s, −473 k A/mo). That matches the
  projection for changes 1–3.
  - `LIST nodes/`: 0.0833 → 0.0167 A/s (once per TTL). The renewal CAS stays at 0.0833.
  - The step's `LIST assign/`: 0.0833 → 0.0033 A/s (every 25 steps).
  - Retention: 0.0333 → 0. Our log's oldest segment is inside the 72 h window, and there are no dead
    logs. Both LISTs still run at least hourly, but no hourly LIST fell in the 30-min window.
  - Class B is unchanged (0.575 → 0.570 B/s).
- **`--log-retention-interval 10m` adds nothing on top.** The skips already make idle passes free.
  `la10idle1`'s 2 LISTs come from its first pass, which falls inside the window at a 10-min interval.
  The flag is for operators who want fewer passes anyway, e.g. while dead logs are being scanned.
- **Under personal use** the same 0.18 A/s goes (0.637 → 0.459 A/s). Per-commit costs (segment PUT,
  flush, compaction, GC deletes) are untouched. R2 at the measured 120 commits/h drops from $3.03 to
  $0.93/mo.
- **Personal PDS at 200 commits/day.** Idle plus 7.56 A per commit comes to ~0.48 M A/mo. That is
  S3 ≈ $3.1/mo, down from ≈ $5.4 at this build. R2's free tier now covers ~2,450 commits/day, up
  from ~400 at `b597b61` (~950 at `37f79d9`). This holds at TTL 60 s, with crash-restart behavior
  unchanged.
- **The before-run is higher than `idle1` (0.346 vs 0.296 A/s).** `b597b61` has a reshard GC dir
  pass every 60 s that `37f79d9` didn't. Each pass makes two `LIST state/` (`state_other`) plus a
  `LIST assign/` and a layout GET (both counted in ctl_assign). That is 0.05 A/s (131 k A/mo), now
  30% of the idle floor. It is the next thing to cut, and was not changed here: skip the pass when
  the layout is unchanged and the last pass found no retired dirs and no orphaned records.

Remaining idle Class A in laidle1 (0.166 A/s):

| source | A/s |
|---|---|
| lease CAS (every TTL/5) | 0.083 |
| reshard GC dir pass | 0.050 |
| `LIST nodes/` (once per TTL) | 0.017 |
| SlateDB GC | 0.012 |
| `LIST assign/` (every 25 steps) | 0.003 |

Run files are `lbidle1`, `laidle1`, `la10idle1`, `lbpers1` and `lapers1` (`*.jsonl.gz`, `*.log`).
`analyze.py --matrix lbidle1 laidle1 la10idle1 lbpers1 lapers1` prints the component table. The
scratch dirs, both builds' target dirs and the MinIO container were deleted.
