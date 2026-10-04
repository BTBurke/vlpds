---
title: Object store
section: Operations
order: 102
status: ready
summary: "Choosing and preparing a bucket: S3, R2, GCS or MinIO, the bucket probe, lifecycle rules, prefixes, and what each provider costs."
---

```hero
diagram:
  caption: "What a node asks of the bucket. Log segments and fences are create-only PUTs, leases and assignments are compare-and-swap on the ETag, and everything else is plain GET, PUT and LIST. `vlpds-bucket-probe` checks all of it before a node is allowed to start."
  nodes:
    - { id: node, label: vlpds node, sub: one prefix = one PDS, at: [0, 0], size: [9, 13.4], tone: accent }
    - { id: probe, label: vlpds-bucket-probe, sub: SAFE or UNSAFE, at: [0, 16], size: [9, 3], tone: muted }
    - { id: log, label: "`log/`", sub: segments · fences, at: [24, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: ctl, label: "`nodes/` `assign/`", sub: leases · ownership, at: [24, 3.6], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`state/{shard}/`", sub: SlateDB SSTs, at: [24, 7.2], size: [10, 2.6], shape: store, tone: amber }
    - { id: blob, label: "`blob/`", sub: images · video, at: [24, 10.8], size: [10, 2.6], shape: store, tone: amber }
  groups:
    - { label: bucket / prefix, around: [log, ctl, state, blob], tone: amber }
  edges:
    - { from: node, to: log, label: "If-None-Match: *" }
    - { from: node, to: ctl, label: "If-Match: <etag>" }
    - { from: node, to: state, label: "GET · PUT · LIST" }
    - { from: node, to: blob, label: multipart PUT }
    - { from: probe.r, to: blob.b, label: checks first, dash: true }
facts:
  - { value: "2", unit: headers, label: must be honoured, note: "If-None-Match and If-Match, strongly consistent; a store that ignores them loses data on failover", tone: rust }
  - { value: "$0", unit: /mo, label: a personal PDS on R2, note: "tiny profile, idle ~0.3 M Class A/mo (measured); ~$2 on S3", tone: amber }
  - { value: "~255 ms", label: R2 PUT p50, note: "~500 ms p99, GET ~80 ms (probe, measured from Oregon)", tone: blue }
  - { value: "~$1.7k", unit: /mo, label: "S3 at Bluesky's write load", note: "3 nodes / 64 shards, modeled; requests, not storage, dominate", tone: violet }
```

vlpds keeps every durable byte in one bucket: the commit log, the repo state, the cluster's leases,
and the blobs. The local disk is only a cache. So the bucket is the most important choice an operator
makes: whether it is correct (conditional writes), how fast it is (every acked write waits for a
segment PUT), and what it costs (requests far more than storage).

## What vlpds needs from a store

| Needs | Relied on by | If it's missing |
|---|---|---|
| `If-None-Match: *` PUTs (create only if absent), strongly consistent | log segments, log fences, handle and email claims | a fenced, dead node could still append to its log: acked writes lost on failover |
| `If-Match: <etag>` PUTs (compare-and-swap), and the ETag a PUT returns | node leases, shard assignments, the shard layout, writer ids | two nodes could own one shard at once |
| Read-after-write and list-after-write, ordered LIST with start-after | the fence scan, replay, retention | replay could skip a segment |
| Multipart upload (complete and abort) | large SSTs and blobs | big uploads fail |

The probe checks every row. These providers have passed it:

| Provider | Endpoint (path-style) | Region | Notes |
|---|---|---|---|
| Cloudflare R2 | `https://<account id>.r2.cloudflarestorage.com` | `auto` | No egress fees, generous free tier. No object versioning. Slower per request (below). |
| AWS S3 | `https://s3.<region>.amazonaws.com` | `<region>` | Fastest in-region. Versioning available. Egress costs if nodes run outside AWS. |
| Google Cloud Storage | `https://storage.googleapis.com` with HMAC keys | `auto` | **Turn soft delete off**: otherwise deleted segments and replaced SSTs stay billable for 7 days. |
| MinIO | your server | any | For development and benchmarks (`just minio` starts one on `:9000`). |

Give the node a key pair for its bucket only. On R2, an API token with "Object Read & Write" on the
bucket, limited to the host's IP where you can. On S3, a role with object get, put, delete and
list on the bucket, and no `s3:DeleteObjectVersion`, bucket-policy or lifecycle permissions.

## Running the probe

```steps
- title: Point it at the bucket
  body: "It reads the node's own settings: `VLPDS_S3_ENDPOINT`, `VLPDS_S3_BUCKET`, `VLPDS_S3_REGION`, and the keys as `VLPDS_S3_*_KEY` or `VLPDS_S3_*_KEY_FILE`. Run it from where the nodes will run: latency is part of the answer."
- title: Run it
  body: "`vlpds-bucket-probe` (it is in the image, next to `vlpds`). With Ansible: `-e vlpds_preflight_probe=true` runs it from the deployed image with the node's settings, and refuses to start the node unless it passes. Off by default, because it writes to the bucket (a few thousand requests, ~1k Class A)."
- title: Correctness checks
  body: "`conditional_create`, `compare_and_swap`, `race_create` and `race_cas` (16 writers race for one key, 4 rounds; exactly one may win), `list_read_delete`, `multipart`. It works under a fresh `vlpds-probe/<random>/` prefix and deletes what it wrote (`--keep` leaves it)."
- title: Latency
  body: "200 requests at 4 in flight for each request shape the node issues: `put_create_64kib` (a segment PUT), `put_cas_small` (a lease renewal), `get_1kib`, `get_range_4kib`, `list` and more, as p50 / p90 / p99 / max. `--skip-latency` runs only the checks; `--json report.json` saves the report."
- title: Read the verdict
  body: "The last line is `SAFE for vlpds` (exit 0) or `UNSAFE: <check>: <reason>` (exit 1). Exit 2 means it could not run: endpoint, credentials, network, or a `--prefix` that isn't empty. Never run vlpds on a bucket that is UNSAFE."
```

The report's notes turn the latency into what it means for the node: an acked write waits for at
least one `put_create_64kib`, so its p50 and p99 are the floor of write latency from that host. The
lease CAS's max should stay well under the renewal interval (TTL/5: 12 s on the tiny profile, 2 s on
standard).

## Bucket layout

Everything lives under `--prefix` (`VLPDS_PREFIX`) in `--s3-bucket`. **One prefix is one PDS**: never
point two deployments, or a test and a real one, at the same prefix. Two PDSes can share a bucket
under different prefixes.

| Key | What | Written |
|---|---|---|
| `log/{log_id}/{ordinal}.seg` | each node incarnation's commit log: the WAL and the firehose | create-only, one segment per batch; deleted after `--log-retention` (72 h) once no replay needs it |
| `state/{shard}/` | one SlateDB per shard (its own WAL off), e.g. `state/0000000042/` | memtable flushes, compaction, manifests |
| `assign/{shard}`, `assign/layout` | who owns each shard, and the slot → shard map | CAS |
| `nodes/{node_id}` | node leases | CAS every TTL/5 |
| `writers/{w}` | each live node's unique sequence-number byte | CAS |
| `retain/{log_id}` | retention reports | per retention pass |
| `cluster/version` | the active feature level and its history | at finalize |
| `handle/{handle}`, `email/{sha256}` | uniqueness claims | create-only |
| `blob/{did}/{cid}` | blob bytes, MIME type as Content-Type | streamed, multipart when large |
| `config/ratelimits.json`, `config/crawlers.json` | settings changed in the console | on change; polled (rate limits every 10 s) |
| `budget/mail.json` | the cluster's mail count for the current day (`mail-cluster-day`) | CAS per account mail; polled every minute |
| `blob-quarantine/{did}/{cid}` | a taken-down blob's bytes, until restored or purged | per takedown |
| `moderation/` | audit log, cases, active takedowns, accounts over their blob quota | per moderation action |

Never edit or delete objects by hand: `assign/` and `nodes/` are how nodes agree on ownership, and a
missing segment is lost history. Details of each part: [Architecture](../architecture.md#shards-and-ownership),
[State storage](../state-storage.md#key-layout), [Firehose](../firehose.md#retention),
[Blobs](../blobs.md).

## Lifecycle rules

```diagram
caption: "The one rule every bucket needs. A node that crashes mid-upload never resumes it; without the rule, its uploaded parts stay billable forever."
nodes:
  - { id: up, label: multipart upload, sub: "an SST or a blob", at: [0, 0], size: [9, 3], tone: accent }
  - { id: crash, label: node crashes, sub: never resumed, at: [13, 0], size: [8, 3], tone: danger }
  - { id: parts, label: orphaned parts, sub: billed as storage, at: [25, 0], size: [9, 3], shape: store, tone: amber }
  - { id: rule, label: lifecycle rule, sub: abort after 1 day, at: [25, 6], size: [9, 3], tone: ok }
edges:
  - up -> crash
  - crash -> parts
  - "rule -> parts: deletes"
```

| Provider | Abort incomplete multipart uploads after 1 day | Also |
|---|---|---|
| R2 | Dashboard: bucket → Settings → Object lifecycle rules, or OpenTofu (`cloudflare_r2_bucket_lifecycle`, as in `deploy/cloudflare/r2.tf`) | Nothing else. |
| S3 | `aws s3api put-bucket-lifecycle-configuration` with an `AbortIncompleteMultipartUpload` rule, `DaysAfterInitiation: 1`, empty prefix filter | Versioning with a noncurrent-version expiry is optional insurance; see [Backups and recovery](backups-and-recovery.md). |
| GCS | an `AbortIncompleteMultipartUpload` lifecycle rule, age 1 day | Disable soft delete. |

Don't add expiry rules for anything else. vlpds deletes old log segments, replaced SSTs and
unreferenced blobs itself, and only when nothing can still need them. A rule that expires `log/` by age
could delete a segment a takeover still has to replay.

## Cost by provider

```facts
- { value: "$0", unit: /mo, label: personal PDS on R2, note: "tiny profile idle: 0.30 M Class A + 1.07 M Class B/mo, inside the free tier up to ~3,000 commits/day", tone: amber }
- { value: "~$2", unit: /mo, label: the same on S3, note: "$1.95 idle, ~$2.30 with 200 commits/day and 2 GB stored", tone: amber }
- { value: "~$50", unit: /mo, label: "one node, 64 shards, idle", note: "per-shard SlateDB polling; why tiny uses 1 shard", tone: rust }
- { value: "~$1.5–1.7k", unit: /mo, label: "Bluesky's write load", note: "3 nodes / 64 shards: R2 / S3 and GCS (modeled); blobs extra", tone: violet }
```

Requests, not storage, set the bill, and they follow **nodes and shards, not traffic**:

- **Per node:** the lease CAS every TTL/5, a membership LIST, retention passes, and segment PUTs: a
  busy node PUTs once per PUT round trip whatever its load (~27/s per node up to ~20k commits/s).
- **Per shard:** SlateDB's manifest and compactor polling, checkpoint flushes and their compactions.
  This is why the tiny profile has 1 shard and a 60 s manifest poll, and the standard profile starts at
  64 shards rather than 256.
- **Per commit:** ~7.6 Class A and ~28 Class B requests at personal-PDS rates (measured on the tiny
  profile).
- **Storage** is small next to requests at Bluesky scale: state plus 72 h of log is ~4.9 TB, ~$110/mo
  on S3. Blobs are the exception: ~350 TB, ~$7.8k/mo on S3.
- **Egress.** R2 charges none. Nodes outside AWS or Google using S3 or GCS also pay egress for every
  state read past the disk cache, every peer's log read, and relay backfill, which the model doesn't
  include.

R2's free tier (1 M Class A, 10 M Class B and 10 GB a month) is per Cloudflare account, so other
buckets on the same account share it. Check the real numbers after the first week with
`vlpds_object_store_requests_total` by `component` and `op`; see
[Monitoring](monitoring.md#object-store-request-accounting).

Sources: `bench/results/tiny-pds-idle-2026-10-02` (single node, measured) and
`bench/results/cost-model-2026-10-02` (cluster, measured and modeled).

## Latency and failure

```diagram
caption: "A commit's ack waits for its segment PUT, so the store's PUT latency is the floor of write latency. The same store carries the lease renewal, which must finish within 0.4 × TTL or the node stops."
nodes:
  - { id: c, label: commit, at: [0, 0], size: [7, 3], tone: accent }
  - { id: seg, label: segment PUT, sub: "R2 ~255 ms · S3 tens of ms", at: [11, 0], size: [10, 3], shape: store, tone: amber }
  - { id: ack, label: ack, at: [25, 0], size: [7, 3], tone: solid }
  - { id: lease, label: lease CAS, sub: every TTL/5, at: [11, 6], size: [10, 3], shape: store, tone: amber }
  - { id: stop, label: fail-stop, sub: "renewal > 0.4 × TTL", at: [25, 6], size: [7, 3], tone: danger }
edges:
  - c -> seg
  - seg -> ack
  - "lease -> stop: too slow"
  - { from: seg.b, to: lease.t, label: same store, dash: true, arrow: none }
```

**Per-request latency.** Measured with the probe, R2 answers a PUT in ~255 ms p50 / ~500 ms p99 and a
GET in ~80 ms p50, and the numbers were the same from a VPS and from a home connection in different
places, so they are R2's own and not the distance. On a small single-node deployment, segment PUTs ran ~300 ms p50 / ~650 ms p99 over a day
(`vlpds_segment_put_seconds`). A write on R2 therefore takes about a third of a second to ack, which is
fine for a personal PDS. For in-region S3, the latency model estimates ~40–50 ms p50 / ~150 ms p99 commit acks (modeled, not measured). A
repo's next commit doesn't wait for the previous one to be durable, so slow PUTs cost latency, not
throughput.

**Hedged PUTs.** A segment PUT still pending after `--hedge-after-ms` (100 ms) gets one duplicate
PUT, and the first to land wins. On R2, whose median is above that threshold, nearly every segment PUT
is hedged: on that single-node deployment all 195 segments in one day were (`vlpds_segment_put_hedges_total` against
`vlpds_segment_put_seconds_count`). Each hedge is one more Class A request. That is noise at a personal
PDS's rate; on a busy node on a slow store, compare the two counters and set the threshold near the
store's PUT p90–p99, so hedges still cut the tail without doubling every PUT.

**When the store slows down or fails:**

```steps
- title: Writes queue, then shed
  body: "Segment PUTs retry until they succeed. Acks stop, write latency climbs, then admission control sheds with 503 `Overloaded`. No write is acked that isn't durable."
- title: Slow renewals stop nodes
  body: "A lease renewal slower than 0.4 × TTL (24 s on tiny, 4 s on standard) opens a validity gap, and the node fail-stops (exit 5). A brownout of the whole store past that stops every node. That costs availability, never an acked write."
- title: Recovery is automatic
  body: "The supervisor restarts the nodes. When the store answers again they rejoin, fence the dead incarnations' logs, replay and serve."
```

Don't lower `--lease-ttl-ms` during an incident (it lowers the ceiling), and don't delete anything.
A store shared with other heavy tenants can slow vlpds' renewals the same way. Alerts and steps:
`VlpdsObjectStoreBrownout`, `VlpdsSegmentPutLatencyHigh` and the lease alerts in the
[runbook](runbook.md#slow-or-failing-object-store).
