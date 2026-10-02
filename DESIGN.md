# vlpds — very large PDS on object storage (design)

Rust atproto PDS whose only durable storage is an S3-compatible object store,
with sync 1.1 firehose semantics.

**Targets:** 200 commits/s sustained on a single repo, 100,000 commits/s across
one PDS process. Latency target (ack after durable): p50 ≈ 40 ms, p99 ≈ 150 ms
on S3 Standard-like latency; much lower on S3 Express / local MinIO.

## What the targets rule out

| Approach | Why it fails |
|---|---|
| One PUT per commit (per-repo objects, per-commit CAR files) | 100k PUT/s ≈ $0.50/s ≈ **$43k/day** in request fees, and over S3's per-prefix PUT limits. |
| Wait for durability before computing a repo's next commit | Each commit chains on the previous MST root, so per-repo rate ≤ 1 / PUT latency ≈ **20–50/s**. Misses 200/s. |
| Persist MST nodes as KV rows (refcount/GC'd) | log₄(n) ≈ 5–10 node rewrites + deletes per commit → ~1–2M KV ops/s at target, plus LSM compaction of that churn. |
| Per-repo SQLite (reference PDS) + Litestream | Not object-storage native; per-repo fsync; doesn't consolidate small writes. |

So the design needs three things: **group commit** across all repos into large
objects, **pipelined per-repo commits** (compute commit N+1 while N is still
uploading, ack both in order), and **no per-commit MST storage churn**.

## Architecture

```
 XRPC (axum) ──► RepoRouter ── hash(did) ──► shard workers (N ≈ cores)
                                               │  per-repo in-memory MST (Arc, copy-on-write)
                                               │  apply ops → new root, diff blocks,
                                               │  sync-1.1 inversion proof, sign commit
                                               ▼
                                  Sequencer / Log writer (single owner of seq)
                                    assign seq, encode firehose frame,
                                    append to open segment; seal every 25 ms or 8 MB
                                               │  K concurrent PUTs (If-None-Match: *)
                                               ▼
                         s3://bucket/log/{first_seq:020}.seg   ◄── this IS the WAL and the firehose
                                               │ durable watermark advances in seq order
                     ┌─────────────────────────┼───────────────────────────┐
                     ▼                         ▼                           ▼
        SlateDB (WAL disabled)        firehose broadcast             ack writers
        materialized state:           (Arc<Bytes> frames,            (HTTP 200 with
        heads, records, accounts      ring of recent segments)        cid/rev)
```

### 1. Repo shards (CPU path)
- DID → shard by hash. A shard owns its repos' in-memory state:
  `RepoState { head: commit cid + rev, mst: Arc<Node>, key: SigningKey }`, LRU-bounded by
  count (`--cache-per-worker`) and approximate bytes (`--repo-cache-mb`; ~240 B of MST
  heap per record). Large repos are pinned and preloaded (§2).
- Writes to one repo are processed in order. A write does not wait for the
  previous commit to be durable; it builds on the in-memory head. Writes are
  acked in log order, so a client never sees commit N+1 acked before N.
- Per commit: MST path rewrite (~log₄ n nodes, encode + SHA-256),
  inversion proof, one secp256k1 signature (13.5 µs with libsecp256k1; 25 µs with k256). Estimated
  60–80 µs CPU per commit → ~10k commits/s per repo before CPU saturates, so
  200/s per repo is comfortable.
- `swapCommit` / `swapRecord` compare against the in-memory head, which includes
  pending commits. This is correct because the log preserves order.
- **Write coalescing:** when a repo's worker runs, it drains every queued write
  into one commit (up to 200 ops / 2 MB). No added delay: an idle repo gets
  one-op commits, a hot one gets bigger batches automatically, so the per-repo
  record rate is bounded by request handling, not commit rate. Rules:
  - a write carrying `swapCommit` starts a fresh commit;
  - `applyWrites` is one all-or-nothing unit inside the batch;
  - each write is validated against the batch-so-far, so a conflicting write
    (duplicate create, failed `swapRecord`) fails alone, not the whole batch;
  - every write in the batch is acked with the shared commit cid/rev.

### 2. MST is derived state, not stored
The MST is fully determined by the set of `(key, record CID)` pairs. So:
- Only **records** are persisted (`R/{did}/{collection}/{rkey}` → cid + bytes).
- On cold load: range-scan the repo's records (contiguous in the LSM, usually 1–2
  SST blocks), rebuild the MST (~1 ms per 1k records), and **verify the rebuilt root
  equals `head.data`**. This verification also gives a free integrity check.
- MST nodes needed by the firehose (diff + proof) are already in the log segment.
- `getRepo` / `getRecord` proofs / `getBlocks` are served from the in-memory tree
  (an Arc snapshot gives consistent exports while writes continue). Internal
  nodes (height >= 1) keep their encoded block after each write, so exports
  and proofs copy them; leaves (~3/4 of the bytes) re-encode on demand.
- `getBlocks` finds nodes by CID through a per-repo `NodeIndex` (node CID ->
  first key in its subtree + height; the node is found by descending to that
  key and checking the CID). It is built lazily, with one walk, the first time a
  request asks for a node. After that the repo worker advances it with each
  commit's written nodes. It covers a rev range, so a miss inside that range
  is final. A broken commit chain (importRepo) or too many stale entries drops
  it, and the next request rebuilds it. Nothing is persisted.
- **Large repos are pinned.** A cold load costs ~1.5 µs of CPU and ~240 B of
  heap per record (real repos: 594k records, 0.9 s, 130 MB; a 5–10 M-record
  repo is 1–2 GB and 8–15 s). Repos with at least `--pin-repo-records`
  (500k) records are never evicted by the LRU (they don't count toward its
  entry or byte budget; they unpin below half the threshold). Crossing the
  threshold logs a private-state entry `L/{did}` → record count, so when a
  shard opens (startup, takeover, handback) its new owner scans `L/` and
  preloads those repos in the background, 4 at a time per node, before
  their first write. The key is a hint: a stale one costs a load.
  Metrics: `vlpds_repo_cache_bytes`, `vlpds_repo_cache_pinned`,
  `vlpds_repo_load_by_size_seconds{records}`, `vlpds_repo_preloads_total`.
- **Recently written repos are preloaded too.** Each shard keeps the
  repos it committed to most recently (`--preload-recent`, 2,048 per
  shard; `partition::RecentRepos`, touched once per commit batch) and
  writes the list, newest first, as `meta/recent` with its checkpoints and
  at close, only when its members changed. The shard's next owner (a
  restart, takeover or handback) reads it right after the open, seeds its
  own set with it, and loads those repos in the background, 32 at a time
  per node, interleaved across shards, beside the large-repo preloads; the
  index reads of all newly opened shards run at once, so they don't queue
  behind the request-driven loads they are meant to spare. Bulk creation
  doesn't touch the set. Metric: `vlpds_repo_preloads_total{kind}`.
- Escape hatch if even preloads are too slow (≥10M records): periodically
  write an MST snapshot object so a cold load doesn't need an O(n) rebuild.
  Not needed for v1.

Persisted state per commit drops from ~15 KV operations to about 2: the record and
the repo head.

### 3. Log = WAL = firehose
> **Superseded by the HA section below:** the log is per *node*, with up to K
> segment PUTs in flight finalized in ordinal order (adaptive batching:
> whatever queues during a PUT forms the next segment; see "Pipelined segment
> PUTs").

- A sequencer task per partition assigns `seq` and splices it into frames that
  workers pre-encoded (header + body DAG-CBOR) in the open segment buffer.
- At 75k/s with 25 ms PUTs and 16 partitions: ~500 PUTs/s of ~450 KB.
- **Fencing:** `PUT log/{first_seq}.seg` with `If-None-Match: *`. A zombie writer
  collides on the next segment name and stops. This is the basis for active/standby
  failover.
- **Failure policy: fail-stop.** A segment PUT is retried until it succeeds (the
  idempotent key makes this safe). If it is unrecoverable, the process exits and
  recovery replays from durable state. Unacked in-memory commits are discarded,
  which is safe because they were never acknowledged or broadcast.
- Segment format: a header (uncompressed, so header-only range GETs work)
  and a body of length-prefixed entries (firehose frame + state mutations),
  stored zstd-compressed (see "Log compression" under HA). Serving the
  firehose is a byte copy out of the decompressed body.
- Retention: 72 h (firehose backfill window), never deleting what a replay
  could need (see "Log retention" under HA).

### 4. Materialized state: SlateDB with its WAL disabled
Why an LSM at all: state needs point reads (heads, `getRecord`), ordered range
scans (`listRecords`, cold MST rebuild) and more records than fit in RAM. Any
replacement (per-repo snapshots + log deltas) needs a DID→segment index and
compaction, i.e. an LSM. SlateDB is used only as a sorted KV, so it is
swappable.

- On durable watermark advance, apply that segment's effects as one `WriteBatch`
  (records put/delete, head updates, `meta/applied_seq`) with `await_durable=false`.
  It is visible in the memtable immediately (read-your-writes before the HTTP ack),
  and SlateDB flushes L0 SSTs to S3 on its own schedule.
- No double-write: the log is the WAL. Crash recovery = open SlateDB, read
  `applied_seq`, replay segments after it (events carry record blocks + commit).
- Keys (each prefixed by `0x01 ‖ slot` of its account, so a shard's state is
  one key range: see "Online shard split/merge"):
  - `h/{did}` → head `{commit cid, signed commit bytes, rev, data cid, status}`
  - `R/{did}\0{collection}/{rkey}` → `{cid, record bytes}`
  - `c/{did}\0{cid8}{path}` → empty: record CID index for `getBlocks` (`cid8` =
    first 8 bytes of the CID's digest). Written in the same batch as the `R/` key,
    one per path (a CID can sit at several). A lookup prefix-scans
    `c/{did}\0{cid8}` on the snapshot and checks each path's record CID.
  - `a/{did}`, `n/{handle}` → account; `k/{did}` → signing key (plaintext in the
    prototype; KMS-wrapped later)
  - `meta/applied_seq`
- Reads (`getRecord`, `listRecords`, `describeRepo`) → SlateDB (memtable → block
  cache → local disk cache → S3).
- SST blocks (16 KiB) are zstd-compressed (`--sst-compression none|lz4|zstd`;
  each SST records its codec). On a real repo's rows (43,649 records of one
  user's repo, R/ values + c/ keys, ×8 repos): SSTs 131.5 MiB uncompressed,
  68.0 MiB lz4 (1.9×), 52.4 MiB zstd (2.5×). Write + flush CPU +20 % (about
  0.35 µs per row); cold scans and gets showed no difference above noise (the
  block cache holds decoded blocks, so only misses decompress, and the local
  disk cache holds 2.5× more).
- One block cache (foyer, `--block-cache-mb`) and one SST metadata cache
  (bloom filters, indexes, stats; a quarter of the block cache) serve every
  shard DB. The metadata cache is our own (`partition::MetaCache`): 64
  RwLock shards with CLOCK eviction, so a hit takes a shared lock and sets
  one bit. Foyer takes its shard's mutex on every hit (eviction state), and
  every point read of one repo checks the same few SSTs' filters (up to 32
  L0s plus the sorted runs of its shard): reads of one hot repo serialized
  on those keys, the more IO threads the worse. Profile of getRecord on one
  10M-record repo (laptop, 14 IO threads): 32 % of CPU spinning in foyer's
  lock at 0b2a322 (66 % at 2e64422) -> under 3 %, getRecord 24.5k -> 30.4k/s
  (2e64422: 16.9k/s); the rest is now the client's single h2 connection.
  Likely the benchbox regression (63k -> 35k/s at 10M, -13-28 % on small
  repos) from 2e64422's 6 IO threads to fa0975c's 32. Decompression was
  not it: blocks are cached decoded, and the sweep reads 2,000 records.
- Each shard's compactor (coordinator + one worker writing the same SST
  format) starts after the DB opens rather than inside the open, so a
  takeover or handback serves ~11 store round trips sooner. Its outputs
  aren't written into the local disk cache (reads fill it). It keeps
  running after the DB is marked closed until every handle is gone (at
  most 60 s): SlateDB marks the DB closed *before* its final memtable
  flush, and with L0 full (a close under bulk ingest: a handback, or
  freezing a hot shard to split it) that flush waits for a compaction;
  stopping at the mark deadlocked such a close.
- **Adaptive compaction polling** (`--compaction-polling`, default
  adaptive). Slow polls are cheap idle, but an unpaced bulk ingest into one
  shard fills L0 (32 x 16 MiB) between cycles and stalls. Always-fast
  (500 ms) polls fix that at ~4x the idle requests. Adaptive runs slow
  polls (`--compaction-poll`, 30 s; it was SlateDB's 5 s) while L0 is
  shallow and restarts the compactor with fast polls once L0 reaches 8
  SSTs, back to slow after 15 s at <= 2 (a graceful worker stop hands
  claimed jobs back). Measured with the old 5 s slow polls and 1 s
  manifest poll (tests/all/compaction_polling.rs, in-memory store with
  10 ms per call, M4 Pro): idle 3.20 / 3.24 / 13.97 requests per shard per
  second (slow / adaptive / fast; mostly the writer's own 1 s manifest
  poll); 2M records unpaced into one shard: worst write 10.7 s / 1.6 s /
  1.3 s, time in writes over 250 ms 25.5 s / 2.2 s / 2.5 s of the run,
  throughput 71k / 376k / 374k records/s.
- **Polling defaults (cost, latency-neutral).** Per-shard polling was the
  largest fixed GET line of the object-store bill (3.26 GETs/s per shard,
  835/s at 256 shards; bench/results/cost-model-2026-10-02 "Defaults
  changed"). Each SlateDB "read latest" of a sequenced file is two GETs (a
  probe of id + 1, usually a 404, plus its `gc/*.boundary` file):
  - *DB manifest poll* (`--slatedb-manifest-poll`, 10 s; SlateDB's default
    1 s). The node is its shards' only writer: writes land in the memtable,
    and its own flushes update its manifest in place, so reads see its
    writes at once whatever the poll (tests/all/cost_defaults.rs). A poll
    only picks up compaction results, which every flush's manifest CAS
    reloads anyway on a conflict. The one wait on it is a writer whose
    view of L0 is full (no flush runs, so only a manifest read shows the
    freed slots): while L0 is >= 8 deep the writer refreshes every 500 ms
    (`partition::spawn_deep_refresh`), as often as the compactor's fast
    polls. (SlateDB also uses this interval as the L0 upload retry
    backoff, after its object-store layer's own retries are exhausted.)
  - *Compactor and worker slow polls* 30 s (were 5 s): while L0 is
    shallow nothing waits on them, and a deep L0 switches to 500 ms.
  Together 3.2 -> 0.4 GETs/s per shard. Unpaced 2M-record single-shard
  ingest is unchanged within run-to-run noise (tests/all/cost_defaults.rs
  `deep_l0_ingest_keeps_up`, 3 runs each, old vs new polls: worst write
  0.14–1.12 s vs 0.18–1.17 s, 207k–378k vs 227k–416k records/s).
- **Bucket settings (deploy).** Replaced SSTs and expired log segments are
  deleted for good, so: on **GCS, disable bucket soft delete** (on by
  default, 7 days: every deleted segment and replaced SST would stay
  billable for a week, ~0.6 TB of log plus compaction churn at Bluesky's
  rate); on **S3 and R2, add the lifecycle rule that aborts incomplete
  multipart uploads** (§6 "Aborted multipart uploads"; vlpds itself uses
  multipart only for large blobs). MinIO needs neither.
- **What keeps replaced SSTs.** A scan or snapshot reads the SSTs of the
  manifest it started with. Before each manifest update that replaces SSTs,
  SlateDB's compactor writes a *checkpoint* of the old manifest that expires
  after `--slatedb-checkpoint-lifetime` (vlpds: 1 h; SlateDB's default 15 min),
  and GC never deletes an SST a live checkpoint references. That lifetime is
  the read guarantee: a scan (a 10M-record getRepo to a slow client) must
  finish within it. vlpds creates no checkpoints of its own (its
  "checkpoints" are applied markers + memtable flushes). Separately, GC
  skips SSTs younger than `--slatedb-gc-min-age` (10 min), counted from the
  SST's *creation*: that only guards SSTs not yet in a manifest. It was
  24 h, which protected nothing extra (an SST created long ago and replaced
  now passes it at once) but kept every SST written in the last day.
- **Bulk import space.** Importing the storage sample (6.94 GB live) wrote
  28 GB of SSTs (4.0x: size-tiered compaction rewrites each row ~3 times).
  Replaced SSTs are now deleted ~checkpoint lifetime after their
  replacement, so peak transient space is the compaction output of the last
  hour (up to ~4x live while an import runs, ~1x of the largest run in
  steady state), not of the last 24 h. Shortening the lifetime after an
  import isn't safe in general (it is what in-flight exports rely on);
  lower `--slatedb-checkpoint-lifetime` for a dedicated import window
  instead.

### 5. Firehose
- Live: after durability, a node's sealed segments go to a byte-bounded live ring
  (`LiveRing`, 128 MiB of segment bytes by default; zero-copy `Bytes` fan-out).
  A peer follower that falls behind the cap is dropped and catches up from S3.
- Merge: the firehose merger queues each log's events until every log's watermark
  passes them. The queues share a byte budget (256 MiB default); a log over
  budget stops being queued and is read back from S3 in chunks until it reaches
  the live ordinal, so a stalled peer can't grow memory without bound.
- Serving: `subscribeRepos` does its own websocket upgrade and moves the socket
  onto a dedicated firehose runtime (`--firehose-threads`, default 4), so fan-out
  never competes with request handling. The merger frames each batch's websocket
  messages once; every subscriber writes zero-copy slices of the same bytes and
  wakes on a watch of emitted bytes (no per-subscriber channel).
- Slow subscribers: a subscriber may lag the head by at most
  `--firehose-max-lag-mb` (128 MiB). Past that it gets `ConsumerTooSlow` and is
  closed; it resumes from its cursor.
- Backfill: a cursor behind the ring is served from S3 with read-ahead (up to 32
  GETs per log, `--backfill-readahead-mb` total) through a shared segment cache
  (`--backfill-cache-mb`), then handed to the live ring once it reaches the ring
  floor. Readers stop at a log's first non-segment (hole rule).
- Events: `#commit` (sync 1.1), `#sync` (account creation / repo reset),
  `#identity`, `#account`.
- Sharded subscriptions (vlpds extension):
  `subscribeRepos?cursor=..&shard=k/n` (0 <= k < n <= 65,536) carries only
  the events whose repo DID (`repo` of a #commit, `did` of the others) hashes
  into slice k of n of the 65,536 hash slots: slots s with s·n/65536 = k,
  so for n dividing the cluster's shard count (or vice versa) a slice is a
  whole set of cluster shards. Seqs, order and cursors are the full
  stream's: a cursor from either works on the other, OutdatedCursor /
  FutureCursor / ConsumerTooSlow behave the same, and the union of the n
  streams is the full stream. A bad `shard` is 400 InvalidRequest.
  Filtering is cheap: a batch's per-event slots are computed once (read
  straight from the frame's CBOR, then sha256 of the DID), lazily by the
  first sharded subscriber and shared by the rest; a subscriber writes only
  the matching events as slices of the shared batch bytes, one slice per
  run of consecutive matches, in one vectored write. Backfill filters the
  same way, with the slots cached alongside each segment in the backfill
  cache.

### 6. Blobs
`uploadBlob` streams to `blob/{did}/{cid}` (multipart if large). This is off the
commit hot path.

- **References.** `b/{did}\0{cid}\0{record path}` rows, written with the
  commit that adds or removes the reference.
- **GC** (`blobs::sweep_blobs`, owned partitions only). A blob unreferenced
  for longer than `--blob-gc-grace-secs` is moved to `blob-gc/{did}/{cid}`,
  not deleted. A write checks that its blob exists before it is sequenced, so
  a write that checked just before the move can apply its reference just
  after it. After a settle time (60 s, or the grace period if shorter) the
  references are checked again: if one appeared, the blob is moved back;
  otherwise it is deleted. A write that checks after the move fails with
  `BlobNotFound`, as it would for any missing blob.
- **Aborted multipart uploads.** Large uploads go through a multipart upload
  to `blob-tmp/{did}/{random}`. A failed upload is aborted. A completed temp
  object left behind by a crash is deleted by the GC after 24 h. But the
  parts of an upload whose process died mid-way are invisible to LIST, and
  object_store can't list or configure them. So the bucket needs a lifecycle
  rule that aborts incomplete multipart uploads. They are billed until then.
  For S3:
  ```json
  {"Rules": [{"ID": "abort-incomplete-mpu", "Status": "Enabled",
              "Filter": {"Prefix": ""},
              "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}
  ```
  Apply it with `aws s3api put-bucket-lifecycle-configuration --bucket B
  --lifecycle-configuration file://rule.json`. MinIO needs no rule: it
  aborts stale uploads itself (`api stale_uploads_expiry`, 24 h by
  default). The rule only touches uploads that were
  never completed, so an empty prefix (the whole bucket) is safe.

### 7. HTTP
Every outbound client is built once in `src/http.rs`, per role, and shared
(no per-request clients). No client follows redirects. New outbound
connections count in `vlpds_http_client_connects_total{role}`; under steady
load it should stay flat (a rising rate means pool churn).

| Role | Used for | Settings |
|---|---|---|
| peer | forwarding, internal calls | h2c prior knowledge; 4 MiB stream / 64 MiB conn windows; PING every 10 s (also idle), dead after 5 s; TCP keepalive 30 s; nodelay; connect 1 s; `--peer-connections` (default 4) connections per peer, round-robin |
| public | PLC, requestCrawl | h2 by ALPN on https, HTTP/1.1 on http with 1,024 idle per host; idle close 60 s; h2 PING 20 s / 10 s; TCP keepalive; connect 5 s, read 30 s |
| proxy | configured AppView / report service | `http://`: hyper HTTP/1.1 connections in per-IO-thread pools (32 idle per thread, overflow to a shared pool of 1,024), idle close 60 s, retry once if a reused connection was closed before the request went out; `https://`: public's settings as one client per IO thread. No read timeout: the proxy arms a 10 s head deadline and a 30 s body-idle timer only while the upstream makes it wait |
| guarded | user-derived URLs: did:web, handle `.well-known`, OAuth client metadata, lexicons, DID-doc service endpoints | public's settings, 32 idle per host, plus a resolver that drops non-public addresses (outside dev mode); pair with `check_outbound_url` |
| S3 (object_store) | log and state stores (separate pools) | HTTP/1.1 only, 256 idle per host, idle close 15 s (S3 closes at ~20 s), connect 2 s, 30 s total |

Why: an HTTP/1.1 peer pool smaller than the forwarding concurrency opened a
connection per request and collapsed a 3-node cluster at 50k/s; one h2
connection fixes the churn. Keepalive PINGs bound how long a half-open peer
connection black-holes forwards. Several connections per peer keep a single
connection (and its driver task, and its 1,024-stream limit at the receiver)
from being the bottleneck or the single point of failure. The AppView stays
on pooled HTTP/1.1 over plaintext: one multiplexed h2c connection was slower
(bench 2026-10-02 §6). The proxy's pools are per IO thread because a shared
pool's mutex (taken at checkout and return) and the timers reqwest arms per
read (tokio has one timer-wheel lock) were ~20% of the proxy's CPU.

Server (`server::serve`, HTTP/1.1 + h2c auto): h1 header read timeout 30 s
(slowloris; also the idle keep-alive bound), h2 windows as above, 1,024
concurrent streams per connection, 32 KiB header list, PING every 20 s with a
10 s timeout, rapid-reset limits at hyper/h2's defaults (20 pending accept
resets, 1,024 local error resets; CVE-2023-44487). Metrics:
`vlpds_http_server_connections_total`, `_connections_open`,
`vlpds_http_server_active_requests{version}` (h2 = streams awaiting a
response head).

Listen backlog: `--listen-backlog` (default 16384) instead of tokio's 1024.
The kernel clamps it to `net.core.somaxconn` (Linux; 4096 on benchbox, older
kernels 128) or `kern.ipc.somaxconn` (macOS, 128), so raise that as well
(`sysctl -w net.core.somaxconn=16384`, and `net.ipv4.tcp_max_syn_backlog`
for SYN floods of new clients). A full accept queue drops the SYN or the
final ACK and the client waits out a retransmit (1 s, then 2 s, ...):
benchbox's `TcpExtListenOverflows` grew 16 -> 1683 over one bench session
(1000 firehose subscribers connecting at once, proxy runs at 1024 in
flight). `ss -ltn` shows the effective queue (Send-Q) per listener;
`netstat -Lan` on macOS.

## Sync 1.1 checklist
- Commit object v3, `prev: null`, `rev` = per-repo monotonic TID, signed.
- `#commit` carries `since` (previous rev), `prevData` (previous MST root), and ops
  with `prev` CIDs for update and delete.
- `blocks` = commit + new record blocks + new MST nodes + **inversion-proof nodes**
  (siblings touched when inverting deletes/creates across merges/splits).
  - Proof generation: run the inverse ops on an Arc-clone of the new tree while
    recording every node read. Read nodes ∪ new nodes = proof set. The inverted root
    must equal `prevData`, which is a self-check on every commit (cheap, in-memory).
- No `tooBig`; enforce the 200-op / 2 MB limits instead.
- **External oracle:** a Go checker consumes our firehose and runs indigo's
  `repo.VerifyCommitMessage` (inversion check) + signature verification + chain
  continuity (`since`/`prevData` match the previous event for that DID) on every
  event. MST code is also tested against atproto interop test vectors.

## Scope for v1
XRPC: `server.createAccount` (locally minted did:plc-shaped DIDs, no PLC registration), `server.createSession`, `repo.{createRecord,putRecord,deleteRecord,applyWrites,getRecord,listRecords,describeRepo,uploadBlob}`,
`sync.{subscribeRepos,getRepo,getRecord,getLatestCommit,getRepoStatus,listRepos,getBlob}`.
Auth: HS256 session JWTs + admin token. No OAuth, email, moderation, or app-view proxying.

## Benchmark plan
- **Object store:** MinIO in Docker (supports conditional PUT), plus a latency-injection
  layer in our object_store wrapper to emulate S3 Standard (p50 ~25 ms, p99 ~80 ms)
  and S3 Express (~5 ms), so the latency results reflect real deployments
  rather than loopback.
- **Load generator (Rust, open-loop with fixed arrival schedule, HDR histograms):**
  - Fleet: 50k accounts pre-populated with ~500–2k records each, mix 80% create /
    10% put / 10% delete, ramp 10k → saturation.
  - Hot repo: 1 repo at 200/s (then push to find its ceiling) on top of the fleet load.
  - Cold-load: writes to evicted repos (rebuild cost).
  - Firehose consumer measuring commit→broadcast lag; Go checker validating.
  - Crash/recovery: kill -9 during load, verify no acked write lost and the firehose
    has no gaps or chain breaks.
- **Expectation on this machine** (M4 Pro, 14 cores, shared with the load generator
  and a Docker VM running MinIO): CPU-bound somewhere around 40–70k commits/s. Hitting
  100k likely needs the server on dedicated cores. We'll report where the
  ceiling is and what's eating it.

## HA: multiple nodes, partitioned write ownership

All nodes serve reads and writes; write *ownership* is partitioned. This is
the per-node-log design of "Planet scale" items 1–5 (`src/cluster.rs`,
`src/node.rs`, `src/nodelog.rs`); `bench/ha/RESULTS.md` has the failure matrix.

- **Shards.** 65,536 fixed hash slots grouped into contiguous ranges by a
  versioned layout (`assign/layout`; `--shards N` uniform ranges when a
  prefix is created), which splits and merges change online (see "Online
  shard split/merge"). A shard is the unit of ownership and state: one
  SlateDB at `state/{id}/` and an assignment object `assign/{id}`.
- **One log per node incarnation.** A node group-commits every shard's entries,
  tagged `(shard, epoch)`, into `log/{log_id}/{ordinal}.seg`. Up to K
  segment PUTs are in flight (`--log-inflight`, default 4), each written with
  `If-None-Match: *` at its ordinal; completions are finalized strictly in
  ordinal order (see "Pipelined segment PUTs"). A write is acked only after
  its segment and every earlier one are durable, and only while the node's
  lease is valid.
- **Node leases.** `nodes/{node_id}` holds `{log_id, addr, writer, renewals}`
  and is renewed by CAS on its ETag every TTL/5 (default TTL 10 s). Renewal
  bumps `renewals`, so every renewal changes the object.
- **Assignments.** `assign/{shard}` holds `{owner, log_id, epoch, seq_floor,
  history[spans]}` and changes only when a shard moves (CAS on its ETag). A
  node takes free or orphaned shards up to its fair share (shards ÷ live
  nodes) and closes and releases extras.
- **Handoff.** A graceful release closes the shards together: one barrier
  segment for all of them (once it is durable, every earlier entry of those
  shards is durable and applied), a checkpoint, then the span end in the
  assignment. A takeover from a dead node first **fences its log**: a
  conditional create of a fence object at the end of its durable prefix (its
  first ordinal that isn't a segment), which ends its last span for good. The new owner replays its shards' previous spans
  (one pass over each dead log for all shards) before serving.
- **Handback to a joiner.** A node owning more than its share hands the
  extras straight to the peers short of theirs (only peers it has seen for a
  join grace): after the close it CASes each assignment to name the joiner
  (epoch + 1, its own span closed at the barrier, an open span for the
  joiner starting at the log ordinal the joiner's lease last published,
  never inside an earlier span of the same log) and POSTs the handoffs to
  the joiner's `/internal/v1/cluster/nudge`. The joiner adopts them with no
  control-plane read (replay the spans before its own, wait out
  `seq_floor`, serve); a lost nudge is caught by its next step. A shard naming
  a node at an epoch it already opened is never adopted again (that is a
  failed release, not a handoff). Every other peer gets an empty nudge so
  its routing follows at once. Graceful shutdown first marks its lease
  `draining` (peers stop counting it toward fair shares or handing it
  shards), then hands its shards out the same way. Release → serving is the joiner's SlateDB open (~11 sequential
  store calls, ~220 ms at 20 ms per call: the shard's compactor starts after
  the open, which halved it from ~450 ms; it was a step interval plus a step
  plus the open, ~3 s at TTL 10 s).
- **Global firehose order with no global sequencer.**
  `seq = unix_micros × 256 + writer`, strictly increasing within a log. Each
  log carries a watermark (every event ≤ W is durable); every node k-way
  merges all node logs and emits an event once `seq ≤ min W`. Live data comes
  from owners over an internal stream, history and catch-up from S3 segments,
  and a dead log is drained to its fence. The merge is deterministic, so
  cursors replay identically on any node.
- **Global uniqueness:** handles are claimed with a conditional PUT of
  `handle/{handle}`; writer ids (the seq low byte) by CAS on `writers/{w}`.

- **Checkpoints.** Every owned shard gets an applied marker plus a
  memtable flush (an L0 SST PUT and a manifest update) once per
  `--checkpoint-every` (10 s), bounding a successor's replay. They are
  staggered (`--checkpoint-stagger`, default on): one shard every
  interval/shards instead of all of them back to back each interval, so
  the flushes' CPU (SST encoding + zstd on the runtime) and store PUTs
  spread evenly. Metrics: `vlpds_checkpoint_shard_seconds`, and the 10 ms
  ticker's lateness `vlpds_runtime_tick_late_seconds` /
  `vlpds_runtime_late_seconds_total` (runtime threads blocked or starved).
  A shard already checkpointed at the log's current durable ordinal is
  skipped: its marker and memtable are durable as of that ordinal (every
  write into a shard comes from a log segment or a checkpoint), so another
  flush would only rewrite the marker, an L0 SST PUT plus a manifest CAS
  per shard per interval on an idle node. While the log moves, every shard
  is still checkpointed each pass, including shards with no new entries:
  a successor's replay starts no earlier than before, and replay floors
  (retention) advance with the log.
  Checkpoints were never a burst: `checkpoint_all` goes one shard at a time
  (~37 ms each, store-bound). In-process (tests/all/checkpoint_stall.rs,
  256 shards, 8,000 writes/s, 3 runtime threads, 10 ms store) neither
  schedule stalls the runtime: worst 10 ms-tick lateness 7.2 ms
  back-to-back vs 9.3 ms staggered, write p99 27 vs 26 ms. The ~700 ms
  stalls after checkpoints in the laptop dry run (load average 20–37 on 14
  cores) were CPU starvation; the lateness metrics are there to check on
  benchbox.

Single-node mode is the same code with one node owning all shards.

### Forwarding deadlines and not-applied writes (`src/forward.rs`)

A forward fails at a time-to-first-byte deadline (3 s for quick calls, 30 s
for exports, uploads and proxying): an owner that doesn't answer is
presumed frozen, the client gets 503 `PartitionUnavailable` + Retry-After,
and the owner's lease moves the shard. That answer is ambiguous (the write
may still be applied), so it is never resent. But after a restart,
takeover or handback, the first write to each repo on its new owner is a
cold load, and on a loaded box these queued past 3 s: ~20 s of failed
writes after a node restart (capacity dry run, 2026-10-01).

Options were a longer write deadline (a frozen owner then holds every
forwarded write for that long), or telling "busy loading" apart from
"frozen". vlpds does the latter, and makes such failures retryable:

- **The owner answers early, unapplied.** A forwarded write (task-local
  marker set while serving a peer's request) carries a `worker::Claim`.
  If its worker hasn't taken it into a commit within
  `--forwarded-write-start-ms` (1 s), the handler abandons it: exactly one
  of take/abandon wins (a CAS), so an abandoned write is never applied and
  a taken one is always answered. The answer is 503 `RepoLoading`, well
  inside the 3 s deadline. A write that finds its shard gone before it
  started (the load says "not owned": the shard is moving) is answered 503
  `ShardMoved`; also never applied.
- **The entry node resends.** The node the client called buffers repo
  writes (createRecord, putRecord, deleteRecord, applyWrites; JSON, at most
  4 MiB) and resends one answered `RepoLoading` (after 10 ms) or
  `ShardMoved` (after 50 ms) to whoever owns the repo by then, itself
  included, for up to 20 s (`--retry-unapplied-writes`); then the last
  503 + Retry-After goes to the client. Own-account writes route by the
  token's DID without parsing the body, so they get this too. Metrics:
  `vlpds_write_retries_total{reason}`, `vlpds_writes_abandoned_total`.
- Directly received writes (the client called the owner) just wait for
  their load. Every 503 vlpds answers carries `Retry-After: 1`.

So a cold start or a shard move shows up as latency, while a frozen owner
still fails in 3 s. Measured (tests/all/cold_start.rs `restart_window_*`,
in-process, M4 Pro): 3 nodes, 48 shards, 50k repos x 100 records, a store
with 10 ms per call and 64 calls in flight, 1,500 writes/s with Zipf(1.0)
repo choice entering through two nodes while the third restarts gracefully
(its shards go to the others at 4 s and come back at 10 s). Before: 400
failed writes in 3 s (all 503s at the two shard moves), p99 78 ms. After:
0 failed, p99 243 ms over the window, worst one-second p99 669 ms (at the
moves; 1,736 resends), 1,991 recently written repos preloaded. Cold loads
in-process stay well under 1 s (the processes share one block cache), so
`RepoLoading` didn't fire there; it is what the capacity dry run's 20 s
restart stall needs.

### Pipelined segment PUTs (K in flight per log)

With one PUT in flight a node log commits at most one segment
(`--max-segment-mb`, 8 MB) per PUT round trip: ~155 MB/s, 44–54k commits/s at
25 ms PUT latency (bench 2026-10-02 §1). Bigger segments raise the ceiling
but each PUT gets slower, so the tail grows. Instead the sequencer keeps up
to K PUTs in flight:

- **Sealing.** Ordinals are assigned at seal time, in order. A segment is
  sealed when a slot is free and either nothing is in flight (the old
  behavior: whatever queued during the PUT is the next segment) or it holds
  at least `max_segment_bytes / K`. Extra PUTs start only under load, so the
  PUT rate at low load is unchanged; the ceiling becomes K full segments per
  round trip. One more trigger: the newest PUT in flight has *stalled* (been
  out for over 2x the moving average PUT latency, clamped to 5 ms ..
  `hedge_after`): what queued behind it goes out now and is acked when the
  stall ends instead of after the stall plus its own PUT
  (`vlpds_segment_stall_seals_total`). One stall seals one segment (timed
  from the newest PUT). Laptop, inj 7 ms lognormal, 20k/s: K=4 p99 41.5 ->
  39.2 ms (K=1 45.7). At low load K=4 and K=1 measure the same on the laptop
  (inj0 and inj7, 5k-20k/s: p50 within 0.4 ms) and on benchbox's 1M/50k grid
  (25k/s: 40/74 vs 42/72); benchbox's one 10k/5k K=4 sample (p50 17.7 vs 8.2)
  had slower PUTs (p50 7.2 vs 5.6 ms) and 2.2x larger segments, i.e. the
  disk, not the seal rule.
- **In-order finalization.** Completions are taken in ordinal order
  (`FuturesOrdered`): a segment that lands early waits for every earlier
  one. Only then does the finalizer apply it, write its applied marker, push
  it to the live ring and the merger, advance the watermark and
  `durable_ordinal`, and ack. So everything downstream of the finalizer
  (acks, SlateDB, `META_APPLIED`, checkpoints, close barriers, the firehose
  watermark, peer streams) covers a gap-free prefix of the log, exactly as
  with K = 1. Hedging is per segment, at most one hedge per ordinal.
- **`prefix_end`.** Each segment header records the writer's promise at seal
  time: every ordinal below it was already durable (the oldest PUT still in
  flight). It is at least `ordinal − K + 1`.

**The hole rule.** A crash can leave holes: ordinal n missing, n+1 present.
A log's *durable prefix* is its longest gap-free run of segments from the
start; it ends at the first ordinal that isn't a segment (missing, or a
fence). Since acks are in order, every acked write is inside the prefix, and
segments after the first hole were never acked, applied or emitted: they are
garbage. Everything that reads a log honors this:

- *Fencing* (`Cluster::fence`, `nodelog::first_free`) puts the fence at the
  end of the durable prefix, not after the highest object. It finds it from
  one LIST plus a few small GETs: the highest segment's `prefix_end` bounds
  where the first hole can be (only `[prefix_end, ordinal)` can hold one).
  Every fencer computes the same ordinal, and once fenced it never changes.
- *Sequential readers* (replay, follower S3 catch-up, backfill cursors, the
  merger's spill read-back) already stop at the first missing object or the
  fence, so they never reach garbage. A closed span ends at a fence or at a
  release's `durable_ordinal + 1`, so a hole inside it is still an error.
- *`backfill::seek`* binary-searches on "present", which holes make
  non-monotone: it could land on garbage past the fence. Its answer is
  checked: the segment before it must be in the prefix (probe its
  `[prefix_end, ordinal)` window), else the hole is the answer.

**Why fencing stays safe.** Let F be the fence ordinal: the first
non-segment when the fencer looked, made permanent by the conditional create
(a zombie segment landing first makes the create fail, and the fencer
re-scans). Every ordinal below F is a segment, so a successor replaying
`[start, F)` sees a gap-free prefix. The zombie can't ack anything at ≥ F:
acking any of it needs its own segment F durable first (acks are in order),
and its PUT at F collides with the fence, so it fail-stops instead (exit 3). Its PUTs at F+1 … F+K−1 may still
land, but nothing reads past F. Garbage is left in place; it's bounded by
K − 1 segments per crash.

### Log compression (`VLSEG05`, `--log-compression`)

Segments are ~5.4 KB per single-record commit on real data, ~85% of it the
firehose frame (MST proof blocks dominate). The sealed body is stored as one
zstd frame (level 1 by default, 0 = off) behind an uncompressed header
(`codec` byte + uncompressed `body_len`), so header-only reads
(`read_head`, `prefix_end`, fencing) are unchanged.

- **Writer.** The finalizer keeps the *uncompressed* sealed object: the live
  ring, the merger and peers' live streams get zero-copy slices of it, as
  before. Compression runs in the segment's upload task on the blocking
  pool (a few ms per full segment), and hedges/retries PUT the same
  compressed bytes (conflict resolution compares those).
- **Readers.** `segment::decode` restores exactly the bytes the writer sealed
  (codec byte reset), so entry offsets agree; `segment::parse` decodes
  first, so replay (decompressing its 16 read-ahead GETs in parallel),
  follower catch-up, the merger's spill read-back and fencing all handle
  either codec. Backfill decodes once per GET and caches the decompressed
  segment (its cache and read-ahead budgets count decompressed bytes), so
  many subscribers on one range cost one decode.

Measured (`bench/results/storage-2026-10-02` method: 300k real records from
815 repos replayed as single-record commits, 4.2 KB/entry, entries re-packed
into segments of each size; one M-series core):

| segment | log order (per-repo runs) | shuffled repos | one entry per repo |
|---|---|---|---|
| 16 KiB | 1.60x | 1.57x | — |
| 256 KiB | 3.84x | 1.95x | 1.95x |
| 1 MiB | 5.15x | 2.04x | — |
| 8 MiB | 5.61x | 2.07x | — |

(zstd 1; level 3 adds 3–30% for ~1.7x the CPU, level −1 loses ~8%.) Under
load segments are 0.5–8 MiB and mix many repos, so expect ~2x: commits
share DIDs, NSIDs, CBOR keys and, within a repo, upper MST nodes. CPU:
compress 0.7–1.0 GB/s (4.3–5.9 µs per 4.2 KB commit), decompress 2.3–3.8
GB/s (1.1–1.8 µs). At 75k commits/s (~80 µs of node CPU each) that's
~0.35 core, ~5%, and it halves PUT bytes (~400 → ~200 MB/s), upload time
and the retention window's storage.

### Log retention (`src/retention.rs`)

Without it the logs grow forever (~2.5 KB per commit stored, ~5 KB before
compression; a new prefix per node restart). A segment is deleted once **(a)** no replay can need it and **(b)**
it is older than the backfill window (`--log-retention`, default 72 h, by the
object's last-modified time). Each pass deletes at most 10,000 objects,
oldest first (one paged LIST from the log's head, one batched DELETE), so
storage is bounded by window × write rate plus a fence object per dead
incarnation.

*Who deletes.* A live log only by its owner. Dead logs (not a live lease's
log) only by the owner of the lowest-numbered shard, and only once fenced.
Deletes are idempotent: two nodes briefly both leading is harmless.

*(a) for a live log L: the replay floor.* For each shard the log applies
into, `nodelog::ShardSinks` keeps the lowest ordinal of L a crash replay
could read: its *insert floor* (L's next ordinal when the shard was opened)
until a checkpoint at or past the insert floor is durable (memtable
flushed), then that ordinal + 1. The log's floor is the minimum over its
shards (and shards closed in the last 2 min), capped at the last durable
segment, which is always kept so `first_free` finds the end of the log.

*(a) for a dead log X: successors opened every shard.* Each node publishes
`retain/{log_id}`: the shards its log's owner opened, with epochs. X is
deletable once, for every shard whose assignment history has a span in X,
some report shows it opened at an epoch above X's last span for it.

*Replay never reads a pruned range.* Replay of shard s starts at its durable
marker m = (log, ord) and reads forward through the spans after it
(`marker_span`, earliest span covering m). Three facts:

1. *Markers are unambiguous and only move forward.* Markers come from the
   finalizer and checkpoints (ordinals at or past the shard's insert floor,
   which is at or past its span start; checkpoints below the insert floor
   are skipped, since a marker at `start − 1` could name the end of an
   earlier span of the same log, A → B → A, and replay would restart there),
   the close barrier (a segment written after the open), and replay (an
   ordinal inside the span it read). Each names a position inside the span
   it was written for, and every later write names a later span or ordinal.
2. *An open leaves nothing before its span to read.* `open_many` replays
   every earlier span and flushes before it serves. Afterwards the durable
   marker lies at or past the end of each earlier span (or, with nothing
   read, the earlier spans hold nothing to read), so by fact 1 no later
   replay of s reads a span from before an epoch it was opened at. That is
   what a report certifies, and it stays true: a stale report is just
   conservative.
3. *A live floor is below every unapplied entry.* Under L, shard s has no
   entries below its insert floor. Below a durable checkpoint everything is
   applied, so replay starts past it. A released shard's marker is at its
   span's end. So every ordinal of L below the floor is, for every shard
   whose replay reaches L, either before its entries or already applied.

Replay starts each log at its lowest stored object (`first_ordinal`), so a
span start inside a pruned head (e.g. between a span's start and the
shard's insert floor, which hold nothing of it) is skipped, not an error. A
hole above the lowest object is still an error inside a closed span.

*Fences stay.* A dead log is pruned down to its fence: the segments below it,
the K − 1 garbage segments past it, and its report go. The fence stays,
because it is what makes a zombie of that incarnation fail-stop whatever
its clock says (a suspended VM waking days later). `first_free` on a
fence-only log returns the fence, and `last_seq_before` treats a pruned
predecessor as "long ago".

*Readers.* Before deleting, the pruner raises its report's `pruned_seq` to
the last seq it deletes: the *retained floor* (max over reports) bounds every
deleted event. A cursor below it gets `#info OutdatedCursor` and continues
from the floor (the protocol's "oldest available"). A reader that finds a
segment missing below the log's lowest object was overtaken by retention
(`backfill::Pruned`): it re-reads the floor and jumps with OutdatedCursor. A
peer follower draining a dead log skips to the lowest object it finds.

### Online shard split/merge (`src/reshard.rs`, `src/slots.rs`)

The slot space stays fixed (65,536 slots, `slot = top 16 bits of
sha256(routing key)`). What changes online is how slots group into shards:
a hot or large shard splits into two, two adjacent cold shards merge into
one. No acked write is lost, the affected slots are unavailable (503
`PartitionUnavailable`, which clients retry) only for a window like a
handback's, and every node routes by the same versioned map.

**The layout is data.** `assign/layout` (JSON, CAS on its ETag) holds
`{version, shards: [{id, lo, hi}], next_id, op_seq, op}`: contiguous slot
ranges covering `[0, 65536)`, each naming a *shard id*. Ids are stable,
never reused identifiers (`state/{id:03}`, `assign/{id:03}`, the `shard`
tag of log entries), no longer positions in a uniform split: a split
allocates two new ids, a merge one. The first node of a prefix creates
version 1 as `--shards` uniform ranges with ids 0..n (the uniform layout
of before). `version` increases only when routing changes (a flip below).
The object sits under `assign/`, so the LIST every step already makes for
assignments returns its ETag: nodes GET it only when it changed, and the
steady state costs no extra request. Each node installs the layout it read
into its partition table (`PartitionTable::shard_of(key)`); routing,
forwarding (forward.rs asks the table), fair shares (`|shards| / live`)
and acquisition all go by it.

**State keys are slot-major.** Every per-account key is
`0x01 ‖ slot (2 bytes, BE) ‖ family ‖ rest` (`state.rs`; the slot is that
of the key's routing key: the DID, or a private entry's routing key; the
handle and collection indexes use their account's DID). Shard-wide keys
(`meta/applied2`, ...) start with ASCII and sort outside `[0x01, 0x02)`,
so a clone never inherits them. A shard's slot range `[lo, hi)` is
exactly the key range `[01‖lo, 01‖hi)`, and a split or merge is a
**SlateDB clone with a projection range**, not a copy:

- split P → C1 `[lo, mid)`, C2 `[mid, hi)`: clone P twice, projected to
  each child's key range;
- merge A, B → M: one clone with two sources, each projected to its range
  (SlateDB's union clone requires disjoint ranges per source, which
  adjacent slot ranges are).

A clone writes a checkpoint into each source's manifest (pinning its SSTs)
and a manifest for the child that references them ("external SSTs"); it is
O(manifest), whatever the shard's size, and SlateDB makes it idempotent (a
retry finds the initialized clone). The child's compaction rewrites the
inherited SSTs into its own over time; until then the standalone
compactor/worker (they only know the DB root) read them through a store
that redirects those SST paths to their owners (`partition.rs`). The
alternatives were rejected: scan-and-route copying moves the whole shard
(hours for a hot 50 GB shard, exactly when it is stressed) and needs a
two-phase copy plus slot-filtered log catch-up to keep the window short;
a lazy read-through child changes every read path. The cost of slot-major
keys is that cross-account scans (listRepos, listReposByCollection,
searchAccounts, the large-repo index, OAuth GC, routing-prefix scans) walk
slot by slot: `state::FamilyScan` keeps one iterator over the shard and
`seek`s past slots without the family, so empty slots cost nothing and a
populated one costs one seek.

**Protocol.** One reshard op at a time, cluster-wide, recorded in the
layout as `op = {id, parents, children, driver}`:

1. *Plan* (admin call on any node, or the policy hook): CAS the layout to
   add `op` (children ids from `next_id`, used up by the plan itself, so
   an aborted op's clones are never mistaken for a later op's; split point
   default = midpoint; merges only of adjacent shards). `driver` = the
   parents' owner if they share one, else the planner. Every peer is
   nudged.
2. *Freeze* (each parent's owner, on its next step or nudge): the same
   close as a release (one barrier segment, `META_APPLIED` marker, memtable
   flush, DB closed), then a CAS of the parent's assignment to
   `owner: None, frozen: op.id`, its span closed at the barrier and
   `seq_floor` raised to the owner's watermark. A frozen shard is never
   acquired or handed out. Freezing is only ever done by an owner after a
   successful close, so **a frozen shard's DB holds every entry of every
   span in its history** (a close that fails fail-stops the node as
   before; a successor fences, replays, and freezes again). An unowned
   parent is acquired normally (replaying its history) and then frozen.
3. *Clone* (driver, once every parent is frozen with `op.id`): clone the
   children; write each child's assignment fresh (`epoch 0`, no history,
   `seq_floor` = max of the parents'). Nothing routes to a child yet. The
   write is a create, or a CAS over a still-fresh one (a retry): never a
   blind overwrite, so a driver presumed dead that wakes up late can't
   reset a child some node already took after the flip.
4. *Flip* (driver): CAS the layout to `version + 1` with the parents'
   ranges replaced by the children's and `op` cleared. This is the commit
   point. The driver then takes the children like free shards (epoch 1, an
   open span in its log), opens them and nudges every peer, whose routing
   follows at once; fair shares rebalance them later as usual.

**Why no acked write is lost.** A slot's writes are applied by exactly one
open shard at a time: the parent stops applying at its barrier (frozen
before any clone exists), the child opens only after the flip, and the
clone is taken of the frozen parent's flushed DB, which by step 2 already
holds every acked entry of the parent. A child's history starts empty: it
never replays a parent's spans, so log entries never need slot filtering.
A node with a stale layout routes the moved slots to the parent, which no
one serves (503, retried) until its next step or the flip's nudge; it
cannot apply them, since only the parent's (frozen) owner had it open.

**Crashes and aborts.** Every step is resumable from object-store state:

| crash point | recovery |
|---|---|
| op planned, parent not frozen | a parent's owner died: its successor fences and replays as always, then freezes |
| parent closed, freeze CAS not written | the parent is an orphan with an open span: taken over (fence, replay nothing new), then frozen |
| frozen, clone partial | the driver (or, if it is dead, the live node with the lowest id, which CASes itself in as driver) re-runs the clone (idempotent) and rewrites the children's assignments |
| flipped, children not taken | children are ordinary free shards in the layout; any node acquires them (epoch 1, empty history, `seq_floor` preserved in their assignment) |

`vlpds.admin.abortReshard` (or the driver on a permanent clone error) works
until the flip: CAS `op` away, then unfreeze the parents' assignments. A
parent left frozen with an op id that is no longer the layout's op while
it is still in the layout (an abort that crashed half-way) is unfrozen by
whichever node notices, after a fresh GET of the layout.

**What carries across.**
- *Epochs, spans, fences, replay markers:* per shard id, unchanged. A
  child starts at epoch 1 with no history; its applied marker is written by
  its own owner's finalizer and checkpoints as for any shard.
- *Seq order:* the children's `seq_floor` is the max over the frozen
  parents', so a repo's firehose order survives the move (commit-wait as for
  a takeover).
- *Retention:* a frozen shard never replays again (its DB is complete), so
  `needed_by` skips frozen assignments; a dead log whose last span of some
  shard is a parent's becomes deletable once the parent froze. Live logs
  release a frozen parent's replay floor after the usual retired grace.
  Dead-log pruning is led by the owner of the shard holding slot 0.
- *Firehose:* events and `?shard=k/n` filtering are by slot, so they are
  unaffected; cursors are seqs.
- *listRepos:* the order is `(slot, DID)`, a global order independent of
  the layout, and the cursor is the last DID (its slot is derived). Any
  node finds the shard holding the cursor's slot in its layout and serves
  or forwards from there, so an enumeration that spans a split or merge
  lists every repo that exists throughout exactly once.
  listReposByCollection and searchAccounts use the same order.
- *Retired parents:* their assignments stay (frozen) and their state
  directories stay: children read their SSTs until compaction rewrites
  them. Deleting a retired directory needs a check that no live manifest
  references it (`external_dbs`); not implemented yet (bounded leak: the
  parent's size at the split).

**Policy hook** (off by default): `--reshard-split-mb` /
`--reshard-split-writes` let the driver-elect (owner of slot 0) plan a split
of a shard whose SST bytes or entry rate exceed them, one op at a time.

### Liveness: observed lease changes on the observer's monotonic clock

No node ever compares its wall clock with another node's.

- **Peers.** Every step (each TTL/5), a node LISTs `nodes/` and records, per
  peer, the instant on *its own* monotonic clock at which it last saw that
  lease's ETag (or `renewals`) change. A lease seen for the first time gets a
  full TTL from first sight. A peer is presumed dead once its lease has gone
  unchanged for **TTL + skew** of the observer's time, judged as of before
  the LIST (a slow LIST can't age a lease). It is also dead once the observer
  has fenced its log: a renewal it sent before lapsing that lands late can't
  resurrect it.
- **Self.** A node's own validity is `send time of its last successful
  renewal + TTL − skew`, on its own monotonic clock. It stops acking and
  PUTting segments past that point. It never renews a lapsed lease, and a
  watchdog fail-stops it 2 × skew after the lapse, which is about when peers
  can first presume it dead.
- **Reassigned under us.** Every step compares the shards a node holds with
  the assignments; if one names another owner, a peer fenced us and the node
  fail-stops instead of serving stale reads until its next PUT collides.
- **Writer ids.** A claim is taken over only if its holder has no node lease
  at all. After creating its lease, the claimant rewrites the claim (CAS),
  which changes its ETag, so a joiner that read it before the lease existed
  fails its CAS instead of sharing the id.

Takeover after a crash is TTL + skew after the last observed renewal, plus at
most one step of observation delay, plus replay.

**Fast paths (benchbox 2026-10-03 found 15 s of 503s per kill -9):**
- *Refused probe.* A peer that has missed a renewal (unchanged for 1.5 renew
  intervals) gets a TCP connect to its advertised address each step. Refused
  means nothing listens there: the process is gone (kill -9, crash, OOM), so
  it is presumed dead at once. Presuming early is safe (the takeover fences
  its log first, see below); anything else (connects, times out, unreachable
  host) leaves the TTL rule in charge: a frozen process still has its socket,
  a dead machine doesn't answer at all. Takeover after a process death is
  ~1.5–2.5 renew intervals (3–5 s at TTL 10 s) plus replay.
- *Greeting.* A joiner's first loop step (not the inline startup step: the
  node must serve before it opens shards) POSTs `/internal/v1/cluster/hello`
  to every live peer, which reads its lease and starts following its log
  (`learn_peer`). Once every peer confirmed, the join grace (which waits for
  exactly that) is over: a node restarted with the same id reclaims the
  shards still assigned to its previous incarnation, which `join` already
  fenced, right away; and peers count a greeted joiner as settled, so
  handbacks start at their next step instead of a grace later.
- *Writes wait out the gap.* A forward refused at connect sent nothing, so
  the entry node resends a write (marker `forward::NotSent`; reason
  `unreachable`) until routing follows the takeover. A shard this node
  doesn't hold (`App::partition`) answers `ShardMoved`, also resent. Resends
  back off (doubling, ≤ 1 s): fixed 50 ms resends of every held write
  starved a restarted node (3 IO threads) and stretched its 0.5 s replay to
  60 s.
- *Replay* writes a shard's batch only for segments holding its entries, and
  its applied marker once at the end (85 shards x 342 segments were 29k
  SlateDB writes, most of them marker-only).
- *Graceful stop keeps serving* until its shards are handed out, its lease is
  gone and 500 ms more: a forward it would drop mid-request is ambiguous to
  the peer (a client 503), one it answers "not owned" is resent.

Laptop (3 nodes x 3+3 threads, 100k accounts / 10k active, inj25, 6k/s
across 3 loadgens, unrouted; per-second errors on the survivors' two
loadgens): kill -9 then restart after 15 s: HEAD 0b2a322 ~650 errors/s per
loadgen for 15 s (9.8k each), now ~40 each, all in the second of the kill
(writes in flight on the dead node). Restart after 2 s (within the TTL):
HEAD ~1.4k errors in 2 s, then 0.4–1.4k/s from +22 s to past the window's
end (the restarted node's replay under resends took 61 s), now ~40 each.
SIGTERM: 0 at HEAD and now (3–8 when a forward is in flight as the node
exits). The killed node's own loadgen fails until its restart either way.

### Why safety needs no clocks

Clocks only decide *when* a node is presumed dead. A wrong presumption must
cost availability, never an acked write. Three mechanisms make that hold
whatever the clocks do:

1. **Fencing.** A successor fences the dead log before it reassigns any of its
   shards, at the end of its durable prefix, and the span it replays ends at
   the fence. Every segment below the fence is replayed. The old owner can't
   ack anything at or past it: its PUT at the fence ordinal collides, so that
   segment never completes, and acks are in ordinal order. It fail-stops
   (exit 3). Segments it had in flight past the fence may land, but no reader
   goes past a fence (see "Pipelined segment PUTs"). An acked write is
   therefore always inside the span the successor replays.
2. **CAS assignments.** An assignment moves only by CAS on its ETag, so each
   epoch has one owner, and its history (spans with fence- or barrier-final
   ends) is what the next owner replays. SlateDB's writer epoch fences a
   second writer on the state itself as well.
3. **Self fail-stop.** A node stops acking when its own monotonic validity
   ends, when a renewal CAS conflicts, when a shard is reassigned under it,
   when a close fails (a shard whose barrier never became durable is never
   released, since its entries may still be in flight past the span end it
   would publish), and when its log is fenced.

Even a peer that presumes a live node dead immediately (clock jumps,
arbitrary offsets) causes only a fence and a fail-stop. Ownership is decided
by CAS on S3 and durability by conditional PUTs, both of which are
linearizable.

**Remaining clock assumptions:**
- **Bounded drift *rate*, not offset.** The owner's validity
  (TTL − skew of its time) must end before an observer's TTL + skew of its
  own time elapses: `(TTL − skew)(1 + ρ) ≤ (TTL + skew)(1 − ρ)`, so
  ρ ≤ skew/TTL = 20 %. Real oscillators drift ~10⁻⁵. Within that bound a
  presumed-dead node has already stopped serving, so reads are not stale
  either. Beyond it only availability and read freshness suffer, not acked
  writes.
- **Monotonic clocks count paused time** (`CLOCK_MONOTONIC` counts SIGSTOP and
  cgroup freezes). A VM or host suspend that stops the monotonic clock makes
  a node believe its lease is still valid on wake. It then serves stale reads
  until its next PUT hits the fence, but acks nothing.
- **Wall-clock offset affects only seq ordering and merge latency.**
  - Seqs are wall-clock based. When a shard moves, the new owner's seqs must
    exceed the old owner's for its repos to keep their firehose order. The
    assignment carries `seq_floor`: the releaser's watermark at release, or a
    dead log's last segment seq at fence time. A new owner whose clock is
    behind waits until its clock passes it (commit-wait, capped at 30 s)
    before serving.
  - The merged firehose emits at `min W`, so it lags by the largest offset
    between nodes.
  - Revs use `next_rev(prev)` and stay monotonic regardless of the clock.

**Renewal RTT ceiling.** Renewals are sequential CAS PUTs, and validity counts
from the send time. A renewal round trip above `(TTL − skew)/2` = 0.4 × TTL
therefore opens a validity gap and fail-stops the node. That is 4 s at the
production default TTL of 10 s (`--lease-ttl-ms`, which warns below 10 s
outside dev mode), and 1.2 s at the 3 s TTL the HA bench uses. A cluster-wide
S3 brownout past the ceiling stops every node. Keep the TTL at 10 s or more.

**Control-plane reads.** Each step makes one LIST of `nodes/` and one of
`assign/` (per 1,000 objects), plus a GET only for objects whose ETag
changed: one per peer renewal, one per moved shard. Every 150 steps (~5 min) it
re-reads every assignment as a safety net. Releases CAS against the cached
assignment and re-read only on a conflict.
A nudge also wakes the step loop early (shards released without a
recipient, or a peer that left).

## What benchmarking changed (Oct 2026)

- **Hedged segment PUTs.** If a PUT hasn't finished after 100 ms, an identical
  conditional PUT is raced against it (safe: same bytes, If-None-Match; the
  loser's AlreadyExists is verified by content). On local MinIO the tail came
  from MinIO serializing same-key writes on a Docker volume, which hedging
  can't fix; native/real S3 doesn't have that pathology.
- **Admission control.** Write requests beyond `--max-inflight-writes` get a
  fast 503 `Overloaded`; without it a latency blip in an open-loop workload
  snowballs into connection storms.
- **Cold loads.** SlateDB scans default to one block per GET; repo loads use
  1 MiB read-ahead, 16 KiB SST blocks, a 256-permit load semaphore, and the
  SlateDB local disk cache (cache-on-flush/compaction). 20k cold loads/s at
  p99 < 1 ms on 5-record repos.
- **Separate HTTP pools** for the commit log and state reads, so a read storm
  never queues in front of commit PUTs.
- **HTTP/2** (h2c) with large flow-control windows (4 MiB stream / 64 MiB
  connection); default 64 KiB windows split request bodies into tiny DATA
  frames and trip h2's flood guard.
- **jemalloc** over mimalloc: same throughput, much better tail (p99.9 162 ms
  vs 1040 ms at 75k/s) and introspection for the metrics endpoint.

## Planet scale: 1–5 B accounts (design analysis, not yet implemented)

Assumptions: 5 B accounts, 100–500 M daily-active repos, 100k–500k repos
active at any moment. Peak writes: active repos × ~10–30 writes/day →
roughly **200–500k commits/s at peak**. Reads plus AppView proxying:
**0.2–2 M req/s**.

### Sizing
| Quantity | Estimate | Notes |
|---|---|---|
| Repo state | ~3 PB (5 B × ~600 KB avg) | Heavy-tailed; most repos are tiny. S3 cost ≈ $70k/month |
| Account + head metadata | ~5 TB | ~1 KB per account |
| Hot MSTs in memory | 500k active × ~150 KB ≈ 75 GB cluster-wide | ~1.5 GB/node at 50 nodes |
| Cold repo activations | 100–500 M/day ≈ 1–6k/s avg, ~20k/s peak | ~600 KB read each, 3–12 GB/s aggregate; local NVMe SST cache + MST snapshots for big repos |
| Commit CPU | ~70 µs × 500k/s ≈ 35 cores | Not the bottleneck at cluster scale |
| Firehose volume | 500k ev/s × ~1.5 KB ≈ 750 MB/s per full subscriber | Sharded subscriptions (`?shard=k/n`) |

### What breaks if we just raise P
The current design ties four things to one *partition*: ownership/lease,
segment log, SlateDB instance, and firehose merge input. Each scales
differently:

- **Log PUTs scale with P, not throughput.** One PUT in flight per partition
  ≈ P × (1/PUT latency) PUTs/s when busy. With P = 4096 that's ~160k PUT/s
  (~$70k/day), versus ~1.5k PUT/s if 1.5 GB/s were written as ~1 MB segments.
- **Leases.** 4096 per-partition leases renewed every ~3 s ≈ 1.4k CAS PUT/s
  of pure overhead.
- **Firehose merge.** Every node streaming every partition is O(N·P)
  connections, and the global watermark is a min over P inputs.
- **The hash is permanent.** `hash(did) % P` can never change without
  rewriting every partition, so P must be chosen huge up front.

### Recommended architecture at this scale
1. **Fixed hash-slot space** (e.g. 65,536 slots = top 16 bits of the DID hash),
   permanent.
2. **Shards own contiguous slot ranges** and are the unit of ownership and
   state. Start with ~2–4k shards (~1–2 M accounts each); split hot or large
   shards online, as Redis Cluster / CockroachDB ranges do (implemented:
   "Online shard split/merge", a metadata-only SlateDB clone per child). Each shard keeps
   one SlateDB (state lives in S3, so moving a shard is cheap: open manifest,
   warm cache, replay tail).
3. **One log per node, not per shard.** Each node group-commits all its
   shards' entries (tagged `(shard, epoch)`) into one segment stream, so
   segment size scales with node throughput (~1 MB segments, ~1–2k PUT/s
   cluster-wide). Shard handoff reads the previous owner's log tail filtered
   by shard. When a node dies, its log is **fenced as a whole**, BookKeeper /
   Pulsar ledger style: a successor conditionally writes a fence object at the
   dead log's next ordinal, then replays its tail. A restarted node opens a
   new log id.
4. **Node-level leases plus a shard-assignment map.** Each node renews one
   lease. Shard→node assignment changes only on moves (CAS on assignment
   objects), so per-shard renew traffic disappears.
5. **Firehose merges N node logs**, not thousands of shard streams. Each node
   log is already ordered and carries one watermark. Per-repo order holds
   across handoff (log A up to the handoff point, then log B).
6. **Separate tiers:**
   - *Write/owner nodes* (16–32 cores): ~50–150 of them at 500k commits/s
     peak, sized by write throughput and hot-repo memory.
   - *Read/proxy and firehose fan-out nodes*: not planned (see "Read
     replicas and fan-out nodes: not planned"); full nodes serve proxying and
     the firehose, and sharded subscriptions (`?shard=k/n`, §5) split the
     stream for consumers that can't take all of it.
7. **Global indexes at 5 B scale.**
   - Handles: S3 objects for uniqueness, plus a cache.
   - listRepos (implemented): repos come in (slot, DID) order, the cursor
     is `{slot}:{last DID}` (layout-independent, so it survives splits and
     merges), and a page is served by the owner of the shard holding that
     slot from its own SlateDB (one snapshot per shard; heads merge-joined
     with accounts), continuing through the following shards it owns and
     hopping to the next owner only to fill the page. Any node accepts the cursor and forwards the page to the
     owner (`/internal/v1/sync/listRepos`, body passed through unparsed),
     so a page costs one shard scan instead of a scan on every node plus a
     merge. (slot, DID) is a stable key order: a repo that exists for the
     whole enumeration is listed exactly once. 1M repos over 64
     shards / 3 in-process nodes: see TODO.md.
   - Rate limits and abuse controls per shard.

The current implementation (per-partition logs, P = 16–64, per-partition
leases) is the right shape for a handful of nodes and tens of millions of
accounts. Moving to items 2–5 is mostly confined to the log and cluster
layers: segments, the sequencer, leases and the merger. Repo workers, the MST,
SlateDB state, XRPC and OAuth are unchanged.

## Initial deployment sizing: Bluesky scale with headroom

Baseline today: **~50 M accounts, ~1.5 M accounts writing per day, 500–2,000
commits/s at daily peak.**
Design headroom: **100× write activity** (→ ~200k commits/s peak) and
**20× general activity** (→ ~1 B accounts, ~30 M daily writers, 20× reads,
proxy traffic and concurrently active repos).

| Dimension | Today | With headroom | Basis |
|---|---|---|---|
| Accounts | 50 M | 1 B | 20× |
| Commits/s (peak) | 0.5–2k | 200k | 100× |
| Concurrently active repos | ~50k | ~1 M | 20× |
| Proxy + read req/s | ~50–100k | 1–2 M | 20× |
| Firehose events/s | ~2k (~3 MB/s) | 200k (~300 MB/s per subscriber) | 100× |
| Repo state in S3 | ~15 TB | ~300 TB | ~300 KB avg repo |

### Owner (write) nodes
- **Throughput.** Measured ~75k commits/s on 6 cores of an M4 Pro, which is
  roughly 12k commits/s per core including HTTP.
  - Today's 2k/s needs well under one core; HA alone sets the minimum at
    **3 nodes**.
  - At 100× (200k/s) that's ~17 cores of commit work. With 50% headroom and
    the loss of one node, plan **5–6 × 16 vCPU / 64 GB** (e.g. c7g/m7g.4xlarge).
- **Memory.** About 1 M active repos at 20× × ~150 KB of in-memory MST is
  ~150 GB cluster-wide, ~25–30 GB per node at 6 nodes. A more compact node
  representation could cut this 2–3×.
- **Shards.** 65,536 hash slots grouped into **256 shards** (~200k accounts
  per shard today, ~4 M at 1 B accounts). That's ~85 shards per node today
  and ~40 at 6 nodes, which leaves enough granularity to rebalance and to
  split shards up to the planet-scale design.

### Log and storage
- **Per-node log is needed early, for cost.**
  - With per-shard logs, PUT rate ≈ min(write rate, shards / PUT latency).
    At today's 2k commits/s nearly every commit is its own PUT: ~2k PUT/s
    ≈ **$26k/month**. At 100× it's ~10k PUT/s ≈ $130k/month.
  - A per-node log costs ~nodes × 40 PUT/s = ~120–240 PUT/s ≈ **$1.5–3k/month**
    at any write rate, and gives larger segments.
- **S3 Standard for everything** (log, SlateDB state, blobs; no S3 Express).
  It survives an AZ loss: ~40–50 ms p50 commit ack, ~150 ms p99 (measured with
  an S3-like latency model).
- **SlateDB state** sits on S3 Standard with each node's NVMe as the SST
  disk cache.
- **Log retention** is ~72 h for firehose backfill: ~1.5 TB today, ~150 TB at
  100× before compression, about half that stored (`src/retention.rs`,
  "Log retention" and "Log compression" above). Single-record commits
  take ~2.3–3.3 KB of segment (repos of 300–500 records; the MST path nodes
  in the CAR dominate): the record and head values aren't stored twice, they
  are rebuilt from the commit's CAR at replay (`segment::derive_commit_muts`).

### Separate tiers
None. Full nodes serve proxying and the firehose; see "Read replicas and
fan-out nodes: not planned" for when that would change.

### Must happen before any production data
- **Switch hashing to fixed 65,536 slots → shard map.** `hash % P` can never
  be changed later without rewriting every partition.
- **Per-node log + node leases + shard-assignment map**, for PUT cost and
  lease overhead (see "Planet scale").

## Read replicas and fan-out nodes: not planned

Separate read/proxy nodes (SlateDB `DbReader` replicas) and firehose fan-out
nodes were designed and rejected for now: full nodes cover both jobs well
past Bluesky's scale. Three full nodes serve today's proxy traffic, and a full
firehose subscriber is ~12 Mbit/s at today's ~345 commits/s. Relays that need
to split the stream use `?shard=k/n` and per-shard `listRepos` cursors. The
thresholds where a dedicated tier would start to pay: proxy traffic above
~300k req/s (a reader is NIC-bound at ~210k proxied req/s per 10 Gbit), or
dozens of full-firehose subscribers at 20-100x write load (a 10 Gbit node
serves ~25 full subscribers at 20x and ~5 at 100x). Before adding either,
prefer a DID-aware load balancer (removes the extra proxy hop) and more full
nodes.

## Partial MSTs (design + prototype; not wired in)

**Problem.** Section 2 keeps the whole tree of every cached repo in memory and
rebuilds it from `R/` on a cold load. Real writers in one hour (~188k repos)
have median 7.3k, mean 19.4k and p99 169k records. At ~215–250 B/record that
is ~850 GB of trees for one hour of writers, while a 256 GB node caches only
~35k average active repos. A write to a cold repo costs O(n): it scans
~315 B/record of `R/` (record bytes included) plus ~0.25–0.33 µs/record of
MST CPU. The goal: memory proportional to the **paths** being written, and a
cold write costing O(log n) node reads.

**Key fact.** A write at key K only touches three root-to-bottom search
paths: K's own, its predecessor P's (the right spine that a delete merges),
and its successor S's (the left spine). `prove_mutation` only walks K's path.
The prototype checks this claim byte for byte (below). The catch: recomputing
the root needs the CID of every sibling hanging off those paths, and each of
those CIDs covers its whole subtree.

### Options (measured with `tests/all/mst_lazy.rs` `bench`, in-memory store)
Shared numbers: node blocks total **79–80 B/record** on the real repo and on
synthetic repos with a real collection mix and TIDs. A write's path is 8–12
nodes deep for 10k–1M records (9 on the 43.6k real repo). A commit emits 8–12
MST blocks.

| | (a) persist every node | (b) persist interior (h>=1) | (c) derived-only, rebuild by key range | (d) persist h>=2 |
|---|---|---|---|---|
| extra state bytes / record | 79 B (+25% of `R/` raw, ~+60% zstd: hashes don't compress) | **28 B** (+9% / ~+22%) | 0 | 7.5 B |
| cold write: dependent reads | depth (8–12) | depth−1 (7–11) + 1 `R/` scan of ~7 records | **O(n)**: every sibling's CID needs its whole subtree, i.e. a full `R/` scan + hash per write | depth−2 + 1 scan of ~25–50 records |
| `M/` puts / commit (100k repo) | 7.7–8.2 nodes, ~5.0 KB | 6.9–7.0 nodes, ~4.5–4.9 KB | 0 | 6 nodes, ~4.0–4.2 KB |
| `M/` deletes / commit | ~8 | ~7 | 0 | ~6 |
| getBlocks of a node CID | point read | point read (interior); leaf needs a locator | walk | point read (h>=2) |

- **(c) fails the goal.** The MST layout is a pure function of the keys, so
  any subtree *can* be rebuilt from an `R/` range scan. But the root CID
  depends on all n keys, so every write pays the scan. For a p99 repo that
  is 59 MB of `R/` and ~40 ms of CPU per write. Its only useful form is
  **(c′)**: keep interior nodes resident and drop leaves (rebuilt per write
  from ~7 records). That cuts memory 2.3x (93 vs 215 B/record) but leaves
  the O(n) cold load as it is.
- **(a)** buys point-read getBlocks for leaves. It costs 3x the storage of
  (b) and saves only one small `R/` scan per write. A leaf's key range
  usually shares an SST block with the records being written anyway.
- **(d)** saves 1 node write per commit and 3.7x of storage compared with
  (b). The cost: each cold write scans 25–50 records (~10–16 KB of `R/`)
  instead of ~7, and 1.5–2x more resident nodes.

**Choice: (b), interior nodes persisted, leaves derived.** It has the lowest
cold-write I/O per stored byte. It is also the smallest change to the
"records are the truth" model: leaves, which are 3/4 of the nodes, stay
derived, and every loaded node is verified against its parent's link.

### Design
- **Layout.** `M/{did}\0{cid digest}` → node block. The key is slot-prefixed
  like `R/` (`state::keyed`), so resharding moves it with the shard's slot
  range and nothing else changes. Lookups use the CID the parent links to,
  and every read is hash-checked.
- **Loading.**
  - Open = read the root by `head.data`. A root that is missing (a small
    repo whose root is a leaf, or a repo not yet backfilled) means a full
    rebuild from `R/` plus a backfill of its interior nodes.
  - Each op walks K, then "before K", then "after K" (`mst_lazy::Mode`).
    - A child of height >= 1 is read from `M/`.
    - A leaf is rebuilt from `R/(lo, hi)`. The bounds are the separator keys
      inherited down the path, so the scan returns exactly the leaf's keys.
    - A rebuilt leaf must hash to the link, otherwise `Invalid`. This keeps
      today's root check per path.
    - A missing `M/` node falls back to an `R/` rebuild of that subtree,
      which is self-healing.
  - Mutations, CIDs and proof marking are `mst::Tree`'s own code, running on
    the partial tree (unloaded children are `Child { node: None, cid }`).
- **Persistence.** The puts and deletes go in the commit's state batch,
  atomic with `R/` and `h/`.
  - Puts = the written blocks with height >= 1, except proof-only
    neighbours, which are already stored.
  - Deletes = persisted nodes seen on the batch's walks that are no longer
    at their position (height, a key below them) in the new tree. Every
    replaced node lies on those walks.
  - Invariant (tested): after every commit, `M/` holds **exactly** the
    interior nodes of the tree at `head.data`. No garbage, nothing missing.
  - Replay: puts come from the commit CAR's blocks (height of a block = the
    height of any key in it; a node without keys is interior), so they add
    no log bytes. Deletes (~7 × 33 B) ride in the segment's `extra` muts:
    ~+4% segment bytes.
- **Invariants.**
  - The root CID, the commit's MST blocks (in order), getRecord proofs and
    getRepo's blocks are byte-identical to the full tree's.
  - Sync 1.1 completeness: creates and deletes carry the neighbour nodes,
    because the P and S spines are loaded before `prove_mutation` runs (it
    ignores `Partial`, so a missing neighbour would have silently shrunk
    the proof; the tests compare full block lists).
- **Cache policy.**
  - Unit: the loaded path nodes, in one LRU per worker by bytes (`heap_bytes`).
  - Eviction turns a clean subtree back into `{node: None, cid}`; the root
    always stays. Dirty nodes are pinned until their commit is written, and
    only clean subtrees are dropped. Unloading happens between commits only:
    the delete check relies on a batch's walked nodes staying loaded until
    its write.
  - "Large repo" pinning and `L/` preloads become unnecessary: a 1M-record
    repo opens with one read.
- **Snapshots / DurableView.** A view keeps its `Arc` root as today. A reader
  that hits an unloaded child reads `M/`/`R/` *as of a SlateDB snapshot*
  taken with the view. Otherwise a later commit may have deleted the node or
  changed the leaf's records, and the hash check would fail. A read-only
  cursor loads into a private copy and never mutates the shared view.
  getRecord proofs: walk K on the view (one path). On a mismatch (no
  snapshot), retry on the newest view.
- **getRepo.** It streams from a DB snapshot with no resident tree. It does a
  pre-order DFS: interior nodes are point reads (or one prefix scan of
  `M/{did}`, 28 B/record, which is 11x less than `R/`), and leaves come from
  the same forward `R/` scan that yields the records, since leaves come up
  in key order. Memory is one path.
- **getBlocks / NodeIndex.**
  - Interior CIDs are a direct `M/` point read, so no index is needed.
  - Leaf CIDs need a locator: keep `NodeIndex` (built by one streaming
    export walk, then advanced per commit as today), or persist
    `l/{did}\0{cid8}` → first key (~12 B/record more). Leaf-node getBlocks
    is rare, so the walk is the default.
- **Storage format.** `M/` is a new family. vlpds is unshipped, so there is
  no migration: bulk import and `importRepo` write `M/` (backfill =
  `mst_lazy::build_tree` + `persisted_nodes`).

### Prototype results (`src/mst_lazy.rs`; `tests/all/mst_lazy.rs`)
- **Correctness.** The lazy tree runs in lockstep with `mst::Tree`.
  - Workloads:
    - random histories: 36 seeds × 80 commits of 1–6 ops, any mix of
      create, update, delete and delete-missing, 40% of commits cold and the
      rest warm with random unloads, persist height 0/1/2 (also passes at
      1,200 seeds);
    - the real repo `~/repo.car` (43,649 records): 1,200 commits of appends,
      random-rkey creates, updates and deletes, at heights 1 and 2.
  - Checked against the full tree: equal previous values, root CIDs, commit
    block lists, getRecord proofs, `get` and getRepo block streams.
  - The store's node set equals the reference tree's interior set after
    every commit.
  - A store with nodes missing, or with no nodes at all, still rebuilds
    exactly. A corrupted record is caught.
- **Measured** (dev-release profile, M4 Pro, in-memory store, so I/O is
  counted, not timed). Per cold write, option (b):

| repo | full tree heap | cold write: CPU / `M/` reads / `R/` recs | resident after | `M/` put B / commit | steady CPU full vs lazy | getRepo walk vs export |
|---|---|---|---|---|---|---|
| real 43.6k | 9.2 MB | 10–13 µs / 8.1 / ~7 | **9–13 KB** | 2.7–4.2 KB | 3.1 vs 6.7 µs | 0.7 vs 7.4 ms |
| 10k | 2.1 MB | 8–11 µs / 7 / ~7 | 10 KB | 3.2 KB | 3.0 vs 5.6 µs | 0.2 vs 1.6 ms |
| 100k | 21.5 MB | 12–13 µs / 7 / ~6 | 13–15 KB | 4.8 KB | 4.2 vs 7.0 µs | 1.4 vs 16.6 ms |
| 1M | 215 MB | 16–18 µs / 11 / ~7 | 17 KB | 5.6 KB | 5.2 vs 10.6 µs | 13.8 vs 169 ms |

- The cold write's latency is its dependent `M/` reads. From the SlateDB
  NVMe cache (~50–100 µs each) that is **~0.5–1.1 ms at any size**. Today
  it is O(n): ~40 ms of CPU plus 59 MB of `R/` for a 169k-record repo.
  Reads that miss to S3 cost ~20–40 ms each and are dependent. Mitigation:
  for repos with `M/` under ~1 MiB (<~35k records, which covers the median
  and the mean), read the whole `M/{did}` prefix in one read-ahead scan and
  keep only the path. Larger repos do point reads, and their top levels
  stay cached.
- In steady state, lazy costs ~2x the full tree's MST CPU: 3 walks per op
  and the delete check. That is 3–5 µs more per commit, against ~70 µs of
  commit CPU in total.
- **The cost is write amplification.** State bytes per commit grow from
  ~740 B to ~3.5–6.3 KB, plus ~7–11 tombstones. CID keys don't coalesce in
  the memtable, so every commit to a hot repo rewrites its top path. That
  is fine at today's 2k commits/s (~10 MB/s). At the 200k/s headroom target
  it is ~1 GB/s into SlateDB before compaction. The fix, if needed:
  write-back per checkpoint window. Keep dirty interior nodes resident,
  persist only the window's final versions, and record in
  `m/{did}` → (root, rev) which version `M/` holds. A stale marker after a
  crash means falling back to an `R/` rebuild of the stale subtrees. Hot
  repos then pay ~1 path per window instead of per commit.

### Recommendation and sizing effect
**Wire it in, behind the existing `RepoState::tree` API, in stages.**
- **Memory.** Resident memory per active repo goes from ~215 B × records
  to ~10–20 KB of paths.
- **Capacity.** A 256 GB node's ~35k cacheable average repos becomes, at a
  64 GB MST budget, ~4M repos' write paths, and the root alone is ~1 KB. An
  hour of writers, ~188k repos × ~15 KB ≈ **3 GB instead of ~850 GB**. The
  3 × 256 GB cluster stops being memory-bound on MSTs, so the cache budget
  can shrink and the memory can go to the SlateDB block cache instead.
- **Cold-load tail.** It no longer depends on repo size: ~8–12 dependent
  reads. The p99 169k-record repo goes from ~40 ms of CPU + 59 MB of I/O to
  ~1 ms (cached) or one 5 MB `M/` prefix scan (not cached). Pinning large
  repos and `L/` preloads can be retired.
- **Costs.** +28 B/record of state (~+9% raw; ~0.5 TB, ~$11/mo at crawl scale) and
  ~4–6 KB more state writes per commit.

**Stages.**
1. `M/` family, written through in the state batch. Puts derived from the
   CAR at replay, deletes in `extra`. Backfill on bulk import and
   `importRepo`, and on first full load. The worker still keeps full trees;
   CI asserts `M/` == interior set.
2. Lazy open behind a flag. `load_tree` = root read. Worker ops call
   `prepare` (the 3 walks), and the existing write path is unchanged
   (`write_diff_blocks` + `Persist`). Byte-level lockstep checks run against
   a full rebuild in debug builds, and `sync11_property` + `go_checker` run
   in lazy mode.
3. Readers: DurableView carries a SlateDB snapshot. Proofs and getRecord use
   read-only lazy cursors, getRepo streams the export, getBlocks reads `M/`
   (leaf locator via the export walk).
4. Byte-budgeted path LRU replaces the per-repo LRU. Retire pinning and `L/`
   preloads, and add the small-repo `M/` prefix prefetch.
5. Only if the write volume matters at the 100x target: checkpoint-window
   write-back with an `m/{did}` marker.
