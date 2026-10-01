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
  `RepoState { head: commit cid + rev, mst: Arc<Node>, key: SigningKey }`, LRU-bounded by bytes.
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
- Escape hatch for very large repos (≥1M records): periodically write an MST snapshot
  object so a cold load doesn't need an O(n) rebuild. Not needed for v1.

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
- Segment format: concatenated length-prefixed firehose frames + footer index
  (seq → offset), so serving the firehose is a byte copy and cursor seeks are one
  range GET.
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
- Keys:
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
- Each shard's compactor (coordinator + one worker writing the same SST
  format) starts after the DB opens rather than inside the open, so a
  takeover or handback serves ~11 store round trips sooner. Its outputs
  aren't written into the local disk cache (reads fill it).
- SlateDB GC deletes SSTs compaction replaced once they are
  `--slatedb-gc-min-age` old (24 h; SlateDB's default is 5 min). A long scan
  (a 10M-record getRepo, listRepos) reads the SSTs of the manifest it started
  with, so a day of compaction output is kept as garbage; it is linear in
  the write rate, not cumulative.

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

- **Shards.** 65,536 fixed hash slots grouped into `--shards N` contiguous
  ranges. A shard is the unit of ownership and state: one SlateDB at
  `state/{shard}/` and an assignment object `assign/{shard}`.
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

Single-node mode is the same code with one node owning all shards.

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
  round trip.
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

### Log retention (`src/retention.rs`)

Without it the logs grow forever (~3.5 KB per commit, a new prefix per node
restart). A segment is deleted once **(a)** no replay can need it and **(b)**
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
| Firehose volume | 500k ev/s × ~1.5 KB ≈ 750 MB/s per full subscriber | Needs a fan-out tier and sharded subscriptions |

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
   shards online, as Redis Cluster / CockroachDB ranges do. Each shard keeps
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
   - *Read/proxy nodes*: stateless. They forward writes to owners, serve
     AppView proxying from a cached signing-key/status index, and serve
     record reads from SlateDB read-only replicas (`DbReader`) for
     stale-tolerant reads.
   - *Firehose fan-out nodes*: consume the node logs, serve subscribers, and
     offer cursor backfill straight from S3 segments. They also serve sharded
     subscriptions (`?shard=k/n` by DID hash, implemented: §5) for consumers
     that can't take the full ~750 MB/s.
7. **Global indexes at 5 B scale.**
   - Handles: S3 objects for uniqueness, plus a cache.
   - listRepos (implemented): the cursor is `{shard}:{last DID}` and a page
     is served by that shard's owner from its own SlateDB (one snapshot per
     shard; heads merge-joined with accounts), continuing through the
     following shards it owns and hopping to the next owner only to fill
     the page. Any node accepts the cursor and forwards the page to the
     owner (`/internal/v1/sync/listRepos`, body passed through unparsed),
     so a page costs one shard scan instead of a scan on every node plus a
     merge. Per-shard DID order is a stable key order: a repo that exists
     for the whole enumeration is listed exactly once. 1M repos over 64
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
  100× (`src/retention.rs`, "Log retention" above). Single-record commits
  take ~2.3–3.3 KB of segment (repos of 300–500 records; the MST path nodes
  in the CAR dominate): the record and head values aren't stored twice, they
  are rebuilt from the commit's CAR at replay (`segment::derive_commit_muts`).

### Separate tiers
- **Read/proxy tier**: stateless, behind the load balancer.
  - Serves AppView proxying from an in-memory signing-key/status cache with
    cached service JWTs. Record reads come from SlateDB `DbReader` replicas;
    writes forward to the owners.
  - At 1–2 M req/s the limit is mostly network (~5 KB average response ⇒
    5–10 GB/s): **2 nodes today, 10–16 × 25 Gbps at 20×**.
- **Firehose fan-out tier.** Merges the node logs and serves relays,
  including cursor backfill from S3.
  - Today ~3 MB/s per subscriber, so **2 nodes** cover HA.
  - At 100×, ~300 MB/s per subscriber × tens of subscribers ⇒ **4–8 nodes**.
    Sharded subscriptions become important here.

### Must happen before any production data
- **Switch hashing to fixed 65,536 slots → shard map.** `hash % P` can never
  be changed later without rewriting every partition.
- **Per-node log + node leases + shard-assignment map**, for PUT cost and
  lease overhead (see "Planet scale").
