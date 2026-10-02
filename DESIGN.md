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
                                               │  per-repo MST paths in memory (Arc, copy-on-write)
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
  `RepoState { head: commit cid + rev, mst: LazyTree, key: SigningKey }`, where the
  MST holds only the paths recent operations visited (§2). LRU-bounded by count
  (`--cache-per-worker`) and approximate bytes of the loaded paths (`--repo-cache-mb`).
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

### 2. MST: interior nodes persisted, leaves derived, paths loaded on demand
The MST is fully determined by the set of `(key, record CID)` pairs, so
records are the truth and the tree is mostly derived. Details, options
considered and measurements: "Partial MSTs".
- **Records** are persisted (`R/{did}\0{collection}/{rkey}` → cid + bytes),
  and so are the tree's **interior nodes** (height >= 1, ~1/4 of the
  nodes): `M/{did}\0{cid}` → node block, +28 B/record. They are put and
  deleted in the commit's own state batch, so `M/{did}` holds exactly the
  interior nodes of the tree at `h/{did}`'s data root. The puts are derived
  from the #commit CAR at replay (no log bytes); the deletes (~7 × 33 B)
  are stored in the segment. **Leaves** are never stored: a leaf is rebuilt
  from the `R/` range between its parent's separator keys.
- A repo's worker holds only the **paths** recent operations visited
  (`mst_lazy::LazyTree`: unvisited subtrees are `Child { node: None, cid }`),
  ~10–20 KB per written repo whatever its size. A write at key K needs K's
  search path and its two neighbours' (the spines a delete merges; sync 1.1
  proofs carry them); everything else stays unloaded. Mutations, CIDs and
  proofs are `mst::Tree`'s own code on the partial tree, so commits are
  byte-identical to a fully loaded tree's.
- **Cold open:** read the repo's `M/` range with one scan (up to
  `--lazy-mst-prefetch-kb`, 1 MiB: repos up to ~35k records), then the root
  by `head.data` and the first request's paths: ~1 object-store round trip,
  or 7–11 dependent node reads for larger repos, at any size (no O(n)
  rebuild). Before a cached repo's requests run, a no-I/O pass checks their
  paths are loaded; missing ones are loaded on the blocking pool, so the
  worker thread never waits on the store.
- **Verification.** Every loaded node is hash-checked against the link its
  parent holds (a rebuilt leaf too), and the root is `head.data`. A root or
  node that is missing or wrong means `M/` is: the open rebuilds the whole
  tree from `R/`, checks the root against `head.data`, and backfills `M/`
  through the log (`vlpds_lazy_mst_fallbacks_total{reason}`). importRepo,
  genesis records and account deletion write or clear the whole node set.
- **Readers** never touch the worker's tree. A repo's `DurableView` (the
  partial tree at its latest durable commit) is paired with a SlateDB
  snapshot taken under the apply lock, so `M/` and `R/` there are that
  version: getRecord proofs walk the snapshot; getRepo streams the tree
  from it (`M/` read ahead, leaves rebuilt from the same forward `R/` scan
  that yields the records, one path in memory); getBlocks answers loaded
  nodes, `M/` point reads, record CIDs (the `c/` index) and, last, leaves
  through a per-repo `NodeIndex` (leaf CID → its first key + height, built
  by one streamed walk the first time a request asks for a leaf, then
  advanced by the worker with each commit's written nodes; it covers a rev
  range, so a miss inside it is final). Loaded nodes are kept process-wide
  by CID (`--lazy-mst-node-cache-mb`): content-addressed, so valid in any
  version that links them.
- **Path cache.** A cached repo is charged ~2 KB plus the heap of its
  loaded nodes (`vlpds_repo_cache_bytes`); `--repo-cache-mb` bounds that
  per node. Over budget, the least recently used idle repos (nothing in
  flight, so every loaded node is in `M/`/`R/`) drop back to their root,
  then the least recently used repos are evicted. A repo charged over
  1 MiB (an import, a rebuild, a repo written without pause) drops all but
  the nodes its in-flight commits wrote right away.
- **Recently written repos are preloaded.** Each shard keeps the
  repos it committed to most recently (`--preload-recent`, 2,048 per
  shard; `partition::RecentRepos`, touched once per commit batch) and
  writes the list, newest first, as `meta/recent` with its checkpoints and
  at close, only when its members changed. The shard's next owner (a
  restart, takeover or handback) reads it right after the open, seeds its
  own set with it, and opens those repos in the background (root and `M/`
  prefetch), 32 at a time per node, interleaved across shards; the reads
  of all newly opened shards' sets run at once, so they don't queue
  behind the request-driven loads they are meant to spare. Bulk creation
  doesn't touch the set. Metric: `vlpds_repo_preloads_total{result}`.

Persisted state per commit: the records, the repo head and the commit's
interior nodes (~3.3–3.5 KB into SlateDB per commit, ~7 node puts and ~7
deletes), all in one state batch.

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
  - `M/{did}\0{cid digest}` → MST node block (lazy MSTs): exactly the
    interior nodes of the tree at `h/{did}`'s data root, put and deleted in
    the commit's batch (puts derived from the #commit CAR at replay).
  - `bl/{did}\0{code}{subject}` → the rkeys of the repo's likes / reposts /
    follows / blocks of that subject: the backlink index createRecord
    prunes duplicates with (see "Backlinks").
  - `a/{did}`, `n/{handle}` → account. The account row carries the repo
    signing key only wrapped under the KEK (`Account::wrapped_signing_key`,
    bound to the DID) next to its public key (`signing_pubkey`, which DID
    documents and service-auth checks read without unwrapping); account
    rows in log segments carry the same wrapped form. See "Secrets at rest".
  - `K/{did}` → empty: a signing-key rotation is pending
    (`Account::pending_signing_key`; see "Signing-key rotation").
  - `p/{routing}\0{name}` → private per-account state: sessions, app
    password hashes, email-token digests, TOTP state (secret wrapped),
    reserved signing keys (`p/_reserved:{did:key}\0k`, wrapped), OAuth rows.
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
  finish within it. vlpds keeps no checkpoints of its own (its
  "checkpoints" are applied markers + memtable flushes); the only others
  are a split/merge clone's pins in its parents and the short-lived
  checkpoint it reads a parent at ("Retired state GC"). Separately, GC
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
- Joining (`Cluster::try_join`): a log followed late starts at the
  merger's position *P* at that moment, and never delivers events at or
  below *P* (the merged stream has emitted past them; live order can't
  take them). So a new node takes no shard, so acks nothing, until every
  live peer confirms it follows its log, with its follower's floor: a
  `/internal/v1/cluster/hello` answer, or the joiner's log in the peer
  lease's `follows` (log id → floor, published at each renewal). Then its
  own seqs must pass every floor (a joiner's clock running behind a peer's
  merger), and for a peer it ignores as dead without a confirmation, that
  peer's published `wm_cap` (its own watermark's cap as of that renewal, so
  its merger can't settle past it). Then it publishes `joined`; peers hand
  shards only to joined nodes. No timer ends the wait: a live peer that
  doesn't confirm keeps the joiner out (it forwards writes meanwhile)
  until the peer confirms or is presumed dead. A node whose lease appears
  after the joiner's lists it at its own startup and follows it from its
  start floor (the cursor backfill serves everything below). A node
  leaving gracefully stops its merger for good before it deletes its lease
  (`Firehose::freeze`): its steps have stopped, so it would never follow a
  node joining after that, and it keeps serving for 500 ms more.

  This replaced a time-based grace (2 renew intervals, ended early by
  hellos), which lost events: `tests/all/join_follow.rs` holds one peer's
  steps and has it ignore hellos while a node joins under write load; the
  grace expired, the joiner took a handback and acked writes, and once the
  peer recovered it followed the joiner's log at its current position. Its
  live subscribers and cursor replays missed 2.1–3.5k of 17–32k commits
  (6 runs of 6), though all of them were in S3 and in the other nodes'
  streams. Clock offset alone could do the same with every hello answered:
  a joiner whose clock ran behind the cluster's slowest by more than its
  hello → first write time assigned seqs at or below a peer's floor.
  (Looping the test also found a lost update: a renewal in flight
  replaced the node's cached lease whole when it landed, dropping a
  `joined` set meanwhile, so peers never handed the joiner anything; it
  could drop a `draining` the same way. A renewal now updates only the
  fields it wrote.)
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
  commit that adds or removes the reference. The repo worker keeps a repo's
  refs by path to drop the old ones on an update or delete, loaded with
  one scan of `b/{did}` on the first such write (or account delete /
  import), not on open: a cold open for a create skips it, and the refs of
  records created meanwhile (maybe not applied yet) stay over what the scan
  reads. Dropped again with the repo's paths when it is idle. A cold first
  write to the real-repo fixture (`~/repo.car`, 812 blob refs;
  `mst_lazy::bench_cold_open_blobs`), every GET +20 ms: median ~150 ms
  instead of ~270 ms.
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
| public | PLC, requestCrawl, Cloud KMS (5 s per call) | h2 by ALPN on https, HTTP/1.1 on http with 1,024 idle per host; idle close 60 s; h2 PING 20 s / 10 s; TCP keepalive; connect 5 s, read 30 s |
| proxy | configured AppView / report service | `http://`: hyper HTTP/1.1 connections, one pool per host with a slot per IO thread: a connection goes back to the slot of the thread that finished its body, a request takes from its own slot, else from another slot, else connects; at most 1,024 connections per host (idle + busy; past that a request waits for one, `vlpds_http_client_pool_waits_total`); idle close 60 s, retry once if a reused connection was closed before the request went out; `https://`: public's settings as one client per IO thread. No read timeout: the proxy arms a 10 s head deadline and a 30 s body-idle timer only while the upstream makes it wait. Responses stream through unbuffered; compressed ones as the upstream encoded them (Content-Encoding/-Length kept, never decoded or re-compressed; the client's Accept-Encoding is forwarded, for the read-after-write methods only its decodable codings: §8); a client that goes away mid-body closes the upstream connection. CORS preflights are answered locally (no auth, no upstream) |
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
read (tokio has one timer-wheel lock) were ~20% of the proxy's CPU. The h1
pool keeps that (a request normally locks only its own thread's slot) but
lets a thread with an empty slot take from the others before connecting:
purely per-thread pools (+ a shared overflow) drifted to 1.45-3x the
concurrency in connections as tasks hopped threads (laptop A/B, 6 IO
threads: 369-398 connections at 256 in flight, 185-202 at 64; now exactly
256 and 64-65), at the same ~39-40 µs CPU per proxied request.

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

### 8. Read-after-write on proxied reads
(`src/xrpc/proxy/read_after_write.rs`, `src/recent_writes.rs`; reference
`packages/pds/src/read-after-write`, `api/app/bsky/{actor,feed}`.) The
AppView indexes a write seconds after it is made, so a user who just posted
or edited their profile would not see it. Like the reference, the proxy
merges the requester's own records written after the AppView's indexed rev
(its `atproto-repo-rev` response header) into exactly the methods the
reference munges, when an AppView is configured:

| Method | Merge (reference parity) |
|---|---|
| actor.getProfile | the local profile record over the view when it is the requester's (`displayName`, `description`, `avatar`, `banner`; fields the record lacks are removed) |
| actor.getProfiles | the same, on the requester's entry |
| feed.getActorLikes | the profile over the requester's post authors (no likes are inserted) |
| feed.getAuthorFeed | only the requester's own feed (first item theirs, or their repost): profile over authors, then new posts inserted by `indexedAt` (newer than the page's last item) |
| feed.getTimeline | new posts inserted by `indexedAt`; cursors untouched |
| feed.getPostThread | new replies placed under their parent anywhere in the tree (first among its replies); an upstream `NotFound` for the requester's own unindexed post (DID or handle URI) is answered with a thread built locally, its parents fetched from the AppView (`depth=0`, the request's `parentHeight`) |

Views are built as the reference's `LocalViewer`: PostViews with zero
counts, the author from the account handle and current profile record,
embeds as `images#view` / `external#view` / `record#view` (post, feed
generator and list embeds are looked up on the AppView as the requester:
getPosts, getFeedGenerator, getList) / `recordWithMedia#view`. Image URLs
use `--bsky-app-view-cdn-url-pattern` (reference
PDS_BSKY_APP_VIEW_CDN_URL_PATTERN, `util.format` with preset, DID, CID),
else this PDS's `com.atproto.sync.getBlob` URL. A munged response is
`application/json; charset=utf-8` with `Atproto-Upstream-Lag` (ms since the
oldest merged post/profile write) and, like the reference's, without the
upstream's other headers; the server's compression layer encodes it per the
client's Accept-Encoding. Which records count is the reference's
`getRecordsSinceRev`: the oldest 10 records (any collection) with a rev
above the AppView's, and none at all when no record is at or below it (an
AppView rev older than every record: a migrated or brand-new repo). Any
count above zero re-serializes the response, as in the reference.

**Finding the records cheaply.** The reference runs an indexed SQL query
per request; vlpds has no rev index (each record value carries its rev, so
the answer is a scan of the repo's records). The owner node keeps a
per-repo *recent-writes log* instead (`recent_writes.rs`, 64 mutex shards,
entries capped by the cache budget as `recent_writes`): the head rev, a
`base` rev, and every current record above `base` (path, rev, CID, and the
CBOR of posts and the profile), at most 32 records / 64 KiB, older commits
dropped by raising `base`. A commit's durable ack extends its repo's entry
before the writer is answered (an entry that missed a commit restarts at
that commit's `since`); imports, deletes and creations drop it; entries are
valid only in the partition epoch they were made in. A request then costs:

- AppView rev >= head (nearly every request): one shard lookup, nothing
  read, the response streams through untouched (compressed or not, the same
  zero-copy path as any proxied call);
- base <= rev < head: the records come from the entry, no store read;
- no entry, or rev < base: one point read of `h/{did}` (rev >= head: the
  entry is created empty) or, when the AppView lags behind, one scan of the
  repo's records for revs above it, which fills the entry so later requests
  hit it. Loads that raced a commit are not cached (per-shard generations).

Only a response with records to merge is buffered (10 MiB bound on the
wire and decoded), decoded (gzip, deflate, br, zstd), parsed and
re-serialized; for these methods the client's Accept-Encoding is narrowed
to codings vlpds can decode (the reference's gzip/deflate/br plus zstd, so
the usual `gzip, deflate, br` passes as is; others such as `compress` are
dropped, `*` = whichever of gzip/deflate/br aren't named; the reference
negotiates its own list for the same reason). As in the reference, a
malformed header is 400 and one ruling out identity, gzip, deflate and br
is 406. Unparseable or unexpected upstream JSON is
returned as received. Metric: `vlpds_proxy_read_after_write_total{result}`
(log_nothing, log_records, store_read; munged, unchanged, failed).
Measured cost on the no-merge path (laptop, shared and loaded ~30-40, 6
IO threads, release builds, stub AppView sending `atproto-repo-rev`, 20k
active repos, getTimeline 2 KiB bodies, 64 in flight, 8 interleaved 10 s
rounds per arm): median 43.2 µs CPU per proxied request before (1e48efd)
vs 43.6 µs after (range 38.6-47.0 vs 41.1-44.4): no difference above the
noise; ~38-40k req/s both. The first round after start read each repo's
head once (20,006 store reads), every later request was a log hit.

Also from the reference's `api/app/bsky/feed`: **getFeed** is proxied with
a service-auth token for the feed generator (aud = the `did` of the
generator record, fetched from the AppView's `com.atproto.repo.getRecord`
and cached a minute; lxm = getFeedSkeleton, which the caller's scope must
also allow), so the AppView can call the generator as the user.

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
XRPC: `server.createAccount` (originally locally minted did:plc-shaped DIDs; now registered with PLC, see "PLC identity"), `server.createSession`, `repo.{createRecord,putRecord,deleteRecord,applyWrites,getRecord,listRecords,describeRepo,uploadBlob}`,
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
  versioned layout (`assign/layout`; `--shards N` uniform ranges, default
  64, when a prefix is created), which splits and merges change online (see "Online
  shard split/merge"). A shard is the unit of ownership and state: one
  SlateDB at `state/{id}/` and an assignment object `assign/{id}`, where
  `{id}` is its u32 shard id as 10 zero-padded decimal digits
  (`state/0000000042/`; `slots::ShardId::key`), so keys LIST in id order and
  read like the plain numbers in logs and the admin API.
- **One log per node incarnation.** A node group-commits every shard's entries,
  tagged `(shard, epoch)` (u32 shard id, u64 epoch; segment format
  `VLSEG06`, `src/segment.rs`), into `log/{log_id}/{ordinal}.seg`. Up to K
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
  extras straight to the peers short of theirs (only peers whose lease says
  `joined`, see §5 "Joining"): after the close it CASes each assignment to name the joiner
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

*Fences go after `--fence-retention` (7 days).* A dead log is pruned down
to its fence: the segments below it, the K − 1 garbage segments past it,
and its report go. The fence is what makes a zombie of that incarnation
fail-stop whatever its clock says (its PUT at the fence ordinal collides),
so it stays for `--fence-retention` after it was written. Then it is
deleted, and the log is gone from `log/` (without this, +1 key and ~80 B
of `log/` LIST per incarnation, forever: an extra LIST page per ~1,000
restarts), if the fence is the only object left, `needed_by` is still None
on freshly read assignments, and no assignment names the log as its
owner's: a successor that has yet to take such an orphan fences the log
at `first_free`, which must find this fence, not an empty log (it would
fence at 0 and close the orphan's span there, cutting its entries).
`first_free` on a fence-only log returns the fence, `last_seq_before`
treats a pruned predecessor as "long ago", and replay of a span in a log
with no objects left reads nothing (by `needed_by`, every such span is
closed and applied; an open span is never in such a log).

*Why the fence may go after a grace.* After it, a zombie of that
incarnation could PUT segments there and ack them, and no one would read
them. So the fence must outlive every zombie of its incarnation ("Liveness"
below). A node stops acking once its monotonic validity ends (TTL − skew
after its last renewal's send), and its watchdog fail-stops it 2 × skew
after that; SIGSTOP and cgroup freezes count on `CLOCK_MONOTONIC`, so a
stopped process wakes with its validity expired and acks nothing. The one
zombie only the fence stops is a node whose monotonic clock itself stopped
(a VM or host suspend): it wakes believing its lease valid. The fence
retention is therefore a bound on such a suspend, listed with the other
clock assumptions ("Why safety needs no clocks"); 7 days is far beyond any
live migration or maintenance pause, and `off` keeps fences forever.

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
never reused identifiers (`state/{id}/`, `assign/{id}` with `{id}` as 10
digits, the `shard` tag of log entries), no longer positions in a uniform
split: a split allocates two new ids, a merge one. The first node of a
prefix creates version 1 as `--shards` uniform ranges with ids 0..n (the
uniform layout of before).

*Shard ids* are u32 (`slots::ShardId`; JSON and logs show the number).
`next_id` is the allocator: every id below it was handed out, none at or
above it was. `Layout::alloc` takes ids from it when an op is planned and
the plan's CAS of the layout advances it past them, so an allocation
happens exactly once (a planner that loses the CAS re-plans from the newer
layout and gets fresh ids) and survives an abort or a crash-resume (the
resuming driver reads the op, it never allocates again). Every layout write
derives from the current object, so `next_id` only grows; a node refuses
to install a layout whose `next_id` went back. Never reusing ids is what
makes stale state safe: an aborted op's half-made clone, a retired
parent's directory or a crashed driver's late write can only ever name an
id no live shard has. 16-bit ids (VLSEG05 and before) allowed ~65k
lifetime split/merge ops, too few for an automatic policy; 32 bits allow
~4.3 B (`alloc` errors rather than wrapping at `u32::MAX`). The slot space
stays 16-bit, so at most 65,536 shards exist at once (`--shards` is capped
there). `version` increases only when routing changes (a flip below).
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

*Patched SlateDB (fork).* A projection keeps each SST view's
id, so right after a split both children hold the parent's L0 SSTs under
the parent's view ids (each with its half as the visible range). Merging
them back before either compacted those L0s gave the union's L0 one view
id twice, and SlateDB 0.17's compactor keys L0 views by id: the merged
shard's first compaction of such a view rewrote one half and dropped both
from the manifest, so the other half's keys (acked writes, account and
handle keys) were gone from the shard and from every later clone of it.
This was `split_and_merge_under_write_load`'s rare "acked record lost"
(the merged shard compacted only when its L0 ran deep under load). vlpds
builds slatedb (and slatedb-common) from the fork
`github.com/jazware/slatedb`, branch `vlpds-0.17-union-l0-view-ids`, rev
`f2461431` (0.17.0 = upstream `c1e36fc` plus one change, via
`[patch.crates-io]`), whose `Manifest::cloned_from_union` gives repeated
L0 view ids fresh ids (same timestamp; the union has no L0 watermark that
could name the old ones). Pending an upstream report; drop the patch once
a release carries a fix. `partition.rs`
`merging_a_splits_halves_keeps_their_shared_l0s` pins it.

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
- *Retired parents:* their assignments (frozen) and state directories stay
  while a child reads their SSTs; then they are deleted ("Retired state
  GC" below).

**Policy hook** (off by default): `--reshard-split-mb` /
`--reshard-split-writes` let the driver-elect (owner of slot 0) plan a split
of a shard whose SST bytes or entry rate exceed them, one op at a time.

#### Retired state GC (`src/reshard_gc.rs`)

Without it every op left its parents behind for good. The benchbox soak of
3cdbfac (bench/results/soak-2026-10-02-benchbox, 100 ops in 7 h) measured
+64 MB and 1.5 state dirs per op, unbounded (6.44 GB retired vs 4.30 GB
live at the end), 32–35 of 64 live shards still reading a parent's SSTs,
state GET+HEAD per client read 3.8 → 6.4–9.6, graceful exit 0.9 → 1.75 s
and post-restart requests per commit up ~2x across the reshard hours, and
+1.5 `assign/` records per op. Three parts:

1. **Forced detach** (every node, for the shards it holds). A clone reads
   its parents' SSTs in place until its compaction rewrites them, and
   size-tiered compaction may never rewrite a quiet child's bottom run, so
   a parent could stay pinned forever. A shard still listing inherited
   SSTs (`external_dbs` entries with SST ids) `--forced-detach-after`
   (5 min) after it opened on this node gets one compaction submitted
   (`Admin::submit_compaction`): the suffix of its tree from the newest
   source holding an inherited SST (L0 view or sorted run) down to the
   oldest sorted run, merged into that run. A suffix is always a valid
   compaction (it holds every older L0 and every run, so recency order is
   kept), and its output is the bottom run. The spec comes from the stored
   manifest (what the coordinator validates against; the writer's view
   lags its results). At most one is in flight per node, none while L0 is
   deep; one that fails validation (a concurrent compaction took a source)
   is resubmitted next pass, as is one whose node died (the new owner
   starts over). A child that never received writes is all inherited and
   is rewritten whole. Once a shard reads no inherited SST in its manifest
   *and* in every manifest a live checkpoint of it names (the compactor's
   read guards of the pre-compaction manifests expire after
   `--slatedb-checkpoint-lifetime`), SlateDB's detach task in that shard's
   DB (every `--slatedb-detach-interval`, 10 min) deletes the *final
   checkpoint* the clone pinned in each parent, then drops the parent from
   its manifest.
2. **Dir GC** (the owner of slot 0's shard, as for dead-log pruning; every
   delete re-checks this node's lease). Every 60 s it GETs the layout and
   does nothing while an op is pending. A state dir whose id is below
   `next_id` and not in the layout is *retired*: a flipped op's parent, or
   an aborted op's half-made clone. Up to 32 are checked per pass (round
   robin, so held dirs don't starve the rest) and up to 8 deleted, each
   only if (i) its assignment names no owner, (ii) its newest manifest
   holds no live checkpoint (no expiry, or expiring in the future), (iii)
   that manifest is older than `--reshard-gc-grace` (1 h; never less than
   3 manifest polls), and (iv) no manifest under `state/` lists SSTs of it.
   The delete is SlateDB's `Admin::delete_db`: it strips the checkpoints
   the dir itself pinned in its own ancestors (so a grandparent is
   released with it), writes a `.deleting` marker, deletes everything, the
   marker last; a pass that dies half-way finishes on the next (a dir with
   a marker and no manifest is deleted). Then its `assign/` record goes.
   Records out of the layout whose dir is gone (a pass that stopped between
   the two deletes, an op aborted before its clone) go on the next pass.
3. **Clone source checkpoints.** `clone_db` reads each source at a
   checkpoint named `vlpds-clone-{child}` (1 h lifetime), reused by a
   resumed clone, and deletes it once the clone is initialized; a clone
   found initialized returns at once. SlateDB's default is an unnamed 5 min
   checkpoint, which nothing could drop for a source the clone's manifest
   doesn't name: a source with no SSTs of its own (a split child that took
   no writes) has no entry there. SlateDB's own retry check wants every
   source named, so a retried merge of such a child also failed.

*Why the GC never deletes state something needs.*
- *Clones and readers.* Every DB that may read a dir's SSTs holds a
  checkpoint in that dir's manifest. A clone holds its final checkpoint
  (no expiry) from its creation until SlateDB's detach, which waits until
  neither its current manifest nor any manifest a live checkpoint of it
  names lists those SSTs, so a scan or snapshot of the clone that started
  on an older manifest keeps the compactor's checkpoint-lifetime guarantee
  (§4), as for its own replaced SSTs. Ancestry is transitive: a clone of a
  clone carries every ancestor it still reads with its own final
  checkpoint there (`Manifest::cloned`, `cloned_from_union`), so a
  grandparent is held by its grandchildren directly, not through the
  retired middle generation. A `DbReader`, or a backup's named checkpoint
  ("Backups and restore"), holds its own. So (ii) is the complete test;
  (iv) checks SlateDB's invariant independently (a dir a manifest lists
  without a checkpoint is kept, counted as `referenced` and alerted on).
- *Stale in-memory views.* A shard's owner reads through its in-memory
  manifest, refreshed every manifest poll (10 s), so after a detach it may
  still list the parent for one poll: the grace is at least 3 polls. (The
  GC history test found this with a 1 s checkpoint lifetime and the 10 s
  poll: reads of a deleted parent's SST, answered 500.)
- *Pending and resuming ops.* Nothing is deleted while the layout carries
  an op: its children's ids are below `next_id` (the plan allocated them)
  and out of the layout until the flip, and the driver, or its successor
  after a crash, may still clone from the parents and write the children's
  assignments. After an abort, the half-made clones are out of the layout
  for good (ids are never reused); a driver presumed dead that wakes late
  finds its clone initialized (no-op) or, once it was deleted, makes a new
  half-clone that the next passes delete again (its pin in the live parent
  goes with it).
- *Replay.* A frozen parent never replays (its DB holds every span of its
  history: "Why no acked write is lost"), and a child's history starts
  empty, so nothing ever replays into a retired dir. Retention's
  `needed_by` already skips frozen assignments: deleting a frozen
  parent's record changes no retention decision.
- *Stale routing.* A node whose layout still names a retired parent can't
  open it: its assignment is frozen while it exists, and once it is gone
  `acquire` creates a missing assignment only for a shard in a freshly
  read layout (a step stalled on an old layout must never recreate a
  deleted shard over an empty directory).
- *Leadership.* Two nodes that both believe they lead run the same
  idempotent deletes on the same evidence; a node whose lease isn't valid
  deletes nothing.

*Opt-in full compactions* (`--full-compaction-every`, off): every held
shard gets one compaction of every source into its bottom run (which drops
tombstones) per interval, one per node at a time. The soak found no
tombstone cost (live bytes per record 207 → 123 B, delete-heavy
listRecords p99 flat at 2.7–3.8 ms), so it stays off.

Metrics: `vlpds_reshard_gc_retired_dirs{state}`,
`vlpds_reshard_gc_deleted_total{kind}`, `vlpds_reshard_gc_passes_total`,
`vlpds_reshard_gc_orphan_assign_records`, `vlpds_shards_with_inherited_ssts`,
`vlpds_forced_compactions_total{kind,result}`; alerts and runbook in `ops/`.
Retention metrics were already labelled by kind (own, dead, fence), not by
log id: the soak's series count was flat across 160 restarts.

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
  to every live peer not yet confirmed (retried each step), which reads its
  lease, starts following its log (`learn_peer`) and answers its
  follower's floor. Once every peer confirmed (here or through its lease)
  and our seqs passed the floors, the node has joined (§5 "Joining"): a
  node restarted with the same id reclaims the shards still assigned to
  its previous incarnation, which `join` already fenced, right away; and it
  publishes `joined` with an immediate renewal, so peers hand back at their
  next step. (A nudge to every peer instead made them all release at once,
  and the joiner's adopts of their batches queued behind each other.)
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
  writes. Firehose completeness across a join (§5 "Joining") rests on the
  same bound for one case only: a joiner skips a peer it presumes dead,
  using the `wm_cap` it last read from that peer's lease. A dead or lapsed
  peer never renews again, so that cap is final; a peer wrongly presumed
  dead that keeps renewing (beyond the bound) raises it and could, until a
  fence or reassignment makes it fail-stop, emit a stream to its own
  subscribers that skips the joiner's first events. Every other node's
  stream, and S3, keep them.
- **Monotonic clocks count paused time** (`CLOCK_MONOTONIC` counts SIGSTOP and
  cgroup freezes). A VM or host suspend that stops the monotonic clock makes
  a node believe its lease is still valid on wake. It then serves stale reads
  until its next PUT hits the fence, but acks nothing.
- **Such a suspend is shorter than `--fence-retention`** (7 days). A dead
  log's fence is deleted after it ("Log retention"); a node suspended
  longer, waking with its lease believed valid, would find no fence and
  could ack writes into a log no one replays. `--fence-retention off`
  removes the assumption, at one `log/` key per incarnation forever.
- **Wall-clock offset affects only seq ordering and merge latency.**
  - Seqs are wall-clock based. When a shard moves, the new owner's seqs must
    exceed the old owner's for its repos to keep their firehose order. The
    assignment carries `seq_floor`: the releaser's watermark at release, or a
    dead log's last segment seq at fence time. A new owner whose clock is
    behind waits until its clock passes it (commit-wait, capped at 30 s)
    before serving.
  - The merged firehose emits at `min W`, so it lags by the largest offset
    between nodes.
  - A joiner whose clock is behind a peer's merger waits (uncapped: it
    stays out, forwarding writes, and retries each step) until its seqs
    pass every peer's follow floor before it joins (§5 "Joining"). The
    one floor it can't read is a node that left gracefully just before
    (lease deleted, merger frozen at *S*): a joiner whose clock runs behind
    *S* by more than the time from that node's exit to its own first ack
    could assign seqs below *S*, which that node's last subscribers (who
    resume elsewhere from cursors near *S*) would miss.
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

Measured load (ClickHouse, 2026-09-24..30; bench/results/cost-model-2026-10-02
"Inputs"): **334 commits/s on average, ~420/s in the peak hour, ~900/s in
minute bursts** (each record op counted as one commit). **56 M** repos are
hosted on Bluesky's PDSes, holding **23.9 B** records at 154 B/record of
zstd SST state (bench/results/storage-2026-10-02). Proxied AppView traffic
is an assumption: **20k req/s** fleet-wide. One user's session on a
production PDS averaged ~5 KB per proxied response (compressed), so that is
~0.8 Gbit/s each direction. Headroom is planned at **10×**; 100× writes are
priced in the cost model (8 nodes / 1,024 shards) and designed for in
"Planet scale".

| Dimension | Today | 10× | Basis |
|---|---|---|---|
| Repos | 56 M | ~560 M | PLC DIDs on `*.bsky.network` |
| Commits/s | 334 avg, ~420 peak hour, ~900 burst | 3.3k / 4.2k / 9k | `repo_records` ops/day |
| Proxied req/s | 20k (assumed) | 200k | |
| Proxy bandwidth, each direction | ~0.8 Gbit/s | ~8 Gbit/s | ~5 KB per response |
| One full firehose subscriber | ~12 Mbit/s | ~120 Mbit/s | ~4.5 KB frame per commit |
| Repo state (zstd SSTs) | 3.7 TB live (+25% replaced SSTs), +~4 GB/day | +~40 GB/day | 154 B/record + 323 B/repo |
| Log, 72 h retention | ~234 GB | ~2.3 TB | 5,370 B/commit, ~2.7 KB stored |

### CPU
| Unit | Cost | Today | 10× |
|---|---|---|---|
| Commit (whole node: HTTP, MST, signing, log, apply) | ~96 µs | 0.03 cores (0.09 at bursts) | 0.3 (0.9) |
| Proxied request | ~50 µs | ~1 core | ~10 |
| Login (Argon2) | ~20 ms | ~1.2 (5 M logins/day, assumed) | ~12 |
| **Busy cores, fleet-wide** | | **~3** | **~25** |

Logins and proxying set the CPU, not commits. Sizing rule: after losing
one node, the survivors stay under ~60% CPU, i.e.
(nodes − 1) × cores × 0.6 ≥ busy cores.

### Nodes
- **Today: 3 × (6–8 cores, 32 GB, ~1 TB NVMe, 3–10 Gbit/s)**, e.g. OVH
  Advance-1 (EPYC 4244P, 6 cores, $147/mo). Two nodes would suffice for HA:
  leases and assignments are CAS on object-store objects, with no quorum.
  The third is for the 60% rule and growth. **Add a fourth node at ~2.4×
  today's load** (~7 busy cores = 2 survivors × 6 cores × 60%).
- **At 10× (~25 busy cores): 3 × 24 cores / 128 GB, or ~8 Advance-1**
  (7 survivors × 6 cores × 60% ≈ 25).
- **Memory.** 32 GB works because of partial MSTs ("Partial MSTs"): full
  trees at ~240 B/record wouldn't fit (one hour of real writers is ~850 GB
  of trees). A day's writers' paths are ~5 GB per node today and ~50–75 GB
  at 10× on 3 nodes (hence 128 GB); `--repo-cache-mb` (4 GiB by default)
  bounds them, and an evicted path costs a few `M/` reads to load again
  (the block cache adds 4 GB + a quarter for metadata by default). The persisted interior nodes (`M/`,
  +28 B/record) are ~220 GB per node's share at 3 nodes; they live in the
  object store, and the NVMe disk cache holds the hot part.
- **Network.** Proxying is ~0.27 Gbit/s per node each direction today, and
  ~2.7 Gbit/s at 10× on 3 nodes (~1 Gbit/s on 8). Each full firehose
  subscriber adds ~12 Mbit/s (~120 at 10×).
- **Shards.** 65,536 hash slots in **64 shards** by default (~875k repos
  each, ~21 per node at 3 nodes). Shard count drives the object-store bill
  (polling, GC and checkpoint flushes are per shard) and busier shards flush
  and compact more efficiently, so start at 64 and split hot or large shards
  online. 64 instead of 256 saves ~$800/mo on S3 at today's load.

### Object store
- **~$1.7k/mo on S3 (~$1.5k on R2, ~$1.7k on GCS)** at 3 nodes / 64
  shards (256 shards: ~$2.5k / $2.2k / $2.5k) with the latency-neutral defaults (10 s manifest poll, 30 s
  compactor polls, idle checkpoints skipped), in-region
  (bench/results/cost-model-2026-10-02, "Defaults changed"). Requests
  dominate: segment PUTs (~27/s per node at any load up to ~20k
  commits/s/node), checkpoint flushes plus compaction, and polling.
  Storage (~4.9 TB: state, replaced SSTs, 72 h of log) is ~$110/mo.
  The model was fitted at ~34 ms mean PUT latency; at measured in-region
  GCS latency (~57 ms mean for small objects) nodes send fewer, larger
  segments and the 64-shard bill is ~$1.3k (GCS).
  8 nodes / 1,024 shards would be ~$7.2k. Off-cloud nodes (OVH) with S3 or
  GCS also pay egress for every state GET past the disk cache, every log
  read by a peer, and relay backfill: not modeled. R2 charges no egress.
- **S3 Standard for everything** (log, state, blobs; no S3 Express): it
  survives an AZ loss at ~40–50 ms p50 / ~150 ms p99 commit ack (S3-like
  latency model). Each node's NVMe is SlateDB's SST disk cache.
- **Log retention** of 72 h for firehose backfill: ~234 GB today, ~2.3 TB
  at 10× ("Log retention", "Log compression"). A real single-record commit
  is ~5,370 B of segment, ~2.7 KB after zstd; the MST proof blocks in its
  CAR dominate. Record and head values aren't stored twice: they are
  rebuilt from the CAR at replay (`segment::derive_commit_muts`).
- Blobs (~350 TB, ~$7.8k/mo on S3) are priced separately in the cost model.

### Separate tiers
None. Full nodes serve proxying and the firehose; see "Read replicas and
fan-out nodes: not planned" for when that would change.

### Before production data
Fixed slots with a shard map, the per-node log, node leases and
assignments are done ("HA"). Still open:
- ~~Signing keys KMS-wrapped (§4; plaintext today).~~ Done: "Secrets at
  rest" (local KEK or Cloud KMS); provision the production KEK.
- ~~Partial MSTs wired in (required by 32 GB nodes).~~ Done, and the only
  mode ("Partial MSTs", "As built").
- Backups ("Backups and restore").

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

## Partial MSTs

The design record of §2's MST (built; the full-tree mode it replaced, and
compares against below, is removed).

**Problem.** The first design kept the whole tree of every cached repo in
memory and rebuilt it from `R/` on a cold load (records only stored). Real writers in one hour (~188k repos)
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

### As built (Oct 2026; the only mode)
The full-tree mode (`--lazy-mst=false`, whole trees rebuilt from `R/` on
every cold load, large repos pinned and preloaded from an `L/` index) was
kept for comparison while this was measured, then removed. Tests check the
node against an independent reference instead: a full `mst::Tree` built
in-test from the same acknowledged writes (`tests/all/mst_lazy.rs`). The
suite also runs with `VLPDS_LAZY_MST_UNLOAD_IDLE=1` (every idle repo's
paths dropped after each worker pass, so every operation walks from the
root through the store).

- **Stage 1: `M/` write-through.** `state::mst_node_key` =
  `0x01 ‖ slot ‖ M/{did}\0{cid digest}` (slot-prefixed, so splits and merges
  carry it with the shard's range). A commit's puts are its CAR's MST blocks
  of height >= 1 (`mst_lazy::persisted_blocks`, found from the data root
  through the links the CAR carries, so proof-only neighbours are re-put,
  idempotently); they are *derived* muts: replay rebuilds them from the
  #commit frame (`segment::derive_commit_muts_n`: an entry deriving more
  muts than the base set derives the node puts too). The deletes (the
  replaced nodes, from `LazyTree::write_diff_blocks`) are stored muts.
  Repo creation with genesis records, `importRepo` (`ReplaceRepo`) and
  account deletion write or clear the whole set. A repo whose `M/` is
  missing or wrong (a bug, a lost key range) is rebuilt from
  `R/` on open and backfilled through the log
  (`vlpds_lazy_mst_fallbacks_total{reason}`).
- **Stage 2: lazy worker.** `RepoState::mst` is a `LazyTree`. A cold open
  reads the repo's whole `M/` range with one scan (up to
  `--lazy-mst-prefetch-kb`, 1 MiB: repos up to ~35k records), the root, and
  the paths of the first request's keys, on the blocking pool. Before a
  repo's queued requests run, a no-I/O pass walks their keys, neighbours
  and collection probes; anything unloaded is loaded on the blocking pool
  (`Worker::start_fetch`, the requests wait in `loading`, the tree is
  swapped in on `Fetched`), so the worker thread never waits on the store
  (`vlpds_lazy_mst_fetches_total{result="inline"}` counts reads it still had
  to do: 0 in every test). The collection index uses `coll/` probes (does
  any key start with it, before and after the batch) instead of per-repo
  counts. A walk that finds a node or leaf not matching its link fails the
  repo, which reopens from durable state (rebuilding from `R/` if needed).
- **Stage 3: readers.** `DurableView` carries the (partial) tree; readers
  pair it with the SlateDB snapshot `App::repo_view` takes under the apply
  lock, so `M/` and `R/` there are exactly the view's version. getRecord
  proofs walk asynchronously (`mst_store::proof_blocks`), never touching the
  shared tree. getRepo streams from the snapshot with one forward `R/`
  scan (`xrpc::sync::export_repo`): the scan hands each record's key and
  CID, in batches, to the MST walk on a blocking thread
  (`mst_lazy::export_blocks` over `mst_store::FedSource`: the `M/` range
  read ahead, leaves encoded straight from their records and checked
  against their links, one path in memory; the root is checked against the
  commit, the `M/` nodes below it are not re-hashed: see "Measured"), and keeps the record blocks in
  a bounded buffer until the nodes are out (the CAR puts every node first:
  same bytes as before). The buffer is 1 MiB grants from a process-wide
  256 MiB, at most 64 MiB per export; records past it are read again by a
  second scan (`tests/all/mst_lazy.rs` `export_buffer_caps_give_same_bytes`).
  getBlocks: loaded nodes, leaves the repo's `NodeIndex` places (once built,
  without probing `M/` and the record index first), `M/` point reads
  (interior), record CIDs as before, then the rest via the `NodeIndex`
  (built once by a streamed walk, advanced by the worker per commit; a miss
  in an index covering the view is final), each read by a walk to its key.
  A walk that has to rebuild a leaf rebuilds its unloaded siblings from the
  same scan of the parent's range (`mst_store::load_leaves`: a small range
  scan costs mostly its setup). Loaded nodes are kept process-wide by CID
  (`mst_store::NODE_CACHE`, `--lazy-mst-node-cache-mb`, 256 MiB): nodes are
  content-addressed, so an entry is valid in any version that links it.
- **Stage 4: path cache.** A repo is charged `REPO_BASE + heap of its
  loaded nodes`; `--repo-cache-mb` (4 GiB per node by default, was 16 GiB
  of whole trees) bounds that, split per worker. Over budget, the
  least recently used idle repos (nothing in flight: every loaded node is
  then in `M/`/`R/`) drop back to their root, and their view is
  republished unloaded. A repo over 1 MiB (an import, a rebuild, a repo
  written without pause) drops everything but the nodes its in-flight
  commits wrote (`RepoState::inflight`; their state isn't applied yet, and
  every node above a changed one changed too), all of it once idle; so a
  repo that is never idle stays bounded (tested: ~1 MiB peak under 32
  concurrent writers over 12k records). Pinning, `L/` preloads and the
  per-repo record counts are gone with the full-tree mode; recent-repo
  preloads remain (now an `M/` prefetch).
- **Stage 5: not needed.** See the measurements: at 10× today's load (3.3k
  commits/s, 9k bursts) the extra state writes are ~11 MB/s cluster-wide
  (~30 MB/s in bursts) into memtables, far from a SlateDB limit, so the
  checkpoint-window write-back with an `m/{did}` marker stays a design.

### Measured, lazy vs full trees (Oct 2026, M4 Pro, dev-release, in-process)
`tests/all/mst_lazy.rs` `bench_*` and `worker::tests::bench_commit_cpu`
(commands in their doc comments; they measure the lazy side only now that
the full-tree mode is removed).

| | full trees | lazy |
|---|---|---|
| Node memory, 20k repos (Zipf, 1M at rank 1: 10.5M records), one write each | 2.56 GB of trees (charged), RSS +1.66 GB | 155 MB of paths + 96 MB node cache, RSS +0.96 GB |
| Those 20k cold writes, 64 at a time | 6.8 s, p50 9.7 ms, p99 126 ms | 5.5 s, p50 16.7 ms, p99 33.7 ms |
| Cold write (median of 3), every GET +20 ms, empty caches: 1k / 10k / 100k / 1M records | 108 / 97 / 191 / 1,147 ms | 108 / 110 / 114 / 460 ms |
| ... without the `M/` prefetch | | 149 / 259 / 367 / 468 ms |
| Commit CPU (worker thread, warm paths) | 16.0 µs | 19.8 µs (+3.7: neighbour walks, no-I/O pass, `coll/` probes, delete check) |
| State bytes / commit (into SlateDB) | 650 B | 3.3–3.5 KB (+`M/` puts) |
| Segment bytes / commit (stored) | 3.36–3.51 KB | 3.74–3.95 KB (+~12%: `M/` deletes) |
| sync.getRecord, 100k-record repo, 32 clients | 80.5k/s | 77.7k/s |
| getBlocks: interior node / record / leaf | 95.8k / 62.3k / 43.9k/s | 82.6k / 57.6k / 21.5k/s |
| getRepo, 100k records (22.5 MB), 4 clients | 47 /s | 16 /s (two `R/` scans; one since Oct 2: below) |
| Process CPU per getRepo export / leaf getBlocks (Oct 2, below) | 112 ms / 98 µs | 148 ms / 196 µs (269 ms / 253 µs before) |
| Instructions per getRepo export, system allocator / jemalloc (Oct 2, below) | 1,382 M / 908 M | 1,575 M / 1,057 M (1,631 M / 1,101 M before) |

- The RSS rows include the writes' own state, which in these runs lives
  in the in-memory object store and memtables (5x more state bytes per
  commit for lazy), and freed buffers the allocator keeps; the charged
  tree and path bytes are the MST memory proper (the full run's RSS grew
  ~160 B/record, under the 240 B/record charge measured with jemalloc).
- A cold write's fixed reads (head, account; blob refs only for an update
  or delete since Oct 2) set a ~100 ms
  floor in both modes; past it the full mode grows with the repo (a 1M
  repo is 44+ GETs of `R/` and ~1 s of rebuild) and lazy doesn't, up to
  the repos whose `M/` range outgrows the prefetch. The prefetch matters:
  without it a cold write is 7-11 dependent node reads. 512 KiB, 1 MiB and
  4 MiB caps measured the same up to 100k records; a 1M-record repo's 28 MB
  range isn't worth reading ahead (its path's point reads cost as much);
  1 MiB is the default. Production reads hit the NVMe disk cache first.
  (GET counts per write were too noisy here to quote: background
  compaction and polls share the state client.)
- getRepo was ~3x slower than walking a resident full tree: leaves were
  rebuilt (key hashes, nodes, encode, CID) from a forward `R/` scan on the
  walking thread (one `block_on` per record), and the records needed a
  second scan. Profiled (macOS `sample`), ~95% of an export's CPU was the
  SlateDB scans (merge iterators per row), the MST work ~5-15%. Now (Oct 2)
  one scan feeds both, on its own task, so it overlaps the walk; leaves are
  encoded from the records without key heights or nodes (their links check
  them); `M/` is still read ahead with one range scan, nodes hash-checked.
  Re-measured on a shared, loaded laptop (load 25-50 on 14 cores) against a
  build of the last full-tree commit, 4 interleaved rounds of
  `bench_readers` (which now also reports process CPU per operation, server
  and client): an export costs 148 ms of CPU (spread 137-163) against 112 ms
  (110-124) for the full tree and 269 ms (261-278) before; rates were too
  noisy to rank precisely (medians 16.5 / 12.2 / 7.2 exports/s, each
  spanning 4-24/s). The remaining gap is the walk itself (`M/` reads and
  hash checks, leaf encode and hash) and the scan feeding it.
- Export follow-up (Oct 2). `bench_readers` also reports instructions
  retired per operation (macOS `proc_pid_rusage`): unlike CPU time, the
  same on a performance or an efficiency core and under load (CPU per
  export spread 105-159 ms across runs of one binary at load 20-70;
  instructions ±1%). Profile (`sample`, 4 concurrent exports): ~75% of an
  export is the `R/` scan inside SlateDB's `DbIterator::next` (~11k
  instructions per row: a boxed future per iterator layer per row, so
  allocator traffic, `RowEntry` moves and the merge heap), ~8% the `M/`
  read-ahead scan (same per-row cost), ~10% the walk (leaf encode and
  SHA-256, interior decode; the hash was ~1/3 of it, `M/` nodes ~60% of
  that), ~6% hyper. SHA-256 is the hardware one: `sha2` 0.10 with `asm`
  takes the ARMv8 SHA2 instructions on aarch64 and SHA-NI on x86-64 (the
  `asm` feature only swaps the software fallback there), detected at run
  time. Changes: the export no longer re-hashes the persisted nodes below
  the root, nor their keys for heights (`mst_lazy::export_blocks`: the root
  is still checked against the commit's data link, every leaf and rebuilt
  subtree against its parent's link; the `M/` blocks are ones this PDS
  encoded and hashed before writing them under their CIDs, imports
  included (their blocks are hash-checked first), and SlateDB checks each
  SST block's CRC32 when it reads it, so bit rot fails the read; a wrong
  node would still mostly fail its children's link checks, else reach the
  CAR as a block not hashing to its CID; the worker's and readers' walks,
  which cache what they load, keep the check); record batches to the walk
  are keys back to back in one buffer (no `Arc` per record), leaves
  encoded entry by entry without a copy, record values decoded borrowed.
  5 interleaved rounds against the previous head and the last full-tree
  commit (the old bench patched to report the same numbers): instructions
  per 22.5 MB export 1,631 M -> 1,575 M (-3.4%; full tree 1,382 M) on the
  system allocator the test binary uses, 1,101 M -> 1,057 M (-4%; full
  tree 908 M) on jemalloc (`--features bench-jemalloc`); CPU medians 144 ->
  137 ms (full 120) and 118 -> 112 ms (full 92), within the run-to-run
  spread. The gap to a resident tree is now ~1.15x (was ~1.2x measured
  this way; the 148 vs 112 ms above were 4 rounds of CPU time), most of it
  the `M/` read-ahead's SlateDB rows and the leaf hashing that checks the
  records against the signed tree. The test binary's allocator alone costs
  an export ~50% more instructions than jemalloc (the server's), so
  benches of scan-heavy paths overstate them. What is left to win is
  mostly in SlateDB's iterator stack (the fork), not in this walk.
- Leaf getBlocks is an index lookup plus a walk to the leaf's key; cold
  leaves (the bench's random leaves are mostly cold) cost a small `R/`
  range scan each, now shared with their siblings: 196 µs of CPU (median of
  8 rounds) against 253 µs before and 98 µs from a resident tree.
  sync.getRecord, interior and record getBlocks are unchanged (CPU within
  noise of the full tree's).
- **Stage 5 decision.** At 10× today's load (3.3k commits/s average, ~9k
  bursts) the extra state writes are ~10 MB/s cluster-wide (~30 MB/s in
  bursts) into memtables, and the extra stored segment bytes ~1.3 MB/s:
  nothing near a limit, so the checkpoint-window write-back stays a design.
  At the 100× planet-scale target (~200k commits/s) it would be ~0.6 GB/s
  of extra memtable writes and should be built then.

## Backlinks (`src/backlinks.rs`)

Reference parity: the reference's createRecord deletes the repo's earlier
records of the same collection and subject in the new record's commit
(`getBacklinkConflicts` over its `backlink` table): likes and reposts by
`subject.uri` (a valid AT-URI), follows and blocks by `subject` (a valid
DID), records whose `$type` is their collection. Only createRecord, and
not with `validate: false`; applyWrites, putRecord and importRepo index
without pruning, so duplicates can exist and a later createRecord deletes
them all. The deletes are ordinary ops of the same #commit.

- **Key.** `bl/{did}\0{code}{subject}` (slot-major like every per-repo
  family, so reshards carry it) → the rkeys with that subject, sorted and
  `\0`-separated; code `l`/`r`/`f`/`b`. One key per (collection, subject),
  so the common no-conflict check is one point read, which the SSTs' bloom
  filters answer without a block read when the key is absent. A key per
  record (`...{subject}\0{rkey}`) would make every check a prefix scan
  that no filter skips (one block read per L0 SST); a hashed subject
  would save ~60 B per like but need the candidates' records read to rule
  out collisions.
- **Reads off the worker thread.** A request that creates, updates or
  deletes a linked record needs the index values its records link to, and
  for an update or delete the old record's link (its `R/` value). The
  worker reads them in the same fetch that loads missing MST paths (the
  blocking pool; a backlink-only fetch skips the tree clone), so a like is
  one fetch hop and one or two point reads. Durable state lags the
  worker by the commits in flight, so the repo keeps those commits'
  entries (`backlinks::Cache`, each tagged with its commit's applied flag)
  over what it reads, and drops every entry durable state holds again
  after each run: between runs it holds only in-flight entries. Concurrent
  creates of one subject thus see each other (one record left), whether
  they share a commit or not. A conflict's delete needs the blob refs
  (loaded once per repo load, as for any delete).
- **Log.** A record's put (`[rkey]`, its value whenever the subject has
  one record) is derived at replay from the #commit frame
  (`segment::derive_commit_muts`, after the record's `R/` put; the debug
  check compares it with the worker's). What differs from that is stored
  after it and wins in the batch: removals (an unlike, unfollow or update
  needs the old record, which the frame doesn't carry) and keys holding
  several rkeys. A create costs no segment bytes; a delete stores one key
  delete (~115 B for a like, ~80 B for a follow, with a did:plc). Imports, account deletes and creations with
  records write the whole index as stored muts (an import or delete first
  reads the repo's whole `bl/` range).
- **Checks.** `vlpds.admin.checkRepo` compares `bl/` with the records
  (`backlinkMissing`, `backlinkExtra`); rebuildRepo rewrites it.
  `tests/all/backlinks.rs`: the reference cases, the firehose commit with
  the deletes, applyWrites and import duplicates, concurrent writes,
  replay after kill -9 (byte-identical `bl/`), split and merge.
  Golden fixtures (level 1): `state/backlinks.json` (link, key, value)
  and `segment/like.seg` (a like's #commit, its derived `bl/` put).

**Cost (M4 Pro, dev-release, shared laptop).** `worker::tests::bench_commit_cpu`,
old and new binaries interleaved, 6 runs each (best of 7 rounds per run),
median, one createRecord per commit on a 5,000-record repo, the backlink
read done inline on the same thread (in production it runs on the blocking
pool), against an in-memory store whose read finds nothing:

| | before | after |
|---|---:|---:|
| post µs/commit (20 / 5000 records) | 37.6 / 37.1 | 38.4 / 38.6 (noise) |
| like µs/commit | 39.6 | 46.9 |
| follow µs/commit | 38.1 | 43.8 |
| like state B/commit | 4,756 | 4,868 (+112) |
| follow state B/commit | 3,562 | 3,638 (+76) |
| segment B/commit (like, follow) | 5,445 / 3,947 | unchanged |

The ~6–7 µs per like or follow is the point read (a SlateDB get through
`block_on`), two CBOR decodes of the record (needs, apply) and the cache
bookkeeping; posts and other collections pay one string compare per
write. The state bytes are uncompressed key + value with the bench's
21-character DID: ~123 B per like and ~85 B per follow with a did:plc
(zstd SST blocks shrink the repeated DID prefixes).

## Backups and restore (design, not implemented)

**Today there are none.** Durability is the object store's (S3 Standard:
multi-AZ, 11 nines against hardware loss). Nothing protects against a
*logical* loss: a delete, an overwrite, a bad write, or losing the bucket,
account or region. Everything durable sits under one prefix of one bucket:

| Prefix | What | Churn |
|---|---|---|
| `log/{log_id}/{ordinal}.seg`, fences | WAL + firehose: frames and state mutations | ~27 new objects/s per node; deleted after 72 h |
| `state/{id}/` | one SlateDB per shard: SSTs, manifests, compactions, `gc/` | L0 flush per shard per 10 s; compaction replaces SSTs; GC deletes them ~1 h after replacement |
| `nodes/`, `assign/` (+ `assign/layout`), `writers/`, `retain/` | leases, ownership + span history, layout, writer ids, retention reports | CAS-overwritten (a lease every 2 s) |
| `handle/`, `email/` | uniqueness claims (conditional PUTs) | per account change |
| `blob/`, `blob-gc/`, `blob-tmp/` | blobs (~350 TB at Bluesky scale) | ~1 M uploads/day; GC moves, then deletes |

### Threats
1. **Bucket deleted, or credentials misused** (leaked node or operator
   keys, a compromised account). Anything that can delete objects can
   delete everything.
2. **A bad build** writes corrupt state (wrong mutations applied), or a bad
   log (bad commits, already sent to relays), or deletes too much (a GC or
   retention bug removing live SSTs or segments still needed for replay).
3. **An operator deletes objects** by hand (wrong prefix, a cleanup script).
4. **Region loss**: S3 Standard survives an AZ, not a region.

### Options and how they interact with vlpds

**S3 bucket versioning + noncurrent-version expiration.** Every overwrite
or delete keeps the old version, so deletes become undoable. vlpds's own
semantics don't change: conditional writes (`If-None-Match`, `If-Match`)
apply to the current version, and a delete just adds a delete marker.
- *Extra storage* = bytes deleted or overwritten per day × the
  noncurrent window. Today: log segments ~78 GB/day (28.9 M commits ×
  2.7 KB), replaced SSTs ~58 GB/day (assumption: the cost model's ~2 KB
  of SST rewritten per commit; at bench scale, size-tiered rewrites of a
  shard's largest run (~15 GB at 256 shards) don't show up, and each one
  adds its size), manifests and compaction files ~20 GB/day (~80 KB/s per
  node measured in the cost-model runs). About **160 GB/day: ~1.1 TB
  (~$26/mo on S3) for 7 days, ~4.8 TB (~$110/mo) for 30**. The ~4× write
  amplification of a bulk import adds ~3× the imported state for the
  window. Lease renewals add ~130k tiny versions per day: negligible bytes.
- *Delete markers.* Retention deletes each log from its head, so ~2.3 M
  delete markers per node per day pile up just ahead of the live
  segments until their noncurrent versions expire. S3 LISTs slow down
  when they scan long runs of delete markers. Retention's paged LIST and
  SlateDB GC's LISTs should start from a known key (start-after), and the
  lifecycle needs `ExpiredObjectDeleteMarker`. Not measured.
- *Providers.* GCS object versioning is equivalent, and GCS soft delete
  (7 days) is a cheaper undelete: if this layer is wanted on GCS, keep it
  on, against §4's "disable soft delete" (~$22/mo for 7 days). R2 has no
  object versioning (assumption, verify); its bucket locks would make
  SlateDB GC and retention deletes fail. On R2 only the off-site copies
  below protect.

**Object Lock** (needs versioning). Locked versions can't be deleted
before their retention date, in compliance mode not even by the root user,
and a bucket holding locked versions can't be deleted. A 7-day default
retention on the primary bucket costs the same bytes as 7 days of
versioning and turns threats 1 and 3 into "recoverable within 7 days".
vlpds's deletes still work (they add markers), but with default retention
S3 requires a checksum header on every PUT: object_store's S3 client has
to be configured to send one (`with_checksum_algorithm`). Not tested with
conditional PUTs. Locks also make a mistakenly written secret impossible
to purge.

**Cross-region / cross-account replication (CRR).** Asynchronous, per
object, unordered: a replica can hold a manifest before the SSTs it names,
`assign/{s}` at epoch e + 1 before epoch e's last segments, or a fence
before the segments under it. So the replica is crash-consistent only at
a cut T before which every source version has replicated (replication
metrics / S3 RTC, plus the assumption that `Last-Modified` follows
causality across objects). Delete markers replicate only if enabled, and
version deletes never do. The cost is per replicated object version:
segments (~82/s), SSTs, manifests and compaction files (~70/s) and leases
come to ~150 PUTs/s, **~$1.9k/mo of requests** plus ~$100 of transfer,
almost doubling today's bill. A `state/`-only filter is still ~$0.9k.
Today's small segments (one PUT per round trip) make CRR a poor fit for
log and state. It fits blobs: large, immutable, ~1 M a day.

**SlateDB checkpoints and clones** (`slatedb::admin`). A *named*
checkpoint (`Admin::create_detached_checkpoint` with
`CheckpointOptions { name, lifetime }`, or `Db::create_checkpoint` on the
owner, which flushes first) pins one manifest's SSTs against GC until it
expires. It is O(manifest) and costs only the replaced SSTs it keeps
alive (~58 GB/day × lifetime). vlpds keeps none of its own: besides the
compactor's 1 h read guards (§4) there are only split/merge clones' pins
and their short-lived source checkpoints. Retired-state GC keeps any dir
holding a live checkpoint, so a named backup checkpoint of a shard that a
split or merge then retires keeps that dir (and the ancestors it reads)
until the checkpoint expires ("Retired state GC"). A clone
(`create_clone_builder_from_source`, as `partition::clone_db` uses for
splits and merges) accepts `CloneSourceSpec::checkpoint`, so a shard can be
restored as a new shard id from any live checkpoint in O(manifest). Two
limits: a checkpoint lives in the same bucket and only references SSTs, so
it protects against threat 2 but not against 1, 3 or 4. And a clone
references the source's SSTs in the same store ("external SSTs"), so it
can't move data to another bucket.

**Each SlateDB state is a consistent snapshot of its shard.** The applied
marker `meta/applied2 = (log, ordinal)` is written in the same `WriteBatch`
as that segment's mutations (`nodelog.rs`), so any manifest, and any
checkpoint of one, holds whole segments up to its marker. Replaying the
shard's spans from the marker (`assign/{s}` history) rolls it forward, as
crash recovery already does.

**Logical export** (`com.atproto.sync.getRepo` CAR per account, plus
`listBlobs`/`getBlob`). This is the format-independent last resort: it
survives a SlateDB or segment-format bug, and every atproto PDS can
import it. ~8 TB uncompressed for 23.9 B records (assumption: ~330 B of CAR
per record, record block + MST share), ~3 TB zstd, ~$3/mo per copy in
Glacier Deep Archive. Export throughput is unmeasured. CARs omit
everything that isn't the repo: signing keys, password hashes, email,
preferences, OAuth sessions, invites, takedown status. Those need an
encrypted dump of the account rows (`a/`, `p/`) alongside.

### A consistent point-in-time restore of the whole cluster
A cut is a firehose seq S: the restored cluster holds exactly the entries
with seq ≤ S of every log, which is what subscribers saw through S (the
merge order). It needs:
1. **Per shard, a state at a marker at or before S**: a named checkpoint,
   or a copy of one manifest and its SSTs (including external SSTs of a
   clone's parents).
2. **Every log entry from each shard's marker through S**: the logs
   (retained or archived) plus the `assign/` span histories that say
   which logs and ordinals belong to the shard. Entries carry seqs, so
   replay stops at S.
3. **The layout the snapshots belong to.** A reshard between snapshot and
   cut means replaying the parents and re-running the clone. Simpler:
   snapshot right after every flip, so a restore never spans one.
4. **Global objects rebuilt, not restored as of S.** `handle/` and
   `email/` are rebuilt from the restored `a/`/`n/` rows. `nodes/`,
   `retain/` and `writers/` start fresh (new log ids, the old logs fenced).
   `assign/` is rewritten with `seq_floor` above the highest seq ever
   emitted, not S, so seqs never go backwards for subscribers.
5. **Blobs: a superset is enough.** They are content-addressed. Undelete
   anything referenced, and blob GC reclaims the rest.
6. **Tell relays.** They saw commits after S that no longer exist. For
   each repo with entries after S in the old logs, emit a sync 1.1 `#sync`
   with the restored head, so relays resync instead of rejecting the next
   commit's `prevData`. New commits get later revs (TIDs) anyway. Writes
   after S are lost: that is the point when S is just before a bad build.
   If the bad build's *stored mutations* are wrong but its frames are
   right, replay with a fixed build can re-derive records and heads from
   the CARs (`derive_commit_muts`); other mutations can't be.

Restore into a **new prefix** (the old one stays untouched until it's
verified), cloning each shard from its checkpoint. Retiring the old prefix
then needs the same `external_dbs` check as retired reshard parents. Blobs
can't follow the prefix without a copy of ~350 TB, so the blob prefix has
to be configurable separately (code). Tooling needed: named checkpoints, a
`restore --cut S` that clones, replays to S and rebuilds the indexes, an
archiver, and the off-site copy job.

### Recommended plan
| Layer | Protects against | RPO | RTO | $/mo (S3) |
|---|---|---|---|---|
| 1. Versioning, 7-day noncurrent expiry, Object Lock governance 7 d; the app role has no `s3:DeleteObjectVersion`, bucket-policy or lifecycle permissions | operator deletes, app-credential misuse, GC/retention bugs | 0 | hours (remove delete markers, restore versions) | ~$26 |
| 2. Named SlateDB checkpoint per shard every 6 h, kept 8 d; `--log-retention 8d` | bad build: roll back to any S in the last 8 days, replaying good entries | 0 up to the cut | ~30–60 min (clone 256 shards in seconds, replay ≤ 6 h of log, rebuild handle/email indexes) | ~$20 |
| 3. Off-site: a separate AWS account in another region, Object Lock compliance 30 d, S3 Standard-IA. Daily incremental copy of each shard's checkpoint (SST names are unique and immutable, so only new SSTs are copied, manifest last) + `assign/`; the log archived per node every minute (one object of concatenated segments, written by the owner after they're durable) | bucket or account loss, leaked admin credentials, region loss | ~1–2 min (archive lag) | ~1–2 h: clone from the copy inside the backup bucket, replay ≤ 24 h of archive, rebuild indexes, move DNS | ~$180 |
| 4. Blobs: CRR to the backup account (Glacier IR) | same, for blobs | minutes | serving from the replica needs code | ~$2.4k |
| 5. Logical export: CARs + encrypted account dump, monthly, Deep Archive | format bugs, leaving vlpds | 1 month | days | ~$5 |

Layer 3's ~$180: ~7.8 TB stored (3.7 TB live state, 30 days of SST
churn, 30 days of log archive) ≈ $100, ~140 GB/day of cross-region
transfer ≈ $85, a few thousand PUTs a day. The 3.7 TB seed is ~$75
once. Layers 1–3 and 5 add **~$230/mo, ~9% of the ~$2.5k object-store
bill.** Blobs dominate. Layer 4 is ~$1.4k of storage (350 TB), ~$600 of
replication PUTs (30 M/mo) and ~$400 of transfer. Packing a day's new blobs
into a few large objects (code) brings it to ~$1.8k on Glacier IR or ~$0.75k
on Deep Archive (12–48 h restores). Without layer 4, blobs are lost on
account or region loss; it is a separate decision. Prices are list prices
from memory (IA $0.0125/GB and $0.01/1k PUT, Glacier IR $0.004/GB and
$0.02/1k PUT, Deep Archive $0.00099/GB and $0.05/1k PUT, inter-region
$0.02/GB): verify before relying on them. Restore times assume
server-side clones and replay at bench rates; none of it is measured.

### Signing keys
Signing keys, reserved keys and TOTP secrets are stored only wrapped under
the KEK ("Secrets at rest"), in account rows (`a/{did}`) and private state,
and so in log segments, SSTs and every backup copy of them. A backup alone
no longer lets its reader sign as any user. What it still holds: argon2id
password hashes, app-password and recovery-code hashes, and email-token
keyed digests (see the table in "Secrets at rest"). The catch: **the KEK is
now part of every backup.** Lose it (a deleted or disabled Cloud KMS key,
a lost `--kek-file`) and no restored account can sign, so every account
would need a PLC rotation. The KMS key needs its own multi-region replica
(or a key in a multi-region location), deletion protection (Cloud KMS
destroy-scheduled duration at its maximum, IAM that keeps
`cloudkms.cryptoKeyVersions.destroy` from node and operator roles), and a
restore drill that unwraps from the backup. The 30-day compliance lock on
backups also keeps blobs wrapped under a KEK version that has since been
rotated out: keep old KEK versions enabled (or their key files) for at
least the backup retention.

## Email

Email confirmation, email update, password reset, account deletion, PLC
operation, sign-in code (email 2FA) and admin `sendEmail` mail go out over
SMTP (`src/mail.rs`, lettre over rustls) when configured:

| Flag | Env | Reference PDS env (also read) |
|---|---|---|
| `--email-smtp-url` (alias `--smtp-url`) | `VLPDS_EMAIL_SMTP_URL` | `PDS_EMAIL_SMTP_URL` |
| `--email-from-address` (alias `--email-from`) | `VLPDS_EMAIL_FROM_ADDRESS` | `PDS_EMAIL_FROM_ADDRESS` |
| `--moderation-email-smtp-url` | `VLPDS_MODERATION_EMAIL_SMTP_URL` | `PDS_MODERATION_EMAIL_SMTP_URL` |
| `--moderation-email-address` | `VLPDS_MODERATION_EMAIL_ADDRESS` | `PDS_MODERATION_EMAIL_ADDRESS` |
| `--email-brand-name` | `VLPDS_EMAIL_BRAND_NAME` | `PDS_SERVICE_NAME` |
| `--email-home-url` | `VLPDS_EMAIL_HOME_URL` | `PDS_HOME_URL` |
| `--email-logo-url` | `VLPDS_EMAIL_LOGO_URL` | `PDS_LOGO_URL` |
| `--email-primary-color` | `VLPDS_EMAIL_PRIMARY_COLOR` | `PDS_PRIMARY_COLOR` |
| `--email-disable-confirmation-link` | `VLPDS_EMAIL_DISABLE_CONFIRMATION_LINK` | `PDS_EMAIL_DISABLE_CONFIRMATION_LINK` |

**Templates** (`src/mail/templates.rs`, `src/mail/layout.html`). The account
emails are the reference's six `ServerMailer` templates (reset password,
delete account, confirm email, update email, PLC operation, sign-in auth
factor), with its subjects ("Password Reset Requested", "Account Deletion
Requested", "Email Confirmation", "Email Update Requested", "PLC Update
Operation Requested", "Sign-in Confirmation") and its wording. They go out as
`multipart/alternative`: a plain-text part (title, intro, the token on its own
line, outro, footer) and the reference's HTML. The reference's `.hbs` files
share one layout and differ only in title, preheader, intro, outro and token,
so vlpds compiles that layout in once (`include_str!`) and fills those slots by
single-pass `{{name}}` substitution, with no template engine. Every value is
HTML-escaped (`& < > " '`), including handles, tokens and the operator's
branding; only the intro and outro fragments, built in Rust from escaped
pieces, go in raw. Branding follows the reference's `BrandingConfig`: the
service name defaults to "{hostname} PDS" (from `--public-url`), and the home
URL, logo and color default to the reference's (bsky.app, the Bluesky logo,
`#067df7`). The sign-in mail's "changing your password" link is
`{public_url}/.well-known/change-password` (the reference uses its OAuth
issuer, which is the same URL). The layout and wording come from
bluesky-social/atproto (MIT / Apache-2.0); `src/mail/NOTICE` carries its MIT
copyright and permission notice.

**Moderation mail.** As in the reference's `ModerationMailer`, admin
`sendEmail` content is HTML from the moderator. It goes out unchanged as the
HTML part, with a plain-text part derived by stripping tags (block tags
become line breaks, entities are decoded), through the moderation mailer when
`--moderation-email-smtp-url` and `--moderation-email-address` are set (both
or neither, as in the reference). **Unlike the reference**, if they are unset
admin mail falls back to the main mailer and its from address. The reference
puts moderation mail on a JSON (log-only) transport in that case, so
`sendEmail` answers `sent: true` and nothing is delivered. vlpds sends it
through the main mailer instead, so a single-SMTP deployment still delivers
moderation mail. With no mailer at all, it is logged like other mail.

The URL follows the reference PDS's (nodemailer) form:
`smtp://user:pass@host[:port]` upgrades with STARTTLS when offered (port 587;
`?tls=required` insists, `?tls=none` is plaintext on 25), `smtps://` is
implicit TLS (465). Set both or neither, as in the reference. Neither: mail is
logged (recipient, subject, purpose; token and body only at debug) and not
sent; dev mode keeps every mail in the per-node dev mailbox
(`vlpds.admin.getDevMail`) either way.

The request path never waits on SMTP: `deliver` enqueues on a bounded queue
(1,024; full drops and counts) and a background task sends up to 4 at a time
over a pooled transport. Connect (and per-command) timeout 10 s, 30 s per
attempt; transient failures (4xx, network, timeout) retry after ~2 s, 10 s and
60 s, 5xx rejections do not. Metrics:
`vlpds_mail_messages_total{result=sent|failed|dropped,purpose}`,
`vlpds_mail_retries_total`, `vlpds_mail_queue_depth`, `vlpds_mail_send_seconds`.
Each node mails for the requests it handles; tokens live in the account's
private state, so any node verifies them. Queued mail is lost if the node
stops (the user asks again). A moderation mailer is a second `SmtpMailer`
with its own queue and pool; the metrics are shared (`purpose="admin"`).

## Choosing a bucket (`vlpds-bucket-probe`)

vlpds is only correct on a store with strongly consistent conditional writes:
segment PUTs and log fences are `If-None-Match: *` creates, and node leases,
shard assignments, the layout and writer ids are `If-Match` CAS on the ETag.
A store that silently ignores either header loses data on failover. Before
pointing a deployment at a bucket, run the probe from the datacenter the nodes
will run in. It uses the node's client (`Store::s3`: same pool, timeouts,
path-style addressing) and its `VLPDS_S3_*` env vars / `--s3-*` flags:

    cargo build --release --bin vlpds-bucket-probe
    VLPDS_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com VLPDS_S3_BUCKET=my-bucket \
    VLPDS_S3_ACCESS_KEY=... VLPDS_S3_SECRET_KEY=... VLPDS_S3_REGION=us-east-1 \
      target/release/vlpds-bucket-probe [--ops 200] [--concurrency 4] [--json report.json]

Endpoints (path-style, as the node uses them): S3
`https://s3.<region>.amazonaws.com`; R2
`https://<account>.r2.cloudflarestorage.com` with region `auto`; OVHcloud
`https://s3.<region>.io.cloud.ovh.net`; GCS `https://storage.googleapis.com`
with HMAC keys.

It works under a fresh `vlpds-probe/<random>/` prefix (or `--prefix`, which
must be empty) and deletes everything it wrote unless `--keep`. Checks:

1. **conditional_create**: a second `If-None-Match: *` create of a key fails
   with AlreadyExists and leaves the first bytes intact.
2. **compare_and_swap**: CAS with the GET's ETag succeeds and changes the
   ETag; a stale ETag and a CAS on a missing key are refused; the ETag a PUT
   returns works for the next CAS (lease renewals rely on it).
3. **race_create / race_cas**: `--racers` (16) concurrent creates, or CASes
   from one ETag, on each of `--race-rounds` (4) keys: exactly one wins, every
   loser gets a conflict, and the object holds the winner's bytes.
4. **list_read_delete / multipart**: LIST right after PUT returns every
   object with its size, in order, and honors start-after offsets (the fence
   scan); deletes are visible to LIST and GET; a two-part multipart upload
   completes with the right bytes and an aborted one leaves nothing.

Then latency: `--ops` requests at `--concurrency` in flight for each request
shape the node issues, `put_1mib`, `put_64kib`, `put_create_64kib` (a segment
PUT), `put_cas_small` (a lease renewal), `get_1kib`, `get_range_4kib` (a
segment header read), `head`, `list` (~`--ops` keys) and `delete`, reported
as p50/p90/p99/max and ops/s. An acked write waits for at least one segment
PUT, so `put_create_64kib` bounds write latency from below; the lease CAS max
should stay well under the renewal interval (TTL/5, 2 s by default).

The last line is the verdict: `SAFE for vlpds` (exit 0) or `UNSAFE: <check>:
<reason>` (exit 1); exit 2 means it could not run (endpoint, credentials, a
non-empty `--prefix`). `--json PATH` also writes the report as JSON (`--json -`
prints only the JSON); `--skip-latency` runs only the checks.

## Rate limits: observability and runtime config

The reference-parity buckets (`src/ratelimit.rs`) are defaults. Operators
can see who is consuming them and change them on a live cluster without a
restart (`src/ratelimit/{config,runtime}.rs`, `src/xrpc/ratelimits.rs`,
console tab "Rate limits").

**Config object.** `{prefix}/config/ratelimits.json` in the shared bucket is
a versioned JSON document of changes from the defaults:
- per built-in bucket: `points`, `windowSecs`, `enabled`;
- a global `enabled` switch;
- extra IP-keyed `routes` for any XRPC method (`route:{nsid}`, max 64,
  proxied methods included);
- `overrides` (max 1000): an IP/CIDR or a DID, optionally limited to named
  buckets, either `exempt` or a custom `points` limit;
- server-written metadata: `version`, `updatedAt`, `updatedBy`, `note`,
  `history` (the last 50 changes).

`{}` (or no object) means the flag defaults. `--no-rate-limits` still
removes the layer from that node; its refresher keeps running, so its
console can show and edit the cluster's config. Unknown fields are
rejected, so a typo never silently does nothing.

**Override semantics.** An IP override matches the request's client IP (as
`trusted_proxies` resolves it) and covers every bucket that request consumes.
A DID override matches DID-keyed buckets (repo writes, updateHandle, email
flows, sign-in-account) by key. It does not touch global-ip: the layer runs
before authentication, and trusting an unverified token's `sub` would let
anyone claim a trusted DID. Exempt beats a custom limit; between custom
limits the larger wins.

**Writes: CAS plus optimistic concurrency.** `vlpds.admin.updateRateLimits
{config, ifVersion, actor?, note?}` works as follows:
1. Reads the object and refuses with 409 `ConfigConflict` unless
   `ifVersion` is its version (an unreadable object's version counts, so a
   hand-broken object can be replaced).
2. Validates, refusing with 400 `InvalidConfig`, which lists every problem
   with its JSON path.
3. Writes version + 1 with `If-Match` (or create-if-absent). A lost race is
   also a 409.
4. Installs the new policy locally, then POSTs
   `/internal/v1/ratelimits/reload` to every live peer (2 s each). The
   response lists the version each node now runs.

Each save logs an audit line (target `vlpds::audit`: version, actor, client
IP, node, a readable diff) and appends the same entry to `history`.

**Reads: every node converges.** Each node re-reads the object at startup,
when nudged, and every 10 s with `If-None-Match` (a 304 when unchanged: one
conditional GET per node per 10 s, about $0.0035/node/day on S3). A missed
nudge costs at most 10 s of staleness. It deliberately doesn't ride on the
cluster step, whose steady-state request budget is asserted in
`cluster::tests`. On one node, loads and saves are serialized, so a slow
load never installs an older version over a newer one.

An object that fails parsing or validation never takes a node down. The
node keeps its last good policy (defaults if it never had one), records
`configError {version, message}` (shown per node in the endpoint and
console) and bumps `vlpds_rate_limit_config_errors_total`.

**Swap without losing state.** The policy is an immutable `Arc<Policy>`
behind a lock; each request takes one snapshot. Counters are keyed by
(bucket, window length, key):
- A new points value or override applies to each key's live window.
- A new window length starts fresh windows. The old ones expire through the
  normal sweep.
- Key types never change: built-ins are fixed, and route buckets are always
  per IP.

**Observability, bounded.**
- *Heavy hitters.* Each of the 64 counter shards keeps up to 8 candidates
  per bucket, updated under the shard lock the consume already holds. A key
  enters only when it outweighs the lightest candidate, and keys are
  truncated to 96 bytes. Memory is at most 64 × 8 × buckets entries. A top-N
  list is exact unless more than 8 of its keys hash to one shard.
- *429 tallies.* Rejections are counted by (bucket, route) in 15 one-minute
  slots, capped at 1024 series; the route is the matched XRPC method or
  path, else `_proxy_or_unmatched`.
- *Metrics* (additive; labels are bucket and route only, never IPs or DIDs):
  - `vlpds_rate_limit_rejections_total{limiter,route}`
  - `vlpds_rate_limit_config_version`
  - `vlpds_rate_limit_config_errors_total`
  - `vlpds_rate_limit_config_loads_total{result}`
  - `vlpds_rate_limited_total` (unchanged)

`vlpds.admin.getRateLimits?top=N` (admin) returns this node's view and
gathers peers' `/internal/v1/ratelimits` over the peer client (3 s per
peer; a peer that fails is listed in `unreachableNodes`). It merges top
keys per bucket: `used` is summed and `maxNodeUsed` is what one node checks
against its limit, since per-IP counters are per node. It also sums the 429
tallies and lists each node's config version, error and load times.
`local=true` skips the fan-out. The console polls it every 5 s and derives
429/s per bucket from successive totals, as Live metrics does from
`/metrics`.

**Cost on the hot path.** Per limited request:
- one policy snapshot (a read lock and an `Arc` clone);
- no extra work for overrides unless any are configured (IP overrides are
  matched once per request, DID overrides are one hash lookup per DID
  consume);
- one small-map lookup plus a scan of up to 8 entries per consume for the
  heavy hitters.

Requests without rate limits (`--no-rate-limits`, non-XRPC paths) are
unchanged.

## Secrets at rest (`src/secrets.rs`)

Everything durable is in one bucket (and its log segments, SSTs, backups
and replicas), so anyone who can read the bucket would get any secret
stored there as-is. Secrets the PDS must be able to recover are stored
only **wrapped under a key-encryption key (KEK)**. Secrets it only verifies
are stored as hashes.

| Secret | Where | At rest |
|---|---|---|
| Repo signing key (secp256k1) | `a/{did}` `wrapped_signing_key` | wrapped, AAD = purpose + DID; public key alongside (`signing_pubkey`) |
| Reserved signing key | `p/_reserved:{did:key}\0k` | wrapped, AAD = purpose + did:key |
| TOTP secret (enabled and pending) | `p/{did}\0totp` | wrapped, AAD = purpose + DID |
| Account password | `a/{did}` `password_hash` | argon2id (unchanged: a verifier) |
| App passwords | `p/{did}\0apphash/{h}` | SHA-256 of DID + server-generated ~80-bit password (unchanged) |
| TOTP recovery codes | in `p/{did}\0totp` | SHA-256 (unchanged; ~50 bits, second factor only, needs the password too) |
| Email tokens (confirm, update, reset, delete, PLC) | `p/{did}\0etok/{purpose}`, `p/_reset:{digest}\0t` | HMAC-SHA256 under a key derived from `jwt_secret` (were plaintext, the reset token even in the key) |
| OAuth codes, refresh tokens | `oauth/*` rows | hashes / MACs under keys derived from `jwt_secret` (unchanged) |
| Sessions | `p/{did}\0sess/{id}` | ids only: tokens are JWTs under `jwt_secret` (unchanged) |
| PLC rotation key | flag / env, or a KEK-wrapped file (`--plc-rotation-key-file`) | never in the bucket; unwrapped at startup ("PLC identity") |
| `jwt_secret`, admin / internal tokens, SMTP credentials | flags / env | never in the bucket |
| DPoP keys | clients | never on the server |

**Wrapping.** A `KeyWrapper` holds one KEK and wraps a secret with
associated data `vlpds-secret-v1 ‖ purpose ‖ subject`. A blob copied into
another account's row, or used for another purpose, fails authentication.
The stored form is `vw1.{kid}.{base64url}`, where `kid` names the KEK (`L` +
16 hex chars of a hash of a local key, `G` + 16 hex chars of a hash of a
Cloud KMS key name). Backends:
- *Local* (`--kek-file` / `VLPDS_KEK`, 32 random bytes): XChaCha20-Poly1305
  with a random 192-bit nonce per wrap. Required outside `--dev-mode` unless
  Cloud KMS is configured. Dev mode falls back to a well-known dev KEK, and
  `check_secrets` refuses that KEK outside dev mode.
- *Google Cloud KMS* (`--gcp-kms-key`): the secret itself (32 bytes) is
  the KMS plaintext: `encrypt`/`decrypt` over the REST API with CRC32C
  integrity fields and `additionalAuthenticatedData`, using the node's
  service-account token from the metadata server. The client is the shared
  public client (§7), with a 5 s deadline per call and one token refresh on
  a 401. No new dependencies, so no cargo feature. A bucket copy is useless
  without decrypt permission on the key, and every unwrap shows in the KMS
  audit log. Not a per-account DEK: a DEK would still need a KMS call to
  unwrap per account, and one deployment-wide DEK held in memory would undo
  the audit and revocation properties.
- AWS KMS is not implemented. It would be another `KeyWrapper` (SigV4
  `Encrypt`/`Decrypt` with `EncryptionContext`).

**Rotation.** The keyring wraps under its current KEK (the Cloud KMS key if
set, else the local KEK) and unwraps under any configured KEK
(`--kek-old-file`, `VLPDS_KEK_OLD`, `--gcp-kms-old-key`), by `kid`. Inside
one CryptoKey, KMS rotates versions by itself, and `decrypt`'s
`usedPrimary: false` marks a blob stale. `vlpds.admin.rewrapSecrets`
(per node, over the shards it owns) rewraps every stale signing key
(an account update with no events; a key that changed meanwhile is left
alone), reserved key and TOTP secret. `dryRun` counts what is left;
`checkVersions` unwraps even blobs under the current kid, to find old KMS
versions. Old KEK material must outlive the backup retention
("Backups and restore").

**Hot path.** Unwrapped signing keys are cached per DID (the `signing_keys`
cache in `caches.rs`: 16 LRU shards bounded by the cache budget; an entry
is valid only for the account's current public key, so a key rotation
misses and a rewrap still hits). `Keypair` erases its scalar on drop, and
plaintext buffers are `Zeroizing`. Keys enter the cache when created
(createAccount, bulkCreate and updateAccountSigningKey wrap and cache in
one step), so new accounts never unwrap. Otherwise a key is unwrapped once
per account per cache lifetime: at a repo's cold load (shard preloads warm
recently written repos after a takeover), on a proxy service-JWT miss, and
for getServiceAuth. Cold unwraps of one DID are coalesced (256 striped
locks). Remote calls are limited to `--kms-concurrency` (64) in flight,
time out after 5 s, and fail fast for 1 s after the key service fails. A
loaded repo holds its `Arc<Keypair>`, so the commit path never touches the
keyring. Readers that only need the public key (DID documents,
describeRepo, service-auth issuer checks, `checkAccountStatus`,
getRecommendedDidCredentials) read `signing_pubkey` and never unwrap.

**Failure behaviour.** If an unwrap fails as unavailable (timeout, 5xx,
auth), the repo still loads without its key: reads and exports work, writes
answer 503 `KeyUnavailable` (nothing applied; every 503 carries
Retry-After), and the next write reloads and tries again. A new account,
reserved key or TOTP secret that can't be wrapped fails with the same 503
and writes nothing (createAccount releases the handle and email claims it
made while the wrap was in flight). A *rejected* unwrap (wrong KEK or
AAD, corrupt, unknown kid) is a 500 and a log line. Metrics:
`vlpds_kms_requests_total{backend,op,result}`,
`vlpds_kms_request_seconds{backend,op}`,
`vlpds_signing_key_cache_total{result}`, and
`vlpds_cache_entries{cache="signing_keys"}`. Alerts:
`VlpdsKeyServiceUnavailable`, `VlpdsSecretUnwrapRejected`
(ops/RUNBOOK.md).

**Cost (M4 Pro, dev-release, shared machine).** `bench_commit_cpu` measured
before and after on a loaded machine (measured before full-tree mode was
removed): lazy trees 20.9–23.4 µs/commit before and 21.3–24.5 after (full
trees 17.2–18.1 vs 17.6–18.6). That is within run-to-run noise: the commit path changed only by an `Option`
deref. Keyring (`secrets::tests::bench_keyring`): a cache hit is about
0.1 µs. A local-KEK cold unwrap, including key parsing and the public-key
check, takes 43–65 µs, against a few milliseconds of store reads for the
cold repo load it is part of. A Cloud KMS unwrap adds one KMS round trip
(typically 5–30 ms in-region) to that cold load, once per account per
cache lifetime. Account creation adds one KMS encrypt, run concurrently
with the argon2 hash (~20 ms).

Tests: `secrets::tests` (round trip, AAD and KEK binding, tampering,
rotation and rewrap, cache, KEK parsing, the dev-KEK rules) and
`tests/all/secrets_at_rest.rs`. The integration tests check:
- no signing key (current, rotated out or reserved), TOTP secret, reset
  token or KEK appears in any common encoding in any bucket object (log
  segments decoded) or in any state key or value;
- rotation end to end: a new KEK with the old one unwrap-only, a dry run,
  a rewrap and a dry run that finds nothing, then the old KEK retired; a
  node without the KEK still serves reads;
- against a mocked Cloud KMS: one wrap per new account and no unwraps
  while warm; at most one decrypt per account after a restart, even with
  racing writes; with KMS down, 503 `KeyUnavailable` with nothing written,
  while reads and getRepo work; writes resume after recovery.

### Signing hardening (`src/crypto.rs`)
With deterministic ECDSA, one faulty signature (Rowhammer, glitching, a bad
DIMM) next to a correct one over the same message gives away the key, and
commit signatures are public on the firehose. So, for every signature that
leaves the node:
- **Hedged nonces.** 32 fresh bytes from a thread-local CSPRNG
  (`rand::thread_rng`, ChaCha12 seeded from the OS) go into the RFC 6979
  nonce as §3.6 additional data (libsecp256k1's `ndata`; our hardware-SHA
  nonce function appends them to the seed exactly as
  `nonce_function_rfc6979` does, tested against it). A broken RNG degrades
  to plain RFC 6979. Signatures stay low-S compact but are no longer
  reproducible; `Keypair::sign_deterministic` remains for tests that compare
  with k256/shrike. The OAuth server key (ES256) uses `p256`'s hedged
  `sign_with_rng`.
- **Verify after sign.** Commits, service-auth JWTs (proxy, getServiceAuth)
  and OAuth access tokens are verified against the key's cached public key,
  over a freshly hashed message, before they can be sequenced or returned.
  A failure is never emitted: it counts
  `vlpds_signature_verify_failures_total{purpose}`, logs an error and signs
  again with a fresh nonce; a second failure is a 503 `SignatureFault` with
  nothing applied (the repo is evicted and reloads). Three failures within a
  minute fail-stop the node (`signature_fault`, exit 6). A repo load also
  re-derives the public key from the cached scalar and checks it against
  `signing_pubkey` (`purpose="key_load"`). Session JWTs are HMACs. PLC
  operations (server rotation key) are verified too
  (`purpose="plc_operation"`).

Alerts `VlpdsSignatureFault`, `VlpdsSignatureFaultFailStop` (RUNBOOK: suspect
hardware; drain and replace the host). Tests: `crypto::tests`,
`oauth::jose::tests::server_key_faults_are_caught`, and
`tests/all/signature_faults.rs`, which injects faults (`crypto::fault`, a
test-only hook flipping a bit of the signature or of the scalar while
signing) and checks that no faulty commit reaches the firehose or getRepo,
that one fault is re-signed and two give a clean 503, and that the metric
moves and the third fault fail-stops.

**Cost (M4 Pro, release, shared laptop; medians of 11 interleaved rounds,
`crypto::tests::bench_sign`).** Deterministic sign 11.4 µs, hedged 11.5 µs
(the CSPRNG is 24 ns: noise), hedged + verify 26.3 µs (verify alone 13.2
µs). `worker::tests::bench_commit_cpu`, old and new binaries interleaved, 6
runs each, median for 20 / 5000 records: 20.1 / 20.4 µs/commit before
(range 19.9–21.3), 35.0 / 34.6 µs after (33.0–36.3).
Verification is the whole ~15 µs: about +15% of the ~96 µs whole-node commit
(see "CPU"). Verifying only a sample would leave the skipped signatures
free to leak the key.

## Handle policy (`src/handle_policy.rs`)

The reference's reserved-handle list and explicit-slur filter
(packages/pds/src/handle), verbatim as data files compiled in
(`src/handle_policy/reserved.txt`, 1,029 labels in its three sections;
`explicit_slurs.txt`, its 7 regexes), so a diff against upstream is a diff of
two text files. Applied where the reference's `normalizeAndValidateHandle`
is, with its error names and messages:
- slurs: every user-chosen handle (createAccount, OAuth sign-up,
  identity.updateHandle), service domain or custom domain, matched as is and
  with `.`, `-`, `_` removed: 400 `InvalidHandle` "Inappropriate language in
  handle". Also client-chosen record keys on create/put/applyWrites (not
  deletes): 400 `InvalidRequest` "Unacceptable slur in record key".
  checkHandleAvailability reports such handles unavailable.
- reserved: the first label of a service-domain handle only: 400
  `HandleNotAvailable` "Reserved handle".
- admins (`com.atproto.admin.updateAccountHandle`, the reference's
  `allowAnyValid`) skip both, but a service-domain handle must still be one
  3-18 character label. Unlike the reference, an admin-set custom domain is
  not resolved first.
- updateHandle now also refuses the disallowed TLDs (`.local`, `.onion`, ...)
  as createAccount does.

The regexes are JS without flags (case-sensitive, ASCII `\b`); handles and
record keys are ASCII, where the `regex` crate's Unicode `\b` agrees. A
differential run against the JS implementation over 200k generated strings
matched (`handle_policy::tests::slurs_corpus_differential`, ignored; needs a
corpus file).

## Moderation service auth, earned invites, disposable email, DNS handles

Four reference PDS features, ported with the reference's behaviour and
messages (tests: `ref_moderator_auth`, `invite_codes::ref_*`,
`ref_account::ref_fails_on_disallowed_emails`, `ref_handles::ref_*dns*`).

- **Moderation service** (`--mod-service-did`, reference `modServiceDid`;
  `src/xrpc/authn.rs`). On the reference's `authVerifier.moderator` methods
  (`MODERATOR_METHODS`: getAccountInfo(s), get/updateSubjectStatus,
  sendEmail, getInviteCodes, disableInviteCodes, enable/disableAccountInvites)
  a Bearer token is only ever a service JWT from that DID (or
  `<did>#atproto_labeler`, keyed by `#atproto_label`): checked by the
  existing inbound verifier (`verify_service_jwt_from`: exp, aud = our
  service DID, lxm = the method, issuer allow-list before key resolution,
  signature with one fresh-document retry). Another issuer, or no flag, is
  401 `UntrustedIss` "Untrusted issuer". The result is
  `Credentials::ModService`, which `require_moderator` accepts and every
  user-permission check refuses; `require_admin` (the reference's
  `adminToken`: deleteAccount, updateAccount*, createInviteCode(s),
  `vlpds.admin.*`) still takes Basic auth only. getPreferences is the
  reference's `authorizationOrModService`: a Bearer token whose (unverified)
  `iss` is the moderation service is verified as one and reads the `did`
  parameter's preferences, personalDetailsPref included, never proxied.
  Not ported: the reference's entryway DID as an accepted `aud`, and routing
  `tools.ozone.*` to `modServiceUrl` (vlpds still sends those to the AppView).
- **Earned invite codes** (`--invite-interval-ms` / `--invite-epoch-ms`,
  reference `inviteInterval` / `inviteEpoch`, only with invites required).
  getAccountInviteCodes (`createAvailable`, default true) creates the codes
  the reference's `calculateCodesToCreate` allows, unchanged: one per
  interval of account age (only age after the epoch for older accounts),
  minus routine codes created since the epoch, at most 5 unused routine
  codes; admin-gifted codes don't count. They are single-use, `createdBy` =
  the account, and created disabled when its invites are disabled. Creation
  is serialized per account on the node, and a concurrent creation on
  another node is caught afterwards (400 `DuplicateCreate`), as in the
  reference. Unlike the reference, enable/disableAccountInvites also flip the
  account's existing codes (unchanged vlpds behaviour).
- **Disposable email** (`src/email_policy.rs`): the reference's
  `disposable-email-domains-js` list (v1.26.0, 8,883 domains, CC0), vendored
  as a text file; exact match on the part after the last `@`, case-folded.
  createAccount (and OAuth sign-up, which goes through it) and updateEmail
  refuse it with "This email address is not supported, please use a
  different email."; admin updateAccountEmail doesn't check it (as the
  reference).
- **DNS TXT handles** (`src/handle_resolver.rs`): updateHandle to an external
  domain resolves it as the reference's `HandleResolver`: `_atproto.<handle>`
  TXT and `/.well-known/atproto-did` in parallel, the DNS answer (exactly
  one `did=` record) winning, else the HTTPS one (a `did:` first line), 3 s
  each. Lookups go to the system resolver only, for the fully qualified name
  (no search domains), considering at most 32 records of 4 KiB; the HTTPS
  fetch stays SSRF-guarded and size-capped. `server::Config::txt_resolver`
  injects a stub for tests. The reference's backup nameservers are not
  ported. Dev mode still skips the proof.

## User service auth on uploadBlob (video uploads)

The reference authorizes `com.atproto.repo.uploadBlob` with
`authorizationOrUserServiceAuth`, and the Bluesky app's video upload relies
on it. The app asks its PDS for a service token (getServiceAuth, aud =
`did:web:<PDS host>`, lxm = uploadBlob, 30 minutes) and sends the video, with
that token, straight to video.bsky.app (`app.bsky.video.uploadVideo`, not via
the PDS). The video service transcodes it and calls the user's PDS's
uploadBlob with the token. The app then writes the `app.bsky.embed.video`
post with its own session. The other `app.bsky.video.*` calls
(getUploadLimits with a token for the video service's DID, getJobStatus,
and the multipart start/upload/finish methods) also go to the video service
directly; the reference PDS has no video-specific code, and nothing here
proxies them specially (sent through the PDS, they take the generic proxy
like any `app.bsky.*` method).

`src/xrpc/authn.rs`: on the methods in `USER_SERVICE_AUTH_METHODS`
(uploadBlob only), a Bearer token whose unverified payload has an `lxm`
claim is a service JWT (the reference's `isDefinitelyServiceAuth`; session
tokens never carry one). It is checked by the inbound verifier
(`verify_service_jwt`: `typ`, exp, aud = our service DID exactly (no
`#atproto_pds` form, and no entryway DID since vlpds has none), lxm = the
method, the issuer's current `#atproto` key with one fresh-document retry).
The issuer must be an account hosted here: a foreign DID or a
`did#service` issuer is the reference's actor-store miss, 400 NotFound
"Repo not found". The result is `Credentials::UserServiceAuth`, which allows
blob uploads (no OAuth-style scope narrowing) and nothing else. Every other
method treats such a token as a session token, which fails as one. As in the
reference there is no `jti` replay check (the video service may retry; tokens
live at most an hour) and no `iat` bound. Unlike the reference, uploadBlob
refuses a taken-down account (401 AccountTakedown) with service auth too:
the reference checks takedown only on the session path, so a token issued
before the takedown would still work. Deactivated accounts may upload.

In a cluster, such a token has no `sub`, so `forward.rs` routes uploadBlob by
the token's `iss` (its DID part) to the account's owner.

createAccount's optional service auth (`userServiceAuthOptional`, migration
in) was already the same check (`authn::optional_service_auth`): any Bearer
token there must be a service JWT for createAccount.

## Push registration (`src/xrpc/proxy/push.rs`)

`app.bsky.notification.{registerPush,unregisterPush}` name their service in
the body (`serviceDid`), so the generic proxy (which would always pick the
AppView) doesn't serve them. As in the reference: the OAuth check is
`rpc:{lxm}?aud={serviceDid}#bsky_notif` (missing: 403 `ScopeMissingError`,
`Missing required scope "rpc:...?aud=...%23bsky_notif"`, the reference's
`assertRpc`, via `Credentials::need_rpc`); the forwarded call carries a
service-auth JWT from the account's repo key through the proxy's signer
(hedged nonce, verify-after-sign) with iss = account, **aud = the bare
`serviceDid`** (the reference's `serviceAuthHeaders(did, serviceDid, lxm)`;
`#bsky_notif` appears only in the scope check), lxm = the method. When
`serviceDid` is the configured AppView's DID its URL is used; otherwise the
DID document's `#bsky_notif` service of type `BskyNotificationService` (400
"invalid notification service details in did document" without one),
called through the SSRF-guarded client (§7). Without an AppView configured
the reference doesn't register these methods; vlpds answers 400 "No service
configured". Upstream errors pass through as for proxied calls.

## Email second factor (`src/xrpc/email2fa.rs`)

The reference's `emailAuthFactor`, the only second factor the Bluesky app
offers; vlpds's TOTP (`vlpds.server.*Totp`) stays as a second option.
- **Toggle** via `com.atproto.server.updateEmail` with the current address
  (case-insensitive): `emailAuthFactor: true` needs a confirmed email and no
  token ("Please change and verify your email before enabling OTP"
  otherwise); `false` is two-phase: without a token it mails an
  `update_email` code and fails `TokenRequired`, with one (from that mail or
  requestEmailUpdate) it clears the factor. Both are idempotent. Any address
  change, user or admin, clears it. Stored as `emailAuthFactorAt` on the
  account; getSession/createSession/refreshSession report `emailAuthFactor`.
- **Sign-in** (createSession and the OAuth sign-in page; app passwords
  bypass it, as in the reference): without `authFactorToken` a fresh
  `auth_factor` code is mailed ("Sign-in Confirmation") and the call fails
  401 `AuthFactorTokenRequired`; the code is an email token like the others
  (15 min, single use, newest replaces older, keyed digest at rest), wrong
  400 `InvalidToken`, stale 400 `ExpiredToken`. The OAuth page shows the
  code step with the obfuscated address (`a***e@e***m`), like the
  reference's `SecondAuthenticationFactorRequiredError('emailOtp', hint)`.
- **Guessing**: on top of createSession's rate limits (the reference's only
  bound), wrong codes count against a per-account lockout with TOTP's
  schedule (5 wrong: locked 5 min, doubling to a day; 429
  `RateLimitExceeded`, no mail sent while locked), persisted in the account's
  private state (`p/{did}\0eotp_lock`) so it holds across nodes, restarts and
  both sign-in paths. The OAuth page's per-attempt limit (3 wrong codes drop
  the pending sign-in) applies to both factors.
- **Precedence**: with TOTP enabled only TOTP is asked for (TOTP or recovery
  code); no email code is mailed or accepted. Both may stay enabled:
  turning TOTP off falls back to the email factor, but the weaker factor
  never substitutes for the stronger one. The Bluesky app shows its generic
  code field on `AuthFactorTokenRequired`, which takes a TOTP code too.
- As in the reference's `login()` (`if (authFactorToken)`), a non-empty
  `authFactorToken` is checked as an `auth_factor` email code whenever one
  is sent: for an account without a factor (no code was mailed: 400
  `InvalidToken`) and for app-password logins too (which otherwise skip the
  factor; a valid mailed code is accepted and spent). Such guesses count
  against the same lockout. With TOTP on, a password login's code is
  checked as TOTP only (Precedence above).

## PLC identity (`src/plc`)

Accounts get real `did:plc` identities, as on the reference PDS: the DID is
registered with the PLC directory (`--plc-url`, default
`https://plc.directory`) before the account exists, and this PDS holds a
**PLC rotation key** that can update it. Before this, DIDs were random
`did:plc`-shaped strings that only this server resolved: accounts were not
on the network and could not migrate out.

**Operations, byte for byte with did-method-plc** (`@did-plc/lib`, used by
the reference). An op is DAG-CBOR (canonical key order) signed with a
rotation key: ES256K over the encoding without `sig`, compact 64-byte low-S,
base64url without padding. Signing goes through `Keypair::sign_verified`
(hedged nonce, verified before use, `purpose="plc_operation"`; see "Signing
hardening"). The DID is `did:plc:` + the first 24 chars of
base32(sha256(signed genesis op)); an update's `prev` is the CID of the op
it follows (`createUpdateOp`). `plc::PlcLog::apply` is the directory's
`assureValidNextOp` (genesis hash, signature by a rotation key of the
previous op, tombstones, recovery forks by a higher-priority key within
72 h); the mock directory and the tests use it. Vectors:
`testdata/plc` (did-method-plc interop audit logs: DIDs, op CIDs,
signatures incl. high-S / DER / bad base64, nullification), all replayed by
`plc::tests`.

**Genesis (createAccount).** Rotation keys `[recoveryKey from the request?,
--plc-recovery-did-key?, server rotation key]` (reference
`formatDidAndPlcOp` ordering), `verificationMethods.atproto` = the new
signing key, `alsoKnownAs` = `at://{handle}`, `services.atproto_pds` =
`--public-url`. Each signature is hedged, so re-signing gives a new DID: the
op is re-signed until the DID lands in a partition this node owns (as
random DIDs were; ~N tries for N nodes, ~30 µs each). Order: validate, then
claim handle/email/invite + hash + wrap the signing key (concurrently, as
before), then **POST the genesis op and wait for the directory's 200**, then
create the repo and account (`CreateRepo`), then reply. A PLC refusal or
outage releases the claims and returns 500 `InternalServerError` (the
reference's error: its `PlcClientError` is not an XRPC error) with the
directory's reason; nothing was written and no event emitted. If the local
create fails after PLC accepted, the DID is tombstoned (best effort,
logged), as the reference does. Bringing a DID (migration in) is unchanged:
no genesis op.

**Updates.** `updateHandle` and admin `updateAccountHandle` claim the new
handle, then submit a PLC update (fetch `/{did}/log/last`, replace the
first `at://` alias, `prev` = its CID), then swap the handle locally and
emit `#identity`. A PLC failure releases the claim and changes nothing; an
unchanged alias submits nothing (the reference would submit a no-op
update). A `did:web` account's document must already name the handle
(reference). Admin `updateAccountSigningKey` records the new key, updates
the `atproto` key in PLC, then re-signs the repo with it: see "Signing-key
rotation".

**Endpoints** (reference semantics): `requestPlcOperationSignature` mails a
`plc_operation` token (full session, taken-down session, or OAuth
`identity:*`; deactivated accounts too); `signPlcOperation` checks and
consumes it, fetches the last op (refusing a tombstoned DID) and returns an
update signed with the server key with the requested `rotationKeys` /
`alsoKnownAs` / `verificationMethods` / `services` replaced (not
submitted); `submitPlcOperation` requires the server rotation key among
`rotationKeys`, `atproto_pds` = this PDS, `atproto` = the account's signing
key and `alsoKnownAs[0]` = `at://{handle}`, forwards the op, and emits
`#identity`. `getRecommendedDidCredentials.rotationKeys` = `[recovery?,
server key]`. Activation and `checkAccountStatus.validDid` check a did:plc
against the directory's `/data` (server rotation key present, endpoint,
signing key; reference `assertValidDidDocumentForService`), so an account
that migrated away can't be reactivated here. **Migration out** is the
reference flow: getServiceAuth for the new PDS, its createAccount with the
DID, repo/blob transfer, then here request + signPlcOperation with the new
PDS's recommended credentials, the new PDS submits it and activates, and
the account is deactivated here (`tests/all/plc.rs` runs it end to end
between two servers).

**The rotation key** is one secp256k1 key for the whole deployment
(reference `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`). It comes from
`--plc-rotation-key` / `VLPDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX` (hex;
`PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX` is read too) or
`--plc-rotation-key-file`: a `vw1.` blob wrapped under the KEK (purpose
`plc-rotation-key`; `vlpds --wrap-plc-rotation-key` makes one, through
Cloud KMS in production) or hex from a mounted secret. It is unwrapped once
at startup (a KMS outage then fails the start) and never written to the
bucket (`tests/all/secrets_at_rest.rs`). Every node of a cluster gets the
same key. **Rotating it**: roll out the new key as current with the old one
in `--plc-rotation-key-old-file`; an update of a DID that lists only the
retired key is signed by it and lists the current key in its place, and
`vlpds.admin.rotatePlcKeys` (per node, its shards' accounts, 4 PLC updates
in flight, `dryRun` to count) does that for every account; then retire the
old key. Activation accepts a retired key meanwhile.

**Modes** (`--plc-mode`). `auto` (default): `directory` with a rotation key,
else `unregistered`, which only `--dev-mode` accepts. `directory`:
everything above. `unregistered` (dev/test/bench only): random local DIDs,
never registered, PLC endpoints 501 — what the in-process suite and the
benches use, so they never reach a directory. The binary refuses to start
outside dev mode without a rotation key. `vlpds.admin.bulkCreate`
(synthetic accounts, deterministic DIDs) never touches the directory in any
mode. For e2e against a real directory, point `--plc-url` at a local
did-method-plc server (plain http is accepted, with a warning outside dev
mode); in-process tests use `plc::mock::MockPlc`, never plc.directory.

**DID documents of hosted accounts** (`identity::account_did_doc`:
`resolveDid`, `resolveIdentity`, `describeRepo`, `getSession` /
`createSession` / `refreshSession`, proxy targets, service-auth issuer
keys). The reference resolves every DID through the directory (cached).
Here an account *active* here gets the locally generated document, which
is the directory's (every change of the DID goes through this PDS) and
costs no network round trip. A **deactivated** account's DID resolves
through the directory (or did:web) like any other: the reference
migration-out flow ends with deactivateAccount on the old PDS (the old PDS
never sees the op that moves the DID: `submitPlcOperation` here and in
the reference refuses an `atproto_pds` that isn't this PDS; the new PDS
submits it), and a migrating-in account isn't pointed here yet.
Deactivation invalidates the node's DID cache entry and emits `#account`
(`active: false, status: deactivated`, as the reference; the new PDS
emits `#identity` on submit and `#account` on activation). After
migration: resolveDid/resolveIdentity/getSession return the directory's
document, describeRepo is `RepoDeactivated`, writes are 401
`AccountDeactivated`, activateAccount is refused (directory `/data` check).
`--plc-mode unregistered` DIDs (nowhere else to resolve) always get the
local document; a failed resolution is `DidNotFound` / 502
`UpstreamFailure` (resolveDid), `InvalidRequest` (describeRepo) or an
omitted `didDoc` (sessions, reference `safeResolveDidDoc`).

**Not done / differences.** An account that migrates away but is left
*active* here keeps the local (stale) document until it is deactivated
or deleted (the reference would serve the directory's after its cache
TTL); other nodes' DID caches (10 min TTL) aren't invalidated on
deactivation (they hold an entry only if they resolved the DID while it
was not active here). No `PDS_PLC_ROTATION_KEY_KMS_KEY_ID` (a KMS-resident signing key): the key
is KMS-*wrapped* instead. signPlcOperation refuses a requested field of the
wrong shape instead of signing it (the reference casts it unchecked). The
mock directory has no rate limits or export.

Metrics `vlpds_plc_requests_total{op,result}` (op: create, update_handle,
update_signing_key, submit, tombstone, rotate_key, get_last_op, get_data; result: ok,
rejected, unavailable, not_found) and `vlpds_plc_request_seconds{op}`; alert
`VlpdsPlcDirectoryUnavailable` and `VlpdsPlcOpsRejected` (RUNBOOK).
Tests: `plc::tests` (vectors, op construction, log rules, config),
`plc::mock::tests`, `tests/all/plc.rs` (genesis, failures, handle and key
updates, sign/submit, migration out, unregistered mode, bulkCreate, key
rotation), `tests/all/secrets_at_rest.rs`
(`plc_rotation_key_never_reaches_the_bucket`).

## Signing-key rotation (`src/xrpc/key_rotation.rs`)

A repo's head commit must verify against the key its DID document lists:
relays check `#commit`/`#sync` signatures and `getRepo` against it. The
reference's rotate-keys script updates the document, then writes an empty
commit signed with the new key and sequences `#identity` and `#sync`. Admin
`updateAccountSigningKey` (CLI `rotate-keys --generate`) does that here,
in three steps ordered with the repo's commits by its worker
(`AccountOp::SigningKey(KeyStep)`):

1. **Begin.** The new key, wrapped under the KEK, goes into the account row
   as `pending_signing_key`, with a `K/{did}` marker, in one durable log
   entry (no events). From then on the worker refuses the repo's writes and
   imports with a retryable 503 `KeyUnavailable`. The key is durable before
   any directory can name it (the old code updated PLC first, so a crash
   right after lost the only copy of the key PLC now listed), and nothing
   is signed with the old key once the document may have changed.
2. **PLC.** A did:plc's `atproto` key is set to it (with PLC registration
   on; a did:web's document is its owner's to change).
3. **Finish.** The account takes the new key (marker deleted) and the head
   is re-signed: same data root, next rev, signed through the hedged
   verify-after-sign signer. One log entry carries the head, the row,
   `#identity` and `#sync` (only `#identity` while the account is
   inactive: activation's `#sync` then announces the re-signed head). The
   call is acknowledged once it is durable, which is also when the durable
   view (`getRepo`, `getLatestCommit`) moves to the new head, so nothing
   served after the acknowledgement is signed with the old key. The next
   `#commit` chains off the `#sync` (`since` = its rev, `prevData` = its
   data).

The firehose sees commits signed with the old key, then `#identity`
immediately followed by `#sync`, then commits signed with the new key
(`tests/all/key_rotation.rs` checks this with writers racing the rotation;
`go_checker::go_checker_accepts_key_rotation_resync` runs the independent
Go checker over it).

**Pending rotations.** A rotation stopped between Begin and Finish (an
outage of the directory or the key service, a crash, the shard moving)
stays pending, its writes fenced. `key_rotation::complete` finishes one
from durable state alone and may run any number of times, concurrently
too: it sets the directory's key to the pending one (a no-op when it
already is), unwraps the pending key and runs Finish (a repeat re-signs
once more with the same key, harmlessly). It abandons the rotation
(`Abort`: pending key and marker dropped) only if the directory refused
the update definitely (4xx, tombstoned DID) and still doesn't name the key;
an ambiguous failure (5xx, timeout) is never taken as "not applied", since
the update may still land. The handler retries in the background after an
undecided failure (1 s backoff doubling to 60 s, while the node owns the
repo), and each node runs `key_rotation::recover_pending` at start and
every 60 s over the `K/` markers of its shards, so after a crash or
takeover the new owner finishes it (`tests/all/key_rotation.rs`
`crash_after_*`: the owner is killed after Begin and after the PLC
update). A second rotation while one is pending is refused (400).

`vlpds.admin.publishIdentity` with `syncPlc` (CLI `rotate-keys`) runs
Finish with the account's current key after setting PLC's key to it, the
script's empty commit and `#sync`.

**Not done.** `rewrapSecrets` doesn't rewrap a pending key (it lives until
the rotation ends; retiring its KEK meanwhile strands it). The marker sweep
costs one seek per populated slot of each owned shard per minute.

## Admin CLI (`src/cli/admin.rs`, `src/xrpc/admin_tools.rs`)

`vlpds admin <command>` covers the reference's `pdsadmin` (account
list/create/delete/takedown/untakedown/reset-password, create-invite-code,
request-crawl) and its maintenance scripts (publish-identity, rotate-keys,
rebuild-repo), plus cluster-status, rotate-plc-keys, rewrap-secrets and
check-repo. ops/RUNBOOK.md "Admin CLI" maps each reference command to
ours. It is a client of admin XRPC on any node, nothing else: no direct
bucket access, so it needs only the URL and the admin token, and a node's
routing (`crate::forward`) sends DID-keyed calls to the repo's owner. Calls
that act on "this node's shards" (rotatePlcKeys, rewrapSecrets) are sent to
every node `getClusterStatus` lists. The library entry (`cli::admin::run`)
is what the binary calls and what `tests/all/admin_cli.rs` drives.

Endpoints added where com.atproto.admin.* has nothing:
`vlpds.admin.publishIdentity {did, syncPlc?}` (the reference's
`sequenceIdentity`, an unchanged account row rewritten through the repo's
worker carrying `#identity`; `syncPlc` is the rotate-keys script: it sets
the PLC `atproto` key to the held signing key, then re-signs the repo with
it, `#identity` + `#sync`, the worker's `KeyStep::Finish` with the current
key), `vlpds.admin.checkRepo?did=` (one shard
snapshot under the apply lock: commit hash / data / DID / signature,
records hash, MST from `R/` vs the head's data root, `M/` vs that tree,
record-CID / blob-ref / collection indexes; no worker involved, so it works
on a repo that won't load), `vlpds.admin.rebuildRepo {did, dryRun?}` (the
rebuild-repo script: the worker's `ReplaceRepo` with the snapshot's records,
new commit and `#sync`, also deleting the stale `M/` nodes and index entries
the snapshot showed; `ReplaceRepo.swap_commit` makes it refuse if a
commit landed after the snapshot, so a concurrent acked write is never
dropped) and `vlpds.admin.requestCrawl {relays?}` (the node's own public
host, per-relay results).

**Not done.** rebuild-repo refuses a repo whose records no longer rebuild
to its head's data root (records lost): such a repo can't be loaded by its
worker, and re-signing the remainder would drop data silently, unlike the
reference, which trusts its records table. The sequencer-recovery scripts
have no counterpart (no single sequencer DB; see "Backups and restore").
`pdsadmin update` is a deploy concern.

## Rolling upgrades and format versioning (design; phase 1 built)

**Before levels:** formats changed outright (`VLSEG05` replaced `VLSEG04`;
VLSEG06 widened shard ids), nothing was migrated, and mixed versions in one
cluster were unsupported. The rolling deploy in the runbook was safe only
because consecutive builds happened to agree on every format. Before the
first production data an SRE needs a contract: deploy one node at a time,
roll back a bad build, and never be able to strand data in a format a
running node can't read.

**Built (phase 1, steps 1-5 of the plan below):** level 1 = the formats
of day one (VLSEG06 etc., `src/version.rs` `LEVELS`; this build runs
`1..=1`); `cluster/version` with the startup gate, the post-lease re-check,
exit 7 `incompatible_level`, per-TTL observation and the raise protocol
(`Cluster::finalize_level`, `vlpds.admin.setFeatureLevel`, `vlpds admin
cluster finalize`); lease and hello fields; the tolerance fixes;
getClusterStatus `version` + the console's Cluster banner and Build
column; the metrics, alerts and runbook procedure; golden fixtures
`testdata/formats/L1/` with their MANIFEST guard. Deviations from the text
below are marked **(built: ...)**. Not built yet: the two-build HA
scenarios and the level-gating test (step 6), `cluster lower`, and
everything under "Later".

### Inventory: what is persisted or on the wire, and how it's versioned

| Format | Where | Version marker | Unknown-version behavior |
|---|---|---|---|
| Log segment | `log/{log_id}/{ord:012}.seg`, `src/segment.rs` | magic `VLSEG06\n` (level 1) is the whole version; `codec` byte (0 none, 1 zstd) | **(built)** `parse_header` accepts every magic of the build's levels (`version::segment_magics`, `SegHeader.level`); others: "bad segment magic", `vlpds_format_errors_total{format="segment"}`, as is an unknown codec. Replay: shard can't open; apply: fail-stop 4; follower: retry loop |
| Fence | same path, `VLFENCE\n` + node id | magic | n/a (stable) |
| Entry muts | inside segments | none: raw SlateDB key/value bytes, plus muts *derived* by the reader from `#commit` frames (`derive_commit_muts`, top bit of `mut_count`) | an old reader writes new-format bytes blindly into state |
| Head `h/` | `state.rs` `Head::encode` | none (fixed binary: cid ‖ cid ‖ rev ‖ block) | `decode` "short head" or garbage |
| Record `R/` | `state::record_value` | none (cid ‖ rev ‖ bytes) | garbage |
| Index `c/ C/ b/ n/ K/` | `state.rs` | key layout only, empty/plain values | key not found |
| MST nodes `M/` | `state.rs`, `mst_lazy.rs` | dag-cbor, content-addressed | stable by construction |
| Account `a/` | `state::Account` JSON | none; tolerant (`#[serde(default)]`, `#[serde(flatten)] extra` keeps unknown fields) | round-trips unknown fields |
| Private `p/` rows | sessions, app passwords, tokens, TOTP (`totp.rs`), OAuth (`oauth/store.rs`), `sec/` revocations/takedowns (`xrpc/server.rs`) | none; mostly JSON | per type; mostly serde-default |
| Shard meta | `meta/applied2` (`nodelog::encode_marker`), `meta/recent` | key-name suffix (`applied2`): the only precedent | **(built)** `decode_marker` of anything but exactly its bytes is an error (the shard doesn't open, `format="applied_marker"`), never "no marker" |
| SlateDB SSTs + manifest | `state/{id}/` | slatedb's own (pinned fork rev, `Cargo.toml`); SST compression from flags | slatedb error at open |
| Node lease | `nodes/{id}`, `cluster::NodeLease` | none; serde ignores unknown fields | **(built)** new fields (`rev`, `min_level`, `max_level`, `seen_level`) default (a lease without them is a level-1 build) |
| Assignment | `assign/{s:03}`, `cluster::Assignment` | none; CAS read-modify-write by *every* node | **(built)** `#[serde(default)]` + `flatten extra`: an old node's CAS (acquire, release, handoff) keeps fields it doesn't know. `Span` (history entries) is closed: a new span field needs a new object |
| Layout | `assign/layout`, `slots::Layout` | `version` = routing generation, not format | **(built)** `flatten extra` on `Layout` and its `op` (`Reshard`); every layout write derives from the one read. `ShardRange` is closed |
| Writer claim | `writers/{w:03}` | none | n/a |
| Retention report | `retain/{log_id}`, `retention::Report` | none | **(built)** `#[serde(default)]` (written by its log's owner only; no `extra`) |
| Global claims | `handle/{h}`, `email/{sha256}` | none (existence + small body) | n/a |
| Blobs | `blob/{did}/{cid}`, `blob-gc/`, `blob-tmp/` | content-addressed | n/a |
| Rate-limit config | `config/ratelimits.json`, `ratelimit/config.rs` `Doc` | `version` = CAS revision; `deny_unknown_fields` on save | **(built)** the stored object is read with `parse_stored`: unknown fields dropped with a warning, the rest applies; the save endpoint stays strict |
| Wrapped secrets | `vw1.{kid}.{b64}` in rows/files (`secrets.rs` `WRAP_VERSION`), AAD `vlpds-secret-v1\0…` | yes, explicit | `Malformed` |
| PLC | rotation key file (`vw1`), DID ops are did-method-plc bytes (`plc/mod.rs`) | external spec | n/a |
| Exit-state file | local, `lifecycle::ExitRecord` | none (local only) | ignored |
| Firehose frames | stored verbatim in segments, served by every node | atproto lexicon (external) | clients' problem; must not flip-flop |
| Log follower ws | `/internal/v1/log/stream`, `remote.rs` | message type byte (0 batch, 1 watermark) | **(built)** skipped (`StreamMsg::Unknown`, `format="log_stream"`, warn) |
| Peer JSON RPC | `/internal/v1/{cluster,cluster/nudge,cluster/hello,private/*,account,oauth/replay,admin/*,sync/*}` (`xrpc/internal.rs`) | path `v1`; serde ignores unknown fields | unknown route = 404; **(built)** admin scatter-gather reports a 404 peer in `unsupportedNodes`, not `unreachableNodes` |
| Cluster version | `cluster/version`, `version::ClusterVersion` | **(built)** the object *is* the version: `{active, target?, history}`, `flatten extra` | unreadable = error (`format="cluster_version"`), never "absent" |
| Forwarded XRPC | `forward.rs`, `x-vlpds-forwarded` | none; error names (`RepoLoading`, `ShardMoved`, `PartitionUnavailable`) are protocol | unknown method on an old owner = 501 |
| Client-held tokens | session JWTs (`auth::Claims`), OAuth tokens/DPoP (`oauth/`) | none | rejected after rollback = forced logout |

Two properties make vlpds stricter than a typical database: **state is a
pure function of the log** (segments carry raw state bytes, and a reader
derives more), so a state encoding change is also a segment change; and
**control objects are read-modify-CAS'd by whichever node acts**, so an
old node silently rewrites a new node's objects.

### Compatibility contract

- **Feature level.** A single integer, cluster-wide, in a CAS'd object
  `cluster/version`: `{active: L, target: L'?, history: [{level, at,
  by}]}`. Each persisted/wire format change gets the next level (a level
  may bundle several changes from one release). A build declares
  `MIN_LEVEL..=MAX_LEVEL`: it reads and writes everything at `MAX_LEVEL`
  and below, and it can still run a cluster whose active level is
  `MIN_LEVEL`. Levels are in a table in `src/version.rs`, each with a
  name, a description and `persistent: bool` (does it put new bytes in
  the bucket, or only gate wire behavior).
- **Readers accept every level in their window; writers emit `active`.**
  Every writer of every format above asks `version::active()` (segment
  sequencer: once per segment, so a segment is homogeneous; derived muts
  are derived at the *segment's* level, not the binary's; control-object
  writers; private-row writers; frame builders; token minting; peer RPC
  senders). A build at `MAX_LEVEL = L+1` running at `active = L` emits
  byte-for-byte what a level-L build emits. That is the rollback window.
- **Raising the level is the point of no return** (Kafka's
  `inter.broker.protocol.version`, CockroachDB's `cluster.preserve_downgrade_option`
  + finalize). It is manual (`vlpds admin cluster finalize`), never
  automatic at startup. Before it, rollback = redeploy the old build;
  after it, rollback = forward-fix. A non-`persistent` level may be
  lowered again (`cluster lower`); a persistent one never.
- **Upgrade one release window at a time.** A new build may start only if
  `active >= its MIN_LEVEL` and `active <= its MAX_LEVEL`. Skipping
  releases is fine whenever that holds; a build only raises `MIN_LEVEL`
  (drops read support for level L) once data at L can no longer exist
  (see "Migrations").
- **Control objects: tolerant both ways.** Every new field is
  `#[serde(default)]` (new build reads old objects); every struct that
  more than one node CASes (`Assignment`, `Layout`, `Report`) gets
  `#[serde(flatten)] extra: Map` like `Account` **(built: `Assignment`,
  `Layout`, `Reshard`, `ClusterVersion`; `Report` is written by one owner
  and only got `default`)**, so an old node's
  read-modify-CAS keeps fields it doesn't know. Semantic fields (an old
  node *must* honor them) are still level-gated: preserving bytes isn't
  understanding them. `deny_unknown_fields` stays only on operator input
  (the rate-limit save endpoint), and the save endpoint rejects fields
  whose level isn't active, so the runtime loader never meets them.
- **Peer protocols: additive or capability-gated.** Paths stay `/v1`; a
  new endpoint or message is used only once the peer's lease advertises a
  `max_level` that has it (or `active` does), and a 404 from a peer reads
  as "unsupported", not "unreachable" (admin scatter-gather today counts
  it as unreachable). The follower stream skips unknown message types
  instead of bailing (fix now, while every reader can be upgraded at
  once). New public XRPC methods may 501 when forwarded to an old owner
  during a deploy; the frontend reports it as `MethodNotImplemented`
  rather than resending.
- **SlateDB is a format.** A slatedb rev bump, and any flag that changes
  stored bytes (`--log-compression` codec, SST compression), is a level:
  a golden DB written by the old rev must open and compact under the new
  one, and the new one's output must open under the old one until the
  level is raised.

### Advertising, refusing, showing

- **Lease.** `NodeLease` gains `rev` (git rev, as in `vlpds_build_info`),
  `min_level`, `max_level`, and `seen_level` (the active level it last
  read). Leases are already read by every peer each step, so every node
  knows the whole cluster's window without a new RPC.
- **Startup gate** (before claiming `writers/`, following logs or taking
  shards): read `cluster/version` (absent = create at `MAX_LEVEL` for a
  fresh prefix, else `1` on legacy prefixes); refuse unless
  `MIN_LEVEL <= active` and `active`/`target` `<= MAX_LEVEL`. **After**
  writing its lease, re-read `cluster/version` and apply the same check.
  Refusal = new fail-stop **exit 7 `incompatible_level`** (lease deleted,
  nothing read), so a supervisor loop on an old image is loud, not harmful.
- **Raise protocol** (`vlpds.admin.setFeatureLevel {level}`, CLI `cluster
  finalize`): (1) CAS `target = L'` onto `cluster/version`; (2) list
  `nodes/*` *after* that write and require every live lease to have
  `max_level >= L'`, else clear `target` and fail with the offending
  nodes; (3) CAS `active = L'`, clear `target`. A node that read the old
  level and wrote its lease after step 2's listing re-reads the object
  after its lease write and sees `target` or the new `active` (store
  writes are linearizable, same argument as "Why safety needs no
  clocks"), so it exits 7 before reading any data. A dead node's stale
  lease is ignored by liveness as today; if it restarts on the old image
  it refuses.
- **Observation.** Every cluster step (and every nudge) re-reads
  `cluster/version` with the lease renewal's cadence (TTL/5); a node
  seeing `active > MAX_LEVEL` fail-stops 7 (can only happen if an
  operator forced it). **(built: once per TTL, in the step, so one GET
  per node per TTL rather than per renewal; a `target` past the node's
  window doesn't stop a running node: the raise lists its lease and
  aborts.)** Writers switch levels at their next segment; mixed
  emission across nodes during the switch is fine because every reader
  already accepts both.
- **Hello.** `HelloIn`/response gain `{rev, min_level, max_level}` so a
  joiner logs a mismatch immediately (informational; the lease is
  authoritative). **(built: both sides log `peer runs a different build`.)**
- **Console / admin.** `getClusterStatus` (`xrpc/admin.rs`, UI
  `ui/src/pages/admin/Cluster.tsx`) shows each node's `rev` and level
  window, the active level and any pending `target`, and a banner: "mixed
  builds", "all nodes can run level L+1: finalize available", "finalized
  at …: rollback no longer possible". **(built: getClusterStatus
  `version.{active,target,history,binary,mixedBuilds,revs,finalizable,
  finalizedAt}` and `nodes[].{rev,minLevel,maxLevel,seenLevel}`; the
  console's Feature level tile, Build column and `LevelsBanner`.)**
- **Metrics / alerts** (ops/alerts.yml): `vlpds_feature_level{kind=
  "active"|"binary_min"|"binary_max"}`; `vlpds_format_errors_total{format}`
  (any decode that fails on an unknown magic/codec/message/value tag);
  keep `VlpdsMixedVersions` (1 h); add `VlpdsFormatErrors` (page: any
  increase), `VlpdsIncompatibleNode` (exit 7 in `vlpds_last_exit_reason_info`),
  and `VlpdsFeatureLevelUnfinalized` (all nodes `binary_max > active` for
  14 d: ticket, so the window doesn't stay open forever and block the
  next `MIN_LEVEL` drop).

### Procedures

**Upgrade (build B, `MAX_LEVEL = L+1`, cluster `active = L`).**
1. Pre-flight: `vlpds admin cluster-status` shows every node healthy and
   `active = L`; B's release notes list its levels and whether they are
   persistent. B's `MIN_LEVEL <= L`.
2. Rolling deploy exactly as ops/RUNBOOK.md "Rolling deploy" (SIGTERM,
   ≥ 60 s stop timeout, same `--node-id`, verify step 4 between nodes),
   plus: the restarted node's lease shows `max_level = L+1` and
   `vlpds_format_errors_total` is flat.
3. Soak with the whole fleet on B at level L (default 24 h). Everything B
   writes is level L: **rollback is a plain redeploy** of the previous
   build, in any order, any time.
4. Finalize: `vlpds admin cluster finalize --level L+1`. Watch format
   errors, commit p99, firehose watermark lag for one TTL; from now on
   rollback is forward-fix only.

**Rollback before finalize.** Redeploy the previous image node by node
with the same procedure. Nothing to clean: no level-(L+1) byte exists.

**Rollback after finalize.** Not possible by redeploy (old builds exit 7).
Ship B' = B + fix. For an emergency where the bug is in the new
level's *writer*: a non-persistent level can be lowered; a persistent one
is a restore question ("Backups and restore": a backup records the
active level and is restored only by a build whose window contains it).

### Migrations where formats change in place

- **Segments: never migrate, roll over.** Readers accept old and new
  magics; writers switch at a segment boundary when the level rises; old
  segments age out with retention (`retention.rs`: `--log-retention`
  window plus replay floors). Read support for an old magic is dropped
  (`MIN_LEVEL` raised) only when no reachable segment can have it:
  `retain/{log_id}` gains `min_seg_format` (lowest magic among the log's
  unpruned segments, known to the writer), and `cluster finalize --min`
  requires every report and every log named in an assignment span to be
  past it. Backups keep old segments; a restore build must still read
  them, so dropping a magic is also gated on backup retention.
- **State key families: dual-read always, lazy write-through, background
  sweep only to drop read support.** Binary values gain a leading tag
  byte where they change: today's `h/` and `R/` values start with a CIDv1
  (`0x01`, `cid.rs`), so `0x80..=0xFF` are free version tags and an
  untagged value is "v0" with no ambiguity; JSON values add an optional
  `"v"` field. A new encoding (level L+1) is written by normal writes
  once active; the decoder reads both forever, until a sweep. The sweep
  (when needed) is a per-shard task on the owner that rewrites a family
  **through the log** as ordinary shard-tagged mutations (so replay,
  handoff, split/merge and backups see one history; rate-limited, resumable
  by a cursor in `meta/migrate`), then sets a shard-wide `meta/format`
  (`{family: version}`) in the same batch as the applied marker. Splits
  and merges copy `meta/format` into the children explicitly (shard-wide
  keys stay with the parent today, `partition.rs` test). `MIN_LEVEL` for
  that family rises once every shard in the layout reports the new
  format (`getClusterStatus` aggregates it). Renaming a key (the
  `meta/applied2` precedent) is the fallback for keys whose value can't
  be tagged; reads then try new, then old.
- **Derived muts.** A level that changes what `derive_commit_muts`
  produces records the derivation in the segment format (new magic), so
  every reader derives the same bytes from the same segment.
- **Control objects: versioned JSON, tolerant.** Additive fields as above;
  a breaking change writes a new object name (`assign/layout2`) read with
  fallback, created at the level raise, old one left for the old window.
- **Secrets.** Already versioned (`vw1`, AAD `vlpds-secret-v1`); a `vw2`
  is read alongside `vw1` and `rewrapSecrets` is its sweep.
- **Tokens.** New claims are optional; a minted token must verify on
  every build in the window, so token changes are levels like any other.

### Tests and CI

- **Golden fixtures per level**: `testdata/formats/L{n}/` with a segment
  (both codecs, derived and stored muts), a fence, `h/ R/ a/ p/` rows,
  `meta/applied2`, every control object, a `retain/` report, the rate-limit
  doc, a `vw1` blob (fixed test KEK), follower-stream messages, a session
  JWT, and a tiny SlateDB directory. `tests/all/formats.rs`: (a) the
  current build decodes every fixture for levels `MIN_LEVEL..=MAX_LEVEL`;
  (b) writing at each level reproduces that level's fixture byte for byte
  (`VLPDS_BLESS=1` regenerates only `MAX_LEVEL`). A CI script fails any
  change to a released level's fixtures (hash manifest
  `testdata/formats/MANIFEST`), and a change to a format writer without a
  new level fails (b). **(built: `tests/all/formats.rs`, whose
  `manifest_freezes_released_levels` is the CI guard, inside `cargo test`.
  L1 holds the segment (stored + derived muts; zstd checked by decode
  only, since zstd's output may change with the library), fence, `h/ R/
  a/` values, the state key layouts, `meta/applied2`, `meta/recent`, every
  control object (lease, assignment, layout with an op, writer claim,
  `retain/` report, rate-limit doc, `cluster/version`), a `vw1` blob under
  a fixed KEK, firehose frames (commit, identity, account, sync, error) and
  both log-stream messages; not yet `p/` rows, a session JWT or a SlateDB
  directory.)**
- **Level-gating test** (in-process, cheap): an `HaCluster` at
  `active = MAX_LEVEL - 1` runs the full write/handoff/split suite and
  asserts every object it wrote matches the previous level's fixture
  encodings (no new-level bytes before finalize).
- **Two-build tests** (`bench/ha/hactl.py`, new scenarios; build the
  previous release tag into `target/prev/` once per CI run):
  `rolling-upgrade` (3 old nodes under loadgen + firehose audit, upgrade
  one by one, finalize; zero lost acks, identical merged firehose on
  every node), `rolling-rollback` (upgrade 2 of 3, roll both back),
  `old-node-refused` (after finalize, an old image exits 7 without
  touching data), and `raise-race` (start an old node concurrently with
  finalize; either the raise aborts or the node exits 7).
- **Release checklist**: the release's level table, fixtures, and the
  previous tag's `rolling-upgrade` run green.

### Implementation plan

**Before the first production data (≈ 9–10 days):** steps 1-5 are built;
6 is TODO.md.
1. **Baseline (0.5 d).** Land the pending breaking changes (VLSEG06 shard
   ids, anything else queued) the old way, then declare **level 1** = the
   formats in production on day one, and record its fixtures.
2. **`src/version.rs` + control object (2 d).** Level table,
   `cluster/version` CAS, lease fields, startup + post-lease checks, exit
   7, per-step observation, `version::active()` plumbed to the writers
   that exist today (segment sequencer, control objects). **(built: the
   segment sequencer writes `version::segment_magic(version::active())`;
   no other writer differs at level 1, so the rest is plumbed when a level
   first changes one.)**
3. **Tolerance fixes (1 d).** `parse_header`/`decode` dispatch on a set of
   magics; follower stream skips unknown message types; `#[serde(default)]`
   on lease/assignment/layout/report fields and `flatten extra` on the
   multi-writer ones; scatter-gather 404 = unsupported; `decode_marker`
   failure becomes an error, not "no marker".
4. **Admin + console + alerts (1.5 d).** `setFeatureLevel` / `cluster
   finalize`, status fields, Cluster page banner and columns, metrics,
   the three alerts, runbook "Upgrade / finalize / rollback" replacing the
   "(unverified)" note under VlpdsMixedVersions.
5. **Golden fixtures + CI guard (1.5 d).**
6. **Two-build HA scenarios (2 d)** on hactl, plus the level-gating test.

**Later, when first needed:**
7. Per-shard `meta/format` + through-the-log sweep framework, tagged value
   decoding (3 d; the first state encoding change after launch).
8. `min_seg_format` in `retain/` and the `--min` finalize check (1 d;
   the first time an old segment magic is dropped).
9. Peer capability gating helper for new internal endpoints/messages
   (0.5 d), token-format levels (as needed), and an optional
   auto-finalize after a configured soak (0.5 d).

## Reference test divergences (`tests/REFERENCE_COVERAGE.md`)

The reference PDS's own test suite is mapped case by case in that file, which lists every deliberate divergence. These are the
notable ones:

- **Deleted accounts in firehose replays.** The reference deletes a deleted DID's earlier `repo_seq` rows. vlpds's firehose is its
  append-only log, so a replay from before the deletion still carries that DID's earlier frames until retention drops the
  segments. The shared guarantee is that `#account` with status `deleted` is the DID's last event, which consumers must treat as
  a tombstone (`ref_sync::account_deletion_is_the_last_event_on_replay`).
- **Writes wait out a signing-key rotation.** Between recording the new key and re-signing the repo with it, the account's writes
  get a retryable 503 `KeyUnavailable`, so no commit is signed with a key the DID document no longer lists. The reference doesn't
  fence them.
- **listRepos order** is `(slot, DID)`, because listings are served per shard, not by creation order.
- **Fresh uploads are readable.** Blobs are written under their final key at upload, so getBlob serves an upload before any record
  references it, until the blob GC collects it. The reference keeps uploads in a temp store.
- **Proxy defaults.** `chat.bsky.*` needs an explicit `atproto-proxy`, and non-`app.bsky`/`tools.ozone` methods are 501. There is
  no separate mod-service default for `tools.ozone.*`.
- **Stricter sessions.** `revokeAppPassword` and `identity.updateHandle` require a full (non-app-password) session.
- **Taken-down accounts can't upload with user service auth** (the reference skips the status check there); see "User service
  auth on uploadBlob".

No known (non-deliberate) gaps are left in that file.
