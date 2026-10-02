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
  - `a/{did}`, `n/{handle}` → account. The account row carries the repo
    signing key only wrapped under the KEK (`Account::wrapped_signing_key`,
    bound to the DID) next to its public key (`signing_pubkey`, which DID
    documents and service-auth checks read without unwrapping); account
    rows in log segments carry the same wrapped form. See "Secrets at rest".
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
| public | PLC, requestCrawl, Cloud KMS (5 s per call) | h2 by ALPN on https, HTTP/1.1 on http with 1,024 idle per host; idle close 60 s; h2 PING 20 s / 10 s; TCP keepalive; connect 5 s, read 30 s |
| proxy | configured AppView / report service | `http://`: hyper HTTP/1.1 connections, one pool per host with a slot per IO thread: a connection goes back to the slot of the thread that finished its body, a request takes from its own slot, else from another slot, else connects; at most 1,024 connections per host (idle + busy; past that a request waits for one, `vlpds_http_client_pool_waits_total`); idle close 60 s, retry once if a reused connection was closed before the request went out; `https://`: public's settings as one client per IO thread. No read timeout: the proxy arms a 10 s head deadline and a 30 s body-idle timer only while the upstream makes it wait. Responses stream through unbuffered; compressed ones as the upstream encoded them (Content-Encoding/-Length kept, never decoded or re-compressed; the client's Accept-Encoding is forwarded); a client that goes away mid-body closes the upstream connection. CORS preflights are answered locally (no auth, no upstream) |
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
  versioned layout (`assign/layout`; `--shards N` uniform ranges, default
  64, when a prefix is created), which splits and merges change online (see "Online
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
  shared tree. getRepo streams from the snapshot (`mst_lazy::export_blocks`:
  the `M/` range read ahead, leaves rebuilt from one forward `R/` scan, one
  path in memory). getBlocks: loaded nodes, `M/` point reads (interior),
  record CIDs as before, then leaves via the repo's `NodeIndex` (built once
  by a streamed walk, advanced by the worker per commit; a miss in an
  index covering the view is final), each found by a
  proof walk to its key. Loaded nodes are kept process-wide by CID
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
| getRepo, 100k records (22.5 MB), 4 clients | 47 /s | 16 /s |

- The RSS rows include the writes' own state, which in these runs lives
  in the in-memory object store and memtables (5x more state bytes per
  commit for lazy), and freed buffers the allocator keeps; the charged
  tree and path bytes are the MST memory proper (the full run's RSS grew
  ~160 B/record, under the 240 B/record charge measured with jemalloc).
- A cold write's fixed reads (head, account, blob refs) set a ~100 ms
  floor in both modes; past it the full mode grows with the repo (a 1M
  repo is 44+ GETs of `R/` and ~1 s of rebuild) and lazy doesn't, up to
  the repos whose `M/` range outgrows the prefetch. The prefetch matters:
  without it a cold write is 7-11 dependent node reads. 512 KiB, 1 MiB and
  4 MiB caps measured the same up to 100k records; a 1M-record repo's 28 MB
  range isn't worth reading ahead (its path's point reads cost as much);
  1 MiB is the default. Production reads hit the NVMe disk cache first.
  (GET counts per write were too noisy here to quote: background
  compaction and polls share the state client.)
- getRepo is ~3x slower than walking a resident full tree: leaves are
  rebuilt (key hashes, encode, CID) from a forward `R/` scan, and the
  records need a second scan (the CAR puts every node before any record,
  as the full mode does: the bytes are identical). The full mode only gets
  its speed for repos it already holds in memory.
- Leaf getBlocks is an index lookup plus a proof walk to the leaf's key.
- **Stage 5 decision.** At 10× today's load (3.3k commits/s average, ~9k
  bursts) the extra state writes are ~10 MB/s cluster-wide (~30 MB/s in
  bursts) into memtables, and the extra stored segment bytes ~1.3 MB/s:
  nothing near a limit, so the checkpoint-window write-back stays a design.
  At the 100× planet-scale target (~200k commits/s) it would be ~0.6 GB/s
  of extra memtable writes and should be built then.

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
alive (~58 GB/day × lifetime). vlpds today creates none of its own: the
only checkpoints are the compactor's 1 h read guards (§4). A clone
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
operation and admin `sendEmail` mail go out over SMTP (`src/mail.rs`, lettre
over rustls) when configured:

| Flag | Env | Reference PDS env (also read) |
|---|---|---|
| `--email-smtp-url` (alias `--smtp-url`) | `VLPDS_EMAIL_SMTP_URL` | `PDS_EMAIL_SMTP_URL` |
| `--email-from-address` (alias `--email-from`) | `VLPDS_EMAIL_FROM_ADDRESS` | `PDS_EMAIL_FROM_ADDRESS` |

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
stops (the user asks again). The reference's separate moderation mailer
(`PDS_MODERATION_EMAIL_*`) is not split out: admin mail uses the same one.

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
| PLC rotation key | none | the PDS holds none (DIDs minted locally; `getRecommendedDidCredentials` recommends none) |
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
  `signing_pubkey` (`purpose="key_load"`). Session JWTs are HMACs; nothing
  here signs PLC operations.

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
