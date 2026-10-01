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
  inversion proof, one k256 signature (25 µs measured). Estimated
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
  (an Arc snapshot gives consistent exports while writes continue).
- Escape hatch for very large repos (≥1M records): periodically write an MST snapshot
  object so a cold load doesn't need an O(n) rebuild. Not needed for v1.

Persisted state per commit drops from ~15 KV operations to about 2: the record and
the repo head.

### 3. Log = WAL = firehose
> **Superseded by the HA section below:** the log is per *partition*, each with
> exactly one segment PUT in flight (adaptive batching: whatever queues during
> a PUT forms the next segment). Throughput comes from P parallel logs.

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
- Retention: e.g. 72 h (firehose backfill window), never deleting past state's
  durable `applied_seq`.

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
  - `a/{did}`, `n/{handle}` → account; `k/{did}` → signing key (plaintext in the
    prototype; KMS-wrapped later)
  - `meta/applied_seq`
- Reads (`getRecord`, `listRecords`, `describeRepo`) → SlateDB (memtable → block
  cache → local disk cache → S3).

### 5. Firehose
- Live: after durability, frames go to a broadcast ring (`Arc<Bytes>`, zero-copy
  fan-out). Slow consumers fall off and must resume from a cursor.
- Backfill: recent segments come from memory; older ones are range-GETs from S3.
- Events: `#commit` (sync 1.1), `#sync` (account creation / repo reset),
  `#identity`, `#account`.

### 6. Blobs
`uploadBlob` streams to `blob/{did}/{cid}` (multipart if large). This is off the
commit hot path.

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

All nodes serve reads and writes; write *ownership* is partitioned.

- **Partitions.** `partition = hash(did) % P` (P ≈ 16–64, fixed at bucket
  creation). A partition is the unit of ownership and owns: a segment log
  `log/{p:03}/{ordinal:012}.seg`, a SlateDB at `state/{p:03}/`, and a lease
  object `lease/{p:03}`.
- **Leases** are S3 objects updated by compare-and-swap (`PutMode::Update` with
  the ETag): `{owner, addr, epoch, expires_at}`. Owners renew every ~2 s with a
  ~10 s TTL. A node acquires expired/unowned partitions up to its fair share
  (P ÷ live nodes); graceful shutdown releases leases.
- **Routing.** Every node polls lease objects (~1 s) into a routing table. A
  request for a DID owned elsewhere is proxied to the owner. Reads are proxied
  too (read-your-writes); stale-tolerant reads could later use a SlateDB
  `DbReader` locally.
- **Log fencing, no holes.** Each partition has exactly one segment PUT in
  flight, written with `If-None-Match: *` to the next ordinal. The log is a
  dense chain, and a zombie owner always collides on the next name. Batching
  becomes adaptive: whatever queues during one PUT goes in the next (latency ≈
  1–2 PUTs). Throughput comes from P parallel logs.
- **Takeover safety.** An owner stops acking once `now > lease_expiry − margin`.
  A new owner waits for `expiry + skew margin`, opens the partition's SlateDB
  (SlateDB's writer epoch fences the old writer a second time), replays the
  log tail after `applied_seq`, and only then accepts writes. If its first PUT
  collides (`AlreadyExists`), the zombie got a segment in; it re-replays.
- **Global firehose order with no global sequencer.**
  `seq = unix_micros × 256 + partition`, strictly increasing within a
  partition, so seqs are unique but not dense (atproto allows gaps). Each
  owner publishes a **watermark** `W_p` (every event with seq ≤ W_p is durable),
  capped at its lease expiry so a successor's seqs always exceed it. Every
  node serves `subscribeRepos` by k-way merging partition streams in seq order,
  emitting an event once `seq ≤ min_p W_p`. Live data comes from owners over an
  internal stream (segments + watermark heartbeats); history and catch-up come
  from S3 segments. The merge is deterministic, so cursors replay identically
  on any node.
- **Global uniqueness:** handles are claimed with a conditional PUT of
  `handle/{handle}`.

Single-node mode is the same code with one node owning all partitions.

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
     subscriptions (`?shard=k/n` by DID hash) for consumers that can't take the
     full ~750 MB/s.
7. **Global indexes at 5 B scale.**
   - Handles: S3 objects for uniqueness, plus a cache.
   - listRepos: scatter-gather over shards, plus periodic per-shard repo-list
     snapshots so relays can backfill without scanning 5 B heads live.
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
  100×.

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
