---
title: Overview
section: vlPDS
order: 1
summary: An atproto PDS whose only durable storage is an object store. Any node serves any request, one node can run a personal server for free, and a few nodes can carry all of Bluesky's write load.
---

```hero
diagram:
  caption: Clients reach any node through Caddy. A node owns some shards, forwards the rest to their owners, and keeps everything durable in the bucket. The local disk is only a cache.
  nodes:
    - { id: apps, label: Apps, sub: XRPC · OAuth, at: [0, 1], size: [7, 3] }
    - { id: relays, label: Relays, sub: firehose consumers, at: [0, 10], size: [7, 3] }
    - { id: caddy, label: Caddy, sub: TLS · handle certs, at: [10, 5.5], size: [7, 3] }
    - { id: n1, label: vlpds node 1, sub: shards 0–21 · log 1, at: [21, 1], size: [9, 3], tone: accent }
    - { id: n2, label: vlpds node 2, sub: shards 22–42 · log 2, at: [21, 5.5], size: [9, 3], tone: accent }
    - { id: n3, label: vlpds node 3, sub: shards 43–63 · log 3, at: [21, 10], size: [9, 3], tone: accent }
    - { id: log, label: "`log/`", sub: segments · WAL + firehose, at: [35, 0.5], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`state/{shard}/`", sub: SlateDB per shard, at: [35, 4], size: [10, 2.6], shape: store, tone: amber }
    - { id: ctl, label: "`nodes/` `assign/`", sub: leases · ownership, at: [35, 7.5], size: [10, 2.6], shape: store, tone: amber }
    - { id: blob, label: "`blob/`", sub: images · video, at: [35, 11], size: [10, 2.6], shape: store, tone: amber }
    - { id: appview, label: AppView, sub: proxied app reads, at: [10, 16], size: [7, 2.6], tone: muted }
    - { id: plc, label: PLC directory, sub: DID documents, at: [21, 16], size: [9, 2.6], tone: muted }
    - { id: kms, label: Cloud KMS, sub: optional KEK, at: [35, 16], size: [10, 2.6], tone: muted }
  groups:
    - { label: vlpds cluster, around: [n1, n2, n3], tone: accent }
    - { label: object store · the only durable state, around: [log, state, ctl, blob], tone: amber }
  edges:
    - "apps.r -> caddy.l30: HTTPS"
    - "caddy.l70 -> relays.r: subscribeRepos"
    - caddy.r -> n1.l
    - "caddy.r -> n2.l: any node"
    - caddy.r -> n3.l
    - "n1 <-> n2: forward to owner"
    - n2 <-> n3
    - "n1.r30 -> log.l: append, then ack"
    - { from: n1.r70, to: state.l, label: apply · read }
    - { from: n2.r, to: ctl.l, label: lease CAS, dash: true }
    - { from: n3.r, to: blob.l, label: blobs }
    - { from: n3.b15, to: appview.t, label: app.bsky.* }
    - { from: n3.b50, to: plc.t50, label: identity ops }
    - { from: n3.b85, to: kms.t, label: unwrap keys, dash: true }
facts:
  - { value: "1", unit: bucket, label: is the whole database, note: "log, state, leases and blobs live in S3 / R2 / GCS" }
  - { value: "~60k", unit: commits/s, label: per 16-core node, note: "measured; Bluesky averages ~350/s today", tone: amber }
  - { value: "$0", unit: /mo, label: object store for a personal PDS, note: "on R2's free tier; ~$2–4 on S3", tone: blue }
  - { value: "~12 s", label: to notice a crashed node, note: "then fence, replay, serve; a planned handoff is ~0.2 s", tone: violet }
```

vlpds is a Rust [atproto](https://atproto.com) personal data server (PDS) built so that **the object store is the database**. Every
acknowledged write is already in the bucket, nodes keep only caches, and adding capacity means adding
nodes that point at the same bucket. It speaks the same XRPC, OAuth and sync 1.1 firehose as the
reference PDS, so apps, relays and AppViews can't tell the difference.

This page is the one-screen tour. Each section links to the page that covers it in depth.

## The shape of the system

```diagram
caption: The write path. A commit is acknowledged only after the segment holding it, and every earlier one, is in the object store.
nodes:
  - { id: req, label: createRecord, sub: any node, at: [0, 0], size: [7, 3] }
  - { id: worker, label: Repo worker, sub: MST path · sign commit, at: [10, 0], size: [8, 3], tone: accent }
  - { id: seq, label: Log writer, sub: group commit, at: [21, 0], size: [8, 3], tone: accent }
  - { id: seg, label: "`log/…/{ordinal}.seg`", sub: If-None-Match PUT, at: [32, 0], size: [9, 3], shape: store, tone: amber }
  - { id: slate, label: SlateDB memtable, sub: apply for reads, at: [32, 6.5], size: [9, 2.6] }
  - { id: ack, label: HTTP 200, sub: cid · rev, at: [21, 6.5], size: [8, 2.6], tone: solid }
  - { id: fh, label: Firehose, sub: merged across nodes, at: [10, 6.5], size: [8, 2.6], tone: blue }
edges:
  - req -> worker
  - "worker -> seq: commit"
  - "seq -> seg: segment"
  - "seg -> slate: durable"
  - "slate -> ack: then ack"
  - { from: seg.b10, to: fh.t, label: sealed segments, tone: blue }
```

- **Repo workers** hold each active repo's MST paths in memory, apply the operations and sign the
  commit. They don't wait for the previous commit to be durable, so one repo can take
  hundreds of commits a second.
- **The log** batches every shard's commits on a node into segments: whatever queued while the
  previous PUT was in flight (up to 8 MiB) becomes the next one, PUT with `If-None-Match: *`. The log is the write-ahead log *and* the firehose: there is
  no second copy.
- **State** (records, repo heads, accounts, MST interior nodes) lives in one SlateDB per shard with its own WAL
  turned off. A durable segment is applied to the memtable before the ack, so a read right after
  a write sees it; SlateDB flushes SSTs to the bucket on its own schedule.
- **The firehose** on every node merges every node's log and emits an event once every log's
  durable watermark has passed it, so the order is the same on every node.

Details: [Architecture](architecture.md), [Record storage](record-storage.md),
[State storage](state-storage.md), [Firehose](firehose.md).

## Shards, leases and ownership

```diagram
caption: 65,536 fixed hash slots grouped into shards (64 by default). Each shard has exactly one owner, recorded in `assign/{shard}`; each node holds one lease in `nodes/{node}`. Nothing else coordinates the nodes.
nodes:
  - { id: did, label: "did:plc:…", sub: sha256 → slot, at: [0, 2], size: [7, 3] }
  - { id: slots, label: "65,536 slots", sub: permanent, at: [10, 2], size: [8, 3], tone: muted }
  - { id: s0, label: shard 0, at: [21, 0], size: [6, 2], tone: accent }
  - { id: s1, label: shard 1, at: [21, 2.5], size: [6, 2], tone: accent }
  - { id: s63, label: shard 63, at: [21, 5], size: [6, 2], tone: blue }
  - { id: na, label: node A, sub: "lease `nodes/a`", at: [31, 0.5], size: [8, 3], tone: accent }
  - { id: nb, label: node B, sub: "lease `nodes/b`", at: [31, 4.5], size: [8, 3], tone: blue }
edges:
  - did -> slots
  - slots.r -> s0.l
  - slots.r -> s1.l
  - "slots.r -> s63.l"
  - "s0.r -> na.l40: owner"
  - s1.r -> na.l70
  - "s63.r -> nb.l: owner"
notes:
  - { at: [21, 8.6], text: "split / merge online" }
```

A DID hashes to one of 65,536 slots, forever. Slots are grouped into contiguous **shards**; a shard is
the unit of ownership and of state (one SlateDB each), and shards can be split or merged online.
Each node takes up to its fair share of shards (shards ÷ live nodes), renewing **one lease** for
itself rather than one per shard. Leases and assignments are compare-and-swap writes on objects in
the bucket: no ZooKeeper, no Raft, no quorum. Two nodes are enough for high availability.

Any node accepts any request. Writes and repo reads for a shard it doesn't own are forwarded to
the owner over HTTP/2 with mutual TLS; app reads are proxied to the AppView from the account's
owner. See [Architecture](architecture.md#shards-and-ownership) and
[Proxying](proxying.md).

## How big it gets

```facts
- { value: "~60k", unit: commits/s, label: one 16-core node, note: "measured with 25 ms injected store latency; ~90k with none", tone: amber }
- { value: "~300k", unit: req/s, label: AppView proxying per node, note: "about 50 µs of CPU per request" }
- { value: "100M", unit: accounts, label: bulk-created in a test cluster, note: "4 nodes on one MinIO", tone: blue }
- { value: "~$1.7k", unit: /mo, label: "S3 bill for all of Bluesky's writes", note: "modeled; 3 nodes · 64 shards · in-region", tone: violet }
```

These are round numbers from the benchmark campaigns (`bench/results/`). The object-store bill at
Bluesky's load is modeled from measured request rates (`bench/results/cost-model-2026-10-02`). What
they mean in practice:

| | Personal | Bluesky today |
|---|---|---|
| Accounts | a handful | 56 M repos, 24 B records |
| Commits/s | a few a day | ~350 avg, ~900 bursts |
| Nodes | 1 small VM (`tiny` profile) | 3 × 6–8 cores, 32 GB, NVMe |
| Busy cores, fleet-wide | ~0 | ~3 |
| Object store requests | $0 on R2, ~$2–4 on S3 | ~$1.7k/mo (S3), ~$1.5k (R2), modeled |

A few things to know about where the costs come from:

- **Logins and proxying use the CPU, not commits.** A commit costs ~100 µs of CPU end to end, while an
  Argon2 login costs ~20 ms.
- **The object-store bill follows shard and node count, not write rate.** A node PUTs about one segment
  per store round trip whenever anything is queued (~27/s), whether it holds 300 or 20,000 commits.
  Per-shard polling and checkpoints are fixed costs too, which is why the default is 64 shards and
  a personal server runs one.
- **Memory follows active repos, not total repos.** Only the MST paths that recent writes visited stay in
  memory (~10–20 KB per active repo), so 32 GB nodes hold a day's writers at Bluesky's scale.

Details: [Scaling and clustering](operations/scaling-and-clustering.md),
[Configuration](operations/configuration.md).

## Design philosophy

- **One source of truth.** The bucket holds everything durable: a lost node or a lost disk costs
  cache, never data. A new host needs only the bucket's credentials and its secrets.
- **Group commit, not per-write objects.** One PUT per commit would cost ~$43k a day at 100k
  commits/s. vlpds batches every repo's commits on a node into one segment, so request cost follows nodes,
  not traffic.
- **Pipeline, don't wait.** A repo's next commit builds on the in-memory head while the previous one
  is still uploading, and acks go out in log order. Durability latency doesn't limit a single repo's
  throughput.
- **The log is the WAL and the firehose.** A write is stored once. Recovery replays the same
  segments that relays receive.
- **Derive what you can.** MST leaves are rebuilt from records rather than stored, and record values
  are rebuilt from a commit's CAR at replay. Less to write and less that can disagree.
- **Fail-stop over guessing.** When a node can't be sure it's still allowed to write, it exits and
  lets its supervisor restart it. Unavailability is recoverable; a forked repo is not.
- **Boring dependencies.** S3-compatible storage, Caddy in front, Prometheus metrics, and a single
  static binary beside its built web UI.

## Robustness

```steps
- title: A node stops renewing its lease
  body: It crashed, lost the network, or fail-stopped on purpose. Peers notice when its lease object hasn't changed for 1.2 × TTL (12 s by default), or within a few seconds if its port refuses connections.
- title: A peer fences the dead node's log
  body: A conditional create of a fence object at the end of the log's durable prefix. From then on, nothing the old process might still be doing can append to that log.
- title: The new owner replays and serves
  body: It takes the shards by compare-and-swap on `assign/{shard}`, replays the dead log's tail for those shards from the bucket, waits out the old owner's last sequence number, and starts serving.
```

- **Acknowledged means durable.** A write is acked only after its segment and every earlier one are in
  the object store, and only while the node's lease is valid. Unacked work that was still in memory
  when a node died was never confirmed to anyone or sent on the firehose, so discarding it is safe.
- **Safety needs no clocks.** Fencing and compare-and-swap decide who may write; lease timing only decides
  *when* a takeover happens. A wrong "it's dead" guess costs availability, never an acked write.
- **Fail-stop is the safety valve.** A segment PUT that can't succeed, a lease renewal that takes too
  long, or a panic in a critical thread makes the process exit with a specific code. The supervisor
  restarts it and it rejoins.
- **Planned moves are fast.** A graceful shutdown or rebalance warms the recipient's caches, writes one
  barrier segment and hands shards over in ~0.2 s each, so rolling deploys cost almost no errors.
- **Retention never outruns replay.** Log segments are kept for 72 h for firehose backfill, and never deleted
  while any shard could still need them.

Details: [Architecture](architecture.md#failure-and-takeover),
[Runbook](operations/runbook.md), [Backups and recovery](operations/backups-and-recovery.md).

## One node or many

```diagram
caption: The same binary and bucket layout either way. A single node owns every shard; a cluster spreads them, and shards move on their own as nodes join and leave.
nodes:
  - { id: one, label: one node, sub: owns all shards, at: [0, 1], size: [8, 3], tone: accent }
  - { id: b1, label: bucket, at: [11, 1], size: [7, 3], shape: store, tone: amber }
  - { id: c1, label: node 1, at: [24, 0], size: [6, 2.2], tone: accent }
  - { id: c2, label: node 2, at: [24, 2.6], size: [6, 2.2], tone: accent }
  - { id: c3, label: node 3, at: [24, 5.2], size: [6, 2.2], tone: accent }
  - { id: b2, label: bucket, at: [34, 2.2], size: [7, 3], shape: store, tone: amber }
groups:
  - { label: personal · tiny profile, around: [one, b1], tone: muted }
  - { label: cluster · standard profile, around: [c1, c2, c3, b2], tone: muted }
edges:
  - one <-> b1
  - c1.r -> b2.l
  - c2.r -> b2.l
  - c3.r -> b2.l
```

| | Single node (`tiny`) | Cluster (`standard`) |
|---|---|---|
| Shards | 1 | 64, split or merged online |
| Lease TTL | 60 s (a crash restart waits ~1 TTL) | 10 s (takeover in ~12 s) |
| Good for | a personal or small community PDS | many accounts, high availability |
| Grows by | adding a second node with the same bucket and prefix | adding nodes; shards rebalance by themselves |

Going from one node to several needs no migration: start another node on the same bucket and prefix, and
it joins, follows the others' logs and takes its share of shards. See
[Deploy](operations/deploy.md) and [Scaling and clustering](operations/scaling-and-clustering.md).

## Where to go next

- Running a server: start at [Operations](operations/index.md), then [Deploy](operations/deploy.md).
- Moving an account here: [Migration](migration.md).
- How identity and keys are protected: [Keys and security](keys-security.md), [OAuth and 2FA](oauth-2fa.md).
- The full design log, with every measurement and rejected alternative, stays in `DESIGN.md` in the
  repository. These pages are the current, curated view.
