---
title: The path of a commit
section: vlPDS
order: 2.5
status: ready
summary: "One write from request to firehose: what each stage costs, when it's durable, when the 200 goes out, and what a crash at each point leaves behind."
---

```hero
timeline:
  caption: "One createRecord sent to a node that doesn't own the repo. Times are for in-region S3, with the PUT modeled at ~30 ms. On R2 the PUT alone is ~255 ms. The 200 always comes after durable. Spacing isn't to scale."
  scale: 44
  lanes:
    - { id: client, label: Client, sub: app or SDK, tone: ink }
    - { id: entry, label: Entry node, sub: any node, tone: ink }
    - { id: worker, label: Repo worker, sub: on the owner, tone: accent }
    - { id: log, label: Node log, sub: sequencer · finalizer, tone: accent }
    - { id: store, label: Object store, sub: "`log/` segments", tone: amber }
    - { id: mem, label: Shard memtable, sub: "SlateDB, WAL off", tone: accent }
    - { id: fh, label: Firehose, sub: merger, tone: blue }
  spans:
    - { lane: client, from: 0, to: 13.6, label: waiting for the 200, tone: muted, dash: true }
    - { lane: entry, from: 0.7, to: 2.6, label: forward, tone: ink }
    - { lane: worker, from: 2.3, to: 4.4, label: build · sign, dur: "~44 µs CPU" }
    - { lane: log, from: 4.1, to: 5.9, label: batch, dur: "0–1 PUT wait" }
    - { lane: store, from: 6.0, to: 9.8, label: segment PUT, dur: "~30 ms · R2 ~255 ms", tone: amber }
    - { lane: log, from: 10.1, to: 12.4, label: finalize, dur: in log order }
    - { lane: mem, from: 10.2, to: 11.6, label: apply, dur: "<1 ms" }
    - { lane: fh, from: 11.2, to: 14.4, label: merge · every 2 ms, tone: blue }
  arrows:
    - { from: client, to: entry, at: 0.3, label: createRecord }
    - { from: entry, to: worker, at: 2.4, label: mTLS }
    - { from: worker, to: log, at: 4.2, label: commit }
    - { from: log, to: store, at: 6.1, label: If-None-Match }
    - { from: store, to: log, at: 9.95 }
    - { from: log, to: mem, at: 10.4, label: batch }
    - { from: log, to: fh, at: 11.9, side: left, label: events, tone: blue }
    - { from: log, to: entry, at: 12.6, side: left, label: ack }
    - { from: entry, to: client, at: 13.2, side: left, label: "200 · cid · rev", tone: accent }
  marks:
    - { at: 9.8, label: durable, tone: amber }
    - { at: 13.2, label: 200 sent }
    - { at: 14.4, label: on the firehose, tone: blue }
  ticks:
    - { at: 0, label: "0" }
    - { at: 4.2, label: "~1 ms" }
    - { at: 9.8, label: "~30–50 ms" }
    - { at: 13.2, label: "+~1 ms" }
facts:
  - { value: "1", unit: PUT, label: makes a write durable, note: "the segment holding it, once every earlier segment is in too", tone: amber }
  - { value: "~40–50 ms", label: to the 200 on in-region S3, note: "p50 · ~150 ms p99 (modeled)" }
  - { value: "~⅓ s", label: to the 200 on R2, note: "segment PUTs ~300 ms p50 on a small node (measured)", tone: amber }
  - { value: "0–4 ms", label: from the 200 to the firehose, note: "one node (measured) · ~20 ms more through a 3-node merge", tone: blue }
  - { value: "~10 s", label: of log replayed after a crash, note: "from the shard's last checkpoint (`--checkpoint-every`)", tone: violet }
```

A write is durable before its 200. The 200 goes out only once the segment holding the commit, and
every segment before it, is in the bucket. Each node has one log, and that log is the write-ahead
log for every shard the node owns, so there's no WAL per shard. If a node dies, the next owner of
its shards replays that log.

Measured numbers come from `bench/results/` and say which store they ran on. Modeled numbers come
from the latency model in `bench/results/cost-model-2026-10-02`.

## The stages

| Stage | Where | Cost | Source |
|---|---|---|---|
| Forward to the owner | entry node → owner, HTTP/2 over mTLS | a forwarded createRecord is ~1.1 ms p50, ~1.7 ms p99 end to end | measured, laptop, in-memory store (`bench/results/peer-mtls-2026-10-02`) |
| Build and sign | repo worker thread | ~44 µs of CPU per commit (MST path, sign, verify, CAR, frame) | measured, `worker::bench_commit_cpu` on a 16-core Linux box |
| Wait for a segment | sequencer | 0 if no PUT is in flight, otherwise until the current one lands (sooner if it stalls) | from the code (`src/nodelog.rs`) |
| Compress | commit pool | ~5 µs per commit, a few ms per full segment | measured (`DESIGN.md` "Log compression") |
| Segment PUT | object store | R2 ~255 ms p50 / ~500 ms p99 (probe) · S3 ~30 ms median (model input) · hedged after 100 ms | measured on R2, modeled on S3 ([Object store](operations/object-store.md#latency-and-failure)) |
| Apply | shard memtables | ~0.2–0.9 ms p50 per segment at 50–75k commits/s | measured, laptop MinIO (`bench/results/2026-10-02`) |
| Ack | finalizer → HTTP | lease check, then every write in the segment gets its 200 | from the code |

End to end, an ack takes ~40–50 ms p50 and ~150 ms p99 on in-region S3 (modeled) and about a third
of a second on R2. With 25 ms injected into every segment PUT, one node acked at ~52 ms p50 /
~129 ms p99 at 10k writes/s (measured on MinIO). `vlpds_commit_durable_seconds` shows it live, and
`vlpds_commit_stage_seconds{stage}` splits it into `seal_wait`, `put`, `apply_lock`, `apply` and
`ack`.

## Many repos, one PUT

```diagram
caption: "Group commit. Whatever queued while the last PUT was in flight goes out as the next segment, whichever repos and shards it touches. One log is the WAL for every shard on the node."
nodes:
  - { id: a, label: repo A, sub: shard 3, at: [0, 0], size: [7, 2.4], tone: accent }
  - { id: b, label: repo B, sub: shard 17, at: [0, 3], size: [7, 2.4], tone: accent }
  - { id: c, label: repo C, sub: shard 42, at: [0, 6], size: [7, 2.4], tone: accent }
  - { id: s, label: Sequencer, sub: assigns seqs, at: [11, 3], size: [8, 2.4], tone: accent }
  - { id: seg, label: "one segment", sub: "A · B · C · …", at: [23, 2.8], size: [9, 2.8], shape: store, tone: amber }
  - { id: m3, label: shard 3, sub: memtable, at: [36, 0], size: [7, 2.4] }
  - { id: m17, label: shard 17, sub: memtable, at: [36, 3], size: [7, 2.4] }
  - { id: m42, label: shard 42, sub: memtable, at: [36, 6], size: [7, 2.4] }
edges:
  - a.r -> s.l30
  - b.r -> s.l
  - c.r -> s.l70
  - "s -> seg: 1 PUT"
  - seg.r -> m3.l
  - "seg.r -> m17.l: apply"
  - seg.r -> m42.l
```

A node PUTs about one segment per store round trip whether it holds 300 commits or 20,000 (up to
8 MiB, `--max-segment-mb`). Under load or when a PUT stalls, up to 4 segments are in flight at once
(`--log-inflight`).

## Pipelining, and why acks still wait

```timeline
caption: "Commit N+1 is built on N's in-memory head while N uploads. Here N+1's segment lands first, but nothing is acked until segment 7 is in: then 7 and 8 are durable and the acks go out in log order."
scale: 50
lanes:
  - { id: w, label: Repo worker, sub: one repo, tone: accent }
  - { id: p1, label: PUT slot 1, tone: amber }
  - { id: p2, label: PUT slot 2, tone: amber }
  - { id: f, label: Finalizer, tone: accent }
  - { id: c, label: Client, tone: ink }
spans:
  - { lane: w, from: 0, to: 1.6, label: build N }
  - { lane: w, from: 2.2, to: 4.2, label: build N+1 }
  - { lane: p1, from: 1.8, to: 8, label: segment 7 holds N, tone: amber }
  - { lane: p2, from: 4.4, to: 6.6, label: segment 8, dur: holds N+1, tone: amber }
  - { lane: f, from: 8.2, to: 10.4, label: apply 7 · 8 }
events:
  - { lane: f, at: 6.6, label: "8 is in, waits for 7", tone: muted }
  - { lane: c, at: 10.6, label: ack N, tone: solid }
  - { lane: c, at: 12, label: ack N+1, tone: solid }
marks:
  - { at: 8, label: 7 and 8 durable, tone: amber }
```

`swapCommit` and `swapRecord` compare against the in-memory head, pending commits included. That's
safe because the log keeps their order. So the store's latency adds to each write's latency but
doesn't cap how fast one repo can write (hundreds of commits a second).

## When it's on the firehose

```timeline
caption: "After the finalizer applies a segment, it hands the events to the merger, then acks. The merger emits up to the lowest watermark across every node's log, so in a cluster an event waits for the slowest log's heartbeat."
scale: 46
lanes:
  - { id: c, label: Client, tone: ink }
  - { id: own, label: Owner node, sub: its log, tone: accent }
  - { id: m1, label: Owner's firehose, sub: one node, tone: blue }
  - { id: peer, label: Peer's firehose, sub: cluster, tone: blue }
spans:
  - { lane: own, from: 0, to: 2, label: apply, tone: accent }
  - { lane: m1, from: 2.4, to: 5.6, label: next cut, tone: blue }
  - { lane: peer, from: 2.4, to: 9.4, label: wait for the lowest watermark, tone: blue }
arrows:
  - { from: own, to: m1, at: 2.2, label: events }
  - { from: own, to: c, at: [3, 3.6], side: left, label: "200", tone: accent }
marks:
  - { at: 0, label: durable, tone: amber }
  - { at: 3.6, label: 200 sent }
  - { at: 5.6, label: on this node's firehose, tone: blue }
  - { at: 9.4, label: on every node, tone: blue }
ticks:
  - { at: 0, label: "0" }
  - { at: 3.6, label: "~1 ms" }
  - { at: 5.6, label: "+0–4 ms" }
  - { at: 9.4, label: "~5–20 ms" }
```

- An event is never on the firehose before it's durable. The finalizer only hands over segments
  that are in the bucket, in ordinal order.
- On one node, firehose delivery lands 0–4 ms after the ack (measured, laptop). The merger cuts
  every 2 ms, so an event can reach a subscriber just before the 200 reaches the client, or just
  after.
- In a cluster, every node merges every log and emits only up to the lowest watermark. Peers send
  a heartbeat every 5 ms. At light load a write reaches a peer's firehose ~5 ms p50 after the
  client sent it (timed from the write at its owner, laptop, in-memory store, where the PUT costs
  almost nothing), and the merge adds ~20 ms p50 with three busy nodes (laptop MinIO). Details: [Firehose](firehose.md#the-merger).

## A crash at each point

```timeline
caption: "Where a crash can land. Each number matches a row of the table below."
scale: 44
lanes:
  - { id: c, label: Client, tone: ink }
  - { id: n, label: Owner node, tone: accent }
  - { id: s, label: "`log/`", sub: segments, tone: amber }
  - { id: m, label: Memtable, tone: accent }
  - { id: st, label: "`state/`", sub: SSTs, tone: amber }
spans:
  - { lane: n, from: 0, to: 2.3, label: build · batch }
  - { lane: s, from: 2.5, to: 5, label: segment PUT, tone: amber }
  - { lane: n, from: 5.2, to: 7.2, label: apply · ack }
  - { lane: m, from: 5.6, to: 13, label: in memory only, dur: "until the next checkpoint (~10 s)", dash: true }
  - { lane: st, from: 13.2, to: 15.6, label: SST flush, tone: amber }
arrows:
  - { from: n, to: c, at: 7, label: "200", tone: accent }
marks:
  - { at: 3.6, label: "1", tone: danger }
  - { at: 6.1, label: "2", tone: danger }
  - { at: 12.2, label: "3", tone: danger }
  - { at: 5, label: durable, tone: amber }
```

| # | Crash | The client saw | In the bucket | What recovery does |
|---|---|---|---|---|
| 1 | before the segment is durable | no 200. A forwarded write gets 503 `PartitionUnavailable`, a direct one a dropped connection. | Nothing, or a segment past the dead log's first hole | Nothing to replay. The write was never acked or emitted, so the client retries it. If its PUT did land inside the durable prefix, it's handled like row 2. |
| 2 | durable, before the 200 | the same as row 1 | the segment, inside the durable prefix | The next owner fences the log after it and replays it. The write is kept and peers emit it while they drain the log to the fence. A blind retry is a second write: a create without an `rkey` makes a second record. Send `swapCommit` or a fixed `rkey` to make a retry fail instead. |
| 3 | after the 200, before the SST flush | the 200 | the segment · the shard's last checkpoint is older | The next owner opens the shard's SlateDB, reads `meta/applied2` and replays every segment after it (~10 s of log). The write comes back. It was already on the firehose, and replay doesn't emit it again. |
| 4 | a takeover while the old process still runs | 503s and resends until the shard moves | the old log, closed by a fence | See below. The old process can't ack anything at or past the fence. |

## Takeover and the fence

```timeline
caption: "A node stops renewing at 0 s (10 s TTL). It stops acking on its own at 8 s. Peers presume it dead at 12 s, fence its log at the first hole and replay. If it wakes up and PUTs at the fenced ordinal, the PUT collides and it exits."
scale: 40
lanes:
  - { id: old, label: Old node, sub: paused or cut off, tone: accent }
  - { id: b, label: Bucket, sub: "`log/` · `assign/`", tone: amber }
  - { id: p, label: New owner, sub: a peer, tone: accent }
  - { id: e, label: Entry nodes, sub: writes to its shards, tone: ink }
spans:
  - { lane: old, from: 0, to: 8, label: lease still valid }
  - { lane: old, from: 8, to: 15, label: no PUTs · no acks, tone: danger, dash: true }
  - { lane: p, from: 12.2, to: 13.6, label: fence }
  - { lane: p, from: 13.8, to: 15.2, label: CAS }
  - { lane: p, from: 15.4, to: 18, label: replay, dur: "~10 s of log" }
  - { lane: e, from: 0.5, to: 18, label: "resend not-applied writes (≤ 20 s) · 503 if unsure", tone: muted, dash: true }
events:
  - { lane: b, at: 12.9, label: fence object, tone: danger }
  - { lane: b, at: 15.9, label: zombie PUT collides, tone: danger }
  - { lane: old, at: 15.9, label: exit 3, tone: danger }
arrows:
  - { from: p, to: b, at: 12.9 }
marks:
  - { at: 0, label: last renewal, tone: muted }
  - { at: 8, label: stops acking, tone: rust }
  - { at: 12, label: presumed dead, tone: danger }
  - { at: 18, label: serving, tone: ok }
ticks:
  - { at: 0, label: 0 s }
  - { at: 8, label: 8 s }
  - { at: 12, label: 12 s }
  - { at: 18, label: "+ replay" }
```

- Acks go out in ordinal order, and a node checks its lease before every PUT and before every ack.
  So everything the old node acked is below the fence, and the new owner replays all of it.
- The fence is a create-only PUT at the end of the durable prefix. Segments the old node wrote past
  a hole were never acked, and nothing reads past the fence.
- Peers notice in 3–5 s instead of 12 s when the dead node's port refuses connections. A planned
  handoff skips all of this and moves a shard in ~0.2 s. Details:
  [Architecture](architecture.md#failure-and-takeover).

## One node or a cluster

| | One node | Cluster |
|---|---|---|
| Forwarding hop | none | ~1 ms when the entry node doesn't own the repo |
| Firehose | own log only, 0–4 ms after the 200 | merged across logs: ~5 ms p50 from the write at light load, ~20 ms more with busy nodes (laptop) |
| After a crash | the restarted node waits out its old lease, ~53 s of write downtime on `tiny` (measured), then replays | a peer takes over in ~12 s and replays |
| Durability | the same: one log per node, acked only once its segment is in the bucket | the same |
