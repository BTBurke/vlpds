---
title: Firehose
section: vlPDS
order: 6
status: ready
summary: "One global, deterministic event order with no global sequencer: per-node logs, watermarks, a k-way merge, backfill from the bucket and sharded subscriptions."
---

```hero
diagram:
  caption: Every node runs this same merge over every node's log, so every node emits the same events in the same order and a cursor works on any of them. Recent events come from memory; older cursors are read back from the bucket.
  nodes:
    - { id: l1, label: node 1 log, sub: "seqs · watermark W₁", at: [0, 0], size: [8, 2.6], tone: accent }
    - { id: l2, label: node 2 log, sub: "seqs · watermark W₂", at: [0, 4], size: [8, 2.6], tone: accent }
    - { id: l3, label: node 3 log, sub: "seqs · watermark W₃", at: [0, 8], size: [8, 2.6], tone: accent }
    - { id: merge, label: Merger, sub: emit seq ≤ min W, at: [13, 4], size: [8, 2.6], tone: blue }
    - { id: ring, label: Firehose ring, sub: 512 MiB in memory, at: [25, 4], size: [8, 2.6], tone: blue }
    - { id: subs, label: Subscribers, sub: "relays · ?shard=k/n", at: [39, 4], size: [8, 2.6] }
    - { id: seg, label: "`log/` in the bucket", sub: "segments · 72 h", at: [0, 12], size: [9, 2.6], shape: store, tone: amber }
    - { id: bf, label: Backfill, sub: older cursors, at: [25, 12], size: [8, 2.6], tone: blue }
  groups:
    - { label: on every node, around: [merge, ring, bf], tone: blue }
  edges:
    - "l1.r -> merge.l30"
    - "l2.r -> merge.l: batches + W"
    - "l3.r -> merge.l70"
    - "merge -> ring"
    - "ring -> subs: subscribeRepos"
    - "seg -> bf: read-ahead"
    - { from: bf.r, to: subs.b, label: catch up }
facts:
  - { value: "1", unit: order, label: on every node, note: "cursors work on any node; no global sequencer" }
  - { value: "~5 ms", label: write to a peer's firehose, note: "p50 after the durable ack (laptop)", tone: blue }
  - { value: "72 h", label: of cursor backfill, note: "`--log-retention`; older cursors get OutdatedCursor", tone: amber }
  - { value: "1,000", unit: subscribers, label: kept up on one node, note: "each at 10k events/s; measured on 16 cores", tone: violet }
```

vlpds serves `com.atproto.sync.subscribeRepos` from every node, with the same sync 1.1 events the
reference PDS sends. There is no single sequencer: each node numbers the events in its own log, and
every node merges all the logs into one stream with a fixed order. A relay can connect to any node
behind the load balancer, and after a reconnect it can resume with its cursor on a different node.

This page covers how that order is built, what holds it back, how subscribers are served and
limited, and how old cursors are served from the bucket. For relay-facing operations (crawl
requests, conformance checks) see [Relays and crawling](operations/relays-and-crawling.md).

## Sequence numbers and watermarks

```diagram
caption: A seq is the wall clock in microseconds with the node's writer id in the low byte. A log's watermark W promises that every event at or below it is durable.
nodes:
  - { id: us, label: unix microseconds, sub: high 56 bits, at: [0, 0], size: [12, 2.6], tone: accent }
  - { id: wr, label: writer, sub: low 8 bits, at: [12, 0], size: [6, 2.6], tone: violet }
  - { id: busy, label: busy log, sub: W = last durable seq, at: [23, 0], size: [9, 2.6], tone: accent }
  - { id: idle, label: idle log, sub: W = clock (≤ lease expiry), at: [23, 4], size: [9, 2.6], tone: muted }
  - { id: hb, label: Log stream, sub: heartbeat every 5 ms, at: [36, 2], size: [9, 2.6], tone: blue }
edges:
  - "busy.r -> hb.l30"
  - "idle.r -> hb.l70"
notes:
  - { at: [0, 4.4], text: "seq = unix_micros × 256 + writer", align: start }
```

- **seq** is `unix_micros × 256 + writer`. The writer id is a byte claimed by compare-and-swap
  on `writers/{w}` and unique among live nodes, so two logs never produce the same seq. Within a log
  seqs strictly increase: a node takes the larger of the clock and its last seq + 256, so a clock
  that steps back can't reorder its own log.
- **Seqs are sparse.** Consecutive events differ by at least 256 and usually by much more. Consumers
  may rely on seqs increasing, never on them being dense. They are also large (about 4.6 × 10¹⁷
  today, above 2⁵³), so keep cursors as 64-bit integers, not JavaScript numbers or doubles.
- **Watermark W.** Each log publishes a watermark: every event with seq ≤ W has been made durable
  and handed on. While writes are in flight it is the last durable seq. An idle log advertises its
  clock instead, capped at its lease expiry so a successor's seqs always start above it. That is
  what lets the stream move while a node has nothing to write.

Because an idle log's watermark is its clock, **clock offset between nodes delays the firehose**: a
node whose clock runs behind holds every node's merge back by the offset. Keep NTP (chrony) on
every host. Clock offset can delay the stream but never reorders or loses an event.

## The merger

```diagram
caption: Each log's batches queue on the merger until the minimum watermark passes them. Everything at or below it is sorted by seq, framed once and appended to the ring. A log that holds the queues past their budget is spilled and read back from the bucket.
nodes:
  - { id: own, label: own log, sub: in-process, at: [0, 0], size: [8, 2.6], tone: accent }
  - { id: peer, label: peer logs, sub: "`/internal/v1/log/stream`", at: [0, 4], size: [8, 2.6], tone: accent, stack: true }
  - { id: dead, label: dead log, sub: drained to its fence, at: [0, 8], size: [8, 2.6], tone: muted }
  - { id: q, label: Per-log queues, sub: 256 MiB budget, at: [13, 4], size: [9, 2.6], tone: blue }
  - { id: cut, label: "cut at min W", sub: sort · frame once, at: [26, 4], size: [9, 2.6], tone: blue }
  - { id: ring, label: Firehose ring, sub: merged batches, at: [39, 4], size: [8, 2.6], tone: solid }
  - { id: s3, label: "`log/`", sub: spill read-back, at: [13, 9.5], size: [9, 2.6], shape: store, tone: amber }
edges:
  - "own.r -> q.l30"
  - "peer.r -> q.l: batches + W"
  - "dead.r -> q.l70"
  - "q -> cut: every 2 ms"
  - "cut -> ring"
  - { from: s3.t, to: q.b, label: over budget, dash: true }
```

- **Inputs.** The node's own log feeds the merger directly. Every peer's log arrives over a
  websocket on the peer mTLS listener (`/internal/v1/log/stream`): durable batches plus a
  watermark heartbeat every 5 ms. The owner serves it from a live ring of sealed segments
  (`--live-ring-mb`, 128 MiB); a follower that falls further behind is dropped and catches up from
  the bucket (`VlpdsPeerLogStreamLagging`). A stream silent for 2 s is presumed dead and reconnected,
  again catching up from the bucket first.
- **The cut.** Every 2 ms the merger reads the minimum watermark over all followed logs, takes every
  queued event at or below it, sorts them by seq and frames them as websocket messages once. The
  result is a deterministic, total order: the same on every node, because every node applies the
  same rule to the same durable events.
- **Dead logs.** When a node dies, its followers keep reading its log from the bucket up to the
  fence that the node taking over its shards writes, then drop it as a source. Until that fence
  exists, the dead log's watermark holds every merge back. That is what `VlpdsFirehoseStalled`
  usually means.
- **Spilling.** While one log holds the minimum back, the others queue. The queues share a budget
  (`--firehose-merge-queue-mb`, 256 MiB). Past it, the merger stops queueing that log and reads it
  back from its bucket segments in chunks as the watermark allows, until it meets the live stream
  again. Memory stays bounded however long a stall lasts, at the cost of extra GETs
  (`vlpds_firehose_merge_spills_total`, `VlpdsFirehoseMergeSpilling`).
- **Fail-stop.** The merger is a critical task: a panic in it exits the process (exit 9) instead of
  leaving a node that serves a frozen firehose.

The delay from a seq being assigned to its emission is `vlpds_firehose_emit_delay_seconds`
(alerts at a p99 over 2 s and 20 s). `vlpds admin cluster status` prints the merger's last emitted
seq and minimum watermark; `--json` adds each source's watermark, which names the laggard.
Procedures: [Runbook](operations/runbook.md#firehose-stalled-or-lagging).

## Joining and leaving

```steps
- title: A node starts with a floor
  body: Its merged stream starts at F, its clock at startup. It emits only events above F; cursors at or below F are served from the bucket.
- title: Every live peer confirms it follows the joiner's log
  body: "A peer confirms by answering the joiner's hello, or by listing the joiner's log in the `follows` field of its own lease, with the floor it started following at. No timer ends this wait: a peer that never confirms keeps the joiner from taking shards (it forwards writes meanwhile) until that peer confirms or is presumed dead."
- title: The joiner's seqs pass every floor, then it publishes `joined`
  body: Peers hand shards only to joined nodes. So no event the joiner acks can fall below the point where some peer's merge began following it, and no subscriber anywhere misses one.
- title: A graceful leave freezes the merger first
  body: "Before deleting its lease the node stops its merger for good, keeps serving for 500 ms, fences its own log and closes its log streams (new ones get 410 `LogClosed`). Its subscribers are disconnected and resume from their cursors on another node."
- title: Peers drain the log to its fence
  body: A follower leaves a live stream within 50 ms of the log's lease no longer being live (deleted, presumed dead or fenced), reads the rest from the bucket up to the fence, and retires it.
```

The order of these steps is what makes a cluster's merged stream gap-free through joins, leaves and
crashes; `tests/all/join_follow.rs` and `tests/all/firehose_startup.rs` cover them. The
mechanism is the same as for shard ownership: see
[Architecture](architecture.md#handoff-handback-and-joining).

## Serving subscribers

```diagram
caption: A merged batch is framed once. Every subscriber writes slices of the same bytes from its own socket, on a runtime separate from request handling.
nodes:
  - { id: ring, label: Firehose ring, sub: pre-framed batches, at: [0, 3], size: [9, 2.6], tone: blue }
  - { id: s1, label: subscriber, sub: full stream, at: [15, 0], size: [8, 2.4] }
  - { id: s2, label: subscriber, sub: "?shard=1/4", at: [15, 3.1], size: [8, 2.4] }
  - { id: s3, label: subscriber, sub: lagging, at: [15, 6.2], size: [8, 2.4], tone: danger }
  - { id: cut, label: ConsumerTooSlow, sub: past 128 MiB of lag, at: [28, 6.2], size: [9, 2.4], tone: danger }
groups:
  - { label: "firehose runtime · 4 threads", around: [s1, s2, s3], tone: blue }
edges:
  - "ring.r -> s1.l: zero-copy"
  - "ring.r -> s2.l"
  - "ring.r -> s3.l"
  - "s3 -> cut"
```

- **Own runtime.** `subscribeRepos` upgrades the connection itself and moves the socket to a
  dedicated runtime (`--firehose-threads`, 4), so fan-out never competes with writes and API
  requests. Subscribers wake on a shared watch of the stream head; there is no per-subscriber
  queue.
- **Cost.** Measured on a 16-core box: 1,000 subscribers each kept up with 10k events/s (~10M
  events/s, ~18 GB/s over loopback) while write p99 stayed under 70 ms
  (`bench/results/2026-10-03-benchbox`). Egress is a memory-copy problem, not a CPU one.
- **Slow consumers.** A subscriber may fall at most `--firehose-max-lag-mb` (128 MiB) behind the
  head. Past that it gets `ConsumerTooSlow` and is closed; it resumes from its cursor. Writes outside
  the live path (backfill chunks, info frames, pongs) that make no progress for 30 s drop the
  subscriber too.
- **Per-client cap.** At most `--firehose-max-per-ip` (256) connections per client address (per
  /64 for IPv6; behind `--trusted-proxies`, the forwarded client), 429 past it. `subscribeRepos` is
  exempt from the rate limiter and doesn't count against `--max-connections`, so this is its only
  per-client limit.
- **Bad cursors.** A cursor above both the stream head and the node's clock gets `FutureCursor`.

Watch `vlpds_firehose_subscribers`, `vlpds_firehose_bytes_sent_total` against the NIC, and
`vlpds_firehose_disconnects_total{reason}`; a rising `too_slow` rate across many subscribers points
at the server (`VlpdsFirehoseConsumersTooSlow`).

## Backfill from the bucket

```diagram
caption: "A cursor older than the ring is served from the bucket: each log is seeked to the cursor, read ahead, merged by seq, and handed to the live ring at the ring's floor."
nodes:
  - { id: cur, label: "?cursor=N", sub: older than the ring, at: [0, 0], size: [8, 2.6] }
  - { id: seek, label: Seek each log, sub: probe + binary search, at: [11, 0], size: [9, 2.6], tone: blue }
  - { id: ra, label: Read-ahead, sub: "≤ 32 GETs per log", at: [23, 0], size: [9, 2.6], tone: blue }
  - { id: kway, label: k-way merge, sub: by seq, at: [35, 0], size: [8, 2.6], tone: blue }
  - { id: seg, label: "`log/`", sub: segments, at: [11, 5], size: [9, 2.6], shape: store, tone: amber }
  - { id: cache, label: Segment cache, sub: shared · 256 MiB, at: [23, 5], size: [9, 2.6], tone: muted }
  - { id: live, label: Live ring, sub: hand-off at its floor, at: [35, 5], size: [8, 2.6], tone: solid }
edges:
  - cur -> seek
  - seek -> ra
  - ra -> kway
  - "seg.r -> cache.l: GET"
  - "cache.t -> ra.b"
  - kway -> live
```

A cursor inside the in-memory ring (`--firehose-ring-mb`, 512 MiB, about 6 minutes at Bluesky's
rate) is served from memory. An older one is read from the log segments in the bucket. Every log
the cursor could need, dead ones included, is seeked by segment header and read ahead (up to 32
GETs per log within `--backfill-readahead-mb`, 64 MiB per subscriber) through a segment cache that
subscribers replaying the same range share (`--backfill-cache-mb`, 256 MiB). The logs are merged by
seq, the same rule as the live merger, so the subscriber sees one continuous stream when it joins
the ring.

- **Concurrency.** At most `--firehose-max-backfills` (16) backfills run at once, so read-ahead
  memory is bounded at 16 × 64 MiB. More wait for a slot (`vlpds_firehose_backfills{state}`),
  still answering pings.
- **Speed.** Over 860k events/s (~1.7 GB/s) for one subscriber against a local store
  (`bench/results/2026-10-03-benchbox`); a remote bucket is slower but the read-ahead hides most of
  the round trips.
- **Errors.** A bucket error is retried a few times, then the subscriber is disconnected and
  resumes from its cursor. vlpds never skips stored events to keep a connection alive.

The `tiny` profile shrinks all of these (64 MiB ring, 4 backfills, 16 MiB read-ahead); see
[Configuration](operations/configuration.md#log-and-firehose).

## Sharded subscriptions

```diagram
caption: "`?shard=k/n` carries only the events whose repo DID hashes into slice k of the 65,536 slots. The union of the n streams is the full stream, with the same seqs and cursors."
nodes:
  - { id: full, label: full stream, sub: every event, at: [0, 3.5], size: [8, 2.6], tone: blue }
  - { id: a, label: "?shard=0/4", sub: "slots 0–16,383", at: [13, 0], size: [9, 2.2] }
  - { id: b, label: "?shard=1/4", sub: "slots 16,384–32,767", at: [13, 2.5], size: [9, 2.2] }
  - { id: c, label: "?shard=2/4", sub: "slots 32,768–49,151", at: [13, 5], size: [9, 2.2] }
  - { id: d, label: "?shard=3/4", sub: "slots 49,152–65,535", at: [13, 7.5], size: [9, 2.2] }
  - { id: w, label: relay workers, sub: one per slice, at: [27, 3.5], size: [8, 2.6] }
edges:
  - full.r -> a.l
  - full.r -> b.l
  - full.r -> c.l
  - full.r -> d.l
  - a.r -> w.l
  - b.r -> w.l
  - c.r -> w.l
  - d.r -> w.l
```

A vlpds extension: `subscribeRepos?shard=k/n` (0 ≤ k < n ≤ 65,536) carries the events whose repo
(`repo` of a `#commit`, `did` of the others) hashes into slots s with s·n/65,536 = k. When n divides
the cluster's shard count, a slice is a whole set of cluster shards. Seqs, order and cursors are
the full stream's: a cursor from one works on the other, `OutdatedCursor`, `FutureCursor` and
`ConsumerTooSlow` behave the same, and a bad `shard` is 400 `InvalidRequest`.

Filtering is cheap: each batch's slots are computed once, by the first sharded subscriber, and every
subscriber writes its matching runs as slices of the shared batch bytes. Backfill filters the same
way. Measured: 400 subscribers on `?shard=k/4` at 10k writes/s: each slice got a quarter of the events,
and the union equalled the full stream. How a relay splits its work this way:
[Relays and crawling](operations/relays-and-crawling.md#sharded-consumers).

## Events

| Event | Sent when | Notes |
|---|---|---|
| `#commit` | a repo write (create, update, delete, applyWrites) | sync 1.1: `since`, `prevData`, `prev` CIDs on updates and deletes, and the inversion-proof MST nodes in `blocks`; `tooBig` and `rebase` are always false |
| `#sync` | an account is activated (a new account, or one migrated in), a repo is imported, the repo is re-signed after a signing-key rotation | carries the signed commit as a one-block CAR; consumers resync the repo |
| `#identity` | a handle or DID document change, `vlpds admin publish-identity` | |
| `#account` | activation, deactivation, takedown, deletion | `active` plus a status |
| `#info` `OutdatedCursor` | a cursor older than the retained history | the stream continues from the oldest retained event |

Errors close the stream: `FutureCursor`, `ConsumerTooSlow`. Every commit is checked before it is
signed: inverting its operations must reproduce `prevData`. Two independent checkers, built on
indigo and on shrike, verify a live stream end to end; see
[Relays and crawling](operations/relays-and-crawling.md#checking-conformance).

## Retention

```diagram
caption: A segment is deleted only when it is past the backfill window and no shard's crash replay could still read it. The highest pruned seq is the retained floor that older cursors are moved up to.
nodes:
  - { id: seg, label: log segment, sub: in the bucket, at: [0, 2], size: [8, 2.6], shape: store, tone: amber }
  - { id: win, label: older than 72 h?, sub: "`--log-retention`", at: [12, 0], size: [9, 2.6] }
  - { id: rep, label: below replay floor?, sub: no shard needs it, at: [12, 4], size: [9, 2.6] }
  - { id: del, label: deleted, sub: oldest first, at: [25, 2], size: [8, 2.6], tone: solid }
  - { id: floor, label: retained floor, sub: "max `pruned_seq`", at: [37, 2], size: [9, 2.6], tone: blue }
edges:
  - seg.r -> win.l
  - seg.r -> rep.l
  - "win.r -> del.l: and"
  - rep.r -> del.l
  - del -> floor
```

The log is the write-ahead log and the firehose, so retention serves both. A segment goes once it
is older than the backfill window (`--log-retention`, 72 h; `off` keeps everything) **and** no
crash replay of any shard could read it. A live log is pruned only by its owner, a dead one only
once fenced, by the owner of the lowest-numbered shard. Passes run every
`--log-retention-interval` (60 s) and LIST only what can be due, so an idle node's passes cost no
requests.

Before deleting, the pruner raises the log's `pruned_seq`. The maximum over all logs is the
retained floor: a cursor below it gets `#info OutdatedCursor` and continues from the floor, which
is the protocol's "oldest available". Inside the window a cursor is always served in full, through
restarts, takeovers and reshards. At Bluesky's write rate 72 h of log is ~230 GB in the bucket.
`VlpdsRetentionFailing` and `VlpdsRetentionNotRunning` cover a pruner that stops; the full rules
(replay floors, fences kept 7 days) are in `DESIGN.md` "Log retention".
