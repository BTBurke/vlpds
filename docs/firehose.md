---
title: Firehose
section: vlPDS
order: 6
status: stub
summary: "One global, deterministic event order with no global sequencer: per-node logs, watermarks, a k-way merge, backfill from the bucket and sharded subscriptions."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: l1, label: "log 1", at: [0, 0], size: [6, 2], tone: accent }
    - { id: l2, label: "log 2", at: [0, 3], size: [6, 2], tone: accent }
    - { id: merge, label: "merger: emit seq ≤ min W", at: [10, 1], size: [11, 3], tone: blue }
    - { id: sub, label: "subscribers", at: [25, 1], size: [8, 3] }
  edges:
    - "l1 -> merge"
    - "l2 -> merge"
    - "merge -> sub: subscribeRepos"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Sequence numbers and watermarks

<!-- Sources: seq = unix_micros × 256 + writer; DESIGN HA "Global firehose order with no global sequencer" -->

TODO.

## The merger

<!-- Sources: DESIGN.md §5 Live, Merge (256 MiB budget, spill to S3) -->

TODO.

## Joining and leaving

<!-- Sources: DESIGN.md §5 Joining (floors, hello, joined), streams end with their log -->

TODO.

## Serving subscribers

<!-- Sources: dedicated runtime, zero-copy frames, --firehose-max-lag-mb, ConsumerTooSlow, per-IP cap -->

TODO.

## Backfill from the bucket

<!-- Sources: cursor behind the ring, read-ahead, --firehose-max-backfills, 72 h retention -->

TODO.

## Sharded subscriptions

<!-- Sources: ?shard=k/n vlpds extension -->

TODO.

## Events

<!-- Sources: #commit (sync 1.1), #sync, #identity, #account; checker / checker-rs -->

TODO.

## Retention

<!-- Sources: DESIGN.md "Log retention" (never delete what a replay needs) -->

TODO.
