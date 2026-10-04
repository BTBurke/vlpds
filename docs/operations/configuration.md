---
title: Configuration
section: Operations
order: 103
status: stub
summary: "The flags that matter, the memory budget and how caches size themselves, shard count and lease TTL, and the trade-offs behind the defaults."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: limit, label: "memory limit", at: [0, 0], size: [8, 3], tone: amber }
    - { id: budget, label: "budget", at: [11, 0], size: [7, 3], tone: accent }
    - { id: caches, label: "repo · block · meta caches", at: [21, 0], size: [11, 3], tone: blue }
  edges:
    - "limit -> budget"
    - "budget -> caches: autosize"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Where configuration comes from

<!-- Sources: VLPDS_* env vars = flags (vlpds --help); secrets as files -->

TODO.

## Memory budget and autosizing

<!-- Sources: src/memory.rs; DESIGN "Memory budget" -->

TODO.

## Shards and lease TTL

<!-- Sources: --shards, --lease-ttl-ms trade-offs (tiny 1 / 60 s vs 64 / 10 s; restart probe numbers) -->

TODO.

## Log and firehose

<!-- Sources: --log-inflight, --max-segment-mb, --log-compression, --log-retention, firehose ring/merge/backfill sizes -->

TODO.

## Disk cache

<!-- Sources: --disk-cache-mb / --disk-cache-shard-mb, --cache-dir -->

TODO.

## Limits and admission

<!-- Sources: --max-inflight-writes, rate limits (DESIGN "Rate limits"), Argon2 permits -->

TODO.

## Reference of every flag

<!-- Sources: generated from vlpds --help? (decide in phase 2) -->

TODO.
