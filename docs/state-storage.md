---
title: State storage
section: vlPDS
order: 3
status: stub
summary: "SlateDB with its WAL turned off: the key layout, how durable segments are applied, checkpoints, compaction and the caches in front of the bucket."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: seg, label: "durable segment", at: [0, 0], size: [8, 3], tone: amber }
    - { id: mem, label: "memtable", at: [11, 0], size: [8, 3], tone: accent }
    - { id: sst, label: "L0 / sorted runs", at: [22, 0], size: [8, 3], tone: amber, shape: store }
    - { id: cache, label: "disk + block cache", at: [22, 5], size: [8, 3], tone: blue }
  edges:
    - "seg -> mem: apply batch"
    - "mem -> sst: checkpoint"
    - "sst -> cache"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Why an LSM, and why SlateDB

<!-- Sources: DESIGN.md §4 intro -->

TODO.

## Key layout

<!-- Sources: DESIGN.md §4 Keys (h/, R/, c/, C/, b/, M/, bl/, S/, a/, n/, G/, T/, p/, meta/) — a table, grouped by purpose -->

TODO.

## Applying the log

<!-- Sources: applied marker meta/applied2, await_durable=false, read-your-writes before ack -->

TODO.

## Checkpoints

<!-- Sources: DESIGN.md HA "Checkpoints" (--checkpoint-every 10 s, staggered, idle skip) -->

TODO.

## Compaction and garbage collection

<!-- Sources: DESIGN.md §4 compaction notes; src/reshard_gc.rs for retired state -->

TODO.

## Caches

<!-- Sources: block cache, SST meta cache (--meta-cache-mb), disk cache (--disk-cache-mb), memory budget (src/memory.rs, DESIGN "Memory budget") -->

TODO.

## Shard split and merge

<!-- Sources: DESIGN.md "Online shard split/merge" (metadata-only SlateDB clone per child); src/reshard.rs -->

TODO.
