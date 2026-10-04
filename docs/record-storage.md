---
title: Record storage
section: vlPDS
order: 4
status: stub
summary: "Repos without per-commit storage churn: records are the truth, MST interior nodes are persisted, leaves are rebuilt, and only visited paths stay in memory."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: rec, label: "R/ records", at: [0, 0], size: [8, 3], tone: accent }
    - { id: mst, label: "M/ interior nodes", at: [11, 0], size: [8, 3], tone: accent }
    - { id: leaf, label: "leaves (derived)", at: [22, 0], size: [8, 3], tone: muted }
    - { id: tree, label: "LazyTree paths", at: [11, 5], size: [8, 3], tone: blue }
  edges:
    - "rec -> leaf: rebuild"
    - "mst -> tree: load on demand"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Repos, commits and the MST

<!-- Sources: short atproto primer: signed commit → MST → records -->

TODO.

## What is stored and what is derived

<!-- Sources: DESIGN.md §2 (R/, M/ +28 B/record, leaves never stored); "Partial MSTs" / "As built" -->

TODO.

## Partial trees in memory

<!-- Sources: mst_lazy::LazyTree; ~10–20 KB per written repo; --repo-cache-mb; path cache eviction -->

TODO.

## Cold opens and verification

<!-- Sources: DESIGN.md §2 Cold open, Verification, fallbacks metric vlpds_lazy_mst_fallbacks_total -->

TODO.

## Write coalescing and pipelining

<!-- Sources: DESIGN.md §1 (drain queued writes into one commit; swapCommit rules; acks in log order) -->

TODO.

## Reads: getRecord, getRepo, getBlocks

<!-- Sources: DurableView + SlateDB snapshot; NodeIndex; streamable CAR getRepo -->

TODO.

## Imports

<!-- Sources: DESIGN.md "Staged imports", "Import admission" -->

TODO.
