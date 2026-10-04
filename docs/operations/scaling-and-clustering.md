---
title: Scaling and clustering
section: Operations
order: 107
status: stub
summary: "Growing from one node to many: adding and removing nodes, how shards rebalance, splitting and merging shards, peer TLS, and sizing rules."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: a, label: "node A", at: [0, 0], size: [6, 3], tone: accent }
    - { id: b, label: "node B", at: [0, 4], size: [6, 3], tone: accent }
    - { id: c, label: "new node C", at: [12, 2], size: [8, 3], tone: blue }
  edges:
    - "a ~> c: handback"
    - "b ~> c: handback"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Sizing rules

<!-- Sources: DESIGN "Initial deployment sizing" (60% rule: (nodes − 1) × cores × 0.6 ≥ busy cores) -->

TODO.

## Adding a node

<!-- Sources: RUNBOOK "Adding a node"; joining waits for every peer to follow its log -->

TODO.

## Removing a node

<!-- Sources: graceful shutdown: draining lease, handoff with prewarm -->

TODO.

## Shard split and merge

<!-- Sources: RUNBOOK "Shard split / merge"; DESIGN "Online shard split/merge" -->

TODO.

## Peer TLS

<!-- Sources: RUNBOOK "Peer TLS" -->

TODO.

## Planet scale

<!-- Sources: DESIGN "Planet scale" (analysis), "Read replicas and fan-out nodes: not planned" -->

TODO.
