---
title: Runbook and incidents
section: Operations
order: 109
status: stub
summary: "The first page to open when an alert fires: exit codes, fail-stops, lease trouble, a slow or failing object store, a stalled firehose, and what not to do."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: alert, label: "alert fires", at: [0, 0], size: [7, 3], tone: danger }
    - { id: triage, label: "triage", at: [10, 0], size: [7, 3], tone: amber }
    - { id: fix, label: "procedure", at: [20, 0], size: [7, 3], tone: accent }
  edges:
    - "alert -> triage"
    - "triage -> fix"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Exit codes and fail-stops

<!-- Sources: RUNBOOK "Tools: endpoints, CLI, logs, exit codes"; VlpdsNodeFailStopped -->

TODO.

## Lease trouble

<!-- Sources: RUNBOOK lease alerts (renewal near/at ceiling, validity low) -->

TODO.

## Slow or failing object store

<!-- Sources: VlpdsObjectStoreBrownout, permits saturated, "Store saturated by the node's own reads" -->

TODO.

## Firehose stalled or lagging

<!-- Sources: VlpdsFirehoseStalled, dead log unfenced -->

TODO.

## Shards unowned or flapping

<!-- Sources: VlpdsShardsUnowned, OwnershipFlapping -->

TODO.

## Memory pressure

<!-- Sources: VlpdsMemoryHigh/Critical, cache at capacity -->

TODO.

## What not to do

<!-- Sources: RUNBOOK "What NOT to do" (verbatim list is fine) -->

TODO.

## The full runbook

<!-- Sources: ops/RUNBOOK.md stays the per-alert reference that alert runbook_urls point at; this page is the map -->

TODO.
