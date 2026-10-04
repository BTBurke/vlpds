---
title: Backups and recovery
section: Operations
order: 105
status: stub
summary: "What can be lost and how to get it back: bucket versioning and replication, point-in-time restore, losing a host, and what is still design rather than built."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: bucket, label: "primary bucket", at: [0, 0], size: [9, 3], tone: amber, shape: store }
    - { id: copy, label: "replica / versions", at: [14, 0], size: [9, 3], tone: muted, shape: store }
  edges:
    - "bucket ~> copy: replicate"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Threat model

<!-- Sources: DESIGN.md "Backups and restore" → Threats -->

TODO.

## What exists today

<!-- Sources: status: design, not implemented — say so clearly -->

TODO.

## Options

<!-- Sources: DESIGN "Options and how they interact with vlpds" -->

TODO.

## Point-in-time restore of a cluster

<!-- Sources: DESIGN "A consistent point-in-time restore of the whole cluster" -->

TODO.

## Losing a host

<!-- Sources: RUNBOOK "Replacing a dead host" (nothing to restore: the bucket has it) -->

TODO.

## Signing keys

<!-- Sources: DESIGN "Signing keys" under Backups; KEK escrow -->

TODO.
