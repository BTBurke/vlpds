---
title: Upgrades
section: Operations
order: 108
status: stub
summary: "Rolling deploys, feature levels and format versioning: how to upgrade a cluster without downtime, finalize, and roll back."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: old, label: "build N", at: [0, 0], size: [7, 3] }
    - { id: mixed, label: "mixed cluster", at: [10, 0], size: [8, 3], tone: amber }
    - { id: new, label: "build N+1 finalized", at: [21, 0], size: [10, 3], tone: accent }
  edges:
    - "old -> mixed: roll"
    - "mixed -> new: finalize"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Rolling deploy

<!-- Sources: RUNBOOK "Rolling deploy"; stop grace ≥ 60 s -->

TODO.

## Feature levels

<!-- Sources: DESIGN "Rolling upgrades and format versioning" (active level, finalize) -->

TODO.

## Rolling upgrade, finalize, rollback

<!-- Sources: RUNBOOK same-named procedure; VlpdsMixedVersions, VlpdsFeatureLevelUnfinalized -->

TODO.

## Compatibility contract

<!-- Sources: DESIGN "Compatibility contract" -->

TODO.

## Testing an upgrade

<!-- Sources: just upgrade-ha / upgrade-ci -->

TODO.
