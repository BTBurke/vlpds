---
title: Migration
section: vlPDS
order: 10
status: stub
summary: "Moving an existing account to this server with /migrate: what the page does in simple and advanced modes, what is copied, and how identity moves safely."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: old, label: "current PDS", at: [0, 0], size: [8, 3] }
    - { id: page, label: "/migrate", at: [11, 0], size: [7, 3], tone: accent }
    - { id: new, label: "this server", at: [21, 0], size: [8, 3], tone: accent }
    - { id: plc, label: "PLC directory", at: [11, 5], size: [7, 3], tone: muted }
  edges:
    - "old -> page: export"
    - "page -> new: import"
    - "page -> plc: identity op"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Before you start

<!-- Sources: what the user needs; invite codes; handle choices -->

TODO.

## Simple and advanced modes

<!-- Sources: ui/src/pages/migrate (don't duplicate UI copy); own recovery key -->

TODO.

## What gets copied

<!-- Sources: repo CAR (streaming importRepo), blobs, preferences, staged imports -->

TODO.

## Moving the identity

<!-- Sources: PLC operation signed by the old PDS, recovery key added; DESIGN "PLC identity" -->

TODO.

## Afterwards

<!-- Sources: deactivate the old account, relay crawl, checkAccountStatus counts -->

TODO.

## Testing a migration

<!-- Sources: bench/migrate/README.md (just migrate-e2e) -->

TODO.
