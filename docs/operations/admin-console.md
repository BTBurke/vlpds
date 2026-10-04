---
title: Admin console and CLI
section: Operations
order: 111
status: stub
summary: "The operator console (on the tailnet) and the admin CLI: accounts, invites, takedowns, rate limits, relays, cluster status and metrics."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: op, label: "operator", at: [0, 0], size: [7, 3] }
    - { id: ui, label: "/admin console", at: [10, 0], size: [8, 3], tone: accent }
    - { id: cli, label: "vlpds admin", at: [10, 4], size: [8, 3], tone: accent }
    - { id: api, label: "admin XRPC", at: [22, 2], size: [8, 3], tone: blue }
  edges:
    - "op -> ui"
    - "op.b -> cli.l"
    - "ui -> api"
    - "cli -> api"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Reaching the console

<!-- Sources: tailnet console port (vlpds_tailnet_console_port), admin token -->

TODO.

## Pages

<!-- Sources: ui/src/pages/admin: Accounts, Invites, Cluster, Metrics, RateLimits, Relays -->

TODO.

## Admin CLI

<!-- Sources: RUNBOOK "Admin CLI"; DESIGN "Admin CLI" (src/cli/admin.rs, src/xrpc/admin_tools.rs) -->

TODO.

## Common tasks

<!-- Sources: takedown, reset a second factor, invite codes, change rate limits -->

TODO.
