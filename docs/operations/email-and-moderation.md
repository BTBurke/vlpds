---
title: Email and moderation
section: Operations
order: 112
status: stub
summary: "Outgoing mail (SMTP, branding, disposable-address policy), the moderation service, earned invites and external handles."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: pds, label: "vlpds", at: [0, 0], size: [7, 3], tone: accent }
    - { id: smtp, label: "SMTP relay", at: [11, 0], size: [7, 3] }
    - { id: mod, label: "moderation service", at: [11, 4], size: [9, 3], tone: muted }
  edges:
    - "pds -> smtp: mail"
    - "mod -> pds: service auth"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Email

<!-- Sources: RUNBOOK "Email (SMTP, moderation mail, branding)"; DESIGN "Email" -->

TODO.

## Moderation service

<!-- Sources: DESIGN "Moderation service auth, earned invites, disposable email, DNS handles" -->

TODO.

## Handle policy

<!-- Sources: DESIGN "Handle policy" (src/handle_policy.rs) -->

TODO.

## Invites

<!-- Sources: earned invites, invites optional -->

TODO.
