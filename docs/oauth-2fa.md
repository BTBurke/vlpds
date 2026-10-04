---
title: OAuth and 2FA
section: vlPDS
order: 9
status: stub
summary: "Signing in: the OAuth authorization server, DPoP, app passwords and legacy sessions, TOTP and email second factors, and how auth state stays correct under concurrency."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: client, label: "OAuth client", at: [0, 0], size: [7, 3] }
    - { id: as, label: "authorize · token", at: [10, 0], size: [9, 3], tone: accent }
    - { id: tfa, label: "TOTP / email code", at: [10, 5], size: [9, 3], tone: amber }
    - { id: res, label: "XRPC with DPoP", at: [23, 0], size: [8, 3], tone: blue }
  edges:
    - "client -> as: PAR"
    - "as -> tfa: second factor"
    - "as -> res: tokens"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Sign-in methods

<!-- Sources: OAuth (src/oauth/), legacy createSession, app passwords -->

TODO.

## The OAuth flow

<!-- Sources: PAR, authorize UI, token, refresh, DPoP nonces (tests/all/oauth.rs, dpop_resend.rs, oauth_replay_durable.rs) -->

TODO.

## Second factors

<!-- Sources: TOTP (src/totp.rs), email 2FA (DESIGN "Email second factor"); RUNBOOK "A user locked out by a second factor" -->

TODO.

## Auth state under concurrency

<!-- Sources: DESIGN.md "Auth state under concurrency" (src/xrpc/cas.rs) -->

TODO.

## Passwords and Argon2

<!-- Sources: ARGON2_PERMITS, shedding with 503 Overloaded -->

TODO.

## Revocation

<!-- Sources: revocation GC, sessions on account changes -->

TODO.
