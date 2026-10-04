---
title: Keys and security
section: vlPDS
order: 8
status: stub
summary: "Which keys exist, where they live and how they are wrapped: the KEK (local or Cloud KMS), repo signing keys, the PLC rotation key, recovery keys, peer mTLS and the HTTP security headers."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: kek, label: "KEK", at: [0, 0], size: [8, 3], sub: "local or Cloud KMS", tone: amber }
    - { id: sk, label: "signing keys", at: [11, 0], size: [8, 3], sub: "wrapped per DID", tone: accent }
    - { id: plc, label: "PLC rotation key", at: [11, 5], size: [9, 3], tone: accent }
    - { id: mtls, label: "peer mTLS", at: [24, 0], size: [8, 3], tone: blue }
  edges:
    - "kek -> sk: wraps"
    - "kek.b -> plc.l: wraps"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Key inventory

<!-- Sources: a table: key, purpose, where stored, how wrapped, how rotated (DESIGN "Secrets at rest", "Signing keys", "PLC identity") -->

TODO.

## Secrets at rest

<!-- Sources: src/secrets.rs; vw1 wrapping, bound to the DID; KEK local file vs Cloud KMS -->

TODO.

## Signing hardening

<!-- Sources: DESIGN.md "Signing hardening" (verify-after-sign, VlpdsSignatureFault) -->

TODO.

## PLC rotation key and recovery keys

<!-- Sources: operator recovery key, user recovery keys on the account page and /migrate -->

TODO.

## Peer TLS

<!-- Sources: RUNBOOK "Peer TLS (mTLS between nodes)"; src/peer_tls.rs -->

TODO.

## Web security

<!-- Sources: CSP per page (src/xrpc/webui.rs), SSRF guard, rate limits, handle policy -->

TODO.
