---
title: Proxying
section: vlPDS
order: 7
status: stub
summary: "How app reads reach the AppView: service auth, the per-account owner, connection pools, read-after-write merging and the limits that keep a slow upstream contained."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: app, label: "app", at: [0, 0], size: [6, 3] }
    - { id: node, label: "account's owner", at: [9, 0], size: [9, 3], tone: accent }
    - { id: av, label: "AppView", at: [22, 0], size: [8, 3], tone: muted }
    - { id: raw, label: "read-after-write merge", at: [9, 5], size: [9, 3], tone: blue }
  edges:
    - "app -> node: app.bsky.*"
    - "node -> av: service auth"
    - "node -> raw"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## What is proxied

<!-- Sources: atproto-proxy header, configured AppView / report service; push registration (src/xrpc/proxy/push.rs) -->

TODO.

## The owner serves

<!-- Sources: requests for an account run on its owner (src/xrpc/proxy.rs); service-auth key caching -->

TODO.

## Connection pools and limits

<!-- Sources: DESIGN.md §7 proxy row: per-thread slots, 1,024 conns/host, 128 KiB buffered, 64 in flight per account -->

TODO.

## Read-after-write

<!-- Sources: DESIGN.md §8; src/xrpc/proxy/read_after_write.rs; Atproto-Upstream-Lag -->

TODO.

## Outbound safety

<!-- Sources: guarded client, check_outbound_url, SSRF (tests/all/ref_ssrf.rs) -->

TODO.

## Throughput

<!-- Sources: ~300k req/s per node measured, ~50 µs/request (bench benchbox head) -->

TODO.
