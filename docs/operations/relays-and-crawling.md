---
title: Relays and crawling
section: Operations
order: 110
status: stub
summary: "Getting this server's repos onto the network: requestCrawl to relays, what relays see, backfill windows, and checking sync 1.1 conformance."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: pds, label: "vlpds", at: [0, 0], size: [7, 3], tone: accent }
    - { id: relay, label: "relay", at: [11, 0], size: [7, 3], tone: blue }
    - { id: av, label: "AppView", at: [22, 0], size: [7, 3], tone: muted }
  edges:
    - "pds -> relay: requestCrawl"
    - "relay -> av"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Crawl requests

<!-- Sources: DESIGN "Relay crawl requests" (src/xrpc/crawlers.rs); admin console Relays page -->

TODO.

## What relays consume

<!-- Sources: subscribeRepos, listRepos, getRepo; 72 h backfill -->

TODO.

## Sharded consumers

<!-- Sources: ?shard=k/n -->

TODO.

## Checking conformance

<!-- Sources: just checker / checker-rs -->

TODO.

## Bandwidth

<!-- Sources: ~12 Mbit/s per full subscriber today (sizing table) -->

TODO.
