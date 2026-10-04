---
title: Architecture
section: vlPDS
order: 2
status: stub
summary: "Processes, threads and objects: how a request moves through a node, how nodes share shards, and what happens when one fails."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: api, label: "XRPC (axum)", at: [0, 0], size: [8, 3] }
    - { id: router, label: "Repo router", at: [11, 0], size: [8, 3], tone: accent }
    - { id: log, label: "Node log", at: [22, 0], size: [8, 3], tone: accent }
    - { id: s3, label: "Object store", at: [33, 0], size: [8, 3], tone: amber, shape: store }
  edges:
    - "api -> router"
    - "router -> log"
    - "log -> s3"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Components of a node

<!-- Sources: DESIGN.md "Architecture" diagram; src/server.rs (build), src/worker.rs, src/nodelog.rs, src/firehose.rs, src/partition.rs -->

TODO.

## Request routing and forwarding

<!-- Sources: DESIGN.md "Forwarding deadlines and not-applied writes"; src/forward.rs; RUNBOOK "Background you need" (Forwarding) -->

TODO.

## Shards and ownership

<!-- Sources: DESIGN.md "HA: multiple nodes, partitioned write ownership" (Shards, Assignments); src/slots.rs, src/cluster.rs. Keep the #shards-and-ownership anchor: overview links it -->

TODO.

## Leases

<!-- Sources: DESIGN.md "Node leases", "Liveness: observed lease changes on the observer's monotonic clock"; RUNBOOK lease timings (TTL/5 renew, 0.8×TTL validity, 1.2×TTL presumed dead) -->

TODO.

## Failure and takeover

<!-- Sources: DESIGN.md "Handoff", "Crash takeovers", "Why safety needs no clocks", fencing; bench/ha/RESULTS.md. Keep the #failure-and-takeover anchor: overview links it -->

TODO.

## Handoff, handback and joining

<!-- Sources: DESIGN.md "Handback to a joiner", §5 "Joining", prewarm ("Moved shards start with warm caches") -->

TODO.

## Threads and runtimes

<!-- Sources: repo worker threads, IO runtime, firehose runtime (--firehose-threads), blocking pool; lifecycle::critical fail-stop -->

TODO.

## Single-node mode

<!-- Sources: DESIGN.md "Lone-node control plane" -->

TODO.
