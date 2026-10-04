---
title: Monitoring
section: Operations
order: 104
status: stub
summary: "Metrics, dashboards and alerts: what a healthy node looks like, the handful of graphs to watch, and how alerts map to the runbook."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: node, label: "vlpds /metrics", at: [0, 0], size: [9, 3], tone: accent }
    - { id: alloy, label: "Alloy", at: [13, 0], size: [6, 3] }
    - { id: prom, label: "Prometheus + Grafana", at: [23, 0], size: [10, 3], tone: blue }
  edges:
    - "node -> alloy: scrape"
    - "alloy -> prom"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## What to watch

<!-- Sources: commit latency, firehose emit delay, lease renew ratio, object-store latency/errors, memory -->

TODO.

## Metrics endpoint

<!-- Sources: --metrics-listen, /debug/pprof; vlpds_* naming -->

TODO.

## Dashboards

<!-- Sources: provisioned by ansible (lab monitoring stack); the operator console's Metrics page -->

TODO.

## Alerts

<!-- Sources: ops/alerts.yml; each links RUNBOOK; group them by area -->

TODO.

## Logs

<!-- Sources: JSON logs, exit codes table (RUNBOOK "Tools") -->

TODO.

## Object-store request accounting

<!-- Sources: vlpds_object_store_requests_total{op,component} -->

TODO.
