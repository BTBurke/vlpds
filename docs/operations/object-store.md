---
title: Object store
section: Operations
order: 102
status: stub
summary: "Choosing and preparing a bucket: S3, R2, GCS or MinIO, the bucket probe, lifecycle rules, prefixes, and what each provider costs."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: probe, label: "vlpds-bucket-probe", at: [0, 0], size: [10, 3], tone: accent }
    - { id: b, label: "bucket / prefix", at: [14, 0], size: [9, 3], tone: amber, shape: store }
  edges:
    - "probe -> b: conditional PUT, CAS, LIST"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## What vlpds needs from a store

<!-- Sources: If-None-Match / If-Match conditional writes, LIST consistency (DESIGN "Choosing a bucket") -->

TODO.

## Running the probe

<!-- Sources: src/bin/vlpds-bucket-probe.rs -->

TODO.

## Bucket layout

<!-- Sources: RUNBOOK "Background you need" prefix list (log/, state/, assign/, nodes/, writers/, retain/, cluster/version, handle/, email/, blob/) -->

TODO.

## Lifecycle rules

<!-- Sources: abort incomplete multipart uploads -->

TODO.

## Cost by provider

<!-- Sources: bench/results/cost-model-2026-10-02, tiny-pds-idle-2026-10-02 (round numbers) -->

TODO.

## Latency and failure

<!-- Sources: object-store outage / brownout behaviour (RUNBOOK "Object-store outage") -->

TODO.
