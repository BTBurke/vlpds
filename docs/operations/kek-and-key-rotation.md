---
title: KEK and key rotation
section: Operations
order: 106
status: stub
summary: "Provisioning and rotating the key-encryption key (local or Cloud KMS), the PLC rotation key and the operator recovery key, and surviving a KMS outage."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: kms, label: "Cloud KMS", at: [0, 0], size: [8, 3], tone: amber }
    - { id: node, label: "vlpds", at: [12, 0], size: [7, 3], tone: accent }
    - { id: rows, label: "wrapped secrets", at: [23, 0], size: [9, 3], tone: amber, shape: store }
  edges:
    - "node -> kms: unwrap"
    - "node -> rows"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## KEK provisioning

<!-- Sources: RUNBOOK "KEK provisioning"; deploy/gcp OpenTofu for KMS -->

TODO.

## KEK rotation

<!-- Sources: RUNBOOK "KEK rotation" -->

TODO.

## Key service outage

<!-- Sources: RUNBOOK "Key service (KMS) outage"; VlpdsKeyServiceUnavailable -->

TODO.

## PLC rotation key

<!-- Sources: RUNBOOK provisioning + rotation; --wrap-plc-rotation-key -->

TODO.

## Operator recovery key

<!-- Sources: RUNBOOK "Operator recovery key"; backfill -->

TODO.

## Repo signing-key rotation

<!-- Sources: DESIGN "Signing-key rotation" -->

TODO.
