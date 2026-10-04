---
title: Deploy
section: Operations
order: 101
status: stub
summary: "A single node with Ansible (vlpds-node1 as the worked example): bucket, DNS, secrets, Caddy, the tiny profile and the first-run checks."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: ans, label: "ansible", at: [0, 0], size: [7, 3] }
    - { id: host, label: "host: compose", at: [10, 0], size: [9, 3], sub: "vlpds + Caddy + Alloy", tone: accent }
    - { id: bucket, label: "R2 bucket", at: [23, 0], size: [8, 3], tone: amber, shape: store }
  edges:
    - "ans -> host: playbooks/vlpds.yml"
    - "host -> bucket"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## What you need

<!-- Sources: a host, a bucket (R2/S3/GCS), DNS for the hostname and *.handle domain, secrets -->

TODO.

## Worked example: vlpds-node1

<!-- Sources: inventories/<name> (tiny profile, R2, Cloudflare DNS-01 wildcard, tailnet console) -->

TODO.

## Profiles

<!-- Sources: deploy/ansible/roles/vlpds/README.md "Profiles" (tiny vs standard) -->

TODO.

## First deploy

<!-- Sources: roles README "First deploy"; bucket probe (-e vlpds_preflight_probe=true) -->

TODO.

## Verify

<!-- Sources: tasks/verify.yml checks: _health, getClusterStatus, /tls-check, did.json -->

TODO.

## Without Ansible

<!-- Sources: Dockerfile / docker run env vars; just dev for a local server -->

TODO.

## Taking over an existing PDS hostname

<!-- Sources: roles README "Cutover" and rollback playbooks -->

TODO.
