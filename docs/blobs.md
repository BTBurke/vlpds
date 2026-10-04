---
title: Blobs
section: vlPDS
order: 5
status: stub
summary: "Images and video: streamed uploads into the bucket, reference tracking with each commit, and a garbage collector that can't race a write."
---

```hero
diagram:
  caption: "Placeholder: replace with this page's at-a-glance diagram (see docs/_style.md)."
  nodes:
    - { id: up, label: "uploadBlob", at: [0, 0], size: [8, 3] }
    - { id: tmp, label: "blob-tmp/", at: [11, 0], size: [8, 3], tone: amber, shape: store }
    - { id: blob, label: "blob/{did}/{cid}", at: [22, 0], size: [9, 3], tone: amber, shape: store }
    - { id: gc, label: "blob-gc/", at: [22, 5], size: [9, 3], tone: muted, shape: store }
  edges:
    - "up -> tmp: multipart"
    - "tmp -> blob"
    - "blob ~> gc: unreferenced"
facts:
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
  - { value: "?", label: "TODO: key fact or round number", tone: muted }
```

## Upload path

<!-- Sources: DESIGN.md §6; multipart to blob-tmp/; user service auth for video (DESIGN "User service auth on uploadBlob") -->

TODO.

## References

<!-- Sources: b/ rows written with the commit; legacy refs from importRepo -->

TODO.

## Garbage collection

<!-- Sources: blobs::sweep_blobs, --blob-gc-grace-secs, move to blob-gc/, settle, HeldBlobs -->

TODO.

## Bucket lifecycle rule

<!-- Sources: abort incomplete multipart uploads after 1 day (S3/R2 JSON from DESIGN §6); MinIO needs none -->

TODO.

## Serving blobs

<!-- Sources: getBlob, CDN pattern, takedowns -->

TODO.

## Sizing

<!-- Sources: ~350 TB for all of Bluesky, priced separately (cost model) -->

TODO.
