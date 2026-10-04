---
title: Deploy
section: Operations
order: 101
status: ready
summary: "A single node with Ansible, as a worked example: bucket, DNS, secrets, Caddy, the tiny profile and the first-run checks."
---

```hero
diagram:
  caption: "The single-node example: one small VPS runs vlpds, Caddy and Alloy under Docker Compose. The bucket holds all the data; the operator console and metrics only go over a private network."
  nodes:
    - { id: apps, label: Apps · relays, sub: HTTPS, at: [0, 1], size: [8, 3] }
    - { id: op, label: Operator, sub: on the private network, at: [0, 6], size: [8, 3] }
    - { id: cf, label: Cloudflare DNS, sub: "_acme-challenge TXT", at: [12, -6.5], size: [8, 2.6], tone: muted }
    - { id: caddy, label: Caddy, sub: ":443 · *.handle cert", at: [12, 1], size: [8, 3] }
    - { id: ts, label: tailscale serve, sub: ":8443 → /admin", at: [12, 6], size: [8, 3] }
    - { id: vlpds, label: vlpds, sub: "tiny · 1 shard\n3 GiB limit", at: [24, 1], size: [9, 8], tone: accent }
    - { id: alloy, label: Alloy, sub: unprivileged, at: [24, 12], size: [9, 3] }
    - { id: r2, label: R2 bucket, sub: one prefix, at: [38, 3.5], size: [9, 3], shape: store, tone: amber }
    - { id: mon, label: Monitoring, sub: metrics · logs, at: [38, 12], size: [9, 3], tone: muted }
  groups:
    - { label: "small VPS · 2 vCPU / 4 GB", around: [caddy, ts, vlpds, alloy], tone: accent }
  edges:
    - "apps -> caddy: HTTPS"
    - "op -> ts: tailnet"
    - { from: caddy.t95, to: cf.b95, label: DNS-01, labelAt: [19.6, -2.4] }
    - caddy -> vlpds
    - ts -> vlpds
    - "vlpds -> alloy: /metrics"
    - "vlpds -> r2: S3 API"
    - "alloy -> mon: tailnet"
facts:
  - { value: "2 vCPU", unit: "/ 4 GB", label: runs a personal PDS, note: "a small VPS with a 3 GiB container limit" }
  - { value: "$0", unit: /mo, label: object store on R2, note: "tiny profile idles at ~0.3 M Class A/mo (measured)", tone: amber }
  - { value: "60 s", label: lease TTL on tiny, note: "a crash waits ~53 s to write again; SIGTERM ~1 s", tone: violet }
  - { value: "5–30 s", label: of 502s per upgrade, note: one node means every restart is a short outage, tone: rust }
```

This page deploys one vlpds node with the Ansible kit in [`deploy/ansible/`](https://github.com/jazware/vlpds/tree/main/deploy/ansible) (playbooks, the
`vlpds`, `common`, `caddy` and `alloy` roles and an example inventory), using a single small VPS serving `pds.example.com` as the worked example. One node owns every shard, so the
setup is simple and cheap. The node keeps nothing on its disk that it can't rebuild, so moving to
more nodes later is a matter of starting another one on the same bucket and prefix. For that, see
[Scaling and clustering](scaling-and-clustering.md#with-the-ansible-role).

## What you need

| | What | Notes |
|---|---|---|
| Host | Linux with Docker, 2+ vCPU, 4+ GB RAM, ~15 GB free disk | The tiny profile's disk cache is 4 GiB on the root disk, and the deploy keeps 10 GiB free beyond it. |
| Bucket | S3, R2 or GCS (MinIO for tests) that passes `vlpds-bucket-probe` | Needs strongly consistent conditional writes. See [Object store](object-store.md). |
| DNS | `A` for the hostname, `A` for `*.<handle domain>` | Both point at the host. Handles resolve over HTTPS at `https://<handle>/.well-known/atproto-did`. |
| Secrets | JWT secret, admin token, internal token, S3 key pair, KEK (or Cloud KMS), PLC rotation key | The three tokens are 32+ bytes and must all differ. The KEK and the PLC key are **not** in the bucket: back them up offline. |
| Mail | an SMTP URL and a sender domain with SPF/DKIM | Optional. Without it, verification and 2FA mails only go to the log. See [Email and moderation](email-and-moderation.md). |

The node only listens on `127.0.0.1`: `:2583` for the app and `:9583` for metrics. Caddy terminates
TLS in front of it and blocks the operator paths (`/admin`, `/xrpc/vlpds.admin.*`, `/metrics`,
`/internal/*`).

## Worked example: one small VPS

The example host is a 2 vCPU / 4 GB VPS with a 40 GB disk, serving `pds.example.com` and handles
under `*.pds.example.com`. Its DNS is on Cloudflare, its bucket is on R2, and it sits on a private
network (Tailscale here) that also reaches your monitoring stack. Four roles set it up:

```steps
- title: "roles/common: harden and join the private network"
  body: "Docker from download.docker.com, keys-only sshd, ufw (22, 80, 443 and everything on `tailscale0`), fail2ban, security updates without automatic reboots. Give the node a Tailscale tag whose ACL lets it reach only your monitoring host. If the provider hands you a password login, run `playbooks/bootstrap.yml` once first to move to keys."
- title: "roles/vlpds: the node"
  body: "chrony, sysctls, the 4 GiB disk cache on the root disk, secret files (0400, uid 10001, mounted read-only at `/run/vlpds`), `/opt/vlpds/docker-compose.yml`, the bucket probe, start, verify. With `vlpds_tailnet_console_port: 8443` the console is served on the private network by `tailscale serve`."
- title: "roles/caddy: TLS"
  body: "Caddy with the caddy-dns/cloudflare module, built on the box. It holds a certificate for `pds.example.com` and one wildcard for `*.pds.example.com`, issued by ACME DNS-01 through a Cloudflare token scoped to the zone. Before proxying a handle request, `forward_auth` asks vlpds' `/tls-check`, so unknown names get an empty 404."
- title: "roles/alloy: metrics and logs"
  body: "Alloy scrapes `127.0.0.1:9583` every 10 s as `job=\"vlpds\"` with the node's name as `instance`, and ships journal and container logs to your Prometheus-compatible / Loki stack (`alloy_monitoring_url`) over the private network. It runs as the plain `alloy` user and reaches Docker only through a filtered read-only proxy, so it can read neither the secrets nor the containers' environment."
```

Copy `inventories/example/` to `inventories/<name>/` (a `hosts.yml`, `group_vars/vlpds.yml`,
`group_vars/all/` and the sops secrets). The kit's `README.md` has the exact commands. These are the choices the example makes:

| Setting | Example | Why |
|---|---|---|
| Host | a small VPS, 2 vCPU / 4 GB / 40 GB NVMe, a current Ubuntu LTS | A personal PDS doesn't need more. |
| Profile | `tiny`, `vlpds_mem_limit_mb: 3072`, 2 I/O threads, 1 worker | Caches size themselves from the 3 GiB limit. ~0.8 GiB stays for the OS, Caddy (~60 MiB) and Alloy (~250 MiB). |
| Object store | R2 bucket `<your-bucket>`, `vlpds_s3_prefix` set to a name for this PDS | Add the lifecycle rule that aborts incomplete multipart uploads. Scope the API token to this bucket and, where the provider allows it, to the host's IP. |
| DNS | `pds.example.com` and `*.pds.example.com`, DNS-only (grey cloud) | Cloudflare's proxy would cap uploads at 100 MB and close the firehose's idle websockets. |
| Secrets | sops: `group_vars/<group>.sops.yaml` beside (not inside) `group_vars/<group>/`, decrypted by the `community.sops` vars plugin | The role writes each one to a file and passes only `VLPDS_*_FILE`, so `docker inspect` shows no secret. |
| KEK | Cloud KMS (`vlpds_gcp_kms_key`) with a service-account key | Nothing on the box can unwrap a signing key without KMS. A local KEK works too and can be rewrapped onto KMS later. |
| Identity | live plc.directory, operator recovery key in every DID | `vlpds_plc_recovery_did_key`; the private half is offline. See [KEK and key rotation](kek-and-key-rotation.md#operator-recovery-key). |
| Console | `https://<node>.<your-tailnet>.ts.net:8443/admin` | `tailscale serve` terminates TLS on the private network only. Caddy can't do this: traffic it gets from Docker comes from the bridge gateway, not a private-network address. |
| Relays | `vlpds_crawlers: ["bsky.network"]` | Set once the first account is verified, so restarts re-announce. |
| Email | off (`vlpds_email_required: false`) | Mail is logged until SMTP credentials exist. |

## Profiles

The role has two profiles. They differ in shard count, lease TTL and the fixed memory costs; caches
size themselves from the memory budget in both (see
[Configuration](configuration.md#memory-budget-and-autosizing)).

| | `tiny` | `standard` |
|---|---|---|
| For | a personal or small community PDS on a small VM | the production sizing: 6–8 cores / 32 GB / NVMe |
| `--shards` | 1 | 64 |
| `--lease-ttl-ms` | 60,000 (renew every 12 s) | 10,000 (renew every 2 s) |
| `--slatedb-manifest-poll` | 60 s | 10 s |
| Container memory limit | 2.5 GiB (the example: 3 GiB) | 85% of RAM |
| Firehose ring / merge queue / backfill cache | 64 / 32 / 32 MiB | 512 / 256 / 256 MiB |
| getRepo exports, cursor backfills at once | 4, 4 | 32, 16 |
| Disk cache | 4 GiB on the root disk, plus 10 GiB kept free | 80% of a dedicated NVMe (`auto`) |
| Idle object-store cost, one node | ~$0 on R2, ~$2/mo on S3 | ~$50/mo: per-shard polling (measured) |

The tiny profile's lease is the trade-off to know about. A lone node has no peer to take over faster,
so a longer lease only saves requests. But after a **crash** (SIGKILL, OOM, power loss) the restarted
node can't write for about one TTL (53 s measured at 60 s), while a graceful restart is ~1 s at any
TTL. Details: [Configuration](configuration.md#shards-and-lease-ttl).

## First deploy

```steps
- title: Bucket
  body: "Create it, add the lifecycle rule that aborts incomplete multipart uploads after 1 day, and make a key pair for this bucket only. One prefix is one PDS: never point two deployments at the same prefix. See [Object store](object-store.md)."
- title: Image
  body: "`just docker-push <tag> <registry>/vlpds` builds `linux/amd64` and pushes `<registry>/vlpds:<tag>`. Pin `vlpds_image` to that tag, never `:latest`. The image includes `vlpds-bucket-probe`."
- title: Secrets
  body: "`openssl rand -hex 32` for the JWT secret, admin token and internal token (three different values), and for a local KEK. Or use Cloud KMS ([KEK and key rotation](kek-and-key-rotation.md#kek-provisioning)). Back up the KEK offline: it wraps every signing key and is not in the bucket."
- title: PLC rotation key
  body: "`docker run --rm -i -e VLPDS_KEK <image> --wrap-plc-rotation-key </dev/null` prints a new key wrapped under the KEK on stdout and its did:key on stderr. Moving an existing PDS here? Wrap its existing key instead (pipe the hex in), so DID documents that list it keep working."
- title: DNS
  body: "`A` records for the hostname and `*.<handle domain>`. Caddy gets the hostname's certificate at start, so it must resolve first. For handles, prefer one wildcard certificate (`vlpds_caddy_wildcard_dns: cloudflare`): per-handle on-demand certificates count against Let's Encrypt's ~50 per week, and a new handle's first request waits a few seconds, long enough for the AppView to cache `handle.invalid`."
- title: Dry run, then run with the probe
  body: "`ansible-playbook -i inventories/<inv>/hosts.yml playbooks/vlpds.yml --check --diff`, then the same with `-e vlpds_preflight_probe=true`. The role refuses to start vlpds unless the probe says `SAFE for vlpds`. On a fresh host `--check` stops at \"Install Docker\", because the apt repository doesn't exist yet."
- title: First account
  body: "`docker exec vlpds vlpds admin create-invite-code`, then sign up or [migrate](../migration.md) an account. When it checks out, announce the server: `docker exec vlpds vlpds admin request-crawl bsky.network`, and set `vlpds_crawlers` so restarts re-announce."
```

Restarts are graceful and only happen when something changed: the compose file, a secret file or the
image (or `-e vlpds_force_restart=true`). Compose sends SIGTERM and waits 90 s
(`vlpds_stop_grace_period`). The node hands its shards back, fences its own log and exits 0. Never
`docker kill` it, and never run a second copy with the same `--node-id`: two processes with one node
id fence each other.

## Verify

The playbook's last tasks run these checks and print a summary. They are also the checks to repeat by
hand after any change.

```steps
- title: Health and cluster status
  body: "`/xrpc/_health` answers, then `vlpds.admin.getClusterStatus` shows a valid lease and every shard owned. The summary prints `lease_valid: true`, `shards_owned: 1 / 1`, the feature level, the build and `previous_exit`."
- title: What Caddy depends on
  body: "`/tls-check?domain=<hostname>` is 200 and an unknown handle is 404; `/.well-known/did.json` is the service DID's document (`did:web:<hostname>`; the Bluesky app's video upload asks for service auth with that audience)."
- title: The previous exit
  body: "`previous_exit` should be `clean`. Anything else is a fail-stop or a crash: look it up in the [runbook](runbook.md#exit-codes-and-fail-stops)."
- title: From outside
  body: "`curl https://<hostname>/xrpc/com.atproto.server.describeServer`, and a test handle's `https://<handle>/.well-known/atproto-did`. In the logs: the `secrets at rest` line (the KEK id) and `PLC registration on` (the rotation key's did:key)."
- title: Metrics
  body: "`up{job=\"vlpds\", instance=\"<node id>\"}` in Grafana, and the \"vlpds\" dashboard for the cluster and node. `VlpdsNotScraped` clears once the series arrive. See [Monitoring](monitoring.md)."
```

A test account created this way registers a **real, permanent** `did:plc` on plc.directory. Delete it
afterwards with `vlpds admin account delete <did>` if it was only a test.

## Without Ansible

The image is the whole deployment: one static binary and its web UI (`/usr/share/vlpds/ui`), running as uid 10001
under `tini`. Every flag has a `VLPDS_*` environment variable, and every secret has a `_FILE` form.
This is the smallest production-shaped run:

```bash
docker run -d --name vlpds --restart unless-stopped --stop-timeout 90 \
  --memory 3g -p 127.0.0.1:2583:2583 -p 127.0.0.1:9583:9583 \
  -v /opt/vlpds/secrets:/run/vlpds:ro -v /var/lib/vlpds-cache:/var/lib/vlpds/cache \
  -e VLPDS_PUBLIC_URL=https://pds.example.com \
  -e VLPDS_HANDLE_DOMAIN=pds.example.com \
  -e VLPDS_SERVICE_DID=did:web:pds.example.com \
  -e VLPDS_S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com -e VLPDS_S3_REGION=auto \
  -e VLPDS_S3_BUCKET=my-pds -e VLPDS_PREFIX=my-pds \
  -e VLPDS_S3_ACCESS_KEY_FILE=/run/vlpds/s3-access-key \
  -e VLPDS_S3_SECRET_KEY_FILE=/run/vlpds/s3-secret-key \
  -e VLPDS_JWT_SECRET_FILE=/run/vlpds/jwt-secret \
  -e VLPDS_ADMIN_TOKEN_FILE=/run/vlpds/admin-token \
  -e VLPDS_INTERNAL_TOKEN_FILE=/run/vlpds/internal-token \
  -e VLPDS_KEK_FILE=/run/vlpds/kek \
  -e VLPDS_PLC_ROTATION_KEY_FILE=/run/vlpds/plc-rotation.key \
  -e VLPDS_NODE_ID=single -e VLPDS_SHARDS=1 -e VLPDS_LEASE_TTL_MS=60000 \
  -e VLPDS_SLATEDB_MANIFEST_POLL=60s \
  -e VLPDS_CACHE_DIR=/var/lib/vlpds/cache -e VLPDS_DISK_CACHE_MB=4096 \
  -e VLPDS_METRICS_LISTEN=0.0.0.0:9583 -e VLPDS_LOG_FORMAT=json \
  <registry>/vlpds:<tag>
```

- Put a TLS proxy in front that serves the hostname and `*.<handle domain>`, sends everything to
  `127.0.0.1:2583`, and blocks `/admin`, `/xrpc/vlpds.admin.*`, `/metrics` and `/internal/*`. For
  on-demand certificates, use `http://127.0.0.1:2583/tls-check` as the `ask` URL. List the proxy in
  `--trusted-proxies` so rate limits see the real client address.
- The restart policy matters: vlpds fail-stops on purpose (exit codes 2–9) and expects its
  supervisor to start it again.
- Missing or weak secrets stop the node at start. Outside `--dev-mode` it refuses an empty secret, a
  secret shorter than 32 bytes, two tokens that are the same, a missing KEK, and the MinIO default
  credentials.
- `vlpds --memory-plan` with the same flags prints how the memory budget will be split, and exits
  non-zero if the flags don't fit.

For a local server to click around in, `just dev` builds the UI and runs an in-memory node on
`127.0.0.1:2620` in dev mode (admin token `dev-admin-token`; nothing persists). `just seed` adds
accounts and records.
