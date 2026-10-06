---
title: Admin console and CLI
section: Operations
order: 111
status: ready
summary: "The operator console (on the tailnet) and the admin CLI: accounts, invites, handle domains, takedowns, rate limits, relays, firehose subscribers, cluster status and metrics."
---

```hero
diagram:
  caption: "Both are clients of admin XRPC with the admin token, on any node; calls about one account are routed to its owner. Neither is ever exposed through the public proxy: the console is reached over the tailnet or an SSH tunnel."
  nodes:
    - { id: op, label: Operator, sub: admin token, at: [0, 3.2], size: [7, 3] }
    - { id: ui, label: "/admin console", sub: "tailnet · SSH tunnel", at: [11, 0], size: [9, 3], tone: accent }
    - { id: cli, label: vlpds admin, sub: "CLI · same binary", at: [11, 6.4], size: [9, 3], tone: accent }
    - { id: api, label: admin XRPC, sub: "com.atproto.admin.* · vlpds.admin.*", at: [24, 3.2], size: [12, 3], tone: blue }
    - { id: owner, label: owning node, sub: forwarded by DID, at: [40, 3.2], size: [8, 3], tone: accent }
    - { id: caddy, label: Caddy, sub: "blocks /admin, vlpds.admin.*", at: [24, 9.4], size: [12, 2.6], tone: muted }
  edges:
    - { from: op.r, to: ui.l }
    - { from: op.r, to: cli.l }
    - "ui.r -> api.l30: Basic auth"
    - cli.r -> api.l70
    - "api -> owner: per DID"
    - { from: caddy.t, to: api.b, label: never public, dash: true, arrow: none }
facts:
  - { value: "9", unit: pages, label: in the console, note: "cluster, live metrics, accounts, moderation, invites, handle domains, rate limits, relays, firehose" }
  - { value: "2 s", label: cluster view refresh, note: "getClusterStatus polled from the node you opened", tone: blue }
  - { value: "pdsadmin", label: every command covered, note: "plus the reference's maintenance scripts and cluster ops", tone: violet }
  - { value: "0", label: direct bucket access, note: "the CLI needs only a node URL and the admin token", tone: amber }
```

vlpds has two operator tools, and both talk admin XRPC to a node. Every node serves the web console
at `/admin` from the same binary, and the `vlpds admin` CLI ships in that binary too. The console is
for looking around and for one-off account work. The CLI covers everything the reference's
`pdsadmin` and maintenance scripts do, plus cluster and key operations, and it's what you script.

## Reaching the console

```diagram
caption: "Two ways in. Caddy refuses `/admin`, `/admin/*`, `/xrpc/vlpds.admin.*`, `/metrics` and `/internal/*` from the internet, so both paths reach the node's port on the host directly."
nodes:
  - { id: lap, label: Your laptop, sub: browser, at: [0, 3.9], size: [8, 3] }
  - { id: ts, label: tailscale serve, sub: "https · tailnet only", at: [13, 0], size: [10, 2.6], tone: muted }
  - { id: ssh, label: SSH tunnel, sub: "-L 2583:127.0.0.1:2583", at: [13, 7.8], size: [10, 2.6], tone: muted }
  - { id: app, label: "vlpds :2583", sub: "/admin · admin XRPC", at: [28, 4.2], size: [9, 2.6], tone: accent }
  - { id: met, label: "vlpds :9583", sub: "/metrics", at: [28, 0], size: [9, 2.6], tone: accent }
edges:
  - { from: lap.r, to: ts.l }
  - { from: lap.r, to: ssh.l }
  - { from: ts.b, to: app.l30, via: [[18, 4.98]] }
  - { from: ts.r, to: met.l, label: "/metrics", dash: true }
  - { from: ssh.r, to: app.l70 }
```

- On the tailnet. With `vlpds_tailnet_console_port` set, the Ansible role runs `tailscale serve` on
  that port. tailscaled terminates TLS and listens on the tailnet only. `/` goes to the node and
  `/metrics` to its metrics listener, so the Live metrics page works. Open
  `https://<tailnet name>:<port>/admin`. Don't use port 443, because Docker's DNAT for Caddy takes it.
- Over SSH. Run `ssh -L 2583:127.0.0.1:2583 <host>` and open `http://localhost:2583/admin`.
  Everything works except Live metrics, which needs `/metrics` on the same origin.
- Unlock it with the node's `--admin-token`. The console checks the token with a
  `getClusterStatus` call and keeps it in that browser tab only. Lock console forgets it.

Every node serves the console, and any node will do. The Cluster page is that node's view of the
cluster, and account pages are routed to each account's owner.

## Pages

```facts
- { value: Cluster, label: "/admin", note: "ownership map, nodes, firehose sources, feature level · every 2 s", tone: accent }
- { value: Metrics, label: "/admin/metrics", note: "charts scraped from /metrics every 2 s, last 6 min", tone: blue }
- { value: Accounts, label: "/admin/accounts", note: "search, details, takedown, handle, email, password, delete", tone: violet }
- { value: Limits, label: "/admin/ratelimits", note: "live 429s, top keys, buckets and overrides, cluster-wide", tone: amber }
```

| Page | What it shows | What you can do |
|---|---|---|
| Cluster | Nodes with a lease, shards owned by this node, its lease, durable log ordinal and feature level (with a finalize or mixed-builds banner) · a shard ownership map coloured by node · a node table (reachable, lease, owned, durable ordinal, firehose lag with the slowest log marked, build and level window) · the firehose's sources | Read only. Click a node to highlight its shards, or its address or build to copy it. |
| Live metrics | Commits and record ops, HTTP requests by method, commit to durable, segment PUT latency, firehose, cold repo loads, forwarded requests, rejected requests, memory | Read only. Needs `/metrics` on the console's origin. Buttons at the bottom download the [Grafana dashboards](monitoring.md#import-into-your-own-grafana). |
| Accounts | Search by email prefix (every node's shards, paged), or jump by handle or DID · an account's details and moderation status, with the date a [scheduled deletion](email-and-moderation.md#scheduled-deletion) is due · the dev mailbox in `--dev-mode` | The handle opens the account, and a DID or email copies when you click it · take down (with a reference) and reverse it · change handle, email or password · enable or disable its invites · reset its two-factor sign-in (passkeys, TOTP, recovery codes and trusted browsers, with a reason, audited and mailed to the user) · delete (type the handle to confirm). |
| Moderation | Look up a subject from a bsky.app URL, at:// URI, handle, DID or DID + blob CID, and see the account, the record's JSON and its blobs (previews load on request, blurred) · active takedowns by kind · cases · the audit log · accounts over their blob quota | Take down or restore an account, record or blob with a reason, filed under a case · open and update cases (notes, status, subjects) · change an account's blob quota. See [Email and moderation](email-and-moderation.md#operator-moderation). |
| Handle domains | The primary (`--handle-domain`) and the domains added here, each with its active accounts, when it was added and by whom | Add a domain · remove one (refused while it has active accounts, with Remove anyway to force it). See [Handle domains](handle-domains.md). |
| Invite codes | Every code, newest first, one per row: uses left out of its total, when it was made, who made it and for whom, who used it, and whether it's active, disabled or used up | Create codes (count, uses, for an account) · copy a code, or a usable code's migrate or sign-up link · disable one code, or select several and disable them together. |
| Rate limits | Each bucket's busiest key, 429s in the last minute and 15 minutes, a 429/s chart, top keys, recent 429s by route, and each node's applied config version | Change a bucket's points, window or on/off · add routes · add IP, CIDR or DID overrides (exempt or a custom limit) · a global off switch. Changes apply to every node within seconds and are kept, with your name, in the last 50 changes. See [Rate limits](rate-limits.md#changing-limits-live). |
| Relays | The relays asked to crawl this PDS (`--crawlers`, or a list stored from here), each one's last ask and result, and the minimum interval | Add or remove relays, reset to the flag's list, change the interval, request a crawl now · click a relay to copy it. See [Relays and crawling](relays-and-crawling.md#crawl-requests). |
| Firehose | Every subscribeRepos connection on every node, every 5 s: subscribers, live vs backfilling, this PDS's events/s and the bytes/s sent · per connection its `#conn` number and node, client address with its AS and verified reverse DNS name, the relay it matched, user agent (first 120 characters), how long it's been connected, start cursor and shard, state, lag, events/s against the PDS's rate, and events and bytes sent · the last 50 disconnects per node with their reason | Read only. Click a client address, reverse DNS name or user agent to copy it. A live connection well under the PDS's events/s is falling behind. |

The rate-limit config lives in the bucket (`config/ratelimits.json`), so it survives restarts and
every node reads the same one. `{}` means the built-in defaults. The relay list lives next to it in
`config/crawlers.json`, and the added handle domains in `config/handle-domains.json`.

Each node serves its own firehose subscribers, so the Firehose page calls
`vlpds.admin.listFirehoseSubscribers` on the node you opened and that node asks its peers over the
peer listener. It lists up to 500 connections, oldest first, and the counts cover all of them. A peer
that doesn't answer is named at the top. The relay column is a hint. vlpds resolves the hostnames of
the configured relays every 5 minutes in the background and names a relay when the client's address
is one of them or its user agent contains the hostname. A relay that connects from other addresses
and doesn't name itself shows up unnamed, unless its reverse DNS name is under the relay's hostname
and resolves back to its address. The client cell shows the address, its AS from bgp.tools (a link to
the AS page; `--asn-lookup off` hides it) and that forward-confirmed reverse DNS name. A name that
doesn't resolve back is only in the tooltip, marked unverified, because anyone can put any name in
their reverse zone. Both are looked up in the background, so a new address shows them on a later
refresh. The `#conn` number is the `conn` label of
`vlpds_firehose_subscriber_events_total`, so a line on the dashboard and a row here can be matched
(see [Per-connection firehose series](monitoring.md#per-connection-firehose-series)).

## Admin CLI

```diagram
caption: "`vlpds admin` is the same binary in client mode. Per-account calls go to any node and are routed to the owner. Per-node maintenance runs on every node `getClusterStatus` lists, and shards that moved mid-run are rerun on their new owner."
nodes:
  - { id: cli, label: vlpds admin, sub: "--url · token", at: [0, 3], size: [8, 3], tone: accent }
  - { id: any, label: any node, sub: "account · invites · layout", at: [13, 0], size: [10, 3], tone: accent }
  - { id: every, label: every node, sub: "rewrap · rotate-plc-keys", at: [13, 6], size: [10, 3], tone: accent }
  - { id: own, label: owner of the DID, sub: forwarded, at: [28, 0], size: [9, 3], tone: blue }
  - { id: cover, label: shards covered, sub: "missing → rerun", at: [28, 6], size: [9, 3], shape: note, tone: muted }
edges:
  - cli.r -> any.l
  - cli.r -> every.l
  - "any -> own: per DID"
  - every -> cover
```

```bash
export VLPDS_ADMIN_TOKEN=...                # or --admin-token-file, as the node reads it
vlpds admin --url http://127.0.0.1:2583 cluster status
docker exec vlpds vlpds admin account list  # inside the container: no token argument needed
```

`--url` (default `http://127.0.0.1:2583`, env `VLPDS_URL`), the token and `--json` can go before or
after the command. Output is a table or a short message, and `--json` prints the raw results. It
exits 1 on an XRPC error or on any failed item in a batch. `account delete` and `rebuild-repo` ask
first, and refuse to run off a terminal without `--yes`.

| Group | Commands |
|---|---|
| Accounts (`pdsadmin account …`) | `account list [--email PREFIX]`, `create EMAIL HANDLE`, `delete DID`, `takedown DID [--ref R]`, `untakedown DID`, `reset-password DID`, `info DID` |
| Invites and relays | `create-invite-code [--uses N] [--count N] [--for-account DID] [--handle-domain D]`, `request-crawl [RELAY,…]` |
| Handle domains | `handle-domain list`, `handle-domain add DOMAIN`, `handle-domain remove DOMAIN [--force]` |
| Identity | `publish-identity [DID…] [--file F]`, `rotate-keys [DID…] [--generate]`, `rotate-plc-keys`, `ensure-recovery-key` |
| Repos | `check-repo DID`, `rebuild-repo DID [--dry-run]` |
| Secrets | `rewrap-secrets [--dry-run] [--check-versions]` |
| Cluster | `cluster status`, `cluster finalize [--level N]`, `cluster lower --level N`, `layout`, `shard-split`, `shard-merge`, `reshard-abort` |
| Peer TLS | `tls ca`, `tls issue`, `tls show` (local files only, no node involved) |

- `check-repo` reads one shard snapshot, so it works on a repo that won't load. It checks the head
  commit and its signature, every record's hash, the MST rebuilt from the records against the head,
  the persisted interior nodes and the indexes. Node or index problems heal on the next cold load,
  so they aren't an emergency.
- `rebuild-repo` re-derives the repo from its records under a new signed commit and a `#sync`. It
  refuses when the records no longer rebuild to the head, which means records were lost and you
  need a restore. If a write lands in between, it fails with `InvalidSwap`, so run it again.
- Batches (`publish-identity`, `rotate-keys`) take one DID per line from a file and run one at a
  time. The PLC directory rate-limits, so keep `rotate-keys` to a few in flight per IP.
- `pdsadmin update` has no counterpart (roll the image instead, see
  [Upgrades](upgrades.md#rolling-deploy)). Neither do the sequencer-recovery scripts, since there's
  no single sequencer database to replay.

The full mapping from each reference command is in RUNBOOK
[Admin CLI](https://github.com/jazware/vlpds/blob/main/ops/RUNBOOK.md#admin-cli).

## Common tasks

```steps
- title: Create an account for someone
  body: "`vlpds admin account create alice@example.com alice.pds.example` prints a generated 24-character password once. If invites are required, it makes a single-use code for the account."
- title: Hand out invite codes
  body: "`vlpds admin create-invite-code --count 5` (one per line), or the console's Invite codes page. To let accounts earn their own, see [Email and moderation](email-and-moderation.md#invites)."
- title: Take an account down
  body: "Run `vlpds admin account takedown <did> --ref <ticket>`, or use the account's Takedown panel. The repo is hidden and its sessions are revoked, and `untakedown` reverses it. For a record or blob, or to keep a reason and a case with it, use the Moderation page ([Operator moderation](email-and-moderation.md#operator-moderation)). A moderation service can do the same with a service token ([Moderation service](email-and-moderation.md#moderation-service))."
- title: A user is locked out
  body: "Too many wrong codes or passwords clear up by themselves. The factor lock doubles from 5 min, and the per-account sign-in bucket clears within the hour. A DID override on the Rate limits page lifts it early. If they lost their email inbox, change the address with `updateAccountEmail`, which drops the email factor. If they lost their authenticator or a passkey, a recovery code works in its place. If they lost those too, check it's them and use the account page's \"Two-factor sign-in\" panel, which resets every strong factor (`vlpds.admin.resetSecondFactors`, audited as `second_factors.reset`). See [OAuth and 2FA](../oauth-2fa.md#second-factors)."
- title: An OAuth client app gets 429s
  body: "Its backend uses one address for all of its users, and `oauth-ip` allows 3,000 per 5 min per IP. Add an IP override for that address on the Rate limits page. See [Rate limits](rate-limits.md#common-tasks)."
- title: Check the cluster after a change
  body: "Run `vlpds admin cluster status` and look for every lease valid, no unowned shards, no stuck split or merge, and one build rev (or the one you're rolling to)."
```
