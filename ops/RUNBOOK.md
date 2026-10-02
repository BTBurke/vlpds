# vlpds operator runbook

Companion to `ops/alerts.yml`. Each alert has a section below whose heading is
the alert name (the `runbook_url` anchors point here). Everything is grounded in
`DESIGN.md` and `src/`. Where something is inferred rather than read from code,
it is marked **(unverified)**.

- [Background you need](#background-you-need)
- [Tools: endpoints, CLI, logs, exit codes](#tools-endpoints-cli-logs-exit-codes)
- [Admin CLI](#admin-cli)
- [Alerts](#alerts)
- [Procedures](#procedures)
- [What NOT to do](#what-not-to-do)
- [Metric gaps](#metric-gaps)

---

## Background you need

- **All durable state is in the object store** under `--prefix` in `--s3-bucket`:
  `log/{log_id}/{ordinal}.seg` (each node incarnation's commit log), `state/{id}/`
  (one SlateDB per shard, its WAL disabled), `assign/{id}` + `assign/layout`
  (who owns what, and the slot -> shard map; `{id}` is the shard id as 10
  zero-padded decimal digits, e.g. shard 42 is `state/0000000042/`), `nodes/{node_id}` (node leases),
  `writers/{w}` (unique seq low byte per live node), `retain/{log_id}` (retention
  reports), `cluster/version` (the cluster's active feature level and its
  history), `handle/`, `email/`, `blob/`. The local disk is only a SlateDB SST
  cache.
- **Shards.** 65,536 hash slots grouped into shards (default `--shards 64`,
  changed online by split/merge). Each shard has exactly one owner node at a time.
  A node takes free or orphaned shards up to its fair share, `ceil(shards / live
  nodes)`, and hands extras to joiners.
- **One log per node incarnation.** A write is acked only after its segment and
  every earlier one are durable (`If-None-Match` PUTs, up to `--log-inflight` 4 in
  flight, finalized in order) **and** only while the node's lease is valid.
- **Leases.** `nodes/{node_id}` is CAS-renewed every TTL/5 (2 s at the default
  `--lease-ttl-ms 10000`, 12 s at the tiny profile's 60 s). A node's own validity
  ends `TTL - skew` = 0.8 x TTL (8 s / 48 s) after the *send time* of its last
  successful renewal. A renewal round trip over `0.4 x TTL` (**4 s** / 24 s)
  opens a validity gap and the node **fail-stops**. A cluster-wide object-store
  brownout past that ceiling stops every node. Each node exports its settings
  (`vlpds_lease_ttl_seconds`, `vlpds_lease_renew_interval_seconds`,
  `vlpds_lease_skew_seconds`) and its renewals as a fraction of the TTL
  (`vlpds_lease_renew_ttl_ratio`); the lease alerts are relative to them.
- **Takeover.** Peers presume a node dead after its lease has not changed for
  `TTL + skew` = 1.2 x TTL (12 s at the default, 72 s at 60 s) of their own
  monotonic time, or within ~1.5-2.5 renew intervals (3-5 s at the default) if
  its advertised port refuses TCP connections (process gone).
  The new owner **fences** the dead log (conditional create at the end of its
  durable prefix), CASes the assignment, replays the shard's spans, waits out the
  previous owner's `seq_floor` (commit-wait, max 30 s), then serves.
- **A lone node lists less** (no flag). While `nodes/` holds only its own lease,
  a node LISTs `nodes/` once per TTL and `assign/` every 25 steps
  (`vlpds_cluster_lone_skips_total`). A joiner is seen at once through its hello,
  or within a TTL if the hello is lost. An `assign/` object edited by hand in the
  bucket (never do this) is noticed within 25 steps instead of the next one.
- **Retention passes** run every `--log-retention-interval` (default 60 s,
  1 s..=10 m; `VlpdsRetentionNotRunning` expects a pass every 15 min). Passes LIST
  only what can be due, so idle passes make no requests
  (`vlpds_retention_lists_skipped_total`). A longer interval saves little more and
  delays deletes and dead-log retirement by up to one interval.
- **Fail-stop is the safety mechanism.** A node that might be wrong exits; the
  supervisor restarts it and it rejoins. A wrong "dead" presumption costs
  availability, never an acked write (DESIGN "Why safety needs no clocks").
- **Firehose.** Every node merges every node's log and emits an event once
  `seq <= min watermark` over all logs. One slow, stalled or unfenced log holds
  the firehose back on every node. Merged emit lags by the largest wall-clock
  offset between nodes.
- **Forwarding.** Any node accepts any request; repo writes/reads for a shard it
  doesn't own are proxied to the owner with a 3 s time-to-first-byte deadline (30 s
  for exports, uploads, proxying). An owner that doesn't answer in time yields 503
  `PartitionUnavailable` (ambiguous, never resent). An owner that can't start a
  forwarded write within `--forwarded-write-start-ms` (1 s) answers 503
  `RepoLoading` (never applied); a write that finds the shard moving gets 503
  `ShardMoved`. The entry node resends those (and connect-refused forwards) to the
  current owner for up to 20 s.

## Tools: endpoints, CLI, logs, exit codes

**Endpoints** (any node). `/metrics` and `/debug/pprof` are served only on
`--metrics-listen` (default `127.0.0.1:9583`; on the app port with
`--dev-mode` or `--metrics-listen app`, which is public unless a proxy blocks
it). Peer traffic (forwards, which carry users' tokens; `/internal/*`; log
streams) goes to `--advertise-url` (DESIGN.md "Exposure"):
- **Cluster:** `--peer-listen` with peer mTLS (`--peer-tls-ca`,
  `--peer-tls-cert`, `--peer-tls-key`; [Peer TLS](#peer-tls-mtls-between-nodes)),
  `--advertise-url https://<host>:<peer port>`. The peer listener serves
  everything `--listen` does plus `/internal/*` (`/metrics` and
  `/debug/pprof` only where `--listen` has them), with the peer HTTP/2
  settings, to clients with a node certificate of the cluster CA only.
  `--listen` then 404s `/internal/*` and drops `x-vlpds-forwarded`,
  `x-vlpds-internal` and `x-vlpds-client-ip` from requests (served as the
  client requests they are).
- **Single host** (no `--peer-listen`, a loopback `--advertise-url`, as the
  Ansible role runs it): nothing to set; `/internal/*` isn't mounted.
- **Cleartext peers** (no TLS, peers reach `--listen` or `--peer-listen`):
  `--dev-mode`, or `--peer-insecure` when the traffic stays on a private
  encrypted network (WireGuard/Tailscale). Anything else refuses to start
  ("refusing to start: peers can reach this node ..."). Block `/internal/`
  at the edge either way (the Ansible Caddy does).

Production refuses the MinIO default S3 credentials, and
`vlpds.admin.bulkCreate` needs `--dev-mode` or `--allow-bulk-create`.

| What | How |
|---|---|
| Liveness | `GET /xrpc/_health` -> `{"version":"vlpds"}` |
| Caddy on-demand TLS | `on_demand_tls { ask http://127.0.0.1:2583/tls-check }`: `GET /tls-check?domain=D` is 200 for the `--public-url` host and handles of active accounts here, 400 outside `--handle-domain`, 404 for unknown/deactivated handles (any node answers) |
| Service DID document | `GET /.well-known/did.json`: the `did:web` `--service-did`'s document (`#atproto_pds` at `--public-url`); 404 for any other DID method |
| Metrics | `GET http://127.0.0.1:9583/metrics` (Prometheus text; `--metrics-listen`) |
| Cluster view (admin) | `GET /xrpc/vlpds.admin.getClusterStatus` with `Authorization: Basic base64(admin:$VLPDS_ADMIN_TOKEN)`. Returns `node`, `log`, `logDurableOrdinal`, `owned` (shard ids), `shards`, `table` (owner per shard in slot order, `null` = unowned), `layout` (`version`, `shards`, `op` = split/merge in progress), `leaseValid`, `leaseExpiresMs`, `fencedLogs`, `firehose.{lastEmitted,minWatermark,sources[{log,watermark,local}]}`, `version` (feature levels: `active`, `target` while a raise runs, `history`, this build's `binary.{min,max,rev}`, `mixedBuilds`, `revs`, `finalizable`, `finalizedAt`; see [Rolling upgrade](#rolling-upgrade-finalize-rollback)), and `nodes[]` with each peer's `reachable`, `leaseValid`, `logDurableOrdinal`, `owned` count, `writer`, `expiresMs`, `rev`, `minLevel`, `maxLevel`, `seenLevel` (peers fetched with a 1.5 s timeout). |
| Feature level raise (admin) | `POST /xrpc/vlpds.admin.setFeatureLevel {"level": N}` (CLI `vlpds admin cluster finalize`): 200 with the new `cluster/version`; 409 `IncompatibleNodes` names live nodes whose build can't run N (nothing changed); 400 below the active level or past the asked node's build. With `"lower": true` (CLI `vlpds admin cluster lower`) it lowers instead: 400 past a persistent level or during a raise, 409 while a live node can't run N. |
| Cluster view (node-to-node) | `GET /internal/v1/cluster` on the peer address (`--advertise-url`; with peer TLS a node certificate is needed: `curl --cacert ca.crt --cert node.crt --key node.key`; prefer getClusterStatus above) with header `x-vlpds-internal: $VLPDS_INTERNAL_TOKEN`: this node's `owned`, `table`, `layout`, `peers`, `lease_valid`, `log_durable_ordinal`, `firehose_last_emitted`, `firehose_min_watermark`. |
| Operator console | `/admin` (Cluster page polls getClusterStatus), `/admin/metrics` (live metrics). |
| Shard layout | `vlpds admin layout --url http://<node>:2583` (or `vlpds admin --url ... layout`; `VLPDS_ADMIN_TOKEN` env), or `GET /xrpc/vlpds.admin.getShardLayout`. Also `shard-split`, `shard-merge`, `reshard-abort` (abort only before the flip). |
| Accounts, identity, repos | `vlpds admin ...`: the pdsadmin equivalents, see [Admin CLI](#admin-cli). |
| CPU profile | `just profile <node:port> [seconds]` (`/debug/pprof/`; only in a build with `--features profiling`, e.g. the image built with that feature: a default build has no profiler and the endpoint is absent) |

A quick cluster check:

```sh
curl -s -u "admin:$VLPDS_ADMIN_TOKEN" http://NODE:2583/xrpc/vlpds.admin.getClusterStatus \
  | jq '{node, leaseValid, owned: (.owned|length), shards, unowned: ([.table[]|select(.==null)]|length),
         op: .layout.op, fenced: .fencedLogs,
         nodes: [.nodes[] | {node, reachable, leaseValid, owned, logDurableOrdinal}],
         firehose: .firehose.minWatermark}'
```

**Exit codes** (fail-stops; the supervisor must restart on any of them):

| Code | `reason` | Meaning | Log line (error level) |
|---|---|---|---|
| 2 | `segment_upload` | Segment upload task failed | `segment upload task failed: ...; exiting` |
| 3 | `fenced` / `ordinal_taken` | Our log was fenced by a successor, or another writer took our segment ordinal | `our log was fenced by a successor: fail-stop` / `segment ordinal taken by another writer: fail-stop` |
| 4 | `state_apply` | SlateDB apply of a durable segment failed | `state apply failed: ...; exiting` |
| 5 | `lease_lost` / `lease_lapsed` | Lease lost or lapsed (any reason) | `node lease lost unexpectedly: fail-stop` (`lease_lost`), preceded by one of: `node lease lost (CAS conflict)`, `node lease lapsed before renewal`, `node lease lapsed past takeover` (watchdog), `a shard we hold was reassigned`, `a shard failed to close cleanly`, `our log did not quiesce`; or `node lease lapsed before segment PUT` / `before ack` (`lease_lapsed`) |
| 6 | `signature_fault` | 3 signatures failed verification right after signing within a minute (suspected memory/CPU fault; see [VlpdsSignatureFault](#vlpdssignaturefault)) | `repeated signature faults: fail-stop (suspect this host's memory or CPU)`, preceded by `signature failed verification against the signing key's public key` (purpose, recent) |
| 7 | `incompatible_level` | This build can't run the cluster's feature level (`cluster/version`): checked before the node reads or writes anything, again right after its lease write (lease deleted), and once per TTL while running. An old image after a finalize, or a new image whose `MIN_LEVEL` is past the cluster's (see [VlpdsIncompatibleNode](#vlpdsincompatiblenode)) | `incompatible feature level: cluster level N is outside this build's levels A..=B` / `cluster is raising its level to N, past this build's max level B; fail-stop (exit 7)` |
| 8 | `shutdown_fence` | A graceful stop could not fence its own log within min(TTL, 30 s) of retries. It keeps its lease, so peers presume it dead and fence the log, or the restart (same `--node-id`) does. Its shards were already handed out | `fencing our log on shutdown failed: giving up`, then `...: exiting nonzero without dropping our lease` |
| 9 | `critical_task_panicked` | A thread or task the node can't run without panicked: a repo worker thread (`repo_worker`), the log sequencer or finalizer (`log_sequencer`, `log_finalizer`), the firehose merger (`firehose_merger`). A bug: the panic message and location are on stderr just before. Peers take its shards over; the restart is clean | the panic (`thread '...' panicked at src/...`), then `critical task panicked: fail-stop (exit 9)` (`task` field) |

**How the previous process ended** is a metric on the next one
(`src/lifecycle.rs`): each fail-stop writes its `reason` and code to the
exit-state file (`--exit-state-file`, default `vlpds-exit-<node-id>.json` in
`--cache-dir`; with neither set nothing is kept) just before exiting, and the
next start exports `vlpds_last_exit_reason_info{reason,code}` = 1 for as long
as it runs. Besides the table: `clean` (graceful stop, code 0), `error` (code
1: startup or serve error), `crash` (the file still says `running`: SIGKILL,
OOM kill, abort, host loss), `none` (first start or no file).
`vlpds_process_start_time_seconds` (and the standard
`process_start_time_seconds`) dates the restart. Cluster-side, whoever fences
an incarnation that ended without fencing its own log counts
`vlpds_peer_takeovers_total{reason="peer"|"restart"}` (a graceful stop fences
its own log, so it never counts): that survives a node that never comes back.

**Serving limits** (DESIGN.md "HTTP", "Firehose", "Stage 3: readers"):

| Flag (env) | Default | Bounds | Past it |
|---|---|---|---|
| `--max-connections` (`VLPDS_MAX_CONNECTIONS`) | 50,000 | open connections per listener | accepts pause (connections wait in the accept queue) |
| `--peer-listen` (`VLPDS_PEER_LISTEN`) | unset | a second listener for peers, with the large h2 windows (mTLS with `--peer-tls-*`); point `--advertise-url` at it. Then `--listen` gets the client settings (1 MiB / 8 MiB windows, 256 streams per connection) and no `/internal/*` | - |
| `--peer-tls-ca` / `--peer-tls-cert` / `--peer-tls-key` (`VLPDS_PEER_TLS_CA` / `_CERT` / `_KEY`) | unset | peer mTLS on `--peer-listen` and in the peer clients ([Peer TLS](#peer-tls-mtls-between-nodes)); re-read on SIGHUP and on file change (60 s poll) | a node cert from another CA, for another node or host: handshake refused (`vlpds_peer_tls_handshake_failures_total`) |
| `--peer-insecure` (`VLPDS_PEER_INSECURE`) | off | allows cleartext peer traffic outside `--dev-mode` on a node peers can reach | - |
| `--max-exports` (`VLPDS_MAX_EXPORTS`) | 32 | getRepo exports streaming at once | waits 10 s for a slot, then 503 `Overloaded` (`vlpds_sync_exports_ended_total{reason="shed"}`) |
| `--export-stall-secs` (`VLPDS_EXPORT_STALL_SECS`) | 60 | how long an export waits for a client that reads nothing | export ended, body errors (`reason="stalled"`) |
| `--max-queued-reads` (`VLPDS_MAX_QUEUED_READS`) | 20,000 | repo-view reads (getRepo, getRecord, getBlocks, ...) queued at the repo workers | 503 `Overloaded` |
| `--firehose-max-backfills` (`VLPDS_FIREHOSE_MAX_BACKFILLS`) | 16 | cursor backfills running at once (x `--backfill-readahead-mb` of read-ahead) | waits for a slot (`vlpds_firehose_backfills{state="waiting"}`) |
| `--firehose-max-per-ip` (`VLPDS_FIREHOSE_MAX_PER_IP`) | 256 | subscribeRepos connections per client IP (IPv6 /64) | 429 `RateLimitExceeded` (`vlpds_firehose_rejected_total{reason="per_ip"}`) |

Not flags: a subscriber that takes no bytes for 30 s outside the live path
(backfill, pongs) is dropped (`vlpds_firehose_disconnects_total{reason="write_stalled"}`);
Argon2 runs at most one per core (16 max) at once; request-path password
checks and hashes wait up to 2 s for a turn, then answer 503 `Overloaded`
(`vlpds_argon2_shed_total`, [VlpdsPasswordHashingShed](#vlpdspasswordhashingshed)),
admin password changes wait; accept
errors are retried every 50 ms (`vlpds_http_server_accept_errors_total`,
log `accept failed (retrying)`: usually out of file descriptors).

**Logs** go to stderr; stdout carries only machine output (wrapped keys,
`vlpds admin` tables and `--json`). `--log-format json` (`VLPDS_LOG_FORMAT`)
for production: one JSON object per line. `text` (the default) colours only
when stderr is a terminal and `NO_COLOR` is unset. Filter with `RUST_LOG`
(default `info,slatedb=warn`).

**Disk cache sizing**: `--cache-dir` on local NVMe, plus `--disk-cache-mb`
(`VLPDS_DISK_CACHE_MB`) = the space you give it on that node. Each shard's
cap is that divided by the layout's shard count, so the total fits even if
this node takes every shard; in steady state an N-node cluster uses about
1/N of it. If the disk is sized for failover, set the per-shard cap instead
(`--disk-cache-shard-mb`). Unset: 16 GiB per shard (1 TiB at 64 shards).
The start-up line `SST disk cache (per shard)` shows `dir` and `shard_mb`.

**Useful log lines** (tracing, info/warn unless noted):
`acquired shards` (shards, owned, fair, live), `shards opened` (shards,
`segments_replayed`, `replayed_ms`, `elapsed_ms`), `shards closed`,
`handing back extra shards`, `fenced dead node's log` (log_id, fence_ordinal),
`peer missed a renewal and refuses connections: presumed dead`,
`node lease renew error (will retry)`, `control-plane <op> timed out after`,
`waited for our clock to pass the previous owner's last seq`,
`previous owner's clock is more than 30s ahead of ours: serving anyway` (error),
`tokio runtime stall` (late_ms), `log retention pass failed`,
`reshard step failed (retried next step)`, `fencing our log on shutdown failed`.

---

## Admin CLI

`vlpds admin [--url URL] [--admin-token T] [--json] <command>` talks admin
XRPC to one node (the three flags are accepted before or after the command) (`--url`, default `http://127.0.0.1:2583`, env `VLPDS_URL`;
token from `VLPDS_ADMIN_TOKEN`, as the node's). Any node will do: calls naming
a DID are routed to the repo's owner, and the per-node maintenance commands
(`rotate-plc-keys`, `rewrap-secrets`) are sent to every node that
`getClusterStatus` lists (`--node-only`: just `--url`). Output is a table or a
short message; `--json` prints the raw results. Exit 1 on an XRPC error, on any
failed item of a batch (the other items still run, as the reference scripts
do), and from `check-repo` when it finds a problem. Destructive commands
(`account delete`, `rebuild-repo`) ask for confirmation; off a terminal they
refuse without `--yes`.

| Reference (`pdsadmin` / `node run-script.js`) | vlpds | Via |
|---|---|---|
| `pdsadmin account list` | `vlpds admin account list [--email PREFIX]` | `admin.searchAccounts` (every node's shards, paged; warns on `unreachableNodes` / `missingShards`) |
| `pdsadmin account create EMAIL HANDLE` | `vlpds admin account create EMAIL HANDLE [--password P] [--invite-code C]` | a single-use invite only if `describeServer.inviteCodeRequired`, then `server.createAccount`; prints the generated 24-char password once |
| `pdsadmin account delete DID` | `vlpds admin account delete DID [--yes]` | `admin.deleteAccount` |
| `pdsadmin account takedown DID` | `vlpds admin account takedown DID [--ref R]` | `admin.updateSubjectStatus` (repoRef, `ref` default unix time) |
| `pdsadmin account untakedown DID` | `vlpds admin account untakedown DID` | same, `applied: false` |
| `pdsadmin account reset-password DID` | `vlpds admin account reset-password DID [--password P]` | `admin.updateAccountPassword`; prints the new password |
| (none) | `vlpds admin account info DID` | `admin.getAccountInfo` + `getSubjectStatus` |
| `pdsadmin create-invite-code` | `vlpds admin create-invite-code [--uses N] [--count N] [--for-account DID]` | `server.createInviteCode`; one code per line |
| `pdsadmin request-crawl [RELAY,...]` | `vlpds admin request-crawl [RELAY,...]` | `vlpds.admin.requestCrawl`: the node asks each relay (default its `--crawlers`) to crawl its `--public-url` host; per-relay result, exit 1 if any refused |
| `pdsadmin update` | (none) | roll the image: [Rolling deploy](#rolling-deploy) |
| `publish-identity DID...` / `publish-identity-file F` | `vlpds admin publish-identity [DID...] [--file F]` | `vlpds.admin.publishIdentity`: `#identity` for each DID (any status but deleted), DID-document caches dropped |
| `rotate-keys DID...` / `rotate-keys-file F` | `vlpds admin rotate-keys [DID...] [--file F]` | `vlpds.admin.publishIdentity {syncPlc: true}`: a did:plc whose PLC `atproto` key isn't the signing key held here gets a PLC update (server rotation key), then the repo is re-signed (an empty commit) with `#identity` + `#sync`, as the reference, so relays that failed commits against the old document resync |
| (admin `updateAccountSigningKey`) | `vlpds admin rotate-keys --generate DID...` | a fresh signing key: recorded as pending (the account's writes get a retryable 503 `KeyUnavailable` meanwhile), PLC updated, then the repo re-signed with `#identity` + `#sync`. A PLC refusal changes nothing; after an outage or a crash the rotation stays pending and the node finishes it (DESIGN "Signing-key rotation") |
| (`PDS_PLC_ROTATION_KEY` change) | `vlpds admin rotate-plc-keys [--dry-run]` | `vlpds.admin.rotatePlcKeys` on every node ([PLC rotation key rotation](#plc-rotation-key-rotation)) |
| (none) | `vlpds admin rewrap-secrets [--dry-run] [--check-versions]` | `vlpds.admin.rewrapSecrets` on every node ([KEK rotation](#kek-rotation)) |
| `rebuild-repo DID` | `vlpds admin rebuild-repo DID [--dry-run] [--yes]` | `vlpds.admin.rebuildRepo`: see below |
| (none) | `vlpds admin check-repo DID` | `vlpds.admin.checkRepo`: see below |
| `sequencer-recovery`, `recovery-repair-repos`, `rotate-keys-recovery` | (none) | no single sequencer DB to replay: durability is the log + SlateDB per shard (DESIGN "Backups and restore") |
| (none) | `vlpds admin cluster-status` (or `cluster status`) | `vlpds.admin.getClusterStatus`: this node, layout, unowned shards, firehose, feature level (and the finalize/mixed-builds banner), a row per node (`*` = the one asked) with its rev and level window |
| (none) | `vlpds admin cluster finalize [--level N] [--yes]` | `vlpds.admin.setFeatureLevel` (default N = active + 1; asks first): [Rolling upgrade](#rolling-upgrade-finalize-rollback) |
| (none) | `vlpds admin cluster lower --level N [--yes]` | `vlpds.admin.setFeatureLevel {"level": N, "lower": true}`: only past wire-only (non-persistent) levels: [Rolling upgrade](#rolling-upgrade-finalize-rollback) |
| (none) | `vlpds admin layout`, `shard-split`, `shard-merge`, `reshard-abort` | [Shard split / merge](#shard-split--merge) |

Per-DID batches (`publish-identity`, `rotate-keys`) run one DID at a time,
like the reference scripts without their sleep; a file is one DID per line,
blank lines and `#` comments skipped. For millions of DIDs split the file and
run several in parallel against different nodes; the PLC directory
rate-limits, so `rotate-keys` should stay at a few in flight per IP.

**check-repo** reads the repo's state from one shard snapshot (it doesn't need
the repo to load) and reports: the head commit (hashes to its CID, names the
head's data root and the DID, signature valid for the account's key), every
record (hashes to its CID), the MST rebuilt from the records against the
head's data root, the persisted interior nodes (`M/`: missing, extra, corrupt)
against that tree, and the record-CID, blob-ref and collection indexes. A
missing or wrong `M/` node is self-healing (the next cold load rebuilds from
`R/` and backfills, `vlpds_lazy_mst_fallbacks_total`), so a check with only
node or index problems is not an emergency; run `rebuild-repo` to clean it
up now.

**rebuild-repo** is the reference script: the repo re-derived from its
records (MST, `M/` written whole and stale nodes deleted, record-CID,
blob-ref and collection indexes) under a new signed commit (rev bumped) and a
`#sync` (none while deactivated: activation sends it). It prints the check
first and asks. It is refused (`RepoUnrecoverable`) when the records can't be
the repo: one doesn't hash to its CID, or they don't rebuild to the head's
data root (records were lost: the repo can't load, and re-signing what is
left would silently drop data; restore from a backup instead), and for a
taken-down account (untakedown first). A write landing between the check and
the rewrite makes it fail with `InvalidSwap`: run it again.

---

## Alerts

### VlpdsNodeDown

**Means:** Prometheus can't scrape a node for 2 minutes. If the process is gone,
peers took its shards within 3-5 s (refused probe) or TTL + skew (12 s at the
default TTL, 72 s at 60 s) plus replay. If the host is up but frozen/partitioned, its socket still exists and
takeover waits the full TTL + skew; the frozen node's own watchdog fail-stops it
2 x skew after its validity lapses.

**Causes:** crash or fail-stop without a restart, OOM kill, host loss, network
partition between Prometheus and the node, or `--metrics-listen` misconfigured.

**Confirm:** `getClusterStatus` from another node: is the node in `nodes[]`,
`reachable`? `VlpdsShardsUnowned` firing? Host/supervisor status and the last
log lines (exit code table above). A bench-scrape config lists ~30 ports that are
normally down: check this is a real node.

**Do:** If `VlpdsShardsUnowned` isn't firing, data-wise nothing is urgent; the
cluster is running with less capacity. Restart the process (same `--node-id`) or
follow [Replacing a dead host](#replacing-a-dead-host). If several nodes are down at
once, look for an object-store outage first ([procedure](#object-store-outage)).

### VlpdsNotScraped

**Means:** no `up{job="vlpds"}` series at all (for this cluster, when the rule
set is scoped with `extra_labels`) for 10 minutes: nothing scrapes the nodes, so
every other vlpds alert is blind, including `VlpdsNodeDown`. Different from a
node that is scraped and down (`up == 0`).

**Causes:** the host's Alloy isn't running or doesn't carry the
`vlpds-monitoring` fragment (on the prod inventory `vlpds_manage_alloy` is off
until the host's existing setup is reviewed, so this fires there until then),
remote_write from Alloy to VictoriaMetrics failing, a renamed job or `cluster`
label (`deploy_env`), or the monitoring host itself is unhealthy (then other
deployments' alerts go quiet too).

**Confirm:** in Grafana / VictoriaMetrics, `up{job="vlpds"}` by `cluster`;
Alloy's UI/logs on the vlpds host (`systemctl status alloy`, its
`prometheus.scrape` and `prometheus.remote_write` components); `curl -s
127.0.0.1:9583/metrics | head` on the host (the node answering locally means the
pipeline, not vlpds, is broken).

**Do:** fix the scrape pipeline (deploy roles/alloy with the `vlpds-monitoring`
fragment, or repair remote_write). Meanwhile check the cluster by hand
(`vlpds admin cluster-status`). Silence it only for a cluster that is
intentionally unmonitored.

### VlpdsNodeRestarted

**Means:** `vlpds_process_start_time_seconds` changed: a new process. Expected
during a deploy. Otherwise a fail-stop, crash or OOM kill.

**Confirm:** `vlpds_last_exit_reason_info` on the node (reason and code: table
above; `crash` = no exit recorded), then the error line just before the exit.
`vlpds_build_info{rev}` changed? (deploy). Kernel/cgroup OOM logs. Exit 5 with
`renew error` warnings before it: store latency (see
[VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling)). Exit 3: a peer
presumed this node dead and fenced it (it was frozen, partitioned, or its
renewals were slow). Exit 3 right after another process started with the same
`--node-id`: see [What NOT to do](#what-not-to-do).

**Do:** nothing if it rejoined (it owns ~fair share again within a step or two)
and the cause is understood. Investigate exit 2/4 (store errors / SlateDB apply)
before they repeat.

### VlpdsNodeFailStopped

**Means:** the node restarted in the last 30 minutes and its previous process
ended with a fail-stop (`reason` and `code` labels, see the exit code table) or
`crash` (no exit recorded: SIGKILL, OOM kill, abort, host loss). `error` = the
previous process failed to start or serve (exit 1). Needs the exit-state file
(`--exit-state-file`, or `--cache-dir`) on a disk that survives restarts.

**Confirm / Do:** as [VlpdsNodeRestarted](#vlpdsnoderestarted) for that reason.
`crash`: kernel/cgroup OOM logs, supervisor logs.

### VlpdsUncleanNodeExit

**Means:** some node fenced the log of an incarnation that ended without
fencing it itself. `reason="peer"`: a survivor took over a dead peer's shards
(crash, kill -9, OOM, fail-stop, partition, frozen past TTL + skew).
`reason="restart"`: a node fenced its own previous incarnation at startup (it
came back before a peer took over). Graceful stops never count. This is
counted on a live node, so it reports deaths of nodes that never come back.
Every node exports both reasons at 0 from startup (`metrics::init_counters`),
so `increase()` sees the first takeover: a counter series that first appeared
at 1 had no earlier sample, and the alert missed it. A `restart` lands before
the new process is first scraped, but in the series its predecessor exported
at 0.

**Confirm:** `fenced dead node's log` (log_id) in the fencer's logs; the dead
incarnation's `vlpds_last_exit_reason_info` once it restarts.

**Do:** as [VlpdsNodeRestarted](#vlpdsnoderestarted).

### VlpdsNodeCrashLooping

**Means:** 3+ restarts in an hour. Each restart moves its shards out and back
(ownership churn, cold repo loads, write resends).

**Causes:** persistent store errors (exit 2/4), renewals regularly over
0.4 x TTL (exit 5), two processes with the same `--node-id` fencing each other
(exit 3 alternating: each start fences the previous incarnation's log at join,
`vlpds_peer_takeovers_total{reason="restart"}`), OOM (see [VlpdsMemoryCritical](#vlpdsmemorycritical)), a
bad binary.

**Do:** stop the node (SIGTERM) and leave it down while you diagnose; its shards
move to peers. Roll back the binary if the loop began with a deploy. Never
"fix" a crash loop by deleting objects.

### VlpdsPeerPresumedDead

**Means:** a node saw a peer miss a renewal **and** its address refuse TCP: the
peer process is gone; takeover starts immediately (fence, CAS, replay).

**Confirm:** `peer missed a renewal and refuses connections` and `fenced dead
node's log` in the observer's logs; the dead node's exit code.

**Do:** treat like [VlpdsNodeRestarted](#vlpdsnoderestarted) for the dead peer.

### VlpdsMixedVersions

**Means:** more than one `rev` in `vlpds_build_info` for over an hour. Normal
for the minutes of a rolling deploy.

**Do:** finish or roll back the deploy ([Rolling upgrade](#rolling-upgrade-finalize-rollback)).
Mixed builds are safe while the cluster's feature level is one every node's
build can run (they all write that level's formats); `vlpds admin
cluster-status` shows each node's rev and level window. Tested with two real
builds: the `upgrade-*` HA scenarios (rolling upgrade, rollback, old-node
refusal, raise race) pass, see `bench/ha/RESULTS.md` "Two-build upgrade
scenarios".

### VlpdsFormatErrors

**Means:** a node failed to decode something on an unknown or malformed
format marker (`vlpds_format_errors_total{format}`): `segment` (magic not in
this build's levels, or unknown codec), `log_stream` (a peer sent a message
type this build doesn't know: skipped), `applied_marker` (a shard's
`meta/applied2` doesn't decode: the shard won't open), `cluster_version`
(`cluster/version` unreadable). With levels working this never happens: a
writer emits a format only once its level is active, and only builds that can
read it run.

**Confirm:** the node's logs at that time (`bad segment magic`, `skipping a
log stream message of an unknown type`, `malformed applied marker`);
`vlpds admin cluster-status`: is a node on a build whose `maxLevel` is above
the active level writing early (a bug), or is the object corrupt?

**Do:** stop the writer that emits it if one build is at fault (roll it
back: before finalize that is a plain redeploy). Corruption of a segment or
marker: treat like [VlpdsShardOpenErrors](#vlpdsshardopenerrors). Never edit
`cluster/version` by hand.

### VlpdsIncompatibleNode

**Means:** the previous process on this node exited 7 `incompatible_level`:
its build can't run the cluster's feature level (`cluster/version`), so it
left before reading or writing anything. A process looping on exit 7 never
serves `/metrics` (it stops before serving), so expect
[VlpdsNodeDown](#vlpdsnodedown) for it too; this alert shows once a good
image runs there again.

**Confirm:** the node's log line `incompatible feature level: ...` names the
active level (and a raise `target`, if one was running) and its build's
window; `vlpds admin cluster-status` shows `version.active`.

**Do:** deploy a build whose level window contains the active level (the
current release). After a finalize, an older image can never rejoin: that
is by design (rollback after finalize is forward-fix only). A raise in
progress (`target` set) that made a starting node refuse finishes or aborts
by itself within seconds; start the node again afterwards. A `target` that
stays (the finalizing node died between its steps; `cluster-status` keeps
saying "raising to N") is cleared by `vlpds admin cluster finalize --level
<active> --yes`.

### VlpdsFeatureLevelUnfinalized

**Means:** for 14 days every node has run a build that supports a higher
feature level than the cluster's active one. Fine during a soak, but the
upgrade was never finished, and the next release can't drop support for the
old formats while the window is open.

**Do:** if the build has soaked cleanly, finalize ([Rolling
upgrade](#rolling-upgrade-finalize-rollback) step 4). If not, decide whether
to roll back instead.

### VlpdsShardsUnowned

**Means:** fewer shards are owned by scraped nodes than the layout has
(`vlpds:layout_shards`: the nodes' `vlpds_shard_layout_shards`, or the last
value seen in the past hour when none is up) for 2 minutes. Requests for repos in
those shards fail or wait for the 20 s resend window to run out. Normal takeover
is seconds; 2 minutes is not.

**Causes:**
- A frozen (not dead) node: its socket accepts, so takeover waits TTL + skew
  (12 s at the default TTL, 72 s at 60 s), then fence + replay. Long replay (see
  [VlpdsTakeoverReplaySlow](#vlpdstakeoverreplayslow)) stretches it.
- Survivors can't take shards: control-plane calls timing out
  ([VlpdsControlPlaneTimeouts](#vlpdscontrolplanetimeouts)), fence or assignment
  CAS failing, SlateDB open failing ([VlpdsShardOpenErrors](#vlpdsshardopenerrors)).
- Commit-wait: the previous owner's clock was ahead; a new owner waits up to 30 s
  (`waited for our clock to pass`).
- A merge just lowered the count and a node that was down during it reports its
  old layout from the past hour (only while no node is up).
- A node is up but not scraped (its gauge is missing, not its ownership).

**Confirm:** `getClusterStatus`: `table` entries that are `null`, `layout.op`,
each node's `owned` and `leaseValid`. Logs on survivors: `acquired shards`,
`shards opened` (`segments_replayed`, `replayed_ms`), control-plane timeouts.
`vlpds_shard_open_seconds{kind="replay"}` and `vlpds_shards_opened_total{result}`
on survivors.

**Do:** fix the blocker (store, frozen host: kill the frozen process so the refused
probe kicks in). Do **not** edit `assign/` objects. If all nodes are down, start
them; each takes its share at its first steps.

### VlpdsShardsOverOwned

**Means:** the nodes' `vlpds_owned_partitions` sum to more than the layout's
shard count (`vlpds:layout_shards`) for 5 minutes.

**Causes:** nodes disagreeing on the layout for that long (a split/merge stuck
mid-flip: `layout.op`, `vlpds_shard_layout_version` per node). Otherwise a
zombie: a node that still believes it owns shards it lost. Safety holds (its next
segment PUT collides with the fence and it exits 3; a step that sees a reassigned
shard exits 5), but a zombie whose monotonic clock was paused (VM suspend) serves
stale reads until then.

**Confirm:** compare `owned` lists across nodes in `getClusterStatus`; the same
shard on two nodes identifies the zombie. `vlpds admin layout` for the count.

**Do:** layout disagreement -> see [Shard split / merge](#shard-split--merge).
Zombie -> SIGKILL it (it has nothing it may ack; a successor already fenced its
log).

### VlpdsOwnershipFlapping

**Means:** more than 4 shard opens per shard in an hour. Every move costs a close
barrier + checkpoint, a fence or handoff, a SlateDB open, replay, and cold repo
loads on the new owner.

**Causes:** nodes restarting repeatedly (see [VlpdsNodeCrashLooping](#vlpdsnodecrashlooping));
a node whose renewals keep lapsing (slow store, CPU starvation: see
[VlpdsRuntimeStalls](#vlpdsruntimestalls)); repeated deploys; reshard activity
(`vlpds_reshard_events_total`).

**Confirm:** `sum by (instance) (increase(vlpds_lease_events_total[1h]))` by
`event`; which node's `opened`/`closed` dominate; restarts.

**Do:** stabilize the flapping node (stop it if needed). Don't lower the TTL to
"speed up" recovery: it shrinks the renewal ceiling and causes more fail-stops.

### VlpdsOwnershipImbalanced

**Means:** one node holds over 1.5x the fair share for 30 minutes.

**Causes:** a joiner hasn't been greeted/settled (handback only goes to peers seen
for the join grace or that confirmed the hello), the over-full node's releases
fail, or nodes are `draining`. A reshard in progress keeps parents in place.

**Confirm:** `handing back extra shards` log lines on the full node; joiner's
`nodes[]` entry `reachable`/`leaseValid`; `layout.op`.

**Do:** usually resolves by itself; if not, a graceful restart (SIGTERM) of the
over-full node hands its shards out evenly.

### VlpdsShardOpenErrors

**Means:** shard opens failed on this node (`vlpds_shards_opened_total{result="error"}`):
the SlateDB open, the replay of previous owners' log spans, or the post-replay
flush. A failed shard is released (nothing was logged for it) and retried by
the next step, here or elsewhere.

**Confirm:** `open failed: ...; releasing` and `replay failed` log lines (shard,
error); [VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) for
`log_segment` reads; [VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors) for
shard state (SlateDB). Replay treats a hole
inside a span as an error: someone deleted log objects by hand?

**Do:** fix the store problem. Do not edit `assign/` or `log/` to "unstick" a
shard.

### VlpdsTakeoverReplaySlow

**Means:** a batch of shard opens that replayed a dead owner's log tail took over
20 s before serving (`vlpds_shard_open_seconds{kind="replay"}`; the replay step
alone is `vlpds_recovery_replay_seconds`, its size
`vlpds_recovery_replayed_segments_total`). Those shards were unavailable for
that long on top of the takeover delay.

**Causes:** the dead node hadn't checkpointed for a while
([VlpdsReplayBacklogHigh](#vlpdsreplaybackloghigh),
[VlpdsCheckpointsStalled](#vlpdscheckpointsstalled)); slow `log_segment` GETs
(`vlpds_object_store_request_seconds{component="log_segment"}`); slow SlateDB
opens (store latency).

**Do:** fix checkpointing on the nodes; investigate store latency.

### VlpdsLeaseRenewalNearCeiling

**Means:** at least one lease renewal (one CAS PUT of `nodes/{node_id}`) took over
0.2 x TTL in the last 5 minutes (`vlpds_lease_renew_ttl_ratio`; 2 s at the
default 10 s TTL, 12 s at 60 s). Validity ends TTL - skew (0.8 x TTL) after a
renewal's *send time* and renewals go out every 0.2 x TTL, so a round trip over
0.4 x TTL lapses the lease and the node fail-stops (exit 5). Several nodes at
once: a store brownout that will stop the whole cluster past the ceiling.
Thresholds follow each node's `vlpds_lease_ttl_seconds`.

**Confirm:** `vlpds_lease_validity_seconds` (dips below ~0.6 x TTL), renew
errors, `vlpds_lease_renew_seconds` (absolute round trips),
`vlpds_object_store_request_seconds{component="ctl_lease"}` and the other
components on the same node (node-side network vs store), runtime stalls
([VlpdsRuntimeStalls](#vlpdsruntimestalls): a starved runtime delays the renew
task itself, not the store).

**Do:** one node -> its network path to the store, CPU. Many -> the
[Object-store outage](#object-store-outage) procedure. Don't lower the TTL.

### VlpdsLeaseRenewalAtCeiling

**Means:** a renewal took over 0.4 x TTL (4 s at the default TTL, 24 s at
60 s): past the ceiling. The node's validity gapped, so it stopped acking and
fail-stopped (exit 5) or is about to; `VlpdsNodeRestarted` /
`VlpdsNodeFailStopped` follow. On several nodes at once, the store is browning
out and the cluster is stopping.

**Do:** as [VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling), now;
for many nodes go straight to [Object-store outage](#object-store-outage).

### VlpdsLeaseRenewalSlow

**Means:** renewal p99 over 500 ms for 10 minutes (normal: one small PUT,
~25-50 ms). Not dangerous yet at any supported TTL; the trend toward the
0.4 x TTL ceiling is.

**Do:** as [VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling), without
the urgency.

### VlpdsLeaseRenewErrors

**Means:** renewals failed with a store error (`kind="error"`) or the HTTP
client's timeout (`kind="timeout"`) and will be retried at the next tick
(TTL / 5). Every failed renewal eats into the 0.8 x TTL validity: four in a row
lapse it. `kind="conflict"` (someone rewrote our lease) and `kind="lapsed"`
(validity ended before a renewal) fail-stop at once and show as restarts.

**Confirm:** `node lease renew error (will retry)` warnings with the error text;
[VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) for `ctl_lease`.
`transport error of kind Connect` on every client at once is the host out of
ephemeral ports (`ss -s`: thousands in TIME_WAIT). vlpds' object-store clients
bound their connections (see
[VlpdsObjectStorePermitsSaturated](#vlpdsobjectstorepermitssaturated)), so
look for another process on the host churning connections.

**Do:** credentials, throttling, network, provider status.

### VlpdsLeaseValidityLow

**Means:** a scrape saw this node with under 0.4 x TTL of lease validity left
(`vlpds_lease_validity_seconds` against `vlpds_lease_ttl_seconds`; normally
0.6-0.8 x TTL, i.e. 6-8 s at the default TTL and 36-48 s at 60 s): its renewals
were 0.2 x TTL or more overdue, so it came within 0.4 x TTL of a fail-stop.
Sampled at scrape time, so short dips can be missed: the renewal histograms are
the complete record.

**Do:** as [VlpdsLeaseRenewalNearCeiling](#vlpdsleaserenewalnearceiling).

### VlpdsCommitLatencyHigh

**Means:** p99 enqueue -> durable + applied + acked over 500 ms for 10 minutes
(design: ~40-50 ms p50, ~150 ms p99 on S3 Standard).

**Confirm:** break it down with `vlpds_commit_stage_seconds{stage}`:
`seal_wait` (queueing before the PUT: load or too few PUT slots), `put` (object
store, see `vlpds_segment_put_seconds`, hedges), `apply_lock` (shard locks held by
exports/checkpoints), `apply` (SlateDB: L0 stalls, backpressure), `ack`.
`vlpds_runtime_tick_late_seconds` for CPU starvation.

**Do:** store slow -> see [VlpdsSegmentPutLatencyHigh](#vlpdssegmentputlatencyhigh).
CPU -> add nodes or shed load. Apply slow -> [VlpdsSlateDbL0Stalls](#vlpdsslatedbl0stalls).

### VlpdsCommitLatencyCritical

**Means:** p99 over 2 s. Forwarded writes start failing at the 3 s deadline and
the node is within 2x of the 4 s renewal ceiling (at the default 10 s TTL) if the
cause is the store.

**Do:** as [VlpdsCommitLatencyHigh](#vlpdscommitlatencyhigh), urgently. If every
node is affected, assume an object-store brownout ([procedure](#object-store-outage)).

### VlpdsCommitLogStalled

**Means:** entries are queued for the sequencer, yet no segment became durable in
2 minutes. Segment PUTs retry until they succeed (fail-stop policy), so a stall
is a store that keeps failing or hanging, or a wedged finalizer.

**Confirm:** `vlpds_segment_puts_inflight`, `vlpds_segment_put_attempts_total{result}`,
`vlpds_segment_put_hedges_total`; logs for PUT errors; `leaseValid` (an invalid
lease stops PUTs, then the node exits 5).

**Do:** store problem -> [object-store outage](#object-store-outage). A node that
is wedged with a healthy store: SIGTERM it (if it doesn't exit within a minute,
SIGKILL; a successor fences its log and replays everything it acked).

### VlpdsWatermarkLagHigh

**Means:** the node's own log watermark (every event <= it is durable) is over 2 s
behind its clock for 5 minutes while it owns shards. An idle log advertises the
clock, so lag means entries are assigned seqs but not durable.

**Do:** as [VlpdsCommitLatencyHigh](#vlpdscommitlatencyhigh). Every node's firehose
waits for this log, so expect [VlpdsFirehoseEmitDelayHigh](#vlpdsfirehoseemitdelayhigh) too.

### VlpdsSegmentPutLatencyHigh

**Means:** p99 log segment PUT (including hedges/retries) over 250 ms for 10
minutes. Design: ~25 ms; a hedged duplicate PUT starts at 100 ms (`--hedge-after-ms`).

**Causes:** object-store latency (region/AZ issue, throttling: S3 503 SlowDown),
network saturation on the `log` HTTP pool, oversized segments (`vlpds_segment_bytes`).

**Confirm:** `vlpds_object_store_bytes_total{client="log",dir="up"}` rate vs NIC,
`vlpds_segment_stall_seals_total`, SlateDB's latency on the same store
(`slatedb_object_store_request_duration_seconds`) to tell store vs. node.

**Do:** store-side: provider status, request rate per prefix. Node-side: network.
Do not raise `--max-segment-mb` to compensate (bigger PUTs are slower; DESIGN
"Pipelined segment PUTs").

### VlpdsSegmentPutErrors

**Means:** segment PUT attempts are failing (not `already_exists`, which is a lost
hedge race verified by content). They are retried; acks wait. Unrecoverable
failure of the upload task exits 2.

**Confirm:** node logs for the store error text; [VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors)
(`log_segment`) and [VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors) (shard
state) on the same node (credentials, bucket policy, throttling).

**Do:** fix credentials/permissions/quotas. A `segment PUT conflicted but no object
is there; retrying` warning is handled by the code.

### VlpdsWritesShed

**Means:** writes rejected with 503 `Overloaded` because more than
`--max-inflight-writes` (20,000) are in flight. This is admission control working:
without it a latency blip snowballs into connection storms.

**Do:** find why writes are slow (commit latency, cold loads) or add capacity.
Raising the limit only helps if the node has headroom.

### VlpdsPasswordHashingShed

**Means:** password checks and hashes on request paths (createSession,
createAccount, OAuth sign-in/sign-up, resetPassword, deleteAccount,
disableTotp) answered 503 `Overloaded` + `Retry-After: 1` (the OAuth forms: a
503 page) because every Argon2 permit (one per core, at most 16; ~20 ms of CPU
and 19 MiB each) stayed busy for 2 s. Shedding keeps a login flood from
queueing without bound; admin password changes still wait their turn.
Counter: `vlpds_argon2_shed_total`.

**Confirm:** `rate(vlpds_http_requests_total{method="com.atproto.server.createSession"}[5m])`
by status; `vlpds_rate_limited_total` (rate limits are checked before any
hashing, so a flood from few IPs or identifiers should be 429s, not 503s); host
CPU.

**Do:** a flood spread over many IPs: tighten the sign-in rate limits or block
upstream. Legitimate load: more cores (the permit count follows them, up to 16)
or nodes.

### VlpdsProxyAccountCapSustained

**Means:** proxied (AppView/service) requests for one account refused 429
`RateLimitExceeded` at 64 in flight on its owner node
(`vlpds_proxy_rejected_total{reason="account_cap"}`). A slot is held until the
response body is done, so a slow upstream or a client that stops reading
holds slots too; bodies whose client stopped reading for 30 s are dropped and
counted in `vlpds_http_stalled_bodies_total` (proxied and forwarded responses).

**Confirm:** the access log for the account sending the requests; upstream
latency; whether `vlpds_http_stalled_bodies_total` grows alongside.

**Do:** a misbehaving client: rate limit or take it down per policy. A slow
upstream: follow the upstream's health; nothing to tune here.

### VlpdsWriteInternalErrors

**Means:** writes failing with `internal` or `unavailable` (other kinds such as
`invalid_swap`, `invalid`, `repo_not_found` are client errors).

**Confirm:** node logs around the errors; `vlpds_repo_loads_total{result="error"}`;
store errors.

**Do:** follow the underlying cause; `unavailable` usually tracks shard moves or
loads (see [VlpdsWriteResendsSustained](#vlpdswriteresendssustained)).

### VlpdsHttp5xxHigh

**Means:** over 5% of locally served XRPC requests return 5xx (appview-proxied
calls excluded). Every 503 vlpds sends carries `Retry-After: 1`.

**Confirm:** `sum by (method, status) (rate(vlpds_http_requests_total{status=~"5.."}[5m]))`
-> which methods. 503 on repo writes: shard moves, `Overloaded`,
`PartitionUnavailable`. 500: internal errors in logs.

**Do:** follow the matching alert (ownership, commit latency, forwards, store).

### VlpdsForwardErrorsHigh

**Means:** over 5% of forwards to shard owners return 5xx, which includes an
unreachable owner or one past its 3 s TTFB deadline (presumed frozen; the client
gets 503 `PartitionUnavailable`).

**Causes:** owner node frozen/overloaded, network between nodes, an owner that
just died (until takeover), `--advertise-url` not reachable from peers.

**Confirm:** `vlpds_forward_seconds` p99; target owner's commit latency and
runtime stalls; `vlpds_http_client_connects_total{role="peer"}` climbing (should
stay flat under steady load).

**Do:** fix the owner; a frozen owner should lose its shards after TTL + skew.

### VlpdsForwardLatencyHigh

**Means:** p99 forward to the owner's response head over 1 s (deadline 3 s).

**Do:** look at the owners' latency (commit, cold loads `vlpds_repo_load_seconds`,
runtime) and the peer network.

### VlpdsWriteResendsSustained

**Means:** an entry node keeps resending writes answered `RepoLoading`
(`reason="loading"`: the owner's worker didn't start them within 1 s, usually a cold
repo load), `ShardMoved` (`moved`) or unreachable owners. Bursts for seconds after a
restart/handoff are by design; 10 minutes is not.

**Confirm:** `vlpds_writes_abandoned_total` on the owners, `vlpds_repos_loading`,
`vlpds_repo_load_seconds` p99, `vlpds_lease_events_total` (moves).

**Do:** `loading`: cold loads too slow (store latency, repo cache too small: see
[VlpdsRepoCacheMissRateHigh](#vlpdsrepocachemissratehigh)). `moved`: ownership
flapping. `unreachable`: an owner is down but still assigned.

### VlpdsFirehoseEmitDelayHigh

**Means:** p99 from seq assignment of a batch's oldest event to its emit over 2 s.
The merger emits only at the minimum watermark over every node log, so one slow
log, a slow peer stream, or a large wall-clock offset between nodes delays the
firehose on every node.

**Confirm:** `getClusterStatus` `firehose.sources[]`: the log whose `watermark`
trails (seqs are `unix_micros x 256 + writer`; divide by 256 for micros). Which
node owns that log (`nodes[].log`)? Its commit latency/watermark lag. Clock sync
on all hosts (NTP/chrony offset). `vlpds_firehose_merge_queue_bytes`.

**Do:** fix the slow node; fix clock sync.

### VlpdsFirehoseEmitDelayCritical

**Means:** p99 over 20 s: relays/consumers see minutes-old data soon.

**Do:** as above. If one node's log is the laggard and the node is unhealthy,
SIGTERM it: a graceful stop fences its own log so followers drain it and drop it
as a source.

### VlpdsFirehoseStalled

**Means:** the cluster commits but this node's merger emitted nothing for 5
minutes: the merge is held at some log's watermark.

**Causes:** a dead log that nobody fenced (followers drain a log only up to a
fence; DESIGN notes an unfenced log stalls every peer's firehose), a peer stream
stuck and S3 catch-up failing, a node whose clock is far behind (its idle
watermark advertises its clock).

**Confirm:** `firehose.minWatermark` and `sources[]` in `getClusterStatus`; the
stuck log's lease (`nodes[]`) and `fencedLogs`.

**Do:** a dead node's log is fenced by the node that takes over its shards; if
none were left to take, restarting the dead node with the same `--node-id`
fences its previous log at join (`vlpds_peer_takeovers_total{reason="restart"}`). Fix clocks. Restart the stalled node
as a last resort.

### VlpdsFirehoseConsumersTooSlow

**Means:** subscribers more than `--firehose-max-lag-mb` (default 128 MiB; the
node's value is `vlpds_firehose_max_lag_bytes`) behind are cut off with `ConsumerTooSlow` and resume from their cursor. Isolated cases are the
consumer's problem; a high rate across subscribers points at the server.

**Confirm:** `vlpds_firehose_subscribers`, `vlpds_firehose_bytes_sent_total` rate vs
NIC, `vlpds_runtime_tick_late_seconds`, `--firehose-threads` (default 4) CPU.

**Do:** server-side: network or firehose threads. Consumer-side: nothing.

### VlpdsFirehoseMergeSpilling

**Means:** a log exceeded the merger's queue budget (`--firehose-merge-queue-mb`,
default 256 MiB, `vlpds_firehose_merge_queue_budget_bytes`) while waiting for the minimum watermark; the merger now reads it back
from S3 in chunks (`vlpds_firehose_merge_spill_segments_total`, extra GETs).

**Do:** find the laggard log as in [VlpdsFirehoseEmitDelayHigh](#vlpdsfirehoseemitdelayhigh).

### VlpdsPeerLogStreamLagging

**Means:** a peer following this node's log fell behind the 128 MiB live ring
(`--live-ring-mb`) and was dropped; it catches up from S3 segments.

**Do:** look at the follower node (CPU, network). Frequent drops raise S3 GETs and
firehose delay.

### VlpdsControlPlaneTimeouts

**Means:** control-plane object-store calls (`get`, `put`, `list`, `delete`,
`fence`, `fence-scan`) were abandoned at `min(TTL, 5 s)`; the step retries next
tick. Lease **renewals** are separate: they are never timed out and not counted
here (their own metrics: `vlpds_lease_renew_seconds`, `vlpds_lease_renew_errors_total`).
But a store that takes 5 s for control-plane calls is past the 0.4 x TTL
renewal ceiling at the default TTL (4 s), so lease lapses (exit 5) are likely
next; at 60 s the ceiling is 24 s and there is more room.

**Confirm:** logs `control-plane <op> timed out`, `node lease renew error`;
`vlpds_object_store_request_seconds{component=~"ctl_.*"}` and SlateDB latency on
the same node; `vlpds_object_store_requests_total{result="cancelled"}` (the
abandoned calls); one node or many (see
[VlpdsObjectStoreBrownout](#vlpdsobjectstorebrownout)).

**Do:** single node -> its network path to the store. Many -> provider status.

### VlpdsObjectStoreBrownout

**Means:** two or more nodes, in the same 5 minutes, either timing out
control-plane calls or failing over 1/s of their object-store requests
(`vlpds_object_store_requests_total{result=~"error|timeout"}`, any component:
the failures [VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors) and
[VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) count on one
node). DESIGN: a cluster-wide brownout past the 0.4 x TTL renewal ceiling (4 s at
the default TTL) stops every node.

**Confirm:** `sum by (instance, component, result) (rate(vlpds_object_store_requests_total{result=~"error|timeout"}[5m]))`
and `sum by (instance, op) (increase(vlpds_cluster_store_timeouts_total[5m]))`
across nodes; the store provider's status page.

**Do:** [Object-store outage](#object-store-outage) procedure.

### VlpdsObjectStoreRequestErrors

**Means:** over 1/s of this node's own object-store requests failed (`result`
`error` or `timeout`) on one key component outside shard state, for 5
minutes. Counted at the bottom of vlpds' store clients (`src/objstats.rs`):
control plane (`ctl_lease`, `ctl_assign`, `ctl_writer`, `ctl_version`),
`log_segment` (segment PUTs, fences, replay, firehose backfill and follower
catch-up), `retention_report`, `account_index`, `blob`. Shard state
(`state_*`, SlateDB's requests) is [VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors),
the same counter and threshold. `not_found` and `precondition` (a lost CAS /
create race) are normal answers; `cancelled` is a caller that gave up
(control-plane deadline, a lost hedge).

**Confirm:** `sum by (component, op, result) (rate(vlpds_object_store_requests_total{instance="..."}[5m]))`;
the matching warn/error log lines.

**Do:** credentials, permissions, throttling (S3 503 SlowDown), provider status.

### VlpdsObjectStorePermitsSaturated

**Means:** for 10 minutes, over 1/s of this node's object-store requests found
every in-flight permit of their client taken and queued
(`vlpds_object_store_permit_waits_total{client,lane}`). Each client (`log`,
`state`, `ctl`) bounds its requests in flight and keeps as many connections
pooled (DESIGN.md §7, "Object-store clients"), so a burst (a takeover's shard
opens, replay and cold loads) queues instead of opening a connection per
request: unbounded, that once took a host's every ephemeral port, failed the
lease renewals and fail-stopped the survivors of a kill -9. Brief waits
during a takeover are by design; sustained ones mean the pool is too small for
the load, or the store got slower (permits are held longer).

**Confirm:** `vlpds_object_store_inflight` against
`vlpds_object_store_inflight_limit` by `client` and `lane` (pinned at the
limit = saturated); `vlpds_object_store_permit_wait_seconds` p99; whether
`vlpds_object_store_request_seconds` rose at the same time
([VlpdsObjectStoreLatencyHigh](#vlpdsobjectstorelatencyhigh): the store is
slow, and more permits won't help). On the host, `ss -tn dst <store addr> | wc -l`
for the store connections and `ss -s` for TIME_WAIT: with the bound,
connections stay at or under the permits and TIME_WAIT stays flat.

**Do:**
- `state` main lane at its limit while the store is healthy: raise
  `--store-inflight` (default 1,024); `log` main lane: `--log-store-inflight`
  (default 256). A node holds at most about the sum of its permits in
  connections, so keep (nodes per host x permits) well under the ephemeral
  port range (`net.ipv4.ip_local_port_range`, 28k ports by default).
- `log` reserved lane (segment PUTs): it is max(64, 4 x `--log-inflight`);
  waits there mean PUTs are slow, see
  [VlpdsSegmentPutLatencyHigh](#vlpdssegmentputlatencyhigh).
- `ctl` reserved lane (lease renewals, 8 permits): renewals stuck behind each
  other mean the store is not answering lease PUTs; treat as
  [VlpdsLeaseRenewalSlow](#vlpdsleaserenewalslow).

### VlpdsControlPlaneLatencyHigh

**Means:** p99 of control-plane object-store requests (`ctl_*` components:
leases, assignments, writer claims) over 1 s for 10 minutes; normally tens of
ms. Every takeover step is a few of these in sequence, and the lease renewal is
one: past 0.4 x TTL nodes fail-stop.

**Confirm:** `vlpds_object_store_request_seconds` by `component` and `op` on the
node; [VlpdsLeaseRenewalSlow](#vlpdsleaserenewalslow); the same on other nodes.

**Do:** as [VlpdsControlPlaneTimeouts](#vlpdscontrolplanetimeouts).

### VlpdsObjectStoreErrors

**Means:** over 1/s of this node's shard-state object-store requests (SlateDB:
WAL, memtable flushes, manifests, SST reads, compaction, GC) failed (`result`
`error` or `timeout`) on one `state_*` component (`state_wal`,
`state_manifest`, `state_sst`, `state_compactions`, `state_gc_boundary`,
`state_other`), for 5 minutes. Counted by vlpds under SlateDB
(`vlpds_object_store_requests_total`, `src/objstats.rs`), like
[VlpdsObjectStoreRequestErrors](#vlpdsobjectstorerequesterrors) for every other
component. SlateDB retries; persistent apply failure exits 4.

Not SlateDB's own `slatedb_object_store_error_count_total`: it counts every
call that didn't succeed, including the not-found GETs of normal traffic
(each node's compactors poll for manifests and compactions about 12 times a
second) and lost CAS races, and has no label to tell them apart. A steady
rate there is not trouble by itself; it is only useful next to this one.

**Confirm:** `sum by (component, op, result) (rate(vlpds_object_store_requests_total{instance="...",component=~"state_.*",result!="ok"}[5m]))`;
logs. Failures only on `state_compactions` / `state_gc_boundary` (compaction,
GC) don't affect acks directly but let L0 grow.

**Do:** credentials, permissions, throttling, provider status.

### VlpdsObjectStoreLatencyHigh

**Means:** SlateDB's store p99 over 1 s. Hurts cold repo loads, checkpoints
(~37 ms each normally) and apply.

**Do:** as [VlpdsSegmentPutLatencyHigh](#vlpdssegmentputlatencyhigh). Check the
local SST disk cache (`--cache-dir`) is set and healthy (hit rates in
`slatedb_db_cache_access_count_total`).

### VlpdsSlateDbL0Stalls

**Means:** SlateDB blocked writes because a shard has too many L0 SSTs:
compaction is behind. Segment apply (and so acks) waits.

**Confirm:** `slatedb_db_l0_sst_count`, `vlpds_compaction_poll_switches_total`,
failed shard-state requests in
`vlpds_object_store_requests_total{component=~"state_.*",result=~"error|timeout"}`
([VlpdsObjectStoreErrors](#vlpdsobjectstoreerrors); SlateDB's
`slatedb_object_store_error_count_total{component="compactor"}` also counts
its normal not-found polls),
`vlpds_commit_stage_seconds{stage="apply"}`.

**Do:** fix store errors/latency; check CPU for the compactor. **(unverified)**
`--compaction-polling` is adaptive by default; switching to fast polling may help.

### VlpdsCheckpointsStalled

**Means:** the node writes segments but checkpointed no shard for 10 minutes.
Checkpoints (applied marker + memtable flush, one shard every `interval/shards`)
bound a successor's replay and let retention delete the log.

**Confirm:** logs for flush/write errors; `vlpds_checkpoint_shard_seconds`;
`vlpds_retention_replay_hold_segments` growing.

**Do:** fix store errors. A node that cannot checkpoint will cost a long replay
on takeover; consider a graceful restart once the store is healthy (a graceful
close checkpoints every shard, so the successor replays nothing).

### VlpdsReplayBacklogHigh

**Means:** this node keeps over ~15 minutes of its log only because a crash
replay could need it (`vlpds_retention_replay_hold_segments`, divided by the
segment rate). A takeover would replay about that much before serving those
shards. Past takeovers' replay: `vlpds_recovery_replayed_segments_total`,
`vlpds_recovery_replay_seconds`, `vlpds_shard_open_seconds{kind="replay"}` on the
nodes that took over.

**Do:** as [VlpdsCheckpointsStalled](#vlpdscheckpointsstalled).

### VlpdsRetentionFailing

**Means:** most retention passes in the last hour failed (`log retention pass
failed`). Logs stop shrinking: storage and LIST cost grow; nothing is lost.

**Do:** read the error; usually store permissions (DELETE) or throttling. Never
delete log objects by hand to compensate.

### VlpdsRetentionNotRunning

**Means:** no pass (ok or error) for 15 minutes on a scraped node. A pass runs
every `--log-retention-interval` (default 60 s); a pass that hangs on a store call would look like this
**(unverified: retention calls are not individually timed out)**.
`vlpds_retention_pass_seconds` shows how long the finished passes took (a
creeping p99 precedes a hang), and `vlpds_object_store_requests_total{result="cancelled"}`
/ request latency for `log_segment` and `retention_report` the store side.

**Do:** check logs; restart the node gracefully if the task is wedged.

### VlpdsDeadLogUnfenced

**Means:** the dead-log pruner (the owner of slot 0's shard) has seen a log with
no live writer that nobody fenced, for 30 minutes
(`vlpds_retention_dead_logs{state="unfenced"}`). Followers drain a log only up to
its fence, so an unfenced dead log holds every node's merged firehose at its
watermark, and retention never prunes it
(`vlpds_retention_dead_log_segments` counts what dead logs still hold).

**Causes:** its node died owning no shards (nobody takes over, so nobody fences);
its shards' takeover keeps failing ([VlpdsShardOpenErrors](#vlpdsshardopenerrors),
control-plane errors).

**Confirm:** `fencedLogs` and `firehose.sources[]` in `getClusterStatus`; the
log id in `log/` (`<node-id>.<micros>`) names the node.

**Do:** restart that node id (startup fences its previous incarnation's log) or
fix the failing takeover. Never write a fence object by hand.

### VlpdsReshardGcFailing

**Means:** most retired-state GC passes (`reshard GC pass failed`, on the owner
of slot 0's shard) failed in the last hour. Split/merge parents' state dirs
(`state/{id}/`) and their `assign/` records stop going away: storage, and the
`assign/` LIST every step makes, grow with each op. Nothing is lost.

**Causes:** store permissions (DELETE) or throttling; the node's lease not
valid (`node lease not valid: no deletes`: a node about to fail-stop); a dir
SlateDB refuses to delete.

**Do:** read the error. A dir half-deleted by a failed pass is finished by the
next one (SlateDB's `.deleting` marker). Never delete `state/` objects by hand:
a live shard may read a retired dir's SSTs (DESIGN.md "Retired state GC").

### VlpdsRetiredStateReferenced

**Means:** a retired shard's dir holds no checkpoint, yet some shard's manifest
lists SSTs in it (`vlpds_reshard_gc_retired_dirs{state="referenced"}`). Every
clone pins what it reads with a checkpoint, so this is a SlateDB invariant
broken (a bug, or hand edits). The GC keeps the dir, so nothing is lost now; a
later fix that deletes the reference would let it go.

**Confirm:** the log line `retired state dir holds no checkpoint but a manifest
lists its SSTs` names the shard; `slatedb` admin `read_manifest` on each live
shard shows which lists it in `external_dbs`.

**Do:** keep the dir; file a bug with both manifests.

### VlpdsRetiredStateGrowing

**Means:** more retired state dirs than live shards for 6 hours
(`vlpds_reshard_gc_retired_dirs{state="total"}`). Dirs normally go within the
grace (`--reshard-gc-grace`, 1 h) after the shards that read them detached.

**Look at:** `vlpds_reshard_gc_retired_dirs` by state on the GC leader:
`checkpoint` (a clone still reads it: see `vlpds_shards_with_inherited_ssts` per
node and [VlpdsForcedDetachFailing](#vlpdsforceddetachfailing); a reader or
backup holds a named checkpoint), `grace`, `other` (no manifest without a
delete marker, or an assignment with an owner: log `a shard out of the layout
has an owner`). A reshard op left pending blocks the dir half entirely
(`getClusterStatus` layout `op`; abort or finish it).

**Do:** fix the cause; GC catches up by itself (8 dirs per pass).

### VlpdsForcedDetachFailing

**Means:** this node's forced detach compactions (a shard still reading SSTs of
a split/merge parent gets one, `--forced-detach-after` after it opened) keep
failing SlateDB's validation and none completed for 2 hours. Occasional
failures are normal (the shard's own compaction took a source first; it is
resubmitted next pass). The parents stay pinned, so their dirs stay.

**Look at:** `slatedb::compactor` `compaction validation failed` lines for the
shard; `vlpds_shards_with_inherited_ssts`; L0 depth (no forced compaction is
submitted while L0 runs deep).

**Do:** usually nothing: a shard under steady ingest compacts its L0 itself and
the next submission lands. If one shard never detaches, a graceful restart of
the node hands it to a peer that starts over.

### VlpdsMemoryHigh

**Means:** RSS over 85% of `vlpds_memory_limit_bytes` (physical RAM, or the
cgroup limit when lower, as the node reads it) for 10 minutes. A node that can't
read either exports no limit and these alerts stay silent.

**Confirm:** `vlpds_jemalloc_bytes{stat}` (allocated vs resident vs retained),
`sum by (instance) (vlpds_repo_cache_bytes)` (loaded MST paths) vs `vlpds_repo_cache_capacity_bytes`
(`--repo-cache-mb`, 4 GiB default), `mst_store` node cache (`--lazy-mst-node-cache-mb`, 256 MiB), `vlpds_cache_bytes`
(`--cache-budget-mb`, default 10% of RAM/cgroup), SlateDB `--block-cache-mb`
(4 GiB), `vlpds_firehose_ring_bytes`, `vlpds_log_live_ring_bytes`,
`vlpds_firehose_merge_queue_bytes`, `slatedb_db_total_mem_size_bytes`.

**Do:** lower the budgets that dominate, or move to a bigger box.

### VlpdsMemoryCritical

**Means:** RSS over 95%: an OOM kill is close. An OOM kill is a crash: peers take
over in 3-5 s (refused probe) and replay; nothing acked is lost.

**Do:** a graceful restart (SIGTERM) now is cheaper than the OOM (handoff without
replay). Then fix as in [VlpdsMemoryHigh](#vlpdsmemoryhigh).

### VlpdsRepoCacheMissRateHigh

**Means:** over 20% of repo lookups for queued requests start a cold load
(head + account reads, `M/` prefetch, MST root and paths) for 30 minutes.

**Confirm:** `vlpds_repo_evictions_total` rate, `vlpds_repo_cache_bytes` near
`--repo-cache-mb`/workers, `vlpds_cached_repos` near `--cache-per-worker`
(50,000), ownership churn (each move empties the cache for those shards),
`vlpds_lazy_mst_fallbacks_total` (opens rebuilt from all records: slow).

**Do:** raise `--repo-cache-mb` / `--cache-per-worker` if memory allows; stop churn.

### VlpdsRepoLoadErrors

**Means:** cold repo loads fail (`result="error"`; `not_found` and `stale` are not
errors). Requests for those repos fail.

**Do:** logs for the repo/shard and error; store errors on the node.

### VlpdsLazyMstInvalid

**Means:** a repo's persisted MST node (or rebuilt subtree) did not
match its link; the repo was rebuilt from its records (correct result, slower).
It indicates inconsistent derived state and should not happen.

**Do:** capture the log lines (repo DID, shard) and file a bug. No operator action
on data.

### VlpdsKeyServiceUnavailable

**Means:** calls to the KEK's key service (Cloud KMS) fail or time out
(`vlpds_kms_requests_total{result="unavailable"}`; the `key service unavailable`
warn log names the key and the error). Accounts whose signing key is cached keep
writing. Cold accounts (first write since the node started or took the shard)
get **503 `KeyUnavailable`**, and nothing is written. createAccount, reserveSigningKey,
setupTotp and TOTP logins fail with 503. Reads, exports, the firehose and the
proxy for warm accounts are unaffected.

**Do:** follow [Key service (KMS) outage](#key-service-kms-outage).

### VlpdsSecretUnwrapRejected

**Means:** a wrapped secret failed authentication under its KEK: a KEK file
with the right id but other bytes, a Cloud KMS key that rejects the ciphertext,
or a corrupt row. That account's writes fail with 500. A blob under a KEK the
node doesn't have at all fails the same way but only logs (`wrapped under unknown
key-encryption key L…/G…`): usually an old KEK retired before the rewrap finished.

**Do:** find the DID in the `secret unwrap failed` / `repo load failed` logs. If
the kid is unknown, add the old KEK back (`--kek-old-file` / `--gcp-kms-old-key`)
on every node and rerun the rewrap ([KEK rotation](#kek-rotation)). Otherwise
compare the node's KEK config with its peers'. Never "fix" a row by hand.

### VlpdsPlcDirectoryUnavailable

**Means:** writes to the PLC directory (`--plc-url`) fail as unavailable
(`vlpds_plc_requests_total{result="unavailable"}`: 5xx, 429, timeouts,
connection errors; the `PLC directory request failed` warn log has the DID,
op and error). New accounts' DIDs are registered before the account exists,
so **createAccount (and OAuth sign-up) fails with 500** and leaves nothing
behind (the handle and email are free again). **updateHandle**, admin
updateAccountHandle and updateAccountSigningKey fail the same way with no
local change; signPlcOperation can't read the last op; submitPlcOperation
can't forward. Everything else (logins, writes, reads, firehose, sync) is
unaffected: existing DIDs resolve from the directory's own replicas.

**Do:** follow [PLC directory outage](#plc-directory-outage).

### VlpdsPlcOpsRejected

**Means:** the directory refused (4xx) an op this server built and signed
(`op` label). Usually: the DID no longer lists this server's rotation key
(the account migrated away, or a user removed the key and the account
wasn't told: `update_handle` / `update_signing_key`), a rotation key the
directory doesn't accept, or a bug in op construction (`create`: every
signup failing). The directory's message is in the warn log and in the 500
the client got.

**Do:** for `create`, treat as an incident: no one can sign up. Check the
log message (`Invalid signature`, `Operation too large`, ...), the node's
`PLC registration on` startup line (rotation key did:key), and whether a
deploy changed op construction; roll back. For updates of single DIDs,
fetch `$PLC/{did}/log/last`: if its `rotationKeys` lack this server's key
(current or retired) the account is no longer ours to update; tell the
user (they hold the remaining rotation keys). For `rotate_key`, the DID's
last op was signed after our read (retry `rotatePlcKeys`).

### VlpdsSignatureFault

Also covers **VlpdsSignatureFaultFailStop** (the previous process exited
with code 6, `signature_fault`).

**Means:** a signature failed verification against the key's own public key
right after it was made (`vlpds_signature_verify_failures_total{purpose}`:
`commit`, `service_auth`, `oauth_token`, `plc_operation`; `key_load` = a cached signing key's
scalar no longer derives its public key). Correct code never does this: the
CPU or memory of this host computed something wrong (bad DIMM, Rowhammer,
overheating, failing CPU). The bad signature was **not** emitted: the node
signed once more with a fresh nonce, and if that failed too the write got a
503 `SignatureFault` with nothing applied. Nonces are hedged, so a faulty
signature leaks nothing even if one had got out, but the host is no longer
trustworthy. Three failures within a minute fail-stop the node (exit 6);
the supervisor restarts it on the same host, so expect it to recur.

**Confirm:** `signature failed verification against the signing key's public
key` error logs (`purpose`, `recent`); `vlpds_last_exit_reason_info{reason="signature_fault"}`.
On the host: `journalctl -k | grep -iE 'mce|edac|machine check|hardware error'`,
`edac-util -v` (or `/sys/devices/system/edac/mc/mc*/ce_count`/`ue_count`),
`rasdaemon`/`ras-mc-ctl --summary` if installed, CPU temperatures, and the
cloud provider's host-maintenance or hardware-degradation events.

**Do:**
1. Drain the host now, even after a single fault: stop vlpds there
   gracefully (`SIGTERM`; peers take its shards over) and keep the
   supervisor from restarting it on that host.
2. Check ECC/EDAC and machine-check logs as above. Corrected-error counts
   that climb, any uncorrected error, or MCEs = hardware fault.
3. Replace the host (cloud: stop/start onto new hardware, or recreate the
   VM; bare metal: pull it and run memtest86+ / the vendor diagnostics)
   before it serves again. Don't return it on the strength of a clean
   restart.
4. Nothing to repair in data: no faulty signature was sequenced, and the
   failed writes were refused with a retryable 503.
5. Faults on several hosts at once point at a software or build problem
   rather than hardware: compare the vlpds versions (VlpdsMixedVersions)
   and escalate to development.

### VlpdsCacheAtCapacity

**Means:** a bounded in-memory cache (`session_tokens`, `oauth_tokens`,
`proxy_accounts`, `proxy_jwts`, `did_docs`, `lexicons`, `oauth_clients`,
`permission_sets`, `security_controls`, `signing_keys`, `recent_writes`) has
been at its entry cap for 6 hours. A full LRU is normal for hot caches; it
matters only with symptoms (proxy latency, PLC lookups, KMS calls).
`vlpds_proxy_cache_total{result}` gives a hit ratio for the proxy fast path and
`vlpds_signing_key_cache_total{result}` for unwrapped signing keys (a full
`signing_keys` cache with many misses means KMS unwraps on cold writes); the
others have no hit/miss metric.

**Do:** raise `--cache-budget-mb` or set `--cache-entries <cache>=<n>` if a
symptom correlates.

### VlpdsFirehoseMergeQueueNearBudget

**Means:** the merger holds over 80% of its byte budget
(`--firehose-merge-queue-mb`, default 256 MiB, exported as
`vlpds_firehose_merge_queue_budget_bytes`) waiting for the minimum watermark. Past the budget it spills (see
[VlpdsFirehoseMergeSpilling](#vlpdsfirehosemergespilling)).

**Do:** find the laggard log (same as emit delay).

### VlpdsRuntimeStalls

**Means:** the 10 ms ticker on the tokio runtime was late over 5% of the time:
runtime threads are blocked or starved. Lease renewals, step loops and acks run on
it, so heavy stalls risk lease lapses.

**Confirm:** `tokio runtime stall` logs (late_ms), host CPU/load, a CPU profile
(`just profile`, on a `--features profiling` build).

**Do:** reduce co-located load; add CPU; profile for blocking work on the runtime.

### VlpdsRuntimeSaturated

**Means:** tokio workers over 90% busy for 15 minutes: the node is CPU bound
(DESIGN: ~12k commits/s per core including HTTP).

**Do:** add nodes (shards rebalance automatically) or bigger instances.

### VlpdsPeerTlsCertExpiring

**Means:** `vlpds_peer_tls_cert_expiry_seconds{cert}` (notAfter, Unix seconds)
is under 14 days away: `cert="node"` is this node's certificate, `cert="ca"`
the earliest-expiring CA in `--peer-tls-ca`. Once a node cert expires every
peer refuses it (forwards to and from it fail, its log stream stalls every
peer's firehose), and the node won't start or reload with it.

**Do:** [renew the node certificate](#peer-tls-mtls-between-nodes) (`cert="node"`) or
rotate the CA (`cert="ca"`). `vlpds admin tls show <cert>` prints what a file
holds.

### VlpdsPeerTlsReloadFailing

**Means:** the peer TLS files changed (or the node got SIGHUP) but the new set
didn't load: unreadable, a cert that doesn't chain to the CA file, is expired,
names another node (`vlpds://node/<id>` must be this `--node-id`), or a key that
doesn't match. The node keeps using the previous set.

**Confirm:** the `peer TLS reload failed` error log says which.

**Do:** fix the files (a half-copied set reloads on the next 60 s poll or
`kill -HUP`); `vlpds admin tls show` on the cert.

### VlpdsPeerTlsHandshakeFailures

**Means:** peer TLS handshakes keep failing. `side="server"`: callers of this
node's `--peer-listen` were refused (no client certificate, one from another
CA, expired, a handshake timeout; also a port scan). `side="client"`: this node
refused a peer's server certificate (another CA, the advertise host not in its
SANs, or a node id other than the lease at that address says).

**Confirm:** `peer TLS handshake failed` (server) / `refused the peer's
certificate` (client) warn logs name the address and reason.

**Do:** compare `vlpds admin tls show` of each node's cert with its
`--node-id` and `--advertise-url` host, and their `--peer-tls-ca` files (all
nodes must trust the CA every node's cert comes from; mid CA rotation, both).

---

## Procedures

### KEK provisioning

Repo signing keys, reserved keys and TOTP secrets are stored wrapped under a
key-encryption key (DESIGN "Secrets at rest"). Outside `--dev-mode` a node
refuses to start without one. Every node of a cluster needs the same KEK set.

- **Cloud KMS (production on GCS).** Create a symmetric ENCRYPT_DECRYPT key in a
  multi-region or dual-region location, e.g.
  `gcloud kms keyrings create vlpds --location us` and
  `gcloud kms keys create secrets --keyring vlpds --location us --purpose encryption`.
  Grant the nodes' service account `roles/cloudkms.cryptoKeyEncrypterDecrypter`
  on that key only. Nobody routinely gets `cloudkms.cryptoKeyVersions.destroy`.
  Set the destroy-scheduled duration to its maximum. Start nodes with
  `--gcp-kms-key projects/P/locations/us/keyRings/vlpds/cryptoKeys/secrets`
  (`VLPDS_GCP_KMS_KEY`). On GCE, tokens come from the metadata server
  (`GCE_METADATA_HOST` overrides it). Elsewhere, create a key for a service
  account holding only that role (`gcloud iam service-accounts keys create
  sa.json --iam-account ...`), distribute it like the other secrets (mode
  0400) and pass `--gcp-credentials-file sa.json`
  (`VLPDS_GCP_CREDENTIALS_FILE`; `GOOGLE_APPLICATION_CREDENTIALS` also
  works). Only `service_account` key files are accepted; the node exchanges a
  signed JWT for an access token on first use, caches it about an hour and
  refreshes it before expiry or on a 401. Losing this key loses every account's
  signing key (each would need a PLC rotation), so it is part of the backup plan.
- **Local KEK.** `openssl rand -out kek.bin 32` (raw 32 bytes; 64 hex chars or
  base64 also work). Distribute it like the other secrets (sops / Ansible Vault),
  mode 0400. Pass `--kek-file /path/kek.bin` (`VLPDS_KEK_FILE`) or the value in
  `VLPDS_KEK`. Back it up offline: it is the only way to read the stored keys.
- Check after start: the `secrets at rest` log line prints `kek=` (the current
  key id: `L…` local, `G…` Cloud KMS) and `unwrap_keks=`. It must match on
  every node. `vlpds_kms_requests_total` shows wraps (account creation) and
  unwraps (cold loads).
- The node caches unwrapped signing keys (`signing_keys` cache, sized from
  `--cache-budget-mb`; `--cache-entries signing_keys=N`). Cloud KMS unwraps are
  limited to `--kms-concurrency` (64) in flight per node, 5 s each; wraps have
  their own pool of a quarter of that (16), and their failures don't trigger the
  1 s fail-fast, so a burst of reserveSigningKey or createAccount calls can't
  starve cold signing-key loads. reserveSigningKey is rate limited (100/h per
  IP, 5000 new reservations/day per node).

### KEK rotation

New wraps use the current KEK. Old blobs keep working as long as their KEK is
configured for unwrap.

1. Roll every node with the new KEK as current and the old one as unwrap-only:
   - local: `--kek-file new.bin --kek-old-file old.bin`;
   - Cloud KMS, new key version in the same CryptoKey: nothing to configure
     (`gcloud kms keys versions create` + set primary). KMS keeps decrypting
     old versions;
   - Cloud KMS, another CryptoKey (or local -> KMS): `--gcp-kms-key NEW
     --gcp-kms-old-key OLD` (or `--gcp-kms-key NEW --kek-file old.bin`; with
     `--gcp-kms-key` set, the local KEK is unwrap-only).
2. On **every** node (each covers the shards it owns):
   `vlpds admin rewrap-secrets` (every node at once), or per node
   `curl -XPOST -u admin:$ADMIN -H 'content-type: application/json' -d '{}' $NODE/xrpc/vlpds.admin.rewrapSecrets`.
   For a version rotation inside one CryptoKey, pass `{"checkVersions": true}`
   (one KMS decrypt per secret). It reports `stale` (rewrapped), `failed` and
   the first errors. Rewrapping doesn't change keys, emits no events and doesn't
   evict repos. Re-run until `failed` is 0.
3. Verify on every node with `{"dryRun": true}` (plus `checkVersions` as above)
   until `stale` is 0. Shards that moved during step 2 show up here: rerun step 2.
4. Only then drop the old KEK (`--kek-old-file`), or disable the old KMS
   version. **Keep the old KEK material (or keep the version disabled, not
   destroyed) for the backup retention period**: backups and log segments still
   hold blobs wrapped under it.

### Key service (KMS) outage

1. Confirm: `vlpds_kms_requests_total{result="unavailable"}` on all nodes, the
   `key service unavailable` log (HTTP status or timeout), Google Cloud status,
   and IAM (a 403 from a removed role looks the same). Clients retry 503s; after
   a failure a node fails cold unwraps fast for 1 s before it tries KMS again.
2. **Don't restart nodes and don't move shards** (no rolling deploys, splits or
   handbacks) while KMS is down. A restart or takeover empties the key cache,
   which turns warm accounts cold: every account the node owns becomes
   unwritable until KMS is back.
3. If one node is affected (network or metadata-server problem), drain it with
   SIGTERM. Its shards move to nodes that can reach KMS.
4. When KMS is back, cold writes succeed on their next retry. Nothing needs
   replaying: refused writes were never applied.
5. If KMS is lost for good (key destroyed), restore the key from a backup if one
   exists. Otherwise, start nodes with a new KEK: accounts can no longer sign, and
   each needs a new signing key installed (`admin.updateAccountSigningKey`) and a
   PLC rotation, which needs the PLC rotation keys the users or their recovery
   flow hold.

### PLC rotation key provisioning

Accounts' DIDs are registered with the PLC directory, and every DID lists
this deployment's **PLC rotation key** (DESIGN "PLC identity"). Outside
`--dev-mode` a node refuses to start without one (`--plc-mode unregistered`
is dev-only). Every node of a cluster needs the same key. Losing it means
the server can no longer update its accounts' DIDs (handle changes,
migrations out); users with their own recovery key can still recover.

1. Generate and wrap it under the KEK (Cloud KMS in production) on a host
   with the node's KEK config:
   `vlpds --gcp-kms-key projects/P/locations/us/keyRings/vlpds/cryptoKeys/secrets --wrap-plc-rotation-key </dev/null >plc-rotation.key`
   (empty stdin = a new key; or pipe 64 hex chars to wrap an existing key,
   e.g. a reference PDS's `PDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX`). The
   did:key goes to stderr with the logs: record it. stdout is the wrapped
   key only: check the file is one `vw1.` line (`head -c 4 plc-rotation.key`;
   builds before the deploy fixes logged to stdout and put log lines in it).
2. Distribute `plc-rotation.key` like the other secrets (sops / Ansible
   Vault), mode 0400, and start nodes with `--plc-rotation-key-file`
   (`VLPDS_PLC_ROTATION_KEY_FILE`). The file is useless without KMS decrypt
   on the KEK. Alternatively pass the hex in
   `VLPDS_PLC_ROTATION_KEY_K256_PRIVATE_KEY_HEX` (plaintext in the
   environment; not recommended).
3. Optional: `--plc-recovery-did-key did:key:...` (an offline key the
   operator holds) goes ahead of the server key in every new DID.
4. Check after start: the `PLC registration on` log line shows `plc_url` and
   `rotation_key` (the did:key from step 1) on every node;
   `getRecommendedDidCredentials` returns it in `rotationKeys`.
5. Back up the wrapped file with the KEK backup plan: it needs the KEK to
   open, and it is not in the bucket.

For local e2e runs, point `--plc-url` at a local did-method-plc server
(`docker run` its image with Postgres) or use `--dev-mode` without a key
(unregistered DIDs). Never point a test cluster at plc.directory.

### PLC rotation key rotation

1. Make a new key (step 1 above). Roll every node with the new key as
   current and the old one retired:
   `--plc-rotation-key-file new.key --plc-rotation-key-old-file old.key`.
   New DIDs list the new key; any update of an old DID (handle change,
   signPlcOperation) is signed by the old key and lists the new one instead.
2. On **every** node (each covers the shards it owns):
   `vlpds admin rotate-plc-keys --dry-run` (every node at once), or per node
   `curl -XPOST -u admin:$ADMIN -H 'content-type: application/json' -d '{"dryRun": true}' $NODE/xrpc/vlpds.admin.rotatePlcKeys`,
   reports `current`, `rotated` (still on the old key), `foreign` (DIDs that
   list neither: migrated away, or synthetic bulkCreate DIDs) and `failed`.
   Run it without `dryRun` to submit the updates (4 in flight per node; the
   directory rate-limits per DID and per IP, so expect it to take a while
   for millions of accounts). Re-run until `failed` is 0, then dry runs
   until `rotated` is 0 on every node (rerun where shards moved).
3. Only then drop `--plc-rotation-key-old-file`. Keep the old key material
   offline until you are sure: an op signed by a key a DID no longer lists
   is refused, but a retired key that leaks can still sign for DIDs that
   list it. For a **compromised** key, also consider the 72 h recovery
   window: a higher-priority key (the user's or `--plc-recovery-did-key`)
   can undo ops the attacker signed within 72 h.

### PLC directory outage

1. Confirm: `vlpds_plc_requests_total{result="unavailable"}` on all nodes,
   the `PLC directory request failed` warn log (status or timeout), the
   directory's status page. A 429 means we are rate-limited (bulk
   `rotatePlcKeys`, a signup flood): slow down.
2. Impact while it lasts: createAccount / sign-up, updateHandle, admin handle
   and signing-key changes and submitPlcOperation return 500 with nothing
   changed; clients may retry. Nothing queues on our side and nothing needs
   replaying afterwards. Existing accounts work normally.
3. Don't switch to `--plc-mode unregistered` to keep signups open: those
   DIDs would never exist on the network.
4. If one node is affected (egress or DNS), drain it with SIGTERM; its
   shards move to nodes that can reach the directory.
5. Afterwards: a createAccount whose local part failed after the directory
   accepted the genesis op tombstones the DID; if the tombstone itself
   failed (`tombstoning the DID of a failed account creation` error log),
   that DID is registered without an account: harmless (nothing points
   users at it).

### Rolling deploy

1. One node at a time. Send **SIGTERM** (never SIGKILL). A graceful stop
   (`Cluster::shutdown`): marks its lease `draining` so peers stop counting it,
   closes its shards with one barrier segment + checkpoint and hands them straight
   to settled peers (their nudges skip the control-plane read), waits for its log
   to quiesce (up to 10 s), **fences its own log**, deletes its lease, nudges peers,
   keeps answering 500 ms more, and exits 0. If the fence keeps failing (store
   errors for min(TTL, 30 s) of retries), it keeps its lease and exits 8
   (`shutdown_fence`). The supervisor's restart fences the old log at startup, and
   so do peers once the lease goes quiet. Without that, followers would wait on the
   log forever.
2. Give the supervisor a stop timeout well above the worst case: the close barrier
   may wait 30 s and the quiesce 10 s, so **at least 60 s (derived estimate)**. A
   stop that times out into SIGKILL becomes a crash (fence + replay by peers).
   Failing fence retries (up to min(TTL, 30 s) more) can run past 60 s. A SIGKILL
   there ends the same way as their exit 8: the shards are already handed out, and
   the kept lease gets the log fenced.
3. Start the new binary with the **same `--node-id`** and the same bucket, prefix,
   tokens and `--advertise-url`. It greets peers; peers hand back its fair share
   at their next step.
4. Before the next node, confirm: `sum(vlpds_owned_partitions)` equals
   `vlpds_shard_layout_shards`; the restarted node's `owned` is about `shards / nodes`;
   its `vlpds_last_exit_reason_info{reason="clean"}`;
   `leaseValid: true` on all `nodes[]`; `vlpds_build_info{rev}` is the new rev;
   commit p99 and `vlpds_write_retries_total` back to baseline.
5. Expect: zero client errors for a clean SIGTERM (3-8 when a forward is in
   flight at exit, per DESIGN), resend bursts, cold-load latency on moved repos.

### Rolling upgrade, finalize, rollback

Every format a node persists or sends belongs to a **feature level**
(`src/version.rs`; DESIGN.md "Rolling upgrades and format versioning"). A
build runs levels `MIN_LEVEL..=MAX_LEVEL` (its release notes list them, and
whether each is persistent); the cluster's **active** level is in
`cluster/version`, and every node writes that level's formats whatever its
build. A new build therefore writes byte for byte what the old one writes
until the level is raised, which is what makes rollback a plain redeploy.

**Upgrade** to build B (`MAX_LEVEL = L+1`) on a cluster at level L:

1. Pre-flight: `vlpds admin cluster-status` shows every node healthy and
   `Feature level: L active`; B's `MIN_LEVEL <= L` (a build whose `MIN_LEVEL`
   is past the active level refuses to start: exit 7).
2. Roll B out exactly as [Rolling deploy](#rolling-deploy) (SIGTERM, >= 60 s
   stop timeout, same `--node-id`, step 4's checks between nodes), plus: the
   restarted node's row shows B's rev and `1..=L+1`-style levels, and
   `vlpds_format_errors_total` stays flat.
3. Soak with the whole fleet on B at level L (default 24 h). The console's
   Cluster page and `cluster-status` say "every node can run level L+1:
   finalize available". **Rollback is a plain redeploy** of the previous
   build, node by node, in any order, at any time.
4. Finalize: `vlpds admin cluster finalize --level L+1` (asks; `--yes` off a
   terminal). It writes a raise `target`, lists every lease after that write
   and requires each live node's build to run L+1 (else it clears the target
   and names the nodes: 409 `IncompatibleNodes`, nothing changed), then sets
   `active = L+1`. A node starting during the raise re-reads the object
   after its lease write and exits 7 if it can't run L+1. Watch format
   errors, commit p99 and firehose watermark lag for one TTL. **From now on
   rollback is forward-fix only.**

**Rollback before finalize:** redeploy the previous image node by node with
the same procedure. Nothing to clean up: no byte of level L+1 exists.

**Rollback after finalize:** not possible by redeploy (the old build exits 7
`incompatible_level` at startup, before touching data: VlpdsIncompatibleNode).
Ship B' = B + fix. A persistent level is never lowered (restore from a backup
taken at the old level instead). A level that only gates wire behavior
(non-persistent; the release notes say which) can be lowered when its new
wire behavior is the bug: `vlpds admin cluster lower --level L` (asks; `--yes`
off a terminal; `vlpds.admin.setFeatureLevel {"level": L, "lower": true}`).
It refuses (400) to go past a persistent level or while a raise `target` is
set, and (409 `IncompatibleNodes`) while a live node's build can't run L.
Nodes pick up the lower level within one lease TTL and switch at their next
segment; then the old build can be redeployed.

Feature levels never change by themselves: a node never raises the level at
startup, and finalize (or `cluster lower`) is the only writer of
`cluster/version` after its creation (a fresh prefix starts at its first
node's max level).

**Before a release** with a new level: `just upgrade-ci` (fixtures and the
MANIFEST freeze, the level-gating test, and the two-build `upgrade-rolling`
scenario against the previous release; `just upgrade-ha` runs every
`upgrade-*` scenario: rollback, old-node refusal, raise race). The test
builds use the test-only feature level (cargo feature `test-level`); never
deploy such a build (it logs `TEST BUILD` at startup).

### Replacing a dead host

1. Nothing must be done for data safety: peers fence the dead log and take its
   shards (3-5 s if the port refuses, TTL + skew + replay if the host is
   unreachable: ~12 s at the default TTL, ~72 s at 60 s).
2. Make sure the old process can never come back on its own (power it off). If it
   did, it would find its log fenced or its shards reassigned and fail-stop, but
   don't rely on it.
3. Start the replacement. Reusing the old `--node-id` lets it fence and reclaim
   its previous incarnation's assignments right away; a new id works too. The dead
   node's `nodes/` lease object is deleted automatically once all its shards have
   moved and its log is fenced.
4. Point `--cache-dir` at local NVMe; the cache starts cold (expect cold-load
   latency for a while).
5. Verify as in rolling deploy step 4.

### Adding a node

1. Pick a **unique** `--node-id`. Same bucket, prefix, KEK flags, `--jwt-secret`,
   `--admin-token`, `--internal-token`, `--peer-tls-ca`; issue its node
   certificate (`vlpds admin tls issue --node-id <id> --host <advertise host>`,
   [Peer TLS](#peer-tls-mtls-between-nodes)); set `--peer-listen` and
   `--advertise-url https://<host>:<peer port>` to an address all peers can
   reach. Nodes don't go in `--trusted-proxies`: a forwarding node
   passes the client address over the internal token (DESIGN "Rate limits");
   list only real proxies (load balancers) there.
2. Start it. After it greets every peer, each peer above the new fair share
   `ceil(shards / live)` hands extras to it at its next step.
3. Add the target to Prometheus (job `vlpds`).
4. Verify `owned` per node converges and `VlpdsOwnershipImbalanced` stays quiet.
5. Writer ids are a single byte (seq low byte), so a cluster can't exceed 256
   live node incarnations **(derived from the seq format)**.

### Peer TLS (mTLS between nodes)

Node-to-node traffic (forwards, `/internal/*`, log streams) runs h2 over TLS
1.3 with client certificates on `--peer-listen` (DESIGN.md "Exposure"). A
node certificate names its node with a URI SAN `vlpds://node/<node-id>` and
carries the DNS name or IP of its `--advertise-url` host; both serverAuth and
clientAuth. Peers check, besides the chain: the host, and that the cert names
the node whose lease advertises that address (log streams: the log's node).
The internal token is still required on top.

**Create the CA** (once per cluster, on an operator machine; ECDSA P-256):

```sh
vlpds admin tls ca --out ./pki            # ./pki/ca.crt, ./pki/ca.key (0600)
```

Keep `ca.key` offline (a vault); nodes get only `ca.crt`.

**Issue a node certificate** (per node; `--host` repeats or takes a
comma-separated list of DNS names / IPs):

```sh
vlpds admin tls issue --ca ./pki/ca.crt --ca-key ./pki/ca.key --out ./pki \
  --node-id node-a --host 10.0.0.5 --host node-a.internal   # 365 days (--days)
vlpds admin tls show ./pki/node-a.crt
```

Install `ca.crt`, `node-a.crt` and `node-a.key` (0600, readable by the vlpds
user) on the node and run it with:

```sh
--peer-listen 0.0.0.0:2584 --advertise-url https://10.0.0.5:2584 \
--peer-tls-ca /run/vlpds/ca.crt --peer-tls-cert /run/vlpds/node-a.crt --peer-tls-key /run/vlpds/node-a.key
```

Startup refuses a cert that doesn't chain to the CA, is expired, lacks the
`vlpds://node/` SAN, names another `--node-id`, or doesn't match the key; and
TLS without `--peer-listen` or with an `http://` advertise URL. Keep
`--listen` behind the edge proxy as before and `--peer-listen` reachable only
by peers (it refuses anyone without a node cert, but needn't be public).

Turning TLS on in a running cleartext cluster: a TLS node only calls `https://`
peers and its peer listener only takes node certs, so mixed nodes can't forward
to each other or stream logs (the firehose stalls until all match). Switch all
nodes in one window: stop all (SIGTERM), start all with TLS.

**Renew a node certificate** (alert [VlpdsPeerTlsCertExpiring](#vlpdspeertlscertexpiring)
`cert="node"`; no restart):
1. `vlpds admin tls issue ... --node-id node-a --host ... --force` (same id
   and hosts).
2. Replace the cert and key files on the node (write both, then `kill -HUP`
   the process, or wait up to 60 s for the file poll). The node checks the new
   pair (chain, expiry, node id, key match) and switches; on failure it logs
   `peer TLS reload failed`, counts `vlpds_peer_tls_reloads_total{result="error"}`
   ([VlpdsPeerTlsReloadFailing](#vlpdspeertlsreloadfailing)) and keeps the old one.
3. Confirm `vlpds_peer_tls_cert_expiry_seconds{cert="node"}` moved. New peer
   connections use the new cert; pooled ones keep the old until they close
   (they verified it once, at the handshake).

**Rotate the CA** (`cert="ca"`, or a suspected CA key leak):
1. `vlpds admin tls ca --out ./pki-new`.
2. On every node, make `--peer-tls-ca` a bundle of old + new CA
   (`cat pki/ca.crt pki-new/ca.crt > ca.crt`), then SIGHUP (or wait for the
   poll). Every node now trusts certs from either CA.
3. Issue each node a cert from the new CA (`--ca ./pki-new/ca.crt --ca-key
   ./pki-new/ca.key`), install, SIGHUP, one node at a time; check
   `vlpds_peer_tls_handshake_failures_total` stays flat.
4. Once every node presents a new-CA cert, make `--peer-tls-ca` the new CA
   alone on every node and SIGHUP. After a key leak, also restart the nodes
   (pooled connections authenticated under the old CA close then) and rotate
   `--internal-token`.

A file left half-written is retried on the next poll or SIGHUP; a cert or key
moved away mid-run doesn't matter (the loaded set stays in memory).

### Object-store outage

What happens, from DESIGN and the code:
- Segment PUTs retry until they succeed; commits queue, acks stop, write latency
  climbs, then admission control sheds (`Overloaded`) and forwards time out.
- Renewals slower than 0.4 x TTL (4 s at TTL 10 s, 24 s at 60 s) open validity gaps: nodes stop acking and
  fail-stop (exit 5). A cluster-wide brownout past that ceiling stops **every**
  node. No acked write is lost: acks require durable segments.
- When the store recovers, restarted nodes rejoin, fence the dead incarnations'
  logs (including their own previous one) and replay.

What to do:
1. Confirm it's the store (provider status, `slatedb_object_store_*`,
   `vlpds_object_store_requests_total{result}` / `vlpds_object_store_request_seconds`,
   `vlpds_lease_renew_seconds` and `vlpds_cluster_store_timeouts_total` on all
   nodes, logs).
2. Make sure the supervisor keeps restarting nodes (with backoff) so they rejoin
   as soon as the store answers.
3. Don't lower `--lease-ttl-ms` (smaller ceiling) and don't delete anything. Raising
   the TTL during an incident is not a supported live operation **(unverified)**.
4. After recovery, watch `VlpdsShardsUnowned`, replay time
   (`vlpds_shard_open_seconds{kind="replay"}`, `shards opened` `replayed_ms`),
   `vlpds_last_exit_reason_info` / `vlpds_peer_takeovers_total` (who fail-stopped),
   firehose emit delay and retention catching up.

### Shard split / merge

`vlpds admin shard-split <shard> [--at <slot>]`, `shard-merge <left> <right>`,
`reshard-abort` (only before the flip), `layout` to watch `op`. Shard ids are
u32 and never reused: each split takes two new ids and each merge one from the
layout's `next_id` (aborted ops' ids stay used), so ids grow past the shard
count; they are not positions. Every node's
`vlpds_shard_layout_shards` / `vlpds_shard_layout_version` follow the flip; the
ownership alerts read the count from there.

### Email (SMTP, moderation mail, branding)

DESIGN "Email" has the details. Every node needs the same flags. A flag left
unset falls back to the reference PDS's variable, so a reference `pds.env`
works as is:

| Flag | Env | Reference env | Notes |
|---|---|---|---|
| `--email-smtp-url` | `VLPDS_EMAIL_SMTP_URL` | `PDS_EMAIL_SMTP_URL` | `smtp://user:pass@host:587` / `smtps://...:465`; unset: mail is only logged |
| `--email-from-address` | `VLPDS_EMAIL_FROM_ADDRESS` | `PDS_EMAIL_FROM_ADDRESS` | required with the URL |
| `--moderation-email-smtp-url` | `VLPDS_MODERATION_EMAIL_SMTP_URL` | `PDS_MODERATION_EMAIL_SMTP_URL` | admin `sendEmail` only; unset: uses the main mailer |
| `--moderation-email-address` | `VLPDS_MODERATION_EMAIL_ADDRESS` | `PDS_MODERATION_EMAIL_ADDRESS` | required with the moderation URL |
| `--email-brand-name` | `VLPDS_EMAIL_BRAND_NAME` | `PDS_SERVICE_NAME` | default "{hostname} PDS" |
| `--email-home-url` | `VLPDS_EMAIL_HOME_URL` | `PDS_HOME_URL` | footer link; default https://bsky.app |
| `--email-logo-url` | `VLPDS_EMAIL_LOGO_URL` | `PDS_LOGO_URL` | default: the Bluesky logo, as in the reference |
| `--email-primary-color` | `VLPDS_EMAIL_PRIMARY_COLOR` | `PDS_PRIMARY_COLOR` | default `#067df7` |
| `--email-disable-confirmation-link` | `VLPDS_EMAIL_DISABLE_CONFIRMATION_LINK` | `PDS_EMAIL_DISABLE_CONFIRMATION_LINK` | drops the bsky.app "click here" link |

Setting a URL without its address (or the reverse) fails startup. If mail is
not arriving, check `vlpds_mail_messages_total{result="failed"|"dropped"}` and
the `mail not sent` / `mail dropped` warnings (they log the recipient and
purpose, never the token). `purpose="admin"` is moderation mail. Dev mode
keeps every mail, with its HTML, in `vlpds.admin.getDevMail`.

### Moderation service, earned invites, external handles

Every node needs the same flags. As for email, an unset flag falls back to the
reference PDS's variable:

| Flag | Env | Reference env | Notes |
|---|---|---|---|
| `--mod-service-did` | `VLPDS_MOD_SERVICE_DID` | `PDS_MOD_SERVICE_DID` | the Ozone DID allowed to call the moderator admin methods with a service JWT; unset: admin Basic auth only |
| `--invite-interval-ms` | `VLPDS_INVITE_INTERVAL_MS` | `PDS_INVITE_INTERVAL` | with `--invite-required`: one earned code per this much account age (at most 5 unused); unset: none |
| `--invite-epoch-ms` | `VLPDS_INVITE_EPOCH_MS` | `PDS_INVITE_EPOCH` | Unix ms; only account age after it earns codes (default 0) |

- **Ozone gets 401 `UntrustedIss` "Untrusted issuer"** on admin calls: the
  token's `iss` is not `--mod-service-did` (or `<did>#atproto_labeler`), or the
  flag is unset on the node that answered. `BadJwtSignature` "jwt signature does
  not match jwt issuer": the key in Ozone's DID document (`#atproto`, or
  `#atproto_label` for the labeler issuer) is not the one it signs with; vlpds
  re-resolves the document once before refusing, so a just-rotated key
  works. `BadJwtAudience`: Ozone addressed the token to another DID, not this
  PDS's `--service-did`. Ozone can call getAccountInfo(s),
  get/updateSubjectStatus, sendEmail, getInviteCodes, disableInviteCodes,
  enable/disableAccountInvites and read any account's preferences
  (`app.bsky.actor.getPreferences?did=`); everything else (deleteAccount,
  updateAccountEmail/Handle/Password/SigningKey, createInviteCode(s), the
  `vlpds.admin.*` methods) stays admin Basic auth only.
- **Earned invites**: to stop new codes being earned, unset
  `--invite-interval-ms` (rolling restart); codes already created stay. To cut
  one account off, `com.atproto.admin.disableAccountInvites` (its codes are
  disabled, and codes it earns afterwards are created disabled). Changing
  `--invite-epoch-ms` to now restarts everyone's earning from zero.
- **External handles**: updateHandle to a domain outside `--handle-domain`
  needs a DNS TXT record `_atproto.<handle>` = `did=<the account's DID>` or
  `https://<handle>/.well-known/atproto-did` serving the DID. Both are tried
  at once with a 3 s deadline each, through the host's resolver
  (`/etc/resolv.conf`): "External handle did not resolve to DID" for a handle
  the user swears is set up usually means the node's resolver can't reach
  the zone (check `dig TXT _atproto.<handle>` from the node) or more than one
  `did=` record exists. Dev mode skips the check.
- **Disposable email** domains are refused at createAccount and updateEmail
  ("This email address is not supported, please use a different email."), as
  in the reference. The list is compiled in
  (`src/email_policy/disposable_email_domains.txt`); updating it is a release.

### A user locked out by a second factor

Two factors exist (DESIGN "Email second factor"): the reference's email code
(`emailAuthFactor`, what the Bluesky app offers) and vlpds TOTP. With both on,
only TOTP is asked for.
- **Too many wrong codes** (429 `RateLimitExceeded` on createSession or the
  sign-in page): the factor is locked for 5 min, doubling per further
  lockout up to a day. It clears by itself; there is nothing to reset.
- **Too many wrong passwords from anywhere** (429 on createSession or the
  sign-in page for one account, from every address): the `sign-in-account`
  bucket (100 attempts per hour per account) is spent, e.g. by someone
  guessing. It clears within the hour. App passwords and live sessions keep
  working. To lift it early, add a DID override for `sign-in-account` in the
  console's Rate limits tab.
- **An OAuth client app gets 429 `rate_limit_exceeded`** from `/oauth/token`
  or `/oauth/par`: its backend shares one address for all its users
  (`oauth-ip`, 3000 per 5 min per IP). Add an IP override for that address.
- **Lost the inbox** (email factor): after verifying the user out of band,
  `com.atproto.admin.updateAccountEmail` to a new address drops the factor
  (any address change does, as in the reference); the user re-confirms and
  re-enables it.
- **Lost the authenticator** (TOTP): a recovery code works in place of a
  code. There is no admin reset of TOTP.
- App passwords bypass both factors (reference behaviour), so a user with
  one can still use apps while sorting out the factor.

---

## What NOT to do

- **Never run two processes with the same `--node-id`.** At startup a node fences
  the log of the previous incarnation of its id; the one already running then hits
  the fence on its next segment PUT and exits 3. With a supervisor restarting both,
  they keep fencing each other.
- **Never delete or edit objects by hand** under `log/`, `assign/`, `nodes/`,
  `writers/`, `retain/`, `state/` or `cluster/` (`cluster/version`):
  - `log/`: segments are the WAL; anything a shard hasn't checkpointed lives only
    there. A missing segment is a hole: readers stop there and replay treats a hole
    inside a span as an error.
  - Fence objects in `log/` are what makes a zombie of that incarnation fail-stop
    whatever its clock says. Retention keeps a retired dead log's fence for
    `--fence-retention` (default 7 days; `off` = forever) and then deletes it
    (`vlpds_retention_deleted_objects_total{log="fence"}`). A zombie paused
    longer than that (a suspended VM whose monotonic clock stopped) would find
    no fence when it wakes: don't pause VMs (below), and raise
    `--fence-retention` (or `off`) where a pause that long is possible.
  - `cluster/version` is the cluster's feature level: only `vlpds admin cluster
    finalize` / `cluster lower` change it.
  - `assign/` holds each shard's epoch and span history: what successors replay.
    `assign/layout` is the slot map.
  - `nodes/` and `writers/`: deleting a live lease or claim breaks liveness and
    seq uniqueness.
  - Retention deletes old log segments safely; let it.
- **Never run `--lease-ttl-ms` below 10 s in production** (the node warns). The
  renewal ceiling is 0.4 x TTL.
- **Don't SIGKILL for routine restarts.** Use SIGTERM and wait.
- **Don't suspend/snapshot-pause a running node's VM.** A paused monotonic clock
  makes the node think its lease is still valid on wake: it serves stale reads
  until its next PUT hits the fence (it still acks nothing). That relies on the
  fence still existing: one paused longer than `--fence-retention` (7 days by
  default) wakes after its fence was deleted.
- **Don't let host clocks drift.** Offsets don't affect safety but delay the
  merged firehose by the largest offset, and a new owner waits out the previous
  owner's `seq_floor` (up to 30 s) before serving.
- **Don't point two clusters at the same bucket + prefix**, and don't change
  `--shards` expecting it to reshard an existing prefix (it applies when a prefix
  is created; use split/merge).
- **Never retire an old KEK** (`--kek-old-file`, a KMS key version) before
  `vlpds.admin.rewrapSecrets` with `dryRun` reports `stale: 0` on every node, and
  never destroy KEK material that backups still need.
- **Don't shrink `--log-retention`** below what firehose consumers need for
  cursor resume; older cursors get `OutdatedCursor`.

---


## Metric gaps

Signals these alerts would want but that no metric exports today (not invented in
`alerts.yml`):

1. **Per-log firehose watermark lag** (merge input lag per source log) and clock
   offset between nodes; only visible in `getClusterStatus`.
2. **Cluster identity label** on metrics (all alerts assume one cluster per
   Prometheus).
3. **Hit/miss counters** for the in-memory caches other than the proxy fast path.
4. **The specific cause of an exit 5** in `vlpds_last_exit_reason_info`:
   `lease_lost` covers a CAS conflict, a lapse before renewal, the watchdog, a
   reassigned shard, a failed close and an unquiesced log (the log line before the
   exit tells them apart; `vlpds_lease_renew_errors_total{kind="conflict"|"lapsed"}`
   covers two of them, but dies with the process).
5. **Lease validity between scrapes.** `vlpds_lease_validity_seconds` is computed
   at scrape time; dips shorter than the scrape interval are only visible through
   the renewal histogram.

Closed (were gaps when the alerts were first written):

| Gap | Now |
|---|---|
| Lease renewal RTT and failures | `vlpds_lease_renew_seconds` (histogram, 1 ms .. 8 s), `vlpds_lease_renew_errors_total{kind=timeout\|error\|conflict\|lapsed}` |
| Lease validity remaining | `vlpds_lease_validity_seconds{node_id}` (at scrape; negative = lapsed) |
| Fail-stops unscrapeable | `vlpds_last_exit_reason_info{reason,code}` + `vlpds_last_exit_time_seconds` from the exit-state file on the next start; `vlpds_peer_takeovers_total{reason=peer\|restart}` on the fencer ([exit codes](#tools-endpoints-cli-logs-exit-codes)) |
| Process start time | `vlpds_process_start_time_seconds` and `process_start_time_seconds` |
| Replay activity | `vlpds_recovery_replayed_segments_total` (wired), `vlpds_recovery_replay_seconds`, `vlpds_shard_open_seconds{kind=replay\|clean}`, `vlpds_shards_opened_total{result}` |
| Shard count (was the hand-kept `vlpds:expected_shards`) | `vlpds_shard_layout_shards`; `vlpds:layout_shards` derives from it |
| Memory limit (was the hand-kept `vlpds:memory_limit_bytes`) | `vlpds_memory_limit_bytes` |
| Repo cache capacity | `vlpds_repo_cache_capacity_bytes` (all workers) |
| Object-store errors/latency on vlpds' own clients | `vlpds_object_store_requests_total{...,result=ok\|not_found\|precondition\|timeout\|error\|cancelled}`, `vlpds_object_store_request_seconds{op,component}` (every request through `objstats.rs`: control plane, segments, replay, backfill, retention, SlateDB) |
| Retention pass duration, dead-log holdings | `vlpds_retention_pass_seconds`, `vlpds_retention_dead_logs{state=unfenced\|needed\|pruning\|fenced}` (fenced = pruned to its fence, kept for `--fence-retention`), `vlpds_retention_dead_log_segments` |
| Lease configuration (alert thresholds were fixed for a 10 s TTL) | `vlpds_lease_ttl_seconds`, `vlpds_lease_renew_interval_seconds`, `vlpds_lease_skew_seconds`, `vlpds_lease_renew_ttl_ratio` (renewal round trip / TTL) |
| Firehose budgets (alerts hard-coded the defaults) | `vlpds_firehose_merge_queue_budget_bytes`, `vlpds_firehose_max_lag_bytes` |
