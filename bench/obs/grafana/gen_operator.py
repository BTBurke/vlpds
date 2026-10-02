"""The operator dashboard (uid `vlpds`): what someone running a PDS for their
users and community looks at, in their words. Built by gen_dashboard.py
(`just dashboards`) with its panel helpers; the engineer's view is the
internals dashboard (uid `vlpds-internals`).

Rules: plain titles and descriptions (no storage-engine or cluster-internals
words); mostly stats and simple time series; every row open; it reads the
same for a one-server PDS and a cluster. Queries are scoped by $cluster only
(the PDS as a whole). Counts "per hour" are trailing-hour counts
(increase over 1 h), so they read the same at any zoom.
"""

INTERNALS_UID = "vlpds-internals"

# Collections of vlpds_records_written_total, in the words of the Bluesky app.
COLLECTION_NAMES = {
    "app.bsky.feed.post": "Posts",
    "app.bsky.feed.like": "Likes",
    "app.bsky.feed.repost": "Reposts",
    "app.bsky.graph.follow": "Follows",
    "app.bsky.graph.block": "Blocks",
    "app.bsky.graph.list": "Lists",
    "app.bsky.graph.listitem": "List members",
    "app.bsky.graph.listblock": "List blocks",
    "app.bsky.graph.starterpack": "Starter packs",
    "app.bsky.graph.verification": "Verifications",
    "app.bsky.actor.profile": "Profile edits",
    "app.bsky.actor.status": "Status updates",
    "app.bsky.feed.threadgate": "Reply settings",
    "app.bsky.feed.postgate": "Quote settings",
    "app.bsky.feed.generator": "Custom feeds",
    "app.bsky.labeler.service": "Labeler",
    "chat.bsky.actor.declaration": "Chat settings",
    "other": "Other apps",
}

# ops/alerts.yml alert names in plain words (the alert list's first column).
ALERT_NAMES = {
    "VlpdsNodeDown": "A server is down",
    "VlpdsNotScraped": "Monitoring can't see any server",
    "VlpdsNodeRestarted": "A server restarted",
    "VlpdsNodeFailStopped": "A server stopped itself to protect data",
    "VlpdsNodeCrashLooping": "A server keeps restarting",
    "VlpdsUncleanNodeExit": "A server stopped uncleanly",
    "VlpdsPeerPresumedDead": "A server lost contact with another",
    "VlpdsMixedVersions": "Servers run different versions (stuck upgrade?)",
    "VlpdsFormatErrors": "Unreadable data found",
    "VlpdsIncompatibleNode": "A server's version is too old to run",
    "VlpdsFeatureLevelUnfinalized": "Upgrade not finished (finalize pending)",
    "VlpdsShardsUnowned": "Part of the data is not being served",
    "VlpdsShardsOverOwned": "Part of the data is claimed twice",
    "VlpdsOwnershipFlapping": "Data keeps moving between servers",
    "VlpdsOwnershipImbalanced": "Work is unevenly spread over servers",
    "VlpdsShardOpenErrors": "A server failed to load data",
    "VlpdsTakeoverReplaySlow": "Recovery after a server failure was slow",
    "VlpdsLeaseRenewalNearCeiling": "A server's heartbeat to storage is slow",
    "VlpdsLeaseRenewalAtCeiling": "A server's heartbeat to storage is too slow (it may stop)",
    "VlpdsLeaseRenewalSlow": "Heartbeats to storage are slow",
    "VlpdsLeaseRenewErrors": "Heartbeats to storage are failing",
    "VlpdsLeaseValidityLow": "A server nearly missed its heartbeat",
    "VlpdsCommitLatencyHigh": "Saving posts is slow",
    "VlpdsCommitLatencyCritical": "Saving posts is very slow (writes may fail)",
    "VlpdsCommitLogStalled": "Saving posts has stalled",
    "VlpdsWatermarkLagHigh": "A server's clock or storage is lagging",
    "VlpdsSegmentPutLatencyHigh": "Storage writes are slow",
    "VlpdsSegmentPutErrors": "Storage writes are failing (retrying)",
    "VlpdsWritesShed": "Overloaded: refusing some posts",
    "VlpdsPasswordHashingShed": "Overloaded: refusing some sign-ins",
    "VlpdsProxyAccountCapSustained": "One account is flooding the Bluesky app proxy",
    "VlpdsWriteInternalErrors": "Posts are failing with server errors",
    "VlpdsHttp5xxHigh": "Many requests are failing",
    "VlpdsForwardErrorsHigh": "Servers can't reach each other",
    "VlpdsForwardLatencyHigh": "Servers are slow to reach each other",
    "VlpdsWriteResendsSustained": "Posts are being retried a lot",
    "VlpdsFirehoseEmitDelayHigh": "Relays get new posts late",
    "VlpdsFirehoseEmitDelayCritical": "Relays get new posts very late",
    "VlpdsFirehoseStalled": "Relays stopped getting new posts",
    "VlpdsFirehoseConsumersTooSlow": "Relays are being dropped for falling behind",
    "VlpdsFirehoseMergeSpilling": "Relay feed is under memory pressure",
    "VlpdsPeerLogStreamLagging": "A server fell behind another",
    "VlpdsControlPlaneTimeouts": "Storage is timing out",
    "VlpdsObjectStoreBrownout": "Storage is failing for several servers (outage risk)",
    "VlpdsObjectStoreRequestErrors": "Storage requests are failing",
    "VlpdsObjectStorePermitsSaturated": "Storage requests are queueing",
    "VlpdsControlPlaneLatencyHigh": "Storage is slow",
    "VlpdsObjectStoreErrors": "Storage requests are failing",
    "VlpdsObjectStoreLatencyHigh": "Storage is slow",
    "VlpdsSlateDbL0Stalls": "Database writes are being held back",
    "VlpdsCheckpointsStalled": "Database housekeeping has stalled",
    "VlpdsReplayBacklogHigh": "Recovery after a crash would be slow",
    "VlpdsRetentionFailing": "Old data cleanup is failing",
    "VlpdsRetentionNotRunning": "Old data cleanup is not running",
    "VlpdsDeadLogUnfenced": "A stopped server's data is not cleaned up",
    "VlpdsReshardGcFailing": "Cleanup after data moves is failing",
    "VlpdsRetiredStateReferenced": "Leftover data needs a look",
    "VlpdsRetiredStateGrowing": "Leftover data is piling up",
    "VlpdsForcedDetachFailing": "Cleanup after data moves keeps failing",
    "VlpdsMemoryHigh": "A server is running low on memory",
    "VlpdsMemoryCritical": "A server is about to run out of memory",
    "VlpdsRepoCacheMissRateHigh": "Accounts load slowly (cache too small)",
    "VlpdsRepoLoadErrors": "Accounts are failing to load",
    "VlpdsKeyServiceUnavailable": "The key service is unreachable (posting fails)",
    "VlpdsSecretUnwrapRejected": "The key service rejected a key",
    "VlpdsPlcDirectoryUnavailable": "The PLC directory is unreachable (sign-ups fail)",
    "VlpdsPlcOpsRejected": "The PLC directory rejected changes",
    "VlpdsSignatureFault": "A signature check failed (suspect hardware)",
    "VlpdsSignatureFaultFailStop": "A server stopped on bad signatures (replace hardware)",
    "VlpdsLazyMstInvalid": "An account's data index was rebuilt",
    "VlpdsCacheAtCapacity": "A memory cache is full",
    "VlpdsFirehoseMergeQueueNearBudget": "Relay feed is near its memory limit",
    "VlpdsRuntimeStalls": "A server is stalling",
    "VlpdsRuntimeSaturated": "A server is out of CPU",
    "VlpdsPeerTlsCertExpiring": "Server certificates expire soon (renew them)",
    "VlpdsPeerTlsReloadFailing": "A server couldn't load its new certificate",
    "VlpdsPeerTlsHandshakeFailures": "Servers are refusing each other's certificates",
}

# Object-store ops by S3/R2 price class (bench/results/tiny-pds-idle-2026-10-02/analyze.py)
CLASS_A_OPS = "put|put_create|put_cas|list|copy|mpu_create|mpu_part|mpu_complete|delete_batch"
CLASS_B_OPS = "get|get_range|head"
MONTH_SECONDS = 730 * 3600


def build(g):
    """Builds the operator panels with gen_dashboard's helpers (module `g`);
    returns them."""
    D = g.Dash()
    g.D = D  # g's helpers place panels on its global D
    row, t, stat, ts, table = D.add_row, g.t, g.stat, g.ts, g.table
    C = g.C
    UP = f'up{{job="vlpds", {C}}}'
    # nodes that have reported in the last hour (idle bench ports don't count)
    KNOWN = f"on (instance) group by (instance) (last_over_time(vlpds_build_info{{{C}}}[1h]))"
    W = "[15m]"  # latency and success-rate windows: smooth at a small PDS's traffic

    def inc(metric, sel="", window="[1h]", by=None):
        s = f"{C}, {sel}" if sel else C
        b = f" by ({by})" if by else ""
        return f"sum{b} (increase({metric}{{{s}}}{window}))"

    def day(metric, sel=""):
        # whole events (increase() extrapolates to fractions)
        return "round(" + inc(metric, sel, "[1d]") + ") or vector(0)"

    def link_internals(title="Open the internals dashboard"):
        D.panels[-1]["links"] = [{"title": title, "url": f"/d/{INTERNALS_UID}?${{__url_time_range}}&${{cluster:queryparam}}"}]

    def names(mapping):
        return [{"matcher": {"id": "byName", "options": k}, "properties": [{"id": "displayName", "value": v}]}
                for k, v in mapping.items()]

    def color(name, c):
        return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}

    def unit(name, u):
        return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": u}]}

    def stat_colors(name, steps, base="green"):
        return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "thresholds", "value": g.thresholds(steps, base)}]}

    all_req = f"sum(rate(vlpds_http_requests_total{{{C}}}[5m]))"
    err_req = f'(sum(rate(vlpds_http_requests_total{{{C}, status=~"5.."}}[5m])) or vector(0))'
    unowned = f"abs(max(vlpds_shard_layout_shards{{{C}}}) - (sum(vlpds_owned_partitions{{{C}}}) or vector(0)))"
    paging = f'count(ALERTS{{alertstate="firing", alertname=~"Vlpds.*", severity="page", {C}}})'
    servers_down = f"count(({UP} == 0) and {KNOWN})"

    # ================================================================ now
    row("Is my PDS up and serving people well?", collapsed=False)
    stat("PDS status",
         f"(clamp_max(count({UP} == 1), 1) or vector(0)) * (2 - clamp_max("
         f"({servers_down} or vector(0)) + ({err_req} / {all_req} > bool 0.05 or vector(0)) "
         f"+ ({paging} or vector(0)) + ({unowned} or vector(0)), 1))",
         w=4, h=5, steps=[(1, "orange"), (2, "green")], spark=False, no_value="no data",
         mappings=[{"type": "value", "options": {"0": {"text": "Down", "color": "red"},
                                                 "1": {"text": "Degraded", "color": "orange"},
                                                 "2": {"text": "Working normally", "color": "green"}}}],
         desc="Down: no server is answering. Degraded: a server is down, over 5% of requests are failing, "
              "an urgent alert is firing, or part of the data has no server. Otherwise working normally.")
    D.panels[-1]["fieldConfig"]["defaults"]["thresholds"] = g.thresholds([(1, "orange"), (2, "green")], base="red")
    stat("Served without error (24 h)",
         f'1 - ((sum(increase(vlpds_http_requests_total{{{C}, status=~"5.."}}[1d])) or vector(0)) '
         f"/ sum(increase(vlpds_http_requests_total{{{C}}}[1d])))",
         "percentunit", w=4, h=5, decimals=2, spark=False, no_value="no traffic",
         desc="Share of all requests over the last day that did not fail on the server side (5xx): "
              "what your users experienced, Bluesky app requests passed through included.")
    D.panels[-1]["fieldConfig"]["defaults"]["thresholds"] = g.thresholds([(0.99, "orange"), (0.999, "green")], base="red")
    D.panels[-1]["options"]["colorMode"] = "value"
    stat("Failing requests right now", f"clamp_max({err_req} / {all_req}, 1)", "percentunit",
         [(0.01, "orange"), (0.05, "red")], w=4, h=5, decimals=1, no_value="no traffic",
         desc="Share of requests in the last 5 minutes that failed on the server side. "
              "Red at 5%: the 'Many requests are failing' alert.")
    stat("Bluesky app working",
         f'sum(rate(vlpds_upstream_requests_total{{{C}, service="appview", result=~"ok|client_error"}}{W})) '
         f'/ sum(rate(vlpds_upstream_requests_total{{{C}, service="appview"}}{W}))',
         "percentunit", w=4, h=5, decimals=1, no_value="no app traffic",
         desc="Share of your users' Bluesky app requests (timelines, threads, notifications, profiles) that the "
              "Bluesky AppView answered, over 15 minutes. Errors here are the AppView's or the network's, "
              "not your server's, but your users see them.")
    D.panels[-1]["fieldConfig"]["defaults"]["thresholds"] = g.thresholds([(0.9, "orange"), (0.99, "green")], base="red")
    D.panels[-1]["options"]["colorMode"] = "background"
    stat("Servers online", [t(f"count({UP} == 1) or vector(0)", "online"), t(f"{servers_down} or vector(0)", "offline")],
         w=4, h=5, text_mode="value_and_name", spark=False,
         overrides=[stat_colors("online", [], "green"), stat_colors("offline", [(1, "red")])],
         desc="vlpds servers answering monitoring, and those that answered within the last hour but don't now.")
    stat("Cluster health", f"clamp_max(({unowned} or vector(0)) + ({paging} or vector(0)), 1)", w=4, h=5, spark=False,
         no_value="no data",
         mappings=[{"type": "value", "options": {"0": {"text": "Healthy", "color": "green"},
                                                 "1": {"text": "Needs attention", "color": "red"}}}],
         steps=[(1, "red")],
         desc="Behind the scenes: green when all of your users' data is being served by a running server and no "
              "urgent alert fires. Red: open the internals dashboard (link in this panel's menu) and the runbook.")
    link_internals()
    D.newline()
    ts("Requests per minute, by outcome",
       [t(f'sum(rate(vlpds_http_requests_total{{{C}, status=~"[23].."}}[5m])) * 60', "served"),
        t(f'sum(rate(vlpds_http_requests_total{{{C}, status=~"4..", status!="429"}}[5m])) * 60', "refused (bad request, not signed in, not found)"),
        t(f'sum(rate(vlpds_http_requests_total{{{C}, status="429"}}[5m])) * 60', "blocked (too many requests)"),
        t(f'sum(rate(vlpds_http_requests_total{{{C}, status=~"5.."}}[5m])) * 60', "failed (server error)")],
       "short", stack=True, w=12,
       overrides=[color("served", "green"), color("refused (bad request, not signed in, not found)", "yellow"),
                  color("blocked (too many requests)", "orange"), color("failed (server error)", "red")],
       desc="Everything your PDS answered: apps, relays, the Bluesky app's requests passed through. "
            "Some refused requests are normal (expired sign-ins, missing records); failures should be near zero.")
    WRITE = g.WRITE_METHODS
    ts("How long common actions take (95% finish within)",
       [t(g.hq(0.95, "vlpds_http_request_duration_seconds", f'{C}, method=~"{WRITE}"', window=W), "posting / liking / following"),
        t(g.hq(0.95, "vlpds_http_request_duration_seconds", f'{C}, method=~"{g.READ_METHODS}"', window=W), "reading accounts' data"),
        t(g.hq(0.95, "vlpds_http_request_duration_seconds", f'{C}, method="com.atproto.server.createSession"', window=W), "signing in"),
        t(g.hq(0.95, "vlpds_http_request_duration_seconds", f'{C}, method="com.atproto.repo.uploadBlob"', window=W), "uploading images / video"),
        t(g.hq(0.95, "vlpds_upstream_request_seconds", f'{C}, service="appview"', window=W), "Bluesky app (via the AppView)")],
       "s", w=12, lines=[(1, "orange")],
       desc="95th percentile over 15 minutes: 95 of 100 requests were at least this fast. Posting includes saving "
            "durably; sign-in includes the deliberately slow password check; the Bluesky app line is the AppView's "
            "time to answer. Dashed at 1 s, where people start to notice.")

    # ================================================================ users
    row("Users and accounts", collapsed=False)
    stat("Accounts", [t(f'sum(vlpds_accounts{{{C}, status="active"}})', "active"),
                      t(f'sum(vlpds_accounts{{{C}, status="deactivated"}})', "deactivated"),
                      t(f'sum(vlpds_accounts{{{C}, status=~"takendown|suspended"}})', "taken down / suspended")],
         w=8, h=4, text_mode="value_and_name", spark=False, no_value="counting...",
         desc="Accounts hosted here by status, counted every 15 minutes (--account-stats-interval-secs) by each server for its share; while a server is down its share is missing until the next count. "
              "Deactivated accounts were paused by their owner; taken down / suspended ones by a moderator.")
    stat("Accounts that posted or changed something", [t(f'sum(vlpds_repos_written_within{{{C}, window="1d"}})', "last day"),
                                                       t(f'sum(vlpds_repos_written_within{{{C}, window="7d"}})', "last week"),
                                                       t(f'sum(vlpds_repos_written_within{{{C}, window="30d"}})', "last month")],
         w=8, h=4, text_mode="value_and_name", spark=False, no_value="counting...",
         desc="Accounts with any new post, like, follow, profile change etc. in the window (your active users), "
              "counted every 15 minutes. People who only read don't show here; see Sign-ins.")
    stat("Sign-ups (24 h)", [t(day("vlpds_signups_total", 'result="created"'), "new accounts"),
                             t(day("vlpds_signups_total", 'result!~"created|error"'), "refused")],
         w=4, h=4, text_mode="value_and_name", spark=False,
         overrides=[stat_colors("refused", [], "text")],
         desc="Accounts created in the last day, and sign-up attempts refused (bad invite code, blocked email "
              "domain, reserved handle, handle taken...). Breakdown: Sign-ups and account changes.")
    stat("Sign-ins (24 h)", [t(day("vlpds_logins_total", 'result="success"'), "succeeded"),
                             t(day("vlpds_logins_total", 'result=~"failed|second_factor_failed"'), "failed")],
         w=4, h=4, text_mode="value_and_name", spark=False,
         overrides=[stat_colors("failed", [], "text")],
         desc="Successful sign-ins (password, app password or the sign-in page apps open) in the last day, "
              "and failed ones (wrong password or wrong 2FA code). Apps stay signed in for weeks, so this counts "
              "new sign-ins, not everyone using the PDS.")
    ts("Sign-ups and account changes per hour",
       [t(inc("vlpds_signups_total", 'result="created"'), "new accounts"),
        t(f'sum by (result) (increase(vlpds_signups_total{{{C}, result!~"created"}}[1h])) > 0', "sign-up refused: {{result}}"),
        t(f"sum by (event) (increase(vlpds_account_events_total{{{C}, event!=\"created\"}}[1h])) > 0", "account {{event}}"),
        t(f"sum by (step) (increase(vlpds_password_resets_total{{{C}}}[1h])) > 0", "password reset {{step}}"),
        t(f"sum by (event) (increase(vlpds_invite_codes_total{{{C}}}[1h])) > 0", "invite code {{event}}")],
       "short", w=12,
       overrides=[color("new accounts", "green")],
       desc="Each point counts the hour before it. Refusal reasons: invite (missing or used-up invite code), "
            "email_policy (disposable or unsupported email), handle_policy (reserved or offensive handle), "
            "taken (handle or email in use), invalid (other bad input), error (server problem). "
            "Account changes: deactivated / reactivated by the owner, deleted.")
    ts("Sign-ins per hour",
       [t(f"sum by (result) (increase(vlpds_logins_total{{{C}}}[1h])) > 0", "{{result}}")],
       "short", w=12, empty="no sign-ins",
       overrides=names({"success": "succeeded", "failed": "wrong password", "second_factor_required": "2FA code asked for",
                        "second_factor_failed": "wrong 2FA code", "blocked": "account taken down / inactive",
                        "rate_limited": "too many attempts (blocked)", "error": "server error"})
       + [color("succeeded", "green"), color("wrong password", "yellow"), color("wrong 2FA code", "orange"),
          color("too many attempts (blocked)", "purple"), color("server error", "red")],
       desc="Password, app-password and sign-in-page (OAuth) sign-ins, each point counting the hour before it. "
            "Lots of wrong passwords from one place get blocked by rate limits (purple).")

    # ================================================================ content
    row("Content", collapsed=False)
    stat("Created in the last 24 h", [t(day("vlpds_records_written_total", f'action="create", collection="{c}"'), n)
                                     for c, n in [("app.bsky.feed.post", "posts"), ("app.bsky.feed.like", "likes"),
                                                  ("app.bsky.feed.repost", "reposts"), ("app.bsky.graph.follow", "follows")]]
         + [t(day("vlpds_blob_uploads_total", 'kind="image"'), "images"),
            t(day("vlpds_blob_uploads_total", 'kind="video"'), "videos"),
            t(day("vlpds_blob_upload_bytes_total"), "uploaded")],
         w=24, h=4, text_mode="value_and_name", spark=False, overrides=[unit("uploaded", "bytes")],
         desc="New posts, likes, reposts and follows your users made in the last day (deletions not subtracted), "
              "images and videos they uploaded, and the uploads' total size.")
    ts("Created per hour, by type",
       [t(f'sum by (collection) (increase(vlpds_records_written_total{{{C}, action="create"}}[1h])) > 0', "{{collection}}"),
        t(f'sum(increase(vlpds_records_written_total{{{C}, action="delete"}}[1h])) > 0', "deleted (any type)")],
       "short", w=12, empty="nothing written yet",
       overrides=names(COLLECTION_NAMES) + [color("Posts", "blue"), color("Likes", "red"), color("Profile edits", "purple"),
                                            color("Follows", "green"), color("deleted (any type)", "text")],
       desc="Posts, likes, follows and everything else your users created, by type, each point counting the hour "
            "before it. 'Other apps' are records of non-Bluesky apps (e.g. other atproto apps your users use).")
    ts("Media uploads per hour",
       [t(f"sum by (kind) (increase(vlpds_blob_uploads_total{{{C}}}[1h])) > 0", "{{kind}}"),
        t(f"sum(increase(vlpds_blob_upload_bytes_total{{{C}}}[1h]))", "size")],
       "short", w=12, empty="no uploads",
       overrides=names({"image": "images", "video": "videos", "other": "other files"}) + [g.right_axis("size", "bytes"), g.dashed("size", "text")],
       desc="Uploads per hour by kind (left) and their total size (right, dashed).")

    # ================================================================ network
    row("Federation: relays, directory, identity", collapsed=False)
    stat("Relays connected", f"sum(vlpds_firehose_subscribers{{{C}}}) or (0 * count({UP} == 1))", w=5, h=4,
         steps=[(1, "green")], spark=True, no_value="no data",
         desc="Relays and other services following your PDS's live update stream (the 'firehose'). "
              "0 means the network isn't hearing about new posts: check requestCrawl below.")
    D.panels[-1]["fieldConfig"]["defaults"]["thresholds"] = g.thresholds([(1, "green")], base="orange")
    stat("Relays dropped for falling behind (24 h)", day("vlpds_firehose_disconnects_total", 'reason="too_slow"'),
         w=5, h=4, steps=[(1, "orange")], spark=False,
         desc="Relays cut off because they read updates too slowly. They reconnect and catch up; "
              "repeated drops mean that relay (or its network) can't keep up.")
    stat("Delay before relays see new posts", g.hq(0.99, "vlpds_firehose_emit_delay_seconds", C, window=W) + " >= 0", "s",
         [(2, "orange"), (20, "red")], w=5, h=4, no_value="no posts",
         desc="99% of new posts, likes, etc. reached the relay stream within this time after being saved (15 min).")
    stat("Last crawl request accepted", f"time() - max(vlpds_request_crawl_last_success_time_seconds{{{C}}} > 0)", "dtdurations",
         w=5, h=4, spark=False, no_value="none since restart",
         desc="How long ago a relay accepted your PDS's requestCrawl ('please follow me'), sent at startup to "
              "--crawlers and by 'vlpds admin request-crawl'. Failed attempts are in Directory and identity.")
    stat("PLC directory errors (24 h)", day("vlpds_plc_requests_total", 'result=~"rejected|unavailable"'),
         w=4, h=4, steps=[(1, "orange")], spark=False,
         desc="Failed calls to the PLC directory (plc.directory), which publishes your users' identities. "
              "While it fails, sign-ups and handle changes fail.")
    ts("Directory and identity, per hour",
       [t(f"sum by (kind) (increase(vlpds_identity_events_total{{{C}}}[1h])) > 0", "{{kind}} updates announced"),
        t(f"sum by (result) (increase(vlpds_plc_requests_total{{{C}}}[1h])) > 0", "PLC directory: {{result}}"),
        t(f"sum by (result) (increase(vlpds_handle_resolutions_total{{{C}}}[1h])) > 0", "custom handle lookup: {{result}}"),
        t(f"sum by (result) (increase(vlpds_request_crawl_total{{{C}}}[1h])) > 0", "crawl request: {{result}}")],
       "short", w=24, h=7, empty="nothing yet",
       desc="Identity updates announced to the network (identity: new account or handle change; account: status "
            "change), PLC directory calls by result (rejected / unavailable are failures), custom-domain handle lookups, "
            "and requestCrawl calls to relays (rejected / failed are failures).")

    # ================================================================ safety
    row("Moderation and safety", collapsed=False)
    stat("Reports and takedowns (24 h)", [t(day("vlpds_reports_total", 'result="ok"'), "reports"),
                                          t(day("vlpds_reports_total", 'result="failed"'), "reports failed"),
                                          t(day("vlpds_moderation_actions_total", 'action="takedown", subject="account"'), "accounts taken down"),
                                          t(day("vlpds_moderation_actions_total", 'action="takedown", subject=~"record|blob"'), "posts / media taken down"),
                                          t(day("vlpds_moderation_actions_total", 'action="reversed"'), "reversed")],
         w=12, h=4, text_mode="value_and_name", spark=False, overrides=[stat_colors("reports failed", [(1, "orange")])],
         desc="Reports your users filed (on posts, accounts...) and passed to the moderation service (failed: the "
              "service refused or couldn't be reached), and takedowns applied by you or your moderation service "
              "(com.atproto.admin.updateSubjectStatus), and reversals.")
    stat("Abusive traffic blocked (24 h)",
         f"({day('vlpds_rate_limited_total')}) + ({day('vlpds_firehose_rejected_total')}) + ({day('vlpds_proxy_rejected_total')})",
         w=6, h=4, spark=False,
         desc="Requests refused for coming too fast (rate limits), plus connections refused from one address opening "
              "too many, plus one account flooding the Bluesky app proxy.")
    stat("Emails (24 h)", [t(day("vlpds_mail_messages_total", 'result="sent"'), "sent"),
                           t(day("vlpds_mail_messages_total", 'result=~"failed|dropped"'), "failed")],
         w=6, h=4, text_mode="value_and_name", spark=False, overrides=[stat_colors("failed", [(1, "red")])],
         desc="Emails sent (address confirmation, password reset, sign-in codes, account deletion...) and ones "
              "that could not be delivered. Failures mean users can't confirm, reset or sign in with 2FA codes.")
    ts("Blocked and refused, per hour",
       [t(inc("vlpds_rate_limited_total") + " > 0", "too many requests"),
        t(f"sum(increase(vlpds_firehose_rejected_total{{{C}}}[1h])) > 0", "too many relay connections from one address"),
        t(f"sum(increase(vlpds_proxy_rejected_total{{{C}}}[1h])) > 0", "one account flooding the app proxy"),
        t(f'sum by (result) (increase(vlpds_signups_total{{{C}, result=~"invite|email_policy|handle_policy"}}[1h])) > 0', "sign-up blocked: {{result}}"),
        t(f'sum(increase(vlpds_logins_total{{{C}, result="rate_limited"}}[1h])) > 0', "sign-in attempts blocked")],
       "short", w=12, empty="nothing blocked",
       desc="Abusive or excessive traffic your PDS turned away, each point counting the hour before it. "
            "Sign-ups blocked by policy: invite (no or bad invite code), email_policy (disposable email), "
            "handle_policy (reserved or offensive handle).")
    ts("Emails per hour",
       [t(f'sum by (purpose) (increase(vlpds_mail_messages_total{{{C}, result="sent"}}[1h])) > 0', "{{purpose}}"),
        t(f'sum(increase(vlpds_mail_messages_total{{{C}, result=~"failed|dropped"}}[1h])) > 0', "not delivered")],
       "short", w=12, empty="no email sent",
       overrides=names({"reset_password": "password reset", "delete_account": "account deletion", "confirm_email": "email confirmation",
                        "update_email": "email change", "plc_operation": "identity change", "auth_factor": "sign-in code (2FA)"})
       + [color("not delivered", "red")],
       desc="Emails sent by kind, and ones that failed or were dropped (red), each point counting the hour before it.")

    # ================================================================ resources
    row("Resources and cost", collapsed=False)
    gauge_steps = [(0.7, "orange"), (0.9, "red")]
    stat("Busiest server: CPU and memory in use",
         [t(f"max(sum by (instance) (rate(vlpds_process_cpu_seconds_total{{{C}}}[5m])) / on (instance) max by (instance) (vlpds_cpu_cores{{{C}}}))", "CPU"),
          t(f"max(max by (instance) (vlpds_process_resident_bytes{{{C}}}) / on (instance) max by (instance) (vlpds_memory_limit_bytes{{{C}}}))", "memory")],
         "percentunit", gauge_steps, w=8, h=4, decimals=0, no_value="no data", text_mode="value_and_name",
         overrides=[stat_colors("memory", [(0.85, "orange"), (0.95, "red")])],
         desc="On the busiest server: CPU used out of the cores available to vlpds (5 min; sustained over 70% leaves "
              "little headroom for spikes), and memory used out of what the server or its container allows (85% and "
              "95% are the memory alerts; at 100% the server is killed and restarts).")
    stat("Disk cache", [t(f'sum(vlpds_disk_cache_bytes{{{C}, kind="used"}})', "used"),
                        t(f'sum(vlpds_disk_cache_bytes{{{C}, kind="used"}}) / sum(vlpds_disk_cache_bytes{{{C}, kind="capacity"}})', "of its limit")],
         "bytes", w=8, h=4, decimals=1, no_value="not configured", spark=False, text_mode="value_and_name",
         overrides=[unit("of its limit", "percentunit")],
         desc="Local disk cache (--cache-dir) used out of its size limit, updated every 15 minutes. It saves storage "
              "requests; full is normal (oldest entries make room).")
    stat("Estimated storage request bill",
         f'(sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_A_OPS}"}}[1h])) * {MONTH_SECONDS} / 1e6 * $class_a_price) '
         f'+ (sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_B_OPS}"}}[1h])) * {MONTH_SECONDS} / 1e6 * $class_b_price)',
         "currencyUSD", w=8, h=4, decimals=2, no_value="no data",
         desc="ESTIMATE: object-storage request charges for a month at the last hour's request rate, at the Class A "
              "(writes, lists) and Class B (reads) prices picked at the top (S3 or R2 list prices; R2's free tier "
              "and S3's free deletes are not subtracted, so the real bill can be lower). Storage space and data "
              "transfer are not included. A one-account PDS is about $2-5/month on S3 and $0 on R2.")
    ts("CPU and memory by server",
       [t(g.by_node(f"sum by (instance) (rate(vlpds_process_cpu_seconds_total{{{C}}}[5m])) / on (instance) max by (instance) (vlpds_cpu_cores{{{C}}})").replace(g.I, C), "CPU {{node_id}}"),
        t(g.by_node(f"max by (instance) (vlpds_process_resident_bytes{{{C}}}) / on (instance) max by (instance) (vlpds_memory_limit_bytes{{{C}}})").replace(g.I, C), "memory {{node_id}}")],
       "percentunit", w=12, max_=1, lines=[(0.85, "orange")],
       desc="Per server: CPU used out of its available cores, and memory used out of its limit.")
    ts("Storage requests and estimated monthly cost",
       [t(f'sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_A_OPS}"}}[15m]))', "writes and lists (Class A) /s"),
        t(f'sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_B_OPS}"}}[15m]))', "reads (Class B) /s"),
        t(f'(sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_A_OPS}"}}[1h])) * {MONTH_SECONDS} / 1e6 * $class_a_price) '
          f'+ (sum(rate(vlpds_object_store_requests_total{{{C}, op=~"{CLASS_B_OPS}"}}[1h])) * {MONTH_SECONDS} / 1e6 * $class_b_price)',
          "estimated $ per month (right)")],
       "reqps", w=12, overrides=[g.right_axis("estimated $ per month (right)", "currencyUSD"), g.dashed("estimated $ per month (right)", "orange")],
       desc="Object-store requests per second by price class (left) and what the last hour's rate would cost "
            "over a month (right, dashed; an estimate, see the bill stat).")

    # ================================================================ alerts
    row("Alerts", collapsed=False)
    table("Alerts firing now",
          [g.instant_table(f'max by (alertname, severity, instance) (ALERTS{{alertstate="firing", alertname=~"Vlpds.*", {C}}})')],
          w=16, h=6,
          desc="Alerts from the PDS's alert rules (ops/alerts.yml), in plain words. 'urgent' (page) means users are "
               "or soon will be affected; 'soon' (ticket) means look at it today. Empty = nothing firing, or the alert "
               "rules aren't loaded in this Prometheus. Each alert's fix is in the runbook.",
          transformations=[{"id": "organize", "options": {
              "excludeByName": {"Time": True, "Value": True},
              "indexByName": {"alertname": 0, "severity": 1, "instance": 2},
              "renameByName": {"alertname": "what is wrong", "severity": "how urgent", "instance": "server"}}}],
          overrides=[{"matcher": {"id": "byName", "options": "what is wrong"}, "properties": [
                          {"id": "mappings", "value": [{"type": "value", "options": {k: {"text": v} for k, v in ALERT_NAMES.items()}}]},
                          {"id": "links", "value": [{"title": "Runbook for this alert", "targetBlank": True,
                                                     "url": f"{g.RB}#${{__data.fields.alertname:raw}}"}]}]},
                     {"matcher": {"id": "byName", "options": "how urgent"}, "properties": [
                          {"id": "mappings", "value": [{"type": "value", "options": {"page": {"text": "urgent", "color": "red"},
                                                                                     "ticket": {"text": "soon", "color": "orange"}}}]},
                          {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}])
    D.panels[-1]["fieldConfig"]["defaults"]["noValue"] = "Nothing firing"
    D.add({"type": "text", "id": D.nid(), "title": "Digging deeper", "gridPos": D.place(8, 6),
           "options": {"mode": "markdown", "content":
                       f"[Internals dashboard](/d/{INTERNALS_UID}) (for engineers) · [Runbook]({g.RB}) (what each alert means "
                       f"and what to do) · [Alert rules]({g.ALERTS_URL})"}})
    # several figures side by side rather than stacked in small type
    for p in D.panels:
        if p["type"] == "stat" and len(p["targets"]) >= 3:
            p["options"]["orientation"] = "vertical"
    return D.panels


def template(g, panels):
    return {
        "uid": "vlpds",
        "title": "vlpds",
        "description": "vlpds PDS for its operator: is it up, who uses it, what they post, how it federates, "
                       "moderation, email, resources and cost. Engineers: the 'vlpds internals' dashboard. Generated by "
                       "packages/vlpds/bench/obs/grafana/gen_dashboard.py; edits in the UI are overwritten.",
        "tags": ["vlpds"],
        "timezone": "browser",
        "editable": True,
        "graphTooltip": 1,
        "refresh": "1m",
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m", "15m"]},
        "schemaVersion": 39,
        "version": 1,
        "links": [
            {"title": "Internals dashboard", "type": "link", "url": f"/d/{INTERNALS_UID}", "icon": "dashboard",
             "keepTime": True, "includeVars": True, "tooltip": "The engineer's view: every subsystem, for debugging"},
            {"title": "Runbook", "type": "link", "url": g.RB, "icon": "doc", "targetBlank": True, "tooltip": "What each alert means and what to do"},
            {"title": "Alert rules", "type": "link", "url": g.ALERTS_URL, "icon": "bolt", "targetBlank": True, "tooltip": "ops/alerts.yml"},
        ],
        "annotations": {"list": [
            {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True,
             "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
            {"datasource": g.PLACEHOLDER, "enable": True, "iconColor": "purple", "name": "Server restarts",
             "expr": f"max by (instance) (changes(vlpds_process_start_time_seconds{{{g.C}}}[2m])) > 0",
             "step": "1m", "titleFormat": "server restarted", "textFormat": "{{instance}}", "tagKeys": "instance", "useValueForTime": False},
        ]},
        "templating": {"list": [
            g.var("cluster", "PDS", "label_values(vlpds_build_info, cluster)"),
            price_var("class_a_price", "Write/list price ($ per million)", [("S3 $5.00", "5"), ("R2 $4.50", "4.5")]),
            price_var("class_b_price", "Read price ($ per million)", [("S3 $0.40", "0.4"), ("R2 $0.36", "0.36")]),
        ]},
        "panels": panels,
    }


def price_var(name, label, options):
    """Custom variable: (text, value) options, the first selected."""
    return {"name": name, "label": label, "type": "custom", "hide": 0, "multi": False, "includeAll": False,
            "query": ",".join(f"{txt} : {v}" for txt, v in options),
            "current": {"selected": True, "text": options[0][0], "value": options[0][1]},
            "options": [{"selected": i == 0, "text": txt, "value": v} for i, (txt, v) in enumerate(options)]}
