#!/usr/bin/env python3
"""1 s sampler of vlpds shard-move metrics (labels kept) per node.
Round 3: round 2's object-store/lease families plus shard warm, security-ctl
loads, meta cache, repo loads/preloads, forwards and write errors.
usage: objsample.py OUT.jsonl name=url [name=url ...]   (runs until killed)"""
import json, sys, time, urllib.request

KEEP = ("vlpds_object_store_", "vlpds_owned_partitions", "vlpds_lease_validity_seconds", "vlpds_lease_renew_errors_total",
        "vlpds_lease_events_total", "vlpds_peer_takeovers_total", "vlpds_shards_opened_total", "vlpds_shard_open_seconds",
        "vlpds_shard_warm_", "vlpds_security_ctl_loads_total", "vlpds_meta_cache_", "vlpds_sst_meta_bytes",
        "vlpds_repo_loads_total", "vlpds_repo_preloads_total", "vlpds_repo_load_seconds", "vlpds_forwards_total",
        "vlpds_write_errors_total", "vlpds_commits_total", "vlpds_cache_bytes", "vlpds_repos_loading")
out = open(sys.argv[1], "a")
nodes = [a.split("=", 1) for a in sys.argv[2:]]
while True:
    t0 = time.time()
    for name, url in nodes:
        rec = {"t": round(t0, 3), "node": name, "m": {}}
        try:
            raw = urllib.request.urlopen(url + "/metrics", timeout=1.5).read().decode(errors="replace")
            for line in raw.splitlines():
                if line.startswith(KEEP) and "_bucket{" not in line:
                    k, _, v = line.rpartition(" ")
                    try:
                        rec["m"][k] = float(v)
                    except ValueError:
                        pass
            rec["up"] = True
        except Exception as e:
            rec["up"] = False
            rec["err"] = str(e)[:80]
        out.write(json.dumps(rec) + "\n")
    out.flush()
    time.sleep(max(0.05, 1 - (time.time() - t0)))
