#!/usr/bin/env python3
"""Shard-sweep tables from shardsweep.jsonl (shardsweep.py sweep), next to the
cost model's prediction for the same phase (../cost-model-2026-10-02:
fit on its own runs, DEFAULT_KNOBS with the 30 s lease TTL these nodes ran).

    sweep_report.py [shardsweep.jsonl] [--json out.json]

Per (shard count, phase): commits/s, loads/s, process CPU per commit (all
nodes), loadgen p50/p99, SlateDB L0 SSTs per shard (SlateDB's per-node gauge is the total over the
node's shard DBs: summed over nodes, averaged over the phase's scrapes, ÷
shards; in parentheses the highest all-node total seen), L0 write stalls,
object-store requests/s by class (segment PUT, SST PUT, manifest+compactions
CAS, polling GETs, SST GETs, LIST, control plane), Class A/B totals, the
model's Class A/B, and S3 request $/month at the measured rates.
"""
import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "cost-model-2026-10-02"))
import analyze  # noqa: E402
import cost_model as cm  # noqa: E402

MONTH = cm.MONTH_S


def classes(req):
    p = lambda pred: cm.pick(req, pred)
    return {
        "seg_put": p(lambda o, k: k == "log_segment" and o.startswith("put")),
        "sst_put": p(lambda o, k: k in ("state_sst", "state_wal") and o.startswith("put")),
        "cas": p(lambda o, k: k in ("state_manifest", "state_compactions", "state_gc_boundary") and o.startswith("put")),
        "poll_get": p(lambda o, k: k in ("state_manifest", "state_compactions", "state_gc_boundary") and o in ("get", "head")),
        "sst_get": p(lambda o, k: k in ("state_sst", "state_wal") and o in ("get", "get_range", "head")),
        "list": p(lambda o, k: o == "list"),
        "ctl": p(lambda o, k: k.startswith("ctl_") and o != "list"),
    }


def extra(path):
    """Per (prefix, phase): CPU, L0, poll switches, loadgen result."""
    recs = [json.loads(l) for l in open(path)]
    by = defaultdict(list)
    for r in recs:
        if r.get("nodes") and all(r["nodes"].values()):
            by[(r["prefix"], r["phase"].split(":")[0])].append(r)
    out = {}
    for key, rs in by.items():
        rs.sort(key=lambda r: r["t"])
        a, b = rs[0], rs[-1]
        s = lambda r, n: sum(m["sums"].get(n, 0.0) for m in r["nodes"].values())
        cpu = s(b, "vlpds_process_cpu_seconds_total") - s(a, "vlpds_process_cpu_seconds_total")
        commits = s(b, "vlpds_commits_total") - s(a, "vlpds_commits_total")
        dt = max(1e-9, b["t"] - a["t"])
        l0 = [s(r, "slatedb_db_l0_sst_count") for r in rs]
        stalls = s(b, "slatedb_db_l0_stall_count_total") - s(a, "slatedb_db_l0_stall_count_total")
        fast = sum(v for m in b["nodes"].values() for k, v in m["labeled"].items() if k.startswith("vlpds_compaction_poll_switches_total") and 'mode="fast"' in k) - \
            sum(v for m in a["nodes"].values() for k, v in m["labeled"].items() if k.startswith("vlpds_compaction_poll_switches_total") and 'mode="fast"' in k)
        lg = next((r.get("loadgen") for r in reversed(rs) if isinstance(r.get("loadgen"), dict)), None)
        out[key] = {"cpu_cores": cpu / dt, "cpu_us_per_commit": cpu * 1e6 / commits if commits > 0 else None,
                    "l0_sum_avg": sum(l0) / len(l0), "l0_max": max(l0), "l0_stalls": stalls,
                    "fast_switches_per_min": fast * 60 / dt, "loadgen": lg,
                    "rss_gb": [round(m["sums"].get("vlpds_process_resident_bytes", 0) / 1e9, 2) for m in b["nodes"].values()]}
    return out


def main():
    args = sys.argv[1:]
    path = args[0] if args and not args[0].startswith("--") else os.path.join(HERE, "shardsweep.jsonl")
    c = cm.fit(cm.phase_list())
    ps = analyze.phases(path)
    ex = extra(path)
    rows = []
    for p in ps:
        if p["phase"].endswith(("settle",)):
            continue
        a, b, _, _ = cm.split(p["req_s"])
        req = cm.requests(c, p["nodes"], p["shards"], p["commits_s"], p["repo_loads_s"], dict(lease_ttl_s=30.0))
        ma, mb = sum(v[0] for v in req.values()), sum(v[1] for v in req.values())
        e = ex.get((p["prefix"], p["phase"]), {})
        lg = e.get("loadgen") or {}
        al = lg.get("all") or {}
        rows.append({"shards": p["shards"], "phase": p["phase"].split("/")[-1], "secs": p["secs"], "commits_s": p["commits_s"],
                     "loads_s": p["repo_loads_s"], "segments_s": p["segments_s"], **classes(p["req_s"]),
                     "A": a, "B": b, "model_A": ma, "model_B": mb,
                     "s3_req_usd_mo": (a * 0.005 + b * 0.0004) / 1000 * MONTH,
                     "model_s3_req_usd_mo": (ma * 0.005 + mb * 0.0004) / 1000 * MONTH,
                     "cpu_cores": e.get("cpu_cores"), "cpu_us_per_commit": e.get("cpu_us_per_commit"),
                     "p50": al.get("p50"), "p99": al.get("p99"), "achieved": lg.get("achieved"), "errors": lg.get("errors"),
                     "l0_per_shard": e["l0_sum_avg"] / p["shards"] if e else None, "l0_max_total": e.get("l0_max"), "l0_stalls": e.get("l0_stalls"), "fast_per_min": e.get("fast_switches_per_min"),
                     "rss_gb": e.get("rss_gb")})
    if "--json" in args:
        json.dump(rows, open(args[args.index("--json") + 1], "w"), indent=1)
    f = lambda x, d=1: "-" if x is None else f"{x:,.{d}f}"
    print("| shards | phase | commits/s | loads/s | CPU µs/commit | cores | p50 / p99 ms | L0 SSTs/shard avg (all-shard max) | L0 stalls | seg PUT | SST PUT | CAS | poll GET | SST GET | LIST | ctl | Class A / B | model A / B | S3 req $/mo (model) |")
    print("|" + "---|" * 19)
    for r in rows:
        print(f"| {r['shards']} | {r['phase']} | {f(r['commits_s'])} | {f(r['loads_s'])} | {f(r['cpu_us_per_commit'], 0)} | {f(r['cpu_cores'], 2)} | "
              f"{f(r['p50'])} / {f(r['p99'])} | {f(r['l0_per_shard'], 2)} ({f(r['l0_max_total'], 0)}) | {f(r['l0_stalls'], 0)} | {f(r['seg_put'])} | {f(r['sst_put'])} | "
              f"{f(r['cas'])} | {f(r['poll_get'])} | {f(r['sst_get'])} | {f(r['list'])} | {f(r['ctl'])} | {f(r['A'])} / {f(r['B'])} | "
              f"{f(r['model_A'])} / {f(r['model_B'])} | ${f(r['s3_req_usd_mo'], 0)} (${f(r['model_s3_req_usd_mo'], 0)}) |")


if __name__ == "__main__":
    main()
