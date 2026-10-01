#!/usr/bin/env python3
"""Render the JSONL results in this directory as markdown tables (stdout)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def rows(name):
    p = os.path.join(HERE, name)
    return [json.loads(l) for l in open(p)] if os.path.exists(p) else []


def grid(name="grid.jsonl"):
    print("| Shape (total/active/inj) | Offered/s | Achieved/s | Err | Dropped | p50 ms | p90 | p99 | p99.9 | Hot p99 | FH lag p50/p99 | Srv CPU % | RSS GB | Loads/s | Commit/seg |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows(name):
        a = r.get("all") or {}
        h = r.get("hot-repo") or {}
        f = r.get("fh-lag") or {}
        m = r.get("metrics_delta", {})
        dur = r["duration_s"] + 10
        cps = m.get("commits_total", 0) / max(1, m.get("segments_total", 1))
        print(f"| {r['shape']} | {r['target']} | {r.get('achieved')} | {r.get('errors')} | {r.get('dropped')} | {a.get('p50')} | {a.get('p90')} | {a.get('p99')} | {a.get('p999')} | {h.get('p99')} | {f.get('p50')}/{f.get('p99')} | {r['server']['cpu_pct_avg']:.0f} | {r['server']['rss_gb_max']} | {m.get('repo_loads_total', 0)/dur:.0f} | {cps:.0f} |")


def cluster(name="cluster.jsonl"):
    print("| Shape | Offered/s | Achieved/s | Err | Dropped | worst p50 | worst p99 | worst p99.9 | FH lag p50/p99 (n1) | CPU % per node | Forwarded/s |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows(name):
        f = (r["per_loadgen"][0].get("fh-lag") or {})
        fw = sum(m.get("requests_forwarded_total", 0) for m in r["metrics_delta"]) / (r["duration_s"] + 10)
        cpu = " / ".join(f"{s['cpu_pct_avg']:.0f}" for s in r["servers"][:r["nodes"]])
        print(f"| {r['shape']} | {r['rate'] + 200} | {r['achieved']} | {r['errors']} | {r['dropped']} | {r['all_p50_max']} | {r['all_p99_max']} | {r['all_p999_max']} | {f.get('p50')}/{f.get('p99')} | {cpu} | {fw:.0f} |")


if __name__ == "__main__":
    {"grid": grid, "cluster": cluster}[sys.argv[1]](*sys.argv[2:])
