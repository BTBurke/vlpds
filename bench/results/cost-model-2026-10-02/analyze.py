#!/usr/bin/env python3
"""Per-phase object-store request rates from raw.jsonl (measure.py).

    analyze.py [raw.jsonl] [--prefix P] [--json out.json]

For each phase: wall seconds, commits/s, and requests/s by (op, component)
summed over nodes (vlpds_object_store_requests_total), plus SlateDB's own
attribution (db / gc / compactor) and bytes/s. Rates use the first and last
in-phase snapshots (":start" .. ":end").
"""
import json
import re
import sys
from collections import defaultdict

LBL = re.compile(r'(\w+)="([^"]*)"')


def labels(k):
    return dict(LBL.findall(k.split("{", 1)[1])) if "{" in k else {}


def total(rec, fam, key_fn):
    out = defaultdict(float)
    for n, m in (rec.get("nodes") or {}).items():
        if not m:
            continue
        for k, v in m["labeled"].items():
            if k.split("{", 1)[0] == fam:
                out[key_fn(labels(k))] += v
    return out


def sums(rec, name):
    return sum((m or {}).get("sums", {}).get(name, 0.0) for m in (rec.get("nodes") or {}).values())


def phases(path, prefix=None):
    recs = [json.loads(l) for l in open(path)]
    # a snapshot where any node failed to answer /metrics (dead or stalled) is dropped
    recs = [r for r in recs if "nodes" in r and r["nodes"] and all(r["nodes"].values()) and (prefix is None or r.get("prefix") == prefix)]
    by = defaultdict(list)
    for r in recs:
        ph = r["phase"].split(":")[0]
        by[(r["prefix"], ph)].append(r)
    out = []
    for (pfx, ph), rs in by.items():
        rs.sort(key=lambda r: r["t"])
        a, b = rs[0], rs[-1]
        dt = b["t"] - a["t"]
        if dt <= 0:
            continue
        if any(set(a["nodes"]) != set(r["nodes"]) for r in rs):
            pass
        d = lambda fam, fn: {k: (v - total(a, fam, fn).get(k, 0.0)) / dt for k, v in total(b, fam, fn).items()}
        req = d("vlpds_object_store_requests_total", lambda l: (l["op"], l["component"]))
        byt = d("vlpds_object_store_bytes_total", lambda l: (l["dir"], l["component"]))
        sl = d("slatedb_object_store_request_count_total", lambda l: (l.get("component", "?"), l.get("store_type", "?"), l.get("op", "?"), l.get("api", l.get("method", "?"))))
        commits = (sums(b, "vlpds_commits_total") - sums(a, "vlpds_commits_total")) / dt
        segs = (sums(b, "vlpds_segments_total") - sums(a, "vlpds_segments_total")) / dt
        hedges = (sums(b, "vlpds_segment_put_hedges_total") - sums(a, "vlpds_segment_put_hedges_total")) / dt
        loads = (sums(b, "vlpds_repo_loads_total") - sums(a, "vlpds_repo_loads_total")) / dt
        stored = (sums(b, "vlpds_segment_stored_bytes_total") - sums(a, "vlpds_segment_stored_bytes_total")) / dt
        raw = (sums(b, "vlpds_segment_bytes_total") - sums(a, "vlpds_segment_bytes_total")) / dt
        out.append({
            "prefix": pfx, "phase": ph, "nodes": len(b["nodes"]), "shards": b.get("shards"), "secs": round(dt),
            "t0": a["t"], "commits_s": commits, "segments_s": segs, "hedges_s": hedges, "repo_loads_s": loads,
            "seg_stored_Bps": stored, "seg_raw_Bps": raw,
            "req_s": {f"{o}|{c}": v for (o, c), v in sorted(req.items()) if v > 0},
            "bytes_s": {f"{o}|{c}": v for (o, c), v in sorted(byt.items()) if v > 0},
            "slatedb_req_s": {"|".join(k): v for k, v in sorted(sl.items()) if v > 0},
            "du_end": b.get("du"), "du_start": a.get("du"),
        })
    out.sort(key=lambda p: p["t0"])
    return out


def main():
    args = sys.argv[1:]
    path = args[0] if args and not args[0].startswith("--") else "raw.jsonl"
    prefix = args[args.index("--prefix") + 1] if "--prefix" in args else None
    ps = phases(path, prefix)
    if "--json" in args:
        json.dump(ps, open(args[args.index("--json") + 1], "w"), indent=1)
    for p in ps:
        tot = sum(p["req_s"].values())
        print(f"== {p['prefix']} {p['phase']} nodes={p['nodes']} shards={p['shards']} {p['secs']}s commits/s={p['commits_s']:.1f} "
              f"segs/s={p['segments_s']:.2f} hedges/s={p['hedges_s']:.3f} loads/s={p['repo_loads_s']:.2f} total req/s={tot:.2f}")
        for k, v in sorted(p["req_s"].items(), key=lambda kv: -kv[1]):
            print(f"   {k:40s} {v:10.3f}/s")
        if "--slatedb" in args:
            for k, v in sorted(p["slatedb_req_s"].items(), key=lambda kv: -kv[1]):
                print(f"   sdb {k:50s} {v:10.3f}/s")
        if "--bytes" in args:
            for k, v in sorted(p["bytes_s"].items(), key=lambda kv: -kv[1]):
                print(f"   B {k:40s} {v/1e3:10.1f} kB/s")


if __name__ == "__main__":
    main()
