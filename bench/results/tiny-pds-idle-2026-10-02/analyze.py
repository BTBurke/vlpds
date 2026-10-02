#!/usr/bin/env python3
"""Per-component object-store request rates of a tinypds.py run's measurement
window (last minus first scrape inside it), by billable class.

    analyze.py idle1 [pers1 ...]        # tables (markdown) on stdout
    analyze.py --json idle1 ...         # machine-readable
    analyze.py --timeline idle1         # per-scrape Class A / B rates
"""
import json
import re
import sys
from collections import defaultdict

CLASS_A = {"put", "put_create", "put_cas", "list", "copy", "mpu_create", "mpu_part", "mpu_complete", "delete_batch"}
CLASS_B = {"get", "get_range", "head"}
FREE = {"delete", "mpu_abort"}
LBL = re.compile(r'(\w+)="([^"]*)"')


def load(name):
    import gzip
    import os
    f = gzip.open(name + ".jsonl.gz", "rt") if os.path.exists(name + ".jsonl.gz") else open(name + ".jsonl")
    return [json.loads(l) for l in f]


def parse(m):
    out = []
    for k, v in (m or {}).items():
        out.append((k.split("{", 1)[0], dict(LBL.findall(k)), v))
    return out


def window(recs, start="measure:start", end="measure:end"):
    a = next(r for r in recs if r["phase"] == start and r["m"])
    b = next(r for r in reversed(recs) if r["phase"] in (end, "measure") and r["m"])
    return a, b


def delta(a, b, metric, keys):
    acc = defaultdict(float)
    pa = {(n, tuple(sorted(l.items()))): v for n, l, v in parse(a["m"]) if n == metric}
    for n, l, v in parse(b["m"]):
        if n != metric:
            continue
        d = v - pa.get((n, tuple(sorted(l.items()))), 0.0)
        if d:
            acc[tuple(l.get(k, "") for k in keys)] += d
    return acc


def klass(op):
    return "A" if op in CLASS_A else "B" if op in CLASS_B else "free" if op in FREE else "?"


def summarize(name):
    recs = load(name)
    a, b = window(recs)
    secs = b["t"] - a["t"]
    req = delta(a, b, "vlpds_object_store_requests_total", ("op", "component", "result"))
    sdb = delta(a, b, "slatedb_object_store_request_count_total", ("component", "api", "store_type"))
    by_comp = defaultdict(lambda: defaultdict(float))
    by_op = defaultdict(float)
    for (op, comp, res), n in req.items():
        by_comp[comp][klass(op)] += n
        by_comp[comp]["ops"] = by_comp[comp].get("ops", 0)
        by_op[(klass(op), op, comp, res)] += n
    counts = {}
    ca, cb = a.get("counts") or {}, b.get("counts") or {}
    for k in set(ca) | set(cb):
        counts[k] = cb.get(k, 0) - ca.get(k, 0)
    return {"name": name, "secs": secs, "by_comp": {c: dict(v) for c, v in by_comp.items()},
            "by_op": {"|".join(k): v for k, v in by_op.items()},
            "slatedb": {"|".join(k): v for k, v in sdb.items()}, "counts": counts,
            "meta": recs[0].get("meta")}


def fmt(x):
    return f"{x:.4f}" if x < 0.1 else f"{x:.3f}" if x < 10 else f"{x:.1f}"


def table(s):
    secs = s["secs"]
    print(f"### {s['name']} ({secs:.0f} s window; {s['meta']['args'] if s['meta'] else ''})\n")
    if s["counts"]:
        print("workload in window:", ", ".join(f"{k} {v}" for k, v in sorted(s["counts"].items())), "\n")
    print("| component | Class A /s | Class B /s | free (DELETE) /s |")
    print("|---|---|---|---|")
    ta = tb = tf = 0
    for c, v in sorted(s["by_comp"].items(), key=lambda kv: -(kv[1].get("A", 0) * 12.5 + kv[1].get("B", 0))):
        A, B, F = v.get("A", 0) / secs, v.get("B", 0) / secs, v.get("free", 0) / secs
        ta, tb, tf = ta + A, tb + B, tf + F
        print(f"| {c} | {fmt(A)} | {fmt(B)} | {fmt(F)} |")
    print(f"| **total** | **{fmt(ta)}** | **{fmt(tb)}** | {fmt(tf)} |\n")
    print("| class | op | component | result | count | /s | per hour |")
    print("|---|---|---|---|---|---|---|")
    for k, n in sorted(s["by_op"].items(), key=lambda kv: (kv[0].split("|")[0], -kv[1])):
        c, op, comp, res = k.split("|")
        print(f"| {c} | {op} | {comp} | {res} | {n:.0f} | {fmt(n / secs)} | {n / secs * 3600:.0f} |")
    print()
    if s["slatedb"]:
        print("SlateDB's own counts (`slatedb_object_store_request_count_total`):\n")
        print("| slatedb component | api | store | /s |")
        print("|---|---|---|---|")
        for k, n in sorted(s["slatedb"].items(), key=lambda kv: -kv[1]):
            c, op, m = k.split("|")
            print(f"| {c} | {op} | {m} | {fmt(n / secs)} |")
        print()
    return ta, tb


def timeline(name):
    recs = [r for r in load(name) if r["m"]]
    prev = None
    for r in recs:
        if prev:
            req = delta(prev, r, "vlpds_object_store_requests_total", ("op",))
            dt = r["t"] - prev["t"]
            A = sum(v for (op,), v in req.items() if op in CLASS_A) / dt
            B = sum(v for (op,), v in req.items() if op in CLASS_B) / dt
            print(f"{r['t'] - recs[0]['t']:7.0f}s {r['phase']:14s} A {A:7.3f}/s  B {B:7.3f}/s")
        prev = r


ORDER = ["ctl_lease", "ctl_assign", "ctl_version", "log_segment", "state_manifest", "state_compactions",
         "state_gc_boundary", "state_sst", "state_wal", "other", "blob"]


def matrix(runs):
    """Class A / Class B req/s per component (rows) and run (columns)."""
    S = {n: summarize(n) for n in runs}

    def f(x):
        return "0" if x == 0 else fmt(x)

    print("| component | " + " | ".join(runs) + " |")
    print("|---" * (len(runs) + 1) + "|")
    for c in ORDER:
        cells = []
        for n in runs:
            v, secs = S[n]["by_comp"].get(c, {}), S[n]["secs"]
            cells.append(f"{f(v.get('A', 0) / secs)} / {f(v.get('B', 0) / secs)}")
        print(f"| {c} | " + " | ".join(cells) + " |")
    cells = []
    for n in runs:
        secs = S[n]["secs"]
        a = sum(v.get("A", 0) for v in S[n]["by_comp"].values()) / secs
        b = sum(v.get("B", 0) for v in S[n]["by_comp"].values()) / secs
        cells.append(f"**{f(a)} / {f(b)}**")
    print("| **total** | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--matrix":
        matrix(args[1:])
    elif args and args[0] == "--timeline":
        timeline(args[1])
    elif args and args[0] == "--json":
        print(json.dumps([summarize(n) for n in args[1:]], indent=1))
    else:
        for n in args:
            table(summarize(n))
