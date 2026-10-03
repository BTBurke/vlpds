#!/usr/bin/env python3
"""Per bulk chunk: SST GET bytes (state_sst, dir=down) per created account and
meta-cache misses, from the 5 s labelled sampler. usage: chunkget.py sampler.jsonl populate.jsonl"""
import json, sys, collections
samp = collections.defaultdict(list)
for l in open(sys.argv[1]):
    r = json.loads(l)
    if r.get("up"):
        samp[r["node"]].append(r)
def delta(a, b, pred):
    tot = 0.0
    for n, rs in samp.items():
        prev = None
        for r in rs:
            if a <= r["t"] <= b:
                if prev:
                    for k, v in r["m"].items():
                        if pred(k):
                            p = prev["m"].get(k, 0.0); tot += v - p if v >= p else v
                prev = r
    return tot
print("| Accounts | secs | accounts/s | SST GET GB | KB per created account | SST GETs/s (all nodes) | meta-cache misses (filter / index) |")
print("|---|---|---|---|---|---|---|")
for l in open(sys.argv[2]):
    c = json.loads(l)
    if "t" not in c or "secs" not in c or "start" not in c:
        continue
    b = c["t"]; a = b - c["secs"]
    gb = delta(a, b, lambda k: k.startswith("vlpds_object_store_bytes_total") and "state_sst" in k and 'dir="down"' in k)
    n = delta(a, b, lambda k: k.startswith("vlpds_object_store_requests_total") and "state_sst" in k and 'op="get' in k)
    mf = delta(a, b, lambda k: k.startswith("vlpds_meta_cache_loads_total") and 'kind="filter",result="fetched"' in k)
    mi = delta(a, b, lambda k: k.startswith("vlpds_meta_cache_loads_total") and 'kind="index",result="fetched"' in k)
    if gb == 0 and n == 0:
        continue
    print(f"| {c['start']/1e6:.0f}–{(c['start']+c['count'])/1e6:.0f}M | {c['secs']:.0f} | {c['accounts_s']:,} | {gb/1e9:.1f} | {gb/1e3/max(c['created'],1):.1f} | {n/c['secs']:,.0f} | {mf:,.0f} / {mi:,.0f} |")
