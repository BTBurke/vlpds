#!/usr/bin/env python3
"""Per failover event: loadgen errors / seconds with errors / worst p99 after the
signal, peak S3 sockets per node (hosts.jsonl), and counter deltas of the
shard-move metrics from the 1 s labelled sampler (resets on restart handled).
usage: events.py <xh dir>"""
import gzip, json, os, re, sys, collections
d = sys.argv[1]
steps = [json.loads(l) for l in open(os.path.join(d, "steps.jsonl"))]
hosts = [json.loads(l) for l in open(os.path.join(d, "hosts.jsonl"))]
samp = collections.defaultdict(list)
for l in gzip.open(os.path.join(d, "sampler", "xh6-all.jsonl.gz"), "rt"):
    r = json.loads(l)
    if r.get("up"):
        samp[r["node"]].append(r)
FAM = ("vlpds_shard_warm_seconds_sum", "vlpds_shard_warm_seconds_count", "vlpds_shard_warm_ssts_total", "vlpds_security_ctl_loads_total",
       "vlpds_meta_cache_loads_total", "vlpds_object_store_permit_waits_total", "vlpds_repo_loads_total", "vlpds_repo_preloads_total",
       "vlpds_shards_opened_total", "vlpds_shard_open_seconds_sum", "vlpds_shard_open_seconds_count")
def deltas(node, a, b):
    out = collections.Counter(); prev = None
    for r in samp[node]:
        if not (a <= r["t"] <= b):
            continue
        if prev is not None:
            for k, v in r["m"].items():
                if not k.startswith(FAM):
                    continue
                p = prev["m"].get(k, 0.0)
                out[k] += v - p if v >= p else v   # reset: counter restarted
        prev = r
    return out
def peak(node, a, b, pat):
    m = 0
    for r in samp[node]:
        if a <= r["t"] <= b:
            for k, v in r["m"].items():
                if re.search(pat, k):
                    m = max(m, v)
    return m
for s in steps:
    ev = s.get("event")
    if not ev:
        continue
    sig = ev["signal_t"]
    errs = collections.Counter(); p99 = 0
    for lg in s["windows"]:   # loadgen windows: err is cumulative
        prev = 0
        for w in lg:
            if w["t"] >= sig:
                errs[w["t"]] += w["err"] - prev; p99 = max(p99, w["p99"] or 0)
            prev = w["err"]
    a, b = s["t0"] + sig - 2, s["t1"]
    socks = {}
    for h in hosts:
        if a <= h["t"] <= b:
            for n, v in h["nodes"].items():
                if n.startswith("n"):
                    socks[n] = max(socks.get(n, 0), v["sock"]["n_s3"])
    print(f"== {s['tag']} @{s['t0']:.0f}: errors {sum(errs.values()):,} in {sum(1 for v in errs.values() if v)} s with errors; worst p99 {p99/1000:.1f} s; "
          f"takeover {ev['takeover_s']} s, exit {ev['exited_t']-sig:.1f} s, rejoin converged {ev['rejoin_converged_s']} s; peak S3 sockets {socks}")
    tot = collections.Counter()
    for n in sorted(samp):
        dl = deltas(n, a, b); tot.update(dl)
        st = peak(n, a, b, r'vlpds_object_store_inflight\{.*state')
        w = dl.get("vlpds_shard_warm_seconds_sum", 0); c = dl.get("vlpds_shard_warm_seconds_count", 0)
        print(f"   {n}: state inflight peak {st:.0f}; warm batches {c:.0f} sum {w:.1f}s; repo loads {sum(v for k,v in dl.items() if k.startswith('vlpds_repo_loads_total')):,.0f}; "
              f"state permit waits {sum(v for k,v in dl.items() if k.startswith('vlpds_object_store_permit_waits_total') and 'state' in k):,.0f}")
    for fam in ("vlpds_shard_warm_ssts_total", "vlpds_security_ctl_loads_total", "vlpds_meta_cache_loads_total", "vlpds_shards_opened_total"):
        items = {k[len(fam):]: v for k, v in tot.items() if k.startswith(fam) and v}
        print(f"   {fam}: {items}")

print("\nSST GETs per repo load, 30 s from the signal (state client, component state_sst, get/get_range):")
for s in steps:
    ev = s.get("event")
    if not ev:
        continue
    a = s["t0"] + ev["signal_t"]; b = a + 30
    row = []
    for n in sorted(samp):
        dl = collections.Counter(); prev = None
        for r in samp[n]:
            if a <= r["t"] <= b:
                if prev:
                    for k, v in r["m"].items():
                        if k.startswith(("vlpds_object_store_requests_total", "vlpds_repo_loads_total")):
                            p = prev["m"].get(k, 0.0); dl[k] += v - p if v >= p else v
                prev = r
        gets = sum(v for k, v in dl.items() if 'component="state_sst"' in k and 'op="get' in k)
        loads = sum(v for k, v in dl.items() if k.startswith("vlpds_repo_loads_total"))
        row.append(f"{n} {gets:,.0f} GETs / {loads:,.0f} loads = {gets/max(loads,1):.2f}")
    print(f"  {s['tag']} @{s['t0']:.0f}: " + "; ".join(row))
