#!/usr/bin/env python3
"""vlpds object-store cost model (requests + storage) for S3 / GCS / R2.

    cost_model.py            # fit on raw*.jsonl, validate, print every table
    cost_model.py --json     # same, machine-readable

Requests/s are modeled as
  per shard  : SlateDB polling (DB manifest poll, compactor coordinator and
               worker polls; each "read latest" = a probe GET of the next id
               + a GET of the GC boundary file), SlateDB GC passes
  per flush  : a checkpoint memtable flush (L0 SST PUT + manifest CAS) and
               the compaction work it causes later (compactions-file CAS,
               compactor SST GETs/PUTs, manifest CAS), measured as a bundle
  per node   : log segment PUTs (latency-bound: one PUT per round trip while
               writes trickle in; size-bound only past ~20k commits/s/node),
               hedges, retention LIST/DELETE, lease CAS + control-plane LISTs
  per load   : cold repo loads (SST range GETs that miss the disk cache)
Checkpoints run sequentially over a node's shards and then sleep 10 s, so
a node's flush rate is s/(10 + s*t_flush) for s shards per node (measured
t_flush ~60 ms with 20/30 ms injected read/write latency).

Coefficients come from measure.py runs (raw.jsonl, raw1024.jsonl,
raw3.jsonl in this directory); `fit()` re-derives them and `validate()`
compares the model against every measured phase.
"""
import glob
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import analyze  # noqa: E402

MONTH_S = 30.4375 * 86400  # average month
GB = 1e9
GIB = 1 << 30

# ---------------------------------------------------------------- prices
# Fetched 2026-10-01.
PRICES = {
    "s3": {
        # https://aws.amazon.com/s3/pricing/ (US East N. Virginia, S3 Standard)
        "name": "AWS S3 Standard (us-east-1)",
        "storage_gb_mo": [(50e3, 0.023), (450e3, 0.022), (math.inf, 0.021)],  # per GB, tiered by TB
        "class_a_per_1k": 0.005,   # PUT, COPY, POST, LIST (conditional PUTs and multipart parts = PUT)
        "class_b_per_1k": 0.0004,  # GET, SELECT, HEAD and all other
        # DELETE and CANCEL are free
        "free_tier": None,
        "egress_note": "S3 -> EC2 in the same region: free",
    },
    "gcs": {
        # https://cloud.google.com/storage/pricing (regional bucket, e.g. us-central1; flat namespace)
        "name": "GCS Standard (regional, us-central1)",
        "storage_gb_mo": [(math.inf, 0.000027397 * 730 * GB / GIB)],  # $0.000027397/GiB-hour = $0.0200/GiB-mo
        "class_a_per_1k": 0.005,   # objects.insert/copy/compose/list, XML PUT/POST, GET Bucket (list)
        "class_b_per_1k": 0.0004,  # objects.get, XML GET Object / HEAD
        # objects.delete is free
        "free_tier": None,
        "egress_note": "same-location transfer to Google Cloud: free",
    },
    "r2": {
        # https://developers.cloudflare.com/r2/pricing/ (Standard)
        "name": "Cloudflare R2 Standard",
        "storage_gb_mo": [(math.inf, 0.015)],
        "class_a_per_1k": 4.50 / 1000,  # $4.50/M: PutObject, CopyObject, ListObjects, Create/CompleteMultipartUpload, UploadPart
        "class_b_per_1k": 0.36 / 1000,  # $0.36/M: GetObject, HeadObject
        # DeleteObject / AbortMultipartUpload are free. The bulk DeleteObjects
        # call object_store uses is not listed; billed here as Class A.
        "free_tier": {"class_a": 1e6, "class_b": 10e6, "storage_gb": 10},
        "egress_note": "egress free",
    },
}

CLASS_A = {"put", "put_create", "put_cas", "list", "copy", "mpu_create", "mpu_part", "mpu_complete"}
CLASS_B = {"get", "get_range", "head"}
# delete_batch (S3 DeleteObjects POST, one per <=1,000 keys): free on S3 (DELETE), GCS has no
# bulk delete (object_store deletes one by one, free); billed as Class A on R2 (not in its free list).
# delete / mpu_abort: free everywhere.

# ---------------------------------------------------------------- measured coefficients
COEF = {}

# SlateDB GC (every shard's DB runs it every 10 min): per pass LIST manifest/, compactions/,
# compacted/, wal/ (x2) + a CAS of each boundary file it advances; deletes are free.
GC_OPS_PER_PASS = 6
GC_INTERVAL_S = 600


def phase_list():
    ps = []
    for f in sorted(glob.glob(os.path.join(HERE, "raw*.jsonl"))):
        ps += analyze.phases(f)
    return ps


def split(req_s):
    """{op|component: r/s} -> (A, B, delete_batch, free) r/s"""
    a = b = db = free = 0.0
    for k, v in req_s.items():
        op = k.split("|")[0]
        if op in CLASS_A:
            a += v
        elif op in CLASS_B:
            b += v
        elif op == "delete_batch":
            db += v
        else:
            free += v
    return a, b, db, free


def pick(req_s, pred):
    return sum(v for k, v in req_s.items() if pred(*k.split("|")))


def fit(ps):
    """Derives the per-shard / per-flush / per-node / per-load coefficients from the 1-node,
    256-shard phases; every other phase (1,024 shards, 3 nodes) is validation only."""
    byname = {(p["prefix"], p["phase"]): p for p in ps}
    c = {}
    # --- per-shard polling: the idle phase of a fresh incarnation (no segment yet -> no
    # checkpoints, no compaction): pure polling. Analytic form: every "read latest" of a
    # SlateDB sequenced file = 1 probe GET of id+1 + 1 GET of gc/<file>.boundary; the DB
    # polls the manifest every 1 s, the compactor coordinator every 5 s (manifest +
    # compactions), the worker every 5 s (compactions): 2*(1/1 + 2/5 + 1/5) = 3.2 GET/s.
    idle = byname[("costmodel", "one/idle")]
    S = idle["shards"]
    c["poll_get_per_shard"] = pick(idle["req_s"], lambda o, k: o in CLASS_B and k.startswith("state_")) / S
    c["poll_analytic_per_shard"] = 2 * (1 / 1.0 + 2 / 5.0 + 1 / 5.0)
    c["gc_ops_per_shard_meas"] = pick(idle["req_s"], lambda o, k: o in CLASS_A and k.startswith("state_")) / S
    # --- control plane (TTL 10 s: a step every 2 s): lease CAS, LIST nodes/, LIST assign/
    # (per 1,000 keys) per node; GETs of assign/ objects scale with shards (periodic
    # re-reads), plus one GET per peer lease renewal.
    c["ctl_put_per_node"] = pick(idle["req_s"], lambda o, k: o.startswith("put") and k.startswith("ctl_"))
    c["ctl_list_per_node"] = pick(idle["req_s"], lambda o, k: o == "list" and k.startswith("ctl_"))
    c["ctl_get_per_shard"] = pick(idle["req_s"], lambda o, k: o in CLASS_B and k.startswith("ctl_")) / S
    # --- loaded 1-node 256-shard phases
    loaded = [p for p in ps if p["prefix"] == "costmodel" and p["nodes"] == 1 and p["commits_s"] > 50 and not p["phase"].endswith(("settle", "idle"))]
    # segment PUTs (one log, K=4, 8 MiB cap; size cap never binds here): a segment is sealed
    # when the previous PUT completes if anything queued, else on the next arrival, so
    # rate = 1 / (L + t_o + exp(-cL)/c), L = mean PUT latency (lognormal 30 ms, sigma .5)
    L = 0.030 * math.exp(0.5 ** 2 / 2)
    c["seg_put_latency_mean_s"] = L
    c["seg_overhead_s"] = sum(1 / p["segments_s"] - L - math.exp(-p["commits_s"] * L) / p["commits_s"] for p in loaded) / len(loaded)
    c["hedge_frac"] = sum(p["hedges_s"] for p in loaded) / sum(p["segments_s"] for p in loaded)
    # Checkpoint flushes: checkpoint_all walks a node's shards one at a time (write the applied
    # marker, flush the memtable: L0 SST PUT + manifest read/CAS + boundary check), then sleeps
    # 10 s, so a node flushes L0s at F = s / (10 s + s * t_flush). t_flush comes from the phases
    # where compaction adds no SST PUTs of its own (one2/idle: checkpoints with no ingest;
    # s1024b/avg: 1,024 shards, ~0.3 commits/s per shard): 0.072 s at 20/30 ms injected
    # read/write latency.
    gc = GC_OPS_PER_PASS / GC_INTERVAL_S
    tf_ph = [p for p in ps if (p["prefix"], p["phase"]) in (("costmodel", "one2/idle"), ("cost1024", "s1024b/avg"))]
    tfl = []
    for p in tf_ph:
        s_n = p["shards"] / p["nodes"]
        sst = pick(p["req_s"], lambda o, k: o == "put" and k == "state_sst") / p["nodes"]
        tfl.append((s_n / sst - 10.0) / s_n)
    c["t_flush_s"] = sum(tfl) / len(tfl)

    def F(p):
        s_n = p["shards"] / p["nodes"]
        return p["nodes"] * s_n / (10.0 + s_n * c["t_flush_s"])

    # Per-flush bundle (Class A): every state Class A op beyond the GC passes per L0 flush, on
    # the loaded 256-shard phases, so it includes the compactions those L0s trigger (compactor
    # SST PUTs, compactions-file CAS x~1, manifest CAS). Without ingest (one2/idle) it is lower;
    # see validation.
    fa = [(pick(p["req_s"], lambda o, k: o in CLASS_A and k.startswith("state_")) - gc * p["shards"]) / F(p) for p in loaded]
    c["flush_classA"] = sum(fa) / len(fa)
    idle_ck = byname[("costmodel", "one2/idle")]
    c["flush_classA_noingest"] = (pick(idle_ck["req_s"], lambda o, k: o in CLASS_A and k.startswith("state_")) - gc * idle_ck["shards"]) / F(idle_ck)
    # Class B beyond polling: per flush (manifest re-reads after each CAS, compaction input
    # GETs) from one2/idle, then per cold repo load from the loaded phases.
    bx = lambda p: pick(p["req_s"], lambda o, k: o in CLASS_B and k.startswith("state_")) - c["poll_get_per_shard"] * p["shards"]
    c["flush_classB"] = bx(idle_ck) / F(idle_ck)
    yl = [(bx(p) - c["flush_classB"] * F(p)) / p["repo_loads_s"] for p in loaded if p["repo_loads_s"] > 1]
    c["load_classB"] = max(0.0, sum(yl) / len(yl))
    # retention, per node: paged LIST of its log from the head + report GET/PUT every 60 s
    c["retention_classA_per_node"] = sum(pick(p["req_s"], lambda o, k: o in CLASS_A and o != "put_create" and k in ("log_segment", "retention_report")) for p in loaded) / len(loaded)
    c["retention_classB_per_node"] = sum(pick(p["req_s"], lambda o, k: o in CLASS_B and k in ("log_segment", "retention_report")) for p in loaded) / len(loaded)
    return c


# ---------------------------------------------------------------- model
DEFAULT_KNOBS = {
    "db_manifest_poll_s": 1.0,   # SlateDB Settings.manifest_poll_interval
    "compactor_poll_s": 5.0,     # CompactorOptions.poll_interval (adaptive: 5 s while L0 is shallow)
    "worker_poll_s": 5.0,        # CompactionWorkerOptions.compactions_poll_interval
    "gc_interval_s": 600.0,      # SlateDB GC directory interval
    "checkpoint_s": 10.0,        # node checkpoint sleep between sequential passes
    "t_flush_s": None,           # per-shard flush time in a checkpoint pass (None = measured)
    "linger_s": 0.0,             # minimum segment age before sealing (not implemented: 0)
    "K": 4,                      # --log-inflight
    "max_segment_mb": 8.0,       # --max-segment-mb
    "put_latency_mean_s": None,  # segment PUT latency (None = measured: lognormal 30 ms median)
    "lease_ttl_s": 10.0,
    "raw_bytes_per_commit": 5370,  # uncompressed segment bytes / commit (real data)
    "compaction_bytes_per_commit": 2000,
}


def seg_puts_per_node(c, cps_node, k):
    """Segment PUTs/s of one node log at cps_node commits/s."""
    if cps_node <= 0:
        return 0.0
    L = k["put_latency_mean_s"] or c["seg_put_latency_mean_s"]
    T = L + c["seg_overhead_s"] + k["linger_s"]
    lat_bound = 1.0 / (T + math.exp(-cps_node * T) / cps_node)
    # with K PUTs in flight a segment seals early at max_segment/K
    size_bound = cps_node * k["raw_bytes_per_commit"] / (k["max_segment_mb"] * (1 << 20) / k["K"])
    return max(min(cps_node, lat_bound), size_bound) * (1 + c["hedge_frac"])


def sst_puts_per_node(c, shards_node, k):
    tf = k["t_flush_s"] or c["t_flush_s"]
    return shards_node / (k["checkpoint_s"] + shards_node * tf)


def requests(c, nodes, shards, cps_avg, loads_s, knobs=None, checkpoints=True):
    """Average requests/s by component -> {component: (A, B, delete_batch, free)}."""
    k = dict(DEFAULT_KNOBS, **(knobs or {}))
    poll = 2 * (1 / k["db_manifest_poll_s"] + 2 / k["compactor_poll_s"] + 1 / k["worker_poll_s"])
    poll_scale = poll / c["poll_analytic_per_shard"]
    s_node = shards / nodes
    segs = nodes * seg_puts_per_node(c, cps_avg / nodes, k)
    # checkpoints flush every owned shard once the node's log has a durable segment, even when
    # no further writes arrive (each pass writes the applied marker); a fresh idle node doesn't
    fl = nodes * sst_puts_per_node(c, s_node, k) if (cps_avg > 0 or checkpoints == "always") else 0.0
    step = k["lease_ttl_s"] / 5 / 2.0  # control-plane step interval relative to TTL 10 s
    return {
        "log segment PUTs (If-None-Match; incl. ~1% hedges)": (segs, 0.0, 0.0, 0.0),
        "log retention (LIST/report; DELETEs)": (nodes * c["retention_classA_per_node"], nodes * c["retention_classB_per_node"], segs / 1000, segs),
        "SlateDB polling, per shard (manifest/compactions probe GET + GC boundary GET)": (0.0, shards * c["poll_get_per_shard"] * poll_scale, 0.0, 0.0),
        "SlateDB GC passes, per shard (LIST x5, boundary CAS)": (shards * GC_OPS_PER_PASS / k["gc_interval_s"], 0.0, shards * 2 / k["gc_interval_s"], 0.0),
        "checkpoint flush + compaction (SST/manifest/compactions PUTs, SST GETs)": (fl * (c["flush_classA"] if cps_avg > 0 else c["flush_classA_noingest"]), fl * c["flush_classB"], 0.0, fl),
        "cold repo loads (SST range GETs past the disk cache)": (0.0, loads_s * c["load_classB"], 0.0, 0.0),
        # Byte-driven compaction (not visible at bench scale): ~2 KB of SST bytes rewritten per
        # commit (~500 B zstd of new rows x ~4 size-tiered write amplification), read in 2 MiB
        # GETs (CompactionWorkerOptions.bytes_to_fetch) and written as SSTs of up to 256 MiB
        "compaction bytes (2 MiB input GETs, <=256 MiB output PUTs)": (cps_avg * k["compaction_bytes_per_commit"] / (256 << 20),
                                                                        cps_avg * k["compaction_bytes_per_commit"] / (2 << 20), 0.0, 0.0),
        "control plane (lease CAS, LIST nodes/ + assign/, assignment + peer lease GETs)": (
            nodes * (c["ctl_put_per_node"] + c["ctl_list_per_node"] * max(1, math.ceil(shards / 1000))) / step,
            (shards * c["ctl_get_per_shard"] + nodes * max(nodes - 1, 0) * 0.5) / step, 0.0, 0.0),
    }


def storage_gb(records, repos, cps_avg, knobs=None):
    k = dict(knobs or {})
    state = (records * 154.2 + repos * 323.0) / GB
    return {"state (zstd SSTs, live)": state,
            "SlateDB transient (replaced SSTs until checkpoint expiry + GC)": state * (k.get("state_headroom", 1.25) - 1),
            "log, 72 h retention (zstd segments)": cps_avg * 86400 * k.get("retention_days", 3) * k.get("stored_bytes_per_commit", 2700) / GB}


def tiered(gb, tiers):
    cost, left, lo = 0.0, gb, 0.0
    for hi_tb, price in tiers:
        span = min(left, hi_tb * 1000 - lo) if hi_tb != math.inf else left
        cost += span * price
        left -= span
        lo += span
        if left <= 0:
            break
    return cost


def price(prov, req, sto):
    p = PRICES[prov]
    a = sum(v[0] + (v[2] if prov == "r2" else 0.0) for v in req.values()) * MONTH_S
    b = sum(v[1] for v in req.values()) * MONTH_S
    sgb = sum(sto.values())
    if p["free_tier"]:
        a = max(0.0, a - p["free_tier"]["class_a"])
        b = max(0.0, b - p["free_tier"]["class_b"])
        sgb = max(0.0, sgb - p["free_tier"]["storage_gb"])
    ca = a / 1000 * p["class_a_per_1k"]
    cb = b / 1000 * p["class_b_per_1k"]
    cs = tiered(sgb, p["storage_gb_mo"])
    return {"class_a_M": a / 1e6, "class_b_M": b / 1e6, "class_a_$": ca, "class_b_$": cb, "storage_$": cs, "total_$": ca + cb + cs}


SCENARIOS = {
    # commits/s averaged over the month; loads/s = cold repo loads (first write of a repo not in memory)
    "bluesky-today": {"cps": 334.0, "repos": 56.0e6, "records": 23.9e9, "loads": 35.0,
                      "desc": "28.9 M record ops/day as commits (334/s avg, ~420/s peak hour), 56 M bsky-hosted repos, 23.9 B records"},
    "bluesky-today-90M": {"cps": 334.0, "repos": 89.9e6, "records": 23.9e9, "loads": 35.0,
                          "desc": "same, every PLC DID (89.9 M) as a repo"},
    "sizing-today": {"cps": 1600.0, "repos": 50e6, "records": 25e9, "loads": 60.0,
                     "desc": "DESIGN sizing baseline: 50 M repos, 25 B records, 2,000/s daily peak (~1,600/s avg)"},
    "sizing-100x": {"cps": 160_000.0, "repos": 1e9, "records": 25e9 * 20, "loads": 1200.0,
                    "desc": "100x writes (200k/s peak, ~160k/s avg), 20x accounts (1 B) and records (500 B)"},
}


def project(c, scen, nodes, shards, knobs=None):
    s = SCENARIOS[scen]
    req = requests(c, nodes, shards, s["cps"], s["loads"], knobs)
    sto = storage_gb(s["records"], s["repos"], s["cps"], knobs)
    return req, sto, {p: price(p, req, sto) for p in PRICES}


SENSITIVITY = [
    ("baseline (defaults)", {}),
    ("DB manifest poll 5 s", {"db_manifest_poll_s": 5}),
    ("DB manifest poll 10 s", {"db_manifest_poll_s": 10}),
    ("DB manifest poll 30 s", {"db_manifest_poll_s": 30}),
    ("compactor + worker polls 30 s", {"compactor_poll_s": 30, "worker_poll_s": 30}),
    ("manifest 10 s + compactor/worker 30 s", {"db_manifest_poll_s": 10, "compactor_poll_s": 30, "worker_poll_s": 30}),
    ("checkpoint every 30 s", {"checkpoint_s": 30}),
    ("checkpoint every 60 s", {"checkpoint_s": 60}),
    ("segment linger 50 ms", {"linger_s": 0.05}),
    ("segment linger 100 ms", {"linger_s": 0.10}),
    ("segment linger 250 ms", {"linger_s": 0.25}),
    ("K = 1", {"K": 1}),
    ("K = 8", {"K": 8}),
    ("segment cap 2 MiB", {"max_segment_mb": 2}),
    ("segment cap 32 MiB", {"max_segment_mb": 32}),
    ("S3 Express-like latency (6 ms PUTs, t_flush 15 ms)", {"put_latency_mean_s": 0.006, "t_flush_s": 0.015}),
    ("GC interval 30 min", {"gc_interval_s": 1800}),
    ("all tuned: manifest 10 s, polls 30 s, checkpoint 30 s, linger 100 ms",
     {"db_manifest_poll_s": 10, "compactor_poll_s": 30, "worker_poll_s": 30, "checkpoint_s": 30, "linger_s": 0.10}),
]


def sensitivity(c, scen, nodes, shards):
    rows = []
    base = None
    for name, kn in SENSITIVITY:
        req, sto, cost = project(c, scen, nodes, shards, kn)
        tot = {p: cost[p]["total_$"] for p in PRICES}
        if base is None:
            base = tot
        rows.append({"knobs": name, "class_a_M": cost["s3"]["class_a_M"], "class_b_M": cost["s3"]["class_b_M"], **{p: tot[p] for p in PRICES},
                     "delta_s3": tot["s3"] - base["s3"]})
    return rows


def validate(c, ps):
    rows = []
    for p in ps:
        # settle/join: populations settling and shard handoffs (one-off); three/idle: right after
        # the handoffs (only n1's log has segments, so only n1 checkpoints), not a steady state
        if p["phase"].endswith(("settle", "join")) or p["phase"] == "three/idle":
            continue
        meas_a, meas_b, _, _ = split(p["req_s"])
        # bench nodes ran a 30 s lease TTL except the first 1-node run (10 s)
        ttl = 10.0 if p["phase"].startswith("one/") else 30.0
        # one2/idle followed writes in the same incarnation (checkpoints keep flushing)
        ck = "always" if p["phase"] in ("one2/idle",) else True
        req = requests(c, p["nodes"], p["shards"], p["commits_s"], p["repo_loads_s"], {"lease_ttl_s": ttl}, ck)
        ma = sum(v[0] for v in req.values())
        mb = sum(v[1] for v in req.values())
        rows.append({"run": f"{p['prefix']} {p['phase']}", "nodes": p["nodes"], "shards": p["shards"], "commits_s": p["commits_s"],
                     "meas_A": meas_a, "model_A": ma, "meas_B": meas_b, "model_B": mb, "secs": p["secs"]})
    return rows


def fmt_money(x):
    return f"${x:,.0f}"


def main():
    ps = phase_list()
    global COEF
    COEF = fit(ps)
    c = COEF
    out = {"coef": c, "validate": validate(c, ps), "projections": []}
    for scen in SCENARIOS:
        for nodes in (3, 8, 16):
            for shards in (256, 1024):
                req, sto, cost = project(c, scen, nodes, shards)
                out["projections"].append({"scenario": scen, "nodes": nodes, "shards": shards,
                                           "req": req, "storage_gb": sto, "cost": cost})
    out["sensitivity"] = {f"{scen} {n}n/{sh}s": sensitivity(c, scen, n, sh) for scen, n, sh in
                          [("bluesky-today", 3, 256), ("bluesky-today", 8, 1024), ("sizing-100x", 8, 1024)]}
    if "--json" in sys.argv:
        print(json.dumps(out, indent=1, default=str))
        return
    print("## coefficients")
    for k, v in c.items():
        print(f"  {k:32s} {v:.5g}")
    print("\n## validation (requests/s: measured vs model)")
    print("| run | nodes | shards | commits/s | Class A meas | model | Class B meas | model |")
    print("|---|---|---|---|---|---|---|---|")
    for r in out["validate"]:
        print(f"| {r['run']} | {r['nodes']} | {r['shards']} | {r['commits_s']:.0f} | {r['meas_A']:.1f} | {r['model_A']:.1f} ({(r['model_A']/r['meas_A']-1)*100:+.0f}%) | "
              f"{r['meas_B']:.0f} | {r['model_B']:.0f} ({(r['model_B']/r['meas_B']-1)*100:+.0f}%) |")
    print("\n## breakdown: bluesky-today, 3 nodes, 256 shards (requests/s; $/mo S3 | GCS | R2)")
    req, sto, cost = project(c, "bluesky-today", 3, 256)
    print("| component | Class A /s | Class B /s | S3 $/mo | GCS $/mo | R2 $/mo |")
    print("|---|---|---|---|---|---|")
    for name, (a, b, db, fr) in req.items():
        one = {name: (a, b, db, fr)}
        cc = {p: price(p, one, {}) for p in PRICES}
        print(f"| {name} | {a + db:.1f} | {b:.0f} | " + " | ".join(fmt_money(cc[p]["class_a_$"] + cc[p]["class_b_$"]) for p in ("s3", "gcs", "r2")) + " |")
    for name, gb in sto.items():
        print(f"| storage: {name} | {gb:,.0f} GB | | " + " | ".join(fmt_money(tiered(gb, PRICES[p]["storage_gb_mo"])) for p in ("s3", "gcs", "r2")) + " |")
    print("| **total** | | | " + " | ".join(f"**{fmt_money(cost[p]['total_$'])}**" for p in ("s3", "gcs", "r2")) + " |")
    for key, rows in out["sensitivity"].items():
        print(f"\n## sensitivity: {key} ($/mo)")
        print("| knobs | Class A M/mo | Class B M/mo | S3 | GCS | R2 | S3 delta |")
        print("|---|---|---|---|---|---|---|")
        for r in rows:
            print(f"| {r['knobs']} | {r['class_a_M']:,.0f} | {r['class_b_M']:,.0f} | {fmt_money(r['s3'])} | {fmt_money(r['gcs'])} | {fmt_money(r['r2'])} | {r['delta_s3']:+,.0f} |")
    for scen in SCENARIOS:
        print(f"\n## {scen}: {SCENARIOS[scen]['desc']}")
        print("| nodes | shards | Class A M/mo | Class B M/mo | storage GB | S3 | GCS | R2 |")
        print("|---|---|---|---|---|---|---|---|")
        for pr in out["projections"]:
            if pr["scenario"] != scen:
                continue
            cs = pr["cost"]
            print(f"| {pr['nodes']} | {pr['shards']} | {cs['s3']['class_a_M']:,.0f} | {cs['s3']['class_b_M']:,.0f} | {sum(pr['storage_gb'].values()):,.0f} | "
                  + " | ".join(f"{fmt_money(cs[p]['total_$'])} (ops {fmt_money(cs[p]['class_a_$'] + cs[p]['class_b_$'])})" for p in ("s3", "gcs", "r2")) + " |")


if __name__ == "__main__":
    main()
