#!/usr/bin/env python3
"""Single-node bulk repro: start vlpds against local MinIO, bulk-create in
chunks, scrape /metrics around each chunk; one JSON line per chunk.

  repro.py <bindir> <name> --total N --chunk C [--block-cache-mb M] [--extra '...']
"""
import argparse, json, os, re, subprocess, sys, time, urllib.request, shutil

S = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("bindir")
ap.add_argument("name")
ap.add_argument("--total", type=int, default=2_000_000)
ap.add_argument("--chunk", type=int, default=250_000)
ap.add_argument("--start", type=int, default=0)
ap.add_argument("--block-cache-mb", type=int, default=64)
ap.add_argument("--port", type=int, default=2790)
ap.add_argument("--workers", type=int, default=3)
ap.add_argument("--io-threads", type=int, default=6)
ap.add_argument("--concurrency", type=int, default=4)
ap.add_argument("--shards", type=int, default=64)
ap.add_argument("--extra", default="")
ap.add_argument("--keep", action="store_true", help="leave the prefix (resume later with --start)")
ap.add_argument("--prefix", default="")
ap.add_argument("--probe", action="store_true", help="open an existing prefix, report the warmed meta cache, exit")
a = ap.parse_args()

prefix = a.prefix or f"filtercache-{a.name}"
d = os.path.join(S, "runs", a.name)
os.makedirs(d, exist_ok=True)
url = f"http://127.0.0.1:{a.port}"
args = [os.path.join(a.bindir, "vlpds"), "--listen", f"127.0.0.1:{a.port}", "--public-url", url,
        "--s3-endpoint", "http://127.0.0.1:9200", "--prefix", prefix, "--no-rate-limits", "--dev-mode",
        "--node-id", "n1", "--peer-listen", f"127.0.0.1:{a.port + 100}", "--advertise-url", f"https://127.0.0.1:{a.port + 100}",
        "--peer-tls-dir", os.path.join(d, "peer-tls"), "--workers", str(a.workers), "--io-threads", str(a.io_threads),
        "--block-cache-mb", str(a.block_cache_mb), "--repo-cache-mb", "512", "--cache-budget-mb", "512",
        "--log-retention", "3m", "--slatedb-checkpoint-lifetime", "2m", "--slatedb-gc-min-age", "2m",
        "--lease-ttl-ms", "30000", "--shards", str(a.shards)] + a.extra.split()
env = dict(os.environ, RUST_LOG="info,slatedb=warn")
log = open(os.path.join(d, "server.log"), "ab")
p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)


def metrics():
    txt = urllib.request.urlopen(url + "/metrics", timeout=10).read().decode()
    out = {}
    for line in txt.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(r'^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+(\S+)', line)
        if not m:
            continue
        out[m[1] + (m[2] or "")] = float(m[3])
    return out


def pick(m, name, **labels):
    tot = 0.0
    for k, v in m.items():
        if not (k == name or k.startswith(name + "{")):
            continue
        if all(f'{lk}="{lv}"' in k for lk, lv in labels.items()):
            tot += v
    return tot


t = time.time()
while True:
    if p.poll() is not None:
        sys.exit(f"node exited; see {d}/server.log")
    try:
        urllib.request.urlopen(url + "/xrpc/_health", timeout=2)
        if b"vlpds serving" in open(os.path.join(d, "server.log"), "rb").read():
            break
    except Exception:
        pass
    if time.time() - t > 300:
        sys.exit("node did not come up")
    time.sleep(0.5)

out = open(os.path.join(d, "chunks.jsonl"), "a")
if a.probe:
    # restart on an existing prefix: open-time warm loads every SST's filter + index
    t = time.time()
    while b"shards warmed" not in open(os.path.join(d, "server.log"), "rb").read() and time.time() - t < 120:
        time.sleep(0.5)
    time.sleep(2)
    m = metrics()
    print(json.dumps({"probe": a.name, **{k: v for k, v in m.items() if k.startswith(("vlpds_meta_cache", "vlpds_sst_meta", "vlpds_shard_warm"))}}))
    p.terminate()
    p.wait(60)
    sys.exit(0)
try:
    s = a.start
    while s < a.total:
        c = min(a.chunk, a.total - s)
        m0 = metrics()
        t0 = time.time()
        r = subprocess.run([os.path.join(a.bindir, "loadgen"), "--host", url, "--threads", "4", "bulk", "--start", str(s), "--count", str(c),
                            "--batch", "1000", "--concurrency", str(a.concurrency), "--dist", "real", "--dist-scale", "128", "--dist-knee", "2",
                            "--dist-group", "1", "--dist-seed", "1"], capture_output=True, text=True)
        secs = time.time() - t0
        m1 = metrics()
        if r.returncode:
            sys.exit(f"bulk failed: {r.stderr[-2000:]}")
        res = json.loads(r.stdout.strip().splitlines()[-1])
        dl = lambda n, **l: pick(m1, n, **l) - pick(m0, n, **l)
        rec = {
            "name": a.name, "at": s + c, "secs": round(secs, 1), "acct_s": round(c / secs),
            "created": res.get("created"), "records": res.get("records"),
            "sst_get_mb": round(dl("vlpds_object_store_bytes_total", dir="down", component="state_sst") / 1e6, 1),
            "sst_gets": dl("vlpds_object_store_requests_total", component="state_sst", op="get_range") + dl("vlpds_object_store_requests_total", component="state_sst", op="get"),
            "sst_up_mb": round(dl("vlpds_object_store_bytes_total", dir="up", component="state_sst") / 1e6, 1),
            "commits": dl("vlpds_commits_total"),
            "filter_neg": dl("slatedb_db_sst_filter_negative_count_total"),
            "cache": {k.split("{", 1)[1].rstrip("}"): v - m0.get(k, 0) for k, v in m1.items() if k.startswith("slatedb_db_cache_access_count_total{")},
            "meta": {k: v for k, v in m1.items() if k.startswith("vlpds_meta_cache") or k.startswith("vlpds_sst_meta")},
            "ssts": m1.get("slatedb_db_sst_count"), "srs": m1.get("slatedb_db_sorted_run_count"), "l0": m1.get("slatedb_db_l0_sst_count"),
            "cpu_s": round(dl("vlpds_process_cpu_seconds_total"), 1),
        }
        rec["sst_get_kb_per_acct"] = round(rec["sst_get_mb"] * 1e3 / c, 2)
        print(json.dumps(rec), flush=True)
        out.write(json.dumps(rec) + "\n")
        out.flush()
        s += c
finally:
    p.terminate()
    try:
        p.wait(60)
    except Exception:
        p.kill()
    if not a.keep:
        shutil.rmtree(os.path.join(S, "..", "minio-native", "vlpds", prefix), ignore_errors=True)
