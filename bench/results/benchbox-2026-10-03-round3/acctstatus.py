#!/usr/bin/env python3
"""checkAccountStatus latency on one repo of N records (round 3, item 5).

One node (bench.py's Node, same flags as the sweep), one fresh repo filled by
`loadgen sweep --reuse --fill-only` (sw<N>.vlpds.test, password hunter2), then
createSession and checkAccountStatus: CALLS sequential calls (latency each),
then CONC concurrent callers for SECS s (throughput). Records go to
$BENCH_OUT_DIR/$OUT (acctstatus.jsonl). Run through runner.sh like bench.py:
    DRIVER=acctstatus.py runner.sh <outdir> 1000000
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
CALLS = int(os.environ.get("CALLS", "20"))
CONC = int(os.environ.get("CONC", "8"))
SECS = float(os.environ.get("SECS", "10"))
OUT = os.path.join(bench.OUTDIR, os.environ.get("OUT", "acctstatus.jsonl"))


def req(method, url, body=None, token=None, timeout=120):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    r = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers=h, method=method)
    with urllib.request.urlopen(r, timeout=timeout) as f:
        return json.loads(f.read() or b"{}")


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 3) if xs else None


def main():
    prefix = "bench-acctstatus"
    node = bench.Node("acctstatus", prefix).start()
    try:
        t = time.time()
        r = subprocess.run([bench.LOADGEN, "--host", node.url, "--threads", "6", "sweep", "--reuse", "--fill-only",
                            "--sizes", str(N), "--fill-concurrency", "16"], capture_output=True, text=True)
        if r.returncode:
            raise SystemExit("fill failed: " + (r.stdout + r.stderr)[-2000:])
        fill_s = time.time() - t
        bench.log(f"filled {N} records in {fill_s:.0f}s")
        s = req("POST", node.url + "/xrpc/com.atproto.server.createSession", {"identifier": f"sw{N}.vlpds.test", "password": "hunter2"})
        tok = s["accessJwt"]
        url = node.url + "/xrpc/com.atproto.server.checkAccountStatus"
        first = req("GET", url, token=tok)
        bench.log(f"checkAccountStatus: {json.dumps(first)[:400]}")
        lat = []
        for _ in range(CALLS):
            t0 = time.perf_counter()
            req("GET", url, token=tok)
            lat.append((time.perf_counter() - t0) * 1000)
        n = [0]
        errs = [0]
        lock = threading.Lock()
        stop = time.time() + SECS

        def worker():
            while time.time() < stop:
                try:
                    req("GET", url, token=tok)
                    with lock:
                        n[0] += 1
                except Exception:
                    with lock:
                        errs[0] += 1
        th = [threading.Thread(target=worker) for _ in range(CONC)]
        t0 = time.time()
        for x in th:
            x.start()
        for x in th:
            x.join()
        rec = {"commit": os.environ.get("BENCH_COMMIT", ""), "records": N, "fill_s": round(fill_s, 1), "calls": CALLS,
               "p50_ms": pct(lat, 50), "p99_ms": pct(lat, 99), "min_ms": round(min(lat), 3), "max_ms": round(max(lat), 3),
               "conc": CONC, "ops_per_s": round(n[0] / (time.time() - t0), 1), "errors": errs[0],
               "response": {k: first.get(k) for k in ("repoBlocks", "indexedRecords", "privateStateValues", "expectedBlobs", "importedBlobs", "repoCommit", "activated", "validDid")}}
        bench.write_jsonl(OUT, rec)
        bench.log(json.dumps(rec))
    finally:
        node.stop()
        node.wipe_cache()
        bench.cleanup_prefix(prefix)


if __name__ == "__main__":
    main()
