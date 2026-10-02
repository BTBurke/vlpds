#!/usr/bin/env python3
"""Object-store op measurement for the cost model (stdlib only).

Runs vlpds against local MinIO with injected S3-like PUT latency, a bulk
population with the real records-per-repo shape (scaled), and phases of
open-loop writes at the real load profile; every --scrape-s it scrapes each
node's /metrics (object-store requests by op/component/client, SlateDB's
own per-component object-store counts, segments, commits, ...) into
raw.jsonl. RESULTS.md / cost_model.py are derived from that file.

    measure.py populate            # 1 node, bulk 1M repos (real/32)
    measure.py run --plan one      # 1 node: idle, avg, peak, burst, avg
    measure.py run --plan three    # 3 nodes: join, idle, avg
    measure.py populate --prefix cost1024 --shards 1024 --total 100000
    measure.py run --plan s1024 --prefix cost1024 --shards 1024 --total 100000 --sim-active 20000
    measure.py cleanup --prefix X  # delete the MinIO prefix + .trash + caches
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.environ.get("BENCH_BIN") or os.path.join(PKG, "target", "agent-tests", "release")
S3 = "http://127.0.0.1:9200"
SCRATCH = os.environ.get("COST_SCRATCH", "/tmp/vlpds-cost")
MINIO_DATA = os.environ.get("BENCH_MINIO_DATA", "")

LABELED = (
    "vlpds_object_store_requests_total",
    "vlpds_object_store_bytes_total",
    "slatedb_object_store_request_count_total",
    "vlpds_cluster_store_requests_total",
    "vlpds_segment_put_attempts_total",
    "vlpds_repo_loads_total",
    "vlpds_retention_deleted_objects_total",
    "vlpds_retention_ticks_total",
    "vlpds_ops_total",
)
SUMS = ("vlpds_", "slatedb_")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


def http(method, url, body=None, timeout=10):
    req = urllib.request.Request(url, data=body, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def scrape(url):
    try:
        _, raw = http("GET", url + "/metrics", timeout=5)
    except Exception:
        return None
    labeled, sums = {}, {}
    for line in raw.decode(errors="replace").splitlines():
        if not line or line[0] == "#":
            continue
        k, _, v = line.rpartition(" ")
        name = k.split("{", 1)[0]
        if name.endswith("_bucket") or not name.startswith(SUMS):
            continue
        try:
            v = float(v)
        except ValueError:
            continue
        if name in LABELED:
            labeled[k] = v
        sums[name] = sums.get(name, 0.0) + v
    return {"labeled": labeled, "sums": sums}


def du(path):
    if not path or not os.path.isdir(path):
        return None
    r = subprocess.run(["du", "-sk", path], capture_output=True, text=True)
    try:
        return int(r.stdout.split()[0]) * 1024
    except (ValueError, IndexError):
        return None


def du_split(prefix):
    d = os.path.join(MINIO_DATA, "vlpds", prefix) if MINIO_DATA else ""
    if not d or not os.path.isdir(d):
        return None
    out = {e: du(os.path.join(d, e)) for e in os.listdir(d)}
    out["total"] = sum(v for v in out.values() if v)
    return out


class Node:
    def __init__(self, a, i):
        self.a, self.i = a, i
        self.name = f"n{i+1}"
        self.port = a.base_port + i
        self.url = f"http://127.0.0.1:{self.port}"
        self.dir = os.path.join(SCRATCH, a.prefix, self.name)
        os.makedirs(self.dir, exist_ok=True)
        self.p = None

    def start(self):
        a = self.a
        args = [os.path.join(BIN, "vlpds"), "--listen", f"127.0.0.1:{self.port}", "--public-url", self.url,
                "--s3-endpoint", S3, "--prefix", a.prefix, "--no-rate-limits", "--dev-mode",
                "--node-id", self.name, "--advertise-url", self.url, "--shards", str(a.shards),
                "--workers", "2", "--io-threads", "4", "--block-cache-mb", "512", "--repo-cache-mb", "2048",
                "--cache-budget-mb", "256", "--cache-dir", os.path.join(self.dir, "cache"),
                "--inject-put-ms", str(a.inject), "--inject-sigma", str(a.sigma),
                "--log-retention", a.log_retention, "--lease-ttl-ms", str(a.lease_ttl_ms)] + a.extra
        self.f = open(os.path.join(self.dir, "server.log"), "ab")
        env = dict(os.environ, RUST_LOG="info,slatedb=warn", VLPDS_INJECT_STATE_MS=a.inject_state)
        self.p = subprocess.Popen(["nice", "-n", "10"] + args, stdout=self.f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
        t = time.time()
        while time.time() - t < 600:
            if self.p.poll() is not None:
                raise RuntimeError(f"{self.name} exited; see {self.dir}/server.log")
            try:
                http("GET", self.url + "/xrpc/_health", timeout=2)
                return self
            except Exception:
                time.sleep(0.5)
        raise RuntimeError(f"{self.name} did not come up")

    def stop(self):
        if self.p and self.p.poll() is None:
            os.killpg(self.p.pid, signal.SIGTERM)
            try:
                self.p.wait(120)
            except subprocess.TimeoutExpired:
                os.killpg(self.p.pid, signal.SIGKILL)
                self.p.wait()


def snap(out, nodes, phase, a, extra=None):
    rec = {"t": time.time(), "phase": phase, "prefix": a.prefix, "shards": a.shards, "nodes": {}}
    for n in nodes:
        rec["nodes"][n.name] = scrape(n.url)
    if extra:
        rec.update(extra)
    with open(out, "a") as f:
        f.write(json.dumps(rec) + "\n")


def loadgen(a, url, rate, secs, offset):
    if rate <= 0:
        return None
    args = ["nice", "-n", "5", os.path.join(BIN, "loadgen"), "--host", url, "--threads", "4", "run",
            "--rate", str(rate), "--duration", str(secs), "--warmup", "0", "--report-secs", "60",
            "--update-pct", "0", "--delete-pct", "2", "--max-inflight", "5000",
            "--sim-total", str(a.total), "--sim-active", str(a.sim_active), "--sim-churn", str(a.sim_churn),
            "--sim-offset", str(offset)]
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=open(os.path.join(SCRATCH, a.prefix, "loadgen.stderr"), "a"), text=True)


PLANS = {
    # (phase, rate commits/s, seconds)
    "one": [("settle", 0, 300), ("idle", 0, 900), ("avg", 345, 1200), ("peak", 440, 900), ("burst", 900, 600), ("avg2", 345, 900)],
    # rerun after a MinIO stall fail-stopped the first "one" run at 15:48 (its idle and the
    # first 14 min of avg are kept)
    "one2": [("settle", 0, 120), ("idle", 0, 300), ("avg", 345, 900), ("peak", 440, 600), ("burst", 900, 600), ("avg2", 345, 600)],
    "three": [("join", 0, 240), ("idle", 0, 720), ("avg", 345, 1200)],
    "s1024b": [("settle", 0, 120), ("idle", 0, 300), ("avg", 345, 900)],
    "s1024": [("settle", 0, 180), ("idle", 0, 420), ("avg", 345, 900)],
    "smoke": [("idle", 0, 30), ("avg", 345, 60)],
    # "Defaults changed" (RESULTS.md): fresh idle, avg, then idle after writes, run once with the
    # binary before the change and once after (BENCH_BIN), 256 shards, 100k repos
    "defaults": [("settle", 0, 120), ("idle", 0, 300), ("avg", 345, 600), ("idle2", 0, 300)],
}


def run(a):
    out = os.path.join(HERE, a.out)
    nodes = [Node(a, i) for i in range(a.nodes)]
    try:
        for n in nodes:
            n.start()
            log(f"{n.name} up at {n.url}")
        offset = a.offset
        for phase, rate, secs in PLANS[a.plan]:
            tag = f"{a.plan}/{phase}"
            log(f"phase {tag}: {rate}/s for {secs}s")
            snap(out, nodes, tag + ":start", a, {"rate": rate, "du": du_split(a.prefix)})
            lg = loadgen(a, nodes[0].url, rate, secs, offset)
            t0 = time.time()
            while time.time() - t0 < secs:
                time.sleep(min(a.scrape_s, max(0.1, secs - (time.time() - t0))))
                snap(out, nodes, tag, a, {"rate": rate})
            res = None
            if lg:
                o, _ = lg.communicate(timeout=300)
                lines = [l for l in o.splitlines() if l.startswith("{")]
                res = json.loads(lines[-1]) if lines else o[-2000:]
                offset += int(secs * a.sim_churn) + a.sim_active
            snap(out, nodes, tag + ":end", a, {"rate": rate, "loadgen": res, "du": du_split(a.prefix)})
    finally:
        for n in nodes:
            n.stop()


def populate(a):
    n = Node(a, 0).start()
    try:
        t = time.time()
        r = subprocess.run(["nice", "-n", "5", os.path.join(BIN, "loadgen"), "--host", n.url, "--threads", "4", "bulk",
                            "--start", "0", "--count", str(a.total), "--concurrency", "8",
                            "--dist", "real", "--dist-scale", "32", "--dist-knee", "2"], capture_output=True, text=True)
        log("bulk:", r.returncode, r.stdout[-1500:], r.stderr[-1500:])
        log(f"populate took {time.time()-t:.0f}s; settling 120 s")
        time.sleep(120)
        with open(os.path.join(HERE, a.out), "a") as f:
            f.write(json.dumps({"t": time.time(), "phase": "populate:end", "prefix": a.prefix, "secs": time.time() - t,
                                "metrics": scrape(n.url), "du": du_split(a.prefix)}) + "\n")
    finally:
        n.stop()


def cleanup(a):
    if MINIO_DATA:
        d = os.path.join(MINIO_DATA, "vlpds", a.prefix)
        shutil.rmtree(d, ignore_errors=True)
        trash = os.path.join(MINIO_DATA, ".minio.sys", "tmp", ".trash")
        if os.path.isdir(trash):
            subprocess.run(f"find '{trash}' -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", shell=True, check=False)
    shutil.rmtree(os.path.join(SCRATCH, a.prefix), ignore_errors=True)
    log("cleaned", a.prefix)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["populate", "run", "cleanup"])
    ap.add_argument("--plan", default="one")
    ap.add_argument("--prefix", default="costmodel")
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--shards", type=int, default=256)
    ap.add_argument("--base-port", type=int, default=7400)
    ap.add_argument("--total", type=int, default=1_000_000)
    ap.add_argument("--sim-active", type=int, default=50_000)
    ap.add_argument("--sim-churn", type=float, default=12.0)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--inject", type=float, default=30.0)
    ap.add_argument("--sigma", type=float, default=0.5)
    ap.add_argument("--inject-state", default="20,30,0.5", help="SlateDB/control-plane latency: read ms, write ms, sigma")
    ap.add_argument("--log-retention", default="20m")
    ap.add_argument("--scrape-s", type=float, default=30.0)
    ap.add_argument("--extra", default="")
    ap.add_argument("--out", default="raw.jsonl")
    # 30 s: a few-second MinIO stall on the shared laptop fail-stopped 10 s leases
    ap.add_argument("--lease-ttl-ms", type=int, default=30000)
    a = ap.parse_args()
    a.extra = a.extra.split() if a.extra else []
    os.makedirs(os.path.join(SCRATCH, a.prefix), exist_ok=True)
    {"populate": populate, "run": run, "cleanup": cleanup}[a.cmd](a)


if __name__ == "__main__":
    main()
