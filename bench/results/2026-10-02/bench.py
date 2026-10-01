#!/usr/bin/env python3
"""Benchmark driver for the 2026-10-02 runs (stdlib only).

Native MinIO on 127.0.0.1:9200 (bucket vlpds), one fresh prefix per run,
release binaries in target/bench/release. Each step writes one JSON line
into the experiment's .jsonl file next to this script.

    python3 bench/results/2026-10-02/bench.py grid <name> <total> <active> <inject_ms|0> <rate,rate,...>
    python3 bench/results/2026-10-02/bench.py hot <name> <inject_ms|0> <hot_rate>
    python3 bench/results/2026-10-02/bench.py cleanup <prefix>
"""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.path.join(PKG, "target", "bench", "release")
VLPDS = os.path.join(BIN, "vlpds")
LOADGEN = os.path.join(BIN, "loadgen")
S3 = "http://127.0.0.1:9200"
SCRATCH = os.environ.get("BENCH_SCRATCH", "/tmp/scratch/bench")
os.makedirs(SCRATCH, exist_ok=True)
ADMIN = "dev-admin-token"
INTERNAL = "dev-internal-token"
ENV = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_DEFAULT_REGION="us-east-1")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def disk_free_gb():
    st = os.statvfs("/")
    return st.f_bavail * st.f_frsize / 1e9


def check_disk(min_gb=150):
    f = disk_free_gb()
    if f < min_gb:
        raise SystemExit(f"disk free {f:.0f} GB < {min_gb} GB, refusing to continue")
    return f


def cleanup_prefix(prefix):
    t = time.time()
    subprocess.run(["aws", "--endpoint-url", S3, "s3", "rm", "--recursive", "--only-show-errors", f"s3://vlpds/{prefix}/"], env=ENV, check=False)
    # native MinIO moves deleted objects to .minio.sys/tmp/.trash and did not
    # purge it during these runs (73 GB piled up): empty it ourselves
    trash = os.path.join(os.path.dirname(SCRATCH), "minio-native", ".minio.sys", "tmp", ".trash")
    if os.path.isdir(trash):
        subprocess.run(f"find '{trash}' -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", shell=True, check=False)
    log(f"deleted s3://vlpds/{prefix}/ in {time.time()-t:.0f}s; disk free {disk_free_gb():.0f} GB")


def http(method, url, body=None, headers=None, timeout=10):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def metrics(url):
    try:
        _, raw = http("GET", url + "/metrics", timeout=5)
    except Exception:
        return {}
    out = {}
    for line in raw.decode().splitlines():
        if not line or line.startswith("#"):
            continue
        k, _, v = line.rpartition(" ")
        try:
            out[k] = float(v)
        except ValueError:
            pass
    return out


def msum(m, prefix):
    return sum(v for k, v in m.items() if k == prefix or k.startswith(prefix + "{"))


class Node:
    def __init__(self, name, prefix, port=2583, inject=0, extra=(), node_id=None, cache=True):
        self.name, self.prefix, self.port = name, prefix, port
        self.url = f"http://127.0.0.1:{port}"
        self.logpath = os.path.join(SCRATCH, f"{name}.server.log")
        self.cache = os.path.join(SCRATCH, f"cache-{name}") if cache else ""
        args = [VLPDS, "--listen", f"127.0.0.1:{port}", "--public-url", self.url, "--s3-endpoint", S3,
                "--prefix", prefix, "--no-rate-limits", "--dev-mode"]
        if self.cache:
            args += ["--cache-dir", self.cache]
        if inject:
            args += ["--inject-put-ms", str(inject)]
        if node_id:
            args += ["--node-id", node_id, "--advertise-url", self.url]
        args += list(extra) + os.environ.get("VLPDS_EXTRA", "").split()
        self.args = args

    def start(self, timeout=180):
        self.f = open(self.logpath, "ab")
        self.p = subprocess.Popen(self.args, stdout=self.f, stderr=subprocess.STDOUT, start_new_session=True)
        t = time.time()
        while time.time() - t < timeout:
            if self.p.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.p.returncode}; see {self.logpath}")
            try:
                http("GET", self.url + "/xrpc/_health", timeout=2)
                if b"vlpds serving" in open(self.logpath, "rb").read():
                    return self
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{self.name} did not come up")

    def stop(self, sig=signal.SIGTERM, timeout=60):
        if self.p.poll() is None:
            self.p.send_signal(sig)
            try:
                self.p.wait(timeout)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
        self.f.close()

    def wipe_cache(self):
        if self.cache:
            shutil.rmtree(self.cache, ignore_errors=True)

    def stat_lines(self, n=4):
        lines = [l for l in open(self.logpath, errors="replace").read().splitlines() if "req/s" in l]
        return [re.sub(r"\x1b\[[0-9;]*m", "", l) for l in lines[-n:]]


class Sampler:
    """Samples %CPU and RSS of pids every `every` s."""

    def __init__(self, pids, every=2.0):
        self.pids, self.every = pids, every
        self.samples = {p: [] for p in pids}
        self._stop = threading.Event()
        self.t = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self._stop.wait(self.every):
            for p in self.pids:
                r = subprocess.run(["ps", "-o", "%cpu=,rss=", "-p", str(p)], capture_output=True, text=True)
                try:
                    cpu, rss = r.stdout.split()
                    self.samples[p].append((float(cpu), int(rss) * 1024))
                except ValueError:
                    pass

    def __enter__(self):
        self.t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self.t.join()

    def summary(self, pid):
        s = self.samples.get(pid) or [(0, 0)]
        cpus = [c for c, _ in s]
        return {"cpu_pct_avg": round(sum(cpus) / len(cpus), 1), "cpu_pct_max": max(cpus), "rss_gb_max": round(max(r for _, r in s) / 1e9, 2), "rss_gb_last": round(s[-1][1] / 1e9, 2)}


RES_RE = re.compile(r"target (\d+)/s .*achieved (\d+)/s ok \| errors (\d+) \| dropped (\d+)")
HIST_RE = re.compile(r"^(\S+)\s+n=(\d+)\s+p50=\s*([\d.]+)ms p90=\s*([\d.]+)ms p99=\s*([\d.]+)ms p99.9=\s*([\d.]+)ms max=\s*([\d.]+)ms")


def parse_loadgen(text):
    out = {}
    m = RES_RE.search(text)
    if m:
        out.update(target=int(m[1]), achieved=int(m[2]), errors=int(m[3]), dropped=int(m[4]))
    for line in text.splitlines():
        h = HIST_RE.match(line)
        if h:
            out[h[1]] = {"n": int(h[2]), "p50": float(h[3]), "p90": float(h[4]), "p99": float(h[5]), "p999": float(h[6]), "max": float(h[7])}
        if line.startswith("first error:"):
            out["first_error"] = line[12:200]
        if line.startswith("firehose events received:"):
            out["firehose_events"] = int(line.split(":")[1])
    return out


METRIC_KEYS = ["vlpds_commits_total", "vlpds_ops_total", "vlpds_segments_total", "vlpds_segment_bytes_total",
               "vlpds_segment_put_attempts_total", "vlpds_segment_put_hedges_total", "vlpds_repo_loads_total",
               "vlpds_repo_evictions_total", "vlpds_cluster_store_requests_total", "vlpds_writes_shed_total",
               "vlpds_requests_forwarded_total", "vlpds_firehose_events_total", "vlpds_commit_requests_sum", "vlpds_commit_requests_count", "vlpds_commit_ops_sum", "vlpds_commit_ops_count"]


def mdelta(a, b):
    return {k.replace("vlpds_", ""): round(msum(b, k) - msum(a, k)) for k in METRIC_KEYS}


def run_loadgen(host, rate, total, active, duration=20, warmup=10, hot=200, churn=None, extra=(), firehose=True, tag="lg"):
    churn = active / 100 if churn is None else churn
    args = [LOADGEN, "--host", host, "--threads", "4", "run", "--rate", str(rate), "--hot-rate", str(hot),
            "--duration", str(duration), "--warmup", str(warmup), "--sim-total", str(total),
            "--sim-active", str(active), "--sim-churn", str(churn)] + list(extra)
    if firehose:
        args.append("--firehose")
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=open(os.path.join(SCRATCH, tag.replace("/", "_") + ".stderr"), "w"), text=True)


def write_jsonl(path, rec):
    with open(path, "a") as f:
        f.write(json.dumps(rec) + "\n")


def bulk(nodes, total, records=5):
    t = time.time()
    procs = [subprocess.Popen([LOADGEN, "--host", n.url, "bulk", "--count", str(total), "--records", str(records),
                               "--batch", "1000", "--concurrency", "16"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
             for n in nodes]
    outs = [p.communicate()[0] for p in procs]
    for p, o in zip(procs, outs):
        if p.returncode != 0:
            raise RuntimeError("bulk failed: " + o[-2000:])
    secs = time.time() - t
    log(f"bulk {total} accounts x{records} in {secs:.0f}s ({total/secs:.0f}/s)")
    return secs


def grid_step(node, out, shape, rate, total, active, inject, duration=20, hot=200):
    check_disk()
    m0 = metrics(node.url)
    lg = run_loadgen(node.url, rate, total, active, duration=duration, hot=hot, tag=f"{shape}-{rate}")
    with Sampler([node.p.pid, lg.pid]) as s:
        text, _ = lg.communicate()
    m1 = metrics(node.url)
    r = parse_loadgen(text)
    rec = {"shape": shape, "total": total, "active": active, "inject_put_ms": inject, "rate": rate, "hot_rate": hot,
           "duration_s": duration, **r, "server": s.summary(node.p.pid), "loadgen": s.summary(lg.pid),
           "metrics_delta": mdelta(m0, m1), "server_stats_tail": node.stat_lines(2)[-1:],
           "jemalloc": {k.split('"')[1]: v for k, v in m1.items() if k.startswith("vlpds_jemalloc_bytes")}}
    write_jsonl(out, rec)
    a = r.get("all", {})
    log(f"{shape} rate={rate}: achieved {r.get('achieved')} err {r.get('errors')} drop {r.get('dropped')} p50 {a.get('p50')} p99 {a.get('p99')} p99.9 {a.get('p999')} | srv cpu {rec['server']['cpu_pct_avg']}% rss {rec['server']['rss_gb_max']}GB")
    return rec


def saturated(rec):
    a = rec.get("all") or {}
    return (rec.get("achieved", 0) < 0.93 * rec["rate"] or rec.get("errors", 0) > 0.01 * rec["rate"] * rec["duration_s"]
            or a.get("p99", 1e9) > 2000)


def cmd_grid(name, total, active, inject, rates, prefix=None, keep=False, duration=20):
    """Fresh prefix, bulk `total` accounts, then stair-step rates until saturation."""
    out = os.path.join(HERE, os.environ.get("OUT", f"grid-{name}.jsonl"))
    prefix = prefix or f"bench-{name}"
    check_disk()
    node = Node(name, prefix, inject=inject).start()
    try:
        if not keep:
            bulk([node], total)
        for rate in rates:
            rec = grid_step(node, out, name, rate, total, active, inject, duration=duration)
            if saturated(rec):
                log(f"{name}: saturated at {rate}/s")
                break
            time.sleep(3)
    finally:
        node.stop()
        node.wipe_cache()
    return node


def cmd_suite(total, actives, injects, rates, duration=20):
    """One prefix per total: bulk once, then for each inject mode a server
    restart and a stair per active window."""
    prefix = f"bench-grid-{total}"
    out = os.path.join(HERE, os.environ.get("OUT", "grid.jsonl"))
    check_disk()
    try:
        for k, inject in enumerate(injects):
            name = f"grid-{total}-inj{int(inject)}"
            node = Node(name, prefix, inject=inject)
            node.cache = os.path.join(SCRATCH, f"cache-grid-{total}")
            node.args[node.args.index("--cache-dir") + 1] = node.cache
            t = time.time()
            node.start(timeout=600)
            log(f"{name}: up in {time.time()-t:.1f}s")
            try:
                if k == 0:
                    bulk([node], total)
                for active in actives:
                    if active > total:
                        continue
                    shape = f"{total}/{active}/inj{int(inject)}"
                    for rate in rates:
                        rec = grid_step(node, out, shape, rate, total, active, inject, duration=duration)
                        if saturated(rec):
                            log(f"{shape}: saturated at {rate}/s")
                            break
                        time.sleep(3)
            finally:
                node.stop()
    finally:
        shutil.rmtree(os.path.join(SCRATCH, f"cache-grid-{total}"), ignore_errors=True)
        cleanup_prefix(prefix)


WIN_RE = re.compile(r"\[\s*(\d+)s\] ok/s\s+(\d+) err (\d+) dropped (\d+) inflight (\d+) \| p50 ([\d.]+)ms p99 ([\d.]+)ms max (\d+)ms")


def windows(stderr_path):
    out = []
    for line in open(stderr_path, errors="replace"):
        m = WIN_RE.search(line)
        if m:
            out.append({"t": int(m[1]), "ok_s": int(m[2]), "err": int(m[3]), "dropped": int(m[4]), "p50": float(m[6]), "p99": float(m[7]), "max": int(m[8])})
    return out


def cluster_up(prefix, n=3, inject=0, base=7101, extra=()):
    nodes = []
    per = ["--workers", "3", "--io-threads", "3", "--lease-ttl-ms", "10000"] + list(extra)
    for i in range(n):
        nd = Node(f"c{n}-n{i+1}", prefix, port=base + i, inject=inject, extra=per, node_id=f"n{i+1}")
        nodes.append(nd)
    for nd in nodes:
        nd.start(timeout=300)
    # wait for shard ownership to converge
    t = time.time()
    wait_converged(nodes)
    return nodes


def wait_converged(nodes, timeout=120):
    """Every node owns something and every node's routing table names a
    live owner for all shards."""
    t = time.time()
    owned = []
    while time.time() - t < timeout:
        try:
            owned = [msum(metrics(nd.url), "vlpds_owned_partitions") for nd in nodes]
            tables = [json.loads(http("GET", nd.url + "/internal/v1/cluster", headers={"x-vlpds-internal": INTERNAL})[1])["table"] for nd in nodes]
            if sum(owned) >= 256 and min(owned) > 0 and all(all(x for x in tb) for tb in tables):
                log(f"cluster converged {owned} in {time.time()-t:.1f}s")
                return time.time() - t
        except Exception:
            pass
        time.sleep(0.5)
    log(f"cluster did not converge: {owned}")
    return None


def cluster_step(nodes, out, shape, rate, total, active, inject, duration=20, event=None):
    """One loadgen per node at rate/len(nodes) (requests not routed: ~2/3 forwarded).
    The hot repo + firehose consumer run on the first loadgen only."""
    check_disk()
    m0 = [metrics(n.url) for n in nodes]
    lgs = []
    for i, nd in enumerate(nodes):
        lgs.append(run_loadgen(nd.url, rate / len(nodes), total, active, duration=duration, hot=200 if i == 0 else 0,
                               firehose=(i == 0), tag=f"{shape}-{rate}-lg{i}".replace("/", "_")))
    ev = None
    with Sampler([n.p.pid for n in nodes] + [lg.pid for lg in lgs]) as s:
        if event:
            ev = event(nodes)
        texts = [lg.communicate()[0] for lg in lgs]
    m1 = [metrics(n.url) for n in nodes]
    parsed = [parse_loadgen(t) for t in texts]
    agg = {"achieved": sum(p.get("achieved", 0) for p in parsed), "errors": sum(p.get("errors", 0) for p in parsed),
           "dropped": sum(p.get("dropped", 0) for p in parsed)}
    rec = {"shape": shape, "nodes": len(nodes), "total": total, "active": active, "inject_put_ms": inject, "rate": rate,
           "duration_s": duration, **agg, "per_loadgen": parsed,
           "servers": [s.summary(n.p.pid) for n in nodes if n.p.poll() is None or True],
           "metrics_delta": [mdelta(a, b) for a, b in zip(m0, m1)],
           "windows": [windows(os.path.join(SCRATCH, f"{shape}-{rate}-lg{i}".replace("/", "_") + ".stderr")) for i in range(len(lgs))],
           "event": ev}
    # worst-of percentiles across loadgens for the headline
    rec["all_p50_max"] = max((p.get("all") or {}).get("p50", 0) for p in parsed)
    rec["all_p99_max"] = max((p.get("all") or {}).get("p99", 0) for p in parsed)
    rec["all_p999_max"] = max((p.get("all") or {}).get("p999", 0) for p in parsed)
    write_jsonl(out, rec)
    log(f"{shape} rate={rate}: achieved {agg['achieved']} err {agg['errors']} drop {agg['dropped']} p50<= {rec['all_p50_max']} p99<= {rec['all_p99_max']} p99.9<= {rec['all_p999_max']} | cpu {[x['cpu_pct_avg'] for x in rec['servers']]}")
    return rec


def cmd_cluster(total, active, inject, rates, duration=20, failover_rate=None):
    prefix = f"bench-cluster-{total}"
    out = os.path.join(HERE, "cluster.jsonl")
    nodes = cluster_up(prefix, 3, inject)
    try:
        bulk(nodes, total)
        shape = f"3n/{total}/{active}/inj{int(inject)}"
        for rate in rates:
            rec = cluster_step(nodes, out, shape, rate, total, active, inject, duration=duration)
            if rec["achieved"] < 0.93 * (rate + 200) or rec["errors"] > 0.01 * rate * duration:
                log(f"{shape}: saturated at {rate}")
                break
            time.sleep(3)
        if failover_rate:
            def kill_and_rejoin(nodes):
                ev = {}
                time.sleep(20)  # warmup 10 s + 10 s into the measured window
                victim = nodes[2]
                ev["kill_t"] = time.time()
                victim.p.send_signal(signal.SIGKILL)
                victim.p.wait()
                victim.f.close()
                time.sleep(15)
                ev["restart_t"] = time.time()
                victim.start(timeout=120)
                ev["rejoined_t"] = time.time()
                return {"killed": victim.name, "kill_at_s": 20, "restart_at_s": round(ev["restart_t"] - ev["kill_t"] + 20, 1),
                        "serving_at_s": round(ev["rejoined_t"] - ev["kill_t"] + 20, 1)}
            cluster_step(nodes, out, shape + "/failover", failover_rate, total, active, inject, duration=60, event=kill_and_rejoin)
            def graceful(nodes):
                time.sleep(20)
                v = nodes[1]
                t = time.time()
                v.stop()
                stopped = time.time() - t
                time.sleep(10)
                v.start(timeout=120)
                return {"sigterm": v.name, "at_s": 20, "stop_took_s": round(stopped, 1), "serving_at_s": round(time.time() - t + 20, 1)}
            cluster_step(nodes, out, shape + "/sigterm", failover_rate, total, active, inject, duration=60, event=graceful)
    finally:
        for n in nodes:
            n.stop()
            n.wipe_cache()
        cleanup_prefix(prefix)


def fanout(url, subs, seconds, cursor=None, threads=8):
    args = [LOADGEN, "--host", url, "--threads", str(threads), "fanout", "--subscribers", str(subs), "--seconds", str(seconds)]
    jo = os.path.join(SCRATCH, "fanout.json")
    args += ["--json-out", jo]
    if cursor is not None:
        args += ["--cursor", str(cursor)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p, jo


def cmd_firehose(inject=0):
    out = os.path.join(HERE, "firehose.jsonl")
    prefix = "bench-firehose"
    node = Node("firehose", prefix, inject=inject, extra=["--firehose-ring-mb", "64"]).start()
    try:
        bulk([node], 100000)
        # fan-out: N subscribers at a fixed write rate
        for rate, subs in [(2000, 1), (2000, 10), (2000, 100), (2000, 1000), (10000, 1), (10000, 10), (10000, 100), (10000, 1000), (50000, 1), (50000, 10)]:
            check_disk()
            lg = run_loadgen(node.url, rate, 100000, 50000, duration=25, warmup=5, hot=0, firehose=False, tag="fh-load")
            time.sleep(7)
            fo, jo = fanout(node.url, subs, 20)
            with Sampler([node.p.pid, fo.pid]) as s:
                text, _ = fo.communicate()
                lgt, _ = lg.communicate()
            r = json.load(open(jo)) if os.path.exists(jo) else {"raw": text[-500:]}
            w = parse_loadgen(lgt)
            rec = {"kind": "fanout", "write_rate": rate, "achieved_writes": w.get("achieved"), "write_p99": (w.get("all") or {}).get("p99"),
                   **r, "server": s.summary(node.p.pid), "subscriber_proc": s.summary(fo.pid)}
            write_jsonl(out, rec)
            log(f"fanout writes {rate}/s subs {subs}: {text.strip()[-330:]} | writes ok {w.get('achieved')} p99 {(w.get('all') or {}).get('p99')} | srv cpu {rec['server']['cpu_pct_avg']}")
            if os.path.exists(jo):
                os.remove(jo)
        # cursor backfill: ring is 64 MB, so cursor 1 starts in S3
        total = msum(metrics(node.url), "vlpds_firehose_events_total")
        for subs in (1, 4):
            fo, jo = fanout(node.url, subs, 10, cursor=1)
            with Sampler([node.p.pid]) as s:
                text, _ = fo.communicate()
            r = json.load(open(jo)) if os.path.exists(jo) else {"raw": text[-500:]}
            rec = {"kind": "backfill", "events_in_log": total, **r, "server": s.summary(node.p.pid)}
            write_jsonl(out, rec)
            log(f"backfill subs {subs} (log has {total:.0f} events): {text.strip()[-330:]}")
    finally:
        node.stop()
        node.wipe_cache()
        cleanup_prefix(prefix)


def cmd_methods(inject=0, only=""):
    out = os.path.join(HERE, "methods.jsonl")
    prefix = f"bench-methods-{int(inject)}"
    node = Node(f"methods-{int(inject)}", prefix, inject=inject).start()
    acc = os.path.join(SCRATCH, f"methods-accounts-{int(inject)}.json")
    try:
        t = time.time()
        r = subprocess.run([LOADGEN, "--host", node.url, "--accounts-file", acc, "setup", "--accounts", "2000", "--records", "100",
                            "--prefix", f"m{int(inject)}x"], capture_output=True, text=True)
        log(f"setup: {r.stderr.strip().splitlines()[-1] if r.stderr.strip() else r.returncode} ({time.time()-t:.0f}s)")
        jo = os.path.join(SCRATCH, "methods.json")
        args = [LOADGEN, "--host", node.url, "--accounts-file", acc, "--threads", "6", "methods", "--concurrency", "64", "--seconds", "10", "--json-out", jo]
        if only:
            args += ["--only", only]
        with Sampler([node.p.pid]) as s:
            r = subprocess.run(args, capture_output=True, text=True)
        print(r.stdout, flush=True)
        for line in open(jo):
            rec = json.loads(line)
            rec["inject_put_ms"] = inject
            write_jsonl(out, rec)
    finally:
        node.stop()
        node.wipe_cache()
        cleanup_prefix(prefix)


def cmd_sweep(sizes, extra=()):
    """One server; one fresh repo per size, filled via applyWrites (200 creates
    per call, 16 in flight), then the read methods + getRepo export."""
    out = os.path.join(HERE, "sweep.jsonl")
    prefix = "bench-sweep"
    node = Node("sweep", prefix, extra=list(extra)).start()
    try:
        for size in sizes:
            check_disk()
            jo = os.path.join(SCRATCH, f"sweep-{size}.jsonl")
            with Sampler([node.p.pid], every=5) as s:
                r = subprocess.run([LOADGEN, "--host", node.url, "--threads", "6", "sweep", "--sizes", str(size), "--concurrency", "16",
                                    "--seconds", "10", "--fill-concurrency", "16", "--json-out", jo], capture_output=True, text=True)
            print(r.stdout, flush=True)
            print("\n".join(l for l in r.stderr.splitlines() if "==" in l or "blobs" in l or "rror" in l), flush=True)
            for line in open(jo):
                rec = json.loads(line)
                rec["server_rss_gb_max"] = s.summary(node.p.pid)["rss_gb_max"]
                write_jsonl(out, rec)
            log(f"size {size} done; server {s.summary(node.p.pid)}")
    finally:
        node.stop()
        node.wipe_cache()
        cleanup_prefix(prefix)


def cmd_proxy(actives, concs, total=1000000, body=2048, extra=()):
    out = os.path.join(HERE, "proxy.jsonl")
    prefix = "bench-proxy"
    stub = subprocess.Popen([LOADGEN, "--threads", "4", "stub-appview", "--listen", "127.0.0.1:2700", "--body-bytes", str(body)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    node = Node("proxy", prefix, extra=["--appview", "http://127.0.0.1:2700,did:web:stub.test"] + list(extra)).start()
    try:
        bulk([node], total)
        for active in actives:
            for conc in concs:
                jo = os.path.join(SCRATCH, "proxy.json")
                m0 = metrics(node.url)
                with Sampler([node.p.pid, stub.pid]) as s:
                    lg = subprocess.Popen([LOADGEN, "--host", node.url, "--threads", "6", "proxy", "--active", str(active), "--concurrency", str(conc),
                                           "--seconds", "15", "--json-out", jo], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    s.pids.append(lg.pid); s.samples[lg.pid] = []
                    text, _ = lg.communicate()
                m1 = metrics(node.url)
                r = json.load(open(jo))
                cache = {k.split('"')[1]: msum(m1, k) - msum(m0, k) for k in m1 if k.startswith("vlpds_proxy_cache_total")}
                rec = {**r, "body_bytes": body, "server": s.summary(node.p.pid), "stub": s.summary(stub.pid), "client": s.summary(lg.pid),
                       "proxy_cache": cache, "extra": list(extra), "upstream_h2c": os.environ.get("VLPDS_PROXY_H2C") == "1"}
                write_jsonl(out, rec)
                log(f"proxy active={active} conc={conc}: {r['req_per_s']:.0f} req/s p50 {r['p50_ms']:.2f} p99 {r['p99_ms']:.2f} err {r['errors']} | cpu srv {rec['server']['cpu_pct_avg']} stub {rec['stub']['cpu_pct_avg']} client {rec['client']['cpu_pct_avg']} | {cache}")
    finally:
        node.stop()
        node.wipe_cache()
        stub.kill()
        cleanup_prefix(prefix)


def jem(m):
    return {k.split('"')[1]: round(v / 1e9, 3) for k, v in m.items() if k.startswith("vlpds_jemalloc_bytes")}


def snapshot(node, label):
    m = metrics(node.url)
    r = subprocess.run(["ps", "-o", "rss=,%cpu=", "-p", str(node.p.pid)], capture_output=True, text=True).stdout.split()
    return {"label": label, "rss_gb": round(int(r[0]) * 1024 / 1e9, 2), "jemalloc_gb": jem(m),
            "cached_repos": msum(m, "vlpds_cached_repos"), "firehose_ring_gb": round(msum(m, "vlpds_firehose_ring_bytes") / 1e9, 3),
            "live_ring_gb": round(msum(m, "vlpds_log_live_ring_bytes") / 1e9, 3)}


def cmd_resource(total=10000000, active=50000, rate=50000, inject=25):
    out = os.path.join(HERE, "resource.json")
    prefix = "bench-resource"
    res = {"total": total, "active": active, "rate": rate, "inject_put_ms": inject, "runs": []}
    for k, cpw in enumerate([50000, 6250]):
        node = Node(f"resource-{cpw}", prefix, inject=inject, extra=["--cache-per-worker", str(cpw)])
        node.cache = os.path.join(SCRATCH, "cache-resource")
        node.args[node.args.index("--cache-dir") + 1] = node.cache
        node.start(timeout=600)
        run = {"cache_per_worker": cpw, "snapshots": [snapshot(node, "started")]}
        try:
            if k == 0:
                bulk([node], total)
                run["snapshots"].append(snapshot(node, "after bulk"))
            time.sleep(15)
            run["snapshots"].append(snapshot(node, "idle"))
            out_g = os.path.join(HERE, "resource-grid.jsonl")
            rec = grid_step(node, out_g, f"resource cpw={cpw}", rate, total, active, inject, duration=60)
            run["load"] = {k2: rec.get(k2) for k2 in ("achieved", "errors", "all", "server", "jemalloc")}
            run["snapshots"].append(snapshot(node, "after 70 s load"))
        finally:
            node.stop()
        res["runs"].append(run)
        json.dump(res, open(out, "w"), indent=1)
        log(json.dumps(run["snapshots"]))
    shutil.rmtree(os.path.join(SCRATCH, "cache-resource"), ignore_errors=True)
    cleanup_prefix(prefix)


def cmd_hot(hot_rates, injects):
    """Single hot repo, no fleet: latency and coalescing (requests/commit)."""
    out = os.path.join(HERE, "hot.jsonl")
    for inject in injects:
        prefix = f"bench-hot-{int(inject)}"
        node = Node(f"hot-{int(inject)}", prefix, inject=inject).start()
        try:
            bulk([node], 1000)
            for hr in hot_rates:
                m0 = metrics(node.url)
                lg = run_loadgen(node.url, 0, 1000, 1000, duration=20, hot=hr, tag=f"hot-{int(inject)}-{hr}")
                with Sampler([node.p.pid]) as s:
                    text, _ = lg.communicate()
                m1 = metrics(node.url)
                r = parse_loadgen(text)
                d = mdelta(m0, m1)
                rec = {"inject_put_ms": inject, "hot_rate": hr, **r, "server": s.summary(node.p.pid), "metrics_delta": d,
                       "requests_per_commit": round(d["commit_requests_sum"] / max(1, d["commit_requests_count"]), 2),
                       "ops_per_commit": round(d["commit_ops_sum"] / max(1, d["commit_ops_count"]), 2),
                       "commits_per_s": round(d["commits_total"] / 30)}
                write_jsonl(out, rec)
                h = r.get("hot-repo") or {}
                log(f"hot {hr}/s inj{inject}: achieved {r.get('achieved')} err {r.get('errors')} p50 {h.get('p50')} p99 {h.get('p99')} p99.9 {h.get('p999')} | req/commit {rec['requests_per_commit']} commits/s {rec['commits_per_s']}")
        finally:
            node.stop()
            node.wipe_cache()
            cleanup_prefix(prefix)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "hot":
        cmd_hot([int(x) for x in sys.argv[2].split(",")], [float(x) for x in sys.argv[3].split(",")])
    elif cmd == "resource":
        cmd_resource()
    elif cmd == "proxy":
        cmd_proxy([int(x) for x in sys.argv[2].split(",")], [int(x) for x in sys.argv[3].split(",")],
                  extra=os.environ.get("VLPDS_EXTRA_PROXY", "").split())
    elif cmd == "sweep":
        cmd_sweep([int(x) for x in sys.argv[2].split(",")], os.environ.get("VLPDS_EXTRA_SWEEP", "").split())
    elif cmd == "methods":
        cmd_methods(float(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else "")
    elif cmd == "firehose":
        cmd_firehose(float(sys.argv[2]) if len(sys.argv) > 2 else 0)
    elif cmd == "suite":
        total = int(sys.argv[2])
        actives = [int(x) for x in sys.argv[3].split(",")]
        injects = [float(x) for x in sys.argv[4].split(",")]
        rates = [int(x) for x in sys.argv[5].split(",")]
        cmd_suite(total, actives, injects, rates, duration=int(os.environ.get("DURATION", "20")))
    elif cmd == "cluster":
        fr = os.environ.get("FAILOVER_RATE")
        cmd_cluster(int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), [int(x) for x in sys.argv[5].split(",")],
                    duration=int(os.environ.get("DURATION", "20")), failover_rate=int(fr) if fr else None)
    elif cmd == "grid":
        name, total, active, inject, rates = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
        keep = os.environ.get("KEEP_PREFIX") == "1"
        prefix = os.environ.get("PREFIX")
        cmd_grid(name, total, active, inject, [int(x) for x in rates.split(",")], prefix=prefix, keep=keep,
                 duration=int(os.environ.get("DURATION", "20")))
        if os.environ.get("NO_CLEANUP") != "1":
            cleanup_prefix(prefix or f"bench-{name}")
    elif cmd == "cleanup":
        cleanup_prefix(sys.argv[2])
    else:
        raise SystemExit(__doc__)
