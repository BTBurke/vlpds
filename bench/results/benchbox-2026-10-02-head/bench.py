#!/usr/bin/env python3
"""Benchmark driver (copy of ../2026-10-02/bench.py for the benchbox head campaign:
64-shard-aware cluster convergence, SHARDS env, CPU/commit and commit-build
metrics per step, `coldload` for partial-MST cold writes). Stdlib only.

Native MinIO on 127.0.0.1:9200 (bucket vlpds), one fresh prefix per run,
release binaries in target/bench/release. Each step writes one JSON line
into the experiment's .jsonl file next to this script.

    python3 bench/results/2026-10-02/bench.py grid <name> <total> <active> <inject_ms|0> <rate,rate,...>
    python3 bench/results/2026-10-02/bench.py hot <name> <inject_ms|0> <hot_rate>
    python3 bench/results/2026-10-02/bench.py proxy <active,active,...> <concurrency,...>
    python3 bench/results/2026-10-02/bench.py cleanup <prefix>

proxy env: PROXY_TOTAL (bulk accounts, 1000000), PROXY_IO_THREADS (cores),
PROXY_STUB_THREADS / PROXY_LG_THREADS (cores/4, min 4 / 6), PROXY_CONNECTIONS
(loadgen h2 connections, 64), PROXY_SECS (15), PROFILE_PROXY ("active:conc,..."),
PROXY_STUB_REV (the stub's atproto-repo-rev, e.g. 7zzzzzzzzzzzz; read-after-write lookups).
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
# BENCH_BIN: another build's release dir (A/B against a baseline binary)
BIN = os.environ.get("BENCH_BIN") or os.path.join(PKG, "target", "bench", "release")
VLPDS = os.path.join(BIN, "vlpds")
LOADGEN = os.path.join(BIN, "loadgen")
S3 = "http://127.0.0.1:9200"
SCRATCH = os.environ.get("BENCH_SCRATCH", "/tmp/scratch/bench")
os.makedirs(SCRATCH, exist_ok=True)
ADMIN = "dev-admin-token"
INTERNAL = "dev-internal-token"
# Remote hosts (bench/benchbox): BENCH_OUT_DIR = where the JSONL goes (default:
# next to this script); BENCH_MINIO_DATA = MinIO's data dir on local disk, so
# cleanup deletes the prefix directory directly (no aws CLI needed) and empties
# that MinIO's .trash.
OUTDIR = os.environ.get("BENCH_OUT_DIR") or HERE
os.makedirs(OUTDIR, exist_ok=True)
MINIO_DATA = os.environ.get("BENCH_MINIO_DATA", "")
ENV = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_DEFAULT_REGION="us-east-1")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def disk_free_gb():
    st = os.statvfs("/")
    return st.f_bavail * st.f_frsize / 1e9


def check_disk(min_gb=None):
    min_gb = min_gb or float(os.environ.get("MIN_FREE_GB", "150"))
    f = disk_free_gb()
    if f < min_gb:
        raise SystemExit(f"disk free {f:.0f} GB < {min_gb} GB, refusing to continue")
    return f


def cleanup_prefix(prefix):
    t = time.time()
    if MINIO_DATA:
        if prefix and "/" not in prefix.strip("/") and ".." not in prefix:
            shutil.rmtree(os.path.join(MINIO_DATA, "vlpds", prefix.strip("/")), ignore_errors=True)
        trash = os.path.join(MINIO_DATA, ".minio.sys", "tmp", ".trash")
        if os.path.isdir(trash):
            subprocess.run(f"find '{trash}' -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", shell=True, check=False)
        log(f"deleted {MINIO_DATA}/vlpds/{prefix}/ in {time.time()-t:.0f}s; disk free {disk_free_gb():.0f} GB")
        return
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


# bench/obs (just obs-up): each step becomes a region annotation on the vlpds
# Grafana dashboard. No-op when Grafana isn't running; GRAFANA_URL="" disables.
GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3300")


def annotate(t0, text, tags=()):
    if not GRAFANA:
        return
    body = json.dumps({"time": int(t0 * 1000), "timeEnd": int(time.time() * 1000), "tags": ["vlpds-bench", *tags], "text": text})
    try:
        http("POST", GRAFANA + "/api/annotations", body.encode(), {"Content-Type": "application/json"}, timeout=2)
    except Exception:
        pass


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
        if os.path.exists("/proc/self/stat"):
            return self.run_proc()
        while not self._stop.wait(self.every):
            for p in self.pids:
                r = subprocess.run(["ps", "-o", "%cpu=,rss=", "-p", str(p)], capture_output=True, text=True)
                try:
                    cpu, rss = r.stdout.split()
                    self.samples[p].append((float(cpu), int(rss) * 1024))
                except ValueError:
                    pass

    def run_proc(self):
        """Linux: `ps %cpu` is the lifetime average, so use /proc tick deltas."""
        hz, page = os.sysconf("SC_CLK_TCK"), os.sysconf("SC_PAGE_SIZE")
        last = {}

        def ticks(p):
            f = open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()
            return int(f[11]) + int(f[12])
        for p in list(self.pids):
            try:
                last[p] = (time.time(), ticks(p))
            except (OSError, ValueError, IndexError):
                pass
        while not self._stop.wait(self.every):
            for p in list(self.pids):
                try:
                    now, tk = time.time(), ticks(p)
                    rss = int(open(f"/proc/{p}/statm").read().split()[1]) * page
                except (OSError, ValueError, IndexError):
                    continue
                if p in last:
                    t0, k0 = last[p]
                    self.samples.setdefault(p, []).append((round(100.0 * (tk - k0) / hz / max(1e-3, now - t0), 1), rss))
                last[p] = (now, tk)

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
               "vlpds_requests_forwarded_total", "vlpds_firehose_events_total", "vlpds_commit_requests_sum", "vlpds_commit_requests_count", "vlpds_commit_ops_sum", "vlpds_commit_ops_count",
               'vlpds_http_client_connects_total{role="peer"}', 'vlpds_http_client_connects_total{role="public"}',
               "vlpds_http_server_connections_total", "vlpds_segment_stored_bytes_total", "vlpds_retention_deleted_bytes_total",
               "vlpds_segment_compress_seconds_count"]
# float deltas (mdelta rounds to integers)
METRIC_KEYS_F = ["vlpds_segment_compress_seconds_sum", "vlpds_process_cpu_seconds_total", "vlpds_commit_build_seconds_sum",
                 "vlpds_repo_load_seconds_sum"]
METRIC_KEYS += ["vlpds_commit_build_seconds_count", "vlpds_repo_load_seconds_count", "vlpds_object_store_requests_total"]


def mdelta(a, b):
    d = {k.replace("vlpds_", ""): round(msum(b, k) - msum(a, k)) for k in METRIC_KEYS}
    d.update({k.replace("vlpds_", ""): round(msum(b, k) - msum(a, k), 4) for k in METRIC_KEYS_F})
    return d


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


def start_profile(node, tag, delay, secs=None):
    """CPU profile (pprof protobuf) of `node` via GET /debug/pprof/profile
    (needs the --features profiling build: BIN_DIR=target-prof on benchbox),
    `delay` s from now. Saved under OUTDIR/prof/<tag>.pb; returns the thread."""
    secs = secs or int(os.environ.get("PROFILE_SECS", "10"))
    path = os.path.join(OUTDIR, "prof", re.sub(r"[^A-Za-z0-9_.=-]+", "_", tag) + ".pb")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    def go():
        time.sleep(delay)
        try:
            _, body = http("GET", f"{node.url}/debug/pprof/profile?seconds={secs}", headers={"Authorization": f"Bearer {ADMIN}"}, timeout=secs + 30)
            open(path, "wb").write(body)
            log(f"profile saved: {path} ({len(body)} B)")
        except Exception as e:
            log(f"profile {tag} failed: {e}")
    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.path = path
    return t


def profile_rates():
    return {int(x) for x in os.environ.get("PROFILE_RATES", "").split(",") if x}


def grid_step(node, out, shape, rate, total, active, inject, duration=20, hot=200):
    check_disk()
    t0 = time.time()
    m0 = metrics(node.url)
    lg = run_loadgen(node.url, rate, total, active, duration=duration, hot=hot, tag=f"{shape}-{rate}")
    prof = start_profile(node, f"{shape}-{rate}", 15) if rate in profile_rates() else None
    with Sampler([node.p.pid, lg.pid]) as s:
        text, _ = lg.communicate()
    if prof:
        prof.join(30)
    m1 = metrics(node.url)
    r = parse_loadgen(text)
    rec = {"shape": shape, "total": total, "active": active, "inject_put_ms": inject, "rate": rate, "hot_rate": hot,
           "duration_s": duration, **r, "server": s.summary(node.p.pid), "loadgen": s.summary(lg.pid),
           "metrics_delta": mdelta(m0, m1), "server_stats_tail": node.stat_lines(2)[-1:],
           "jemalloc": {k.split('"')[1]: v for k, v in m1.items() if k.startswith("vlpds_jemalloc_bytes")},
           "vlpds_extra": os.environ.get("VLPDS_EXTRA", "") + " " + " ".join(node.args[node.args.index("--dev-mode") + 1:]),
           "profile": os.path.basename(prof.path) if prof else None}
    md = rec["metrics_delta"]
    rec["cpu_us_per_commit"] = round(md["process_cpu_seconds_total"] * 1e6 / md["commits_total"], 1) if md.get("commits_total") else None
    rec["commit_build_us"] = round(md["commit_build_seconds_sum"] * 1e6 / md["commit_build_seconds_count"], 2) if md.get("commit_build_seconds_count") else None
    write_jsonl(out, rec)
    a = r.get("all", {})
    log(f"{shape} rate={rate}: achieved {r.get('achieved')} err {r.get('errors')} drop {r.get('dropped')} p50 {a.get('p50')} p99 {a.get('p99')} p99.9 {a.get('p999')} | srv cpu {rec['server']['cpu_pct_avg']}% rss {rec['server']['rss_gb_max']}GB | {rec['cpu_us_per_commit']} us cpu/commit, build {rec['commit_build_us']} us")
    annotate(t0, f"{shape} rate={rate}: achieved {r.get('achieved')} p99 {a.get('p99')} ms", (shape,))
    return rec


def saturated(rec):
    a = rec.get("all") or {}
    return (rec.get("achieved", 0) < 0.93 * rec["rate"] or rec.get("errors", 0) > 0.01 * rec["rate"] * rec["duration_s"]
            or a.get("p99", 1e9) > 2000)


def cmd_grid(name, total, active, inject, rates, prefix=None, keep=False, duration=20):
    """Fresh prefix, bulk `total` accounts, then stair-step rates until saturation."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", f"grid-{name}.jsonl"))
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
    out = os.path.join(OUTDIR, os.environ.get("OUT", "grid.jsonl"))
    check_disk()
    # VARIANTS="--log-inflight 1|--log-inflight 4 --max-segment-mb 32": one
    # server config per entry (same bulk), shape tagged with the variant
    variants = [v.strip() for v in os.environ.get("VARIANTS", "").split("|")] or [""]
    combos = [(v, i) for v in variants for i in injects]
    try:
        for k, (variant, inject) in enumerate(combos):
            vtag = ("/" + variant.replace("--", "").replace(" ", "")) if variant else ""
            name = f"grid-{total}-inj{int(inject)}"
            node = Node(name, prefix, inject=inject, extra=variant.split())
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
                    shape = f"{total}/{active}/inj{int(inject)}{vtag}"
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
    if os.environ.get("SHARDS"):
        per += ["--shards", os.environ["SHARDS"]]
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
            want = msum(metrics(nodes[0].url), "vlpds_shard_layout_shards") or 64
            if sum(owned) >= want and min(owned) > 0 and all(all(x for x in tb) for tb in tables):
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
    t0 = time.time()
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
    annotate(t0, f"{shape} rate={rate}: achieved {agg['achieved']} p99<= {rec['all_p99_max']} ms", (shape,))
    return rec


def cmd_cluster(total, active, inject, rates, duration=20, failover_rate=None):
    prefix = f"bench-cluster-{total}"
    out = os.path.join(OUTDIR, os.environ.get("OUT", "cluster.jsonl"))
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


def fanout(url, subs, seconds, cursor=None, threads=8, shard_of=0):
    args = [LOADGEN, "--host", url, "--threads", str(threads), "fanout", "--subscribers", str(subs), "--seconds", str(seconds)]
    if shard_of:  # needs the loadgen with `fanout --shard-of` (bench/results/2026-10-03-benchbox/loadgen-shard-listrepos.patch)
        args += ["--shard-of", str(shard_of)]
    jo = os.path.join(SCRATCH, "fanout.json")
    args += ["--json-out", jo]
    if cursor is not None:
        args += ["--cursor", str(cursor)]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p, jo


def cmd_firehose(inject=0):
    out = os.path.join(OUTDIR, os.environ.get("OUT", "firehose.jsonl"))
    prefix = "bench-firehose"
    node = Node("firehose", prefix, inject=inject, extra=["--firehose-ring-mb", "64"]).start()
    try:
        bulk([node], 100000)
        # fan-out: N subscribers at a fixed write rate
        plan = [(2000, 1, 0), (2000, 10, 0), (2000, 100, 0), (2000, 1000, 0), (10000, 1, 0), (10000, 10, 0), (10000, 100, 0), (10000, 1000, 0), (50000, 1, 0), (50000, 10, 0)]
        # FH_SHARD_OF=4: also sharded consumer sets (subscriber i takes ?shard=(i%4)/4)
        so = int(os.environ.get("FH_SHARD_OF", "0"))
        if so:
            plan += [(10000, so, so), (10000, 10 * so, so), (10000, 100 * so, so), (50000, so, so), (50000, 10 * so, so)]
        for rate, subs, shard_of in plan:
            check_disk()
            lg = run_loadgen(node.url, rate, 100000, 50000, duration=25, warmup=5, hot=0, firehose=False, tag="fh-load")
            time.sleep(7)
            fo, jo = fanout(node.url, subs, 20, shard_of=shard_of)
            with Sampler([node.p.pid, fo.pid]) as s:
                text, _ = fo.communicate()
                lgt, _ = lg.communicate()
            r = json.load(open(jo)) if os.path.exists(jo) else {"raw": text[-500:]}
            w = parse_loadgen(lgt)
            rec = {"kind": "fanout", "shard_of": shard_of, "write_rate": rate, "achieved_writes": w.get("achieved"), "write_p99": (w.get("all") or {}).get("p99"),
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
    out = os.path.join(OUTDIR, os.environ.get("OUT", "methods.jsonl"))
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
            rec["bin"] = BIN
            write_jsonl(out, rec)
    finally:
        node.stop()
        node.wipe_cache()
        cleanup_prefix(prefix)


def cmd_sweep(sizes, extra=()):
    """One server; one fresh repo per size, filled via applyWrites (200 creates
    per call, 16 in flight), then the read methods + getRepo export."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", "sweep.jsonl"))
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


def cmd_proxy(actives, concs, total=int(os.environ.get("PROXY_TOTAL", "1000000")), body=2048, extra=()):
    """Proxied reads (loadgen proxy) through one node to a stub AppView.
    Sized to the box: the node gets --io-threads = cores (PROXY_IO_THREADS),
    the stub and loadgen a quarter of the cores each (PROXY_STUB_THREADS,
    PROXY_LG_THREADS). Per step: req/s, latency, CPU per proxied request
    (process CPU / requests over the step, from the node's metrics)."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", "proxy.jsonl"))
    prefix = "bench-proxy"
    cores = os.cpu_count() or 8
    io = os.environ.get("PROXY_IO_THREADS", str(cores))
    stub_t = os.environ.get("PROXY_STUB_THREADS", str(max(4, cores // 4)))
    lgt = os.environ.get("PROXY_LG_THREADS", str(max(6, cores // 4)))
    conns = os.environ.get("PROXY_CONNECTIONS", "64")
    secs = os.environ.get("PROXY_SECS", "15")
    stub = subprocess.Popen([LOADGEN, "--threads", stub_t, "stub-appview", "--listen", "127.0.0.1:2700", "--body-bytes", str(body)]
                            + (["--repo-rev", os.environ["PROXY_STUB_REV"]] if os.environ.get("PROXY_STUB_REV") else []),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    args = ["--appview", "http://127.0.0.1:2700,did:web:stub.test"] + list(extra)
    if "--io-threads" not in args + os.environ.get("VLPDS_EXTRA", "").split():
        args += ["--io-threads", io]
    node = Node("proxy", prefix, extra=args).start()
    prof_at = set(os.environ.get("PROFILE_PROXY", "").split(","))  # "active:conc"
    try:
        bulk([node], total)
        for active in actives:
            for conc in concs:
                jo = os.path.join(SCRATCH, "proxy.json")
                m0 = metrics(node.url)
                with Sampler([node.p.pid, stub.pid]) as s:
                    prof = start_profile(node, f"proxy-{active}-{conc}-{'_'.join(extra)}", 4) if f"{active}:{conc}" in prof_at else None
                    lg = subprocess.Popen([LOADGEN, "--host", node.url, "--threads", lgt, "proxy", "--active", str(active), "--concurrency", str(conc),
                                           "--connections", conns, "--seconds", secs, "--json-out", jo], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    s.pids.append(lg.pid); s.samples[lg.pid] = []
                    text, _ = lg.communicate()
                if prof:
                    prof.join(30)
                m1 = metrics(node.url)
                r = json.load(open(jo))
                cache = {k.split('"')[1]: msum(m1, k) - msum(m0, k) for k in m1 if k.startswith("vlpds_proxy_cache_total")}
                # one service-JWT lookup per proxied request (warmup included, like the CPU)
                reqs = cache.get("jwt_hit", 0) + cache.get("jwt_miss", 0)
                cpu = msum(m1, "vlpds_process_cpu_seconds_total") - msum(m0, "vlpds_process_cpu_seconds_total")
                rec = {**r, "body_bytes": body, "server": s.summary(node.p.pid), "stub": s.summary(stub.pid), "client": s.summary(lg.pid),
                       "server_cpu_us_per_req": round(cpu * 1e6 / reqs, 1) if reqs else None,
                       "proxy_cache": cache, "extra": list(extra), "vlpds_args": node.args[node.args.index("--dev-mode") + 1:],
                       "stub_threads": int(stub_t), "loadgen_threads": int(lgt), "connections": int(conns), "cores": cores,
                       "profile": os.path.basename(prof.path) if prof else None}
                write_jsonl(out, rec)
                log(f"proxy active={active} conc={conc}: {r['req_per_s']:.0f} req/s p50 {r['p50_ms']:.2f} p99 {r['p99_ms']:.2f} err {r['errors']} | "
                    f"{rec['server_cpu_us_per_req']} us cpu/req | cpu srv {rec['server']['cpu_pct_avg']} stub {rec['stub']['cpu_pct_avg']} client {rec['client']['cpu_pct_avg']} | {cache}")
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
            "cache_metrics": {k: v for k, v in m.items() if "cache" in k and not k.startswith("vlpds_proxy_cache")},
            "cached_repos": msum(m, "vlpds_cached_repos"), "firehose_ring_gb": round(msum(m, "vlpds_firehose_ring_bytes") / 1e9, 3),
            "live_ring_gb": round(msum(m, "vlpds_log_live_ring_bytes") / 1e9, 3)}


def cmd_resource(total=10000000, active=50000, rate=50000, inject=25):
    out = os.path.join(OUTDIR, "resource.json")
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
            out_g = os.path.join(OUTDIR, "resource-grid.jsonl")
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


def cmd_resource2(total, actives, rate, inject):
    """One bulk of `total`; per active window a fresh server: snapshots
    (started / idle / after 70 s at `rate`), CPU and RSS during the load.
    RESOURCE_EXTRA: extra server flags."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", "resource.jsonl"))
    prefix = "bench-resource"
    cache = os.path.join(SCRATCH, "cache-resource")
    extra = os.environ.get("RESOURCE_EXTRA", "").split()
    try:
        for k, active in enumerate(actives):
            node = Node(f"resource-{active}", prefix, inject=inject, extra=extra)
            node.cache = cache
            node.args[node.args.index("--cache-dir") + 1] = node.cache
            node.start(timeout=600)
            run = {"total": total, "active": active, "rate": rate, "inject_put_ms": inject, "extra": extra, "snapshots": [snapshot(node, "started")]}
            try:
                if k == 0:
                    run["bulk_s"] = round(bulk([node], total))
                    run["snapshots"].append(snapshot(node, "after bulk"))
                    if os.environ.get("RESOURCE_LISTREPOS") == "1":  # patched loadgen: list-repos
                        for lim in (1000, 1000):
                            r = subprocess.run([LOADGEN, "--host", node.url, "list-repos", "--limit", str(lim)], capture_output=True, text=True)
                            try:
                                lr = json.loads(r.stdout.strip().splitlines()[-1])
                            except Exception:
                                lr = {"error": (r.stderr or r.stdout)[-300:]}
                            run.setdefault("list_repos", []).append(lr)
                            log(f"listRepos enumeration: {lr}")
                time.sleep(15)
                run["snapshots"].append(snapshot(node, "idle"))
                rec = grid_step(node, os.path.join(OUTDIR, os.environ.get("OUT", "resource.jsonl").replace(".jsonl", "-grid.jsonl")), f"resource {total}/{active}/inj{int(inject)}", rate, total, active, inject, duration=60)
                run["load"] = {k2: rec.get(k2) for k2 in ("achieved", "errors", "all", "server", "jemalloc")}
                run["snapshots"].append(snapshot(node, "after 70 s load"))
            finally:
                node.stop()
            write_jsonl(out, run)
            log(json.dumps(run["snapshots"]))
    finally:
        shutil.rmtree(cache, ignore_errors=True)
        cleanup_prefix(prefix)


def cmd_hot(hot_rates, injects):
    """Single hot repo, no fleet: latency and coalescing (requests/commit)."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", "hot.jsonl"))
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


def _b64(b):
    import base64
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def bulk_did(i):
    import base64, hashlib
    h = hashlib.sha256(f"vlpds-bulk:{i}".encode()).digest()
    return "did:plc:" + base64.b32encode(h).decode().lower().rstrip("=")[:24]


def access_jwt(did, secret="dev-secret-change-me", aud="did:web:localhost"):
    import hashlib, hmac
    now = int(time.time())
    h = _b64(b'{"alg":"HS256","typ":"at+jwt"}')
    c = _b64(json.dumps({"scope": "com.atproto.access", "sub": did, "aud": aud, "iat": now, "exp": now + 7200}, separators=(",", ":")).encode())
    si = f"{h}.{c}"
    return si + "." + _b64(hmac.new(secret.encode(), si.encode(), hashlib.sha256).digest())


def hist_buckets(m, name):
    """{le: cumulative count} for an unlabeled histogram."""
    out = {}
    for k, v in m.items():
        if k.startswith(name + "_bucket{"):
            le = re.search(r'le="([^"]+)"', k)[1]
            out[le] = out.get(le, 0) + v
    return out


def objstats(m):
    out = {}
    for k, v in m.items():
        if k.startswith("vlpds_object_store_requests_total{"):
            op = re.search(r'op="([^"]+)"', k)[1]; comp = re.search(r'component="([^"]+)"', k)[1]
            out[f"{op}/{comp}"] = out.get(f"{op}/{comp}", 0) + v
    return out


def cold_writes(url, idxs, conc=8):
    """One createRecord per repo index (closed loop, `conc` in flight): latency ms per write."""
    import http.client
    from urllib.parse import urlparse
    u = urlparse(url)
    lats, errs, lock = [], [], threading.Lock()
    todo = list(idxs)

    def worker():
        c = http.client.HTTPConnection(u.hostname, u.port, timeout=120)
        while True:
            with lock:
                if not todo:
                    break
                i = todo.pop()
            did = bulk_did(i)
            body = json.dumps({"repo": did, "collection": "app.bsky.feed.post",
                               "record": {"$type": "app.bsky.feed.post", "text": f"cold write {i}", "createdAt": "2026-10-02T00:00:00.000Z"}}).encode()
            t = time.time()
            try:
                c.request("POST", "/xrpc/com.atproto.repo.createRecord", body, {"Content-Type": "application/json", "Authorization": "Bearer " + access_jwt(did)})
                r = c.getresponse(); data = r.read()
                dt = (time.time() - t) * 1000
                with lock:
                    (lats if r.status == 200 else errs).append(dt if r.status == 200 else (r.status, data[:200].decode(errors="replace")))
            except Exception as e:
                with lock:
                    errs.append(str(e)[:200])
                c = http.client.HTTPConnection(u.hostname, u.port, timeout=120)
    ts = [threading.Thread(target=worker) for _ in range(conc)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return lats, errs


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2)


def cmd_coldload(tiers, inject_state="20,30,0.5", inject_put=25):
    """Partial MSTs + disk cache: big repos (tiers "records:count,..."), then
    one createRecord per repo (a cold repo load each) in three passes:
    cold (fresh process, disk cache wiped), disk-warm (restart, cache dir
    kept: the same repos again, their SSTs on local NVMe), mem-warm (no
    restart: repo cache hit). VLPDS_INJECT_STATE_MS emulates S3 latency for
    the disk-cache misses. COLD_SAMPLE caps writes per tier and pass."""
    out = os.path.join(OUTDIR, os.environ.get("OUT", "coldload.jsonl"))
    prefix = "bench-coldload"
    plan, start = [], 0
    for t in tiers.split(","):
        recs, n = (int(x) for x in t.split(":"))
        plan.append((recs, start, n)); start += n
    cache = os.path.join(SCRATCH, "cache-coldload")
    sample = int(os.environ.get("COLD_SAMPLE", "100000"))
    conc = int(os.environ.get("COLD_CONC", "8"))
    extra = os.environ.get("COLD_EXTRA", "").split()

    def mk(name, inject=inject_put):
        nd = Node(name, prefix, inject=inject, extra=extra)
        nd.cache = cache
        nd.args[nd.args.index("--cache-dir") + 1] = cache
        return nd
    shutil.rmtree(cache, ignore_errors=True)
    os.environ.pop("VLPDS_INJECT_STATE_MS", None)
    node = mk("coldload-fill", inject=0).start(timeout=600)
    try:
        for recs, s0, n in plan:
            t = time.time()
            r = subprocess.run([LOADGEN, "--host", node.url, "bulk", "--start", str(s0), "--count", str(n), "--records", str(recs),
                                "--batch", str(max(1, min(1000, 2_000_000 // recs))), "--concurrency", "16" if recs < 100_000 else "8" if recs < 1_000_000 else "4"], capture_output=True, text=True)
            if r.returncode:
                raise RuntimeError("bulk failed: " + (r.stdout + r.stderr)[-2000:])
            log(f"bulk {n} x {recs} records in {time.time()-t:.0f}s ({n*recs/(time.time()-t):.0f} rec/s)")
            write_jsonl(out, {"kind": "fill", "records": recs, "count": n, "secs": round(time.time() - t, 1), "rss": snapshot(node, "after fill")["rss_gb"]})
        log("fill done; settling 90 s for compaction")
        time.sleep(90)
    finally:
        node.stop()
    sst = None
    if MINIO_DATA:
        r = subprocess.run(["du", "-sk", os.path.join(MINIO_DATA, "vlpds", prefix)], capture_output=True, text=True)
        sst = int(r.stdout.split()[0]) * 1024 if r.stdout else None
    write_jsonl(out, {"kind": "stored", "minio_prefix_bytes": sst, "records": sum(r * n for r, _, n in plan)})
    os.environ["VLPDS_INJECT_STATE_MS"] = inject_state  # S3-like latency on disk-cache misses
    try:
        for pss in ("cold", "disk-warm", "mem-warm"):
            if pss == "cold":
                shutil.rmtree(cache, ignore_errors=True)
            if pss != "mem-warm":
                node = mk(f"coldload-{pss}").start(timeout=600)
                time.sleep(5)
            cache_bytes = subprocess.run(["du", "-sk", cache], capture_output=True, text=True).stdout.split()[:1]
            for recs, s0, n in plan:
                idxs = list(range(s0, s0 + min(n, sample)))
                m0 = metrics(node.url)
                with Sampler([node.p.pid]) as smp:
                    t = time.time()
                    lats, errs = cold_writes(node.url, idxs, conc)
                    secs = time.time() - t
                m1 = metrics(node.url)
                o0, o1 = objstats(m0), objstats(m1)
                rec = {"kind": "pass", "pass": pss, "records": recs, "repos": len(idxs), "conc": conc, "secs": round(secs, 1),
                       "ok": len(lats), "errors": len(errs), "first_error": errs[0] if errs else None,
                       "p50": pct(lats, .5), "p90": pct(lats, .9), "p99": pct(lats, .99), "max": round(max(lats), 1) if lats else None,
                       "mean": round(sum(lats) / len(lats), 2) if lats else None,
                       "repo_load_hist_delta": {k: v - hist_buckets(m0, "vlpds_repo_load_seconds").get(k, 0) for k, v in hist_buckets(m1, "vlpds_repo_load_seconds").items()},
                       "metrics_delta": mdelta(m0, m1),
                       "lazy": {k: msum(m1, k) - msum(m0, k) for k in ("vlpds_lazy_mst_fetches_total", "vlpds_lazy_mst_reads_total", "vlpds_lazy_mst_fallbacks_total", "vlpds_lazy_mst_prefetch_bytes_sum", "vlpds_lazy_mst_prefetch_bytes_count")},
                       "objstore_delta": {k: v - o0.get(k, 0) for k, v in o1.items() if v - o0.get(k, 0)},
                       "server": smp.summary(node.p.pid), "snapshot": snapshot(node, f"after {pss} {recs}"),
                       "disk_cache_kb_at_pass_start": int(cache_bytes[0]) if cache_bytes else None,
                       "inject_state_ms": inject_state, "inject_put_ms": inject_put}
                write_jsonl(out, rec)
                log(f"{pss} {recs}-record repos x{len(idxs)}: p50 {rec['p50']} p90 {rec['p90']} p99 {rec['p99']} max {rec['max']} ms err {len(errs)} | loads {rec['metrics_delta']['repo_loads_total']} | objstore {rec['objstore_delta']} | rss {rec['snapshot']['rss_gb']} GB")
            if pss == "disk-warm":
                continue  # mem-warm reuses this process
            if pss == "mem-warm":
                node.stop()
            elif pss == "cold":
                node.stop()
    finally:
        try:
            node.stop()
        except Exception:
            pass
        shutil.rmtree(cache, ignore_errors=True)
        cleanup_prefix(prefix)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "hot":
        cmd_hot([int(x) for x in sys.argv[2].split(",")], [float(x) for x in sys.argv[3].split(",")])
    elif cmd == "resource" and len(sys.argv) > 2:
        # resource <total> <active,active> <rate> <inject>
        cmd_resource2(int(sys.argv[2]), [int(x) for x in sys.argv[3].split(",")], int(sys.argv[4]), float(sys.argv[5]))
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
    elif cmd == "coldload":
        cmd_coldload(sys.argv[2], *(sys.argv[3:4] or ["20,30,0.5"]), inject_put=float(sys.argv[4]) if len(sys.argv) > 4 else 25)
    elif cmd == "cleanup":
        cleanup_prefix(sys.argv[2])
    else:
        raise SystemExit(__doc__)
