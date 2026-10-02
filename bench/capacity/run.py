#!/usr/bin/env python3
"""Capacity test driver for vlpds (stdlib only): N nodes on one box, a bulk
population with the real records-per-repo distribution (scaled), then a
sliding active window at increasing write rates, a kill -9 mid-run, and a
RESULTS.md. Phases are resumable: the population (expensive) stays in MinIO
across invocations; progress is checkpointed in <state dir>/state.json.

    bench/capacity/run.py plan     --total 100000000 [--dist-scale 128 --dist-knee 2]
    bench/capacity/run.py all      --total 1000000 --nodes 3 --active 20000 --rates 5000,10000,20000
    bench/capacity/run.py populate|stairs|kill|report|status|cleanup  (same flags)

Phases of `all` (each skipped when state.json says it's done):
  up -> populate (chunked, resumable) -> settle -> stairs (each rate once;
  stops at saturation) -> kill (kill -9 one node mid-run, restart) -> report
  -> down. `cleanup` deletes the MinIO prefix + .trash, caches, containers and
  the state. `all --cleanup` cleans up at the end.

Nodes: --mode native (processes) or docker (one container per node:
--network host, --ipc host, --log-driver none (logs to a bind-mounted file),
seccomp unconfined, nofile 1M, no cgroup limits; the release binary is
bind-mounted into DOCKER_IMAGE (default ubuntu:24.04), or DOCKER_BIN=image
uses the image's own `vlpds`). Ports --base-port.. (benchbox's Alloy scrapes
2700-2715 and 7100-7105/7700-7705 every second into the vlpds dashboard).

Every --scrape-s (1 s) every node's /metrics (vlpds_*/slatedb_* family sums,
no histogram buckets) and MinIO's cluster metrics (S3 requests by API, bytes)
go to metrics.jsonl; the prefix's on-disk bytes (when BENCH_MINIO_DATA or the
laptop's native MinIO dir is known) every 30 s.

Env (bench/benchbox/remote/runner.sh sets these): BENCH_BIN (release dir with
vlpds + loadgen), BENCH_SCRATCH (state, logs, caches), BENCH_OUT_DIR (results),
BENCH_MINIO_DATA (MinIO data dir: direct prefix delete + .trash purge + du),
MIN_FREE_GB (refuse to continue below; default 150), NODE_EXTRA (extra vlpds
flags for every node), GRAFANA_URL (annotations; "" = off).
"""
import argparse
import base64
import hashlib
import hmac
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
PKG = os.path.abspath(os.path.join(HERE, "..", ".."))
BIN = os.environ.get("BENCH_BIN") or os.path.join(PKG, "target", "release")
VLPDS = os.path.join(BIN, "vlpds")
LOADGEN = os.path.join(BIN, "loadgen")
S3 = os.environ.get("BENCH_S3", "http://127.0.0.1:9200")
BUCKET = "vlpds"
SCRATCH = os.environ.get("BENCH_SCRATCH") or os.path.join(PKG, "target", "capacity-scratch")
ADMIN = "dev-admin-token"
INTERNAL = "dev-internal-token"
GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3300")


def default_minio_data():
    d = os.environ.get("BENCH_MINIO_DATA")
    if d:
        return d
    # laptop: native MinIO started from a scratchpad (ps shows its data dir)
    try:
        out = subprocess.run(["ps", "-axo", "args="], capture_output=True, text=True).stdout
        for line in out.splitlines():
            m = re.search(r"minio server (\S+) .*--address 127\.0\.0\.1:9200", line)
            if m and os.path.isdir(m[1]):
                return m[1]
    except Exception:
        pass
    return ""


MINIO_DATA = default_minio_data()


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def disk_free_gb(path="/"):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / 1e9


def check_disk():
    lim = float(os.environ.get("MIN_FREE_GB", "150"))
    f = disk_free_gb()
    if f < lim:
        raise SystemExit(f"disk free {f:.0f} GB < {lim:.0f} GB, refusing to continue")
    return f


def http(method, url, body=None, headers=None, timeout=10):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def parse_prom(raw, keep=lambda name: True):
    """Family sums (labels dropped, histogram buckets skipped)."""
    out = {}
    for line in raw.splitlines():
        if not line or line[0] == "#":
            continue
        k, _, v = line.rpartition(" ")
        name = k.split("{", 1)[0]
        if name.endswith("_bucket") or not keep(name):
            continue
        try:
            out[name] = out.get(name, 0.0) + float(v)
        except ValueError:
            pass
    return out


def node_metrics(url, timeout=2):
    try:
        _, raw = http("GET", url + "/metrics", timeout=timeout)
    except Exception:
        return None
    return parse_prom(raw.decode(errors="replace"), lambda n: n.startswith(("vlpds_", "slatedb_")))


def minio_token():
    b = lambda x: base64.urlsafe_b64encode(x).rstrip(b"=").decode()
    h = b(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    p = b(json.dumps({"exp": int(time.time()) + 30 * 86400, "sub": "minioadmin", "iss": "prometheus"}, separators=(",", ":")).encode())
    return f"{h}.{p}." + b(hmac.new(b"minioadmin", f"{h}.{p}".encode(), hashlib.sha512).digest())


MINIO_TOKEN = minio_token()


def minio_metrics():
    try:
        _, raw = http("GET", S3 + "/minio/v2/metrics/cluster", headers={"Authorization": "Bearer " + MINIO_TOKEN}, timeout=3)
    except Exception:
        return None
    out = {}
    for line in raw.decode(errors="replace").splitlines():
        if line.startswith("minio_s3_requests_total{"):
            m = re.search(r'api="([^"]+)"', line)
            if m:
                out["req_" + m[1]] = float(line.rpartition(" ")[2])
        elif line.startswith(("minio_s3_traffic_received_bytes", "minio_s3_traffic_sent_bytes")):
            out[line.split("{")[0].replace("minio_s3_traffic_", "")] = float(line.rpartition(" ")[2])
    return out


def prefix_dir(prefix):
    return os.path.join(MINIO_DATA, BUCKET, prefix) if MINIO_DATA else ""


def du_bytes(path):
    if not path or not os.path.isdir(path):
        return None
    r = subprocess.run(["du", "-sk", path], capture_output=True, text=True)
    try:
        return int(r.stdout.split()[0]) * 1024
    except (ValueError, IndexError):
        return None


def du_split(prefix):
    """{"state": bytes, "log": bytes, ..., "total": bytes} of the prefix on MinIO's disk."""
    d = prefix_dir(prefix)
    if not d or not os.path.isdir(d):
        return None
    out = {}
    for e in os.listdir(d):
        b = du_bytes(os.path.join(d, e))
        if b is not None:
            out[e] = b
    out["total"] = sum(out.values())
    return out


def purge_trash():
    if not MINIO_DATA:
        return
    trash = os.path.join(MINIO_DATA, ".minio.sys", "tmp", ".trash")
    if os.path.isdir(trash):
        subprocess.run(f"find '{trash}' -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", shell=True, check=False)


def annotate(t0, text, tags=()):
    if not GRAFANA:
        return
    body = json.dumps({"time": int(t0 * 1000), "timeEnd": int(time.time() * 1000), "tags": ["vlpds-capacity", *tags], "text": text})
    try:
        http("POST", GRAFANA + "/api/annotations", body.encode(), {"Content-Type": "application/json"}, timeout=2)
    except Exception:
        pass


# ---------------------------------------------------------------- nodes

class Node:
    """One vlpds node: a native process or a docker container."""

    def __init__(self, cfg, i):
        self.cfg, self.i = cfg, i
        self.name = f"n{i+1}"
        self.port = cfg.base_port + i
        self.url = f"http://127.0.0.1:{self.port}"
        self.dir = os.path.join(cfg.state_dir, self.name)
        os.makedirs(self.dir, exist_ok=True)
        self.logpath = os.path.join(self.dir, "server.log")
        self.cache = os.path.join(self.dir, "cache") if cfg.cache_dir else ""
        self.container = f"vlpds-cap-{cfg.name}-{self.name}"
        self.p = None

    def args(self, exe):
        c = self.cfg
        a = [exe, "--listen", f"127.0.0.1:{self.port}", "--public-url", self.url, "--s3-endpoint", S3,
             "--prefix", c.prefix, "--no-rate-limits", "--dev-mode",
             "--node-id", self.name, "--advertise-url", self.url,
             "--workers", str(c.workers), "--io-threads", str(c.io_threads),
             "--block-cache-mb", str(c.block_cache_mb), "--repo-cache-mb", str(c.repo_cache_mb),
             "--cache-budget-mb", str(c.cache_budget_mb),
             "--log-retention", c.log_retention, "--slatedb-checkpoint-lifetime", c.checkpoint_lifetime,
             "--slatedb-gc-min-age", c.gc_min_age, "--lease-ttl-ms", str(c.lease_ttl_ms)]
        if self.cache:
            a += ["--cache-dir", self.cache]
        if c.inject:
            a += ["--inject-put-ms", str(c.inject)]
        return a + c.node_extra

    def start(self, timeout=600):
        mark = os.path.getsize(self.logpath) if os.path.exists(self.logpath) else 0
        if self.cfg.mode == "native":
            self.f = open(self.logpath, "ab")
            env = dict(os.environ)
            env.setdefault("RUST_LOG", "info,slatedb=warn")
            self.p = subprocess.Popen(self.args(VLPDS), stdout=self.f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
        else:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
            image = os.environ.get("DOCKER_IMAGE", "ubuntu:24.04")
            own = os.environ.get("DOCKER_BIN", "mount") == "image"
            exe = "vlpds" if own else "/opt/vlpds/vlpds"
            cmd = " ".join(shq(x) for x in self.args(exe)) + f" >> {shq(self.logpath)} 2>&1"
            run = ["docker", "run", "-d", "--name", self.container, "--network", "host", "--ipc", "host",
                   "--log-driver", "none", "--security-opt", "seccomp=unconfined",
                   "--ulimit", "nofile=1048576:1048576", "--user", f"{os.getuid()}:{os.getgid()}",
                   "-e", "RUST_LOG=info,slatedb=warn", "-v", f"{self.dir}:{self.dir}"]
            if not own:
                run += ["-v", f"{BIN}:/opt/vlpds:ro"]
            run += ["--entrypoint", "/bin/sh", image, "-c", "exec " + cmd]
            r = subprocess.run(run, capture_output=True, text=True)
            if r.returncode:
                raise RuntimeError(f"docker run {self.name}: {r.stderr.strip()}")
        t = time.time()
        while time.time() - t < timeout:
            if not self.alive():
                raise RuntimeError(f"{self.name} exited; see {self.logpath}")
            try:
                http("GET", self.url + "/xrpc/_health", timeout=2)
                with open(self.logpath, "rb") as f:
                    f.seek(mark)
                    if b"vlpds serving" in f.read():
                        return self
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{self.name} did not come up in {timeout}s")

    def alive(self):
        if self.cfg.mode == "native":
            return self.p is not None and self.p.poll() is None
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", self.container], capture_output=True, text=True)
        return r.stdout.strip() == "true"

    def signal(self, sig):
        if self.cfg.mode == "native":
            if self.p and self.p.poll() is None:
                self.p.send_signal(sig)
        else:
            subprocess.run(["docker", "kill", "-s", signal.Signals(sig).name, self.container], capture_output=True)

    def wait(self, timeout):
        t = time.time()
        while self.alive() and time.time() - t < timeout:
            time.sleep(0.2)
        return not self.alive()

    def kill9(self):
        self.signal(signal.SIGKILL)
        self.wait(30)
        self.close()

    def stop(self, timeout=120):
        if self.alive():
            self.signal(signal.SIGTERM)
            if not self.wait(timeout):
                self.signal(signal.SIGKILL)
                self.wait(30)
        self.close()

    def close(self):
        if self.cfg.mode == "native":
            if self.p:
                self.p.wait()
            if getattr(self, "f", None):
                self.f.close()
                self.f = None
        else:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


def shq(s):
    s = str(s)
    return s if re.fullmatch(r"[A-Za-z0-9_./:=,@%+-]+", s) else "'" + s.replace("'", "'\\''") + "'"


def wait_converged(nodes, shards=256, timeout=180):
    """Every live node's routing table names an owner for all shards and
    the owned counts sum to `shards`."""
    t = time.time()
    owned = []
    while time.time() - t < timeout:
        try:
            ms = [node_metrics(n.url) or {} for n in nodes]
            owned = [int(m.get("vlpds_owned_partitions", 0)) for m in ms]
            tables = [json.loads(http("GET", n.url + "/internal/v1/cluster", headers={"x-vlpds-internal": INTERNAL})[1])["table"] for n in nodes]
            if sum(owned) >= shards and min(owned) > 0 and all(all(x for x in tb) for tb in tables):
                log(f"cluster converged {owned} in {time.time()-t:.1f}s")
                return time.time() - t
        except Exception:
            pass
        time.sleep(0.5)
    log(f"cluster did not converge in {timeout}s: owned {owned}")
    return None


# ---------------------------------------------------------------- scraper

class Scraper:
    """1 s scrape of every node + MinIO into metrics.jsonl; prefix du every 30 s."""

    def __init__(self, cfg, nodes, path):
        self.cfg, self.nodes, self.path = cfg, nodes, path
        self.stop_ev = threading.Event()
        self.last = {}  # node -> last metrics
        self.lock = threading.Lock()
        self.du = None
        self.threads = [threading.Thread(target=self.loop, daemon=True), threading.Thread(target=self.du_loop, daemon=True)]

    def __enter__(self):
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *a):
        self.stop_ev.set()
        for t in self.threads:
            t.join(timeout=60)

    def loop(self):
        with open(self.path, "a") as f:
            while not self.stop_ev.is_set():
                t0 = time.time()
                for n in self.nodes:
                    m = node_metrics(n.url, timeout=max(0.5, self.cfg.scrape_s))
                    if m is not None:
                        with self.lock:
                            self.last[n.name] = m
                    m = {k: v for k, v in (m or {}).items() if v}
                    f.write(json.dumps({"t": round(t0, 3), "node": n.name, "up": bool(m), "m": m}) + "\n")
                mm = minio_metrics()
                if mm is not None:
                    f.write(json.dumps({"t": round(t0, 3), "minio": mm}) + "\n")
                f.flush()
                self.stop_ev.wait(max(0.05, self.cfg.scrape_s - (time.time() - t0)))

    def du_loop(self):
        with open(self.path, "a") as f:
            while True:
                t0 = time.time()
                d = du_split(self.cfg.prefix)
                if d is not None:
                    self.du = d
                    f.write(json.dumps({"t": round(t0, 3), "du": d, "free_gb": round(disk_free_gb(), 1)}) + "\n")
                    f.flush()
                if self.stop_ev.wait(30):
                    return

    def gauge(self, name, fn=sum):
        with self.lock:
            return fn([m.get(name, 0) for m in self.last.values()] or [0])


# ---------------------------------------------------------------- state

def load_state(cfg):
    p = os.path.join(cfg.state_dir, "state.json")
    st = json.load(open(p)) if os.path.exists(p) else {}
    want = {"prefix": cfg.prefix, "total": cfg.total, "dist": cfg.dist_args_str, "nodes": cfg.nodes}
    if st.get("population") and st["population"] != want:
        # the node count may change between invocations: shards rebalance
        a = {k: v for k, v in st["population"].items() if k != "nodes"}
        b = {k: v for k, v in want.items() if k != "nodes"}
        if a != b:
            raise SystemExit(f"state {p} is for population {st['population']}, not {want}: use another --name or `cleanup`")
    st["population"] = want
    return st


def save_state(cfg, st):
    p = os.path.join(cfg.state_dir, "state.json")
    with open(p + ".tmp", "w") as f:
        json.dump(st, f, indent=1)
    os.replace(p + ".tmp", p)


def write_jsonl(path, rec):
    with open(path, "a") as f:
        f.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------- phases

def dist_flags(cfg):
    if cfg.dist == "fixed":
        return ["--dist", "fixed", "--records", str(cfg.records)]
    return ["--dist", "real", "--dist-scale", str(cfg.dist_scale), "--dist-knee", str(cfg.dist_knee),
            "--dist-group", str(cfg.dist_group), "--dist-seed", "1"]


def plan(cfg, count=None):
    r = subprocess.run([LOADGEN, "dist", "--count", str(count or cfg.total), "--batch", str(cfg.bulk_batch)] + dist_flags(cfg),
                       capture_output=True, text=True, check=True)
    return json.loads(r.stdout.strip().splitlines()[-1])


def phase_populate(cfg, st, nodes, scr):
    out = os.path.join(cfg.out, "populate.jsonl")
    pop = st.setdefault("populate", {"watermark": 0, "secs": 0.0, "records": 0, "chunks": 0})
    if pop.get("done"):
        log(f"populate: done already ({pop['watermark']} accounts)")
        return
    p = plan(cfg)
    log(f"populate: {cfg.total} accounts, plan {p['records']} records (mean {p['mean']:.2f}, p99 {p['p99']}, max {p['max']}), "
        f"{p['requests']} requests over all nodes (each gets only its DIDs); resuming at {pop['watermark']}")
    while pop["watermark"] < cfg.total:
        check_disk()
        s = pop["watermark"]
        c = min(cfg.chunk - s % cfg.chunk, cfg.total - s)
        pfiles = [os.path.join(cfg.state_dir, f"bulk-{n.name}.json") for n in nodes]
        st["populate_inflight"] = {"start": s, "count": c}
        save_state(cfg, st)
        t0 = time.time()
        du0 = scr.du
        procs = [subprocess.Popen([LOADGEN, "--host", n.url, "--threads", str(cfg.loadgen_threads), "bulk", "--start", str(s), "--count", str(c),
                                   "--batch", str(cfg.bulk_batch), "--concurrency", str(cfg.bulk_concurrency), "--progress-file", pf] + dist_flags(cfg),
                                  stdout=subprocess.PIPE, stderr=open(os.path.join(cfg.state_dir, f"bulk-{n.name}.stderr"), "a"), text=True)
                 for n, pf in zip(nodes, pfiles)]
        def resume_point():
            # the lowest watermark any node's loadgen reached (every account
            # below it exists; bulkCreate skips existing accounts, so a
            # resume from it is idempotent)
            wms = []
            for pf in pfiles:
                try:
                    j = json.load(open(pf))
                    wms.append(j["watermark"] if j.get("start") == s else s)
                except Exception:
                    wms.append(s)
            pop["watermark"] = max(s, min(wms))
            save_state(cfg, st)
        try:
            outs = [pr.communicate()[0] for pr in procs]
        except BaseException:
            for pr in procs:
                pr.terminate()
            for pr in procs:
                pr.wait()
            resume_point()
            log(f"populate: interrupted in chunk {s}+{c}, resume at {pop['watermark']}")
            raise
        secs = time.time() - t0
        if any(pr.returncode for pr in procs):
            resume_point()
            raise RuntimeError(f"bulk chunk {s}+{c} failed (resume at {pop['watermark']}); see {cfg.state_dir}/bulk-*.stderr")
        res = [json.loads(o.strip().splitlines()[-1]) for o in outs]
        recs = sum(r["records"] for r in res)
        created = sum(r["created"] for r in res)
        existing = sum(r.get("existing", 0) for r in res)
        nreq = sum(r.get("requests", 0) for r in res)
        pop["watermark"] = s + c
        pop["secs"] += secs
        pop["records"] += recs
        pop["chunks"] += 1
        st.pop("populate_inflight", None)
        save_state(cfg, st)
        rss = scr.gauge("vlpds_process_resident_bytes", max) / 1e9
        rec = {"t": t0, "start": s, "count": c, "created": created, "existing": existing, "requests": nreq,
               "requests_per_1k": round(nreq * 1000 / max(c, 1), 2), "records": recs, "secs": round(secs, 1),
               "accounts_s": round(c / secs), "records_s": round(recs / secs), "rss_gb_max_node": round(rss, 2),
               "du_before": du0, "du": scr.du, "free_gb": round(disk_free_gb(), 1)}
        write_jsonl(out, rec)
        log(f"populate: {s+c}/{cfg.total} (+{c} in {secs:.0f}s: {c/secs:.0f} accounts/s, {recs/secs:.0f} records/s; created {created}, "
            f"existing {existing}, {nreq * 1000 / max(c, 1):.2f} requests/1k accounts) "
            f"rss<= {rss:.1f} GB, s3 {fmt_gb((scr.du or {}).get('total'))}")
        annotate(t0, f"populate {s}+{c}: {c/secs:.0f} accounts/s", ("populate",))
        if created + existing != c:
            log(f"populate: WARNING created {created} + existing {existing} != {c} (an account no node owned?)")
    # settle: wait for compaction / GC to stop shrinking the prefix
    t0 = time.time()
    last, stable = None, 0
    while time.time() - t0 < cfg.settle_s:
        purge_trash()
        d = du_split(cfg.prefix)
        tot = d["total"] if d else None
        if tot is not None and last is not None and abs(tot - last) < 0.01 * max(last, 1):
            stable += 1
            if stable >= 3:
                break
        else:
            stable = 0
        last = tot
        time.sleep(20)
    d = du_split(cfg.prefix)
    pop.update(done=True, settled_du=d, settle_s=round(time.time() - t0))
    save_state(cfg, st)
    write_jsonl(out, {"summary": True, "accounts": cfg.total, "records": pop["records"], "secs": round(pop["secs"], 1),
                      "accounts_s": round(cfg.total / max(pop["secs"], 1e-9)), "records_s": round(pop["records"] / max(pop["secs"], 1e-9)),
                      "settled_du": d, "settle_s": pop["settle_s"], "plan": p})
    log(f"populate: done, {cfg.total} accounts / {pop['records']} records in {pop['secs']:.0f}s; settled at {fmt_gb((d or {}).get('total'))} "
        f"after {pop['settle_s']}s")


def fmt_gb(b):
    return "?" if b is None else f"{b/1e9:.2f} GB"


WIN_RE = re.compile(r"\[\s*(\d+)s\] ok/s\s+(\d+) err (\d+) dropped (\d+) inflight (\d+) \| p50 ([\d.]+)ms p99 ([\d.]+)ms max (\d+)ms")
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
            out["first_error"] = line[12:300]
    return out


def windows(path):
    out = []
    for line in open(path, errors="replace"):
        m = WIN_RE.search(line)
        if m:
            out.append({"t": int(m[1]), "ok_s": int(m[2]), "err": int(m[3]), "dropped": int(m[4]), "inflight": int(m[5]),
                        "p50": float(m[6]), "p99": float(m[7]), "max": int(m[8])})
    return out


def run_step(cfg, st, nodes, tag, rate, duration, event=None, lg_nodes=None):
    """One loadgen per node of `lg_nodes` (default all) at rate/N over the
    sliding window (not DID-routed: (N-1)/N of writes are forwarded, like a
    plain load balancer). The first loadgen also runs the firehose consumer
    and a 200/s hot repo."""
    check_disk()
    purge_trash()
    offset = st.get("sim_offset", 0)
    t0 = time.time()
    lgs, errs = [], []
    lg_nodes = lg_nodes or nodes
    for i, n in enumerate(lg_nodes):
        ep = os.path.join(cfg.state_dir, f"{tag}-lg{i}.stderr")
        errs.append(ep)
        a = [LOADGEN, "--host", n.url, "--threads", str(cfg.loadgen_threads), "run", "--rate", str(rate / len(lg_nodes)),
             "--hot-rate", str(cfg.hot_rate if i == 0 else 0), "--duration", str(duration), "--warmup", str(cfg.warmup),
             "--sim-total", str(cfg.total), "--sim-active", str(cfg.active), "--sim-churn", str(cfg.churn),
             "--sim-offset", str(offset), "--report-secs", "1", "--max-inflight", str(cfg.max_inflight)]
        if i == 0:
            a.append("--firehose")
        lgs.append(subprocess.Popen(a, stdout=subprocess.PIPE, stderr=open(ep, "w"), text=True))
    ev = event(nodes, t0) if event else None
    texts = [lg.communicate()[0] for lg in lgs]
    t1 = time.time()
    # the window continues where this step left it
    st["sim_offset"] = (offset + int(cfg.churn * (t1 - t0))) % max(cfg.total, 1)
    parsed = [parse_loadgen(t) for t in texts]
    agg = {k: sum(p.get(k, 0) for p in parsed) for k in ("achieved", "errors", "dropped")}
    rec = {"tag": tag, "t0": t0, "t1": t1, "rate": rate, "hot_rate": cfg.hot_rate, "nodes": len(nodes), "total": cfg.total,
           "active": cfg.active, "churn": cfg.churn, "sim_offset": offset, "duration_s": duration, "warmup_s": cfg.warmup,
           "inject_put_ms": cfg.inject, **agg, "per_loadgen": parsed, "windows": [windows(e) for e in errs], "event": ev,
           "p50_max": max((p.get("all") or {}).get("p50", 0) for p in parsed),
           "p99_max": max((p.get("all") or {}).get("p99", 0) for p in parsed),
           "p999_max": max((p.get("all") or {}).get("p999", 0) for p in parsed),
           "fh_lag": parsed[0].get("fh-lag"), "hot": parsed[0].get("hot-repo"),
           "first_error": next((p["first_error"] for p in parsed if p.get("first_error")), None)}
    # errors inside the measured window (the totals include the warmup)
    rec["errors_measured"] = sum(max(0, w[-1]["err"] - next((x["err"] for x in w if x["t"] >= cfg.warmup), 0)) for w in rec["windows"] if w)
    rec["summary"] = summarize_metrics(cfg, t0 + cfg.warmup, t1)
    write_jsonl(os.path.join(cfg.out, "steps.jsonl"), rec)
    s = rec["summary"]
    log(f"{tag} rate={rate}: achieved {agg['achieved']} err {rec['errors_measured']} (incl. warmup {agg['errors']}) drop {agg['dropped']} p50<= {rec['p50_max']} "
        f"p99<= {rec['p99_max']} p99.9<= {rec['p999_max']} fh p99 {(rec['fh_lag'] or {}).get('p99')} | loads/s {s.get('loads_s')} "
        f"rss {s.get('rss_gb_max')} GB cpu {s.get('cpu_pct')} | s3 req/s {s.get('s3_req_s')} put/s {s.get('s3_put_s')}")
    annotate(t0, f"{tag} rate={rate}: achieved {agg['achieved']} p99<= {rec['p99_max']} ms", (tag,))
    return rec


def saturated(rec):
    tgt = rec["rate"] + rec["hot_rate"]
    errs = rec.get("errors_measured", rec["errors"])
    return (rec["achieved"] < 0.93 * tgt or errs > 0.01 * tgt * rec["duration_s"] or rec["p99_max"] > 2000)


COUNTERS = {"commits": "vlpds_commits_total", "loads": "vlpds_repo_loads_total", "evictions": "vlpds_repo_evictions_total",
            "seg_puts": "vlpds_segment_put_attempts_total", "seg_bytes": "vlpds_segment_stored_bytes_total",
            "forwarded": "vlpds_requests_forwarded_total", "fh_events": "vlpds_firehose_events_total",
            "retention_deleted_bytes": "vlpds_retention_deleted_bytes_total", "cpu_s": "vlpds_process_cpu_seconds_total"}


def read_metrics(cfg, t0, t1):
    """Rows of metrics.jsonl with t0 <= t <= t1: ({node: [(t, m)]}, [(t, minio)], [(t, du)])."""
    per, mn, du = {}, [], []
    p = os.path.join(cfg.out, "metrics.jsonl")
    if not os.path.exists(p):
        return per, mn, du
    for line in open(p):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if not (t0 <= r["t"] <= t1):
            continue
        if "node" in r and r["up"]:
            per.setdefault(r["node"], []).append((r["t"], r["m"]))
        elif "minio" in r:
            mn.append((r["t"], r["minio"]))
        elif "du" in r:
            du.append((r["t"], r["du"]))
    return per, mn, du


def summarize_metrics(cfg, t0, t1):
    per, mn, du = read_metrics(cfg, t0, t1)
    secs = max(t1 - t0, 1e-9)
    out = {"nodes": {}}
    tot = {k: 0.0 for k in COUNTERS}
    slate = {}
    for node, rows in sorted(per.items()):
        if len(rows) < 2:
            continue
        (ta, a), (tb, b) = rows[0], rows[-1]
        dt = max(tb - ta, 1e-9)
        d = {k: max(0.0, b.get(v, 0) - a.get(v, 0)) for k, v in COUNTERS.items()}
        for k in d:
            tot[k] += d[k] / dt
        for k, v in b.items():
            if k.startswith("slatedb_") and k.endswith("_total"):
                slate[k] = slate.get(k, 0) + max(0.0, v - a.get(k, 0)) / dt
        out["nodes"][node] = {"rss_gb_max": round(max(m.get("vlpds_process_resident_bytes", 0) for _, m in rows) / 1e9, 2),
                              "cpu_pct": round(100 * d["cpu_s"] / dt), "commits_s": round(d["commits"] / dt),
                              "loads_s": round(d["loads"] / dt), "cached_repos_max": int(max(m.get("vlpds_cached_repos", 0) for _, m in rows)),
                              "owned_last": int(b.get("vlpds_owned_partitions", 0))}
    out.update({k + "_s": round(v) for k, v in tot.items() if k != "cpu_s"})
    out["cpu_pct"] = [n["cpu_pct"] for n in out["nodes"].values()]
    out["rss_gb_max"] = [n["rss_gb_max"] for n in out["nodes"].values()]
    out["slatedb_s"] = {k.replace("slatedb_", ""): round(v, 1) for k, v in sorted(slate.items()) if v > 0}
    if len(mn) >= 2:
        (ta, a), (tb, b) = mn[0], mn[-1]
        dt = max(tb - ta, 1e-9)
        d = {k: max(0.0, b.get(k, 0) - a.get(k, 0)) / dt for k in b}
        out["s3_req_s"] = round(sum(v for k, v in d.items() if k.startswith("req_")))
        out["s3_put_s"] = round(d.get("req_putobject", 0) + d.get("req_putobjectpart", 0))
        out["s3_get_s"] = round(d.get("req_getobject", 0))
        out["s3_head_s"] = round(d.get("req_headobject", 0))
        out["s3_list_s"] = round(d.get("req_listobjectsv2", 0) + d.get("req_listobjectsv1", 0))
        out["s3_delete_s"] = round(d.get("req_deleteobject", 0) + d.get("req_deletemultipleobjects", 0))
        out["s3_in_mb_s"] = round(d.get("received_bytes", 0) / 1e6, 1)
        out["s3_out_mb_s"] = round(d.get("sent_bytes", 0) / 1e6, 1)
    if du:
        out["s3_bytes_last"] = du[-1][1].get("total")
    return out


def phase_stairs(cfg, st, nodes):
    done = st.setdefault("stairs_done", [])
    if st.get("stairs_saturated"):
        log(f"stairs: saturated already at {st['stairs_saturated']}")
        return
    for rate in cfg.rates:
        if rate in done:
            continue
        rec = run_step(cfg, st, nodes, f"stair-{rate}", rate, cfg.duration)
        done.append(rate)
        if saturated(rec):
            st["stairs_saturated"] = rate
            log(f"stairs: saturated at {rate}/s")
            save_state(cfg, st)
            break
        save_state(cfg, st)
        time.sleep(3)


def phase_kill(cfg, st, nodes):
    if st.get("kill_done"):
        log("kill: done already")
        return
    if len(nodes) < 2:
        log("kill: needs >= 2 nodes, skipped")
        return
    rate = cfg.kill_rate or kill_rate_default(cfg, st)
    victim = nodes[-1]

    def event(nodes, t0):
        ev = {"victim": victim.name}
        # kill once the measured window is kill_at s in
        time.sleep(max(0, t0 + cfg.warmup + cfg.kill_at - time.time()))
        ev["kill_t"] = time.time()
        victim.kill9()
        log(f"kill: {victim.name} killed (-9)")
        # watch the survivors take over the dead node's shards
        t = time.time()
        survivors = [n for n in nodes if n is not victim]
        while time.time() - t < cfg.kill_down:
            owned = sum(int((node_metrics(n.url) or {}).get("vlpds_owned_partitions", 0)) for n in survivors)
            if owned >= 256 and "takeover_s" not in ev:
                ev["takeover_s"] = round(time.time() - ev["kill_t"], 1)
            time.sleep(0.5)
        ev["restart_t"] = time.time()
        victim.start(timeout=600)
        ev["serving_t"] = time.time()
        c = wait_converged(nodes, timeout=120)
        ev["rejoin_converged_s"] = None if c is None else round(c, 1)
        log(f"kill: {victim.name} back after {ev['serving_t']-ev['kill_t']:.1f}s (takeover {ev.get('takeover_s')}s)")
        return {k: (round(v - t0, 1) if k.endswith("_t") else v) for k, v in ev.items()}

    # load enters through the survivors only (a load balancer drops the dead
    # node at once): errors are writes to the victim's shards until takeover
    rec = run_step(cfg, st, nodes, "kill9", rate, cfg.kill_duration, event=event, lg_nodes=nodes[:-1])
    st["kill_done"] = True
    st["kill_rate"] = rate
    save_state(cfg, st)
    return rec


def kill_rate_default(cfg, st):
    """60% of the highest clean stair (or of the lowest rate)."""
    clean = [r["rate"] for r in load_steps(cfg) if r["tag"].startswith("stair-") and not saturated(r)]
    return int(0.6 * max(clean)) if clean else int(0.6 * cfg.rates[0])


def load_steps(cfg):
    p = os.path.join(cfg.out, "steps.jsonl")
    return [json.loads(l) for l in open(p)] if os.path.exists(p) else []


# ---------------------------------------------------------------- report

def kill_timeline(rec):
    """Fleet ok/s per second (sum over loadgens) around the kill."""
    ws = rec["windows"]
    n = max((len(w) for w in ws), default=0)
    out = []
    for i in range(n):
        row = [w[i] for w in ws if i < len(w)]
        out.append({"t": row[0]["t"], "ok_s": sum(r["ok_s"] for r in row), "err": sum(r["err"] for r in row),
                    "p99": max(r["p99"] for r in row)})
    # per-second error deltas
    prev = 0
    for r in out:
        r["err_s"], prev = r["err"] - prev, r["err"]
    return out


def phase_report(cfg, st):
    steps = load_steps(cfg)
    pops = []
    p = os.path.join(cfg.out, "populate.jsonl")
    if os.path.exists(p):
        pops = [json.loads(l) for l in open(p)]
    summ = next((x for x in reversed(pops) if x.get("summary")), None)
    L = [f"# vlpds capacity test: {cfg.name}", ""]
    L.append(f"Driver: `bench/capacity/run.py` (`{' '.join(sys.argv[1:])}`). {cfg.nodes} {cfg.mode} nodes on one box "
             f"(ports {cfg.base_port}-{cfg.base_port + cfg.nodes - 1}), MinIO at {S3}, prefix `{cfg.prefix}`, binaries `{BIN}`.")
    L.append(f"Per node: `--workers {cfg.workers} --io-threads {cfg.io_threads} --block-cache-mb {cfg.block_cache_mb} "
             f"--repo-cache-mb {cfg.repo_cache_mb} --log-retention {cfg.log_retention} --slatedb-checkpoint-lifetime "
             f"{cfg.checkpoint_lifetime} --slatedb-gc-min-age {cfg.gc_min_age}` {' '.join(cfg.node_extra)}"
             f"{' --inject-put-ms ' + str(cfg.inject) if cfg.inject else ''}{' (SST disk cache on)' if cfg.cache_dir else ' (no SST disk cache)'}.")
    L.append("")
    L.append("## Population")
    L.append("")
    if summ:
        pl = summ["plan"]
        L.append(f"{cfg.total:,} accounts, records per repo `{cfg.dist_args_str}`: {summ['records']:,} records "
                 f"(mean {pl['mean']:.2f}, p50 {pl['p50']}, p90 {pl['p90']}, p99 {pl['p99']}, p99.9 {pl['p999']}, max {pl['max']}; "
                 f"{pl['zero_repos']:,} empty repos). Real network: mean 455 (508 over repos with records), p50 10, p99 9,803, max 593,772.")
        L.append("")
        du = summ.get("settled_du") or {}
        L.append("| Accounts | Records | Time | Accounts/s | Records/s | S3 bytes settled | Bytes/account | Settle |")
        L.append("|---|---|---|---|---|---|---|---|")
        L.append(f"| {cfg.total:,} | {summ['records']:,} | {summ['secs']:.0f} s | {summ['accounts_s']:,} | {summ['records_s']:,} | "
                 f"{fmt_gb(du.get('total'))} ({', '.join(f'{k} {v/1e9:.2f}' for k, v in sorted(du.items()) if k != 'total')}) | "
                 f"{(du.get('total') or 0)/cfg.total:.0f} | {summ['settle_s']} s |")
        L.append("")
    chunks = [x for x in pops if not x.get("summary")]
    if chunks:
        L.append("Per chunk: " + ", ".join(f"{c['accounts_s']:,}/s" for c in chunks[:20]) + (" ..." if len(chunks) > 20 else ""))
        L.append("")
    stairs = [r for r in steps if r["tag"].startswith("stair-")]
    if stairs:
        L.append(f"## Active window stairs ({cfg.active:,} active of {cfg.total:,}, churn {cfg.churn:g} repos/s)")
        L.append("")
        L.append("Open-loop, latency from the scheduled send; one loadgen per node at rate/N (not DID-routed: (N-1)/N forwarded), "
                 f"+{cfg.hot_rate}/s hot repo and a firehose consumer on n1. Window {cfg.warmup} s warmup + {cfg.duration} s measured.")
        L.append("")
        L.append("| Offered/s | Achieved/s | Err (warmup incl.) | Dropped | p50 | p99 | p99.9 | FH lag p50/p99 | Loads/s | Commits/s | CPU % per node | RSS GB per node | S3 req/s (PUT/GET) | S3 in/out MB/s | Seg PUT/s |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in stairs:
            s = r["summary"]
            fh = r.get("fh_lag") or {}
            L.append(f"| {r['rate'] + r['hot_rate']:,} | {r['achieved']:,} | {r.get('errors_measured', '-')} ({r['errors']}) | {r['dropped']} | {r['p50_max']} | {r['p99_max']} | "
                     f"{r['p999_max']} | {fh.get('p50', '-')}/{fh.get('p99', '-')} | {s.get('loads_s', '-')} | {s.get('commits_s', '-')} | "
                     f"{' / '.join(map(str, s.get('cpu_pct', [])))} | {' / '.join(map(str, s.get('rss_gb_max', [])))} | "
                     f"{s.get('s3_req_s', '-')} ({s.get('s3_put_s', '-')}/{s.get('s3_get_s', '-')}) | {s.get('s3_in_mb_s', '-')}/{s.get('s3_out_mb_s', '-')} | "
                     f"{s.get('seg_puts_s', '-')} |")
        L.append("")
        sat = st.get("stairs_saturated")
        L.append(f"Saturated at {sat:,}/s offered." if sat else "No stair saturated.")
        L.append("")
        cmp_ = {}
        for r in stairs:
            for k, v in (r["summary"].get("slatedb_s") or {}).items():
                if "compact" in k:
                    cmp_.setdefault(k, []).append(v)
        if cmp_:
            L.append("SlateDB compaction counters (per s, per stair): " + "; ".join(f"`{k}` {' / '.join(map(str, v))}" for k, v in sorted(cmp_.items())))
            L.append("")
    kills = [r for r in steps if r["tag"] == "kill9"]
    if kills:
        r = kills[-1]
        ev = r.get("event") or {}
        tl = kill_timeline(r)
        L.append(f"## kill -9 of {ev.get('victim')} at {r['rate']:,}/s")
        L.append("")
        tgt = r["rate"] + r["hot_rate"]
        low = [x for x in tl if x["ok_s"] < 0.5 * tgt]
        L.append(f"Killed at {ev.get('kill_t')} s (from loadgen start), restarted at {ev.get('restart_t')} s, serving at {ev.get('serving_t')} s; "
                 f"survivors owned every shard {ev.get('takeover_s') or f"> {cfg.kill_down}"} s after the kill; load entered through the survivors only; rejoin converged in {ev.get('rejoin_converged_s')} s. "
                 f"Measured window: achieved {r['achieved']:,}/s of {tgt:,}, {r['errors']:,} errors, p99<= {r['p99_max']} ms. "
                 f"Seconds below 50% of offered: {len(low)} ({', '.join(str(x['t']) for x in low[:30])}).")
        L.append("")
        L.append("| t (s) | ok/s | errors/s | p99 ms |")
        L.append("|---|---|---|---|")
        kt = ev.get("kill_t") or 0
        for x in tl:
            if kt - 5 <= x["t"] <= (ev.get("serving_t") or kt) + 15:
                L.append(f"| {x['t']} | {x['ok_s']:,} | {x['err_s']:,} | {x['p99']} |")
        L.append("")
    notes = os.path.join(cfg.out, "NOTES.md")
    if os.path.exists(notes):  # hand-written analysis, kept across regenerations
        L.append(open(notes).read().rstrip())
        L.append("")
    L.append("## Files")
    L.append("")
    L.append("`populate.jsonl` (per chunk + summary), `steps.jsonl` (per step: loadgen results, 1 s windows, metric summary), "
             "`metrics.jsonl` (1 s scrape of every node + MinIO; prefix du every 30 s). Regenerate: `bench/capacity/run.py report <same flags>`.")
    path = os.path.join(cfg.out, "RESULTS.md")
    open(path, "w").write("\n".join(L) + "\n")
    log(f"report: {path}")


# ---------------------------------------------------------------- cleanup

def phase_cleanup(cfg):
    for i in range(cfg.nodes):
        subprocess.run(["docker", "rm", "-f", f"vlpds-cap-{cfg.name}-n{i+1}"], capture_output=True)
    t = time.time()
    d = prefix_dir(cfg.prefix)
    if d and cfg.prefix and "/" not in cfg.prefix and ".." not in cfg.prefix:
        shutil.rmtree(d, ignore_errors=True)
        log(f"cleanup: deleted {d} in {time.time()-t:.0f}s")
    else:
        subprocess.run(["aws", "--endpoint-url", S3, "s3", "rm", "--recursive", "--only-show-errors", f"s3://{BUCKET}/{cfg.prefix}/"],
                       env=dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_DEFAULT_REGION="us-east-1"),
                       check=False)
        log(f"cleanup: s3 rm s3://{BUCKET}/{cfg.prefix}/ in {time.time()-t:.0f}s")
    purge_trash()
    shutil.rmtree(cfg.state_dir, ignore_errors=True)
    log(f"cleanup: removed {cfg.state_dir}; disk free {disk_free_gb():.0f} GB")


# ---------------------------------------------------------------- main

def config(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["plan", "all", "populate", "stairs", "kill", "report", "status", "cleanup"])
    ap.add_argument("--name", default="", help="run name: state dir, prefix, containers (default cap-<total>-<nodes>n)")
    ap.add_argument("--out", default="", help="results dir (default BENCH_OUT_DIR or bench/results/capacity-<date>)")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--mode", choices=["native", "docker"], default="native")
    ap.add_argument("--base-port", type=int, default=2700)
    ap.add_argument("--total", type=int, default=1_000_000)
    ap.add_argument("--dist", choices=["real", "fixed"], default="real")
    ap.add_argument("--records", type=int, default=5, help="--dist fixed: records per repo")
    ap.add_argument("--dist-scale", type=float, default=128)
    ap.add_argument("--dist-knee", type=int, default=2)
    ap.add_argument("--dist-group", type=int, default=1, help="accounts sharing one records draw (1 = independent)")
    ap.add_argument("--chunk", type=int, default=0, help="accounts per checkpointed bulk chunk (default total/20, 100k..5M)")
    ap.add_argument("--bulk-batch", type=int, default=1000)
    ap.add_argument("--bulk-concurrency", type=int, default=16)
    ap.add_argument("--settle-s", type=int, default=300, help="max wait for the prefix size to settle after the bulk")
    ap.add_argument("--active", type=int, default=500_000)
    ap.add_argument("--churn", type=float, default=-1, help="window advance, repos/s (default active/100)")
    ap.add_argument("--rates", default="10000,25000,50000,75000,100000")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=15)
    ap.add_argument("--hot-rate", type=int, default=200)
    ap.add_argument("--max-inflight", type=int, default=20000)
    ap.add_argument("--kill-rate", type=int, default=0, help="default 60%% of the best clean stair")
    ap.add_argument("--kill-at", type=int, default=20, help="s into the measured window")
    ap.add_argument("--kill-down", type=int, default=20, help="s before the killed node restarts")
    ap.add_argument("--kill-duration", type=int, default=90)
    ap.add_argument("--inject", type=float, default=0, help="--inject-put-ms on every node")
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--io-threads", type=int, default=0)
    ap.add_argument("--mem-gb", type=float, default=0, help="RAM budget for all nodes' caches (default 60%% of RAM)")
    ap.add_argument("--block-cache-mb", type=int, default=0)
    ap.add_argument("--repo-cache-mb", type=int, default=0)
    ap.add_argument("--cache-budget-mb", type=int, default=512)
    ap.add_argument("--log-retention", default="5m", help="segments kept for firehose backfill (bulk + stairs write ~0.6 KB/account and ~2-5 KB/commit)")
    ap.add_argument("--checkpoint-lifetime", default="2m")
    ap.add_argument("--gc-min-age", default="2m")
    ap.add_argument("--lease-ttl-ms", type=int, default=10000)
    ap.add_argument("--cache-dir", action="store_true", help="SST disk cache per node (counts against disk)")
    ap.add_argument("--loadgen-threads", type=int, default=4)
    ap.add_argument("--scrape-s", type=float, default=1.0)
    ap.add_argument("--cleanup", action="store_true", help="`all`: delete the population at the end")
    ap.add_argument("--skip-kill", action="store_true")
    c = ap.parse_args(argv)
    c.name = c.name or f"cap-{c.total}-{c.nodes}n"
    c.prefix = c.name
    c.state_dir = os.path.join(SCRATCH, c.name)
    os.makedirs(c.state_dir, exist_ok=True)
    c.out = c.out or os.environ.get("BENCH_OUT_DIR") or os.path.join(PKG, "bench", "results", f"capacity-{time.strftime('%Y-%m-%d')}")
    os.makedirs(c.out, exist_ok=True)
    c.rates = [int(x) for x in c.rates.split(",") if x]
    c.churn = c.active / 100 if c.churn < 0 else c.churn
    c.chunk = c.chunk or max(100_000, min(5_000_000, c.total // 20))
    cores = os.cpu_count() or 8
    # share the box: N nodes + loadgens + MinIO
    c.io_threads = c.io_threads or max(2, cores // (c.nodes + 1))
    c.workers = c.workers or max(2, cores // (2 * (c.nodes + 1)))
    try:
        ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9
    except (ValueError, OSError):
        ram = 32
    mem = c.mem_gb or 0.6 * ram
    per = mem * 1024 / c.nodes
    c.block_cache_mb = c.block_cache_mb or int(per * 0.35)
    c.repo_cache_mb = c.repo_cache_mb or int(per * 0.35)
    c.node_extra = os.environ.get("NODE_EXTRA", "").split()
    c.dist_args_str = (f"real/scale={c.dist_scale:g}/knee={c.dist_knee}/group={c.dist_group}" if c.dist == "real" else f"fixed/{c.records}")
    return c


def main():
    cfg = config(sys.argv[1:])
    if cfg.cmd == "plan":
        p = plan(cfg)
        print(json.dumps(p, indent=1))
        return
    if cfg.cmd == "cleanup":
        phase_cleanup(cfg)
        return
    st = load_state(cfg)
    save_state(cfg, st)
    if cfg.cmd == "status":
        print(json.dumps(st, indent=1))
        return
    if cfg.cmd == "report":
        phase_report(cfg, st)
        return
    check_disk()
    log(f"{cfg.cmd}: {cfg.name}: {cfg.nodes} {cfg.mode} nodes, {cfg.total} accounts ({cfg.dist_args_str}), active {cfg.active} churn {cfg.churn:g}/s, "
        f"rates {cfg.rates}; per node workers {cfg.workers} io {cfg.io_threads} block cache {cfg.block_cache_mb} MB repo cache {cfg.repo_cache_mb} MB; "
        f"minio data {MINIO_DATA or '?'}; out {cfg.out}")
    nodes = [Node(cfg, i) for i in range(cfg.nodes)]
    stopping = []

    def on_term(sig, frm):
        stopping.append(sig)
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_term)
    try:
        t = time.time()
        for n in nodes:
            n.start(timeout=900)
        log(f"nodes up in {time.time()-t:.1f}s")
        st.setdefault("startups", []).append({"t": t, "up_s": round(time.time() - t, 1), "converge_s": wait_converged(nodes, timeout=300)})
        save_state(cfg, st)
        with Scraper(cfg, nodes, os.path.join(cfg.out, "metrics.jsonl")) as scr:
            if cfg.cmd in ("all", "populate"):
                phase_populate(cfg, st, nodes, scr)
            if cfg.cmd in ("all", "stairs"):
                if not st.get("populate", {}).get("done"):
                    raise SystemExit("stairs: population not done")
                phase_stairs(cfg, st, nodes)
            if cfg.cmd == "kill" or (cfg.cmd == "all" and not cfg.skip_kill):
                phase_kill(cfg, st, nodes)
    finally:
        t = time.time()
        for n in nodes:
            try:
                n.stop()
            except Exception as e:
                log(f"stop {n.name}: {e}")
        log(f"nodes stopped in {time.time()-t:.1f}s")
        save_state(cfg, st)
        try:
            phase_report(cfg, st)
        except Exception as e:
            log(f"report failed: {e}")
    if cfg.cmd == "all" and cfg.cleanup:
        phase_cleanup(cfg)


if __name__ == "__main__":
    main()
