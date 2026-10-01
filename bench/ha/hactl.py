#!/usr/bin/env python3
"""vlpds HA end-to-end harness: multi-node clusters on native processes with
fault injection, continuous load, the sync-1.1 checker, and acked-write
verification.

    python3 bench/ha/hactl.py list
    python3 bench/ha/hactl.py run baseline-3 kill9-1of3 ...
    python3 bench/ha/hactl.py run all

Every node gets two faultproxy instances (bench/ha/faultproxy):
  * an HTTP proxy in front of MinIO (its S3 endpoint), so S3 faults hit one node
  * a TCP proxy in front of its port that it *advertises* to peers, so peer
    traffic (request forwarding, partition streams) can be cut or slowed
    while clients (loadgen, checker) still reach it directly.

Outputs land in bench/ha/out/<run-id>/<scenario>/ (node logs, loadgen logs,
checker output, probe CSV, result.json); a summary table is appended to
bench/ha/out/<run-id>/summary.md.
"""

import atexit
import csv
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, "..", ".."))
BIN_DIR = os.environ.get("VLPDS_BIN_DIR", os.path.join(PKG, "target", "agent-ha", "dev-release"))
VLPDS = os.path.join(BIN_DIR, "vlpds")
LOADGEN = os.path.join(BIN_DIR, "loadgen")
CHECKER = os.environ.get("VLPDS_CHECKER", os.path.join(PKG, "checker", "checker"))
FAULTPROXY = os.path.join(HERE, "faultproxy", "faultproxy")
FHAUDIT = os.path.join(HERE, "fhaudit", "fhaudit")
S3 = os.environ.get("VLPDS_HA_S3", "127.0.0.1:9200")
ADMIN = "dev-admin-token"
# x-vlpds-internal (node-to-node / status) token: VLPDS_INTERNAL_TOKEN on the
# nodes; dev default. Older builds took the admin token there (dev mode only).
INTERNAL = os.environ.get("VLPDS_HA_INTERNAL_TOKEN", "dev-internal-token")
PARTITIONS = int(os.environ.get("VLPDS_HA_PARTITIONS", "64"))  # shards
TTL_MS = int(os.environ.get("VLPDS_HA_TTL_MS", "3000"))
RATE = float(os.environ.get("VLPDS_HA_RATE", "150"))  # writes/s per loadgen (one per node)
# Node command line (after the binary). Placeholders: {listen} {url} {advertise}
# {s3} {prefix} {id} {ttl_ms} {partitions}. Override for other designs/flags.
NODE_ARGS = os.environ.get(
    "VLPDS_HA_NODE_ARGS",
    "--listen {listen} --public-url {url} --advertise-url {advertise} --s3-endpoint {s3} --prefix {prefix} "
    "--node-id {id} --lease-ttl-ms {ttl_ms} --shards {partitions} --no-rate-limits --dev-mode --workers 2 "
    "--io-threads 3 --firehose-ring-mb 256",
)
BASE_PORT = int(os.environ.get("VLPDS_HA_BASE_PORT", "7100"))
# Ownership observation (optional): the /internal/v1/cluster status endpoint if
# the build has one, else the first of these gauges a node exports. If neither
# exists, convergence is not observed (scenarios still run; verdicts rest on
# verify / checker / firehose audits).
OWNED_METRICS = os.environ.get("VLPDS_HA_OWNED_METRICS", "vlpds_owned_shards,vlpds_owned_partitions").split(",")

PROCS = []


def cleanup():
    for p in PROCS:
        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                pass


atexit.register(cleanup)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def spawn(args, out_path, env=None):
    f = open(out_path, "ab")
    e = dict(os.environ)
    if env:
        e.update(env)
    p = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT, env=e, start_new_session=True)
    PROCS.append(p)
    return p


def http(method, url, body=None, headers=None, timeout=10.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("content-type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, raw


def partition_of(did, n=PARTITIONS):
    """Shard of a DID: fixed 65,536 hash slots (top 16 bits of sha256(did)),
    uniform contiguous ranges per shard (src/slots.rs)."""
    slot = int.from_bytes(hashlib.sha256(did.encode()).digest()[:2], "big")
    return slot * n // 65536


# probe writers: one account per shard, capped (each probes at 10 Hz)
PROBES = int(os.environ.get("VLPDS_HA_PROBES", "32"))
# delete each scenario's bucket prefix once its results are recorded
CLEANUP = os.environ.get("VLPDS_HA_CLEANUP", "1") == "1"
MINIO_IMAGE = os.environ.get("VLPDS_HA_MINIO_IMAGE", "vlpds-minio:local")


def delete_prefix(prefix):
    """Removes every object under `prefix` (disk hygiene between scenarios)."""
    host = S3 if not S3.startswith("127.0.0.1") else S3.replace("127.0.0.1", "host.docker.internal")
    r = subprocess.run(["docker", "run", "--rm", "--entrypoint", "sh", MINIO_IMAGE, "-c",
                        f"mc alias set n http://{host} minioadmin minioadmin >/dev/null && mc rm -r --force n/vlpds/{prefix} >/dev/null"],
                       capture_output=True, text=True)
    return r.returncode == 0


# --------------------------------------------------------------------------- nodes


class Proxy:
    def __init__(self, mode, listen, target, ctl, out):
        self.ctl = ctl
        self.proc = spawn([FAULTPROXY, "-mode", mode, "-listen", listen, "-target", target, "-ctl", ctl], out)

    def set(self, **kw):
        q = "&".join(f"{k}={v}" for k, v in kw.items())
        http("GET", f"http://{self.ctl}/set?{q}")

    def clear(self):
        http("GET", f"http://{self.ctl}/clear")

    def reset(self):
        http("GET", f"http://{self.ctl}/reset")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()


class Node:
    def __init__(self, idx, prefix, outdir, extra=None, env=None):
        self.idx = idx
        self.id = f"n{idx}"
        self.port = BASE_PORT + idx
        self.url = f"http://127.0.0.1:{self.port}"
        self.prefix = prefix
        self.outdir = outdir
        self.extra = extra or []
        self.env = env or {}
        self.proc = None
        self.exits = []  # (time, returncode)
        self.s3 = Proxy("http", f"127.0.0.1:{BASE_PORT + 2300 + idx}", S3, f"127.0.0.1:{BASE_PORT + 2500 + idx}", os.path.join(outdir, f"{self.id}.s3proxy.log"))
        self.peer = Proxy("tcp", f"127.0.0.1:{BASE_PORT + 200 + idx}", f"127.0.0.1:{self.port}", f"127.0.0.1:{BASE_PORT + 400 + idx}", os.path.join(outdir, f"{self.id}.peerproxy.log"))
        self.advertise = f"http://127.0.0.1:{BASE_PORT + 200 + idx}"

    def start(self):
        args = [VLPDS] + NODE_ARGS.format(
            listen=f"127.0.0.1:{self.port}", url=self.url, advertise=self.advertise,
            s3=f"http://127.0.0.1:{BASE_PORT + 2300 + self.idx}", prefix=self.prefix, id=self.id,
            ttl_ms=TTL_MS, partitions=PARTITIONS).split() + self.extra
        env = {"RUST_LOG": "info,slatedb=warn", "VLPDS_NO_RATE_LIMITS": "true"}  # load tests
        env.update(self.env)
        self.proc = spawn(args, os.path.join(self.outdir, f"{self.id}.log"), env)
        self.started_at = time.time()
        return self

    def alive(self):
        if self.proc is None:
            return False
        rc = self.proc.poll()
        if rc is not None and (not self.exits or self.exits[-1][2] is not self.proc):
            self.exits.append((time.time(), rc, self.proc))
        return rc is None

    def exit_codes(self):
        self.alive()
        return [rc for _, rc, _ in self.exits]

    def signal(self, sig):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, sig)

    def wait_exit(self, timeout=30):
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.alive()

    def status(self, timeout=2.0):
        try:
            _, raw = http("GET", self.url + "/internal/v1/cluster", headers={"x-vlpds-internal": INTERNAL}, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code not in (401, 403):
                raise
            _, raw = http("GET", self.url + "/internal/v1/cluster", headers={"x-vlpds-internal": ADMIN}, timeout=timeout)
        return json.loads(raw)

    def metrics(self):
        _, raw = http("GET", self.url + "/metrics", timeout=3)
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

    def ready(self):
        try:
            http("GET", self.url + "/xrpc/_health", timeout=1)
            return True
        except Exception:
            return False

    def teardown(self):
        self.signal(signal.SIGKILL)
        self.s3.stop()
        self.peer.stop()
        if self.proc:
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                pass
            self.alive()


# ---- containers: real network partitions (docker network disconnect), docker
# pause, and per-node clock skew via libfaketime (LD_PRELOAD, realtime only).

DOCKER_NET = os.environ.get("VLPDS_HA_DOCKER_NET", "vlpds-ha")
DOCKER_IMAGE = os.environ.get("VLPDS_HA_IMAGE", "vlpds-ha:local")
DOCKER_S3 = os.environ.get("VLPDS_HA_DOCKER_S3", "http://host.docker.internal:9200")
FAKETIME_LIB = "/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1" if os.uname().machine in ("arm64", "aarch64") \
    else "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1"


def docker(*args, check=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout.strip()


class _NoProxy:
    def set(self, **kw):
        raise RuntimeError("no proxy in container mode")

    clear = reset = stop = lambda self: None


class CNode(Node):
    """A node in a container on DOCKER_NET. `skew` (e.g. "+2.5s") offsets its
    wall clock with libfaketime; monotonic time (lease validity) is untouched."""

    skews = {}

    def __init__(self, idx, prefix, outdir, extra=None, env=None):
        self.idx = idx
        self.id = f"n{idx}"
        self.port = BASE_PORT + 600 + idx
        self.url = f"http://127.0.0.1:{self.port}"
        self.prefix, self.outdir, self.extra = prefix, outdir, extra or []
        self.env = dict(env or {})
        self.name = f"vha-{self.id}"
        self.exits = []
        self.proc = None
        self.s3 = self.peer = _NoProxy()
        self.advertise = f"http://{self.name}:2583"
        self.skew = CNode.skews.get(self.id)
        self.started_at = 0
        self.runs = 0

    def start(self):
        docker("rm", "-f", self.name, check=False)
        docker("network", "create", DOCKER_NET, check=False)
        args = NODE_ARGS.format(listen="0.0.0.0:2583", url=self.url, advertise=self.advertise, s3=DOCKER_S3,
                                prefix=self.prefix, id=self.id, ttl_ms=TTL_MS, partitions=PARTITIONS).split() + self.extra
        env = []
        for k, v in self.env.items():
            env += ["-e", f"{k}={v}"]
        if self.skew:
            env += ["-e", f"LD_PRELOAD={FAKETIME_LIB}", "-e", f"FAKETIME={self.skew}", "-e", "DONT_FAKE_MONOTONIC=1"]
        docker("run", "-d", "--name", self.name, "--network", DOCKER_NET, "-p", f"127.0.0.1:{self.port}:2583",
               "--cpus", "2", *env, DOCKER_IMAGE, *args)
        self.runs += 1
        self.started_at = time.time()
        self._logpump()
        return self

    def _logpump(self):
        f = open(os.path.join(self.outdir, f"{self.id}.log"), "ab")
        p = subprocess.Popen(["docker", "logs", "-f", self.name], stdout=f, stderr=subprocess.STDOUT)
        PROCS.append(p)

    def alive(self):
        st = docker("inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", self.name, check=False)
        if not st:
            return False
        status, code = st.split()
        if status in ("exited", "dead"):
            if len(self.exits) < self.runs:
                self.exits.append((time.time(), int(code), self.runs))
            return False
        return True

    def signal(self, sig):
        if sig == signal.SIGSTOP:
            docker("pause", self.name, check=False)
        elif sig == signal.SIGCONT:
            docker("unpause", self.name, check=False)
        else:
            docker("kill", "-s", signal.Signals(sig).name, self.name, check=False)

    def wait_exit(self, timeout=30):
        end = time.time() + timeout
        while time.time() < end and self.alive():
            time.sleep(0.2)
        if self.alive():
            docker("kill", self.name, check=False)
            time.sleep(0.5)
        self.alive()

    def disconnect(self):
        docker("network", "disconnect", DOCKER_NET, self.name)

    def connect(self):
        docker("network", "connect", DOCKER_NET, self.name)

    def teardown(self):
        self.alive()
        docker("rm", "-f", self.name, check=False)


def wait_ready(nodes, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        if all(n.ready() for n in nodes):
            return
        time.sleep(0.2)
    raise RuntimeError("nodes not ready")


def ownership(nodes):
    """{node id: owned partition ids} for nodes that answer. Uses the
    /internal/v1/cluster status endpoint when the build has one; otherwise
    falls back to the vlpds_owned_partitions gauge ({id: count})."""
    out = {}
    for n in nodes:
        if not n.alive():
            continue
        try:
            out[n.id] = n.status()["owned"]
        except urllib.error.HTTPError:
            try:
                m = n.metrics()
                for name in OWNED_METRICS:
                    if name in m:
                        out[n.id] = int(m[name])
                        break
            except Exception:
                pass
        except Exception:
            pass
    return out


def converged(nodes, expect_nodes=None):
    """Every partition owned exactly once by a live node (and, optionally,
    spread over `expect_nodes` nodes within fair share). With only counts
    (no status endpoint) this checks the counts sum to PARTITIONS."""
    own = ownership(nodes)
    if not own:
        # ownership not observable on this build: treat "all live nodes healthy" as converged
        live = [n for n in nodes if n.alive()]
        return bool(live) and all(n.ready() for n in live), {"unobserved": True}
    if all(isinstance(v, int) for v in own.values()):
        ok = sum(own.values()) == PARTITIONS and (not expect_nodes or len([1 for v in own.values() if v]) >= min(expect_nodes, PARTITIONS))
        return ok, own
    seen = {}
    for nid, ps in own.items():
        for p in ps:
            if p in seen:
                return False, own
            seen[p] = nid
    if len(seen) != PARTITIONS:
        return False, own
    if expect_nodes:
        fair = -(-PARTITIONS // expect_nodes)
        if any(len(ps) > fair for ps in own.values()) or len([1 for ps in own.values() if ps]) < min(expect_nodes, PARTITIONS):
            return False, own
    return True, own


def wait_converged(nodes, expect_nodes=None, timeout=60):
    t0 = time.time()
    own = {}
    while time.time() - t0 < timeout:
        ok, own = converged(nodes, expect_nodes)
        if ok:
            return time.time() - t0, own
        time.sleep(0.2)
    return None, own


# --------------------------------------------------------------------------- load


def setup_accounts(nodes, outdir, per_node=40, records=3, tag="a"):
    files = []
    procs = []
    for n in nodes:
        f = os.path.join(outdir, f"accounts-{n.id}.json")
        files.append(f)
        procs.append(spawn([LOADGEN, "--host", n.url, "--accounts-file", f, "setup", "--accounts", str(per_node),
                            "--records", str(records), "--concurrency", "16", "--prefix", f"{tag}{n.id}x"],
                           os.path.join(outdir, f"setup-{n.id}.log")))
    for p in procs:
        if p.wait() != 0:
            raise RuntimeError("loadgen setup failed (see setup-*.log)")
    accts = []
    for f in files:
        accts += json.load(open(f))
    path = os.path.join(outdir, "accounts.json")
    json.dump(accts, open(path, "w"))
    return path, accts


class Loadgen:
    def __init__(self, node, accounts_file, outdir, duration, rate=RATE, tag=""):
        self.node = node
        self.log = os.path.join(outdir, f"loadgen-{node.id}{tag}.log")
        self.acked = os.path.join(outdir, f"acked-{node.id}{tag}.json")
        self.rate = rate
        self.t0 = time.time()
        self.proc = spawn([LOADGEN, "--host", node.url, "--accounts-file", accounts_file, "--threads", "2", "run",
                           "--rate", str(rate), "--duration", str(duration), "--warmup", "0",
                           "--update-pct", "0", "--delete-pct", "0", "--max-inflight", "4000",
                           "--acked-out", self.acked], self.log)

    def wait(self, timeout=None):
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait()

    def windows(self):
        """[(t_end_s, ok_per_s, err_delta, p50_ms, p99_ms, max_ms)] from the 5 s lines."""
        out, last_err = [], 0
        for line in open(self.log, errors="replace"):
            if not line.startswith("[") or "ok/s" not in line:
                continue
            try:
                t = float(line[1:line.index("s]")])
                parts = line.split()
                ok = float(parts[parts.index("ok/s") + 1])
                err = int(parts[parts.index("err") + 1])
                p50 = float(parts[parts.index("p50") + 1].rstrip("ms"))
                p99 = float(parts[parts.index("p99") + 1].rstrip("ms"))
                mx = float(parts[parts.index("max") + 1].rstrip("ms"))
            except (ValueError, IndexError):
                continue
            out.append((t, ok, err - last_err, p50, p99, mx))
            last_err = err
        return out

    def summary(self):
        txt = open(self.log, errors="replace").read()
        res = {"node": self.node.id}
        for line in txt.splitlines():
            if line.startswith("target "):
                res["result"] = line.strip()
            if line.startswith("all "):
                res["latency"] = line.strip()
            if line.startswith("first error"):
                res["first_error"] = line.strip()[:300]
        return res


class Prober:
    """Writes to one account per partition through `node` every `interval`,
    recording each outcome: a precise per-partition availability timeline."""

    def __init__(self, nodes, accts, outdir, interval=0.1):
        self.nodes = nodes  # candidates (first alive one is used)
        self.interval = interval
        self.outdir = outdir
        self.rows = []
        self.acked = {}
        self.stop_ev = threading.Event()
        by_p = {}
        for a in accts:
            by_p.setdefault(partition_of(a["did"]), a)
        # sample shards across all nodes: accounts come grouped by the node that
        # minted them (on its own shards), so "the first N" probed only n1/n2
        picks = list(by_p.values())
        random.Random(1).shuffle(picks)
        self.accts = picks[:PROBES]
        self.tokens = {}
        self.lock = threading.Lock()
        self.threads = [threading.Thread(target=self.loop, args=(a,), daemon=True) for a in self.accts]

    def target(self):
        for n in self.nodes:
            if n.alive():
                return n
        return self.nodes[0]

    def session(self, a, n):
        _, raw = http("POST", n.url + "/xrpc/com.atproto.server.createSession", {"identifier": a["did"], "password": "hunter2"})
        return json.loads(raw)["accessJwt"]

    def loop(self, a):
        p = partition_of(a["did"])
        i = 0
        while not self.stop_ev.is_set():
            n = self.target()
            t0 = time.time()
            ok, code, err = False, 0, ""
            try:
                if a["did"] not in self.tokens:
                    self.tokens[a["did"]] = self.session(a, n)
                i += 1
                _, raw = http("POST", n.url + "/xrpc/com.atproto.repo.createRecord",
                              {"repo": a["did"], "collection": "app.bsky.feed.post",
                               "record": {"$type": "app.bsky.feed.post", "text": f"probe {i}", "createdAt": "2026-09-30T00:00:00Z"}},
                              headers={"authorization": "Bearer " + self.tokens[a["did"]]}, timeout=15)
                rkey = json.loads(raw)["uri"].rsplit("/", 1)[1]
                with self.lock:
                    self.acked.setdefault(a["did"], []).append(rkey)
                ok, code = True, 200
            except urllib.error.HTTPError as e:
                code = e.code
                err = e.read()[:200].decode(errors="replace")
            except Exception as e:
                err = str(e)[:200]
            t1 = time.time()
            with self.lock:
                self.rows.append((t0, t1, p, n.id, ok, code, err))
            time.sleep(max(0.0, self.interval - (t1 - t0)))

    def start(self):
        for t in self.threads:
            t.start()
        return self

    def stop(self):
        self.stop_ev.set()
        for t in self.threads:
            t.join(20)
        with open(os.path.join(self.outdir, "probe.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_send", "t_done", "partition", "via", "ok", "status", "error"])
            w.writerows(sorted(self.rows))
        path = os.path.join(self.outdir, "acked-probe.json")
        json.dump(self.acked, open(path, "w"))
        return path

    def analyze(self, fault_at, slow_s=2.0, t0=None):
        self.t0 = t0 or fault_at
        """Unavailability = time covered by failed or slow (> slow_s) probes
        after `fault_at`; recovery = last bad probe end - fault_at."""
        bad = [(t0, t1) for (t0, t1, p, _, ok, _, _) in self.rows if t0 >= fault_at - 0.5 and (not ok or t1 - t0 > slow_s)]
        errors = sum(1 for r in self.rows if not r[4])
        total = len(self.rows)
        # per shard: longest *contiguous* outage (bad probes less than 1 s
        # apart merge), so a takeover and a later rebalance blip on the same
        # shard count as two windows, not one long one
        per_p_rows = {}
        for (t0, t1, p, _, ok, _, _) in self.rows:
            if t0 >= fault_at - 0.5 and (not ok or t1 - t0 > slow_s):
                per_p_rows.setdefault(p, []).append((t0, t1))
        per_p = {}
        for p, rs in per_p_rows.items():
            rs.sort()
            wins = []
            for a, b in rs:
                if wins and a <= wins[-1][1] + 1.0:
                    wins[-1][1] = max(wins[-1][1], b)
                else:
                    wins.append([a, b])
            per_p[p] = max(wins, key=lambda w: w[1] - w[0])
        if not bad:
            return {"probes": total, "probe_errors": errors, "unavail_s": 0.0, "recovery_s": 0.0, "partitions_hit": 0,
                    "max_partition_outage_s": 0.0, "windows": []}
        bad.sort()
        merged = []
        for s, e in bad:
            if merged and s <= merged[-1][1] + self.interval * 2:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        return {
            "probes": total,
            "probe_errors": errors,
            "unavail_s": round(sum(e - s for s, e in merged), 2),
            "recovery_s": round(max(e for _, e in bad) - fault_at, 2),
            "partitions_hit": len(per_p),
            "max_partition_outage_s": round(max(e - s for s, e in per_p.values()), 2),
            # every outage window, relative to the scenario start: [start, end, failed probes]
            "windows": [[round(s - self.t0, 1), round(e - self.t0, 1),
                         sum(1 for r in self.rows if s <= r[0] <= e and not r[4])] for s, e in merged],
        }


class Checker:
    def __init__(self, node, outdir, tag="", cursor=None):
        self.node = node
        self.out = os.path.join(outdir, f"checker-{node.id}{tag}.log")
        args = [CHECKER, "-host", node.url, "-quiet", "-strict", "-workers", "4"]
        if cursor is not None:
            args += ["-cursor", str(cursor)]
        self.proc = spawn(args, self.out)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        return self.result()

    def result(self):
        txt = open(self.out, errors="replace").read()
        res = {"node": self.node.id, "rc": self.proc.poll()}
        for line in txt.splitlines():
            if line.startswith("RESULT:"):
                res["result"] = line.split()[1]
            elif line.startswith("events:"):
                res["events"] = int(line.split()[1])
            elif line.startswith("  #commit:"):
                res["commits"] = int(line.split()[1])
            elif line.startswith("failures:"):
                res["failures"] = line.split(None, 1)[1]
            elif line.startswith("=== vlpds firehose checker summary"):
                res["ended"] = line.strip("= \n")[len("vlpds firehose checker summary "):]
            elif line.startswith("seq range:"):
                res["last_seq"] = int(line.split()[-1])
        fails = [l for l in txt.splitlines() if l.startswith("FAIL #")][:5]
        if fails:
            res["first_failures"] = fails
        return res


class FhAudit:
    """Firehose completeness: records every create seen on a node's
    subscribeRepos, so we can require every acked create to appear."""

    def __init__(self, node, outdir, tag="", cursor=None):
        self.node = node
        self.out = os.path.join(outdir, f"fhaudit-{node.id}{tag}.json")
        args = [FHAUDIT, "-host", node.url, "-out", self.out]
        if cursor is not None:
            args += ["-cursor", str(cursor)]
        self.proc = spawn(args, os.path.join(outdir, f"fhaudit-{node.id}{tag}.log"))

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        try:
            return json.load(open(self.out))
        except Exception:
            return None


def audit_report(data, acked, node):
    """Compare an audit's creates against the acked set."""
    if data is None:
        return {"node": node, "error": "no output"}
    seen = {d: set(v) for d, v in data["seen"].items()}
    missing = 0
    missing_dids = set()
    for did, rkeys in acked.items():
        s = seen.get(did, set())
        for k in rkeys:
            if k not in s:
                missing += 1
                missing_dids.add(did)
    return {"node": node, "commits": data["events"], "fh_missing": missing, "dids_affected": len(missing_dids),
            "reorders": data["reorders"], "dups": data["dups"], "infos": data.get("infos"), "last_time": data["last_time"], "end": data["end"],
            "first_seq": data["first_seq"], "last_seq": data["last_seq"]}


def history_diff(datas, common_range=False):
    """Cross-node agreement of the merged firehose (old bug B5): every node
    must emit the same commits, (seq, did, rev), in the same order. With
    `common_range` only the seq range every node covered is compared (live
    audits start at slightly different points)."""
    logs = {n: [tuple(c) if isinstance(c, list) else (c["s"], c["d"], c["r"]) for c in (d or {}).get("commits") or []]
            for n, d in datas.items() if d}
    if len(logs) < 2:
        return {"compared": list(logs), "agree": True}
    lo = max(l[0][0] for l in logs.values() if l) if common_range and all(logs.values()) else None
    hi = min(l[-1][0] for l in logs.values() if l) if common_range and all(logs.values()) else None
    if lo is not None:
        logs = {n: [c for c in l if lo <= c[0] <= hi] for n, l in logs.items()}
    names = sorted(logs)
    ref = names[0]
    out = {"compared": names, "range": [lo, hi] if lo is not None else None, "agree": True, "pairs": {}}
    rs = set(logs[ref])
    for n in names[1:]:
        ns = set(logs[n])
        only_ref, only_n = sorted(rs - ns), sorted(ns - rs)
        same_order = logs[ref] == logs[n]
        out["pairs"][f"{ref}-{n}"] = {"len": [len(logs[ref]), len(logs[n])], f"only_{ref}": len(only_ref), f"only_{n}": len(only_n),
                                       "same_order": same_order, f"sample_only_{ref}": only_ref[:5], f"sample_only_{n}": only_n[:5]}
        if only_ref or only_n or not same_order:
            out["agree"] = False
    return out


def load_acked(files):
    merged = {}
    for f in files:
        if os.path.exists(f):
            try:
                for did, rkeys in json.load(open(f)).items():
                    merged.setdefault(did, []).extend(rkeys)
            except Exception:
                pass
    return merged


def verify(acked_files, node, outdir):
    """Merge acked files and verify every acked create is readable via `node`."""
    merged = {}
    for f in acked_files:
        if not os.path.exists(f):
            continue
        try:
            for did, rkeys in json.load(open(f)).items():
                merged.setdefault(did, []).extend(rkeys)
        except Exception as e:
            log(f"bad acked file {f}: {e}")
    path = os.path.join(outdir, "acked-all.json")
    json.dump(merged, open(path, "w"))
    total = sum(len(v) for v in merged.values())
    r = subprocess.run([LOADGEN, "--host", node.url, "verify", "--acked", path], capture_output=True, text=True)
    open(os.path.join(outdir, "verify.log"), "w").write(r.stdout + r.stderr)
    missing = None
    for line in r.stdout.splitlines():
        if line.startswith("verify:"):
            missing = int(line.split(",")[1].split()[0])
    return {"acked": total, "missing": missing, "ok": r.returncode == 0}


# --------------------------------------------------------------------------- scenarios


@dataclass
class Ctx:
    name: str
    outdir: str
    prefix: str
    nodes: list = field(default_factory=list)
    events: list = field(default_factory=list)  # (t, what)
    t0: float = 0.0
    factory: object = None  # Node class (native processes) or CNode (containers)

    def __post_init__(self):
        self.factory = self.factory or Node

    def mark(self, what):
        t = time.time()
        self.events.append((round(t - self.t0, 2), what))
        log(f"  [{self.name} +{t - self.t0:5.1f}s] {what}")
        return t


def make_cluster(ctx, n, start=True, extra=None, env=None):
    nodes = [ctx.factory(i + 1, ctx.prefix, ctx.outdir, extra=extra, env=env) for i in range(n)]
    time.sleep(0.3)
    if start:
        for nd in nodes:
            nd.start()
        wait_ready(nodes)
    ctx.nodes = nodes
    return nodes


def teardown(ctx):
    for n in ctx.nodes:
        n.teardown()


def run_load_scenario(ctx, n_nodes, duration, actions, checker_on=0, expect_final=None, per_node=30, rate=RATE,
                      extra_checkers=None, start_nodes=None):
    """Generic shape: cluster up -> accounts -> checker + probes + load on all
    nodes -> `actions` [(at_s, fn(ctx))] -> drain -> verify -> results."""
    nodes = make_cluster(ctx, n_nodes, start=False)
    for nd in nodes[: (start_nodes or n_nodes)]:
        nd.start()
    wait_ready(nodes[: (start_nodes or n_nodes)])
    conv, own = wait_converged(nodes, expect_nodes=start_nodes or n_nodes)
    res = {"initial_convergence_s": round(conv, 2) if conv else None, "initial_distribution": own}
    live = nodes[: (start_nodes or n_nodes)]
    accounts_file, accts = setup_accounts(live, ctx.outdir, per_node=per_node)
    ctx.t0 = time.time()
    checker = Checker(nodes[checker_on], ctx.outdir)
    audits = {nd.id: FhAudit(nd, ctx.outdir) for nd in live}
    time.sleep(1)
    probe_nodes = [nodes[checker_on]] + [n for n in nodes if n is not nodes[checker_on]]
    prober = Prober(probe_nodes, accts, ctx.outdir).start()
    lgs = [Loadgen(nd, accounts_file, ctx.outdir, duration, rate=rate) for nd in live]
    fault_at = None
    extra = []
    for at, fn in sorted(actions, key=lambda a: a[0]):
        dt = ctx.t0 + at - time.time()
        if dt > 0:
            time.sleep(dt)
        r = fn(ctx)
        if r == "fault" and fault_at is None:
            fault_at = time.time()
        if isinstance(r, Loadgen):
            lgs.append(r)
        if isinstance(r, Checker):
            extra.append(r)
    for lg in lgs:
        lg.wait(duration + 120)
    load_end = time.time()
    time.sleep(3)
    prober_acked = prober.stop()
    time.sleep(3)
    res["checker"] = checker.stop()
    res["extra_checkers"] = [c.stop() for c in extra]
    survivors = [n for n in nodes if n.alive()]
    final_conv, final_own = wait_converged(nodes, expect_nodes=expect_final or len(survivors), timeout=30)
    res["final_convergence_wait_s"] = round(final_conv, 2) if final_conv is not None else None
    res["final_distribution"] = final_own
    acked_files = [lg.acked for lg in lgs] + [prober_acked]
    res["verify"] = verify(acked_files, survivors[0] if survivors else nodes[0], ctx.outdir)
    acked = load_acked(acked_files)
    # live audits: only nodes that stayed up for the whole run must be complete
    res["fh_live"] = []
    first_seqs = []
    live_raw = {}
    for nid, a in audits.items():
        data = a.stop()
        rep = audit_report(data, acked, nid)
        node = next(n for n in nodes if n.id == nid)
        rep["node_stayed_up"] = not node.exit_codes() and node.alive()
        if rep["node_stayed_up"]:
            live_raw[nid] = data
        if rep.get("first_seq", -1) > 0:
            first_seqs.append(rep["first_seq"])
        res["fh_live"].append(rep)
    # post-hoc replay from a cursor before the run on every survivor: the merged
    # stream must be complete and identical on every node
    res["fh_replay"] = []
    if first_seqs:
        cur = min(first_seqs) - 1
        reps = [FhAudit(n, ctx.outdir, tag="-replay", cursor=cur) for n in survivors]
        # rejoined nodes backfill the run from S3 segments (slower than the ring)
        time.sleep(8 if all(not n.exit_codes() and n.started_at < ctx.t0 for n in survivors) else 30)
        replay_raw = {}
        for a in reps:
            data = a.stop()
            if not a.node.exit_codes() and a.node.started_at < ctx.t0:
                replay_raw[a.node.id] = data
            rep = audit_report(data, acked, a.node.id)
            # a node (re)started mid-run only has events from its join onwards
            # in memory (no S3 backfill for cursors yet): informational only
            # (also: a node that joined mid-run has no history from before its join)
            rep["node_stayed_up"] = not a.node.exit_codes() and a.node.started_at < ctx.t0
            res["fh_replay"].append(rep)
        res["fh_replay_diff"] = history_diff(replay_raw)
    res["fh_live_diff"] = history_diff(live_raw, common_range=True)
    res["probe"] = prober.analyze(fault_at or ctx.t0 + 1e9, t0=ctx.t0)
    res["loadgens"] = []
    for lg in lgs:
        w = lg.windows()
        s = lg.summary()
        s["windows"] = w
        bad = [x for x in w if x[2] > 0 or x[1] < 0.8 * lg.rate]
        s["degraded_windows"] = len(bad)
        s["errors"] = sum(x[2] for x in w)
        s["max_p99_ms"] = max((x[4] for x in w), default=0)
        res["loadgens"].append(s)
    res["exit_codes"] = {n.id: n.exit_codes() for n in nodes}
    try:
        res["forwarded"] = {n.id: n.metrics().get("vlpds_requests_forwarded_total", 0) for n in survivors}
        res["lease_events"] = {n.id: {k.split('"')[1]: v for k, v in n.metrics().items() if k.startswith("vlpds_lease_events_total")} for n in survivors}
    except Exception:
        pass
    res["events"] = ctx.events
    return res


def judge(res, allow_lost=0):
    v = res.get("verify", {})
    ck = res.get("checker", {})
    ok = (v.get("missing") == 0 and v.get("ok")) and ck.get("result") == "PASS"
    for c in res.get("extra_checkers", []):
        ok = ok and c.get("result") == "PASS"
    ok = ok and res.get("final_distribution") is not None and res.get("final_convergence_wait_s") is not None
    for a in res.get("fh_live", []):
        if a.get("node_stayed_up"):
            ok = ok and a.get("fh_missing") == 0 and not a.get("reorders")
    for a in res.get("fh_replay", []):
        if a.get("node_stayed_up"):
            ok = ok and a.get("fh_missing") == 0 and not a.get("reorders")
    if len({(a.get("commits"), a.get("last_seq")) for a in res.get("fh_replay", []) if a.get("node_stayed_up")}) > 1:
        ok = False  # nodes disagree on the merged history
    for k in ("fh_replay_diff", "fh_live_diff"):
        if res.get(k) and not res[k].get("agree", True):
            ok = False
    if res.get("unexpected_exits"):
        ok = False
    # e.g. a zombie must fail-stop (3 = fenced log, 5 = lease lapsed) before any restart
    for nid, allowed in (res.get("expect_exit") or {}).items():
        codes = (res.get("exit_codes") or {}).get(nid) or []
        if not codes or codes[0] not in allowed:
            ok = False
    return "PASS" if ok else "FAIL"


# ---- actions


def kill(idx, sig=signal.SIGKILL, label=None):
    def f(ctx):
        n = ctx.nodes[idx]
        ctx.mark(label or f"signal {signal.Signals(sig).name} -> {n.id}")
        n.signal(sig)
        return "fault"
    return f


def restart(idx):
    def f(ctx):
        n = ctx.nodes[idx]
        if n.alive():
            n.signal(signal.SIGKILL)
            n.wait_exit()
        ctx.mark(f"start {n.id}")
        n.start()
    return f


def graceful_restart(idx):
    def f(ctx):
        n = ctx.nodes[idx]
        t = ctx.mark(f"SIGTERM {n.id} (rolling)")
        n.signal(signal.SIGTERM)
        n.wait_exit(30)
        ctx.mark(f"{n.id} exited rc={n.exit_codes()[-1:]} after {time.time() - t:.1f}s; restarting")
        n.start()
        return "fault"
    return f


def fault(idx, which, label, **kw):
    def f(ctx):
        n = ctx.nodes[idx]
        ctx.mark(f"{label} -> {n.id}")
        px = n.s3 if which == "s3" else n.peer
        px.set(**kw)
        if which == "peer" and kw.get("blackhole") == 1:
            pass
        return "fault"
    return f


def heal(idx, which=("s3", "peer")):
    def f(ctx):
        n = ctx.nodes[idx]
        ctx.mark(f"heal {','.join(which)} -> {n.id}")
        for w in which:
            (n.s3 if w == "s3" else n.peer).clear()
    return f


def start_node(idx, accounts=None):
    def f(ctx):
        n = ctx.nodes[idx]
        if n.alive():
            return None
        ctx.mark(f"add node {n.id}")
        n.start()
        return "fault"
    return f


def checker_with_cursor(on_idx, back_s=15):
    def f(ctx):
        n = ctx.nodes[on_idx]
        st = n.status()
        cur = st["firehose_last_emitted"] - (int(back_s * 1e6) << 8)
        ctx.mark(f"mid-way checker on {n.id} cursor={cur} ({back_s}s back)")
        return Checker(n, ctx.outdir, tag="-cursor", cursor=cur)
    return f


def snapshot_ownership(label):
    def f(ctx):
        ctx.mark(f"{label}: {json.dumps(ownership(ctx.nodes))}")
    return f


SCEN = {}


def scenario(name, desc):
    def deco(fn):
        SCEN[name] = (fn, desc)
        return fn
    return deco


@scenario("baseline-2", "2 nodes, steady split, load through both")
def s_base2(ctx):
    return run_load_scenario(ctx, 2, 30, [])


@scenario("baseline-3", "3 nodes, steady split, load through all")
def s_base3(ctx):
    return run_load_scenario(ctx, 3, 30, [])


@scenario("baseline-5", "5 nodes, steady split, load through all")
def s_base5(ctx):
    return run_load_scenario(ctx, 5, 30, [], per_node=20)


@scenario("kill9-1of3", "kill -9 one of 3 nodes under load, restart it 25 s later")
def s_kill1(ctx):
    return run_load_scenario(ctx, 3, 60, [(15, kill(1)), (40, restart(1))])


@scenario("kill9-2of5", "kill -9 two of 5 nodes at once under load, restart both 25 s later")
def s_kill2(ctx):
    return run_load_scenario(ctx, 5, 60, [(15, kill(2)), (15.01, kill(3)), (40, restart(2)), (40.01, restart(3))], per_node=20)


@scenario("sigterm", "SIGTERM one of 3 nodes under load (graceful handoff), restart it later")
def s_term(ctx):
    return run_load_scenario(ctx, 3, 50, [(15, kill(1, signal.SIGTERM)), (35, restart(1))])


@scenario("rolling-restart", "graceful rolling restart of all 3 nodes, one every 12 s")
def s_roll(ctx):
    # checker lives on n1, restarted last; a second checker resumes on n2 by cursor
    acts = [(10, graceful_restart(1)), (22, graceful_restart(2)), (34, graceful_restart(0)),
            (33, checker_with_cursor(1, back_s=25))]
    return run_load_scenario(ctx, 3, 50, acts)


@scenario("zombie", "SIGSTOP one of 3 nodes past its lease TTL, SIGCONT after takeover")
def s_zombie(ctx):
    acts = [(15, kill(1, signal.SIGSTOP)), (15 + 4 * TTL_MS / 1000, kill(1, signal.SIGCONT, "SIGCONT n2 (zombie wakes)")),
            (45, restart(1))]
    return run_load_scenario(ctx, 3, 60, acts)


@scenario("zombie-short", "SIGSTOP one of 3 nodes for ~TTL (wakes around the takeover)")
def s_zombie_short(ctx):
    acts = [(15, kill(1, signal.SIGSTOP)), (15 + TTL_MS / 1000 * 1.1, kill(1, signal.SIGCONT, "SIGCONT n2")),
            (40, restart(1))]
    return run_load_scenario(ctx, 3, 55, acts)


@scenario("s3-partition", "cut one node off from S3 (requests hang) for 12 s, then heal")
def s_s3part(ctx):
    acts = [(15, fault(1, "s3", "S3 blackhole", blackhole=1)), (27, heal(1, ("s3",))), (40, restart(1))]
    return run_load_scenario(ctx, 3, 55, acts)


@scenario("peer-partition", "cut one node's inbound peer traffic (forwarding+streams) for 12 s, S3 still reachable")
def s_peerpart(ctx):
    acts = [(15, fault(1, "peer", "peer blackhole", blackhole=1)), (27, heal(1, ("peer",)))]
    return run_load_scenario(ctx, 3, 50, acts)


@scenario("full-partition", "cut one node from S3 and peers for 12 s, then heal")
def s_fullpart(ctx):
    def both(ctx):
        n = ctx.nodes[1]
        ctx.mark(f"S3+peer blackhole -> {n.id}")
        n.s3.set(blackhole=1)
        n.peer.set(blackhole=1)
        return "fault"
    acts = [(15, both), (27, heal(1)), (40, restart(1))]
    return run_load_scenario(ctx, 3, 55, acts)


@scenario("s3-slow", "S3 latency spikes (400 ms +- 400 ms) on one node for 15 s")
def s_s3slow(ctx):
    acts = [(15, fault(1, "s3", "S3 latency 400ms+400ms jitter", latency_ms=400, jitter_ms=400)), (30, heal(1, ("s3",))),
            (42, restart(1))]
    return run_load_scenario(ctx, 3, 55, acts)


@scenario("s3-slow-all", "S3 latency 1500 ms on every node for 10 s (lease renewals near TTL)")
def s_s3slowall(ctx):
    def slow(ctx):
        ctx.mark("S3 latency 1500ms on all nodes")
        for n in ctx.nodes:
            n.s3.set(latency_ms=1500)
        return "fault"

    def fix(ctx):
        ctx.mark("heal S3 latency on all nodes")
        for n in ctx.nodes:
            n.s3.clear()

    def revive(ctx):
        for n in ctx.nodes:
            if not n.alive():
                ctx.mark(f"restart dead {n.id}")
                n.start()
    return run_load_scenario(ctx, 3, 50, [(15, slow), (25, fix), (35, revive)])


@scenario("s3-5xx", "S3 503 SlowDown on 30% of one node's requests for 15 s, then 100% for 6 s")
def s_s35xx(ctx):
    acts = [(15, fault(1, "s3", "S3 30% 503", err_pct=30, err_code=503)),
            (30, fault(1, "s3", "S3 100% 500", err_pct=100, err_code=500)),
            (36, heal(1, ("s3",))), (45, restart(1))]
    return run_load_scenario(ctx, 3, 60, acts)


@scenario("add-remove", "grow 2 -> 4 nodes under load, then remove two (SIGTERM, then kill -9)")
def s_addremove(ctx):
    acts = [(10, start_node(2)), (20, start_node(3)), (32, kill(3, signal.SIGTERM)), (44, kill(2, signal.SIGKILL))]
    return run_load_scenario(ctx, 4, 60, acts, start_nodes=2, expect_final=2)


@scenario("cas-contention", "8 nodes start at the same instant on a fresh prefix")
def s_cas(ctx):
    nodes = make_cluster(ctx, 8, start=False)
    t = time.time()
    for n in nodes:
        n.start()
    wait_ready(nodes)
    conv, own = wait_converged(nodes, expect_nodes=8, timeout=90)
    res = {"convergence_s": round(conv, 2) if conv is not None else None, "distribution": own}
    ctx.t0 = time.time()
    accounts_file, accts = setup_accounts(nodes, ctx.outdir, per_node=10)
    checker = Checker(nodes[0], ctx.outdir)
    lgs = [Loadgen(n, accounts_file, ctx.outdir, 20, rate=60) for n in nodes]
    for lg in lgs:
        lg.wait(200)
    time.sleep(3)
    res["checker"] = checker.stop()
    res["final_convergence_wait_s"], res["final_distribution"] = wait_converged(nodes, expect_nodes=8, timeout=10)
    res["verify"] = verify([lg.acked for lg in lgs], nodes[0], ctx.outdir)
    res["loadgens"] = [dict(lg.summary(), errors=sum(x[2] for x in lg.windows())) for lg in lgs]
    res["exit_codes"] = {n.id: n.exit_codes() for n in nodes}
    res["lease_events"] = {}
    for n in nodes:
        try:
            res["lease_events"][n.id] = {k.split('"')[1]: v for k, v in n.metrics().items() if k.startswith("vlpds_lease_events_total")}
        except Exception:
            pass
    res["events"] = ctx.events
    return res


@scenario("handoff-firehose", "checker on n1 while partitions move between n2/n3/n4 (restarts + joins); mid-way cursor subscriber")
def s_handoff(ctx):
    acts = [(10, graceful_restart(1)), (18, start_node(3)), (26, kill(2)), (34, restart(2)),
            (40, checker_with_cursor(0, back_s=20)), (42, graceful_restart(3))]
    return run_load_scenario(ctx, 4, 55, acts, start_nodes=3, expect_final=4)


# ---- scenarios added for the per-node-log design


def watch_log_then(idx, needle, then, label, delay=0.0, timeout=40):
    """Tails node idx's log from now on; when `needle` shows up, waits `delay`
    and runs `then(ctx)` (e.g. kill -9 mid-checkpoint)."""
    def f(ctx):
        n = ctx.nodes[idx]
        path = os.path.join(ctx.outdir, f"{n.id}.log")
        pos = os.path.getsize(path)
        end = time.time() + timeout
        while time.time() < end:
            with open(path, "rb") as fh:
                fh.seek(pos)
                chunk = fh.read()
            if needle.encode() in chunk:
                time.sleep(delay)
                ctx.mark(f"{label} ('{needle}' seen in {n.id}.log)")
                then(ctx)
                return "fault"
            time.sleep(0.005)
        ctx.mark(f"{label}: '{needle}' never seen")
        return None
    return f


@scenario("kill9-rebalance-drainer", "3 nodes; n4 joins under load; kill -9 n2 while it drains shards to n4; restart n2 later")
def s_k9_reb_drainer(ctx):
    grace = 2 * TTL_MS / 5000  # join grace = two renew intervals
    acts = [(12, start_node(3)), (12 + grace + 0.4, kill(1, label="kill -9 n2 (mid-rebalance, draining)")), (32, restart(1))]
    res = run_load_scenario(ctx, 4, 55, acts, start_nodes=3, expect_final=4)
    return res


@scenario("kill9-rebalance-joiner", "3 nodes; n4 joins under load; kill -9 n4 while it opens/replays the shards it took; restart it later")
def s_k9_reb_joiner(ctx):
    grace = 2 * TTL_MS / 5000
    acts = [(12, start_node(3)), (12 + grace + 0.5, kill(3, label="kill -9 n4 (mid-rebalance, acquiring)")), (32, restart(3))]
    return run_load_scenario(ctx, 4, 55, acts, start_nodes=3, expect_final=4)


@scenario("zombie-check", "SIGSTOP n2 for 4x TTL; after SIGCONT it must exit 3 (fenced) or 5 (lease lapsed); no restart")
def s_zombie_check(ctx):
    acts = [(15, kill(1, signal.SIGSTOP)), (15 + 4 * TTL_MS / 1000, kill(1, signal.SIGCONT, "SIGCONT n2 (zombie wakes)"))]
    res = run_load_scenario(ctx, 3, 45, acts)
    res["expect_exit"] = {"n2": [3, 5]}
    return res


@scenario("grow-1-to-3", "fresh prefix: n1 starts alone (its inline first step takes every shard), n2/n3 join under load; nobody may exit")
def s_grow(ctx):
    res = run_load_scenario(ctx, 3, 40, [(8, start_node(1)), (16, start_node(2))], start_nodes=1, expect_final=3)
    res["expect_exit"] = {}
    if any(res["exit_codes"].values()):
        res["unexpected_exits"] = res["exit_codes"]
    return res


@scenario("s3-5xx-all", "S3 503 SlowDown on 30% of every node's requests for 15 s")
def s_s35xx_all(ctx):
    def bad(ctx):
        ctx.mark("S3 30% 503 on all nodes")
        for n in ctx.nodes:
            n.s3.set(err_pct=30, err_code=503)
        return "fault"

    def fix(ctx):
        ctx.mark("heal S3 on all nodes")
        for n in ctx.nodes:
            n.s3.clear()
    return run_load_scenario(ctx, 3, 50, [(15, bad), (30, fix), (38, revive_dead)])


@scenario("s3-slow-one-long", "S3 latency 1500 ms on one node for 10 s")
def s_s3slow1(ctx):
    acts = [(15, fault(1, "s3", "S3 latency 1500ms", latency_ms=1500)), (25, heal(1, ("s3",))), (35, revive_dead)]
    return run_load_scenario(ctx, 3, 50, acts)


@scenario("kill9-mid-checkpoint", "kill -9 n2 right as its 10 s checkpoint starts (twice), restarting it each time")
def s_k9_ckpt(ctx):
    k = lambda ctx: ctx.nodes[1].signal(signal.SIGKILL)
    acts = [(5, watch_log_then(1, "checkpoint start", k, "kill -9 n2 mid-checkpoint", delay=0.03)),
            (25, restart(1)),
            (27, watch_log_then(1, "checkpoint start", k, "kill -9 n2 mid-checkpoint (2nd)", delay=0.01)),
            (50, restart(1))]
    return run_load_scenario(ctx, 3, 65, acts)


# ---- container scenarios


def net(idx, up):
    def f(ctx):
        n = ctx.nodes[idx]
        ctx.mark(f"network {'reconnect' if up else 'disconnect'} {n.id}")
        n.connect() if up else n.disconnect()
        return None if up else "fault"
    return f


def revive_dead(ctx):
    for n in ctx.nodes:
        if not n.alive():
            ctx.mark(f"restart dead {n.id} (exit codes {n.exit_codes()})")
            n.start()


def containers(skews=None):
    def deco(fn):
        def wrapped(ctx):
            ctx.factory = CNode
            CNode.skews = skews or {}
            return fn(ctx)
        return wrapped
    return deco


@scenario("ctr-baseline-3", "[containers] 3 nodes, steady split (sanity for the container path)")
@containers()
def s_ctr_base(ctx):
    return run_load_scenario(ctx, 3, 30, [])


@scenario("ctr-partition", "[containers] docker network disconnect one of 3 nodes (S3+peers+clients) for 12 s")
@containers()
def s_ctr_part(ctx):
    return run_load_scenario(ctx, 3, 55, [(15, net(1, False)), (27, net(1, True)), (40, revive_dead)])


@scenario("ctr-pause", "[containers] docker pause (cgroup freeze) one of 3 nodes for 4x TTL, then unpause")
@containers()
def s_ctr_pause(ctx):
    return run_load_scenario(ctx, 3, 55, [(15, kill(1, signal.SIGSTOP)), (15 + 4 * TTL_MS / 1000, kill(1, signal.SIGCONT, "unpause n2")),
                                          (40, revive_dead)])


@scenario("ctr-skew-small", "[containers] clocks n2 +250 ms, n3 -250 ms (inside the ttl/5 margin); kill -9 n2, restart")
@containers({"n2": "+0.25s", "n3": "-0.25s"})
def s_ctr_skew_small(ctx):
    return run_load_scenario(ctx, 3, 55, [(15, kill(1)), (35, restart(1))])


@scenario("ctr-skew-large", "[containers] clocks n2 +2.5 s, n3 -2.5 s (beyond the margin); kill -9 n2, restart")
@containers({"n2": "+2.5s", "n3": "-2.5s"})
def s_ctr_skew_large(ctx):
    return run_load_scenario(ctx, 3, 55, [(15, kill(1)), (35, restart(1))])


@scenario("ctr-skew-steady", "[containers] clocks n2 +2.5 s, n3 -2.5 s, no faults (merge lag/ordering under skew)")
@containers({"n2": "+2.5s", "n3": "-2.5s"})
def s_ctr_skew_steady(ctx):
    return run_load_scenario(ctx, 3, 30, [])


def run_one(name, run_id):
    fn, desc = SCEN[name]
    outdir = os.path.join(HERE, "out", run_id, name)
    os.makedirs(outdir, exist_ok=True)
    prefix = f"ha-{run_id}-{name}"
    ctx = Ctx(name, outdir, prefix)
    log(f"=== {name}: {desc} (prefix {prefix})")
    t = time.time()
    try:
        res = fn(ctx)
        res["verdict"] = judge(res) if "convergence_s" not in res else (
            "PASS" if res.get("convergence_s") is not None and res["checker"].get("result") == "PASS" and res["verify"].get("missing") == 0 else "FAIL")
    except Exception as e:
        import traceback
        traceback.print_exc()
        res = {"verdict": "ERROR", "error": repr(e), "events": ctx.events}
    finally:
        teardown(ctx)
    res["scenario"] = name
    if CLEANUP:
        res["prefix_deleted"] = delete_prefix(prefix)
    res["description"] = desc
    res["wall_s"] = round(time.time() - t, 1)
    json.dump(res, open(os.path.join(outdir, "result.json"), "w"), indent=1, default=str)
    line = summarize(res)
    with open(os.path.join(HERE, "out", run_id, "summary.md"), "a") as f:
        f.write(line + "\n")
    log(line)
    return res


def summarize(r):
    v = r.get("verify") or {}
    ck = r.get("checker") or {}
    pr = r.get("probe") or {}
    errs = sum(lg.get("errors", 0) for lg in r.get("loadgens", []))
    dist = r.get("final_distribution") or r.get("distribution") or {}
    dist_s = " ".join(f"{k}:{v if isinstance(v, (int, bool)) else len(v)}" for k, v in sorted(dist.items()))
    fl = [f"{a['node']}:{a.get('fh_missing')}" for a in r.get("fh_live", []) if a.get("node_stayed_up")]
    fr = [f"{a['node']}:{a.get('fh_missing')}{'' if a.get('node_stayed_up') else '(rejoined)'}" for a in r.get("fh_replay", [])]
    errs_by = {lg["node"]: lg.get("errors", 0) for lg in r.get("loadgens", [])}
    return (f"| {r['scenario']} | {r['verdict']} | acked {v.get('acked')} lost {v.get('missing')} | checker {ck.get('result')} "
            f"| fh-missing live {' '.join(fl)} replay {' '.join(fr)} | windows {pr.get('windows')} | errs {errs_by} "
            f"({ck.get('commits')} commits, fails {ck.get('failures')}) | unavail {pr.get('unavail_s')}s recov {pr.get('recovery_s')}s "
            f"(max partition {pr.get('max_partition_outage_s')}s) | loadgen errs {errs} | final {dist_s} | exits {r.get('exit_codes')} "
            f"| history agree replay={(r.get('fh_replay_diff') or {}).get('agree')} live={(r.get('fh_live_diff') or {}).get('agree')} |")


def main():
    if len(sys.argv) < 2 or sys.argv[1] == "list":
        for k, (_, d) in SCEN.items():
            print(f"{k:18} {d}")
        return
    names = sys.argv[2:] if sys.argv[1] == "run" else sys.argv[1:]
    if names == ["all"]:
        names = list(SCEN)
    for n in names:
        if n not in SCEN:
            sys.exit(f"unknown scenario {n}")
    if not os.path.exists(FAULTPROXY):
        subprocess.check_call(["go", "build", "-o", FAULTPROXY, "."], cwd=os.path.join(HERE, "faultproxy"))
    run_id = os.environ.get("HA_RUN_ID", time.strftime("%Y%m%d-%H%M%S"))
    os.makedirs(os.path.join(HERE, "out", run_id), exist_ok=True)
    verdicts = []
    for n in names:
        verdicts.append((n, run_one(n, run_id)["verdict"]))
    print("\n".join(f"{n:18} {v}" for n, v in verdicts))
    sys.exit(0 if all(v == "PASS" for _, v in verdicts) else 1)


if __name__ == "__main__":
    main()
