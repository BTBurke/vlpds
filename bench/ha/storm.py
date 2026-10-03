#!/usr/bin/env python3
"""Failover storm repro (DESIGN.md "Crash takeovers"): 3 local nodes on a
MinIO at 127.0.0.1:9200 (bucket `vlpds`, minioadmin), injected state latency
and a small state pool, steady sim writes through n1 + n2, kill -9 (or
SIGTERM) of n3 at --event-at, restart --down s later. Prints per-second
errors by kind (from loadgen's report lines) and keeps 1 s node samples in
<out>/<label>/result.json.

  storm.py run --bin target/dev-release --label before --rate 6000 --total 1000000 \
      --active 100000 --churn 500 --store-inflight 192 --inject-state 30,40 \
      --event-at 100 --duration 180
"""
import argparse, json, os, re, signal, subprocess, sys, time, urllib.request, threading

OUT = os.environ.get("STORM_OUT", "/tmp/vlpds-storm")
TLS = os.path.join(OUT, "peer-tls")
BASE = 7440
DEV = "minioadmin"
WIN_RE = re.compile(r"\[\s*(\d+)s\] ok/s\s+(\d+) err (\d+) dropped (\d+) inflight (\d+) \| p50 ([\d.]+)ms p99 ([\d.]+)ms max (\d+)ms")
KIND_RE = re.compile(r"\[([^\]]+)\]=(\d+)")


def http(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def metrics(port):
    try:
        raw = http(f"http://127.0.0.1:{port}/metrics", 2).decode()
    except Exception:
        return None
    out = {}
    for l in raw.splitlines():
        if l.startswith("#") or not l:
            continue
        k, _, v = l.rpartition(" ")
        try:
            out[k] = float(v)
        except ValueError:
            pass
    return out


def msum(m, prefix, *must):
    return sum(v for k, v in m.items() if k.startswith(prefix) and all(x in k for x in must))


class Node:
    def __init__(self, a, i, outdir):
        self.a, self.i, self.id = a, i, f"n{i}"
        self.port = BASE + i
        self.peer = BASE + 100 + i
        self.log = os.path.join(outdir, f"{self.id}.log")
        self.proc = None

    def start(self):
        a = self.a
        args = [os.path.join(a.bin, "vlpds"), "--listen", f"127.0.0.1:{self.port}", "--public-url", f"http://127.0.0.1:{self.port}",
                "--peer-listen", f"127.0.0.1:{self.peer}", "--advertise-url", f"https://127.0.0.1:{self.peer}",
                "--peer-tls-dir", TLS, "--s3-endpoint", "http://127.0.0.1:9200", "--prefix", a.prefix, "--node-id", self.id,
                "--lease-ttl-ms", str(a.ttl_ms), "--shards", str(a.shards), "--no-rate-limits", "--dev-mode",
                "--workers", str(a.workers), "--io-threads", str(a.io), "--block-cache-mb", str(a.block_cache_mb),
                "--repo-cache-mb", str(a.repo_cache_mb), "--inject-put-ms", str(a.inject_put), "--cache-budget-mb", "512", "--store-inflight", str(a.store_inflight)]
        args += a.extra.split() if a.extra else []
        env = dict(os.environ, RUST_LOG="info,slatedb=warn", VLPDS_INJECT_STATE_MS=a.inject_state)
        os.makedirs(TLS, exist_ok=True)
        self.proc = subprocess.Popen(args, stdout=open(self.log, "a"), stderr=subprocess.STDOUT, env=env)
        return self

    def ready(self):
        try:
            http(f"http://127.0.0.1:{self.port}/xrpc/_health", 1)
            return True
        except Exception:
            return False


def owned(n):
    m = metrics(n.port)
    return int(m.get("vlpds_owned_partitions", -1)) if m else -1


def wait(pred, t, what):
    s = time.time()
    while time.time() - s < t:
        if pred():
            return time.time() - s
        time.sleep(0.25)
    raise SystemExit(f"timeout waiting for {what}")


def start_cluster(a, outdir):
    nodes = [Node(a, i, outdir) for i in (1, 2, 3)]
    nodes[0].start()
    wait(nodes[0].ready, 120, "n1")
    for n in nodes[1:]:
        n.start()
    wait(lambda: all(n.ready() for n in nodes), 120, "nodes")
    want = a.shards // 3
    wait(lambda: all(owned(n) >= want for n in nodes) and sum(owned(n) for n in nodes) == a.shards, 180, "balanced")
    return nodes


def stop(nodes):
    for n in nodes:
        if n.proc and n.proc.poll() is None:
            n.proc.send_signal(signal.SIGTERM)
    for n in nodes:
        if n.proc:
            try:
                n.proc.wait(60)
            except subprocess.TimeoutExpired:
                n.proc.kill()


def populate(a):
    outdir = os.path.join(OUT, f"pop-{a.prefix}")
    os.makedirs(outdir, exist_ok=True)
    nodes = start_cluster(a, outdir)
    try:
        t = time.time()
        ps = [subprocess.Popen([os.path.join(a.lg or a.bin, "loadgen"), "--host", f"http://127.0.0.1:{n.port}", "--threads", "4", "bulk", "--start", "0",
                                "--count", str(a.total), "--batch", "1000", "--concurrency", "8", "--dist", "real", "--dist-scale", "128",
                                "--dist-knee", "2", "--dist-seed", "1"], stdout=subprocess.PIPE, stderr=open(os.path.join(outdir, f"bulk-{n.id}.err"), "w"), text=True)
              for n in nodes]
        outs = [p.communicate()[0] for p in ps]
        print("populate", [o.strip().splitlines()[-1] if o.strip() else "?" for o in outs], f"{time.time()-t:.0f}s", flush=True)
    finally:
        stop(nodes)


def run(a):
    outdir = os.path.join(OUT, a.label)
    os.makedirs(outdir, exist_ok=True)
    a.prefix = f"{a.prefix}-{a.label}-{int(time.time())}"
    nodes = start_cluster(a, outdir)
    tp = time.time()
    ps = [subprocess.Popen([os.path.join(a.lg or a.bin, "loadgen"), "--host", f"http://127.0.0.1:{n.port}", "--threads", "4", "bulk", "--start", "0",
                            "--count", str(a.total), "--batch", "1000", "--concurrency", "8", "--dist", "real", "--dist-scale", "128",
                            "--dist-knee", "2", "--dist-seed", "1"], stdout=subprocess.PIPE, stderr=open(os.path.join(outdir, f"bulk-{n.id}.err"), "w"), text=True)
          for n in nodes]
    for p in ps:
        p.communicate()
    print(f"populated {a.total} in {time.time()-tp:.0f}s", flush=True)
    ev = []
    t0 = time.time()
    mark = lambda s: (ev.append((round(time.time() - t0, 1), s)), print(f"[{time.time()-t0:6.1f}] {s}", flush=True))
    lgs = []
    for k, n in enumerate(nodes[:2]):
        lgs.append(subprocess.Popen([os.path.join(a.lg or a.bin, "loadgen"), "--host", f"http://127.0.0.1:{n.port}", "--threads", "4", "run", "--rate", str(a.rate / 2),
                                     "--duration", str(a.duration), "--warmup", "5", "--sim-total", str(a.total), "--sim-active", str(a.active),
                                     "--sim-churn", str(a.churn), "--report-secs", "1", "--max-inflight", "20000"],
                                    stdout=subprocess.PIPE, stderr=open(os.path.join(outdir, f"lg{k}.err"), "w"), text=True))
    samples = []
    stop_s = threading.Event()

    def sampler():
        while not stop_s.is_set():
            row = {"t": round(time.time() - t0, 2)}
            for n in nodes:
                m = metrics(n.port)
                if m is None:
                    continue
                row[n.id] = {
                    "owned": m.get("vlpds_owned_partitions", 0),
                    "sst_get": msum(m, "vlpds_object_store_requests_total", 'component="state_sst"', 'op="get'),
                    "state_get": msum(m, "vlpds_object_store_requests_total", 'client="state"', 'op="get'),
                    "inflight": msum(m, "vlpds_object_store_inflight", 'client="state"'),
                    "waits": msum(m, "vlpds_object_store_permit_waits_total", 'client="state"'),
                    "loads": msum(m, "vlpds_repo_loads_total"),
                    "preloads": msum(m, "vlpds_repo_preloads_total"),
                    "commits": m.get("vlpds_commits_total", 0),
                    "fwd_5xx": msum(m, "vlpds_forwards_total", "5xx"),
                    "retries": msum(m, "vlpds_write_retries_total"),
                    "abandoned": m.get("vlpds_writes_abandoned_total", 0),
                    "warm": msum(m, "vlpds_shard_warm"),
                    "fwd": msum(m, "vlpds_requests_forwarded_total"),
                    "http_inflight": msum(m, "vlpds_http_requests_inflight"),
                    "ctl": {k.split('result="')[1].rstrip('"}'): v for k, v in m.items() if k.startswith("vlpds_security_ctl_loads_total{")},
                    "retry": {k.split('reason="')[1].rstrip('"}'): v for k, v in m.items() if k.startswith("vlpds_write_retries_total{")},
                    "load_s": msum(m, "vlpds_repo_load_seconds_sum"),
                    "load_n": msum(m, "vlpds_repo_load_seconds_count"),
                    "wait_s": msum(m, "vlpds_object_store_permit_wait_seconds_sum", 'client="state"'),
                }
            samples.append(row)
            time.sleep(1)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    time.sleep(a.event_at)
    v = nodes[2]
    if a.scenario == "kill9":
        v.proc.kill()
        mark("kill -9 n3")
    else:
        v.proc.send_signal(signal.SIGTERM)
        mark("SIGTERM n3")
    v.proc.wait()
    mark("n3 exited")
    try:
        wait(lambda: owned(nodes[0]) + owned(nodes[1]) == a.shards, 60, "takeover")
        mark("survivors own all")
    except SystemExit:
        mark("takeover timeout")
    time.sleep(max(0, a.event_at + a.down - (time.time() - t0)))
    v.start()
    mark("n3 restarted")
    try:
        wait(lambda: owned(nodes[2]) >= a.shards // 3 - 1 and sum(owned(n) for n in nodes) == a.shards, 90, "rebalanced")
        mark("rebalanced")
    except SystemExit:
        mark("rebalance timeout")
    outs = [p.communicate()[0] for p in lgs]
    stop_s.set()
    th.join()
    # per-second errors (sum over loadgens), loadgen clock ~ t0
    per = {}
    for k in range(len(lgs)):
        last = 0
        for line in open(os.path.join(outdir, f"lg{k}.err"), errors="replace"):
            m = WIN_RE.search(line)
            if m:
                s, ok, err = int(m[1]), int(m[2]), int(m[3])
                d = per.setdefault(s, [0, 0, 0.0, {}])
                d[0] += ok
                d[1] += err - last
                d[2] = max(d[2], float(m[7]))
                for k, n in KIND_RE.findall(line[m.end():]):
                    d[3][k] = d[3].get(k, 0) + int(n)
                last = err
    stop(nodes)
    res = {"label": a.label, "args": vars(a), "events": ev, "per_s": per, "samples": samples, "lg": [o.strip().splitlines()[-3:] for o in outs]}
    json.dump(res, open(os.path.join(outdir, "result.json"), "w"))
    try:
        subprocess.run(["aws", "--endpoint-url", "http://127.0.0.1:9200", "s3", "rm", "--recursive", "--quiet", f"s3://vlpds/{a.prefix}/"],
                       env=dict(os.environ, AWS_ACCESS_KEY_ID=DEV, AWS_SECRET_ACCESS_KEY=DEV, AWS_DEFAULT_REGION="us-east-1"))
    except FileNotFoundError:
        print(f"aws CLI missing: delete s3://vlpds/{a.prefix}/ by hand", flush=True)
    summarize(res)


def summarize(res):
    ev = res["events"]
    per = {int(k): v for k, v in res["per_s"].items()}
    t_sig = next(t for t, s in ev if "n3" in s and ("kill" in s or "SIGTERM" in s))
    t_up = next(t for t, s in ev if s == "n3 restarted")
    def window(lo, hi):
        secs = [s for s in sorted(per) if lo <= s < hi and per[s][1] > 0]
        return sum(per[s][1] for s in range(int(lo), int(hi)) if s in per), (secs[0], secs[-1]) if secs else None
    e1, w1 = window(t_sig, t_up)
    e2, w2 = window(t_up, max(per) + 1)
    pre = sum(per[s][1] for s in per if s < t_sig)
    def kinds(lo, hi):
        out = {}
        for s in per:
            if lo <= s < hi:
                for k, n in per[s][3].items():
                    out[k] = out.get(k, 0) + n
        return dict(sorted(out.items(), key=lambda kv: -kv[1]))
    t_own = next((t for t, s in ev if s == "survivors own all"), t_up)
    print(f"{res['label']}: pre-event errors {pre}; takeover storm {e1} errors, error secs {w1}; rejoin storm {e2} errors, error secs {w2}")
    print(f"  kinds signal..owned(+{t_own - t_sig:.1f}s) {kinds(t_sig, t_own)}")
    print(f"  kinds owned..restart {kinds(t_own, t_up)}")
    print(f"  kinds rejoin {kinds(t_up, max(per) + 1)}")
    print("events", ev)
    print("  t   ok/s  err  p99")
    for s in sorted(per):
        if s >= t_sig - 3:
            print(f"{s:4} {per[s][0]:6} {per[s][1]:5} {per[s][2]:7.0f} {per[s][3] if per[s][3] else ''}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd")
    ap.add_argument("--bin", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--label", default="x")
    ap.add_argument("--scenario", default="kill9")
    ap.add_argument("--total", type=int, default=500_000)
    ap.add_argument("--active", type=int, default=100_000)
    ap.add_argument("--churn", type=float, default=200)
    ap.add_argument("--rate", type=float, default=3000)
    ap.add_argument("--duration", type=int, default=110)
    ap.add_argument("--event-at", type=float, default=40)
    ap.add_argument("--down", type=float, default=20)
    ap.add_argument("--shards", type=int, default=64)
    ap.add_argument("--ttl-ms", type=int, default=10000)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--io", type=int, default=4)
    ap.add_argument("--block-cache-mb", type=int, default=512)
    ap.add_argument("--repo-cache-mb", type=int, default=1024)
    ap.add_argument("--inject-put", type=float, default=25)
    ap.add_argument("--inject-state", default="20,30")
    ap.add_argument("--extra", default="")
    ap.add_argument("--lg", default="")
    ap.add_argument("--store-inflight", type=int, default=1024)
    a = ap.parse_args()
    {"populate": populate, "run": run}[a.cmd](a)
