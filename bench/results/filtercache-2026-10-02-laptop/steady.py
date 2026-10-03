#!/usr/bin/env python3
"""Steady-state A/B: one node, default caches, small population; open-loop
writes then closed-loop reads. Prints loadgen's summaries.

  steady.py <bindir> <name> [--rate 3000]
"""
import argparse, os, shutil, subprocess, sys, time, urllib.request

S = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("bindir")
ap.add_argument("name")
ap.add_argument("--rate", type=int, default=3000)
ap.add_argument("--port", type=int, default=2791)
a = ap.parse_args()
prefix = f"filtercache-steady-{a.name}"
d = os.path.join(S, "runs", "steady-" + a.name)
shutil.rmtree(d, ignore_errors=True)
os.makedirs(d)
url = f"http://127.0.0.1:{a.port}"
args = [os.path.join(a.bindir, "vlpds"), "--listen", f"127.0.0.1:{a.port}", "--public-url", url,
        "--s3-endpoint", "http://127.0.0.1:9200", "--prefix", prefix, "--no-rate-limits", "--dev-mode",
        "--node-id", "n1", "--peer-listen", f"127.0.0.1:{a.port + 100}", "--advertise-url", f"https://127.0.0.1:{a.port + 100}",
        "--peer-tls-dir", os.path.join(d, "peer-tls"), "--workers", "3", "--io-threads", "6",
        "--block-cache-mb", "1024", "--lease-ttl-ms", "30000"]
log = open(os.path.join(d, "server.log"), "ab")
p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="info,slatedb=warn"), start_new_session=True)
try:
    t = time.time()
    while True:
        if p.poll() is not None:
            sys.exit("node exited")
        try:
            urllib.request.urlopen(url + "/xrpc/_health", timeout=2)
            if b"vlpds serving" in open(os.path.join(d, "server.log"), "rb").read():
                break
        except Exception:
            pass
        if time.time() - t > 300:
            sys.exit("node did not come up")
        time.sleep(0.5)
    lg = [os.path.join(a.bindir, "loadgen"), "--host", url, "--accounts-file", os.path.join(d, "accounts.json")]
    r = subprocess.run(lg + ["setup", "--accounts", "2000", "--records", "50", "--prefix", "steadyuser"], capture_output=True, text=True)
    if r.returncode:
        sys.exit("setup failed: " + r.stdout[-1500:] + r.stderr[-1500:])
    r = subprocess.run(lg + ["run", "--rate", str(a.rate), "--duration", "40", "--warmup", "10"], capture_output=True, text=True)
    print(f"== {a.name} run\n" + r.stdout[-3000:], flush=True)
    r = subprocess.run(lg + ["methods", "--only", "getRecord,listRecords,describeRepo", "--seconds", "10"], capture_output=True, text=True)
    print(f"== {a.name} methods\n" + r.stdout[-3000:] + r.stderr[-1500:], flush=True)
finally:
    p.terminate()
    try:
        p.wait(60)
    except Exception:
        p.kill()
    shutil.rmtree(os.path.join(S, "..", "minio-native", "vlpds", prefix), ignore_errors=True)
