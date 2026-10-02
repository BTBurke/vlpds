#!/usr/bin/env python3
"""Two small probes on a fresh one-shard node (TTL from --lease-ttl-ms):

1. Object-store requests per read: N getRepo, then N getBlob of one blob,
   each batch measured as a /metrics delta on an otherwise idle node.
2. Crash-restart with a long lease TTL: kill -9, restart with the same
   --node-id, time until a createRecord succeeds (does a lone node wait out
   its own lease?).

    probe.py --port 2706 --lease-ttl-ms 300000
"""
import argparse
import json
import os
import signal
import subprocess
import time

import analyze
import tinypds as t


def counts(url):
    m = t.scrape(url)
    out = {}
    for n, l, v in analyze.parse(m):
        if n == "vlpds_object_store_requests_total":
            k = (analyze.klass(l["op"]), l["op"], l["component"])
            out[k] = out.get(k, 0) + v
    return out


def diff(a, b):
    return {"|".join(k): b[k] - a.get(k, 0) for k in b if b[k] - a.get(k, 0)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=2706)
    p.add_argument("--lease-ttl-ms", type=int, default=300000)
    p.add_argument("--n", type=int, default=10)
    p.add_argument("--name", default="probe")
    p.add_argument("--skip-reads", action="store_true")
    a = p.parse_args()
    name = a.name
    d = os.path.join(t.SCRATCH, name)
    os.makedirs(d, exist_ok=True)
    url = f"http://127.0.0.1:{a.port}"
    args = [os.path.join(t.BIN, "vlpds"), "--listen", f"127.0.0.1:{a.port}", "--public-url", url,
            "--s3-endpoint", t.S3, "--prefix", "tiny-" + name, "--dev-mode", "--node-id", "n1",
            "--shards", "1", "--lease-ttl-ms", str(a.lease_ttl_ms),
            "--repo-cache-mb", "512", "--block-cache-mb", "256", "--cache-budget-mb", "256",
            "--cache-dir", os.path.join(d, "cache"), "--inject-put-ms", "30"]
    env = dict(os.environ, RUST_LOG="info,slatedb=warn", VLPDS_INJECT_STATE_MS="20,30")
    f = open(os.path.join(d, "server.log"), "ab")

    def start():
        pr = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
        while True:
            try:
                t.http("GET", url + "/xrpc/_health", timeout=2)
                return pr
            except Exception:
                if pr.poll() is not None:
                    raise RuntimeError("exited")
                time.sleep(0.2)

    out = {}
    pr = start()
    try:
        r = t.xrpc(url, "POST", "com.atproto.server.createAccount",
                   {"handle": "probe.vlpds.test", "email": "p@example.com", "password": "hunter22hunter22"})
        did, tok = r["did"], r["accessJwt"]
        for i in range(100):
            t.xrpc(url, "POST", "com.atproto.repo.createRecord",
                   {"repo": did, "collection": "app.bsky.feed.post",
                    "record": {"$type": "app.bsky.feed.post", "createdAt": t.now_iso(), "text": f"p{i}"}}, tok)
        b = t.xrpc(url, "POST", "com.atproto.repo.uploadBlob", token=tok, raw=os.urandom(300 * 1024), ctype="image/jpeg")["blob"]
        t.xrpc(url, "POST", "com.atproto.repo.createRecord",
               {"repo": did, "collection": "app.bsky.feed.post",
                "record": {"$type": "app.bsky.feed.post", "createdAt": t.now_iso(), "text": "pic",
                           "embed": {"$type": "app.bsky.embed.images", "images": [{"alt": "", "image": b}]}}}, tok)
        time.sleep(90)  # checkpoint + compaction settle
        for kind, fn in [] if a.skip_reads else [
            ("getRepo", lambda: t.http("GET", f"{url}/xrpc/com.atproto.sync.getRepo?did={did}")),
            ("getBlob", lambda: t.http("GET", f"{url}/xrpc/com.atproto.sync.getBlob?did={did}&cid={b['ref']['$link']}")),
            ("baseline (sleep)", lambda: time.sleep(1)),
        ]:
            c0, t0 = counts(url), time.time()
            for _ in range(a.n):
                fn()
            time.sleep(max(0, a.n * 1.0 - (time.time() - t0)))  # same wall time as the baseline
            out[kind] = {"n": a.n, "secs": round(time.time() - t0, 1), "delta": diff(c0, counts(url))}
            print(kind, json.dumps(out[kind]), flush=True)
        # graceful restart (SIGTERM: lease deleted), then crash restart (SIGKILL)
        for how, sig in [("sigterm", signal.SIGTERM), ("sigkill", signal.SIGKILL)]:
            time.sleep(20)
            # a session made before the kill (JWTs survive restarts; createSession
            # retries would trip its rate limit)
            tok = t.xrpc(url, "POST", "com.atproto.server.createSession",
                         {"identifier": did, "password": "hunter22hunter22"})["accessJwt"]
            os.killpg(pr.pid, sig)
            pr.wait(120)
            t0 = time.time()
            pr = start()
            up = time.time() - t0
            while True:
                try:
                    t.xrpc(url, "POST", "com.atproto.repo.createRecord",
                           {"repo": did, "collection": "app.bsky.feed.post",
                            "record": {"$type": "app.bsky.feed.post", "createdAt": t.now_iso(), "text": "after"}}, tok)
                    break
                except Exception as e:
                    last = str(e)[:200]
                    time.sleep(2)
            out["restart_" + how] = {"lease_ttl_ms": a.lease_ttl_ms, "health_after_s": round(up, 2),
                                     "first_write_after_s": round(time.time() - t0, 2),
                                     "last_error_before": locals().get("last")}
            print("restart", how, json.dumps(out["restart_" + how]), flush=True)
    finally:
        if pr.poll() is None:
            os.killpg(pr.pid, signal.SIGTERM)
            try:
                pr.wait(120)
            except subprocess.TimeoutExpired:
                os.killpg(pr.pid, signal.SIGKILL)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), name + ".json"), "w") as fo:
        json.dump(out, fo, indent=1)


if __name__ == "__main__":
    main()
