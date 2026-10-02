#!/usr/bin/env python3
"""Tiny-PDS object-store request rate (stdlib only).

One vlpds node (a one-node cluster, as production runs it) on a local MinIO,
one account, then either nothing (idle) or a personal-use trickle (commits,
blob uploads, relay/AppView-style getRepo/getBlob). /metrics is scraped every
--scrape-s from startup into <name>.jsonl; analyze.py turns the measurement
window into per-component request rates and prices.

    tinypds.py --name idle1 --port 2701 --shards 1 --lease-ttl-ms 60000 --workload idle
    tinypds.py --name pers1 --port 2702 --shards 1 --lease-ttl-ms 60000 --workload personal
    tinypds.py --name idle64 --port 2703 --shards 64 --lease-ttl-ms 10000 --workload idle

Env: BENCH_BIN (dir with vlpds), S3 (MinIO endpoint), TINY_SCRATCH (node dirs).
"""
import argparse
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

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.environ.get("BENCH_BIN") or os.path.join(PKG, "target", "release")
S3 = os.environ.get("S3", "http://127.0.0.1:9310")
SCRATCH = os.environ.get("TINY_SCRATCH", "/tmp/vlpds-tiny")
LABELED = ("vlpds_object_store_requests_total", "vlpds_object_store_bytes_total",
           "slatedb_object_store_request_count_total", "vlpds_ops_total")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


def http(method, url, body=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url.split('?')[0]}: {e.code} {e.read()[:300]!r}") from None


def xrpc(base, method, nsid, body=None, token=None, params="", raw=None, ctype="application/json"):
    h = {}
    if token:
        h["Authorization"] = "Bearer " + token
    data = None
    if raw is not None:
        data, h["Content-Type"] = raw, ctype
    elif body is not None:
        data, h["Content-Type"] = json.dumps(body).encode(), "application/json"
    _, out = http(method, f"{base}/xrpc/{nsid}{params}", data, h)
    try:
        return json.loads(out)
    except ValueError:
        return out


def scrape(url):
    try:
        _, raw = http("GET", url + "/metrics", timeout=10)
    except Exception:
        return None
    labeled = {}
    for line in raw.decode(errors="replace").splitlines():
        if not line or line[0] == "#":
            continue
        k, _, v = line.rpartition(" ")
        if k.split("{", 1)[0] in LABELED:
            try:
                labeled[k] = float(v)
            except ValueError:
                pass
    return labeled


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


class Personal(threading.Thread):
    """A personal account's trickle: one commit every --commit-every s (likes 60%,
    posts 20%, reposts 10%, follows 10%), a blob upload + image post every
    --blob-every s, and relay/AppView reads: getBlob every --getblob-every s,
    getRepo every --getrepo-every s."""

    def __init__(self, a, base, did, token):
        super().__init__(daemon=True)
        self.a, self.base, self.did, self.token = a, base, did, token
        self.stop = threading.Event()
        self.counts = {}
        self.uris, self.blobs = [], []

    def bump(self, k):
        self.counts[k] = self.counts.get(k, 0) + 1

    def create(self, coll, rec):
        r = xrpc(self.base, "POST", "com.atproto.repo.createRecord",
                 {"repo": self.did, "collection": coll, "record": rec}, self.token)
        self.uris.append((r["uri"], r["cid"]))
        return r

    def commit(self):
        x = random.random()
        if x < 0.6 or not self.uris:
            subj = random.choice(self.uris) if self.uris and random.random() < 0.3 else None
            if subj is None:
                subj = (f"at://did:plc:{random.randrange(10**12):024d}/app.bsky.feed.post/3l{random.randrange(10**10)}",
                        "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm")
            self.create("app.bsky.feed.like", {"$type": "app.bsky.feed.like", "createdAt": now_iso(),
                                               "subject": {"uri": subj[0], "cid": subj[1]}})
            self.bump("like")
        elif x < 0.8:
            self.create("app.bsky.feed.post", {"$type": "app.bsky.feed.post", "createdAt": now_iso(),
                                               "text": "personal pds post %d" % random.randrange(10**9)})
            self.bump("post")
        elif x < 0.9:
            u = random.choice(self.uris)
            self.create("app.bsky.feed.repost", {"$type": "app.bsky.feed.repost", "createdAt": now_iso(),
                                                 "subject": {"uri": u[0], "cid": u[1]}})
            self.bump("repost")
        else:
            self.create("app.bsky.graph.follow", {"$type": "app.bsky.graph.follow", "createdAt": now_iso(),
                                                  "subject": f"did:plc:{random.randrange(10**12):024d}"})
            self.bump("follow")

    def blob(self):
        data = os.urandom(self.a.blob_kb * 1024)
        r = xrpc(self.base, "POST", "com.atproto.repo.uploadBlob", token=self.token, raw=data, ctype="image/jpeg")
        b = r["blob"]
        self.create("app.bsky.feed.post", {"$type": "app.bsky.feed.post", "createdAt": now_iso(), "text": "pic",
                                           "embed": {"$type": "app.bsky.embed.images",
                                                     "images": [{"alt": "", "image": b}]}})
        self.blobs.append(b["ref"]["$link"])
        self.bump("blob_upload")
        self.bump("post")

    def run(self):
        nxt = {"commit": 0.0, "blob": self.a.blob_every / 2, "getblob": 30.0, "getrepo": 60.0}
        every = {"commit": self.a.commit_every, "blob": self.a.blob_every,
                 "getblob": self.a.getblob_every, "getrepo": self.a.getrepo_every}
        t0 = time.time()
        while not self.stop.is_set():
            t = time.time() - t0
            for k in sorted(nxt, key=nxt.get):
                if nxt[k] > t or every[k] <= 0:
                    continue
                nxt[k] += every[k]
                try:
                    if k == "commit":
                        self.commit()
                    elif k == "blob":
                        self.blob()
                    elif k == "getblob" and self.blobs:
                        http("GET", f"{self.base}/xrpc/com.atproto.sync.getBlob?did={self.did}&cid={random.choice(self.blobs)}")
                        self.bump("getBlob")
                    elif k == "getrepo":
                        http("GET", f"{self.base}/xrpc/com.atproto.sync.getRepo?did={self.did}", timeout=60)
                        self.bump("getRepo")
                except Exception as e:  # keep going; the error shows up in the counts
                    self.bump("error_" + k)
                    log("personal", k, e)
            self.stop.wait(0.5)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--prefix")
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--lease-ttl-ms", type=int, default=60000)
    p.add_argument("--workload", choices=["idle", "personal"], default="idle")
    p.add_argument("--warmup", type=int, default=300)
    p.add_argument("--measure", type=int, default=1200)
    p.add_argument("--scrape-s", type=float, default=30)
    p.add_argument("--commit-every", type=float, default=30)
    p.add_argument("--blob-every", type=float, default=240)
    p.add_argument("--blob-kb", type=int, default=300)
    p.add_argument("--getblob-every", type=float, default=120)
    p.add_argument("--getrepo-every", type=float, default=300)
    p.add_argument("--inject-put-ms", default="30")
    p.add_argument("--inject-state", default="20,30")
    p.add_argument("extra", nargs="*", help="extra vlpds args (after --)")
    a = p.parse_args()
    prefix = a.prefix or "tiny-" + a.name
    d = os.path.join(SCRATCH, a.name)
    os.makedirs(d, exist_ok=True)
    url = f"http://127.0.0.1:{a.port}"
    args = [os.path.join(BIN, "vlpds"), "--listen", f"127.0.0.1:{a.port}", "--public-url", url,
            "--s3-endpoint", S3, "--prefix", prefix, "--dev-mode", "--node-id", "n1",
            "--shards", str(a.shards), "--lease-ttl-ms", str(a.lease_ttl_ms),
            "--repo-cache-mb", "512", "--block-cache-mb", "256", "--cache-budget-mb", "256",
            "--cache-dir", os.path.join(d, "cache"),
            "--inject-put-ms", a.inject_put_ms] + a.extra
    env = dict(os.environ, RUST_LOG="info,slatedb=warn", VLPDS_INJECT_STATE_MS=a.inject_state)
    out = os.path.join(HERE, a.name + ".jsonl")
    meta = {"name": a.name, "prefix": prefix, "args": args[1:], "inject_state": a.inject_state,
            "workload": a.workload, "warmup": a.warmup, "measure": a.measure}
    f = open(os.path.join(d, "server.log"), "ab")
    proc = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    t_start = time.time()

    def snap(phase, extra=None):
        rec = {"t": time.time(), "phase": phase, "m": scrape(url)}
        if extra:
            rec.update(extra)
        with open(out, "a") as fo:
            fo.write(json.dumps(rec) + "\n")

    try:
        while True:
            if proc.poll() is not None:
                raise RuntimeError(f"vlpds exited; see {d}/server.log")
            try:
                http("GET", url + "/xrpc/_health", timeout=2)
                break
            except Exception:
                time.sleep(0.5)
        snap("up", {"meta": meta, "t_start": t_start})
        log(a.name, "up in %.1fs" % (time.time() - t_start))
        r = xrpc(url, "POST", "com.atproto.server.createAccount",
                 {"handle": "personal.vlpds.test", "email": "me@example.com", "password": "hunter22hunter22"})
        did, token = r["did"], r["accessJwt"]
        xrpc(url, "POST", "com.atproto.repo.putRecord",
             {"repo": did, "collection": "app.bsky.actor.profile", "rkey": "self",
              "record": {"$type": "app.bsky.actor.profile", "displayName": "Me"}}, token)
        snap("account", {"did": did})
        log(a.name, "account", did)
        pers = None
        if a.workload == "personal":
            pers = Personal(a, url, did, token)
            pers.start()
        t0 = time.time()
        while time.time() - t0 < a.warmup:
            time.sleep(a.scrape_s)
            snap("warmup")
        snap("measure:start", {"counts": dict(pers.counts) if pers else {}})
        log(a.name, "measuring", a.measure, "s")
        t0 = time.time()
        while time.time() - t0 < a.measure:
            time.sleep(min(a.scrape_s, max(0.1, a.measure - (time.time() - t0))))
            snap("measure", {"counts": dict(pers.counts) if pers else {}})
        snap("measure:end", {"counts": dict(pers.counts) if pers else {}})
        if pers:
            pers.stop.set()
            pers.join(30)
        log(a.name, "done", pers.counts if pers else "")
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(60)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)


if __name__ == "__main__":
    main()
