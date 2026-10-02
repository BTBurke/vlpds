#!/usr/bin/env python3
"""History-compressed soak test for vlpds (stdlib only).

Compresses months of operational history into hours: N nodes against MinIO
under a steady realistic write + read load, while node restarts (rolling
SIGTERM, some kill -9) and shard split/merge cycles pile up log
incarnations, fences, retired shards and cloned (pinned) SSTs, with a short
log retention and short SlateDB GC/checkpoint windows so pruning runs many
times. Every --sample-s (10 s) it records latency, object-store requests per
commit by component, LIST sizes of assign/ and log/, fence/incarnation
counts, retired vs live state bytes (parent pinning, from S3 listings and the
live manifests), per-node RSS / Prometheus series / tasks / caches, then
fits trends against time and against cumulative history (incarnations,
reshard ops, commits) and gives a verdict per suspected growth path.

    bench/soak/soak.py run     [--name soak] [--nodes 3] [--hours 6] ...
    bench/soak/soak.py report  [--name soak]      # regenerate RESULTS.md + CSV
    bench/soak/soak.py status | cleanup           # (same --name)

Schedule: the soak clock (seconds under load, summed over resumes) walks a
repeating cycle, default `restart:600,calm:300,reshard:600,calm:300`.
Restarts happen only in `restart` phases (every --restart-every s, rotating
over nodes n{K+1}..nN, --kill9-frac of them kill -9; the first
--stable-nodes nodes are never restarted, so their RSS / series / task
counts show in-process growth), split/merge only in `reshard` phases (every
--reshard-every s, keeping the shard count near --shards), nothing in `calm`.
Writes and reads run at a constant rate throughout. Alternating storm types
decorrelates the history counters from each other and from data volume, so
the report can attribute a trend to restarts, reshards or data.

Load (own generator, --procs processes, open loop, latency from the
scheduled send, client-side failover like a load balancer: connection
refused -> next node; 502/503/504 -> backoff + retry):
  writes: Zipf(--zipf-s, capped at --zipf-cap of the traffic per writer)
          over a fixed bulk population; creates ~96% (posts, likes),
          deletes ~3.8% (every --dh-every-th repo is delete-heavy: likes
          created and deleted, ~45% deletes), updates ~0.3% (profile put)
  reads:  getRecord (mid repos + fresh records), listRecords (mid repos,
          delete-heavy repos' likes), sync.getRecord, sync.getRepo (mid repos)
  firehose: one live subscriber (on n1; reconnects with its cursor) + a
          cursor backfill every --backfill-every s from --backfill-age s back

Resumable: <scratch>/<name>/state.json keeps the setup flags, the soak clock,
the history counters and each load process's tracked rkeys. SIGTERM/SIGINT or
a failing guard (SOAK_GUARD_CMD, checked every 60 s; on benchbox
`guard.sh 25`) stops cleanly (graceful node shutdown, state saved, report
written) and exits 75; `run` again resumes. Exit 0 once --hours of soak are
done.

Env: BENCH_BIN (release dir with vlpds + loadgen), BENCH_SCRATCH, BENCH_OUT_DIR,
BENCH_MINIO_DATA (MinIO data dir: .trash purge + fast cleanup), BENCH_S3
(default http://127.0.0.1:9200), MIN_FREE_GB (default 150), NODE_EXTRA,
SOAK_GUARD_CMD, GRAFANA_URL (annotations; default off).
"""
import argparse
import base64
import bisect
import collections
import datetime
import hashlib
import hmac
import http.client
import json
import math
import multiprocessing as mp
import os
import queue
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.abspath(os.path.join(HERE, "..", ".."))
BIN = os.environ.get("BENCH_BIN") or os.path.join(PKG, "target", "release")
VLPDS = os.path.join(BIN, "vlpds")
LOADGEN = os.path.join(BIN, "loadgen")
S3_URL = os.environ.get("BENCH_S3", "http://127.0.0.1:9200")
BUCKET = "vlpds"
SCRATCH = os.environ.get("BENCH_SCRATCH") or os.path.join(PKG, "target", "soak-scratch")
ADMIN_TOKEN = "dev-admin-token"
INTERNAL_TOKEN = "dev-internal-token"
JWT_SECRET = b"dev-secret-change-me"
SERVICE_DID = "did:web:localhost"
GRAFANA = os.environ.get("GRAFANA_URL", "")
EXIT_PAUSED = 75


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def default_minio_data():
    d = os.environ.get("BENCH_MINIO_DATA")
    if d:
        return d
    try:  # laptop: a native MinIO (ps shows its data dir)
        out = subprocess.run(["ps", "-axo", "args="], capture_output=True, text=True).stdout
        for line in out.splitlines():
            m = re.search(r"minio server (\S+) .*--address 127\.0\.0\.1:9200", line)
            if m and os.path.isdir(m[1]):
                return m[1]
    except Exception:
        pass
    return ""


MINIO_DATA = default_minio_data()


def disk_free_gb(path="/"):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / 1e9


def purge_trash():
    if MINIO_DATA:
        trash = os.path.join(MINIO_DATA, ".minio.sys", "tmp", ".trash")
        if os.path.isdir(trash):
            subprocess.run(f"find '{trash}' -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", shell=True, check=False)


def http_req(method, url, body=None, headers=None, timeout=10):
    u = urllib.parse.urlsplit(url)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        c.request(method, u.path + ("?" + u.query if u.query else ""), body=body, headers=headers or {})
        r = c.getresponse()
        return r.status, r.read()
    finally:
        c.close()


def admin_headers():
    return {"Authorization": "Basic " + base64.b64encode(f"admin:{ADMIN_TOKEN}".encode()).decode(),
            "Content-Type": "application/json"}


def annotate(text, tags=()):
    if not GRAFANA:
        return
    body = json.dumps({"time": int(time.time() * 1000), "tags": ["vlpds-soak", *tags], "text": text})
    try:
        http_req("POST", GRAFANA + "/api/annotations", body.encode(), {"Content-Type": "application/json"}, timeout=2)
    except Exception:
        pass


# ---------------------------------------------------------------- identities

def b32(data):
    return base64.b32encode(data).decode().lower().rstrip("=")


def bulk_did(i):
    """state::bulk_did"""
    return "did:plc:" + b32(hashlib.sha256(f"vlpds-bulk:{i}".encode()).digest())[:24]


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def mint_access(did, ttl=2 * 3600):
    """auth::Jwt::access with the dev secret (what loadgen's sim mode does)."""
    now = int(time.time())
    h = b64u(b'{"alg":"HS256","typ":"at+jwt"}')
    p = b64u(json.dumps({"scope": "com.atproto.access", "sub": did, "aud": SERVICE_DID, "iat": now, "exp": now + ttl},
                        separators=(",", ":")).encode())
    sig = hmac.new(JWT_SECRET, f"{h}.{p}".encode(), hashlib.sha256).digest()
    return f"{h}.{p}.{b64u(sig)}"


# ---------------------------------------------------------------- S3 (SigV4, path style)

S3NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class S3:
    def __init__(self, url=S3_URL, bucket=BUCKET, ak="minioadmin", sk="minioadmin", region="us-east-1"):
        u = urllib.parse.urlsplit(url)
        self.host, self.port = u.hostname, u.port or 80
        self.hosthdr = f"{self.host}:{self.port}" if u.port else self.host
        self.bucket, self.ak, self.sk, self.region = bucket, ak, sk, region

    def _sign_key(self, ds):
        k = hmac.new(("AWS4" + self.sk).encode(), ds.encode(), hashlib.sha256).digest()
        for part in (self.region, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        return k

    def req(self, method, key="", query=None, body=b"", extra=None, timeout=60):
        q = sorted((query or {}).items())
        cq = "&".join(f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}" for k, v in q)
        path = "/" + self.bucket + ("/" + urllib.parse.quote(key, safe="/-_.~") if key else "")
        amz = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        ph = hashlib.sha256(body).hexdigest()
        hdrs = {"host": self.hosthdr, "x-amz-content-sha256": ph, "x-amz-date": amz, **{k.lower(): v for k, v in (extra or {}).items()}}
        names = sorted(hdrs)
        ch = "".join(f"{n}:{hdrs[n]}\n" for n in names)
        sh = ";".join(names)
        creq = "\n".join([method, path, cq, ch, sh, ph])
        scope = f"{amz[:8]}/{self.region}/s3/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
        sig = hmac.new(self._sign_key(amz[:8]), sts.encode(), hashlib.sha256).hexdigest()
        hdrs["Authorization"] = f"AWS4-HMAC-SHA256 Credential={self.ak}/{scope}, SignedHeaders={sh}, Signature={sig}"
        c = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            c.request(method, path + ("?" + cq if cq else ""), body=body or None, headers=hdrs)
            r = c.getresponse()
            return r.status, r.read()
        finally:
            c.close()

    def list(self, prefix, delimiter=None, max_pages=0):
        """{"objs": [(key, size)], "prefixes": [...], "pages", "bytes" (response bytes), "secs", "truncated"}"""
        out = {"objs": [], "prefixes": [], "pages": 0, "bytes": 0, "secs": 0.0, "truncated": False}
        t = time.time()
        token = None
        while True:
            q = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if delimiter:
                q["delimiter"] = delimiter
            if token:
                q["continuation-token"] = token
            st, body = self.req("GET", "", q)
            if st != 200:
                raise RuntimeError(f"LIST {prefix}: {st} {body[:200]!r}")
            out["pages"] += 1
            out["bytes"] += len(body)
            root = ET.fromstring(body)
            for c in root.findall(S3NS + "Contents"):
                out["objs"].append((c.findtext(S3NS + "Key"), int(c.findtext(S3NS + "Size") or 0)))
            for p in root.findall(S3NS + "CommonPrefixes"):
                out["prefixes"].append(p.findtext(S3NS + "Prefix"))
            if root.findtext(S3NS + "IsTruncated") != "true":
                break
            token = root.findtext(S3NS + "NextContinuationToken")
            if max_pages and out["pages"] >= max_pages:
                out["truncated"] = True
                break
        out["secs"] = time.time() - t
        return out

    def get(self, key):
        st, body = self.req("GET", key)
        if st != 200:
            raise RuntimeError(f"GET {key}: {st}")
        return body

    def delete_prefix(self, prefix):
        n = 0
        while True:
            objs = self.list(prefix, max_pages=1)["objs"]
            if not objs:
                return n
            xml = "<Delete><Quiet>true</Quiet>" + "".join(
                f"<Object><Key>{k.replace('&', '&amp;').replace('<', '&lt;')}</Key></Object>" for k, _ in objs) + "</Delete>"
            body = xml.encode()
            md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
            st, resp = self.req("POST", "", {"delete": ""}, body, {"Content-MD5": md5, "Content-Type": "application/xml"})
            if st != 200:
                raise RuntimeError(f"DeleteObjects: {st} {resp[:200]!r}")
            n += len(objs)


# ---------------------------------------------------------------- Prometheus text

LINE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)')
LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_prom(raw):
    """[(name, {labels}, value)], series count (non-comment lines)."""
    out, n = [], 0
    for line in raw.splitlines():
        if not line or line[0] == "#":
            continue
        n += 1
        m = LINE_RE.match(line)
        if not m:
            continue
        try:
            v = float(m[3])
        except ValueError:
            continue
        out.append((m[1], dict(LABEL_RE.findall(m[2] or "")), v))
    return out, n


def minio_token():
    h = b64u(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    p = b64u(json.dumps({"exp": int(time.time()) + 30 * 86400, "sub": "minioadmin", "iss": "prometheus"}, separators=(",", ":")).encode())
    return f"{h}.{p}." + b64u(hmac.new(b"minioadmin", f"{h}.{p}".encode(), hashlib.sha512).digest())


MINIO_TOKEN = minio_token()


def minio_metrics():
    try:
        _, raw = http_req("GET", S3_URL + "/minio/v2/metrics/cluster", headers={"Authorization": "Bearer " + MINIO_TOKEN}, timeout=3)
    except Exception:
        return None
    out = {}
    for line in raw.decode(errors="replace").splitlines():
        if line.startswith("minio_s3_requests_total{"):
            m = re.search(r'api="([^"]+)"', line)
            if m:
                out[m[1]] = out.get(m[1], 0.0) + float(line.rpartition(" ")[2])
    return out


# ---------------------------------------------------------------- CBOR + websocket (firehose)

def cbor_decode(b, i=0):
    ib = b[i]
    i += 1
    mt, ai = ib >> 5, ib & 31
    if ai < 24:
        val = ai
    elif ai == 24:
        val = b[i]; i += 1
    elif ai == 25:
        val = int.from_bytes(b[i:i + 2], "big"); i += 2
    elif ai == 26:
        val = int.from_bytes(b[i:i + 4], "big"); i += 4
    elif ai == 27:
        val = int.from_bytes(b[i:i + 8], "big"); i += 8
    else:
        raise ValueError("indefinite CBOR")
    if mt == 0:
        return val, i
    if mt == 1:
        return -1 - val, i
    if mt == 2:
        return b[i:i + val], i + val
    if mt == 3:
        return b[i:i + val].decode("utf-8", "replace"), i + val
    if mt == 4:
        arr = []
        for _ in range(val):
            x, i = cbor_decode(b, i)
            arr.append(x)
        return arr, i
    if mt == 5:
        d = {}
        for _ in range(val):
            k, i = cbor_decode(b, i)
            v, i = cbor_decode(b, i)
            d[k] = v
        return d, i
    if mt == 6:
        return cbor_decode(b, i)  # tag (CID 42): the bytes
    if mt == 7:
        return {20: False, 21: True, 22: None}.get(ai, None), i
    raise ValueError("bad CBOR")


class WS:
    """Minimal websocket client (server frames are unmasked)."""

    def __init__(self, url, timeout=10):
        u = urllib.parse.urlsplit(url)
        self.sock = socket.create_connection((u.hostname, u.port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        path = u.path + ("?" + u.query if u.query else "")
        self.sock.sendall((f"GET {path} HTTP/1.1\r\nHost: {u.hostname}:{u.port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        self.f = self.sock.makefile("rb", buffering=1 << 16)
        status = self.f.readline()
        if b" 101 " not in status:
            rest = self.f.read1(400) if hasattr(self.f, "read1") else b""
            raise RuntimeError(f"websocket handshake: {status.strip()!r} {rest!r}")
        while self.f.readline() not in (b"\r\n", b"\n", b""):
            pass

    def _exact(self, n):
        b = self.f.read(n)
        if b is None or len(b) < n:
            raise EOFError("websocket closed")
        return b

    def _send(self, op, payload=b""):
        mask = os.urandom(4)
        hdr = bytes([0x80 | op])
        n = len(payload)
        hdr += bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + n.to_bytes(2, "big")
        self.sock.sendall(hdr + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(payload)))

    def recv(self):
        """One complete data message (bytes)."""
        buf = b""
        while True:
            h = self._exact(2)
            fin, op = h[0] & 0x80, h[0] & 0x0F
            n = h[1] & 0x7F
            if n == 126:
                n = int.from_bytes(self._exact(2), "big")
            elif n == 127:
                n = int.from_bytes(self._exact(8), "big")
            if h[1] & 0x80:
                mk = self._exact(4)
                p = bytes(c ^ mk[i % 4] for i, c in enumerate(self._exact(n)))
            else:
                p = self._exact(n)
            if op == 8:
                raise EOFError("websocket close frame")
            if op == 9:
                self._send(10, p)
                continue
            if op == 10:
                continue
            buf += p
            if fin:
                return buf

    def close(self):
        try:
            self._send(8)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


def parse_frame(msg):
    """(header, body) of a firehose message."""
    hdr, i = cbor_decode(msg, 0)
    body, _ = cbor_decode(msg, i)
    return hdr, body


def iso_ts(s):
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


# ---------------------------------------------------------------- load processes

def zipf_cdf(n, s, cap):
    """Cumulative weights over ranks 0..n-1 (index = rank), each weight capped
    at `cap` of the total (excess spread over the uncapped)."""
    w = [1.0 / (i + 1) ** s for i in range(n)]
    tot = sum(w)
    w = [x / tot for x in w]
    if cap > 0 and cap * n > 1:
        for _ in range(50):
            excess = sum(x - cap for x in w if x > cap)
            if excess <= 1e-12:
                break
            free = sum(x for x in w if x < cap)
            w = [cap if x >= cap else x + excess * x / free for x in w]
    acc, cdf = 0.0, []
    for x in w:
        acc += x
        cdf.append(acc)
    return w, cdf


def op_mix(cfg):
    """Delete probability for ordinary repos so the overall delete share is
    cfg.delete_pct with delete-heavy repos at cfg.dh_delete_pct."""
    w, _ = zipf_cdf(cfg["population"], cfg["zipf_s"], cfg["zipf_cap"])
    f = sum(x for i, x in enumerate(w) if is_dh(cfg, i))
    d_dh = cfg["dh_delete_pct"] / 100
    rest = (cfg["delete_pct"] / 100 - f * d_dh) / max(1e-9, 1 - f)
    return f, max(0.0, rest)


def is_dh(cfg, i):
    return cfg["dh_every"] > 0 and i % cfg["dh_every"] == cfg["dh_every"] - 1


class Client:
    """Per-thread keep-alive connections to every node; load-balancer-like
    failover (refused -> next node; 502/503/504 -> backoff, retry)."""

    def __init__(self, urls, stats):
        self.urls = urls
        self.conns = {}
        self.stats = stats

    def conn(self, u):
        c = self.conns.get(u)
        if c is None:
            p = urllib.parse.urlsplit(u)
            c = self.conns[u] = http.client.HTTPConnection(p.hostname, p.port, timeout=30)
        return c

    def drop(self, u):
        c = self.conns.pop(u, None)
        if c:
            c.close()

    def call(self, method, path, body=None, token=None, deadline_s=60):
        hdrs = {}
        if body is not None:
            body = json.dumps(body).encode()
            hdrs["Content-Type"] = "application/json"
        if token:
            hdrs["Authorization"] = "Bearer " + token
        t_end = time.time() + deadline_s
        start = random.randrange(len(self.urls))
        attempt = 0
        last = None
        while time.time() < t_end:
            u = self.urls[(start + attempt) % len(self.urls)]
            attempt += 1
            try:
                c = self.conn(u)
                c.request(method, path, body=body, headers=hdrs)
                r = c.getresponse()
                data = r.read()
                if r.getheader("connection", "").lower() == "close":
                    self.drop(u)
            except (ConnectionError, socket.timeout, http.client.HTTPException, OSError) as e:
                self.drop(u)
                self.stats["conn_retry"] += 1
                last = f"{type(e).__name__}"
                if attempt % len(self.urls) == 0:
                    time.sleep(min(1.0, 0.05 * attempt))
                continue
            if r.status in (502, 503, 504):
                self.stats["retry_5xx"] += 1
                last = f"{r.status} {data[:120]!r}"
                time.sleep(min(1.0, 0.02 * 2 ** min(attempt, 6)))
                continue
            return r.status, data
        raise TimeoutError(f"{path}: gave up after {attempt} attempts ({last})")


def load_proc(k, cfg, stats_q, stop_ev, rk_path, mid_dids):
    """One load process: writes over population indices i % procs == k and
    reads, at 1/procs of the configured rates. Ships per-window latency lists
    to stats_q."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    random.seed(os.urandom(8))
    P = cfg["procs"]
    urls = cfg["urls"]
    w, _ = zipf_cdf(cfg["population"], cfg["zipf_s"], cfg["zipf_cap"])
    mine = [i for i in range(cfg["population"]) if i % P == k]
    acc, cdf = 0.0, []
    for i in mine:
        acc += w[i]
        cdf.append(acc)
    _, p_del_norm = op_mix(cfg)
    p_upd = cfg["update_pct"] / 100
    p_del_dh = cfg["dh_delete_pct"] / 100
    dh_mine = [i for i in mine if is_dh(cfg, i)]
    lock = threading.Lock()
    tracked = {}  # idx -> [(collection, rkey)]
    try:
        tracked = {int(a): [tuple(x) for x in b] for a, b in json.load(open(rk_path)).items()}
    except Exception:
        pass
    recent = collections.deque(maxlen=2000)  # (did, collection, rkey, uri, cid)
    tokens = {}
    mid_rkeys = {}  # did -> [rkey]
    win = {"t": int(time.time() // cfg["sample_s"] * cfg["sample_s"]), "lat": collections.defaultdict(list),
           "err": collections.Counter(), "ops": collections.Counter(), "first_err": {}}
    stats = collections.Counter()
    q = queue.Queue()

    def token(did):
        t = tokens.get(did)
        if t is None or time.time() - t[1] > 3600:
            t = tokens[did] = (mint_access(did), time.time())
        return t[0]

    def record(kind, sched, ok, err=None):
        lat = (time.time() - sched) * 1000
        with lock:
            if ok:
                win["lat"][kind].append(round(lat, 2))
            else:
                win["err"][kind] += 1
                win["first_err"].setdefault(kind, str(err)[:300])

    def flush(force=False):
        now = time.time()
        with lock:
            if not force and now < win["t"] + cfg["sample_s"]:
                return
            out = {"k": k, "t": win["t"], "lat": dict(win["lat"]), "err": dict(win["err"]), "ops": dict(win["ops"]),
                   "first_err": dict(win["first_err"]), "stats": dict(stats), "tracked": sum(len(v) for v in tracked.values())}
            win["t"] = int(now // cfg["sample_s"] * cfg["sample_s"])
            win["lat"] = collections.defaultdict(list)
            win["err"] = collections.Counter()
            win["ops"] = collections.Counter()
            win["first_err"] = {}
            stats.clear()
        stats_q.put(out)

    def now_iso():
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def do_write(cl, sched):
        x = random.random() * cdf[-1]
        idx = mine[min(bisect.bisect_left(cdf, x), len(mine) - 1)]
        did = bulk_did(idx)
        dh = is_dh(cfg, idx)
        r = random.random()
        with lock:
            mine_t = tracked.get(idx)
            target = None
            if r < p_upd:
                kind = "update"
            elif r < p_upd + (p_del_dh if dh else p_del_norm) and mine_t:
                kind = "delete"
                target = mine_t.pop(random.randrange(len(mine_t)))
            else:
                kind = "create"
            subj = recent[random.randrange(len(recent))] if recent else None
        if kind == "update":
            body = {"repo": did, "collection": "app.bsky.actor.profile", "rkey": "self",
                    "record": {"$type": "app.bsky.actor.profile", "displayName": f"soak {random.randrange(1 << 30)}",
                               "description": "x" * random.randrange(0, 200)}}
            path = "/xrpc/com.atproto.repo.putRecord"
        elif kind == "delete":
            body = {"repo": did, "collection": target[0], "rkey": target[1]}
            path = "/xrpc/com.atproto.repo.deleteRecord"
        else:
            like = subj is not None and (dh or random.random() < cfg["like_frac"])
            if like:
                coll = "app.bsky.feed.like"
                rec = {"$type": coll, "subject": {"uri": subj[3], "cid": subj[4]}, "createdAt": now_iso()}
            else:
                coll = "app.bsky.feed.post"
                rec = {"$type": coll, "text": f"soak post {random.randrange(1 << 40)} " + "lorem ipsum " * random.randrange(1, 20),
                       "createdAt": now_iso(), "langs": ["en"]}
            body = {"repo": did, "collection": coll, "record": rec}
            path = "/xrpc/com.atproto.repo.createRecord"
        try:
            st, data = cl.call("POST", path, body, token(did))
        except Exception as e:
            record("w_" + kind, sched, False, e)
            if kind == "delete":
                with lock:
                    tracked.setdefault(idx, []).append(target)
            return
        if st != 200:
            if kind == "delete" and st == 400:
                pass  # already gone (a retried delete): not an error for the soak
            record("w_" + kind, sched, False, f"{st} {data[:200]!r}")
            return
        record("w_" + kind, sched, True)
        with lock:
            win["ops"][kind] += 1
            if kind == "create":
                v = json.loads(data)
                rkey = v["uri"].rsplit("/", 1)[1]
                lst = tracked.setdefault(idx, [])
                lst.append((body["collection"], rkey))
                if len(lst) > cfg["track_per_repo"]:
                    lst.pop(0)
                if body["collection"] == "app.bsky.feed.post":
                    recent.append((did, body["collection"], rkey, v["uri"], v["cid"]))

    def mid_rkey(cl, did):
        r = mid_rkeys.get(did)
        if r is None:
            st, data = cl.call("GET", f"/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.feed.post&limit=100")
            if st != 200:
                raise RuntimeError(f"listRecords {did}: {st}")
            r = mid_rkeys[did] = [x["uri"].rsplit("/", 1)[1] for x in json.loads(data)["records"]]
        return random.choice(r) if r else None

    def do_read(cl, sched):
        r = random.random() * 100
        try:
            if r < 35:
                kind = "r_getRecord"
                if recent and random.random() < 0.5:
                    did, coll, rkey = random.choice(recent)[:3]
                else:
                    did, coll = random.choice(mid_dids), "app.bsky.feed.post"
                    rkey = mid_rkey(cl, did)
                st, data = cl.call("GET", f"/xrpc/com.atproto.repo.getRecord?repo={did}&collection={coll}&rkey={rkey}")
                ok = st == 200 or (st == 400 and b"RecordNotFound" in data)
            elif r < 60:
                kind = "r_listRecords"
                did = random.choice(mid_dids)
                rev = "&reverse=true" if random.random() < 0.5 else ""
                st, data = cl.call("GET", f"/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.feed.post&limit=50{rev}")
                ok = st == 200
            elif r < 70 and dh_mine:
                kind = "r_listRecords_dh"
                # the most-written delete-heavy repos (low index = high rank)
                did = bulk_did(dh_mine[min(int(random.expovariate(1 / 3)), len(dh_mine) - 1)])
                st, data = cl.call("GET", f"/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.feed.like&limit=50")
                ok = st == 200
            elif r < 90:
                kind = "r_syncGetRecord"
                did = random.choice(mid_dids)
                rkey = mid_rkey(cl, did)
                st, data = cl.call("GET", f"/xrpc/com.atproto.sync.getRecord?did={did}&collection=app.bsky.feed.post&rkey={rkey}")
                ok = st == 200
            else:
                kind = "r_getRepo"
                did = random.choice(mid_dids)
                st, data = cl.call("GET", f"/xrpc/com.atproto.sync.getRepo?did={did}", deadline_s=120)
                ok = st == 200
        except Exception as e:
            record("r_err", sched, False, e)
            return
        record(kind, sched, ok, None if ok else f"{st} {data[:200]!r}")

    def worker():
        cl = Client(urls, stats)
        while True:
            item = q.get()
            if item is None:
                return
            kind, sched = item
            try:
                (do_write if kind == "w" else do_read)(cl, sched)
            except Exception as e:
                record("internal", sched, False, repr(e))

    def scheduler(kind, rate):
        if rate <= 0:
            return
        iv = 1.0 / rate
        nxt = time.time()
        while not stop_ev.is_set():
            now = time.time()
            if nxt > now:
                time.sleep(min(nxt - now, 0.05))
                continue
            while nxt <= now:
                if q.qsize() >= cfg["max_queue"]:
                    with lock:
                        win["err"]["dropped_" + kind] += 1
                else:
                    q.put((kind, nxt))
                nxt += iv

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(cfg["threads"])]
    for t in threads:
        t.start()
    scheds = [threading.Thread(target=scheduler, args=("w", cfg["write_rate"] / P), daemon=True),
              threading.Thread(target=scheduler, args=("r", cfg["read_rate"] / P), daemon=True)]
    for t in scheds:
        t.start()
    while not stop_ev.is_set():
        stop_ev.wait(0.5)
        flush()
    for t in scheds:
        t.join(timeout=5)
    # drain what is queued (bounded), then stop the workers
    t_end = time.time() + 20
    while not q.empty() and time.time() < t_end:
        time.sleep(0.1)
    for _ in threads:
        q.put(None)
    for t in threads:
        t.join(timeout=30)
    flush(force=True)
    with lock:
        try:
            with open(rk_path + ".tmp", "w") as f:
                json.dump({str(a): b for a, b in tracked.items() if b}, f)
            os.replace(rk_path + ".tmp", rk_path)
        except Exception:
            pass


# ---------------------------------------------------------------- nodes

class Node:
    def __init__(self, cfg, i):
        self.cfg, self.i = cfg, i
        self.name = f"n{i+1}"
        self.port = cfg.base_port + i
        self.url = f"http://127.0.0.1:{self.port}"
        self.dir = os.path.join(cfg.state_dir, self.name)
        os.makedirs(self.dir, exist_ok=True)
        self.logpath = os.path.join(self.dir, "server.log")
        self.p = None
        self.f = None
        self.started_at = None

    def args(self):
        c = self.cfg
        a = [VLPDS, "--listen", f"127.0.0.1:{self.port}", "--public-url", self.url, "--s3-endpoint", S3_URL,
             "--prefix", c.prefix, "--no-rate-limits", "--dev-mode", "--node-id", self.name, "--advertise-url", self.url,
             "--shards", str(c.shards), "--workers", str(c.workers), "--io-threads", str(c.io_threads),
             "--block-cache-mb", str(c.block_cache_mb), "--repo-cache-mb", str(c.repo_cache_mb),
             "--cache-budget-mb", str(c.cache_budget_mb), "--log-retention", c.log_retention,
             "--slatedb-checkpoint-lifetime", c.checkpoint_lifetime, "--slatedb-gc-min-age", c.gc_min_age,
             "--lease-ttl-ms", str(c.lease_ttl_ms)]
        if c.cache_dir:
            a += ["--cache-dir", os.path.join(self.dir, "cache")]
        if c.inject:
            a += ["--inject-put-ms", str(c.inject)]
        return a + c.node_extra

    def start(self, timeout=600):
        mark = os.path.getsize(self.logpath) if os.path.exists(self.logpath) else 0
        self.f = open(self.logpath, "ab")
        env = dict(os.environ)
        env.setdefault("RUST_LOG", "info,slatedb=warn")
        self.p = subprocess.Popen(self.args(), stdout=self.f, stderr=subprocess.STDOUT, start_new_session=True, env=env)
        t = time.time()
        while time.time() - t < timeout:
            if self.p.poll() is not None:
                raise RuntimeError(f"{self.name} exited {self.p.returncode}; see {self.logpath}")
            try:
                http_req("GET", self.url + "/xrpc/_health", timeout=2)
                with open(self.logpath, "rb") as f:
                    f.seek(mark)
                    if b"vlpds serving" in f.read():
                        self.started_at = time.time()
                        return time.time() - t
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError(f"{self.name} did not come up in {timeout}s")

    def alive(self):
        return self.p is not None and self.p.poll() is None

    def wait(self, timeout):
        t = time.time()
        while self.alive() and time.time() - t < timeout:
            time.sleep(0.1)
        return not self.alive()

    def kill9(self):
        if self.alive():
            self.p.send_signal(signal.SIGKILL)
        self.wait(30)
        self.close()

    def stop(self, timeout=120):
        """Graceful SIGTERM; returns (seconds to exit, exit code)."""
        t = time.time()
        if self.alive():
            self.p.send_signal(signal.SIGTERM)
            if not self.wait(timeout):
                self.p.send_signal(signal.SIGKILL)
                self.wait(30)
        rc = self.p.returncode if self.p else None
        self.close()
        return time.time() - t, rc

    def close(self):
        if self.p:
            try:
                self.p.wait(timeout=30)
            except Exception:
                pass
        if self.f:
            self.f.close()
            self.f = None

    def pid(self):
        return self.p.pid if self.alive() else None


def get_layout(nodes):
    for n in sorted(nodes, key=lambda n: (not n.alive(), n.i)):
        if not n.alive():
            continue
        try:
            st, body = http_req("GET", n.url + "/xrpc/vlpds.admin.getShardLayout", headers=admin_headers(), timeout=5)
            if st == 200:
                return json.loads(body)
        except Exception:
            pass
    return None


def owned(n):
    try:
        _, raw = http_req("GET", n.url + "/metrics", timeout=3)
    except Exception:
        return None
    m = re.search(rb"^vlpds_owned_partitions (\S+)", raw, re.M)
    return int(float(m[1])) if m else 0


def wait_converged(nodes, timeout=180):
    """Every live shard of the layout has an owner and owned counts sum to it."""
    t = time.time()
    last = None
    while time.time() - t < timeout:
        lay = get_layout(nodes)
        if lay and not lay.get("op"):
            want = len(lay["shards"])
            o = [owned(n) for n in nodes]
            last = (want, o)
            if None not in o and sum(o) == want and all(s.get("owner") for s in lay["shards"]) and min(o) > 0:
                return time.time() - t
        time.sleep(0.5)
    log(f"not converged in {timeout}s: {last}")
    return None


# ---------------------------------------------------------------- state

def load_state(cfg):
    p = os.path.join(cfg.state_dir, "state.json")
    st = json.load(open(p)) if os.path.exists(p) else {}
    want = {"prefix": cfg.prefix, "population": cfg.population, "pop_records": cfg.pop_records,
            "mid_repos": cfg.mid_repos, "mid_records": cfg.mid_records, "nodes": cfg.nodes}
    if st.get("setup_key") and st["setup_key"] != want:
        raise SystemExit(f"state {p} is for {st['setup_key']}, not {want}: use another --name or `cleanup`")
    st["setup_key"] = want
    st.setdefault("soak_s", 0.0)
    st.setdefault("h", {"incarnations": 0, "restarts": 0, "sigterm": 0, "kill9": 0, "reshards": 0, "splits": 0, "merges": 0,
                        "reshard_fail": 0, "resumes": 0, "commits": 0, "creates": 0, "deletes": 0, "updates": 0})
    st.setdefault("next_victim", 0)
    st.setdefault("next_restart_at", 0.0)
    st.setdefault("next_reshard_at", 0.0)
    st.setdefault("next_backfill_at", 0.0)
    st.setdefault("fh_cursor", None)
    return st


def save_state(cfg, st):
    p = os.path.join(cfg.state_dir, "state.json")
    with open(p + ".tmp", "w") as f:
        json.dump(st, f, indent=1)
    os.replace(p + ".tmp", p)


def jsonl(path, rec):
    with open(path, "a") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ---------------------------------------------------------------- firehose

class Firehose:
    """Live subscriber (prefers n1; reconnects with its last seq) and the
    seq/time history the backfill prober picks cursors from."""

    def __init__(self, cfg, nodes, st):
        self.cfg, self.nodes, self.st = cfg, nodes, st
        self.lock = threading.Lock()
        self.stop_ev = threading.Event()
        self.hist = collections.deque(maxlen=20000)  # (wall, seq) every ~0.5 s
        self.last_seq = st.get("fh_cursor")
        self.win = self._new_win()
        self.t = threading.Thread(target=self.loop, daemon=True)

    def _new_win(self):
        return {"events": 0, "lag": [], "reconnects": 0, "outdated": 0, "errors": 0, "out_of_order": 0, "info": 0}

    def take(self):
        with self.lock:
            w, self.win = self.win, self._new_win()
        return w

    def loop(self):
        k = 0
        last_hist = 0
        while not self.stop_ev.is_set():
            n = self.nodes[k % len(self.nodes)]
            if not n.alive():
                k += 1
                self.stop_ev.wait(0.5)
                continue
            q = f"?cursor={self.last_seq}" if self.last_seq else ""
            try:
                ws = WS(n.url.replace("http", "ws", 1) + "/xrpc/com.atproto.sync.subscribeRepos" + q, timeout=30)
            except Exception:
                k += 1
                self.stop_ev.wait(1)
                continue
            try:
                while not self.stop_ev.is_set():
                    hdr, body = parse_frame(ws.recv())
                    now = time.time()
                    with self.lock:
                        if hdr.get("op") == -1:
                            self.win["errors"] += 1
                            raise EOFError(f"error frame {body}")
                        if hdr.get("t") == "#info":
                            self.win["info"] += 1
                            if body.get("name") == "OutdatedCursor":
                                self.win["outdated"] += 1
                            continue
                        seq = body.get("seq")
                        if isinstance(seq, int):
                            if self.last_seq and seq <= self.last_seq:
                                self.win["out_of_order"] += 1
                            else:
                                self.last_seq = seq
                            if now - last_hist >= 0.5:
                                self.hist.append((now, seq))
                                last_hist = now
                        self.win["events"] += 1
                        ts = iso_ts(body.get("time", "")) if isinstance(body.get("time"), str) else None
                        if ts:
                            self.win["lag"].append((now - ts) * 1000)
            except Exception:
                pass
            finally:
                ws.close()
            with self.lock:
                self.win["reconnects"] += 1
            # stay on n1 (the stable node) when it is alive
            k = 0 if self.nodes[0].alive() else k + 1
            self.stop_ev.wait(0.2)

    def cursor_at(self, wall):
        with self.lock:
            for t, s in self.hist:
                if t >= wall:
                    return s, t
            return (self.hist[0][1], self.hist[0][0]) if self.hist else (None, None)

    def backfill_probe(self, age_s, timeout=90):
        """Subscribe at a cursor `age_s` back on a random live node; time to the
        first event and to the live head (the subscriber's seq at start)."""
        cur, cur_t = self.cursor_at(time.time() - age_s)
        head = self.last_seq
        live = [n for n in self.nodes if n.alive()]
        if not cur or not head or not live:
            return None
        n = random.choice(live)
        ev = {"node": n.name, "cursor_age_s": round(time.time() - cur_t, 1), "outdated": False}
        t0 = time.time()
        try:
            ws = WS(n.url.replace("http", "ws", 1) + f"/xrpc/com.atproto.sync.subscribeRepos?cursor={cur}", timeout=30)
        except Exception as e:
            ev["error"] = repr(e)[:200]
            return ev
        cnt = 0
        try:
            ws.sock.settimeout(30)
            while time.time() - t0 < timeout:
                hdr, body = parse_frame(ws.recv())
                if hdr.get("t") == "#info":
                    ev["outdated"] = ev["outdated"] or body.get("name") == "OutdatedCursor"
                    continue
                if hdr.get("op") == -1:
                    ev["error"] = str(body)[:200]
                    break
                cnt += 1
                if cnt == 1:
                    ev["first_event_s"] = round(time.time() - t0, 3)
                    ev["first_seq_gap"] = body.get("seq", 0) - cur if isinstance(body.get("seq"), int) else None
                if isinstance(body.get("seq"), int) and body["seq"] >= head:
                    ev["caught_up_s"] = round(time.time() - t0, 3)
                    break
        except Exception as e:
            ev.setdefault("error", repr(e)[:200])
        finally:
            ws.close()
        ev["events"] = cnt
        ev["events_s"] = round(cnt / max(1e-3, (ev.get("caught_up_s") or (time.time() - t0))))
        return ev


# ---------------------------------------------------------------- sampler

COUNTER_KEEP = ("vlpds_object_store_requests_total", "vlpds_object_store_bytes_total", "vlpds_commits_total",
                "vlpds_ops_total", "vlpds_cluster_store_requests_total", "vlpds_retention_deleted_objects_total",
                "vlpds_retention_deleted_bytes_total", "vlpds_retention_ticks_total", "vlpds_recovery_replayed_segments_total",
                "vlpds_repo_loads_total", "vlpds_firehose_backfill_gets_total", "vlpds_segments_total",
                "vlpds_process_cpu_seconds_total", "vlpds_reshard_events_total", "vlpds_lease_events_total",
                "vlpds_write_errors_total", "vlpds_compaction_poll_switches_total")
GAUGE_KEEP = ("vlpds_process_resident_bytes", "vlpds_process_threads", "vlpds_tokio_alive_tasks", "vlpds_owned_partitions",
              "vlpds_shard_layout_version", "vlpds_cache_bytes", "vlpds_cache_entries", "vlpds_repo_cache_bytes",
              "vlpds_cached_repos", "vlpds_jemalloc_bytes", "vlpds_retention_replay_hold_segments",
              "vlpds_firehose_merge_queue_bytes", "vlpds_log_live_ring_bytes", "vlpds_firehose_ring_bytes",
              "vlpds_http_server_connections_open")


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 2)


class Sampler:
    def __init__(self, cfg, nodes, st, fh, out):
        self.cfg, self.nodes, self.st, self.fh, self.out = cfg, nodes, st, fh, out
        self.s3 = S3()
        self.prev = {}  # (node, series key) -> value (counter reset aware)
        self.prev_minio = None
        self.fence_only = set()  # dead logs seen pruned to their fence (they never change: no fence GC at HEAD)
        self.state_snap = {}
        self.last_state_t = 0
        self.last_purge = 0
        self.load_wins = collections.defaultdict(list)  # window t -> [proc msgs]
        self.events_since = []
        self.seen_series = {}  # node -> set of "name{labels}" (new series show up per sample)

    def delta(self, node, key, v):
        p = self.prev.get((node, key))
        self.prev[(node, key)] = v
        if p is None:
            return 0.0
        return v - p if v >= p else v  # a restarted process starts from 0

    def scrape(self):
        per = {}
        for n in self.nodes:
            row = {"up": False}
            per[n.name] = row
            if not n.alive():
                continue
            try:
                t = time.time()
                _, raw = http_req("GET", n.url + "/metrics", timeout=5)
                row["scrape_ms"] = round((time.time() - t) * 1000, 1)
            except Exception:
                continue
            series, nlines = parse_prom(raw.decode(errors="replace"))
            keys = {name + "{" + ",".join(f"{a}={b}" for a, b in sorted(lab.items())) + "}" for name, lab, _ in series
                    if not name.endswith("_bucket")}
            seen = self.seen_series.get(n.name)
            if seen is not None:
                new = sorted(keys - seen)
                row["new_series"] = len(new)
                row["new_series_names"] = new[:10]
                seen |= keys
            else:
                self.seen_series[n.name] = keys
            row.update(up=True, series=nlines, metrics_bytes=len(raw), uptime_s=round(time.time() - (n.started_at or time.time())))
            gauges = collections.defaultdict(float)
            deltas = collections.defaultdict(float)
            slate_g = collections.defaultdict(float)
            for name, lab, v in series:
                if name.endswith("_bucket"):
                    continue
                if name in COUNTER_KEEP:
                    key = name + "|" + ",".join(f"{a}={b}" for a, b in sorted(lab.items()))
                    deltas[key] += self.delta(n.name, key, v)
                elif name in GAUGE_KEEP:
                    gauges[name] += v
                    if name in ("vlpds_cache_bytes", "vlpds_cache_entries") and "cache" in lab:
                        gauges[f"{name}|{lab['cache']}"] += v
                    if name == "vlpds_jemalloc_bytes" and "stat" in lab:
                        gauges[f"jemalloc|{lab['stat']}"] = v
                elif name.startswith("slatedb_") and not name.endswith(("_total", "_sum", "_count")):
                    slate_g[name] += v
            pid = n.pid()
            row["gauges"] = dict(gauges)
            row["deltas"] = {k: v for k, v in deltas.items() if v}
            row["slatedb"] = dict(slate_g)
            row["pid"] = pid
        return per

    def s3_sample(self, lay):
        out = {}
        pre = self.cfg.prefix + "/"
        try:
            a = self.s3.list(pre + "assign/")
            keys = [k.rsplit("/", 1)[1] for k, _ in a["objs"]]
            ids = {int(k) for k in keys if k.isdigit()}
            live = {s["id"] for s in lay["shards"]} if lay else set()
            out.update({"assign.keys": len(keys), "assign.list_bytes": a["bytes"], "assign.list_pages": a["pages"],
                        "assign.list_ms": round(a["secs"] * 1000, 1), "assign.retired": len(ids - live) if lay else None,
                        "assign.bytes": sum(s for _, s in a["objs"])})
        except Exception as e:
            out["assign.error"] = repr(e)[:200]
        try:
            # what backfill::list_logs and retention do: one delimiter LIST of log/
            d = self.s3.list(pre + "log/", delimiter="/")
            logs = [p[len(pre) + 4:].strip("/") for p in d["prefixes"]]
            out.update({"log.ids": len(logs), "log.list_bytes": d["bytes"], "log.list_pages": d["pages"],
                        "log.list_ms": round(d["secs"] * 1000, 1)})
            # live logs from the leases
            live_logs = set()
            ln = self.s3.list(pre + "nodes/")
            for k, _ in ln["objs"]:
                try:
                    live_logs.add(json.loads(self.s3.get(k)).get("log_id"))
                except Exception:
                    pass
            dead = [x for x in logs if x not in live_logs]
            fence_only, dead_objs, dead_unpruned, live_objs, live_bytes = 0, 0, 0, 0, 0
            for x in dead:
                if x in self.fence_only:
                    fence_only += 1
                    dead_objs += 1
                    continue
                r = self.s3.list(pre + f"log/{x}/", max_pages=3)
                objs = r["objs"]
                dead_objs += len(objs)
                if len(objs) == 1 and objs[0][1] < 256:
                    fence_only += 1
                    self.fence_only.add(x)
                elif objs:
                    dead_unpruned += 1
            for x in logs:
                if x in live_logs:
                    r = self.s3.list(pre + f"log/{x}/", max_pages=50)
                    live_objs += len(r["objs"])
                    live_bytes += sum(s for _, s in r["objs"])
            out.update({"log.live": len(live_logs & set(logs)), "log.dead": len(dead), "log.fence_only": fence_only,
                        "log.dead_unpruned": dead_unpruned, "log.dead_objects": dead_objs, "log.live_objects": live_objs,
                        "log.live_bytes": live_bytes})
        except Exception as e:
            out["log.error"] = repr(e)[:200]
        try:
            r = self.s3.list(pre + "retain/")
            out["retain.keys"] = len(r["objs"])
            out["retain.list_bytes"] = r["bytes"]
        except Exception as e:
            out["retain.error"] = repr(e)[:200]
        return out

    def state_sample(self, lay):
        """state/ per shard: SST count/bytes, manifests; live shards' latest
        manifest scanned for other shards' state paths (external_dbs)."""
        pre = self.cfg.prefix + "/state/"
        r = self.s3.list(pre)
        per = collections.defaultdict(lambda: {"sst": 0, "sst_bytes": 0, "manifests": 0, "other": 0, "bytes": 0, "last_manifest": ""})
        for k, size in r["objs"]:
            rest = k[len(pre):].split("/")
            if not rest[0].isdigit():
                continue
            s = per[int(rest[0])]
            s["bytes"] += size
            sub = rest[1] if len(rest) > 1 else ""
            if sub == "compacted" and k.endswith(".sst"):
                s["sst"] += 1
                s["sst_bytes"] += size
            elif sub == "manifest":
                s["manifests"] += 1
                if k > s["last_manifest"]:
                    s["last_manifest"] = k
            else:
                s["other"] += 1
        live = {s["id"] for s in lay["shards"]} if lay else set(per)
        refs = {}  # live shard -> referenced other shard ids
        for sid in live:
            m = per.get(sid, {}).get("last_manifest")
            if not m:
                continue
            try:
                body = self.s3.get(m)
                ids = {int(x) for x in re.findall(rb"/state/(\d{10})(?![0-9])", body)} - {sid}
                refs[sid] = ids
            except Exception:
                pass
        retired = set(per) - live
        referenced = set().union(*refs.values()) if refs else set()
        live_sst = [per[s]["sst"] for s in live if s in per]
        ext = [len(v) for v in refs.values()]
        out = {
            "state.list_objects": len(r["objs"]), "state.list_pages": r["pages"], "state.list_ms": round(r["secs"] * 1000),
            "state.dirs": len(per), "state.live_shards": len(live), "state.retired_dirs": len(retired),
            "state.live_bytes": sum(per[s]["bytes"] for s in live if s in per),
            "state.live_sst_bytes": sum(per[s]["sst_bytes"] for s in live if s in per),
            "state.retired_bytes": sum(per[s]["bytes"] for s in retired),
            "state.pinned_bytes": sum(per[s]["bytes"] for s in retired & referenced),
            "state.unreferenced_retired_bytes": sum(per[s]["bytes"] for s in retired - referenced),
            "state.retired_referenced": len(retired & referenced),
            "state.total_bytes": sum(v["bytes"] for v in per.values()),
            "state.sst_live_total": sum(live_sst), "state.sst_per_shard_mean": round(sum(live_sst) / max(1, len(live_sst)), 1),
            "state.sst_per_shard_max": max(live_sst or [0]),
            "state.manifests_live_total": sum(per[s]["manifests"] for s in live if s in per),
            "state.ext_refs_mean": round(sum(ext) / max(1, len(ext)), 2), "state.ext_refs_max": max(ext or [0]),
            "state.live_with_ext": sum(1 for x in ext if x),
        }
        out["state.total_over_live"] = round(out["state.total_bytes"] / max(1, out["state.live_bytes"]), 3)
        return out

    def drain_load(self, q):
        while True:
            try:
                m = q.get_nowait()
            except queue.Empty:
                break
            self.load_wins[m["t"]].append(m)

    def take_load(self, upto):
        """Merge every complete window (all processes reported, or older than
        upto) into one record."""
        lat = collections.defaultdict(list)
        err, ops, stats, first = collections.Counter(), collections.Counter(), collections.Counter(), {}
        span = 0
        tracked = 0
        for t in sorted(list(self.load_wins)):
            if t + self.cfg.sample_s > upto:
                continue
            span += self.cfg.sample_s
            for m in self.load_wins.pop(t):
                for k, v in m["lat"].items():
                    lat[k].extend(v)
                err.update(m["err"])
                ops.update(m["ops"])
                stats.update(m["stats"])
                first.update(m["first_err"])
                tracked += m.get("tracked", 0)
        return lat, err, ops, stats, first, span

    def sample(self, q, now, phase, lay):
        cfg, st = self.cfg, self.st
        row = {"t": round(now, 1), "soak_s": round(st["soak_s_now"], 1), "phase": phase}
        h = st["h"]
        # load
        self.drain_load(q)
        lat, err, ops, stats, first, span = self.take_load(now - 2)
        for kind, xs in sorted(lat.items()):
            row[f"lat.{kind}.n"] = len(xs)
            row[f"lat.{kind}.p50"] = pct(xs, 0.5)
            row[f"lat.{kind}.p99"] = pct(xs, 0.99)
            row[f"lat.{kind}.max"] = round(max(xs), 1)
        w_all = [x for k, xs in lat.items() if k.startswith("w_") for x in xs]
        r_all = [x for k, xs in lat.items() if k.startswith("r_") for x in xs]
        row.update({"lat.w.p50": pct(w_all, 0.5), "lat.w.p99": pct(w_all, 0.99), "lat.w.n": len(w_all),
                    "lat.r.p50": pct(r_all, 0.5), "lat.r.p99": pct(r_all, 0.99), "lat.r.n": len(r_all)})
        for k, v in err.items():
            row[f"err.{k}"] = v
        row["load.conn_retry"] = stats.get("conn_retry", 0)
        row["load.retry_5xx"] = stats.get("retry_5xx", 0)
        if first:
            row["first_err"] = first
        h["creates"] += ops.get("create", 0)
        h["deletes"] += ops.get("delete", 0)
        h["updates"] += ops.get("update", 0)
        h["commits"] += sum(ops.values())
        row["load.span_s"] = span
        row["load.write_ok_s"] = round(len(w_all) / span, 1) if span else None
        # nodes
        per = self.scrape()
        commits = 0.0
        req_by = collections.Counter()
        bytes_by = collections.Counter()
        for name, r in per.items():
            row[f"node.{name}.up"] = r["up"]
            if not r["up"]:
                continue
            g = r["gauges"]
            row[f"node.{name}.series"] = r["series"]
            row[f"node.{name}.metrics_bytes"] = r["metrics_bytes"]
            row[f"node.{name}.uptime_s"] = r["uptime_s"]
            row[f"node.{name}.rss_mb"] = round(g.get("vlpds_process_resident_bytes", 0) / 1e6, 1)
            row[f"node.{name}.tasks"] = g.get("vlpds_tokio_alive_tasks")
            row[f"node.{name}.threads"] = g.get("vlpds_process_threads")
            row[f"node.{name}.owned"] = g.get("vlpds_owned_partitions")
            row[f"node.{name}.layout_version"] = g.get("vlpds_shard_layout_version")
            row[f"node.{name}.cache_mb"] = round(g.get("vlpds_cache_bytes", 0) / 1e6, 2)
            row[f"node.{name}.cache_entries"] = g.get("vlpds_cache_entries")
            row[f"node.{name}.repo_cache_mb"] = round(g.get("vlpds_repo_cache_bytes", 0) / 1e6, 1)
            row[f"node.{name}.cached_repos"] = g.get("vlpds_cached_repos")
            row[f"node.{name}.replay_hold"] = g.get("vlpds_retention_replay_hold_segments")
            row[f"node.{name}.merge_queue_mb"] = round(g.get("vlpds_firehose_merge_queue_bytes", 0) / 1e6, 2)
            row[f"node.{name}.scrape_ms"] = r.get("scrape_ms")
            if r.get("new_series"):
                row[f"node.{name}.new_series"] = r["new_series"]
                row[f"node.{name}.new_series_names"] = r["new_series_names"]
            for k, v in g.items():
                if k.startswith("jemalloc|"):
                    row[f"node.{name}.jemalloc_{k.split('|')[1]}_mb"] = round(v / 1e6, 1)
                if k.startswith("vlpds_cache_bytes|"):
                    row[f"node.{name}.cache.{k.split('|')[1]}_mb"] = round(v / 1e6, 3)
            for k, v in r["slatedb"].items():
                row[f"node.{name}.{k}"] = v
            for k, v in r["deltas"].items():
                name_, _, labs = k.partition("|")
                lab = dict(x.split("=", 1) for x in labs.split(",") if "=" in x)
                if name_ == "vlpds_commits_total":
                    commits += v
                elif name_ == "vlpds_object_store_requests_total":
                    req_by[(lab.get("op"), lab.get("component"))] += v
                elif name_ == "vlpds_object_store_bytes_total":
                    bytes_by[(lab.get("dir"), lab.get("component"))] += v
                elif name_ == "vlpds_process_cpu_seconds_total":
                    row[f"node.{name}.cpu_pct"] = round(100 * v / cfg.sample_s, 1)
                else:
                    short = name_.replace("vlpds_", "").replace("_total", "")
                    tag = ".".join(lab.values())
                    key = f"ctr.{short}" + (f".{tag}" if tag else "")
                    row[key] = row.get(key, 0) + v
        row["commits"] = commits
        row["commits_s"] = round(commits / cfg.sample_s, 1)
        req_tot = sum(req_by.values())
        row["req.total_s"] = round(req_tot / cfg.sample_s, 1)
        row["req.per_commit"] = round(req_tot / commits, 3) if commits else None
        comp = collections.Counter()
        for (op, c), v in req_by.items():
            comp[c] += v
            row[f"req.{op}.{c}"] = v
        for c, v in comp.items():
            row[f"reqc.{c}_s"] = round(v / cfg.sample_s, 2)
            row[f"reqc.{c}_per_commit"] = round(v / commits, 4) if commits else None
        row["req.ctl_assign_list_s"] = round(req_by.get(("list", "ctl_assign"), 0) / cfg.sample_s, 2)
        row["req.ctl_assign_get_s"] = round(req_by.get(("get", "ctl_assign"), 0) / cfg.sample_s, 2)
        row["req.log_list_s"] = round(req_by.get(("list", "log_segment"), 0) / cfg.sample_s, 2)
        row["req.state_reads_s"] = round(sum(v for (op, c), v in req_by.items() if c == "state_sst" and op in ("get", "get_range", "head")) / cfg.sample_s, 2)
        reads = row["lat.r.n"]
        row["req.state_reads_per_read"] = round(row["req.state_reads_s"] * cfg.sample_s / reads, 3) if reads else None
        for (d, c), v in bytes_by.items():
            row[f"bytes.{d}.{c}"] = v
        # minio's own request counters (independent of the wrapper)
        mm = minio_metrics()
        if mm is not None:
            if self.prev_minio is not None:
                dd = {k: v - self.prev_minio.get(k, 0) for k, v in mm.items()}
                row["minio.req_s"] = round(sum(max(0, v) for v in dd.values()) / cfg.sample_s, 1)
                row["minio.list_s"] = round((dd.get("listobjectsv2", 0) + dd.get("listobjectsv1", 0)) / cfg.sample_s, 2)
            self.prev_minio = mm
        # firehose
        fw = self.fh.take()
        row.update({"fh.events_s": round(fw["events"] / cfg.sample_s, 1), "fh.lag_p50": pct(fw["lag"], 0.5),
                    "fh.lag_p99": pct(fw["lag"], 0.99), "fh.reconnects": fw["reconnects"], "fh.outdated": fw["outdated"],
                    "fh.errors": fw["errors"], "fh.out_of_order": fw["out_of_order"]})
        row["fh.events_per_commit"] = round(fw["events"] / commits, 3) if commits else None
        # object store layout
        row.update(self.s3_sample(lay))
        if lay:
            row["layout.version"] = lay["version"]
            row["layout.shards"] = len(lay["shards"])
            row["layout.next_id"] = lay["nextId"]
        if now - self.last_state_t >= cfg.state_every:
            self.last_state_t = now
            try:
                self.state_snap = self.state_sample(lay)
            except Exception as e:
                self.state_snap = {"state.error": repr(e)[:200]}
        row.update(self.state_snap)
        live = cfg.population * cfg.pop_records + cfg.mid_repos * cfg.mid_records + h["creates"] - h["deletes"]
        row["records.live_est"] = live
        if row.get("state.live_bytes"):
            row["state.live_bytes_per_record"] = round(row["state.live_bytes"] / max(1, live), 1)
            row["state.total_bytes_per_record"] = round(row["state.total_bytes"] / max(1, live), 1)
        if now - self.last_purge > 60:
            self.last_purge = now
            purge_trash()
        row["disk_free_gb"] = round(disk_free_gb(), 1)
        # history counters (cumulative)
        for k, v in h.items():
            row[f"h.{k}"] = v
        row["h.soak_h"] = round(st["soak_s_now"] / 3600, 4)
        return row


# ---------------------------------------------------------------- orchestration

def parse_cycle(s):
    out = []
    for part in s.split(","):
        k, _, v = part.partition(":")
        out.append((k.strip(), float(v)))
    return out


def phase_at(cfg, soak_s):
    tot = sum(d for _, d in cfg.cycle)
    pos = soak_s % tot
    for k, d in cfg.cycle:
        if pos < d:
            return k, d - pos
        pos -= d
    return cfg.cycle[-1][0], 0


def setup(cfg, st, nodes):
    if st.get("setup_done"):
        return
    t = time.time()
    for name, start, count, recs in (("population", 0, cfg.population, cfg.pop_records),
                                     ("mid repos", cfg.population, cfg.mid_repos, cfg.mid_records)):
        if count <= 0 or st.get(f"setup_{name}"):
            continue
        log(f"setup: bulk {name}: {count} accounts x {recs} records")
        procs = [subprocess.Popen([LOADGEN, "--host", n.url, "--threads", "4", "bulk", "--start", str(start), "--count", str(count),
                                   "--batch", str(1 if recs > 500 else 500), "--concurrency", "8", "--dist", "fixed", "--records", str(recs)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for n in nodes]
        outs = [p.communicate() for p in procs]
        if any(p.returncode for p in procs):
            raise RuntimeError(f"bulk {name} failed: {[o[1][-500:] for o in outs]}")
        res = [json.loads(o[0].strip().splitlines()[-1]) for o in outs]
        created = sum(r.get("created", 0) for r in res)
        existing = sum(r.get("existing", 0) for r in res)
        log(f"setup: {name}: created {created}, existing {existing} of {count}")
        st[f"setup_{name}"] = True
        save_state(cfg, st)
    st["setup_done"] = True
    st["setup_s"] = round(time.time() - t, 1)
    save_state(cfg, st)


def do_restart(cfg, st, nodes, ev_path, rng):
    cands = nodes[cfg.stable_nodes:] or nodes
    victim = cands[st["next_victim"] % len(cands)]
    st["next_victim"] += 1
    kind = "kill9" if rng.random() < cfg.kill9_frac else "sigterm"
    ev = {"t": time.time(), "soak_s": round(st["soak_s_now"], 1), "type": "restart", "kind": kind, "node": victim.name}
    t0 = time.time()
    if kind == "kill9":
        victim.kill9()
        time.sleep(cfg.kill_down)
    else:
        ev["exit_s"], ev["exit_code"] = victim.stop(timeout=120)
        ev["exit_s"] = round(ev["exit_s"], 2)
    try:
        ev["start_s"] = round(victim.start(timeout=600), 2)
    except Exception as e:
        ev["error"] = repr(e)[:300]
        log(f"restart: {victim.name} failed to start: {e}")
        jsonl(ev_path, ev)
        raise
    c = wait_converged(nodes, timeout=180)
    ev["converge_s"] = None if c is None else round(c, 2)
    ev["total_s"] = round(time.time() - t0, 2)
    st["h"]["restarts"] += 1
    st["h"][kind] += 1
    st["h"]["incarnations"] += 1
    jsonl(ev_path, ev)
    annotate(f"soak restart {victim.name} ({kind})", ("restart",))
    log(f"restart: {victim.name} {kind}: exit {ev.get('exit_s', '-')} s, serving {ev['start_s']} s, converged {ev['converge_s']} s "
        f"(restarts {st['h']['restarts']})")


def do_reshard(cfg, st, nodes, ev_path, rng):
    lay = get_layout(nodes)
    ev = {"t": time.time(), "soak_s": round(st["soak_s_now"], 1), "type": "reshard"}
    if not lay:
        ev["error"] = "no layout"
        jsonl(ev_path, ev)
        return
    if lay.get("op"):
        ev["error"] = f"op in progress: {lay['op']}"
        jsonl(ev_path, ev)
        return
    shards = sorted(lay["shards"], key=lambda s: s["lo"])
    n = len(shards)
    if n < cfg.shards - cfg.shard_band:
        kind = "split"
    elif n > cfg.shards + cfg.shard_band:
        kind = "merge"
    else:
        kind = "split" if st["h"]["reshards"] % 2 == 0 else "merge"
    if kind == "split":
        cands = [s for s in shards if s["hi"] - s["lo"] >= 2]
        s = rng.choices(cands, weights=[c["hi"] - c["lo"] for c in cands])[0]
        body = {"shard": s["id"], "wait": True}
        path = "/xrpc/vlpds.admin.splitShard"
        ev.update(kind="split", parent=s["id"], width=s["hi"] - s["lo"])
    else:
        i = rng.randrange(n - 1)
        body = {"left": shards[i]["id"], "right": shards[i + 1]["id"], "wait": True}
        path = "/xrpc/vlpds.admin.mergeShards"
        ev.update(kind="merge", parents=[shards[i]["id"], shards[i + 1]["id"]])
    live = [x for x in nodes if x.alive()]
    via = rng.choice(live)
    t0 = time.time()
    try:
        status, resp = http_req("POST", via.url + path, json.dumps(body).encode(), admin_headers(), timeout=180)
        ev["status"] = status
        if status == 200:
            j = json.loads(resp)
            ev["done"] = j.get("done")
            ev["children"] = [c.get("id") for c in (j.get("op") or {}).get("children", [])]
        else:
            ev["error"] = resp[:300].decode(errors="replace")
    except Exception as e:
        ev["error"] = repr(e)[:300]
    ev["secs"] = round(time.time() - t0, 2)
    ev["via"] = via.name
    if ev.get("done"):
        c = wait_converged(nodes, timeout=120)
        ev["converge_s"] = None if c is None else round(c, 2)
        st["h"]["reshards"] += 1
        st["h"]["splits" if kind == "split" else "merges"] += 1
    else:
        st["h"]["reshard_fail"] += 1
    jsonl(ev_path, ev)
    annotate(f"soak {kind} {body}", ("reshard",))
    log(f"reshard: {kind} {body} via {via.name}: {'ok' if ev.get('done') else 'FAILED ' + str(ev.get('error'))} in {ev['secs']} s "
        f"(reshards {st['h']['reshards']}, shards {n} -> {n + (1 if kind == 'split' else -1) if ev.get('done') else n})")


def run(cfg):
    st = load_state(cfg)
    lim = float(os.environ.get("MIN_FREE_GB", "150"))
    if disk_free_gb() < lim:
        raise SystemExit(f"disk free {disk_free_gb():.0f} GB < {lim:.0f} GB")
    if st["soak_s"] >= cfg.hours * 3600:
        log(f"soak done already ({st['soak_s']/3600:.2f} h); `report` to regenerate")
        report(cfg)
        return 0
    os.makedirs(cfg.out, exist_ok=True)
    samples_path = os.path.join(cfg.out, "samples.jsonl")
    ev_path = os.path.join(cfg.out, "events.jsonl")
    rng = random.Random()
    log(f"soak {cfg.name}: {cfg.nodes} nodes (stable {cfg.stable_nodes}), {cfg.shards} shards, population {cfg.population} "
        f"x {cfg.pop_records} + {cfg.mid_repos} mid x {cfg.mid_records}; writes {cfg.write_rate}/s reads {cfg.read_rate}/s; "
        f"cycle {cfg.cycle_str}; resume at {st['soak_s']/3600:.2f} h of {cfg.hours} h; minio data {MINIO_DATA or '?'}; out {cfg.out}")
    nodes = [Node(cfg, i) for i in range(cfg.nodes)]
    stop = {"why": None}

    def on_sig(sig, frm):
        stop["why"] = stop["why"] or f"signal {sig}"
    signal.signal(signal.SIGTERM, on_sig)
    signal.signal(signal.SIGINT, on_sig)
    procs, fh, stop_ev = [], None, None
    ops_holder = []
    rc = 1
    try:
        t = time.time()
        for n in nodes:
            n.start(timeout=900)
        st["h"]["incarnations"] += len(nodes)
        st["h"]["resumes"] += 1
        conv = wait_converged(nodes, timeout=300)
        log(f"nodes up in {time.time()-t:.1f}s, converged {conv}")
        jsonl(ev_path, {"t": time.time(), "soak_s": st["soak_s"], "type": "start", "up_s": round(time.time() - t, 1), "converge_s": conv})
        setup(cfg, st, nodes)
        save_state(cfg, st)
        # load
        lcfg = {"procs": cfg.procs, "urls": [n.url for n in nodes], "population": cfg.population, "zipf_s": cfg.zipf_s,
                "zipf_cap": cfg.zipf_cap, "dh_every": cfg.dh_every, "dh_delete_pct": cfg.dh_delete_pct,
                "delete_pct": cfg.delete_pct, "update_pct": cfg.update_pct, "like_frac": cfg.like_frac,
                "write_rate": cfg.write_rate, "read_rate": cfg.read_rate, "threads": cfg.threads, "max_queue": cfg.max_queue,
                "sample_s": cfg.sample_s, "track_per_repo": 500}
        f_dh, p_norm = op_mix(lcfg)
        log(f"load: delete-heavy repos carry {100*f_dh:.1f}% of writes; ordinary repos delete {100*p_norm:.2f}%")
        mid = [bulk_did(cfg.population + i) for i in range(cfg.mid_repos)]
        ctx = mp.get_context("spawn")
        stats_q = ctx.Queue()
        stop_ev = ctx.Event()
        procs = [ctx.Process(target=load_proc, args=(k, lcfg, stats_q, stop_ev, os.path.join(cfg.state_dir, f"rkeys-{k}.json"), mid), daemon=True)
                 for k in range(cfg.procs)]
        for p in procs:
            p.start()
        fh = Firehose(cfg, nodes, st)
        fh.t.start()
        sampler = Sampler(cfg, nodes, st, fh, cfg.out)
        run_t0 = time.time()
        base_soak = st["soak_s"]
        st["soak_s_now"] = base_soak
        ops_lock = threading.Lock()
        ops_state = {"busy": None, "err": None}

        def ops_loop():
            while stop["why"] is None:
                time.sleep(0.5)
                s = st.get("soak_s_now")
                if s is None or stop["why"] is not None:
                    return
                ph, _ = phase_at(cfg, s)
                try:
                    if ph in ("restart", "mixed") and s >= st["next_restart_at"]:
                        st["next_restart_at"] = s + cfg.restart_every
                        ops_state["busy"] = "restart"
                        do_restart(cfg, st, nodes, ev_path, rng)
                    if ph in ("reshard", "mixed") and s >= st["next_reshard_at"]:
                        st["next_reshard_at"] = s + cfg.reshard_every
                        ops_state["busy"] = "reshard"
                        do_reshard(cfg, st, nodes, ev_path, rng)
                    if s >= st["next_backfill_at"] and cfg.backfill_every > 0 and s - base_soak > cfg.backfill_age:
                        st["next_backfill_at"] = s + cfg.backfill_every
                        ev = fh.backfill_probe(cfg.backfill_age)
                        if ev:
                            ev.update(t=time.time(), soak_s=round(s, 1), type="backfill")
                            jsonl(ev_path, ev)
                except Exception as e:
                    ops_state["err"] = repr(e)
                    stop["why"] = stop["why"] or f"ops failed: {e!r}"
                finally:
                    ops_state["busy"] = None
                with ops_lock:
                    save_state(cfg, st)

        ops_t = threading.Thread(target=ops_loop, daemon=True)
        ops_t.start()
        ops_holder.append(ops_t)
        next_sample = time.time() + cfg.sample_s
        next_guard = time.time() + 60
        guard = os.environ.get("SOAK_GUARD_CMD", "")
        while stop["why"] is None:
            now = time.time()
            st["soak_s_now"] = base_soak + (now - run_t0)
            if st["soak_s_now"] >= cfg.hours * 3600:
                stop["why"] = "done"
                break
            if now >= next_sample:
                next_sample += cfg.sample_s
                ph, _ = phase_at(cfg, st["soak_s_now"])
                lay = get_layout(nodes)
                try:
                    row = sampler.sample(stats_q, now, ph, lay)
                    row["busy"] = ops_state["busy"]
                    jsonl(samples_path, row)
                    log(f"[{st['soak_s_now']/3600:5.2f}h {ph:7s}] w {row.get('load.write_ok_s')}/s p50 {row.get('lat.w.p50')} p99 {row.get('lat.w.p99')} "
                        f"| r p99 {row.get('lat.r.p99')} | req/commit {row.get('req.per_commit')} | logs {row.get('log.ids')} "
                        f"(fence-only {row.get('log.fence_only')}) assign {row.get('assign.keys')} | state {fmt_b(row.get('state.total_bytes'))} "
                        f"(retired {fmt_b(row.get('state.retired_bytes'))}) | rss n1 {row.get('node.n1.rss_mb')} MB series {row.get('node.n1.series')} "
                        f"| fh {row.get('fh.events_s')}/s | h r{st['h']['restarts']} x{st['h']['reshards']}")
                except Exception as e:
                    log(f"sample failed: {e!r}")
            if guard and now >= next_guard:
                next_guard = now + 60
                g = subprocess.run(guard, shell=True, capture_output=True, text=True)
                if g.returncode != 0:
                    stop["why"] = f"guard: {g.stderr.strip() or g.stdout.strip()}"
            if disk_free_gb() < lim:
                stop["why"] = f"disk free < {lim} GB"
            time.sleep(0.2)
        log(f"stopping: {stop['why']}")
        # let a restart/reshard in flight finish (bounded)
        t = time.time()
        while ops_state["busy"] and time.time() - t < 200:
            time.sleep(0.5)
        rc = 0 if stop["why"] == "done" else (11 if "disk" in stop["why"] else (1 if "failed" in stop["why"] else EXIT_PAUSED))
    except Exception as e:
        log(f"soak failed: {e!r}")
        rc = 1
    finally:
        if stop_ev is not None:
            stop_ev.set()
        # Drain stats_q while the load processes wind down: a child's final
        # flush blocks its queue feeder (and so its exit) on a full pipe if
        # nobody reads. Their last windows' ops still count toward history.
        # Children ignore SIGTERM (terminate() is a no-op): kill stragglers.
        t_end = time.time() + 90
        while procs and any(p.is_alive() for p in procs) and time.time() < t_end:
            try:
                m = stats_q.get(timeout=0.5)
            except queue.Empty:
                continue
            except Exception:
                break
            ops = m.get("ops") or {}
            for k_ in ("create", "delete", "update"):
                st["h"][k_ + "s"] += ops.get(k_, 0)
            st["h"]["commits"] += sum(ops.values())
        for p in procs:
            if p.is_alive():
                p.kill()
            p.join(timeout=5)
        if stop["why"] is None:
            stop["why"] = "error"
        if ops_holder:
            ops_holder[0].join(timeout=240)
        if fh:
            st["fh_cursor"] = fh.last_seq
            fh.stop_ev.set()
        if "soak_s_now" in st:
            st["soak_s"] = st.pop("soak_s_now")
        t = time.time()
        for n in nodes:
            try:
                n.stop(timeout=90)
            except Exception as e:
                log(f"stop {n.name}: {e}")
        log(f"nodes stopped in {time.time()-t:.1f}s; soak clock {st['soak_s']/3600:.2f} h of {cfg.hours} h")
        jsonl(ev_path, {"t": time.time(), "soak_s": round(st["soak_s"], 1), "type": "stop", "why": stop["why"], "rc": rc})
        save_state(cfg, st)
        try:
            report(cfg)
        except Exception as e:
            log(f"report failed: {e!r}")
    return rc


def fmt_b(b):
    if b is None:
        return "?"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1000:
            return f"{b:.1f} {u}"
        b /= 1000
    return f"{b:.1f} PB"


# ---------------------------------------------------------------- analysis

def lstsq(X, y):
    """OLS via normal equations: (coefs, std errors, r2)."""
    k = len(X[0])
    A = [[sum(r[i] * r[j] for r in X) for j in range(k)] for i in range(k)]
    b = [sum(r[i] * yy for r, yy in zip(X, y)) for i in range(k)]
    # invert A (Gauss-Jordan with partial pivoting)
    M = [A[i][:] + [1.0 if i == j else 0.0 for j in range(k)] for i in range(k)]
    for c in range(k):
        p = max(range(c, k), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-12:
            return None
        M[c], M[p] = M[p], M[c]
        piv = M[c][c]
        M[c] = [x / piv for x in M[c]]
        for r in range(k):
            if r != c and M[r][c]:
                f = M[r][c]
                M[r] = [x - f * yv for x, yv in zip(M[r], M[c])]
    inv = [row[k:] for row in M]
    coef = [sum(inv[i][j] * b[j] for j in range(k)) for i in range(k)]
    res = [yy - sum(c * x for c, x in zip(coef, r)) for r, yy in zip(X, y)]
    n = len(y)
    sse = sum(e * e for e in res)
    my = sum(y) / n
    sst = sum((yy - my) ** 2 for yy in y) or 1e-12
    s2 = sse / max(1, n - k)
    se = [math.sqrt(max(0.0, s2 * inv[i][i])) for i in range(k)]
    return coef, se, 1 - sse / sst


def corr(a, b):
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return 0.0
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / math.sqrt(va * vb)


# metric key, label, suspected path, mode: "all" samples or "calm" (no restart/reshard
# in progress, none in the last --settle-s), and "n1" means the stable node's series
METRICS = [
    ("lat.w.p50", "write p50 (ms)", "latency", "calm"),
    ("lat.w.p99", "write p99 (ms)", "latency", "calm"),
    ("lat.r_getRecord.p99", "getRecord p99 (ms)", "a: read amplification", "calm"),
    ("lat.r_listRecords.p99", "listRecords p99 (ms)", "a: read amplification", "calm"),
    ("lat.r_syncGetRecord.p99", "sync.getRecord p99 (ms)", "a: read amplification", "calm"),
    ("lat.r_getRepo.p50", "getRepo p50 (ms)", "a: read amplification", "calm"),
    ("lat.r_listRecords_dh.p99", "listRecords delete-heavy p99 (ms)", "f: tombstones", "calm"),
    ("req.per_commit", "object-store requests / commit", "requests", "calm"),
    ("req.state_reads_per_read", "state SST GET+HEAD / client read", "a: read amplification", "calm"),
    ("reqc.state_manifest_s", "manifest requests/s", "requests", "calm"),
    ("reqc.state_sst_s", "SST requests/s", "requests", "calm"),
    ("reqc.ctl_assign_s", "assign/ requests/s (all ops)", "c: retired assign/ records", "calm"),
    ("req.ctl_assign_get_s", "assign/ GETs/s", "c: retired assign/ records", "calm"),
    ("req.log_list_s", "log/ LIST pages/s (wrapper)", "d: dead-log fences", "calm"),
    ("reqc.retention_report_s", "retain/ requests/s", "d: dead-log fences", "calm"),
    ("assign.keys", "assign/ objects", "c: retired assign/ records", "all"),
    ("assign.list_bytes", "assign/ LIST response bytes", "c: retired assign/ records", "all"),
    ("log.ids", "log ids under log/ (incarnations kept)", "d: dead-log fences", "all"),
    ("log.fence_only", "fence-only dead logs", "d: dead-log fences", "all"),
    ("log.list_bytes", "log/ delimiter LIST response bytes", "d: dead-log fences", "all"),
    ("log.live_objects", "objects in live logs", "retention", "all"),
    ("retain.keys", "retain/ reports", "d: dead-log fences", "all"),
    ("state.retired_dirs", "retired state dirs", "b: retired parents", "all"),
    ("state.retired_bytes", "retired state bytes", "b: retired parents", "all"),
    ("state.pinned_bytes", "retired bytes still referenced by live manifests", "a: parent pinning", "all"),
    ("state.live_with_ext", "live shards with external SSTs", "a: parent pinning", "all"),
    ("state.ext_refs_mean", "external dbs per live shard", "a: parent pinning", "all"),
    ("state.total_over_live", "state bytes / live-shard bytes", "a/b: pinning + leak", "all"),
    ("state.live_bytes_per_record", "live-shard bytes / live record", "f: tombstones", "all"),
    ("state.sst_per_shard_mean", "SSTs per live shard", "f: compaction", "all"),
    ("state.manifests_live_total", "manifest objects (live shards)", "requests", "all"),
    ("node.n1.series", "n1 /metrics series", "e: metric cardinality", "all"),
    ("node.n1.rss_mb", "n1 RSS (MB)", "memory", "all"),
    ("node.n1.tasks", "n1 tokio alive tasks", "memory", "all"),
    ("node.n1.cache_mb", "n1 in-memory caches (MB)", "memory", "all"),
    ("node.n1.merge_queue_mb", "n1 firehose merge queues (MB)", "memory", "all"),
    ("fh.lag_p99", "firehose lag p99 (ms)", "latency", "calm"),
]

PATHS = [
    ("a", "Parents pinned by children -> read amplification",
     ["state.pinned_bytes", "state.live_with_ext", "state.ext_refs_mean", "req.state_reads_per_read", "lat.r_getRecord.p99", "lat.r_listRecords.p99"]),
    ("b", "Retired parents' state dirs never deleted", ["state.retired_dirs", "state.retired_bytes", "state.total_over_live"]),
    ("c", "Retired assign/ records grow the per-step LIST", ["assign.keys", "assign.list_bytes", "reqc.ctl_assign_s", "req.ctl_assign_get_s"]),
    ("d", "Dead-log fences grow the log/ LIST with every restart", ["log.ids", "log.fence_only", "log.list_bytes", "req.log_list_s", "backfill.first_event_s"]),
    ("e", "Per-log metric labels (cardinality per restart)", ["node.n1.series"]),
    ("f", "Tombstones linger under size-tiered compaction", ["state.live_bytes_per_record", "lat.r_listRecords_dh.p99", "state.sst_per_shard_mean"]),
    ("-", "Generic: latency, requests/commit, memory", ["lat.w.p99", "req.per_commit", "node.n1.rss_mb", "node.n1.tasks", "fh.lag_p99"]),
]

REGRESSORS = [("h.incarnations", "incarnations"), ("h.reshards", "reshard ops"), ("h.commits", "commits")]


def load_jsonl(p):
    out = []
    if os.path.exists(p):
        for line in open(p):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def analyze(cfg, rows, events):
    """Per metric: simple slopes (per hour, per 100 incarnations, per 100
    reshards, per 1M commits) and a multiple regression on the three history
    counters; verdict by effect size over the run."""
    disturbed = []
    for e in events:
        if e.get("type") in ("restart", "reshard", "start"):
            dur = e.get("total_s") or e.get("secs") or e.get("up_s") or 0
            disturbed.append((e["t"] - 1, e["t"] + dur + (e.get("converge_s") or 0) + cfg.settle_s))
    t_first = rows[0]["t"] if rows else 0
    warm = cfg.warmup_s

    def calm(r):
        return r.get("busy") is None and not any(a <= r["t"] <= b for a, b in disturbed)

    out = {}
    for key, label, path, mode in METRICS:
        pts = [r for r in rows if r.get(key) is not None and r["soak_s"] - rows[0]["soak_s"] >= warm and r["t"] - t_first >= warm
               and (mode == "all" or calm(r))]
        out[key] = analyze_series(key, label, path, mode, pts, lambda r: r[key])
    # backfill: per probe
    bf = [e for e in events if e.get("type") == "backfill" and e.get("first_event_s") is not None]
    hist = rows_hist(rows)
    for e in bf:
        e.update(nearest_hist(hist, e["t"]))
    out["backfill.first_event_s"] = analyze_series("backfill.first_event_s", "backfill time to first event (s)", "d: dead-log fences",
                                                   "probe", bf, lambda r: r["first_event_s"])
    return out


def rows_hist(rows):
    return [(r["t"], {k: r.get(k) for k, _ in REGRESSORS}, r.get("h.soak_h")) for r in rows]


def nearest_hist(hist, t):
    if not hist:
        return {}
    i = min(range(len(hist)), key=lambda j: abs(hist[j][0] - t))
    d = dict(hist[i][1])
    d["h.soak_h"] = hist[i][2]
    return d


def analyze_series(key, label, path, mode, pts, get):
    res = {"key": key, "label": label, "path": path, "mode": mode, "n": len(pts)}
    if len(pts) < 12:
        res["verdict"] = "insufficient data"
        return res
    y = [float(get(r)) for r in pts]
    q = max(1, len(y) // 10)
    first, last = sum(y[:q]) / q, sum(y[-q:]) / q
    mean_abs = sum(abs(v) for v in y) / len(y)
    # counters that start at 0 (retired dirs, fences): relative to the run's mean
    base = first if abs(first) >= 0.05 * mean_abs else (mean_abs or 1e-9)
    res.update(first=round(first, 4), last=round(last, 4), mean=round(sum(y) / len(y), 4),
               change_pct=round(100 * (last - first) / abs(base), 1) if base else None)
    xs = {"h.soak_h": [float(r.get("h.soak_h") or 0) for r in pts]}
    for k, _ in REGRESSORS:
        xs[k] = [float(r.get(k) or 0) for r in pts]
    unit = {"h.soak_h": 1.0, "h.incarnations": 100.0, "h.reshards": 100.0, "h.commits": 1e6}
    res["slopes"] = {}
    for k, xv in xs.items():
        f = lstsq([[1.0, x] for x in xv], y)
        if f:
            res["slopes"][k] = {"per_unit": f[0][1] * unit[k], "r2": round(f[2], 3), "t": f[0][1] / f[1][1] if f[1][1] else 0}
    # multiple regression on the history counters (columns with no variance dropped)
    cols = [k for k, _ in REGRESSORS if max(xs[k]) - min(xs[k]) > 0]
    res["collinear"] = {f"{a}~{b}": round(corr(xs[a], xs[b]), 3) for i, a in enumerate(cols) for b in cols[i + 1:]}
    eff = {}
    if cols:
        f = lstsq([[1.0] + [xs[k][j] for k in cols] for j in range(len(y))], y)
        if f:
            coef, se, r2 = f
            res["mr2"] = round(r2, 3)
            for i, k in enumerate(cols):
                rng_ = max(xs[k]) - min(xs[k])
                e = coef[i + 1] * rng_
                eff[k] = {"coef_per_unit": coef[i + 1] * unit[k], "effect": e, "effect_pct": 100 * e / abs(base) if base else None,
                          "t": coef[i + 1] / se[i + 1] if se[i + 1] else 0.0}
    res["effects"] = eff
    # verdict: a history counter explains >= 10% growth over the run with |t| >= 3
    thr = 10.0
    grows = res["change_pct"] is not None and res["change_pct"] >= thr
    hist_hits = [(k, v) for k, v in eff.items() if grows and k != "h.commits" and v["effect_pct"] is not None and v["effect_pct"] >= thr and v["t"] >= 3]
    data_hit = eff.get("h.commits") if grows else None
    if hist_hits:
        k, v = max(hist_hits, key=lambda kv: kv[1]["effect_pct"])
        res["verdict"] = f"grows with history ({dict(REGRESSORS)[k]}: +{v['effect_pct']:.0f}%)"
    elif data_hit and data_hit["effect_pct"] is not None and data_hit["effect_pct"] >= thr and data_hit["t"] >= 3:
        res["verdict"] = f"grows with data (+{data_hit['effect_pct']:.0f}%)"
    elif res["change_pct"] is not None and abs(res["change_pct"]) < thr:
        res["verdict"] = "flat"
    elif res["change_pct"] is not None and res["change_pct"] <= -thr:
        res["verdict"] = f"falls ({res['change_pct']:.0f}%)"
    else:
        best = max(((k, v) for k, v in res["slopes"].items() if k != "h.soak_h"), key=lambda kv: kv[1]["r2"], default=None)
        name = {"h.soak_h": "time", **dict(REGRESSORS)}
        tag = f"; best single fit: {name[best[0]]} r2 {best[1]['r2']}" if best else ""
        res["verdict"] = f"changes {res['change_pct']:+.0f}%, attribution unclear{tag}"
    return res


def phase_segments(rows):
    """Contiguous runs of samples in one phase: [(phase, [rows])]."""
    segs = []
    for r in rows:
        if segs and segs[-1][0] == r.get("phase"):
            segs[-1][1].append(r)
        else:
            segs.append((r.get("phase"), [r]))
    return segs


def segment_deltas(rows, key, k=3):
    """Per phase type: summed change of `key` across its segments (mean of
    the last k samples minus the first k). Growth that happens only in
    restart (reshard) segments is history-driven; growth in calm segments
    is time/data-driven."""
    out = collections.defaultdict(float)
    nseg = collections.Counter()
    for ph, seg in phase_segments(rows):
        ys = [r.get(key) for r in seg if r.get(key) is not None]
        if len(ys) < 2 * k:
            continue
        out[ph] += sum(ys[-k:]) / k - sum(ys[:k]) / k
        nseg[ph] += 1
    return dict(out), dict(nseg)


def calm_means(rows, key):
    """Mean of `key` in each calm segment, in order (the "is the steady state
    drifting" view for latency and request metrics)."""
    out = []
    for ph, seg in phase_segments(rows):
        ys = [r.get(key) for r in seg if r.get(key) is not None]
        if ph == "calm" and len(ys) >= 3:
            ys = sorted(ys)
            out.append(ys[len(ys) // 2])  # median: robust to a stray spike
    return out


def fmt(v, nd=3):
    if v is None:
        return "-"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:.{nd}g}"
    return str(v)


CSV_COLS = ["t", "soak_s", "phase", "busy", "h.soak_h", "h.incarnations", "h.restarts", "h.kill9", "h.reshards", "h.splits", "h.merges",
            "h.commits", "load.write_ok_s", "commits_s", "lat.w.p50", "lat.w.p99", "lat.r.p50", "lat.r.p99",
            "lat.r_getRecord.p99", "lat.r_listRecords.p99", "lat.r_listRecords_dh.p99", "lat.r_syncGetRecord.p99", "lat.r_getRepo.p50",
            "req.total_s", "req.per_commit", "req.state_reads_per_read", "req.ctl_assign_list_s", "req.ctl_assign_get_s", "req.log_list_s",
            "reqc.log_segment_s", "reqc.state_sst_s", "reqc.state_manifest_s", "reqc.ctl_assign_s", "reqc.ctl_lease_s",
            "reqc.retention_report_s", "reqc.state_gc_boundary_s", "reqc.state_compactions_s", "minio.req_s", "minio.list_s",
            "assign.keys", "assign.retired", "assign.list_bytes", "assign.list_ms", "log.ids", "log.live", "log.dead", "log.fence_only",
            "log.dead_unpruned", "log.list_bytes", "log.live_objects", "log.live_bytes", "retain.keys",
            "layout.shards", "layout.next_id", "state.dirs", "state.retired_dirs", "state.live_bytes", "state.retired_bytes",
            "state.pinned_bytes", "state.unreferenced_retired_bytes", "state.total_bytes", "state.total_over_live",
            "state.live_with_ext", "state.ext_refs_mean", "state.ext_refs_max", "state.sst_live_total", "state.sst_per_shard_mean",
            "state.sst_per_shard_max", "state.manifests_live_total", "records.live_est", "state.live_bytes_per_record",
            "fh.events_s", "fh.events_per_commit", "fh.lag_p50", "fh.lag_p99", "fh.reconnects", "fh.outdated",
            "load.conn_retry", "load.retry_5xx", "disk_free_gb"]


def write_csv(cfg, rows):
    node_cols = []
    for i in range(cfg.nodes):
        n = f"n{i+1}"
        node_cols += [f"node.{n}.rss_mb", f"node.{n}.series", f"node.{n}.tasks", f"node.{n}.cache_mb", f"node.{n}.repo_cache_mb",
                      f"node.{n}.cpu_pct", f"node.{n}.owned", f"node.{n}.uptime_s"]
    slate = sorted({k for r in rows for k in r if k.startswith("node.n1.slatedb_")})
    cols = CSV_COLS + node_cols + slate
    with open(os.path.join(cfg.out, "samples.csv"), "w") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + "\n")
    return cols


def report(cfg):
    rows = load_jsonl(os.path.join(cfg.out, "samples.jsonl"))
    events = load_jsonl(os.path.join(cfg.out, "events.jsonl"))
    if not rows:
        log("report: no samples")
        return
    write_csv(cfg, rows)
    an = analyze(cfg, rows, events)
    with open(os.path.join(cfg.out, "analysis.json"), "w") as f:
        json.dump(an, f, indent=1, default=str)
    last = rows[-1]
    rs = [e for e in events if e.get("type") == "restart"]
    xs = [e for e in events if e.get("type") == "reshard"]
    bfs = [e for e in events if e.get("type") == "backfill"]
    L = [f"# vlpds soak: {cfg.name}", ""]
    L.append(f"Driver `bench/soak/soak.py` (`{' '.join(sys.argv[1:])}`), binaries `{BIN}`, MinIO `{S3_URL}` prefix `{cfg.prefix}`. "
             f"{cfg.nodes} native nodes (ports {cfg.base_port}-{cfg.base_port + cfg.nodes - 1}; n1..n{cfg.stable_nodes} never restarted), "
             f"initial {cfg.shards} shards (kept within +-{cfg.shard_band}), `--log-retention {cfg.log_retention} "
             f"--slatedb-checkpoint-lifetime {cfg.checkpoint_lifetime} --slatedb-gc-min-age {cfg.gc_min_age} --lease-ttl-ms {cfg.lease_ttl_ms}` "
             f"{' '.join(cfg.node_extra)}.")
    L.append("")
    L.append(f"Load: {cfg.write_rate} writes/s (Zipf s={cfg.zipf_s} cap {cfg.zipf_cap} over {cfg.population:,} bulk repos x {cfg.pop_records} "
             f"records; creates/deletes/updates target {100 - cfg.delete_pct - cfg.update_pct:.1f}/{cfg.delete_pct}/{cfg.update_pct} %, every "
             f"{cfg.dh_every}th repo delete-heavy at {cfg.dh_delete_pct}% deletes), {cfg.read_rate} reads/s over {cfg.mid_repos} mid repos x "
             f"{cfg.mid_records} records + fresh records, one firehose subscriber, a cursor backfill every {cfg.backfill_every:g} s "
             f"({cfg.backfill_age:g} s back). Cycle `{cfg.cycle_str}` on the soak clock; restarts every {cfg.restart_every:g} s in `restart` "
             f"phases ({100*cfg.kill9_frac:.0f}% kill -9), split/merge every {cfg.reshard_every:g} s in `reshard` phases.")
    L.append("")
    h = {k[2:]: v for k, v in last.items() if k.startswith("h.")}
    L.append("## History reached")
    L.append("")
    L.append(f"Soak clock {h.get('soak_h', 0):.2f} h; {h.get('incarnations')} node incarnations ({h.get('restarts')} restarts: "
             f"{h.get('sigterm')} SIGTERM, {h.get('kill9')} kill -9; {h.get('resumes')} cluster starts), {h.get('reshards')} reshard ops "
             f"({h.get('splits')} splits, {h.get('merges')} merges, {h.get('reshard_fail')} failed), {h.get('commits'):,} client-acked writes "
             f"({h.get('creates'):,} creates, {h.get('deletes'):,} deletes, {h.get('updates'):,} updates). Layout now {last.get('layout.shards')} shards, "
             f"next id {last.get('layout.next_id')}.")
    L.append("")
    if rs:
        def med(xs_):
            xs_ = sorted(x for x in xs_ if x is not None)
            return xs_[len(xs_) // 2] if xs_ else None
        L.append(f"Restarts: median exit {med([e.get('exit_s') for e in rs])} s (SIGTERM), serving {med([e.get('start_s') for e in rs])} s, "
                 f"converged {med([e.get('converge_s') for e in rs])} s; first/last 5 converge: "
                 f"{[e.get('converge_s') for e in rs[:5]]} / {[e.get('converge_s') for e in rs[-5:]]}.")
    if xs:
        ok = [e for e in xs if e.get("done")]
        L.append(f"Reshards: {len(ok)}/{len(xs)} done; secs first/last 5: {[e.get('secs') for e in ok[:5]]} / {[e.get('secs') for e in ok[-5:]]}. "
                 f"Errors: {[e.get('error') for e in xs if not e.get('done')][:3]}")
    if bfs:
        L.append(f"Backfills: {len(bfs)} probes; first-event s first/last 5: {[e.get('first_event_s') for e in bfs[:5]]} / "
                 f"{[e.get('first_event_s') for e in bfs[-5:]]}; outdated {sum(1 for e in bfs if e.get('outdated'))}; errors "
                 f"{sum(1 for e in bfs if e.get('error'))}.")
    errs = collections.Counter()
    for r in rows:
        for k, v in r.items():
            if k.startswith("err."):
                errs[k[4:]] += v
    L.append(f"Client errors (after retries): {dict(errs) or 'none'}; retried on refused connections {sum(r.get('load.conn_retry', 0) for r in rows):,}, "
             f"on 502/503/504 {sum(r.get('load.retry_5xx', 0) for r in rows):,}. Firehose reconnects {sum(r.get('fh.reconnects', 0) for r in rows)}, "
             f"out-of-order {sum(r.get('fh.out_of_order', 0) for r in rows)}, OutdatedCursor {sum(r.get('fh.outdated', 0) for r in rows)}.")
    L.append("")
    L.append("## Verdict per suspected growth path")
    L.append("")
    L.append("A metric *grows with history* when a multiple regression on cumulative incarnations, reshard ops and commits attributes "
             ">= 10% growth over the run to incarnations or reshards with |t| >= 3; *grows with data* when only commits do; *flat* when the "
             f"last-10% mean is within 10% of the first-10% mean. Latency/request metrics use calm samples only (no restart/reshard in "
             f"progress or within {cfg.settle_s:g} s after converging); the first {cfg.warmup_s:g} s are skipped. Samples are autocorrelated, "
             "so t values are optimistic: read the effect sizes.")
    L.append("")
    L.append("| Path | Metric | n | first -> last | change | verdict |")
    L.append("|---|---|---|---|---|---|")
    for pid, title, keys in PATHS:
        for k in keys:
            a = an.get(k)
            if not a:
                continue
            L.append(f"| {pid}: {title} | {a['label']} | {a['n']} | {fmt(a.get('first'))} -> {fmt(a.get('last'))} | "
                     f"{fmt(a.get('change_pct'))}% | {a.get('verdict')} |")
    L.append("")
    L.append("## Trends (all metrics)")
    L.append("")
    L.append("Slopes from simple linear fits (r2 in parentheses); multiple-regression effects are the growth over the run attributed to each "
             "counter, % of the first-10% mean (t).")
    L.append("")
    L.append("| Metric | mode | per hour | per 100 incarnations | per 100 reshards | per 1M commits | MR effect: incarn. / reshards / commits | MR r2 | verdict |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for key, label, path, mode in METRICS + [("backfill.first_event_s", "backfill time to first event (s)", "", "probe")]:
        a = an.get(key)
        if not a or "slopes" not in a:
            continue
        sl = a["slopes"]
        cell = lambda k: f"{fmt(sl[k]['per_unit'])} ({sl[k]['r2']})" if k in sl else "-"
        ef = a.get("effects", {})
        ecell = " / ".join(f"{ef[k]['effect_pct']:+.0f}% ({ef[k]['t']:.1f})" if k in ef and ef[k]["effect_pct"] is not None else "-"
                           for k, _ in REGRESSORS)
        L.append(f"| {label} | {mode} | {cell('h.soak_h')} | {cell('h.incarnations')} | {cell('h.reshards')} | {cell('h.commits')} | "
                 f"{ecell} | {a.get('mr2', '-')} | {a['verdict']} |")
    L.append("")
    L.append("## Growth by phase segment")
    L.append("")
    L.append("State-like metrics: summed change inside each phase type's segments (last 3 minus first 3 samples of a segment). Growth "
             "concentrated in `restart` segments is per-incarnation, in `reshard` segments per split/merge, in `calm` segments time/data-"
             "driven (writes never stop). Latency/request metrics: median of each calm segment, in order (a drifting steady state shows "
             "as a rising sequence).")
    L.append("")
    phases = [p for p in dict.fromkeys(ph for ph, _ in cfg.cycle)]
    L.append("| Metric | " + " | ".join(f"sum delta in {p} (segments)" for p in phases) + " |")
    L.append("|---|" + "---|" * len(phases))
    warm_rows = [r for r in rows if r["soak_s"] - rows[0]["soak_s"] >= cfg.warmup_s] or rows
    for key, label, path, mode in METRICS:
        if mode != "all":
            continue
        d, n = segment_deltas(warm_rows, key)
        if not d:
            continue
        L.append(f"| {label} | " + " | ".join(f"{fmt(d.get(p))} ({n.get(p, 0)})" for p in phases) + " |")
    L.append("")
    L.append("| Metric (calm segments) | medians in order |")
    L.append("|---|---|")
    for key, label, path, mode in METRICS:
        if mode != "calm":
            continue
        cm = calm_means(warm_rows, key)
        if cm:
            L.append(f"| {label} | {' -> '.join(fmt(x) for x in cm)} |")
    coll = next((a.get("collinear") for a in an.values() if a.get("collinear")), {})
    L.append("")
    L.append(f"Regressor correlations (calm/all samples of the first metric): {coll}. Above ~0.95 the attribution between those counters is unreliable "
             "(run longer cycles, or a schedule with uneven storms).")
    L.append("")
    L.append("## Last sample")
    L.append("")
    keys = ["lat.w.p50", "lat.w.p99", "lat.r.p99", "req.per_commit", "req.total_s", "assign.keys", "assign.list_bytes", "log.ids",
            "log.fence_only", "log.list_bytes", "retain.keys", "state.dirs", "state.retired_dirs", "state.live_bytes", "state.retired_bytes",
            "state.pinned_bytes", "state.total_over_live", "state.live_with_ext", "state.ext_refs_mean", "state.sst_per_shard_mean",
            "node.n1.rss_mb", "node.n1.series", "node.n1.tasks", "fh.lag_p99"]
    L.append(" | ".join(f"`{k}` {fmt(last.get(k))}" for k in keys))
    L.append("")
    notes = os.path.join(cfg.out, "NOTES.md")
    if os.path.exists(notes):
        L.append(open(notes).read().rstrip())
        L.append("")
    L.append("## Files")
    L.append("")
    L.append("`samples.jsonl` (one row per sample: load latency per op, requests by op/component, LIST sizes, state/ breakdown, per-node "
             "gauges + SlateDB gauges, history counters), `samples.csv` (plots-ready subset), `events.jsonl` (restarts, reshards, "
             "backfill probes, starts/stops), `analysis.json` (fits). Regenerate: `bench/soak/soak.py report <same flags>`.")
    path = os.path.join(cfg.out, "RESULTS.md")
    open(path, "w").write("\n".join(L) + "\n")
    log(f"report: {path}")


# ---------------------------------------------------------------- cleanup / main

def cleanup(cfg):
    t = time.time()
    d = os.path.join(MINIO_DATA, BUCKET, cfg.prefix) if MINIO_DATA else ""
    if d and os.path.isdir(d) and cfg.prefix and "/" not in cfg.prefix and ".." not in cfg.prefix:
        shutil.rmtree(d, ignore_errors=True)
        log(f"cleanup: deleted {d} in {time.time()-t:.0f}s")
    else:
        n = S3().delete_prefix(cfg.prefix + "/")
        log(f"cleanup: deleted {n} objects under {cfg.prefix}/ in {time.time()-t:.0f}s")
    purge_trash()
    shutil.rmtree(cfg.state_dir, ignore_errors=True)
    log(f"cleanup: removed {cfg.state_dir}; disk free {disk_free_gb():.0f} GB")


def config(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["run", "report", "status", "cleanup"])
    ap.add_argument("--name", default="soak", help="state dir, MinIO prefix")
    ap.add_argument("--out", default="", help="results dir (default BENCH_OUT_DIR or bench/results/soak-<date>)")
    ap.add_argument("--hours", type=float, default=6.0, help="soak clock length (sum over resumes)")
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--stable-nodes", type=int, default=1, help="first K nodes are never restarted")
    ap.add_argument("--base-port", type=int, default=2700, help="benchbox's Alloy scrapes 2700-2715 every second")
    ap.add_argument("--shards", type=int, default=32)
    ap.add_argument("--shard-band", type=int, default=2, help="split/merge keep the shard count within shards +- band")
    ap.add_argument("--population", type=int, default=50_000)
    ap.add_argument("--pop-records", type=int, default=10)
    ap.add_argument("--mid-repos", type=int, default=16)
    ap.add_argument("--mid-records", type=int, default=2000)
    ap.add_argument("--write-rate", type=float, default=500)
    ap.add_argument("--read-rate", type=float, default=100)
    ap.add_argument("--zipf-s", type=float, default=1.0)
    ap.add_argument("--zipf-cap", type=float, default=0.002, help="max share of writes per writer")
    ap.add_argument("--delete-pct", type=float, default=3.8)
    ap.add_argument("--update-pct", type=float, default=0.3)
    ap.add_argument("--dh-every", type=int, default=25, help="every Nth repo is delete-heavy (0 = none)")
    ap.add_argument("--dh-delete-pct", type=float, default=45)
    ap.add_argument("--like-frac", type=float, default=0.3, help="creates on ordinary repos that are likes")
    ap.add_argument("--procs", type=int, default=4, help="load processes")
    ap.add_argument("--threads", type=int, default=48, help="request threads per load process")
    ap.add_argument("--max-queue", type=int, default=5000)
    ap.add_argument("--cycle", dest="cycle_str", default="restart:600,calm:300,reshard:600,calm:300")
    ap.add_argument("--restart-every", type=float, default=90)
    ap.add_argument("--kill9-frac", type=float, default=0.3)
    ap.add_argument("--kill-down", type=float, default=3, help="s a kill -9'd node stays down")
    ap.add_argument("--reshard-every", type=float, default=90)
    ap.add_argument("--backfill-every", type=float, default=120)
    ap.add_argument("--backfill-age", type=float, default=45, help="cursor this many s back (keep < --log-retention)")
    ap.add_argument("--sample-s", type=float, default=10)
    ap.add_argument("--state-every", type=float, default=60, help="s between state/ LISTs + manifest scans")
    ap.add_argument("--settle-s", type=float, default=30, help="calm = this long after an op converged")
    ap.add_argument("--warmup-s", type=float, default=300, help="skipped by the trend fits")
    ap.add_argument("--log-retention", default="90s")
    ap.add_argument("--checkpoint-lifetime", default="2m")
    ap.add_argument("--gc-min-age", default="2m")
    ap.add_argument("--lease-ttl-ms", type=int, default=10000)
    ap.add_argument("--inject", type=float, default=0, help="--inject-put-ms on every node")
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--io-threads", type=int, default=0)
    ap.add_argument("--block-cache-mb", type=int, default=0)
    ap.add_argument("--repo-cache-mb", type=int, default=0)
    ap.add_argument("--cache-budget-mb", type=int, default=256)
    ap.add_argument("--cache-dir", action="store_true", help="SST disk cache per node")
    c = ap.parse_args(argv)
    c.prefix = c.name
    c.state_dir = os.path.join(SCRATCH, c.name)
    os.makedirs(c.state_dir, exist_ok=True)
    c.out = c.out or os.environ.get("BENCH_OUT_DIR") or os.path.join(PKG, "bench", "results", f"soak-{time.strftime('%Y-%m-%d')}")
    c.cycle = parse_cycle(c.cycle_str)
    cores = os.cpu_count() or 8
    c.io_threads = c.io_threads or max(2, cores // (c.nodes + 1))
    c.workers = c.workers or max(2, cores // (2 * (c.nodes + 1)))
    try:
        ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9
    except (ValueError, OSError):
        ram = 32
    per = 0.3 * ram * 1024 / c.nodes
    c.block_cache_mb = c.block_cache_mb or int(per * 0.4)
    c.repo_cache_mb = c.repo_cache_mb or int(per * 0.4)
    c.node_extra = os.environ.get("NODE_EXTRA", "").split()
    return c


def main():
    cfg = config(sys.argv[1:])
    if cfg.cmd == "cleanup":
        cleanup(cfg)
        return 0
    if cfg.cmd == "status":
        p = os.path.join(cfg.state_dir, "state.json")
        print(open(p).read() if os.path.exists(p) else "{}")
        return 0
    if cfg.cmd == "report":
        report(cfg)
        return 0
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
