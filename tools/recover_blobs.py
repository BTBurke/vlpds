#!/usr/bin/env python3
"""Recover blobs a migration left behind, from the server the account lived on before.

After a move, `com.atproto.repo.listMissingBlobs` on the new PDS lists blobs the
repo references but the new server doesn't have (the old one couldn't serve
them). If an earlier PDS still has them -- a deactivated account there still
serves its own blobs to its owner -- this downloads each one with the owner's
session there, checks its bytes against the CID, and uploads it to the new PDS.

    python3 recover_blobs.py --did did:plc:... \\
        --new https://pds.example.com \\
        --old https://morel.us-east.host.bsky.network [--login https://bsky.social]

Passwords are prompted for (never arguments). Logging in to the old server
does not reactivate the account there. Blobs are kept in --dir, so a rerun
skips what it already downloaded. Standard library only.
"""

import argparse
import base64
import concurrent.futures as cf
import getpass
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

RAW = 0x55
SHA256 = 0x12


def xrpc(base, nsid, *, params=None, body=None, data=None, ctype=None, token=None, timeout=120):
    url = f"{base.rstrip('/')}/xrpc/{nsid}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    payload = None
    if body is not None:
        payload = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    elif data is not None:
        payload = data
        headers["Content-Type"] = ctype or "application/octet-stream"
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST" if payload is not None else "GET")
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                if "json" in (r.headers.get("Content-Type") or ""):
                    return json.loads(raw or b"{}"), r.headers
                return raw, r.headers
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 5:
                wait = int(e.headers.get("Retry-After") or 30)
                print(f"  rate limited by {base}, waiting {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            try:
                err = json.loads(e.read() or b"{}")
            except ValueError:
                err = {}
            raise RuntimeError(f"{nsid} -> {e.code} {err.get('error', '')} {err.get('message', '')}".strip()) from None
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"{nsid} -> {e}") from None


def login(base, identifier, label):
    password = getpass.getpass(f"Password for {identifier} on {label}: ")
    body = {"identifier": identifier, "password": password}
    try:
        s, _ = xrpc(base, "com.atproto.server.createSession", body=body)
    except RuntimeError as e:
        if "AuthFactorTokenRequired" not in str(e):
            raise
        body["authFactorToken"] = input("Email sign-in code: ").strip()
        s, _ = xrpc(base, "com.atproto.server.createSession", body=body)
    del password, body
    return s


def raw_cid(data):
    """CIDv1, raw codec, sha2-256, base32 -- the form atproto blob CIDs take."""
    digest = hashlib.sha256(data).digest()
    b = bytes([0x01, RAW, SHA256, 32]) + digest
    return "b" + base64.b32encode(b).decode().lower().rstrip("=")


def missing_blobs(new, did, token):
    out, cursor = [], None
    while True:
        params = {"limit": 500}
        if cursor:
            params["cursor"] = cursor
        r, _ = xrpc(new, "com.atproto.repo.listMissingBlobs", params=params, token=token)
        out += [b["cid"] for b in r.get("blobs", [])]
        cursor = r.get("cursor")
        if not cursor or not r.get("blobs"):
            return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--did", required=True)
    ap.add_argument("--new", required=True, help="the PDS the account lives on now")
    ap.add_argument("--old", required=True, help="the earlier PDS that still has the blobs")
    ap.add_argument("--login", help="where to sign in for --old (e.g. https://bsky.social for Bluesky-hosted PDSes); default --old")
    ap.add_argument("--dir", default="recovered-blobs")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--download-only", action="store_true", help="fetch and verify, don't upload")
    a = ap.parse_args()

    new_s = login(a.new, a.did, a.new)
    missing = missing_blobs(a.new, a.did, new_s["accessJwt"])
    print(f"{len(missing)} blobs missing on {a.new}")
    if not missing:
        return

    old_s = login(a.login or a.old, a.did, a.login or a.old)
    if old_s.get("active") is False:
        print(f"(account on the old server is {old_s.get('status', 'inactive')}; reading blobs as its owner)")
    old_tok = old_s["accessJwt"]
    os.makedirs(a.dir, exist_ok=True)

    def one(cid):
        path = os.path.join(a.dir, cid)
        if os.path.exists(path):
            data = open(path, "rb").read()
            ctype = open(path + ".type").read() if os.path.exists(path + ".type") else "application/octet-stream"
        else:
            data, headers = xrpc(a.old, "com.atproto.sync.getBlob", params={"did": a.did, "cid": cid}, token=old_tok)
            ctype = headers.get("Content-Type") or "application/octet-stream"
        if raw_cid(data) != cid:
            return cid, "bytes don't match the CID"
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(data)
            with open(path + ".type", "w") as f:
                f.write(ctype)
        if a.download_only:
            return cid, None
        r, _ = xrpc(a.new, "com.atproto.repo.uploadBlob", data=data, ctype=ctype, token=new_s["accessJwt"])
        got = r.get("blob", {}).get("ref", {}).get("$link")
        return cid, None if got == cid else f"uploaded as {got}"

    ok, failed = 0, {}
    with cf.ThreadPoolExecutor(a.concurrency) as ex:
        futs = {ex.submit(one, c): c for c in missing}
        for i, f in enumerate(cf.as_completed(futs), 1):
            cid = futs[f]
            try:
                _, err = f.result()
            except Exception as e:
                err = str(e)
            if err:
                failed[cid] = err
            else:
                ok += 1
            if i % 25 == 0 or i == len(missing):
                print(f"  {i}/{len(missing)}: {ok} ok, {len(failed)} failed")

    verb = "downloaded" if a.download_only else "recovered"
    print(f"{verb} {ok} of {len(missing)}")
    if failed:
        with open(os.path.join(a.dir, "failed.txt"), "w") as f:
            for c, e in failed.items():
                f.write(f"{c}\t{e}\n")
        print(f"{len(failed)} failed (reasons in {a.dir}/failed.txt); first few:")
        for c, e in list(failed.items())[:5]:
            print(f"  {c}: {e}")
    if not a.download_only:
        left = missing_blobs(a.new, a.did, new_s["accessJwt"])
        print(f"listMissingBlobs on {a.new} now reports {len(left)}")


if __name__ == "__main__":
    main()
