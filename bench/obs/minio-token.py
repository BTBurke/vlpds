#!/usr/bin/env python3
"""Writes prometheus/minio.token: the bearer JWT MinIO accepts on
/minio/v2/metrics/* (what `mc admin prometheus generate` prints), HS512 over
{sub: access key, iss: prometheus, exp} with the secret key. Stdlib only.

    python3 bench/obs/minio-token.py [access_key] [secret_key]   (default minioadmin/minioadmin)
"""
import base64
import hashlib
import hmac
import json
import os
import sys
import time

access = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
secret = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("MINIO_SECRET_KEY", "minioadmin")


def b64(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


head = b64(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
body = b64(json.dumps({"exp": int(time.time()) + 10 * 365 * 86400, "sub": access, "iss": "prometheus"}, separators=(",", ":")).encode())
sig = b64(hmac.new(secret.encode(), f"{head}.{body}".encode(), hashlib.sha512).digest())
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prometheus", "minio.token")
with open(out, "w") as f:
    f.write(f"{head}.{body}.{sig}\n")
print(f"wrote {out}")
