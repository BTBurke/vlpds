#!/usr/bin/env python3
"""Monthly R2 / S3 prices of the tinypds.py runs, and the personal-use
extrapolation (200 commits/day). Markdown on stdout.

    price.py            # uses idle1 pers1 idle64 [tidle1 tpers1] + probe300.json
"""
import json
import os

import analyze

MO = 730 * 3600  # seconds per billing month
R2 = dict(a=4.50e-6, b=0.36e-6, gb=0.015, free_a=1e6, free_b=10e6, free_gb=10)
S3 = dict(a=0.005e-3, b=0.0004e-3, gb=0.023)


def rates(name):
    s = analyze.summarize(name)
    secs = s["secs"]
    a = b = batch = 0.0
    for k, n in s["by_op"].items():
        c, op, comp, res = k.split("|")
        if c == "A":
            a += n
            if op == "delete_batch":
                batch += n
        elif c == "B":
            b += n
    return s, a / secs, b / secs, batch / secs


def r2(a_mo, b_mo, gb=0.0):
    return (max(0, a_mo - R2["free_a"]) * R2["a"] + max(0, b_mo - R2["free_b"]) * R2["b"]
            + max(0, gb - R2["free_gb"]) * R2["gb"])


def s3(a_mo, b_mo, gb=0.0):
    return a_mo * S3["a"] + b_mo * S3["b"] + gb * S3["gb"]


def row(label, a_s, b_s, batch_s, gb=0.0):
    A, B = a_s * MO, b_s * MO
    A_s3 = (a_s - batch_s) * MO  # bulk deletes are free on S3
    return (f"| {label} | {a_s:.3f} | {b_s:.3f} | {A / 1e6:.2f} M | {B / 1e6:.2f} M | "
            f"${r2(A, B, gb):.2f} (${r2(A - batch_s * MO, B, gb):.2f}) | ${s3(A_s3, B, gb):.2f} |")


def main():
    names = [n for n in ["idle1", "pers1", "idle64", "tidle1", "tpers1", "bidle1", "bpers1"] if os.path.exists(n + ".jsonl") or os.path.exists(n + ".jsonl.gz")]
    R = {n: rates(n) for n in names}
    print("| run | Class A /s | Class B /s | Class A /mo | Class B /mo | R2 $/mo (bulk deletes free) | S3 $/mo |")
    print("|---|---|---|---|---|---|---|")
    for n in names:
        s, a, b, batch = R[n]
        print(row(n, a, b, batch))
    print()
    # personal-use extrapolation
    probe = json.load(open("probe_reads.json")) if os.path.exists("probe_reads.json") else {}

    def per_read(kind):
        # only the components a read can touch (blob for getBlob, state SSTs for
        # getRepo): the 10 s probe windows are too short to subtract polling noise
        p = probe.get(kind)
        if not p:
            return 0.0, 0.0
        a = b = 0.0
        for k, v in p["delta"].items():
            c, op, comp = k.split("|")
            if comp in ("blob", "state_sst"):
                a += v if c == "A" else 0
                b += v if c == "B" else 0
        return a / p["n"], b / p["n"]

    for idle, pers in [("idle1", "pers1"), ("tidle1", "tpers1"), ("bidle1", "bpers1")]:
        if idle not in R or pers not in R:
            continue
        s, a, b, batch = R[pers]
        _, a0, b0, batch0 = R[idle]
        c = s["counts"]
        commits = sum(c.get(k, 0) for k in ("like", "post", "repost", "follow"))
        secs = s["secs"]
        # marginal per commit: everything above idle except the blob component and read probes
        blob_a = blob_b = 0.0
        for k, n in s["by_op"].items():
            cl, op, comp, res = k.split("|")
            if comp == "blob":
                blob_a += n if cl == "A" else 0
                blob_b += n if cl == "B" else 0
        ra, rb = per_read("getRepo")
        reads_b = rb * c.get("getRepo", 0)
        ma = ((a - a0) * secs - blob_a) / commits
        mb = ((b - b0) * secs - blob_b - reads_b) / commits
        mbatch = (batch - batch0) * secs / commits
        print(f"**{pers} vs {idle}**: {commits} commits ({c}) in {secs:.0f} s; marginal per commit "
              f"{ma:.2f} Class A (of which {mbatch:.2f} bulk deletes) + {mb:.2f} Class B; "
              f"per blob upload {blob_a / max(1, c.get('blob_upload', 1)):.2f} A; getRepo {ra:.2f} A + {rb:.2f} B; "
              f"getBlob {per_read('getBlob')[1]:.2f} B\n")
        day = dict(commits=200, blobs=5, getblob=20, getrepo=10)
        ga, gb_ = per_read("getBlob")
        A_mo = a0 * MO + 30.42 * (day["commits"] * ma + day["blobs"] * (blob_a / max(1, c.get("blob_upload", 1)))
                                  + day["getrepo"] * ra + day["getblob"] * ga)
        B_mo = b0 * MO + 30.42 * (day["commits"] * mb + day["getrepo"] * rb + day["getblob"] * gb_
                                  + day["blobs"] * 1.0)  # HEAD per upload
        batch_mo = (batch0 + 0) * MO + 30.42 * day["commits"] * mbatch
        for gb in (0.01, 2.0, 20.0):
            print(f"| {idle} + 200 commits/day, 5 blobs/day, 20 getBlob/day, 10 getRepo/day, {gb:g} GB stored | "
                  f"{A_mo / 1e6:.3f} M A | {B_mo / 1e6:.3f} M B | R2 ${r2(A_mo, B_mo, gb):.2f} "
                  f"(bulk deletes free ${r2(A_mo - batch_mo, B_mo, gb):.2f}) | S3 ${s3(A_mo - batch_mo, B_mo, gb):.2f} |")
        print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
