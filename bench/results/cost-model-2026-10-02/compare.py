#!/usr/bin/env python3
""""Defaults changed" (RESULTS.md): per-phase request rates of the before/after runs
(raw_defaults_before.jsonl, raw_defaults_after.jsonl; measure.py --plan defaults), in the
columns of RESULTS.md's measured table, plus the same rates priced as S3 $/mo per node.

    compare.py
"""
import os

import analyze
import cost_model as cm

HERE = os.path.dirname(os.path.abspath(__file__))
MONTH = 730 * 3600


def cols(r):
    p = lambda pred: cm.pick(r, pred)
    return {
        "segment PUT": p(lambda o, c: c == "log_segment" and o in cm.CLASS_A),
        "SST PUT": p(lambda o, c: c == "state_sst" and o in cm.CLASS_A),
        "manifest CAS": p(lambda o, c: c == "state_manifest" and o in cm.CLASS_A),
        "compactions CAS": p(lambda o, c: c == "state_compactions" and o in cm.CLASS_A),
        "polling GETs": p(lambda o, c: c in ("state_manifest", "state_compactions", "state_gc_boundary") and o in cm.CLASS_B),
        "SST GET": p(lambda o, c: c == "state_sst" and o in cm.CLASS_B),
        "LIST": p(lambda o, c: o == "list"),
    }


def main():
    rows = []
    for tag in ("before", "after"):
        for ph in analyze.phases(os.path.join(HERE, f"raw_defaults_{tag}.jsonl")):
            if ph["phase"].endswith("settle"):
                continue
            a, b, db, _ = cm.split(ph["req_s"])
            s3 = (a + db) * MONTH * 0.005 / 1000 + b * MONTH * 0.0004 / 1000
            rows.append((tag, ph, cols(ph["req_s"]), a, b, s3))
    heads = list(rows[0][2])
    print("| run | phase | secs | commits/s | " + " | ".join(heads) + " | Class A | Class B | S3 req $/mo |")
    print("|---" * (len(heads) + 7) + "|")
    for tag, ph, c, a, b, s3 in rows:
        print(f"| {tag} | {ph['phase'].split('/')[-1]} | {ph['secs']} | {ph['commits_s']:.0f} | " + " | ".join(f"{c[h]:.1f}" for h in heads)
              + f" | {a:.1f} | {b:.1f} | ${s3:,.0f} |")


if __name__ == "__main__":
    main()
