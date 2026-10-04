#!/usr/bin/env python3
"""Memory caps and cache budgets for bench processes (stdlib only).

One host budget: RAM minus BENCH_RESERVE_GB (12) for whatever else lives on
the box (other services, the OS). It is split into
MinIO (docker --memory), the driver, the loadgens and the vlpds nodes; every
vlpds and loadgen process runs in its own `systemd-run --user --scope` with
MemoryMax and MemorySwapMax=0 inside the vlpds-bench slice, whose MemoryMax
is the budget minus MinIO. A runaway is OOM-killed inside its own cgroup and
never pushes the host into swap.

Each node runs with `--memory-budget-mb <its cap>` and sizes its own caches
(src/memory.rs); a requested flag (driver option, NODE_EXTRA / VLPDS_EXTRA)
that `vlpds --memory-plan` says doesn't fit the cap makes the driver refuse
to start (node_flags). Binaries from before the budget get every cache flag
derived here instead (legacy_node_flags).

Env:
  BENCH_MEMCAP        auto (default: wrap when `systemd-run --user` works),
                      require (refuse to start without it; runner.sh sets this), off
  BENCH_RESERVE_GB    12: left for everything that isn't the bench
  BENCH_MEM_GB        the bench budget itself (default RAM - reserve)
  MINIO_MEM_GB        MinIO's cap (default min(12, 25% of the budget))
  BENCH_LOADGEN_MB    1024 per loadgen process
  BENCH_DRIVER_MB     2048 for the driver and its unwrapped helpers
  BENCH_SLICE         vlpds-bench.slice

    memcap.py plan --nodes 4 [--loadgens 4] [--no-minio]   # the split, as JSON
    memcap.py node --cap-mb 8000 [--vlpds BIN] [-- <vlpds flags>]  # one node's memory flags (checked by BIN)
    memcap.py minio-mb | slice-mb | disk <path> | check-avail
"""
import json
import os
import re
import shutil
import subprocess
import sys

GiB = 1024
RESERVE_MB = int(float(os.environ.get("BENCH_RESERVE_GB", "12")) * GiB)
LOADGEN_MB = int(os.environ.get("BENCH_LOADGEN_MB", "1024"))
DRIVER_MB = int(os.environ.get("BENCH_DRIVER_MB", "2048"))
NODE_FLOOR_MB = 2048
SLICE = os.environ.get("BENCH_SLICE", "vlpds-bench.slice")
MODE = os.environ.get("BENCH_MEMCAP", "auto")

if sys.platform.startswith("linux"):
    os.environ.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")


class BudgetError(SystemExit):
    pass


def host_ram_mb():
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    try:
        return int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout) >> 20
    except (ValueError, OSError):
        return 32 * GiB


def mem_available_mb():
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        return None


def bench_mb(ram_mb=None):
    if os.environ.get("BENCH_MEM_GB"):
        return int(float(os.environ["BENCH_MEM_GB"]) * GiB)
    return (ram_mb or host_ram_mb()) - RESERVE_MB


def minio_mb(budget_mb=None):
    if os.environ.get("MINIO_MEM_GB"):
        return int(float(os.environ["MINIO_MEM_GB"]) * GiB)
    b = budget_mb or bench_mb()
    return int(min(12 * GiB, 0.25 * b))


def host_plan(nodes, loadgens=0, minio=True, ram_mb=None, driver=True, budget_mb=None):
    """The host budget split: MiB for MinIO, the driver, each loadgen, each node.
    budget_mb overrides the bench budget (RAM - reserve, or BENCH_MEM_GB)."""
    b = budget_mb or bench_mb(ram_mb)
    m = minio_mb(b) if minio else 0
    d = DRIVER_MB if driver else 0
    lg = LOADGEN_MB * loadgens
    node = (b - m - d - lg) // max(1, nodes) if nodes else 0
    p = {"ram_mb": ram_mb or host_ram_mb(), "reserve_mb": RESERVE_MB, "bench_mb": b, "minio_mb": m, "slice_mb": b - m,
         "driver_mb": d, "loadgen_mb": LOADGEN_MB, "loadgens": loadgens, "nodes": nodes, "node_mb": node}
    if nodes and node < NODE_FLOOR_MB:
        raise BudgetError(f"memcap: {nodes} nodes get {node} MiB each (< {NODE_FLOOR_MB}) after the reserve, MinIO, "
                          f"driver and {loadgens} loadgens: {json.dumps(p)}; lower --nodes or raise BENCH_MEM_GB")
    return p


# ------------------------------------------------------------------ vlpds cache flags

# vlpds sizes its own caches from a memory budget (src/memory.rs): each node
# gets `--memory-budget-mb <its cap>` (the same as the scope's MemoryMax, so
# runs without scopes size alike), and only flags the operator asked for.
# `vlpds --memory-plan` is the refusal check: it exits non-zero when the
# requested flags don't fit the cap.
BUDGET_FLAG = "--memory-budget-mb"

# The flags split_flags pulls out of NODE_EXTRA (passed to vlpds as requested
# sizes), with the defaults of binaries older than BUDGET_FLAG (MiB), which
# legacy_node_flags budgets itself.
DEFAULTS = {"--block-cache-mb": 4096, "--meta-cache-mb": 0, "--repo-cache-mb": 4096, "--lazy-mst-node-cache-mb": 256,
            "--cache-budget-mb": 0, "--firehose-ring-mb": 512, "--live-ring-mb": 128, "--firehose-merge-queue-mb": 256,
            "--backfill-cache-mb": 256, "--backfill-readahead-mb": 64}
POOL = ("--block-cache-mb", "--meta-cache-mb", "--repo-cache-mb")
# readahead is per backfilling subscriber (up to 16); bench runs have a few
BACKFILLS = 4


def headroom_mb(cap):
    return max(1024, int(0.15 * cap))


def split_flags(argv):
    """(the budgeted flags in argv as {flag: MiB}, the rest of argv)."""
    got, rest, i = {}, [], 0
    argv = list(argv)
    while i < len(argv):
        a = argv[i]
        k, eq, v = a.partition("=")
        if k in DEFAULTS:
            if not eq:
                i += 1
                v = argv[i] if i < len(argv) else ""
            try:
                got[k] = int(float(v))
            except ValueError:
                raise BudgetError(f"memcap: {k} needs a number, got {v!r}")
        else:
            rest.append(a)
        i += 1
    return got, rest


_HELP = {}


def vlpds_flags(exe):
    """The long flags a vlpds binary takes (A/B runs use older binaries)."""
    if exe not in _HELP:
        try:
            out = subprocess.run([exe, "--help"], capture_output=True, text=True, timeout=30).stdout
        except OSError:
            out = ""
        _HELP[exe] = set(re.findall(r"(--[a-z0-9-]+)", out)) or set(DEFAULTS)
    return _HELP[exe]


def local_plan(exe):
    """A `check` for node_flags that runs `exe --memory-plan` here."""
    def check(argv):
        # vlpds checks the budget against its own cgroup's memory.max: run it
        # in a scope of the node's cap, not in the (much smaller) driver's
        cap = argv[argv.index(BUDGET_FLAG) + 1] if BUDGET_FLAG in argv else None
        pre = scope_args(int(cap), name="memory-plan") if cap else []
        try:
            r = subprocess.run([*pre, exe, "--memory-plan", *argv], capture_output=True, text=True, timeout=60)
        except OSError as e:
            return False, str(e)
        return r.returncode == 0, (r.stdout.strip().splitlines() or [""])[-1] if r.returncode == 0 else r.stderr.strip()[-600:]
    return check


def node_flags(cap_mb, requested=None, prefer=None, meta_mb=None, supported=None, check=None):
    """The memory flags of one node capped at `cap_mb`: (argv, breakdown).

    A binary with BUDGET_FLAG gets it and the requested flags (operator
    choices: driver options, NODE_EXTRA), which `check(argv) -> (ok, out)`
    (`vlpds --memory-plan`, here or on the node's host) must accept, or this
    refuses; without `check`, vlpds refuses at start instead. A size of 0
    for the block or metadata cache means automatic. `prefer` and `meta_mb`
    only steer older binaries (legacy_node_flags)."""
    supported = supported or set(DEFAULTS) | {BUDGET_FLAG}
    if BUDGET_FLAG not in supported:
        return legacy_node_flags(cap_mb, requested, prefer, meta_mb, supported)
    req = {k: v for k, v in (requested or {}).items() if not (k in ("--block-cache-mb", "--meta-cache-mb") and not v)}
    argv = [BUDGET_FLAG, str(int(cap_mb))]
    for k, v in req.items():
        if k in supported:
            argv += [k, str(int(v))]
    brk = {"cap_mb": int(cap_mb), "requested": req, "flags": dict(zip(argv[::2], argv[1::2]))}
    if check:
        ok, out = check(argv)
        if not ok:
            raise BudgetError(f"memcap: vlpds refuses {' '.join(argv)} for a {int(cap_mb)} MiB node: {out}. "
                              f"Lower the requested sizes or give the node more (fewer nodes, BENCH_MEM_GB).")
        try:
            brk["plan"] = json.loads(out)
        except ValueError:
            pass
    return argv, brk


def legacy_node_flags(cap_mb, requested=None, prefer=None, meta_mb=None, supported=None):
    """node_flags for binaries without BUDGET_FLAG: every budgeted flag,
    derived here. requested: kept as given (refused if they don't fit the
    cap). prefer: wanted sizes for the pool caches not requested, shrunk to
    fit. Without `prefer` the pool fills what's left: meta_mb (or 10%), the
    rest split evenly between block and repo."""
    BLK, META = "--block-cache-mb", "--meta-cache-mb"
    req = dict(requested or {})
    supported = supported or set(DEFAULTS)
    cap = int(cap_mb)
    val = {k: req.get(k, v) for k, v in DEFAULTS.items() if k not in POOL}
    if not val["--cache-budget-mb"]:
        val["--cache-budget-mb"] = max(64, cap // 10)
    fixed = (val["--lazy-mst-node-cache-mb"] + val["--cache-budget-mb"] + val["--firehose-ring-mb"] + val["--live-ring-mb"]
             + val["--firehose-merge-queue-mb"] + val["--backfill-cache-mb"] + val["--backfill-readahead-mb"] * BACKFILLS)
    head = headroom_mb(cap)
    pool = cap - fixed - head
    # meta 0 (or a binary without the flag): a quarter of the block cache, on top of it
    implicit = META not in supported or req.get(META) == 0
    given = {k: req[k] for k in POOL if k in req and not (k == META and implicit)}
    free = [k for k in POOL if k not in given and not (k == META and implicit)]
    left = pool - sum(given.values()) - (given.get(BLK, 0) // 4 if implicit else 0)
    want = {}
    if prefer:
        for k in free:
            if k == META:
                want[k] = prefer.get(META) or (prefer.get(BLK) or DEFAULTS[BLK]) // 4
            else:
                want[k] = prefer.get(k) or DEFAULTS[k]
        need = sum(want.values()) + (want.get(BLK, 0) // 4 if implicit else 0)
        if need > left > 0:
            want = {k: int(v * left / need) for k, v in want.items()}
    elif free:
        if META in free:
            want[META] = int(min(max(meta_mb or 0, 0.1 * left), 0.3 * left))
        rest = [k for k in free if k != META]
        share = (left - want.get(META, 0)) / (len(rest) + (0.25 if implicit and BLK in rest else 0)) if rest else 0
        for k in rest:
            want[k] = int(share)
    out = {**val, **given, **want}
    used = fixed + sum(given.values()) + sum(want.values()) + (out.get(BLK, 0) // 4 if implicit else 0)
    brk = {"cap_mb": cap, "fixed_mb": fixed, "headroom_mb": head, "pool_mb": pool, "used_mb": used + head,
           "meta_implicit": implicit, "flags": {k: v for k, v in out.items() if k in supported}}
    small = [k for k in want if want[k] < 64]
    if used + head > cap or (small and given):
        need = max(used + head, cap - left + 64 * len(free))
        raise BudgetError(f"memcap: requested flags need {need} MiB but the node cap is {cap} MiB "
                          f"(fixed {fixed}: rings, backfill, MST node cache, --cache-budget-mb; headroom {head}; "
                          f"requested {req}, leaving {left} MiB for {free or 'nothing'}): {json.dumps(brk)}. "
                          f"Lower them or give the node more (fewer nodes, BENCH_MEM_GB).")
    if small:
        raise BudgetError(f"memcap: node cap {cap} MiB leaves {', '.join(f'{k} {want[k]}' for k in small)} MiB: {json.dumps(brk)}")
    argv = []
    for k, v in out.items():
        if k in supported and (k in req or k in want or k in given or k == "--cache-budget-mb"):
            argv += [k, str(int(v))]
    return argv, brk


def plan_node_args(exe, cap_mb, extra=(), prefer=None, meta_mb=None, requested=None):
    """node_flags for a native binary, checked with its --memory-plan: pulls
    budgeted flags out of `extra` (they'd be passed twice otherwise, which
    clap rejects). Returns (argv to append, the rest of extra, breakdown)."""
    got, rest = split_flags(extra)
    req = {**(requested or {}), **got}
    sup = vlpds_flags(exe)
    argv, brk = node_flags(cap_mb, req, prefer=prefer, meta_mb=meta_mb, supported=sup,
                           check=local_plan(exe) if BUDGET_FLAG in sup else None)
    return argv, rest, brk


# ------------------------------------------------------------------ scopes

_AVAIL = None


def available():
    """`systemd-run --user --scope` works here (with memory delegated)."""
    global _AVAIL
    if _AVAIL is None:
        _AVAIL = False
        if MODE != "off" and sys.platform.startswith("linux") and shutil.which("systemd-run"):
            try:
                r = subprocess.run(["systemd-run", "--user", "--scope", "--quiet", "--collect", "-p", "MemoryMax=64M", "true"],
                                   capture_output=True, text=True, timeout=30)
                _AVAIL = r.returncode == 0
                if not _AVAIL:
                    log(f"memcap: systemd-run --user --scope failed: {r.stderr.strip()[:300]}")
            except (OSError, subprocess.TimeoutExpired) as e:
                log(f"memcap: systemd-run --user --scope failed: {e}")
        if not _AVAIL and MODE == "require":
            raise BudgetError("memcap: BENCH_MEMCAP=require but `systemd-run --user --scope -p MemoryMax=...` doesn't work here")
    return _AVAIL


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def scope_args(mem_mb, name=None, slice_=None):
    """The `systemd-run ... --` prefix that runs a command in its own capped
    scope (empty when caps are off). systemd-run execs the command in place,
    so the pid a caller holds is the command's."""
    if not available():
        return []
    a = ["systemd-run", "--user", "--scope", "--quiet", "--collect", f"--slice={slice_ or SLICE}",
         "-p", f"MemoryMax={int(mem_mb)}M", "-p", "MemorySwapMax=0"]
    if name:
        a += ["--description", f"vlpds bench {name}"]
    return a + ["--"]


def setup_slice(limit_mb, slice_=None):
    """Cap the whole slice (every wrapped process together)."""
    if not available():
        return False
    r = subprocess.run(["systemctl", "--user", "set-property", "--runtime", slice_ or SLICE, f"MemoryMax={int(limit_mb)}M",
                        "MemorySwapMax=0"], capture_output=True, text=True)
    if r.returncode:
        raise BudgetError(f"memcap: capping {slice_ or SLICE} failed: {r.stderr.strip()[:300]}")
    return True


def minio_cap_mb(container):
    """docker's memory limit of `container` in MiB (0: none; None: not running / no docker)."""
    try:
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}} {{.HostConfig.Memory}} {{.HostConfig.MemorySwap}}", container],
                           capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    p = r.stdout.split()
    if r.returncode or len(p) < 3 or p[0] != "true":
        return None
    mem, swap = int(p[1]), int(p[2])
    if mem and swap != mem:
        return 0  # swap allowed: not a hard cap
    return mem >> 20


def setup_host(nodes, loadgens, minio_container="vlpds-bench-minio", ram_mb=None):
    """Driver start-up on a node host: the split, the slice cap, MinIO's cap
    checked, and the reserve actually free. Returns host_plan's dict."""
    p = host_plan(nodes, loadgens, minio=bool(minio_container), ram_mb=ram_mb)
    if minio_container:
        have = minio_cap_mb(minio_container)
        if have == 0 and (MODE == "require" or available()):
            raise BudgetError(f"memcap: MinIO ({minio_container}) runs without a memory cap (or with swap); "
                              f"restart it with minio.sh (docker --memory {p['minio_mb']}m --memory-swap {p['minio_mb']}m)")
        if have and have > p["minio_mb"]:
            raise BudgetError(f"memcap: MinIO's cap {have} MiB is above its budget {p['minio_mb']} MiB; restart it with minio.sh")
    if available():
        setup_slice(p["slice_mb"])
        p["scopes"] = True
    else:
        log("memcap: no systemd user scopes here; the processes run uncapped (BENCH_MEMCAP=require to refuse)")
        p["scopes"] = False
    avail = mem_available_mb()
    if avail is not None and MODE != "off":
        # what the bench may still claim must fit in what is free now, minus the guard's floor
        floor = min_avail_mb()
        if avail - floor < p["slice_mb"] * 0.5:
            raise BudgetError(f"memcap: only {avail} MiB available now; the bench budget is {p['bench_mb']} MiB with a "
                              f"{floor} MiB floor. Something else is using the reserve")
    log(f"memcap: {json.dumps(p)}")
    return p


def min_avail_mb():
    """runner.sh's memory guard floor: MemAvailable below this aborts the run."""
    return int(float(os.environ.get("MIN_AVAIL_GB", RESERVE_MB / GiB / 2)) * GiB)


# ------------------------------------------------------------------ disks

def disk_of(path):
    """The physical disk(s) holding `path` (through LVM / dm / partitions),
    as a sorted tuple of kernel names; st_dev where /sys isn't there."""
    p = os.path.abspath(path)
    while not os.path.exists(p):
        p = os.path.dirname(p)
    dev = os.stat(p).st_dev
    base = f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}"
    if not os.path.exists(base):
        return (str(dev),)

    def roots(sysdir):
        sysdir = os.path.realpath(sysdir)
        slaves = os.path.join(sysdir, "slaves")
        kids = os.listdir(slaves) if os.path.isdir(slaves) else []
        if kids:
            return set().union(*(roots(os.path.join(slaves, k)) for k in kids))
        if os.path.exists(os.path.join(sysdir, "partition")):
            return {os.path.basename(os.path.dirname(sysdir))}
        return {os.path.basename(sysdir)}
    return tuple(sorted(roots(base)))


def check_cache_disk(cache_dir, minio_data, bulk):
    """An SST disk cache next to MinIO's data during a bulk load saturates the
    drive (measured on a bulk load): refuse it."""
    if not (cache_dir and minio_data and bulk):
        return
    a, b = disk_of(cache_dir), disk_of(minio_data)
    if set(a) & set(b):
        raise BudgetError(f"memcap: the SST disk cache ({cache_dir}, disk {','.join(a)}) is on MinIO's disk ({minio_data}) "
                          f"and a bulk load is pending; populate without the disk cache, or put it on another drive (--cache-root)")


def main(argv):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["plan", "node", "minio-mb", "slice-mb", "disk", "check-avail", "scope-ok"])
    ap.add_argument("path", nargs="?")
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--loadgens", type=int, default=1)
    ap.add_argument("--no-minio", action="store_true")
    ap.add_argument("--ram-gb", type=float, default=0)
    ap.add_argument("--cap-mb", type=int, default=0)
    ap.add_argument("--meta-mb", type=int, default=0)
    ap.add_argument("--prefer-defaults", action="store_true")
    ap.add_argument("--vlpds", default="", help="node: the binary whose --memory-plan checks the flags")
    a, extra = ap.parse_known_args(argv)
    extra = [x for x in extra if x != "--"]
    ram = int(a.ram_gb * GiB) if a.ram_gb else None
    if a.cmd == "plan":
        p = host_plan(a.nodes, a.loadgens, minio=not a.no_minio, ram_mb=ram)
        got, rest = split_flags(extra)
        p["node_flags"] = node_flags(p["node_mb"], got, prefer=DEFAULTS if a.prefer_defaults else None, meta_mb=a.meta_mb)[1]
        print(json.dumps(p, indent=1))
    elif a.cmd == "node":
        got, rest = split_flags(extra)
        if a.vlpds:
            argv_, rest, brk = plan_node_args(a.vlpds, a.cap_mb, extra, prefer=DEFAULTS if a.prefer_defaults else None, meta_mb=a.meta_mb)
        else:
            argv_, brk = node_flags(a.cap_mb, got, prefer=DEFAULTS if a.prefer_defaults else None, meta_mb=a.meta_mb)
        print(" ".join(argv_ + rest))
        log(json.dumps(brk))
    elif a.cmd == "minio-mb":
        print(minio_mb(bench_mb(ram)))
    elif a.cmd == "slice-mb":
        print(bench_mb(ram) - minio_mb(bench_mb(ram)))
    elif a.cmd == "disk":
        print(",".join(disk_of(a.path or ".")))
    elif a.cmd == "check-avail":
        print(json.dumps({"mem_available_mb": mem_available_mb(), "min_avail_mb": min_avail_mb(), "bench_mb": bench_mb(ram)}))
    elif a.cmd == "scope-ok":
        sys.exit(0 if available() else 1)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except BudgetError as e:
        print(e, file=sys.stderr)
        sys.exit(2)
