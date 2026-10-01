#!/usr/bin/env bash
# CPU profile of a running vlpds as compact text: top functions by self and
# by cumulative time (go tool pprof). Needs a binary built with
# `--features profiling`. See bench/obs/README.md.
#
#   bench/obs/profile.sh [opts] <host:port> [seconds]   sample on demand (GET /debug/pprof/profile)
#   bench/obs/profile.sh -p [opts] [node_id] [seconds]  the last <seconds> from Pyroscope
#                                                       (nodes run with --pyroscope-url)
# opts:
#   -n N       rows per table (default 30)
#   -f REGEX   only stacks through functions matching REGEX (pprof -focus)
#   -i REGEX   drop frames matching REGEX (pprof -ignore)
#   -l         per source line instead of per function (pprof -lines)
#   -svg       also save a flamegraph SVG (on-demand mode only)
#   -o FILE    keep the raw profile at FILE (go tool pprof -http=: FILE to browse)
# env: VLPDS_ADMIN_TOKEN (default dev-admin-token), PYROSCOPE_URL (default http://127.0.0.1:4040)
set -euo pipefail

usage() { sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

rows=30 focus="" ignore="" lines="" svg="" out="" pyro=""
while [[ $# -gt 0 && $1 == -* ]]; do
  case $1 in
    -n) rows=$2; shift 2 ;;
    -f) focus=$2; shift 2 ;;
    -i) ignore=$2; shift 2 ;;
    -l) lines=1; shift ;;
    -svg) svg=1; shift ;;
    -o) out=$2; shift 2 ;;
    -p) pyro=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option $1" >&2; usage ;;
  esac
done

token=${VLPDS_ADMIN_TOKEN:-dev-admin-token}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/vlpds-profile.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
pb=$tmp/cpu.pb

if [[ -n $pyro ]]; then
  node=${1:-}
  secs=${2:-60}
  pyro_url=${PYROSCOPE_URL:-http://127.0.0.1:4040}
  sel='{service_name="vlpds"'
  [[ -n $node ]] && sel+=",node_id=\"$node\""
  sel+='}'
  end=$(($(date +%s) * 1000))
  start=$((end - secs * 1000))
  # connect-protocol protobuf request (a JSON request gets a JSON profile back):
  # SelectMergeProfileRequest{1: profile_typeID, 2: label_selector, 3: start, 4: end}
  python3 - "$sel" "$start" "$end" >"$tmp/req.pb" <<'PY'
import sys
def varint(n):
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)
def string(field, s):
    b = s.encode()
    return varint(field << 3 | 2) + varint(len(b)) + b
sel, start, end = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
sys.stdout.buffer.write(string(1, "process_cpu:cpu:nanoseconds:cpu:nanoseconds") + string(2, sel)
                        + varint(3 << 3) + varint(start) + varint(4 << 3) + varint(end))
PY
  code=$(curl -sS -o "$pb" -w '%{http_code}' -X POST -H 'Content-Type: application/proto' \
    --data-binary @"$tmp/req.pb" "$pyro_url/querier.v1.QuerierService/SelectMergeProfile")
  src="pyroscope $sel, last ${secs}s"
else
  [[ $# -ge 1 ]] || usage
  host=$1
  secs=${2:-10}
  [[ $host == http* ]] || host="http://$host"
  echo "sampling $host for ${secs}s ..." >&2
  code=$(curl -sS -o "$pb" -w '%{http_code}' -H "Authorization: Bearer $token" \
    "$host/debug/pprof/profile?seconds=$secs")
  src="$host, ${secs}s"
fi
if [[ $code != 200 ]]; then
  echo "profile fetch failed: HTTP $code: $(head -c 300 "$pb")" >&2
  exit 1
fi
if [[ ! -s $pb ]]; then
  echo "empty profile (no samples yet? Pyroscope ingests every 10 s)" >&2
  exit 1
fi

args=(-symbolize=none -nodecount="$rows")
[[ -n $focus ]] && args+=(-focus="$focus")
[[ -n $ignore ]] && args+=(-ignore="$ignore")
[[ -n $lines ]] && args+=(-lines)

# Rust symbols: drop hashes, std/core/alloc prefixes, closure noise; cap width
tidy() {
  sed -E -e 's/::h[0-9a-f]{16}//g' -e 's/::\{\{closure\}\}/::{closure}/g' \
    -e 's/(^|<|[ (,&])(std|core|alloc)::/\1/g' -e 's/^(.{150}).*/\1…/'
}
# frames every thread has (thread start, tokio task polling, unwinding
# guards): dropped from the cumulative table's display, numbers unchanged
noise='thread_start|__rust_begin_short_backtrace|catch_unwind|do_call|call_once|FnOnce|poll_future|enter_runtime|\[libsystem\]|tokio::runtime::(task|scheduler|context|blocking|park)|^ *[0-9.]+[a-z]* +[0-9.]+% +[0-9.]+% +[0-9.]+[a-z]* +[0-9.]+% +(run|poll|\{closure#[0-9]+\}|with_mut.*|set_scheduler.*|block_on.*|run_task|[a-z_]+ @(tokio|std|core|alloc):[^ ]+)( @[^ ]+)?$'

report() { # extra pprof flags...
  go tool pprof "${args[@]}" "$@" -top "$pb" 2>/dev/null
}

all=$(report)
summary=$(grep -E '^Duration' <<<"$all" | sed -E 's/Total samples = ([^ ]+) \(([^)]+)\)/\1 CPU (\2 of one core)/')
# macOS can't name system-library frames (profiling.rs folds them into [libsystem])
sys=$(awk '$NF=="[libsystem]" {print $2}' <<<"$all")
echo "== CPU profile: $src | $summary${sys:+ | syscalls/libsystem: $sys self}"
echo "== top $rows by self (flat; [libsystem] time charged to its caller) =="
report -hide='^\[libsystem\]$' | sed -n '/flat%/,$p' | tidy
echo "== top $rows by cumulative (thread/runtime plumbing rows omitted) =="
args[1]=-nodecount=$((rows * 6))
report -cum | sed -n '/flat%/,$p' | grep -Ev "$noise" | head -n $((rows + 1)) | tidy

if [[ -n $out ]]; then
  cp "$pb" "$out"
  echo "raw profile: $out (go tool pprof -http=: $out)" >&2
fi
if [[ -n $svg && -z $pyro ]]; then
  f="${out:-./vlpds-cpu}"; f="${f%.pb}.svg"
  echo "sampling again for the flamegraph ..." >&2
  curl -sS -f -o "$f" -H "Authorization: Bearer $token" "$host/debug/pprof/profile?seconds=$secs&format=svg"
  echo "flamegraph: $f" >&2
fi
