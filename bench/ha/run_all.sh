#!/usr/bin/env bash
# Runs the whole HA matrix (native processes + containers) and prints the
# summary table. Prereqs: MinIO on $VLPDS_HA_S3 (default 127.0.0.1:9200) with a
# `vlpds` bucket (minioadmin/minioadmin); Go; Docker for the ctr-* scenarios.
#
#   bench/ha/run_all.sh                 # everything
#   bench/ha/run_all.sh native          # native-process scenarios only
#   bench/ha/run_all.sh ctr             # container scenarios only
#   bench/ha/run_all.sh kill9-1of3 ...  # named scenarios
#
# Knobs (env): VLPDS_HA_TTL_MS (3000), VLPDS_HA_RATE (150 writes/s per node),
# VLPDS_HA_PARTITIONS (16), VLPDS_HA_NODE_ARGS (node flag template, see
# hactl.py), VLPDS_BIN_DIR (vlpds + loadgen), HA_RUN_ID, SKIP_BUILD=1.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PKG/target/agent-ha}"
export VLPDS_BIN_DIR="${VLPDS_BIN_DIR:-$CARGO_TARGET_DIR/dev-release}"
export HA_RUN_ID="${HA_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"

if [[ "${SKIP_BUILD:-}" != 1 ]]; then
  (source ~/.cargo/env 2>/dev/null || true; cd "$PKG" && cargo build --profile dev-release --bins)
  (cd "$HERE/faultproxy" && go build -o faultproxy .)
  (cd "$HERE/fhaudit" && go build -o fhaudit .)
  (cd "$PKG/checker" && go build -o checker .)
fi

NATIVE=$(python3 "$HERE/hactl.py" list | awk '{print $1}' | grep -v '^ctr-')
CTR=$(python3 "$HERE/hactl.py" list | awk '{print $1}' | grep '^ctr-')
case "${1:-all}" in
  all) SCEN="$NATIVE $CTR" ;;
  native) SCEN="$NATIVE" ;;
  ctr) SCEN="$CTR" ;;
  *) SCEN="$*" ;;
esac
if grep -q ctr- <<<"$SCEN" && [[ "${SKIP_BUILD:-}" != 1 ]]; then
  docker build -f "$HERE/Dockerfile" -t vlpds-ha:local "$PKG"
fi
rc=0
python3 "$HERE/hactl.py" run $SCEN || rc=$?
echo; echo "summary: $HERE/out/$HA_RUN_ID/summary.md"
cat "$HERE/out/$HA_RUN_ID/summary.md"
exit $rc
