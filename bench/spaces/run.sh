#!/usr/bin/env bash
# Spaces harness (README.md): local PLC, two reference PDSes and MinIO in
# docker, the newest vlpds Spaces branch built from a detached checkout, then
# one of the Node drivers. Tears the stack down afterwards unless KEEP=1.
#
#   bench/spaces/run.sh e2e [config ...]        (just spaces-e2e)
#   bench/spaces/run.sh sim [seed] [scale]      (just spaces-sim)
#   bench/spaces/run.sh fault [seed]            (just spaces-fault)
#   bench/spaces/run.sh cost                    (just spaces-cost)
#
# Env: VLPDS_BIN (skip the build), BRANCH (default: origin/spaces-1, else
# origin/spaces-0), CLUSTER=1 (3 vlpds nodes on MinIO behind a balancer),
# MEMORY=1 (vlpds --memory), KEEP=1, REF_PDS_IMAGE (use a prebuilt image).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
mode="${1:-e2e}"
shift || true

export PLC_PORT=2860 REF_A_PORT=2861 REF_B_PORT=2862 VLPDS_PORT=2863 MINIO_PORT=2868
ports="$PLC_PORT $REF_A_PORT $REF_B_PORT $VLPDS_PORT 2864 2865 2866 $MINIO_PORT $(seq 2870 2882 | tr '\n' ' ')"
running="$(docker compose ps -q 2>/dev/null | wc -l | tr -d ' ')"
for p in $ports; do
  case "$p" in $PLC_PORT|$REF_A_PORT|$REF_B_PORT|$MINIO_PORT) [ "$running" != 0 ] && continue ;; esac
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $p is in use (an earlier run? KEEP=1 leftovers: docker compose -p vlpds-spaces down -v)" >&2
    exit 1
  fi
done

./refpds.sh
if [ -z "${VLPDS_BIN:-}" ]; then
  VLPDS_BIN="$(./build-vlpds.sh | tail -1)"
fi
export VLPDS_BIN
export VLPDS_REV="$(cat .scratch/built-branch 2>/dev/null || echo '?') $(cut -c1-12 .scratch/built-rev 2>/dev/null || echo '?')"
npm install --no-audit --no-fund --silent

cleanup() {
  if [ "${KEEP:-}" = 1 ]; then
    echo "KEEP=1: stack left up; stop with: docker compose -p vlpds-spaces down -v"
    return
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --wait ref-a ref-b minio
docker compose run --rm minio-init >/dev/null

mkdir -p out
case "$mode" in
  e2e) node e2e.mjs "$@" ;;
  sim) node sim.mjs "$@" ;;
  fault) FAULTS=1 node sim.mjs "$@" ;;
  cost) node cost.mjs "$@" ;;
  *) echo "unknown mode $mode (e2e | sim | fault | cost)" >&2; exit 2 ;;
esac
