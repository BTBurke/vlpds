#!/usr/bin/env bash
# Spaces harness (README.md): local PLC, two reference PDSes and MinIO in
# docker, this checkout's vlpds (build-vlpds.sh), then one of the Node
# drivers. Tears the stack down afterwards unless KEEP=1.
#
#   bench/spaces/run.sh e2e [config ...]        (just spaces-e2e)
#   bench/spaces/run.sh sim [seed] [scale]      (just spaces-sim)
#   bench/spaces/run.sh fault [seed]            (just spaces-fault)
#   bench/spaces/run.sh cost                    (just spaces-cost)
#   bench/spaces/run.sh boards [config ...]     (just spaces-boards; boards/README.md)
#   bench/spaces/run.sh boards-ui               (just spaces-boards-ui; UI_E2E=1 runs the headless check and exits)
#   bench/spaces/run.sh boards-prod             (just spaces-boards-prod; the production server; UI_E2E=1 likewise)
#
# Env: VLPDS_BIN (skip the build), BRANCH (build that ref instead of this
# checkout), CLUSTER=1 (3 vlpds nodes on MinIO behind a balancer),
# MEMORY=1 (vlpds --memory), KEEP=1, REF_PDS_IMAGE (use a prebuilt image),
# STORE=r2 (vlpds on a real bucket under bench/<run-id>/, keys from R2_ENV,
# default ~/.config/cloudflare/vlpds-bench-r2.env; README.md "Real R2").
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
# lib/space.mjs and lib/syncer.mjs run on the boards app's Spaces code (packages/boards)
boards="$here/../../../boards"
(cd "$boards" && npm install --no-audit --no-fund --silent)

cleanup() {
  if [ "${KEEP:-}" = 1 ]; then
    echo "KEEP=1: stack left up; stop with: docker compose -p vlpds-spaces down -v"
    return
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ "${STORE:-minio}" = r2 ]; then
  r2env="${R2_ENV:-$HOME/.config/cloudflare/vlpds-bench-r2.env}"
  [ -r "$r2env" ] || { echo "STORE=r2: no $r2env" >&2; exit 2; }
  set -a
  . "$r2env"
  set +a
  export STORE R2_PREFIX="${R2_PREFIX:-bench/$(date -u +%Y%m%d-%H%M%S)-$mode}"
  echo "STORE=r2: prefix $R2_PREFIX (delete it afterwards: ./r2-clean.sh $R2_PREFIX)"
  docker compose up -d --wait ref-a ref-b
else
  docker compose up -d --wait ref-a ref-b minio
  docker compose run --rm minio-init >/dev/null
fi

mkdir -p out
case "$mode" in
  e2e) node e2e.mjs "$@" ;;
  sim) node sim.mjs "$@" ;;
  fault) FAULTS=1 node sim.mjs "$@" ;;
  cost) node cost.mjs "$@" ;;
  boards) node boards/scenarios.mjs "$@" ;;
  boards-ui | boards-prod)
    (cd "$boards/web" && npm install --no-audit --no-fund --silent && npm run build --silent) >/dev/null
    if [ "$mode" = boards-ui ]; then runner=boards/ui.mjs seed=boards/.local/seed-accounts.json check=ui.mjs shots=out/boards-ui/
    else runner=boards/prod.mjs seed=boards/.local/prod-seed.json check=prod.mjs shots=out/boards-prod/; fi
    if [ "${UI_E2E:-}" = 1 ]; then
      rm -f "$seed"
      node "$runner" &
      ui=$!
      until [ -f "$seed" ]; do kill -0 "$ui" 2>/dev/null || exit 1; sleep 1; done
      rc=0
      (cd "$boards/e2e" && npm install --no-audit --no-fund --silent && npx playwright install chromium >/dev/null &&
        SEED_FILE="$here/$seed" SHOTS="$here/$shots" node "$check") || rc=$?
      kill "$ui"
      wait "$ui" || true
      exit "$rc"
    fi
    START_SERVER=1 node "$runner"
    ;;
  *) echo "unknown mode $mode (e2e | sim | fault | cost | boards | boards-ui | boards-prod)" >&2; exit 2 ;;
esac
