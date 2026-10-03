#!/usr/bin/env bash
# Migration e2e (README.md): local PLC + reference PDS + mail catcher in
# docker, a local vlpds, then e2e.mjs (seed, drive /migrate headlessly,
# verify). Tears everything down afterwards unless KEEP=1.
#
#   bench/migrate/run.sh      (or: just migrate-e2e)
#
# Env: VLPDS_BIN (default: build target/dev-release/vlpds), KEEP=1 (leave
# the stack and vlpds running), HEADED=1 (watch the browser), VLPDS_EXTRA
# (more vlpds flags).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$here"

PLC_PORT=2782 REF_PDS_PORT=2783 VLPDS_PORT=2784 MAIL_PORT=2785
export PLC_PORT REF_PDS_PORT MAIL_PORT
export VLPDS="http://127.0.0.1:$VLPDS_PORT" REF_PDS="http://localhost:$REF_PDS_PORT" PLC="http://127.0.0.1:$PLC_PORT" MAIL="http://127.0.0.1:$MAIL_PORT"

for p in $PLC_PORT $REF_PDS_PORT $VLPDS_PORT $MAIL_PORT; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $p is in use (an earlier run? docker compose -p vlpds-migrate-e2e down -v)" >&2
    exit 1
  fi
done

bin="${VLPDS_BIN:-}"
if [ -z "$bin" ]; then
  (cd "$root/ui" && npm install --no-audit --no-fund --silent && npm run build --silent)
  (cd "$root" && cargo build --profile dev-release --bin vlpds)
  bin="${CARGO_TARGET_DIR:-$root/target}/dev-release/vlpds"
fi
npm install --no-audit --no-fund --silent
npx playwright install chromium >/dev/null

vlpds_pid=""
cleanup() {
  if [ "${KEEP:-}" = 1 ]; then
    echo "KEEP=1: stack and vlpds (pid $vlpds_pid) left running; stop with: kill $vlpds_pid; docker compose -p vlpds-migrate-e2e down -v"
    return
  fi
  [ -n "$vlpds_pid" ] && kill "$vlpds_pid" 2>/dev/null || true
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --build --wait

mkdir -p out
# a fixed dev rotation key: vlpds signs PLC operations with it (local PLC only)
"$bin" --memory --dev-mode --no-rate-limits \
  --listen "127.0.0.1:$VLPDS_PORT" --public-url "$VLPDS" \
  --handle-domain vlpds.test --service-did did:web:vlpds.test \
  --plc-url "$PLC" --plc-mode directory \
  --plc-rotation-key 9f2c1d4e5b6a79880716253443526170f1e2d3c4b5a69788a9b8c7d6e5f40312 \
  --invite-required ${VLPDS_EXTRA:-} >out/vlpds.log 2>&1 &
vlpds_pid=$!
for _ in $(seq 1 100); do
  curl -sf "$VLPDS/xrpc/_health" >/dev/null && break
  sleep 0.2
done
curl -sf "$VLPDS/xrpc/_health" >/dev/null || { tail -20 out/vlpds.log; exit 1; }

node e2e.mjs
