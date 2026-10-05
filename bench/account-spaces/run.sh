#!/usr/bin/env bash
# The account page's "Your spaces" section in headless Chromium (README.md):
# a local in-memory vlpds with --spaces on http://127.0.0.1 (where the page
# uses a loopback OAuth client), a second one without --spaces, then e2e.mjs.
#
#   bench/account-spaces/run.sh      (or: just account-spaces-e2e)
#
# Env: VLPDS_BIN (default: build target/dev-release/vlpds), HEADED=1 (watch
# the browser), SHOTS (screenshot directory, default out/shots/).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$here"

PORT=2793
OFF_PORT=2794
export VLPDS="http://127.0.0.1:$PORT"
export VLPDS_OFF="http://127.0.0.1:$OFF_PORT"
for p in "$PORT" "$OFF_PORT"; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $p is in use" >&2
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

mkdir -p out
pids=()
cleanup() { [ ${#pids[@]} -gt 0 ] && kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

"$bin" --memory --dev-mode --spaces --listen "127.0.0.1:$PORT" --public-url "$VLPDS" \
  --handle-domain vlpds.test --service-did did:web:vlpds.test >out/vlpds.log 2>&1 &
pids+=($!)
"$bin" --memory --dev-mode --listen "127.0.0.1:$OFF_PORT" --public-url "$VLPDS_OFF" \
  --handle-domain vlpds.test --service-did did:web:vlpds.test >out/vlpds-off.log 2>&1 &
pids+=($!)
for p in "$PORT" "$OFF_PORT"; do
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$p/xrpc/_health" >/dev/null && break
    sleep 0.2
  done
  curl -sf "http://127.0.0.1:$p/xrpc/_health" >/dev/null || { tail -20 out/vlpds*.log; exit 1; }
done

node e2e.mjs
