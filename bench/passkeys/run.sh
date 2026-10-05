#!/usr/bin/env bash
# Passkeys in headless Chromium (README.md): a local in-memory vlpds on
# http://localhost (a secure context, so WebAuthn works without TLS), then
# e2e.mjs with a CDP virtual authenticator.
#
#   bench/passkeys/run.sh      (or: just passkeys-e2e)
#
# Env: VLPDS_BIN (default: build target/dev-release/vlpds), HEADED=1 (watch
# the browser), SHOTS (screenshot directory, default out/shots/).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$here"

PORT=2790
export VLPDS="http://localhost:$PORT"
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT is in use" >&2
  exit 1
fi

bin="${VLPDS_BIN:-}"
if [ -z "$bin" ]; then
  (cd "$root/ui" && npm install --no-audit --no-fund --silent && npm run build --silent)
  (cd "$root" && cargo build --profile dev-release --bin vlpds)
  bin="${CARGO_TARGET_DIR:-$root/target}/dev-release/vlpds"
fi
npm install --no-audit --no-fund --silent
npx playwright install chromium >/dev/null

mkdir -p out
vlpds_pid=""
cleanup() { [ -n "$vlpds_pid" ] && kill "$vlpds_pid" 2>/dev/null || true; }
trap cleanup EXIT

"$bin" --memory --dev-mode --listen "127.0.0.1:$PORT" --public-url "$VLPDS" \
  --handle-domain vlpds.test --service-did did:web:localhost >out/vlpds.log 2>&1 &
vlpds_pid=$!
for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$PORT/xrpc/_health" >/dev/null && break
  sleep 0.2
done
curl -sf "http://127.0.0.1:$PORT/xrpc/_health" >/dev/null || { tail -20 out/vlpds.log; exit 1; }

node e2e.mjs
