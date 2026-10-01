#!/usr/bin/env bash
# One benchmark step against a fresh RAM-backed MinIO:
#   bench/step.sh <name> <rate> [hot_rate] [inject_put_ms] [extra vlpds args...]
# Env: ACCOUNTS (20000), RECORDS (200), DURATION (30), OUT (bench/out)
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=$1; RATE=$2; HOT=${3:-0}; INJECT=${4:-}; shift $(( $# < 4 ? $# : 4 ))
ACCOUNTS=${ACCOUNTS:-20000}; RECORDS=${RECORDS:-200}; DURATION=${DURATION:-30}
OUT=${OUT:-bench/out}; mkdir -p "$OUT"
docker rm -f vlpds-bench-minio >/dev/null 2>&1 || true
docker run -d --name vlpds-bench-minio -p 9100:9000 --tmpfs /data:size=12g \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin vlpds-minio:local server /data >/dev/null
until curl -sf http://localhost:9100/minio/health/live >/dev/null; do sleep 0.3; done
docker exec vlpds-bench-minio sh -c 'mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null && mc mb -q l/vlpds' >/dev/null
pkill -f "target/release/vlpds " || true; sleep 0.5
rm -rf "$OUT/cache-$NAME"
LAT=(); [ -n "$INJECT" ] && LAT=(--inject-put-ms "$INJECT")
./target/release/vlpds --listen 127.0.0.1:2583 --s3-endpoint http://localhost:9100 --prefix b --no-rate-limits --dev-mode \
  --cache-dir "$OUT/cache-$NAME" "${LAT[@]}" "$@" > "$OUT/$NAME.server.log" 2>&1 &
PID=$!
until grep -q "vlpds serving" "$OUT/$NAME.server.log"; do sleep 0.3; done
./target/release/loadgen --threads 6 --accounts-file "$OUT/accounts-$NAME.json" setup \
  --accounts "$ACCOUNTS" --records "$RECORDS" --prefix "b$NAME" 2>&1 | tail -1
# sample server CPU during the run
( sleep 10; for i in 1 2 3; do ps -o %cpu= -p $PID; sleep 5; done ) > "$OUT/$NAME.cpu" &
./target/release/loadgen --threads 4 --accounts-file "$OUT/accounts-$NAME.json" run \
  --rate "$RATE" --hot-rate "$HOT" --duration "$DURATION" --max-inflight ${MAX_INFLIGHT:-20000} --firehose \
  > "$OUT/$NAME.loadgen.log" 2>&1
sed -n '/=== result ===/,$p' "$OUT/$NAME.loadgen.log"
echo "server cpu% samples: $(tr '\n' ' ' < "$OUT/$NAME.cpu")"
grep "req/s" "$OUT/$NAME.server.log" | sed 's/\x1b\[[0-9;]*m//g; s/.*vlpds[^ ]* //' | tail -2 | cut -c1-330
kill $PID; wait $PID 2>/dev/null || true
