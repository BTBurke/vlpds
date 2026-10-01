#!/usr/bin/env bash
# Sliding active-set step against an already-running server with bulk accounts.
#   bench/sim-step.sh <name> <total> <active> <rate> [churn]
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=$1; TOTAL=$2; ACTIVE=$3; RATE=$4; CHURN=${5:-$(( ACTIVE / 100 ))}
OUT=${OUT:-bench/out}; mkdir -p "$OUT"; LOG=${SERVER_LOG:?set SERVER_LOG}
P=$(pgrep -f "release/vlpds.* --listen")
./target/release/loadgen --threads 4 run --rate "$RATE" --hot-rate 200 --duration "${DURATION:-30}" --firehose \
  --sim-total "$TOTAL" --sim-active "$ACTIVE" --sim-churn "$CHURN" > "$OUT/$NAME.loadgen.log" 2>&1
echo "### $NAME: total=$TOTAL active=$ACTIVE rate=$RATE/s churn=$CHURN/s"
sed -n '/=== result ===/,$p' "$OUT/$NAME.loadgen.log" | grep -v "^firehose events"
grep "req/s" "$LOG" | sed 's/\x1b\[[0-9;]*m//g; s/.*vlpds[^ ]* //' | tail -2 | sed -E 's/.*(commit p50[^|]*).*(loads.*)/  server: \1| \2/'
echo "  server RSS: $(ps -o rss= -p $P | awk '{printf "%.1f GB", $1/1048576}')"
