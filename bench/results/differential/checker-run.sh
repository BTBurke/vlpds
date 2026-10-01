#!/bin/bash
# vlpds (in-memory) + loadgen writes + both firehose checkers from cursor 0
set -u
V=/path/to/vlpds
B=$V/target/agent-tests
D=/tmp/scratch/ckrun
PORT=${PORT:-2641}
H=http://127.0.0.1:$PORT
cd $D
nice -n 10 $B/dev-release/vlpds --memory --listen 127.0.0.1:$PORT --public-url $H --dev-mode --no-rate-limits > vlpds.log 2>&1 &
VP=$!
for i in $(seq 1 100); do curl -sf $H/xrpc/_health >/dev/null && break; sleep 0.2; done
nice -n 10 $B/dev-release/loadgen --host $H --accounts-file $D/accounts.json setup --prefix ckuser --accounts ${ACCOUNTS:-24} --records ${RECORDS:-40} > setup.log 2>&1
nice -n 10 ./go-checker -host $H -cursor 0 -quiet > go.log 2>&1 &
GP=$!
nice -n 10 $B/release/checker-rs -host $H -cursor 0 -quiet > rs.log 2>&1 &
RP=$!
nice -n 10 $B/dev-release/loadgen --host $H --accounts-file $D/accounts.json run --rate ${RATE:-150} --duration ${DURATION:-60} --update-pct 25 --delete-pct 15 --warmup 0 --firehose > run.log 2>&1
sleep 8
kill -INT $GP $RP
wait $GP $RP
kill $VP; wait $VP 2>/dev/null
echo RUN_DONE
