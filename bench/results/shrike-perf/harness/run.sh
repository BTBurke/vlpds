#!/bin/sh
cd "$(dirname "$0")"
date; uptime
nice -n 5 ./target/compare/release/shrike-perf --out results/compare.jsonl > results/compare.txt 2>&1
date; uptime
nice -n 5 ./target/shipped/release/shrike-perf --out results/shipped.jsonl cid car firehose mst proof crypto > results/shipped.txt 2>&1
date; uptime
nice -n 5 ./target/sha2asm/release/shrike-perf --out results/sha2asm.jsonl cid car firehose mst proof crypto > results/sha2asm.txt 2>&1
date; uptime
echo RUN-DONE
