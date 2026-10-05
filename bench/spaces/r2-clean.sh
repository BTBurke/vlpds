#!/usr/bin/env bash
# Deletes one STORE=r2 run's prefix and prints how many requests that took
# (LIST pages, then one DELETE per object; R2 bills neither DELETE nor the
# 1-day expiry, but the bench budget counts them). Needs the aws CLI.
#
#   bench/spaces/r2-clean.sh bench/<run-id>      (R2_ENV as in run.sh)
set -euo pipefail
prefix="${1:?usage: r2-clean.sh bench/<run-id>}"
case "$prefix" in bench/?*) ;; *) echo "refusing prefix $prefix (only bench/<run-id>)" >&2; exit 2 ;; esac
set -a
. "${R2_ENV:-$HOME/.config/cloudflare/vlpds-bench-r2.env}"
set +a
export AWS_REGION=auto AWS_DEFAULT_REGION=auto
s3=(aws --endpoint-url "$VLPDS_BENCH_ENDPOINT")
keys="$(mktemp)"
trap 'rm -f "$keys"' EXIT
"${s3[@]}" s3api list-objects-v2 --bucket "$VLPDS_BENCH_BUCKET" --prefix "${prefix%/}/" --query 'Contents[].Key' --output text \
  | tr '\t' '\n' | grep -v '^None$' | grep . >"$keys" || true
n="$(wc -l <"$keys" | tr -d ' ')"
pages=$(( n / 1000 + 1 ))
if [ "$n" -gt 0 ]; then
  "${s3[@]}" s3 rm "s3://$VLPDS_BENCH_BUCKET/${prefix%/}/" --recursive --only-show-errors
fi
left="$("${s3[@]}" s3api list-objects-v2 --bucket "$VLPDS_BENCH_BUCKET" --prefix "${prefix%/}/" --query 'KeyCount' --output text)"
echo "r2-clean $prefix: $n objects, ~$(( 2 * pages + 1 + n )) requests (list pages x2, deletes, a final list); left $left"
