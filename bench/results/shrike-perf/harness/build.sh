#!/bin/sh
# builds the three modes into separate target dirs
set -e
. ~/.cargo/env
cd "$(dirname "$0")"
CARGO_TARGET_DIR=target/compare nice -n 5 cargo build --release --features compare
CARGO_TARGET_DIR=target/shipped nice -n 5 cargo build --release
CARGO_TARGET_DIR=target/sha2asm nice -n 5 cargo build --release --features sha2-asm
echo BUILD-DONE
