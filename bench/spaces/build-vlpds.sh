#!/usr/bin/env bash
# Builds the vlpds under test from a detached checkout of the newest Spaces
# branch (origin/spaces-1, else origin/spaces-0) into one persistent target
# dir, and skips the build when that branch hasn't moved since the last one.
# The binary's path is the last line on stdout.
#
#   bench/spaces/build-vlpds.sh        (BRANCH=origin/spaces-0 pins a branch)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
scratch="$here/.scratch"
src="$scratch/vlpds-src"
target="$scratch/target"
mkdir -p "$scratch"
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"

git -C "$here" fetch -q origin '+refs/heads/spaces-*:refs/remotes/origin/spaces-*' || echo "fetch failed; using local refs" >&2
branch="${BRANCH:-}"
if [ -z "$branch" ]; then
  if git -C "$here" rev-parse -q --verify origin/spaces-1 >/dev/null; then branch=origin/spaces-1; else branch=origin/spaces-0; fi
fi
rev="$(git -C "$here" rev-parse "$branch")"
bin="$target/dev-release/vlpds"
if [ -x "$bin" ] && [ "$(cat "$scratch/built-rev" 2>/dev/null)" = "$rev" ]; then
  echo "vlpds $branch ${rev:0:12} already built" >&2
  echo "$bin"
  exit 0
fi
if [ ! -d "$src" ]; then
  git -C "$here" worktree add -q --detach "$src" "$rev"
else
  git -C "$src" checkout -q --detach "$rev"
fi
echo "building vlpds $branch ${rev:0:12}" >&2
cd "$src/packages/vlpds"
(cd ui && npm install --no-audit --no-fund --silent && npm run build --silent) >&2
CARGO_TARGET_DIR="$target" nice cargo build --profile dev-release --bin vlpds >&2
echo "$branch" >"$scratch/built-branch"
echo "$rev" >"$scratch/built-rev"
echo "$bin"
