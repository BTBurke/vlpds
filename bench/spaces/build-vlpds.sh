#!/usr/bin/env bash
# Builds the vlpds under test (--profile dev-release) and prints the binary's
# path as the last line on stdout. By default that's this checkout's
# packages/vlpds as it is on disk, uncommitted changes included; BRANCH=<ref>
# builds a detached checkout of that ref under .scratch/vlpds-src instead.
# Skips the build when the source is clean and hasn't changed since the last.
#
#   bench/spaces/build-vlpds.sh        (BRANCH=origin/some-branch pins a ref)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
scratch="$here/.scratch"
mkdir -p "$scratch"
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"

# One target dir for every checkout of the repo (the main one's), so a
# worktree's first run reuses its compiled dependencies.
common="$(git -C "$here" rev-parse --path-format=absolute --git-common-dir)"
if [ "$(basename "$common")" = .git ]; then
  target="${VLPDS_TARGET_DIR:-$(dirname "$common")/packages/vlpds/bench/spaces/.scratch/target}"
else
  target="${VLPDS_TARGET_DIR:-$scratch/target}"
fi

if [ -n "${BRANCH:-}" ]; then
  case "$BRANCH" in origin/*) git -C "$here" fetch -q origin || echo "fetch failed; using local refs" >&2 ;; esac
  rev="$(git -C "$here" rev-parse "$BRANCH^{commit}")"
  src="$scratch/vlpds-src"
  if [ ! -d "$src" ]; then
    git -C "$here" worktree add -q --detach "$src" "$rev"
  else
    git -C "$src" checkout -q --detach "$rev"
  fi
  label="$BRANCH" dirty=""
  key="$src $rev"
  src="$src/packages/vlpds"
else
  src="$(cd "$here/../.." && pwd)"
  rev="$(git -C "$src" rev-parse HEAD)"
  label=HEAD
  dirty="$(git -C "$src" status --porcelain -- . | head -1)"
  [ -n "$dirty" ] && label="HEAD+dirty"
  key="$src $(git -C "$src" rev-parse HEAD:./)"
fi

# A copy per checkout: another checkout's build replaces the target dir's
# binary, and a driver may restart vlpds from this path mid-run.
bin="$scratch/vlpds"
if [ -z "$dirty" ] && [ -x "$bin" ] && [ "$(cat "$scratch/built-key" 2>/dev/null)" = "$key" ]; then
  echo "vlpds $label ${rev:0:12} already built" >&2
  echo "$bin"
  exit 0
fi
echo "building vlpds $label ${rev:0:12} from $src into $target" >&2
cd "$src"
(cd ui && npm install --no-audit --no-fund --silent && npm run build --silent) >&2
CARGO_TARGET_DIR="$target" nice cargo build --profile dev-release --bin vlpds >&2
rm -f "$bin"
cp -c "$target/dev-release/vlpds" "$bin" 2>/dev/null || cp "$target/dev-release/vlpds" "$bin"
echo "$label" >"$scratch/built-branch"
echo "$rev" >"$scratch/built-rev"
echo "$key" >"$scratch/built-key"
echo "$bin"
