#!/usr/bin/env bash
# The reference implementation, pinned: clones bluesky-social/atproto at the
# Spaces alpha pin into .scratch/atproto (blobless, so small) and builds the
# reference PDS image from it natively, with the Dockerfile of the published
# pds-spaces-alpha build (79d6307e: the same packages as the pin; the pin's
# own Dockerfile misses two workspace packages). Skips what already exists.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PIN=5b95b2f2723a2882824fbb0b819fd6aad884ef20
DOCKERFILE_REV=79d6307e19908f3af406838ea0e5fae42847f182
IMAGE="vlpds-spaces-refpds:${PIN:0:8}"
src="$here/.scratch/atproto"
mkdir -p "$here/.scratch"
if [ ! -d "$src/.git" ]; then
  git clone -q --filter=blob:none --no-checkout https://github.com/bluesky-social/atproto.git "$src"
fi
if [ "$(git -C "$src" rev-parse HEAD 2>/dev/null)" != "$PIN" ]; then
  git -C "$src" fetch -q origin "$PIN" "$DOCKERFILE_REV"
  git -C "$src" -c advice.detachedHead=false checkout -q "$PIN"
fi
if [ -n "${REF_PDS_IMAGE:-}" ] || docker image inspect "$IMAGE" >/dev/null 2>&1; then
  exit 0
fi
git -C "$src" cat-file -e "$DOCKERFILE_REV" 2>/dev/null || git -C "$src" fetch -q origin "$DOCKERFILE_REV"
git -C "$src" show "$DOCKERFILE_REV:services/pds/Dockerfile" >"$here/.scratch/ref-pds.Dockerfile"
echo "building $IMAGE (atproto $PIN; ~10 min the first time)" >&2
docker build -q -t "$IMAGE" -f "$here/.scratch/ref-pds.Dockerfile" "$src" >&2
