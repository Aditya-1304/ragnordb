#!/usr/bin/env bash
set -euo pipefail

BASE="${1:?usage: clone-cluster-baseline.sh BASE DEST}"
DEST="${2:?usage: clone-cluster-baseline.sh BASE DEST}"

rm -rf "$DEST"
mkdir -p "$DEST"

for id in 1 2 3; do
  cp -a --reflink=auto "$BASE/node-$id" "$DEST/node-$id"
done
