#!/usr/bin/env bash
set -euo pipefail

for id in 1 2 3; do
  admin_port=$((7200 + id))

  if curl -fs "http://127.0.0.1:$admin_port/status" 2>/dev/null \
      | jq -e '.replication.is_leader == true' >/dev/null; then
    echo "$id"
    exit 0
  fi
done

exit 1
