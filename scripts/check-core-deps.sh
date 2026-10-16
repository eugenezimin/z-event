#!/usr/bin/env bash
# The core crate must stay database- and transport-agnostic: it may not
# depend on a database driver, a gRPC stack or any z-event adapter.
set -euo pipefail

bad=$(cargo tree -p z-event --edges normal,build --all-features --target all \
        --prefix none --format '{p}' \
      | awk '{print $1}' | sort -u \
      | grep -E '^(sqlx|sqlx-.+|tokio-postgres|mongodb|tonic|tonic-.+|prost|prost-.+|z-event-.+)$' || true)

if [ -n "$bad" ]; then
  echo "z-event (core) must not depend on:"
  echo "$bad"
  exit 1
fi
echo "z-event (core) has no adapter, database or gRPC dependencies."
