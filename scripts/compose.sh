#!/usr/bin/env bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  exec docker compose "$@"
fi

echo "error: Docker Compose v2 is required (the 'docker compose' plugin)." >&2
echo "      Install it or use Docker Desktop. See https://docs.docker.com/compose/install/" >&2
exit 1
