#!/usr/bin/env bash
# Start object storage + Nessie + Trino (idempotent) and make sure the bucket exists.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d >/dev/null
until curl -sf localhost:19120/api/v2/config >/dev/null; do sleep 2; done
until docker compose exec -T trino trino --execute "select 1" >/dev/null 2>&1; do sleep 3; done
uv run python scripts/create_bucket.py
docker compose ps --format '{{.Service}}: {{.Status}}'
