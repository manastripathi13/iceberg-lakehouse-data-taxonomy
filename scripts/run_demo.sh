#!/usr/bin/env bash
# Full local run from an empty lakehouse:
#   storage + Nessie + Trino -> generate revenue data -> Spark WAP ingestion (3 batches)
#   -> dbt Silver/Gold on Trino -> taxonomy check + catalog -> time travel -> benchmark
set -uo pipefail
cd "$(dirname "$0")/.."
quiet() { grep -E "^\[|^ {10}(PASS|FAIL|bronze|schema)" ; }

echo "== 1. fresh lakehouse (object storage, Nessie catalog, Trino)"
docker compose down -v >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1
until curl -sf localhost:19120/api/v2/config >/dev/null; do sleep 2; done
until docker compose exec -T trino trino --execute "select 1" >/dev/null 2>&1; do sleep 3; done
uv run python scripts/create_bucket.py

echo "== 2. generate messy revenue source data"
uv run python generator/generate_revenue_data.py | head -1

for b in batch_001 batch_002 batch_003; do
  echo "== 3. ingest $b (write -> audit -> publish)"
  uv run python ingestion/ingest_batch.py "$b" 2>&1 | quiet
done

echo "== 4. dbt: build Silver + Gold on Trino and run tests"
(cd dbt && uv run dbt build --profiles-dir . 2>&1 | grep -E "Done\.|ERROR|FAIL")

echo "== 5. taxonomy check on every layer + regenerate data catalog"
uv run python governance/check_taxonomy.py --write-catalog

echo "== 6. time travel and recovery"
uv run python scripts/demo_time_travel.py 2>&1 | grep -vE "WARN|log4j|ivy|::|artifact|jars|added as|^\s*\||Setting default|To adjust|^\s*-+$|found |confs"

echo "== 7. benchmark: same question from Bronze vs Gold"
uv run python scripts/benchmark_queries.py
