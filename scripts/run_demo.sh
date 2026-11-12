#!/usr/bin/env bash
# Same steps as the Airflow DAGs (revenue_lakehouse_daily x 3 batches + revenue_lakehouse_maintenance), without Airflow.
set -uo pipefail
cd "$(dirname "$0")/.."
quiet() { grep -E "^\[|^ {10}(PASS|FAIL|bronze|schema)" ; }
docker compose down -v >/dev/null 2>&1
./scripts/start_services.sh
uv run python generator/generate_revenue_data.py | head -1
for b in batch_001 batch_002 batch_003; do
  echo "== ingest $b"; uv run python ingestion/ingest_batch.py "$b" 2>&1 | quiet
done
(cd dbt && uv run dbt build --profiles-dir . 2>&1 | grep -E "Done\.|ERROR|FAIL")
uv run python governance/check_taxonomy.py --write-catalog
uv run python scripts/demo_time_travel.py 2>&1 | grep -vE "WARN|log4j|ivy|::|artifact|jars|added as|^\s*\||Setting default|To adjust|^\s*-+$|found |confs"
uv run python scripts/benchmark_queries.py
