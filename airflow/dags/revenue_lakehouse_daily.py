"""Airflow DAG: daily write-audit-publish load into the Iceberg/Nessie lakehouse, then dbt and taxonomy.

    start_services ─► ensure_source_files ─► ingest_bronze_wap ─► dbt_build_silver_gold ─► taxonomy_check

One run = one daily batch. The schedule covers three business days with catch-up, so
unpausing the DAG runs them in order:
    2026-07-01 -> batch_001   (published)
    2026-07-02 -> batch_002   (published, schema evolution: billing adds tax_amount)
    2026-07-03 -> batch_003   (broken export: the audit FAILS, main is untouched,
                               and dbt + taxonomy are skipped so nothing downstream sees it)
Trigger it manually with `batch_id` to load a specific batch.

Locally: SeaweedFS (S3 API) for storage, Nessie catalog, Trino, PySpark on the laptop.
On GCP: GCS + Dataproc Serverless for the Spark task, Trino/Starburst for dbt (see README).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

REPO = Path(__file__).resolve().parents[2]
START = datetime(2026, 7, 1)


def sh(cmd: str) -> str:
    return f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; {cmd} "  # trailing space: stops Airflow treating "*.sh" as a template file


BATCH = (
    "{% if params.batch_id %}{{ params.batch_id }}"
    "{% else %}batch_{{ '%03d' % ((data_interval_start.date() - dag.start_date.date()).days + 1) }}{% endif %}"
)

with DAG(
    dag_id="revenue_lakehouse_daily",
    description="Write-audit-publish into Iceberg on Nessie branches, dbt Silver/Gold on Trino, taxonomy enforcement",
    start_date=START,
    end_date=datetime(2026, 7, 3),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    params={"batch_id": Param(None, type=["null", "string"], description="e.g. batch_002; empty = derive from the run date")},
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["project-2", "lakehouse", "iceberg", "nessie", "dbt", "revenue"],
    doc_md=__doc__,
) as dag:
    start_services = BashOperator(task_id="start_services", bash_command=sh("./scripts/start_services.sh"))
    ensure_source_files = BashOperator(
        task_id="ensure_source_files",
        bash_command=sh(
            "if [ -d data/landing/batch_001 ]; then echo 'landing files present'; "
            "else uv run python generator/generate_revenue_data.py; fi"
        ),
    )
    ingest = BashOperator(
        task_id="ingest_bronze_wap",
        bash_command=sh(f"uv run python ingestion/ingest_batch.py {BATCH} 2>&1 | grep -E '^\\[|^ {{10}}' ; exit ${{PIPESTATUS[0]}}"),
    )
    dbt_build = BashOperator(
        task_id="dbt_build_silver_gold",
        bash_command=sh("cd dbt && uv run dbt build --profiles-dir . --no-use-colors"),
    )
    taxonomy = BashOperator(
        task_id="taxonomy_check",
        bash_command=sh("uv run python governance/check_taxonomy.py --write-catalog"),
    )
    start_services >> ensure_source_files >> ingest >> dbt_build >> taxonomy
