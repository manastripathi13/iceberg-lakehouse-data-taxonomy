"""Airflow DAG (manual): lakehouse demos and maintenance.

    time_travel_and_rollback ─► benchmark_bronze_vs_gold

time_travel_and_rollback reads a table as of the `published_batch_001` tag, then deletes a
Gold table and rolls `main` back to the previous Nessie commit. benchmark_bronze_vs_gold times
the same revenue question on raw Bronze vs modelled Gold.
Run after revenue_lakehouse_daily has published at least batch_001.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = Path(__file__).resolve().parents[2]


def sh(cmd: str) -> str:
    return f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; {cmd} "  # trailing space: stops Airflow treating "*.sh" as a template file


with DAG(
    dag_id="revenue_lakehouse_maintenance",
    description="Time travel + rollback demo and Bronze vs Gold benchmark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["project-2", "lakehouse", "maintenance"],
    doc_md=__doc__,
) as dag:
    tt = BashOperator(
        task_id="time_travel_and_rollback",
        bash_command=sh("uv run python scripts/demo_time_travel.py 2>&1 | grep -vE 'WARN|ivy|::|artifact|jars|added as|confs|found |Setting default|To adjust|^\\s*-+$|^\\s*\\|'"),
    )
    bench = BashOperator(task_id="benchmark_bronze_vs_gold", bash_command=sh("uv run python scripts/benchmark_queries.py"))
    tt >> bench
