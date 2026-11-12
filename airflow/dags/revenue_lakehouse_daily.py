"""Cloud Composer (managed Airflow) DAG for the revenue lakehouse.

Daily, for the batch that landed in GCS:
  ingest_bronze_wap   Spark job (Dataproc Serverless in production) writes to a Nessie branch,
                      audits it, and merges into main only if every check passes.
                      A failed audit fails this task, so nothing downstream runs on bad data.
  dbt_build           builds Silver + Gold on Trino and runs all dbt tests.
  taxonomy_check      enforces naming/ownership/layer rules on every table and refreshes the catalog doc.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.getenv("LAKEHOUSE_DIR", "/home/airflow/gcs/data/revenue-lakehouse")

with DAG(
    dag_id="revenue_lakehouse_daily",
    description="WAP ingestion to Iceberg/Nessie, dbt Silver/Gold on Trino, taxonomy enforcement",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    params={"batch_id": "batch_001"},
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "iceberg", "nessie", "dbt", "revenue"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_bronze_wap",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/ingest_batch.py {{{{ params.batch_id }}}}",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt build --profiles-dir .",
    )
    taxonomy = BashOperator(
        task_id="taxonomy_check",
        bash_command=f"cd {PROJECT_DIR} && python governance/check_taxonomy.py --write-catalog",
    )
    ingest >> dbt_build >> taxonomy
