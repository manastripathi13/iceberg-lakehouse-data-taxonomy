# Revenue Data Lakehouse: Apache Iceberg, Medallion Architecture & Data Taxonomy

A lakehouse for a B2B travel-tech SaaS company's revenue data. CRM (Salesforce-style), billing (ERP) and finance (FX) data are loaded into Apache Iceberg tables with Git-like version control (Project Nessie), then modelled into Bronze, Silver and Gold layers with dbt. The output is trusted revenue reporting: MRR/ARR bridge, churn, AR aging and sales pipeline.

**Stack:** Apache Iceberg · Trino · GCS (S3-compatible storage locally) · Cloud Composer (Airflow) · dbt · Project Nessie · PySpark

**System design:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the architecture diagrams and a step-by-step walkthrough of the design, decisions and failure handling.

```
 CRM (Salesforce CSV) ┐                        ┌──────────── Nessie catalog (branches, tags, commits) ───────────┐
 Billing (ERP JSON)   ├─► landing ─► PySpark ─►│ branch ingest_<batch>: BRONZE ─► audit ─► merge to main + tag   │
 Finance (FX CSV)     ┘   files     (WAP)      │ main: BRONZE ─► dbt on Trino ─► SILVER ─► GOLD ─► BI / finance  │
                                               └──────────── data files: Iceberg/Parquet on object storage ──────┘
                                     governance/taxonomy.yml enforced on every table ─► docs/DATA_CATALOG.md
```

## Results from a local run (`make demo`)

| What | Result |
|---|---|
| Batches published | `batch_001`, `batch_002`. `batch_003` (null IDs, negative amounts) was **blocked**; main never saw it. |
| Schema evolution | Billing added `tax_amount` in batch 2. The column was added to the live table with no rewrite or downtime; older rows read as null. |
| Account de-duplication | 197 CRM account records resolved to **180 real customers** (17 duplicates merged). |
| dbt | 43 models, seeds and tests pass, including an MRR bridge reconciliation test. |
| Taxonomy | 20 of 20 tables pass naming, ownership, layer and required-column rules. |
| Time travel | `bronze.billing_invoices` at tag `published_batch_001`: 883 rows, no `tax_amount`. On `main`: 1,196 rows, with `tax_amount`. |
| Recovery | Accidental `DELETE` of a Gold table, then rollback of `main` to the previous Nessie commit: all rows back in one step. |
| Query time | "Monthly revenue by segment" takes 283 ms from Bronze and 111 ms from Gold, a **61% reduction**, with identical answers. |

Sample Gold output (`gold.rpt_revenue_monthly`, synthetic data):

| Month | MRR | ARR | New | Expansion | Contraction | Churn | Customers |
|---|---|---|---|---|---|---|---|
| 2026-04 | $869K | $10.4M | $42K | $13K | -$31K | $0 | 108 |
| 2026-05 | $905K | $10.9M | $43K | $25K | -$30K | -$2K | 115 |
| 2026-06 | $942K | $11.3M | $37K | $27K | -$16K | -$11K | 121 |

## Run it with Airflow (the main way)

All pipeline runs go through Airflow. The DAGs live in [`airflow/dags/`](airflow/dags/):

| DAG | Schedule | Tasks |
|---|---|---|
| `revenue_lakehouse_daily` | `@daily`, 2026-07-01 to 2026-07-03 with catch-up | `start_services` → `ensure_source_files` → `ingest_bronze_wap` → `dbt_build_silver_gold` → `taxonomy_check` |
| `revenue_lakehouse_maintenance` | manual | `time_travel_and_rollback` → `benchmark_bronze_vs_gold` |

One daily run loads one batch. Unpausing `revenue_lakehouse_daily` plays the three days in order:

| Run | Batch | Result in Airflow |
|---|---|---|
| 2026-07-01 | `batch_001` | all green: published to `main` |
| 2026-07-02 | `batch_002` | all green: published, `tax_amount` column added without downtime |
| 2026-07-03 | `batch_003` | `ingest_bronze_wap` **red**: audit found 5 null IDs and 5 negative amounts, branch kept, `main` untouched; dbt and taxonomy marked `upstream_failed` so nothing downstream sees bad data |

Trigger it manually with `batch_id` to load a specific batch.

**Start a local Airflow 3 pointed at this repo's DAGs** (web UI on http://localhost:8081, no login):

```bash
uv venv --python 3.12 ~/airflow-venv
curl -sO https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt
uv pip install --python ~/airflow-venv/bin/python -c constraints-3.12.txt apache-airflow==3.2.2 apache-airflow-providers-standard

export AIRFLOW_HOME=~/airflow AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False \
       AIRFLOW__API__PORT=8081 AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
       AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8081/execution/ PATH="$HOME/airflow-venv/bin:$PATH"
airflow standalone
```

Tasks call this repo's own `uv` environment (`uv run ...`), so Airflow and the project keep separate dependencies. The constraints file pins Airflow's dependencies to the versions it was tested with; without it a newer Pydantic breaks Airflow 3.2's API server.

`start_services` brings up storage, Nessie and Trino with Docker, so Docker Desktop must be running.

## Run it without Airflow

Requirements: Docker Desktop, [uv](https://docs.astral.sh/uv/), Java 17+ (for Spark).

```bash
uv sync
make demo      # fresh lakehouse → generate data → ingest 3 batches → dbt → taxonomy → time travel → benchmark
make test      # unit tests for the taxonomy rules
```

Services: SeaweedFS (S3 API, stands in for GCS) on `:8333`, Nessie on `:19120`, Trino on `:8080`. You can query Trino directly:

```bash
docker compose exec trino trino --execute "select * from lakehouse.gold.rpt_revenue_monthly order by revenue_month"
```

## On Google Cloud

| Local | GCP |
|---|---|
| SeaweedFS bucket `s3://lakehouse` | GCS bucket `gs://<bucket>/warehouse`. Set Nessie's `nessie.catalog.warehouses.warehouse.location` to it and configure `nessie.catalog.service.gcs.*`. |
| PySpark on a laptop | Dataproc Serverless: same `ingest_batch.py`, with `iceberg-gcp-bundle` and `GCSFileIO` instead of the AWS bundle. |
| Trino in Docker | Trino on GKE or Starburst Galaxy, with `fs.native-gcs.enabled=true` in the catalog file. |
| `scripts/run_demo.sh` | `airflow/dags/revenue_lakehouse_daily.py` on Cloud Composer. |

> Only the local setup has been run end to end. The GCP column is the standard mapping for each component.

---

## Explained from zero

### The business problem
Revenue data was spread across systems that disagree with each other:
- **CRM**: who the customers are and what deals are in progress.
- **Billing**: what was invoiced and paid, in many currencies.
- **Finance**: exchange rates.

The old data lake was just files in a bucket:
- When a source added a column, downstream jobs broke.
- Nobody knew which table to trust, who owned it, or what it meant.
- A bad load overwrote good data, with no way back.
- The same customer appeared twice in the CRM, so revenue per customer was wrong.

### The building blocks

| Piece | Plain-words explanation | Role here |
|---|---|---|
| **Object storage (GCS)** | A huge, cheap hard drive in the cloud that stores files. | Holds every data file (Parquet) and metadata file. |
| **Apache Iceberg** | A *table format*: rules plus metadata files that make a folder of Parquet files behave like a proper database table. | Gives ACID transactions (a write either fully happens or not at all), schema evolution, and snapshots. |
| **Project Nessie** | A *catalog* (the list of tables and where their metadata lives), with Git-like branches, commits and tags. | Load into a branch, check it, merge to main. Tag every published batch. Roll back the whole lakehouse. |
| **PySpark** | Python API for Apache Spark, an engine that processes big data in parallel. | Reads landing files and writes Bronze Iceberg tables. |
| **Trino** | A fast SQL engine that queries data where it sits, without loading it into a database first. | Runs dbt's SQL and serves BI queries. |
| **dbt** | You write `select` statements; dbt builds them in dependency order and tests them. | Builds Silver and Gold, with 43 tests. |
| **Cloud Composer (Airflow)** | A scheduler that runs tasks in order and retries or alerts on failure. | Daily: ingest, then dbt build, then taxonomy check. |

### Medallion architecture (Bronze, Silver, Gold)
- **Bronze**: raw data exactly as received, plus load metadata (`_batch_id`, `_ingested_at`, `_source_file`). Never edited, so you can always reprocess.
- **Silver**: cleaned and conformed.
  - Types are cast, and only the latest version of each record is kept.
  - Free-text countries ("India", "IND", "india") are mapped to ISO codes.
  - Duplicate CRM accounts are mapped to one master account (`int_account_master_map`). This is **master data management (MDM)**.
- **Gold**: business-ready tables for dashboards and finance.
  - `dim_account`: the golden customer record.
  - `fct_invoices`: one row per invoice, in USD, with payment status.
  - `fct_opportunities`: one row per deal.
  - `rpt_mrr_movements`, `rpt_revenue_monthly`, `rpt_ar_aging`, `rpt_pipeline_by_segment`.

### Write-audit-publish (WAP), step by step
1. **Write.** Create a Nessie branch `ingest_batch_00X` (instant; nothing is copied) and load the files into Bronze there.
2. **Audit.** On the branch, run the checks:
   - rows were loaded;
   - keys are not null and are unique;
   - amounts are not negative;
   - every table follows the taxonomy.
3. **Publish.** If all checks pass, merge the branch into `main` in one atomic step and tag it `published_batch_00X`. If any check fails, `main` is untouched and the branch stays for debugging.

Re-running a batch is safe. Bronze is partitioned by `_batch_id`, and each load replaces only its own partition. This property is called **idempotent**.

### Data taxonomy
`governance/taxonomy.yml` is the rulebook:
- **Naming** per layer:
  - Bronze: `crm_accounts`, i.e. `<domain>_<entity>`.
  - Silver: `stg_billing__invoices`, `int_…`, `dim_…`, `ref_…`.
  - Gold: `fct_`, `dim_`, `rpt_`, `agg_`.
- **Ownership**: every table carries `data_owner`, `domain`, `layer`, `description` and `pii` as Iceberg table properties.
- **Required columns** per layer.

`governance/check_taxonomy.py` reads the live catalog and fails if any table breaks a rule. During this build it caught two reference tables named wrongly, which led to the `ref_` convention. It also generates [`docs/DATA_CATALOG.md`](docs/DATA_CATALOG.md), so every asset is discoverable.

### Revenue terms used in Gold
- **MRR (monthly recurring revenue)**: subscription revenue billed per month. **ARR** = MRR × 12.
- **MRR bridge**: last month's MRR + new + expansion + reactivation + contraction + churn = this month's MRR. A dbt test checks it adds up every month.
- **New / expansion / contraction / churn**: a first-ever paying month / paying more / paying less / stopped paying.
- **AR aging**: unpaid invoices grouped by how overdue they are (1-30, 31-60, 61-90, 90+ days).
- **Weighted pipeline**: deal value × probability of winning at its stage.
- **Win rate**: won deals ÷ closed deals.

---

## Talking about it in an interview

**30-second version.** "Revenue data from CRM, billing and finance sat in a plain data lake: no ACID, no enforced naming, schema changes broke pipelines, and nobody could find or trust tables. I built a lakehouse on Apache Iceberg with Nessie as a versioned catalog. Spark loads each batch into a Nessie branch, audits it, and merges to main only if every check passes. dbt on Trino builds Bronze, Silver and Gold. Silver de-duplicates customer records and standardises countries and currencies. Gold produces MRR, churn, AR aging and pipeline. A taxonomy rulebook is enforced on every table and generates the data catalog. We got zero-downtime schema evolution, one-step rollback, and Gold queries about 60% faster than raw."

**Likely follow-up questions**
- *Iceberg vs Delta Lake vs Hudi?* All three add ACID tables on top of files. Iceberg is engine-neutral (Spark, Trino, Flink, BigQuery and Snowflake can all read it), has hidden partitioning, and evolves partitions without rewriting data.
- *What does Nessie add over a normal Iceberg catalog?* Multi-table commits, branches for isolated loads (WAP), tags for reproducible reporting, and catalog-wide rollback.
- *How do you handle a schema change?* Iceberg tracks columns by ID, so adding a column is a metadata-only change. Ingestion lines up existing columns, appends new ones, and Silver defaults them (`coalesce(tax_amount, 0)`).
- *How did you de-duplicate accounts?* Build a match key (lower case, no punctuation or legal suffixes) plus the ISO country. Records sharing it are one company, and the earliest becomes the master. In production you'd add fuzzy matching and a steward review queue.
- *How do you know the revenue numbers are right?* dbt tests (unique, not null, relationships, accepted values) plus business reconciliations: the MRR bridge adds up, and every invoice has an FX rate.
- *Why did queries get faster?* Gold stores the answer already typed, de-duplicated and converted, so queries skip the window functions, casts and joins that raw Bronze needs.

## Repo layout
```
generator/generate_revenue_data.py   messy CRM / billing / FX source data in 3 batches
ingestion/ingest_batch.py            PySpark write-audit-publish into Bronze
ingestion/nessie_client.py           branches, merges, tags, rollback (Nessie REST API v2)
ingestion/spark_session.py           Spark + Iceberg + Nessie configuration
governance/taxonomy.yml              the data taxonomy rulebook
governance/check_taxonomy.py         taxonomy enforcement + data catalog generator
dbt/                                 Silver + Gold models, seeds, tests (dbt-trino)
airflow/dags/                        Airflow DAGs: revenue_lakehouse_daily (WAP + dbt + taxonomy), revenue_lakehouse_maintenance
scripts/start_services.sh            start storage, Nessie and Trino (used by the DAG)
scripts/run_demo.sh                  one-command local demo
scripts/demo_time_travel.py          time travel + rollback demo
scripts/benchmark_queries.py         Bronze vs Gold query benchmark
docs/DATA_CATALOG.md                 generated catalog of every table and column
```
