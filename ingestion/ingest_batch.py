"""Load one landing batch into the Bronze layer using write-audit-publish (WAP).

    1. WRITE    create Nessie branch `ingest_<batch>` and load the files into Bronze Iceberg tables there
    2. AUDIT    run quality + taxonomy checks against the branch (main is untouched)
    3. PUBLISH  if everything passes, merge the branch into main in one atomic step;
                if anything fails, keep the branch for debugging and exit non-zero

Re-running a batch is safe: Bronze tables are partitioned by `_batch_id` and each
load replaces only its own partition (idempotent).

Usage:  python ingestion/ingest_batch.py batch_001
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from governance.check_taxonomy import check_catalog  # noqa: E402
from ingestion import nessie_client as nessie  # noqa: E402
from ingestion.spark_session import get_spark  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
AUDIT_DIR = ROOT / "data" / "audit"

# What we know about each source table (goes into Iceberg table properties = the catalog).
TABLES = {
    "crm_accounts": dict(file="crm/accounts.csv", key="Id", pii="low", owner="revops-data@company.example",
                         description="Salesforce Account export: one row per CRM account record (may contain duplicates)."),
    "crm_opportunities": dict(file="crm/opportunities.csv", key="Id", pii="none", owner="revops-data@company.example",
                              description="Salesforce Opportunity export: deals and their pipeline stage."),
    "billing_invoices": dict(file="billing/invoices.jsonl", key="invoice_id", pii="none", owner="finance-data@company.example",
                             description="Invoices from the billing system, in the customer's local currency.",
                             non_negative=["amount"]),
    "billing_payments": dict(file="billing/payments.jsonl", key="payment_id", pii="none", owner="finance-data@company.example",
                             description="Payments received against invoices.", non_negative=["amount"]),
    "finance_fx_rates": dict(file="finance/fx_rates.csv", key=None, pii="none", owner="finance-data@company.example",
                             description="Monthly FX rates: USD value of one unit of each currency."),
}


def read_source(spark, path: Path):
    if path.suffix == ".csv":
        # Bronze keeps CSV values as text, exactly as exported; typing happens in Silver.
        return spark.read.option("header", True).option("inferSchema", False).csv(str(path))
    return spark.read.json(str(path))


def write_bronze(spark, table: str, meta: dict, df, batch: str) -> int:
    df = (
        df.withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(meta["file"]))
    )
    full = f"bronze.{table}"
    if not spark.catalog.tableExists(full):
        props = {
            "data_owner": meta["owner"], "domain": table.split("_")[0], "layer": "bronze",
            "description": meta["description"], "pii": meta["pii"],
            "format-version": "2", "write.spark.accept-any-schema": "true",
        }
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
        prop_sql = ", ".join("'{}' = '{}'".format(k, str(v).replace("'", "\\'")) for k, v in props.items())
        spark.sql(f"create table {full} ({cols}) using iceberg partitioned by (_batch_id) tblproperties ({prop_sql})")
    else:
        # Line the batch up with the table: existing columns in table order (null if the source
        # dropped one), then any brand-new source columns at the end.
        existing = spark.table(full).schema
        new_cols = [c for c in df.columns if c not in existing.fieldNames()]
        df = df.select(
            *[F.col(f"`{f.name}`") if f.name in df.columns else F.lit(None).cast(f.dataType).alias(f.name) for f in existing.fields],
            *[F.col(f"`{c}`") for c in new_cols],
        )
        if new_cols:
            print(f"          schema evolution on {full}: adding {new_cols}")
    # mergeSchema: new columns in the source (e.g. tax_amount) are added to the table automatically
    df.writeTo(full).option("mergeSchema", "true").overwritePartitions()
    return df.count()


def audit(spark, batch: str, loaded: dict[str, int], ref: str) -> list[dict]:
    checks = []

    def add(name, ok, detail=""):
        checks.append({"check": name, "passed": bool(ok), "detail": detail})

    for table, n in loaded.items():
        meta = TABLES[table]
        df = spark.table(f"bronze.{table}").where(F.col("_batch_id") == batch)
        add(f"{table}: rows loaded", n > 0, f"{n} rows")
        if meta["key"]:
            nulls = df.where(F.col(meta["key"]).isNull()).count()
            dups = n - df.select(meta["key"]).where(F.col(meta["key"]).isNotNull()).distinct().count() - nulls
            add(f"{table}: {meta['key']} not null", nulls == 0, f"{nulls} nulls")
            add(f"{table}: {meta['key']} unique in batch", dups == 0, f"{dups} duplicates")
        for col in meta.get("non_negative", []):
            neg = df.where(F.col(col) < 0).count()
            add(f"{table}: {col} >= 0", neg == 0, f"{neg} negative values")

    problems = check_catalog(ref=ref, namespaces=["bronze"])
    add("taxonomy: bronze naming, properties, required columns", not problems, "; ".join(problems) or "ok")
    return checks


def main(batch: str) -> int:
    src = LANDING / batch
    if not src.exists():
        raise SystemExit(f"no landing folder {src}")
    branch = f"ingest_{batch}"

    nessie.ensure_namespaces(["bronze", "silver", "gold"], ref="main")
    print(f"[write]   branch {branch} from main")
    nessie.create_branch(branch, "main")
    spark = get_spark(ref=branch, app=f"ingest-{batch}")
    spark.sql("create namespace if not exists bronze")

    loaded = {}
    for table, meta in TABLES.items():
        path = src / meta["file"]
        if path.exists():
            loaded[table] = write_bronze(spark, table, meta, read_source(spark, path), batch)
            print(f"          bronze.{table:<20} {loaded[table]:>6} rows")

    print("[audit]   running checks on the branch (main is not affected)")
    checks = audit(spark, batch, loaded, ref=branch)
    spark.stop()
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        print(f"          {'PASS' if c['passed'] else 'FAIL'}  {c['check']}  ({c['detail']})")

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    report = {"batch": batch, "branch": branch, "run_at": datetime.now(timezone.utc).isoformat(),
              "published": not failed, "checks": checks}
    (AUDIT_DIR / f"{batch}.json").write_text(json.dumps(report, indent=2))

    if failed:
        print(f"[blocked] {len(failed)} check(s) failed. Branch '{branch}' kept for investigation; main unchanged.")
        return 1
    nessie.merge_branch(branch, "main", message=f"publish {batch}: " + ", ".join(f"{t}={n}" for t, n in loaded.items()))
    nessie.delete_branch(branch)
    nessie.create_tag(f"published_{batch}", "main")
    print(f"[publish] merged {branch} into main and tagged it 'published_{batch}'")
    return 0


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "batch_001"))
