"""Time travel and recovery with Nessie + Iceberg.

With Nessie as the catalog, history lives in Nessie's commit log (like Git):
every publish is a commit, and each published batch is tagged.

1. Time travel: read bronze.billing_invoices as it was at tag `published_batch_001`
   (before the billing upgrade) and compare it with main today.
2. Recovery: simulate an accident on a Gold table (DELETE every row through Trino),
   then roll `main` back to the commit before the accident. Every table returns
   to its earlier state in one step, with no backup restore.
3. Print the Nessie commit log.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion import nessie_client as nessie  # noqa: E402
from ingestion.spark_session import get_spark  # noqa: E402
from scripts.trino_conn import connect, q  # noqa: E402

print("1) time travel with Spark: bronze.billing_invoices at two points in history")
for ref in ["published_batch_001", "main"]:
    spark = get_spark(ref=ref, app=f"time-travel-{ref}")
    df = spark.table("bronze.billing_invoices")
    print(f"   {ref:<22} rows={df.count():>5}  has tax_amount column={'tax_amount' in df.columns}")
    spark.stop()

print("\n2) accident + recovery on gold.rpt_revenue_monthly")
cur = connect().cursor()
before = q(cur, "select count(*), round(sum(mrr_usd)) from gold.rpt_revenue_monthly")[0]
safe_point = nessie.get_ref("main")["hash"]
q(cur, "delete from gold.rpt_revenue_monthly")
after_delete = q(cur, "select count(*) from gold.rpt_revenue_monthly")[0][0]
nessie.reset_branch("main", safe_point)
cur = connect().cursor()  # new session so Trino re-reads the catalog
restored = q(cur, "select count(*), round(sum(mrr_usd)) from gold.rpt_revenue_monthly")[0]
print(f"   before: {before[0]} rows | after accidental DELETE: {after_delete} rows | after rollback: {restored[0]} rows")
assert restored == before, "rollback did not restore the table"

print("\n3) Nessie commit log on main (latest first)")
for c in nessie.log("main", limit=8):
    print(f"   {c.get('commitTime', '')[:19]}  {c['message'][:95]}")
