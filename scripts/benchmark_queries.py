"""Compare the same business question answered from raw Bronze vs the modelled Gold layer.

Question: "monthly revenue in USD by customer segment".
- Bronze: must de-duplicate records, cast text, convert currency, and resolve duplicate accounts at query time.
- Gold:   reads pre-modelled, typed, USD-converted tables.
Each query runs several times; we report the median wall-clock time.
"""

import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.trino_conn import connect, q  # noqa: E402

BRONZE = """
with inv as (
  select *, row_number() over (partition by invoice_id order by _ingested_at desc) rn from bronze.billing_invoices
), acc as (
  select *, row_number() over (partition by Id order by _ingested_at desc) rn from bronze.crm_accounts
), acc_clean as (
  select Id, Segment__c as segment, cast(CreatedDate as date) created,
         trim(regexp_replace(regexp_replace(lower(Name), '[^a-z0-9 ]|\\b(ltd|limited|inc|llc|pvt)\\b', ''), '\\s+', ' ')) k,
         lower(trim(BillingCountry)) c
  from acc where rn = 1
), master as (
  select a.Id, first_value(a.segment) over (partition by a.k, cc.country_code order by a.created, a.Id) segment
  from acc_clean a left join silver.ref_country_codes cc on a.c = cc.raw_value
), fx as (
  select distinct cast(rate_month as date) m, currency, cast(usd_per_unit as double) r from bronze.finance_fx_rates
)
select date_trunc('month', cast(i.period_start as date)) month, m.segment, round(sum(i.amount * fx.r), 2) usd
from inv i
join master m on m.Id = i.account_ref
join fx on fx.currency = i.currency and fx.m = date_trunc('month', cast(i.invoice_date as date))
where i.rn = 1
group by 1, 2
"""

GOLD = """
select f.revenue_month month, a.segment, round(sum(f.amount_usd), 2) usd
from gold.fct_invoices f join gold.dim_account a on a.master_account_id = f.master_account_id
group by 1, 2
"""


def bench(cur, sql, runs=7):
    q(cur, sql)  # warm-up
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        rows = q(cur, sql)
        times.append(time.perf_counter() - t)
    return statistics.median(times), rows


cur = connect().cursor()
tb, rb = bench(cur, BRONZE)
tg, rg = bench(cur, GOLD)
same = sorted((str(r[0]), r[1], round(float(r[2]))) for r in rb) == sorted((str(r[0]), r[1], round(float(r[2]))) for r in rg)
print(f"bronze (raw, modelled at query time): {tb * 1000:7.0f} ms median")
print(f"gold   (pre-modelled tables):         {tg * 1000:7.0f} ms median")
print(f"reduction in query time:              {100 * (1 - tg / tb):7.1f} %")
print(f"same answer from both:                {same}")
