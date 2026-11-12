-- Revenue bridge must add up: last month MRR + new + expansion + reactivation + contraction + churn = this month MRR.
-- Returns months where it does not (should be none).
with m as (
    select *, lag(mrr_usd) over (order by revenue_month) as prev_total
    from {{ ref('rpt_revenue_monthly') }}
)
select revenue_month
from m
where prev_total is not null
  and abs(prev_total + new_mrr_usd + expansion_mrr_usd + reactivation_mrr_usd
          + contraction_mrr_usd + churned_mrr_usd - mrr_usd) > 0.01
