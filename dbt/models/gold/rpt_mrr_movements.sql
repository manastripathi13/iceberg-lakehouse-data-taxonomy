-- Monthly recurring revenue (MRR) per customer and what changed vs last month.
-- new = first revenue ever, expansion = up, contraction = down, churn = dropped to zero,
-- reactivation = came back after churning.
with mrr as (
    select master_account_id, revenue_month, sum(amount_usd) as mrr_usd
    from {{ ref('fct_invoices') }}
    group by 1, 2
),
bounds as (
    select master_account_id, min(revenue_month) as first_month
    from mrr group by 1
),
spine as (   -- every customer x every month from their first month to the reporting date
    select b.master_account_id, m.revenue_month
    from bounds b
    cross join unnest(sequence(b.first_month, date_trunc('month', date '{{ var("as_of_date") }}'), interval '1' month))
        as m(revenue_month)
),
filled as (
    select s.master_account_id, s.revenue_month, coalesce(mrr.mrr_usd, 0) as mrr_usd
    from spine s
    left join mrr on mrr.master_account_id = s.master_account_id and mrr.revenue_month = s.revenue_month
),
with_prev as (
    select *,
        coalesce(lag(mrr_usd) over (partition by master_account_id order by revenue_month), 0) as prev_mrr_usd,
        sum(mrr_usd) over (partition by master_account_id order by revenue_month
                           rows between unbounded preceding and 1 preceding) as revenue_before
    from filled
)
select
    master_account_id,
    revenue_month,
    mrr_usd,
    prev_mrr_usd,
    mrr_usd - prev_mrr_usd as mrr_change_usd,
    case
        when prev_mrr_usd = 0 and mrr_usd > 0 and coalesce(revenue_before, 0) = 0 then 'new'
        when prev_mrr_usd = 0 and mrr_usd > 0 then 'reactivation'
        when prev_mrr_usd > 0 and mrr_usd = 0 then 'churn'
        when mrr_usd > prev_mrr_usd then 'expansion'
        when mrr_usd < prev_mrr_usd then 'contraction'
        else 'retained'
    end as movement_type
from with_prev
where not (mrr_usd = 0 and prev_mrr_usd = 0)
