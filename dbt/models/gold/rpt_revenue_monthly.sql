-- The monthly revenue page: MRR/ARR bridge, customers, invoicing and cash collected.
with bridge as (
    select
        revenue_month,
        sum(mrr_usd)                                                          as mrr_usd,
        sum(case when movement_type = 'new' then mrr_change_usd else 0 end)          as new_mrr_usd,
        sum(case when movement_type = 'expansion' then mrr_change_usd else 0 end)    as expansion_mrr_usd,
        sum(case when movement_type = 'reactivation' then mrr_change_usd else 0 end) as reactivation_mrr_usd,
        sum(case when movement_type = 'contraction' then mrr_change_usd else 0 end)  as contraction_mrr_usd,
        sum(case when movement_type = 'churn' then mrr_change_usd else 0 end)        as churned_mrr_usd,
        count_if(mrr_usd > 0)                                                 as paying_customers,
        count_if(movement_type = 'churn')                                     as churned_customers
    from {{ ref('rpt_mrr_movements') }}
    group by 1
),
cash as (
    select date_trunc('month', cast(paid_at as date)) as revenue_month, sum(paid_usd) as collected_usd
    from {{ ref('fct_invoices') }} where paid_at is not null group by 1
),
billed as (
    select date_trunc('month', invoice_date) as revenue_month, sum(amount_usd) as invoiced_usd
    from {{ ref('fct_invoices') }} group by 1
)
select
    b.*,
    b.mrr_usd * 12                                   as arr_usd,
    coalesce(bl.invoiced_usd, 0)                     as invoiced_usd,
    coalesce(c.collected_usd, 0)                     as collected_usd
from bridge b
left join billed bl on bl.revenue_month = b.revenue_month
left join cash c on c.revenue_month = b.revenue_month
