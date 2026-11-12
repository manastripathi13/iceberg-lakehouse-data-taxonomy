-- One row per deal with stage probability, for pipeline and forecast reporting.
select
    o.opportunity_id,
    m.master_account_id,
    a.segment,
    o.product,
    o.stage_name,
    s.stage_order,
    s.is_open,
    s.is_won,
    o.amount_usd,
    round(o.amount_usd * s.win_probability, 2)   as weighted_amount_usd,
    o.created_date,
    o.close_date,
    date_trunc('quarter', o.close_date)          as close_quarter,
    date_diff('day', o.created_date, o.close_date) as sales_cycle_days
from {{ ref('stg_crm__opportunities') }} o
join {{ ref('int_account_master_map') }} m on o.account_id = m.account_id
join {{ ref('dim_account') }} a on a.master_account_id = m.master_account_id
join {{ ref('ref_opportunity_stages') }} s on s.stage_name = o.stage_name
