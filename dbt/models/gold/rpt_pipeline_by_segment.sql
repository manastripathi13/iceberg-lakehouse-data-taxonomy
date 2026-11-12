-- Sales pipeline health by segment: open and weighted pipeline, win rate, deal size, cycle time.
select
    segment,
    count_if(is_open)                                              as open_deals,
    round(sum(case when is_open then amount_usd else 0 end), 0)    as open_pipeline_usd,
    round(sum(case when is_open then weighted_amount_usd else 0 end), 0) as weighted_pipeline_usd,
    count_if(is_won)                                               as won_deals,
    count_if(not is_open and not is_won)                           as lost_deals,
    round(100.0 * count_if(is_won) / nullif(count_if(not is_open), 0), 1) as win_rate_pct,
    round(avg(case when is_won then amount_usd end), 0)            as avg_won_deal_usd,
    round(avg(case when is_won then sales_cycle_days end), 0)      as avg_sales_cycle_days
from {{ ref('fct_opportunities') }}
group by 1
