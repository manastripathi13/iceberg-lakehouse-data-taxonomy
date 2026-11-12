with latest as (
    select *, row_number() over (partition by Id order by _ingested_at desc) as rn
    from {{ source('bronze', 'crm_opportunities') }}
)
select
    Id                                  as opportunity_id,
    AccountId                           as account_id,
    Name                                as opportunity_name,
    StageName                           as stage_name,
    Product__c                          as product,
    cast(Amount as decimal(18, 2))      as amount_usd,     -- CRM amounts are booked in USD
    cast(CloseDate as date)             as close_date,
    cast(CreatedDate as date)           as created_date
from latest
where rn = 1
