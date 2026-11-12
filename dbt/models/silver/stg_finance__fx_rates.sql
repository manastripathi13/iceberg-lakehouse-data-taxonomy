select distinct
    cast(rate_month as date)             as rate_month,
    currency,
    cast(usd_per_unit as decimal(18, 6)) as usd_per_unit
from {{ source('bronze', 'finance_fx_rates') }}
