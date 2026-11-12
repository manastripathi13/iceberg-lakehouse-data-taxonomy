with latest as (
    select *, row_number() over (partition by payment_id order by _ingested_at desc) as rn
    from {{ source('bronze', 'billing_payments') }}
)
select
    payment_id,
    invoice_id,
    cast(from_iso8601_timestamp(paid_at) as timestamp(6)) as paid_at,
    currency,
    cast(amount as decimal(18, 2))  as amount_local,
    method
from latest
where rn = 1
