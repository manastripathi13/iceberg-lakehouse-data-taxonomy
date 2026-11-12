{#- tax_amount only exists once the billing system upgrade lands (batch_002).
    Check the live Bronze schema so this model works before and after that schema change. -#}
{%- set bronze_cols = adapter.get_columns_in_relation(source('bronze', 'billing_invoices')) | map(attribute='name') | map('lower') | list -%}
with latest as (
    select *, row_number() over (partition by invoice_id order by _ingested_at desc) as rn
    from {{ source('bronze', 'billing_invoices') }}
)
select
    invoice_id,
    account_ref                                   as account_id,
    product,
    cast(invoice_date as date)                    as invoice_date,
    cast(due_date as date)                        as due_date,
    cast(period_start as date)                    as period_start,
    cast(period_end as date)                      as period_end,
    currency,
    cast(amount as decimal(18, 2))                as amount_local,
    cast({{ 'coalesce(tax_amount, 0)' if 'tax_amount' in bronze_cols else '0' }} as decimal(18, 2)) as tax_local,
    status
from latest
where rn = 1
