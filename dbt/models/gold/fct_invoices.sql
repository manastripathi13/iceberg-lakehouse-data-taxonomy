-- One row per invoice, converted to USD and linked to the master account and its payment.
with pay as (
    select invoice_id, sum(amount_local) as paid_local, max(paid_at) as paid_at
    from {{ ref('stg_billing__payments') }}
    group by 1
)
select
    i.invoice_id,
    m.master_account_id,
    i.account_id                                            as crm_account_id,
    i.product,
    date_trunc('month', i.period_start)                     as revenue_month,
    i.invoice_date,
    i.due_date,
    i.currency,
    i.amount_local,
    fx.usd_per_unit,
    round(i.amount_local * fx.usd_per_unit, 2)              as amount_usd,
    round(i.tax_local * fx.usd_per_unit, 2)                 as tax_usd,
    round(coalesce(pay.paid_local, 0) * fx.usd_per_unit, 2) as paid_usd,
    pay.paid_at,
    date_diff('day', i.invoice_date, cast(pay.paid_at as date)) as days_to_pay,
    case
        when pay.paid_at is not null and cast(pay.paid_at as date) <= date '{{ var("as_of_date") }}' then 'paid'
        when i.due_date < date '{{ var("as_of_date") }}' then 'overdue'
        else 'open'
    end                                                     as payment_status
from {{ ref('stg_billing__invoices') }} i
join {{ ref('int_account_master_map') }} m on i.account_id = m.account_id
left join {{ ref('stg_finance__fx_rates') }} fx
       on fx.currency = i.currency and fx.rate_month = date_trunc('month', i.invoice_date)
left join pay on pay.invoice_id = i.invoice_id
