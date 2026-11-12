-- Accounts receivable aging as of the reporting date: unpaid invoices by days past due.
select
    a.segment,
    case
        when f.due_date >= date '{{ var("as_of_date") }}' then '0 not yet due'
        when date_diff('day', f.due_date, date '{{ var("as_of_date") }}') <= 30 then '1-30 days'
        when date_diff('day', f.due_date, date '{{ var("as_of_date") }}') <= 60 then '31-60 days'
        when date_diff('day', f.due_date, date '{{ var("as_of_date") }}') <= 90 then '61-90 days'
        else '90+ days'
    end                                   as aging_bucket,
    count(*)                              as unpaid_invoices,
    round(sum(f.amount_usd), 2)           as outstanding_usd
from {{ ref('fct_invoices') }} f
join {{ ref('dim_account') }} a on a.master_account_id = f.master_account_id
where f.payment_status <> 'paid'
group by 1, 2
