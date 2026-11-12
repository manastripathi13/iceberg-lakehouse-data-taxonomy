-- Every invoice must find an FX rate, otherwise its USD value would silently be null.
select invoice_id from {{ ref('fct_invoices') }} where usd_per_unit is null
