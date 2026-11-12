-- Golden record: one row per real customer, after merging duplicate CRM records.
select
    a.account_id                     as master_account_id,
    a.account_name,
    a.country_code,
    a.country_name,
    a.industry,
    a.segment,
    a.account_owner,
    a.created_date,
    m.records_for_company            as crm_records_merged
from {{ ref('stg_crm__accounts') }} a
join {{ ref('int_account_master_map') }} m
  on a.account_id = m.account_id
where a.account_id = m.master_account_id
