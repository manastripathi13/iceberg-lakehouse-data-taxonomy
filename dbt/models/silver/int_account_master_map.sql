-- Master data management: map every CRM account record to one "master" account.
-- Records with the same match key and country are the same company; the earliest
-- created record becomes the master (the "golden record").
select
    account_id,
    first_value(account_id) over (
        partition by name_match_key, country_code
        order by created_date, account_id
    )                                                     as master_account_id,
    count(*) over (partition by name_match_key, country_code) as records_for_company
from {{ ref('stg_crm__accounts') }}
