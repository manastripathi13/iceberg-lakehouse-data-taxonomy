-- One row per CRM account record, latest version, with a standardised country and a match key.
with latest as (
    select *, row_number() over (partition by Id order by _ingested_at desc) as rn
    from {{ source('bronze', 'crm_accounts') }}
)
select
    Id                                                          as account_id,
    trim(regexp_replace(Name, '\s+', ' '))                      as account_name,
    -- match key: lower case, no punctuation, no legal suffixes, single spaces
    trim(regexp_replace(
        regexp_replace(lower(Name), '[^a-z0-9 ]|\b(ltd|limited|inc|llc|pvt)\b', ''),
        '\s+', ' '))                                            as name_match_key,
    c.country_code,
    c.country_name,
    BillingCountry                                              as country_raw,
    Industry                                                    as industry,
    Segment__c                                                  as segment,
    OwnerName                                                   as account_owner,
    cast(CreatedDate as date)                                   as created_date,
    _batch_id                                                   as source_batch_id
from latest
left join {{ ref('ref_country_codes') }} c
    on lower(trim(latest.BillingCountry)) = c.raw_value
where rn = 1
