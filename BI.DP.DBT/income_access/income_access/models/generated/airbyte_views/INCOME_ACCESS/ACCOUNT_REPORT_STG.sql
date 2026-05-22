{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "INCOME_ACCESS",
    tags = [ "top-level-intermediate" ]
) }}

-- depends_on: {{ ref('ACCOUNT_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'DEPOSITS',
        'COMMISSIONS',
        'BONUS',
        'CPA_COMMISSIONS',
        'CHARGEBACKS',
        'GROSS_REVENUE',
        'NET_REVENUE',
        'AFF_CUSTOM_ID',
        'BANNER_ID',
        'BANNER_TYPE',
        'CPA_COMMISSION_COUNT',
        'CREATIVE_NAME',
        'CURRENCY_SYMBOL',
        'FIRST_DEPOSIT',
        'MEMBER_ID',
        'MERCHANT_NAME',
        'NEW',
        'PLAYER_ID',
        'TRACKER_LOGIN_ID',
        'PLAYER_COUNTRY',
        'REGISTRATION_DATE',
        'ROW_ID',
        'SITE_ID',
        'STAKE',
        'TOTAL_COMMISSION',
        'TOTAL_RECORDS',
        'USERNAME',
        
    ]) }} as _AIRBYTE_ACCOUNT_REPORT_HASHID,
    tmp.*
from {{ ref('ACCOUNT_REPORT_AB2') }} tmp
-- ACCOUNT_REPORT
where 1 = 1 
AND AFF_CUSTOM_ID is not null
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

