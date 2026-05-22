{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "API_ENDPOINT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('API_STREAMIA_CAMPAIGN_AB1') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'TIER_LEVEL',
        'ADVERTISER_ID',
        'ADVERTISER_NAME',
        'AFFILIATE_ID',
        'BRAND_NAME',
        'CAMPAIGN_ID',
        'CAMPAIGN_NAME',
        'CAMPAIGN_TYPE',
        'CAMPAIGN_STATUS',
        'CURRENCY',
        'COUNTRY',
        'BASELINE_WAGER',
        'BASELINE_DEPOSIT',
        'REVSHARE_DEAL',
        'CPA_DEAL',
        'CLICK_CNT',
        'UNIQUE_CLICKS',
        'SIGNUP_CNT',
        'FTD_CNT',
        'CPA_CNT',
        'DEPOSIT_CNT',
        'FTD_AMT',
        'DEPOSIT_AMT',
        'NET_DEPOSIT_AMT',
        'NET_REVENUE_AMT',
        'FTD_INCOME_AMT',
        'CPA_INCOME_AMT',
        'REVSHARE_INCOME_AMT',
    ]) }} as _AIRBYTE_API_STREAMIA_CAMPAIGN_HASHID,
    tmp.*
from {{ ref('API_STREAMIA_CAMPAIGN_AB1') }} tmp
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
