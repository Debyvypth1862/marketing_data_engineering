{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFERS_HISTORY_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'REVSHARE_OPERATOR',
        'CPL_OPERATOR',
        'REVSHARE_DIFF',
        'CR_SIGNUP_TO_FTD_UNVERIFIED',
        'ID',
        'CPA_AFFILIATE',
        'CPA_REVENUE_PER_CLICK',
        'CR_SIGNUP_TO_FTD_VERIFIED',
        'SIGN_UPS',
        'CPL_DIFF',
        'TOTAL_INCOME_PER_CLICK',
        'BASELINE',
        'DEPOSITS',
        'TOTAL_REVENUE_PER_CLICK',
        'CPL_PAYOUT',
        'TOTAL_INCOME',
        'CPL_REVENUE_PER_CLICK',
        'CPA_INCOME',
        'TOTAL_PAYOUT_UNVERIFIED',
        'REVSHARE_REVENUE',
        'REVSHARE_PAYOUT',
        'REVSHARE_AFFILIATE',
        'CR_CLICK_TO_SIGNUP',
        'CR_CLICK_TO_FTD_UNVERIFIED',
        'REVSHARE_INCOME_PER_CLICK',
        'REVSHARE_REVENUE_PER_CLICK',
        'CREATED_AT',
        'CPA_OPERATOR',
        'CR_CLICK_TO_FTD_VERIFIED',
        'UPDATED_AT',
        'CPA_INCOME_PER_CLICK',
        'TOTAL_PAYOUT_PER_CLICK_UNVERIFIED',
        'TOTAL_PAYOUT_VERIFIED',
        'FTDS_UNVERIFIED',
        'CPL_PAYOUT_PER_CLICK',
        'REVSHARE_INCOME',
        'CPL_INCOME_PER_CLICK',
        'CPL_INCOME',
        'FTDS_VERIFIED',
        'REVSHARE_PAYOUT_PER_CLICK',
        'CPA_DIFF',
        'OFFER_ID',
        'CPA_REVENUE',
        'CPL_REVENUE',
        'CPA_PAYOUT_PER_CLICK',
        'CPA_PAYOUT',
        'TOTAL_REVENUE',
        'CLICKS',
        'CPL_AFFILIATE',
        'TOTAL_PAYOUT_PER_CLICK_VERIFIED',
        'S3_PATH'
    ]) }} as _AIRBYTE_OFFERS_HISTORY_HASHID,
    tmp.*
from {{ ref('OFFERS_HISTORY_AB2') }} tmp
-- OFFERS_HISTORY
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

