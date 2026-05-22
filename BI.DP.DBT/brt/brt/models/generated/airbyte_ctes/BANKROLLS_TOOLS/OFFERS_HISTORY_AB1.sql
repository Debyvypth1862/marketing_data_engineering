{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}

WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OFFERS_HISTORY') }}
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['revshare_operator'], ['revshare_operator']) }} as REVSHARE_OPERATOR,
    {{ json_extract_scalar('_airbyte_data', ['cpl_operator'], ['cpl_operator']) }} as CPL_OPERATOR,
    {{ json_extract_scalar('_airbyte_data', ['revshare_diff'], ['revshare_diff']) }} as REVSHARE_DIFF,
    {{ json_extract_scalar('_airbyte_data', ['cr_signup_to_ftd_unverified'], ['cr_signup_to_ftd_unverified']) }} as CR_SIGNUP_TO_FTD_UNVERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['cpa_affiliate'], ['cpa_affiliate']) }} as CPA_AFFILIATE,
    {{ json_extract_scalar('_airbyte_data', ['cpa_revenue_per_click'], ['cpa_revenue_per_click']) }} as CPA_REVENUE_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cr_signup_to_ftd_verified'], ['cr_signup_to_ftd_verified']) }} as CR_SIGNUP_TO_FTD_VERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['sign_ups'], ['sign_ups']) }} as SIGN_UPS,
    {{ json_extract_scalar('_airbyte_data', ['cpl_diff'], ['cpl_diff']) }} as CPL_DIFF,
    {{ json_extract_scalar('_airbyte_data', ['total_income_per_click'], ['total_income_per_click']) }} as TOTAL_INCOME_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['baseline'], ['baseline']) }} as BASELINE,
    {{ json_extract_scalar('_airbyte_data', ['deposits'], ['deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['total_revenue_per_click'], ['total_revenue_per_click']) }} as TOTAL_REVENUE_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cpl_payout'], ['cpl_payout']) }} as CPL_PAYOUT,
    {{ json_extract_scalar('_airbyte_data', ['total_income'], ['total_income']) }} as TOTAL_INCOME,
    {{ json_extract_scalar('_airbyte_data', ['cpl_revenue_per_click'], ['cpl_revenue_per_click']) }} as CPL_REVENUE_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cpa_income'], ['cpa_income']) }} as CPA_INCOME,
    {{ json_extract_scalar('_airbyte_data', ['total_payout_unverified'], ['total_payout_unverified']) }} as TOTAL_PAYOUT_UNVERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['revshare_revenue'], ['revshare_revenue']) }} as REVSHARE_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['revshare_payout'], ['revshare_payout']) }} as REVSHARE_PAYOUT,
    {{ json_extract_scalar('_airbyte_data', ['revshare_affiliate'], ['revshare_affiliate']) }} as REVSHARE_AFFILIATE,
    {{ json_extract_scalar('_airbyte_data', ['cr_click_to_signup'], ['cr_click_to_signup']) }} as CR_CLICK_TO_SIGNUP,
    {{ json_extract_scalar('_airbyte_data', ['cr_click_to_ftd_unverified'], ['cr_click_to_ftd_unverified']) }} as CR_CLICK_TO_FTD_UNVERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['revshare_income_per_click'], ['revshare_income_per_click']) }} as REVSHARE_INCOME_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['revshare_revenue_per_click'], ['revshare_revenue_per_click']) }} as REVSHARE_REVENUE_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['cpa_operator'], ['cpa_operator']) }} as CPA_OPERATOR,
    {{ json_extract_scalar('_airbyte_data', ['cr_click_to_ftd_verified'], ['cr_click_to_ftd_verified']) }} as CR_CLICK_TO_FTD_VERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['cpa_income_per_click'], ['cpa_income_per_click']) }} as CPA_INCOME_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['total_payout_per_click_unverified'], ['total_payout_per_click_unverified']) }} as TOTAL_PAYOUT_PER_CLICK_UNVERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['total_payout_verified'], ['total_payout_verified']) }} as TOTAL_PAYOUT_VERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['ftds_unverified'], ['ftds_unverified']) }} as FTDS_UNVERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['cpl_payout_per_click'], ['cpl_payout_per_click']) }} as CPL_PAYOUT_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['revshare_income'], ['revshare_income']) }} as REVSHARE_INCOME,
    {{ json_extract_scalar('_airbyte_data', ['cpl_income_per_click'], ['cpl_income_per_click']) }} as CPL_INCOME_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cpl_income'], ['cpl_income']) }} as CPL_INCOME,
    {{ json_extract_scalar('_airbyte_data', ['ftds_verified'], ['ftds_verified']) }} as FTDS_VERIFIED,
    {{ json_extract_scalar('_airbyte_data', ['revshare_payout_per_click'], ['revshare_payout_per_click']) }} as REVSHARE_PAYOUT_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cpa_diff'], ['cpa_diff']) }} as CPA_DIFF,
    {{ json_extract_scalar('_airbyte_data', ['offer_id'], ['offer_id']) }} as OFFER_ID,
    {{ json_extract_scalar('_airbyte_data', ['cpa_revenue'], ['cpa_revenue']) }} as CPA_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['cpl_revenue'], ['cpl_revenue']) }} as CPL_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['cpa_payout_per_click'], ['cpa_payout_per_click']) }} as CPA_PAYOUT_PER_CLICK,
    {{ json_extract_scalar('_airbyte_data', ['cpa_payout'], ['cpa_payout']) }} as CPA_PAYOUT,
    {{ json_extract_scalar('_airbyte_data', ['total_revenue'], ['total_revenue']) }} as TOTAL_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['clicks'], ['clicks']) }} as CLICKS,
    {{ json_extract_scalar('_airbyte_data', ['cpl_affiliate'], ['cpl_affiliate']) }} as CPL_AFFILIATE,
    {{ json_extract_scalar('_airbyte_data', ['total_payout_per_click_verified'], ['total_payout_per_click_verified']) }} as TOTAL_PAYOUT_PER_CLICK_VERIFIED,
    S3_PATH,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OFFERS_HISTORY') }} as table_alias
-- OFFERS_HISTORY
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

