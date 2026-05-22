{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SMARTICO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('SMARTICO', '_AIRBYTE_RAW_UTM_CODE_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','id'],['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['data','link_id'],['link_id']) }} as LINK_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','link_name'],['link_name']) }} as LINK_NAME,
    {{ json_extract_scalar('_airbyte_data', ['data','brand_id'],['brand_id']) }} as BRAND_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','brand_name'],['brand_name']) }} as BRAND_NAME,
    {{ json_extract_scalar('_airbyte_data', ['data','adjustments'],['adjustments']) }} as ADJUSTMENTS,
    {{ json_extract_scalar('_airbyte_data', ['data','qftd_count'],['qftd_count']) }} as qftd_count,
    {{ json_extract_scalar('_airbyte_data', ['data','qlead_count'],['qlead_count']) }} as qlead_count,
    {{ json_extract_scalar('_airbyte_data', ['data','deposit_count'],['deposit_count']) }} as deposit_count,
    {{ json_extract_scalar('_airbyte_data', ['data','deposit_total'],['deposit_total']) }} as deposit_total,
    {{ json_extract_scalar('_airbyte_data', ['data','net_deposits'],['net_deposits']) }} as net_deposits,
    {{ json_extract_scalar('_airbyte_data', ['data','net_pl'],['net_pl']) }} as net_pl,
    {{ json_extract_scalar('_airbyte_data', ['data','net_pl_sport'],['net_pl_sport']) }} as net_pl_sport,
    {{ json_extract_scalar('_airbyte_data', ['data','net_pl_casino'],['net_pl_casino']) }} as net_pl_casino,
    {{ json_extract_scalar('_airbyte_data', ['data','net_win'],['net_win']) }} as net_win,
    {{ json_extract_scalar('_airbyte_data', ['data','pl'],['pl']) }} as pl,
    {{ json_extract_scalar('_airbyte_data', ['data','bonus_amount'],['bonus_amount']) }} as bonus_amount,
    {{ json_extract_scalar('_airbyte_data', ['data','ftd_total'],['ftd_total']) }} as ftd_total,
    {{ json_extract_scalar('_airbyte_data', ['data','withdrawal_count'],['withdrawal_count']) }} as withdrawal_count,
    {{ json_extract_scalar('_airbyte_data', ['data','withdrawal_total'],['withdrawal_total']) }} as withdrawal_total,
    {{ json_extract_scalar('_airbyte_data', ['data','chargeback_total'],['chargeback_total']) }} as chargeback_total,
    {{ json_extract_scalar('_airbyte_data', ['data','volume'],['volume']) }} as volume,
    {{ json_extract_scalar('_airbyte_data', ['data','operations'],['operations']) }} as operations,
    {{ json_extract_scalar('_airbyte_data', ['data','afp'],['afp']) }} as AFP,
    {{ json_extract_scalar('_airbyte_data', ['data','balance'],['balance']) }} as BALANCE,
    {{ json_extract_scalar('_airbyte_data', ['data','chargback_total'],['chargback_total']) }} as CHARGBACK_TOTAL,
    {{ json_extract_scalar('_airbyte_data', ['data','commissions_cpa'],['commissions_cpa']) }} as COMMISSIONS_CPA,
    {{ json_extract_scalar('_airbyte_data', ['data','commissions_cpl'],['commissions_cpl']) }} as COMMISSIONS_CPL,
    {{ json_extract_scalar('_airbyte_data', ['data','commissions_rev_share'],['commissions_rev_share']) }} as COMMISSIONS_REV_SHARE,
    {{ json_extract_scalar('_airbyte_data', ['data','commissions_total'],['commissions_total']) }} as COMMISSIONS_TOTAL,
    {{ json_extract_scalar('_airbyte_data', ['data','conversion_rate'],['conversion_rate']) }} as CONVERSION_RATE,
    {{ json_extract_scalar('_airbyte_data', ['data','dt'],['dt']) }} as DT,
    {{ json_extract_scalar('_airbyte_data', ['data','deductions'],['deductions']) }} as deductions,
    {{ json_extract_scalar('_airbyte_data', ['data','adjustment_affiliate'],['adjustment_affiliate']) }} as adjustment_affiliate,
    {{ json_extract_scalar('_airbyte_data', ['data','adjustment_registration'],['adjustment_registration']) }} as adjustment_registration,
    {{ json_extract_scalar('_airbyte_data', ['data','net_deposit_total'],['net_deposit_total']) }} as net_deposit_total,
    {{ json_extract_scalar('_airbyte_data', ['data','ftd_count'],['ftd_count']) }} as FTD_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','payments'],['payments']) }} as PAYMENTS,
    {{ json_extract_scalar('_airbyte_data', ['data','registration_count'],['registration_count']) }} as REGISTRATION_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','sub_commission_from_child'],['sub_commission_from_child']) }} as SUB_COMMISSION_FROM_CHILD,
    {{ json_extract_scalar('_airbyte_data', ['data','utm_campaign'],['utm_campaign']) }} as UTM_CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['data','utm_medium'],['utm_medium']) }} as UTM_MEDIUM,
    {{ json_extract_scalar('_airbyte_data', ['data','utm_source'],['utm_source']) }} as UTM_SOURCE,
    {{ json_extract_scalar('_airbyte_data', ['data','visit_count'],['visit_count']) }} as VISIT_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('SMARTICO', '_AIRBYTE_RAW_UTM_CODE_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
-- AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

