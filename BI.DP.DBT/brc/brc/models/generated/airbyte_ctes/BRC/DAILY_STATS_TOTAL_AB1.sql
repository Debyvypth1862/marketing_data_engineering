{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['dast_clicks'], ['dast_clicks']) }} as DAST_CLICKS,
        {{ json_extract_scalar('_airbyte_data', ['dast_cpa'], ['dast_cpa']) }} as DAST_CPA,
        {{ json_extract_scalar('_airbyte_data', ['dast_day'], ['dast_day']) }} as DAST_DAY,
        {{ json_extract_scalar('_airbyte_data', ['dast_deposits'], ['dast_deposits']) }} as DAST_DEPOSITS,
        {{ json_extract_scalar('_airbyte_data', ['dast_ftd'], ['dast_ftd']) }} as DAST_FTD,
        {{ json_extract_scalar('_airbyte_data', ['dast_id'], ['dast_id']) }} as DAST_ID,
        {{ json_extract_scalar('_airbyte_data', ['dast_income'], ['dast_income']) }} as DAST_INCOME,
        {{ json_extract_scalar('_airbyte_data', ['dast_month'], ['dast_month']) }} as DAST_MONTH,
        {{ json_extract_scalar('_airbyte_data', ['dast_payout'], ['dast_payout']) }} as DAST_PAYOUT,
        {{ json_extract_scalar('_airbyte_data', ['dast_profit'], ['dast_profit']) }} as DAST_PROFIT,
        {{ json_extract_scalar('_airbyte_data', ['dast_signups'], ['dast_signups']) }} as DAST_SIGNUPS,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_DAILY_STATS_TOTAL') }} as table_alias
where 1 = 1
