{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['tdat_clickbot'], ['tdat_clickbot']) }} as TDAT_CLICKBOT,
        {{ json_extract_scalar('_airbyte_data', ['tdat_clicks'], ['tdat_clicks']) }} as TDAT_CLICKS,
        {{ json_extract_scalar('_airbyte_data', ['tdat_cpa'], ['tdat_cpa']) }} as TDAT_CPA,
        {{ json_extract_scalar('_airbyte_data', ['tdat_date'], ['tdat_date']) }} as TDAT_DATE,
        {{ json_extract_scalar('_airbyte_data', ['tdat_deposit_value'], ['tdat_deposit_value']) }} as TDAT_DEPOSIT_VALUE,
        {{ json_extract_scalar('_airbyte_data', ['tdat_deposits'], ['tdat_deposits']) }} as TDAT_DEPOSITS,
        {{ json_extract_scalar('_airbyte_data', ['tdat_extra_income'], ['tdat_extra_income']) }} as TDAT_EXTRA_INCOME,
        {{ json_extract_scalar('_airbyte_data', ['tdat_extra_payout'], ['tdat_extra_payout']) }} as TDAT_EXTRA_PAYOUT,
        {{ json_extract_scalar('_airbyte_data', ['tdat_fk_campaign_key'], ['tdat_fk_campaign_key']) }} as TDAT_FK_CAMPAIGN_KEY,
        {{ json_extract_scalar('_airbyte_data', ['tdat_fk_campaign_tracker'], ['tdat_fk_campaign_tracker']) }} as TDAT_FK_CAMPAIGN_TRACKER,
        {{ json_extract_scalar('_airbyte_data', ['tdat_fk_custom'], ['tdat_fk_custom']) }} as TDAT_FK_CUSTOM,
        {{ json_extract_scalar('_airbyte_data', ['tdat_new_deposits'], ['tdat_new_deposits']) }} as TDAT_NEW_DEPOSITS,
        {{ json_extract_scalar('_airbyte_data', ['tdat_postback'], ['tdat_postback']) }} as TDAT_POSTBACK,
        {{ json_extract_scalar('_airbyte_data', ['tdat_script'], ['tdat_script']) }} as TDAT_SCRIPT,
        {{ json_extract_scalar('_airbyte_data', ['tdat_signups'], ['tdat_signups']) }} as TDAT_SIGNUPS,
        {{ json_extract_scalar('_airbyte_data', ['tdat_total'], ['tdat_total']) }} as TDAT_TOTAL,
        {{ json_extract_scalar('_airbyte_data', ['tdat_views'], ['tdat_views']) }} as TDAT_VIEWS,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_TRACKER_DATA') }} as table_alias
where 1 = 1
