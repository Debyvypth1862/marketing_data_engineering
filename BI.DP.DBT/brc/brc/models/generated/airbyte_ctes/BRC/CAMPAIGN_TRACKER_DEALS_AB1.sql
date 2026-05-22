{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['catd_CPA_in'], ['catd_CPA_in']) }} as CATD_CPA_IN,
	{{ json_extract_scalar('_airbyte_data', ['catd_CPA_out'], ['catd_CPA_out']) }} as CATD_CPA_OUT,
	{{ json_extract_scalar('_airbyte_data', ['catd_CPL_in'], ['catd_CPL_in']) }} as CATD_CPL_IN,
	{{ json_extract_scalar('_airbyte_data', ['catd_CPL_out'], ['catd_CPL_out']) }} as CATD_CPL_OUT,
	{{ json_extract_scalar('_airbyte_data', ['catd_display_deal'], ['catd_display_deal']) }} as CATD_DISPLAY_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['catd_fk_camt_id'], ['catd_fk_camt_id']) }} as CATD_FK_CAMT_ID,
	{{ json_extract_scalar('_airbyte_data', ['catd_id'], ['catd_id']) }} as CATD_ID,
	{{ json_extract_scalar('_airbyte_data', ['catd_REV_in'], ['catd_REV_in']) }} as CATD_REV_IN,
	{{ json_extract_scalar('_airbyte_data', ['catd_REV_out'], ['catd_REV_out']) }} as CATD_REV_OUT,
	{{ json_extract_scalar('_airbyte_data', ['catd_start_month'], ['catd_start_month']) }} as CATD_START_MONTH,
	{{ json_extract_scalar('_airbyte_data', ['catd_updated'], ['catd_updated']) }} as CATD_UPDATED,
	{{ json_extract_scalar('_airbyte_data', ['catd_updated_by'], ['catd_updated_by']) }} as CATD_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_TRACKER_DEALS') }} as table_alias
-- CAMPAIGN_TRACKER_DEALS
where 1 = 1
