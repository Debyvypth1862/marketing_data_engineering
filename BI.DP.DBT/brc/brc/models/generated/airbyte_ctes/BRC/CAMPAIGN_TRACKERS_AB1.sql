{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['camt_campaign_key'], ['camt_campaign_key']) }} as CAMT_CAMPAIGN_KEY,
	{{ json_extract_scalar('_airbyte_data', ['camt_child'], ['camt_child']) }} as CAMT_CHILD,
	{{ json_extract_scalar('_airbyte_data', ['camt_child_name'], ['camt_child_name']) }} as CAMT_CHILD_NAME,
	{{ json_extract_scalar('_airbyte_data', ['camt_CPA_in'], ['camt_CPA_in']) }} as CAMT_CPA_IN,
	{{ json_extract_scalar('_airbyte_data', ['camt_CPA_out'], ['camt_CPA_out']) }} as CAMT_CPA_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camt_CPL_in'], ['camt_CPL_in']) }} as CAMT_CPL_IN,
	{{ json_extract_scalar('_airbyte_data', ['camt_CPL_out'], ['camt_CPL_out']) }} as CAMT_CPL_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camt_created'], ['camt_created']) }} as CAMT_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['camt_created_by'], ['camt_created_by']) }} as CAMT_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['camt_deal_date'], ['camt_deal_date']) }} as CAMT_DEAL_DATE,
	{{ json_extract_scalar('_airbyte_data', ['camt_display_deal'], ['camt_display_deal']) }} as CAMT_DISPLAY_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['camt_fk_advertiser'], ['camt_fk_advertiser']) }} as CAMT_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['camt_fk_campaign'], ['camt_fk_campaign']) }} as CAMT_FK_CAMPAIGN,
	{{ json_extract_scalar('_airbyte_data', ['camt_fk_login'], ['camt_fk_login']) }} as CAMT_FK_LOGIN,
	{{ json_extract_scalar('_airbyte_data', ['camt_fk_publisher'], ['camt_fk_publisher']) }} as CAMT_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['camt_hidden'], ['camt_hidden']) }} as CAMT_HIDDEN,
	{{ json_extract_scalar('_airbyte_data', ['camt_history'], ['camt_history']) }} as CAMT_HISTORY,
	{{ json_extract_scalar('_airbyte_data', ['camt_id'], ['camt_id']) }} as CAMT_ID,
	{{ json_extract_scalar('_airbyte_data', ['camt_publisher_last_visit'], ['camt_publisher_last_visit']) }} as CAMT_PUBLISHER_LAST_VISIT,
	{{ json_extract_scalar('_airbyte_data', ['camt_REV_in'], ['camt_REV_in']) }} as CAMT_REV_IN,
	{{ json_extract_scalar('_airbyte_data', ['camt_REV_out'], ['camt_REV_out']) }} as CAMT_REV_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camt_show'], ['camt_show']) }} as CAMT_SHOW,
	{{ json_extract_scalar('_airbyte_data', ['camt_status'], ['camt_status']) }} as CAMT_STATUS,
	{{ json_extract_scalar('_airbyte_data', ['camt_subid_activated'], ['camt_subid_activated']) }} as CAMT_SUBID_ACTIVATED,
	{{ json_extract_scalar('_airbyte_data', ['camt_type'], ['camt_type']) }} as CAMT_TYPE,
	{{ json_extract_scalar('_airbyte_data', ['camt_updated'], ['camt_updated']) }} as CAMT_UPDATED,
	{{ json_extract_scalar('_airbyte_data', ['camt_updated_by'], ['camt_updated_by']) }} as CAMT_UPDATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['camt_url'], ['camt_url']) }} as CAMT_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_TRACKERS') }} as table_alias
-- CAMPAIGN_TRACKERS
where 1 = 1
