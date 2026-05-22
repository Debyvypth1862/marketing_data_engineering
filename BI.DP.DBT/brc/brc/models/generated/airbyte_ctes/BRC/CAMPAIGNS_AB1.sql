{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['camp_category'], ['camp_category']) }} as CAMP_CATEGORY,
	{{ json_extract_scalar('_airbyte_data', ['camp_country'], ['camp_country']) }} as CAMP_COUNTRY,
	{{ json_extract_scalar('_airbyte_data', ['camp_CPA_in'], ['camp_CPA_in']) }} as CAMP_CPA_IN,
	{{ json_extract_scalar('_airbyte_data', ['camp_CPA_out'], ['camp_CPA_out']) }} as CAMP_CPA_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camp_CPL_in'], ['camp_CPL_in']) }} as CAMP_CPL_IN,
	{{ json_extract_scalar('_airbyte_data', ['camp_CPL_out'], ['camp_CPL_out']) }} as CAMP_CPL_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camp_created'], ['camp_created']) }} as CAMP_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['camp_created_by'], ['camp_created_by']) }} as CAMP_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['camp_currency'], ['camp_currency']) }} as CAMP_CURRENCY,
	{{ json_extract_scalar('_airbyte_data', ['camp_deposit_baseline'], ['camp_deposit_baseline']) }} as CAMP_DEPOSIT_BASELINE,
	{{ json_extract_scalar('_airbyte_data', ['camp_description'], ['camp_description']) }} as CAMP_DESCRIPTION,
	{{ json_extract_scalar('_airbyte_data', ['camp_display_deal'], ['camp_display_deal']) }} as CAMP_DISPLAY_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['camp_ended'], ['camp_ended']) }} as CAMP_ENDED,
	{{ json_extract_scalar('_airbyte_data', ['camp_fk_advertiser'], ['camp_fk_advertiser']) }} as CAMP_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['camp_fk_brand'], ['camp_fk_brand']) }} as CAMP_FK_BRAND,
	{{ json_extract_scalar('_airbyte_data', ['camp_hidden'], ['camp_hidden']) }} as CAMP_HIDDEN,
	{{ json_extract_scalar('_airbyte_data', ['camp_history'], ['camp_history']) }} as CAMP_HISTORY,
	{{ json_extract_scalar('_airbyte_data', ['camp_hybrid'], ['camp_hybrid']) }} as CAMP_HYBRID,
	{{ json_extract_scalar('_airbyte_data', ['camp_id'], ['camp_id']) }} as CAMP_ID,
	{{ json_extract_scalar('_airbyte_data', ['camp_language'], ['camp_language']) }} as CAMP_LANGUAGE,
	{{ json_extract_scalar('_airbyte_data', ['camp_licensed'], ['camp_licensed']) }} as CAMP_LICENSED,
	{{ json_extract_scalar('_airbyte_data', ['camp_name'], ['camp_name']) }} as CAMP_NAME,
	{{ json_extract_scalar('_airbyte_data', ['camp_REV_deal'], ['camp_REV_deal']) }} as CAMP_REV_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['camp_REV_in'], ['camp_REV_in']) }} as CAMP_REV_IN,
	{{ json_extract_scalar('_airbyte_data', ['camp_REV_out'], ['camp_REV_out']) }} as CAMP_REV_OUT,
	{{ json_extract_scalar('_airbyte_data', ['camp_status'], ['camp_status']) }} as CAMP_STATUS,
	{{ json_extract_scalar('_airbyte_data', ['camp_traffic_source'], ['camp_traffic_source']) }} as CAMP_TRAFFIC_SOURCE,
	{{ json_extract_scalar('_airbyte_data', ['camp_type'], ['camp_type']) }} as CAMP_TYPE,
	{{ json_extract_scalar('_airbyte_data', ['camp_updated'], ['camp_updated']) }} as CAMP_UPDATED,
	{{ json_extract_scalar('_airbyte_data', ['camp_updated_by'], ['camp_updated_by']) }} as CAMP_UPDATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['camp_wager_baseline'], ['camp_wager_baseline']) }} as CAMP_WAGER_BASELINE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGNS') }} as table_alias
-- CAMPAIGNS
where 1 = 1
