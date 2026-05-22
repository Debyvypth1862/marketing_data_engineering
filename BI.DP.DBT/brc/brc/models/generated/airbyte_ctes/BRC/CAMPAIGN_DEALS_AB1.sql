{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['cade_camp_array'], ['cade_camp_array']) }} as CADE_CAMP_ARRAY,
	{{ json_extract_scalar('_airbyte_data', ['cade_created'], ['cade_created']) }} as CADE_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['cade_created_by'], ['cade_created_by']) }} as CADE_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['cade_deal'], ['cade_deal']) }} as CADE_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['cade_deal_text'], ['cade_deal_text']) }} as CADE_DEAL_TEXT,
	{{ json_extract_scalar('_airbyte_data', ['cade_description'], ['cade_description']) }} as CADE_DESCRIPTION,
	{{ json_extract_scalar('_airbyte_data', ['cade_featured'], ['cade_featured']) }} as CADE_FEATURED,
	{{ json_extract_scalar('_airbyte_data', ['cade_fk_advertiser'], ['cade_fk_advertiser']) }} as CADE_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['cade_id'], ['cade_id']) }} as CADE_ID,
	{{ json_extract_scalar('_airbyte_data', ['cade_lang_array'], ['cade_lang_array']) }} as CADE_LANG_ARRAY,
	{{ json_extract_scalar('_airbyte_data', ['cade_live'], ['cade_live']) }} as CADE_LIVE,
	{{ json_extract_scalar('_airbyte_data', ['cade_position'], ['cade_position']) }} as CADE_POSITION,
	{{ json_extract_scalar('_airbyte_data', ['cade_url'], ['cade_url']) }} as CADE_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_DEALS') }} as table_alias
-- CAMPAIGN_DEALS
where 1 = 1
