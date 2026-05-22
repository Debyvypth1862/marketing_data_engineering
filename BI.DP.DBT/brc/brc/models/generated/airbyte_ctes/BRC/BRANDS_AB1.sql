{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['bran_id'], ['bran_id']) }} as BRAN_ID,
	{{ json_extract_scalar('_airbyte_data', ['bran_name'], ['bran_name']) }} as BRAN_NAME,
	{{ json_extract_scalar('_airbyte_data', ['bran_platform'], ['bran_platform']) }} as BRAN_PLATFORM,
	{{ json_extract_scalar('_airbyte_data', ['bran_slug'], ['bran_slug']) }} as BRAN_SLUG,
	{{ json_extract_scalar('_airbyte_data', ['bran_url'], ['bran_url']) }} as BRAN_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_BRANDS') }} as table_alias
-- BRANDS
where 1 = 1
