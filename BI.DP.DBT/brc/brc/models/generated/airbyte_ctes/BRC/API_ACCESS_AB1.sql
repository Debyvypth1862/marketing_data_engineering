{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['api_id'], ['api_id']) }} as API_ID,
	{{ json_extract_scalar('_airbyte_data', ['api_key'], ['api_key']) }} as API_KEY,
	{{ json_extract_scalar('_airbyte_data', ['api_whitelist_json'], ['api_whitelist_json']) }} as API_WHITELIST_JSON,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_API_ACCESS') }} as table_alias
where 1 = 1
