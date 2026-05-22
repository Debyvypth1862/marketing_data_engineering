{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['pune_active'], ['pune_active']) }} as PUNE_ACTIVE,
	{{ json_extract_scalar('_airbyte_data', ['pune_created'], ['pune_created']) }} as PUNE_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['pune_created_by'], ['pune_created_by']) }} as PUNE_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['pune_headline'], ['pune_headline']) }} as PUNE_HEADLINE,
	{{ json_extract_scalar('_airbyte_data', ['pune_id'], ['pune_id']) }} as PUNE_ID,
	{{ json_extract_scalar('_airbyte_data', ['pune_important'], ['pune_important']) }} as PUNE_IMPORTANT,
	{{ json_extract_scalar('_airbyte_data', ['pune_status'], ['pune_status']) }} as PUNE_STATUS,
	{{ json_extract_scalar('_airbyte_data', ['pune_text'], ['pune_text']) }} as PUNE_TEXT,
	{{ json_extract_scalar('_airbyte_data', ['pune_thumb'], ['pune_thumb']) }} as PUNE_THUMB,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_NEWS') }} as table_alias
where 1 = 1
