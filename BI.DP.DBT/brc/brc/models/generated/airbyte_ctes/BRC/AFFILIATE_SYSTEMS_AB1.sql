{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['afsy_active'], ['afsy_active']) }} as AFSY_ACTIVE,
	{{ json_extract_scalar('_airbyte_data', ['afsy_columns'], ['afsy_columns']) }} as AFSY_COLUMNS,
	{{ json_extract_scalar('_airbyte_data', ['afsy_custom'], ['afsy_custom']) }} as AFSY_CUSTOM,
	{{ json_extract_scalar('_airbyte_data', ['afsy_id'], ['afsy_id']) }} as AFSY_ID,
	{{ json_extract_scalar('_airbyte_data', ['afsy_name'], ['afsy_name']) }} as AFSY_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_AFFILIATE_SYSTEMS') }} as table_alias
-- AFFILIATE_SYSTEMS
where 1 = 1
