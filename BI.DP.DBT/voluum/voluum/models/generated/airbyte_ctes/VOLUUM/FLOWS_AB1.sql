{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}

select 
    {{ json_extract_scalar('_airbyte_data', ['allowedActions'], ['allowedActions']) }} as ALLOWED_ACTIONS,
	{{ json_extract_scalar('_airbyte_data', ['conditionalPathsGroups'], ['conditionalPathsGroups']) }} as CONDITIONAL_PATHS_GROUPS,
	{{ json_extract_scalar('_airbyte_data', ['countries'], ['countries']) }} as COUNTRIES,
	{{ json_extract_scalar('_airbyte_data', ['createdTime'], ['createdTime']) }} as CREATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['defaultOfferRedirectMode'], ['defaultOfferRedirectMode']) }} as DEFAULT_OFFER_REDIRECT_MODE,
	{{ json_extract_scalar('_airbyte_data', ['defaultPaths'], ['defaultPaths']) }} as DEFAULT_PATHS,
	{{ json_extract_scalar('_airbyte_data', ['defaultPathsSmartRotation'], ['defaultPathsSmartRotation']) }} as DEFAULT_PATHS_SMART_ROTATION,
	{{ json_extract_scalar('_airbyte_data', ['deleted'], ['deleted']) }} as DELETED,
	{{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
	{{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
	{{ json_extract_scalar('_airbyte_data', ['realtimeRoutingApi'], ['realtimeRoutingApi']) }} as REALTIME_ROUTING_API,
	{{ json_extract_scalar('_airbyte_data', ['updatedTime'], ['updatedTime']) }} as UPDATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['workspace'], ['workspace']) }} as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('VOLUUM', '_AIRBYTE_RAW_FLOWS') }} as table_alias
