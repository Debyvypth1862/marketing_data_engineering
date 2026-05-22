{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}

select 
    {{ json_extract_scalar('_airbyte_data', ['allowedActions'], ['allowedActions']) }} as ALLOWED_ACTIONS,
	{{ json_extract_scalar('_airbyte_data', ['clickIdVariable'], ['clickIdVariable']) }} as CLICK_ID_VARIABLE,
	{{ json_extract_scalar('_airbyte_data', ['costVariable'], ['costVariable']) }} as COST_VARIABLE,
	{{ json_extract_scalar('_airbyte_data', ['createdTime'], ['createdTime']) }} as CREATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['currencyCode'], ['currencyCode']) }} as CURRENCY_CODE,
	{{ json_extract_scalar('_airbyte_data', ['customPostbacksConfiguration'], ['customPostbacksConfiguration']) }} as CUSTOM_POSTBACKS_CONFIGURATION,
	{{ json_extract_scalar('_airbyte_data', ['customVariables'], ['customVariables']) }} as CUSTOM_VARIABLES,
	{{ json_extract_scalar('_airbyte_data', ['deleted'], ['deleted']) }} as DELETED,
	{{ json_extract_scalar('_airbyte_data', ['directTracking'], ['directTracking']) }} as DIRECT_TRACKING,
	{{ json_extract_scalar('_airbyte_data', ['externalIds'], ['externalIds']) }} as EXTERNAL_IDS,
	{{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
	{{ json_extract_scalar('_airbyte_data', ['impressionSpecific'], ['impressionSpecific']) }} as IMPRESSION_SPECIFIC,
	{{ json_extract_scalar('_airbyte_data', ['limitedGeoTracking'], ['limitedGeoTracking']) }} as LIMITED_GEO_TRACKING,
	{{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
	{{ json_extract_scalar('_airbyte_data', ['pixelRedirectUrl'], ['pixelRedirectUrl']) }} as PIXEL_REDIRECT_URL,
	{{ json_extract_scalar('_airbyte_data', ['postbackUrl'], ['postbackUrl']) }} as POSTBACK_URL,
	{{ json_extract_scalar('_airbyte_data', ['predefinedType'], ['predefinedType']) }} as PREDEFINED_TYPE,
	{{ json_extract_scalar('_airbyte_data', ['skipSendingPostback'], ['skipSendingPostback']) }} as SKIP_SENDING_POSTBACK,
	{{ json_extract_scalar('_airbyte_data', ['templateId'], ['templateId']) }} as TEMPLATE_ID,
	{{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as TYPE,
	{{ json_extract_scalar('_airbyte_data', ['updatedTime'], ['updatedTime']) }} as UPDATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['workspace', 'id'], ['workspace']) }} as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('VOLUUM', '_AIRBYTE_RAW_TRAFFIC_SOURCES') }} as table_alias
