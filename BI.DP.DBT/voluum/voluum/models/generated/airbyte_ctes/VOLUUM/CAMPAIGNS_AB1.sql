{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}

select 
    {{ json_extract_scalar('_airbyte_data', ['allowedActions'], ['allowedActions']) }} as ALLOWED_ACTIONS,
	{{ json_extract_scalar('_airbyte_data', ['basic'], ['basic']) }} as BASIC,
	{{ json_extract_scalar('_airbyte_data', ['costModel', 'type'], ['costModel']) }} as COST_MODEL,
	{{ json_extract_scalar('_airbyte_data', ['country', 'code'], ['country']) }} as COUNTRY,
	{{ json_extract_scalar('_airbyte_data', ['createdTime'], ['createdTime']) }} as CREATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['customPostbacksConfiguration', 'customConversionPostbacks'], ['customConversionPostbacks']) }} as CUSTOM_POSTBACKS_CONFIGURATION,
	{{ json_extract_scalar('_airbyte_data', ['deleted'], ['deleted']) }} as DELETED,
	{{ json_extract_scalar('_airbyte_data', ['directTracking'], ['directTracking']) }} as DIRECT_TRACKING,
	{{ json_extract_scalar('_airbyte_data', ['directTrackingLanderId'], ['directTrackingLanderId']) }} as DIRECT_TRACKING_LANDER_ID,
	{{ json_extract_scalar('_airbyte_data', ['directTrackingOfferId'], ['directTrackingOfferId']) }} as DIRECT_TRACKING_OFFER_ID,
	{{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
	{{ json_extract_scalar('_airbyte_data', ['impressionUrl'], ['impressionUrl']) }} as IMPRESSION_URL,
	{{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
	{{ json_extract_scalar('_airbyte_data', ['namePostfix'], ['namePostfix']) }} as NAME_POSTFIX,
	{{ json_extract_scalar('_airbyte_data', ['preferredTrackingDomain'], ['preferredTrackingDomain']) }} as PREFERRED_TRACKING_DOMAIN,
	{{ json_extract_scalar('_airbyte_data', ['redirectTarget'], ['redirectTarget']) }} as REDIRECT_TARGET,
	{{ json_extract_scalar('_airbyte_data', ['revenueModel'], ['revenueModel']) }} as REVENUE_MODEL,
	{{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as TAGS,
	{{ json_extract_scalar('_airbyte_data', ['trafficSource', 'id'], ['trafficSource']) }} as TRAFFIC_SOURCE,
	{{ json_extract_scalar('_airbyte_data', ['trafficType'], ['trafficType']) }} as TRAFFIC_TYPE,
	{{ json_extract_scalar('_airbyte_data', ['updatedTime'], ['updatedTime']) }} as UPDATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
	{{ json_extract_scalar('_airbyte_data', ['workspace', 'id'], ['workspace']) }} as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
FROM {{ source('VOLUUM', '_AIRBYTE_RAW_CAMPAIGNS') }} as table_alias
