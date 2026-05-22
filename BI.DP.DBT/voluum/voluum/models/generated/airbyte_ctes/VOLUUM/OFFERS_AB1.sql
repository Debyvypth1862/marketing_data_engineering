{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}

select 
    {{ json_extract_scalar('_airbyte_data', ['affiliateNetwork'], ['affiliateNetwork']) }} as AFFILIATE_NETWORK,
	{{ json_extract_scalar('_airbyte_data', ['allowedActions'], ['allowedActions']) }} as ALLOWED_ACTIONS,
	{{ json_extract_scalar('_airbyte_data', ['capConfiguration'], ['capConfiguration']) }} as CAP_CONFIGURATION,
	{{ json_extract_scalar('_airbyte_data', ['conversionTrackingMethod'], ['conversionTrackingMethod']) }} as CONVERSION_TRACKING_METHOD,
	{{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as COUNTRY,
	{{ json_extract_scalar('_airbyte_data', ['createdTime'], ['createdTime']) }} as CREATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['currencyCode'], ['currencyCode']) }} as CURRENCY_CODE,
	{{ json_extract_scalar('_airbyte_data', ['deleted'], ['deleted']) }} as DELETED,
	{{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
	{{ json_extract_scalar('_airbyte_data', ['marketplace'], ['marketplace']) }} as MARKETPLACE,
	{{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
	{{ json_extract_scalar('_airbyte_data', ['namePostfix'], ['namePostfix']) }} as NAME_POSTFIX,
	{{ json_extract_scalar('_airbyte_data', ['payout'], ['payout']) }} as PAYOUT,
	{{ json_extract_scalar('_airbyte_data', ['preferredTrackingDomain'], ['preferredTrackingDomain']) }} as PREFERRED_TRACKING_DOMAIN,
	{{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as TAGS,
	{{ json_extract_scalar('_airbyte_data', ['updatedTime'], ['updatedTime']) }} as UPDATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
	{{ json_extract_scalar('_airbyte_data', ['workspace'], ['workspace']) }} as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('VOLUUM', '_AIRBYTE_RAW_OFFERS') }} as table_alias
