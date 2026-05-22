{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}

select 
    {{ json_extract_scalar('_airbyte_data', ['allowedActions'], ['allowedActions']) }} as ALLOWED_ACTIONS,
	{{ json_extract_scalar('_airbyte_data', ['appendClickIdToOfferUrl'], ['appendClickIdToOfferUrl']) }} as APPEND_CLICK_ID_TO_OFFER_URL,
	{{ json_extract_scalar('_airbyte_data', ['conversionTrackingMethod'], ['conversionTrackingMethod']) }} as CONVERSION_TRACKING_METHOD,
	{{ json_extract_scalar('_airbyte_data', ['createdTime'], ['createdTime']) }} as CREATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['currencyCode'], ['currencyCode']) }} as CURRENCY_CODE,
	{{ json_extract_scalar('_airbyte_data', ['deleted'], ['deleted']) }} as DELETED,
	{{ json_extract_scalar('_airbyte_data', ['duplicatedPostbackIsUpsell'], ['duplicatedPostbackIsUpsell']) }} as DUPLICATED_POSTBACK_IS_UPSELL,
	{{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
	{{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
	{{ json_extract_scalar('_airbyte_data', ['offerUrlClickIdParameter'], ['offerUrlClickIdParameter']) }} as OFFER_URL_CLICK_ID_PARAMETER,
	{{ json_extract_scalar('_airbyte_data', ['postbackClickIdToken'], ['postbackClickIdToken']) }} as POSTBACK_CLICK_ID_TOKEN,
	{{ json_extract_scalar('_airbyte_data', ['postbackEventTypeToken'], ['postbackEventTypeToken']) }} as POSTBACK_EVENT_TYPE_TOKEN,
	{{ json_extract_scalar('_airbyte_data', ['postbackPayoutToken'], ['postbackPayoutToken']) }} as POSTBACK_PAYOUT_TOKEN,
	{{ json_extract_scalar('_airbyte_data', ['postbackTransactionIdToken'], ['postbackTransactionIdToken']) }} as POSTBACK_TRANSACTION_ID_TOKEN,
	{{ json_extract_scalar('_airbyte_data', ['postbackUrl'], ['postbackUrl']) }} as POSTBACK_URL,
	{{ json_extract_scalar('_airbyte_data', ['preferredTrackingDomain'], ['preferredTrackingDomain']) }} as PREFERRED_TRACKING_DOMAIN,
	{{ json_extract_scalar('_airbyte_data', ['updatedTime'], ['updatedTime']) }} as UPDATED_TIME,
	{{ json_extract_scalar('_airbyte_data', ['workspace'], ['workspace']) }} as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('VOLUUM', '_AIRBYTE_RAW_AFFILIATE_NETWORKS') }} as table_alias
