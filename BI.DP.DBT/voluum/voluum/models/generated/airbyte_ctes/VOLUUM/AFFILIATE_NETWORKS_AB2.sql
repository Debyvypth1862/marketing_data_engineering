{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
    try_cast(ALLOWED_ACTIONS as {{ dbt_utils.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(APPEND_CLICK_ID_TO_OFFER_URL as {{ dbt_utils.type_string() }}) as APPEND_CLICK_ID_TO_OFFER_URL,
	try_cast(CONVERSION_TRACKING_METHOD as {{ dbt_utils.type_string() }}) as CONVERSION_TRACKING_METHOD,
	try_cast(CREATED_TIME as {{ dbt_utils.type_string() }}) as CREATED_TIME,
	try_cast(CURRENCY_CODE as {{ dbt_utils.type_string() }}) as CURRENCY_CODE,
	try_cast(DELETED as {{ dbt_utils.type_string() }}) as DELETED,
	try_cast(DUPLICATED_POSTBACK_IS_UPSELL as {{ dbt_utils.type_string() }}) as DUPLICATED_POSTBACK_IS_UPSELL,
	try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
	try_cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
	try_cast(OFFER_URL_CLICK_ID_PARAMETER as {{ dbt_utils.type_string() }}) as OFFER_URL_CLICK_ID_PARAMETER,
	try_cast(POSTBACK_CLICK_ID_TOKEN as {{ dbt_utils.type_string() }}) as POSTBACK_CLICK_ID_TOKEN,
	try_cast(POSTBACK_EVENT_TYPE_TOKEN as {{ dbt_utils.type_string() }}) as POSTBACK_EVENT_TYPE_TOKEN,
	try_cast(POSTBACK_PAYOUT_TOKEN as {{ dbt_utils.type_string() }}) as POSTBACK_PAYOUT_TOKEN,
	try_cast(POSTBACK_TRANSACTION_ID_TOKEN as {{ dbt_utils.type_string() }}) as POSTBACK_TRANSACTION_ID_TOKEN,
	try_cast(POSTBACK_URL as {{ dbt_utils.type_string() }}) as POSTBACK_URL,
	try_cast(PREFERRED_TRACKING_DOMAIN as {{ dbt_utils.type_string() }}) as PREFERRED_TRACKING_DOMAIN,
	try_cast(UPDATED_TIME as {{ dbt_utils.type_string() }}) as UPDATED_TIME,
	try_cast(WORKSPACE as {{ dbt_utils.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('AFFILIATE_NETWORKS_AB1') }}
where 1 = 1