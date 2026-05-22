{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
   	try_cast(AFFILIATE_NETWORK as {{ dbt_utils.type_string() }}) as AFFILIATE_NETWORK,
	try_cast(ALLOWED_ACTIONS as {{ dbt_utils.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(CAP_CONFIGURATION as {{ dbt_utils.type_string() }}) as CAP_CONFIGURATION,
	try_cast(CONVERSION_TRACKING_METHOD as {{ dbt_utils.type_string() }}) as CONVERSION_TRACKING_METHOD,
	try_cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
	try_cast(CREATED_TIME as {{ dbt_utils.type_string() }}) as CREATED_TIME,
	try_cast(CURRENCY_CODE as {{ dbt_utils.type_string() }}) as CURRENCY_CODE,
	try_cast(DELETED as {{ dbt_utils.type_string() }}) as DELETED,
	try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
	try_cast(MARKETPLACE as {{ dbt_utils.type_string() }}) as MARKETPLACE,
	try_cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
	try_cast(NAME_POSTFIX as {{ dbt_utils.type_string() }}) as NAME_POSTFIX,
	try_cast(PAYOUT as {{ dbt_utils.type_string() }}) as PAYOUT,
	try_cast(PREFERRED_TRACKING_DOMAIN as {{ dbt_utils.type_string() }}) as PREFERRED_TRACKING_DOMAIN,
	try_cast(TAGS as {{ dbt_utils.type_string() }}) as TAGS,
	try_cast(UPDATED_TIME as {{ dbt_utils.type_string() }}) as UPDATED_TIME,
	try_cast(URL as {{ dbt_utils.type_string() }}) as URL,
	try_cast(WORKSPACE as {{ dbt_utils.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFERS_AB1') }}
where 1 = 1