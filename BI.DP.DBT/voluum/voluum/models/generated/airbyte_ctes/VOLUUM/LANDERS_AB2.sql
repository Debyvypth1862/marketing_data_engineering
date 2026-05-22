{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
    try_cast(ALLOWED_ACTIONS as {{ dbt_utils.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
	try_cast(CREATED_TIME as {{ dbt_utils.type_string() }}) as CREATED_TIME,
	try_cast(DELETED as {{ dbt_utils.type_string() }}) as DELETED,
	try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
	try_cast(LANDER_TYPE as {{ dbt_utils.type_string() }}) as LANDER_TYPE,
	try_cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
	try_cast(NAME_POSTFIX as {{ dbt_utils.type_string() }}) as NAME_POSTFIX,
	try_cast(NUMBER_OF_OFFERS as {{ dbt_utils.type_float() }}) as NUMBER_OF_OFFERS,
	try_cast(PREFERRED_TRACKING_DOMAIN as {{ dbt_utils.type_string() }}) as PREFERRED_TRACKING_DOMAIN,
	try_cast(SHOULD_HAVE_TRACKING_SCRIPT as {{ dbt_utils.type_string() }}) as SHOULD_HAVE_TRACKING_SCRIPT,
	try_cast(TAGS as {{ dbt_utils.type_string() }}) as TAGS,
	try_cast(UPDATED_TIME as {{ dbt_utils.type_string() }}) as UPDATED_TIME,
	try_cast(URL as {{ dbt_utils.type_string() }}) as URL,
	try_cast(WORKSPACE as {{ dbt_utils.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('LANDERS_AB1') }}
where 1 = 1