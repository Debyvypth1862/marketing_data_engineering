{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
    try_cast(ALLOWED_ACTIONS as {{ dbt_utils.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(CONDITIONAL_PATHS_GROUPS as {{ dbt_utils.type_string() }}) as CONDITIONAL_PATHS_GROUPS,
	try_cast(COUNTRIES as {{ dbt_utils.type_string() }}) as COUNTRIES,
	try_cast(CREATED_TIME as {{ dbt_utils.type_string() }}) as CREATED_TIME,
	try_cast(DEFAULT_OFFER_REDIRECT_MODE as {{ dbt_utils.type_string() }}) as DEFAULT_OFFER_REDIRECT_MODE,
	try_cast(DEFAULT_PATHS as {{ dbt_utils.type_string() }}) as DEFAULT_PATHS,
	try_cast(DEFAULT_PATHS_SMART_ROTATION as {{ dbt_utils.type_string() }}) as DEFAULT_PATHS_SMART_ROTATION,
	try_cast(DELETED as {{ dbt_utils.type_string() }}) as DELETED,
	try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
	try_cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
	try_cast(REALTIME_ROUTING_API as {{ dbt_utils.type_string() }}) as REALTIME_ROUTING_API,
	try_cast(UPDATED_TIME as {{ dbt_utils.type_string() }}) as UPDATED_TIME,
	try_cast(WORKSPACE as {{ dbt_utils.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('FLOWS_AB1') }}
where 1 = 1