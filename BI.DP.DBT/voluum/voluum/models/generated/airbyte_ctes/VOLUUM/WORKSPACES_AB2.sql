{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
	try_cast(MEMBERSHIPS as {{ dbt_utils.type_string() }}) as MEMBERSHIPS,
	try_cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('WORKSPACES_AB1') }}
where 1 = 1