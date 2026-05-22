{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(API_ID as {{ dbt_utils.type_float() }}) as API_ID,
	try_cast(API_KEY as {{ dbt_utils.type_string() }}) as API_KEY,
	try_cast(API_WHITELIST_JSON as {{ dbt_utils.type_string() }}) as API_WHITELIST_JSON,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('API_ACCESS_AB1') }}
-- API_ACCESS
where 1 = 1