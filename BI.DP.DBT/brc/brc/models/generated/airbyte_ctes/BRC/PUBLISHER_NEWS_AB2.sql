{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PUNE_ACTIVE as {{ dbt_utils.type_float() }}) as PUNE_ACTIVE,
	try_cast(PUNE_CREATED as {{ dbt_utils.type_string() }}) as PUNE_CREATED,
	try_cast(PUNE_CREATED_BY as {{ dbt_utils.type_float() }}) as PUNE_CREATED_BY,
	try_cast(PUNE_HEADLINE as {{ dbt_utils.type_string() }}) as PUNE_HEADLINE,
	try_cast(PUNE_ID as {{ dbt_utils.type_float() }}) as PUNE_ID,
	try_cast(PUNE_IMPORTANT as {{ dbt_utils.type_float() }}) as PUNE_IMPORTANT,
	try_cast(PUNE_STATUS as {{ dbt_utils.type_string() }}) as PUNE_STATUS,
	try_cast(PUNE_TEXT as {{ dbt_utils.type_string() }}) as PUNE_TEXT,
	try_cast(PUNE_THUMB as {{ dbt_utils.type_string() }}) as PUNE_THUMB,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_NEWS_AB1') }}
-- PUBLISHER_NEWS
where 1 = 1
