{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(AFSY_ACTIVE as {{ dbt_utils.type_float() }}) as AFSY_ACTIVE,
	try_cast(AFSY_COLUMNS as {{ dbt_utils.type_string() }}) as AFSY_COLUMNS,
	try_cast(AFSY_CUSTOM as {{ dbt_utils.type_float() }}) as AFSY_CUSTOM,
	try_cast(AFSY_ID as {{ dbt_utils.type_float() }}) as AFSY_ID,
	try_cast(AFSY_NAME as {{ dbt_utils.type_string() }}) as AFSY_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('AFFILIATE_SYSTEMS_AB1') }}
-- AFFILIATE_SYSTEMS
where 1 = 1