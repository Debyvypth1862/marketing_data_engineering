{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PROM_BANNER as {{ dbt_utils.type_string() }}) as PROM_BANNER,
	try_cast(PROM_FK_ADVERTISERS as {{ dbt_utils.type_string() }}) as PROM_FK_ADVERTISERS,
	try_cast(PROM_ID as {{ dbt_utils.type_float() }}) as PROM_ID,
	try_cast(PROM_MAIN_COMP as {{ dbt_utils.type_float() }}) as PROM_MAIN_COMP,
	try_cast(PROM_MONTH as {{ dbt_utils.type_string() }}) as PROM_MONTH,
	try_cast(PROM_NAME as {{ dbt_utils.type_string() }}) as PROM_NAME,
	try_cast(PROM_POSITION as {{ dbt_utils.type_float() }}) as PROM_POSITION,
	try_cast(PROM_TEXT as {{ dbt_utils.type_string() }}) as PROM_TEXT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PROMOTIONS_AB1') }}
-- PROMOTIONS
where 1 = 1