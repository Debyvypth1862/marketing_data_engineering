{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PUMA_FK_ADMIN as {{ dbt_utils.type_float() }}) as PUMA_FK_ADMIN,
	try_cast(PUMA_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as PUMA_FK_PUBLISHER,
	try_cast(PUMA_ID as {{ dbt_utils.type_float() }}) as PUMA_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_MANAGERS_AB1') }}
-- PUBLISHER_MANAGERS
where 1 = 1