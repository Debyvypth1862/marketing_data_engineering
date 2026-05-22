{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(LOGI_FK_ADMIN as {{ dbt_utils.type_float() }}) as LOGI_FK_ADMIN,
	try_cast(LOGI_ID as {{ dbt_utils.type_float() }}) as LOGI_ID,
	try_cast(LOGI_IP as {{ dbt_utils.type_string() }}) as LOGI_IP,
	try_cast(LOGI_TIMESTAMP as {{ dbt_utils.type_string() }}) as LOGI_TIMESTAMP,
	try_cast(LOGI_USERAGENT as {{ dbt_utils.type_string() }}) as LOGI_USERAGENT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADMIN_LOGINS_AB1') }}
-- ADMIN_LOGINS
where 1 = 1