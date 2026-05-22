{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PAYM_CREATED as {{ dbt_utils.type_string() }}) as PAYM_CREATED,
	try_cast(PAYM_CREATED_BY as {{ dbt_utils.type_float() }}) as PAYM_CREATED_BY,
	try_cast(PAYM_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as PAYM_FK_PUBLISHER,
	try_cast(PAYM_ID as {{ dbt_utils.type_float() }}) as PAYM_ID,
	try_cast(PAYM_NOTE as {{ dbt_utils.type_string() }}) as PAYM_NOTE,
	try_cast(PAYM_PAYMENT_INFO as {{ dbt_utils.type_string() }}) as PAYM_PAYMENT_INFO,
	try_cast(PAYM_PERIOD_FROM as {{ dbt_utils.type_string() }}) as PAYM_PERIOD_FROM,
	try_cast(PAYM_PERIOD_TO as {{ dbt_utils.type_string() }}) as PAYM_PERIOD_TO,
	try_cast(PAYM_STATUS as {{ dbt_utils.type_string() }}) as PAYM_STATUS,
	try_cast(PAYM_UPDATED as {{ dbt_utils.type_string() }}) as PAYM_UPDATED,
	try_cast(PAYM_UPDATED_BY as {{ dbt_utils.type_float() }}) as PAYM_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENTS_AB1') }}
-- PAYMENTS
where 1 = 1