{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PAHI_FK_PAYMENT as {{ dbt_utils.type_float() }}) as PAHI_FK_PAYMENT,
	try_cast(PAHI_ID as {{ dbt_utils.type_float() }}) as PAHI_ID,
	try_cast(PAHI_TABLE_PAYMENT_DETAILS as {{ dbt_utils.type_string() }}) as PAHI_TABLE_PAYMENT_DETAILS,
	try_cast(PAHI_TABLE_PAYMENTS as {{ dbt_utils.type_string() }}) as PAHI_TABLE_PAYMENTS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENT_HISTORY_AB1') }}
-- PAYMENT_HISTORY
where 1 = 1