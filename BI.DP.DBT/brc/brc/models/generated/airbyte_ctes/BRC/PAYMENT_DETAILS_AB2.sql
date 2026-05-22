{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PADE_AMOUNT as {{ dbt_utils.type_float() }}) as PADE_AMOUNT,
	try_cast(PADE_DESCRIPTION as {{ dbt_utils.type_string() }}) as PADE_DESCRIPTION,
	try_cast(PADE_FK_PAYMENT as {{ dbt_utils.type_float() }}) as PADE_FK_PAYMENT,
	try_cast(PADE_ID as {{ dbt_utils.type_float() }}) as PADE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENT_DETAILS_AB1') }}
-- PAYMENT_DETAILS
where 1 = 1