{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PAYD_AMOUNT as {{ dbt_utils.type_float() }}) as PAYD_AMOUNT,
	try_cast(PAYD_COMMENT as {{ dbt_utils.type_string() }}) as PAYD_COMMENT,
	try_cast(PAYD_DATE as {{ dbt_utils.type_string() }}) as PAYD_DATE,
	try_cast(PAYD_FK_ADDE_ID as {{ dbt_utils.type_float() }}) as PAYD_FK_ADDE_ID,
	try_cast(PAYD_FK_ADPA_ID as {{ dbt_utils.type_float() }}) as PAYD_FK_ADPA_ID,
	try_cast(PAYD_ID as {{ dbt_utils.type_float() }}) as PAYD_ID,
	try_cast(PAYD_RECEIVED_BY as {{ dbt_utils.type_string() }}) as PAYD_RECEIVED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_DETAILS_PAID_AB1') }}
-- ADVERTISER_PAYMENTS_DETAILS_PAID
where 1 = 1