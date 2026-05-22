{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(ADDE_CREATED as {{ dbt_utils.type_string() }}) as ADDE_CREATED,
	try_cast(ADDE_FK_ADPA_ID as {{ dbt_utils.type_float() }}) as ADDE_FK_ADPA_ID,
	try_cast(ADDE_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as ADDE_FK_ADVERTISER,
	try_cast(ADDE_FK_PAYER as {{ dbt_utils.type_float() }}) as ADDE_FK_PAYER,
	try_cast(ADDE_ID as {{ dbt_utils.type_float() }}) as ADDE_ID,
	try_cast(ADDE_INCOME as {{ dbt_utils.type_float() }}) as ADDE_INCOME,
	try_cast(ADDE_UPDATED as {{ dbt_utils.type_string() }}) as ADDE_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_DETAILS_AB1') }}
-- ADVERTISER_PAYMENTS_DETAILS
where 1 = 1