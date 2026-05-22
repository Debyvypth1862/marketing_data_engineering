{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CARE_DATE as {{ dbt_utils.type_string() }}) as CARE_DATE,
	try_cast(CARE_FK_CAMA_ID as {{ dbt_utils.type_float() }}) as CARE_FK_CAMA_ID,
	try_cast(CARE_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as CARE_FK_PUBLISHER,
	try_cast(CARE_ID as {{ dbt_utils.type_float() }}) as CARE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_MATERIALS_REQUEST_NEW_AB1') }}
-- CAMPAIGN_MATERIALS_REQUEST_NEW
where 1 = 1