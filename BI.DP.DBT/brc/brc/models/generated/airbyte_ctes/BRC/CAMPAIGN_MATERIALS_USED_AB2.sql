{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CAUS_FK_CAMA_ID as {{ dbt_utils.type_float() }}) as CAUS_FK_CAMA_ID,
	try_cast(CAUS_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as CAUS_FK_PUBLISHER,
	try_cast(CAUS_ID as {{ dbt_utils.type_float() }}) as CAUS_ID,
	try_cast(CAUS_LAST_VISIT as {{ dbt_utils.type_string() }}) as CAUS_LAST_VISIT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_MATERIALS_USED_AB1') }}
-- CAMPAIGN_MATERIALS_USED
where 1 = 1