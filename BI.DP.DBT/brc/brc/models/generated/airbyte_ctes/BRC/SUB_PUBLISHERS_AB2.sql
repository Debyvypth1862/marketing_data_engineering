{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(SUBP_DATE as {{ dbt_utils.type_string() }}) as SUBP_DATE,
	try_cast(SUBP_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as SUBP_FK_PUBLISHER,
	try_cast(SUBP_FK_SUBPUBLISHER as {{ dbt_utils.type_float() }}) as SUBP_FK_SUBPUBLISHER,
	try_cast(SUBP_ID as {{ dbt_utils.type_float() }}) as SUBP_ID,
	try_cast(SUBP_PERCENTAGE as {{ dbt_utils.type_float() }}) as SUBP_PERCENTAGE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('SUB_PUBLISHERS_AB1') }}
-- SUB_PUBLISHERS
where 1 = 1