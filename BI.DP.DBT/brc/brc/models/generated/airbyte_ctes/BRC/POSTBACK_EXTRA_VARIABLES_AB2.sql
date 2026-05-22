{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select

	try_cast(poev_clickid as {{ dbt_utils.type_string() }})  as poev_clickid,
	try_cast(poev_msclkid as {{ dbt_utils.type_string() }}) as poev_msclkid,
	try_cast(poev_mst_contact_id as {{ dbt_utils.type_string() }})  as poev_mst_contact_id,
	try_cast(poev_id as {{ dbt_utils.type_float() }}) as poev_id,
	try_cast(poev_tabclid as {{ dbt_utils.type_string() }}) as poev_tabclid,
	try_cast(poev_twclid as {{ dbt_utils.type_string() }})  as poev_twclid,
	try_cast(poev_variable5 as {{ dbt_utils.type_string() }}) as poev_variable5,
	try_cast(poev_variable6 as {{ dbt_utils.type_string() }})as poev_variable6,
	try_cast(poev_variable7 as {{ dbt_utils.type_string() }}) as poev_variable7,
	try_cast(poev_variable8 as {{ dbt_utils.type_string() }}) as poev_variable8,
	try_cast(poev_variable9 as {{ dbt_utils.type_string() }}) as poev_variable9,
	try_cast(poev_variable10 as {{ dbt_utils.type_string() }}) as poev_variable10,
	try_cast(poev_variable11 as {{ dbt_utils.type_string() }}) as poev_variable11,
	try_cast(poev_variable12 as {{ dbt_utils.type_string() }}) as poev_variable12,
	try_cast(poev_variable13 as {{ dbt_utils.type_string() }}) as poev_variable13,
	try_cast(poev_variable14 as {{ dbt_utils.type_string() }}) as poev_variable14,
	try_cast(poev_variable15 as {{ dbt_utils.type_string() }}) as poev_variable15,
	try_cast(poev_variable16 as {{ dbt_utils.type_string() }}) as poev_variable16,
	try_cast(poev_variable17 as {{ dbt_utils.type_string() }}) as poev_variable17,
	try_cast(poev_variable18 as {{ dbt_utils.type_string() }}) as poev_variable18,
	try_cast(poev_variable19 as {{ dbt_utils.type_string() }}) as poev_variable19,
	try_cast(poev_variable20 as {{ dbt_utils.type_string() }}) as poev_variable20,
	
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_EXTRA_VARIABLES_AB1') }}
-- POSTBACK_EXTRA_VARIABLES
where 1 = 1