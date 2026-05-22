{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['poev_clickid'], ['poev_clickid']) }} as poev_clickid,
	{{ json_extract_scalar('_airbyte_data', ['poev_msclkid'], ['poev_msclkid']) }} as poev_msclkid,
	{{ json_extract_scalar('_airbyte_data', ['poev_mst_contact_id'], ['poev_mst_contact_id']) }} as poev_mst_contact_id,
	{{ json_extract_scalar('_airbyte_data', ['poev_id'], ['poev_id']) }} as poev_id,
	{{ json_extract_scalar('_airbyte_data', ['poev_tabclid'], ['poev_tabclid']) }} as poev_tabclid,
	{{ json_extract_scalar('_airbyte_data', ['poev_twclid'], ['poev_twclid']) }} as poev_twclid,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable5'], ['poev_variable5']) }} as poev_variable5,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable6'], ['poev_variable6']) }} as poev_variable6,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable7'], ['poev_variable7']) }} as poev_variable7,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable8'], ['poev_variable8']) }} as poev_variable8,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable9'], ['poev_variable9']) }} as poev_variable9,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable10'], ['poev_variable10']) }} as poev_variable10,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable11'], ['poev_variable11']) }} as poev_variable11,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable12'], ['poev_variable12']) }} as poev_variable12,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable13'], ['poev_variable13']) }} as poev_variable13,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable14'], ['poev_variable14']) }} as poev_variable14,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable15'], ['poev_variable15']) }} as poev_variable15,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable16'], ['poev_variable16']) }} as poev_variable16,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable17'], ['poev_variable17']) }} as poev_variable17,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable18'], ['poev_variable18']) }} as poev_variable18,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable19'], ['poev_variable19']) }} as poev_variable19,
	{{ json_extract_scalar('_airbyte_data', ['poev_variable20'], ['poev_variable20']) }} as poev_variable20,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_EXTRA_VARIABLES') }} as table_alias
where 1 = 1
