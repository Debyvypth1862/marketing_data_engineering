{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	{{ json_extract_scalar('_airbyte_data', ['puma_fk_admin'], ['puma_fk_admin']) }} as PUMA_FK_ADMIN,
	{{ json_extract_scalar('_airbyte_data', ['puma_fk_publisher'], ['puma_fk_publisher']) }} as PUMA_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['puma_id'], ['puma_id']) }} as PUMA_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_MANAGERS') }} as table_alias
where 1 = 1
