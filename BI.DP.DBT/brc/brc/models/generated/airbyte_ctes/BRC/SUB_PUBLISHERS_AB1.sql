{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['subp_date'], ['subp_date']) }} as SUBP_DATE,
	{{ json_extract_scalar('_airbyte_data', ['subp_fk_publisher'], ['subp_fk_publisher']) }} as SUBP_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['subp_fk_subpublisher'], ['subp_fk_subpublisher']) }} as SUBP_FK_SUBPUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['subp_id'], ['subp_id']) }} as SUBP_ID,
	{{ json_extract_scalar('_airbyte_data', ['subp_percentage'], ['subp_percentage']) }} as SUBP_PERCENTAGE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_SUB_PUBLISHERS') }} as table_alias
where 1 = 1
