{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['caus_fk_cama_id'], ['caus_fk_cama_id']) }} as CAUS_FK_CAMA_ID,
	{{ json_extract_scalar('_airbyte_data', ['caus_fk_publisher'], ['caus_fk_publisher']) }} as CAUS_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['caus_id'], ['caus_id']) }} as CAUS_ID,
	{{ json_extract_scalar('_airbyte_data', ['caus_last_visit'], ['caus_last_visit']) }} as CAUS_LAST_VISIT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS_USED') }} as table_alias
-- CAMPAIGN_MATERIALS_USED
where 1 = 1
