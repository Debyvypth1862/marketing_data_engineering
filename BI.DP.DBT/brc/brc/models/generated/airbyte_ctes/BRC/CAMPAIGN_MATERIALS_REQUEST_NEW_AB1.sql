{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['care_date'], ['care_date']) }} as CARE_DATE,
	{{ json_extract_scalar('_airbyte_data', ['care_fk_cama_id'], ['care_fk_cama_id']) }} as CARE_FK_CAMA_ID,
	{{ json_extract_scalar('_airbyte_data', ['care_fk_publisher'], ['care_fk_publisher']) }} as CARE_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['care_id'], ['care_id']) }} as CARE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS_REQUEST_NEW') }} as table_alias
-- CAMPAIGN_MATERIALS_REQUEST_NEW
where 1 = 1
