{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['prom_banner'], ['prom_banner']) }} as PROM_BANNER,
	{{ json_extract_scalar('_airbyte_data', ['prom_fk_advertisers'], ['prom_fk_advertisers']) }} as PROM_FK_ADVERTISERS,
	{{ json_extract_scalar('_airbyte_data', ['prom_id'], ['prom_id']) }} as PROM_ID,
	{{ json_extract_scalar('_airbyte_data', ['prom_main_comp'], ['prom_main_comp']) }} as PROM_MAIN_COMP,
	{{ json_extract_scalar('_airbyte_data', ['prom_month'], ['prom_month']) }} as PROM_MONTH,
	{{ json_extract_scalar('_airbyte_data', ['prom_name'], ['prom_name']) }} as PROM_NAME,
	{{ json_extract_scalar('_airbyte_data', ['prom_position'], ['prom_position']) }} as PROM_POSITION,
	{{ json_extract_scalar('_airbyte_data', ['prom_text'], ['prom_text']) }} as PROM_TEXT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PROMOTIONS') }} as table_alias
where 1 = 1
