{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['cama_created'], ['cama_created']) }} as CAMA_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['cama_created_by'], ['cama_created_by']) }} as CAMA_CREATED_BY,
	{{ json_extract_scalar('_airbyte_data', ['cama_deleted'], ['cama_deleted']) }} as CAMA_DELETED,
	{{ json_extract_scalar('_airbyte_data', ['cama_file_height'], ['cama_file_height']) }} as CAMA_FILE_HEIGHT,
	{{ json_extract_scalar('_airbyte_data', ['cama_file_width'], ['cama_file_width']) }} as CAMA_FILE_WIDTH,
	{{ json_extract_scalar('_airbyte_data', ['cama_filename'], ['cama_filename']) }} as CAMA_FILENAME,
	{{ json_extract_scalar('_airbyte_data', ['cama_filetype'], ['cama_filetype']) }} as CAMA_FILETYPE,
	{{ json_extract_scalar('_airbyte_data', ['cama_fk_campaign'], ['cama_fk_campaign']) }} as CAMA_FK_CAMPAIGN,
	{{ json_extract_scalar('_airbyte_data', ['cama_hidden'], ['cama_hidden']) }} as CAMA_HIDDEN,
	{{ json_extract_scalar('_airbyte_data', ['cama_id'], ['cama_id']) }} as CAMA_ID,
	{{ json_extract_scalar('_airbyte_data', ['cama_lang'], ['cama_lang']) }} as CAMA_LANG,
	{{ json_extract_scalar('_airbyte_data', ['cama_name'], ['cama_name']) }} as CAMA_NAME,
	{{ json_extract_scalar('_airbyte_data', ['cama_text_headline'], ['cama_text_headline']) }} as CAMA_TEXT_HEADLINE,
	{{ json_extract_scalar('_airbyte_data', ['cama_text_text'], ['cama_text_text']) }} as CAMA_TEXT_TEXT,
	{{ json_extract_scalar('_airbyte_data', ['cama_type'], ['cama_type']) }} as CAMA_TYPE,
	{{ json_extract_scalar('_airbyte_data', ['cama_updated'], ['cama_updated']) }} as CAMA_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS') }} as table_alias
-- CAMPAIGN_MATERIALS
where 1 = 1
