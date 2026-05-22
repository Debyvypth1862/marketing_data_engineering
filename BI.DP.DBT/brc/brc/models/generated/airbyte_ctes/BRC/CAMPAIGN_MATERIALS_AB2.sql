{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(CAMA_CREATED as {{ dbt_utils.type_string() }}) as CAMA_CREATED,
	try_cast(CAMA_CREATED_BY as {{ dbt_utils.type_float() }}) as CAMA_CREATED_BY,
	try_cast(CAMA_DELETED as {{ dbt_utils.type_float() }}) as CAMA_DELETED,
	try_cast(CAMA_FILE_HEIGHT as {{ dbt_utils.type_float() }}) as CAMA_FILE_HEIGHT,
	try_cast(CAMA_FILE_WIDTH as {{ dbt_utils.type_float() }}) as CAMA_FILE_WIDTH,
	try_cast(CAMA_FILENAME as {{ dbt_utils.type_string() }}) as CAMA_FILENAME,
	try_cast(CAMA_FILETYPE as {{ dbt_utils.type_string() }}) as CAMA_FILETYPE,
	try_cast(CAMA_FK_CAMPAIGN as {{ dbt_utils.type_float() }}) as CAMA_FK_CAMPAIGN,
	try_cast(CAMA_HIDDEN as {{ dbt_utils.type_float() }}) as CAMA_HIDDEN,
	try_cast(CAMA_ID as {{ dbt_utils.type_float() }}) as CAMA_ID,
	try_cast(CAMA_LANG as {{ dbt_utils.type_string() }}) as CAMA_LANG,
	try_cast(CAMA_NAME as {{ dbt_utils.type_string() }}) as CAMA_NAME,
	try_cast(CAMA_TEXT_HEADLINE as {{ dbt_utils.type_string() }}) as CAMA_TEXT_HEADLINE,
	try_cast(CAMA_TEXT_TEXT as {{ dbt_utils.type_string() }}) as CAMA_TEXT_TEXT,
	try_cast(CAMA_TYPE as {{ dbt_utils.type_string() }}) as CAMA_TYPE,
	try_cast(CAMA_UPDATED as {{ dbt_utils.type_string() }}) as CAMA_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_MATERIALS_AB1') }}
-- CAMPAIGN_MATERIALS
where 1 = 1