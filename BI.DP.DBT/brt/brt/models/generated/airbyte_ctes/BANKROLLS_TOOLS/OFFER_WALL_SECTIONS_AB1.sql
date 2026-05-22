{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_SECTIONS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['footer_script'], ['footer_script']) }} as FOOTER_SCRIPT,
    {{ json_extract_scalar('_airbyte_data', ['cloaker_configuration_id'], ['cloaker_configuration_id']) }} as CLOAKER_CONFIGURATION_ID,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['marketing_site_id'], ['marketing_site_id']) }} as MARKETING_SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['params'], ['params']) }} as PARAMS,
    {{ json_extract_scalar('_airbyte_data', ['uuid'], ['uuid']) }} as UUID,
    {{ json_extract_scalar('_airbyte_data', ['created_by'], ['created_by']) }} as CREATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['deleted_at'], ['deleted_at']) }} as DELETED_AT,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['after_body_script'], ['after_body_script']) }} as AFTER_BODY_SCRIPT,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
    {{ json_extract_scalar('_airbyte_data', ['updated_by'], ['updated_by']) }} as UPDATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['header_script'], ['header_script']) }} as HEADER_SCRIPT,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_SECTIONS') }} as table_alias
-- OFFER_WALL_SECTIONS
where 1 = 1

