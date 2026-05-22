{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OPERATOR_PLATFORMS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['note'], ['note']) }} as NOTE,
    {{ json_extract_scalar('_airbyte_data', ['postback'], ['postback']) }} as POSTBACK,
    {{ json_extract_scalar('_airbyte_data', ['url_logo'], ['url_logo']) }} as URL_LOGO,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['has_api'], ['has_api']) }} as HAS_API,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['api_documentation_url'], ['api_documentation_url']) }} as API_DOCUMENTATION_URL,
    {{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
    {{ json_extract_scalar('_airbyte_data', ['has_player_level_data'], ['has_player_level_data']) }} as HAS_PLAYER_LEVEL_DATA,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OPERATOR_PLATFORMS') }} as table_alias
-- OPERATOR_PLATFORMS
where 1 = 1

