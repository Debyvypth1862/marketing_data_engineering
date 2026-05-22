{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_ACCOUNTS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['developer_token'], ['developer_token']) }} as DEVELOPER_TOKEN,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['remote_desktop_id'], ['remote_desktop_id']) }} as REMOTE_DESKTOP_ID,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as TITLE,
    {{ json_extract_scalar('_airbyte_data', ['created_by'], ['created_by']) }} as CREATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['deleted_at'], ['deleted_at']) }} as DELETED_AT,
    {{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
    {{ json_extract_scalar('_airbyte_data', ['client_id'], ['client_id']) }} as CLIENT_ID,
    {{ json_extract_scalar('_airbyte_data', ['access_token'], ['access_token']) }} as ACCESS_TOKEN,
    {{ json_extract_scalar('_airbyte_data', ['refresh_token'], ['refresh_token']) }} as REFRESH_TOKEN,
    {{ json_extract_scalar('_airbyte_data', ['password'], ['password']) }} as PASSWORD,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['persona_id'], ['persona_id']) }} as PERSONA_ID,
    {{ json_extract_scalar('_airbyte_data', ['updated_by'], ['updated_by']) }} as UPDATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['client_secret'], ['client_secret']) }} as CLIENT_SECRET,
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as EMAIL,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['username'], ['username']) }} as USERNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_ACCOUNTS') }} as table_alias
-- GOOGLE_ADS_ACCOUNTS
where 1 = 1

