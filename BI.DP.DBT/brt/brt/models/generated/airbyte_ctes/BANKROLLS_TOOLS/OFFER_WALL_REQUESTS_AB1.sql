{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_REQUESTS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['request_x_forwarded_for'], ['request_x_forwarded_for']) }} as REQUEST_X_FORWARDED_FOR,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['offer_wall_id'], ['offer_wall_id']) }} as OFFER_WALL_ID,
    {{ json_extract_scalar('_airbyte_data', ['cloaker_status'], ['cloaker_status']) }} as CLOAKER_STATUS,
    {{ json_extract_scalar('_airbyte_data', ['request_url_params'], ['request_url_params']) }} as REQUEST_URL_PARAMS,
    {{ json_extract_scalar('_airbyte_data', ['cloaker_configuration_id'], ['cloaker_configuration_id']) }} as CLOAKER_CONFIGURATION_ID,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['marketing_site_id'], ['marketing_site_id']) }} as MARKETING_SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['request_referer'], ['request_referer']) }} as REQUEST_REFERER,
    {{ json_extract_scalar('_airbyte_data', ['request_user_agent'], ['request_user_agent']) }} as REQUEST_USER_AGENT,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['request_ip_address'], ['request_ip_address']) }} as REQUEST_IP_ADDRESS,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_REQUESTS') }} as table_alias
-- OFFER_WALL_REQUESTS
where 1 = 1

