{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_OFFER_WALL_SECTION') }}
select
    {{ json_extract_scalar('_airbyte_data', ['offer_wall_section_id'], ['offer_wall_section_id']) }} as OFFER_WALL_SECTION_ID,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['offer_wall_id'], ['offer_wall_id']) }} as OFFER_WALL_ID,
    {{ json_extract_scalar('_airbyte_data', ['header'], ['header']) }} as HEADER,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['order'], ['order']) }} as {{ adapter.quote('order') }},
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_OFFER_WALL_SECTION') }} as table_alias
-- OFFER_WALL_OFFER_WALL_SECTION
where 1 = 1

