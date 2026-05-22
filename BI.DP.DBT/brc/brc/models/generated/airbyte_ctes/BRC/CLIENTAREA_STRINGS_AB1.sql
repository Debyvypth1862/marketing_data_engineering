{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['string_en'], ['string_en']) }} as STRING_EN,
        {{ json_extract_scalar('_airbyte_data', ['string_id'], ['string_id']) }} as STRING_ID,
        {{ json_extract_scalar('_airbyte_data', ['string_name'], ['string_name']) }} as STRING_NAME,
        {{ json_extract_scalar('_airbyte_data', ['string_page'], ['string_page']) }} as STRING_PAGE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CLIENTAREA_STRINGS') }} as table_alias
-- CLIENTAREA_STRINGS
where 1 = 1
