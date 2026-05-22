{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['imst_id'], ['imst_id']) }} as IMST_ID,
        {{ json_extract_scalar('_airbyte_data', ['imst_name'], ['imst_name']) }} as IMST_NAME,
        {{ json_extract_scalar('_airbyte_data', ['imst_output'], ['imst_output']) }} as IMST_OUTPUT,
        {{ json_extract_scalar('_airbyte_data', ['imst_timestamp'], ['imst_timestamp']) }} as IMST_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_IMPORT_STATUS') }} as table_alias
-- IMPORT_STATUS
where 1 = 1
