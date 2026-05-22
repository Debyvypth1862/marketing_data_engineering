{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['block_id'], ['block_id']) }} as BLOCK_ID,
        {{ json_extract_scalar('_airbyte_data', ['block_ip'], ['block_ip']) }} as BLOCK_IP,
        {{ json_extract_scalar('_airbyte_data', ['block_timestamp'], ['block_timestamp']) }} as BLOCK_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_IP_BLOCK') }} as table_alias
-- IP_BLOCK
where 1 = 1
