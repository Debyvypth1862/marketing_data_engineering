{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',       
    schema = "BRC",
    tags = [ "top-level-intermediate" ]  
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['curr_date'], ['curr_date']) }} as CURR_DATE,
        {{ json_extract_scalar('_airbyte_data', ['curr_id'], ['curr_id']) }} as CURR_ID,
        {{ json_extract_scalar('_airbyte_data', ['curr_month'], ['curr_month']) }} as CURR_MONTH,
        {{ json_extract_scalar('_airbyte_data', ['curr_name'], ['curr_name']) }} as CURR_NAME,
        {{ json_extract_scalar('_airbyte_data', ['curr_value'], ['curr_value']) }} as CURR_VALUE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CURRENCY') }} as table_alias
-- CURRENCY
where 1 = 1
