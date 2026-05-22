{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['cuda_created'], ['cuda_created']) }} as CUDA_CREATED,
        {{ json_extract_scalar('_airbyte_data', ['cuda_created_by'], ['cuda_created_by']) }} as CUDA_CREATED_BY,
        {{ json_extract_scalar('_airbyte_data', ['cuda_filename'], ['cuda_filename']) }} as CUDA_FILENAME,
        {{ json_extract_scalar('_airbyte_data', ['cuda_fk_campaign_tracker'], ['cuda_fk_campaign_tracker']) }} as CUDA_FK_CAMPAIGN_TRACKER,
        {{ json_extract_scalar('_airbyte_data', ['cuda_id'], ['cuda_id']) }} as CUDA_ID,
        {{ json_extract_scalar('_airbyte_data', ['cuda_subid'], ['cuda_subid']) }} as CUDA_SUBID,
        {{ json_extract_scalar('_airbyte_data', ['cuda_updated'], ['cuda_updated']) }} as CUDA_UPDATED,
        {{ json_extract_scalar('_airbyte_data', ['cuda_updated_by'], ['cuda_updated_by']) }} as CUDA_UPDATED_BY,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CUSTOM_TRACKER_DATA') }} as table_alias
-- CUSTOM_TRACKER_DATA
where 1 = 1
