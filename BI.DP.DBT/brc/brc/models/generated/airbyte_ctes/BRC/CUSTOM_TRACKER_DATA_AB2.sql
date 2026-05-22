{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(CUDA_CREATED as {{ dbt_utils.type_string() }}) as CUDA_CREATED,
        try_cast(CUDA_CREATED_BY as {{ dbt_utils.type_float() }}) as CUDA_CREATED_BY,
        try_cast(CUDA_FILENAME as {{ dbt_utils.type_string() }}) as CUDA_FILENAME,
        try_cast(CUDA_FK_CAMPAIGN_TRACKER as {{ dbt_utils.type_float() }}) as CUDA_FK_CAMPAIGN_TRACKER,
        try_cast(CUDA_ID as {{ dbt_utils.type_float() }}) as CUDA_ID,
        try_cast(CUDA_SUBID as {{ dbt_utils.type_string() }}) as CUDA_SUBID,
        try_cast(CUDA_UPDATED as {{ dbt_utils.type_string() }}) as CUDA_UPDATED,
        try_cast(CUDA_UPDATED_BY as {{ dbt_utils.type_float() }}) as CUDA_UPDATED_BY,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CUSTOM_TRACKER_DATA_AB1') }}
-- CUSTOM_TRACKER_DATA
where 1 = 1