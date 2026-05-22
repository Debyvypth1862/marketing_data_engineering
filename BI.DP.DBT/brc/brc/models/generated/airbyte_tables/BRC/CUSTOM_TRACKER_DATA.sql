{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CUSTOM_TRACKER_DATA_AB2') }}
select
        CUDA_CREATED,
        CUDA_CREATED_BY,
        CUDA_FILENAME,
        CUDA_FK_CAMPAIGN_TRACKER,
        CUDA_ID,
        CUDA_SUBID,
        CUDA_UPDATED,
        CUDA_UPDATED_BY,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CUSTOM_TRACKER_DATA_AB2') }}
-- CUSTOM_TRACKER_DATA from {{ source('BRC', '_AIRBYTE_RAW_CUSTOM_TRACKER_DATA') }}
where 1 = 1