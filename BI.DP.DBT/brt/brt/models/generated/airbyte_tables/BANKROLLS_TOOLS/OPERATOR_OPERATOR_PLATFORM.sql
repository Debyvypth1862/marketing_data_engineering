{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OPERATOR_OPERATOR_PLATFORM_STG') }}
select
    
    UPDATED_AT,
    OPERATOR_ID,
    OPERATOR_PLATFORM_ID,
    CREATED_AT,
    ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OPERATOR_OPERATOR_PLATFORM_HASHID
from {{ ref('OPERATOR_OPERATOR_PLATFORM_STG') }}
-- OPERATOR_OPERATOR_PLATFORM from {{ source('BRT', '_AIRBYTE_RAW_OPERATOR_OPERATOR_PLATFORM') }}
where 1 = 1


