{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('BRAND_OPERATOR_STG') }}
select
    
    UPDATED_AT,
    OPERATOR_ID,
    CREATED_AT,
    ID,
    BRAND_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_BRAND_OPERATOR_HASHID
from {{ ref('BRAND_OPERATOR_STG') }}
-- BRAND_OPERATOR from {{ source('BRT', '_AIRBYTE_RAW_BRAND_OPERATOR') }}
where 1 = 1


