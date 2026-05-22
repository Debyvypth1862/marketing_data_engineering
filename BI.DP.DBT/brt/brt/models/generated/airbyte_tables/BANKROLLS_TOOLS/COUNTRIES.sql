{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('COUNTRIES_STG') }}
select
    
    UPDATED_AT,
    NAME,
    UPDATED_BY,
    CREATED_AT,
    ID,
    ABBREV,
    CREATED_BY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_COUNTRIES_HASHID
from {{ ref('COUNTRIES_STG') }}
-- COUNTRIES from {{ source('BRT', '_AIRBYTE_RAW_COUNTRIES') }}
where 1 = 1


