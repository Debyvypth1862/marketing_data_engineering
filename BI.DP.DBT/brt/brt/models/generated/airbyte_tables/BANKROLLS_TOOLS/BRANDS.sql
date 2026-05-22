{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('BRANDS_STG') }}
select
    
    NOTES,
    URL_LOGO,
    UPDATED_AT,
    NAME,
    UPDATED_BY,
    CREATED_AT,
    ID,
    TYPE,
    CREATED_BY,
    URL,
    SLUG,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_BRANDS_HASHID
from {{ ref('BRANDS_STG') }}
-- BRANDS from {{ source('BRT', '_AIRBYTE_RAW_BRANDS') }}
where 1 = 1


