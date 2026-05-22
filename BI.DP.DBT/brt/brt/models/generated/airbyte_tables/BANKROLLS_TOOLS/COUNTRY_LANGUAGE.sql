{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('COUNTRY_LANGUAGE_STG') }}
select
    
    UPDATED_AT,
    CREATED_AT,
    ID,
    LANGUAGE_ID,
    COUNTRY_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_COUNTRY_LANGUAGE_HASHID
from {{ ref('COUNTRY_LANGUAGE_STG') }}
-- COUNTRY_LANGUAGE from {{ source('BRT', '_AIRBYTE_RAW_COUNTRY_LANGUAGE') }}
where 1 = 1


