{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PUBLISHER_KEYS_STG') }}
select
    
    UPDATED_AT,
    USER_ID,
    NAME,
    UPDATED_BY,
    CREATED_AT,
    ID,
    TYPE,
    CREATED_BY,
    SLUG,
    REMARKS,
    TOKEN,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_PUBLISHER_KEYS_HASHID
from {{ ref('PUBLISHER_KEYS_STG') }}
-- PUBLISHER_KEYS from {{ source('BRT', '_AIRBYTE_RAW_PUBLISHER_KEYS') }}
where 1 = 1


