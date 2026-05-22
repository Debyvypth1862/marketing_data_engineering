{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OPERATOR_PLATFORMS_STG') }}
select
    
    NOTE,
    POSTBACK,
    URL_LOGO,
    UPDATED_AT,
    NAME,
    CREATED_AT,
    HAS_API,
    ID,
    API_DOCUMENTATION_URL,
    URL,
    HAS_PLAYER_LEVEL_DATA,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OPERATOR_PLATFORMS_HASHID
from {{ ref('OPERATOR_PLATFORMS_STG') }}
-- OPERATOR_PLATFORMS from {{ source('BRT', '_AIRBYTE_RAW_OPERATOR_PLATFORMS') }}
where 1 = 1


