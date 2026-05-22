{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_WALL_COSMETICS_STG') }}
select
    
    UPDATED_AT,
    OFFER_WALL_ID,
    PARAMETER,
    CREATED_AT,
    ID,
    VALUE,
    DELETED_AT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_WALL_COSMETICS_HASHID
from {{ ref('OFFER_WALL_COSMETICS_STG') }}
-- OFFER_WALL_COSMETICS from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_COSMETICS') }}
where 1 = 1


