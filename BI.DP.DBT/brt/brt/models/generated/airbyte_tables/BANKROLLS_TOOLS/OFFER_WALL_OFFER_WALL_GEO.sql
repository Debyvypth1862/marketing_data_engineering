{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_WALL_OFFER_WALL_GEO_STG') }}
select
    
    UPDATED_AT,
    OFFER_WALL_ID,
    OFFER_WALL_GEO_ID,
    CREATED_AT,
    ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_WALL_OFFER_WALL_GEO_HASHID
from {{ ref('OFFER_WALL_OFFER_WALL_GEO_STG') }}
-- OFFER_WALL_OFFER_WALL_GEO from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_OFFER_WALL_GEO') }}
where 1 = 1


