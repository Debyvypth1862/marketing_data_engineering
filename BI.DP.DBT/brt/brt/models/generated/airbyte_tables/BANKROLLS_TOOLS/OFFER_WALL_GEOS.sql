{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_WALL_GEOS_STG') }}
select
    
    UPDATED_AT,
    USER_ID,
    NAME,
    UPDATED_BY,
    CREATED_AT,
    ID,
    UUID,
    CREATED_BY,
    DELETED_AT,
    DEFAULT_OFFER_WALL_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_WALL_GEOS_HASHID
from {{ ref('OFFER_WALL_GEOS_STG') }}
-- OFFER_WALL_GEOS from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_GEOS') }}
where 1 = 1


