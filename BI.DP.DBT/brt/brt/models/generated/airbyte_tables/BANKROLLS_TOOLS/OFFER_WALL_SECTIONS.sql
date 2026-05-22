{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_WALL_SECTIONS_STG') }}
select
    
    FOOTER_SCRIPT,
    CLOAKER_CONFIGURATION_ID,
    CREATED_AT,
    MARKETING_SITE_ID,
    PARAMS,
    UUID,
    CREATED_BY,
    DELETED_AT,
    UPDATED_AT,
    AFTER_BODY_SCRIPT,
    USER_ID,
    NAME,
    UPDATED_BY,
    HEADER_SCRIPT,
    ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_WALL_SECTIONS_HASHID
from {{ ref('OFFER_WALL_SECTIONS_STG') }}
-- OFFER_WALL_SECTIONS from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_SECTIONS') }}
where 1 = 1


