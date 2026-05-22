{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_WALL_REQUESTS_STG') }}
select
    
    REQUEST_X_FORWARDED_FOR,
    NOTES,
    OFFER_WALL_ID,
    CLOAKER_STATUS,
    REQUEST_URL_PARAMS,
    CLOAKER_CONFIGURATION_ID,
    CREATED_AT,
    MARKETING_SITE_ID,
    REQUEST_REFERER,
    REQUEST_USER_AGENT,
    UPDATED_AT,
    REQUEST_IP_ADDRESS,
    ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_WALL_REQUESTS_HASHID
from {{ ref('OFFER_WALL_REQUESTS_STG') }}
-- OFFER_WALL_REQUESTS from {{ source('BRT', '_AIRBYTE_RAW_OFFER_WALL_REQUESTS') }}
where 1 = 1


