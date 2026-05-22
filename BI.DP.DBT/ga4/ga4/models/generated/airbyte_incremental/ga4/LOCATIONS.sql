{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('LOCATIONS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    AVERAGE_SESSION_DURATION,
    BOUNCE_RATE,
    CITY,
    COUNTRY,
    DATE,
    NEW_USERS,
    PROPERTY_ID,
    REGION,
    SCREEN_PAGE_VIEWS,
    SCREEN_PAGE_VIEWS_PER_SESSION,
    SESSIONS,
    SESSIONS_PERUSER,
    TOTAL_USERS,
    UUID,
    _AIRBYTE_UNIQUE_KEY as EVENT_GENERATED_UNIQUE_KEY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_LOCATIONS_HASHID
from {{ ref('LOCATIONS_SCD') }}
-- LOCATIONS from {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_LOCATIONS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

