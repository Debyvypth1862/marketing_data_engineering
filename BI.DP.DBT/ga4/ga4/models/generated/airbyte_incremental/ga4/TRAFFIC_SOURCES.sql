{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('TRAFFIC_SOURCES_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    AVERAGE_SESSION_DURATION,
    BOUNCE_RATE,
    DATE,
    NEW_USERS,
    PROPERTY_ID,
    SCREEN_PAGE_VIEWS,
    SCREEN_PAGE_VIEWS_PER_SESSION,
    SESSION_MEDIUM,
    SESSIONS,
    SESSION_SOURCE,
    SESSIONS_PERUSER,
    TOTAL_USERS,
    UUID,
    _AIRBYTE_UNIQUE_KEY as EVENT_GENERATED_UNIQUE_KEY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_TRAFFIC_SOURCES_HASHID
from {{ ref('TRAFFIC_SOURCES_SCD') }}
-- DEVICES from {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_TRAFFIC_SOURCES') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

