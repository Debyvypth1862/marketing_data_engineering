{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('DEVICES_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    PROPERTY_ID,
    UUID,
    DATE,
    DEVICE_CATEGORY,
    OPERATING_SYSTEM,
    BROWSER,
    TOTAL_USERS,
    NEW_USERS,
    SESSIONS,
    SESSIONS_PERUSER,
    AVERAGE_SESSION_DURATION,
    SCREEN_PAGE_VIEWS,
    SCREEN_PAGE_VIEWS_PER_SESSION,
    BOUNCE_RATE,
    _AIRBYTE_UNIQUE_KEY as EVENT_GENERATED_UNIQUE_KEY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_DEVICES_HASHID
from {{ ref('DEVICES_SCD') }}
-- DEVICES from {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_DEVICES') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

