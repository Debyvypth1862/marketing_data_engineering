{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('DAILY_ACTIVE_USERS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    ACTIVE1DAYUSERS,
    PROPERTY_ID,
    UUID,
    DATE,
    _AIRBYTE_UNIQUE_KEY as EVENT_GENERATED_UNIQUE_KEY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_DAILY_ACTIVE_USERS_HASHID
from {{ ref('DAILY_ACTIVE_USERS_SCD') }}
-- DAILY_ACTIVE_USERS from {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_DAILY_ACTIVE_USERS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

