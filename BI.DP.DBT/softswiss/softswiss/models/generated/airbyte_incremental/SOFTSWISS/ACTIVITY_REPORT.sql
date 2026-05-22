{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "SOFTSWISS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ACTIVITY_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    TRACKER_LOGIN_ID,
    START_DATE,
    END_DATE,
    DATE,
    BRAND_ID,
    CAMPAIGN_ID,
    DYNAMIC_TAG_CLICKID,
    VISITS_COUNT,
    REGISTRATIONS_COUNT,
    CURRENCY,
    NGR,
    DEPOSITS_SUM,
    DEPOSIT_COUNT,
    FIRST_DEPOSITS_COUNT,
    FIRST_DEPOSITS_SUM,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ACTIVITY_REPORT_HASHID
from {{ ref('ACTIVITY_REPORT_SCD') }}
-- ACTIVITY_REPORT from {{ source('SOFTSWISS', '_AIRBYTE_RAW_ACTIVITY_REPORT') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

