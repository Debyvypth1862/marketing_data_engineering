{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('TRAFFIC_CHANNELS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,

    ALIAS,
    CAMPAIGN_COUNT,
    COST_ID,
    COST_LEVEL,
    COST_MODELS,
    CREATED_AT,
    CURRENCY,
    ENABLE_DIRECT_TRAFFIC,
    ENABLE_IMPRESSIONS,
    ENABLE_PARALLEL_TRACKING,
    EXTERNAL_ID,
    EXTERNAL_ID_ALIAS,
    FORMATS,
    GOOGLE_ANALYTICS_KEY,
    ID,
    IMP_COST_ID,
    IMP_ID,
    INTEGRATION_ID,
    INTEGRATION_TYPES,
    INTEGRATIONS,
    POSTBACK_PIXEL,
    POSTBACK_URL,
    PRESET_ID,
    REF_ID,
    REF_ID_ALIAS,
    SERIAL_NUMBER,
    STAT,
    STATUS,
    SUBS,
    TITLE,
    TYPE,
    UPDATED_AT,
    USER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_TRAFFIC_CHANNELS_HASHID
from {{ ref('TRAFFIC_CHANNELS_SCD') }}
-- TRAFFIC_CHANNELS from {{ source('REDTRACK', '_AIRBYTE_RAW_TRAFFIC_CHANNELS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

