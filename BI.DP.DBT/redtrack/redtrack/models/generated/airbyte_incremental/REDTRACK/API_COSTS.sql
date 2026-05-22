{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('API_COSTS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    CAMPAIGN,
    CAMPAIGN_ID,
    COUNTRY,
    CREATED_AT,
    CURRENCY,
    ID,
    LEVEL,
    PERIOD,
    RT_AD_ID,
    RT_ADGROUP_ID,
    RT_CAMPAIGN_ID,
    RT_PLACEMENT_ID,
    SOURCE_ALIAS,
    SOURCE_COST,
    SOURCE_TIMEZONE,
    TIME_FROM,
    TIME_TO,
    USER_ID,
    USER_TIMEZONE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_API_COSTS_HASHID
from {{ ref('API_COSTS_SCD') }}
-- API_COSTS from {{ source('REDTRACK', '_AIRBYTE_RAW_API_COSTS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

