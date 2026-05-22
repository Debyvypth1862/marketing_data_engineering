{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('API_POSTBACKS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    ALIAS,
    ATTEMPTS,
    CAMPAIGN,
    CAMPAIGN_ID,
    CONVERSION_ID,
    CREATED_AT,
    DESTINATION,
    ERROR,
    ID,
    PREVIOUS_AT,
    REF_ID,
    SOURCE,
    SOURCE_ID,
    STATUS,
    TOTAL,
    TRACK_ID,
    TYPE,
    USER_ID
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_API_POSTBACKS_HASHID
from {{ ref('API_POSTBACKS_SCD') }}
-- API_POSTBACKS from {{ source('REDTRACK', '_AIRBYTE_RAW_API_POSTBACKS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

