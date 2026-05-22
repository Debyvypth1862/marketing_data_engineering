{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_SOURCES_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    
    ALIAS,
    CLICK_EXPIRATION,
    CLICKID,
    CREATED_AT,
    CURRENCY,
    ENABLE_IP_WHITELIST,
    EVENT_TRACKING,
    HINTS,
    ID,
    IP_WHITELIST,
    NOTES,
    OFFER_COUNT,
    OFFER_URL,
    POSTBACK_MODE,
    POSTBACK_PROTECTED,
    POSTBACK_STATUS,
    POSTBACK_TOKEN,
    POSTBACK_URL,
    PRESET_ID,
    SERIAL_NUMBER,
    STAT,
    STATUS,
    SUB1,
    SUB10,
    SUB11,
    SUB12,
    SUB13,
    SUB14,
    SUB15,
    SUB16,
    SUB17,
    SUB18,
    SUB19,
    SUB2,
    SUB20,
    SUB3,
    SUB4,
    SUB5,
    SUB6,
    SUB7,
    SUB8,
    SUB9,
    SUBS,
    SUM,
    TITLE,
    UPDATED_AT,
    USER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_SOURCES_HASHID
from {{ ref('OFFER_SOURCES_SCD') }}
-- OFFER_SOURCES from {{ source('REDTRACK', '_AIRBYTE_RAW_OFFER_SOURCES') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

