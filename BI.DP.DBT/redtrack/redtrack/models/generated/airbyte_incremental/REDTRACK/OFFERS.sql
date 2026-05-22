{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFERS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    ACTION_SOURCE_FB,
    CAP,
    CAP_ALERT,
    CLCAP,
    CLCAP_ALERT,
    CLICK_CAP,
    CLICK_CAP_PERIOD,
    CLICK_CAP_TYPE,
    COUNTRY_CODES,
    CREATED_AT,
    DEFAULT_CONVERSION_STATUS,
    EVENT_SOURCE_URL_FB,
    EXPIRES_AT,
    FACEBOOK_PIXELS,
    FINGERPRINT_SETTINGS,
    ID,
    NETWORK_TITLE,
    NOTES,
    PAYMENT,
    POSTBACK_URL,
    PROGRAM_ID,
    SERIAL_NUMBER,
    SNAPCHAT_MATCHING,
    SNAPCHAT_PIXELS,
    STAT,
    STATUS,
    TAGS,
    TITLE,
    UPDATED_AT,
    URL,
    USER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFERS_HASHID
from {{ ref('OFFERS_SCD') }}
-- OFFERS from {{ source('REDTRACK', '_AIRBYTE_RAW_OFFERS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

