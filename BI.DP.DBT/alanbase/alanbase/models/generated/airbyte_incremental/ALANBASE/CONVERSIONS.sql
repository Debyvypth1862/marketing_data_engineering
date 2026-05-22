{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "ALANBASE",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CONVERSIONS_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    CONVERSION_ID,
    STATUS,
    CONVERSION_DATETIME,
    PAYMENT_MODEL,
    PAYOUT,
    PAYOUT_CURRENCY,
    SUB1,
    SUB2,
    EDITED_BY_MANAGER,
    CLICK_ID,
    CLICK_DATETIME,
    CLICK_REDIRECT_URL,
    CLICK_IP,
    BROWSER,
    OS,
    DEVICE_TYPE,
    COUNTRY,
    REFERER,
    CONDITION_ID,
    IS_QUALIFICATION,
    USER_AGENT,
    LANDING_ID,
    GOAL,
    OFFER_ID,
    OFFER_NAME,
    OFFER_TAGS,
    PARTNER_ID,
    PARTNER_EMAIL,
    DATE,
    TRACKER_LOGIN_ID,
    DECLINE_REASON,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_CONVERSIONS_HASHID
from {{ ref('CONVERSIONS_SCD') }}
-- CONVERSIONS from {{ source('ALANBASE', '_AIRBYTE_RAW_CONVERSIONS') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

