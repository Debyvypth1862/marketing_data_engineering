{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFERS_AB1') }}
select

    try_cast(ACTION_SOURCE_FB as {{ dbt_utils.type_string() }}) as ACTION_SOURCE_FB,
    try_cast(CAP as {{ dbt_utils.type_string() }}) as CAP,
    try_cast(CAP_ALERT as {{ dbt_utils.type_string() }}) as CAP_ALERT,
    try_cast(CLCAP as {{ dbt_utils.type_string() }}) as CLCAP,
    try_cast(CLCAP_ALERT as {{ dbt_utils.type_string() }}) as CLCAP_ALERT,
    try_cast(CLICK_CAP as {{ dbt_utils.type_string() }}) as CLICK_CAP,
    try_cast(CLICK_CAP_PERIOD as {{ dbt_utils.type_string() }}) as CLICK_CAP_PERIOD,
    try_cast(CLICK_CAP_TYPE as {{ dbt_utils.type_string() }}) as CLICK_CAP_TYPE,
    try_cast(COUNTRY_CODES as {{ dbt_utils.type_string() }}) as COUNTRY_CODES,
    try_cast(CREATED_AT as {{ dbt_utils.type_string() }}) as CREATED_AT,
    try_cast(DEFAULT_CONVERSION_STATUS as {{ dbt_utils.type_string() }}) as DEFAULT_CONVERSION_STATUS,
    try_cast(EVENT_SOURCE_URL_FB as {{ dbt_utils.type_string() }}) as EVENT_SOURCE_URL_FB,
    try_cast(EXPIRES_AT as {{ dbt_utils.type_string() }}) as EXPIRES_AT,
    try_cast(FACEBOOK_PIXELS as {{ dbt_utils.type_string() }}) as FACEBOOK_PIXELS,
    try_cast(FINGERPRINT_SETTINGS as {{ dbt_utils.type_string() }}) as FINGERPRINT_SETTINGS,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(NETWORK_TITLE as {{ dbt_utils.type_string() }}) as NETWORK_TITLE,
    try_cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    try_cast(PAYMENT as {{ dbt_utils.type_string() }}) as PAYMENT,
    try_cast(POSTBACK_URL as {{ dbt_utils.type_string() }}) as POSTBACK_URL,
    try_cast(PROGRAM_ID as {{ dbt_utils.type_string() }}) as PROGRAM_ID,
    try_cast(SERIAL_NUMBER as {{ dbt_utils.type_string() }}) as SERIAL_NUMBER,
    try_cast(SNAPCHAT_MATCHING as {{ dbt_utils.type_string() }}) as SNAPCHAT_MATCHING,
    try_cast(SNAPCHAT_PIXELS as {{ dbt_utils.type_string() }}) as SNAPCHAT_PIXELS,
    try_cast(STAT as {{ dbt_utils.type_string() }}) as STAT,
    try_cast(STATUS as {{ dbt_utils.type_string() }}) as STATUS,
    try_cast(TAGS as {{ dbt_utils.type_string() }}) as TAGS,
    try_cast(TITLE as {{ dbt_utils.type_string() }}) as TITLE,
    try_cast(UPDATED_AT as {{ dbt_utils.type_string() }}) as UPDATED_AT,
    try_cast(URL as {{ dbt_utils.type_string() }}) as URL,
    try_cast(USER_ID as {{ dbt_utils.type_string() }}) as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFERS_AB1') }}
-- OFFERS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

