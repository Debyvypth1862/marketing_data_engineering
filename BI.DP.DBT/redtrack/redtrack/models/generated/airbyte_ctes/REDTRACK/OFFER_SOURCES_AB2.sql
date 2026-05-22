{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFER_SOURCES_AB1') }}
select

    try_cast(ALIAS as {{ dbt_utils.type_string() }}) as ALIAS,
    try_cast(CLICK_EXPIRATION as {{ dbt_utils.type_string() }}) as CLICK_EXPIRATION,
    try_cast(CLICKID as {{ dbt_utils.type_string() }}) as CLICKID,
    try_cast(CREATED_AT as {{ dbt_utils.type_string() }}) as CREATED_AT,
    try_cast(CURRENCY as {{ dbt_utils.type_string() }}) as CURRENCY,
    try_cast(ENABLE_IP_WHITELIST as {{ dbt_utils.type_string() }}) as ENABLE_IP_WHITELIST,
    try_cast(EVENT_TRACKING as {{ dbt_utils.type_string() }}) as EVENT_TRACKING,
    try_cast(HINTS as {{ dbt_utils.type_string() }}) as HINTS,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(IP_WHITELIST as {{ dbt_utils.type_string() }}) as IP_WHITELIST,
    try_cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    try_cast(OFFER_COUNT as {{ dbt_utils.type_string() }}) as OFFER_COUNT,
    try_cast(OFFER_URL as {{ dbt_utils.type_string() }}) as OFFER_URL,
    try_cast(POSTBACK_MODE as {{ dbt_utils.type_string() }}) as POSTBACK_MODE,
    try_cast(POSTBACK_PROTECTED as {{ dbt_utils.type_string() }}) as POSTBACK_PROTECTED,
    try_cast(POSTBACK_STATUS as {{ dbt_utils.type_string() }}) as POSTBACK_STATUS,
    try_cast(POSTBACK_TOKEN as {{ dbt_utils.type_string() }}) as POSTBACK_TOKEN,
    try_cast(POSTBACK_URL as {{ dbt_utils.type_string() }}) as POSTBACK_URL,
    try_cast(PRESET_ID as {{ dbt_utils.type_string() }}) as PRESET_ID,
    try_cast(SERIAL_NUMBER as {{ dbt_utils.type_string() }}) as SERIAL_NUMBER,
    try_cast(STAT as {{ dbt_utils.type_string() }}) as STAT,
    try_cast(STATUS as {{ dbt_utils.type_string() }}) as STATUS,
    try_cast(SUB1 as {{ dbt_utils.type_string() }}) as SUB1,
    try_cast(SUB10 as {{ dbt_utils.type_string() }}) as SUB10,
    try_cast(SUB11 as {{ dbt_utils.type_string() }}) as SUB11,
    try_cast(SUB12 as {{ dbt_utils.type_string() }}) as SUB12,
    try_cast(SUB13 as {{ dbt_utils.type_string() }}) as SUB13,
    try_cast(SUB14 as {{ dbt_utils.type_string() }}) as SUB14,
    try_cast(SUB15 as {{ dbt_utils.type_string() }}) as SUB15,
    try_cast(SUB16 as {{ dbt_utils.type_string() }}) as SUB16,
    try_cast(SUB17 as {{ dbt_utils.type_string() }}) as SUB17,
    try_cast(SUB18 as {{ dbt_utils.type_string() }}) as SUB18,
    try_cast(SUB19 as {{ dbt_utils.type_string() }}) as SUB19,
    try_cast(SUB2 as {{ dbt_utils.type_string() }}) as SUB2,
    try_cast(SUB20 as {{ dbt_utils.type_string() }}) as SUB20,
    try_cast(SUB3 as {{ dbt_utils.type_string() }}) as SUB3,
    try_cast(SUB4 as {{ dbt_utils.type_string() }}) as SUB4,
    try_cast(SUB5 as {{ dbt_utils.type_string() }}) as SUB5,
    try_cast(SUB6 as {{ dbt_utils.type_string() }}) as SUB6,
    try_cast(SUB7 as {{ dbt_utils.type_string() }}) as SUB7,
    try_cast(SUB8 as {{ dbt_utils.type_string() }}) as SUB8,
    try_cast(SUB9 as {{ dbt_utils.type_string() }}) as SUB9,
    try_cast(SUBS as {{ dbt_utils.type_string() }}) as SUBS,
    try_cast(SUM as {{ dbt_utils.type_string() }}) as SUM,
    try_cast(TITLE as {{ dbt_utils.type_string() }}) as TITLE,
    try_cast(UPDATED_AT as {{ dbt_utils.type_string() }}) as UPDATED_AT,
    try_cast(USER_ID as {{ dbt_utils.type_string() }}) as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFER_SOURCES_AB1') }}
-- OFFER_SOURCES
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

