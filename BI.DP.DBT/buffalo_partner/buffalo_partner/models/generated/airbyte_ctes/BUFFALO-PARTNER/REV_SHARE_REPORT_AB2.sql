{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BUFFALO_PARTNERS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('REV_SHARE_REPORT_AB1') }}
select
    try_cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(AFFILIATE_ID as {{ dbt_utils.type_string() }}) as AFFILIATE_ID,
    try_cast(BRAND as {{ dbt_utils.type_string() }}) as BRAND,
    try_cast(CAMPAIGN as {{ dbt_utils.type_string() }}) as CAMPAIGN,
    try_cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    try_cast(CURRENCY as {{ dbt_utils.type_string() }}) as CURRENCY,
    try_cast(DATE_LAST_PLAYED as {{ dbt_utils.type_string() }}) as DATE_LAST_PLAYED,
    try_cast(DATE_OPENED as {{ dbt_utils.type_string() }}) as DATE_OPENED,
    try_cast(DATE_FIRST_DEPOSITED as {{ dbt_utils.type_string() }}) as DATE_FIRST_DEPOSITED,
    try_cast(DAY as {{ dbt_utils.type_string() }}) as DAY,
    try_cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    try_cast(DEVICE as {{ dbt_utils.type_string() }}) as DEVICE,
    try_cast(EARNINGS as {{ dbt_utils.type_float() }}) as EARNINGS,
    try_cast(FIRST_DEPOSIT_AMOUNT as {{ dbt_utils.type_float() }}) as FIRST_DEPOSIT_AMOUNT,
    try_cast(GENERIC_1 as {{ dbt_utils.type_string() }}) as GENERIC_1,
    try_cast(GENERIC_2 as {{ dbt_utils.type_string() }}) as GENERIC_2,
    try_cast(GENERIC_3 as {{ dbt_utils.type_string() }}) as GENERIC_3,
    try_cast(GENERIC_4 as {{ dbt_utils.type_string() }}) as GENERIC_4,
    try_cast(GENERIC_5 as {{ dbt_utils.type_string() }}) as GENERIC_5,
    try_cast(HIGH_ROLLER_ADJUSTED as {{ dbt_utils.type_string() }}) as HIGH_ROLLER_ADJUSTED,
    try_cast(HIGH_ROLLER_ADJUSTMENT as {{ dbt_utils.type_float() }}) as HIGH_ROLLER_ADJUSTMENT,
    try_cast(IS_NEW_ACTIVE_P as {{ dbt_utils.type_string() }}) as IS_NEW_ACTIVE_P,
    try_cast(IS_PLAYER_LOCKED as {{ dbt_utils.type_string() }}) as IS_PLAYER_LOCKED,
    try_cast(MEDIA as {{ dbt_utils.type_string() }}) as MEDIA,
    try_cast(NET_REVENUE as {{ dbt_utils.type_float() }}) as NET_REVENUE,
    try_cast(NUMBER_OF_DEPOSITS as {{ dbt_utils.type_float() }}) as NUMBER_OF_DEPOSITS,
    try_cast(PLAYER_REFERENCE as {{ dbt_utils.type_string() }}) as PLAYER_REFERENCE,
    try_cast(PRODUCT as {{ dbt_utils.type_string() }}) as PRODUCT,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('REV_SHARE_REPORT_AB1') }}
-- REV_SHARE_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
