{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "NETREFER",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('DYNAMIC_VARIABLES_REPORT_AB1') }}
select
    CAST(CASE 
            WHEN TRY_TO_DATE(DATE, 'YYYY-MM-DD') IS NOT NULL 
                THEN TRY_TO_DATE(DATE, 'YYYY-MM-DD')
            WHEN TRY_TO_DATE(DATE, 'DD-MM-YYYY') IS NOT NULL 
                THEN TRY_TO_DATE(DATE, 'DD-MM-YYYY')
            ELSE NULL
            END AS {{ dbt_utils.type_string() }}) as DATE,
    try_cast(AFFILIATES_ID as {{ dbt_utils.type_float() }}) as AFFILIATES_ID,
    try_cast(MARKETING_SOURCE_ID as {{ dbt_utils.type_float() }}) as MARKETING_SOURCE_ID,
    try_cast(MARKETING_SOURCE_NAME as {{ dbt_utils.type_string() }}) as MARKETING_SOURCE_NAME,
    try_cast(MEDIA_ID as {{ dbt_utils.type_float() }}) as MEDIA_ID,
    try_cast(ACTIVE_CUSTOMERS as {{ dbt_utils.type_float() }}) as ACTIVE_CUSTOMERS,
    try_cast(CLICKS as {{ dbt_utils.type_float() }}) as CLICKS,
    try_cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    try_cast(DEPOSITING_CUSTOMERS as {{ dbt_utils.type_float() }}) as DEPOSITING_CUSTOMERS,
    try_cast(FIRST_TIME_ACTIVE_CUSTOMERS as {{ dbt_utils.type_float() }}) as FIRST_TIME_ACTIVE_CUSTOMERS,
    try_cast(FIRST_TIME_DEPOSITING_CUSTOMER as {{ dbt_utils.type_float() }}) as FIRST_TIME_DEPOSITING_CUSTOMER,
    try_cast(NET_REVENUE as {{ dbt_utils.type_float() }}) as NET_REVENUE,
    try_cast(NEW_ACTIVE_CUSTOMERS as {{ dbt_utils.type_float() }}) as NEW_ACTIVE_CUSTOMERS,
    try_cast(NEW_DEPOSITING_CUSTOMERS as {{ dbt_utils.type_float() }}) as NEW_DEPOSITING_CUSTOMERS,
    try_cast(SIGNUPS as {{ dbt_utils.type_float() }}) as SIGNUPS,
    try_cast(UNIQUE_CLICKS as {{ dbt_utils.type_float() }}) as UNIQUE_CLICKS,
    try_cast(CLICK_ID as {{ dbt_utils.type_string() }}) as CLICK_ID,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_float() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('DYNAMIC_VARIABLES_REPORT_AB1') }}
-- DYNAMIC_VARIABLES_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
-- AND CLICK_ID IS NOT NULL
