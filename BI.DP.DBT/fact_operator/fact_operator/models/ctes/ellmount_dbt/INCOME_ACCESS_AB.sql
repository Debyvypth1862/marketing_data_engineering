{{ config( 
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
SELECT
  CAST(SIGNUP_DATE AS DATE) AS SIGNUP_DATE
  , CAST(FTD_DATE AS DATE) AS FTD_DATE
  , CAST(FTD_DATE_AGG AS DATE) AS FTD_DATE_AGG
  , CAST(DATE AS DATE) AS DATE
  , CAST(BRAND_NAME AS {{ dbt_utils.type_string() }}) AS BRAND_NAME
  , CAST(PLAYER_IPADDRESS AS {{ dbt_utils.type_string() }}) AS PLAYER_IPADDRESS
  , CAST(CLICKID AS {{ dbt_utils.type_string() }}) AS CLICKID
  , CAST(CLICK_CNT AS {{ dbt_utils.type_int() }}) AS CLICK_CNT
  , CAST(DEPOSIT_AMT AS {{ dbt_utils.type_float() }}) AS DEPOSIT_AMT
  , CAST(DEPOSIT_CNT AS {{ dbt_utils.type_int() }}) AS DEPOSIT_CNT
  , CAST(FTD_AMT AS {{ dbt_utils.type_float() }}) AS FTD_AMT
  , CAST(FTD_CNT AS {{ dbt_utils.type_int() }}) AS FTD_CNT
  , CAST(NET_DEPOSIT_AMT AS {{ dbt_utils.type_float() }}) AS NET_DEPOSIT_AMT
  , CAST(NET_REVENUE_AMT AS {{ dbt_utils.type_float() }}) AS NET_REVENUE_AMT
  , CAST(SIGNUP_CNT AS {{ dbt_utils.type_int() }}) AS SIGNUP_CNT
  , CAST(WITHDRAWAL_AMT AS {{ dbt_utils.type_float() }}) AS WITHDRAWAL_AMT
  , CAST(COMMISSION_AMT AS {{ dbt_utils.type_float() }}) AS COMMISSION_AMT
  , CAST(COUNTRY AS {{ dbt_utils.type_string() }}) AS COUNTRY
  , CAST(TRACKER_LOGIN_ID AS {{ dbt_utils.type_string() }}) AS TRACKER_LOGIN_ID
  , CAST(TRACKER_USERNAME AS {{ dbt_utils.type_string() }}) AS TRACKER_USERNAME
  , CAST(ADVERTISER_ID AS {{ dbt_utils.type_int() }}) AS ADVERTISER_ID
  , CAST(ADVERTISER_NAME AS {{ dbt_utils.type_string() }}) AS ADVERTISER_NAME
  , CAST(PUBLISHER_NAME AS {{ dbt_utils.type_string() }}) AS PUBLISHER_NAME
  , CAST(OPERATOR_PLATFORM AS {{ dbt_utils.type_string() }}) AS OPERATOR_PLATFORM
  , CAST(SOURCE_CURRENCY AS {{ dbt_utils.type_string() }}) AS SOURCE_CURRENCY
  , {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
  , _AIRBYTE_EMITTED_AT
FROM
(
  {{ ref('INCOME_ACCESS') }}

)
