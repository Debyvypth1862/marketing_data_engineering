{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_AIRBYTE_APILAYER",
    tags = [ "top-level-intermediate" ]
) }}
SELECT
  CAST(DATE AS {{ dbt_utils.type_string() }}) AS DATE
  , CAST(CURRENCY AS {{ dbt_utils.type_string() }}) AS CURRENCY
  , CAST(RATE AS {{ dbt_utils.type_float() }}) AS RATE
  , _AIRBYTE_AB_ID
  , _AIRBYTE_EMITTED_AT
  , S3_PATH
  , _AIRBYTE_NORMALIZED_AT
FROM {{ ref('USD_HISTORICAL_AB1') }}
