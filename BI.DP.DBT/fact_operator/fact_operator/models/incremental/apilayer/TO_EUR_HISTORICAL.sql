{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "APILAYER",
    tags = [ "top-level" ]
) }}
SELECT
  _AIRBYTE_UNIQUE_KEY
  , DATE
  , SUBSTRING(CURRENCY, 4, LENGTH(CURRENCY) - 3) AS CURRENCY_SOURCE
  , SUBSTRING(CURRENCY, 1, 3) AS CURRENCY_DEST
  , 1 / RATE AS RATE
  , _AIRBYTE_AB_ID
  , _AIRBYTE_EMITTED_AT
  , {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
  , _AIRBYTE_EUR_HISTORICAL_HASHID
FROM {{ ref('EUR_HISTORICAL_SCD') }}
WHERE
  1 = 1
  AND _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
