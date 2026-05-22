{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "APILAYER",
    on_schema_change = "sync_all_columns",
    tags = [ "top-level" ]
) }}
-- EUR to USD conversion rates
-- Source: EUR_HISTORICAL (contains EURUSD rates = how many USD per 1 EUR)
-- Output: Direct rate (no inversion needed)
SELECT
  _AIRBYTE_UNIQUE_KEY
  , DATE
  , CURRENCY_SOURCE                       -- EUR
  , CURRENCY_DEST                         -- USD
  , RATE                                  -- Direct: EUR → USD
  , _AIRBYTE_AB_ID
  , _AIRBYTE_EMITTED_AT
  , {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
  , _AIRBYTE_EUR_HISTORICAL_HASHID
FROM {{ ref('EUR_HISTORICAL') }}
WHERE
  1 = 1
  AND CURRENCY_DEST = 'USD'
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
