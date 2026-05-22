{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "APILAYER",
    on_schema_change = "sync_all_columns",
    tags = [ "top-level" ]
) }}
-- BRL to USD conversion rates
-- Source: USD_HISTORICAL_SCD (contains USDBRL rates = how many BRL per 1 USD)
-- Output: Inverted to get how many USD per 1 BRL
SELECT
  _AIRBYTE_UNIQUE_KEY
  , DATE
  , SUBSTRING(CURRENCY, 4, LENGTH(CURRENCY) - 3) AS CURRENCY_SOURCE  -- BRL
  , SUBSTRING(CURRENCY, 1, 3) AS CURRENCY_DEST                       -- USD
  , 1 / RATE AS RATE                                                  -- Inverted: BRL → USD
  , _AIRBYTE_AB_ID
  , _AIRBYTE_EMITTED_AT
  , {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
  , _AIRBYTE_USD_HISTORICAL_HASHID
FROM {{ ref('USD_HISTORICAL_SCD') }}
WHERE
  1 = 1
  AND _AIRBYTE_ACTIVE_ROW = 1
  AND SUBSTRING(CURRENCY, 4, LENGTH(CURRENCY) - 3) = 'BRL'
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
