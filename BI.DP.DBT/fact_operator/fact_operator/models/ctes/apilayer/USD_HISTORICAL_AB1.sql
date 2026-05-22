{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_AIRBYTE_APILAYER",
    tags = [ "top-level-intermediate" ]
) }}
WITH s3_status AS (
  SELECT
    PATH
    , IS_PROCESSED
    , PICKED_FOR_REPROCESS
  FROM {{ source('PUBLIC', 'S3_FILES_STATS') }}
)

, table_alias AS (
  SELECT
    qt.KEY AS DATE
    , vl.KEY AS CURRENCY
    , vl.VALUE AS RATE
    , _AIRBYTE_AB_ID
    , _AIRBYTE_EMITTED_AT
    , S3_PATH
    , {{ current_timestamp() }} AS _AIRBYTE_NORMALIZED_AT
  FROM {{ source('APILAYER', '_AIRBYTE_RAW_USD_HISTORICAL') }} AS s
  , TABLE(FLATTEN(PARSE_JSON(S._AIRBYTE_DATA), 'quotes')) AS qt
  , TABLE(FLATTEN(qt.VALUE)) AS vl
)

SELECT table_alias.*
FROM table_alias
INNER JOIN s3_status AS s3
  ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
