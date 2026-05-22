{{ config(
    materialized = "table",
    cluster_by = ["GEONAME_ID"],
    database = env_var('RAW_DATABASE'),
    schema = "MAXMIND"
) }}
SELECT
  GEONAME_ID
  , LOCALE_CODE
  , CONTINENT_CODE
  , CONTINENT_NAME
  , COUNTRY_ISO_CODE
  , COUNTRY_NAME
  , IS_IN_EUROPEAN_UNION
FROM {{ source('MAXMIND', '_AIRBYTE_RAW_GEOIP2_COUNTRY') }}
