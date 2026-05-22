{{ 
  config(
    materialized = "table",
    cluster_by = ["range_start","range_end"],
    database = "INTM",
    schema = "MAXMIND"
  ) 
}}
SELECT
  CAST(
    PARSE_IP(NETWORK, 'INET'):hex_ipv6_range_start AS VARCHAR
  ) AS range_start
  , CAST(
    PARSE_IP(NETWORK, 'INET'):hex_ipv6_range_end AS VARCHAR
  ) AS range_end
  , NETWORK
  , CNRT.country_name
  , CNRT.city_name
  , CNRT.SUBDIVISION_1_NAME AS region_name
FROM {{ ref('GEOIP2_CITY_BLOCKS_IPV6') }} AS BL
INNER JOIN {{ ref('GEOIP2_CITY_LOCATIONS') }} AS CNRT
  ON bl.geoname_id = CNRT.GEONAME_id
