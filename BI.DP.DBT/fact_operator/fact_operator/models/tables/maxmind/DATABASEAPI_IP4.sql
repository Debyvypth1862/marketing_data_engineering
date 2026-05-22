{{ 
  config(
    materialized = "table",
    cluster_by = ["range_start","range_end"],
    database = "INTM",
    schema = "MAXMIND"
  ) 
}} 

SELECT
  NETWORK
  , CAST(PARSE_IP(NETWORK, 'INET'):ipv4_range_start AS BIGINT) AS range_start
  , CAST(PARSE_IP(NETWORK, 'INET'):ipv4_range_end AS BIGINT) AS range_end
  , CNRT.country_name
  , CNRT.city_name
  , CNRT.SUBDIVISION_1_NAME AS region_name
FROM {{ ref('GEOIP2_CITY_BLOCKS_IPV4') }} AS BL
INNER JOIN {{ ref('GEOIP2_CITY_LOCATIONS') }} AS CNRT
  ON bl.geoname_id = CNRT.GEONAME_id
