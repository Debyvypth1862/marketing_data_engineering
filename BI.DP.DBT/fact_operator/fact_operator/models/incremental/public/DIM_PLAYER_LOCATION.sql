{{ 
  config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key='IP',
    cluster_by = ["IP"],
    database = "EXP",
    schema = "PUBLIC"
  ) 
}} 

WITH
increment_ip_addresses AS (
  SELECT * FROM {{ ref('INCREMENT_IP_ADDRESSES') }}
  {% if is_incremental() %} 
    WHERE ip NOT IN (SELECT ip FROM {{ this }})
  {% endif %}
)

, ipv4_data AS (
  SELECT
    LOC.ip
    , LOC.family
    , LOC.ip4
    , LOC.ip6
    , upd.city_name
    , upd.country_name
    , upd.region_name
    , CASE WHEN upd.country_name IS NULL THEN 'no country' ELSE 'normal' END AS check_ip
  FROM increment_ip_addresses AS LOC
  LEFT JOIN {{ ref('DATABASEAPI_IP4') }} AS upd
    ON
      LOC.ip4 BETWEEN upd.range_start AND upd.range_end
  WHERE LOC.family = 4
)

, ipv6_data AS (
  SELECT
    LOC.ip
    , LOC.family
    , LOC.ip4
    , LOC.ip6
    , upd.city_name
    , upd.country_name
    , upd.region_name
    , CASE WHEN upd.country_name IS NULL THEN 'no country' ELSE 'normal' END AS check_ip
  FROM increment_ip_addresses AS LOC
  LEFT JOIN {{ ref('DATABASEAPI_IP6') }} AS upd
    ON
      LOC.ip6 BETWEEN upd.range_start AND upd.range_end
  WHERE LOC.family = 6
)

, ip_updated AS (
  SELECT * FROM ipv4_data
  UNION ALL
  SELECT * FROM ipv6_data
)

,no_result_ip AS (
  SELECT * FROM ip_updated
  WHERE check_ip IS NULL
)

, webapi_service_result AS (
  -- Fetch geolocation for remaining IPs using external function
  SELECT
    ips.ip
    , ips.city_name
    , ips.region_name
    , ips.country_name
    , CASE
      WHEN ips.COMMENT LIKE '%(private%' THEN 'private'
      WHEN ips.COMMENT LIKE '%not in our database%' THEN 'no data'
      WHEN ips.country_name IS NULL THEN 'no country'
      WHEN ips.COMMENT IS NULL THEN 'normal'
      ELSE ips.COMMENT
    END AS check_ip
  FROM no_result_ip AS t
  , TABLE(EXP.PUBLIC.GET_IP(t.ip)) AS ips
)

SELECT
  ip
  , city_name
  , region_name
  , country_name
  , check_ip
FROM ip_updated
WHERE check_ip IS NOT NULL
UNION DISTINCT
SELECT
  ip
  , city_name
  , region_name
  , country_name
  , check_ip
FROM webapi_service_result
