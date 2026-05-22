{{ 
  config(
    materialized = "incremental",
    incremental_strategy = "append",
    cluster_by = ["IP4", "IP6"],
    database = "INTM",
    schema = "MAXMIND"
  ) 
}} 

WITH pre_increment_ip_addresses AS (
  SELECT DISTINCT POST_IP AS IP
  FROM {{ source('BRC', 'POSTBACK_TRACKING') }}
  WHERE
    POST_IP IS NOT NULL
    AND TRY_PARSE_IP(post_IP, 'INET'):family IN (4, 6)
  UNION DISTINCT
  -- some cases has double IP in one row, below splitting them
  SELECT DISTINCT TRIM(REPLACE(REPLACE(value, ' ', ''), '"', '')) AS IP
  FROM {{ source('BRC', 'POSTBACK_TRACKING') }}
  , LATERAL FLATTEN(input => SPLIT(POST_IP, ','))
  WHERE
    POST_IP IS NOT NULL
    AND TRY_PARSE_IP(POST_IP, 'INET'):family IS NULL -- only process rows where direct parsing fails
    AND TRY_PARSE_IP(
      TRIM(REPLACE(REPLACE(value, ' ', ''), '"', ''))
      , 'INET'
    ):family IN (4, 6) -- but individual parts are valid
-- UNION
-- SELECT DISTINCT IP_ADDRESS AS IP
-- FROM "BE_AUDIENCE_BUILDER"."PUBLIC"."DIM_CONTACT"
-- WHERE IP_ADDRESS IS NOT NULL
--     AND TRY_PARSE_IP(IP_ADDRESS, 'INET') :family IN (4, 6)
)

, cleaned_ip AS (
  SELECT
    CASE
      WHEN
        TRY_PARSE_IP(ip, 'INET'):family IS NULL
        AND TRY_PARSE_IP(SPLIT_PART(ip, ':', 1), 'INET'):family IN (4, 6) THEN SPLIT_PART(ip, ':', 1)
      ELSE ip
    END AS ip
  FROM pre_increment_ip_addresses
  WHERE
    TRY_PARSE_IP(ip, 'INET'):family IS NOT NULL
    OR TRY_PARSE_IP(SPLIT_PART(ip, ':', 1), 'INET'):family IN (4, 6)
)

SELECT
  t.IP
  , PARSE_IP(t.IP, 'INET'):family AS FAMILY
  , CAST(
    CASE
      WHEN PARSE_IP(t.IP, 'INET'):family = 4 THEN PARSE_IP(t.IP, 'INET'):ipv4
    END AS BIGINT
  ) AS IP4
  , CAST(
    CASE
      WHEN PARSE_IP(t.IP, 'INET'):family = 6 THEN PARSE_IP(t.IP, 'INET'):hex_ipv6
    END AS VARCHAR
  ) AS IP6
  , NULL AS country_name
  , NULL AS region_name
  , NULL AS city_name
  , NULL AS check_ip
FROM pre_increment_ip_addresses AS t
-- Only process new IPs in incremental runs

{% if is_incremental() %}
  WHERE t.ip NOT IN (SELECT ip FROM {{ this }})
{% endif %}
