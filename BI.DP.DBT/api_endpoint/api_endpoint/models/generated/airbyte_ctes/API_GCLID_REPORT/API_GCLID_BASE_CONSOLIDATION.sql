{{ config(
    materialized = "ephemeral"
) }}
-- Union all conversion types
select * from {{ ref('API_GCLID_BASE_LEAD_CONVERSIONS') }}
UNION ALL
select * from {{ ref('API_GCLID_BASE_REGISTRATION_CONVERSIONS') }}
