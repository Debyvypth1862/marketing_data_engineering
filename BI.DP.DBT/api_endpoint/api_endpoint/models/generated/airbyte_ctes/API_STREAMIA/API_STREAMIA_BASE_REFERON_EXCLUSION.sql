{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: Referon Exclusion filter
-- Identifies tracker logins that should be excluded from BRC and included in Referon processing
Select
    tracker_login_id
from {{ source('EXP_PUBLIC', 'FACT_OPERATOR_AGG') }}
where upper(PUBLISHER_NAME) like '%TIER%' and OPERATOR_PLATFORM in ('Referon')
group by all
