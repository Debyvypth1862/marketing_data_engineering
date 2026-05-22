{{ config(
    materialized = "ephemeral"
) }}
-- Platform Campaign data from RedTrack and Voluum
select
    CLICKID,
    rt_campaign_id as Pl_CampaignID,
    'RedTrack' as Source_System
from {{ source('REDTRACK', 'CONVERSIONS') }}
group by all
UNION ALL
select
    CLICKID,
    CUSTOM_VARIABLE_1 as Pl_CampaignID,
    'Voluum' as Source_System
from {{ source('VOLUUM', 'CONVERSIONS') }}
group by all
