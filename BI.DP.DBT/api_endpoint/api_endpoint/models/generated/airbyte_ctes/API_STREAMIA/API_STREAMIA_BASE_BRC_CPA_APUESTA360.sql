{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: BRC CPA Income for Apuesta360
-- Calculates CPA income based on FTD counts from Apuesta360
select
  Event_Date,
  Tier_Level,
  Advertiser_ID,
  Advertiser_Name,
  Affiliate_ID,
  Brand_Name,
  Campaign_ID,
  Campaign_Name,
  Campaign_Status,
  Campaign_Type,
  Currency,
  Country,
  Campiagn_Deal,
  Baseline_Wager,
  Baseline_Deposit,
  RevShare_Deal,
  CPA_Deal,
  FTD_Cnt,
  Case when FTD_Cnt > 0 then FTD_Cnt * CPA_Deal else 0 end as CPA_Income
from {{ ref('API_STREAMIA_BASE_BRC_FTD_APUESTA360') }}
