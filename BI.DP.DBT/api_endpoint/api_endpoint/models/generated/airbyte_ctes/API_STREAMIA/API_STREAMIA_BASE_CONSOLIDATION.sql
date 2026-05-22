{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: Main Consolidation with FTD Income calculation
-- Combines all source data and calculates FTD_CNT * CPA_Deal at campaign level
-- This is the shared model used by both ADVERTISER and CAMPAIGN pipelines
WITH Consolidation as
(
SELECT
Coalesce(br.EVENT_DATE, ops.EVENT_DATE, rev.EVENT_DATE, ref.EVENT_DATE, rclk.EVENT_DATE) as Date,
Coalesce(br.Tier_Level, ops.Tier_Level, rev.Tier_Level, ref.Tier_Level, rclk.Tier_Level) as Tier_Level,
Coalesce(br.Advertiser_ID, ops.Advertiser_ID, rev.Advertiser_ID, ref.Advertiser_ID, rclk.Advertiser_ID) as Advertiser_ID,
Coalesce(br.Advertiser_Name, ops.Advertiser_Name, rev.Advertiser_Name, ref.Advertiser_Name, rclk.Advertiser_Name) as Advertiser_Name,
Coalesce(br.Affiliate_ID, ops.Affiliate_ID, rev.Affiliate_ID, ref.Affiliate_ID, rclk.Affiliate_ID) as Affiliate_ID,
Coalesce(br.Brand_Name, ops.Brand_Name, rev.Brand_Name, ref.Brand_Name, rclk.Brand_Name) as Brand_Name,
Coalesce(br.Campaign_ID, ops.Campaign_ID, rev.Campaign_ID, ref.Campaign_ID, rclk.Campaign_ID) as Campaign_ID,
Coalesce(br.Campaign_Name, ops.Campaign_Name, rev.Campaign_Name, ref.Campaign_Name, rclk.Campaign_Name) as Campaign_Name,
Coalesce(br.Campaign_Status, ops.Campaign_Status, rev.Campaign_Status, ref.Campaign_Status, rclk.Campaign_Status) as Campaign_Status,
Coalesce(br.Campaign_Type, ops.Campaign_Type, rev.Campaign_Type, ref.Campaign_Type, rclk.Campaign_Type) as Campaign_Type,
Coalesce(br.Currency, ops.Currency, rev.Currency, ref.Currency, rclk.Currency) as Currency,
Coalesce(br.Country, ops.Country, rev.Country, ref.Country, rclk.Country) as Country,
Coalesce(br.Campiagn_Deal, ops.Campiagn_Deal, rev.Campiagn_Deal, ref.Campiagn_Deal, rclk.Campiagn_Deal) as Campiagn_Deal,
Coalesce(br.Baseline_Wager, ops.Baseline_Wager, rev.Baseline_Wager, ref.Baseline_Wager, rclk.Baseline_Wager) as Baseline_Wager,
Coalesce(br.Baseline_Deposit, ops.Baseline_Deposit, rev.Baseline_Deposit, ref.Baseline_Deposit, rclk.Baseline_Deposit) as Baseline_Deposit,
Coalesce(br.RevShare_Deal, ops.RevShare_Deal, rev.RevShare_Deal, ref.RevShare_Deal, rclk.RevShare_Deal) as RevShare_Deal,
Coalesce(br.CPA_Deal, ops.CPA_Deal, rev.CPA_Deal, ref.CPA_Deal, rclk.CPA_Deal) as CPA_Deal,
SUM(IFNULL(br.CLICK_CNT,0) + IFNULL(rclk.CLICK_CNT,0)) AS CLICK_CNT,
SUM(IFNULL(rclk.Unique_Clicks,0)) as Unique_Clicks,
SUM(IFNULL(br.Reg_Cnt,0) + IFNULL(ops.Reg_Cnt,0)) AS SIGNUP_CNT,
SUM(IFNULL(br.FTD_CNT,0) + IFNULL(ops.FTD_CNT,0)) AS FTD_CNT,
SUM(IFNULL(br.FTD_CNT,0) + IFNULL(rev.CPA_CNT,0) + IFNULL(ref.CPA_CNT,0)) AS CPA_CNT,
SUM(IFNULL(rev.DEPOSIT_CNT,0) + IFNULL(ref.DEPOSIT_CNT,0)) AS DEPOSIT_CNT,
SUM(IFNULL(ops.FTD_AMT,0)) AS FTD_AMT,
SUM(IFNULL(rev.DEPOSIT_AMT,0) + IFNULL(ref.DEPOSIT_AMT,0)) AS DEPOSIT_AMT,
SUM(IFNULL(rev.NET_DEPOSIT_AMT,0) + IFNULL(ref.NET_DEPOSIT_AMT,0)) AS NET_DEPOSIT_AMT,
SUM(IFNULL(rev.NET_REVENUE_AMT,0) + IFNULL(ref.NET_REVENUE_AMT,0)) AS NET_REVENUE_AMT,
SUM(IFNULL(br.CPA_INCOME_AMT,0) + IFNULL(rev.CPA_INCOME_AMT,0) + IFNULL(ref.CPA_INCOME_AMT,0)) AS CPA_INCOME_AMT,
SUM(IFNULL(rev.Revshare_Payment,0) + IFNULL(ref.Revshare_Payment,0)) AS REVSHARE_INCOME_AMT
FROM {{ ref('API_STREAMIA_BASE_BRC_CONVERSIONS') }} br
full outer join {{ ref('API_STREAMIA_BASE_REFERON_CLICKS') }} rclk
  on br.EVENT_DATE = rclk.EVENT_DATE and br.Campaign_ID = rclk.Campaign_ID and br.Affiliate_ID = rclk.Affiliate_ID
full outer join {{ ref('API_STREAMIA_BASE_OPS_CONSOLIDATION') }} ops
     on br.EVENT_DATE = ops.EVENT_DATE and br.Campaign_ID = ops.Campaign_ID and br.Affiliate_ID = ops.Affiliate_ID
full outer join {{ ref('API_STREAMIA_BASE_REVSHARE') }} rev
     on br.EVENT_DATE = rev.EVENT_DATE and br.Campaign_ID = rev.Campaign_ID and br.Affiliate_ID = rev.Affiliate_ID
full outer join {{ ref('API_STREAMIA_BASE_REFERON_REVSHARE') }} ref
     on br.EVENT_DATE = ref.EVENT_DATE and br.Campaign_ID = ref.Campaign_ID and br.Affiliate_ID = ref.Affiliate_ID
Group by All
)

SELECT
Date,
Tier_Level,
Advertiser_ID,
Advertiser_Name,
Affiliate_ID,
Brand_Name,
Campaign_ID,
Campaign_Name,
Campaign_Type,
Campaign_Status,
Currency,
Country,
Baseline_Wager,
Baseline_Deposit,
RevShare_Deal,
CPA_Deal,
CLICK_CNT,
UNIQUE_CLICKS,
SIGNUP_CNT,
FTD_CNT,
CPA_CNT,
DEPOSIT_CNT,
FTD_AMT,
DEPOSIT_AMT,
NET_DEPOSIT_AMT,
NET_REVENUE_AMT,
FTD_CNT * CPA_Deal AS FTD_INCOME_AMT,
CPA_INCOME_AMT,
REVSHARE_INCOME_AMT
from Consolidation
