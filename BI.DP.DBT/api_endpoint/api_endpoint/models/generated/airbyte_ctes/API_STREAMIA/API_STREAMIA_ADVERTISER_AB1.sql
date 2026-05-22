{{ config(
    materialized = 'ephemeral',
    unique_key = '_AIRBYTE_AB_ID',
    schema = "API_STREAMIA",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model for API_STREAMIA_ADVERTISER AB1
-- Aggregates campaign-level data from base consolidation to advertiser level
-- Refs shared base consolidation model and joins with advertiser-level unique clicks
WITH Final AS
(
SELECT
Date,
Tier_Level,
Advertiser_ID,
Advertiser_Name,
Affiliate_ID,
sum(CLICK_CNT) as CLICK_CNT,
sum(UNIQUE_CLICKS) as UNIQUE_CLICKS,
sum(SIGNUP_CNT) as SIGNUP_CNT,
sum(FTD_CNT) as FTD_CNT,
sum(CPA_CNT) as CPA_CNT,
sum(DEPOSIT_CNT) as DEPOSIT_CNT,
sum(FTD_AMT) as FTD_AMT,
sum(DEPOSIT_AMT) as DEPOSIT_AMT,
sum(NET_DEPOSIT_AMT) as NET_DEPOSIT_AMT,
sum(NET_REVENUE_AMT) as NET_REVENUE_AMT,
sum(FTD_INCOME_AMT) as FTD_INCOME_AMT,
sum(CPA_INCOME_AMT) as CPA_INCOME_AMT,
sum(REVSHARE_INCOME_AMT) as REVSHARE_INCOME_AMT
from {{ ref('API_STREAMIA_BASE_CONSOLIDATION') }}
group by all
)

SELECT
    {{ dbt_utils.surrogate_key(["coalesce(con.Date, brc.Event_Date)", "coalesce(con.Advertiser_ID, brc.Advertiser_ID)", "coalesce(con.Affiliate_ID, brc.Affiliate_ID)"]) }} as _AIRBYTE_AB_ID,
    coalesce(con.Date, brc.Event_Date) as DATE,
    coalesce(con.Tier_Level, brc.Tier_Level) as TIER_LEVEL,
    coalesce(con.Advertiser_ID, brc.Advertiser_ID) as ADVERTISER_ID,
    ltrim(rtrim(coalesce(con.Advertiser_Name, brc.Advertiser_Name))) as ADVERTISER_NAME,
    coalesce(con.Affiliate_ID, brc.Affiliate_ID) as AFFILIATE_ID,
    sum(IFNULL(con.CLICK_CNT,0)) as CLICK_CNT,
    sum(IFNULL(brc.Unique_Clicks,0) + IFNULL(con.UNIQUE_CLICKS,0)) as UNIQUE_CLICKS,
    sum(IFNULL(con.SIGNUP_CNT,0)) as SIGNUP_CNT,
    sum(IFNULL(con.FTD_CNT,0)) as FTD_CNT,
    sum(IFNULL(con.CPA_CNT,0)) as CPA_CNT,
    sum(IFNULL(con.DEPOSIT_CNT,0)) as DEPOSIT_CNT,
    sum(IFNULL(con.FTD_AMT,0)) as FTD_AMT,
    sum(IFNULL(con.DEPOSIT_AMT,0)) as DEPOSIT_AMT,
    sum(IFNULL(con.NET_DEPOSIT_AMT,0)) as NET_DEPOSIT_AMT,
    sum(IFNULL(con.NET_REVENUE_AMT,0)) as NET_REVENUE_AMT,
    sum(IFNULL(con.FTD_INCOME_AMT,0)) AS FTD_INCOME_AMT,
    sum(IFNULL(con.CPA_INCOME_AMT,0)) as CPA_INCOME_AMT,
    sum(IFNULL(con.REVSHARE_INCOME_AMT,0)) as REVSHARE_INCOME_AMT,
    {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from Final con
full outer join {{ ref('API_STREAMIA_ADVERTISER_BASE_UNIQUE_CLICKS') }} brc
    on con.date = brc.Event_Date and con.Advertiser_ID = brc.Advertiser_ID and con.Affiliate_ID = brc.Affiliate_ID
where
    coalesce(con.Date, brc.Event_Date) is not null
    and coalesce(con.Affiliate_ID, brc.Affiliate_ID) not in ('streamer-phillip','phil-test-1')
group by all
