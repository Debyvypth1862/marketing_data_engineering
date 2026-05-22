{{ config(
    materialized = 'ephemeral',
    unique_key = '_AIRBYTE_AB_ID',
    schema = "API_STREAMIA",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model for API_STREAMIA_CAMPAIGN AB1
-- Keeps campaign-level data from base consolidation (no aggregation)
-- Refs shared base consolidation model and joins with campaign-level unique clicks
SELECT
    {{ dbt_utils.surrogate_key(["coalesce(con.Date, brc.Event_Date)", "coalesce(con.Campaign_ID, brc.Campaign_ID)", "coalesce(con.Affiliate_ID, brc.Affiliate_ID)"]) }} as _AIRBYTE_AB_ID,
    coalesce(con.Date, brc.Event_Date) as DATE,
    coalesce(con.Tier_Level, brc.Tier_Level) as TIER_LEVEL,
    coalesce(con.Advertiser_ID, brc.Advertiser_ID) as ADVERTISER_ID,
    ltrim(rtrim(coalesce(con.Advertiser_Name, brc.Advertiser_Name))) as ADVERTISER_NAME,
    coalesce(con.Affiliate_ID, brc.Affiliate_ID) as AFFILIATE_ID,
    coalesce(con.Brand_Name, brc.Brand_Name) as BRAND_NAME,
    coalesce(con.Campaign_ID, brc.Campaign_ID) as CAMPAIGN_ID,
    coalesce(con.Campaign_Name, brc.Campaign_Name) as CAMPAIGN_NAME,
    coalesce(con.Campaign_Type, brc.Campaign_Type) as CAMPAIGN_TYPE,
    coalesce(con.Campaign_Status, brc.Campaign_Status) as CAMPAIGN_STATUS,
    coalesce(con.Currency, brc.Currency) as CURRENCY,
    coalesce(con.Country, brc.Country) as COUNTRY,
    coalesce(con.Baseline_Wager, brc.Baseline_Wager) as BASELINE_WAGER,
    coalesce(con.Baseline_Deposit, brc.Baseline_Deposit) as BASELINE_DEPOSIT,
    coalesce(con.RevShare_Deal, brc.RevShare_Deal) as REVSHARE_DEAL,
    coalesce(con.CPA_Deal, brc.CPA_Deal) as CPA_DEAL,
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
from {{ ref('API_STREAMIA_BASE_CONSOLIDATION') }} con
full outer join {{ ref('API_STREAMIA_CAMPAIGN_BASE_UNIQUE_CLICKS') }} brc
    on con.date = brc.Event_Date and con.Affiliate_ID = brc.Affiliate_ID and con.Campaign_ID = brc.Campaign_ID
where
    coalesce(con.Date, brc.Event_Date) is not null
    and coalesce(con.Affiliate_ID, brc.Affiliate_ID) not in ('streamer-phillip','phil-test-1')
group by all
