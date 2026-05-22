{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: BRC Conversions combining clicks, registrations, FTDs, and CPA
-- Consolidates all BRC conversion data including Apuesta360 specific metrics
Select
coalesce(clks.Event_Date, reg.Event_Date, ftd.Event_Date, cpa.Event_Date) as Event_Date,
coalesce(clks.Tier_Level, reg.Tier_Level, ftd.Tier_Level, cpa.Tier_Level) as Tier_Level,
coalesce(clks.Advertiser_ID, reg.Advertiser_ID, ftd.Advertiser_ID, cpa.Advertiser_ID) as Advertiser_ID,
coalesce(clks.Advertiser_Name, reg.Advertiser_Name, ftd.Advertiser_Name, cpa.Advertiser_Name) as Advertiser_Name,
coalesce(clks.Affiliate_ID, reg.Affiliate_ID, ftd.Affiliate_ID, cpa.Affiliate_ID) as Affiliate_ID,
coalesce(clks.Brand_Name, reg.Brand_Name, ftd.Brand_Name, cpa.Brand_Name) as Brand_Name,
coalesce(clks.Campaign_ID, reg.Campaign_ID, ftd.Campaign_ID, cpa.Campaign_ID) as Campaign_ID,
coalesce(clks.Campaign_Name, reg.Campaign_Name, ftd.Campaign_Name, cpa.Campaign_Name) as Campaign_Name,
coalesce(clks.Campaign_Status, reg.Campaign_Status, ftd.Campaign_Status, cpa.Campaign_Status) as Campaign_Status,
coalesce(clks.Campaign_Type, reg.Campaign_Type, ftd.Campaign_Type, cpa.Campaign_Type) as Campaign_Type,
coalesce(clks.Currency, reg.Currency, ftd.Currency, cpa.Currency) as Currency,
coalesce(clks.Country, reg.Country, ftd.Country, cpa.Country) as Country,
coalesce(clks.Campiagn_Deal, reg.Campiagn_Deal, ftd.Campiagn_Deal, cpa.Campiagn_Deal) as Campiagn_Deal,
coalesce(clks.Baseline_Wager, reg.Baseline_Wager, ftd.Baseline_Wager, cpa.Baseline_Wager) as Baseline_Wager,
coalesce(clks.Baseline_Deposit, reg.Baseline_Deposit, ftd.Baseline_Deposit, cpa.Baseline_Deposit) as Baseline_Deposit,
coalesce(clks.RevShare_Deal, reg.RevShare_Deal, ftd.RevShare_Deal, cpa.RevShare_Deal) as RevShare_Deal,
coalesce(clks.CPA_Deal, reg.CPA_Deal, ftd.CPA_Deal, cpa.CPA_Deal) as CPA_Deal,
sum(clks.Click_Cnt) as Click_Cnt,
sum(IFNULL(reg.Reg_Cnt,0)) as Reg_Cnt,
sum(IFNULL(ftd.FTD_Cnt,0)) as FTD_Cnt,
sum(IFNULL(cpa.CPA_Income,0)) as CPA_Income_Amt
from {{ ref('API_STREAMIA_BASE_BRC_CLICKS') }} clks
full outer join {{ ref('API_STREAMIA_BASE_BRC_REG_APUESTA360') }} reg
    on clks.Event_Date = reg.Event_Date and clks.Affiliate_ID = reg.Affiliate_ID and clks.Campaign_ID = reg.Campaign_ID
full outer join {{ ref('API_STREAMIA_BASE_BRC_FTD_APUESTA360') }} ftd
    on clks.Event_Date = ftd.Event_Date and clks.Affiliate_ID = ftd.Affiliate_ID and clks.Campaign_ID = ftd.Campaign_ID
full outer join {{ ref('API_STREAMIA_BASE_BRC_CPA_APUESTA360') }} cpa
    on clks.Event_Date = cpa.Event_Date and clks.Affiliate_ID = cpa.Affiliate_ID and clks.Campaign_ID = cpa.Campaign_ID
group by all
