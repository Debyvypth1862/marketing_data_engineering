{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: Operator Consolidation
-- Combines registration and first deposit data at campaign level
Select
    Coalesce(reg.Event_Date, ftd.Event_Date) as Event_Date,
    Coalesce(reg.Tier_Level, ftd.Tier_Level) as Tier_Level,
    Coalesce(reg.Advertiser_ID, ftd.Advertiser_ID) as Advertiser_ID,
    Coalesce(reg.Advertiser_Name, ftd.Advertiser_Name) as Advertiser_Name,
    Coalesce(reg.Affiliate_ID, ftd.Affiliate_ID) as Affiliate_ID,
    Coalesce(reg.Brand_Name, ftd.Brand_Name) as Brand_Name,
    Coalesce(reg.Campaign_ID, ftd.Campaign_ID) as Campaign_ID,
    Coalesce(reg.Campaign_Name, ftd.Campaign_Name) as Campaign_Name,
    Coalesce(reg.Campaign_Status, ftd.Campaign_Status) as Campaign_Status,
    Coalesce(reg.Campaign_Type, ftd.Campaign_Type) as Campaign_Type,
    Coalesce(reg.Currency, ftd.Currency) as Currency,
    Coalesce(reg.Country, ftd.Country) as Country,
    Coalesce(reg.Campiagn_Deal, ftd.Campiagn_Deal) as Campiagn_Deal,
    Coalesce(reg.Baseline_Wager, ftd.Baseline_Wager) as Baseline_Wager,
    Coalesce(reg.Baseline_Deposit, ftd.Baseline_Deposit) as Baseline_Deposit,
    Coalesce(reg.RevShare_Deal, ftd.RevShare_Deal) as RevShare_Deal,
    Coalesce(reg.CPA_Deal, ftd.CPA_Deal) as CPA_Deal,
    Sum(IFNULL(reg.Reg_Cnt,0)) as Reg_Cnt,
    Sum(IFNULL(ftd.FTD_Cnt,0)) as FTD_Cnt,
    Sum(IFNULL(ftd.FTD_Amt,0)) as FTD_Amt
from {{ ref('API_STREAMIA_BASE_OPS_REGISTRATION') }} reg
full outer join {{ ref('API_STREAMIA_BASE_OPS_FIRST_DEPOSIT') }} ftd
on reg.Event_Date = ftd.Event_Date and reg.Campaign_ID = ftd.Campaign_ID and reg.Affiliate_ID = ftd.Affiliate_ID
group by all
