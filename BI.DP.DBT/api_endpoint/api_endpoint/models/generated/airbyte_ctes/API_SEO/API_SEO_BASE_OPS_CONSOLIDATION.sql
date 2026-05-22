{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-base" ]
) }}
-- Base model: Consolidates Operator Registration and FTD data
Select
    Coalesce(reg.Event_Date, ftd.Event_Date) as Date,
    Coalesce(reg.Country, ftd.Country) as Country,
    Coalesce(reg.Publisher, ftd.Publisher) as Publisher,
    Coalesce(reg.Advertiser_ID, ftd.Advertiser_ID) as Advertiser_ID,
    Coalesce(reg.Advertiser_Name, ftd.Advertiser_Name) as Advertiser_Name,
    Coalesce(reg.Brand_Name, ftd.Brand_Name) as Brand_Name,
    Coalesce(reg.Campaign_Name, ftd.Campaign_Name) as Campaign_Name,
    Coalesce(reg.Affiliate_ID, ftd.Affiliate_ID) as Affiliate_ID,
    Coalesce(reg.SubID, ftd.SubID) as SubID,
    Coalesce(reg.SubID2, ftd.SubID2) as SubID2,
    Coalesce(reg.SubID3, ftd.SubID3) as SubID3,
    Coalesce(reg.SubID4, ftd.SubID4) as SubID4,
    Coalesce(reg.SubID5, ftd.SubID5) as SubID5,
    Coalesce(reg.GA4_Device_ID, ftd.GA4_Device_ID) as GA4_Device_ID,
    Coalesce(reg.IP, ftd.IP) as IP,
    Coalesce(reg.OW_ID, ftd.OW_ID) as OW_ID,
    Coalesce(reg.Site_Member_ID, ftd.Site_Member_ID) as Site_Member_ID,
    Coalesce(reg.Marketing_Site_Id, ftd.Marketing_Site_Id) as Marketing_Site_Id,
    Coalesce(reg.Test_Variation, ftd.Test_Variation) as Test_Variation,
    Coalesce(reg.Page, ftd.Page) as Page,
    Coalesce(reg.Page_Location, ftd.Page_Location) as Page_Location,
    Coalesce(reg.Environment, ftd.Environment) as Environment,
    Coalesce(reg."3RD_PARTY_CLICKID", ftd."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
    Coalesce(reg.ClickID, ftd.ClickID) as ClickID,
    sum(reg.SignUp_Cnt) as SignUp_Cnt,
    COALESCE(sum(ftd.FTD_Cnt),0) as FTD_Cnt,
    COALESCE(sum(ftd.FTD_Amt),0) as FTD_Amt
from {{ ref('API_SEO_BASE_OPS_REGISTRATION') }} reg
full outer join {{ ref('API_SEO_BASE_OPS_FIRST_DEPOSIT') }} ftd
    on reg.ClickID = ftd.ClickID
group by all
