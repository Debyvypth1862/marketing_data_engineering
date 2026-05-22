{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-base" ]
) }}
-- Base model: Consolidates all BRC postback data
-- Combines Clicks, Registrations, FTDs, and CPA conversions
With Consolidation AS
(
    Select
        Coalesce(clk.Event_Date, reg.Event_Date, ftd.Event_Date, cpa.Event_Date) as Date,
        Coalesce(clk.Country, reg.Country, ftd.Country, cpa.Country) as Country,
        Coalesce(clk.Publisher, reg.Publisher, ftd.Publisher, cpa.Publisher) as Publisher,
        Coalesce(clk.Advertiser_ID, reg.Advertiser_ID, ftd.Advertiser_ID, cpa.Advertiser_ID) as Advertiser_ID,
        Coalesce(clk.Advertiser_Name, reg.Advertiser_Name, ftd.Advertiser_Name, cpa.Advertiser_Name) as Advertiser_Name,
        Coalesce(clk.Brand_Name, reg.Brand_Name, ftd.Brand_Name, cpa.Brand_Name) as Brand_Name,
        Coalesce(clk.Campaign_Name, reg.Campaign_Name, ftd.Campaign_Name, cpa.Campaign_Name) as Campaign_Name,
        Coalesce(clk.Affiliate_ID, reg.Affiliate_ID, ftd.Affiliate_ID, cpa.Affiliate_ID) as Affiliate_ID,
        Coalesce(clk.SubID, reg.SubID, ftd.SubID, cpa.SubID) as SubID,
        Coalesce(clk.SubID2, reg.SubID2, ftd.SubID2, cpa.SubID2) as SubID2,
        Coalesce(clk.SubID3, reg.SubID3, ftd.SubID3, cpa.SubID3) as SubID3,
        Coalesce(clk.SubID4, reg.SubID4, ftd.SubID4, cpa.SubID4) as SubID4,
        Coalesce(clk.SubID5, reg.SubID5, ftd.SubID5, cpa.SubID5) as SubID5,
        Coalesce(clk.GA4_Device_ID, reg.GA4_Device_ID, ftd.GA4_Device_ID, cpa.GA4_Device_ID) as GA4_Device_ID,
        Coalesce(clk.IP, reg.IP, ftd.IP, cpa.IP) as IP,
        Coalesce(clk.OW_ID, reg.OW_ID, ftd.OW_ID, cpa.OW_ID) as OW_ID,
        Coalesce(clk.Site_Member_ID, reg.Site_Member_ID, ftd.Site_Member_ID, cpa.Site_Member_ID) as Site_Member_ID,
        Coalesce(clk.Marketing_Site_Id, reg.Marketing_Site_Id, ftd.Marketing_Site_Id, cpa.Marketing_Site_Id) as Marketing_Site_Id,
        Coalesce(clk.Test_Variation, reg.Test_Variation, ftd.Test_Variation, cpa.Test_Variation) as Test_Variation,
        Coalesce(clk.Page, reg.Page, ftd.Page, cpa.Page) as Page,
        Coalesce(clk.Page_Location, reg.Page_Location, ftd.Page_Location, cpa.Page_Location) as Page_Location,
        Coalesce(clk.Environment, reg.Environment, ftd.Environment, cpa.Environment) as Environment,
        Coalesce(clk."3RD_PARTY_CLICKID", reg."3RD_PARTY_CLICKID", ftd."3RD_PARTY_CLICKID", cpa."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
        Coalesce(clk.ClickID, reg.ClickID, ftd.ClickID, cpa.ClickID) as ClickID,
        clk.Click_Cnt,
        reg.Signup_Cnt,
        ftd.FTD_Cnt,
        cpa.CPA_Cnt
    from {{ ref('API_SEO_BASE_BRC_CLICKS') }} clk
    full outer join {{ ref('API_SEO_BASE_BRC_REGISTRATION') }} reg
        on clk.ClickID = reg.ClickID
    full outer join {{ ref('API_SEO_BASE_BRC_FIRST_DEPOSIT') }} ftd
        on clk.ClickID = ftd.ClickID
    full outer join {{ ref('API_SEO_BASE_BRC_CPA') }} cpa
        on clk.ClickID = cpa.ClickID
)

Select * from Consolidation
