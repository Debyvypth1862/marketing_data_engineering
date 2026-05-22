{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-base" ]
) }}
-- Base model: Final consolidation combining BRC and Operator data
-- Primary key: DATE + ClickID
Select
    Coalesce(brc.Date, reg.Date, rev.Date) as Date,
    Coalesce(brc.Country, reg.Country, rev.Country) as Country,
    Coalesce(brc.Publisher, reg.Publisher, rev.Publisher) as Publisher,
    Coalesce(brc.Advertiser_ID, reg.Advertiser_ID, rev.Advertiser_ID) as Advertiser_ID,
    Coalesce(brc.Advertiser_Name, reg.Advertiser_Name, rev.Advertiser_Name) as Advertiser_Name,
    Coalesce(brc.Brand_Name, reg.Brand_Name, rev.Brand_Name) as Brand_Name,
    Coalesce(brc.Campaign_Name, reg.Campaign_Name, rev.Campaign_Name) as Campaign_Name,
    Coalesce(brc.Affiliate_ID, reg.Affiliate_ID, rev.Affiliate_ID) as Affiliate_ID,
    Coalesce(brc.SubID, reg.SubID, rev.SubID) as SubID,
    Coalesce(brc.SubID2, reg.SubID2, rev.SubID2) as SubID2,
    Coalesce(brc.SubID3, reg.SubID3, rev.SubID3) as SubID3,
    Coalesce(brc.SubID4, reg.SubID4, rev.SubID4) as SubID4,
    Coalesce(brc.SubID5, reg.SubID5, rev.SubID5) as SubID5,
    Coalesce(brc.GA4_Device_ID, reg.GA4_Device_ID, rev.GA4_Device_ID) as GA4_Device_ID,
    Coalesce(brc.IP, reg.IP, rev.IP) as IP,
    Coalesce(brc.OW_ID, reg.OW_ID, rev.OW_ID) as OW_ID,
    Coalesce(brc.Site_Member_ID, reg.Site_Member_ID, rev.Site_Member_ID) as Site_Member_ID,
    Coalesce(brc.Marketing_Site_Id, reg.Marketing_Site_Id, rev.Marketing_Site_Id) as Marketing_Site_Id,
    Coalesce(brc.Test_Variation, reg.Test_Variation, rev.Test_Variation) as Test_Variation,
    Coalesce(brc.Page, reg.Page, rev.Page) as Page,
    Coalesce(brc.Page_Location, reg.Page_Location, rev.Page_Location) as Page_Location,
    Coalesce(brc.Environment, reg.Environment, rev.Environment) as Environment,
    Coalesce(brc."3RD_PARTY_CLICKID", reg."3RD_PARTY_CLICKID", rev."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
    Coalesce(brc.ClickID, reg.ClickID, rev.ClickID) as ClickID,
    sum(brc.Click_Cnt) as Click_Cnt,
    COALESCE(sum(Coalesce(reg.Signup_Cnt, brc.Signup_Cnt)),0) as Signup_Cnt,
    COALESCE(sum(Coalesce(reg.FTD_Cnt, brc.FTD_Cnt)),0) as FTD_Cnt,
    COALESCE(sum(reg.FTD_Amt),0) as FTD_Amt,
    COALESCE(sum(Coalesce(rev.CPA_Cnt, brc.CPA_Cnt)),0) as CPA_Cnt,
    COALESCE(sum(rev.CPA_Income),0) as CPA_Income,
    COALESCE(sum(rev.CPA_Payment),0) as CPA_Payment,
    COALESCE(sum(rev.CPA_Revenue),0) as CPA_Revenue
from {{ ref('API_SEO_BASE_BRC_CONSOLIDATION') }} brc
full outer join {{ ref('API_SEO_BASE_OPS_CONSOLIDATION') }} reg
    on upper(brc.ClickID) = upper(reg.ClickID)
    and brc.Date = reg.Date
full outer join {{ ref('API_SEO_BASE_OPS_REVSHARE') }} rev
    on upper(Coalesce(brc.ClickID, reg.ClickID)) = upper(rev.ClickID)
    and Coalesce(brc.Date, reg.Date) = rev.Date
group by all
