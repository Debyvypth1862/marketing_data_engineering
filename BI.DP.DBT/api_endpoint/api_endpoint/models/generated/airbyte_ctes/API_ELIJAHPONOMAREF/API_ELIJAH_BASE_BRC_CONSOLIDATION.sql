{{ config(
    materialized = "ephemeral"
) }}
-- BRC Consolidation for ElijahPonomaref
Select
    Coalesce(brc.Event_Date, reg.Event_Date, ftd.Event_Date, cpa.Event_Date) as Date,
    Coalesce(brc.Country, reg.Country, ftd.Country, cpa.Country) as Country,
    Coalesce(brc.Publisher, reg.Publisher, ftd.Publisher, cpa.Publisher) as Publisher,
    Coalesce(brc.Advertiser_ID, reg.Advertiser_ID, ftd.Advertiser_ID, cpa.Advertiser_ID) as Advertiser_ID,
    Coalesce(brc.Advertiser_Name, reg.Advertiser_Name, ftd.Advertiser_Name, cpa.Advertiser_Name) as Advertiser_Name,
    Coalesce(brc.Brand_Name, reg.Brand_Name, ftd.Brand_Name, cpa.Brand_Name) as Brand_Name,
    Coalesce(brc.Campaign_Name, reg.Campaign_Name, ftd.Campaign_Name, cpa.Campaign_Name) as Campaign_Name,
    Coalesce(brc.SubID, reg.SubID, ftd.SubID, cpa.SubID) as SubID,
    Coalesce(brc.SubID2, reg.SubID2, ftd.SubID2, cpa.SubID2) as SubID2,
    Coalesce(brc.SubID3, reg.SubID3, ftd.SubID3, cpa.SubID3) as SubID3,
    Coalesce(brc.SubID4, reg.SubID4, ftd.SubID4, cpa.SubID4) as SubID4,
    Coalesce(brc.SubID5, reg.SubID5, ftd.SubID5, cpa.SubID5) as SubID5,
    Coalesce(brc."3RD_PARTY_CLICKID", reg."3RD_PARTY_CLICKID", ftd."3RD_PARTY_CLICKID", cpa."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
    Coalesce(brc.ClickID, reg.ClickID, ftd.ClickID, cpa.ClickID) as ClickID,
    brc.Click_Cnt,
    IFNULL(reg.signup_cnt,0) as Signup_Cnt,
    IFNULL(ftd.FTD_Cnt,0) as FTD_Cnt,
    IFNULL(cpa.CPA_Cnt,0) as CPA_Cnt
from {{ ref('API_ELIJAH_BASE_BRC_CLICKS') }} brc
full outer join {{ ref('API_ELIJAH_BASE_BRC_REGISTRATION') }} reg
    on brc.ClickID = reg.ClickID
full outer join {{ ref('API_ELIJAH_BASE_BRC_FIRST_DEPOSIT') }} ftd
    on brc.clickid = ftd.clickid
full outer join {{ ref('API_ELIJAH_BASE_BRC_CPA') }} cpa
    on brc.clickid = cpa.clickid
