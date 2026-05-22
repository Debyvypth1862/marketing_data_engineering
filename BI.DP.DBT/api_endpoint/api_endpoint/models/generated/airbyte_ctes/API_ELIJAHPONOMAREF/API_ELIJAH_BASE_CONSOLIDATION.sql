{{ config(
    materialized = "ephemeral"
) }}
-- Final Consolidation for ElijahPonomaref
Select
    Coalesce(brc.Date, reg.Date, rev.Date ) as Date,
    Coalesce(brc.Country,reg.Country, rev.Country) as Country,
    Coalesce(brc.Publisher, reg.Publisher, rev.Publisher) as Publisher,
    Coalesce(brc.Advertiser_ID, reg.Advertiser_ID, rev.Advertiser_ID) as Advertiser_ID,
    Coalesce(brc.Advertiser_Name,reg.Advertiser_Name, rev.Advertiser_Name) as Advertiser_Name,
    Coalesce(brc.Brand_Name, reg.Brand_Name, rev.Brand_Name) as Brand_Name,
    Coalesce(brc.Campaign_Name, reg.Campaign_Name, rev.Campaign_Name) as Campaign_Name,
    Coalesce(brc.SubID, reg.SubID, rev.SubID) as SubID,
    Coalesce(brc.SubID2, reg.SubID2, rev.SubID2) as SubID2,
    Coalesce(brc.SubID3, reg.SubID3, rev.SubID3) as SubID3,
    Coalesce(brc.SubID4, reg.SubID4, rev.SubID4) as SubID4,
    Coalesce(brc.SubID5, reg.SubID5, rev.SubID5) as SubID5,
    Coalesce(brc."3RD_PARTY_CLICKID", rev."3RD_PARTY_CLICKID", reg."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
    Coalesce(brc.ClickID, rev.ClickID, reg.ClickID) as ClickID,
    brc.Click_Cnt,
    IFNULL(Coalesce(reg.Signup_Cnt, brc.Signup_Cnt),0) as Signup_Cnt,
    IFNULL(Coalesce(reg.FTD_Cnt, brc.FTD_Cnt),0) as FTD_Cnt,
    IFNULL(reg.FTD_Amt,0) as FTD_Amt,
    IFNULL(Coalesce(rev.CPA_Cnt, brc.CPA_Cnt),0) as CPA_Cnt,
    IFNULL(rev.Deposit_Cnt, 0) as Deposit_Cnt,
    IFNULL(rev.Deposit_Amt,0) as Deposit_Amt,
    IFNULL(rev.Net_Revenue_Amt, 0) as Net_Revenue_Amt,
    IFNULL(rev.RevShare_Revenue_Amt, 0) as RevShare_Revenue_Amt
from {{ ref('API_ELIJAH_BASE_BRC_CONSOLIDATION') }} brc
full outer join {{ ref('API_ELIJAH_BASE_OPS_CONSOLIDATION') }} reg
    on upper(brc.ClickID) = upper(reg.ClickID) and brc.Date = reg.Date
full outer join {{ ref('API_ELIJAH_BASE_OPS_REVSHARE') }} rev
    on upper(Coalesce(brc.ClickID, reg.ClickID)) = upper(rev.ClickID) and Coalesce(brc.Date, reg.Date) = rev.Date
