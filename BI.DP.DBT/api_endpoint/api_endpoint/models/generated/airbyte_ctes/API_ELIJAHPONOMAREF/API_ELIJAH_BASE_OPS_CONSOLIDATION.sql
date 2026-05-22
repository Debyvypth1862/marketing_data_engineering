{{ config(
    materialized = "ephemeral"
) }}
-- Ops Consolidation (Reg + FTD) for ElijahPonomaref
Select
    Coalesce(reg.Event_Date, ftd.Event_Date) as Date,
    Coalesce(reg.Country, ftd.Country) as Country,
    Coalesce(reg.Publisher, ftd.Publisher) as Publisher,
    Coalesce(reg.Advertiser_ID, ftd.Advertiser_ID) as Advertiser_ID,
    Coalesce(reg.Advertiser_Name, ftd.Advertiser_Name) as Advertiser_Name,
    Coalesce(reg.Brand_Name, ftd.Brand_Name) as Brand_Name,
    Coalesce(reg.Campaign_Name, ftd.Campaign_Name) as Campaign_Name,
    Coalesce(reg.SubID, ftd.SubID) as SubID,
    Coalesce(reg.SubID2, ftd.SubID2) as SubID2,
    Coalesce(reg.SubID3, ftd.SubID3) as SubID3,
    Coalesce(reg.SubID4, ftd.SubID4) as SubID4,
    Coalesce(reg.SubID5, ftd.SubID5) as SubID5,
    Coalesce(reg."3RD_PARTY_CLICKID", ftd."3RD_PARTY_CLICKID") as "3RD_PARTY_CLICKID",
    Coalesce(reg.ClickID, ftd.ClickID) as ClickID,
    sum(reg.SignUp_Cnt) as SignUp_Cnt,
    IFNULL(sum(ftd.FTD_CNT),0) as FTD_Cnt,
    IFNULL(sum(ftd.FTD_Amt),0) as FTD_Amt
from {{ ref('API_ELIJAH_BASE_OPS_REGISTRATION') }} reg
full outer join {{ ref('API_ELIJAH_BASE_OPS_FIRST_DEPOSIT') }} ftd
    on reg.ClickID = ftd.ClickID
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
