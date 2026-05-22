{{ config(
    materialized = "ephemeral"
) }}
-- Ops RevShare for trafficx
Select
    off.DATE as Date,
    off.Country,
    off.PUBLISHER_NAME as Publisher,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    off.Brand_Name,
    off.PARENT_CAMPAIGN_NAME as Campaign_Name,
    pstbk.POST_SUBID as SubID,
    pstbk.POST_SUBID2 as SubID2,
    pstbk.POST_SUBID3 as SubID3,
    pstbk.POST_SUBID4 as SubID4,
    pstbk.POST_SUBID5 as SubID5,
    pstbk.POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
    off.ClickID,
    Sum(Case when off.CPA_PAYMENT > 0 then 1 else 0 end) as CPA_Cnt,
    Sum(off.DEPOSIT_CNT) as Deposit_Cnt,
    Sum(off.DEPOSIT_AMT_EUR) as Deposit_Amt,
    Sum(off.NET_REVENUE_AMT_EUR) as Net_Revenue_Amt,
    Sum(off.REVSHARE_PAYMENT_EUR) as RevShare_Revenue_Amt
from {{ source('EXP_PUBLIC', 'FACT_OFFER') }} off
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(off.ClickID) = upper(pstbk.Post_ClickID)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
    on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'TRACKER_LOGINS') }} trk
    on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join {{ source('BRC', 'ADVERTISERS') }} adv
    on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
Where off.PUBLISHER_NAME = 'trafficx'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
