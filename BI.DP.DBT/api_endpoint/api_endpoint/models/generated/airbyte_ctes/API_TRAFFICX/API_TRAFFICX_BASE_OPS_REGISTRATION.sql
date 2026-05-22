{{ config(
    materialized = "ephemeral"
) }}
-- Ops Registration for trafficx
Select
    off.SIGNUP_DATE as Event_Date,
    off.Country,
    off.PUBLISHER_NAME as Publisher,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    off.Brand_Name,
    a.Camp_Name as Campaign_Name,
    pstbk.POST_SUBID as SubID,
    pstbk.POST_SUBID2 as SubID2,
    pstbk.POST_SUBID3 as SubID3,
    pstbk.POST_SUBID4 as SubID4,
    pstbk.POST_SUBID5 as SubID5,
    pstbk.POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
    off.ClickID,
    Sum(SIGNUP_CNT) as SignUp_Cnt
from {{ source('EXP_PUBLIC', 'FACT_OPERATOR_AGG') }} off
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(off.ClickID) = upper(pstbk.Post_ClickID)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
    on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'CAMPAIGNS') }} a
    on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join {{ source('BRC', 'BRANDS') }} b
    on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join {{ source('BRC', 'TRACKER_LOGINS') }} trk
    on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join {{ source('BRC', 'PUBLISHERS') }} pub
    on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join {{ source('BRC', 'ADVERTISERS') }} adv
    on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
where off.SIGNUP_CNT > 0
and off.PUBLISHER_NAME = 'trafficx'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
