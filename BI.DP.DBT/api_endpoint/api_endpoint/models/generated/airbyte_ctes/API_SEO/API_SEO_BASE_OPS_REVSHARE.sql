{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-base" ]
) }}
-- Base model: Operator Revenue Share and CPA data from FACT_OFFER
-- Filtered for SEO publishers: Clovr, Lines, CasinoRoom, Esportiva
Select
    off.DATE as Date,
    off.Country,
    off.PUBLISHER_NAME as Publisher,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    off.Brand_Name,
    off.PARENT_CAMPAIGN_NAME as Campaign_Name,
    pstbk.POST_AFFILIATE_ID as Affiliate_ID,
    pstbk.POST_SUBID as SubID,
    pstbk.POST_SUBID2 as SubID2,
    pstbk.POST_SUBID3 as SubID3,
    pstbk.POST_SUBID4 as SubID4,
    pstbk.POST_SUBID5 as SubID5,
    pstbk.POST_GA4_DEVICE_ID as GA4_Device_ID,
    pstbk.POST_IP as IP,
    pstbk.POST_OW_ID as OW_ID,
    pstbk.POST_SITE_MEMBER_ID as Site_Member_ID,
    pstbk.POST_MARKETING_SITE_ID as Marketing_Site_Id,
    pstbk.POST_TEST_VARIATION as Test_Variation,
    pstbk.POST_PAGE as Page,
    pstbk.POST_PAGE_LOCATION as Page_Location,
    pstbk.POST_ENV as Environment,
    pstbk.POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
    off.ClickID,
    COALESCE(sum(Case when off.CPA_PAYMENT > 0 then 1 else 0 end),0) as CPA_Cnt,
    COALESCE(sum(off.CPA_INCOME),0) as CPA_Income,
    COALESCE(sum(off.CPA_PAYMENT),0) as CPA_Payment,
    COALESCE(sum(off.CPA_REVENUE),0) as CPA_Revenue
from {{ source('EXP_PUBLIC', 'FACT_OFFER') }} off
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(off.ClickID) = upper(pstbk.POST_CLICKID)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
    on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'TRACKER_LOGINS') }} trk
    on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join {{ source('BRC', 'ADVERTISERS') }} adv
    on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
Where off.PUBLISHER_NAME in ('Clovr','Lines','CasinoRoom','Esportiva')
group by all
