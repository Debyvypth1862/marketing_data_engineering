{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-base" ]
) }}
-- Base model: BRC CPA Conversions from POSTBACK_TRACKING
-- Filtered for SEO publishers: Clovr, Lines, CasinoRoom, Esportiva
Select
    pstbk.POST_CPA_DATE as Event_Date,
    Case when loc.COUNTRY_NAME is null then 'Unknown' else loc.COUNTRY_NAME end as Country,
    pub.PUBL_USERNAME as Publisher,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    b.BRAN_NAME as Brand_Name,
    a.CAMP_NAME as Campaign_Name,
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
    pstbk.POST_CLICKID as ClickID,
    COUNT(pstbk.POST_CPA_DATE) as CPA_Cnt
from {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
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
left outer join {{ source('EXP_PUBLIC', 'DIM_PLAYER_LOCATION') }} loc
    on pstbk.POST_IP = loc.IP
where pub.PUBL_USERNAME in ('Clovr','Lines','CasinoRoom','Esportiva')
group by all
