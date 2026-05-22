{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: BRC Unique Clicks at ADVERTISER level
-- Used for final join in ADVERTISER pipeline (no campaign fields)
select
    pstbk.POST_CLICK_DATE AS Event_Date,
    CASE WHEN UPPER(pub.PUBL_USERNAME) like '%TIER 1%' THEN 'Tier 1'
         WHEN UPPER(pub.PUBL_USERNAME) like '%TIER 2%' THEN 'Tier 2'
         ELSE 'Unknown' END
    AS Tier_Level,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    pstbk.POST_AFFILIATE_ID as Affiliate_ID,
    count(distinct pstbk.Post_IP) as Unique_Clicks
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
left outer join {{ source('EXP_PUBLIC', 'DIM_COUNTRY') }} cntry
    on a.CAMP_COUNTRY = cntry.country_iso_code
where upper(pub.PUBL_USERNAME) like '%TIER%' and Post_IP is not null
 AND trk.TLOG_ID NOT IN (Select tracker_login_id from {{ ref('API_STREAMIA_BASE_REFERON_EXCLUSION') }})
group by all
