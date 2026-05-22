{{ config(
    materialized = "ephemeral"
) }}
-- Lead conversions from BRC with all joins
SELECT
    pub.PUBL_USERNAME as Publisher,
    Case
        when loc.COUNTRY_NAME is null then 'Unknown' else loc.COUNTRY_NAME
    end as Country,
    pstbk.post_adgroupid as AdGroupId,
    pstbk.post_adaccountid as AdAccountId,
    cmtkr.CAMT_ID as BRC_CampaignID,
    a.CAMP_NAME as BRC_CampaignName,
    pstbk.Post_CampaignID as CampaignID,
    Case
        when cp.Pl_CampaignID = '' and pstbk.Post_CampaignID <> '' then pstbk.Post_CampaignID
        when cp.Pl_CampaignID IS NULL and pstbk.Post_CampaignID <> '' then pstbk.Post_CampaignID
        when cp.Pl_CampaignID = '' and pstbk.Post_CampaignID = '' then 'Unknown'
        else cp.Pl_CampaignID
    end as Pl_CampaignID,
    cast(pstbk.POST_CLICK_TIMESTAMP as date) as Click_Date,
    pstbk.POST_CLICK_TIMESTAMP AS Conversion_Time,
    pstbk.post_ip as IPAddress,
    pstbk.POST_GCLID as GCLID,
    pstbk.POST_3RD_PARTY_CLICKID,
    'Lead' as Conversion_Name,
    'USD' as Conversion_Currency,
    sum(Case when pstbk.POST_SIGNUP_DATE is not null then 1 else 0 end) as Conversion_Value
FROM {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
left outer join {{ source('EXP_PUBLIC', 'DIM_PLAYER_LOCATION') }} loc
    on pstbk.POST_IP = loc.IP
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
left outer join {{ ref('API_GCLID_BASE_PLATFORM_CAMPAIGN') }} cp
    on upper(pstbk.POST_3RD_PARTY_CLICKID) = upper(cp.Clickid)
WHERE
    length(pstbk.POST_GCLID) > 10 and
    pstbk.POST_SIGNUP_DATE is not null
group by all
