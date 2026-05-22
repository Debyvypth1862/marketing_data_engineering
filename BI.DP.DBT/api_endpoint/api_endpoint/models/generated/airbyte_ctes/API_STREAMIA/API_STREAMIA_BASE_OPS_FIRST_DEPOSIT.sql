{{ config(
    materialized = 'ephemeral',
    tags = [ "streamia-base" ]
) }}
-- Base model: Operator First Deposit data with campaign details
-- Extracts FTD (First Time Deposit) data from operator aggregation
Select
    ops.FTD_DATE as EVENT_DATE,
    CASE WHEN UPPER(ops.PUBLISHER_NAME) like '%TIER 1%' THEN 'Tier 1'
         WHEN UPPER(ops.PUBLISHER_NAME) like '%TIER 2%' THEN 'Tier 2'
         ELSE 'Unknown' END
    AS TIER_LEVEL,
    adv.ADVE_ID as Advertiser_ID,
    adv.ADVE_NAME as Advertiser_Name,
    pstbk.POST_AFFILIATE_ID as Affiliate_ID,
    b.BRAN_NAME as Brand_Name,
    a.CAMP_ID as Campaign_ID,
    a.CAMP_NAME as Campaign_Name,
    a.CAMP_STATUS as Campaign_Status,
    a.CAMP_TYPE as Campaign_Type,
    a.CAMP_CURRENCY as Currency,
    cntry.COUNTRY_NAME Country,
    a.CAMP_DISPLAY_DEAL as Campiagn_Deal,
    a.CAMP_WAGER_BASELINE as Baseline_Wager,
    a.CAMP_DEPOSIT_BASELINE as Baseline_Deposit,
    a.CAMP_REV_OUT as RevShare_Deal,
    Case 
            When a.CAMP_CURRENCY <> 'EUR' then min(a.CAMP_CPA_OUT) * min(conv.rate) 
            Else min(a.CAMP_CPA_OUT) end 
        as CPA_Deal,
    SUM(ops.FTD_AMT) AS FTD_AMT,
    SUM(ops.FTD_CNT) AS FTD_CNT
FROM {{ source('EXP_PUBLIC', 'FACT_OPERATOR_AGG') }} ops
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(ops.clickid) = upper(pstbk.post_clickid)
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
left outer join {{ source('RAW_APILAYER', 'TO_EUR_HISTORICAL') }} conv
      on pstbk.post_ftd_date = conv.date and  a.CAMP_CURRENCY = conv.currency_source
WHERE UPPER(ops.PUBLISHER_NAME) like '%TIER%'
GROUP BY All
