{{ config(
    materialized='incremental',
    cluster_by = ["Event_Date"],
    unique_key = "UNIQUE_KEY",
    database = "EXP_AI",
    schema = "CLAUDE_AI",
    enabled = false
) }}

WITH BRC_CLICKS AS (
  SELECT
    pstbk.POST_CLICK_DATE AS Event_Date
    , pub.PUBL_USERNAME AS PUBLISHER_NAME
    , adv.ADVE_NAME AS ADVERTISER_NAME
    , b.BRAN_NAME AS BRAND_NAME
    , a.CAMP_NAME AS Campaign_Name
    , loc.COUNTRY_NAME AS Country
    , COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_DEPOSIT_BASELINE, '$', '')), 0) AS Campaign_Baseline
    , COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_WAGER_BASELINE, '$', '')), 0) AS Campaign_Wager_Baseline
    , CAST(a.CAMP_REV_DEAL AS DECIMAL(20, 2)) AS RevShare_In
    , (a.CAMP_REV_DEAL * .1) * (a.CAMP_REV_OUT * .1) AS RevShare_Out
    , a.CAMP_CPA_IN AS CPA_In
    , a.CAMP_CPA_OUT AS CPA_Out
    , pstbk.POST_CLICKID AS ClickID
    , COUNT(pstbk.POST_CLICKID) AS Click_Total
  FROM {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  LEFT OUTER JOIN {{ source('BRC', 'BRANDS') }} AS b
    ON a.CAMP_FK_BRAND = b.BRAN_ID
  LEFT OUTER JOIN {{ source('BRC', 'TRACKER_LOGINS') }} AS trk
    ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
  LEFT OUTER JOIN {{ source('BRC', 'PUBLISHERS') }} AS pub
    ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
  LEFT OUTER JOIN {{ source('BRC', 'ADVERTISERS') }} AS adv
    ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
  LEFT OUTER JOIN {{ ref('DIM_PLAYER_LOCATION') }} AS loc
    ON
      pstbk.post_ip = loc.IP
      {{ incremental_clause_with_custom_column('Event_Date', this, 'DATE(pstbk.POST_CLICK_DATE)') }}
  GROUP BY ALL
)

, OPERATOR_REGISTRATION AS (
  SELECT
    fct.SIGNUP_DATE AS EVENT_DATE
    , fct.PUBLISHER_NAME
    , fct.ADVERTISER_NAME
    , fct.BRAND_NAME
    , a.CAMP_NAME AS Campaign_Name
    , fct.COUNTRY
    , CASE WHEN TRIM(fct.ClickID) = '' THEN NULL ELSE fct.ClickID END AS ClickID
    , MAX(COALESCE(fct.CAMPAIGN_BASELINE, 0)) AS Campaign_Baseline
    , MAX(COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_WAGER_BASELINE, '$', '')), 0))
      AS Campaign_Wager_Baseline
    , MAX(COALESCE(fct.CPA_IN_EUR, 0)) AS CPA_IN
    , MAX(COALESCE(fct.CPA_OUT_EUR, 0)) AS CPA_OUT
    , MAX(COALESCE(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20, 2)), 0)) AS REVSHARE_IN
    , COALESCE((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1), 0) AS REVSHARE_OUT
    , SUM(fct.SIGNUP_CNT) AS SIGNUP_CNT
  FROM {{ ref('FACT_OFFER') }} AS fct
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(fct.clickid) = UPPER(pstbk.POST_CLICKID)
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  WHERE
    fct.SIGNUP_CNT = 1
    {{ incremental_clause_with_custom_column('EVENT_DATE', this, 'DATE(fct.SIGNUP_DATE)') }}
  GROUP BY ALL
)

, OPERATOR_FIRSTDEPOSIT AS (
  SELECT
    fct.FTD_DATE AS EVENT_DATE
    , fct.PUBLISHER_NAME
    , fct.ADVERTISER_NAME
    , fct.BRAND_NAME
    , a.CAMP_NAME AS Campaign_Name
    , fct.COUNTRY
    , CASE WHEN TRIM(fct.ClickID) = '' THEN NULL ELSE fct.ClickID END AS ClickID
    , MAX(COALESCE(fct.CAMPAIGN_BASELINE, 0)) AS Campaign_Baseline
    , MAX(COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_WAGER_BASELINE, '$', '')), 0))
      AS Campaign_Wager_Baseline
    , MAX(COALESCE(fct.CPA_IN_EUR, 0)) AS CPA_IN
    , MAX(COALESCE(fct.CPA_OUT_EUR, 0)) AS CPA_OUT
    , MAX(COALESCE(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20, 2)), 0)) AS REVSHARE_IN
    , COALESCE((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1), 0) AS REVSHARE_OUT
    , SUM(fct.FTD_CNT) AS FTD_CNT
  FROM {{ ref('FACT_OFFER') }} AS fct
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(fct.clickid) = UPPER(pstbk.POST_CLICKID)
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  WHERE
    fct.FTD_CNT = 1
    {{ incremental_clause_with_custom_column('Event_Date', this, 'DATE(fct.FTD_DATE)') }}
  GROUP BY ALL
)

, OPERATOR_CPA AS (
  SELECT
    fct.CPA_DATE AS EVENT_DATE
    , fct.PUBLISHER_NAME
    , fct.ADVERTISER_NAME
    , fct.BRAND_NAME
    , a.CAMP_NAME AS Campaign_Name
    , fct.COUNTRY
    , CASE WHEN TRIM(fct.ClickID) = '' THEN NULL ELSE fct.ClickID END AS ClickID
    , MAX(COALESCE(fct.CAMPAIGN_BASELINE, 0)) AS Campaign_Baseline
    , MAX(COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_WAGER_BASELINE, '$', '')), 0))
      AS Campaign_Wager_Baseline
    , MAX(COALESCE(fct.CPA_IN_EUR, 0)) AS CPA_IN
    , MAX(COALESCE(fct.CPA_OUT_EUR, 0)) AS CPA_OUT
    , MAX(COALESCE(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20, 2)), 0)) AS REVSHARE_IN
    , COALESCE((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1), 0) AS REVSHARE_OUT
    , SUM(fct.CPA_INCOME_CNT) AS CPA_CNT
    , SUM(fct.CPA_INCOME_EUR) AS CPA_INCOME_EUR
    , SUM(fct.CPA_PAYMENT_EUR) AS CPA_PAYOUT_EUR
    , SUM(fct.CPA_INCOME_EUR) - SUM(CPA_PAYMENT_EUR) AS CPA_REVENUE_EUR
  FROM {{ ref('FACT_OFFER') }} AS fct
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(fct.clickid) = UPPER(pstbk.POST_CLICKID)
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  WHERE
    fct.CPA_INCOME_CNT = 1
    {{ incremental_clause_with_custom_column('Event_Date', this, 'DATE(fct.CPA_DATE)') }}
  GROUP BY ALL
)

, OPERATOR_REVENUE AS (
  SELECT
    fct.DATE AS EVENT_DATE
    , fct.PUBLISHER_NAME
    , fct.ADVERTISER_NAME
    , fct.BRAND_NAME
    , fct.COUNTRY
    , a.CAMP_NAME AS Campaign_Name
    , CASE WHEN TRIM(fct.ClickID) = '' THEN NULL ELSE fct.ClickID END AS ClickID
    , MAX(COALESCE(fct.CAMPAIGN_BASELINE, 0)) AS Campaign_Baseline
    , MAX(COALESCE(TRY_TO_NUMBER(REPLACE(a.CAMP_WAGER_BASELINE, '$', '')), 0))
      AS Campaign_Wager_Baseline
    , MAX(COALESCE(fct.CPA_IN_EUR, 0)) AS CPA_IN
    , MAX(COALESCE(fct.CPA_OUT_EUR, 0)) AS CPA_OUT
    , MAX(COALESCE(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20, 2)), 0)) AS REVSHARE_IN
    , COALESCE((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1), 0) AS REVSHARE_OUT
    , SUM(fct.DEPOSIT_CNT) AS DEPOSIT_CNT
    , SUM(fct.DEPOSIT_AMT_EUR) AS DEPOSIT_AMT_EUR
    , SUM(fct.NET_REVENUE_AMT_EUR) AS NET_REVENUE_AMT_EUR
  FROM {{ ref('FACT_OFFER') }} AS fct
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(fct.clickid) = UPPER(pstbk.POST_CLICKID)
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
    ON
      cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
      {{ incremental_clause_with_custom_column('Event_Date', this, 'DATE(fct.DATE)') }}
  GROUP BY ALL
)

, OPERATOR_CONSOLIDATION AS (
  SELECT
    COALESCE(reg.Event_Date, ftd.Event_Date, rev.Event_Date, brc.Event_Date, cpa.Event_Date) AS Event_Date
    , COALESCE(reg.PUBLISHER_NAME, ftd.PUBLISHER_NAME, rev.PUBLISHER_NAME, brc.PUBLISHER_NAME, cpa.PUBLISHER_NAME)
      AS PUBLISHER_NAME
    , COALESCE(
      reg.ADVERTISER_NAME, ftd.ADVERTISER_NAME, rev.ADVERTISER_NAME, brc.ADVERTISER_NAME, cpa.ADVERTISER_NAME
    ) AS ADVERTISER_NAME
    , COALESCE(reg.BRAND_NAME, ftd.BRAND_NAME, rev.BRAND_NAME, brc.BRAND_NAME, cpa.BRAND_NAME) AS BRAND_NAME
    , COALESCE(reg.COUNTRY, ftd.COUNTRY, rev.COUNTRY, brc.COUNTRY, cpa.COUNTRY) AS COUNTRY
    , COALESCE(reg.Campaign_Name, ftd.Campaign_Name, rev.Campaign_Name, brc.Campaign_Name, cpa.Campaign_Name)
      AS Campaign_Name
    , COALESCE(reg.ClickID, ftd.ClickID, rev.ClickID, brc.ClickID, cpa.ClickID) AS ClickID
    , COALESCE(
      reg.Campaign_Baseline, ftd.Campaign_Baseline, rev.Campaign_Baseline, brc.Campaign_Baseline, cpa.Campaign_Baseline
    ) AS Campaign_Baseline
    , COALESCE(
      reg.Campaign_Wager_Baseline
      , ftd.Campaign_Wager_Baseline
      , rev.Campaign_Wager_Baseline
      , brc.Campaign_Wager_Baseline
      , cpa.Campaign_Wager_Baseline
    ) AS Campaign_Wager_Baseline
    , COALESCE(reg.CPA_In, ftd.CPA_In, rev.CPA_In, brc.CPA_In, cpa.CPA_In) AS CPA_In
    , COALESCE(reg.CPA_Out, ftd.CPA_Out, rev.CPA_Out, brc.CPA_Out, cpa.CPA_Out) AS CPA_Out
    , COALESCE(reg.RevShare_In, ftd.RevShare_In, rev.RevShare_In, brc.RevShare_In, cpa.RevShare_In) AS RevShare_In
    , COALESCE(reg.RevShare_Out, ftd.RevShare_Out, rev.RevShare_Out, brc.RevShare_Out, cpa.RevShare_Out) AS RevShare_Out
    , COALESCE(SUM(brc.Click_Total), 0) AS Click_Total
    , COALESCE(SUM(reg.SIGNUP_CNT), 0) AS SIGNUP_CNT
    , COALESCE(SUM(ftd.FTD_CNT), 0) AS FTD_CNT
    , COALESCE(SUM(rev.DEPOSIT_CNT), 0) AS DEPOSIT_CNT
    , COALESCE(SUM(cpa.CPA_CNT), 0) AS CPA_CNT
    , COALESCE(SUM(rev.DEPOSIT_AMT_EUR), 0) AS DEPOSIT_AMT_EUR
    , COALESCE(SUM(rev.NET_REVENUE_AMT_EUR), 0) AS NET_REVENUE_AMT_EUR
    , COALESCE(SUM(cpa.CPA_INCOME_EUR), 0) AS CPA_INCOME_EUR
    , COALESCE(SUM(cpa.CPA_PAYOUT_EUR), 0) AS CPA_PAYOUT_EUR
    , COALESCE(SUM(cpa.CPA_REVENUE_EUR), 0) AS CPA_REVENUE_EUR
  FROM
    OPERATOR_REGISTRATION AS reg
  FULL OUTER JOIN OPERATOR_FIRSTDEPOSIT AS ftd
    ON reg.Event_Date = ftd.Event_Date AND reg.ClickID = ftd.ClickID
  FULL OUTER JOIN OPERATOR_CPA AS cpa
    ON reg.Event_Date = cpa.Event_Date AND reg.ClickID = cpa.ClickID
  FULL OUTER JOIN OPERATOR_REVENUE AS rev
    ON reg.Event_Date = rev.Event_Date AND reg.ClickID = rev.ClickID
  LEFT OUTER JOIN BRC_CLICKS AS brc
    ON reg.Event_Date = ftd.Event_Date AND UPPER(reg.ClickID) = UPPER(brc.ClickID)
  GROUP BY ALL
)

, final_cte AS (
  SELECT
    COALESCE(brc.Event_Date, ops.Event_Date) AS Event_Date
    , COALESCE(brc.PUBLISHER_NAME, ops.PUBLISHER_NAME) AS PUBLISHER_NAME
    , COALESCE(brc.ADVERTISER_NAME, ops.ADVERTISER_NAME) AS ADVERTISER_NAME
    , COALESCE(brc.BRAND_NAME, ops.BRAND_NAME) AS BRAND_NAME
    , COALESCE(brc.COUNTRY, ops.COUNTRY) AS COUNTRY
    , COALESCE(brc.Campaign_Name, ops.Campaign_Name) AS Campaign_Name
    , COALESCE(brc.ClickID, ops.ClickID) AS ClickID
    , COALESCE(brc.Campaign_Baseline, ops.Campaign_Baseline) AS Campaign_Baseline
    , COALESCE(brc.Campaign_Wager_Baseline, ops.Campaign_Wager_Baseline) AS Campaign_Wager_Baseline
    , COALESCE(brc.CPA_In, ops.CPA_In) AS CPA_In
    , COALESCE(brc.CPA_Out, ops.CPA_Out) AS CPA_Out
    , COALESCE(brc.RevShare_In, ops.RevShare_In) AS RevShare_In
    , COALESCE(brc.RevShare_Out, ops.RevShare_Out) AS RevShare_Out
    , pstbk.POST_SUBID AS SubID
    , pstbk.POST_SUBID2 AS SubID2
    , pstbk.POST_SUBID3 AS SubID3
    , pstbk.POST_SUBID4 AS SubID4
    , pstbk.POST_SUBID5 AS SubID5
    , pstbk.POST_ADGROUPID AS AdGroupID
    , pstbk.POST_FBCLID AS FBCLID
    , pstbk.POST_GCLID AS GCLID
    , ev.POEV_MSCLKID AS MSCLKID
    , ev.POEV_TABCLID AS TABCLID
    , ev.POEV_TWCLID AS TWCLID
    , pstbk.POST_CAMPAIGNID AS AdsCampaignID
    , COALESCE(SUM(brc.CLICK_TOTAL), 0) AS Click_Total
    , COALESCE(SUM(ops.FTD_Cnt), 0) AS FTD_Cnt
    , COALESCE(SUM(ops.SIGNUP_CNT), 0) AS Signup_Cnt
    , COALESCE(SUM(ops.CPA_CNT), 0) AS CPA_CNT
    , COALESCE(SUM(ops.CPA_INCOME_EUR), 0) AS CPA_Income
    , COALESCE(SUM(ops.CPA_PAYOUT_EUR), 0) AS CPA_Payout
    , COALESCE(SUM(ops.CPA_REVENUE_EUR), 0) AS CPA_Revenue
    , COALESCE(SUM(ops.DEPOSIT_CNT), 0) AS Deposit_Cnt
    , CAST(COALESCE(SUM(ops.DEPOSIT_AMT_EUR), 0) AS DECIMAL(20, 2)) AS Deposit_Amt
    , COALESCE(SUM(ops.NET_REVENUE_AMT_EUR), 0) AS Net_Revenue_Amt
    , COALESCE(CASE
      WHEN
        SUM(ops.NET_REVENUE_AMT_EUR) <> 0
        THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(brc.RevShare_In) / 100) AS DECIMAL(20, 2))
      ELSE 0
    END, 0) AS RevShare_Income
    , COALESCE(CASE
      WHEN
        SUM(ops.NET_REVENUE_AMT_EUR) <> 0
        THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.REVSHARE_OUT) / 100) AS DECIMAL(20, 2))
      ELSE 0
    END, 0) AS RevShare_Payout
    , COALESCE(CASE
      WHEN
        SUM(ops.NET_REVENUE_AMT_EUR) <> 0
        THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(brc.RevShare_In) / 100) AS DECIMAL(20, 2))
      ELSE 0 END, 0) - COALESCE(CASE
      WHEN
        SUM(ops.NET_REVENUE_AMT_EUR) <> 0
        THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.REVSHARE_OUT) / 100) AS DECIMAL(20, 2))
      ELSE 0
    END, 0) AS RevShare_Revenue
  FROM
    OPERATOR_CONSOLIDATION AS ops
  LEFT OUTER JOIN BRC_CLICKS AS brc
    ON ops.Event_Date = brc.Event_Date AND ops.ClickID = brc.ClickID
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(ops.ClickID) = UPPER(pstbk.Post_ClickID)
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_EXTRA_VARIABLES') }} AS ev
    ON UPPER(pstbk.POST_CLICKID) = UPPER(ev.POEV_CLICKID)
  GROUP BY ALL

)

-- Deduplicate to avoid duplicate unique keys
, final_cte_dedup AS (
  SELECT
    *
    , ROW_NUMBER() OVER (
        PARTITION BY Event_Date, PUBLISHER_NAME, ADVERTISER_NAME, BRAND_NAME, COUNTRY, Campaign_Name, ClickID
        ORDER BY Click_Total DESC, FTD_Cnt DESC
      ) AS rn
  FROM final_cte
)

SELECT
{{ dbt_utils.surrogate_key([
        'EVENT_DATE',
        'PUBLISHER_NAME',
        'ADVERTISER_NAME',
        'BRAND_NAME',
        'COUNTRY',
        'CAMPAIGN_NAME',
        'CLICKID'
      ])
}} AS UNIQUE_KEY
  , Event_Date
  , PUBLISHER_NAME
  , ADVERTISER_NAME
  , BRAND_NAME
  , COUNTRY
  , Campaign_Name
  , ClickID
  , Campaign_Baseline
  , Campaign_Wager_Baseline
  , CPA_In
  , CPA_Out
  , RevShare_In
  , RevShare_Out
  , SubID
  , SubID2
  , SubID3
  , SubID4
  , SubID5
  , AdGroupID
  , FBCLID
  , GCLID
  , MSCLKID
  , TABCLID
  , TWCLID
  , AdsCampaignID
  , Click_Total
  , FTD_Cnt
  , Signup_Cnt
  , CPA_CNT
  , CPA_Income
  , CPA_Payout
  , CPA_Revenue
  , Deposit_Cnt
  , Deposit_Amt
  , Net_Revenue_Amt
  , RevShare_Income
  , RevShare_Payout
  , RevShare_Revenue
FROM final_cte_dedup
WHERE rn = 1
