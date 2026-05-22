{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- Deduplicate by keeping the row with MAX(_AIRBYTE_EMITTED_AT) per unique key (DATE, CLICK_ID, TRACKER_LOGIN_ID)
WITH mexos_grouped AS (
  SELECT
    TO_DATE(ops.DATE) AS Date
    , CASE
      WHEN ops.CASINO_SIGNUPS_CNT > 0 THEN TO_DATE(ops.DATE)
      WHEN
        ops.CASINO_SIGNUPS_CNT = 0
        AND ops.CASINO_RFD_CNT > 0 THEN TO_DATE(ops.DATE)
    END AS SignUp_Date
    , CASE
      WHEN ops.CASINO_RFD_CNT = 1 THEN TO_DATE(ops.DATE)
    END AS FTD_Date
    , pstbk.POST_FTD_DATE AS FTD_Date_Agg
    , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
    , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
    , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
    , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
    , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
    , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
    , ops.CLICK_ID AS ClickId
    , 1 AS Click_Cnt
    , CASE
      WHEN ops.CASINO_SIGNUPS_CNT = 0 AND ops.CASINO_RFD_CNT > 0 THEN 1
      ELSE ops.CASINO_SIGNUPS_CNT
    END AS Signup_Cnt
    , ops.CASINO_RFD_CNT AS FTD_Cnt
    , TO_NUMBER(COALESCE(ops.CASINO_RFD_AMT, 0), 32, 2) AS FTD_Amt
    , TO_NUMBER(WITHDRAWAL_AMT, 32, 2) AS Withdrawal_Amt
    , TO_NUMBER(COMMISSION, 32, 2) AS Commission_Amt
    , TOTAL_DEPOSIT_CNT AS Deposit_Cnt
    , TOTAL_DEPOSIT_AMT AS Deposit_Amt
    , 0.00 AS Net_Deposit_Amt
    , COALESCE(ops.NET_GAMING_AFTER_DEDUCTION, 0) AS Net_Revenue_Amt
    , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
    , trk.TLOG_USERNAME AS Tracker_UserName
    , 'Mexos' AS Operator_Platform
    , NULL AS Source_Currency
    , ops._AIRBYTE_EMITTED_AT
  FROM {{ source('MEXOS', 'STATISTICS_REPORT') }} AS ops
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(ops.CLICK_ID) = UPPER(pstbk.POST_CLICKID)
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
    ON pstbk.POST_IP = loc.IP
  GROUP BY ALL
),
mexos_base AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY Date, ClickId, Tracker_Login_Id
      ORDER BY _AIRBYTE_EMITTED_AT DESC
    ) AS rn
  FROM mexos_grouped
)
SELECT
  Date
  , SignUp_Date
  , FTD_Date
  , FTD_Date_Agg
  , Country
  , Publisher_Name
  , Advertiser_ID
  , Advertiser_Name
  , Brand_Name
  , Player_IPAddress
  , ClickId
  , Click_Cnt
  , Signup_Cnt
  , FTD_Cnt
  , FTD_Amt
  , Withdrawal_Amt
  , Commission_Amt
  , Deposit_Cnt
  , Deposit_Amt
  , Net_Deposit_Amt
  , Net_Revenue_Amt
  , Tracker_Login_Id
  , Tracker_UserName
  , Operator_Platform
  , Source_Currency
  , _AIRBYTE_EMITTED_AT
FROM mexos_base
WHERE rn = 1