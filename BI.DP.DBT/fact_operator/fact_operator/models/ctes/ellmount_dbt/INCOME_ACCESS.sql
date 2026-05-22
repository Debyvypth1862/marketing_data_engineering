{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
WITH IncomeAccess_Reg AS (
  SELECT
    AFF_CUSTOM_ID
    , MIN(DATE) AS DATE
    , MIN(DATE(REGISTRATION_DATE)) AS REGISTRATION_DATE
  FROM {{ source('INCOME_ACCESS', 'ACCOUNT_REPORT') }}
  WHERE
    DATE(REGISTRATION_DATE) <= DATE
  GROUP BY 1
)

, IncomeAccess_FTD AS (
  SELECT
    AFF_CUSTOM_ID
    , MIN(DATE) AS FIRST_DEPOSIT
  FROM {{ source('INCOME_ACCESS', 'ACID_REPORT') }}
  WHERE PURCHASES > 0
  GROUP BY 1
)

, IncomeAccess AS (
  SELECT
    TO_DATE(ops.DATE) AS Date
    , ops1.REGISTRATION_DATE AS SignUp_Date
    , ops2.FIRST_DEPOSIT AS FTD_Date
    , CASE WHEN ops3.FIRST_DEPOSIT <= TO_DATE(ops.DATE) THEN ops3.FIRST_DEPOSIT END AS FTD_Date_Agg
    , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
    , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
    , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
    , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
    , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
    , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
    , ops.AFF_CUSTOM_ID AS ClickId
    , 1 AS Click_Cnt
    , CASE WHEN ops1.REGISTRATION_DATE IS NOT NULL THEN 1 ELSE 0 END AS Signup_Cnt
    , CASE WHEN ops2.FIRST_DEPOSIT IS NOT NULL THEN 1 ELSE 0 END AS FTD_Cnt
    , COALESCE(CASE WHEN ops2.FIRST_DEPOSIT IS NOT NULL THEN ops.PURCHASES ELSE 0 END, 0) AS FTD_Amt
    , 0.00 AS Withdrawal_Amt
    , COALESCE(ops.TOTAL_COMMISSION, 0) AS Commission_Amt
    , CASE WHEN ops.PURCHASES > 0 THEN 1 ELSE 0 END AS Deposit_Cnt
    , COALESCE(ops.PURCHASES, 0) AS Deposit_Amt
    , 0.00 AS Net_Deposit_Amt
    , ops.NET_REVENUE AS Net_Revenue_Amt
    , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
    , trk.TLOG_USERNAME AS Tracker_UserName
    , 'Income Access' AS Operator_Platform
    , NULL AS Source_Currency
    , ops._AIRBYTE_EMITTED_AT
  FROM {{ source('INCOME_ACCESS', 'ACID_REPORT') }} AS ops
  LEFT OUTER JOIN IncomeAccess_Reg AS ops1
    ON UPPER(ops.AFF_CUSTOM_ID) = UPPER(ops1.AFF_CUSTOM_ID) AND DATE(ops.DATE) = DATE(ops1.DATE)
  LEFT OUTER JOIN IncomeAccess_FTD AS ops2
    ON UPPER(ops.AFF_CUSTOM_ID) = UPPER(ops2.AFF_CUSTOM_ID) AND DATE(ops.DATE) = DATE(ops2.FIRST_DEPOSIT)
  LEFT OUTER JOIN IncomeAccess_FTD AS ops3
    ON UPPER(ops.AFF_CUSTOM_ID) = UPPER(ops3.AFF_CUSTOM_ID)
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(ops.AFF_CUSTOM_ID) = UPPER(pstbk.POST_CLICKID)
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
)

SELECT
  DATE
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
  , DEPOSIT_CNT
  , DEPOSIT_AMT
  , NET_DEPOSIT_AMT
  , NET_REVENUE_AMT
  , Tracker_Login_Id
  , Tracker_UserName
  , Operator_Platform
  , Source_Currency
  , _AIRBYTE_EMITTED_AT
FROM IncomeAccess
