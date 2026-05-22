{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
SELECT
  DATE(ops.TRANSACTION_DATE) AS Date
  , ops1.SignUp_Date
  , ops1.FTD_Date
  , pstbk.POST_FTD_DATE AS FTD_Date_Agg
  , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
  , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
  , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
  , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
  , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
  , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
  , ops.AN_ID1 AS ClickId
  , 1 AS Click_Cnt
  , CASE WHEN ops1.SignUp_Date IS NOT NULL THEN 1 ELSE 0 END AS Signup_Cnt
  , CASE WHEN ops1.FTD_Date IS NOT NULL THEN 1 ELSE 0 END AS FTD_Cnt
  , 0.00 AS FTD_Amt
  , COALESCE(WITHDRAWALS, 0) AS Withdrawal_Amt
  , 0.00 AS Commission_Amt
  , 0 AS Deposit_Cnt
  , 0.00 AS Deposit_Amt
  , 0.00 AS Net_Deposit_Amt
  , 0.00 AS Net_Revenue_Amt
  , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
  , trk.TLOG_USERNAME AS Tracker_UserName
  , 'AffTech' AS Operator_Platform
  , NULL AS Source_Currency
  , ops._AIRBYTE_EMITTED_AT
FROM {{ source('AFFTECH', 'AFFILIATE_STATISTICS_REPORTS') }} AS ops
LEFT OUTER JOIN
  (
    SELECT
      AN_ID1 AS CLICKID
      , pstbk.POST_SIGNUP_DATE AS SIGNUP_DATE
      , pstbk.POST_FTD_DATE AS FTD_DATE
      , MIN(DATE(ops.TRANSACTION_DATE)) AS DATE
    FROM {{ source('AFFTECH', 'AFFILIATE_STATISTICS_REPORTS') }} AS ops
    LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
      ON UPPER(ops.AN_ID1) = UPPER(pstbk.POST_CLICKID)
    WHERE pstbk.POST_SIGNUP_DATE <= DATE(ops.TRANSACTION_DATE) OR pstbk.POST_FTD_DATE <= DATE(ops.TRANSACTION_DATE)
    GROUP BY 1, 2, 3
  ) AS ops1
  ON ops.TRANSACTION_DATE = ops1.Date AND ops.AN_ID1 = ops1.CLICKID
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.AN_ID1) = UPPER(pstbk.POST_CLICKID)
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