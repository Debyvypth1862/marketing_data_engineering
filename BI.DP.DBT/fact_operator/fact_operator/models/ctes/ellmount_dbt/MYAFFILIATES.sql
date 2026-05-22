{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
SELECT
  TO_DATE(ops.DATE) AS Date
  , CASE WHEN ops.SIGNUPS = 1 THEN ops.DATE END AS SignUp_Date
  , CASE WHEN (CASE
    WHEN ops.FIRST_DEPOSIT > 0 AND ops.FIRST_DEPOSIT_COUNT IS NULL THEN 1 ELSE ops.FIRST_DEPOSIT_COUNT
  END) > 0 THEN TO_DATE(ops.date) END
    AS FTD_Date
  , pstbk.POST_FTD_DATE AS FTD_Date_Agg
  , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
  , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
  , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
  , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
  , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
  , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
  , ops.PAYLOAD AS ClickId
  , 1 AS Click_Cnt
  , ops.SIGNUPS AS Signup_Cnt
  , CASE
    WHEN ops.FIRST_DEPOSIT > 0 AND ops.FIRST_DEPOSIT_COUNT IS NULL THEN 1 ELSE ops.FIRST_DEPOSIT_COUNT
  END AS FTD_Cnt
  , COALESCE(CASE
    WHEN ops.FIRST_DEPOSIT_COUNT = 1 AND (ops.FIRST_DEPOSIT = 0 OR ops.FIRST_DEPOSIT IS NULL) THEN ops.DEPOSITS ELSE
      ops.FIRST_DEPOSIT
  END, 0) AS FTD_Amt
  , 0.00 AS Withdrawal_Amt
  , 0.00 AS Commission_Amt
  , CASE WHEN ops.DEPOSITS > 0 THEN 1 ELSE 0 END AS Deposit_Cnt
  -- , IFNULL(ops.DEPOSITS,0) + IFNULL(TOTAL_DEPOSITS,0) AS Deposit_Amt
  -- , 0.00 AS Net_Deposit_Amt
  -- , IFNULL(ops.NET_REVENUE,0) + IFNULL(TOTAL_PL,0) + IFNULL(NGR,0) as Net_Revenue_Amt
  , IFNULL(ops.DEPOSITS,0) as Deposit_Amt
  , 0.00 as Net_Deposit_Amt
  , IFNULL(ops.NET_REVENUE,0) as Net_Revenue_Amt
  , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
  , trk.TLOG_USERNAME AS Tracker_UserName
  , 'MyAffiliates' AS Operator_Platform
  , NULL AS Source_Currency
  , ops._AIRBYTE_EMITTED_AT
FROM {{ source('MYAFFILIATES', 'CUSTOMER_REPORT') }} AS ops
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.PAYLOAD) = UPPER(pstbk.POST_CLICKID)
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