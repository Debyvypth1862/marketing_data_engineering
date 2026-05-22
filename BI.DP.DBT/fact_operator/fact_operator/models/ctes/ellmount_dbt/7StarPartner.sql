{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
SELECT
  TO_DATE(ops.DATE) AS Date
  , CASE
    WHEN ops.SIGNUPS > 0 THEN TO_DATE(ops.DATE)
    WHEN
      ops.SIGNUPS = 0
      AND ops."First Time Depositing Customers" = 1 THEN TO_DATE(ops.DATE)
  END AS SignUp_Date
  , CASE
    WHEN ops."First Time Depositing Customers" = 1 THEN TO_DATE(ops.DATE)
  END AS FTD_Date
  , pstbk.POST_FTD_DATE AS FTD_Date_Agg
  , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
  , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
  , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
  , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
  , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
  , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
  , ops.VAR1 AS ClickId
  , 1 AS Click_Cnt
  , CASE
    WHEN ops.SIGNUPS > 0 THEN 1
    WHEN
      ops.SIGNUPS = 0
      AND ops."First Time Depositing Customers" = 1 THEN 1
    ELSE 0
  END AS Signup_Cnt
  , CASE
    WHEN ops."First Time Depositing Customers" = 1 THEN 1
    ELSE 0
  END AS FTD_Cnt
  , TO_NUMBER(
    CASE
      WHEN ops."First Time Depositing Customers" = 1 THEN ops.DEPOSITS
      ELSE 0.00
    END, 38, 2
  ) AS FTD_Amt
  , 0.00 AS Withdrawal_Amt
  , 0.00 AS Commission_Amt
  , CASE WHEN ops.DEPOSITS > 0 THEN 1 ELSE 0 END AS Deposit_Cnt
  , TO_NUMBER(
    COALESCE(ops.DEPOSITS, 0.00), 32, 2
  ) AS Deposit_Amt
  , 0.00 AS Net_Deposit_Amt
  , TO_NUMBER(
    COALESCE(ops."Net Revenue", 0.00), 38, 2
  ) AS Net_Revenue_Amt
  , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
  , trk.TLOG_USERNAME AS Tracker_UserName
  , '7StarPartner' AS Operator_Platform
  , NULL AS Source_Currency
  , ops._AIRBYTE_EMITTED_AT
FROM {{ source('NETREFER', '7_STAR_DYNAMIC_VARIABLE') }} AS ops
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.VAR1) = UPPER(pstbk.POST_CLICKID)
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