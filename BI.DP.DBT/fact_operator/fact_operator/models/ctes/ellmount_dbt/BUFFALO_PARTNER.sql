{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
WITH BuffaloPartner_RegFTD AS (
  SELECT
    media
    , DATE(SUBSTR(date_opened, 7, 4) || '-' || SUBSTR(date_opened, 4, 2) || '-' || SUBSTR(date_opened, 1, 2))
      AS DATE_OPENED
    , DATE(
      SUBSTR(date_first_deposited, 7, 4)
      || '-'
      || SUBSTR(date_first_deposited, 4, 2)
      || '-'
      || SUBSTR(date_first_deposited, 1, 2)
    ) AS Date_First_Deposited
    , FIRST_DEPOSIT_AMOUNT
    , MIN(DATE) AS DATE
  FROM {{ source('BUFFALO_PARTNERS', 'REV_SHARE_REPORT') }}
  WHERE (
    DATE(SUBSTR(date_opened, 7, 4) || '-' || SUBSTR(date_opened, 4, 2) || '-' || SUBSTR(date_opened, 1, 2))
    <= DATE(DATE)
    OR DATE(
      SUBSTR(date_first_deposited, 7, 4)
      || '-'
      || SUBSTR(date_first_deposited, 4, 2)
      || '-'
      || SUBSTR(date_first_deposited, 1, 2)
    )
    <= DATE(DATE)
  )
  AND media <> ''
  GROUP BY 1, 2, 3, 4
)

, BuffaloPartner AS (
  SELECT
    TO_DATE(ops.DATE) AS Date
    , ops1.DATE_OPENED AS SignUp_Date
    , ops1.Date_First_Deposited AS FTD_Date
    , DATE(
      SUBSTR(ops.date_first_deposited, 7, 4)
      || '-'
      || SUBSTR(ops.date_first_deposited, 4, 2)
      || '-'
      || SUBSTR(ops.date_first_deposited, 1, 2)
    ) AS FTD_Date_Agg
    , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
    , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
    , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
    , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
    , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
    , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
    , ops.MEDIA AS ClickId
    , 1 AS Click_Cnt
    , CASE
      WHEN ops1.Date_Opened IS NOT NULL THEN 1
      ELSE 0 END
      AS Signup_Cnt
    , CASE WHEN ops1.Date_First_Deposited IS NOT NULL THEN 1 ELSE 0 END AS FTD_Cnt
    , ops1.FIRST_DEPOSIT_AMOUNT AS FTD_Amt
    , 0.00 AS Withdrawal_Amt
    , 0.00 AS Commission_Amt
    , CASE WHEN ops.DEPOSITS > 0 THEN 1 ELSE 0 END AS Deposit_Cnt
    , COALESCE(ops.DEPOSITS, 0) AS Deposit_Amt
    , 0.00 AS Net_Deposit_Amt
    , COALESCE(ops.NET_REVENUE, 0) AS Net_Revenue_Amt
    , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
    , trk.TLOG_USERNAME AS Tracker_UserName
    , 'Buffalo Partners' AS Operator_Platform
    , NULL AS Source_Currency
    , ops._AIRBYTE_EMITTED_AT
  FROM {{ source('BUFFALO_PARTNERS', 'REV_SHARE_REPORT') }} AS ops
  LEFT OUTER JOIN BuffaloPartner_RegFTD AS ops1
    ON ops.MEDIA = ops1.MEDIA AND DATE(ops.DATE) = DATE(ops1.DATE)
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(ops.MEDIA) = UPPER(pstbk.POST_CLICKID)
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
FROM BuffaloPartner
