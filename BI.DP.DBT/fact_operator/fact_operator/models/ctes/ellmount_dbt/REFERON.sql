{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
WITH Referon_Temp AS (
  SELECT
    DATE
    , ClickId
    , TRACKER_LOGIN_ID
    , SUM(REG_COUNT) AS REG_COUNT
    , SUM(FTD_COUNT) AS FTD_COUNT
    , SUM(FTDS_DEPOSITS) AS FTDS_DEPOSITS
    , SUM(DEPOSITS) AS DEPOSITS
    , SUM(NGR) AS NGR
    , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ source('REFERON', 'DYNAMIC_VARIABLES_REPORT') }}
  where (REG_COUNT > 0 or FTD_COUNT >0 or FTDS_DEPOSITS > 0 or DEPOSITS > 0 or NGR <> 0)
  GROUP BY ALL
)

SELECT
  DATE(ops.DATE) AS Date
  , CASE
    WHEN ops.REG_COUNT > 0 THEN DATE(ops.DATE)
  END AS SignUp_Date
  , CASE
    WHEN ops.FTD_COUNT > 0 THEN ops.DATE
  END AS FTD_Date
  , pstbk.POST_FTD_DATE AS FTD_Date_Agg
  , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
  , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
  , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
  , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
  , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
  , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
  , ops.CLICKID AS ClickId
  , SUM(1) AS Click_Cnt
  , SUM(ops.REG_COUNT) AS Signup_Cnt
  , SUM(ops.FTD_COUNT) AS FTD_Cnt
  , SUM(TO_NUMBER(COALESCE(ops.FTDS_DEPOSITS, 0), 38, 2)) AS FTD_Amt
  , SUM(0.00) AS Withdrawal_Amt
  , SUM(0.00) AS Commission_Amt
  , SUM(CASE WHEN ops.DEPOSITS > 0 THEN 1 ELSE 0 END) AS Deposit_Cnt
  , SUM(TO_NUMBER(COALESCE(ops.DEPOSITS, 0), 38, 2)) AS Deposit_Amt
  , SUM(0.00) AS Net_Deposit_Amt
  , SUM(COALESCE(ops.NGR, 0)) AS Net_Revenue_Amt
  , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
  , trk.TLOG_USERNAME AS Tracker_UserName
  , 'Referon' AS Operator_Platform
  , NULL AS Source_Currency
  , MAX(ops._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
FROM Referon_Temp AS ops
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.CLICKID) = UPPER(pstbk.POST_CLICKID)
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