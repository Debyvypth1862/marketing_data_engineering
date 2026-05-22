{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
SELECT
  DATE(ops.DATE) AS Date
  , CASE WHEN ops.REGISTRATION_COUNT > 0 THEN ops.DATE END AS SIGNUP_DATE
  , CASE WHEN ops.FTD_COUNT > 0 THEN ops.DATE END AS FTD_DATE
  , pstbk.POST_FTD_DATE AS FTD_Date_Agg
  , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
  , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
  , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
  , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
  , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
  , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
  , ops.AFP AS ClickId
  , ops.VISIT_COUNT AS Click_Cnt
  , ops.REGISTRATION_COUNT AS Signup_Cnt
  , ops.FTD_COUNT AS FTD_Cnt
  , ops.FTD_TOTAL AS FTD_Amt
  , ops.WITHDRAWAL_TOTAL AS Withdrawal_Amt
  , ops.COMMISSIONS_TOTAL AS Commission_Amt
  , ops.DEPOSIT_COUNT AS Deposit_Cnt
  , ops.NET_DEPOSITS AS Deposit_Amt
  , 0 AS Net_Deposit_Amt
  , ops.NET_PL AS Net_Revenue_Amt
  , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
  , trk.TLOG_USERNAME AS Tracker_UserName
  , 'Smartico' AS Operator_Platform
  , NULL AS Source_Currency
  , ops._AIRBYTE_EMITTED_AT
FROM {{ source('SMARTICO', 'UTM_CODE_REPORT') }} AS ops
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.AFP) = UPPER(pstbk.POST_CLICKID)
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