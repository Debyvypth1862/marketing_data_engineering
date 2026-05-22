{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
SELECT
      date(ops.DATE) as Date,
    Case
          when ops.SIGN_UPS > 0 then to_date(ops.DATE)
          else NULL
    end as SignUp_Date,
    Case
          when ops.FIRST_DEPOSITS_QTY > 0 then to_date(ops.DATE)
          else NULL
    end as FTD_Date,
    pstbk.POST_FTD_DATE as FTD_Date_Agg,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    ops.DYNID as ClickId,
    1 as Click_Cnt,
    sum(ops.SIGN_UPS) as Signup_Cnt,
    sum(ops.FIRST_DEPOSITS_QTY) as FTD_Cnt,
    0.00 as FTD_Amt,
    0.00 as Withdrawal_Amt,
    0.00 as Commission_Amt,
    0 as Deposit_Cnt,
    0.00 as Deposit_Amt,
    0.00 as Net_Deposit_Amt,
    sum(IFNULL(ops.NET_INCOME,0)) as Net_Revenue_Amt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
  'Ego' as Operator_Platform
  , NULL AS Source_Currency
  , MAX(ops._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
FROM {{ source('EGO', 'BRAND_REPORT') }} AS ops
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.DYNID) = UPPER(pstbk.POST_CLICKID)
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
