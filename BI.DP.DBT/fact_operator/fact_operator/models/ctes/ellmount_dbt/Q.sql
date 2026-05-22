{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
WITH QPlatform_Temp AS (
    SELECT
      date(ops.TRANSACTION_DATE) as TRANSACTION_DATE,
      ops.AN_ID,
      max(START_DATE) as START_DATE,
      max(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
    FROM {{ source('Q_PLATFORM', 'UTM_CODE_REPORT') }} ops
    group by all
)
SELECT
    date(ops.TRANSACTION_DATE) as Date,
    Case when ops1.SIGNUP_DATE IS NOT NULL then ops1.SIGNUP_DATE
         when ops1.SIGNUP_DATE is NULL and ops1.FTD_Date is not null then ops1.FTD_Date
         else NULL end as SignUp_Date,
    ops1.FTD_Date,
    pstbk.POST_FTD_DATE as FTD_Date_Agg,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    ops.AN_ID as ClickId,
    1 as Click_Cnt,
    sum(Case when ops1.SIGNUP_DATE is not null then 1
         when ops1.SIGNUP_DATE is null and ops1.FTD_Date is not null then 1
         else 0 end) as SignUp_Cnt,
    sum(Case when ops1.FTD_Date is not null then 1 else 0 end) as FTD_Cnt,
    sum(Case when ops1.FTD_Date is not null then ops.DEPOSITS else 0 end) as FTD_Amt,
    ops.WITHDRAWALS as Withdrawal_Amt,
    0.00 as Commission_Amt,
    sum(Case when ops.DEPOSITS > 0 then 1 else 0 end) as Deposit_Cnt,
    sum(IFNULL(ops.DEPOSITS,0)) as Deposit_Amt,
    0.00 as Net_Deposit_Amt,
    sum(IFNULL(ops.NGR,0)) as Net_Revenue_Amt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    'Q Platform' as Operator_Platform
  , NULL AS Source_Currency
  , ops._AIRBYTE_EMITTED_AT
FROM {{ source('Q_PLATFORM', 'UTM_CODE_REPORT') }} ops
join QPlatform_Temp tmp
on ops.AN_ID = tmp.AN_ID and date(ops.TRANSACTION_DATE) = tmp.TRANSACTION_DATE and ops.START_DATE = tmp.START_DATE
LEFT OUTER JOIN
  (
    SELECT
      AN_ID AS CLICKID
      , pstbk.POST_SIGNUP_DATE AS SIGNUP_DATE
      , pstbk.POST_FTD_DATE AS FTD_DATE
      , MIN(DATE(ops.TRANSACTION_DATE)) AS DATE
    FROM {{ source('Q_PLATFORM', 'UTM_CODE_REPORT') }} ops
    LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
      ON UPPER(ops.AN_ID) = UPPER(pstbk.POST_CLICKID)
    WHERE pstbk.POST_SIGNUP_DATE <= DATE(ops.TRANSACTION_DATE) OR pstbk.POST_FTD_DATE <= DATE(ops.TRANSACTION_DATE)
    GROUP BY 1, 2, 3
  ) AS ops1
  ON ops.AN_ID = ops1.CLICKID AND ops.TRANSACTION_DATE = ops1.DATE
LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
  ON UPPER(ops.AN_ID) = UPPER(pstbk.POST_CLICKID)
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
