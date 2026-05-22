{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema

WITH  
CX_Reg_ClickID_Parse_Start_Position as 
(
    Select 
    AFP,
    tracker_login_id,
    userid,
    POSITION('trk', AFP) AS start_pos,
    substr(AFP,POSITION('trk', AFP),100) as ClickID
    FROM {{ source('CELLXPERT', 'ICT_FTD_REGISTRATION_REPORT') }}
    group by all
)
,CX_Reg_ClickID_Parse_Last_Position as 
(
select
AFP,
tracker_login_id,
userid,
ClickID as Start_ClickID,
Case when AFP like '%trk%' then substr(clickid,1,REGEXP_INSTR(clickid, 'trk', 1, 2)+2) else AFP end as ClickID
from CX_Reg_ClickID_Parse_Start_Position 
group by all
)
,CX_Reg_ClickID as 
(
select 
AFP,
ClickID,
tracker_login_id,
userid,
from CX_Reg_ClickID_Parse_Last_Position
group by all
)
,CellXpertRegistration_Temp AS (
  SELECT
      ClickID
    , reg.TRACKER_LOGIN_ID
    , MIN(DATE(DATE)) AS DATE
    , SUM(FIRST_DEPOSIT) AS FIRST_DEPOSIT
    , MIN(DATE(REGISTRATION_DATE)) AS REGISTRATION_DATE
    , CASE
      WHEN reg.TRACKER_LOGIN_ID = 5485 AND SUM(reg.COMMISSIONS) > 0 THEN MIN(DATE(reg.QUALIFICATION_DATE)) ELSE
        MIN(DATE(FIRST_DEPOSIT_DATE))
    END AS FIRST_DEPOSIT_DATE
    , MAX(reg._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ source('CELLXPERT', 'ICT_FTD_REGISTRATION_REPORT') }} AS reg
  join CX_Reg_ClickID cx 
     on reg.afp = cx.afp  and reg.tracker_login_id = cx.tracker_login_id and reg.userid = cx.userid
  GROUP BY ALL
)

, CellXpertRegistration AS (
  SELECT DISTINCT
    reg.Date
    , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
    , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
    , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
    , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
    , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
    , CASE
      WHEN reg.REGISTRATION_DATE IS NOT NULL THEN reg.REGISTRATION_DATE
      WHEN
        reg.REGISTRATION_DATE IS NULL
        AND reg.First_Deposit_Date IS NOT NULL THEN reg.First_Deposit_Date
      ELSE reg.REGISTRATION_DATE
    END AS SignUp_Date
    , reg.FIRST_DEPOSIT_DATE AS FTD_Date
    , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
    , reg.ClickId
    , COALESCE(reg.First_Deposit, 0.00) AS FTD_Amt
    , CASE
      WHEN reg.First_Deposit_Date IS NOT NULL THEN 1
      ELSE 0
    END AS FTD_Cnt
    , CASE
      WHEN reg.REGISTRATION_DATE IS NOT NULL THEN 1
      WHEN
        reg.REGISTRATION_DATE IS NULL
        AND reg.First_Deposit_Date IS NOT NULL THEN 1
      ELSE 0
    END AS Signup_Cnt
    , reg.TRACKER_LOGIN_ID AS Tracker_Login_Id
    , trk.TLOG_USERNAME AS Tracker_UserName
    , 'CellXpert' AS Operator_Platform
    , NULL AS Source_Currency
    , reg._AIRBYTE_EMITTED_AT
  FROM CellXpertRegistration_Temp AS reg
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(reg.ClickID) = UPPER(pstbk.POST_CLICKID)
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
  WHERE reg.TRACKER_LOGIN_ID <> 4428
  group by all
)
,CX_Dyn_ClickID_Parse_Start_Position as 
(
    Select 
    AFP,
    tracker_login_id,
    userid,
    POSITION('trk', AFP) AS start_pos,
    substr(AFP,POSITION('trk', AFP),100) as ClickID
    from {{ source('CELLXPERT', 'DYNAMIC_VARIABLES_REPORT') }}
    group by all
)

,CX_Dyn_ClickID_Parse_Last_Position as 
(
select
AFP,
tracker_login_id,
userid,
ClickID as Start_ClickID,
Case when AFP like '%trk%' then substr(clickid,1,REGEXP_INSTR(clickid, 'trk', 1, 2)+2) else AFP end as ClickID
from CX_Dyn_ClickID_Parse_Start_Position 
group by all
)

,CX_Dyn_ClickID as 
(
select 
AFP,
ClickID,
tracker_login_id,
userid,
from CX_Dyn_ClickID_Parse_Last_Position
group by all
)

, CellXpertDynamicVariable_Temp AS (
  SELECT
    DATE(dyn.Date) AS Date
    ,cx.ClickId
    , dyn.userid
    , COUNT(dyn.AFP) AS Click_Cnt
    , SUM(COALESCE(dyn.DEPOSITS, 0.00)) AS Deposit_Amt
    , SUM(CASE WHEN dyn.DEPOSITS > 0 THEN 1 ELSE 0 END) AS Deposit_Cnt
    , COALESCE(SUM(dyn.Net_Deposits), 0) AS Net_Deposit_Amt
    , SUM(dyn.PL) AS Net_Revenue_Amt
    , SUM(dyn.Withdrawals) AS Withdrawal_Amt
    , SUM(dyn.Commissions) AS Commission_Amt
    , dyn.TRACKER_LOGIN_ID AS Tracker_Login_Id
    , 'CellXpert' AS Operator_Platform
    , NULL AS Source_Currency
    , MAX(dyn._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ source('CELLXPERT', 'DYNAMIC_VARIABLES_REPORT') }} AS dyn
  join CX_Dyn_ClickID cx 
     on dyn.afp = cx.afp and dyn.tracker_login_id = cx.tracker_login_id and dyn.userid = cx.userid
  WHERE dyn.TRACKER_LOGIN_ID <> 4428
  GROUP BY ALL
)

, CellXpertDynamicVariable AS (
  SELECT
    dyn.Date
    , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
    , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
    , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
    , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
    , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
    , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
    , dyn.ClickId
    , SUM(dyn.Click_Cnt) AS Click_Cnt
    , SUM(dyn.Deposit_Amt) AS Deposit_Amt
    , SUM(dyn.Deposit_Cnt) AS Deposit_Cnt
    , SUM(dyn.Net_Deposit_Amt) AS Net_Deposit_Amt
    , SUM(dyn.Net_Revenue_Amt) AS Net_Revenue_Amt
    , SUM(dyn.Withdrawal_Amt) AS Withdrawal_Amt
    , SUM(dyn.Commission_Amt) AS Commission_Amt
    , dyn.Tracker_Login_Id
    , trk.TLOG_USERNAME AS Tracker_UserName
    , 'CellXpert' AS Operator_Platform
    , NULL AS Source_Currency
    , MAX(dyn._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM CellXpertDynamicVariable_Temp AS dyn
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(dyn.ClickId) = UPPER(pstbk.POST_CLICKID)
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
  WHERE dyn.TRACKER_LOGIN_ID <> 4428
  GROUP BY ALL
)

, CellXpert_Con AS (
  SELECT
    COALESCE(reg.DATE, dyn.DATE) AS DATE
    , reg.SignUp_Date
    , reg.FTD_Date
    , pstbk.POST_FTD_DATE AS FTD_Date_Agg
    , COALESCE(reg.Country, dyn.Country) AS Country
    , COALESCE(reg.Publisher_Name, dyn.Publisher_Name) AS Publisher_Name
    , COALESCE(reg.Advertiser_ID, dyn.Advertiser_ID) AS Advertiser_ID
    , COALESCE(reg.Advertiser_Name, dyn.Advertiser_Name) AS Advertiser_Name
    , COALESCE(reg.Brand_Name, dyn.Brand_Name) AS Brand_Name
    , COALESCE(reg.Player_IPAddress, dyn.Player_IPAddress) AS Player_IPAddress
    , COALESCE(reg.ClickId, dyn.ClickId) AS ClickId
    , 1 AS Click_Cnt
    , COALESCE(SUM(reg.Signup_Cnt), 0) AS Signup_Cnt
    , COALESCE(SUM(reg.FTD_Cnt), 0) AS FTD_Cnt
    , COALESCE(SUM(reg.FTD_AMT), 0) AS FTD_Amt
    , COALESCE(SUM(dyn.Withdrawal_Amt), 0) AS Withdrawal_Amt
    , COALESCE(SUM(dyn.Commission_Amt), 0) AS Commission_Amt
    , COALESCE(SUM(dyn.DEPOSIT_CNT), 0) AS DEPOSIT_CNT
    , COALESCE(SUM(dyn.DEPOSIT_AMT), 0) AS DEPOSIT_AMT
    , COALESCE(SUM(dyn.NET_DEPOSIT_AMT), 0) AS NET_DEPOSIT_AMT
    , COALESCE(SUM(dyn.NET_REVENUE_AMT), 0) AS NET_REVENUE_AMT
    , COALESCE(reg.Tracker_Login_Id, dyn.Tracker_Login_Id) AS Tracker_Login_Id
    , COALESCE(reg.Tracker_UserName, dyn.Tracker_UserName) AS Tracker_UserName
    , COALESCE(reg.Operator_Platform, dyn.Operator_Platform) AS Operator_Platform
    , COALESCE(reg.Source_Currency, dyn.Source_Currency) AS Source_Currency
    , MAX(dyn._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM CellXpertDynamicVariable AS dyn
  FULL OUTER JOIN CellXpertRegistration AS reg
    ON dyn.DATE = reg.DATE AND dyn.ClickID = reg.ClickID AND dyn.Tracker_Login_Id = reg.Tracker_Login_Id
  LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
    ON UPPER(reg.ClickID) = UPPER(pstbk.POST_CLICKID)
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
FROM CellXpert_Con
