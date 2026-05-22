{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
WITH CurrencyRateSW AS (
  SELECT TO_DATE(DATE) as DATE,
    CURRENCY_SOURCE,
    RATE as REVERSE_RATE
    , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ ref('TO_EUR_HISTORICAL') }}
  -- where CURRENCY_SOURCE = 'USD'
  GROUP BY ALL
  order by 1 desc
),

Operator_Currency as 
(
    Select 
        opa.br_tracker_login_id as TRACKER_LOGIN_ID,
        upper(c.abbrev) as OPERATOR_CURRENCY
    from {{ source('BRT', 'OPERATOR_ACCOUNTS') }} opa 
    LEFT outer JOIN {{ source('BRT', 'CURRENCY_OPERATOR_ACCOUNT') }} coa 
        ON TO_NUMBER(opa.ID) = TO_NUMBER(coa.OPERATOR_ACCOUNT_ID)
    LEFT outer JOIN {{ source('BRT', 'CURRENCIES') }} c 
        ON TO_NUMBER(coa.CURRENCY_ID) = TO_NUMBER(c.ID)
    group by all
),

Softswiss_Temp AS (
  SELECT
    Date,
    DYNAMIC_TAG_CLICKID,
    CASE WHEN CURRENCY = 'USDT' THEN 'USD' ELSE CURRENCY END AS Currency,
    VISITS_COUNT,
    REGISTRATIONS_COUNT,
    FIRST_DEPOSITS_COUNT,
    FIRST_DEPOSITS_SUM,
    DEPOSITS_SUM,
    NGR,
    TRACKER_LOGIN_ID,
    _AIRBYTE_EMITTED_AT
  FROM {{ source('SOFTSWISS', 'ACTIVITY_REPORT') }}
  GROUP BY ALL
),
-- SELECT
--   TO_DATE(ops.DATE) AS Date
--   , CASE
--     WHEN ops.REGISTRATIONS_COUNT > 0 THEN TO_DATE(ops.DATE)
--   END AS SignUp_Date
--   , CASE
--     WHEN ops.FIRST_DEPOSITS_COUNT > 0 THEN TO_DATE(ops.DATE)
--   END AS FTD_Date
--   , pstbk.POST_FTD_DATE AS FTD_Date_Agg
--   , COALESCE(loc.COUNTRY_NAME, 'Unknown') AS Country
--   , COALESCE(pub.PUBL_USERNAME, 'Unknown') AS Publisher_Name
--   , COALESCE(trk.TLOG_FK_ADVERTISER, -1) AS Advertiser_ID
--   , COALESCE(adv.ADVE_NAME, 'Unknown') AS Advertiser_Name
--   , COALESCE(b.BRAN_NAME, 'Unknown') AS Brand_Name
--   , SPLIT_PART(pstbk.POST_IP, ',', 1) AS Player_IPAddress
--   , ops.DYNAMIC_TAG_CLICKID AS ClickId
--   , 1 AS Click_Cnt
--   , ops.REGISTRATIONS_COUNT AS Signup_Cnt
--   , ops.FIRST_DEPOSITS_COUNT AS FTD_Cnt
--   , SUM(Case when ops.FIRST_DEPOSITS_SUM is null then 0
--          when ops.FIRST_DEPOSITS_SUM > 0 and UPPER(ops.currency) <> 'EUR' and UPPER(c.abbrev) = 'EUR' then ops.FIRST_DEPOSITS_SUM * cur.REVERSE_RATE
--     	 else ops.FIRST_DEPOSITS_SUM end) as FTD_Amt
--   , 0.00 AS Withdrawal_Amt
--   , 0.00 AS Commission_Amt
--   , SUM(Case when ops.DEPOSITS_SUM > 0 then 1 else 0 end) AS Deposit_Cnt
--   , SUM(Case
--     	when ops.DEPOSITS_SUM is null then 0
--         when ops.DEPOSITS_SUM > 0 and UPPER(ops.currency) <> 'EUR' and UPPER(c.abbrev) = 'EUR' then ops.DEPOSITS_SUM * cur.REVERSE_RATE
--     	else ops.DEPOSITS_SUM end) as Deposit_Amt
--   , 0.00 AS Net_Deposit_Amt
--   , SUM(Case
--         when ops.NGR is null then 0
--         when ops.NGR <> 0 and UPPER(ops.currency) <> 'EUR' and UPPER(c.abbrev) = 'EUR' then ops.NGR * cur.REVERSE_RATE
--         else ops.NGR end) as Net_Revenue_Amt
--   , ops.TRACKER_LOGIN_ID AS Tracker_Login_Id
--   , trk.TLOG_USERNAME AS Tracker_UserName
--   , 'Softswiss' AS Operator_Platform
--   , UPPER(ops.currency) AS Source_Currency
--   , ops._AIRBYTE_EMITTED_AT
-- FROM Softswiss_Temp ops
-- LEFT OUTER JOIN {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
--   ON UPPER(ops.DYNAMIC_TAG_CLICKID) = UPPER(pstbk.POST_CLICKID)
-- LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
--   ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
-- LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
--   ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
-- LEFT OUTER JOIN {{ source('BRC', 'BRANDS') }} AS b
--   ON a.CAMP_FK_BRAND = b.BRAN_ID
-- LEFT OUTER JOIN {{ source('BRC', 'TRACKER_LOGINS') }} AS trk
--   ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
-- LEFT OUTER JOIN {{ source('BRC', 'PUBLISHERS') }} AS pub
--   ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
-- LEFT OUTER JOIN {{ source('BRC', 'ADVERTISERS') }} AS adv
--   ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
-- LEFT OUTER JOIN {{ ref('DIM_PLAYER_LOCATION') }} AS loc
--   ON pstbk.POST_IP = loc.IP
-- left outer join CurrencyRateSW cur
--     on to_date(ops.Date) = cur.Date 
--     and UPPER(ops.currency) = UPPER(cur.CURRENCY_SOURCE) 
-- LEFT JOIN {{ source('BRT', 'OPERATOR_ACCOUNTS') }} opa
--     ON ops.TRACKER_LOGIN_ID = opa.BR_TRACKER_LOGIN_ID
-- LEFT JOIN {{ source('BRT', 'CURRENCY_OPERATOR_ACCOUNT') }} coa
--     ON TO_NUMBER(opa.ID) = TO_NUMBER(coa.OPERATOR_ACCOUNT_ID)
-- LEFT JOIN {{ source('BRT', 'CURRENCIES') }} c
--     ON TO_NUMBER(coa.CURRENCY_ID) = TO_NUMBER(c.ID)
-- GROUP BY ALL


Softswiss_Currency_Default as 
(
Select 
tmp.Date,
DYNAMIC_TAG_CLICKID,
sum(tmp.VISITS_COUNT) as VISITS_COUNT,
sum(tmp.REGISTRATIONS_COUNT) as REGISTRATIONS_COUNT,
sum(tmp.FIRST_DEPOSITS_COUNT) as FIRST_DEPOSITS_COUNT,
sum(CASE 
    WHEN tmp.Currency = cur.OPERATOR_CURRENCY THEN tmp.FIRST_DEPOSITS_SUM
    WHEN tmp.Currency = 'N/A' then tmp.FIRST_DEPOSITS_SUM
    ELSE tmp.FIRST_DEPOSITS_SUM * conv.REVERSE_RATE 
    END) as FIRST_DEPOSITS_SUM,
   sum(CASE 
    WHEN tmp.Currency = cur.OPERATOR_CURRENCY THEN tmp.DEPOSITS_SUM
    WHEN tmp.Currency = 'N/A' then tmp.DEPOSITS_SUM
    ELSE tmp.DEPOSITS_SUM * conv.REVERSE_RATE 
    END) as DEPOSITS_SUM,
sum(CASE
    WHEN tmp.Currency = cur.OPERATOR_CURRENCY THEN tmp.NGR
    WHEN tmp.Currency = 'N/A' then tmp.NGR
    ELSE tmp.NGR * conv.REVERSE_RATE
    END) as NGR,
tmp.TRACKER_LOGIN_ID,
MAX(tmp._AIRBYTE_EMITTED_AT) as _AIRBYTE_EMITTED_AT
from Softswiss_Temp tmp
left outer join Operator_Currency cur
    on tmp.TRACKER_LOGIN_ID = cur.TRACKER_LOGIN_ID
left outer join CurrencyRateSW conv
    on tmp.Date = conv.DATE AND tmp.Currency = conv.CURRENCY_SOURCE
GROUP BY ALL
),

Softswiss_Registration as 
(
SELECT 
    to_date(ops.DATE) as Date,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    ops.DYNAMIC_TAG_CLICKID as ClickId,
    Case
          when ops.REGISTRATIONS_COUNT > 0 then to_date(ops.DATE)
          else NULL
    end as SignUp_Date,
    sum(Case
          when ops.REGISTRATIONS_COUNT > 0 then 1
          else 0
    end) as SignUp_CNT,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    MAX(ops._AIRBYTE_EMITTED_AT) as _AIRBYTE_EMITTED_AT
from {{ source('SOFTSWISS', 'ACTIVITY_REPORT') }} ops
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(ops.DYNAMIC_TAG_CLICKID) = upper(pstbk.POST_CLICKID)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
  on  pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'CAMPAIGNS') }} a
  on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join {{ source('BRC', 'BRANDS') }} b
  on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join {{ source('BRC', 'TRACKER_LOGINS') }} trk
  on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join {{ source('BRC', 'PUBLISHERS') }} pub
  on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join {{ source('BRC', 'ADVERTISERS') }} adv
  on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
left outer join {{ ref('DIM_PLAYER_LOCATION') }} loc
  on pstbk.POST_IP = loc.IP
where ops.REGISTRATIONS_COUNT > 0
Group By All
),

Softswiss_Revenue as 
(
SELECT 
    to_date(ops.DATE) as Date,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    ops."DYNAMIC_TAG_CLICKID" as ClickId,
    pstbk.POST_FTD_DATE as FTD_Date_Agg,  
     Case
          when ops.FIRST_DEPOSITS_COUNT > 0 then to_date(ops.DATE)
          else NULL
    end as FTD_Date,
    Case
          when ops.FIRST_DEPOSITS_COUNT > 0 then 1
          else 0
    end as FTD_Cnt,
    sum(ops.FIRST_DEPOSITS_SUM) as FTD_Amt,
    sum(Case when ops.DEPOSITS_SUM > 0 then 1 else 0 end) as Deposit_Cnt,
    sum(ops.DEPOSITS_SUM) as Deposit_Amt,
    sum(ops.NGR) as Net_Revenue_Amt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    MAX(ops._AIRBYTE_EMITTED_AT) as _AIRBYTE_EMITTED_AT
from Softswiss_Currency_Default ops
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk 
    on upper(ops.DYNAMIC_TAG_CLICKID) = upper(pstbk.POST_CLICKID)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
  on  pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'CAMPAIGNS') }} a
  on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join {{ source('BRC', 'BRANDS') }} b
  on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join {{ source('BRC', 'TRACKER_LOGINS') }} trk
  on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join {{ source('BRC', 'PUBLISHERS') }} pub
  on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join {{ source('BRC', 'ADVERTISERS') }} adv
  on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
left outer join {{ ref('DIM_PLAYER_LOCATION') }} loc
  on pstbk.POST_IP = loc.IP
Group By All
),

Softswiss_Consolidation as 
(
Select 
Coalesce(reg.Date, rev.Date) as Date,
reg.SignUp_Date, 
rev.FTD_Date,
rev.FTD_Date_Agg,
Coalesce(reg.Country, rev.Country) as Country,
Coalesce(reg.Publisher_Name, rev.Publisher_Name) as Publisher_Name,
Coalesce(reg.Advertiser_ID, rev.Advertiser_ID) as Advertiser_ID,
Coalesce(reg.Advertiser_Name, rev.Advertiser_Name) as Advertiser_Name,
Coalesce(reg.Brand_Name, rev.Brand_Name) as Brand_Name,
Coalesce(reg.Player_IPAddress, rev.Player_IPAddress) as Player_IPAddress,
Coalesce(reg.ClickId, rev.ClickId) as ClickId,
1 as Click_Cnt,
sum(IFNull(reg.SignUp_CNT,0)) as SignUp_Cnt,
sum(IFNull(rev.FTD_CNT,0)) as FTD_Cnt,
sum(IFNULL(rev.FTD_Amt,0)) as FTD_Amt,
0.00 as Withdrawal_Amt,
0.00 as Commission_Amt,
sum(IFNULL(rev.Deposit_Cnt,0)) as Deposit_Cnt,
sum(IFNULL(rev.Deposit_Amt,0)) as Deposit_Amt,
0.00 as Net_Deposit_Amt,
sum(IFNULL(rev.Net_Revenue_Amt,0)) as Net_Revenue_Amt,
Coalesce(reg.TRACKER_LOGIN_ID, rev.TRACKER_LOGIN_ID) AS Tracker_Login_Id,
Coalesce(reg.Tracker_UserName, rev.Tracker_UserName) as Tracker_UserName,
'Softswiss' as Operator_Platform,
NULL as Source_Currency,
MAX(COALESCE(reg._AIRBYTE_EMITTED_AT, rev._AIRBYTE_EMITTED_AT)) as _AIRBYTE_EMITTED_AT
from Softswiss_Registration reg
full outer join Softswiss_Revenue rev
    on reg.Date = rev.Date and reg.ClickID = rev.ClickID and reg.Tracker_Login_ID = rev.Tracker_Login_ID
group by all
)

SELECT
  Date
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
  , Deposit_Cnt
  , Deposit_Amt
  , Net_Deposit_Amt
  , Net_Revenue_Amt
  , Tracker_Login_Id
  , Tracker_UserName
  , Operator_Platform
  , Source_Currency
  , _AIRBYTE_EMITTED_AT
FROM Softswiss_Consolidation
