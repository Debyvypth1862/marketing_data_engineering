{{ config(
    schema = "PUBLIC",
    tags = [ "top-level-intermediate" ]
) }}
WITH Neterfer_Reg_442 as
(
SELECT
    to_date(ops.Date) as Date,
    ops.CLICK_ID as ClickId,
    Case
          when ops.SIGNUPS > 0 then to_date(ops.Date)
          else NULL
    end as SignUp_Date,
    pstbk.post_ftd_date as FTD_Date_Agg,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    sum(ops.SignUps) AS Signup_Cnt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    'Netrefer' as Operator_Platform,
    NULL AS Source_Currency,
    MAX(ops._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
from RAW.NETREFER.DYNAMIC_VARIABLES_REPORT ops
left outer join RAW.BRC.POSTBACK_TRACKING pstbk
  on upper(ops.CLICK_ID) = upper(pstbk.POST_CLICKID)
left outer join RAW.BRC.CAMPAIGN_TRACKERS cmtkr
  on  pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join RAW.BRC.CAMPAIGNS a
  on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join RAW.BRC.BRANDS b
  on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join RAW.BRC.TRACKER_LOGINS trk
  on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join RAW.BRC.PUBLISHERS pub
  on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join RAW.BRC.ADVERTISERS adv
  on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
left outer join EXP.PUBLIC.DIM_PLAYER_LOCATION loc
  on pstbk.POST_IP = loc.IP
Where ops.Tracker_Login_Id = 442
and ops.SIGNUPS > 0
Group By All
),

Neterfer_FTD_442 as 
(
SELECT 
    to_date(ops.Date) as Date,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    Case
          when ops.FIRST_TIME_DEPOSITING_CUSTOMER > 0 then to_date(ops.Date) else NULL
       end as FTD_Date,
    pstbk.post_ftd_date as FTD_Date_Agg,
    ops.CLICK_ID as ClickId,
    sum(Case
          when ops.FIRST_TIME_DEPOSITING_CUSTOMER > 0 then 1
          else 0
    end) as FTD_Cnt,
    sum(Case
          when ops.FIRST_TIME_DEPOSITING_CUSTOMER > 0 then ops.DEPOSITS
          else 0.00
    end) as FTD_Amt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    'Netrefer' as Operator_Platform,
    NULL AS Source_Currency,
    MAX(ops._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
from RAW.NETREFER.DYNAMIC_VARIABLES_REPORT ops
left outer join RAW.BRC.POSTBACK_TRACKING pstbk
    on upper(ops.CLICK_ID) = upper(pstbk.POST_CLICKID)
left outer join RAW.BRC.CAMPAIGN_TRACKERS cmtkr
  on  pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join RAW.BRC.CAMPAIGNS a
  on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join RAW.BRC.BRANDS b
  on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join RAW.BRC.TRACKER_LOGINS trk
  on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join RAW.BRC.PUBLISHERS pub
  on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join RAW.BRC.ADVERTISERS adv
  on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
left outer join EXP.PUBLIC.DIM_PLAYER_LOCATION loc
  on pstbk.POST_IP = loc.IP
Where ops.TRACKER_LOGIN_ID = 442
and ops.FIRST_TIME_DEPOSITING_CUSTOMER > 0
Group By All
),

Neterfer_Activity as 
(
SELECT 
    to_date(ops.Date) as Date,
    IFNULL(loc.COUNTRY_NAME, 'Unknown') as Country,
    IFNULL(pub.PUBL_USERNAME,'Unknown') as Publisher_Name,
    IFNULL(trk.TLOG_FK_ADVERTISER, -1) as Advertiser_ID,
    IFNULL(adv.ADVE_NAME,'Unknown') as Advertiser_Name,
    IFNULL(b.BRAN_NAME, 'Unknown') as Brand_Name,
    SPLIT_PART(pstbk.POST_IP, ',',1) as Player_IPAddress,
    ops.CLICK_ID as ClickId,
    pstbk.post_ftd_date as FTD_Date_Agg,
     sum(0.00) as Withdrawal_Amt,
    sum(0.00) as Commission_Amt,
    sum(Case when ops.DEPOSITS > 0 then 1 else 0 end) as Deposit_Cnt,
    sum(IFNULL(ops.DEPOSITS,0)) as Deposit_Amt,
    sum(0.00) as Net_Deposit_Amt,
    sum(IFNULL(ops.NET_REVENUE,0)) as Net_Revenue_Amt,
    ops.TRACKER_LOGIN_ID AS Tracker_Login_Id,
    trk.TLOG_USERNAME as Tracker_UserName,
    'Netrefer' as Operator_Platform,
    NULL AS Source_Currency,
    MAX(ops._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
from RAW.NETREFER.DYNAMIC_VARIABLES_REPORT ops
left outer join RAW.BRC.POSTBACK_TRACKING pstbk
    on upper(ops.CLICK_ID) = upper(pstbk.POST_CLICKID)
left outer join RAW.BRC.CAMPAIGN_TRACKERS cmtkr
  on  pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join RAW.BRC.CAMPAIGNS a
  on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
left outer join RAW.BRC.BRANDS b
  on a.CAMP_FK_BRAND = b.BRAN_ID
left outer join RAW.BRC.TRACKER_LOGINS trk
  on cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
left outer join RAW.BRC.PUBLISHERS pub
  on trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
left outer join RAW.BRC.ADVERTISERS adv
  on trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
left outer join EXP.PUBLIC.DIM_PLAYER_LOCATION loc
  on pstbk.POST_IP = loc.IP
Where Tracker_Login_Id <> 442
Group By All
),

Neterfer_Consolidation as 
(
  Select 
  coalesce(reg.Date, ftd.Date, act.Date) as Date,
  reg.SignUp_Date,
  ftd.FTD_Date,
  act.FTD_Date_Agg,
  coalesce(reg.Country, ftd.Country, act.Country) as Country,
  coalesce(reg.Publisher_Name, ftd.Publisher_Name, act.Publisher_Name) as Publisher_Name,
  coalesce(reg.Advertiser_ID, ftd.Advertiser_ID, act.Advertiser_ID) as Advertiser_ID,
  coalesce(reg.Advertiser_Name, ftd.Advertiser_Name, act.Advertiser_Name) as Advertiser_Name,
  coalesce(reg.Brand_Name, ftd.Brand_Name, act.Brand_Name) as Brand_Name,
  coalesce(reg.Player_IPAddress, ftd.Player_IPAddress, act.Player_IPAddress) as Player_IPAddress,
  coalesce(reg.ClickId, ftd.ClickId, act.ClickId) as ClickId,
  1 as Click_Cnt,
  IFNULL(reg.Signup_Cnt, 0) as Signup_Cnt,
  IFNULL(ftd.FTD_Cnt, 0) as FTD_Cnt,
  IFNULL(ftd.FTD_Amt, 0) as FTD_Amt,
  IFNULL(act.Withdrawal_Amt, 0) as Withdrawal_Amt,
  IFNULL(act.Commission_Amt, 0) as Commission_Amt,
  IFNULL(act.Deposit_Cnt, 0) as Deposit_Cnt,
  IFNULL(act.Deposit_Amt, 0) as Deposit_Amt,
  IFNULL(act.Net_Deposit_Amt, 0) as Net_Deposit_Amt,
  IFNULL(act.Net_Revenue_Amt, 0) as Net_Revenue_Amt,
  coalesce(reg.Tracker_Login_Id, ftd.Tracker_Login_Id, act.Tracker_Login_Id) as Tracker_Login_Id,
  coalesce(reg.Tracker_UserName, ftd.Tracker_UserName, act.Tracker_UserName) as Tracker_UserName,
  coalesce(reg.Operator_Platform, ftd.Operator_Platform, act.Operator_Platform) as Operator_Platform,
  coalesce(reg.Source_Currency, ftd.Source_Currency, act.Source_Currency) as Source_Currency,
  MAX(reg._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  from Neterfer_Reg_442 reg
  full outer join Neterfer_FTD_442 ftd
      on reg.Date = ftd.Date and reg.ClickID = ftd.ClickID
  full outer join Neterfer_Activity act
      on reg.Date = act.Date and reg.ClickID = act.ClickID
  group by all

)
select * from Neterfer_Consolidation
