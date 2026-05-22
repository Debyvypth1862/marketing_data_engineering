{{ config(
    materialized = "ephemeral"
) }}
-- Base transformation for BRT_PARENT_LAST_6MONTHS_TO_PRIOR_MONTH
-- Converts V_BRT_PARENT_LAST_6MONTHS_TO_PIOR_MONTH.sql to dbt model with source() references

WITH OFFER_Latest AS 
(
    Select 
        TRACKING_ID,
        max(updated_at) as updated_at,
        max(_airbyte_emitted_at) as _airbyte_emitted_at
    from {{ source('BRT', 'OFFERS') }} 
    group by all
),

OFFER AS 
(
    Select
    off.ID,
    off.PARENT_ID,
    off.TRACKING_ID,
    off.Title as Offer_Name,
    off.Deleted
    from {{ source('BRT', 'OFFERS') }}  off
    join OFFER_Latest ls
        on off.TRACKING_ID = ls.TRACKING_ID and off.updated_at = ls.updated_at and off._airbyte_emitted_at = ls._airbyte_emitted_at
    Group By All
),

OPERATOR_TRACKER AS
(
  Select fct.Tracker_Login_Id
  FROM {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
  GROUP BY ALL
),

BRC_CLICK AS 
(
  SELECT 
      'Last 6 Months to Prior Month' as DATE_RANGE,
      off.PARENT_ID,
      off.ID as OFFER_ID,
      Case 
          when a.CAMP_DEPOSIT_BASELINE = '' then 0
          when a.CAMP_DEPOSIT_BASELINE = NULL then 0 ELSE TO_NUMBER(a.CAMP_DEPOSIT_BASELINE) 
      end as Campaign_Baseline,
      Case 
          when a.CAMP_WAGER_BASELINE = '' then 0
          when a.CAMP_WAGER_BASELINE = NULL then 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) 
      end as Campaign_Wager_Baseline,
      CAST(a.CAMP_REV_DEAL as Decimal (20,2)) as RevShare_In,
      (a.CAMP_REV_DEAL * .1) * (a.CAMP_REV_OUT * .1) as RevShare_Out,
      a.CAMP_REV_DEAL - ((a.CAMP_REV_DEAL * .1) * (a.CAMP_REV_OUT * .1)) as RevShare_Diff,
      a.CAMP_CPA_IN as CPA_In,
      a.CAMP_CPA_OUT as CPA_Out, 
      a.CAMP_CPA_IN - a.CAMP_CPA_OUT as CPA_Diff,      
      a.CAMP_CPL_IN as CPL_In,
      a.CAMP_CPL_OUT as CPL_Out,
      a.CAMP_CPL_IN -  a.CAMP_CPL_OUT as CPL_Diff,  
      IFNULL(SUM(brc.STATS_CLICKS),0) as Click_Total
  FROM {{ source('BRC', 'CAMPAIGN_EARNING') }} brc
  JOIN OFFER off
      ON cast(brc.TRACKING_ID as string) = off.TRACKING_ID
  left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      on brc.TRACKING_ID = cmtkr.CAMT_ID
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
  WHERE 
      substr(brc.STATS_END_DATE,1,4)||'-'||substr(brc.STATS_END_DATE,6,2) BETWEEN SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),6,2) AND 
  SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),6,2)
  GROUP BY ALL
),

BRC_CONVERSION AS 
(
  SELECT 
      'Last 6 Months to Prior Month' as DATE_RANGE,
      off.PARENT_ID,
      off.ID as OFFER_ID,
      Case 
          when a.CAMP_DEPOSIT_BASELINE = '' then 0
          when a.CAMP_DEPOSIT_BASELINE = NULL then 0 ELSE TO_NUMBER(a.CAMP_DEPOSIT_BASELINE) 
      end as Campaign_Baseline,
      Case 
          when a.CAMP_WAGER_BASELINE = '' then 0
          when a.CAMP_WAGER_BASELINE = NULL then 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) 
      end as Campaign_Wager_Baseline,
      CAST(a.CAMP_REV_DEAL as Decimal (20,2)) as RevShare_In,
      (a.CAMP_REV_DEAL * .1) * (a.CAMP_REV_OUT * .1) as RevShare_Out,
      a.CAMP_REV_DEAL - ((a.CAMP_REV_DEAL * .1) * (a.CAMP_REV_OUT * .1)) as RevShare_Diff,
      a.CAMP_CPA_IN as CPA_In,
      a.CAMP_CPA_OUT as CPA_Out, 
      a.CAMP_CPA_IN - a.CAMP_CPA_OUT as CPA_Diff,      
      a.CAMP_CPL_IN as CPL_In,
      a.CAMP_CPL_OUT as CPL_Out,
      a.CAMP_CPL_IN -  a.CAMP_CPL_OUT as CPL_Diff,  
      IFNULL(SUM(brc.STATS_SIGNUPS),0) as Signup_Cnt,
      IFNULL(SUM(brc.STATS_FTDS),0) AS FTD_Cnt,
      IFNULL(sum(brc.STATS_CPAS),0) AS CPA_Cnt,
      IFNULL(SUM(brc.STATS_CPA_INCOME),0) AS CPA_Income,
      IFNULL(SUM(brc.STATS_CPA_PAYOUT),0) AS CPA_Payout,
      IFNULL(SUM(brc.STATS_CPA_INCOME),0) - IFNULL(SUM(brc.STATS_CPA_PAYOUT),0) as CPA_Revenue,
      CASE WHEN SUM(brc.STATS_REV_INCOME) < 0 THEN 0 ELSE IFNULL(SUM(brc.STATS_REV_INCOME),0) END as RevShare_Income,
      CASE WHEN SUM(brc.STATS_REV_INCOME) < 0 THEN 0 ELSE IFNULL(SUM(brc.STATS_REV_PAYOUT),0) END as RevShare_Payout,
      CASE WHEN SUM(brc.STATS_REV_INCOME) < 0 THEN 0 ELSE IFNULL(SUM(brc.STATS_REV_INCOME),0) - IFNULL(SUM(brc.STATS_REV_PAYOUT),0) END as RevShare_Revenue
  FROM {{ source('BRC', 'CAMPAIGN_EARNING') }} brc
  JOIN OFFER off
      ON cast(brc.TRACKING_ID as string) = off.TRACKING_ID
  left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      on brc.TRACKING_ID = cmtkr.CAMT_ID
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
  WHERE 
      brc.TRACKER_ID not in (Select * from OPERATOR_TRACKER)
      AND substr(brc.STATS_END_DATE,1,4)||'-'||substr(brc.STATS_END_DATE,6,2) BETWEEN SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),6,2) AND 
  SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),6,2)
  GROUP BY ALL
),

BRC_CONSOLIDATION_TEMP AS 
(
  SELECT 
      COALESCE(clks.DATE_RANGE, conv.DATE_RANGE) as DATE_RANGE,
      COALESCE(clks.PARENT_ID, conv.PARENT_ID) as PARENT_ID,
      COALESCE(clks.OFFER_ID, conv.OFFER_ID) as OFFER_ID,
      COALESCE(clks.Campaign_Baseline, conv.Campaign_Baseline) as Campaign_Baseline,
      COALESCE(clks.Campaign_Wager_Baseline, conv.Campaign_Wager_Baseline) as Campaign_Wager_Baseline,
      COALESCE(clks.RevShare_In, conv.RevShare_In) as RevShare_In,
      COALESCE(clks.RevShare_Out, conv.RevShare_Out) as RevShare_Out,
      COALESCE(clks.RevShare_Diff, conv.RevShare_Diff) as RevShare_Diff,
      COALESCE(clks.CPA_In, conv.CPA_In) as CPA_In,
      COALESCE(clks.CPA_Out, conv.CPA_Out) as CPA_Out,
      COALESCE(clks.CPA_Diff, conv.CPA_Diff) as CPA_Diff,      
      COALESCE(clks.CPL_In, conv.CPL_In) as CPL_In,
      COALESCE(clks.CPL_Out, conv.CPL_Out) as CPL_Out,
      COALESCE(clks.CPL_Diff, conv.CPL_Diff) as CPL_Diff, 
      clks.Click_Total,
      conv.Signup_Cnt,
      conv.FTD_Cnt,
      conv.CPA_Cnt,
      conv.CPA_Income,
      conv.CPA_Payout,
      conv.CPA_Revenue,
      conv.RevShare_Income,
      conv.RevShare_Payout,
      conv.RevShare_Revenue
  FROM BRC_CLICK clks
  FULL OUTER JOIN BRC_CONVERSION conv
    ON clks.DATE_RANGE = conv.DATE_RANGE and clks.OFFER_ID = conv.OFFER_ID
),

BRC_CONSOLIDATION AS
(
 SELECT 
        DATE_RANGE,
        Parent_ID,
        Offer_ID,
        RevShare_In,  
        CASE WHEN RevShare_In <> 0 THEN 1 ELSE 0 END AS RevShare_In_0,
        Campaign_Baseline,
        CASE WHEN Campaign_Baseline <> 0 THEN 1 ELSE 0 END AS Campaign_Baseline_0,
        Campaign_Wager_Baseline,
        CASE WHEN Campaign_Wager_Baseline <> 0 THEN 1 ELSE 0 END AS Campaign_Wager_Baseline_0,
        RevShare_Out,
        CASE WHEN RevShare_Out <> 0 THEN 1 ELSE 0 END AS RevShare_Out_0,
        RevShare_Diff,
        CASE WHEN RevShare_Diff <> 0 THEN 1 ELSE 0 END AS RevShare_Diff_0,
        CPA_In,
        CASE WHEN CPA_In <> 0 THEN 1 ELSE 0 END AS CPA_In_0,
        CPA_Out,
        CASE WHEN CPA_Out <> 0 THEN 1 ELSE 0 END AS CPA_Out_0,
        CPA_Diff,
        CASE WHEN CPA_Diff <> 0 THEN 1 ELSE 0 END AS CPA_Diff_0,
        CPL_In,
        CASE WHEN CPL_In <> 0 THEN 1 ELSE 0 END AS CPL_In_0,
        CPL_Out,
        CASE WHEN CPL_Out <> 0 THEN 1 ELSE 0 END AS CPL_Out_0,
        CPL_Diff,
        CASE WHEN CPL_Diff <> 0 THEN 1 ELSE 0 END AS CPL_Diff_0,
        Click_Total,
        Signup_Cnt,
        FTD_Cnt,
        CPA_Cnt,
        CPA_Income,
        CPA_Payout,
        CPA_Revenue,
        RevShare_Income,
        RevShare_Payout,
        RevShare_Revenue
 FROM BRC_CONSOLIDATION_Temp
 GROUP BY ALL
),

OPERATOR_REGISTRATION AS
(
  SELECT
      SUBSTR(fct.DATE,1,4)||'-'||SUBSTR(fct.DATE,6,2) AS EVENT_MONTH,
      off.PARENT_ID,
      off.ID as OFFER_ID,
      pstbk.POST_FK_CAMT_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN,0)) - MAX(IFNULL(fct.CPA_OUT,0)) as CPA_DIFF,
      MAX(IFNULL(fct.CPL_IN,0)) AS CPL_IN,
      MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_OUT,
      MAX(IFNULL(fct.CPL_IN,0)) - MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_DIFF,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) AS REVSHARE_IN,
      IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) as REVSHARE_OUT,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) - IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) AS REVSHARE_DIFF,
      SUM(fct.SIGNUP_CNT) AS SIGNUP_CNT
  FROM {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
  left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk 
  on upper(fct.clickid) = upper(pstbk.POST_CLICKID)
  JOIN OFFER off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} a
      ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  WHERE fct.SIGNUP_CNT = 1
  GROUP BY ALL
),

OPERATOR_FIRSTDEPOSIT AS
(
  SELECT
      SUBSTR(fct.DATE,1,4)||'-'||SUBSTR(fct.DATE,6,2) AS EVENT_MONTH,
      off.PARENT_ID,
      off.ID as OFFER_ID,
      pstbk.POST_FK_CAMT_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN,0)) - MAX(IFNULL(fct.CPA_OUT,0)) as CPA_DIFF,
      MAX(IFNULL(fct.CPL_IN,0)) AS CPL_IN,
      MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_OUT,
      MAX(IFNULL(fct.CPL_IN,0)) - MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_DIFF,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) AS REVSHARE_IN,
      IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) as REVSHARE_OUT,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) - IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) AS REVSHARE_DIFF,
      sum(fct.FTD_CNT) AS FTD_CNT
  FROM {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
  left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk 
  on upper(fct.clickid) = upper(pstbk.POST_CLICKID)
  JOIN OFFER off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} a
      ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  WHERE fct.FTD_CNT = 1
  GROUP BY ALL
),

OPERATOR_REVENUE AS
(
  SELECT
      SUBSTR(fct.DATE,1,4)||'-'||SUBSTR(fct.DATE,6,2) AS EVENT_MONTH,
      off.PARENT_ID,
      off.ID as OFFER_ID,
      pstbk.POST_FK_CAMT_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN,0)) - MAX(IFNULL(fct.CPA_OUT,0)) as CPA_DIFF,
      MAX(IFNULL(fct.CPL_IN,0)) AS CPL_IN,
      MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_OUT,
      MAX(IFNULL(fct.CPL_IN,0)) - MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_DIFF,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) AS REVSHARE_IN,
      IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) as REVSHARE_OUT,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) - IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) AS REVSHARE_DIFF,
      SUM(fct.DEPOSIT_CNT) AS DEPOSIT_CNT,
      CASE WHEN SUM(fct.CPA_INCOME_EUR) > 0 THEN 1 ELSE 0 END as CPA_CNT,
      SUM(fct.DEPOSIT_AMT_EUR) AS DEPOSIT_AMT_EUR,
      SUM(fct.NET_REVENUE_AMT_EUR) AS NET_REVENUE_AMT_EUR,
      SUM(fct.CPA_INCOME_EUR) AS CPA_INCOME_EUR,
      SUM(fct.CPA_PAYMENT_EUR) AS CPA_PAYOUT_EUR,
      SUM(fct.CPA_INCOME_EUR) - SUM(CPA_PAYMENT_EUR) AS CPA_REVENUE_EUR,
      SUM(fct.CPL_INCOME_EUR) AS CPL_INCOME_EUR,
      SUM(fct.CPL_PAYMENT_EUR) AS CPL_PAYOUT_EUR,
      SUM(fct.CPL_INCOME_EUR) - SUM(CPL_PAYMENT_EUR)  AS CPL_REVENUE_EUR
  FROM {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
  left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk 
  on upper(fct.clickid) = upper(pstbk.POST_CLICKID)
  JOIN OFFER off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} a
      ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  GROUP BY ALL
),

OPERATOR_CONSOLIDATION AS 
(
  SELECT 
      'Last 6 Months to Prior Month' as DATE_RANGE,
      COALESCE(reg.Parent_ID, ftd.Parent_ID, rev.Parent_ID) as Parent_ID,
      COALESCE(reg.Offer_ID, ftd.Offer_ID, rev.Offer_ID) as Offer_ID,
      COALESCE(reg.Campaign_Baseline, ftd.Campaign_Baseline, rev.Campaign_Baseline) AS Campaign_Baseline,
      CASE WHEN COALESCE(reg.Campaign_Baseline, ftd.Campaign_Baseline, rev.Campaign_Baseline) <> 0 THEN 1 ELSE 0 END AS Campaign_Baseline_0,
      COALESCE(reg.Campaign_Wager_Baseline, ftd.Campaign_Wager_Baseline, rev.Campaign_Wager_Baseline) AS Campaign_Wager_Baseline,
      CASE WHEN COALESCE(reg.Campaign_Wager_Baseline, ftd.Campaign_Wager_Baseline, rev.Campaign_Wager_Baseline) <> 0 THEN 1 ELSE 0 END AS Campaign_Wager_Baseline_0,
      COALESCE(reg.CPA_In, ftd.CPA_In, rev.CPA_In) AS CPA_In,
      CASE WHEN COALESCE(reg.CPA_In, ftd.CPA_In, rev.CPA_In) <> 0 THEN 1 ELSE 0 END AS CPA_In_0,
      COALESCE(reg.CPA_Out, ftd.CPA_Out, rev.CPA_Out) AS CPA_Out,
      CASE WHEN COALESCE(reg.CPA_Out, ftd.CPA_Out, rev.CPA_Out) <> 0 THEN 1 ELSE 0 END CPA_Out_0,
      COALESCE(reg.CPA_Diff, ftd.CPA_Diff, rev.CPA_Diff) AS  CPA_Diff,
      CASE WHEN COALESCE(reg.CPA_Diff, ftd.CPA_Diff, rev.CPA_Diff) <> 0 THEN 1 ELSE 0 END AS CPA_Diff_0,
      COALESCE(reg.CPL_In, ftd.CPL_In, rev.CPL_In) AS CPL_In,
      CASE WHEN COALESCE(reg.CPL_In, ftd.CPL_In, rev.CPL_In) <> 0 THEN 1 ELSE 0 END AS CPL_In_0,
      COALESCE(reg.CPL_Out, ftd.CPL_Out, rev.CPL_Out) AS CPL_Out,
      CASE WHEN COALESCE(reg.CPL_Out, ftd.CPL_Out, rev.CPL_Out) <> 0 THEN 1 ELSE 0 END AS CPL_Out_0,
      COALESCE(reg.CPL_Diff, ftd.CPL_Diff, rev.CPL_Diff) AS CPL_Diff,
      CASE WHEN COALESCE(reg.CPL_Diff, ftd.CPL_Diff, rev.CPL_Diff) <> 0 THEN 1 ELSE 0 END AS CPL_Diff_0,
      COALESCE(reg.RevShare_In, ftd.RevShare_In, rev.RevShare_In) AS RevShare_In,
      CASE WHEN COALESCE(reg.RevShare_In, ftd.RevShare_In, rev.RevShare_In) <> 0 THEN 1 ELSE 0 END AS RevShare_In_0,
      COALESCE(reg.RevShare_Out, ftd.RevShare_Out, rev.RevShare_Out) AS RevShare_Out,
      CASE WHEN COALESCE(reg.RevShare_Out, ftd.RevShare_Out, rev.RevShare_Out) <> 0 THEN 1 ELSE 0 END AS RevShare_Out_0,
      COALESCE(reg.RevShare_Diff, ftd.RevShare_Diff, rev.RevShare_Diff) AS RevShare_Diff,
      CASE WHEN COALESCE(reg.RevShare_Diff, ftd.RevShare_Diff, rev.RevShare_Diff) <> 0 THEN 1 ELSE 0 END AS RevShare_Diff_0,
      IFNULL(sum(reg.SIGNUP_CNT),0) as SIGNUP_CNT,
      IFNULL(sum(ftd.FTD_CNT),0) as FTD_CNT,
      IFNULL(sum(rev.DEPOSIT_CNT),0) as DEPOSIT_CNT,
      IFNULL(sum(rev.CPA_CNT),0) as CPA_CNT,
      IFNULL(sum(rev.DEPOSIT_AMT_EUR),0) as DEPOSIT_AMT_EUR,
      IFNULL(sum(rev.NET_REVENUE_AMT_EUR),0) as NET_REVENUE_AMT_EUR,
      IFNULL(sum(rev.CPA_INCOME_EUR),0) as CPA_INCOME_EUR,
      IFNULL(sum(rev.CPA_PAYOUT_EUR),0) as CPA_PAYOUT_EUR,
      IFNULL(sum(rev.CPA_REVENUE_EUR),0) as CPA_REVENUE_EUR,
      IFNULL(sum(rev.CPL_INCOME_EUR),0) as CPL_INCOME_EUR,
      IFNULL(sum(rev.CPL_PAYOUT_EUR),0) as CPL_PAYOUT_EUR,
      IFNULL(sum(rev.CPL_REVENUE_EUR),0) as CPL_REVENUE_EUR
  FROM 
  OPERATOR_REGISTRATION reg
  FULL OUTER JOIN OPERATOR_FIRSTDEPOSIT ftd
  ON reg.Event_Month = ftd.Event_Month and reg.Offer_ID = ftd.Offer_ID
  FULL OUTER JOIN OPERATOR_REVENUE rev
  ON reg.Event_Month = rev.Event_Month and reg.Offer_ID = rev.Offer_ID
  WHERE COALESCE(reg.Event_Month, ftd.Event_Month, rev.Event_Month) BETWEEN SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -6),6,2) AND 
  SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),1,4)||'-'||SUBSTR(ADD_MONTHS(CURRENT_DATE -1, -1),6,2)
  GROUP BY ALL
),

CONSOLIDATION_BRC_OPERATOR_TEMP AS 
(
  SELECT 
      COALESCE(brc.DATE_RANGE, ops.DATE_RANGE) as DATE_RANGE,
      COALESCE(brc.Parent_ID, ops.Parent_ID) as Parent_ID,
      COALESCE(brc.Offer_ID, ops.Offer_ID) as Offer_ID,
      COALESCE(brc.Campaign_Baseline, ops.Campaign_Baseline) AS Campaign_Baseline,
      COALESCE(brc.Campaign_Baseline_0, ops.Campaign_Baseline_0) AS Campaign_Baseline_0,
      COALESCE(brc.Campaign_Wager_Baseline, ops.Campaign_Wager_Baseline) AS Campaign_Wager_Baseline,
      COALESCE(brc.Campaign_Wager_Baseline_0, ops.Campaign_Wager_Baseline_0) AS Campaign_Wager_Baseline_0,
      COALESCE(brc.CPA_In, ops.CPA_In) AS CPA_In,
      COALESCE(brc.CPA_In_0, ops.CPA_In_0) AS CPA_In_0,
      COALESCE(brc.CPA_Out, ops.CPA_Out) AS CPA_Out,
      COALESCE(brc.CPA_Out_0, ops.CPA_Out_0) AS CPA_Out_0,
      COALESCE(brc.CPA_Diff, ops.CPA_Diff) AS  CPA_Diff,
      COALESCE(brc.CPA_Diff_0, ops.CPA_Diff_0) AS  CPA_Diff_0,
      COALESCE(brc.CPL_In, ops.CPL_In) AS CPL_In,
      COALESCE(brc.CPL_In_0, ops.CPL_In_0) AS CPL_In_0,
      COALESCE(brc.CPL_Out, ops.CPL_Out) AS CPL_Out,
      COALESCE(brc.CPL_Out_0, ops.CPL_Out_0) AS CPL_Out_0,
      COALESCE(brc.CPL_Diff, ops.CPL_Diff) AS CPL_Diff,
      COALESCE(brc.CPL_Diff_0, ops.CPL_Diff_0) AS CPL_Diff_0,
      COALESCE(brc.RevShare_In, ops.RevShare_In) AS RevShare_In,
      COALESCE(brc.RevShare_In_0, ops.RevShare_In_0) AS RevShare_In_0,
      COALESCE(brc.RevShare_Out, ops.RevShare_Out) AS RevShare_Out,
      COALESCE(brc.RevShare_Out_0, ops.RevShare_Out_0) AS RevShare_Out_0,
      COALESCE(brc.RevShare_Diff, ops.RevShare_Diff) AS RevShare_Diff,
      COALESCE(brc.RevShare_Diff_0, ops.RevShare_Diff_0) AS RevShare_Diff_0,
      'Children' AS From_BRC,
      SUM(IFNULL(brc.Click_Total,0)) as Click_Total,
      IFNULL(CASE WHEN SUM(ops.SIGNUP_CNT) IS NULL THEN SUM(brc.SIGNUP_CNT) ELSE SUM(ops.SIGNUP_CNT) END,0) AS SIGNUP_CNT,
      IFNULL(CASE WHEN SUM(ops.FTD_CNT) IS NULL THEN SUM(brc.FTD_CNT) ELSE SUM(ops.FTD_CNT) END,0) AS FTD_CNT,
      IFNULL(CASE WHEN SUM(ops.CPA_CNT) IS NULL THEN SUM(brc.CPA_CNT) ELSE SUM(ops.CPA_CNT) END,0) AS CPA_CNT,
      IFNULL(CASE WHEN SUM(ops.CPA_INCOME_EUR) IS NULL THEN SUM(brc.CPA_Income) ELSE SUM(ops.CPA_INCOME_EUR) END,0) AS CPA_Income,
      IFNULL(CASE WHEN SUM(ops.CPA_PAYOUT_EUR) IS NULL THEN SUM(brc.CPA_Payout) ELSE SUM(ops.CPA_PAYOUT_EUR) END,0) AS CPA_Payout,
      IFNULL(CASE WHEN SUM(ops.CPA_REVENUE_EUR) IS NULL THEN SUM(brc.CPA_Revenue) ELSE SUM(ops.CPA_REVENUE_EUR) END,0) AS CPA_Revenue,
      IFNULL(SUM(CPL_INCOME_EUR),0) AS CPL_Income,
      IFNULL(SUM(CPL_PAYOUT_EUR),0) AS CPL_Payout,
      IFNULL(SUM(CPL_REVENUE_EUR),0) AS CPL_Revenue,
      IFNULL(SUM(ops.DEPOSIT_CNT),0) AS Deposit_Cnt,
      CAST(IFNULL(SUM(ops.DEPOSIT_AMT_EUR),0) AS DECIMAL(20,2)) AS Deposit_Amt,
      IFNULL(SUM(ops.NET_REVENUE_AMT_EUR),0) AS Net_Revenue_Amt,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <= 0 THEN 0
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) > 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.RevShare_In)/100) AS DECIMAL(20,2))
                    ELSE SUM(brc.RevShare_Income) END,0) AS RevShare_Income,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <= 0 THEN 0
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) > 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.REVSHARE_OUT)/100) AS DECIMAL(20,2))
                    ELSE SUM(brc.RevShare_Payout) END,0) AS RevShare_Payout,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <= 0 THEN 0
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) > 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.RevShare_Diff)/100) AS DECIMAL(20,2)) 
                    ELSE SUM(brc.RevShare_Revenue) END,0) AS RevShare_Revenue
  FROM 
  BRC_CONSOLIDATION brc
  FULL OUTER JOIN OPERATOR_CONSOLIDATION ops
  ON brc.DATE_RANGE = ops.DATE_RANGE and brc.Offer_ID = ops.Offer_ID
  GROUP BY ALL
),

CONSOLIDATION_BRC_OPERATOR AS 
(
  SELECT 
      rev.DATE_RANGE,
      rev.Parent_ID,
      rev.From_BRC AS From_BRC,
      CASE WHEN SUM(rev.Campaign_Baseline_0) <> 0 THEN CAST(SUM(rev.Campaign_Baseline) / SUM(rev.Campaign_Baseline_0) AS DECIMAL(20,2)) ELSE 0 END AS Campaign_Baseline_Avg,
      CASE WHEN SUM(rev.Campaign_Wager_Baseline_0) <> 0 THEN CAST(SUM(rev.Campaign_Wager_Baseline) / SUM(rev.Campaign_Wager_Baseline_0) AS DECIMAL(20,2)) ELSE 0 END AS Campaign_Wager_Baseline_Avg,
      CASE WHEN SUM(rev.CPA_In_0) <> 0 THEN CAST(SUM(rev.CPA_In) / SUM(rev.CPA_In_0) AS DECIMAL(20,2)) ELSE 0 END AS CPA_In_Avg,
      CASE WHEN SUM(rev.CPA_Out_0) <> 0 THEN CAST(SUM(rev.CPA_Out) / SUM(rev.CPA_Out_0) AS DECIMAL(20,2)) ELSE 0 END AS CPA_Out_Avg,
      CASE WHEN SUM(rev.CPA_Diff_0) <> 0 THEN CAST(SUM(rev.CPA_Diff) / SUM(rev.CPA_Diff_0) AS DECIMAL(20,2)) ELSE 0 END AS  CPA_Diff_Avg,
      CASE WHEN SUM(rev.CPL_In_0) <> 0 THEN CAST(SUM(rev.CPL_In) / SUM(rev.CPL_In_0) AS DECIMAL(20,2)) ELSE 0 END AS CPL_In_Avg,
      CASE WHEN SUM(rev.CPL_Out_0) <> 0 THEN CAST(SUM(rev.CPL_Out) / SUM(rev.CPL_Out_0) AS DECIMAL(20,2)) ELSE 0 END AS CPL_Out_Avg,
      CASE WHEN SUM(rev.CPL_Diff_0) <> 0 THEN CAST(SUM(rev.CPL_Diff) / SUM(rev.CPL_Diff_0) AS DECIMAL(20,2)) ELSE 0 END AS CPL_Diff_Avg,
      CASE WHEN SUM(rev.RevShare_In_0) <> 0 THEN CAST(SUM(rev.RevShare_In) / SUM(rev.RevShare_In_0) AS DECIMAL(20,2)) ELSE 0 END AS RevShare_In_Avg,
      CASE WHEN SUM(rev.RevShare_Out_0) <> 0 THEN CAST(SUM(rev.RevShare_Out) / SUM(rev.RevShare_Out_0) AS DECIMAL(20,2)) ELSE 0 END AS RevShare_Out_Avg,
      CASE WHEN SUM(rev.RevShare_Diff_0) <> 0 THEN CAST(SUM(rev.RevShare_Diff) / SUM(rev.RevShare_Diff_0) AS DECIMAL(20,2)) ELSE 0 END AS RevShare_Diff_Avg,
      SUM(rev.Click_Total) as Click_Total,
      SUM(rev.SIGNUP_CNT) AS SIGNUP_CNT,
      SUM(rev.FTD_CNT) AS FTD_CNT,
      SUM(rev.CPA_CNT) AS CPA_CNT,
      SUM(rev.CPA_Income) AS CPA_Income,
      SUM(rev.CPA_Payout) AS CPA_Payout,
      SUM(rev.CPA_Revenue) AS CPA_Revenue,
      SUM(rev.CPL_Income) AS CPL_Income,
      SUM(rev.CPL_Payout) AS CPL_Payout,
      SUM(rev.CPL_Revenue) AS CPL_Revenue,
      SUM(rev.Deposit_Cnt) AS Deposit_Cnt,
      SUM(rev.Deposit_Amt) AS Deposit_Amt,
      SUM(rev.Net_Revenue_Amt) AS Net_Revenue_Amt,
      SUM(rev.RevShare_Income) AS RevShare_Income,
      SUM(rev.RevShare_Payout) AS RevShare_Payout,
      SUM(rev.RevShare_Revenue) AS RevShare_Revenue
  FROM 
  CONSOLIDATION_BRC_OPERATOR_TEMP rev
  GROUP BY ALL
)

  SELECT 
      rev.DATE_RANGE,
      rev.Parent_ID,
      rev.Campaign_Baseline_Avg AS Baseline_Deposit,
      rev.Campaign_Wager_Baseline_Avg AS Baseline_Wager,
      rev.CPA_In_Avg AS CPA_In,
      rev.CPA_Out_Avg AS CPA_Out,
      rev.CPA_Diff_Avg AS CPA_Diff,
      rev.CPL_In_Avg AS CPL_In,
      rev.CPL_Out_Avg AS CPL_Out,
      rev.CPL_Diff_Avg AS CPL_Diff,
      rev.RevShare_In_Avg AS RevShare_In,
      rev.RevShare_Out_Avg AS RevShare_Out,
      rev.RevShare_Diff_Avg AS RevShare_Diff,
      rev.From_BRC,
      IFNULL(SUM(rev.CLICK_TOTAL),0) AS CLICK_CNT,
      IFNULL(SUM(rev.FTD_CNT),0)  FTD_CNT,
      IFNULL(SUM(rev.SIGNUP_CNT),0) AS SIGNUP_CNT,
      IFNULL(SUM(rev.DEPOSIT_CNT),0) AS DEPOSIT_CNT,
      IFNULL(SUM(rev.CPA_CNT),0) AS CPA_CNT,
      IFNULL(SUM(rev.DEPOSIT_AMT),0) AS DEPOSIT_AMT,
      IFNULL(SUM(rev.NET_REVENUE_AMT),0) AS NET_REVENUE_AMT,
      IFNULL(SUM(rev.CPA_Income),0) AS CPA_INCOME_AMT,      
      IFNULL(SUM(rev.CPA_Payout),0) AS CPA_PAYOUT_AMT,
      IFNULL(SUM(rev.CPA_Revenue),0) AS CPA_REVENUE_AMT,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPA_Income) / SUM(rev.CLICK_TOTAL) AS DECIMAL (20,2)) ELSE 0 END,0) AS CPA_INCOME_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPA_Payout) / SUM(rev.CLICK_TOTAL) AS DECIMAL (20,2)) ELSE 0 END,0) AS CPA_PAYOUT_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPA_Revenue) / SUM(rev.CLICK_TOTAL) AS DECIMAL (20,2)) ELSE 0 END,0) AS CPA_REVENUE_PER_CLICK,
      IFNULL(SUM(rev.CPL_Income),0) AS CPL_INCOME_AMT,
      IFNULL(SUM(rev.CPL_Payout),0) AS CPL_PAYOUT_AMT,
      IFNULL(SUM(rev.CPL_Revenue),0) AS CPL_REVENUE_AMT,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPL_INCOME) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS CPL_INCOME_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPL_PAYOUT) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS CPL_PAYOUT_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPL_REVENUE) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS CPL_REVENUE_PER_CLICK,
      CAST(IFNULL(SUM(rev.RevShare_Income),0) AS DECIMAL(20,2)) AS REVSHARE_INCOME_AMT,
      CAST(IFNULL(sum(rev.RevShare_Payout),0) AS DECIMAL(20,2)) AS REVSHARE_PAYOUT_AMT,
      CAST(IFNULL(SUM(rev.RevShare_Revenue),0) AS DECIMAL(20,2)) AS REVSHARE_REVENUE_AMT,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.RevShare_Income) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS REVSHARE_INCOME_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.RevShare_Payout) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS REVSHARE_PAYOUT_PER_CLICK,
      IFNULL(CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.RevShare_Revenue) / SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END,0) AS REVSHARE_REVENUE_PER_CLICK,
      IFNULL(CAST(SUM(rev.CPA_Income + rev.CPL_Income + rev.RevShare_Income) AS DECIMAL(20,2)), 0.00) as TOTAL_INCOME_AMT,
      IFNULL(CAST(SUM(rev.CPA_Payout + rev.CPL_Payout + rev.RevShare_Payout) AS DECIMAL(20,2)), 0.00) AS TOTAL_PAYOUT_AMT,
      IFNULL(CAST(SUM(rev.CPA_Revenue + rev.CPL_Revenue + rev.RevShare_Revenue) AS DECIMAL(20,2)), 0.00) AS TOTAL_REVENUE_AMT,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST((SUM(rev.CPA_Income) + SUM(rev.CPL_Income) +  SUM(rev.RevShare_Income))/ SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END AS TOTAL_INCOME_PER_CLICK,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST((SUM(rev.CPA_Payout) + SUM(rev.CPL_Payout) +  SUM(rev.RevShare_Payout))/ SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END AS TOTAL_PAYOUT_PER_CLICK,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST((SUM(rev.CPA_Revenue) + SUM(rev.CPL_Revenue) +  SUM(rev.RevShare_Revenue))/ SUM(rev.CLICK_TOTAL) AS DECIMAL(20,2)) ELSE 0 END AS TOTAL_REVENUE_PER_CLICK,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.SIGNUP_CNT) / SUM(rev.CLICK_TOTAL) * 100 AS DECIMAL(20,2)) ELSE 0 END AS CLICK_TO_SIGNUP,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.FTD_CNT) / SUM(rev.CLICK_TOTAL) * 100 AS DECIMAL(20,2)) ELSE 0 END AS CLICK_TO_FTD,
      CASE WHEN SUM(rev.CLICK_TOTAL) > 0 THEN CAST(SUM(rev.CPA_CNT) / sum(rev.CLICK_TOTAL) * 100 AS DECIMAL(20,2)) ELSE 0 END AS CLICK_TO_CPA,
      CASE WHEN SUM(rev.SIGNUP_CNT) > 0 THEN CAST(SUM(rev.FTD_CNT) / sum(rev.SIGNUP_CNT) * 100 AS DECIMAL(20,2)) ELSE 0 END AS SIGNUP_TO_FTD,
      CASE WHEN SUM(rev.SIGNUP_CNT) > 0 THEN CAST(SUM(rev.CPA_CNT) / SUM(rev.SIGNUP_CNT) * 100 AS DECIMAL(20,2)) ELSE 0 END AS SIGNUP_TO_CPA,
      {{ dbt_utils.surrogate_key(['rev.DATE_RANGE', 'rev.PARENT_ID']) }} as _AIRBYTE_AB_ID
  FROM CONSOLIDATION_BRC_OPERATOR rev
  GROUP BY ALL
