{{ config(
    materialized = "ephemeral"
) }}
-- Base transformation for BRT_OFFER_PAST_30DAYS
-- Converts V_BRT_OFFER_PAST_30DAYS.sql to dbt model with source() references

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

BRC_CLICKS AS 
(
  SELECT 
      'Past 30 Days' as DATE_RANGE,
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
      IFNULL(SUM(brc.STATS_CLICKS),0) as Click_Total,
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
      brc.STATS_END_DATE BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
  GROUP BY ALL
),

BRC_CONVERSIONS AS 
(
  SELECT 
      'Past 30 Days' as DATE_RANGE,
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
      IFNULL(SUM(brc.STATS_REV_INCOME),0) as RevShare_Income,
      IFNULL(SUM(brc.STATS_REV_PAYOUT),0) as RevShare_Payout,
      IFNULL(SUM(brc.STATS_REV_INCOME),0) - IFNULL(SUM(brc.STATS_REV_PAYOUT),0) as RevShare_Revenue
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
      AND brc.STATS_END_DATE BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
  GROUP BY ALL
),

BRC_CONSOLIDATION AS 
(
  SELECT 
      COALESCE(clks.DATE_RANGE, conv.DATE_RANGE) as DATE_RANGE,
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
      IFNULL(conv.Signup_Cnt,0) AS Signup_Cnt,
      IFNULL(conv.FTD_Cnt,0) AS FTD_Cnt,
      IFNULL(conv.CPA_Cnt,0) AS CPA_Cnt,
      IFNULL(conv.CPA_Income,0) as CPA_Income,
      IFNULL(conv.CPA_Payout,0) AS CPA_Payout,
      IFNULL(conv.CPA_Revenue,0) AS CPA_Revenue,
      IFNULL(conv.RevShare_Income,0) AS RevShare_Income,
      IFNULL(conv.RevShare_Payout,0) AS RevShare_Payout,
      IFNULL(conv.RevShare_Revenue,0) AS RevShare_Revenue
  FROM BRC_CLICKS clks
  FULL OUTER JOIN BRC_CONVERSIONS conv
    ON clks.DATE_RANGE = conv.DATE_RANGE and clks.OFFER_ID = conv.OFFER_ID
  GROUP BY ALL
),

OPERATOR_REGISTRATION AS
(
  SELECT
      fct.DATE AS CLICK_DATE,
      off.ID as OFFER_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT_EUR,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) - MAX(IFNULL(fct.CPA_OUT_EUR,0)) as CPA_DIFF,
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
      fct.DATE AS CLICK_DATE,
      off.ID as OFFER_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT_EUR,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) - MAX(IFNULL(fct.CPA_OUT_EUR,0)) as CPA_DIFF,
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
      fct.DATE AS CLICK_DATE,
      off.ID as OFFER_ID,
      MAX(IFNULL(fct.CAMPAIGN_BASELINE,0)) AS Campaign_Baseline,
      MAX(IFNULL(CASE WHEN a.CAMP_WAGER_BASELINE = '' THEN 0 ELSE TO_NUMBER(a.CAMP_WAGER_BASELINE) END,0)) AS Campaign_Wager_Baseline,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) AS CPA_IN,
      MAX(IFNULL(fct.CPA_OUT_EUR,0)) AS CPA_OUT,
      MAX(IFNULL(fct.CPA_IN_EUR,0)) - MAX(IFNULL(fct.CPA_OUT_EUR,0)) as CPA_DIFF,
      MAX(IFNULL(fct.CPL_IN,0)) AS CPL_IN,
      MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_OUT,
      MAX(IFNULL(fct.CPL_IN,0)) - MAX(IFNULL(fct.CPL_OUT,0)) AS CPL_DIFF,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) AS REVSHARE_IN,
      IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) as REVSHARE_OUT,
      MAX(IFNULL(CAST(fct.CAMPAIGN_REVSHARE_DEAL AS DECIMAL(20,2)),0)) - IFNULL((MAX(fct.CAMPAIGN_REVSHARE_DEAL) * .1) * (MAX(fct.REVSHARE_OUT) * .1),0) AS REVSHARE_DIFF,
      SUM(fct.DEPOSIT_CNT) AS DEPOSIT_CNT,
      SUM(CASE WHEN fct.CPA_INCOME > 0 THEN 1 ELSE 0 END) AS CPA_CNT,
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
      'Past 30 Days' as DATE_RANGE,
      COALESCE(reg.Offer_ID, ftd.Offer_ID, rev.Offer_ID) as Offer_ID,
      COALESCE(reg.Campaign_Baseline, ftd.Campaign_Baseline, rev.Campaign_Baseline) AS Campaign_Baseline,
      COALESCE(reg.Campaign_Wager_Baseline, ftd.Campaign_Wager_Baseline, rev.Campaign_Wager_Baseline) AS Campaign_Wager_Baseline,
      COALESCE(reg.CPA_In, ftd.CPA_In, rev.CPA_In) AS CPA_In,
      COALESCE(reg.CPA_Out, ftd.CPA_Out, rev.CPA_Out) AS CPA_Out,
      COALESCE(reg.CPA_Diff, ftd.CPA_Diff, rev.CPA_Diff) AS  CPA_Diff,
      COALESCE(reg.CPL_In, ftd.CPL_In, rev.CPL_In) AS CPL_In,
      COALESCE(reg.CPL_Out, ftd.CPL_Out, rev.CPL_Out) AS CPL_Out,
      COALESCE(reg.CPL_Diff, ftd.CPL_Diff, rev.CPL_Diff) AS CPL_Diff,
      COALESCE(reg.RevShare_In, ftd.RevShare_In, rev.RevShare_In) AS RevShare_In,
      COALESCE(reg.RevShare_Out, ftd.RevShare_Out, rev.RevShare_Out) AS RevShare_Out,
      COALESCE(reg.RevShare_Diff, ftd.RevShare_Diff, rev.RevShare_Diff) AS RevShare_Diff,
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
  ON reg.CLICK_DATE = ftd.CLICK_DATE and reg.Offer_ID = ftd.Offer_ID 
  FULL OUTER JOIN OPERATOR_REVENUE rev
  ON reg.CLICK_DATE = rev.CLICK_DATE and reg.Offer_ID = rev.Offer_ID 
  WHERE COALESCE(reg.Click_Date, ftd.Click_Date, rev.Click_Date) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
  GROUP BY ALL
),

CONSOLIDATION_BRC_OPERATOR AS 
(
  SELECT 
      COALESCE(brc.DATE_RANGE, ops.DATE_RANGE) as DATE_RANGE,
      COALESCE(brc.Offer_ID, ops.Offer_ID) as Offer_ID,
      COALESCE(brc.Campaign_Baseline, ops.Campaign_Baseline) AS Campaign_Baseline,
      COALESCE(brc.Campaign_Wager_Baseline, ops.Campaign_Wager_Baseline) AS Campaign_Wager_Baseline,
      COALESCE(brc.CPA_In, ops.CPA_In) AS CPA_In,
      COALESCE(brc.CPA_Out, ops.CPA_Out) AS CPA_Out,
      COALESCE(brc.CPA_Diff, ops.CPA_Diff) AS  CPA_Diff,
      COALESCE(brc.CPL_In, ops.CPL_In) AS CPL_In,
      COALESCE(brc.CPL_Out, ops.CPL_Out) AS CPL_Out,
      COALESCE(brc.CPL_Diff, ops.CPL_Diff) AS CPL_Diff,
      COALESCE(brc.RevShare_In, ops.RevShare_In) AS RevShare_In,
      COALESCE(brc.RevShare_Out, ops.RevShare_Out) AS RevShare_Out,
      COALESCE(brc.RevShare_Diff, ops.RevShare_Diff) AS RevShare_Diff,
      CASE WHEN ops.Offer_ID IS NULL THEN 'BRC' ELSE 'Operator' END AS From_BRC,
      IFNULL(SUM(brc.CLICK_TOTAL),0) AS Click_Total,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.FTD_CNT ELSE ops.FTD_CNT END),0) AS FTD_Cnt,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.SIGNUP_CNT ELSE ops.SIGNUP_CNT END),0) AS Signup_Cnt,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.CPA_CNT ELSE ops.CPA_CNT END),0) AS CPA_CNT,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.CPA_Income ELSE ops.CPA_INCOME_EUR END),0) AS CPA_Income,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.CPA_Payout ELSE ops.CPA_PAYOUT_EUR END),0) AS CPA_Payout,
      IFNULL(SUM(CASE WHEN ops.Offer_ID IS NULL THEN brc.CPA_Revenue ELSE ops.CPA_REVENUE_EUR END),0) AS CPA_Revenue,
      IFNULL(SUM(ops.CPL_INCOME_EUR),0) AS CPL_Income,
      IFNULL(SUM(ops.CPL_PAYOUT_EUR),0) AS CPL_Payout,
      IFNULL(SUM(ops.CPL_REVENUE_EUR),0) AS CPL_Revenue,
      IFNULL(SUM(ops.DEPOSIT_CNT),0) AS Deposit_Cnt,
      CAST(IFNULL(SUM(ops.DEPOSIT_AMT_EUR),0) AS DECIMAL(20,2)) AS Deposit_Amt,
      IFNULL(SUM(ops.NET_REVENUE_AMT_EUR),0) AS Net_Revenue_Amt,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <> 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(brc.RevShare_In)/100) AS DECIMAL(20,2))
                    ELSE SUM(brc.RevShare_Income) END,0) AS RevShare_Income,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <> 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.REVSHARE_OUT)/100) AS DECIMAL(20,2))
                    ELSE SUM(brc.RevShare_Payout) END,0) AS RevShare_Payout,
      IFNULL(CASE 
                    WHEN SUM(ops.NET_REVENUE_AMT_EUR) <> 0 THEN CAST(SUM(ops.NET_REVENUE_AMT_EUR) * (MAX(ops.RevShare_Diff)/100) AS DECIMAL(20,2)) 
                    ELSE SUM(brc.RevShare_Revenue) END,0) AS RevShare_Revenue
  FROM 
  BRC_CONSOLIDATION brc
  FULL OUTER JOIN OPERATOR_CONSOLIDATION ops
  ON brc.DATE_RANGE = ops.DATE_RANGE and brc.Offer_ID = ops.Offer_ID 
  GROUP BY ALL
)

  SELECT 
      rev.DATE_RANGE,
      rev.Offer_ID,
      rev.Campaign_Baseline AS Baseline_Deposit,
      rev.Campaign_Wager_Baseline AS Baseline_Wager,
      rev.CPA_IN,
      rev.CPA_OUT,
      rev.CPA_Diff,
      rev.CPL_IN,
      rev.CPL_OUT,
      rev.CPL_Diff,
      rev.RevShare_In,
      rev.RevShare_Out,
      rev.RevShare_Diff,
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
      {{ dbt_utils.surrogate_key(['rev.DATE_RANGE', 'rev.OFFER_ID']) }} as _AIRBYTE_AB_ID
  FROM CONSOLIDATION_BRC_OPERATOR rev
  GROUP BY ALL
