{{ config(
    cluster_by = ["DATE","COUNTRY", "ADVERTISER_NAME", "PUBLISHER_NAME","TRACKER_LOGIN_ID"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = "EXP",
    schema = "PUBLIC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- Fix: Use full FACT_OPERATOR_AGG data for window functions to calculate CPA correctly
-- The incremental filter is applied later to only output new/changed records

WITH FACT_OPERATOR_AGG_FULL AS (
  -- Get ALL data from FACT_OPERATOR_AGG for correct cumulative calculations
  SELECT
    DATE
    , SIGNUP_DATE
    , FTD_DATE
    , ADVERTISER_NAME
    , PUBLISHER_NAME
    , BRAND_NAME
    , TRACKER_LOGIN_ID
    , COUNTRY
    , CLICKID
    , PLAYER_IPADDRESS
    , OPERATOR_PLATFORM
    , SUM(DEPOSIT_AMT) AS Deposit_Amt
    , SUM(NET_DEPOSIT_AMT) AS Net_Deposit_Amt
    , SUM(FTD_AMT) AS FTD_Amt
    , SUM(NET_REVENUE_AMT) AS Net_Revenue_Amt
    , SUM(CLICK_CNT) AS Click_Cnt
    , SUM(SIGNUP_CNT) AS SignUp_Cnt
    , SUM(FTD_CNT) AS FTD_Cnt
    , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ ref('FACT_OPERATOR_AGG') }}
  GROUP BY ALL
)

, FACT_OPERATOR_AGG_INCREMENTAL AS (
  -- Get only new records for filtering output later
  SELECT
    DATE
    , CLICKID
    , TRACKER_LOGIN_ID
    , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
  FROM {{ ref('FACT_OPERATOR_AGG') }}
  WHERE 1=1
  {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
  GROUP BY ALL
)

, MAX_CURRENCY_OPERATOR_ACCOUNT AS (
  SELECT a.* FROM
    {{ source('BRT', 'CURRENCY_OPERATOR_ACCOUNT') }} AS a
  INNER JOIN (
    SELECT
      OPERATOR_ACCOUNT_ID
      , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
    FROM {{ source('BRT', 'CURRENCY_OPERATOR_ACCOUNT') }}
    GROUP BY 1
  ) AS b
    ON a.OPERATOR_ACCOUNT_ID = b.OPERATOR_ACCOUNT_ID
    AND a._AIRBYTE_EMITTED_AT = b._AIRBYTE_EMITTED_AT
)

, FACT_OFFER AS (
  SELECT
    TO_DATE(ops.DATE) AS Date
    , SIGNUP_DATE
    , FTD_DATE
    , ops.COUNTRY
    , ops.ADVERTISER_NAME
    , ops.PUBLISHER_NAME
    , ops.OPERATOR_PLATFORM
    , brc.Parent_Campaign_ID
    , brc.Parent_Campaign_Name
    , brc.Child_Campaign_Id
    , brc.Child_Campaign_Name
    , brc.Campaign_RevShare_Deal
    , ops.Brand_Name
    , ops.ClickId
    , ops.Player_IPAddress
    , brc.Currency
    , brc.Campaign_Status
    , brc.Campaign_Type
    , brc.RevShare_In
    , brc.RevShare_Out
    , brc.CPA_In
    , brc.CPA_Out
    , COALESCE(brc.CPL_In, 0) AS CPL_In
    , COALESCE(brc.CPL_Out, 0) AS CPL_Out
    , ops.Net_Revenue_Amt
    , brc.Campaign_Baseline
    , ops.Deposit_Amt
    , ops.Net_Deposit_Amt
    , ops.FTD_Amt
    , ops.Cumm_Deposit_Amt
    , CASE
      WHEN
        (SUM(CASE
          WHEN ops.Cumm_Deposit_Amt > brc.Campaign_Baseline THEN 1 ELSE 0 END) OVER (
          PARTITION BY ops.ClickId
          ORDER BY TO_DATE(ops.Date) ASC
        )) = 1
        THEN brc.CPA_In
      ELSE 0
    END AS CPA_Income
    , CASE
      WHEN ops.Deposit_Amt > 0 THEN brc.CPL_In
      ELSE 0
    END AS CPL_Income
    , CASE
      WHEN
        brc.Campaign_RevShare_Deal > 0 AND ops.Net_Revenue_Amt <> 0
        THEN (brc.Campaign_RevShare_Deal / 100) * ops.Net_Revenue_Amt
      ELSE 0
    END AS RevShare_Income
    , ops.Click_Cnt
    , ops.SignUp_Cnt
    , ops.FTD_Cnt
    , ops.Tracker_login_Id
    , ops._AIRBYTE_EMITTED_AT
  FROM (
    SELECT
      DATE
      , SIGNUP_DATE
      , FTD_DATE
      , ADVERTISER_NAME
      , PUBLISHER_NAME
      , BRAND_NAME
      , TRACKER_LOGIN_ID
      , COUNTRY
      , CLICKID
      , PLAYER_IPADDRESS
      , OPERATOR_PLATFORM
      , SUM(DEPOSIT_AMT) OVER (
        PARTITION BY CLICKID
        ORDER BY TO_DATE(DATE) ASC
      ) AS CUMM_DEPOSIT_AMT
      , Deposit_Amt
      , Net_Deposit_Amt
      , FTD_Amt
      , Net_Revenue_Amt
      , Click_Cnt
      , SignUp_Cnt
      , FTD_Cnt
      , _AIRBYTE_EMITTED_AT
    FROM FACT_OPERATOR_AGG_FULL
  ) AS ops
  LEFT OUTER JOIN
    (
      SELECT
        POST_CLICKID AS ClickId
        , a.CAMP_COUNTRY AS Country
        , a.CAMP_ID AS Parent_Campaign_Id
        , a.CAMP_NAME AS Parent_Campaign_Name
        , cmtkr.CAMT_CHILD AS Child_Campaign_Id
        , CASE
          WHEN cmtkr.CAMT_CHILD > 0 THEN a.CAMP_NAME || '' || '[' || cmtkr.CAMT_CHILD_NAME || ']'
          WHEN cmtkr.CAMT_CHILD = 0 THEN a.CAMP_NAME ELSE 'Unknown'
        END AS Child_Campaign_Name
        , a.CAMP_REV_DEAL AS Campaign_RevShare_Deal
        , a.CAMP_CURRENCY AS Currency
        , a.CAMP_STATUS AS Campaign_Status
        , CASE
          WHEN a.CAMP_DEPOSIT_BASELINE = '' THEN 0
          WHEN a.CAMP_DEPOSIT_BASELINE IS NULL THEN 0 ELSE TO_NUMBER(REGEXP_SUBSTR(a.CAMP_DEPOSIT_BASELINE, '\\d*\\.?\\d+'))
        END AS Campaign_Baseline
        , a.CAMP_TYPE AS Campaign_Type
        , a.CAMP_REV_OUT AS RevShare_Out
        , a.CAMP_REV_IN AS RevShare_In
        , a.CAMP_CPA_OUT AS CPA_Out
        , a.CAMP_CPA_IN AS CPA_In
        , a.CAMP_CPL_IN AS CPL_In
        , a.CAMP_CPL_OUT AS CPL_Out
      FROM {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbk
      LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGN_TRACKERS') }} AS cmtkr
        ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
      LEFT OUTER JOIN {{ source('BRC', 'CAMPAIGNS') }} AS a
        ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
      GROUP BY ALL
    ) AS brc
    ON ops.CLICKID = brc.ClickId
  -- Filter to only include records that have new activity (for incremental processing)
  WHERE EXISTS (
    SELECT 1 FROM FACT_OPERATOR_AGG_INCREMENTAL inc
    WHERE inc.CLICKID = ops.CLICKID
  )
  ORDER BY brc.Parent_Campaign_Id ASC
),
  -- CellXpert_Qualfication_CPA as 
  -- (
  -- SELECT 
  --     op.TRACKER_LOGIN_ID,
  --     Date(op.DATE) as Date,
  --     op.CLICKID,
  --     SUM(CASE 
  --         WHEN op.FTD_Cnt > 0 then op.FTD_Cnt * a.CAMP_CPA_IN
  --         ELSE 0 END) 
  --     AS CPA_INCOME,
  --     SUM(CASE 
  --         WHEN op.FTD_Cnt > 0 then op.FTD_Cnt * a.CAMP_CPA_OUT
  --         ELSE 0 END)
  --     AS CPA_PAYMENT
  -- FROM EXP.PUBLIC.FACT_OPERATOR_AGG op
  -- left outer join RAW.BRC.POSTBACK_TRACKING pstbk 
  --     on upper(op.CLICKID) = upper(pstbk.post_clickid)
  -- left outer join RAW.BRC.CAMPAIGN_TRACKERS cmtkr
  --       on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
  -- left outer join RAW.BRC.CAMPAIGNS a
  --       on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
  -- where TRACKER_LOGIN_ID = 5485
  -- group by all
  -- )
  CPARevShareRevenue AS (
  SELECT
    plt.name as OPERATOR_PLATFORM,
    opa.ID as Operator_Account,
    opa.Name as Operator_Name,
    CAST(op.TRACKER_LOGIN_ID AS VARCHAR) as TRACKER_LOGIN_ID,
    UPPER(c.abbrev) as Affiliate_Currency,
    Date(op.qualification_date) as Date,
    op.REGISTRATION_DATE as SIGNUP_DATE,
    op.QUALIFICATION_DATE as FTD_DATE,
    a.CAMP_COUNTRY as Country,
    adv.ADVE_NAME as ADVERTISER_NAME,
    pub.PUBL_USERNAME as PUBLISHER_NAME,    
    a.CAMP_ID as Parent_Campaign_Id,
    a.CAMP_NAME as Parent_Campaign_Name,
    cmtkr.CAMT_CHILD as Child_Campaign_Id,
    Case 
        when cmtkr.CAMT_CHILD > 0 then a.CAMP_NAME||''||'['||cmtkr.CAMT_CHILD_NAME||']' 
        when cmtkr.CAMT_CHILD = 0 then a.CAMP_NAME else 'Unknown' end 
    as Child_Campaign_Name,
    a.CAMP_REV_DEAL as Campaign_RevShare_Deal,
      Case
        when a.CAMP_DEPOSIT_BASELINE = '' then 0
        when a.CAMP_DEPOSIT_BASELINE IS NULL then 0 ELSE TO_NUMBER(a.CAMP_DEPOSIT_BASELINE) end 
    as Campaign_Baseline,
    b.bran_name as BRAND_NAME,
    op.AFP as CLICKID,
    loc.IP as PLAYER_IPADDRESS,
    a.CAMP_CURRENCY as Currency,
    a.camp_status as Campaign_Status,
    a.CAMP_TYPE as Campaign_Type, 
    a.CAMP_CPA_IN as CPA_In,
    a.CAMP_CPA_OUT as CPA_Out,
    a.CAMP_CPL_IN as CPL_In,
    a.CAMP_CPL_OUT as CPL_Out,
    a.CAMP_REV_IN as RevShare_In,
    a.CAMP_REV_OUT as RevShare_Out,   
    0 as FTD_AMT,
    0 as DEPOSIT_AMT,
    0 as NET_DEPOSIT_AMT,
    0 as CUMM_DEPOSIT_AMT,
    0 AS NET_REVENUE_AMT,
    SUM(CASE 
        WHEN op.QUALIFICATION_DATE is not null then a.CAMP_CPA_IN
        ELSE 0 END) 
    as CPA_INCOME,
    SUM(CASE 
        WHEN op.QUALIFICATION_DATE is not null then a.CAMP_CPA_OUT
        ELSE 0 END)
    as CPA_PAYMENT,
    0 as CPL_INCOME,
    0 as CPL_PAYMENT,
    0 as REVSHARE_INCOME,
    0 as REVSHARE_PAYMENT,
    1 as CLICK_CNT,
    Case 
        when registration_date is not null then 1 
        else 0 end 
    as SIGNUP_CNT,
    Case 
        when qualification_date is not null then 1 
        else 0 end 
    as FTD_CNT,
    MAX(op._AIRBYTE_EMITTED_AT) as _AIRBYTE_EMITTED_AT
  FROM  {{ source('CELLXPERT', 'ICT_FTD_REGISTRATION_REPORT') }} op
  left outer join {{ source('BRT', 'OPERATOR_ACCOUNTS') }} opa
    on op.TRACKER_LOGIN_ID = opa.BR_TRACKER_LOGIN_ID
  LEFT OUTER JOIN {{ source('BRT', 'OPERATOR_OPERATOR_PLATFORM') }} oop
    ON oop.operator_id = opa.operator_id
  LEFT OUTER JOIN {{ source('BRT', 'OPERATOR_PLATFORMS') }} plt
    ON oop.operator_platform_id = plt.id
  LEFT OUTER JOIN MAX_CURRENCY_OPERATOR_ACCOUNT AS coa
    ON TO_NUMBER(opa.ID) = TO_NUMBER(coa.OPERATOR_ACCOUNT_ID)
  left outer join {{ source('BRT', 'CURRENCIES') }} c
    ON TO_NUMBER(coa.CURRENCY_ID) = TO_NUMBER(c.ID)
  left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(op.AFP) = upper(pstbk.post_clickid)
  left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr
      on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
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
      on pstbk.post_ip = loc.IP
  where TRACKER_LOGIN_ID = 5485
  and op.qualification_date is not null
  and op.date >= '2025-08-01'
  group by all

  UNION ALL

  SELECT
    fo.OPERATOR_PLATFORM
    , opa.ID AS Operator_Account
    , opa.Name AS Operator_Name
    , fo.TRACKER_LOGIN_ID
    , UPPER(c.abbrev) AS Affiliate_Currency
    , fo.DATE
    , fo.SIGNUP_DATE
    , fo.FTD_DATE
    , fo.COUNTRY
    , fo.ADVERTISER_NAME
    , fo.PUBLISHER_NAME
    , fo.PARENT_CAMPAIGN_ID
    , fo.PARENT_CAMPAIGN_NAME
    , fo.CHILD_CAMPAIGN_ID
    , fo.CHILD_CAMPAIGN_NAME
    , fo.CAMPAIGN_REVSHARE_DEAL
    , fo.CAMPAIGN_BASELINE
    , fo.BRAND_NAME
    , fo.CLICKID
    , fo.PLAYER_IPADDRESS
    , fo.CURRENCY
    , fo.CAMPAIGN_STATUS
    , fo.CAMPAIGN_TYPE
    , fo.CPA_IN
    , fo.CPA_OUT
    , fo.CPL_IN
    , fo.CPL_OUT
    , fo.REVSHARE_IN
    , fo.REVSHARE_OUT
    , fo.FTD_AMT
    , fo.DEPOSIT_AMT
    , fo.NET_DEPOSIT_AMT
    , fo.CUMM_DEPOSIT_AMT
    , fo.NET_REVENUE_AMT
    , fo.CPA_INCOME
    , CASE
        WHEN fo.CPA_INCOME = fo.CPA_OUT THEN fo.CPA_INCOME
        WHEN fo.CPA_INCOME > fo.CPA_OUT THEN fo.CPA_OUT
        ELSE 0 END
    AS CPA_PAYMENT
    , fo.CPL_INCOME
    , CASE
      WHEN fo.CPL_INCOME = fo.CPL_OUT THEN fo.CPL_INCOME
      WHEN fo.CPL_INCOME > fo.CPL_OUT THEN fo.CPL_OUT
      ELSE 0
    END AS CPL_PAYMENT
    , fo.REVSHARE_INCOME
    , CASE
      WHEN fo.REVSHARE_INCOME > 0 THEN fo.REVSHARE_INCOME * (fo.REVSHARE_OUT / 100)
      ELSE 0
    END AS REVSHARE_PAYMENT
    , fo.CLICK_CNT
    , fo.SIGNUP_CNT
    , fo.FTD_CNT
    , fo._AIRBYTE_EMITTED_AT
  FROM FACT_OFFER AS fo
  LEFT OUTER JOIN {{ source('BRT', 'OPERATOR_ACCOUNTS') }} AS opa ON fo.TRACKER_LOGIN_ID = opa.BR_TRACKER_LOGIN_ID
  LEFT OUTER JOIN {{ source('BRT', 'OPERATOR_OPERATOR_PLATFORM') }} oop
    ON oop.operator_id = opa.operator_id
  LEFT OUTER JOIN {{ source('BRT', 'OPERATOR_PLATFORMS') }} op_plt
    ON oop.operator_platform_id = op_plt.id
  LEFT JOIN
    MAX_CURRENCY_OPERATOR_ACCOUNT AS coa
    ON TO_NUMBER(opa.ID) = TO_NUMBER(coa.OPERATOR_ACCOUNT_ID)
  LEFT JOIN {{ source('BRT', 'CURRENCIES') }} AS c ON TO_NUMBER(coa.CURRENCY_ID) = TO_NUMBER(c.ID)
  -- left outer join CellXpert_Qualfication_CPA cx
  --   ON fo.TRACKER_LOGIN_ID = cx.TRACKER_LOGIN_ID and fo.Date = cx.Date and fo.Clickid = cx.Clickid
  -- Note: CellXpert exclusion filter removed to match V_FACT_OFFER view behavior
  -- WHERE NOT (fo.TRACKER_LOGIN_ID = '5485' AND fo.DATE >= '2025-08-01')

)
,

CurrencyRate AS (
  SELECT
    TO_DATE(DATE) AS DATE
    , CURRENCY_SOURCE
    , RATE AS REVERSE_RATE
  FROM {{ ref('TO_EUR_HISTORICAL') }}
  WHERE CURRENCY_SOURCE IN (
    SELECT DISTINCT Affiliate_Currency FROM CPARevShareRevenue
  )
)

SELECT
  {{ dbt_utils.surrogate_key([
        'fo1.OPERATOR_PLATFORM',
        'fo1.DATE',
        'fo1.CLICKID',
        'fo1.TRACKER_LOGIN_ID',
        'fo1.SIGNUP_DATE',
        'fo1.FTD_DATE'
        ]) }} AS _AIRBYTE_UNIQUE_KEY
  , fo1.DATE
  , fo1.SIGNUP_DATE
  , fo1.FTD_DATE
  , CASE
    WHEN fo1.CPA_INCOME > 0 THEN fo1.DATE
    ELSE NULL END
    AS CPA_DATE
  , fo1.COUNTRY
  , fo1.OPERATOR_PLATFORM
  , fo1.PUBLISHER_NAME
  , fo1.ADVERTISER_NAME
  , fo1.AFFILIATE_CURRENCY
  , fo1.BRAND_NAME
  , fo1.PARENT_CAMPAIGN_ID
  , fo1.PARENT_CAMPAIGN_NAME
  , fo1.CHILD_CAMPAIGN_ID
  , fo1.CHILD_CAMPAIGN_NAME
  , fo1.CAMPAIGN_REVSHARE_DEAL
  , CASE
    WHEN fo1.CAMPAIGN_BASELINE IS NULL THEN 0
    ELSE fo1.CAMPAIGN_BASELINE
    END AS CAMPAIGN_BASELINE
  , fo1.CAMPAIGN_STATUS
  , fo1.CAMPAIGN_TYPE
  , fo1.CLICKID
  , fo1.PLAYER_IPADDRESS
  , fo1.CURRENCY
  , fo1.CPA_IN
  , fo1.CPA_OUT
  , fo1.CPL_IN
  , fo1.CPL_OUT
  , fo1.REVSHARE_IN
  , fo1.REVSHARE_OUT
  , SUM(fo1.FTD_AMT) AS FTD_AMT
  , SUM(fo1.DEPOSIT_AMT) AS DEPOSIT_AMT
  , SUM(fo1.CUMM_DEPOSIT_AMT) AS CUMM_DEPOSIT_AMT
  , SUM(fo1.NET_DEPOSIT_AMT) AS NET_DEPOSIT_AMT
  , SUM(fo1.NET_REVENUE_AMT) AS NET_REVENUE_AMT
  , SUM(fo1.CPA_INCOME) AS CPA_INCOME
  , SUM(fo1.CPA_PAYMENT) AS CPA_PAYMENT
  , SUM(fo1.CPA_INCOME - fo1.CPA_PAYMENT) AS CPA_REVENUE
  , SUM(fo1.CPL_INCOME) AS CPL_INCOME
  , SUM(fo1.CPL_PAYMENT) AS CPL_PAYMENT
  , SUM(fo1.CPL_INCOME - fo1.CPL_PAYMENT) AS CPL_REVENUE
  , SUM(fo1.REVSHARE_INCOME) AS REVSHARE_INCOME
  , SUM(fo1.REVSHARE_PAYMENT) AS REVSHARE_PAYMENT
  , SUM(fo1.REVSHARE_INCOME - fo1.REVSHARE_PAYMENT) AS REVSHARE_REVENUE
  , CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN 1 ELSE dc.REVERSE_RATE
  END AS REVERSE_RATE
  , CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN campaign_revshare_deal ELSE
      ROUND(campaign_revshare_deal * dc.REVERSE_RATE, 2)
  END AS CAMPAIGN_REVSHARE_DEAL_EUR
  , CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN campaign_baseline ELSE ROUND(campaign_baseline * dc.REVERSE_RATE, 2)
  END AS campaign_baseline_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN ftd_amt ELSE ROUND(ftd_amt * dc.REVERSE_RATE, 2)
  END) AS ftd_amt_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN deposit_amt ELSE ROUND(deposit_amt * dc.REVERSE_RATE, 2)
  END) AS deposit_amt_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN cumm_deposit_amt ELSE ROUND(cumm_deposit_amt * dc.REVERSE_RATE, 2)
  END) AS cumm_deposit_amt_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN net_deposit_amt ELSE ROUND(net_deposit_amt * dc.REVERSE_RATE, 2)
  END) AS net_deposit_amt_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN net_revenue_amt ELSE ROUND(net_revenue_amt * dc.REVERSE_RATE, 2)
  END) AS net_revenue_amt_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPA_IN ELSE ROUND(CPA_IN * dc.REVERSE_RATE, 2)
  END) AS CPA_IN_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPA_OUT ELSE ROUND(CPA_OUT * dc.REVERSE_RATE, 2)
  END) AS cpa_out_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPL_IN ELSE ROUND(CPL_IN * dc.REVERSE_RATE, 2)
  END) AS CPL_IN_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPL_OUT ELSE ROUND(CPL_OUT * dc.REVERSE_RATE, 2)
  END) AS CPL_OUT_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPA_INCOME ELSE ROUND(CPA_INCOME * dc.REVERSE_RATE, 2)
  END) AS CPA_INCOME_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPA_PAYMENT ELSE ROUND(CPA_PAYMENT * dc.REVERSE_RATE, 2)
  END) AS CPA_PAYMENT_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPA_INCOME - CPA_PAYMENT ELSE
      ROUND((CPA_INCOME - CPA_PAYMENT) * dc.REVERSE_RATE, 2)
  END) AS CPA_REVENUE_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPL_INCOME ELSE ROUND(CPL_INCOME * dc.REVERSE_RATE, 2)
  END) AS CPL_INCOME_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPL_PAYMENT ELSE ROUND(CPL_PAYMENT * dc.REVERSE_RATE, 2)
  END) AS CPL_PAYMENT_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN CPL_INCOME - CPL_PAYMENT ELSE
      ROUND((CPL_INCOME - CPL_PAYMENT) * dc.REVERSE_RATE, 2)
  END) AS CPL_REVENUE_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN revshare_in ELSE ROUND(revshare_in * dc.REVERSE_RATE, 2)
  END) AS REVSHARE_IN_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN revshare_out ELSE ROUND(revshare_out * dc.REVERSE_RATE, 2)
  END) AS REVSHARE_OUT_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN REVSHARE_INCOME ELSE ROUND(REVSHARE_INCOME * dc.REVERSE_RATE, 2)
  END) AS REVSHARE_INCOME_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN REVSHARE_PAYMENT ELSE ROUND(REVSHARE_PAYMENT * dc.REVERSE_RATE, 2)
  END) AS REVSHARE_PAYMENT_EUR
  , SUM(CASE
    WHEN fo1.Affiliate_Currency = 'EUR' THEN REVSHARE_INCOME - REVSHARE_PAYMENT ELSE
      ROUND((REVSHARE_INCOME - REVSHARE_PAYMENT) * dc.REVERSE_RATE, 2)
  END) AS REVSHARE_REVENUE_EUR
  , SUM(fo1.CLICK_CNT) AS CLICK_CNT
  , SUM(fo1.SIGNUP_CNT) AS SIGNUP_CNT
  , SUM(fo1.FTD_CNT) AS FTD_CNT
  , SUM(CASE
    WHEN fo1.CPA_INCOME > 0 THEN 1 ELSE 0
  END) AS CPA_INCOME_CNT
  , SUM(CASE
    WHEN fo1.CPA_PAYMENT > 0 THEN 1 ELSE 0
  END) AS CPA_PAYMENT_CNT
  , SUM(CASE
    WHEN fo1.CAMPAIGN_TYPE IN ('HYBRID', 'CPA') AND fo1.CAMPAIGN_BASELINE = 0 AND fo1.FTD_AMT > 0 THEN 1
    WHEN fo1.CAMPAIGN_TYPE IN ('HYBRID', 'CPA') AND fo1.FTD_AMT > fo1.CAMPAIGN_BASELINE THEN 1 ELSE 0
  END) AS QUALIFIED_FTD_CNT
  , SUM(CASE
    WHEN fo1.DEPOSIT_AMT > 0 THEN 1 ELSE 0
  END) AS DEPOSIT_CNT
  , fo1.TRACKER_LOGIN_ID
  , MAX(fo1._AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
FROM CPARevShareRevenue AS fo1
LEFT JOIN CurrencyRate AS dc
  ON
    UPPER(fo1.Affiliate_Currency) = UPPER(dc.CURRENCY_SOURCE)
    AND (TO_DATE(fo1.DATE) = TO_DATE(dc.DATE))
GROUP BY ALL
