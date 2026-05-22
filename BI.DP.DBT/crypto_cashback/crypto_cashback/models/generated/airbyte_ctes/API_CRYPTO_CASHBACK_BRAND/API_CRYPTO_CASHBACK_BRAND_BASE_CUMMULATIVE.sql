{{ config(
    materialized = "ephemeral"
) }}
-- Cummulative NGR, CPA, RevShare, and Deposit for Crypto Cashback Brand
-- Uses window functions to calculate cumulative amounts partitioned by site_member_id, brt_offer_id, and brand
Select
    fct.Date,
    fct.BRAND_NAME as Brands,
    off.ID as brt_offer_id,
    pstbk.POST_SITE_MEMBER_ID as site_member_id,
    fct.DEPOSIT_AMT_EUR as DEPOSIT_AMT_EUR,
    Sum(fct.DEPOSIT_AMT_EUR) OVER(PARTITION BY pstbk.POST_SITE_MEMBER_ID, off.ID, fct.BRAND_NAME ORDER BY to_date(DATE) ASC) as CUMM_DEPOSIT_AMT_EUR,
    Sum(fct.NET_REVENUE_AMT_EUR) OVER(PARTITION BY pstbk.POST_SITE_MEMBER_ID, off.ID, fct.BRAND_NAME ORDER BY to_date(DATE) ASC) as CUMM_NGR_AMT,
    Sum(fct.CPA_INCOME_EUR) OVER(PARTITION BY pstbk.POST_SITE_MEMBER_ID, off.ID, fct.BRAND_NAME ORDER BY to_date(DATE) ASC) as CUMM_CPA_AMT,
    Sum(fct.REVSHARE_INCOME_EUR) OVER(PARTITION BY pstbk.POST_SITE_MEMBER_ID, off.ID, fct.BRAND_NAME ORDER BY to_date(DATE) ASC) as CUMM_REVSHARE_AMT
from {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(fct.clickid) = upper(pstbk.post_clickid)
LEFT OUTER JOIN {{ ref('API_CRYPTO_CASHBACK_BRAND_BASE_OFFER') }} off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
where fct.Publisher_Name = 'Cryptoback'
and pstbk.POST_SITE_MEMBER_ID <> ''
and (fct.NET_REVENUE_AMT_EUR <> 0 or fct.CPA_INCOME_EUR > 0 or fct.REVSHARE_INCOME_EUR <> 0 or fct.DEPOSIT_AMT_EUR > 0)
