{{ config(
    materialized = "ephemeral"
) }}
-- Incremental deposit for Crypto Cashback Brand
-- Aggregates deposit amounts by date, brand, offer, and site member
Select
    fct.Date,
    fct.BRAND_NAME as Brands,
    off.ID as brt_offer_id,
    pstbk.POST_SITE_MEMBER_ID as site_member_id,
    sum(fct.DEPOSIT_AMT_EUR) as DEPOSIT_AMT_EUR
from {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(fct.clickid) = upper(pstbk.post_clickid)
LEFT OUTER JOIN {{ ref('API_CRYPTO_CASHBACK_BRAND_BASE_OFFER') }} off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
where fct.Publisher_Name = 'Cryptoback'
and pstbk.POST_SITE_MEMBER_ID <> ''
and fct.DEPOSIT_AMT_EUR > 0
Group by all
