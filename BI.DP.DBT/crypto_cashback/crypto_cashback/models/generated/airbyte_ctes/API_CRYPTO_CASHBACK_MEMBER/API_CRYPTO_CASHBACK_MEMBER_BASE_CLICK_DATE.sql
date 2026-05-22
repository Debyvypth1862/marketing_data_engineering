{{ config(
    materialized = "ephemeral"
) }}
-- Click Date CTE for Crypto Cashback Member
Select
    pstbk.POST_SITE_MEMBER_ID as site_member_id,
    fct.BRAND_NAME as Brands,
    off.ID as brt_offer_id,
    min(pstbk.post_click_date) as click_date
from {{ source('EXP_PUBLIC', 'FACT_OFFER') }} fct
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(fct.clickid) = upper(pstbk.post_clickid)
LEFT OUTER JOIN {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_OFFER') }} off
      ON cast(pstbk.POST_FK_CAMT_ID as string) = off.TRACKING_ID
where
    fct.Publisher_Name = 'Cryptoback'
    and pstbk.POST_SITE_MEMBER_ID <> ''
    and off.Deleted = 'FALSE'
Group by all
