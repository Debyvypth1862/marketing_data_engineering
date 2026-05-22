{{ config(
    materialized = "ephemeral"
) }}
-- Final consolidation for Crypto Cashback Brand
Select
    coalesce(ngr.Date, incr.Date) as Date,
    coalesce(ngr.Brands, incr.Brands) as Brands,
    coalesce(ngr.brt_offer_id, incr.brt_offer_id) as brt_offer_id,
    coalesce(ngr.site_member_id, incr.site_member_id) as site_member_id,
    Case
        when sum(ngr.CUMM_DEPOSIT_AMT_EUR) = 0 and sum(ngr.CUMM_NGR_AMT) <> 0 then 'lost money without deposit'
        when sum(ngr.CUMM_DEPOSIT_AMT_EUR) IS NULL and sum(ngr.CUMM_NGR_AMT) <> 0 then 'lost money without deposit'
        else NULL end as flag,
    sum(IFNULL(incr.DEPOSIT_AMT_EUR, 0)) as deposit_amount,
    sum(IFNULL(ngr.CUMM_DEPOSIT_AMT_EUR, 0)) as cumulated_deposit_amount,
    Case
        when sum(ngr.CUMM_NGR_AMT) > 0 then Cast(sum(ngr.CUMM_NGR_AMT)/.3945 as decimal(10,2))
        else 0.00 end as cumulated_loss_amount,
    SUM(IFNULL(ngr.CUMM_CPA_AMT,0)) AS cumulated_cpa_amount,
    SUM(IFNULL(ngr.CUMM_REVSHARE_AMT,0)) AS cumulated_revshare_amount
from {{ ref('API_CRYPTO_CASHBACK_BRAND_BASE_CUMMULATIVE') }} ngr
full outer join {{ ref('API_CRYPTO_CASHBACK_BRAND_BASE_FACT_OFFER') }} incr
    on ngr.Date = incr.Date and ngr.site_member_id = incr.site_member_id and ngr.brt_offer_id = incr.brt_offer_id
group by all
