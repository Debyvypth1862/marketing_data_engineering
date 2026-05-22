{{ config(
    materialized = "ephemeral"
) }}
-- Crypto Cashback Member data consolidation
-- Combines all member activity CTEs with 5-way full outer join
select
    coalesce(cd.site_member_id, reg.site_member_id, ftd.site_member_id, cpa.site_member_id, bq.site_member_id) as site_member_id,
    coalesce(cd.Brands, reg.Brands, ftd.Brands, cpa.Brands, bq.Brands) as Brands,
    coalesce(cd.brt_offer_id, reg.brt_offer_id, ftd.brt_offer_id, cpa.brt_offer_id, bq.brt_offer_id) as brt_offer_id,
    cd.click_date,
    reg.signup_date,
    ftd.ftd_date,
    cpa.cpa_date,
    bq.baseline_qualified
from {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_CLICK_DATE') }} cd
full outer join {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_REG_DATE') }} reg
    on cd.site_member_id = reg.site_member_id and cd.brt_offer_id = reg.brt_offer_id
full outer join {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_FTD_DATE') }} ftd
    on cd.site_member_id = ftd.site_member_id and cd.brt_offer_id = ftd.brt_offer_id
full outer join {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_CPA_DATE') }} cpa
    on cd.site_member_id = cpa.site_member_id and cd.brt_offer_id = cpa.brt_offer_id
full outer join {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_BASELINE_QUALIFIED') }} bq
    on cd.site_member_id = bq.site_member_id and cd.brt_offer_id = bq.brt_offer_id
