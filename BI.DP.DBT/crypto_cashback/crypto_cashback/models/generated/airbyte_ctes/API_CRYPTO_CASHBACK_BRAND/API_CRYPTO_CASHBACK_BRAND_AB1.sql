{{ config(
    materialized = "ephemeral"
) }}
-- AB1: Add unique identifier based on primary key (DATE + SITE_MEMBER_ID + BRANDS)
select
    {{ dbt_utils.surrogate_key([
        "Date",
        "site_member_id",
        "Brands"
    ]) }} as _AIRBYTE_AB_ID,
    {{ dbt_utils.surrogate_key([
        "Date",
        "site_member_id",
        "Brands"
    ]) }} as _AIRBYTE_UNIQUE_KEY,
    Date,
    Brands,
    brt_offer_id,
    site_member_id,
    flag,
    deposit_amount,
    cumulated_deposit_amount,
    cumulated_loss_amount,
    cumulated_cpa_amount,
    cumulated_revshare_amount,
    {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT
from {{ ref('API_CRYPTO_CASHBACK_BRAND_BASE_CONSOLIDATION') }}
