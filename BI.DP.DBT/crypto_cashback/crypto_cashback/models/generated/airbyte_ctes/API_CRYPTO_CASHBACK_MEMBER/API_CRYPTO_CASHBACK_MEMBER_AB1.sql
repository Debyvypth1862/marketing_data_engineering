{{ config(
    materialized = "ephemeral"
) }}
-- AB1: Add unique identifier based on primary key (SITE_MEMBER_ID + BRT_OFFER_ID)
select
    {{ dbt_utils.surrogate_key([
        "site_member_id",
        "brt_offer_id"
    ]) }} as _AIRBYTE_AB_ID,
    {{ dbt_utils.surrogate_key([
        "site_member_id",
        "brt_offer_id"
    ]) }} as _AIRBYTE_UNIQUE_KEY,
    site_member_id,
    Brands,
    brt_offer_id,
    click_date,
    signup_date,
    ftd_date,
    cpa_date,
    baseline_qualified,
    {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT
from {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_CONSOLIDATION') }}
