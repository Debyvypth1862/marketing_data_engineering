{{ config(
    materialized = "view",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "CRYPTO_CASHBACK"
) }}
-- STG: Add hash for change detection
with base as (
    select
        {{ dbt_utils.surrogate_key([
            "DATE",
            "BRANDS",
            "BRT_OFFER_ID",
            "SITE_MEMBER_ID",
            "FLAG",
            "DEPOSIT_AMOUNT",
            "CUMULATED_DEPOSIT_AMOUNT",
            "CUMULATED_LOSS_AMOUNT",
            "CUMULATED_CPA_AMOUNT",
            "CUMULATED_REVSHARE_AMOUNT"
        ]) }} as _AIRBYTE_API_CRYPTO_CASHBACK_BRAND_HASHID,
        *
    from {{ ref('API_CRYPTO_CASHBACK_BRAND_AB1') }}
)
select * from base
