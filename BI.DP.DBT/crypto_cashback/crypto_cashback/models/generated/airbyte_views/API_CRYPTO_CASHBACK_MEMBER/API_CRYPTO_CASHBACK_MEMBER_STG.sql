{{ config(
    materialized = "view",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "CRYPTO_CASHBACK"
) }}
-- STG: Add hash for change detection
with base as (
    select
        {{ dbt_utils.surrogate_key([
            "SITE_MEMBER_ID",
            "BRANDS",
            "BRT_OFFER_ID",
            "CLICK_DATE",
            "SIGNUP_DATE",
            "FTD_DATE",
            "CPA_DATE",
            "BASELINE_QUALIFIED"
        ]) }} as _AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID,
        *
    from {{ ref('API_CRYPTO_CASHBACK_MEMBER_AB1') }}
)
select * from base
