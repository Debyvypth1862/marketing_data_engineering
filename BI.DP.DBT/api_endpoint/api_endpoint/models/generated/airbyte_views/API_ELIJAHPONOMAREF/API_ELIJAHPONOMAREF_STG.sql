{{ config(
    materialized = "view",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "API_ENDPOINT"
) }}
-- STG: Add hash for change detection
with base as (
    select
        {{ dbt_utils.surrogate_key([
            "DATE",
            "COUNTRY",
            "PUBLISHER",
            "ADVERTISER_ID",
            "ADVERTISER_NAME",
            "BRAND_NAME",
            "CAMPAIGN_NAME",
            "SUBID",
            "SUBID2",
            "SUBID3",
            "SUBID4",
            "SUBID5",
            '"3RD_PARTY_CLICKID"',
            "CLICKID",
            "CLICK_CNT",
            "SIGNUP_CNT",
            "FTD_CNT",
            "FTD_AMT",
            "CPA_CNT",
            "DEPOSIT_CNT",
            "DEPOSIT_AMT",
            "NET_REVENUE_AMT",
            "REVSHARE_REVENUE_AMT"
        ]) }} as _AIRBYTE_API_ELIJAHPONOMAREF_HASHID,
        {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT,
        *
    from {{ ref('API_ELIJAHPONOMAREF_AB1') }}
)
select * from base
