{{ config(
    materialized = 'view',
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "API_ENDPOINT",
    tags = [ "seo-stg" ]
) }}
-- STG model: Generates hash for change detection
-- References AB1 model
SELECT
    *,
    {{ dbt_utils.surrogate_key([
        "DATE",
        "COUNTRY",
        "PUBLISHER",
        "ADVERTISER_ID",
        "ADVERTISER_NAME",
        "BRAND_NAME",
        "CAMPAIGN_NAME",
        "AFFILIATE_ID",
        "SUBID",
        "SUBID2",
        "SUBID3",
        "SUBID4",
        "SUBID5",
        "GA4_DEVICE_ID",
        "IP",
        "OW_ID",
        "SITE_MEMBER_ID",
        "MARKETING_SITE_ID",
        "TEST_VARIATION",
        "PAGE",
        "PAGE_LOCATION",
        "ENVIRONMENT",
        '"3RD_PARTY_CLICKID"',
        "CLICKID",
        "CLICK_CNT",
        "SIGNUP_CNT",
        "FTD_CNT",
        "FTD_AMT",
        "CPA_CNT",
        "CPA_INCOME",
        "CPA_PAYMENT",
        "CPA_REVENUE"
    ]) }} as _AIRBYTE_API_SEO_HASHID
FROM {{ ref('API_SEO_AB1') }}
