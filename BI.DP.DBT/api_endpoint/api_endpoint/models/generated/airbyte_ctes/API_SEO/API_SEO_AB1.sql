{{ config(
    materialized = 'ephemeral',
    tags = [ "seo-ab1" ]
) }}
-- AB1 model: Generates surrogate key for SEO_API
-- Primary key: DATE + ClickID
-- References the base consolidation model
SELECT
    {{ dbt_utils.surrogate_key([
        "Date",
        "ClickID"
    ]) }} as _AIRBYTE_AB_ID,
    Date as DATE,
    Country as COUNTRY,
    Publisher as PUBLISHER,
    Advertiser_ID as ADVERTISER_ID,
    Advertiser_Name as ADVERTISER_NAME,
    Brand_Name as BRAND_NAME,
    Campaign_Name as CAMPAIGN_NAME,
    Affiliate_ID as AFFILIATE_ID,
    SubID as SUBID,
    SubID2 as SUBID2,
    SubID3 as SUBID3,
    SubID4 as SUBID4,
    SubID5 as SUBID5,
    GA4_Device_ID as GA4_DEVICE_ID,
    IP,
    OW_ID,
    Site_Member_ID as SITE_MEMBER_ID,
    Marketing_Site_Id as MARKETING_SITE_ID,
    Test_Variation as TEST_VARIATION,
    Page as PAGE,
    Page_Location as PAGE_LOCATION,
    Environment as ENVIRONMENT,
    "3RD_PARTY_CLICKID",
    ClickID as CLICKID,
    Click_Cnt as CLICK_CNT,
    Signup_Cnt as SIGNUP_CNT,
    FTD_Cnt as FTD_CNT,
    FTD_Amt as FTD_AMT,
    CPA_Cnt as CPA_CNT,
    CPA_Income as CPA_INCOME,
    CPA_Payment as CPA_PAYMENT,
    CPA_Revenue as CPA_REVENUE,
    {{ current_timestamp() }} as _AIRBYTE_EMITTED_AT
FROM {{ ref('API_SEO_BASE_CONSOLIDATION') }}
