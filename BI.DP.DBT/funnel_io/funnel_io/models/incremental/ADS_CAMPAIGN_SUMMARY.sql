{{
    config(
        materialized='incremental',
        unique_key=['FUNNEL_WORKSPACE_NAME', 'DATE', 'TRAFFIC_SOURCE', 'MEDIA_TYPE', 'PAID_ORGANIC', 'CURRENCY', 'AD_ACCOUNT_NAME', 'CAMPAIGN', 'CAMPAIGN_ID', 'AD_ACCOUNT_ID'],
        incremental_strategy='merge',
        database=env_var('RAW_DATABASE'),
        schema="FUNNEL_IO",
        alias="ADS_CAMPAIGN_SUMMARY"
    )
}}

-- Upsert data from Funnel.io Facebook Ads view to ADS_CAMPAIGN_SUMMARY table
WITH source_data AS (
    SELECT
        FUNNEL_WORKSPACE_NAME,
        DATE,
        TRAFFIC_SOURCE,
        MEDIA_TYPE,
        PAID__ORGANIC AS PAID_ORGANIC,
        CURRENCY,
        AD_ACCOUNT_NAME__FACEBOOK_ADS AS AD_ACCOUNT_NAME,
        NULL AS AD_ACCOUNT_TIMEZONE,  -- Not available in source
        CAMPAIGN,
        CAMPAIGN_ID__FACEBOOK_ADS AS CAMPAIGN_ID,
        AD_ACCOUNT_ID__FACEBOOK_ADS AS AD_ACCOUNT_ID,
        NULL AS COUNTRY,  -- Not available in source
        SUM(COST) AS COST,
        SUM(CLICKS_ALL__FACEBOOK_ADS) AS CLICKS,
        SUM(IMPRESSIONS__FACEBOOK_ADS) AS IMPRESSIONS
    FROM {{ source('funnel_ads_data', 'PROD_AWS_RAW_STANDARD_WS_FACEBOOK_ADS_CAMPAIGN_SUMMARY_APOSTE_PREMIA') }}
    WHERE DATE IS NOT NULL
        AND AD_ACCOUNT_NAME__FACEBOOK_ADS IS NOT NULL
        {% if is_incremental() %}
        -- Only process data from recent days for incremental runs
        AND DATE >= CURRENT_DATE - 30  -- Process last 30 days to handle late-arriving data
        AND DATE < CURRENT_DATE
        {% endif %}
    GROUP BY 
        FUNNEL_WORKSPACE_NAME,
        DATE,
        TRAFFIC_SOURCE,
        MEDIA_TYPE,
        PAID__ORGANIC,
        CURRENCY,
        AD_ACCOUNT_NAME__FACEBOOK_ADS,
        CAMPAIGN,
        CAMPAIGN_ID__FACEBOOK_ADS,
        AD_ACCOUNT_ID__FACEBOOK_ADS
)

SELECT
    FUNNEL_WORKSPACE_NAME,
    DATE,
    TRAFFIC_SOURCE,
    MEDIA_TYPE,
    PAID_ORGANIC,
    CURRENCY,
    AD_ACCOUNT_NAME,
    AD_ACCOUNT_TIMEZONE,
    CAMPAIGN,
    CAMPAIGN_ID,
    AD_ACCOUNT_ID,
    COUNTRY,
    COST,
    CLICKS,
    IMPRESSIONS
FROM source_data