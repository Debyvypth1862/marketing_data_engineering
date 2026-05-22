{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT 
    DATE,
    TRAFFIC_SOURCE,
    CAMPAIGN as CAMPAIGN_NAME,
    CASE 
        WHEN TRAFFIC_SOURCE = 'Snapchat' THEN REGION__SNAPCHAT
        WHEN TRAFFIC_SOURCE = 'Facebook' THEN REGION__FACEBOOK_ADS
        WHEN TRAFFIC_SOURCE = 'Google' THEN REGION__GOOGLE_ADS
        WHEN TRAFFIC_SOURCE = 'Twitter' THEN REGION__X_ADS
        WHEN TRAFFIC_SOURCE = 'Bing' THEN STATE__MICROSOFT_ADVERTISING
        -- For Taboola, region will be NULL as there's no corresponding region column
    END AS REGION,
    SUM(CAST(COST AS NUMERIC(38, 9))) AS COST
FROM {{ source('ADS_DATA_FUNNEL', 'PROD_AWS_RAW_ALARICK_WS_ADS_SPEND_BY_STATE_REPORT') }}
WHERE TRAFFIC_SOURCE IN ('Snapchat', 'Facebook', 'Google', 'Twitter', 'Bing', 'Taboola')
GROUP BY DATE, TRAFFIC_SOURCE, CAMPAIGN_NAME, REGION