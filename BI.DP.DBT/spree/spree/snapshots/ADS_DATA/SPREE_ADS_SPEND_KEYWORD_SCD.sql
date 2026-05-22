{% snapshot SPREE_ADS_SPEND_KEYWORD_SCD %}

{{
    config(
        database=env_var('SCD_DATABASE'),
        schema="ADS_DATA",
        unique_key="BUSINESS_KEY",
        strategy="check",
        check_cols=['COST', 'CLICKS', 'IMPRESSIONS','INTERACTIONS','CONVERSIONS'],
        hard_deletes='new_record',
        cluster_by='DBT_UPDATED_AT'
    )
}}

SELECT 
    COALESCE(DATE, 'N/A') || '~' || 
    COALESCE(TRAFFIC_SOURCE, 'N/A') || '~' || 
    COALESCE(ADS_ACCOUNT_CUSTOMER_ID, 'N/A') || '~' || 
    COALESCE(CAMPAIGN, 'N/A') || '~' || 
    COALESCE(CAMPAIGN_ID, 'N/A') || '~' || 
    COALESCE(KEYWORD_ID, 'N/A') || '~' || 
    COALESCE(KEYWORD, 'N/A') || '~' || 
    COALESCE(KEYWORD_MATCH_TYPE, 'N/A') || '~' || 
    COALESCE(AD_GROUP_ID, 'N/A') || '~' || 
    COALESCE(AD_GROUP_NAME, 'N/A') || '~' || 
    COALESCE(AD_GROUP_STATUS, 'N/A') || '~' || 
    COALESCE(AD_GROUP_TYPE, 'N/A') AS BUSINESS_KEY,
    DATE,
    TRAFFIC_SOURCE,
    ADS_ACCOUNT_CUSTOMER_ID,
    CAMPAIGN,
    CAMPAIGN_ID,
    KEYWORD_ID,
    KEYWORD,
    KEYWORD_MATCH_TYPE,
    AD_GROUP_ID,
    AD_GROUP_NAME,
    AD_GROUP_STATUS,
    AD_GROUP_TYPE,
    COST,
    CLICKS,
    IMPRESSIONS,
    INTERACTIONS,
    CONVERSIONS
FROM {{ ref('SPREE_ADS_SPEND_KEYWORD') }}

{% endsnapshot %}