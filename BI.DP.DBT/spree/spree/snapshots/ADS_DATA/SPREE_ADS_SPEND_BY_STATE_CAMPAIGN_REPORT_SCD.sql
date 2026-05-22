{% snapshot SPREE_ADS_SPEND_BY_STATE_CAMPAIGN_REPORT_SCD %}

{{
    config(
        database=env_var('SCD_DATABASE'),
        schema="ADS_DATA",
        unique_key="BUSINESS_KEY",
        strategy="check",
        check_cols=['COST'],
        hard_deletes='new_record',
        cluster_by='DBT_UPDATED_AT'
    )
}}

SELECT 
    COALESCE(DATE, 'N/A') || '~' || 
    COALESCE(TRAFFIC_SOURCE, 'N/A') || '~' || 
    COALESCE(CAMPAIGN_NAME, 'N/A') || '~' || 
    COALESCE(REGION, 'N/A') AS BUSINESS_KEY,
    *
FROM {{ ref('SPREE_ADS_SPEND_BY_STATE_CAMPAIGN_REPORT') }}

{% endsnapshot %}
