{% snapshot SPREE_ADS_SPEND_BY_STATE_REPORT_SCD %}

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
    COALESCE(REGION, 'N/A') AS BUSINESS_KEY,
    DATE,
    TRAFFIC_SOURCE,
    REGION,
    SUM(COST) AS COST
FROM {{ ref('SPREE_ADS_SPEND_BY_STATE_CAMPAIGN_REPORT') }}
GROUP BY BUSINESS_KEY, DATE, TRAFFIC_SOURCE, REGION

{% endsnapshot %}
