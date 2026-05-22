{% snapshot FACEBOOK_SPREE_ADS_ACCOUNT_SCD %}

{{
    config(
        database=env_var('SCD_DATABASE'),
        schema="ADS_DATA",
        unique_key="business_key",
        strategy="check",
        check_cols=['START_DATE','END_DATE'],
        hard_deletes='new_record',
        cluster_by='DBT_UPDATED_AT'
    )
}}

SELECT 
    COALESCE(ADS_ACCOUNT_ID, 'N/A') || '~' || 
    COALESCE(ADS_ACCOUNT_NAME, 'N/A') || '~' || 
    COALESCE(PRODUCT, 'N/A') AS BUSINESS_KEY,
    *
FROM {{ ref('FACEBOOK_SPREE_ADS_ACCOUNT') }}

{% endsnapshot %}
