{% snapshot TRAFFIC_SOURCES_SCD %}

{{
    config(
        database=env_var('SCD_DATABASE'),
        schema="VOLUUM",
        strategy='check',
        unique_key="ID",
        check_cols=['UPDATED_TIME'],
        cluster_by='DBT_VALID_TO'
    )
}}

SELECT 
    *
FROM {{ ref('TRAFFIC_SOURCES_AB2') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY _AIRBYTE_EMITTED_AT DESC) = 1

{% endsnapshot %}
