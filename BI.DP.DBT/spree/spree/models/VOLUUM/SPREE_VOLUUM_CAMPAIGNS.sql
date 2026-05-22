{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT DISTINCT
    a.ALLOWED_ACTIONS,
    a.BASIC,
    a.COST_MODEL,
    a.COUNTRY,
    a.CREATED_TIME,
    a.CUSTOM_POSTBACKS_CONFIGURATION,
    a.DELETED,
    a.DIRECT_TRACKING,
    a.ID,
    a.IMPRESSION_URL,
    a.NAME,
    a.NAME_POSTFIX,
    a.PREFERRED_TRACKING_DOMAIN,
    a.REDIRECT_TARGET,
    a.REVENUE_MODEL,
    a.TAGS,
    a.TRAFFIC_SOURCE,
    a.UPDATED_TIME,
    a.URL,
    a.WORKSPACE,
    a._AIRBYTE_EMITTED_AT
FROM
    {{ source('VOLUUM', 'CAMPAIGNS') }} AS a
WHERE UPPER(NAME) LIKE '%SPREE%'