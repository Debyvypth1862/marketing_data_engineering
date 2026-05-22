{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT * FROM {{ source('REDTRACK', 'CONVERSIONS') }} AS a
WHERE UPPER(CAMPAIGN) LIKE '%SPREE%'