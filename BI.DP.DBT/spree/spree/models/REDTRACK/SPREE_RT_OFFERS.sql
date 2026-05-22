{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT * FROM {{ source('REDTRACK', 'OFFERS') }} AS a
WHERE UPPER(TITLE) LIKE '%SPREE%'