{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT * FROM {{ source('REDTRACK', 'OFFER_SOURCES') }} AS a
