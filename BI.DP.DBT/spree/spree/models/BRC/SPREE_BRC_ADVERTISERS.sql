{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT * FROM {{ source('BRC', 'ADVERTISERS') }} AS a