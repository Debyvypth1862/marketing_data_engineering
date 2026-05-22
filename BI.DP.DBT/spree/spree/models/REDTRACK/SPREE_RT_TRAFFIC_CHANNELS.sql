{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT * FROM {{ source('REDTRACK', 'TRAFFIC_CHANNELS') }} AS a
