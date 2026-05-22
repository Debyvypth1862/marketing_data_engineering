{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT 	
    ADS_ACCOUNT_ID,
	ADS_ACCOUNT_NAME,
	CAST(START_DATE AS DATE) AS START_DATE,
	CAST(END_DATE AS DATE) AS END_DATE,
	PRODUCT 
FROM {{ source('ADS_DATA', 'FACEBOOK_SPREE_ADS_ACCOUNTS') }}
WHERE PRODUCT = 'Spree'