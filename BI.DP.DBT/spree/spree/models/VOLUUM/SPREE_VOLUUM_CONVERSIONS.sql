{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT *
FROM (
        SELECT 
            AFFILIATE_NETWORK_ID,
            AFFILIATE_NETWORK_NAME,
            CASE
                WHEN trf.TRAFFIC_SOURCE_NAME Like '%Google%' THEN REGEXP_REPLACE(trf.TRAFFIC_SOURCE_NAME, '[^0-9]', '')
                WHEN trf.TRAFFIC_SOURCE_NAME Like '%Microsoft%' THEN REGEXP_REPLACE(trf.TRAFFIC_SOURCE_NAME, '[^0-9]', '')
                WHEN trf.TRAFFIC_SOURCE_NAME Like '[%' THEN SUBSTR(
                    trf.TRAFFIC_SOURCE_NAME,
                    POSITION('[' in trf.TRAFFIC_SOURCE_NAME) + 1,
                    POSITION(']' in trf.TRAFFIC_SOURCE_NAME) -2
                )
                ELSE NULL
            END AS AD_ACCOUNT_ID,
            BRAND,
            BROWSER,
            BROWSER_VERSION,
            CAMPAIGN_ID,
            CAMPAIGN_NAME,
            CAMPAIGN_URL_CONFIGURED,
            CITY,
            CLICKID,
            CONNECTION_TYPE,
            CONNECTION_TYPE_NAME,
            CONVERSION_ORIGINAL_CURRENCY,
            CONVERSION_TYPE,
            CONVERSION_TYPE_ID,
            COST,
            COUNTRY_CODE,
            COUNTRY_NAME,
            CUSTOM_VARIABLE_1,
            CUSTOM_VARIABLE_10,
            CUSTOM_VARIABLE_2,
            CUSTOM_VARIABLE_3,
            CUSTOM_VARIABLE_4,
            CUSTOM_VARIABLE_5,
            CUSTOM_VARIABLE_6,
            CUSTOM_VARIABLE_7,
            CUSTOM_VARIABLE_8,
            CUSTOM_VARIABLE_9,
            DEVICE,
            DEVICE_NAME,
            EXTERNAL_ID,
            FLOW_ID,
            IP,
            IS_DSP,
            ISP,
            LANDER_ID,
            LANDER_NAME,
            MOBILE_CARRIER,
            MODEL,
            OFFER_ID,
            OFFER_NAME,
            OS,
            OS_VERSION,
            OUTGOING_POSTBACK_URL,
            PATH_ID,
            POSTBACK_PARAM_1,
            POSTBACK_PARAM_2,
            POSTBACK_PARAM_3,
            POSTBACK_PARAM_4,
            POSTBACK_PARAM_5,
            POSTBACK_TIMESTAMP,
            REFERRER,
            REGION,
            REVENUE,
            REVENUE_IN_ORIGINAL_CURRENCY,
            SUB_LANDER_ID,
            TRAFFIC_SOURCE_ID,
            TRAFFIC_SOURCE_NAME,
            TRANSACTION_ID,
            TYPE,
            USER_AGENT,
            VISIT_TIMESTAMP,
            _AIRBYTE_EMITTED_AT
            FROM {{ source('VOLUUM', 'CONVERSIONS') }} AS trf
            WHERE UPPER(TRAFFIC_SOURCE_NAME) LIKE '%SPREE%'
    )