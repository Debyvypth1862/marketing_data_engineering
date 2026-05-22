{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT 
    pstbck.POST_SUBID4,
    CAST(pstbck.POST_FTD_TIMESTAMP AS DATETIME) AS POST_FTD_TIMESTAMP,
    pstbck.POST_FBCLID,
    pstbck.POST_SUBID5,
    pstbck.POST_OW_ID,
    pstbck.POST_AFFILIATE_ID,
    pstbck.POST_IP,
    CAST(pstbck.POST_CLICK_DATE AS STRING) AS POST_CLICK_DATE,
    pstbck.POST_PAGE_LOCATION,
    pstbck.POST_FK_TRACKER,
    pstbck.POST_CLICK_TIMESTAMP,
    pstbck.POST_SUBID2,
    pstbck.POST_FK_CAMT_ID,
    pstbck.POST_SUBID3,
    pstbck.POST_ADGROUPID,
    pstbck.POST_UTM_CONTENT,
    pstbck.POST_UTM_ID,
    pstbck.POST_SUBID,
    pstbck.POST_GA4_DEVICE_ID,
    pstbck.POST_MODIFIED_TIMESTAMP,
    CAST(pstbck.POST_CPA_TIMESTAMP AS DATETIME) AS POST_CPA_TIMESTAMP,
    pstbck.POST_UTM_SOURCE,
    CAST(pstbck.POST_SIGNUP_DATE AS STRING) AS POST_SIGNUP_DATE,
    pstbck.POST_SITE_MEMBER_ID,
    pstbck.POST_UTM_CAMPAIGN,
    pstbck.POST_CAMPAIGNID,
    pstbck.POST_UTM_MEDIUM,
    pstbck.POST_ENV,
    pstbck.POST_KEYWORD,
    pstbck.POST_CREATIVE,
    CAST(pstbck.POST_FTD_DATE AS STRING) AS POST_FTD_DATE,
    pstbck.POST_PAGE,
    pstbck.POST_GCLID,
    CAST(pstbck.POST_SIGNUP_TIMESTAMP AS DATETIME) AS POST_SIGNUP_TIMESTAMP,
    pstbck.POST_UTM_TERM,
    pstbck.POST_3RD_PARTY_CLICKID,
    pstbck.POST_ID,
    pstbck.POST_ADACCOUNTID,
    pstbck.POST_MARKETING_SITE_ID,
    pstbck.POST_CLICKID,
    CAST(pstbck.POST_CPA_DATE AS STRING) AS POST_CPA_DATE,
    pstbck.POST_TEST_VARIATION,
    pstbck.POST_APP_INSTANCE_ID,
    pstbck.POST_FIREBASE_APP_ID,
    pstbck._AIRBYTE_AB_ID,
    pstbck._AIRBYTE_EMITTED_AT,
    pstbck._AIRBYTE_NORMALIZED_AT,
    pstbck._AIRBYTE_POSTBACK_TRACKING_HASHID,
    pstbck._AIRBYTE_UNIQUE_KEY
FROM {{ source('BRC', 'POSTBACK_TRACKING') }} AS pstbck
WHERE UPPER(pstbck.post_clickid) IN 
(
    SELECT UPPER(AFP) FROM {{ source('SWEEP', 'ICT_FTD_REGISTRATION_REPORT') }}
)
