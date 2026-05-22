{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
	{{ json_extract_scalar('_airbyte_data', ['post_subid4'], ['post_subid4']) }} as POST_SUBID4,
    {{ json_extract_scalar('_airbyte_data', ['post_ftd_timestamp'], ['post_ftd_timestamp']) }} as POST_FTD_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['post_fbclid'], ['post_fbclid']) }} as POST_FBCLID,
    {{ json_extract_scalar('_airbyte_data', ['post_subid5'], ['post_subid5']) }} as POST_SUBID5,
    {{ json_extract_scalar('_airbyte_data', ['post_ow_id'], ['post_ow_id']) }} as POST_OW_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_affiliate_id'], ['post_affiliate_id']) }} as POST_AFFILIATE_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_ip'], ['post_ip']) }} as POST_IP,
    {{ json_extract_scalar('_airbyte_data', ['post_click_date'], ['post_click_date']) }} as POST_CLICK_DATE,
    {{ json_extract_scalar('_airbyte_data', ['post_page_location'], ['post_page_location']) }} as POST_PAGE_LOCATION,
    {{ json_extract_scalar('_airbyte_data', ['post_fk_tracker'], ['post_fk_tracker']) }} as POST_FK_TRACKER,
    {{ json_extract_scalar('_airbyte_data', ['post_click_timestamp'], ['post_click_timestamp']) }} as POST_CLICK_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['post_subid2'], ['post_subid2']) }} as POST_SUBID2,
    {{ json_extract_scalar('_airbyte_data', ['post_fk_camt_id'], ['post_fk_camt_id']) }} as POST_FK_CAMT_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_subid3'], ['post_subid3']) }} as POST_SUBID3,
    {{ json_extract_scalar('_airbyte_data', ['post_adgroupid'], ['post_adgroupid']) }} as POST_ADGROUPID,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_content'], ['post_utm_content']) }} as POST_UTM_CONTENT,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_id'], ['post_utm_id']) }} as POST_UTM_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_subid'], ['post_subid']) }} as POST_SUBID,
    {{ json_extract_scalar('_airbyte_data', ['post_ga4_device_id'], ['post_ga4_device_id']) }} as POST_GA4_DEVICE_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_modified_timestamp'], ['post_modified_timestamp']) }} as POST_MODIFIED_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['post_cpa_timestamp'], ['post_cpa_timestamp']) }} as POST_CPA_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_source'], ['post_utm_source']) }} as POST_UTM_SOURCE,
    {{ json_extract_scalar('_airbyte_data', ['post_signup_date'], ['post_signup_date']) }} as POST_SIGNUP_DATE,
    {{ json_extract_scalar('_airbyte_data', ['post_site_member_id'], ['post_site_member_id']) }} as POST_SITE_MEMBER_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_campaign'], ['post_utm_campaign']) }} as POST_UTM_CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['post_campaignid'], ['post_campaignid']) }} as POST_CAMPAIGNID,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_medium'], ['post_utm_medium']) }} as POST_UTM_MEDIUM,
    {{ json_extract_scalar('_airbyte_data', ['post_env'], ['post_env']) }} as POST_ENV,
    {{ json_extract_scalar('_airbyte_data', ['post_keyword'], ['post_keyword']) }} as POST_KEYWORD,
    {{ json_extract_scalar('_airbyte_data', ['post_creative'], ['post_creative']) }} as POST_CREATIVE,
    {{ json_extract_scalar('_airbyte_data', ['post_ftd_date'], ['post_ftd_date']) }} as POST_FTD_DATE,
    {{ json_extract_scalar('_airbyte_data', ['post_page'], ['post_page']) }} as POST_PAGE,
    {{ json_extract_scalar('_airbyte_data', ['post_gclid'], ['post_gclid']) }} as POST_GCLID,
    {{ json_extract_scalar('_airbyte_data', ['post_signup_timestamp'], ['post_signup_timestamp']) }} as POST_SIGNUP_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['post_utm_term'], ['post_utm_term']) }} as POST_UTM_TERM,
    {{ json_extract_scalar('_airbyte_data', ['post_3rd_party_clickid'], ['post_3rd_party_clickid']) }} as POST_3RD_PARTY_CLICKID,
    {{ json_extract_scalar('_airbyte_data', ['post_id'], ['post_id']) }} as POST_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_adaccountid'], ['post_adaccountid']) }} as POST_ADACCOUNTID,
    {{ json_extract_scalar('_airbyte_data', ['post_marketing_site_id'], ['post_marketing_site_id']) }} as POST_MARKETING_SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_clickid'], ['post_clickid']) }} as POST_CLICKID,
    {{ json_extract_scalar('_airbyte_data', ['post_cpa_date'], ['post_cpa_date']) }} as POST_CPA_DATE,
    {{ json_extract_scalar('_airbyte_data', ['post_test_variation'], ['post_test_variation']) }} as POST_TEST_VARIATION,
    {{ json_extract_scalar('_airbyte_data', ['post_app_instance_id'], ['post_app_instance_id']) }} as POST_APP_INSTANCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['post_firebase_app_id'], ['post_firebase_app_id']) }} as POST_FIREBASE_APP_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	S3_PATH,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from 
(
SELECT to_varchar(_airbyte_data) _airbyte_data, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT ,S3_PATH,_AIRBYTE_EMMITTED_DATE FROM {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_TRACKING') }}
UNION ALL 
SELECT to_varchar(_airbyte_data) _airbyte_data, _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT ,S3_PATH,_AIRBYTE_EMMITTED_DATE  FROM {{ source('BRC', '_AIRBYTE_RAW_MODIFIED_DATE_POSTBACK_TRACKING') }}
 ) as table_alias
LEFT JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)
