{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    
    {{ json_extract_scalar('_airbyte_data', ['cache_buster_enabled'], ['cache_buster_enabled']) }} as CACHE_BUSTER_ENABLED,
    {{ json_extract_scalar('_airbyte_data', ['cost_model'], ['cost_model']) }} as COST_MODEL,
    {{ json_extract_scalar('_airbyte_data', ['coupon'], ['coupon']) }} as COUPON,
    {{ json_extract_scalar('_airbyte_data', ['cpc'], ['cpc']) }} as CPC,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['creatives'], ['creatives']) }} as CREATIVES,
    {{ json_extract_scalar('_airbyte_data', ['custom_conv_type_conv_subs_geo_payouts'], ['custom_conv_type_conv_subs_geo_payouts']) }} as CUSTOM_CONV_TYPE_CONV_SUBS_GEO_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_conv_type_conv_subs_payouts'], ['custom_conv_type_conv_subs_payouts']) }} as CUSTOM_CONV_TYPE_CONV_SUBS_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_conversion_subs_payouts'], ['custom_conversion_subs_payouts']) }} as CUSTOM_CONVERSION_SUBS_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_conversion_type_payouts'], ['custom_conversion_type_payouts']) }} as CUSTOM_CONVERSION_TYPE_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_geo_os_payouts'], ['custom_geo_os_payouts']) }} as CUSTOM_GEO_OS_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_geo_payouts'], ['custom_geo_payouts']) }} as CUSTOM_GEO_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_pub_payouts'], ['custom_pub_payouts']) }} as CUSTOM_PUB_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_pub_sub_geo_payouts'], ['custom_pub_sub_geo_payouts']) }} as CUSTOM_PUB_SUB_GEO_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_pub_sub_payouts'], ['custom_pub_sub_payouts']) }} as CUSTOM_PUB_SUB_PAYOUTS,
    {{ json_extract_scalar('_airbyte_data', ['custom_pub_sub_throttles'], ['custom_pub_sub_throttles']) }} as CUSTOM_PUB_SUB_THROTTLES,
    {{ json_extract_scalar('_airbyte_data', ['custom_pub_throttles'], ['custom_pub_throttles']) }} as CUSTOM_PUB_THROTTLES,
    {{ json_extract_scalar('_airbyte_data', ['custom_throttles'], ['custom_throttles']) }} as CUSTOM_THROTTLES,
    {{ json_extract_scalar('_airbyte_data', ['domain_id'], ['domain_id']) }} as DOMAIN_ID,
    {{ json_extract_scalar('_airbyte_data', ['expires_at'], ['expires_at']) }} as EXPIRES_AT,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['impression_postbacks'], ['impression_postbacks']) }} as IMPRESSION_POSTBACKS,
    {{ json_extract_scalar('_airbyte_data', ['impression_url'], ['impression_url']) }} as IMPRESSION_URL,
    {{ json_extract_scalar('_airbyte_data', ['integration_postback'], ['integration_postback']) }} as INTEGRATION_POSTBACK,
    {{ json_extract_scalar('_airbyte_data', ['integrations'], ['integrations']) }} as INTEGRATIONS,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['notifications'], ['notifications']) }} as NOTIFICATIONS,
    {{ json_extract_scalar('_airbyte_data', ['pixels'], ['pixels']) }} as PIXELS,
    {{ json_extract_scalar('_airbyte_data', ['postback_url'], ['postback_url']) }} as POSTBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['postbacks'], ['postbacks']) }} as POSTBACKS,
    {{ json_extract_scalar('_airbyte_data', ['publisher_details'], ['publisher_details']) }} as PUBLISHER_DETAILS,
    {{ json_extract_scalar('_airbyte_data', ['redirect_type'], ['redirect_type']) }} as REDIRECT_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['rev_share'], ['rev_share']) }} as REV_SHARE,
    {{ json_extract_scalar('_airbyte_data', ['serial_number'], ['serial_number']) }} as SERIAL_NUMBER,
    {{ json_extract_scalar('_airbyte_data', ['source_campaign_id'], ['source_campaign_id']) }} as SOURCE_CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['source_campaigns'], ['source_campaigns']) }} as SOURCE_CAMPAIGNS,
    {{ json_extract_scalar('_airbyte_data', ['source_id'], ['source_id']) }} as SOURCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['source_title'], ['source_title']) }} as SOURCE_TITLE,
    {{ json_extract_scalar('_airbyte_data', ['stat'], ['stat']) }} as STAT,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['streams'], ['streams']) }} as STREAMS,
    {{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as TAGS,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as TITLE,
    {{ json_extract_scalar('_airbyte_data', ['trackback_url'], ['trackback_url']) }} as TRACKBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as TYPE,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_CAMPAIGNS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}