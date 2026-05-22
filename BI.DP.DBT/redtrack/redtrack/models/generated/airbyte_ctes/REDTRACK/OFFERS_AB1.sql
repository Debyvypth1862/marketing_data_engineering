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
    {{ json_extract_scalar('_airbyte_data', ['action_source_fb'], ['action_source_fb']) }} as ACTION_SOURCE_FB,
    {{ json_extract_scalar('_airbyte_data', ['cap'], ['cap']) }} as CAP,
    {{ json_extract_scalar('_airbyte_data', ['cap_alert'], ['cap_alert']) }} as CAP_ALERT,
    {{ json_extract_scalar('_airbyte_data', ['clcap'], ['clcap']) }} as CLCAP,
    {{ json_extract_scalar('_airbyte_data', ['clcap_alert'], ['clcap_alert']) }} as CLCAP_ALERT,
    {{ json_extract_scalar('_airbyte_data', ['click_cap'], ['click_cap']) }} as CLICK_CAP,
    {{ json_extract_scalar('_airbyte_data', ['click_cap_period'], ['click_cap_period']) }} as CLICK_CAP_PERIOD,
    {{ json_extract_scalar('_airbyte_data', ['click_cap_type'], ['click_cap_type']) }} as CLICK_CAP_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['country_codes'], ['country_codes']) }} as COUNTRY_CODES,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['default_conversion_status'], ['default_conversion_status']) }} as DEFAULT_CONVERSION_STATUS,
    {{ json_extract_scalar('_airbyte_data', ['event_source_url_fb'], ['event_source_url_fb']) }} as EVENT_SOURCE_URL_FB,
    {{ json_extract_scalar('_airbyte_data', ['expires_at'], ['expires_at']) }} as EXPIRES_AT,
    {{ json_extract_scalar('_airbyte_data', ['facebook_pixels'], ['facebook_pixels']) }} as FACEBOOK_PIXELS,
    {{ json_extract_scalar('_airbyte_data', ['fingerprint_settings'], ['fingerprint_settings']) }} as FINGERPRINT_SETTINGS,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['network_title'], ['network_title']) }} as NETWORK_TITLE,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['payment'], ['payment']) }} as PAYMENT,
    {{ json_extract_scalar('_airbyte_data', ['postback_url'], ['postback_url']) }} as POSTBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['program_id'], ['program_id']) }} as PROGRAM_ID,
    {{ json_extract_scalar('_airbyte_data', ['serial_number'], ['serial_number']) }} as SERIAL_NUMBER,
    {{ json_extract_scalar('_airbyte_data', ['snapchat_matching'], ['snapchat_matching']) }} as SNAPCHAT_MATCHING,
    {{ json_extract_scalar('_airbyte_data', ['snapchat_pixels'], ['snapchat_pixels']) }} as SNAPCHAT_PIXELS,
    {{ json_extract_scalar('_airbyte_data', ['stat'], ['stat']) }} as STAT,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as TAGS,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as TITLE,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_OFFERS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}