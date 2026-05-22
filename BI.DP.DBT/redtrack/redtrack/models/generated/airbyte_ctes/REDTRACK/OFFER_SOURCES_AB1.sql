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
    {{ json_extract_scalar('_airbyte_data', ['alias'], ['alias']) }} as ALIAS,
    {{ json_extract_scalar('_airbyte_data', ['click_expiration'], ['click_expiration']) }} as CLICK_EXPIRATION,
    {{ json_extract_scalar('_airbyte_data', ['clickid'], ['clickid']) }} as CLICKID,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['currency'], ['currency']) }} as CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['enable_ip_whitelist'], ['enable_ip_whitelist']) }} as ENABLE_IP_WHITELIST,
    {{ json_extract_scalar('_airbyte_data', ['event_tracking'], ['event_tracking']) }} as EVENT_TRACKING,
    {{ json_extract_scalar('_airbyte_data', ['hints'], ['hints']) }} as HINTS,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['ip_whitelist'], ['ip_whitelist']) }} as IP_WHITELIST,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['offer_count'], ['offer_count']) }} as OFFER_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['offer_url'], ['offer_url']) }} as OFFER_URL,
    {{ json_extract_scalar('_airbyte_data', ['postback_mode'], ['postback_mode']) }} as POSTBACK_MODE,
    {{ json_extract_scalar('_airbyte_data', ['postback_protected'], ['postback_protected']) }} as POSTBACK_PROTECTED,
    {{ json_extract_scalar('_airbyte_data', ['postback_status'], ['postback_status']) }} as POSTBACK_STATUS,
    {{ json_extract_scalar('_airbyte_data', ['postback_token'], ['postback_token']) }} as POSTBACK_TOKEN,
    {{ json_extract_scalar('_airbyte_data', ['postback_url'], ['postback_url']) }} as POSTBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['preset_id'], ['preset_id']) }} as PRESET_ID,
    {{ json_extract_scalar('_airbyte_data', ['serial_number'], ['serial_number']) }} as SERIAL_NUMBER,
    {{ json_extract_scalar('_airbyte_data', ['stat'], ['stat']) }} as STAT,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['sub1'], ['sub1']) }} as SUB1,
    {{ json_extract_scalar('_airbyte_data', ['sub10'], ['sub10']) }} as SUB10,
    {{ json_extract_scalar('_airbyte_data', ['sub11'], ['sub11']) }} as SUB11,
    {{ json_extract_scalar('_airbyte_data', ['sub12'], ['sub12']) }} as SUB12,
    {{ json_extract_scalar('_airbyte_data', ['sub13'], ['sub13']) }} as SUB13,
    {{ json_extract_scalar('_airbyte_data', ['sub14'], ['sub14']) }} as SUB14,
    {{ json_extract_scalar('_airbyte_data', ['sub15'], ['sub15']) }} as SUB15,
    {{ json_extract_scalar('_airbyte_data', ['sub16'], ['sub16']) }} as SUB16,
    {{ json_extract_scalar('_airbyte_data', ['sub17'], ['sub17']) }} as SUB17,
    {{ json_extract_scalar('_airbyte_data', ['sub18'], ['sub18']) }} as SUB18,
    {{ json_extract_scalar('_airbyte_data', ['sub19'], ['sub19']) }} as SUB19,
    {{ json_extract_scalar('_airbyte_data', ['sub2'], ['sub2']) }} as SUB2,
    {{ json_extract_scalar('_airbyte_data', ['sub20'], ['sub20']) }} as SUB20,
    {{ json_extract_scalar('_airbyte_data', ['sub3'], ['sub3']) }} as SUB3,
    {{ json_extract_scalar('_airbyte_data', ['sub4'], ['sub4']) }} as SUB4,
    {{ json_extract_scalar('_airbyte_data', ['sub5'], ['sub5']) }} as SUB5,
    {{ json_extract_scalar('_airbyte_data', ['sub6'], ['sub6']) }} as SUB6,
    {{ json_extract_scalar('_airbyte_data', ['sub7'], ['sub7']) }} as SUB7,
    {{ json_extract_scalar('_airbyte_data', ['sub8'], ['sub8']) }} as SUB8,
    {{ json_extract_scalar('_airbyte_data', ['sub9'], ['sub9']) }} as SUB9,
    {{ json_extract_scalar('_airbyte_data', ['subs'], ['subs']) }} as SUBS,
    {{ json_extract_scalar('_airbyte_data', ['sum'], ['sum']) }} as SUM,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as TITLE,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_OFFER_SOURCES') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}