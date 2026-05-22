{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "ALANBASE",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('ALANBASE', '_AIRBYTE_RAW_COMMON_STATISTIC') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select * FROM
(
select
    {{ json_extract_scalar('_airbyte_data', ['click_count'], ['click_count']) }} as CLICK_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['click_unique_count'], ['click_unique_count']) }} as CLICK_UNIQUE_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','confirmed','count'], ['count']) }} as CONFIRMED_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','confirmed','payout'], ['payout']) }} as CONFIRMED_PAYOUT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','hold','count'], ['count']) }} as hold_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','hold','payout'], ['payout']) }} as hold_PAYOUT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','pending','count'], ['count']) }} as pending_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','pending','payout'], ['payout']) }} as pending_PAYOUT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','rejected','count'], ['count']) }} as rejected_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','rejected','payout'], ['payout']) }} as rejected_PAYOUT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','total','count'], ['count']) }} as total_COUNT,    
    {{ json_extract_scalar('_airbyte_data', ['conversions','total','payout'], ['payout']) }} as total_PAYOUT,    
    s.value:"id" as CLICKID,
    
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    _AIRBYTE_EMMITTED_DATE,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('ALANBASE', '_AIRBYTE_RAW_COMMON_STATISTIC') }} as table_alias,
    LATERAL FLATTEN(INPUT => get_path(parse_json(_airbyte_data), '"group_fields"')) s
) js
JOIN s3_status s3
    ON js.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND js._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)

