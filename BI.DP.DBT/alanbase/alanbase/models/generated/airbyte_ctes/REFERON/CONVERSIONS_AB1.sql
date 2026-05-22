{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "ALANBASE",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('ALANBASE', '_AIRBYTE_RAW_CONVERSIONS') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)

select
    {{ json_extract_scalar('_airbyte_data', ['conversion_id'], ['conversion_id']) }} as conversion_id,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['conversion_datetime'], ['conversion_datetime']) }} as conversion_datetime,
    {{ json_extract_scalar('_airbyte_data', ['payment_model'], ['payment_model']) }} as payment_model,
    {{ json_extract_scalar('_airbyte_data', ['payout'], ['payout']) }} as payout,
    {{ json_extract_scalar('_airbyte_data', ['payout_currency'], ['payout_currency']) }} as payout_currency,
    {{ json_extract_scalar('_airbyte_data', ['sub1'], ['sub1']) }} as sub1,
    {{ json_extract_scalar('_airbyte_data', ['sub2'], ['sub2']) }} as sub2,
    {{ json_extract_scalar('_airbyte_data', ['edited_by_manager'], ['edited_by_manager']) }} as edited_by_manager,
    {{ json_extract_scalar('_airbyte_data', ['click_id'], ['click_id']) }} as click_id,
    {{ json_extract_scalar('_airbyte_data', ['click_datetime'], ['click_datetime']) }} as click_datetime,
    {{ json_extract_scalar('_airbyte_data', ['click_redirect_url'], ['click_redirect_url']) }} as click_redirect_url,
    {{ json_extract_scalar('_airbyte_data', ['click_ip'], ['click_ip']) }} as click_ip,
    {{ json_extract_scalar('_airbyte_data', ['browser'], ['browser']) }} as browser,
    {{ json_extract_scalar('_airbyte_data', ['os'], ['os']) }} as os,
    {{ json_extract_scalar('_airbyte_data', ['device_type'], ['device_type']) }} as device_type,
    {{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as country,
    {{ json_extract_scalar('_airbyte_data', ['referer'], ['referer']) }} as referer,
    {{ json_extract_scalar('_airbyte_data', ['condition_id'], ['condition_id']) }} as condition_id,
    {{ json_extract_scalar('_airbyte_data', ['is_qualification'], ['is_qualification']) }} as is_qualification,
    {{ json_extract_scalar('_airbyte_data', ['user_agent'], ['user_agent']) }} as user_agent,
    {{ json_extract_scalar('_airbyte_data', ['landing_id'], ['landing_id']) }} as landing_id,
    {{ json_extract_scalar('_airbyte_data', ['goal','name'], ['name']) }} as goal,
    {{ json_extract_scalar('_airbyte_data', ['offer','id'], ['id']) }} as offer_id,
    {{ json_extract_scalar('_airbyte_data', ['offer','name'], ['name']) }} as offer_name,
    {{ json_extract_scalar('_airbyte_data', ['offer','tags'], ['tags']) }} as offer_tags,
    {{ json_extract_scalar('_airbyte_data', ['partner','id'], ['id']) }} as partner_id,
    {{ json_extract_scalar('_airbyte_data', ['partner','email'], ['email']) }} as partner_email,
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as date,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as tracker_login_id,
    {{ json_extract_scalar('_airbyte_data', ['decline_reason'], ['decline_reason']) }} as decline_reason,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    _AIRBYTE_EMMITTED_DATE,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('ALANBASE', '_AIRBYTE_RAW_CONVERSIONS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE 1=1
-- AND s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)

