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
    {{ json_extract_scalar('_airbyte_data', ['attempts'], ['attempts']) }} as ATTEMPTS,
    {{ json_extract_scalar('_airbyte_data', ['campaign'], ['campaign']) }} as CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['campaign_id'], ['campaign_id']) }} as CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['conversion_id'], ['conversion_id']) }} as CONVERSION_ID,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['destination'], ['destination']) }} as DESTINATION,
    {{ json_extract_scalar('_airbyte_data', ['error'], ['error']) }} as ERROR,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['previous_at'], ['previous_at']) }} as PREVIOUS_AT,
    {{ json_extract_scalar('_airbyte_data', ['ref_id'], ['ref_id']) }} as REF_ID,
    {{ json_extract_scalar('_airbyte_data', ['source'], ['source']) }} as SOURCE,
    {{ json_extract_scalar('_airbyte_data', ['source_id'], ['source_id']) }} as SOURCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['total'], ['total']) }} as TOTAL,
    {{ json_extract_scalar('_airbyte_data', ['track_id'], ['track_id']) }} as TRACK_ID,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as TYPE,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_API_POSTBACKS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}