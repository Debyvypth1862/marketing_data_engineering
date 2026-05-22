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
    {{ json_extract_scalar('_airbyte_data', ['campaign_count'], ['campaign_count']) }} as CAMPAIGN_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['cost_id'], ['cost_id']) }} as COST_ID,
    {{ json_extract_scalar('_airbyte_data', ['cost_level'], ['cost_level']) }} as COST_LEVEL,
    {{ json_extract_scalar('_airbyte_data', ['cost_models'], ['cost_models']) }} as COST_MODELS,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['currency'], ['currency']) }} as CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['enable_direct_traffic'], ['enable_direct_traffic']) }} as ENABLE_DIRECT_TRAFFIC,
    {{ json_extract_scalar('_airbyte_data', ['enable_impressions'], ['enable_impressions']) }} as ENABLE_IMPRESSIONS,
    {{ json_extract_scalar('_airbyte_data', ['enable_parallel_tracking'], ['enable_parallel_tracking']) }} as ENABLE_PARALLEL_TRACKING,
    {{ json_extract_scalar('_airbyte_data', ['external_id'], ['external_id']) }} as EXTERNAL_ID,
    {{ json_extract_scalar('_airbyte_data', ['external_id_alias'], ['external_id_alias']) }} as EXTERNAL_ID_ALIAS,
    {{ json_extract_scalar('_airbyte_data', ['formats'], ['formats']) }} as FORMATS,
    {{ json_extract_scalar('_airbyte_data', ['google_analytics_key'], ['google_analytics_key']) }} as GOOGLE_ANALYTICS_KEY,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['imp_cost_id'], ['imp_cost_id']) }} as IMP_COST_ID,
    {{ json_extract_scalar('_airbyte_data', ['imp_id'], ['imp_id']) }} as IMP_ID,
    {{ json_extract_scalar('_airbyte_data', ['integration_id'], ['integration_id']) }} as INTEGRATION_ID,
    {{ json_extract_scalar('_airbyte_data', ['integration_types'], ['integration_types']) }} as INTEGRATION_TYPES,
    {{ json_extract_scalar('_airbyte_data', ['integrations'], ['integrations']) }} as INTEGRATIONS,
    {{ json_extract_scalar('_airbyte_data', ['postback_pixel'], ['postback_pixel']) }} as POSTBACK_PIXEL,
    {{ json_extract_scalar('_airbyte_data', ['postback_url'], ['postback_url']) }} as POSTBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['preset_id'], ['preset_id']) }} as PRESET_ID,
    {{ json_extract_scalar('_airbyte_data', ['ref_id'], ['ref_id']) }} as REF_ID,
    {{ json_extract_scalar('_airbyte_data', ['ref_id_alias'], ['ref_id_alias']) }} as REF_ID_ALIAS,
    {{ json_extract_scalar('_airbyte_data', ['serial_number'], ['serial_number']) }} as SERIAL_NUMBER,
    {{ json_extract_scalar('_airbyte_data', ['stat'], ['stat']) }} as STAT,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['subs'], ['subs']) }} as SUBS,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as TITLE,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as TYPE,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_TRAFFIC_CHANNELS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}