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
    {{ json_extract_scalar('_airbyte_data', ['campaign'], ['campaign']) }} as CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['campaign_id'], ['campaign_id']) }} as CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as COUNTRY,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['currency'], ['currency']) }} as CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['level'], ['level']) }} as LEVEL,
    {{ json_extract_scalar('_airbyte_data', ['period'], ['period']) }} as PERIOD,
    {{ json_extract_scalar('_airbyte_data', ['rt_ad_id'], ['rt_ad_id']) }} as RT_AD_ID,
    {{ json_extract_scalar('_airbyte_data', ['rt_adgroup_id'], ['rt_adgroup_id']) }} as RT_ADGROUP_ID,
    {{ json_extract_scalar('_airbyte_data', ['rt_campaign_id'], ['rt_campaign_id']) }} as RT_CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['rt_placement_id'], ['rt_placement_id']) }} as RT_PLACEMENT_ID,
    {{ json_extract_scalar('_airbyte_data', ['source_alias'], ['source_alias']) }} as SOURCE_ALIAS,
    {{ json_extract_scalar('_airbyte_data', ['source_cost'], ['source_cost']) }} as SOURCE_COST,
    {{ json_extract_scalar('_airbyte_data', ['source_timezone'], ['source_timezone']) }} as SOURCE_TIMEZONE,
    {{ json_extract_scalar('_airbyte_data', ['time_from'], ['time_from']) }} as TIME_FROM,
    {{ json_extract_scalar('_airbyte_data', ['time_to'], ['time_to']) }} as TIME_TO,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
    {{ json_extract_scalar('_airbyte_data', ['user_timezone'], ['user_timezone']) }} as USER_TIMEZONE,
    _AIRBYTE_AB_ID,
    S3_PATH,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REDTRACK', '_AIRBYTE_RAW_API_COSTS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH
WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}