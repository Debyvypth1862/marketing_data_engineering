{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_PAGES') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['bounceRate'], ['bounceRate']) }} as BOUNCE_RATE,
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['hostName'], ['hostName']) }} as HOST_NAME,
    {{ json_extract_scalar('_airbyte_data', ['pagePathPlusQueryString'], ['pagePathPlusQueryString']) }} as PAGE_PATH_PLUS_QUERY_STRING,
    {{ json_extract_scalar('_airbyte_data', ['property_id'], ['property_id']) }} as PROPERTY_ID,
    {{ json_extract_scalar('_airbyte_data', ['screenPageViews'], ['screenPageViews']) }} as SCREEN_PAGE_VIEWS,
    {{ json_extract_scalar('_airbyte_data', ['uuid'], ['uuid']) }} as UUID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('GOOGLE_ANALYTICS', '_AIRBYTE_RAW_PAGES') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}