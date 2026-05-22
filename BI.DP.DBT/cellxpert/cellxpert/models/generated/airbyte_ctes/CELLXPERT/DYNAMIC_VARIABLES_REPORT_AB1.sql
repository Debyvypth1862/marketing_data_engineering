{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "CELLXPERT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('CELLXPERT', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposits'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Withdrawals'], ['Withdrawals']) }} as WITHDRAWALS,
    {{ json_extract_scalar('_airbyte_data', ['data','afp'], ['afp']) }} as AFP,
    {{ json_extract_scalar('_airbyte_data', ['data','Net_Deposits'], ['Net_Deposits']) }} as NET_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','userId'], ['userId']) }} as USERID,
    {{ json_extract_scalar('_airbyte_data', ['data','Commissions'], ['Commissions']) }} as COMMISSIONS,
    {{ json_extract_scalar('_airbyte_data', ['data','Brand'], ['Brand']) }} as BRAND,
    {{ json_extract_scalar('_airbyte_data', ['data','Volume'], ['Volume']) }} as VOLUME,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposit_Count'], ['Deposit_Count']) }} as DEPOSIT_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Commission_Count'], ['Commission_Count']) }} as COMMISSION_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Position_Count'], ['Position_Count']) }} as POSITION_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','PL'], ['PL']) }} as PL,
    {{ json_extract_scalar('_airbyte_data', ['data','Tracking_Code'], ['Tracking_Code']) }} as TRACKING_CODE,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('CELLXPERT', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)
