{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SWEEP",
    pre_hook = "ALTER EXTERNAL TABLE STG.SWEEP._AIRBYTE_RAW_REGISTRATION_REPORT_STREAM REFRESH;",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('SWEEP', '_AIRBYTE_RAW_REGISTRATION_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['data','Registration_Date'], ['Registration_Date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Commissions'], ['Commissions']) }} as COMMISSIONS,
    {{ json_extract_scalar('_airbyte_data', ['data','Commission'], ['Commission']) }} as COMMISSION,
    {{ json_extract_scalar('_airbyte_data', ['data','Country'], ['Country']) }} as COUNTRY,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposit_Count'], ['Deposit_Count']) }} as DEPOSIT_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposits'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','External_Date'], ['External_Date']) }} as EXTERNAL_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','First_Deposit'], ['First_Deposit']) }} as FIRST_DEPOSIT,
    {{ json_extract_scalar('_airbyte_data', ['data','First_Deposit_Date'], ['First_Deposit_Date']) }} as FIRST_DEPOSIT_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Net_Deposits'], ['Net_Deposits']) }} as NET_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','PL'], ['PL']) }} as PL,
    {{ json_extract_scalar('_airbyte_data', ['data','Qualification_Date'], ['Qualification_Date']) }} as QUALIFICATION_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Registration_Date'], ['Registration_Date']) }} as REGISTRATION_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Status'], ['Status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['data','Tracking_Code'], ['Tracking_Code']) }} as TRACKING_CODE,
    {{ json_extract_scalar('_airbyte_data', ['data','User_ID'], ['User_ID']) }} as USERID,
    {{ json_extract_scalar('_airbyte_data', ['data','Withdrawals'], ['Withdrawals']) }} as WITHDRAWALS,
    {{ json_extract_scalar('_airbyte_data', ['data','afp'], ['afp']) }} as AFP,
    {{ json_extract_scalar('_airbyte_data', ['data','generic1'], ['generic1']) }} as GENERIC_1,
    {{ json_extract_scalar('_airbyte_data', ['data','generic2'], ['generic2']) }} as GENERIC_2,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('SWEEP', '_AIRBYTE_RAW_REGISTRATION_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)

