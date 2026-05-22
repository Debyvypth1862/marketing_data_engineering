{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "NETREFER",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('NETREFER', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    --{{ json_extract_scalar('_airbyte_data', ['data','Date'], ['Date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Active Customers'], ['Active Customers']) }} as ACTIVE_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','Affiliate ID'], ['Affiliate ID']) }} as AFFILIATES_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Clicks'], ['Clicks']) }} as CLICKS,
    {{ json_extract_scalar('_airbyte_data', ['data','Depositing Customers'], ['Depositing Customers']) }} as DEPOSITING_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposits'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','First Time Active Customers'], ['First Time Active Customers']) }} as FIRST_TIME_ACTIVE_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','First Time Depositing Customers'], ['First Time Depositing Customers']) }} as FIRST_TIME_DEPOSITING_CUSTOMER,
    {{ json_extract_scalar('_airbyte_data', ['data','Marketing Source ID'], ['Marketing Source ID']) }} as MARKETING_SOURCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Marketing Source Name'], ['Marketing Source Name']) }} as MARKETING_SOURCE_NAME,
    {{ json_extract_scalar('_airbyte_data', ['data','Media ID'], ['Media ID']) }} as MEDIA_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Net Revenue'], ['Net Revenue']) }} as NET_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['data','New Active Customers'], ['New Active Customers']) }} as NEW_ACTIVE_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','New Depositing Customers'], ['New Depositing Customers']) }} as NEW_DEPOSITING_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','Signups'], ['Signups']) }} as SIGNUPS,
    {{ json_extract_scalar('_airbyte_data', ['data','Unique Clicks'], ['Unique Clicks']) }} as UNIQUE_CLICKS,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    -- variables list
    CASE 
        WHEN 
            {{ json_extract_scalar('_airbyte_data', ['data','click_id'], ['click_id']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['data','click_id'], ['click_id']) }} <> 'N/A' 
            THEN {{ json_extract_scalar('_airbyte_data', ['data','click_id'], ['click_id']) }}
        WHEN 
            {{ json_extract_scalar('_airbyte_data', ['data','clickid'], ['clickid']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['data','clickid'], ['clickid']) }} <> 'N/A'
            THEN {{ json_extract_scalar('_airbyte_data', ['data','clickid'], ['clickid']) }}
        WHEN 
            {{ json_extract_scalar('_airbyte_data', ['data','subid'], ['subid']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['data','subid'], ['subid']) }} <> 'N/A'
            THEN {{ json_extract_scalar('_airbyte_data', ['data','subid'], ['subid']) }}
        WHEN 
         {{ json_extract_scalar('_airbyte_data', ['data','ClickId_'], ['ClickId_']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['data','ClickId_'], ['ClickId_']) }} <> 'N/A'
         THEN {{ json_extract_scalar('_airbyte_data', ['data','ClickId_'], ['ClickId_']) }}
        WHEN
         {{ json_extract_scalar('_airbyte_data', ['data','subID'], ['subid']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['data','subID'], ['subID']) }} <> 'N/A'
         THEN {{ json_extract_scalar('_airbyte_data', ['data','subID'], ['subID']) }}
        WHEN 
            {{ json_extract_scalar('_airbyte_data', ['clickid'], ['clickid']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['clickid'], ['clickid']) }} <> 'N/A'
            THEN {{ json_extract_scalar('_airbyte_data', ['clickid'], ['clickid']) }}
        WHEN 
            {{ json_extract_scalar('_airbyte_data', ['subid'], ['subid']) }} IS NOT NULL AND {{ json_extract_scalar('_airbyte_data', ['subid'], ['subid']) }} <> 'N/A'
            THEN {{ json_extract_scalar('_airbyte_data', ['subid'], ['subid']) }}
    END AS CLICK_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('NETREFER', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= DATEADD(DAY,-7,CURRENT_DATE)

