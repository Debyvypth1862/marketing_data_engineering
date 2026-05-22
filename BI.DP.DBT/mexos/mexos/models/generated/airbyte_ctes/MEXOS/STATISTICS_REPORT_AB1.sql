{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "MEXOS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('MEXOS', '_AIRBYTE_RAW_STATISTICS_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Casino Net Gaming Commission'], ['Casino Net Gaming Commission']) }} as CASINO_NET_GAMING_COMMISSION,
    {{ json_extract_scalar('_airbyte_data', ['data','Casino RFD Amt'], ['Casino RFD Amt']) }} as CASINO_RFD_AMT,
    {{ json_extract_scalar('_airbyte_data', ['data','Casino RFD Cnt'], ['Casino RFD Cnt']) }} as CASINO_RFD_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Casino Signups Cnt'], ['Casino Signups Cnt']) }} as CASINO_SIGNUPS_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Commission'], ['Commission']) }} as COMMISSION,
    --{{ json_extract_scalar('_airbyte_data', ['data','Date'], ['Date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposit Cnt'], ['Deposit Cnt']) }} as DEPOSIT_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Impressions'], ['Impressions']) }} as IMPRESSIONS,
    {{ json_extract_scalar('_airbyte_data', ['data','Net Gaming After Deduction'], ['Net Gaming After Deduction']) }} as NET_GAMING_AFTER_DEDUCTION,
    {{ json_extract_scalar('_airbyte_data', ['data','Sport Net Gaming Commission'], ['Sport Net Gaming Commission']) }} as SPORT_NET_GAMING_COMMISSION,
    {{ json_extract_scalar('_airbyte_data', ['data','Sport RFD Amt'], ['Sport RFD Amt']) }} as SPORT_RFD_AMT,
    {{ json_extract_scalar('_airbyte_data', ['data','Sport RFD Cnt'], ['Sport RFD Cnt']) }} as SPORT_RFD_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Sport Signups Cnt'], ['Sport Signups Cnt']) }} as SPORT_SIGNUPS_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Bonuses Eur'], ['Total Bonuses Eur']) }} as TOTAL_BONUSES_EUR,
    {{ json_extract_scalar('_airbyte_data', ['data','Unique Clicks'], ['Unique Clicks']) }} as UNIQUE_CLICKS,
    {{ json_extract_scalar('_airbyte_data', ['data','Var 1'], ['Var 1']) }} as VAR_1,
    {{ json_extract_scalar('_airbyte_data', ['data','Var 2'], ['Var 2']) }} as VAR_2,
    {{ json_extract_scalar('_airbyte_data', ['data','Var 9'], ['Var 9']) }} as VAR_9,
    {{ json_extract_scalar('_airbyte_data', ['data','Var 10'], ['Var 10']) }} as VAR_10,
    {{ json_extract_scalar('_airbyte_data', ['data','Wins'], ['Wins']) }} as WINS,
    {{ json_extract_scalar('_airbyte_data', ['data','Withdrawal Amt'], ['Withdrawal Amt']) }} as WITHDRAWAL_AMT,
    {{ json_extract_scalar('_airbyte_data', ['data','Withdrawal Cnt'], ['Withdrawal Cnt']) }} as WITHDRAWAL_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Deposit Cnt'], ['Total Deposit Cnt']) }} as TOTAL_DEPOSIT_CNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Deposit Amt'], ['Total Deposit Amt']) }} as TOTAL_DEPOSIT_AMT,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('MEXOS', '_AIRBYTE_RAW_STATISTICS_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

