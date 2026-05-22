{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "EGO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('EGO', '_AIRBYTE_RAW_BRAND_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['data','Affiliate'],['Affiliate']) }} as AFFILIATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Affiliate revenue'],['Affiliate revenue']) }} as AFFILIATE_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['data','ChargeBack QTY'],['ChargeBack QTY']) }} as CHARGEBACK_QTY,
    {{ json_extract_scalar('_airbyte_data', ['data','Complete downloads'],['Complete downloads']) }} as COMPLETE_DOWNLOADS,
    {{ json_extract_scalar('_airbyte_data', ['data','Credit QTY'],['Credit QTY']) }} as CREDIT_QTY,
    {{ json_extract_scalar('_airbyte_data', ['data','Date'],['Date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','DynID'],['DynID']) }} as DYNID,
    {{ json_extract_scalar('_airbyte_data', ['data','First Deposits Qty'],['First Deposits Qty']) }} as FIRST_DEPOSITS_QTY,
    {{ json_extract_scalar('_airbyte_data', ['data','Flat Fee'],['Flat Fee']) }} as FLAT_FEE,
    {{ json_extract_scalar('_airbyte_data', ['data','Fraud QTY'],['Fraud QTY']) }} as FRAUD_QTY,
    {{ json_extract_scalar('_airbyte_data', ['data','Hits'],['Hits']) }} as HITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Net Income'],['Net Income']) }} as NET_INCOME,
    {{ json_extract_scalar('_airbyte_data', ['data','Revenue CPA'],['Revenue CPA']) }} as REVENUE_CPA,
    {{ json_extract_scalar('_airbyte_data', ['data','Revenue Override'],['Revenue Override']) }} as REVENUE_OVERRIDE,
    {{ json_extract_scalar('_airbyte_data', ['data','Revenue Share'],['Revenue Share']) }} as REVENUE_SHARE,
    {{ json_extract_scalar('_airbyte_data', ['data','Revenue Subs'],['Revenue Subs']) }} as REVENUE_SUBS,
    {{ json_extract_scalar('_airbyte_data', ['data','Sign Ups'],['Sign Ups']) }} as SIGN_UPS,
    {{ json_extract_scalar('_airbyte_data', ['data','Valid Sign Ups'],['Valid Sign Ups']) }} as VALID_SIGN_UPS,
    {{ json_extract_scalar('_airbyte_data', ['data','Void QTY'],['Void QTY']) }} as VOID_QTY,
    {{ json_extract_scalar('_airbyte_data', ['data','Zone ID'],['Zone ID']) }} as ZONE_ID,
    {{ json_extract_scalar('_airbyte_data', ['report'], ['report']) }} as REPORT,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('EGO', '_AIRBYTE_RAW_BRAND_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

