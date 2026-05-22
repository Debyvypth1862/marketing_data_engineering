{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "Q_PLATFORM",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('Q_PLATFORM', '_AIRBYTE_RAW_UTM_CODE_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['start'], ['start']) }} as START_DATE,
    {{ json_extract_scalar('_airbyte_data', ['end'], ['end']) }} as END_DATE,
    {{ json_extract_scalar('_airbyte_data', ['merchant'], ['merchant']) }} as MERCHANT,
    {{ json_extract_scalar('_airbyte_data', ['data','affiliate_id'],['affiliate_id']) }} as AFFILIATE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','an_id'],['an_id']) }} as AN_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','anid1'],['anid1']) }} as ANID1,
    {{ json_extract_scalar('_airbyte_data', ['data','anid2'],['anid2']) }} as ANID2,
    {{ json_extract_scalar('_airbyte_data', ['data','anid3'],['anid3']) }} as ANID3,
    {{ json_extract_scalar('_airbyte_data', ['data','anid4'],['anid4']) }} as ANID4,
    {{ json_extract_scalar('_airbyte_data', ['data','anid5'],['anid5']) }} as ANID5,
    {{ json_extract_scalar('_airbyte_data', ['data','cpa_profit'],['cpa_profit']) }} as CPA_PROFIT,
    {{ json_extract_scalar('_airbyte_data', ['data','cpl_profit'],['cpl_profit']) }} as CPL_PROFIT,
    {{ json_extract_scalar('_airbyte_data', ['data','creative_id'],['creative_id']) }} as CREATIVE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','deposits'],['deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','ggr'],['ggr']) }} as GGR,
    {{ json_extract_scalar('_airbyte_data', ['data','merchant_name'],['merchant_name']) }} as MERCHANT_NAME,
    {{ json_extract_scalar('_airbyte_data', ['data','ngr'],['ngr']) }} as NGR,
    {{ json_extract_scalar('_airbyte_data', ['data','profit'],['profit']) }} as PROFIT,
    {{ json_extract_scalar('_airbyte_data', ['data','revenue_share_profit'],['revenue_share_profit']) }} as REVENUE_SHARE_PROFIT,
    {{ json_extract_scalar('_airbyte_data', ['data','serial_id'],['serial_id']) }} as SERIAL_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','site_id'],['site_id']) }} as SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','transaction_date'],['transaction_date']) }} as TRANSACTION_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','withdrawals'],['withdrawals']) }} as WITHDRAWALS,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('Q_PLATFORM', '_AIRBYTE_RAW_UTM_CODE_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

