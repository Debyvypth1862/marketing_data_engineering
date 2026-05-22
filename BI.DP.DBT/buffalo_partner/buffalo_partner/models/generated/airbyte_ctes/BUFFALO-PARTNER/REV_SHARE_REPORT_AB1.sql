{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BUFFALO_PARTNERS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BUFFALO_PARTNERS', '_AIRBYTE_RAW_REV_SHARE_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['''date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','affiliateId'], ['affiliateId']) }} as AFFILIATE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','brand'], ['brand']) }} as BRAND,
    {{ json_extract_scalar('_airbyte_data', ['data','campaign'], ['campaign']) }} as CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['data','campaignId'], ['campaignId']) }} as CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','currency'], ['currency']) }} as CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['data','dateLastPlayed'], ['dateLastPlayed']) }} as DATE_LAST_PLAYED,
    {{ json_extract_scalar('_airbyte_data', ['data','dateOpened'], ['dateOpened']) }} as DATE_OPENED,
    {{ json_extract_scalar('_airbyte_data', ['data','datefirstDeposited'], ['datefirstDeposited']) }} as DATE_FIRST_DEPOSITED,
    {{ json_extract_scalar('_airbyte_data', ['data','day'], ['day']) }} as DAY,
    {{ json_extract_scalar('_airbyte_data', ['data','deposits'], ['deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','device'], ['device']) }} as DEVICE,
    {{ json_extract_scalar('_airbyte_data', ['data','earnings'], ['earnings']) }} as EARNINGS,
    {{ json_extract_scalar('_airbyte_data', ['data','firstDepositAmount'], ['firstDepositAmount']) }} as FIRST_DEPOSIT_AMOUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','generic1'], ['generic1']) }} as GENERIC_1,
    {{ json_extract_scalar('_airbyte_data', ['data','generic5'], ['generic5']) }} as GENERIC_5,
    {{ json_extract_scalar('_airbyte_data', ['data','generic2'], ['generic2']) }} as GENERIC_2,
    {{ json_extract_scalar('_airbyte_data', ['data','generic3'], ['generic3']) }} as GENERIC_3,
    {{ json_extract_scalar('_airbyte_data', ['data','generic4'], ['generic4']) }} as GENERIC_4,
    {{ json_extract_scalar('_airbyte_data', ['data','highRollerAdjusted'], ['highRollerAdjusted']) }} as HIGH_ROLLER_ADJUSTED,
    {{ json_extract_scalar('_airbyte_data', ['data','highRollerAdjustment'], ['highRollerAdjustment']) }} as HIGH_ROLLER_ADJUSTMENT,
    {{ json_extract_scalar('_airbyte_data', ['data','isNewActiveP'], ['isNewActiveP']) }} as IS_NEW_ACTIVE_P,
    {{ json_extract_scalar('_airbyte_data', ['data','isPlayerLocked'], ['isPlayerLocked']) }} as IS_PLAYER_LOCKED,
    {{ json_extract_scalar('_airbyte_data', ['data','media'], ['media']) }} as MEDIA,
    {{ json_extract_scalar('_airbyte_data', ['data','netRevenue'], ['netRevenue']) }} as NET_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['data','numberOfDeposits'], ['numberOfDeposits']) }} as NUMBER_OF_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','playerReference'], ['playerReference']) }} as PLAYER_REFERENCE,
    {{ json_extract_scalar('_airbyte_data', ['data','product'], ['product']) }} as PRODUCT,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BUFFALO_PARTNERS', '_AIRBYTE_RAW_REV_SHARE_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}