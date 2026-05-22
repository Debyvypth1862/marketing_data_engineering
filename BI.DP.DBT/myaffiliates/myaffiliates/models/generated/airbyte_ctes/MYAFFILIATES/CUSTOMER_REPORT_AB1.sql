{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "MYAFFILIATES",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('MYAFFILIATES', '_AIRBYTE_RAW_CUSTOMER_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }}
)
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Payload'], ['Payload']) }} as PAYLOAD,
    {{ json_extract_scalar('_airbyte_data', ['data','Campaign'], ['Campaign']) }} as CAMPAIGN,
    {{ json_extract_scalar('_airbyte_data', ['data','Campaign group'], ['Campaign group']) }} as CAMPAIGN_GROUP,
    {{ json_extract_scalar('_airbyte_data', ['data','Clicks'], ['Clicks']) }} as CLICKS,
    {{ json_extract_scalar('_airbyte_data', ['data','Hits'], ['Hits']) }} as HITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Customer'], ['Customer']) }} as CUSTOMER,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Deposit'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','First Time Depositors'], ['First Deposit']) }} as FIRST_DEPOSIT,
    {{ json_extract_scalar('_airbyte_data', ['data','First Deposit Count'], ['First Deposit Count']) }} as FIRST_DEPOSIT_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Impressions'], ['Impressions']) }} as IMPRESSIONS,
    {{ json_extract_scalar('_airbyte_data', ['data','Income'], ['Income']) }} as INCOME,
    {{ json_extract_scalar('_airbyte_data', ['data','Media'], ['Media']) }} as MEDIA,
    {{ json_extract_scalar('_airbyte_data', ['data','Net Revenue'], ['Net Revenue']) }} as NET_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['data','Qualified Players'], ['Qualified Players']) }} as QUALIFIED_PLAYERS,
    {{ json_extract_scalar('_airbyte_data', ['data','Signups'], ['Signups']) }} as SIGNUPS,
    {{ json_extract_scalar('_airbyte_data', ['data','billing_title'], ['billing_title']) }} as BILLING_TITLE,
    {{ json_extract_scalar('_airbyte_data', ['data','currencyRate'], ['currencyRate']) }} as CURRENCY_RATE,
    {{ json_extract_scalar('_airbyte_data', ['data','current_subscription'], ['current_subscription']) }} as CURRENT_SUBSCRIPTION,
    {{ json_extract_scalar('_airbyte_data', ['data','customer_group'], ['customer_group']) }} as CUSTOMER_GROUP,
    {{ json_extract_scalar('_airbyte_data', ['data','group_description'], ['group_description']) }} as GROUP_DESCRIPTION,
    {{ json_extract_scalar('_airbyte_data', ['data','linear'], ['linear']) }} as LINEAR,
    {{ json_extract_scalar('_airbyte_data', ['data','plan_id'], ['plan_id']) }} as PLAN_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','sub_end_date'], ['sub_end_date']) }} as SUB_END_DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','subscription'], ['subscription']) }} as SUBSCRIPTION,
    {{ json_extract_scalar('_airbyte_data', ['data','systemCurrency'], ['systemCurrency']) }} as SYSTEMCURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['data','userCurrency'], ['userCurrency']) }} as USERCURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['data','FTD'], ['FTD']) }} as FTD,
    {{ json_extract_scalar('_airbyte_data', ['data','FTD Count'], ['FTD Count']) }} as FTD_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','NDC'], ['NDC']) }} as NDC,
    {{ json_extract_scalar('_airbyte_data', ['data','Bonuses'], ['Bonuses']) }} as BONUSES,
    {{ json_extract_scalar('_airbyte_data', ['data','Admin fee'], ['Admin fee']) }} as ADMIN_FEE,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Net Revenue'], ['Total Net Revenue']) }} as TOTAL_NET_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Deposits'], ['Total Deposits']) }} as TOTAL_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Total P/L'], ['Total P/L']) }} as TOTAL_PL,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Stake'], ['Total Stake']) }} as TOTAL_STAKE,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Valid Turnover'], ['Total Valid Turnover']) }} as TOTAL_VALID_TURNOVER,
     {{ json_extract_scalar('_airbyte_data', ['data','NGR'], ['NGR']) }} as NGR,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('MYAFFILIATES', '_AIRBYTE_RAW_CUSTOMER_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE 1=1
AND s3.IS_PROCESSED = FALSE 
AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= DATEADD(DAY,-7,CURRENT_DATE)

