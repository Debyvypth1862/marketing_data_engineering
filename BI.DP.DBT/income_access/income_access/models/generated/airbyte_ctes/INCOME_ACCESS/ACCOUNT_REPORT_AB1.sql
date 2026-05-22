{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "INCOME_ACCESS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('INCOME_ACCESS', '_AIRBYTE_RAW_ACCOUNT_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['row','Deposits'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['row','Commissions'], ['Commissions']) }} as COMMISSIONS,
    {{ json_extract_scalar('_airbyte_data', ['row','Bonus'], ['Bonus']) }} as BONUS,
    {{ json_extract_scalar('_airbyte_data', ['row','CPACommission'], ['CPACommission']) }} as CPA_COMMISSIONS,
    {{ json_extract_scalar('_airbyte_data', ['row','Chargebacks'], ['Chargebacks']) }} as CHARGEBACKS,
    {{ json_extract_scalar('_airbyte_data', ['row','Grossrevenue'],['Grossrevenue']) }} as GROSS_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['row','Netrevenue'], ['Netrevenue']) }} as NET_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['row','affcustomid'], ['affcustomid']) }} as AFF_CUSTOM_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','bannerid'], ['bannerid']) }} as BANNER_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','bannertype'], ['bannertype']) }} as BANNER_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['row','cpacommissioncount'], ['cpacommissioncount']) }} as CPA_COMMISSION_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['row','creativename'], ['creativename']) }} as CREATIVE_NAME,
    {{ json_extract_scalar('_airbyte_data', ['row','currencysymbol'], ['currencysymbol']) }} as CURRENCY_SYMBOL,
    {{ json_extract_scalar('_airbyte_data', ['row','firstdeposit'], ['firstdeposit']) }} as FIRST_DEPOSIT,
    {{ json_extract_scalar('_airbyte_data', ['row','memberid'], ['memberid']) }} as MEMBER_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','merchantname'], ['merchantname']) }} as MERCHANT_NAME,
    {{ json_extract_scalar('_airbyte_data', ['row','new'], ['new'] ) }} as NEW,
    {{ json_extract_scalar('_airbyte_data', ['row','playercountry'], ['playercountry']) }} as PLAYER_COUNTRY,
    {{ json_extract_scalar('_airbyte_data', ['row','playerid'], ['newplayerid']) }} as PLAYER_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','registrationdate'], ['registrationdate']) }} as REGISTRATION_DATE,
    {{ json_extract_scalar('_airbyte_data', ['row','rowid'], ['rowid'] ) }} as ROW_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','siteid'], ['siteid'] ) }} as SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['row','stake'], ['stake'] ) }} as STAKE,
    {{ json_extract_scalar('_airbyte_data', ['row','totalcommission'], ['totalcommission'] ) }} as TOTAL_COMMISSION,
    {{ json_extract_scalar('_airbyte_data', ['row','totalrecords'], ['totalrecords'] ) }} as TOTAL_RECORDS,
    {{ json_extract_scalar('_airbyte_data', ['row','username'], ['username'] ) }} as USERNAME,
    case 
        when {{ json_extract_scalar('_airbyte_data', ['Tracker_Login_Id'], ['Tracker_Login_Id']) }} is not null  then  {{ json_extract_scalar('_airbyte_data', ['Tracker_Login_Id'], ['Tracker_Login_Id']) }} 
        else {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }}  
    end as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('INCOME_ACCESS', '_AIRBYTE_RAW_ACCOUNT_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

