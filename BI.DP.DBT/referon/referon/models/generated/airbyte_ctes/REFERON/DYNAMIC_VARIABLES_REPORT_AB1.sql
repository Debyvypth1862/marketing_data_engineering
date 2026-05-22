{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REFERON",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('REFERON', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['data','Click Count'], ['Click Count']) }} as CLICK_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Traffic Type'], ['Traffic Type']) }} as TRAFFIC_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['data','FTDs Deposits'], ['FTDs Deposits']) }} as FTDS_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Banner View Count'], ['Banner View Count']) }} as BANNER_VIEW_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Marketing Source'], ['Marketing Source']) }} as MARKETING_SOURCE,
    {{ json_extract_scalar('_airbyte_data', ['data','Dynamic variables'], ['Dynamic variables']) }} as DYNAMIC_VARIABLES,
    {{ json_extract_scalar('_airbyte_data', ['data','Avg. deposit'], ['Avg. deposit']) }} as AVG_DEPOSIT,
    {{ json_extract_scalar('_airbyte_data', ['data','Program ID'], ['Program ID']) }} as PROGRAM_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Turnover'], ['Turnover']) }} as TURNOVER,
    {{ json_extract_scalar('_airbyte_data', ['data','CPA count'], ['CPA count']) }} as CPA_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Unique Click Count'], ['Unique Click Count']) }} as UNIQUE_CLICK_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposits per FTD'], ['Deposits per FTD']) }} as DEPOSITS_PER_FTD,
    {{ json_extract_scalar('_airbyte_data', ['data','Count of deposits'], ['Count of deposits']) }} as COUNT_OF_DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Brand ID'], ['Brand ID']) }} as BRAND_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Active customers'], ['Active customers']) }} as ACTIVE_CUSTOMERS,
    {{ json_extract_scalar('_airbyte_data', ['data','Deposits'], ['Deposits']) }} as DEPOSITS,
    {{ json_extract_scalar('_airbyte_data', ['data','Reg. to FTD'], ['Reg. to FTD']) }} as REG_TO_FTD,
    {{ json_extract_scalar('_airbyte_data', ['data','Marketing Source ID'], ['Marketing Source ID']) }} as MARKETING_SOURCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Media Item ID'], ['Media Item ID']) }} as MEDIA_ITEM_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','CPC count'], ['CPC count']) }} as CPC_COUNT,
    {{ json_extract_scalar('_airbyte_data', ['data','Geo'], ['Geo']) }} as GEO,
    {{ json_extract_scalar('_airbyte_data', ['data','Customer ID'], ['Customer ID']) }} as CUSTOMER_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','click_id'], ['click_id']) }} as CLICK_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Media Item Name'], ['Media Item Name']) }} as Media_Item_Name,
    {{ json_extract_scalar('_airbyte_data', ['data','NGR'], ['NGR']) }} as NGR,
    {{ json_extract_scalar('_airbyte_data', ['data','pubid'], ['pubid']) }} as PUBID,
    {{ json_extract_scalar('_airbyte_data', ['data','Product ID'], ['Product ID']) }} as Product_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Program Name'], ['Program Name']) }} as Program_Name,
    {{ json_extract_scalar('_airbyte_data', ['data','Media Campaign Name'], ['Media Campaign Name']) }} as Media_Campaign_Name,
    {{ json_extract_scalar('_airbyte_data', ['data','FTD Count'], ['FTD Count']) }} as FTD_Count,
    {{ json_extract_scalar('_airbyte_data', ['data','Rewarding Plan'], ['Rewarding Plan']) }} as Rewarding_Plan,
    {{ json_extract_scalar('_airbyte_data', ['data','Customer Reg. Date'], ['Customer Reg. Date']) }} as Customer_Reg_Date,
    {{ json_extract_scalar('_airbyte_data', ['data','NDC'], ['NDC']) }} as NDC,
    {{ json_extract_scalar('_airbyte_data', ['data','subid'], ['subid']) }} as SUBID,
    {{ json_extract_scalar('_airbyte_data', ['data','var5'], ['var5']) }} as VAR5,
    {{ json_extract_scalar('_airbyte_data', ['data','var4'], ['var4']) }} as VAR4,
    {{ json_extract_scalar('_airbyte_data', ['data','Click to FTD'], ['Click to FTD']) }} as Click_to_FTD,
    {{ json_extract_scalar('_airbyte_data', ['data','RS distribution'], ['RS distribution']) }} as RS_distribution,
    {{ json_extract_scalar('_airbyte_data', ['data','var3'], ['var3']) }} as VAR3,
    {{ json_extract_scalar('_airbyte_data', ['data','var2'], ['var2']) }} as VAR2,
    {{ json_extract_scalar('_airbyte_data', ['data','var1'], ['var1']) }} as VAR1,
    {{ json_extract_scalar('_airbyte_data', ['data','Depositing customers'], ['Depositing customers']) }} as Depositing_customers,
    {{ json_extract_scalar('_airbyte_data', ['data','Product Name'], ['Product Name']) }} as Product_Name,
    {{ json_extract_scalar('_airbyte_data', ['data','Media Campaign ID'], ['Media Campaign ID']) }} as Media_Campaign_ID,
    {{ json_extract_scalar('_airbyte_data', ['data','Total Reward'], ['Total Reward']) }} as Total_Reward,
    {{ json_extract_scalar('_airbyte_data', ['data','clickid'], ['clickid']) }} as CLICKID,
    {{ json_extract_scalar('_airbyte_data', ['data','Revenue Share'], ['Revenue Share']) }} as Revenue_Share,
    {{ json_extract_scalar('_airbyte_data', ['date'], ['date']) }} as DATE,
    {{ json_extract_scalar('_airbyte_data', ['data','Brand Name'], ['Brand Name']) }} as Brand_Name,
    {{ json_extract_scalar('_airbyte_data', ['data','Reg. Count'], ['Reg. Count']) }} as Reg_Count,
    {{ json_extract_scalar('_airbyte_data', ['data','CPA'], ['CPA']) }} as CPA,
    {{ json_extract_scalar('_airbyte_data', ['data','CPC'], ['CPC']) }} as CPC,
    {{ json_extract_scalar('_airbyte_data', ['data','Fixed Fee'], ['Fixed Fee']) }} as Fixed_Fee,
    {{ json_extract_scalar('_airbyte_data', ['tracker_login_id'], ['tracker_login_id']) }} as TRACKER_LOGIN_ID,

    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('REFERON', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMMITTED_DATE >= DATEADD(DAY,-7,CURRENT_DATE)

