{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SOFTSWISS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('SOFTSWISS', '_AIRBYTE_RAW_ACTIVITY_REPORT') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
SELECT
    PIVOT_STEP_3.tracker_login_id,
    PIVOT_STEP_3.start_date,
    PIVOT_STEP_3.end_date,
    PIVOT_STEP_3."'date'" as date,
    PIVOT_STEP_3."'brand_id'" as brand_id,
    PIVOT_STEP_3."'campaign_id'" as campaign_id,
    CASE 
        WHEN PIVOT_STEP_3."'dynamic_tag_clickid'" IS NOT NULL THEN PIVOT_STEP_3."'dynamic_tag_clickid'" 
        ELSE PIVOT_STEP_3."'dynamic_tag_visit_id'"
    END as dynamic_tag_clickid,
    PIVOT_STEP_3."'visits_count'" as visits_count,
    PIVOT_STEP_3."'registrations_count'" as registrations_count,
    PIVOT_STEP_3."'currency'" as currency,
    PIVOT_STEP_3."'ngr'"  as ngr,
    PIVOT_STEP_3."'deposits_sum'" as deposits_sum ,
    PIVOT_STEP_3."'deposits_count'" as deposits_count,
    PIVOT_STEP_3."'first_deposits_count'" as first_deposits_count,
    PIVOT_STEP_3."'first_deposits_sum'" as first_deposits_sum,
    {{ dbt_utils.surrogate_key([
          'PIVOT_STEP_3._AIRBYTE_AB_ID',
          'PIVOT_STEP_3.TRACKER_LOGIN_ID',
        ]) }} as _AIRBYTE_AB_ID,
    PIVOT_STEP_3._AIRBYTE_EMITTED_AT,
    PIVOT_STEP_3.S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT

FROM (
    SELECT 
        RAW._airbyte_ab_id,
        RAW._airbyte_emitted_at,
        RAW.S3_PATH,
        RAW._AIRBYTE_DATA:start_date as start_date,
        RAW._AIRBYTE_DATA:end_date as end_date,
        RAW._AIRBYTE_DATA:tracker_login_id as tracker_login_id,
        TSF_STEP_1.value:name as col_name,
        case
            when TSF_STEP_1.value:type = 'money' then TSF_STEP_1.value:value:amount
            else TSF_STEP_1.value:value
        end as col_value
    FROM 
    {{ source('SOFTSWISS', '_AIRBYTE_RAW_ACTIVITY_REPORT') }} AS RAW
    JOIN s3_status s3
        ON RAW.S3_PATH = s3.PATH,
    TABLE(FLATTEN(input => _airbyte_data:data)) AS TSF_STEP_1
    WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
)
PIVOT(
    MAX(col_value) FOR col_name IN (
        'date',
        'brand_id',
        'campaign_id',
        'dynamic_tag_clickid',
        'dynamic_tag_visit_id',
        'visits_count',
        'registrations_count',
        'currency',
        'ngr',
        'deposits_sum',
        'deposits_count',
        'first_deposits_count',
        'first_deposits_sum'
    )
)
AS PIVOT_STEP_3 
-- ACTIVITY_REPORT
where 1 = 1
    -- and dynamic_tag_clickid is not null 
    -- and dynamic_tag_clickid <> ''
AND PIVOT_STEP_3._AIRBYTE_EMITTED_AT >= DATEADD(DAY,-7,CURRENT_DATE)
