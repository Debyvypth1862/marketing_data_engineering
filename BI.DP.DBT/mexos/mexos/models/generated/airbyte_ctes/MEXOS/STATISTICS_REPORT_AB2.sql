{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "MEXOS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('STATISTICS_REPORT_AB1') }}
select
    try_cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(CASINO_NET_GAMING_COMMISSION as {{ dbt_utils.type_float() }}) as CASINO_NET_GAMING_COMMISSION,
    try_cast(CASINO_RFD_AMT as {{ dbt_utils.type_float() }}) as CASINO_RFD_AMT,
    try_cast(CASINO_RFD_CNT as {{ dbt_utils.type_float() }}) as CASINO_RFD_CNT,
    try_cast(CASINO_SIGNUPS_CNT as {{ dbt_utils.type_float() }}) as CASINO_SIGNUPS_CNT,
    try_cast(COMMISSION as {{ dbt_utils.type_float() }}) as COMMISSION,
    try_cast(DEPOSIT_CNT as {{ dbt_utils.type_float() }}) as DEPOSIT_CNT,
    try_cast(IMPRESSIONS as {{ dbt_utils.type_float() }}) as IMPRESSIONS,
    try_cast(NET_GAMING_AFTER_DEDUCTION as {{ dbt_utils.type_float() }}) as NET_GAMING_AFTER_DEDUCTION,
    try_cast(SPORT_NET_GAMING_COMMISSION as {{ dbt_utils.type_float() }}) as SPORT_NET_GAMING_COMMISSION,
    try_cast(SPORT_RFD_AMT as {{ dbt_utils.type_float() }}) as SPORT_RFD_AMT,
    try_cast(SPORT_RFD_CNT as {{ dbt_utils.type_float() }}) as SPORT_RFD_CNT,
    try_cast(SPORT_SIGNUPS_CNT as {{ dbt_utils.type_float() }}) as SPORT_SIGNUPS_CNT,
    try_cast(TOTAL_BONUSES_EUR as {{ dbt_utils.type_float() }}) as TOTAL_BONUSES_EUR,
    try_cast(UNIQUE_CLICKS as {{ dbt_utils.type_float() }}) as UNIQUE_CLICKS,
    try_cast(VAR_9 as {{ dbt_utils.type_string() }}) as VAR_9,
    try_cast(WINS as {{ dbt_utils.type_float() }}) as WINS,
    try_cast(WITHDRAWAL_AMT as {{ dbt_utils.type_float() }}) as WITHDRAWAL_AMT,
    try_cast(WITHDRAWAL_CNT as {{ dbt_utils.type_float() }}) as WITHDRAWAL_CNT,
    try_cast(TOTAL_DEPOSIT_CNT as {{ dbt_utils.type_float() }}) as TOTAL_DEPOSIT_CNT,
    try_cast(TOTAL_DEPOSIT_AMT as {{ dbt_utils.type_float() }}) as TOTAL_DEPOSIT_AMT,
    CASE 
        WHEN VAR_1 IS NOT NULL THEN try_cast(VAR_1 as {{ dbt_utils.type_string() }})
        WHEN VAR_9 IS NOT NULL THEN  try_cast(VAR_9 as {{ dbt_utils.type_string() }})
        WHEN VAR_2 IS NOT NULL THEN  try_cast(VAR_2 as {{ dbt_utils.type_string() }})
        WHEN VAR_10 IS NOT NULL THEN  try_cast(VAR_10 as {{ dbt_utils.type_string() }})
        ELSE NULL
    END as CLICK_ID,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('STATISTICS_REPORT_AB1') }}
-- STATISTICS_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

