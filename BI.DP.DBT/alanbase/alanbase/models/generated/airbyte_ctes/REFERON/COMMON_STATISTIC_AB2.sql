{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REFERON",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('COMMON_STATISTIC_AB1') }}
select
    cast(Click_Count as {{ dbt_utils.type_float() }}) as Click_Count ,
    cast(Click_Count as {{ dbt_utils.type_float() }}) as Click_Unique_Count ,
    cast(hold_Count as {{ dbt_utils.type_float() }}) as hold_Count ,
    cast(hold_Payout as {{ dbt_utils.type_float() }}) as hold_Payout ,
    cast(confirmed_Count as {{ dbt_utils.type_float() }}) as confirmed_Count ,
    cast(confirmed_Payout as {{ dbt_utils.type_float() }}) as confirmed_Payout ,
    cast(pending_Count as {{ dbt_utils.type_float() }}) as pendingCount ,
    cast(pending_Payout as {{ dbt_utils.type_float() }}) as pending_Payout ,
    cast(rejected_Count as {{ dbt_utils.type_float() }}) as rejected_Count ,
    cast(rejected_Payout as {{ dbt_utils.type_float() }}) as rejected_Payout,
    cast(total_Count as {{ dbt_utils.type_float() }}) as total_Count ,
    cast(total_Payout as {{ dbt_utils.type_float() }}) as total_Payout ,
    cast(CLICKID as {{ dbt_utils.type_string() }}) as CLICKID,
    cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID ,
    _AIRBYTE_AB_ID,
    cast(_AIRBYTE_EMITTED_AT as TIMESTAMP_TZ(9)) as _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('COMMON_STATISTIC_AB1') }}
-- COMMON_STATISTIC
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
-- AND CLICK_ID IS NOT NULL
