{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(DAST_CLICKS as {{ dbt_utils.type_float() }}) as DAST_CLICKS,
        try_cast(DAST_CPA as {{ dbt_utils.type_float() }}) as DAST_CPA,
        try_cast(DAST_DAY as {{ dbt_utils.type_string() }}) as DAST_DAY,
        try_cast(DAST_DEPOSITS as {{ dbt_utils.type_float() }}) as DAST_DEPOSITS,
        try_cast(DAST_FTD as {{ dbt_utils.type_float() }}) as DAST_FTD,
        try_cast(DAST_ID as {{ dbt_utils.type_float() }}) as DAST_ID,
        try_cast(DAST_INCOME as {{ dbt_utils.type_float() }}) as DAST_INCOME,
        try_cast(DAST_MONTH as {{ dbt_utils.type_string() }}) as DAST_MONTH,
        try_cast(DAST_PAYOUT as {{ dbt_utils.type_float() }}) as DAST_PAYOUT,
        try_cast(DAST_PROFIT as {{ dbt_utils.type_float() }}) as DAST_PROFIT,
        try_cast(DAST_SIGNUPS as {{ dbt_utils.type_float() }}) as DAST_SIGNUPS,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('DAILY_STATS_TOTAL_AB1') }}
-- DAILY_STATS_TOTAL
where 1 = 1