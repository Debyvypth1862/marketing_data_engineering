{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(CURR_DATE as {{ dbt_utils.type_string() }}) as CURR_DATE,
        try_cast(CURR_ID as {{ dbt_utils.type_float() }}) as CURR_ID,
        try_cast(CURR_MONTH as {{ dbt_utils.type_string() }}) as CURR_MONTH,
        try_cast(CURR_NAME as {{ dbt_utils.type_string() }}) as CURR_NAME,
        try_cast(CURR_VALUE as {{ dbt_utils.type_float() }}) as CURR_VALUE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CURRENCY_AB1') }}
-- CURRENCY
where 1 = 1